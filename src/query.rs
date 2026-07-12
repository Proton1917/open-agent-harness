use std::{future::Future, sync::Arc};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::{
    agents::AgentRuntime,
    api::ModelClient,
    compact::{CompactConfig, CompactStats, compact_prompt, continuation_message},
    messages::normalize_for_api,
    permissions::PermissionMode,
    prompt::{permission_mode_section, registered_tools_section},
    tokens::{estimate_messages, rough_token_count},
    tools::{ToolContext, ToolExecutionObserver, ToolRegistry},
    types::{Message, SessionUsage},
};

const MAX_TOOL_ROUNDS: usize = 64;
const MAX_TOOL_CALLS_PER_ROUND: usize = 32;
const MAX_TOOL_CALLS_PER_TURN: usize = 128;
pub type TextDeltaSink = Arc<dyn Fn(&str) + Send + Sync>;
pub type QueryEventSink = Arc<dyn Fn(&QueryEvent) + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryEvent {
    TurnStarted,
    RequestStarted {
        round: usize,
    },
    ToolStarted {
        id: String,
        name: String,
        summary: String,
    },
    ToolFinished {
        id: String,
        name: String,
        preview: String,
        is_error: bool,
        elapsed_ms: u128,
    },
    CompactStarted,
    CompactFinished {
        before_tokens: usize,
        after_tokens: usize,
    },
    TurnFinished,
    TurnInterrupted,
    TurnFailed {
        message: String,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("turn interrupted by user")]
struct TurnInterrupted;

pub struct QueryEngine {
    client: ModelClient,
    pub model: String,
    max_tokens: u32,
    system: String,
    registry: ToolRegistry,
    tool_context: ToolContext,
    pub messages: Vec<Message>,
    pub usage: SessionUsage,
    debug: bool,
    text_delta_sink: Option<TextDeltaSink>,
    event_sink: Option<QueryEventSink>,
    compact_config: CompactConfig,
    pub compaction_count: usize,
}

#[derive(Debug, Clone)]
pub struct TurnResult {
    pub text: String,
    pub new_messages: Vec<Message>,
    pub streamed_text: bool,
    pub compacted: bool,
}

pub struct QueryOptions {
    pub model: String,
    pub max_tokens: u32,
    pub system: String,
    pub messages: Vec<Message>,
    pub debug: bool,
    pub text_delta_sink: Option<TextDeltaSink>,
    pub compact_config: Option<CompactConfig>,
}

impl QueryEngine {
    pub fn new(
        client: ModelClient,
        registry: ToolRegistry,
        tool_context: ToolContext,
        options: QueryOptions,
    ) -> Self {
        if tool_context.agent_depth() == 0 && tool_context.agent_runtime().is_err() {
            let runtime = AgentRuntime::new(
                client.clone(),
                registry.clone(),
                options.model.clone(),
                options.max_tokens,
                options.system.clone(),
                options.debug,
                tool_context.agent_limits(),
            );
            tool_context
                .install_agent_runtime(runtime)
                .expect("root agent runtime initialization must be unique");
        }
        Self {
            client,
            model: options.model,
            max_tokens: options.max_tokens,
            system: options.system,
            registry,
            tool_context,
            messages: options.messages,
            usage: SessionUsage::default(),
            debug: options.debug,
            text_delta_sink: options.text_delta_sink,
            event_sink: None,
            compact_config: options
                .compact_config
                .unwrap_or_else(|| CompactConfig::from_env(options.max_tokens)),
            compaction_count: 0,
        }
    }

    pub async fn run_turn(&mut self, prompt: String) -> Result<TurnResult> {
        self.run_turn_with_cancel(prompt, std::future::pending())
            .await?
            .context("non-interruptible turn ended without a result")
    }

    pub async fn run_turn_interruptible(&mut self, prompt: String) -> Result<Option<TurnResult>> {
        self.run_turn_with_cancel(prompt, async {
            if tokio::signal::ctrl_c().await.is_err() {
                std::future::pending::<()>().await;
            }
        })
        .await
    }

    async fn run_turn_with_cancel<F>(
        &mut self,
        prompt: String,
        cancel: F,
    ) -> Result<Option<TurnResult>>
    where
        F: Future<Output = ()> + Send,
    {
        self.emit(QueryEvent::TurnStarted);
        let message_checkpoint = self.messages.clone();
        let compaction_checkpoint = self.compaction_count;
        let task_checkpoint = self.tool_context.background_task_ids().await;
        let agent_runtime = self.tool_context.agent_runtime().ok();
        let agent_checkpoint = match &agent_runtime {
            Some(runtime) => runtime.background_checkpoint().await,
            None => Default::default(),
        };
        tokio::pin!(cancel);
        let result = tokio::select! {
            result = self.run_turn_inner(prompt) => Some(result),
            () = &mut cancel => None,
        };
        match result {
            None => {
                self.messages = message_checkpoint;
                self.compaction_count = compaction_checkpoint;
                self.tool_context
                    .rollback_background_tasks(&task_checkpoint)
                    .await;
                if let Some(runtime) = agent_runtime {
                    runtime.rollback_new_background(&agent_checkpoint).await;
                }
                self.emit(QueryEvent::TurnInterrupted);
                Ok(None)
            }
            Some(Ok(result)) => {
                self.emit(QueryEvent::TurnFinished);
                Ok(Some(result))
            }
            Some(Err(error)) => {
                self.messages = message_checkpoint;
                self.compaction_count = compaction_checkpoint;
                self.tool_context
                    .rollback_background_tasks(&task_checkpoint)
                    .await;
                if let Some(runtime) = agent_runtime {
                    runtime.rollback_new_background(&agent_checkpoint).await;
                }
                if error.downcast_ref::<TurnInterrupted>().is_some() {
                    self.emit(QueryEvent::TurnInterrupted);
                    Ok(None)
                } else {
                    self.emit(QueryEvent::TurnFailed {
                        message: format!("{error:#}"),
                    });
                    Err(error)
                }
            }
        }
    }

    pub fn set_event_sink(&mut self, event_sink: Option<QueryEventSink>) {
        self.event_sink = event_sink;
    }

    pub fn permission_mode(&self) -> PermissionMode {
        self.tool_context.permissions.effective_mode()
    }

    pub fn set_permission_mode(&self, mode: PermissionMode) -> Result<bool> {
        self.tool_context.permissions.set_session_mode(mode)
    }

    pub fn permission_mode_locked(&self) -> bool {
        self.tool_context.permissions.mode == PermissionMode::Plan
    }

    async fn run_turn_inner(&mut self, prompt: String) -> Result<TurnResult> {
        let hook_outcome = self
            .tool_context
            .hooks()
            .run(
                "UserPromptSubmit",
                None,
                json!({"prompt": &prompt}),
                &self.tool_context.cwd(),
            )
            .await?;
        let prompt = if hook_outcome.additional_context.is_empty() {
            prompt
        } else {
            format!(
                "{prompt}\n\n<user-prompt-hook-context>\n{}\n</user-prompt-hook-context>",
                hook_outcome.additional_context.join("\n")
            )
        };
        let start = self.messages.len();
        self.messages.push(Message::user_text(prompt));
        let mut final_text = String::new();
        let mut streamed_text = false;
        let mut compacted = false;
        let mut tool_call_count = 0usize;

        for round in 0..MAX_TOOL_ROUNDS {
            if !compacted
                && self.compact_config.auto_enabled
                && self.messages.len() >= 2
                && self.estimated_tokens() >= self.compact_config.auto_threshold()
            {
                let stats = self.compact(None).await?;
                compacted = true;
                if self.debug {
                    eprintln!(
                        "[debug] auto compact: {} -> {} estimated tokens",
                        stats.before_tokens, stats.after_tokens
                    );
                }
            }
            if self.debug {
                eprintln!(
                    "[debug] API round {}, messages={}",
                    round + 1,
                    self.messages.len()
                );
            }
            self.emit(QueryEvent::RequestStarted { round: round + 1 });
            let api_messages = normalize_for_api(&self.messages);
            let system = self.effective_system_prompt();
            let message_result = self
                .client
                .messages(
                    &self.model,
                    self.max_tokens,
                    &system,
                    &api_messages,
                    &self.registry.definitions(),
                    self.text_delta_sink.as_deref(),
                )
                .await?;
            streamed_text |= message_result.streamed_text;
            let response = message_result.response;
            if let Some(usage) = &response.usage {
                self.usage.add(usage);
            }
            for block in &response.content {
                if block.get("type").and_then(Value::as_str) != Some("text") {
                    continue;
                }
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    final_text.push_str(text)
                }
            }
            self.messages
                .push(Message::assistant(response.content.clone()));
            let tool_uses = response
                .content
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
                .cloned()
                .collect::<Vec<_>>();
            if tool_uses.is_empty() {
                return Ok(TurnResult {
                    text: final_text,
                    new_messages: if compacted {
                        self.messages.clone()
                    } else {
                        self.messages[start..].to_vec()
                    },
                    streamed_text,
                    compacted,
                });
            }
            if tool_uses.len() > MAX_TOOL_CALLS_PER_ROUND
                || tool_call_count.saturating_add(tool_uses.len()) > MAX_TOOL_CALLS_PER_TURN
            {
                bail!(
                    "工具调用超过每轮 {MAX_TOOL_CALLS_PER_ROUND} 个或每 turn {MAX_TOOL_CALLS_PER_TURN} 个的限制"
                )
            }
            tool_call_count += tool_uses.len();
            let mut calls = Vec::with_capacity(tool_uses.len());
            for use_block in tool_uses {
                let id = use_block
                    .get("id")
                    .and_then(Value::as_str)
                    .context("tool_use 缺少 id")?
                    .to_owned();
                let name = use_block
                    .get("name")
                    .and_then(Value::as_str)
                    .context("tool_use 缺少 name")?
                    .to_owned();
                let input = use_block.get("input").cloned().unwrap_or_else(|| json!({}));
                if self.debug {
                    eprintln!("[debug] tool {name}({input})");
                }
                calls.push((id, name, input));
            }
            let execution_inputs = calls
                .iter()
                .map(|(_, name, input)| (name.clone(), input.clone()))
                .collect::<Vec<_>>();
            let call_events = Arc::new(
                calls
                    .iter()
                    .map(|(id, name, input)| {
                        (id.clone(), name.clone(), self.registry.summary(name, input))
                    })
                    .collect::<Vec<_>>(),
            );
            let observer = self.event_sink.as_ref().map(|event_sink| {
                let started_sink = Arc::clone(event_sink);
                let started_calls = Arc::clone(&call_events);
                let finished_sink = Arc::clone(event_sink);
                let finished_calls = Arc::clone(&call_events);
                ToolExecutionObserver::new(
                    Arc::new(move |index| {
                        if let Some((id, name, summary)) = started_calls.get(index) {
                            started_sink(&QueryEvent::ToolStarted {
                                id: id.clone(),
                                name: name.clone(),
                                summary: summary.clone(),
                            });
                        }
                    }),
                    Arc::new(move |index, output, elapsed| {
                        if let Some((id, name, _)) = finished_calls.get(index) {
                            finished_sink(&QueryEvent::ToolFinished {
                                id: id.clone(),
                                name: name.clone(),
                                preview: output_preview(&output.content),
                                is_error: output.is_error,
                                elapsed_ms: elapsed.as_millis(),
                            });
                        }
                    }),
                )
            });
            let outputs = self
                .registry
                .execute_batch_observed(&self.tool_context, &execution_inputs, observer.as_ref())
                .await;
            if outputs.iter().any(|output| output.interrupted) {
                return Err(TurnInterrupted.into());
            }
            let mut tool_results = Vec::with_capacity(calls.len());
            for ((id, _, _), output) in calls.into_iter().zip(outputs) {
                tool_results.push(json!({
                    "type": "tool_result",
                    "tool_use_id": id,
                    "content": output.content,
                    "is_error": output.is_error,
                }));
            }
            self.messages.push(Message::tool_results(tool_results));
            if response.stop_reason.as_deref() != Some("tool_use") && self.debug {
                eprintln!(
                    "[debug] 响应包含工具调用，但 stop_reason={:?}",
                    response.stop_reason
                );
            }
        }
        bail!("单轮工具调用超过 {MAX_TOOL_ROUNDS} 轮，已停止以避免无限循环")
    }

    pub fn clear(&mut self) {
        self.messages.clear();
    }

    pub fn estimated_tokens(&self) -> usize {
        let messages = estimate_messages(&normalize_for_api(&self.messages));
        let system = rough_token_count(&self.effective_system_prompt(), 4);
        let tools = self
            .registry
            .definitions()
            .iter()
            .map(|tool| rough_token_count(&tool.to_string(), 2))
            .sum::<usize>();
        messages.saturating_add(system).saturating_add(tools)
    }

    pub fn context_status(&self) -> (usize, usize, usize) {
        (
            self.estimated_tokens(),
            self.compact_config.auto_threshold(),
            self.compact_config.effective_window(),
        )
    }

    pub async fn compact(&mut self, custom_instructions: Option<&str>) -> Result<CompactStats> {
        if !self.compact_config.enabled {
            bail!("compaction 已被 HARNESS_DISABLE_COMPACT 禁用")
        }
        if self.messages.len() < 2 {
            bail!("消息不足，至少需要一轮对话才能 compact")
        }
        self.emit(QueryEvent::CompactStarted);
        self.tool_context
            .hooks()
            .run(
                "PreCompact",
                None,
                json!({
                    "message_count": self.messages.len(),
                    "custom_instructions": custom_instructions,
                }),
                &self.tool_context.cwd(),
            )
            .await?;

        let messages_before = self.messages.len();
        let before_tokens = self.estimated_tokens();
        let mut summary_input = normalize_for_api(&self.messages);
        summary_input.push(Message::user_text(compact_prompt(custom_instructions)));
        summary_input = normalize_for_api(&summary_input);

        let system = self.effective_system_prompt();
        let result = self
            .client
            .messages(
                &self.model,
                self.max_tokens.min(20_000),
                &system,
                &summary_input,
                &[],
                None,
            )
            .await?;
        if let Some(usage) = &result.response.usage {
            self.usage.add(usage);
        }
        let raw_summary = result
            .response
            .content
            .iter()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|block| block.get("text").and_then(Value::as_str))
            .collect::<String>();
        if raw_summary.trim().is_empty() {
            bail!("compact endpoint 返回了空摘要")
        }

        self.messages = vec![Message::user_text(continuation_message(&raw_summary))];
        self.compaction_count += 1;
        let stats = CompactStats {
            before_tokens,
            after_tokens: self.estimated_tokens(),
            messages_before,
            messages_after: self.messages.len(),
        };
        let post_hook = self
            .tool_context
            .hooks()
            .run(
                "PostCompact",
                None,
                json!({
                    "before_tokens": stats.before_tokens,
                    "after_tokens": stats.after_tokens,
                    "messages_before": stats.messages_before,
                    "messages_after": stats.messages_after,
                }),
                &self.tool_context.cwd(),
            )
            .await;
        if let (true, Err(error)) = (self.debug, post_hook) {
            eprintln!("[debug] PostCompact hook failed after compaction: {error:#}");
        }
        self.emit(QueryEvent::CompactFinished {
            before_tokens: stats.before_tokens,
            after_tokens: stats.after_tokens,
        });
        Ok(stats)
    }

    fn emit(&self, event: QueryEvent) {
        if let Some(sink) = &self.event_sink {
            sink(&event);
        }
    }

    fn effective_system_prompt(&self) -> String {
        let workspace = self.tool_context.workspace_system_context();
        let permission = permission_mode_section(self.permission_mode());
        let tool_names = self
            .registry
            .definitions()
            .into_iter()
            .filter_map(|definition| definition["name"].as_str().map(ToOwned::to_owned))
            .collect::<Vec<_>>();
        let tools = registered_tools_section(&tool_names);
        match workspace.is_empty() {
            true => format!("{}\n\n{}\n\n{}", self.system, tools, permission),
            false => format!(
                "{}\n\n{}\n\n{}\n\n{}",
                self.system, tools, permission, workspace
            ),
        }
    }

    pub async fn shutdown(&self) {
        self.tool_context.shutdown_background_tasks().await;
        if self.tool_context.agent_depth() == 0 {
            if let Ok(runtime) = self.tool_context.agent_runtime() {
                runtime.shutdown_all().await;
            }
            self.registry.shutdown().await;
        }
    }
}

fn output_preview(content: &str) -> String {
    const MAX_CHARS: usize = 180;
    let line = content
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("")
        .trim();
    let mut preview = line.chars().take(MAX_CHARS).collect::<String>();
    if line.chars().count() > MAX_CHARS {
        preview.push('…');
    }
    preview
}

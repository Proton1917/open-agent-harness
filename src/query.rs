use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::{
    api::ModelClient,
    compact::{CompactConfig, CompactStats, compact_prompt, continuation_message},
    messages::normalize_for_api,
    tokens::estimate_messages,
    tools::{ToolContext, ToolRegistry},
    types::{Message, SessionUsage},
};

const MAX_TOOL_ROUNDS: usize = 64;
pub type TextDeltaSink = Arc<dyn Fn(&str) + Send + Sync>;

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
            compact_config: options
                .compact_config
                .unwrap_or_else(|| CompactConfig::from_env(options.max_tokens)),
            compaction_count: 0,
        }
    }

    pub async fn run_turn(&mut self, prompt: String) -> Result<TurnResult> {
        let start = self.messages.len();
        self.messages.push(Message::user_text(prompt));
        let mut final_text = String::new();
        let mut streamed_text = false;
        let mut compacted = false;

        for round in 0..MAX_TOOL_ROUNDS {
            if !compacted && self.compact_config.should_auto_compact(&self.messages) {
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
            let api_messages = normalize_for_api(&self.messages);
            let message_result = self
                .client
                .messages(
                    &self.model,
                    self.max_tokens,
                    &self.system,
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
                if block.get("type").and_then(Value::as_str) == Some("text")
                    && let Some(text) = block.get("text").and_then(Value::as_str)
                {
                    final_text.push_str(text);
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
            let mut tool_results = Vec::with_capacity(tool_uses.len());
            for use_block in tool_uses {
                let id = use_block
                    .get("id")
                    .and_then(Value::as_str)
                    .context("tool_use 缺少 id")?;
                let name = use_block
                    .get("name")
                    .and_then(Value::as_str)
                    .context("tool_use 缺少 name")?;
                let input = use_block.get("input").cloned().unwrap_or_else(|| json!({}));
                if self.debug {
                    eprintln!("[debug] tool {name}({input})");
                }
                let output = self.registry.execute(&self.tool_context, name, input).await;
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
        estimate_messages(&normalize_for_api(&self.messages))
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

        let messages_before = self.messages.len();
        let before_tokens = self.estimated_tokens();
        let mut summary_input = normalize_for_api(&self.messages);
        summary_input.push(Message::user_text(compact_prompt(custom_instructions)));
        summary_input = normalize_for_api(&summary_input);

        let result = self
            .client
            .messages(
                &self.model,
                self.max_tokens.min(20_000),
                &self.system,
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
        Ok(CompactStats {
            before_tokens,
            after_tokens: self.estimated_tokens(),
            messages_before,
            messages_after: self.messages.len(),
        })
    }
}

pub fn default_system_prompt(cwd: &std::path::Path) -> String {
    format!(
        "You are an open, provider-neutral coding agent. Work directly toward the user's goal and use the available tools whenever they help.\n\
         Current working directory: {}\n\
         Inspect relevant evidence before changing files. Read an existing file completely before Edit or Write. \
         Preserve unrelated work, keep decisions transparent, and verify changes with the project's real build or tests when available. \
         You may propose or implement any technically sound approach within the user's requested scope.",
        cwd.display()
    )
}

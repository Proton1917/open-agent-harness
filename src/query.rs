use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::{
    api::ModelClient,
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
}

#[derive(Debug, Clone)]
pub struct TurnResult {
    pub text: String,
    pub new_messages: Vec<Message>,
    pub streamed_text: bool,
}

pub struct QueryOptions {
    pub model: String,
    pub max_tokens: u32,
    pub system: String,
    pub messages: Vec<Message>,
    pub debug: bool,
    pub text_delta_sink: Option<TextDeltaSink>,
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
        }
    }

    pub async fn run_turn(&mut self, prompt: String) -> Result<TurnResult> {
        let start = self.messages.len();
        self.messages.push(Message::user_text(prompt));
        let mut final_text = String::new();
        let mut streamed_text = false;

        for round in 0..MAX_TOOL_ROUNDS {
            if self.debug {
                eprintln!(
                    "[debug] API round {}, messages={}",
                    round + 1,
                    self.messages.len()
                );
            }
            let message_result = self
                .client
                .messages(
                    &self.model,
                    self.max_tokens,
                    &self.system,
                    &self.messages,
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
                    new_messages: self.messages[start..].to_vec(),
                    streamed_text,
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

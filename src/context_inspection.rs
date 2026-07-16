use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use crate::{
    tokens::{estimate_block, estimate_content, rough_token_count},
    types::{Message, Role},
};

const MAX_TOP_TOOLS: usize = 5;
const MAX_RENDER_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextCategory {
    pub name: String,
    pub tokens: usize,
    pub percentage_tenths: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ToolContextUsage {
    pub name: String,
    pub calls: usize,
    pub call_tokens: usize,
    pub result_tokens: usize,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionSeverity {
    Info,
    Warning,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextSuggestion {
    pub severity: SuggestionSeverity,
    pub title: String,
    pub detail: String,
    pub savings_tokens: Option<usize>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextMemoryStatus {
    pub enabled: bool,
    pub indexed_entries: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContextUsageReport {
    pub model: String,
    pub total_tokens: usize,
    pub max_tokens: usize,
    pub raw_max_tokens: usize,
    pub auto_compact_threshold: usize,
    pub percentage_tenths: usize,
    pub free_before_auto_compact: usize,
    pub auto_compact_reserve: usize,
    pub free_in_window: usize,
    pub categories: Vec<ContextCategory>,
    pub top_tools: Vec<ToolContextUsage>,
    pub suggestions: Vec<ContextSuggestion>,
    pub memory: ContextMemoryStatus,
}

impl ContextUsageReport {
    pub fn analyze(
        model: &str,
        base_system: &str,
        effective_system: &str,
        tool_definitions: &[Value],
        messages: &[Message],
        auto_compact_threshold: usize,
        max_tokens: usize,
    ) -> Self {
        let base_tokens = rough_token_count(base_system, 4);
        let effective_system_tokens = rough_token_count(effective_system, 4);
        let runtime_tokens = effective_system_tokens.saturating_sub(base_tokens);
        let tool_definition_tokens = tool_definitions
            .iter()
            .map(|tool| rough_token_count(&tool.to_string(), 2))
            .sum::<usize>();
        let mut usage = MessageUsage::default();
        usage.consume(messages);
        let raw_categories = [
            ("Base instructions", base_tokens),
            ("Runtime and workspace instructions", runtime_tokens),
            ("Tool definitions", tool_definition_tokens),
            ("User messages", usage.user_tokens),
            ("Assistant messages", usage.assistant_tokens),
            ("Tool calls", usage.tool_call_tokens),
            ("Tool results", usage.tool_result_tokens),
            ("Media and documents", usage.media_tokens),
            ("Thinking", usage.thinking_tokens),
        ];
        let total_tokens = raw_categories
            .iter()
            .map(|(_, tokens)| *tokens)
            .sum::<usize>();
        let denominator = max_tokens.max(1);
        let categories = raw_categories
            .into_iter()
            .filter(|(_, tokens)| *tokens > 0)
            .map(|(name, tokens)| ContextCategory {
                name: name.to_owned(),
                tokens,
                percentage_tenths: percentage_tenths(tokens, denominator),
            })
            .collect::<Vec<_>>();
        let mut top_tools = usage.tools.into_values().collect::<Vec<_>>();
        top_tools.sort_by(|left, right| {
            right
                .call_tokens
                .saturating_add(right.result_tokens)
                .cmp(&left.call_tokens.saturating_add(left.result_tokens))
                .then_with(|| left.name.cmp(&right.name))
        });
        top_tools.truncate(MAX_TOP_TOOLS);
        let free_before_auto_compact = auto_compact_threshold.saturating_sub(total_tokens);
        let auto_compact_reserve = max_tokens.saturating_sub(auto_compact_threshold);
        let free_in_window = max_tokens.saturating_sub(total_tokens);
        let suggestions = suggestions(
            total_tokens,
            max_tokens,
            auto_compact_threshold,
            runtime_tokens,
            tool_definition_tokens,
            usage.tool_result_tokens,
            usage.media_tokens,
        );
        Self {
            model: model.to_owned(),
            total_tokens,
            max_tokens,
            raw_max_tokens: max_tokens,
            auto_compact_threshold,
            percentage_tenths: percentage_tenths(total_tokens, denominator),
            free_before_auto_compact,
            auto_compact_reserve,
            free_in_window,
            categories,
            top_tools,
            suggestions,
            memory: ContextMemoryStatus {
                enabled: false,
                indexed_entries: 0,
            },
        }
    }

    pub fn with_memory(mut self, enabled: bool, indexed_entries: usize) -> Self {
        self.memory = ContextMemoryStatus {
            enabled,
            indexed_entries,
        };
        self
    }
}

#[derive(Default)]
struct MessageUsage {
    user_tokens: usize,
    assistant_tokens: usize,
    tool_call_tokens: usize,
    tool_result_tokens: usize,
    media_tokens: usize,
    thinking_tokens: usize,
    tools: HashMap<String, ToolContextUsage>,
    tool_names: HashMap<String, String>,
}

impl MessageUsage {
    fn consume(&mut self, messages: &[Message]) {
        for message in messages {
            match &message.content {
                Value::Array(blocks) => {
                    for block in blocks {
                        self.consume_block(message.role, block);
                    }
                }
                content => self.add_role_tokens(message.role, estimate_content(content)),
            }
        }
    }

    fn consume_block(&mut self, role: Role, block: &Value) {
        let tokens = estimate_block(block);
        let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
        match block_type {
            "tool_use" | "server_tool_use" | "mcp_tool_use" => {
                self.tool_call_tokens = self.tool_call_tokens.saturating_add(tokens);
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .unwrap_or("unknown")
                    .to_owned();
                if let Some(id) = block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                {
                    self.tool_names.insert(id.to_owned(), name.clone());
                }
                let entry = self.tool_entry(&name);
                entry.calls = entry.calls.saturating_add(1);
                entry.call_tokens = entry.call_tokens.saturating_add(tokens);
            }
            "tool_result" => {
                self.tool_result_tokens = self.tool_result_tokens.saturating_add(tokens);
                let name = block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .and_then(|id| self.tool_names.get(id))
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_owned());
                let entry = self.tool_entry(&name);
                entry.result_tokens = entry.result_tokens.saturating_add(tokens);
            }
            "image" | "document" => {
                self.media_tokens = self.media_tokens.saturating_add(tokens);
            }
            "thinking" | "redacted_thinking" => {
                self.thinking_tokens = self.thinking_tokens.saturating_add(tokens);
            }
            _ => self.add_role_tokens(role, tokens),
        }
    }

    fn add_role_tokens(&mut self, role: Role, tokens: usize) {
        match role {
            Role::User => self.user_tokens = self.user_tokens.saturating_add(tokens),
            Role::Assistant => self.assistant_tokens = self.assistant_tokens.saturating_add(tokens),
        }
    }

    fn tool_entry(&mut self, name: &str) -> &mut ToolContextUsage {
        self.tools
            .entry(name.to_owned())
            .or_insert_with(|| ToolContextUsage {
                name: name.to_owned(),
                calls: 0,
                call_tokens: 0,
                result_tokens: 0,
            })
    }
}

fn percentage_tenths(value: usize, denominator: usize) -> usize {
    value.saturating_mul(1_000).div_ceil(denominator.max(1))
}

fn suggestions(
    total: usize,
    window: usize,
    threshold: usize,
    runtime: usize,
    tools: usize,
    tool_results: usize,
    media: usize,
) -> Vec<ContextSuggestion> {
    let mut output = Vec::new();
    if total >= threshold {
        output.push(ContextSuggestion {
            severity: SuggestionSeverity::Warning,
            title: "Automatic compaction threshold reached".to_owned(),
            detail: "The next model turn may compact older conversation context. Run /compact now to control the continuation summary.".to_owned(),
            savings_tokens: None,
        });
    } else if total.saturating_mul(100) >= window.saturating_mul(80) {
        output.push(ContextSuggestion {
            severity: SuggestionSeverity::Warning,
            title: "Context is nearing capacity".to_owned(),
            detail: "Use /compact before a long tool run, and narrow future reads or searches to the needed ranges.".to_owned(),
            savings_tokens: None,
        });
    }
    let large_result_floor = 10_000usize.max(window.saturating_mul(15) / 100);
    if tool_results >= large_result_floor {
        output.push(ContextSuggestion {
            severity: SuggestionSeverity::Warning,
            title: "Tool results occupy a large context share".to_owned(),
            detail: "Prefer narrower Read offsets, more specific Grep patterns, and bounded command output.".to_owned(),
            savings_tokens: Some(tool_results / 2),
        });
    }
    if media >= 4_000usize.max(window / 10) {
        output.push(ContextSuggestion {
            severity: SuggestionSeverity::Info,
            title: "Media and documents are using substantial context".to_owned(),
            detail: "Attach only the pages or images needed for the current decision.".to_owned(),
            savings_tokens: Some(media / 2),
        });
    }
    if runtime >= 5_000usize.max(window / 10) {
        output.push(ContextSuggestion {
            severity: SuggestionSeverity::Info,
            title: "Workspace instructions are large".to_owned(),
            detail: "Keep AGENTS.md and hook-provided context concise and scoped to durable engineering rules.".to_owned(),
            savings_tokens: Some(runtime / 4),
        });
    }
    if tools >= 5_000usize.max(window / 10) {
        output.push(ContextSuggestion {
            severity: SuggestionSeverity::Info,
            title: "Tool definitions are large".to_owned(),
            detail: "Use deferred tool discovery or --tools when a session needs only a narrow tool set.".to_owned(),
            savings_tokens: Some(tools / 4),
        });
    }
    output
}

pub fn render_context_report(report: &ContextUsageReport, terminal_width: usize) -> String {
    let width = terminal_width.clamp(48, 160);
    let bar_width = width.saturating_sub(26).clamp(12, 48);
    let filled = report
        .percentage_tenths
        .saturating_mul(bar_width)
        .div_ceil(1_000)
        .min(bar_width);
    let bar = format!("{}{}", "█".repeat(filled), "░".repeat(bar_width - filled));
    let mut lines = vec![
        "Context usage".to_owned(),
        format!(
            "[{bar}] {} / {} tokens ({}.{:01}%) · {}",
            format_tokens(report.total_tokens),
            format_tokens(report.max_tokens),
            report.percentage_tenths / 10,
            report.percentage_tenths % 10,
            report.model
        ),
        String::new(),
        "Estimated usage by category".to_owned(),
    ];
    for category in &report.categories {
        lines.push(format!(
            "  {:<36} {:>8}  {:>5}.{:01}%",
            truncate_label(&category.name, 36),
            format_tokens(category.tokens),
            category.percentage_tenths / 10,
            category.percentage_tenths % 10
        ));
    }
    lines.push(format!(
        "  {:<36} {:>8}",
        "Free before auto-compact",
        format_tokens(report.free_before_auto_compact)
    ));
    lines.push(format!(
        "  {:<36} {:>8}",
        "Auto-compact reserve",
        format_tokens(report.auto_compact_reserve)
    ));
    lines.push(format!(
        "  {:<36} {:>8}",
        "Workspace memory",
        if report.memory.enabled {
            format!("{} indexed", report.memory.indexed_entries)
        } else {
            "disabled".to_owned()
        }
    ));
    if !report.top_tools.is_empty() {
        lines.push(String::new());
        lines.push("Top tools in context".to_owned());
        for tool in &report.top_tools {
            lines.push(format!(
                "  {:<28} calls {:>7} · results {:>7}",
                truncate_label(&tool.name, 28),
                format_tokens(tool.call_tokens),
                format_tokens(tool.result_tokens)
            ));
        }
    }
    if !report.suggestions.is_empty() {
        lines.push(String::new());
        lines.push("Suggestions".to_owned());
        for suggestion in &report.suggestions {
            let marker = match suggestion.severity {
                SuggestionSeverity::Info => "i",
                SuggestionSeverity::Warning => "!",
            };
            let savings = suggestion
                .savings_tokens
                .map(|tokens| format!(" · save ~{}", format_tokens(tokens)))
                .unwrap_or_default();
            lines.push(format!("  [{marker}] {}{savings}", suggestion.title));
            lines.push(format!("      {}", suggestion.detail));
        }
    }
    let mut rendered = lines.join("\n");
    if rendered.len() > MAX_RENDER_BYTES {
        rendered.truncate(MAX_RENDER_BYTES);
    }
    rendered
}

fn format_tokens(tokens: usize) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}m", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn truncate_label(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    value
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>()
        + "…"
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn report_partitions_every_estimated_token_and_associates_tool_results() {
        let messages = vec![
            Message::user_text("inspect this"),
            Message::assistant(vec![
                json!({"type":"text","text":"checking"}),
                json!({"type":"tool_use","id":"read-1","name":"Read","input":{"file_path":"src/lib.rs"}}),
            ]),
            Message::tool_results(vec![json!({
                "type":"tool_result","tool_use_id":"read-1","content":"file body"
            })]),
        ];
        let tools = vec![json!({"name":"Read","input_schema":{"type":"object"}})];
        let report = ContextUsageReport::analyze(
            "model",
            "base",
            "base\n\nruntime",
            &tools,
            &messages,
            8_000,
            10_000,
        );
        assert_eq!(
            report.total_tokens,
            report
                .categories
                .iter()
                .map(|category| category.tokens)
                .sum::<usize>()
        );
        assert_eq!(report.top_tools[0].name, "Read");
        assert_eq!(report.top_tools[0].calls, 1);
        assert!(report.top_tools[0].result_tokens > 0);
    }

    #[test]
    fn near_capacity_and_large_results_emit_bounded_actionable_suggestions() {
        let messages = vec![Message::tool_results(vec![json!({
            "type":"tool_result","tool_use_id":"large","content":"x".repeat(48_000)
        })])];
        let report =
            ContextUsageReport::analyze("model", "base", "base", &[], &messages, 10_000, 12_000);
        assert!(
            report
                .suggestions
                .iter()
                .any(|item| item.severity == SuggestionSeverity::Warning)
        );
        let rendered = render_context_report(&report, 80);
        assert!(rendered.contains("Context usage"));
        assert!(rendered.contains("Tool results"));
        assert!(rendered.len() < MAX_RENDER_BYTES);
    }
}

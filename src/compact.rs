use std::env;

use crate::{tokens::estimate_messages, types::Message};

pub const SUMMARY_OUTPUT_RESERVE: usize = 20_000;
pub const AUTO_COMPACT_BUFFER: usize = 13_000;

#[derive(Debug, Clone, Copy)]
pub struct CompactConfig {
    pub enabled: bool,
    pub auto_enabled: bool,
    pub context_window: usize,
    pub max_output_tokens: usize,
}

impl CompactConfig {
    pub fn from_env(max_output_tokens: u32) -> Self {
        let context_window = positive_usize("HARNESS_CONTEXT_WINDOW").unwrap_or(200_000);
        Self {
            enabled: !truthy("HARNESS_DISABLE_COMPACT"),
            auto_enabled: !truthy("HARNESS_DISABLE_COMPACT")
                && !truthy("HARNESS_DISABLE_AUTO_COMPACT"),
            context_window,
            max_output_tokens: max_output_tokens as usize,
        }
    }

    pub fn effective_window(self) -> usize {
        self.context_window
            .saturating_sub(self.max_output_tokens.min(SUMMARY_OUTPUT_RESERVE))
    }

    pub fn auto_threshold(self) -> usize {
        let default = self.effective_window().saturating_sub(AUTO_COMPACT_BUFFER);
        let Some(percent) = env::var("HARNESS_AUTO_COMPACT_PCT")
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| *value > 0.0 && *value <= 100.0)
        else {
            return default;
        };
        default.min((self.effective_window() as f64 * percent / 100.0).floor() as usize)
    }

    pub fn should_auto_compact(self, messages: &[Message]) -> bool {
        self.auto_enabled
            && messages.len() >= 2
            && estimate_messages(messages) >= self.auto_threshold()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactStats {
    pub before_tokens: usize,
    pub after_tokens: usize,
    pub messages_before: usize,
    pub messages_after: usize,
}

pub fn compact_prompt(custom_instructions: Option<&str>) -> String {
    let mut prompt = String::from(
        r#"Summarize the conversation so another coding agent can continue the work without access to earlier messages.

Respond with exactly two XML sections:
<analysis>A brief private inventory of the conversation.</analysis>
<summary>
1. Primary Request and Intent
2. Key Technical Concepts
3. Files and Code Sections
4. Errors and Fixes
5. Problem Solving and Decisions
6. User Messages and Constraints
7. Pending Tasks
8. Current Work
9. Verification Evidence
10. Context for Continuing Work
</summary>

Preserve exact file paths, commands, error messages, identifiers, decisions, incomplete work, and verification results. Do not call tools. Return plain text only."#,
    );
    if let Some(instructions) = custom_instructions.filter(|value| !value.trim().is_empty()) {
        prompt.push_str("\n\nAdditional instructions:\n");
        prompt.push_str(instructions.trim());
    }
    prompt
}

pub fn format_summary(raw: &str) -> String {
    let without_analysis = remove_tagged_section(raw, "analysis");
    let summary = extract_tagged_section(&without_analysis, "summary")
        .map(|value| format!("Summary:\n{}", value.trim()))
        .unwrap_or_else(|| without_analysis.trim().to_owned());
    collapse_blank_lines(&summary)
}

pub fn continuation_message(summary: &str) -> String {
    format!(
        "This session continues from an earlier conversation that reached its context limit. The summary below contains the prior state.\n\n{}\n\nContinue directly from the recorded current work. Do not greet the user, recap the summary, or ask what to do next unless a required decision is genuinely missing.",
        format_summary(summary)
    )
}

fn positive_usize(name: &str) -> Option<usize> {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
}

fn truthy(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn remove_tagged_section(input: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let Some(start) = input.find(&open) else {
        return input.to_owned();
    };
    let Some(relative_end) = input[start + open.len()..].find(&close) else {
        return input.to_owned();
    };
    let end = start + open.len() + relative_end + close.len();
    format!("{}{}", &input[..start], &input[end..])
}

fn extract_tagged_section(input: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = input.find(&open)? + open.len();
    let end = start + input[start..].find(&close)?;
    Some(input[start..end].to_owned())
}

fn collapse_blank_lines(input: &str) -> String {
    let mut output = String::new();
    let mut previous_blank = false;
    for line in input.lines() {
        let blank = line.trim().is_empty();
        if blank && previous_blank {
            continue;
        }
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(line.trim_end());
        previous_blank = blank;
    }
    output.trim().to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_analysis_and_formats_summary() {
        let raw = "<analysis>draft</analysis>\n<summary>state\n\n\nnext</summary>";
        assert_eq!(format_summary(raw), "Summary:\nstate\n\nnext");
    }

    #[test]
    fn threshold_reserves_output_and_buffer() {
        let config = CompactConfig {
            enabled: true,
            auto_enabled: true,
            context_window: 200_000,
            max_output_tokens: 16_384,
        };
        assert_eq!(config.effective_window(), 183_616);
        assert_eq!(config.auto_threshold(), 170_616);
    }
}

use anyhow::{Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::cron::{
    MAX_CRON_PROMPT_BYTES, MAX_WAKEUP_DELAY_SECONDS, MAX_WAKEUP_REASON_BYTES,
    MIN_WAKEUP_DELAY_SECONDS, ScheduleWakeupOutcome, ScheduleWakeupRequest,
};

use super::{Tool, ToolContext, ToolOutput, parse_input};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct WakeupInput {
    delay_seconds: Option<f64>,
    scheduled_for: Option<i64>,
    reason: Option<String>,
    prompt: Option<String>,
    #[serde(default)]
    stop: bool,
}

pub struct ScheduleWakeupTool;

#[async_trait]
impl Tool for ScheduleWakeupTool {
    fn name(&self) -> &str {
        "ScheduleWakeup"
    }

    fn description(&self) -> &str {
        "Schedules the next session-scoped dynamic-pacing wakeup, atomically replacing any prior dynamic wakeup. Use reference-compatible delaySeconds/reason/prompt, or the provider-neutral scheduledFor epoch-ms absolute extension. delaySeconds and scheduledFor are mutually exclusive. stop:true must be the only field and cancels dynamic wakeups without touching fixed CronCreate jobs."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "delaySeconds": {
                    "type": "number",
                    "description": format!(
                        "Seconds from now. Finite values are rounded and clamped to {MIN_WAKEUP_DELAY_SECONDS}..={MAX_WAKEUP_DELAY_SECONDS}."
                    )
                },
                "scheduledFor": {
                    "type": "integer",
                    "minimum": 0,
                    "description": format!(
                        "Provider-neutral absolute extension: epoch milliseconds, strictly {MIN_WAKEUP_DELAY_SECONDS}..={MAX_WAKEUP_DELAY_SECONDS} seconds from now."
                    )
                },
                "reason": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_WAKEUP_REASON_BYTES,
                    "description": "One short sentence explaining the chosen pacing delay."
                },
                "prompt": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_CRON_PROMPT_BYTES,
                    "description": "The session prompt to submit when this one-shot wakeup fires."
                },
                "stop": {
                    "type": "boolean",
                    "description": "true ends the dynamic loop and must be supplied without any other field."
                }
            },
            "additionalProperties": false,
            "oneOf": [
                {
                    "required": ["stop"],
                    "properties": {"stop": {"const": true}},
                    "not": {"anyOf": [
                        {"required": ["delaySeconds"]},
                        {"required": ["scheduledFor"]},
                        {"required": ["reason"]},
                        {"required": ["prompt"]}
                    ]}
                },
                {
                    "required": ["delaySeconds", "reason", "prompt"],
                    "properties": {"stop": {"const": false}},
                    "not": {"required": ["scheduledFor"]}
                },
                {
                    "required": ["scheduledFor", "reason", "prompt"],
                    "properties": {"stop": {"const": false}},
                    "not": {"required": ["delaySeconds"]}
                }
            ]
        })
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, input: &Value) -> bool {
        input.get("stop").and_then(Value::as_bool) == Some(true)
    }

    fn summary(&self, input: &Value) -> String {
        if input.get("stop").and_then(Value::as_bool) == Some(true) {
            return "stop dynamic wakeups".to_owned();
        }
        let schedule = input
            .get("delaySeconds")
            .map(|value| format!("in {value}s"))
            .or_else(|| input.get("scheduledFor").map(|value| format!("at {value}")))
            .unwrap_or_else(|| "<missing schedule>".to_owned());
        let reason = input
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("<missing reason>");
        let mut preview = reason.chars().take(80).collect::<String>();
        if reason.chars().count() > 80 {
            preview.push('…');
        }
        format!("{schedule}: {preview}")
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if context.agent_depth() > 0 {
            bail!("subagent 不得操作 root session 的 dynamic wakeup slot")
        }
        let input: WakeupInput = parse_input(input)?;
        let outcome = context
            .cron_service()
            .schedule_wakeup(ScheduleWakeupRequest {
                delay_seconds: input.delay_seconds,
                scheduled_for_ms: input.scheduled_for,
                reason: input.reason,
                prompt: input.prompt,
                stop: input.stop,
            })?;
        match outcome {
            ScheduleWakeupOutcome::Scheduled {
                job,
                replaced_wakeups,
            } => Ok(ToolOutput::success(format!(
                "Next dynamic wakeup {} scheduled for {} ({}s; clamped={}; replaced={}). Nothing more is scheduled after it unless that turn calls ScheduleWakeup again.",
                job.id,
                job.scheduled_for_ms,
                job.clamped_delay_seconds,
                job.was_clamped,
                replaced_wakeups,
            ))),
            ScheduleWakeupOutcome::Stopped { cancelled_wakeups } => {
                Ok(ToolOutput::success(format!(
                    "Dynamic loop stopped; cancelled {cancelled_wakeups} pending wakeup(s). Fixed CronCreate jobs were not changed."
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::ToolRegistry,
    };

    #[test]
    fn schema_is_closed_and_encodes_the_exclusive_modes() {
        let tool = ScheduleWakeupTool;
        assert_eq!(tool.input_schema()["additionalProperties"], false);
        assert_eq!(tool.input_schema()["oneOf"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn scheduling_uses_the_normal_permission_path_and_strict_schema() {
        let temp = tempfile::tempdir().unwrap();
        let denied = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Default, false, vec![], vec![]),
        );
        let registry = ToolRegistry::default();
        let output = registry
            .execute(
                &denied,
                "ScheduleWakeup",
                json!({
                    "delaySeconds": 60,
                    "reason": "check build",
                    "prompt": "continue build check"
                }),
            )
            .await;
        assert!(output.is_error);
        assert!(denied.cron_service().current_wakeup().unwrap().is_none());

        let allowed = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::BypassPermissions, false, vec![], vec![]),
        );
        let malformed = registry
            .execute(
                &allowed,
                "ScheduleWakeup",
                json!({
                    "delaySeconds": 60,
                    "scheduledFor": 1,
                    "reason": "bad",
                    "prompt": "bad"
                }),
            )
            .await;
        assert!(malformed.is_error);
        assert!(allowed.cron_service().current_wakeup().unwrap().is_none());

        let scheduled = registry
            .execute(
                &allowed,
                "ScheduleWakeup",
                json!({
                    "delaySeconds": 60,
                    "reason": "continue integration check",
                    "prompt": "resume integration check"
                }),
            )
            .await;
        assert!(!scheduled.is_error, "{}", scheduled.content);
        assert!(allowed.cron_service().current_wakeup().unwrap().is_some());
        let listed = registry.execute(&allowed, "CronList", json!({})).await;
        assert!(!listed.is_error, "{}", listed.content);
        assert!(listed.content.contains("dynamic wakeup"));
        let stopped = registry
            .execute(&allowed, "ScheduleWakeup", json!({"stop": true}))
            .await;
        assert!(!stopped.is_error, "{}", stopped.content);
        assert!(allowed.cron_service().current_wakeup().unwrap().is_none());
    }
}

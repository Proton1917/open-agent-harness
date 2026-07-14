use anyhow::{Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::cron::{
    CronCreateRequest, MAX_CRON_EXPRESSION_BYTES, MAX_CRON_PROMPT_BYTES, RECURRING_MAX_AGE_MS,
};

use super::{Tool, ToolContext, ToolOutput, object_schema, parse_input};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateInput {
    cron: String,
    prompt: String,
    #[serde(default = "default_true")]
    recurring: bool,
    #[serde(default)]
    durable: bool,
}

fn default_true() -> bool {
    true
}

pub struct CronCreateTool;

#[async_trait]
impl Tool for CronCreateTool {
    fn name(&self) -> &str {
        "CronCreate"
    }

    fn description(&self) -> &str {
        "Schedules a provider-neutral prompt using a strict local-time 5-field cron expression. Jobs are session-only unless durable=true is explicitly requested."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "cron": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_CRON_EXPRESSION_BYTES,
                    "description": "Local-time 5-field cron: minute hour day-of-month month day-of-week."
                },
                "prompt": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_CRON_PROMPT_BYTES,
                    "description": "Prompt placed into the root session when the schedule fires."
                },
                "recurring": {
                    "type": "boolean",
                    "description": "true by default; false fires once and auto-deletes."
                },
                "durable": {
                    "type": "boolean",
                    "description": "false by default; true persists privately across harness restarts."
                }
            }),
            &["cron", "prompt"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        let cron = input
            .get("cron")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let prompt = input
            .get("prompt")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let mut preview = prompt.chars().take(80).collect::<String>();
        if prompt.chars().count() > 80 {
            preview.push('…');
        }
        format!("{cron}: {preview}")
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if context.agent_depth() > 0 {
            bail!("subagent 不得操作 root session 的 scheduled jobs")
        }
        let input: CreateInput = parse_input(input)?;
        let job = context.cron_service().create(CronCreateRequest {
            cron: input.cron,
            prompt: input.prompt,
            recurring: input.recurring,
            durable: input.durable,
        })?;
        let lifetime = if job.recurring {
            format!(
                "auto-expires after {} days",
                RECURRING_MAX_AGE_MS / (24 * 60 * 60 * 1_000)
            )
        } else {
            "fires once then auto-deletes".to_owned()
        };
        let storage = if job.durable {
            "private durable store"
        } else {
            "this session only"
        };
        Ok(ToolOutput::success(format!(
            "Scheduled job {} ({}) at {} [{}; {}; next={}]. Use CronDelete to cancel.",
            job.id,
            if job.recurring {
                "recurring"
            } else {
                "one-shot"
            },
            job.human_schedule,
            storage,
            lifetime,
            job.next_fire_at_ms
        )))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteInput {
    id: String,
}

pub struct CronDeleteTool;

#[async_trait]
impl Tool for CronDeleteTool {
    fn name(&self) -> &str {
        "CronDelete"
    }

    fn description(&self) -> &str {
        "Cancels a session-only or durable scheduled prompt by its job ID."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "id": {
                    "type": "string",
                    "pattern": "^[0-9A-Fa-f]{8}$",
                    "description": "Eight-character job ID returned by CronCreate."
                }
            }),
            &["id"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, _: &Value) -> bool {
        true
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if context.agent_depth() > 0 {
            bail!("subagent 不得操作 root session 的 scheduled jobs")
        }
        let input: DeleteInput = parse_input(input)?;
        if !context.cron_service().delete(&input.id)? {
            bail!("没有 ID 为 {} 的 scheduled job", input.id)
        }
        Ok(ToolOutput::success(format!(
            "Cancelled scheduled job {}.",
            input.id
        )))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListInput {}

pub struct CronListTool;

#[async_trait]
impl Tool for CronListTool {
    fn name(&self) -> &str {
        "CronList"
    }

    fn description(&self) -> &str {
        "Lists bounded session-only and durable scheduled prompts."
    }

    fn input_schema(&self) -> Value {
        object_schema(json!({}), &[])
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        true
    }

    fn summary(&self, _: &Value) -> String {
        "scheduled jobs".to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if context.agent_depth() > 0 {
            bail!("subagent 不得读取 root session 的 scheduled jobs")
        }
        let _: ListInput = parse_input(input)?;
        let cron = context.cron_service();
        let jobs = cron.list()?;
        let wakeup = cron.current_wakeup()?;
        if jobs.is_empty() && wakeup.is_none() {
            return Ok(ToolOutput::success("No scheduled jobs."));
        }
        let mut lines =
            Vec::with_capacity(jobs.len().saturating_add(usize::from(wakeup.is_some())));
        for job in jobs {
            let mut prompt = job.prompt.chars().take(120).collect::<String>();
            if job.prompt.chars().count() > 120 {
                prompt.push('…');
            }
            lines.push(format!(
                "{} — {} ({}, {}) next={}: {}",
                job.id,
                job.human_schedule,
                if job.recurring {
                    "recurring"
                } else {
                    "one-shot"
                },
                if job.durable {
                    "durable"
                } else {
                    "session-only"
                },
                job.next_fire_at_ms,
                prompt
            ));
        }
        if let Some(job) = wakeup {
            let mut prompt = job.prompt.chars().take(120).collect::<String>();
            if job.prompt.chars().count() > 120 {
                prompt.push('…');
            }
            lines.push(format!(
                "{} — dynamic wakeup (session-only, one-shot) next={}: {}",
                job.id, job.scheduled_for_ms, prompt
            ));
        }
        Ok(ToolOutput::success(lines.join("\n")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::ToolRegistry,
    };

    #[tokio::test]
    async fn schemas_are_strict_and_mutation_is_permission_gated() {
        let temp = tempfile::tempdir().unwrap();
        let denied = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Default, false, vec![], vec![]),
        );
        let registry = ToolRegistry::default();
        let denied_output = registry
            .execute(
                &denied,
                "CronCreate",
                json!({"cron":"* * * * *", "prompt":"check"}),
            )
            .await;
        assert!(denied_output.is_error);
        assert!(denied.cron_service().list().unwrap().is_empty());

        let malformed = registry
            .execute(
                &denied,
                "CronCreate",
                json!({"cron":"* * * * *", "prompt":"check", "extra":true}),
            )
            .await;
        assert!(malformed.is_error);

        let allowed = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::Default,
                false,
                vec!["CronCreate(*)".into()],
                vec![],
            ),
        );
        let created = registry
            .execute(
                &allowed,
                "CronCreate",
                json!({"cron":"* * * * *", "prompt":"check"}),
            )
            .await;
        assert!(!created.is_error, "{}", created.content);
        assert_eq!(allowed.cron_service().list().unwrap().len(), 1);
    }

    #[test]
    fn definitions_use_closed_object_schemas() {
        for tool in [
            CronCreateTool.api_definition(),
            CronDeleteTool.api_definition(),
            CronListTool.api_definition(),
        ] {
            assert_eq!(tool["input_schema"]["additionalProperties"], false);
        }
    }

    #[tokio::test]
    async fn subagents_cannot_create_list_or_delete_root_schedules() {
        let temp = tempfile::tempdir().unwrap();
        let root = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::BypassPermissions, false, vec![], vec![]),
        );
        let root_job = root
            .cron_service()
            .create(CronCreateRequest {
                cron: "* * * * *".to_owned(),
                prompt: "root-only future prompt".to_owned(),
                recurring: true,
                durable: false,
            })
            .unwrap();
        let child = root.fork_for_agent();
        let registry = ToolRegistry::default();

        let create = registry
            .execute(
                &child,
                "CronCreate",
                json!({"cron":"* * * * *", "prompt":"child injection"}),
            )
            .await;
        let list = registry.execute(&child, "CronList", json!({})).await;
        let delete = registry
            .execute(&child, "CronDelete", json!({"id":root_job.id}))
            .await;

        assert!(create.is_error);
        assert!(list.is_error);
        assert!(delete.is_error);
        let jobs = root.cron_service().list().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].prompt, "root-only future prompt");
    }
}

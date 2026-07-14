use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    permissions::PermissionDecision,
    workflow::{
        DEFAULT_STEP_TIMEOUT_MS, MAX_NESTED_WORKFLOW_STEPS, MAX_STEP_TIMEOUT_MS,
        MAX_WORKFLOW_COMMAND_BYTES, MAX_WORKFLOW_INPUT_BYTES, MAX_WORKFLOW_PARALLELISM,
        MAX_WORKFLOW_STEPS, MAX_WORKFLOW_TIMEOUT_MS, WorkflowDefinition, WorkflowStep,
        validate_workflow,
    },
};

use super::{BashTool, Tool, ToolContext, ToolOutput, command_is_destructive, parse_input};

pub struct RunWorkflowTool;

#[derive(Debug)]
enum Authorization {
    Allowed,
    Denied(String),
    Interrupted,
}

#[async_trait]
impl Tool for RunWorkflowTool {
    fn name(&self) -> &str {
        "RunWorkflow"
    }

    fn description(&self) -> &str {
        "Runs a strict provider-neutral JSON workflow in the background. Steps form a bounded depends_on DAG; ready command steps may run in parallel through the existing Bash permission, sandbox, timeout, and process-tree controls. Use TaskOutput or TaskStop with the returned task ID. Arbitrary JavaScript is never executed."
    }

    fn input_schema(&self) -> Value {
        workflow_schema(false)
    }

    fn validate_input(&self, input: &Value) -> std::result::Result<(), String> {
        super::schema::validate(&self.input_schema(), input)?;
        let size = serde_json::to_vec(input)
            .map_err(|error| format!("workflow JSON 编码失败: {error}"))?
            .len();
        if size > MAX_WORKFLOW_INPUT_BYTES {
            return Err(format!(
                "workflow JSON 超过 {MAX_WORKFLOW_INPUT_BYTES} 字节限制"
            ));
        }
        let definition: WorkflowDefinition = serde_json::from_value(input.clone())
            .map_err(|error| format!("workflow JSON 无效: {error}"))?;
        validate_workflow(&definition).map_err(|error| format!("{error:#}"))
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, input: &Value) -> bool {
        serde_json::from_value::<WorkflowDefinition>(input.clone())
            .is_ok_and(|definition| workflow_is_destructive(&definition))
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        let Ok(definition) = serde_json::from_value::<WorkflowDefinition>(input.clone()) else {
            return "<invalid workflow>".to_owned();
        };
        let commands = collect_commands(&definition)
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        if commands.is_empty() {
            definition.name
        } else {
            format!("{}: {}", definition.name, commands.join(" | "))
        }
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if !context.execution_registry_has_active("Bash") {
            bail!(
                "RunWorkflow command steps require active Bash; include Bash in --tools or the agent tool policy"
            )
        }
        let mut definition: WorkflowDefinition = parse_input(input)?;
        validate_workflow(&definition)?;
        match authorize_commands(context, &mut definition)? {
            Authorization::Allowed => {}
            Authorization::Denied(step) => {
                bail!("workflow command step {step} 被 Bash 权限规则或用户拒绝")
            }
            Authorization::Interrupted => return Ok(ToolOutput::interrupted()),
        }
        // Permission handlers may narrow command text or timeout. Revalidate the
        // complete graph before detaching so no rewritten input crosses limits.
        validate_workflow(&definition)?;
        let name = definition.name.clone();
        let id = context
            .workflow_runtime()
            .launch(definition, context.clone())
            .await?;
        Ok(ToolOutput::success_with_model_content(
            format!(
                "Workflow running in background\ntask_id={id}\nname={name}\nUse TaskOutput or TaskStop with this task_id."
            ),
            json!({
                "status": "async_launched",
                "taskId": id,
                "taskType": "local_workflow",
                "workflowName": name,
            }),
        ))
    }
}

fn workflow_schema(nested: bool) -> Value {
    let step_schema = if nested {
        command_step_schema()
    } else {
        json!({
            "anyOf": [command_step_schema(), nested_step_schema()]
        })
    };
    json!({
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "pattern": "^[A-Za-z0-9_.-]+$"
            },
            "description": {"type": "string", "minLength": 1, "maxLength": 2048},
            "timeout_ms": {"type": "integer", "minimum": 1000, "maximum": MAX_WORKFLOW_TIMEOUT_MS},
            "max_parallel": {"type": "integer", "minimum": 1, "maximum": MAX_WORKFLOW_PARALLELISM},
            "steps": {
                "type": "array",
                "minItems": 1,
                "maxItems": if nested { MAX_NESTED_WORKFLOW_STEPS } else { MAX_WORKFLOW_STEPS },
                "items": step_schema
            }
        },
        "required": ["name", "steps"],
        "additionalProperties": false
    })
}

fn common_step_properties() -> Value {
    json!({
        "id": {
            "type": "string",
            "minLength": 1,
            "maxLength": 64,
            "pattern": "^[A-Za-z0-9_.-]+$"
        },
        "depends_on": {
            "type": "array",
            "maxItems": 32,
            "items": {"type": "string", "minLength": 1, "maxLength": 64}
        },
        "timeout_ms": {"type": "integer", "minimum": 1, "maximum": MAX_STEP_TIMEOUT_MS}
    })
}

fn command_step_schema() -> Value {
    let mut properties = common_step_properties();
    properties.as_object_mut().expect("object").insert(
        "command".to_owned(),
        json!({"type": "string", "minLength": 1, "maxLength": MAX_WORKFLOW_COMMAND_BYTES}),
    );
    json!({
        "type": "object",
        "properties": properties,
        "required": ["id", "command"],
        "additionalProperties": false
    })
}

fn nested_step_schema() -> Value {
    let mut properties = common_step_properties();
    properties
        .as_object_mut()
        .expect("object")
        .insert("workflow".to_owned(), workflow_schema(true));
    json!({
        "type": "object",
        "properties": properties,
        "required": ["id", "workflow"],
        "additionalProperties": false
    })
}

fn workflow_is_destructive(definition: &WorkflowDefinition) -> bool {
    definition.steps.iter().any(|step| {
        step.command.as_deref().is_some_and(command_is_destructive)
            || step
                .workflow
                .as_deref()
                .is_some_and(workflow_is_destructive)
    })
}

fn collect_commands(definition: &WorkflowDefinition) -> Vec<String> {
    let mut commands = Vec::new();
    collect_commands_into(definition, &mut commands);
    commands
}

fn collect_commands_into(definition: &WorkflowDefinition, commands: &mut Vec<String>) {
    for step in &definition.steps {
        if let Some(command) = &step.command {
            let command = if command.len() > 160 {
                let mut end = 159;
                while !command.is_char_boundary(end) {
                    end -= 1;
                }
                format!("{}…", &command[..end])
            } else {
                command.clone()
            };
            commands.push(command);
        }
        if let Some(nested) = &step.workflow {
            collect_commands_into(nested, commands);
        }
    }
}

fn authorize_commands(
    context: &ToolContext,
    definition: &mut WorkflowDefinition,
) -> Result<Authorization> {
    for step in &mut definition.steps {
        if step.command.is_some() {
            match authorize_command(context, step)? {
                Authorization::Allowed => {}
                result => return Ok(result),
            }
        }
        if let Some(nested) = &mut step.workflow {
            match authorize_commands(context, nested)? {
                Authorization::Allowed => {}
                result => return Ok(result),
            }
        }
    }
    Ok(Authorization::Allowed)
}

fn authorize_command(context: &ToolContext, step: &mut WorkflowStep) -> Result<Authorization> {
    let command = step.command.as_deref().context("workflow command 缺失")?;
    let input = json!({
        "command": command,
        "timeout": step.timeout_ms.unwrap_or(DEFAULT_STEP_TIMEOUT_MS),
        "run_in_background": false,
        "description": format!("workflow step {}", step.id),
    });
    let tool_use_id = format!("workflow-permission-{}", Uuid::new_v4());
    let decision = context.permissions.decide_invocation(
        "Bash",
        &input,
        &tool_use_id,
        command,
        false,
        command_is_destructive(command),
        false,
    )?;
    match decision {
        PermissionDecision::Allow => Ok(Authorization::Allowed),
        PermissionDecision::Deny => Ok(Authorization::Denied(step.id.clone())),
        PermissionDecision::Interrupt => Ok(Authorization::Interrupted),
        PermissionDecision::AllowWithUpdatedInput(updated) => {
            BashTool
                .validate_input(&updated)
                .map_err(|error| anyhow::anyhow!("Bash 权限响应修改后的输入无效: {error}"))?;
            if updated
                .get("run_in_background")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                bail!("workflow command 不允许权限响应改为独立 background Bash")
            }
            let updated_command = updated
                .get("command")
                .and_then(Value::as_str)
                .context("Bash 权限响应移除了 command")?;
            if !context.permissions.permits_updated_invocation(
                "Bash",
                updated_command,
                false,
                false,
            ) {
                return Ok(Authorization::Denied(step.id.clone()));
            }
            step.command = Some(updated_command.to_owned());
            step.timeout_ms = updated.get("timeout").and_then(Value::as_u64);
            Ok(Authorization::Allowed)
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::time::Duration;

    #[cfg(unix)]
    use crate::permissions::{PermissionManager, PermissionMode};

    use super::*;
    #[cfg(unix)]
    use crate::tools::ToolRegistry;

    #[cfg(unix)]
    fn context(path: &std::path::Path) -> ToolContext {
        context_with_permissions(
            path,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        )
    }

    #[cfg(unix)]
    fn context_with_permissions(
        path: &std::path::Path,
        permissions: PermissionManager,
    ) -> ToolContext {
        let context = ToolContext::new(path.to_owned(), permissions);
        context
            .set_task_capture_root(path.join(".test-task-captures"))
            .unwrap();
        context
    }

    #[test]
    fn schema_and_validation_are_strict() {
        let tool = RunWorkflowTool;
        assert!(
            tool.validate_input(&json!({
                "name": "valid",
                "steps": [{"id":"one", "command":"true"}]
            }))
            .is_ok()
        );
        assert!(
            tool.validate_input(&json!({
                "name": "valid",
                "steps": [{"id":"one", "command":"true", "extra":true}]
            }))
            .is_err()
        );
        assert!(
            tool.validate_input(&json!({
                "name": "cycle",
                "steps": [
                    {"id":"one", "command":"true", "depends_on":["two"]},
                    {"id":"two", "command":"true", "depends_on":["one"]}
                ]
            }))
            .is_err()
        );

        let too_many = (0..=MAX_WORKFLOW_STEPS)
            .map(|index| json!({"id":format!("step-{index}"), "command":"true"}))
            .collect::<Vec<_>>();
        assert!(
            tool.validate_input(&json!({"name":"too-many", "steps":too_many}))
                .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bash_deny_rules_are_rechecked_before_launch() {
        let temp = tempfile::tempdir().unwrap();
        let context = context_with_permissions(
            temp.path(),
            PermissionManager::new(
                PermissionMode::Default,
                false,
                vec!["RunWorkflow(*)".to_owned()],
                vec!["Bash(*)".to_owned()],
            ),
        );
        let output = ToolRegistry::default()
            .execute(
                &context,
                "RunWorkflow",
                json!({
                    "name":"denied",
                    "steps":[{"id":"write", "command":"touch should-not-exist"}]
                }),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("Bash 权限"));
        assert!(!temp.path().join("should-not-exist").exists());
        assert!(context.workflow_runtime().task_ids().await.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn workflow_runs_dag_and_is_observable_through_task_output() {
        let temp = tempfile::tempdir().unwrap();
        let context = context(temp.path());
        let registry = ToolRegistry::default();
        let launched = registry
            .execute(
                &context,
                "RunWorkflow",
                json!({
                    "name": "dag",
                    "max_parallel": 2,
                    "steps": [
                        {"id":"a", "command":"printf a"},
                        {"id":"b", "command":"printf b"},
                        {"id":"join", "command":"printf joined", "depends_on":["a","b"]}
                    ]
                }),
            )
            .await;
        assert!(!launched.is_error, "{}", launched.content);
        let id = launched.model_content.unwrap()["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let output = registry
            .execute(
                &context,
                "TaskOutput",
                json!({"task_id":id, "block":true, "timeout":10_000}),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("step a completed"));
        assert!(output.content.contains("step b completed"));
        assert!(output.content.contains("step join completed"));
        assert!(output.content.contains("joined"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failure_cancels_dependents_and_nested_workflow_runs() {
        let temp = tempfile::tempdir().unwrap();
        let context = context(temp.path());
        let registry = ToolRegistry::default();
        let failed = registry
            .execute(
                &context,
                "RunWorkflow",
                json!({
                    "name": "failure",
                    "steps": [
                        {"id":"fail", "command":"exit 7"},
                        {"id":"never", "command":"printf should-not-run", "depends_on":["fail"]}
                    ]
                }),
            )
            .await;
        let id = failed.model_content.unwrap()["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let output = registry
            .execute(
                &context,
                "TaskOutput",
                json!({"task_id":id, "block":true, "timeout":10_000}),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("step fail failed"));
        assert!(!output.content.contains("should-not-run"));

        let nested = registry
            .execute(
                &context,
                "RunWorkflow",
                json!({
                    "name":"parent",
                    "steps":[{
                        "id":"child",
                        "workflow":{
                            "name":"nested",
                            "steps":[{"id":"inside", "command":"printf nested-ok"}]
                        }
                    }]
                }),
            )
            .await;
        let id = nested.model_content.unwrap()["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let output = registry
            .execute(
                &context,
                "TaskOutput",
                json!({"task_id":id, "block":true, "timeout":10_000}),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("nested-ok"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn task_stop_cancels_running_workflow() {
        let temp = tempfile::tempdir().unwrap();
        let context = context(temp.path());
        let registry = ToolRegistry::default();
        let launched = registry
            .execute(
                &context,
                "RunWorkflow",
                json!({
                    "name":"stoppable",
                    "steps":[{"id":"sleep", "command":"sleep 30", "timeout_ms":60_000}]
                }),
            )
            .await;
        let id = launched.model_content.unwrap()["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stopped = registry
            .execute(&context, "TaskStop", json!({"task_id":id}))
            .await;
        assert!(!stopped.is_error, "{}", stopped.content);
        assert!(stopped.content.contains("Stopped workflow"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn total_timeout_cancels_running_step() {
        let temp = tempfile::tempdir().unwrap();
        let context = context(temp.path());
        let registry = ToolRegistry::default();
        let launched = registry
            .execute(
                &context,
                "RunWorkflow",
                json!({
                    "name":"bounded",
                    "timeout_ms":1_000,
                    "steps":[{"id":"sleep", "command":"sleep 30", "timeout_ms":60_000}]
                }),
            )
            .await;
        let id = launched.model_content.unwrap()["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let output = registry
            .execute(
                &context,
                "TaskOutput",
                json!({"task_id":id, "block":true, "timeout":5_000}),
            )
            .await;
        assert!(output.is_error);
        assert!(output.content.contains("timed out after 1000ms"));
    }
}

use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    agents::{
        AgentToolPolicy, CustomAgentCatalog,
        team::{MemberSpec, TeamLimits, TeamService},
    },
    hooks::blocking_feedback,
    session::sanitize_transport_text,
};

use super::{Tool, ToolContext, ToolOutput, object_schema, parse_input, schema};

const MAX_TEAM_RESULT_SUMMARY_BYTES: usize = 32 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Input {
    action: String,
    team_id: Option<String>,
    name: Option<String>,
    member_id: Option<String>,
    agent: Option<String>,
    depth: Option<usize>,
    #[serde(default)]
    allowed_tools: Vec<String>,
    #[serde(default)]
    disallowed_tools: Vec<String>,
    task: Option<String>,
    to: Option<String>,
    message: Option<String>,
    after_sequence: Option<u64>,
    maximum: Option<usize>,
    through_sequence: Option<u64>,
    succeeded: Option<bool>,
    summary: Option<String>,
}

pub struct TeamTool {
    custom_agents: CustomAgentCatalog,
}

impl TeamTool {
    pub fn new(custom_agents: CustomAgentCatalog) -> Self {
        Self { custom_agents }
    }

    pub fn into_tool(self) -> Arc<dyn Tool> {
        Arc::new(self)
    }

    fn open(&self, context: &ToolContext, input: &Input) -> Result<(TeamService, Uuid, Uuid)> {
        let team_id = parse_uuid(input.team_id.as_deref(), "teamId")?;
        let team = TeamService::open(&context.cwd(), team_id)?;
        let actor = if context.agent_depth() == 0 {
            team.coordinator_id()
        } else {
            context.bound_team_actor(team_id)?
        };
        context.track_team_mailbox(team.clone(), actor);
        Ok((team, team_id, actor))
    }
}

#[async_trait]
impl Tool for TeamTool {
    fn name(&self) -> &str {
        "Team"
    }

    fn description(&self) -> &str {
        "Coordinates a bounded persistent team of local subagents. Actor identity is bound by the runtime; callers never provide or impersonate actor IDs."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "action":{"type":"string", "enum":[
                    "create", "status", "add_member", "assign", "send", "read",
                    "acknowledge", "finish", "stop", "shutdown", "delete", "gc"
                ]},
                "teamId":{"type":"string", "maxLength":64},
                "name":{"type":"string", "maxLength":128},
                "memberId":{"type":"string", "maxLength":64},
                "agent":{"type":"string", "maxLength":64},
                "depth":{"type":"integer", "minimum":1, "maximum":8},
                "allowedTools":{"type":"array", "maxItems":128, "items":{"type":"string", "maxLength":128}},
                "disallowedTools":{"type":"array", "maxItems":128, "items":{"type":"string", "maxLength":128}},
                "task":{"type":"string", "maxLength":262144},
                "to":{"type":"string", "maxLength":64},
                "message":{"type":"string", "maxLength":262144},
                "afterSequence":{"type":"integer", "minimum":0},
                "maximum":{"type":"integer", "minimum":1, "maximum":256},
                "throughSequence":{"type":"integer", "minimum":0},
                "succeeded":{"type":"boolean"},
                "summary":{"type":"string", "maxLength":262144}
            }),
            &["action"],
        )
    }

    fn validate_input(&self, value: &Value) -> std::result::Result<(), String> {
        schema::validate(&self.input_schema(), value)?;
        let input: Input =
            serde_json::from_value(value.clone()).map_err(|error| error.to_string())?;
        let allowed_fields: &[&str] = match input.action.as_str() {
            "create" => &["action", "name"],
            "status" => &["action", "teamId"],
            "add_member" => &[
                "action",
                "teamId",
                "name",
                "agent",
                "depth",
                "allowedTools",
                "disallowedTools",
            ],
            "assign" => &["action", "teamId", "memberId", "task"],
            "send" => &["action", "teamId", "to", "message"],
            "read" => &["action", "teamId", "afterSequence", "maximum"],
            "acknowledge" => &["action", "teamId", "throughSequence"],
            "finish" => &["action", "teamId", "succeeded", "summary"],
            "stop" => &["action", "teamId", "memberId"],
            "shutdown" => &["action", "teamId"],
            "delete" => &["action", "teamId"],
            "gc" => &["action", "maximum"],
            _ => return Err("未知 Team action".into()),
        };
        if let Some(field) = value.as_object().and_then(|object| {
            object
                .keys()
                .find(|field| !allowed_fields.contains(&field.as_str()))
        }) {
            return Err(format!("Team action {} 不接受字段 {field}", input.action));
        }
        let has_team = input
            .team_id
            .as_ref()
            .is_some_and(|value| !value.is_empty());
        let valid = match input.action.as_str() {
            "create" => {
                input
                    .name
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty())
                    && !has_team
                    && input.member_id.is_none()
                    && input.task.is_none()
            }
            "status" => has_team,
            "add_member" => {
                has_team
                    && input
                        .name
                        .as_ref()
                        .is_some_and(|value| !value.trim().is_empty())
            }
            "assign" => {
                has_team
                    && input.member_id.is_some()
                    && input
                        .task
                        .as_ref()
                        .is_some_and(|value| !value.trim().is_empty())
            }
            "send" => {
                has_team
                    && input.to.is_some()
                    && input
                        .message
                        .as_ref()
                        .is_some_and(|value| !value.trim().is_empty())
            }
            "read" => has_team,
            "acknowledge" => has_team && input.through_sequence.is_some(),
            "finish" => {
                has_team
                    && input.succeeded.is_some()
                    && input
                        .summary
                        .as_ref()
                        .is_some_and(|value| !value.trim().is_empty())
            }
            "stop" => has_team && input.member_id.is_some(),
            "shutdown" => has_team,
            "delete" => has_team,
            "gc" => !has_team,
            _ => unreachable!("action was checked above"),
        };
        if !valid {
            return Err(format!(
                "Team action {} 缺少必需字段或字段组合无效",
                input.action
            ));
        }
        Ok(())
    }

    fn read_only(&self, input: &Value) -> bool {
        matches!(
            input.get("action").and_then(Value::as_str),
            Some("status" | "read")
        )
    }

    fn destructive(&self, input: &Value) -> bool {
        matches!(
            input.get("action").and_then(Value::as_str),
            Some("stop" | "shutdown" | "delete" | "gc")
        )
    }

    fn concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        let action = input
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let target = input
            .get("name")
            .or_else(|| input.get("memberId"))
            .or_else(|| input.get("teamId"))
            .and_then(Value::as_str)
            .unwrap_or("");
        format!("{action} {target}").trim_end().to_owned()
    }

    async fn execute(&self, context: &ToolContext, value: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(value)?;
        let output = match input.action.as_str() {
            "create" => {
                if context.agent_depth() != 0 {
                    bail!("只有根 agent 可以创建 team")
                }
                let team = TeamService::create(
                    &context.cwd(),
                    input.name.as_deref().context("create 缺少 name")?,
                    "coordinator",
                    TeamLimits::default(),
                )?;
                context.track_team_mailbox(team.clone(), team.coordinator_id());
                json!({
                    "teamId":team.id(),
                    "coordinatorId":team.coordinator_id(),
                    "status":team.snapshot(team.coordinator_id())?
                })
            }
            "status" => {
                let (team, team_id, actor) = self.open(context, &input)?;
                if actor == team.coordinator_id() {
                    serde_json::to_value(team.snapshot(actor)?)?
                } else {
                    json!({
                        "teamId":team_id,
                        "coordinatorId":team.coordinator_id(),
                        "actorId":actor,
                        "member":team.member(actor, actor)?
                    })
                }
            }
            "add_member" => {
                let (team, _, actor) = self.open(context, &input)?;
                let requested = AgentToolPolicy {
                    allowed_tools: (!input.allowed_tools.is_empty())
                        .then(|| input.allowed_tools.into_iter().collect::<BTreeSet<_>>()),
                    disallowed_tools: input.disallowed_tools.into_iter().collect(),
                };
                let requested = if let Some(agent) = input.agent.as_deref() {
                    let definition = self
                        .custom_agents
                        .get(agent)
                        .with_context(|| format!("custom agent 不存在: {agent}"))?;
                    AgentToolPolicy::narrow(&definition.tool_policy(), &requested)
                } else {
                    requested
                };
                serde_json::to_value(team.add_member(
                    actor,
                    MemberSpec {
                        name: input.name.context("add_member 缺少 name")?,
                        custom_agent: input.agent,
                        depth: input.depth.unwrap_or(1),
                        requested_policy: requested,
                    },
                    &AgentToolPolicy::default(),
                )?)?
            }
            "assign" => {
                let (team, team_id, actor) = self.open(context, &input)?;
                let member_id = parse_uuid(input.member_id.as_deref(), "memberId")?;
                let task = input.task.as_deref().context("assign 缺少 task")?;
                let created_hook = context
                    .hooks()
                    .run(
                        "TaskCreated",
                        Some(&member_id.to_string()),
                        json!({
                            "task_id":member_id,
                            "task_subject":format!("team assignment for {member_id}"),
                            "task_description":task,
                            "team_id":team_id,
                        }),
                        &context.cwd(),
                    )
                    .await?;
                let assignment = team.assign(actor, member_id, task)?;
                let team_name = team.snapshot(actor)?.name;
                let member_name = assignment.member.name.clone();
                let assignment_task = assignment.prompt.clone();
                let runtime = context.agent_runtime()?;
                let runtime_id = match runtime
                    .start_team_assignment(context, team_id, &assignment)
                    .await
                {
                    Ok(id) => id,
                    Err(error) => {
                        let _ = team.mark_start_failed(actor, member_id);
                        return Err(error);
                    }
                };
                if let Err(error) = team.mark_running(actor, member_id, runtime_id) {
                    let _ = runtime.stop_team_agent(runtime_id).await;
                    let _ = team.mark_start_failed(actor, member_id);
                    return Err(error);
                }
                let completion_team = team.clone();
                let completion_runtime = Arc::clone(&runtime);
                let completion_cwd = context.cwd();
                let completion_hooks = context.hooks();
                tokio::spawn(async move {
                    let result = completion_runtime.wait_team_agent(runtime_id).await;
                    let (mut succeeded, mut summary) = match result {
                        Ok(output) => (
                            !output.is_error,
                            sanitize_team_summary(&output.content, &completion_cwd),
                        ),
                        Err(error) => (
                            false,
                            sanitize_team_summary(&format!("{error:#}"), &completion_cwd),
                        ),
                    };
                    if succeeded {
                        match completion_hooks
                            .run(
                                "TaskCompleted",
                                Some(&member_id.to_string()),
                                json!({
                                    "task_id":member_id,
                                    "task_subject":format!("team assignment for {member_name}"),
                                    "task_description":assignment_task,
                                    "teammate_name":member_name,
                                    "team_name":team_name,
                                }),
                                &completion_cwd,
                            )
                            .await
                        {
                            Ok(outcome) if !outcome.additional_context.is_empty() => {
                                summary.push_str("\n\n[TaskCompleted hook context]\n");
                                summary.push_str(&outcome.additional_context.join("\n"));
                                summary = sanitize_team_summary(&summary, &completion_cwd);
                            }
                            Ok(_) => {}
                            Err(error) => {
                                succeeded = false;
                                summary = format_hook_rejection("TaskCompleted", &error);
                            }
                        }
                    }
                    if succeeded {
                        match completion_hooks
                            .run(
                                "TeammateIdle",
                                Some(&member_name),
                                json!({
                                    "teammate_name":member_name,
                                    "team_name":team_name,
                                    "team_id":team_id,
                                    "member_id":member_id,
                                }),
                                &completion_cwd,
                            )
                            .await
                        {
                            Ok(outcome) if !outcome.additional_context.is_empty() => {
                                summary.push_str("\n\n[TeammateIdle hook context]\n");
                                summary.push_str(&outcome.additional_context.join("\n"));
                                summary = sanitize_team_summary(&summary, &completion_cwd);
                            }
                            Ok(_) => {}
                            Err(error) => {
                                succeeded = false;
                                summary = format_hook_rejection("TeammateIdle", &error);
                            }
                        }
                    }
                    summary = sanitize_team_summary(&summary, &completion_cwd);
                    let _ = completion_team.finish(actor, member_id, succeeded, &summary);
                });
                json!({
                    "memberId":member_id,
                    "runtimeAgentId":runtime_id,
                    "status":"running",
                    "hookContext":created_hook.additional_context,
                })
            }
            "send" => {
                let (team, _, actor) = self.open(context, &input)?;
                serde_json::to_value(team.send(
                    actor,
                    parse_uuid(input.to.as_deref(), "to")?,
                    input.message.as_deref().context("send 缺少 message")?,
                )?)?
            }
            "read" => {
                let (team, _, actor) = self.open(context, &input)?;
                let messages = team.read_mailbox(
                    actor,
                    actor,
                    input.after_sequence.unwrap_or(0),
                    input.maximum.unwrap_or(64),
                )?;
                if let Some(sequence) = messages.iter().map(|message| message.sequence).max() {
                    context.record_team_mailbox_cursor(team.id(), actor, sequence);
                }
                serde_json::to_value(messages)?
            }
            "acknowledge" => {
                let (team, _, actor) = self.open(context, &input)?;
                let through = input
                    .through_sequence
                    .context("acknowledge 缺少 throughSequence")?;
                let removed = team.acknowledge(actor, actor, through)?;
                context.record_team_mailbox_cursor(team.id(), actor, through);
                json!({"removed":removed})
            }
            "finish" => {
                let (team, _, actor) = self.open(context, &input)?;
                if actor == team.coordinator_id() {
                    bail!("coordinator 不能用 finish 冒充 member")
                }
                serde_json::to_value(team.finish(
                    actor,
                    actor,
                    input.succeeded.context("finish 缺少 succeeded")?,
                    input.summary.as_deref().context("finish 缺少 summary")?,
                )?)?
            }
            "stop" => {
                let (team, _, actor) = self.open(context, &input)?;
                let member_id = parse_uuid(input.member_id.as_deref(), "memberId")?;
                let runtime_id = team.stop_member(actor, member_id)?;
                if let Some(runtime_id) = runtime_id {
                    let _ = context.agent_runtime()?.stop_team_agent(runtime_id).await;
                }
                json!({"memberId":member_id, "status":"stopped"})
            }
            "shutdown" => {
                let (team, _, actor) = self.open(context, &input)?;
                let runtime_ids = team.shutdown(actor)?;
                let runtime = context.agent_runtime()?;
                for runtime_id in &runtime_ids {
                    let _ = runtime.stop_team_agent(*runtime_id).await;
                }
                json!({"teamId":team.id(), "stoppedRuntimeAgents":runtime_ids})
            }
            "delete" => {
                let (team, team_id, actor) = self.open(context, &input)?;
                team.delete(actor)?;
                context.untrack_team_mailbox(team_id);
                json!({"teamId":team_id, "deleted":true})
            }
            "gc" => {
                if context.agent_depth() != 0 {
                    bail!("只有根 agent 可以回收 team")
                }
                serde_json::to_value(TeamService::gc_closed(
                    &context.cwd(),
                    input.maximum.unwrap_or(32),
                )?)?
            }
            other => bail!("未知 Team action: {other}"),
        };
        Ok(ToolOutput::success(serde_json::to_string_pretty(&output)?))
    }
}

fn parse_uuid(value: Option<&str>, field: &str) -> Result<Uuid> {
    value
        .with_context(|| format!("缺少 {field}"))?
        .parse()
        .with_context(|| format!("{field} 必须是 UUID"))
}

fn sanitize_team_summary(value: &str, cwd: &std::path::Path) -> String {
    let value = sanitize_transport_text(value, cwd);
    if value.len() <= MAX_TEAM_RESULT_SUMMARY_BYTES {
        return value;
    }
    let mut end = MAX_TEAM_RESULT_SUMMARY_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}\n[truncated by team result limit]", &value[..end])
}

fn format_hook_rejection(event: &str, error: &anyhow::Error) -> String {
    let detail = blocking_feedback(error).unwrap_or_else(|| format!("{error:#}"));
    format!("{event} hook rejected team completion: {detail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_is_not_part_of_the_model_schema_and_actions_are_semantically_checked() {
        let tool = TeamTool::new(CustomAgentCatalog::default());
        let schema = tool.input_schema();
        assert!(schema["properties"].get("actor").is_none());
        assert!(tool.validate_input(&json!({"action":"assign"})).is_err());
        assert!(
            tool.validate_input(&json!({
                "action":"assign", "teamId":Uuid::new_v4(),
                "memberId":Uuid::new_v4(), "task":"audit"
            }))
            .is_ok()
        );
        assert!(
            tool.validate_input(&json!({
                "action":"delete", "teamId":Uuid::new_v4()
            }))
            .is_ok()
        );
        assert!(
            tool.validate_input(&json!({"action":"gc", "maximum":8}))
                .is_ok()
        );
        assert!(
            tool.validate_input(&json!({
                "action":"gc", "teamId":Uuid::new_v4()
            }))
            .is_err()
        );
    }
}

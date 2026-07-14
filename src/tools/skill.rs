use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::skills::{SkillExecutionContext, SkillInvocationSource};

use super::{Tool, ToolContext, ToolOutput, object_schema, parse_input};

#[derive(Deserialize)]
struct Input {
    name: String,
    arguments: Option<String>,
}

pub struct SkillTool;

#[async_trait]
impl Tool for SkillTool {
    fn name(&self) -> &str {
        "Skill"
    }

    fn description(&self) -> &str {
        "Loads a local workflow by name. Project-discovered skills cannot run hooks or grant tool permission; trusted user/plugin execution metadata remains invocation-scoped."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "name": {"type": "string", "maxLength": 64},
                "arguments": {"type": "string", "maxLength": 32768}
            }),
            &["name"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn requires_permission_for(&self, context: &ToolContext, input: &Value) -> bool {
        let Some(name) = input.get("name").and_then(Value::as_str) else {
            return true;
        };
        context.skill(name).is_none_or(|skill| {
            skill.execution_context == SkillExecutionContext::Fork
                || (skill.trust == crate::skills::SkillTrust::Trusted
                    && !skill.allowed_tools.is_empty())
                || skill.model.is_some()
                || skill.agent.is_some()
                || skill.hooks.is_some()
        })
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        let skill = context
            .skill(&input.name)
            .with_context(|| format!("未知 skill: {}", input.name))?;
        let mut invocation = skill.prepare_invocation(
            input.arguments.as_deref().unwrap_or_default(),
            SkillInvocationSource::Model,
        )?;
        let _ = context.trigger_skill_monitors(&skill.name).await;
        let base = skill.path.parent().unwrap_or(&skill.path);
        let base = context.display_path(base);
        let prompt = format!(
            "<skill name=\"{}\" base=\"{}\">\n{}\n</skill>",
            skill.name, base, invocation.prompt
        );
        if invocation.execution_context == SkillExecutionContext::Fork {
            let mut scoped = context.clone();
            if invocation.trusted_execution_metadata && !invocation.allowed_tools.is_empty() {
                let rules = invocation.allowed_tools.iter().cloned().collect::<Vec<_>>();
                scoped.permissions =
                    std::sync::Arc::new(context.permissions.with_scoped_allow(&rules)?);
            }
            if let Some(hooks) = &invocation.hooks {
                scoped.set_hooks(std::sync::Arc::new(
                    context.hooks().with_scoped_hooks(hooks)?,
                ));
            }
            return context
                .agent_runtime()?
                .run_skill(
                    &scoped,
                    &skill.name,
                    prompt,
                    invocation.agent,
                    invocation.model,
                    skill.allowed_tool_names()?,
                )
                .await;
        }
        invocation.prompt = prompt;
        let content = invocation.prompt.clone();
        Ok(ToolOutput::success_with_skill_invocation(
            content, invocation,
        ))
    }
}

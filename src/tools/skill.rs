use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

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
        "Loads a user-provided local workflow by name. Loading is read-only and never executes bundled scripts automatically."
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
        true
    }

    fn requires_permission(&self) -> bool {
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
        let base = skill.path.parent().unwrap_or(&skill.path);
        let base = context.display_path(base);
        let arguments = input
            .arguments
            .filter(|value| !value.trim().is_empty())
            .map(|value| format!("\n\nArguments supplied by the caller:\n{}", value.trim()))
            .unwrap_or_default();
        Ok(ToolOutput::success(format!(
            "<skill name=\"{}\" base=\"{}\">\n{}{}\n</skill>",
            skill.name, base, skill.content, arguments
        )))
    }
}

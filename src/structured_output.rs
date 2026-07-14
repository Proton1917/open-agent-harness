use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use jsonschema::Validator;
use serde_json::Value;

use crate::tools::{Tool, ToolContext, ToolOutput};

pub const STRUCTURED_OUTPUT_TOOL_NAME: &str = "StructuredOutput";
const MAX_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_VALIDATION_ERRORS: usize = 8;

/// A synthetic, permission-free tool whose input is the caller supplied JSON Schema.
/// The query loop treats a successful call as the turn's structured result.
#[derive(Clone, Debug)]
pub struct StructuredOutputTool {
    schema: Value,
    validator: Validator,
}

impl StructuredOutputTool {
    pub fn new(schema: Value) -> Result<Self> {
        if !schema.is_object() {
            bail!("--json-schema 顶层必须是 JSON object")
        }
        let encoded = serde_json::to_vec(&schema)?;
        if encoded.len() > MAX_SCHEMA_BYTES {
            bail!("--json-schema 超过 {MAX_SCHEMA_BYTES} 字节限制")
        }
        if schema
            .get("type")
            .is_some_and(|kind| kind != "object" && kind != &serde_json::json!(["object"]))
        {
            bail!("--json-schema 顶层必须描述 object，才能注册为工具输入")
        }
        let validator = Validator::new(&schema).context("--json-schema 不是有效 JSON Schema")?;
        Ok(Self { schema, validator })
    }

    pub fn into_tool(self) -> Arc<dyn Tool> {
        Arc::new(self)
    }
}

#[async_trait]
impl Tool for StructuredOutputTool {
    fn name(&self) -> &str {
        STRUCTURED_OUTPUT_TOOL_NAME
    }

    fn description(&self) -> &str {
        "Return the final response as structured JSON. Call this tool exactly once at the end of the response."
    }

    fn input_schema(&self) -> Value {
        self.schema.clone()
    }

    fn read_only(&self, _input: &Value) -> bool {
        true
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    fn validate_input(&self, input: &Value) -> std::result::Result<(), String> {
        let errors = self
            .validator
            .iter_errors(input)
            .take(MAX_VALIDATION_ERRORS)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "structured output 不符合要求: {}",
                errors.join("; ")
            ))
        }
    }

    fn summary(&self, input: &Value) -> String {
        let count = input.as_object().map_or(0, serde_json::Map::len);
        format!("{count} field(s)")
    }

    async fn execute(&self, _context: &ToolContext, _input: Value) -> Result<ToolOutput> {
        Ok(ToolOutput::success(
            "Structured output provided successfully",
        ))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn rejects_invalid_schema_and_non_object_roots() {
        assert!(StructuredOutputTool::new(json!([])).is_err());
        assert!(StructuredOutputTool::new(json!({"type": "string"})).is_err());
        assert!(
            StructuredOutputTool::new(json!({"type": "object", "minProperties": "bad"})).is_err()
        );
    }

    #[test]
    fn validates_with_full_json_schema_implementation() {
        let tool = StructuredOutputTool::new(json!({
            "type": "object",
            "properties": {
                "status": {"type": "string", "pattern": "^(ok|error)$"},
                "value": {"type": "integer", "multipleOf": 2}
            },
            "required": ["status", "value"],
            "additionalProperties": false
        }))
        .unwrap();
        assert!(
            tool.validate_input(&json!({"status": "ok", "value": 4}))
                .is_ok()
        );
        assert!(
            tool.validate_input(&json!({"status": "other", "value": 3}))
                .is_err()
        );
    }
}

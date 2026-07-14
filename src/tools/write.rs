use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    MAX_EDITABLE_FILE_BYTES, Tool, ToolContext, ToolOutput, atomic_write, object_schema,
    parse_input, read_text_bounded, reject_direct_symlink_write,
};

const MAX_PATH_BYTES: usize = 4096;

#[derive(Deserialize)]
struct Input {
    file_path: String,
    content: String,
}

pub struct WriteTool;

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &str {
        "Write"
    }
    fn description(&self) -> &str {
        "Creates a new file or replaces a fully-read file. Rejects stale writes if the file changed after Read."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "file_path": {"type": "string", "maxLength": MAX_PATH_BYTES},
                "content": {"type": "string", "maxLength": MAX_EDITABLE_FILE_BYTES}
            }),
            &["file_path", "content"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        false
    }
    fn destructive(&self, _: &Value) -> bool {
        true
    }
    fn path_fields(&self) -> &'static [&'static str] {
        &["file_path"]
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("file_path")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        validate_input_bytes(&input)?;
        let path = context.resolve_path(&input.file_path)?;
        if path.extension().and_then(|s| s.to_str()) == Some("ipynb") {
            bail!("Jupyter Notebook 不能通过 Write 修改")
        }
        reject_direct_symlink_write(&path)?;
        if path.exists() {
            context.require_full_read(&path).await?;
            let current = read_text_bounded(&path)
                .with_context(|| format!("无法读取现有文件 {}", path.display()))?;
            context.verify_fresh_full_read(&path, &current).await?;
        }
        context.track_before_edit(&path)?;
        context.expect_after_edit(&path, input.content.as_bytes())?;
        atomic_write(&path, &input.content)?;
        context
            .remember_read(path.clone(), input.content, false)
            .await?;
        Ok(ToolOutput::success(format!(
            "Wrote {}",
            context.display_path(&path)
        )))
    }
}

fn validate_input_bytes(input: &Input) -> Result<()> {
    ensure_utf8_bytes("file_path", &input.file_path, MAX_PATH_BYTES)?;
    ensure_utf8_bytes("content", &input.content, MAX_EDITABLE_FILE_BYTES)
}

fn ensure_utf8_bytes(field: &str, value: &str, limit: usize) -> Result<()> {
    if value.len() > limit {
        bail!("{field} 超过 {limit} 字节限制")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_byte_unicode_respects_utf8_byte_boundaries() {
        let at_limit = "🦀".repeat(MAX_EDITABLE_FILE_BYTES / 4);
        let over_limit = format!("{at_limit}🦀");
        let valid: Input = serde_json::from_value(json!({
            "file_path": "output.txt",
            "content": at_limit,
        }))
        .unwrap();
        let oversized: Input = serde_json::from_value(json!({
            "file_path": "output.txt",
            "content": over_limit,
        }))
        .unwrap();

        assert!(validate_input_bytes(&valid).is_ok());
        assert!(validate_input_bytes(&oversized).is_err());
    }

    #[test]
    fn path_limit_is_checked_in_utf8_bytes() {
        let at_limit = "🦀".repeat(MAX_PATH_BYTES / 4);
        let over_limit = format!("{at_limit}🦀");
        let valid = Input {
            file_path: at_limit,
            content: String::new(),
        };
        let oversized = Input {
            file_path: over_limit,
            content: String::new(),
        };

        assert!(validate_input_bytes(&valid).is_ok());
        assert!(validate_input_bytes(&oversized).is_err());
    }
}

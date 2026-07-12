use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    MAX_EDITABLE_FILE_BYTES, Tool, ToolContext, ToolOutput, atomic_write, object_schema,
    parse_input, read_text_bounded, reject_direct_symlink_write,
};

#[derive(Deserialize)]
struct Input {
    file_path: String,
    content: String,
}

pub struct WriteTool;

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &'static str {
        "Write"
    }
    fn description(&self) -> &'static str {
        "Creates a new file or replaces a fully-read file. Rejects stale writes if the file changed after Read."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "file_path": {"type": "string", "maxLength": 4096},
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

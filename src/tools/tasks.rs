use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Tool, ToolContext, ToolOutput, object_schema, parse_input};

#[derive(Deserialize)]
struct Input {
    task_id: String,
}

pub struct TaskOutputTool;
pub struct TaskStopTool;

#[async_trait]
impl Tool for TaskOutputTool {
    fn name(&self) -> &'static str {
        "TaskOutput"
    }
    fn description(&self) -> &'static str {
        "Reads current output and completion status for a background Bash task."
    }
    fn input_schema(&self) -> Value {
        object_schema(json!({"task_id": {"type": "string"}}), &["task_id"])
    }
    fn read_only(&self, _: &Value) -> bool {
        true
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("task_id")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .into()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        let mut tasks = context.tasks.lock().await;
        let task = tasks.get_mut(&input.task_id).context("未找到后台任务")?;
        let status = task
            .child
            .try_wait()?
            .map(|s| format!("completed ({s})"))
            .unwrap_or_else(|| "running".into());
        let output = tokio::fs::read_to_string(&task.output_path)
            .await
            .unwrap_or_default();
        Ok(ToolOutput::success(format!(
            "Status: {status}\nCommand: {}\nOutput:\n{}",
            task.command, output
        )))
    }
}

#[async_trait]
impl Tool for TaskStopTool {
    fn name(&self) -> &'static str {
        "TaskStop"
    }
    fn description(&self) -> &'static str {
        "Stops a running background Bash task."
    }
    fn input_schema(&self) -> Value {
        object_schema(json!({"task_id": {"type": "string"}}), &["task_id"])
    }
    fn read_only(&self, _: &Value) -> bool {
        false
    }
    fn destructive(&self, _: &Value) -> bool {
        true
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("task_id")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .into()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        let mut tasks = context.tasks.lock().await;
        let task = tasks.get_mut(&input.task_id).context("未找到后台任务")?;
        if task.child.try_wait()?.is_some() {
            bail!("任务已经结束")
        }
        task.child.kill().await?;
        Ok(ToolOutput::success(format!(
            "Stopped task {}",
            input.task_id
        )))
    }
}

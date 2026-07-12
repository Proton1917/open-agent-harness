use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    Tool, ToolContext, ToolOutput,
    bash::{MAX_OUTPUT_BYTES, read_output_preview, terminate_task},
    object_schema, parse_input,
};

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
        object_schema(
            json!({"task_id": {"type": "string", "maxLength": 128}}),
            &["task_id"],
        )
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
        let completed = task.child.try_wait()?;
        let finished = completed.is_some();
        let status = completed
            .map(|s| format!("completed ({s})"))
            .unwrap_or_else(|| "running".into());
        if finished {
            terminate_task(task).await;
        }
        let (mut output, preview_truncated, size) =
            read_output_preview(&task.output_path, MAX_OUTPUT_BYTES)?;
        let capture_truncated = task
            .output_truncated
            .load(std::sync::atomic::Ordering::Relaxed);
        let keep_output = preview_truncated || capture_truncated;
        let output_path = task.output_path.clone();
        if keep_output {
            output.push_str(&format!(
                "\n[Captured output: {} ({} bytes{})]",
                context.display_path(&task.output_path),
                size,
                if capture_truncated {
                    "; additional output discarded at the 8 MiB limit"
                } else {
                    ""
                }
            ));
        }
        let result = ToolOutput::success(format!(
            "Status: {status}\nCommand: {}\nOutput:\n{}",
            task.command, output
        ));
        if finished {
            tasks.remove(&input.task_id);
            if !keep_output {
                let _ = std::fs::remove_file(output_path);
            }
        }
        Ok(result)
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
        object_schema(
            json!({"task_id": {"type": "string", "maxLength": 128}}),
            &["task_id"],
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
            .get("task_id")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .into()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        let mut tasks = context.tasks.lock().await;
        let mut task = tasks.remove(&input.task_id).context("未找到后台任务")?;
        let already_finished = task.child.try_wait()?.is_some()
            && task.drains.iter().all(tokio::task::JoinHandle::is_finished);
        if already_finished {
            bail!("任务已经结束")
        }
        terminate_task(&mut task).await;
        let (mut output, preview_truncated, size) =
            read_output_preview(&task.output_path, MAX_OUTPUT_BYTES)?;
        let capture_truncated = task
            .output_truncated
            .load(std::sync::atomic::Ordering::Relaxed);
        if preview_truncated || capture_truncated {
            output.push_str(&format!(
                "\n[Captured output retained at {} ({} bytes{})]",
                context.display_path(&task.output_path),
                size,
                if capture_truncated {
                    "; additional output discarded at the 8 MiB limit"
                } else {
                    ""
                }
            ));
        } else {
            let _ = std::fs::remove_file(&task.output_path);
        }
        Ok(ToolOutput::success(format!(
            "Stopped task {}\nOutput:\n{}",
            input.task_id, output
        )))
    }
}

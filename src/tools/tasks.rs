use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::{Duration, Instant, sleep};

use super::{
    Tool, ToolContext, ToolOutput,
    bash::{MAX_OUTPUT_BYTES, read_output_preview, terminate_task},
    object_schema, parse_input,
};

#[derive(Deserialize)]
struct Input {
    task_id: String,
    #[serde(default = "default_block")]
    block: bool,
    #[serde(default = "default_timeout")]
    timeout: u64,
}

fn default_block() -> bool {
    true
}

fn default_timeout() -> u64 {
    30_000
}

pub struct TaskOutputTool;
pub struct TaskStopTool;

#[async_trait]
impl Tool for TaskOutputTool {
    fn name(&self) -> &str {
        "TaskOutput"
    }
    fn description(&self) -> &str {
        "Reads current output and completion status for a background Bash task, Monitor, declarative workflow, or local agent. It waits up to 30 seconds by default; set block=false to poll."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "task_id": {"type": "string", "maxLength": 128},
                "block": {"type": "boolean"},
                "timeout": {"type": "integer", "minimum": 0, "maximum": 600000}
            }),
            &["task_id"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        true
    }
    fn concurrency_safe(&self, _: &Value) -> bool {
        false
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
        let started = Instant::now();
        let wait_for = Duration::from_millis(input.timeout.min(600_000));
        let mut saw_bash_task = false;
        loop {
            let mut tasks = context.tasks.lock().await;
            let Some(task) = tasks.get_mut(&input.task_id) else {
                drop(tasks);
                if saw_bash_task {
                    bail!("后台 Bash 任务已被其他调用取走或停止")
                }
                if let Some(output) = context
                    .workflow_runtime()
                    .task_output(&input.task_id, input.block, input.timeout)
                    .await?
                {
                    return Ok(output);
                }
                if let Some(output) = context
                    .monitor_service()
                    .task_output(context, &input.task_id, input.block, input.timeout)
                    .await?
                {
                    return Ok(output);
                }
                return context
                    .agent_runtime()
                    .context("未找到后台 Bash 任务，且 agent runtime 不可用")?
                    .task_output_alias(context, &input.task_id, input.block, input.timeout)
                    .await;
            };
            saw_bash_task = true;
            let completed = task.child.try_wait()?;
            if completed.is_none() && input.block && started.elapsed() < wait_for {
                drop(tasks);
                sleep(Duration::from_millis(25)).await;
                continue;
            }
            let finished = completed.is_some();
            let status = if task.timed_out {
                format!("timed out after {}ms", task.timeout_ms)
            } else {
                completed
                    .map(|status| format!("completed ({status})"))
                    .unwrap_or_else(|| "running".into())
            };
            if finished {
                terminate_task(task).await;
            }
            let (mut output, preview_truncated, size) =
                read_output_preview(&task.output_path, MAX_OUTPUT_BYTES)?;
            let capture_truncated = task
                .output_truncated
                .load(std::sync::atomic::Ordering::Relaxed);
            let keep_output = preview_truncated || capture_truncated;
            if keep_output {
                task.disarm_output_cleanup();
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
            }
            let result = ToolOutput::success(format!(
                "Status: {status}\nCommand: {}\nOutput:\n{}",
                task.command, output
            ));
            if finished {
                tasks.remove(&input.task_id);
            }
            return Ok(result);
        }
    }
}

#[async_trait]
impl Tool for TaskStopTool {
    fn name(&self) -> &str {
        "TaskStop"
    }
    fn description(&self) -> &str {
        "Stops a running background Bash task, Monitor, declarative workflow, or local agent."
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
        if !tasks.contains_key(&input.task_id) {
            drop(tasks);
            if let Some(output) = context.workflow_runtime().task_stop(&input.task_id).await? {
                return Ok(output);
            }
            if let Some(output) = context
                .monitor_service()
                .task_stop(context, &input.task_id)
                .await?
            {
                return Ok(output);
            }
            return context
                .agent_runtime()
                .context("未找到后台 Bash 任务，且 agent runtime 不可用")?
                .task_stop_alias(context, &input.task_id)
                .await;
        }
        let already_finished = {
            let task = tasks.get_mut(&input.task_id).context("未找到后台任务")?;
            task.child.try_wait()?.is_some()
                && task.drains.iter().all(tokio::task::JoinHandle::is_finished)
        };
        if already_finished {
            bail!("任务已经结束；请用 TaskOutput 读取最终结果")
        }
        let mut task = tasks.remove(&input.task_id).context("未找到后台任务")?;
        terminate_task(&mut task).await;
        let (mut output, preview_truncated, size) =
            read_output_preview(&task.output_path, MAX_OUTPUT_BYTES)?;
        let capture_truncated = task
            .output_truncated
            .load(std::sync::atomic::Ordering::Relaxed);
        if preview_truncated || capture_truncated {
            task.disarm_output_cleanup();
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
        }
        Ok(ToolOutput::success(format!(
            "Stopped task {}\nOutput:\n{}",
            input.task_id, output
        )))
    }
}

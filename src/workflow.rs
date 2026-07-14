use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Semaphore, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout},
};
use uuid::Uuid;

use crate::tools::{BashTool, ToolContext, ToolOutput};

pub(crate) const DEFAULT_WORKFLOW_TIMEOUT_MS: u64 = 15 * 60 * 1_000;
pub(crate) const MAX_WORKFLOW_TIMEOUT_MS: u64 = 60 * 60 * 1_000;
pub(crate) const DEFAULT_STEP_TIMEOUT_MS: u64 = 2 * 60 * 1_000;
pub(crate) const MAX_STEP_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
pub(crate) const MAX_WORKFLOW_STEPS: usize = 64;
pub(crate) const MAX_NESTED_WORKFLOW_STEPS: usize = 32;
pub(crate) const MAX_WORKFLOW_TOTAL_STEPS: usize = 128;
pub(crate) const MAX_WORKFLOW_PARALLELISM: usize = 16;
pub(crate) const MAX_WORKFLOW_INPUT_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_WORKFLOW_COMMAND_BYTES: usize = 65_536;
const MAX_WORKFLOW_NAME_BYTES: usize = 128;
const MAX_WORKFLOW_DESCRIPTION_BYTES: usize = 2_048;
const MAX_STEP_ID_BYTES: usize = 64;
const MAX_STEP_DEPENDENCIES: usize = 32;
const MAX_TOTAL_COMMAND_BYTES: usize = 1024 * 1024;
const MAX_BACKGROUND_WORKFLOWS: usize = 16;
const MAX_STEP_OUTPUT_BYTES: usize = 8 * 1024;
const MAX_WORKFLOW_REPORT_BYTES: usize = 128 * 1024;
const WORKFLOW_STOP_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowDefinition {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_parallel: Option<usize>,
    pub steps: Vec<WorkflowStep>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowStep {
    pub id: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub workflow: Option<Box<WorkflowDefinition>>,
}

#[derive(Default)]
struct ValidationCounters {
    total_steps: usize,
    total_command_bytes: usize,
}

pub(crate) fn validate_workflow(definition: &WorkflowDefinition) -> Result<()> {
    let mut counters = ValidationCounters::default();
    validate_definition(definition, 0, &mut counters)
}

fn validate_definition(
    definition: &WorkflowDefinition,
    depth: usize,
    counters: &mut ValidationCounters,
) -> Result<()> {
    validate_identifier("workflow name", &definition.name, MAX_WORKFLOW_NAME_BYTES)?;
    if definition.description.as_ref().is_some_and(|value| {
        value.trim().is_empty() || value.len() > MAX_WORKFLOW_DESCRIPTION_BYTES
    }) {
        bail!("workflow description 为空或超过 {MAX_WORKFLOW_DESCRIPTION_BYTES} 字节限制")
    }
    if let Some(timeout_ms) = definition.timeout_ms {
        if !(1_000..=MAX_WORKFLOW_TIMEOUT_MS).contains(&timeout_ms) {
            bail!("workflow timeout_ms 必须在 1000..={MAX_WORKFLOW_TIMEOUT_MS}")
        }
    }
    if let Some(max_parallel) = definition.max_parallel {
        if !(1..=MAX_WORKFLOW_PARALLELISM).contains(&max_parallel) {
            bail!("workflow max_parallel 必须在 1..={MAX_WORKFLOW_PARALLELISM}")
        }
    }
    let step_limit = if depth == 0 {
        MAX_WORKFLOW_STEPS
    } else {
        MAX_NESTED_WORKFLOW_STEPS
    };
    if definition.steps.is_empty() || definition.steps.len() > step_limit {
        bail!("workflow steps 必须在 1..={step_limit} 项")
    }
    counters.total_steps = counters
        .total_steps
        .checked_add(definition.steps.len())
        .context("workflow step 数量溢出")?;
    if counters.total_steps > MAX_WORKFLOW_TOTAL_STEPS {
        bail!("workflow 总 step 数超过 {MAX_WORKFLOW_TOTAL_STEPS} 项限制")
    }

    let mut ids = HashSet::with_capacity(definition.steps.len());
    for step in &definition.steps {
        validate_identifier("workflow step id", &step.id, MAX_STEP_ID_BYTES)?;
        if !ids.insert(step.id.as_str()) {
            bail!("workflow 包含重复 step id: {}", step.id)
        }
        if step.depends_on.len() > MAX_STEP_DEPENDENCIES {
            bail!(
                "workflow step {} 的 depends_on 超过 {MAX_STEP_DEPENDENCIES} 项限制",
                step.id
            )
        }
        let unique = step.depends_on.iter().collect::<BTreeSet<_>>();
        if unique.len() != step.depends_on.len() {
            bail!("workflow step {} 包含重复依赖", step.id)
        }
        if step
            .depends_on
            .iter()
            .any(|dependency| dependency == &step.id)
        {
            bail!("workflow step {} 不能依赖自身", step.id)
        }
        if let Some(timeout_ms) = step.timeout_ms {
            if !(1..=MAX_STEP_TIMEOUT_MS).contains(&timeout_ms) {
                bail!(
                    "workflow step {} timeout_ms 必须在 1..={MAX_STEP_TIMEOUT_MS}",
                    step.id
                )
            }
        }
        match (&step.command, &step.workflow) {
            (Some(command), None) => {
                if command.trim().is_empty() || command.len() > MAX_WORKFLOW_COMMAND_BYTES {
                    bail!(
                        "workflow step {} command 为空或超过 {MAX_WORKFLOW_COMMAND_BYTES} 字节限制",
                        step.id
                    )
                }
                counters.total_command_bytes = counters
                    .total_command_bytes
                    .checked_add(command.len())
                    .context("workflow command 总字节数溢出")?;
                if counters.total_command_bytes > MAX_TOTAL_COMMAND_BYTES {
                    bail!("workflow command 总字节数超过 {MAX_TOTAL_COMMAND_BYTES} 限制")
                }
            }
            (None, Some(nested)) if depth == 0 => {
                validate_definition(nested, depth + 1, counters)?;
            }
            (None, Some(_)) => bail!("workflow 只允许一层受限嵌套"),
            _ => bail!(
                "workflow step {} 必须且只能设置 command 或 workflow 之一",
                step.id
            ),
        }
    }
    for step in &definition.steps {
        for dependency in &step.depends_on {
            if !ids.contains(dependency.as_str()) {
                bail!("workflow step {} 引用了未知依赖 {dependency}", step.id)
            }
        }
    }
    ensure_acyclic(definition)
}

fn validate_identifier(label: &str, value: &str, maximum: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        bail!("{label} 不是有效标识符或超过 {maximum} 字节限制")
    }
    Ok(())
}

fn ensure_acyclic(definition: &WorkflowDefinition) -> Result<()> {
    let index = definition
        .steps
        .iter()
        .enumerate()
        .map(|(position, step)| (step.id.as_str(), position))
        .collect::<HashMap<_, _>>();
    let mut indegree = definition
        .steps
        .iter()
        .map(|step| step.depends_on.len())
        .collect::<Vec<_>>();
    let mut dependents = vec![Vec::new(); definition.steps.len()];
    for (position, step) in definition.steps.iter().enumerate() {
        for dependency in &step.depends_on {
            dependents[index[dependency.as_str()]].push(position);
        }
    }
    let mut ready = indegree
        .iter()
        .enumerate()
        .filter_map(|(position, count)| (*count == 0).then_some(position))
        .collect::<VecDeque<_>>();
    let mut visited = 0usize;
    while let Some(position) = ready.pop_front() {
        visited += 1;
        for dependent in &dependents[position] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                ready.push_back(*dependent);
            }
        }
    }
    if visited != definition.steps.len() {
        bail!("workflow depends_on 构成环")
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct WorkflowOutcome {
    content: String,
    is_error: bool,
    status: &'static str,
}

impl WorkflowOutcome {
    fn into_tool_output(self) -> ToolOutput {
        if self.is_error {
            ToolOutput::error(self.content)
        } else {
            ToolOutput::success(self.content)
        }
    }
}

struct BackgroundWorkflow {
    name: String,
    notification_delivered: bool,
    cancel: Option<oneshot::Sender<()>>,
    result: watch::Receiver<Option<Arc<WorkflowOutcome>>>,
    handle: JoinHandle<()>,
}

impl Drop for BackgroundWorkflow {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct WorkflowRuntime {
    tasks: Arc<Mutex<HashMap<String, BackgroundWorkflow>>>,
}

impl WorkflowRuntime {
    pub(crate) async fn launch(
        &self,
        definition: WorkflowDefinition,
        context: ToolContext,
    ) -> Result<String> {
        validate_workflow(&definition)?;
        // Detach from the caller's workflow registry before spawning. Keeping a
        // clone of the root context inside the actor would form an Arc cycle
        // (runtime -> task -> context -> runtime) if normal session shutdown
        // were skipped. The fork also freezes this run's starting cwd while
        // retaining the same permission, sandbox, and file-history boundaries.
        let mut execution_context = context.fork_for_agent();
        execution_context.set_agent_depth(context.agent_depth());
        let mut tasks = self.tasks.lock().await;
        if tasks.len() >= MAX_BACKGROUND_WORKFLOWS {
            bail!(
                "background workflow 达到 {MAX_BACKGROUND_WORKFLOWS} 个限制；请先读取或停止已有 workflow"
            )
        }
        let id = format!("wf_{}", Uuid::new_v4());
        let name = definition.name.clone();
        let timeout_ms = definition.timeout_ms.unwrap_or(DEFAULT_WORKFLOW_TIMEOUT_MS);
        let parallelism = definition.max_parallel.unwrap_or(4);
        let slots = Arc::new(Semaphore::new(parallelism));
        let (cancel, cancel_rx) = oneshot::channel();
        let (result_tx, result) = watch::channel(None);
        let task_id = id.clone();
        let task_name = name.clone();
        let handle = tokio::spawn(async move {
            let started = Instant::now();
            let outcome = tokio::select! {
                _ = cancel_rx => WorkflowOutcome {
                    content: format!("Workflow {task_name} ({task_id}) stopped"),
                    is_error: true,
                    status: "stopped",
                },
                result = timeout(
                    Duration::from_millis(timeout_ms),
                    execute_definition(definition, execution_context, slots),
                ) => match result {
                    Ok(Ok(report)) => WorkflowOutcome {
                        content: format!(
                            "Workflow {task_name} ({task_id}) completed in {}ms\n{report}",
                            started.elapsed().as_millis()
                        ),
                        is_error: false,
                        status: "completed",
                    },
                    Ok(Err(error)) => WorkflowOutcome {
                        content: format!("Workflow {task_name} ({task_id}) failed: {error:#}"),
                        is_error: true,
                        status: "failed",
                    },
                    Err(_) => WorkflowOutcome {
                        content: format!(
                            "Workflow {task_name} ({task_id}) timed out after {timeout_ms}ms and was cancelled"
                        ),
                        is_error: true,
                        status: "timed_out",
                    },
                },
            };
            let _ = result_tx.send(Some(Arc::new(outcome)));
        });
        tasks.insert(
            id.clone(),
            BackgroundWorkflow {
                name,
                notification_delivered: false,
                cancel: Some(cancel),
                result,
                handle,
            },
        );
        Ok(id)
    }

    pub(crate) async fn task_output(
        &self,
        task_id: &str,
        block: bool,
        timeout_ms: u64,
    ) -> Result<Option<ToolOutput>> {
        let mut receiver = {
            let tasks = self.tasks.lock().await;
            let Some(task) = tasks.get(task_id) else {
                return Ok(None);
            };
            task.result.clone()
        };
        let wait = Duration::from_millis(timeout_ms.min(600_000));
        if receiver.borrow().is_none() && block && !wait.is_zero() {
            let _ = timeout(wait, receiver.changed()).await;
        }
        let outcome = receiver.borrow().clone();
        let Some(outcome) = outcome else {
            let tasks = self.tasks.lock().await;
            let task = tasks
                .get(task_id)
                .context("workflow task 在读取状态时消失")?;
            return Ok(Some(ToolOutput::success(format!(
                "Status: running\nWorkflow: {}\nTask ID: {task_id}",
                task.name
            ))));
        };
        self.tasks.lock().await.remove(task_id);
        Ok(Some((*outcome).clone().into_tool_output()))
    }

    pub(crate) async fn task_stop(&self, task_id: &str) -> Result<Option<ToolOutput>> {
        let (name, mut result, cancel) = {
            let mut tasks = self.tasks.lock().await;
            let Some(task) = tasks.get_mut(task_id) else {
                return Ok(None);
            };
            if task.result.borrow().is_some() {
                bail!("workflow task 已经结束: {task_id}；请用 TaskOutput 读取最终结果")
            }
            (task.name.clone(), task.result.clone(), task.cancel.take())
        };
        if let Some(cancel) = cancel {
            let _ = cancel.send(());
        }
        if result.borrow().is_none() {
            let _ = timeout(WORKFLOW_STOP_GRACE, result.changed()).await;
        }
        let outcome = result.borrow().clone();
        if let Some(outcome) = &outcome {
            if outcome.status != "stopped" {
                // Natural completion may win the select after the initial
                // status check but before cancellation is observed. Keep that
                // terminal result registered so TaskOutput and notifications
                // can consume it.
                bail!("workflow task 已经结束: {task_id}；请用 TaskOutput 读取最终结果")
            }
        }
        // A concurrent TaskOutput may already have consumed a stopped outcome,
        // in which case there is nothing left to reap here. If the actor did
        // not publish an outcome within the grace window, abort it explicitly;
        // otherwise the stopped outcome is its final action and joining it is
        // bounded by normal scheduler progress.
        let removed = self.tasks.lock().await.remove(task_id);
        if let Some(mut task) = removed {
            if outcome.is_none() {
                task.handle.abort();
            }
            let _ = (&mut task.handle).await;
        }
        Ok(Some(ToolOutput::success(format!(
            "Stopped workflow task {task_id} ({name})"
        ))))
    }

    pub(crate) async fn task_ids(&self) -> HashSet<String> {
        self.tasks.lock().await.keys().cloned().collect()
    }

    pub(crate) async fn notification_checkpoint(&self) -> HashMap<String, bool> {
        self.tasks
            .lock()
            .await
            .iter()
            .map(|(id, task)| (id.clone(), task.notification_delivered))
            .collect()
    }

    pub(crate) async fn restore_notification_checkpoint(&self, checkpoint: &HashMap<String, bool>) {
        let mut tasks = self.tasks.lock().await;
        for (id, delivered) in checkpoint {
            if let Some(task) = tasks.get_mut(id) {
                task.notification_delivered = *delivered;
            }
        }
    }

    pub(crate) async fn drain_notifications(&self, maximum: usize) -> Vec<String> {
        let mut tasks = self.tasks.lock().await;
        let mut ids = tasks.keys().cloned().collect::<Vec<_>>();
        ids.sort_unstable();
        let mut notifications = Vec::new();
        for id in ids {
            if notifications.len() >= maximum {
                break;
            }
            let Some(task) = tasks.get_mut(&id) else {
                continue;
            };
            let outcome = task.result.borrow().clone();
            let Some(outcome) = outcome else {
                continue;
            };
            if task.notification_delivered {
                continue;
            }
            task.notification_delivered = true;
            notifications.push(format!(
                "Background workflow {id} {} ({}). Use TaskOutput for the bounded result.",
                outcome.status, task.name
            ));
        }
        notifications
    }

    pub(crate) async fn rollback_new(&self, keep: &HashSet<String>) {
        self.rollback_new_with_grace(keep, WORKFLOW_STOP_GRACE)
            .await;
    }

    async fn rollback_new_with_grace(&self, keep: &HashSet<String>, grace: Duration) {
        let removed = {
            let mut tasks = self.tasks.lock().await;
            let ids = tasks
                .keys()
                .filter(|id| !keep.contains(*id))
                .cloned()
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| tasks.remove(&id))
                .collect::<Vec<_>>()
        };
        stop_all(removed, grace).await;
    }

    pub(crate) async fn shutdown(&self) {
        self.shutdown_with_grace(WORKFLOW_STOP_GRACE).await;
    }

    async fn shutdown_with_grace(&self, grace: Duration) {
        let tasks = self
            .tasks
            .lock()
            .await
            .drain()
            .map(|(_, task)| task)
            .collect();
        stop_all(tasks, grace).await;
    }
}

async fn stop_all(mut tasks: Vec<BackgroundWorkflow>, grace: Duration) {
    for task in &mut tasks {
        if let Some(cancel) = task.cancel.take() {
            let _ = cancel.send(());
        }
    }

    // The registry itself is capped at MAX_BACKGROUND_WORKFLOWS. Reap that
    // bounded set concurrently so every task shares one grace window instead
    // of extending shutdown by another full window per task.
    let mut pending = tasks.into_iter().collect::<VecDeque<_>>();
    let mut reapers = JoinSet::new();
    loop {
        while reapers.len() < MAX_BACKGROUND_WORKFLOWS {
            let Some(mut task) = pending.pop_front() else {
                break;
            };
            reapers.spawn(async move {
                if timeout(grace, &mut task.handle).await.is_err() {
                    task.handle.abort();
                    let _ = (&mut task.handle).await;
                }
            });
        }
        if reapers.is_empty() {
            break;
        }
        let _ = reapers.join_next().await;
    }
}

struct StepCompletion {
    position: usize,
    id: String,
    output: ToolOutput,
    elapsed: Duration,
}

fn execute_definition(
    definition: WorkflowDefinition,
    context: ToolContext,
    command_slots: Arc<Semaphore>,
) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> {
    Box::pin(async move {
        let count = definition.steps.len();
        let index = definition
            .steps
            .iter()
            .enumerate()
            .map(|(position, step)| (step.id.clone(), position))
            .collect::<HashMap<_, _>>();
        let mut remaining_dependencies = definition
            .steps
            .iter()
            .map(|step| step.depends_on.len())
            .collect::<Vec<_>>();
        let mut dependents = vec![Vec::new(); count];
        for (position, step) in definition.steps.iter().enumerate() {
            for dependency in &step.depends_on {
                dependents[index[dependency]].push(position);
            }
        }
        let mut ready = remaining_dependencies
            .iter()
            .enumerate()
            .filter_map(|(position, count)| (*count == 0).then_some(position))
            .collect::<VecDeque<_>>();
        let logical_parallelism = definition.max_parallel.unwrap_or(4);
        let mut running = JoinSet::new();
        let mut completed = 0usize;
        let mut step_reports = (0..count).map(|_| None).collect::<Vec<_>>();

        while completed < count {
            while running.len() < logical_parallelism {
                let Some(position) = ready.pop_front() else {
                    break;
                };
                let step = definition.steps[position].clone();
                let step_context = isolated_step_context(&context);
                let slots = Arc::clone(&command_slots);
                running.spawn(async move {
                    let id = step.id.clone();
                    let started = Instant::now();
                    let output = execute_step(step, step_context, slots).await;
                    StepCompletion {
                        position,
                        id,
                        output,
                        elapsed: started.elapsed(),
                    }
                });
            }
            let Some(joined) = running.join_next().await else {
                bail!("workflow scheduler 无 ready step 但尚未完成；DAG 状态无效")
            };
            let completion = joined.context("workflow step task 被意外取消")?;
            if completion.output.is_error || completion.output.interrupted {
                running.abort_all();
                while running.join_next().await.is_some() {}
                bail!(
                    "step {} failed after {}ms: {}",
                    completion.id,
                    completion.elapsed.as_millis(),
                    bounded_text(&completion.output.content, MAX_STEP_OUTPUT_BYTES)
                )
            }
            step_reports[completion.position] =
                Some((completion.id, completion.elapsed, completion.output.content));
            completed += 1;
            for dependent in &dependents[completion.position] {
                remaining_dependencies[*dependent] -= 1;
                if remaining_dependencies[*dependent] == 0 {
                    ready.push_back(*dependent);
                }
            }
        }
        let mut report = String::new();
        for step_report in step_reports.into_iter().flatten() {
            append_report(&mut report, &step_report.0, step_report.1, &step_report.2);
        }
        Ok(report)
    })
}

fn isolated_step_context(context: &ToolContext) -> ToolContext {
    let mut isolated = context.fork_for_agent();
    isolated.set_agent_depth(context.agent_depth());
    isolated
}

async fn execute_step(
    step: WorkflowStep,
    context: ToolContext,
    command_slots: Arc<Semaphore>,
) -> ToolOutput {
    let timeout_ms = step.timeout_ms.unwrap_or(DEFAULT_STEP_TIMEOUT_MS);
    if let Some(command) = step.command {
        let permit = match command_slots.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return ToolOutput::error("workflow command scheduler 已关闭"),
        };
        let output = BashTool
            .execute_for_workflow(
                &context,
                serde_json::json!({
                    "command": command,
                    "timeout": timeout_ms,
                    "run_in_background": false,
                    "description": format!("workflow step {}", step.id),
                }),
            )
            .await;
        drop(permit);
        return output.unwrap_or_else(|error| ToolOutput::error(format!("{error:#}")));
    }
    let Some(nested) = step.workflow else {
        return ToolOutput::error("workflow step 缺少 command/workflow");
    };
    let nested_timeout = nested.timeout_ms.unwrap_or(timeout_ms).min(timeout_ms);
    match timeout(
        Duration::from_millis(nested_timeout),
        execute_definition(*nested, context, command_slots),
    )
    .await
    {
        Ok(Ok(report)) => ToolOutput::success(report),
        Ok(Err(error)) => ToolOutput::error(format!("nested workflow failed: {error:#}")),
        Err(_) => ToolOutput::error(format!(
            "nested workflow timed out after {nested_timeout}ms and was cancelled"
        )),
    }
}

fn append_report(report: &mut String, id: &str, elapsed: Duration, output: &str) {
    if report.len() >= MAX_WORKFLOW_REPORT_BYTES {
        return;
    }
    let output = bounded_text(output, MAX_STEP_OUTPUT_BYTES);
    let section = format!(
        "{}[step {id} completed in {}ms]\n{}",
        if report.is_empty() { "" } else { "\n" },
        elapsed.as_millis(),
        output
    );
    let remaining = MAX_WORKFLOW_REPORT_BYTES - report.len();
    report.push_str(&bounded_text(&section, remaining));
}

fn bounded_text(value: &str, maximum: usize) -> String {
    if value.len() <= maximum {
        return value.to_owned();
    }
    const MARKER: &str = "\n[workflow output truncated]";
    let mut end = maximum.saturating_sub(MARKER.len());
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let mut bounded = value[..end].to_owned();
    bounded.push_str(MARKER);
    bounded
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    struct CountOnDrop(Arc<AtomicUsize>);

    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn pending_background_workflow(name: &str, dropped: &Arc<AtomicUsize>) -> BackgroundWorkflow {
        let (cancel, cancel_rx) = oneshot::channel();
        let (result_tx, result) = watch::channel::<Option<Arc<WorkflowOutcome>>>(None);
        let drop_counter = CountOnDrop(Arc::clone(dropped));
        let handle = tokio::spawn(async move {
            let _drop_counter = drop_counter;
            let _cancel_rx = cancel_rx;
            let _result_tx = result_tx;
            pending::<()>().await;
        });
        BackgroundWorkflow {
            name: name.to_owned(),
            notification_delivered: false,
            cancel: Some(cancel),
            result,
            handle,
        }
    }

    fn command_step(id: &str, dependencies: &[&str]) -> WorkflowStep {
        WorkflowStep {
            id: id.to_owned(),
            depends_on: dependencies
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
            timeout_ms: None,
            command: Some("true".to_owned()),
            workflow: None,
        }
    }

    fn definition(steps: Vec<WorkflowStep>) -> WorkflowDefinition {
        WorkflowDefinition {
            name: "test-workflow".to_owned(),
            description: None,
            timeout_ms: None,
            max_parallel: Some(4),
            steps,
        }
    }

    #[test]
    fn rejects_cycles_duplicates_and_unknown_dependencies() {
        let cycle = definition(vec![command_step("a", &["b"]), command_step("b", &["a"])]);
        assert!(
            validate_workflow(&cycle)
                .unwrap_err()
                .to_string()
                .contains("环")
        );

        let duplicate = definition(vec![command_step("a", &[]), command_step("a", &[])]);
        assert!(
            validate_workflow(&duplicate)
                .unwrap_err()
                .to_string()
                .contains("重复")
        );

        let unknown = definition(vec![command_step("a", &["missing"])]);
        assert!(
            validate_workflow(&unknown)
                .unwrap_err()
                .to_string()
                .contains("未知依赖")
        );
    }

    #[test]
    fn allows_one_nested_level_and_rejects_recursion() {
        let nested = definition(vec![command_step("child", &[])]);
        let parent = definition(vec![WorkflowStep {
            id: "nested".to_owned(),
            depends_on: Vec::new(),
            timeout_ms: None,
            command: None,
            workflow: Some(Box::new(nested.clone())),
        }]);
        assert!(validate_workflow(&parent).is_ok());

        let recursive_child = definition(vec![WorkflowStep {
            id: "too-deep".to_owned(),
            depends_on: Vec::new(),
            timeout_ms: None,
            command: None,
            workflow: Some(Box::new(nested)),
        }]);
        let recursive_parent = definition(vec![WorkflowStep {
            id: "child".to_owned(),
            depends_on: Vec::new(),
            timeout_ms: None,
            command: None,
            workflow: Some(Box::new(recursive_child)),
        }]);
        assert!(
            validate_workflow(&recursive_parent)
                .unwrap_err()
                .to_string()
                .contains("一层")
        );
    }

    #[tokio::test]
    async fn task_stop_preserves_natural_result_that_wins_cancellation_race() {
        let runtime = WorkflowRuntime::default();
        let task_id = "wf_race".to_owned();
        let (cancel, cancel_rx) = oneshot::channel();
        let (result_tx, result) = watch::channel::<Option<Arc<WorkflowOutcome>>>(None);
        let handle = tokio::spawn(async move {
            let _ = cancel_rx.await;
            let _ = result_tx.send(Some(Arc::new(WorkflowOutcome {
                content: "natural result".to_owned(),
                is_error: false,
                status: "completed",
            })));
        });
        runtime.tasks.lock().await.insert(
            task_id.clone(),
            BackgroundWorkflow {
                name: "race".to_owned(),
                notification_delivered: false,
                cancel: Some(cancel),
                result,
                handle,
            },
        );

        let error = runtime.task_stop(&task_id).await.unwrap_err();
        assert!(error.to_string().contains("已经结束"));
        assert!(runtime.task_ids().await.contains(&task_id));

        let notifications = runtime.drain_notifications(1).await;
        assert_eq!(notifications.len(), 1);
        assert!(notifications[0].contains("completed"));
        let output = runtime
            .task_output(&task_id, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(!output.is_error);
        assert_eq!(output.content, "natural result");
        assert!(!runtime.task_ids().await.contains(&task_id));
    }

    #[tokio::test]
    async fn shutdown_reaps_maximum_background_workflows_in_one_grace_window() {
        let runtime = WorkflowRuntime::default();
        let dropped = Arc::new(AtomicUsize::new(0));
        {
            let mut tasks = runtime.tasks.lock().await;
            for index in 0..MAX_BACKGROUND_WORKFLOWS {
                tasks.insert(
                    format!("wf_{index}"),
                    pending_background_workflow(&format!("pending-{index}"), &dropped),
                );
            }
        }

        timeout(
            Duration::from_secs(1),
            runtime.shutdown_with_grace(Duration::from_millis(200)),
        )
        .await
        .expect("workflow shutdown exceeded one bounded grace window");

        assert_eq!(dropped.load(Ordering::SeqCst), MAX_BACKGROUND_WORKFLOWS);
        assert!(runtime.task_ids().await.is_empty());
        assert!(runtime.drain_notifications(1).await.is_empty());
    }

    #[tokio::test]
    async fn rollback_keeps_existing_task_and_notification_checkpoint() {
        let runtime = WorkflowRuntime::default();
        let kept_id = "wf_kept".to_owned();
        let (kept_cancel, _kept_cancel_rx) = oneshot::channel();
        let (_kept_result_tx, kept_result) = watch::channel(Some(Arc::new(WorkflowOutcome {
            content: "kept result".to_owned(),
            is_error: false,
            status: "completed",
        })));
        runtime.tasks.lock().await.insert(
            kept_id.clone(),
            BackgroundWorkflow {
                name: "kept".to_owned(),
                notification_delivered: true,
                cancel: Some(kept_cancel),
                result: kept_result,
                handle: tokio::spawn(async {}),
            },
        );

        let dropped = Arc::new(AtomicUsize::new(0));
        for index in 0..3 {
            runtime.tasks.lock().await.insert(
                format!("wf_new_{index}"),
                pending_background_workflow(&format!("new-{index}"), &dropped),
            );
        }
        runtime
            .rollback_new_with_grace(&HashSet::from([kept_id.clone()]), Duration::from_millis(20))
            .await;

        assert_eq!(dropped.load(Ordering::SeqCst), 3);
        assert_eq!(runtime.task_ids().await, HashSet::from([kept_id.clone()]));
        assert_eq!(
            runtime.notification_checkpoint().await,
            HashMap::from([(kept_id.clone(), true)])
        );
        assert!(runtime.drain_notifications(1).await.is_empty());
        let kept = runtime
            .task_output(&kept_id, false, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(kept.content, "kept result");
    }
}

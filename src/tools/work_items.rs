use std::{
    collections::{BTreeMap, HashSet},
    io::Read,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::{
    TodoItem, Tool, ToolContext, ToolOutput, atomic_write_private, object_schema, parse_input,
};

const STATUSES: &[&str] = &["pending", "in_progress", "completed"];
const MAX_TODOS: usize = 100;
const MAX_TASKS: usize = 1000;
const MAX_TASK_STORE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const MAX_METADATA_KEYS: usize = 128;
const MAX_TASK_UI_ITEMS: usize = 448;
const MAX_TASK_UI_ITEMS_PER_SOURCE: usize = 64;
const MAX_TASK_UI_SNAPSHOT_BYTES: usize = 384 * 1024;
const MAX_TASK_UI_ID_BYTES: usize = 128;
const MAX_TASK_UI_TITLE_BYTES: usize = 512;
const MAX_TASK_UI_DETAIL_BYTES: usize = 1024;

/// Origin of one entry in the local task UI snapshot.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TaskUiItemKind {
    PersistentTask,
    Todo,
    BackgroundTask,
    WorkflowTask,
    MonitorTask,
    CronJob,
    DynamicWakeup,
}

/// Small, provider-neutral state vocabulary used by the task UI.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskUiStatus {
    Pending,
    InProgress,
    Completed,
    Tracked,
    Scheduled,
    Unknown,
}

/// One bounded, display-only task entry. It contains no process handles,
/// captured output paths, task metadata, or mutable runtime capability.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskUiItem {
    pub kind: TaskUiItemKind,
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub status: TaskUiStatus,
}

/// Point-in-time local task state for terminal rendering. `truncated` is set
/// whenever a source or field exceeded a UI-specific bound.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskUiSnapshot {
    pub items: Vec<TaskUiItem>,
    pub truncated: bool,
}

impl TaskUiSnapshot {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

pub(super) async fn task_ui_snapshot(context: &ToolContext) -> Result<TaskUiSnapshot> {
    let store = {
        let _guard = context.task_store_lock.lock().await;
        load_store(context)?
    };
    let todos = context.todos.lock().await.clone();
    let mut background_ids = context
        .tasks
        .lock()
        .await
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let mut workflow_ids = context
        .workflow_runtime()
        .task_ids()
        .await
        .into_iter()
        .collect::<Vec<_>>();
    let mut monitor_ids = context
        .monitor_service()
        .owned_task_ids(&context.async_owner())
        .await
        .into_iter()
        .collect::<Vec<_>>();
    background_ids.sort();
    workflow_ids.sort();
    monitor_ids.sort();

    let (cron_jobs, wakeup) = if context.agent_depth() == 0 {
        let cron = context.cron_service();
        (cron.list()?, cron.current_wakeup()?)
    } else {
        (Vec::new(), None)
    };

    let mut snapshot = SnapshotBuilder::default();
    let visible_tasks = store
        .tasks
        .iter()
        .filter(|task| task.metadata.get("_internal").and_then(Value::as_bool) != Some(true));
    snapshot.extend_limited(visible_tasks, |task| {
        bounded_ui_item(
            TaskUiItemKind::PersistentTask,
            &task.id,
            &task.subject,
            task.active_form.as_deref(),
            task_ui_status(&task.status),
        )
    });
    snapshot.extend_limited(todos.iter().enumerate(), |(index, todo)| {
        bounded_ui_item(
            TaskUiItemKind::Todo,
            &format!("todo-{}", index + 1),
            &todo.content,
            Some(&todo.active_form),
            task_ui_status(&todo.status),
        )
    });
    snapshot.extend_limited(background_ids.iter(), |id| {
        bounded_ui_item(
            TaskUiItemKind::BackgroundTask,
            id,
            "Background task",
            None,
            TaskUiStatus::Tracked,
        )
    });
    snapshot.extend_limited(workflow_ids.iter(), |id| {
        bounded_ui_item(
            TaskUiItemKind::WorkflowTask,
            id,
            "Workflow task",
            None,
            TaskUiStatus::Tracked,
        )
    });
    snapshot.extend_limited(monitor_ids.iter(), |id| {
        bounded_ui_item(
            TaskUiItemKind::MonitorTask,
            id,
            "Monitor task",
            None,
            TaskUiStatus::Tracked,
        )
    });
    snapshot.extend_limited(cron_jobs.iter(), |job| {
        let detail = format!(
            "{}; {}; {}; next={}",
            job.human_schedule,
            if job.recurring {
                "recurring"
            } else {
                "one-shot"
            },
            if job.durable {
                "durable"
            } else {
                "session-only"
            },
            job.next_fire_at_ms
        );
        bounded_ui_item(
            TaskUiItemKind::CronJob,
            &job.id,
            &job.prompt,
            Some(&detail),
            TaskUiStatus::Scheduled,
        )
    });
    if let Some(job) = wakeup {
        let detail = format!("{}; next={}", job.reason, job.scheduled_for_ms);
        snapshot.push(bounded_ui_item(
            TaskUiItemKind::DynamicWakeup,
            &job.id,
            &job.prompt,
            Some(&detail),
            TaskUiStatus::Scheduled,
        ));
    }
    Ok(snapshot.finish())
}

fn task_ui_status(status: &str) -> TaskUiStatus {
    match status {
        "pending" => TaskUiStatus::Pending,
        "in_progress" => TaskUiStatus::InProgress,
        "completed" => TaskUiStatus::Completed,
        _ => TaskUiStatus::Unknown,
    }
}

fn bounded_ui_item(
    kind: TaskUiItemKind,
    id: &str,
    title: &str,
    detail: Option<&str>,
    status: TaskUiStatus,
) -> (TaskUiItem, bool) {
    let (id, id_truncated) = bounded_ui_text(id, MAX_TASK_UI_ID_BYTES);
    let (title, title_truncated) = bounded_ui_text(title, MAX_TASK_UI_TITLE_BYTES);
    let (detail, detail_truncated) = detail.map_or((None, false), |detail| {
        let (detail, truncated) = bounded_ui_text(detail, MAX_TASK_UI_DETAIL_BYTES);
        (Some(detail), truncated)
    });
    (
        TaskUiItem {
            kind,
            id,
            title,
            detail,
            status,
        },
        id_truncated || title_truncated || detail_truncated,
    )
}

fn bounded_ui_text(value: &str, max_bytes: usize) -> (String, bool) {
    let mut output = String::with_capacity(value.len().min(max_bytes));
    let mut truncated = false;
    for character in value.trim().chars() {
        let replacement = match character {
            '\t' | '\r' | '\n' => ' ',
            character if character.is_control() => '�',
            character => character,
        };
        if output.len().saturating_add(replacement.len_utf8()) > max_bytes {
            truncated = true;
            break;
        }
        output.push(replacement);
    }
    if truncated && max_bytes >= '…'.len_utf8() {
        while output.len().saturating_add('…'.len_utf8()) > max_bytes {
            output.pop();
        }
        output.push('…');
    }
    (output, truncated)
}

#[derive(Default)]
struct SnapshotBuilder {
    snapshot: TaskUiSnapshot,
    bytes: usize,
}

impl SnapshotBuilder {
    fn extend_limited<T>(
        &mut self,
        values: impl IntoIterator<Item = T>,
        mut make_item: impl FnMut(T) -> (TaskUiItem, bool),
    ) {
        for (count, value) in values.into_iter().enumerate() {
            if count == MAX_TASK_UI_ITEMS_PER_SOURCE {
                self.snapshot.truncated = true;
                break;
            }
            self.push(make_item(value));
        }
    }

    fn push(&mut self, (item, field_truncated): (TaskUiItem, bool)) {
        self.snapshot.truncated |= field_truncated;
        let item_bytes = item
            .id
            .len()
            .saturating_add(item.title.len())
            .saturating_add(item.detail.as_ref().map_or(0, String::len))
            .saturating_add(32);
        if self.snapshot.items.len() >= MAX_TASK_UI_ITEMS
            || self.bytes.saturating_add(item_bytes) > MAX_TASK_UI_SNAPSHOT_BYTES
        {
            self.snapshot.truncated = true;
            return;
        }
        self.bytes = self.bytes.saturating_add(item_bytes);
        self.snapshot.items.push(item);
    }

    fn finish(self) -> TaskUiSnapshot {
        self.snapshot
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct TaskStore {
    #[serde(default = "default_next_id")]
    next_id: u64,
    #[serde(default)]
    tasks: Vec<TaskItem>,
}

fn default_next_id() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskItem {
    id: String,
    subject: String,
    description: String,
    active_form: Option<String>,
    status: String,
    owner: Option<String>,
    #[serde(default)]
    blocks: Vec<String>,
    #[serde(default)]
    blocked_by: Vec<String>,
    #[serde(default)]
    metadata: Map<String, Value>,
}

#[derive(Deserialize)]
struct TodoInput {
    todos: Vec<TodoItem>,
}

pub struct TodoWriteTool;

#[async_trait]
impl Tool for TodoWriteTool {
    fn name(&self) -> &str {
        "TodoWrite"
    }

    fn description(&self) -> &str {
        "Replaces the current session checklist with pending, in_progress, or completed items."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "todos": {
                    "type": "array",
                    "maxItems": MAX_TODOS,
                    "items": {
                        "type": "object",
                        "properties": {
                            "content": {"type": "string", "maxLength": 4096},
                            "status": {"type": "string", "enum": STATUSES},
                            "activeForm": {"type": "string", "maxLength": 4096}
                        },
                        "required": ["content", "status", "activeForm"],
                        "additionalProperties": false
                    }
                }
            }),
            &["todos"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        format!(
            "{} items",
            input
                .get("todos")
                .and_then(Value::as_array)
                .map_or(0, Vec::len)
        )
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: TodoInput = parse_input(input)?;
        for (index, todo) in input.todos.iter().enumerate() {
            if todo.content.trim().is_empty() || todo.active_form.trim().is_empty() {
                bail!("todo #{} 的 content 和 activeForm 不能为空", index + 1)
            }
            validate_status(&todo.status)?;
        }
        let old = context.todos.lock().await.clone();
        let all_done = input.todos.iter().all(|todo| todo.status == "completed");
        *context.todos.lock().await = if all_done {
            Vec::new()
        } else {
            input.todos.clone()
        };
        Ok(ToolOutput::success(format!(
            "Todos updated: {} → {} items",
            old.len(),
            input.todos.len()
        )))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateInput {
    subject: String,
    description: String,
    active_form: Option<String>,
    metadata: Option<Map<String, Value>>,
}

pub struct TaskCreateTool;

#[async_trait]
impl Tool for TaskCreateTool {
    fn name(&self) -> &str {
        "TaskCreate"
    }

    fn description(&self) -> &str {
        "Creates a persistent task in the current workspace task list."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "subject": {"type": "string", "maxLength": 512},
                "description": {"type": "string", "maxLength": 16384},
                "activeForm": {"type": "string", "maxLength": 512},
                "metadata": {"type": "object"}
            }),
            &["subject", "description"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("subject")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: CreateInput = parse_input(input)?;
        if input.subject.trim().is_empty() || input.description.trim().is_empty() {
            bail!("subject 和 description 不能为空")
        }
        let _guard = context.task_store_lock.lock().await;
        let mut store = load_store(context)?;
        if store.tasks.len() >= MAX_TASKS {
            bail!("任务数量达到 {MAX_TASKS} 个限制")
        }
        if let Some(metadata) = &input.metadata {
            validate_metadata(metadata)?;
        }
        let id = store.next_id.max(1).to_string();
        let hook_outcome = context
            .hooks()
            .run(
                "TaskCreated",
                Some(&id),
                json!({
                    "task_id":&id,
                    "task_subject":input.subject.trim(),
                    "task_description":input.description.trim(),
                }),
                &context.cwd(),
            )
            .await?;
        store.next_id = store
            .next_id
            .max(1)
            .checked_add(1)
            .context("任务 ID 已耗尽")?;
        store.tasks.push(TaskItem {
            id: id.clone(),
            subject: input.subject.trim().to_owned(),
            description: input.description.trim().to_owned(),
            active_form: input.active_form.filter(|value| !value.trim().is_empty()),
            status: "pending".into(),
            owner: None,
            blocks: Vec::new(),
            blocked_by: Vec::new(),
            metadata: input.metadata.unwrap_or_default(),
        });
        save_store(context, &store)?;
        let mut output = ToolOutput::success(format!(
            "Task #{id} created successfully: {}",
            input.subject.trim()
        ));
        if !hook_outcome.additional_context.is_empty() {
            output.append_context(
                "TaskCreated hook context",
                &hook_outcome.additional_context.join("\n"),
            );
        }
        Ok(output)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetInput {
    task_id: String,
}

pub struct TaskGetTool;

#[async_trait]
impl Tool for TaskGetTool {
    fn name(&self) -> &str {
        "TaskGet"
    }

    fn description(&self) -> &str {
        "Gets a persistent task by ID."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({"taskId": {"type": "string", "maxLength": 64}}),
            &["taskId"],
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
            .get("taskId")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: GetInput = parse_input(input)?;
        let _guard = context.task_store_lock.lock().await;
        let store = load_store(context)?;
        let Some(task) = store.tasks.iter().find(|task| task.id == input.task_id) else {
            return Ok(ToolOutput::success("Task not found"));
        };
        Ok(ToolOutput::success(render_task(task)))
    }
}

pub struct TaskListTool;

#[async_trait]
impl Tool for TaskListTool {
    fn name(&self) -> &str {
        "TaskList"
    }

    fn description(&self) -> &str {
        "Lists persistent tasks for the current workspace."
    }

    fn input_schema(&self) -> Value {
        object_schema(json!({}), &[])
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn summary(&self, _: &Value) -> String {
        "current workspace".into()
    }

    async fn execute(&self, context: &ToolContext, _: Value) -> Result<ToolOutput> {
        let _guard = context.task_store_lock.lock().await;
        let store = load_store(context)?;
        let visible = store
            .tasks
            .iter()
            .filter(|task| task.metadata.get("_internal").and_then(Value::as_bool) != Some(true))
            .collect::<Vec<_>>();
        if visible.is_empty() {
            return Ok(ToolOutput::success("No tasks found"));
        }
        let completed = store
            .tasks
            .iter()
            .filter(|task| task.status == "completed")
            .map(|task| task.id.as_str())
            .collect::<HashSet<_>>();
        let lines = visible
            .into_iter()
            .map(|task| {
                let owner = task
                    .owner
                    .as_deref()
                    .map(|owner| format!(" ({owner})"))
                    .unwrap_or_default();
                let unresolved = task
                    .blocked_by
                    .iter()
                    .filter(|id| !completed.contains(id.as_str()))
                    .map(|id| format!("#{id}"))
                    .collect::<Vec<_>>();
                let blocked = if unresolved.is_empty() {
                    String::new()
                } else {
                    format!(" [blocked by {}]", unresolved.join(", "))
                };
                format!(
                    "#{} [{}] {}{owner}{blocked}",
                    task.id, task.status, task.subject
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(ToolOutput::success(lines))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct UpdateInput {
    task_id: String,
    subject: Option<String>,
    description: Option<String>,
    active_form: Option<String>,
    status: Option<String>,
    owner: Option<String>,
    add_blocks: Option<Vec<String>>,
    add_blocked_by: Option<Vec<String>>,
    metadata: Option<Map<String, Value>>,
}

pub struct TaskUpdateTool;

#[async_trait]
impl Tool for TaskUpdateTool {
    fn name(&self) -> &str {
        "TaskUpdate"
    }

    fn description(&self) -> &str {
        "Updates, links, completes, or deletes a persistent task."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "taskId": {"type": "string", "maxLength": 64},
                "subject": {"type": "string", "maxLength": 512},
                "description": {"type": "string", "maxLength": 16384},
                "activeForm": {"type": "string", "maxLength": 512},
                "status": {"type": "string", "enum": ["pending", "in_progress", "completed", "deleted"]},
                "owner": {"type": "string", "maxLength": 512},
                "addBlocks": {"type": "array", "maxItems": 100, "items": {"type": "string", "maxLength": 64}},
                "addBlockedBy": {"type": "array", "maxItems": 100, "items": {"type": "string", "maxLength": 64}},
                "metadata": {"type": "object"}
            }),
            &["taskId"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("taskId")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: UpdateInput = parse_input(input)?;
        if let Some(metadata) = &input.metadata {
            validate_metadata(metadata)?;
        }
        let _guard = context.task_store_lock.lock().await;
        let mut store = load_store(context)?;
        let Some(position) = store.tasks.iter().position(|task| task.id == input.task_id) else {
            return Ok(ToolOutput::error("Task not found"));
        };

        let completion_hook = if input.status.as_deref() == Some("completed")
            && store.tasks[position].status != "completed"
        {
            let task = &store.tasks[position];
            Some(
                context
                    .hooks()
                    .run(
                        "TaskCompleted",
                        Some(&task.id),
                        json!({
                            "task_id":&task.id,
                            "task_subject":&task.subject,
                            "task_description":&task.description,
                            "owner":&task.owner,
                        }),
                        &context.cwd(),
                    )
                    .await?,
            )
        } else {
            None
        };

        if input.status.as_deref() == Some("deleted") {
            store.tasks.remove(position);
            for task in &mut store.tasks {
                task.blocks.retain(|id| id != &input.task_id);
                task.blocked_by.retain(|id| id != &input.task_id);
            }
            save_store(context, &store)?;
            return Ok(ToolOutput::success(format!(
                "Deleted task #{}",
                input.task_id
            )));
        }
        if let Some(status) = &input.status {
            validate_status(status)?;
        }
        let relationships = input
            .add_blocks
            .iter()
            .flatten()
            .chain(input.add_blocked_by.iter().flatten())
            .collect::<Vec<_>>();
        for target in relationships {
            if target == &input.task_id {
                bail!("任务不能阻塞自身")
            }
            if !store.tasks.iter().any(|task| &task.id == target) {
                bail!("关联任务不存在: #{target}")
            }
        }

        let mut updated = Vec::new();
        {
            let task = &mut store.tasks[position];
            update_string(&mut task.subject, input.subject, "subject", &mut updated)?;
            update_string(
                &mut task.description,
                input.description,
                "description",
                &mut updated,
            )?;
            if let Some(active_form) = input.active_form {
                task.active_form =
                    (!active_form.trim().is_empty()).then(|| active_form.trim().to_owned());
                updated.push("activeForm");
            }
            if let Some(status) = input.status.filter(|status| task.status != *status) {
                task.status = status;
                updated.push("status");
            }
            if let Some(owner) = input.owner {
                task.owner = (!owner.trim().is_empty()).then(|| owner.trim().to_owned());
                updated.push("owner");
            }
            if let Some(metadata) = input.metadata {
                for (key, value) in metadata {
                    if value.is_null() {
                        task.metadata.remove(&key);
                    } else {
                        task.metadata.insert(key, value);
                    }
                }
                updated.push("metadata");
            }
        }

        for target_id in input.add_blocks.unwrap_or_default() {
            add_relation(&mut store, position, &target_id, true);
            updated.push("blocks");
        }
        for target_id in input.add_blocked_by.unwrap_or_default() {
            add_relation(&mut store, position, &target_id, false);
            updated.push("blockedBy");
        }
        updated.sort_unstable();
        updated.dedup();
        save_store(context, &store)?;
        let mut output = ToolOutput::success(format!(
            "Updated task #{}: {}",
            input.task_id,
            if updated.is_empty() {
                "no changes".into()
            } else {
                updated.join(", ")
            }
        ));
        if let Some(outcome) = completion_hook {
            if !outcome.additional_context.is_empty() {
                output.append_context(
                    "TaskCompleted hook context",
                    &outcome.additional_context.join("\n"),
                );
            }
        }
        Ok(output)
    }
}

fn validate_status(status: &str) -> Result<()> {
    if STATUSES.contains(&status) {
        Ok(())
    } else {
        bail!("无效任务状态: {status}")
    }
}

fn update_string(
    target: &mut String,
    value: Option<String>,
    field: &'static str,
    updated: &mut Vec<&'static str>,
) -> Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        bail!("{field} 不能为空")
    }
    if *target != value.trim() {
        *target = value.trim().to_owned();
        updated.push(field);
    }
    Ok(())
}

fn add_relation(store: &mut TaskStore, source: usize, target_id: &str, source_blocks: bool) {
    let target = store
        .tasks
        .iter()
        .position(|task| task.id == target_id)
        .expect("relationship target was validated");
    let source_id = store.tasks[source].id.clone();
    if source_blocks {
        push_unique(&mut store.tasks[source].blocks, target_id);
        push_unique(&mut store.tasks[target].blocked_by, &source_id);
    } else {
        push_unique(&mut store.tasks[source].blocked_by, target_id);
        push_unique(&mut store.tasks[target].blocks, &source_id);
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_owned());
    }
}

fn render_task(task: &TaskItem) -> String {
    let mut fields = BTreeMap::new();
    fields.insert("Task", format!("#{}: {}", task.id, task.subject));
    fields.insert("Status", task.status.clone());
    fields.insert("Description", task.description.clone());
    if let Some(owner) = &task.owner {
        fields.insert("Owner", owner.clone());
    }
    if !task.blocked_by.is_empty() {
        fields.insert("Blocked by", prefixed_ids(&task.blocked_by));
    }
    if !task.blocks.is_empty() {
        fields.insert("Blocks", prefixed_ids(&task.blocks));
    }
    fields
        .into_iter()
        .map(|(name, value)| format!("{name}: {value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn prefixed_ids(ids: &[String]) -> String {
    ids.iter()
        .map(|id| format!("#{id}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn load_store(context: &ToolContext) -> Result<TaskStore> {
    let path = context.task_store_path();
    if !path.exists() {
        return Ok(TaskStore::default());
    }
    if std::fs::symlink_metadata(&path)?.file_type().is_symlink() {
        bail!("拒绝读取 symlink task store")
    }
    let size = std::fs::metadata(&path)?.len();
    if size > MAX_TASK_STORE_BYTES {
        bail!("任务存储超过 {MAX_TASK_STORE_BYTES} 字节限制")
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&path)?
        .take(MAX_TASK_STORE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_TASK_STORE_BYTES as usize {
        bail!("任务存储超过 {MAX_TASK_STORE_BYTES} 字节限制")
    }
    let content = String::from_utf8(bytes)
        .with_context(|| format!("任务存储不是有效 UTF-8: {}", path.display()))?;
    let store: TaskStore = serde_json::from_str(&content)
        .with_context(|| format!("任务存储 JSON 损坏: {}", path.display()))?;
    if store.tasks.len() > MAX_TASKS {
        bail!("任务存储超过 {MAX_TASKS} 个任务限制")
    }
    Ok(store)
}

fn save_store(context: &ToolContext, store: &TaskStore) -> Result<()> {
    let content = serde_json::to_string_pretty(store)?;
    if content.len() > MAX_TASK_STORE_BYTES as usize {
        bail!("任务存储超过 {MAX_TASK_STORE_BYTES} 字节限制")
    }
    atomic_write_private(&context.task_store_path(), &(content + "\n"))
}

fn validate_metadata(metadata: &Map<String, Value>) -> Result<()> {
    if metadata.len() > MAX_METADATA_KEYS {
        bail!("metadata 超过 {MAX_METADATA_KEYS} 个键限制")
    }
    if serde_json::to_vec(metadata)?.len() > MAX_METADATA_BYTES {
        bail!("metadata 超过 {MAX_METADATA_BYTES} 字节限制")
    }
    Ok(())
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use crate::permissions::{PermissionManager, PermissionMode};

    fn test_context(workspace: &std::path::Path) -> ToolContext {
        let root = ToolContext::new(
            workspace.to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        root.set_task_store_path(workspace.join("tasks.json"));
        // Agent contexts intentionally cannot see root cron jobs. Using one
        // keeps this unit test isolated from the real per-user durable store.
        root.fork_for_agent()
    }

    fn task(id: &str, subject: String, status: &str) -> TaskItem {
        TaskItem {
            id: id.to_owned(),
            subject,
            description: "private long description is not part of the UI snapshot".to_owned(),
            active_form: Some("Working safely".to_owned()),
            status: status.to_owned(),
            owner: None,
            blocks: Vec::new(),
            blocked_by: Vec::new(),
            metadata: Map::new(),
        }
    }

    #[tokio::test]
    async fn ui_snapshot_reads_persistent_tasks_and_todos_without_tool_dispatch() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let mut hidden = task("2", "hidden".to_owned(), "pending");
        hidden.metadata.insert("_internal".to_owned(), json!(true));
        let subject = format!("visible\n\u{1b}{}", "界".repeat(300));
        save_store(
            &context,
            &TaskStore {
                next_id: 3,
                tasks: vec![task("1", subject, "in_progress"), hidden],
            },
        )
        .unwrap();
        *context.todos.lock().await = vec![TodoItem {
            content: "review snapshot".to_owned(),
            status: "pending".to_owned(),
            active_form: "Reviewing snapshot".to_owned(),
        }];

        let snapshot = context.task_ui_snapshot().await.unwrap();
        assert_eq!(snapshot.items.len(), 2);
        assert!(snapshot.truncated);
        let persistent = &snapshot.items[0];
        assert_eq!(persistent.kind, TaskUiItemKind::PersistentTask);
        assert_eq!(persistent.status, TaskUiStatus::InProgress);
        assert!(persistent.title.len() <= MAX_TASK_UI_TITLE_BYTES);
        assert!(
            !persistent
                .title
                .chars()
                .any(|character| matches!(character, '\n' | '\r' | '\u{1b}'))
        );
        assert_eq!(persistent.detail.as_deref(), Some("Working safely"));
        assert!(
            !serde_json::to_string(persistent)
                .unwrap()
                .contains("private long description")
        );
        let todo = &snapshot.items[1];
        assert_eq!(todo.kind, TaskUiItemKind::Todo);
        assert_eq!(todo.id, "todo-1");
        assert_eq!(todo.status, TaskUiStatus::Pending);
    }

    #[test]
    fn ui_snapshot_caps_each_source_and_reports_omissions() {
        let mut builder = SnapshotBuilder::default();
        builder.extend_limited(0..=MAX_TASK_UI_ITEMS_PER_SOURCE, |index| {
            bounded_ui_item(
                TaskUiItemKind::BackgroundTask,
                &format!("task-{index}"),
                "Background task",
                None,
                TaskUiStatus::Tracked,
            )
        });
        let snapshot = builder.finish();
        assert_eq!(snapshot.items.len(), MAX_TASK_UI_ITEMS_PER_SOURCE);
        assert!(snapshot.truncated);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        config::Settings,
        hooks::HookRunner,
        permissions::{PermissionManager, PermissionMode},
    };

    fn context_with_hooks(workspace: &std::path::Path, hooks: Value) -> ToolContext {
        let mut context = ToolContext::new(
            workspace.to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_task_store_path(workspace.join("tasks.json"));
        context.set_hooks(Arc::new(
            HookRunner::from_settings(&Settings {
                raw: json!({"hooks":hooks}),
            })
            .unwrap(),
        ));
        context
    }

    #[tokio::test]
    async fn task_hooks_add_context_and_block_completion_before_persistence() {
        let temp = tempfile::tempdir().unwrap();
        let context = context_with_hooks(
            temp.path(),
            json!({
                "TaskCreated":[{"matcher":"*", "hooks":[{
                    "type":"command",
                    "command":"printf '%s' '{\"additionalContext\":\"created-context\"}'"
                }]}],
                "TaskCompleted":[{"matcher":"*", "hooks":[{
                    "type":"command", "command":"printf blocked >&2; exit 2"
                }]}]
            }),
        );
        let created = TaskCreateTool
            .execute(
                &context,
                json!({"subject":"audit", "description":"verify parity"}),
            )
            .await
            .unwrap();
        assert!(created.content.contains("created-context"));

        let blocked = TaskUpdateTool
            .execute(&context, json!({"taskId":"1", "status":"completed"}))
            .await
            .unwrap_err();
        assert!(blocked.to_string().contains("blocked"));
        let task = TaskGetTool
            .execute(&context, json!({"taskId":"1"}))
            .await
            .unwrap();
        assert!(task.content.contains("Status: pending"));
    }
}

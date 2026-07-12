use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::{TodoItem, Tool, ToolContext, ToolOutput, atomic_write, object_schema, parse_input};

const STATUSES: &[&str] = &["pending", "in_progress", "completed"];

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
    fn name(&self) -> &'static str {
        "TodoWrite"
    }

    fn description(&self) -> &'static str {
        "Replaces the current session checklist with pending, in_progress, or completed items."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "todos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "content": {"type": "string"},
                            "status": {"type": "string", "enum": STATUSES},
                            "activeForm": {"type": "string"}
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
    fn name(&self) -> &'static str {
        "TaskCreate"
    }

    fn description(&self) -> &'static str {
        "Creates a persistent task in the current workspace task list."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "subject": {"type": "string"},
                "description": {"type": "string"},
                "activeForm": {"type": "string"},
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
        let id = store.next_id.max(1).to_string();
        store.next_id = store.next_id.max(1) + 1;
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
        Ok(ToolOutput::success(format!(
            "Task #{id} created successfully: {}",
            input.subject.trim()
        )))
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
    fn name(&self) -> &'static str {
        "TaskGet"
    }

    fn description(&self) -> &'static str {
        "Gets a persistent task by ID."
    }

    fn input_schema(&self) -> Value {
        object_schema(json!({"taskId": {"type": "string"}}), &["taskId"])
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
    fn name(&self) -> &'static str {
        "TaskList"
    }

    fn description(&self) -> &'static str {
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
    fn name(&self) -> &'static str {
        "TaskUpdate"
    }

    fn description(&self) -> &'static str {
        "Updates, links, completes, or deletes a persistent task."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "taskId": {"type": "string"},
                "subject": {"type": "string"},
                "description": {"type": "string"},
                "activeForm": {"type": "string"},
                "status": {"type": "string", "enum": ["pending", "in_progress", "completed", "deleted"]},
                "owner": {"type": "string"},
                "addBlocks": {"type": "array", "items": {"type": "string"}},
                "addBlockedBy": {"type": "array", "items": {"type": "string"}},
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
        let _guard = context.task_store_lock.lock().await;
        let mut store = load_store(context)?;
        let Some(position) = store.tasks.iter().position(|task| task.id == input.task_id) else {
            return Ok(ToolOutput::error("Task not found"));
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
            if let Some(status) = input.status
                && task.status != status
            {
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
        Ok(ToolOutput::success(format!(
            "Updated task #{}: {}",
            input.task_id,
            if updated.is_empty() {
                "no changes".into()
            } else {
                updated.join(", ")
            }
        )))
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
    if !context.task_store_path.exists() {
        return Ok(TaskStore::default());
    }
    let content = std::fs::read_to_string(&context.task_store_path)
        .with_context(|| format!("无法读取任务存储 {}", context.task_store_path.display()))?;
    serde_json::from_str(&content)
        .with_context(|| format!("任务存储 JSON 损坏: {}", context.task_store_path.display()))
}

fn save_store(context: &ToolContext, store: &TaskStore) -> Result<()> {
    let content = serde_json::to_string_pretty(store)?;
    atomic_write(&context.task_store_path, &(content + "\n"))
}

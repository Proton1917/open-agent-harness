mod bash;
mod edit;
mod glob;
mod grep;
mod notebook;
mod read;
mod schema;
mod skill;
mod tasks;
mod work_items;
mod write;

use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{process::Child, sync::Mutex};

use crate::permissions::{PermissionDecision, PermissionManager};
use crate::skills::SkillCatalog;

pub use bash::BashTool;
pub use edit::EditTool;
pub use glob::GlobTool;
pub use grep::GrepTool;
pub use notebook::NotebookEditTool;
pub use read::ReadTool;
pub use skill::SkillTool;
pub use tasks::{TaskOutputTool, TaskStopTool};
pub use work_items::{TaskCreateTool, TaskGetTool, TaskListTool, TaskUpdateTool, TodoWriteTool};
pub use write::WriteTool;

#[derive(Debug, Clone)]
pub struct FileSnapshot {
    pub content: String,
    pub partial: bool,
}

pub(crate) const MAX_EDITABLE_FILE_BYTES: usize = 256 * 1024;
const MAX_TOOL_RESULT_BYTES: usize = 256 * 1024;
const MAX_CONCURRENT_READ_TOOLS: usize = 8;

#[derive(Debug)]
pub struct BackgroundTask {
    pub child: Child,
    pub output_path: PathBuf,
    pub command: String,
    pub process_group_id: Option<u32>,
    pub drains: Vec<tokio::task::JoinHandle<()>>,
    pub output_truncated: Arc<AtomicBool>,
}

impl Drop for BackgroundTask {
    fn drop(&mut self) {
        let child_running = self.child.try_wait().ok().flatten().is_none();
        let drains_running = self.drains.iter().any(|drain| !drain.is_finished());
        if child_running || drains_running {
            #[cfg(unix)]
            if let Some(group) = self.process_group_id {
                // SAFETY: this group ID belongs to the child or descendants still holding pipes.
                unsafe {
                    libc::kill(-(group as i32), libc::SIGKILL);
                }
            }
            let _ = self.child.start_kill();
        }
        for drain in &self.drains {
            drain.abort();
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TodoItem {
    pub content: String,
    pub status: String,
    #[serde(rename = "activeForm")]
    pub active_form: String,
}

#[derive(Clone)]
pub struct ToolContext {
    pub cwd: PathBuf,
    pub workspace_root: PathBuf,
    pub permissions: Arc<PermissionManager>,
    pub read_cache: Arc<Mutex<HashMap<PathBuf, FileSnapshot>>>,
    pub tasks: Arc<Mutex<HashMap<String, BackgroundTask>>>,
    pub todos: Arc<Mutex<Vec<TodoItem>>>,
    pub skills: Arc<SkillCatalog>,
    pub task_store_lock: Arc<Mutex<()>>,
    pub task_store_path: PathBuf,
}

impl ToolContext {
    pub fn new(cwd: PathBuf, permissions: PermissionManager) -> Self {
        let task_store_path = task_store_path(&cwd);
        let workspace_root = std::fs::canonicalize(&cwd).unwrap_or_else(|_| cwd.clone());
        Self {
            cwd,
            workspace_root,
            permissions: Arc::new(permissions),
            read_cache: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            todos: Arc::new(Mutex::new(Vec::new())),
            skills: Arc::new(SkillCatalog::default()),
            task_store_lock: Arc::new(Mutex::new(())),
            task_store_path,
        }
    }

    pub fn resolve_path(&self, value: &str) -> Result<PathBuf> {
        if value.trim().is_empty() {
            bail!("路径不能为空");
        }
        let expanded = if value == "~" {
            dirs::home_dir().context("无法确定用户主目录")?
        } else if let Some(rest) = value.strip_prefix("~/") {
            dirs::home_dir().context("无法确定用户主目录")?.join(rest)
        } else {
            PathBuf::from(value)
        };
        Ok(if expanded.is_absolute() {
            expanded
        } else {
            self.cwd.join(expanded)
        })
    }

    pub fn is_outside_workspace(&self, value: &str) -> Result<bool> {
        let path = self.resolve_path(value)?;
        let resolved = canonicalize_for_scope(&path)
            .with_context(|| format!("无法解析路径边界: {}", path.display()))?;
        Ok(!resolved.starts_with(&self.workspace_root))
    }

    pub fn display_path(&self, path: &Path) -> String {
        if let Ok(relative) = path.strip_prefix(&self.cwd) {
            return if relative.as_os_str().is_empty() {
                ".".into()
            } else {
                relative.display().to_string()
            };
        }
        if let Some(home) = dirs::home_dir()
            && let Ok(relative) = path.strip_prefix(home)
        {
            return format!("~/{}", relative.display());
        }
        path.display().to_string()
    }

    pub async fn remember_read(&self, path: PathBuf, content: String, partial: bool) -> Result<()> {
        self.read_cache
            .lock()
            .await
            .insert(path, FileSnapshot { content, partial });
        Ok(())
    }

    pub async fn require_full_read(&self, path: &Path) -> Result<()> {
        let cache = self.read_cache.lock().await;
        let snapshot = cache
            .get(path)
            .ok_or_else(|| anyhow!("文件尚未读取；请先使用 Read"))?;
        if snapshot.partial {
            bail!("此前只读取了文件的一部分；修改前必须完整读取文件");
        }
        Ok(())
    }

    pub async fn verify_fresh_full_read(&self, path: &Path, current: &str) -> Result<()> {
        let cache = self.read_cache.lock().await;
        let snapshot = cache
            .get(path)
            .ok_or_else(|| anyhow!("文件尚未读取；请先使用 Read"))?;
        if snapshot.partial {
            bail!("此前只读取了文件的一部分；修改前必须完整读取文件");
        }
        if current != snapshot.content {
            bail!("文件在读取后已被用户或其他进程修改；请重新 Read 后再写入");
        }
        Ok(())
    }

    pub async fn shutdown_background_tasks(&self) {
        let mut tasks = self.tasks.lock().await;
        for task in tasks.values_mut() {
            bash::terminate_task(task).await;
        }
        tasks.clear();
    }

    pub async fn background_task_ids(&self) -> HashSet<String> {
        self.tasks.lock().await.keys().cloned().collect()
    }

    pub async fn rollback_background_tasks(&self, keep: &HashSet<String>) {
        let mut added = {
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
        for task in &mut added {
            bash::terminate_task(task).await;
            let _ = std::fs::remove_file(&task.output_path);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub content: String,
    pub is_error: bool,
}

impl ToolOutput {
    pub fn success(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: false,
        }
    }

    pub fn error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: true,
        }
    }

    fn bounded(mut self) -> Self {
        const MARKER: &str = "\n[Tool result truncated at the 256 KiB harness limit]";
        if self.content.len() <= MAX_TOOL_RESULT_BYTES {
            return self;
        }
        let mut end = MAX_TOOL_RESULT_BYTES.saturating_sub(MARKER.len());
        while !self.content.is_char_boundary(end) {
            end -= 1;
        }
        self.content.truncate(end);
        self.content.push_str(MARKER);
        self
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn input_schema(&self) -> Value;
    fn read_only(&self, input: &Value) -> bool;
    fn destructive(&self, _input: &Value) -> bool {
        false
    }
    fn requires_permission(&self) -> bool {
        true
    }
    fn path_fields(&self) -> &'static [&'static str] {
        &[]
    }
    fn concurrency_safe(&self, input: &Value) -> bool {
        self.read_only(input)
    }
    fn summary(&self, input: &Value) -> String;
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput>;

    fn api_definition(&self) -> Value {
        json!({
            "name": self.name(),
            "description": self.description(),
            "input_schema": self.input_schema(),
        })
    }
}

#[derive(Clone)]
pub struct ToolRegistry {
    tools: Arc<HashMap<String, Arc<dyn Tool>>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        let tools: Vec<Arc<dyn Tool>> = vec![
            Arc::new(BashTool),
            Arc::new(GlobTool),
            Arc::new(GrepTool),
            Arc::new(ReadTool),
            Arc::new(EditTool),
            Arc::new(NotebookEditTool),
            Arc::new(WriteTool),
            Arc::new(TaskOutputTool),
            Arc::new(TaskStopTool),
            Arc::new(TodoWriteTool),
            Arc::new(TaskCreateTool),
            Arc::new(TaskGetTool),
            Arc::new(TaskListTool),
            Arc::new(TaskUpdateTool),
            Arc::new(SkillTool),
        ];
        Self {
            tools: Arc::new(
                tools
                    .into_iter()
                    .map(|tool| (tool.name().to_owned(), tool))
                    .collect(),
            ),
        }
    }
}

impl ToolRegistry {
    pub fn definitions(&self) -> Vec<Value> {
        let mut tools: Vec<_> = self
            .tools
            .values()
            .map(|tool| tool.api_definition())
            .collect();
        tools.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        tools
    }

    pub async fn execute(&self, context: &ToolContext, name: &str, input: Value) -> ToolOutput {
        let Some(tool) = self.tools.get(name) else {
            return ToolOutput::error(format!("未知工具: {name}"));
        };
        if let Err(error) = schema::validate(&tool.input_schema(), &input) {
            return ToolOutput::error(format!("工具输入校验失败: {error}"));
        }
        let outside_workspace = match tool
            .path_fields()
            .iter()
            .filter_map(|field| input.get(*field).and_then(Value::as_str))
            .try_fold(false, |outside, path| {
                context
                    .is_outside_workspace(path)
                    .map(|current| outside || current)
            }) {
            Ok(outside) => outside,
            Err(error) => return ToolOutput::error(format!("路径边界检查失败: {error:#}")),
        };
        let summary = tool.summary(&input);
        if tool.requires_permission() {
            match context.permissions.decide(
                tool.name(),
                &summary,
                tool.read_only(&input),
                tool.destructive(&input),
                outside_workspace,
            ) {
                Ok(PermissionDecision::Allow) => {}
                Ok(PermissionDecision::Deny) => {
                    return ToolOutput::error("用户或权限规则拒绝了此工具调用");
                }
                Err(error) => return ToolOutput::error(format!("权限检查失败: {error:#}")),
            }
        }
        match tool.execute(context, input).await {
            Ok(output) => output,
            Err(error) => ToolOutput::error(format!("{error:#}")),
        }
        .bounded()
    }

    pub async fn execute_batch(
        &self,
        context: &ToolContext,
        calls: &[(String, Value)],
    ) -> Vec<ToolOutput> {
        let mut outputs = Vec::with_capacity(calls.len());
        let mut index = 0;
        while index < calls.len() {
            let (name, input) = &calls[index];
            let concurrency_safe = self
                .tools
                .get(name)
                .is_some_and(|tool| tool.concurrency_safe(input));
            if !concurrency_safe {
                outputs.push(self.execute(context, name, input.clone()).await);
                index += 1;
                continue;
            }

            let end = calls[index..]
                .iter()
                .position(|(candidate_name, candidate_input)| {
                    !self
                        .tools
                        .get(candidate_name)
                        .is_some_and(|tool| tool.concurrency_safe(candidate_input))
                })
                .map_or(calls.len(), |offset| index + offset);
            for chunk in calls[index..end].chunks(MAX_CONCURRENT_READ_TOOLS) {
                let concurrent = chunk.iter().map(|(candidate_name, candidate_input)| {
                    self.execute(context, candidate_name, candidate_input.clone())
                });
                outputs.extend(futures_util::future::join_all(concurrent).await);
            }
            index = end;
        }
        outputs
    }
}

fn task_store_path(cwd: &Path) -> PathBuf {
    let key = workspace_key(cwd);
    dirs::home_dir()
        .unwrap_or_else(|| cwd.to_owned())
        .join(".open-agent-harness/task-lists")
        .join(format!("{key}.json"))
}

pub(crate) fn workspace_key(path: &Path) -> String {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let hash = path
        .as_os_str()
        .as_encoded_bytes()
        .iter()
        .fold(OFFSET, |hash, byte| {
            (hash ^ u128::from(*byte)).wrapping_mul(PRIME)
        });
    format!("{hash:032x}")
}

fn canonicalize_for_scope(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return std::fs::canonicalize(path).context("canonicalize 失败");
    }
    if std::fs::symlink_metadata(path).is_ok() {
        return std::fs::canonicalize(path).context("拒绝无法解析的 symlink");
    }
    let parent = path.parent().context("路径没有可解析的父目录")?;
    let name = path.file_name().context("路径没有文件名")?;
    Ok(canonicalize_for_scope(parent)?.join(name))
}

pub(crate) fn parse_input<T: serde::de::DeserializeOwned>(input: Value) -> Result<T> {
    serde_json::from_value(input).context("工具输入不符合 schema")
}

pub(crate) fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

pub(crate) fn atomic_write(path: &Path, content: &str) -> Result<()> {
    let parent = path.parent().context("目标文件没有父目录")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("无法创建目录 {}", parent.display()))?;
    let temp = parent.join(format!(".open-agent-harness-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .with_context(|| format!("无法创建临时文件 {}", temp.display()))?;
        file.write_all(content.as_bytes())?;
        file.flush()?;
        if let Ok(metadata) = std::fs::metadata(path) {
            let _ = file.set_permissions(metadata.permissions());
        }
        std::fs::rename(&temp, path).with_context(|| format!("无法原子替换 {}", path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

pub(crate) fn read_text_bounded(path: &Path) -> Result<String> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("无法打开文本文件 {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take((MAX_EDITABLE_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_EDITABLE_FILE_BYTES {
        bail!(
            "文件超过 {} 字节的可编辑限制: {}",
            MAX_EDITABLE_FILE_BYTES,
            path.display()
        );
    }
    String::from_utf8(bytes).context("文件不是有效 UTF-8 文本")
}

pub(crate) fn reject_direct_symlink_write(path: &Path) -> Result<()> {
    if std::fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        bail!(
            "拒绝直接写入 symlink；请使用其规范化目标路径: {}",
            path.display()
        )
    }
    Ok(())
}

pub(crate) fn atomic_write_private(path: &Path, content: &str) -> Result<()> {
    let parent = path.parent().context("私有文件没有父目录")?;
    ensure_private_directory(parent)?;
    let temp = parent.join(format!(".open-agent-harness-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(content.as_bytes())?;
        file.flush()?;
        std::fs::rename(&temp, path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result.with_context(|| format!("无法原子替换私有文件 {}", path.display()))
}

pub(crate) fn ensure_private_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Some(home) = dirs::home_dir() {
            let harness_root = home.join(".open-agent-harness");
            if path.starts_with(&harness_root) {
                let mut current = harness_root.clone();
                std::fs::set_permissions(&current, std::fs::Permissions::from_mode(0o700))?;
                if let Ok(relative) = path.strip_prefix(&harness_root) {
                    for component in relative.components() {
                        current.push(component);
                        if current.is_dir() {
                            std::fs::set_permissions(
                                &current,
                                std::fs::Permissions::from_mode(0o700),
                            )?;
                        }
                    }
                }
                return Ok(());
            }
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{sync::Barrier, time::timeout};

    use super::*;
    use crate::permissions::PermissionMode;

    struct BarrierTool {
        barrier: Arc<Barrier>,
    }

    #[async_trait]
    impl Tool for BarrierTool {
        fn name(&self) -> &'static str {
            "BarrierRead"
        }

        fn description(&self) -> &'static str {
            "Test-only concurrent read"
        }

        fn input_schema(&self) -> Value {
            object_schema(json!({}), &[])
        }

        fn read_only(&self, _: &Value) -> bool {
            true
        }

        fn summary(&self, _: &Value) -> String {
            "barrier".into()
        }

        async fn execute(&self, _: &ToolContext, _: Value) -> Result<ToolOutput> {
            self.barrier.wait().await;
            Ok(ToolOutput::success("done"))
        }
    }

    #[tokio::test]
    async fn concurrency_safe_batch_runs_in_parallel_and_preserves_order() {
        let barrier = Arc::new(Barrier::new(2));
        let tool: Arc<dyn Tool> = Arc::new(BarrierTool { barrier });
        let registry = ToolRegistry {
            tools: Arc::new(HashMap::from([(tool.name().to_owned(), tool)])),
        };
        let temp = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        let calls = vec![
            ("BarrierRead".into(), json!({})),
            ("BarrierRead".into(), json!({})),
        ];
        let outputs = timeout(
            Duration::from_secs(1),
            registry.execute_batch(&context, &calls),
        )
        .await
        .expect("calls should reach the barrier together");
        assert_eq!(outputs.len(), 2);
        assert!(outputs.iter().all(|output| output.content == "done"));
    }

    #[test]
    fn tool_results_have_a_global_size_ceiling() {
        let output = ToolOutput::success("x".repeat(MAX_TOOL_RESULT_BYTES + 1024)).bounded();
        assert!(output.content.len() <= MAX_TOOL_RESULT_BYTES);
        assert!(output.content.ends_with("harness limit]"));
    }

    #[test]
    fn workspace_keys_are_fixed_length_and_path_safe() {
        let key = workspace_key(Path::new("/a/very/deep/workspace"));
        assert_eq!(key.len(), 32);
        assert!(key.chars().all(|character| character.is_ascii_hexdigit()));
        assert_eq!(key, workspace_key(Path::new("/a/very/deep/workspace")));
        assert_ne!(key, workspace_key(Path::new("/a/different/workspace")));
    }

    #[test]
    fn public_tool_surface_contains_the_documented_fifteen_tools() {
        let names = ToolRegistry::default()
            .definitions()
            .into_iter()
            .filter_map(|definition| definition["name"].as_str().map(ToOwned::to_owned))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), 15);
        for required in [
            "Read",
            "Write",
            "Edit",
            "NotebookEdit",
            "Glob",
            "Grep",
            "Bash",
            "TaskOutput",
            "TaskStop",
            "TodoWrite",
            "TaskCreate",
            "TaskGet",
            "TaskList",
            "TaskUpdate",
            "Skill",
        ] {
            assert!(names.contains(required), "missing {required}");
        }
    }
}

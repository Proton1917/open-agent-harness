mod bash;
mod edit;
mod glob;
mod grep;
mod notebook;
mod read;
pub(crate) mod schema;
mod skill;
mod tasks;
mod work_items;
mod write;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{process::Child, sync::Mutex};

use crate::agents::{AgentLimits, AgentRuntime};
use crate::hooks::HookRunner;
use crate::permissions::{PermissionDecision, PermissionManager};
use crate::{
    config::project_deny_rules,
    context::{discover_agent_instructions, render_agent_instructions},
    skills::{SkillCatalog, SkillDefinition, discover_skills, render_skill_index},
};

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

#[derive(Default)]
struct ReadCache {
    values: HashMap<PathBuf, FileSnapshot>,
    order: VecDeque<PathBuf>,
    bytes: usize,
}

pub(crate) const MAX_EDITABLE_FILE_BYTES: usize = 256 * 1024;
const MAX_TOOL_RESULT_BYTES: usize = 256 * 1024;
const MAX_CONCURRENT_READ_TOOLS: usize = 8;
const MAX_ACTIVE_TOOLS: usize = 128;
const MAX_DEFERRED_TOOLS: usize = 512;
const MAX_SELECTED_TOOLS: usize = 32;
const MAX_TOOL_NAME_BYTES: usize = 128;
const MAX_TOOL_DESCRIPTION_BYTES: usize = 8 * 1024;
const MAX_TOOL_SCHEMA_BYTES: usize = 256 * 1024;
const DEFAULT_WORKSPACE_CONTEXT_BUDGET: usize = 2 * 1024 * 1024;
const MAX_READ_CACHE_FILES: usize = 512;
const MAX_READ_CACHE_BYTES: usize = 64 * 1024 * 1024;

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
            crate::process::terminate_process_tree(self.process_group_id);
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
    location: Arc<RwLock<WorkspaceLocation>>,
    pub permissions: Arc<PermissionManager>,
    read_cache: Arc<Mutex<ReadCache>>,
    pub tasks: Arc<Mutex<HashMap<String, BackgroundTask>>>,
    pub todos: Arc<Mutex<Vec<TodoItem>>>,
    skills: Arc<RwLock<SkillCatalog>>,
    pub task_store_lock: Arc<Mutex<()>>,
    task_store_path: Arc<RwLock<PathBuf>>,
    agent_runtime: Arc<OnceLock<Arc<AgentRuntime>>>,
    agent_depth: usize,
    agent_scope: uuid::Uuid,
    agent_limits: AgentLimits,
    hooks: Arc<HookRunner>,
    bare: bool,
    workspace_context_base: Arc<OnceLock<String>>,
    workspace_context_overlay: Arc<RwLock<String>>,
    workspace_context_budget: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct WorkspaceLocation {
    cwd: PathBuf,
    root: PathBuf,
}

impl ToolContext {
    pub fn new(cwd: PathBuf, permissions: PermissionManager) -> Self {
        let cwd = std::fs::canonicalize(&cwd).unwrap_or(cwd);
        let task_store_path = task_store_path(&cwd);
        let workspace_root = cwd.clone();
        Self {
            location: Arc::new(RwLock::new(WorkspaceLocation {
                cwd,
                root: workspace_root,
            })),
            permissions: Arc::new(permissions),
            read_cache: Arc::new(Mutex::new(ReadCache::default())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            todos: Arc::new(Mutex::new(Vec::new())),
            skills: Arc::new(RwLock::new(SkillCatalog::default())),
            task_store_lock: Arc::new(Mutex::new(())),
            task_store_path: Arc::new(RwLock::new(task_store_path)),
            agent_runtime: Arc::new(OnceLock::new()),
            agent_depth: 0,
            agent_scope: uuid::Uuid::new_v4(),
            agent_limits: AgentLimits::default(),
            hooks: Arc::new(HookRunner::default()),
            bare: false,
            workspace_context_base: Arc::new(OnceLock::new()),
            workspace_context_overlay: Arc::new(RwLock::new(String::new())),
            workspace_context_budget: Arc::new(AtomicUsize::new(DEFAULT_WORKSPACE_CONTEXT_BUDGET)),
        }
    }

    pub fn set_bare(&mut self, bare: bool) {
        self.bare = bare;
    }

    pub fn set_workspace_context_budget(&self, bytes: usize) {
        self.workspace_context_budget
            .store(bytes, Ordering::Release);
    }

    pub fn set_skills(&self, skills: SkillCatalog) {
        *self
            .skills
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = skills;
    }

    pub fn set_task_store_path(&self, path: PathBuf) {
        *self
            .task_store_path
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = path;
    }

    pub fn task_store_path(&self) -> PathBuf {
        self.task_store_path
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn skill(&self, name: &str) -> Option<SkillDefinition> {
        self.skills
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(name)
            .cloned()
    }

    pub fn workspace_system_context(&self) -> String {
        let base = self
            .workspace_context_base
            .get()
            .map(String::as_str)
            .unwrap_or("");
        let overlay = self
            .workspace_context_overlay
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match (base.is_empty(), overlay.is_empty()) {
            (true, true) => String::new(),
            (false, true) => base.to_owned(),
            (true, false) => overlay.clone(),
            (false, false) => format!("{base}\n\n{overlay}"),
        }
    }

    pub async fn reload_workspace_context(&self) -> Result<()> {
        let cwd = self.cwd();
        let instructions = discover_agent_instructions(&cwd, self.bare).await?;
        let skill_cwd = cwd.clone();
        let bare = self.bare;
        let (skills, workspace_deny) = tokio::task::spawn_blocking(move || {
            let skills = discover_skills(&skill_cwd, bare)?;
            let deny = project_deny_rules(&skill_cwd, bare)?;
            Ok::<_, anyhow::Error>((skills, deny))
        })
        .await
        .context("workspace discovery worker 失败")??;
        let instruction_text = render_agent_instructions(&instructions);
        let skill_text = render_skill_index(&skills);
        let rendered = match (instruction_text.is_empty(), skill_text.is_empty()) {
            (true, true) => String::new(),
            (false, true) => instruction_text,
            (true, false) => skill_text,
            (false, false) => format!("{instruction_text}\n\n{skill_text}"),
        };
        let budget = self.workspace_context_budget.load(Ordering::Acquire);
        if rendered.len() > budget {
            bail!("workspace system context 超过 {budget} 字节预算")
        }
        let base = self.workspace_context_base.get_or_init(|| rendered.clone());
        let overlay = if base == &rendered {
            String::new()
        } else {
            format!(
                "# Current workspace context\n\nThe session changed working directories. The following current-workspace instructions and skills take precedence over the launch context.\n\n{rendered}"
            )
        };
        let effective_bytes = base
            .len()
            .saturating_add(if base.is_empty() || overlay.is_empty() {
                0
            } else {
                2
            })
            .saturating_add(overlay.len());
        if effective_bytes > budget {
            bail!("combined workspace system context 超过 {budget} 字节预算")
        }
        self.permissions.set_workspace_deny(workspace_deny);
        self.set_skills(skills);
        *self
            .workspace_context_overlay
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = overlay;
        Ok(())
    }

    pub fn set_agent_limits(&mut self, limits: AgentLimits) {
        self.agent_limits = limits;
    }

    pub fn set_hooks(&mut self, hooks: Arc<HookRunner>) {
        self.hooks = hooks;
    }

    pub fn hooks(&self) -> Arc<HookRunner> {
        Arc::clone(&self.hooks)
    }

    pub(crate) fn agent_limits(&self) -> AgentLimits {
        self.agent_limits
    }

    pub(crate) fn agent_depth(&self) -> usize {
        self.agent_depth
    }

    pub(crate) fn agent_scope(&self) -> uuid::Uuid {
        self.agent_scope
    }

    pub(crate) fn install_agent_runtime(&self, runtime: Arc<AgentRuntime>) -> Result<()> {
        self.agent_runtime
            .set(runtime)
            .map_err(|_| anyhow!("agent runtime 已经初始化"))
    }

    pub(crate) fn agent_runtime(&self) -> Result<Arc<AgentRuntime>> {
        self.agent_runtime
            .get()
            .cloned()
            .context("agent runtime 尚未初始化")
    }

    pub(crate) fn fork_for_agent(&self) -> Self {
        Self {
            location: Arc::new(RwLock::new(
                self.location
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            permissions: Arc::clone(&self.permissions),
            read_cache: Arc::new(Mutex::new(ReadCache::default())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            todos: Arc::new(Mutex::new(Vec::new())),
            skills: Arc::new(RwLock::new(
                self.skills
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            task_store_lock: Arc::clone(&self.task_store_lock),
            task_store_path: Arc::new(RwLock::new(self.task_store_path())),
            agent_runtime: Arc::clone(&self.agent_runtime),
            agent_depth: self.agent_depth.saturating_add(1),
            agent_scope: uuid::Uuid::new_v4(),
            agent_limits: self.agent_limits,
            hooks: Arc::clone(&self.hooks),
            bare: self.bare,
            workspace_context_base: Arc::clone(&self.workspace_context_base),
            workspace_context_overlay: Arc::new(RwLock::new(
                self.workspace_context_overlay
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            workspace_context_budget: Arc::clone(&self.workspace_context_budget),
        }
    }

    pub fn cwd(&self) -> PathBuf {
        self.location
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cwd
            .clone()
    }

    pub fn workspace_root(&self) -> PathBuf {
        self.location
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .root
            .clone()
    }

    pub async fn switch_workspace(&self, cwd: PathBuf, root: PathBuf) -> Result<()> {
        let cwd = std::fs::canonicalize(&cwd)
            .with_context(|| format!("无法解析新工作目录 {}", cwd.display()))?;
        let root = std::fs::canonicalize(&root)
            .with_context(|| format!("无法解析新工作区根目录 {}", root.display()))?;
        if !cwd.is_dir() || !root.is_dir() || !cwd.starts_with(&root) {
            bail!("新工作目录必须位于有效工作区根目录内")
        }
        let mut read_cache = self.read_cache.lock().await;
        *self
            .location
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = WorkspaceLocation { cwd, root };
        self.set_task_store_path(task_store_path(&self.cwd()));
        *read_cache = ReadCache::default();
        Ok(())
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
            self.cwd().join(expanded)
        })
    }

    pub fn is_outside_workspace(&self, value: &str) -> Result<bool> {
        let path = self.resolve_path(value)?;
        let resolved = canonicalize_for_scope(&path)
            .with_context(|| format!("无法解析路径边界: {}", path.display()))?;
        Ok(!resolved.starts_with(self.workspace_root()))
    }

    pub fn display_path(&self, path: &Path) -> String {
        if let Ok(relative) = path.strip_prefix(self.cwd()) {
            return if relative.as_os_str().is_empty() {
                ".".into()
            } else {
                relative.display().to_string()
            };
        }
        if let Some(relative) =
            dirs::home_dir().and_then(|home| path.strip_prefix(home).ok().map(Path::to_path_buf))
        {
            return format!("~/{}", relative.display());
        }
        path.display().to_string()
    }

    pub async fn remember_read(&self, path: PathBuf, content: String, partial: bool) -> Result<()> {
        let mut cache = self.read_cache.lock().await;
        if let Some(previous) = cache.values.remove(&path) {
            cache.bytes = cache.bytes.saturating_sub(previous.content.len());
            cache.order.retain(|candidate| candidate != &path);
        }
        cache.bytes = cache.bytes.saturating_add(content.len());
        cache.order.push_back(path.clone());
        cache.values.insert(path, FileSnapshot { content, partial });
        while cache.values.len() > MAX_READ_CACHE_FILES || cache.bytes > MAX_READ_CACHE_BYTES {
            let Some(oldest) = cache.order.pop_front() else {
                break;
            };
            if let Some(removed) = cache.values.remove(&oldest) {
                cache.bytes = cache.bytes.saturating_sub(removed.content.len());
            }
        }
        Ok(())
    }

    pub async fn require_full_read(&self, path: &Path) -> Result<()> {
        let cache = self.read_cache.lock().await;
        let snapshot = cache
            .values
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
            .values
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
    fn name(&self) -> &str;
    fn description(&self) -> &str;
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
    fn validate_input(&self, input: &Value) -> std::result::Result<(), String> {
        schema::validate(&self.input_schema(), input)
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

#[async_trait]
pub trait ToolService: Send + Sync {
    async fn shutdown(&self);
}

pub struct ToolRefresh {
    pub upsert: Vec<Arc<dyn Tool>>,
    pub remove: Vec<String>,
}

#[async_trait]
pub trait ToolDiscovery: Send + Sync {
    async fn refresh(&self) -> Result<ToolRefresh>;
}

struct RegistryState {
    active: HashMap<String, Arc<dyn Tool>>,
    deferred: HashMap<String, Arc<dyn Tool>>,
}

#[derive(Clone)]
pub struct ToolRegistry {
    state: Arc<RwLock<RegistryState>>,
    services: Arc<Vec<Arc<dyn ToolService>>>,
    discoverers: Arc<Vec<Arc<dyn ToolDiscovery>>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::with_extensions(Vec::new(), Vec::new()).expect("built-in tool registry must be valid")
    }
}

impl ToolRegistry {
    fn builtins() -> Vec<Arc<dyn Tool>> {
        vec![
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
        ]
    }

    pub fn with_extensions(
        active_extensions: Vec<Arc<dyn Tool>>,
        deferred_extensions: Vec<Arc<dyn Tool>>,
    ) -> Result<Self> {
        Self::with_services(active_extensions, deferred_extensions, Vec::new())
    }

    pub fn with_services(
        active_extensions: Vec<Arc<dyn Tool>>,
        deferred_extensions: Vec<Arc<dyn Tool>>,
        services: Vec<Arc<dyn ToolService>>,
    ) -> Result<Self> {
        Self::with_integrations(active_extensions, deferred_extensions, services, Vec::new())
    }

    pub fn with_integrations(
        active_extensions: Vec<Arc<dyn Tool>>,
        deferred_extensions: Vec<Arc<dyn Tool>>,
        services: Vec<Arc<dyn ToolService>>,
        discoverers: Vec<Arc<dyn ToolDiscovery>>,
    ) -> Result<Self> {
        let mut active = HashMap::new();
        let mut deferred = HashMap::new();
        for tool in Self::builtins().into_iter().chain(active_extensions) {
            validate_registry_tool(tool.as_ref())?;
            insert_unique_tool(&mut active, &deferred, tool)?;
        }
        for tool in deferred_extensions {
            validate_registry_tool(tool.as_ref())?;
            insert_unique_tool(&mut deferred, &active, tool)?;
        }
        let search_slots = usize::from(!deferred.is_empty());
        if active.len().saturating_add(search_slots) > MAX_ACTIVE_TOOLS {
            bail!("active tool 数量超过 {MAX_ACTIVE_TOOLS} 个限制")
        }
        if deferred.len() > MAX_DEFERRED_TOOLS {
            bail!("deferred tool 数量超过 {MAX_DEFERRED_TOOLS} 个限制")
        }
        let state = Arc::new(RwLock::new(RegistryState { active, deferred }));
        let discoverers = Arc::new(discoverers);
        if !read_registry(&state).deferred.is_empty() {
            let search: Arc<dyn Tool> = Arc::new(ToolSearchTool {
                state: Arc::downgrade(&state),
                discoverers: Arc::clone(&discoverers),
            });
            write_registry(&state)
                .active
                .insert(search.name().to_owned(), search);
        }
        Ok(Self {
            state,
            services: Arc::new(services),
            discoverers,
        })
    }

    pub fn definitions(&self) -> Vec<Value> {
        let state = read_registry(&self.state);
        let mut tools: Vec<_> = state
            .active
            .values()
            .map(|tool| tool.api_definition())
            .collect();
        tools.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        tools
    }

    pub async fn execute(&self, context: &ToolContext, name: &str, input: Value) -> ToolOutput {
        let tool = read_registry(&self.state).active.get(name).cloned();
        let Some(tool) = tool else {
            return ToolOutput::error(format!("未知工具: {name}"));
        };
        if let Err(error) = tool.validate_input(&input) {
            return ToolOutput::error(format!("工具输入校验失败: {error}"));
        }
        let hooks = context.hooks();
        let (input, pre_context) = match hooks.pre_tool(tool.name(), input, &context.cwd()).await {
            Ok(result) => result,
            Err(error) => return ToolOutput::error(format!("Pre-tool hook 拒绝调用: {error:#}")),
        };
        if let Err(error) = tool.validate_input(&input) {
            return ToolOutput::error(format!("hook 修改后的工具输入校验失败: {error}"));
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
        let mut output = match tool.execute(context, input.clone()).await {
            Ok(output) => output,
            Err(error) => ToolOutput::error(format!("{error:#}")),
        };
        if !pre_context.is_empty() {
            output.content.push_str("\n\n[Pre-tool hook context]\n");
            output.content.push_str(&pre_context.join("\n"));
        }
        hooks
            .post_tool(tool.name(), &input, output.bounded(), &context.cwd())
            .await
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
            let concurrency_safe = self.concurrency_safe(name, input);
            if !concurrency_safe {
                outputs.push(self.execute(context, name, input.clone()).await);
                index += 1;
                continue;
            }

            let end = calls[index..]
                .iter()
                .position(|(candidate_name, candidate_input)| {
                    !self.concurrency_safe(candidate_name, candidate_input)
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

    fn concurrency_safe(&self, name: &str, input: &Value) -> bool {
        read_registry(&self.state)
            .active
            .get(name)
            .is_some_and(|tool| tool.concurrency_safe(input))
    }

    pub fn deferred_count(&self) -> usize {
        read_registry(&self.state).deferred.len()
    }

    pub fn add_deferred(&self, tools: Vec<Arc<dyn Tool>>) -> Result<usize> {
        let mut state = write_registry(&self.state);
        let had_search = state.active.contains_key("ToolSearch");
        for tool in &tools {
            validate_registry_tool(tool.as_ref())?;
            if state.active.contains_key(tool.name()) || state.deferred.contains_key(tool.name()) {
                bail!("工具名称冲突: {}", tool.name())
            }
        }
        if state.deferred.len().saturating_add(tools.len()) > MAX_DEFERRED_TOOLS {
            bail!("deferred tool 数量超过 {MAX_DEFERRED_TOOLS} 个限制")
        }
        if !had_search && !tools.is_empty() && state.active.len() >= MAX_ACTIVE_TOOLS {
            bail!("active tool 数量将超过 {MAX_ACTIVE_TOOLS} 个限制")
        }
        let count = tools.len();
        for tool in tools {
            state.deferred.insert(tool.name().to_owned(), tool);
        }
        if !had_search && count > 0 {
            let search: Arc<dyn Tool> = Arc::new(ToolSearchTool {
                state: Arc::downgrade(&self.state),
                discoverers: Arc::clone(&self.discoverers),
            });
            state.active.insert(search.name().to_owned(), search);
        }
        Ok(count)
    }

    pub async fn shutdown(&self) {
        futures_util::future::join_all(self.services.iter().map(|service| service.shutdown()))
            .await;
    }
}

fn read_registry(
    state: &Arc<RwLock<RegistryState>>,
) -> std::sync::RwLockReadGuard<'_, RegistryState> {
    state
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_registry(
    state: &Arc<RwLock<RegistryState>>,
) -> std::sync::RwLockWriteGuard<'_, RegistryState> {
    state
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn insert_unique_tool(
    target: &mut HashMap<String, Arc<dyn Tool>>,
    other: &HashMap<String, Arc<dyn Tool>>,
    tool: Arc<dyn Tool>,
) -> Result<()> {
    if target.contains_key(tool.name()) || other.contains_key(tool.name()) {
        bail!("工具名称冲突: {}", tool.name())
    }
    target.insert(tool.name().to_owned(), tool);
    Ok(())
}

fn validate_registry_tool(tool: &dyn Tool) -> Result<()> {
    let name = tool.name();
    if name.is_empty()
        || name.len() > MAX_TOOL_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("无效工具名称: {name:?}")
    }
    if tool.description().len() > MAX_TOOL_DESCRIPTION_BYTES {
        bail!("工具 {name} 的 description 超过 {MAX_TOOL_DESCRIPTION_BYTES} 字节限制")
    }
    let schema = tool.input_schema();
    if !schema.is_object() {
        bail!("工具 {name} 的 input schema 必须是 object")
    }
    if serde_json::to_vec(&schema)?.len() > MAX_TOOL_SCHEMA_BYTES {
        bail!("工具 {name} 的 input schema 超过 {MAX_TOOL_SCHEMA_BYTES} 字节限制")
    }
    Ok(())
}

struct ToolSearchTool {
    state: Weak<RwLock<RegistryState>>,
    discoverers: Arc<Vec<Arc<dyn ToolDiscovery>>>,
}

#[async_trait]
impl Tool for ToolSearchTool {
    fn name(&self) -> &str {
        "ToolSearch"
    }

    fn description(&self) -> &str {
        "Search deferred tools by keyword, or load exact tools with query `select:name1,name2`. Load all tools needed for a task in one call."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({"query": {"type": "string", "minLength": 1, "maxLength": 4096}}),
            &["query"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let query = input
            .get("query")
            .and_then(Value::as_str)
            .context("query 必须是字符串")?
            .trim();
        let state = self.state.upgrade().context("工具注册表已经关闭")?;
        let refresh_errors = refresh_discovered_tools(&state, &self.discoverers).await;
        if let Some(selection) = query.strip_prefix("select:") {
            let requested = selection
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>();
            if requested.is_empty() {
                bail!("select 查询至少需要一个工具名")
            }
            if requested.len() > MAX_SELECTED_TOOLS {
                bail!("单次最多加载 {MAX_SELECTED_TOOLS} 个工具")
            }
            let mut registry = write_registry(&state);
            let new_active = requested
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .into_iter()
                .filter(|name| registry.deferred.contains_key(*name))
                .count();
            if registry.active.len().saturating_add(new_active) > MAX_ACTIVE_TOOLS {
                bail!("active tool 数量将超过 {MAX_ACTIVE_TOOLS} 个限制")
            }
            let mut loaded = Vec::new();
            let mut already_active = Vec::new();
            let mut missing = Vec::new();
            for name in requested {
                if registry.active.contains_key(name) {
                    already_active.push(name.to_owned());
                } else if let Some(tool) = registry.deferred.remove(name) {
                    registry.active.insert(name.to_owned(), tool);
                    loaded.push(name.to_owned());
                } else {
                    missing.push(name.to_owned());
                }
            }
            return Ok(ToolOutput::success(serde_json::to_string_pretty(&json!({
                "loaded": loaded,
                "already_active": already_active,
                "missing": missing,
                "remaining_deferred": registry.deferred.len(),
                "refresh_errors": refresh_errors,
            }))?));
        }

        let terms = query
            .split_whitespace()
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>();
        let registry = read_registry(&state);
        let mut matches = registry
            .deferred
            .values()
            .filter_map(|tool| {
                let haystack = format!(
                    "{} {}",
                    tool.name().to_ascii_lowercase(),
                    tool.description().to_ascii_lowercase()
                );
                terms.iter().all(|term| haystack.contains(term)).then(|| {
                    json!({
                        "name": tool.name(),
                        "description": truncate_utf8(tool.description(), 512),
                    })
                })
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
        matches.truncate(20);
        Ok(ToolOutput::success(serde_json::to_string_pretty(&json!({
            "query": query,
            "matches": matches,
            "total_deferred": registry.deferred.len(),
            "refresh_errors": refresh_errors,
        }))?))
    }
}

async fn refresh_discovered_tools(
    state: &Arc<RwLock<RegistryState>>,
    discoverers: &[Arc<dyn ToolDiscovery>],
) -> Vec<String> {
    let mut errors = Vec::new();
    for discoverer in discoverers {
        let refresh = match discoverer.refresh().await {
            Ok(refresh) => refresh,
            Err(error) => {
                errors.push(format!("{error:#}"));
                continue;
            }
        };
        if let Err(error) = apply_tool_refresh(state, refresh) {
            errors.push(format!("{error:#}"));
        }
    }
    errors
}

fn apply_tool_refresh(state: &Arc<RwLock<RegistryState>>, refresh: ToolRefresh) -> Result<()> {
    for tool in &refresh.upsert {
        validate_registry_tool(tool.as_ref())?;
    }
    let mut registry = write_registry(state);
    let removals = refresh.remove.into_iter().collect::<HashSet<_>>();
    let active_count = registry
        .active
        .keys()
        .filter(|name| !removals.contains(*name) || name.as_str() == "ToolSearch")
        .count();
    let mut deferred_count = registry
        .deferred
        .keys()
        .filter(|name| !removals.contains(*name))
        .count();
    for tool in &refresh.upsert {
        let name = tool.name();
        if registry.active.contains_key(name) && !removals.contains(name) {
            continue;
        }
        if registry.deferred.contains_key(name) && !removals.contains(name) {
            continue;
        }
        deferred_count = deferred_count.saturating_add(1);
    }
    if active_count > MAX_ACTIVE_TOOLS {
        bail!("refresh 后 active tool 数量超过 {MAX_ACTIVE_TOOLS} 个限制")
    }
    if deferred_count > MAX_DEFERRED_TOOLS {
        bail!("refresh 后 deferred tool 数量超过 {MAX_DEFERRED_TOOLS} 个限制")
    }
    for name in removals {
        if name != "ToolSearch" {
            registry.active.remove(&name);
            registry.deferred.remove(&name);
        }
    }
    for tool in refresh.upsert {
        let name = tool.name().to_owned();
        let RegistryState { active, deferred } = &mut *registry;
        match active.entry(name) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(tool);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                deferred.insert(entry.into_key(), tool);
            }
        }
    }
    Ok(())
}

fn truncate_utf8(value: &str, limit: usize) -> &str {
    if value.len() <= limit {
        return value;
    }
    let mut end = limit;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
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
        fn name(&self) -> &str {
            "BarrierRead"
        }

        fn description(&self) -> &str {
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
        let registry = ToolRegistry::with_extensions(vec![tool], Vec::new()).unwrap();
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

    #[tokio::test]
    async fn read_guard_cache_evicts_old_entries_at_its_file_limit() {
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
        for index in 0..=MAX_READ_CACHE_FILES {
            context
                .remember_read(
                    temp.path().join(format!("{index}.txt")),
                    index.to_string(),
                    false,
                )
                .await
                .unwrap();
        }
        assert!(
            context
                .require_full_read(&temp.path().join("0.txt"))
                .await
                .is_err()
        );
        assert!(
            context
                .require_full_read(&temp.path().join(format!("{MAX_READ_CACHE_FILES}.txt")))
                .await
                .is_ok()
        );
    }

    #[test]
    fn workspace_keys_are_fixed_length_and_path_safe() {
        let key = workspace_key(Path::new("/a/very/deep/workspace"));
        assert_eq!(key.len(), 32);
        assert!(key.chars().all(|character| character.is_ascii_hexdigit()));
        assert_eq!(key, workspace_key(Path::new("/a/very/deep/workspace")));
        assert_ne!(key, workspace_key(Path::new("/a/different/workspace")));
    }

    #[tokio::test]
    async fn workspace_relocation_refreshes_instructions_and_skills() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let current = temp.path().join("current");
        for (root, marker) in [(&launch, "launch-rule"), (&current, "current-rule")] {
            std::fs::create_dir_all(root.join(".open-agent-harness/skills/demo")).unwrap();
            std::fs::write(root.join("AGENTS.md"), marker).unwrap();
            std::fs::write(
                root.join(".open-agent-harness/skills/demo/SKILL.md"),
                format!("---\nname: demo\ndescription: {marker}\n---\n{marker}"),
            )
            .unwrap();
        }
        std::fs::write(
            current.join(".open-agent-harness/settings.json"),
            r#"{"permissions":{"deny":["WorkspaceDenyProbe"]}}"#,
        )
        .unwrap();
        let context = ToolContext::new(
            launch.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.reload_workspace_context().await.unwrap();
        let launch_task_store = context.task_store_path();
        assert_eq!(
            context
                .permissions
                .decide("WorkspaceDenyProbe", "probe", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert!(context.workspace_system_context().contains("launch-rule"));
        assert_eq!(context.skill("demo").unwrap().description, "launch-rule");

        context
            .switch_workspace(current.clone(), current.clone())
            .await
            .unwrap();
        context.reload_workspace_context().await.unwrap();
        assert_ne!(context.task_store_path(), launch_task_store);
        assert_eq!(
            context
                .permissions
                .decide("WorkspaceDenyProbe", "probe", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );
        let moved = context.workspace_system_context();
        assert!(moved.contains("launch-rule"));
        assert!(moved.contains("current-rule"));
        assert!(moved.contains("take precedence"));
        assert_eq!(context.skill("demo").unwrap().description, "current-rule");

        context
            .switch_workspace(launch.clone(), launch)
            .await
            .unwrap();
        context.reload_workspace_context().await.unwrap();
        assert_eq!(context.task_store_path(), launch_task_store);
        assert_eq!(
            context
                .permissions
                .decide("WorkspaceDenyProbe", "probe", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert!(
            !context
                .workspace_system_context()
                .contains("# Current workspace context")
        );
        assert_eq!(context.skill("demo").unwrap().description, "launch-rule");
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

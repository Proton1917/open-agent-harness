mod ask_user;
pub(crate) mod bash;
mod cron;
mod edit;
mod glob;
mod grep;
mod memory;
mod notebook;
mod read;
pub(crate) mod schema;
mod skill;
mod tasks;
mod team;
mod wakeup;
mod work_items;
mod workflow;
mod write;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    fs::OpenOptions,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, OnceLock, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use globset::Glob;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{process::Child, sync::Mutex};
use walkdir::WalkDir;

use crate::agents::{
    AgentLimits, AgentRuntime, AgentToolPolicy,
    team::{TeamMessageKind, TeamService},
};
use crate::cron::CronService;
use crate::file_history::{
    CheckpointBoundary, CheckpointInfo, DiffStats, FileHistory, RewindReport,
};
use crate::hooks::HookRunner;
use crate::interactions::{UserInteractionHandler, UserInteractionRequest};
use crate::monitor::{MonitorNotificationCheckpoint, MonitorService, MonitorTool};
use crate::permissions::{PermissionDecision, PermissionManager, PermissionTarget};
use crate::plugins::PluginMonitorDefinition;
use crate::process::SecretEnvScrubber;
use crate::sandbox::SandboxRuntime;
use crate::session::sanitize_transport_text;
use crate::workflow::WorkflowRuntime;
use crate::{
    config::{Settings, project_deny_rules},
    context::{
        InstructionFile, discover_agent_instructions, discover_nested_agent_instructions,
        render_agent_instructions,
    },
    skills::{SkillCatalog, SkillDefinition, discover_skills, render_skill_index},
};

pub type WorkspaceStateRecorder = Arc<dyn Fn(&Path, &Path) -> Result<()> + Send + Sync>;
pub type CurrentCwdStateRecorder = Arc<dyn Fn(&Path, &Path) -> Result<()> + Send + Sync>;

const MAX_FILE_SERVICE_CONTEXTS: usize = 64;
const MAX_FILE_SERVICE_CONTEXT_BYTES: usize = 128 * 1024;

fn append_bounded_file_service_context(
    output: &mut ToolOutput,
    contexts: &mut usize,
    bytes: &mut usize,
    label: &str,
    mut message: String,
) {
    if *contexts >= MAX_FILE_SERVICE_CONTEXTS || *bytes >= MAX_FILE_SERVICE_CONTEXT_BYTES {
        return;
    }
    let remaining = MAX_FILE_SERVICE_CONTEXT_BYTES.saturating_sub(*bytes);
    if message.len() > remaining {
        let mut end = remaining;
        while !message.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        message.truncate(end);
    }
    *contexts = (*contexts).saturating_add(1);
    *bytes = (*bytes).saturating_add(message.len());
    output.append_context(label, &message);
}

pub use ask_user::AskUserQuestionTool;
pub use bash::BashTool;
pub(crate) use bash::command_is_destructive;
pub use cron::{CronCreateTool, CronDeleteTool, CronListTool};
pub use edit::EditTool;
pub use glob::GlobTool;
pub use grep::GrepTool;
pub use memory::MemoryTool;
pub use notebook::NotebookEditTool;
pub use read::ReadTool;
pub use skill::SkillTool;
pub use tasks::{TaskOutputTool, TaskStopTool};
pub use team::TeamTool;
pub use wakeup::ScheduleWakeupTool;
pub use work_items::{
    TaskCreateTool, TaskGetTool, TaskListTool, TaskUiItem, TaskUiItemKind, TaskUiSnapshot,
    TaskUiStatus, TaskUpdateTool, TodoWriteTool,
};
pub use workflow::RunWorkflowTool;
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

#[derive(Default)]
struct HotRefreshTransactionManager {
    next_id: u64,
    retained_bytes: usize,
    frames: BTreeMap<u64, HotRefreshFileTransaction>,
    path_owners: HashMap<PathBuf, u64>,
}

struct HotRefreshFileTransaction {
    parent: Option<u64>,
    snapshots: BTreeMap<PathBuf, HotRefreshFileSnapshot>,
}

struct HotRefreshFileSnapshot {
    original: Option<Vec<u8>>,
    original_permissions: Option<std::fs::Permissions>,
    expected: Option<Vec<u8>>,
}

pub(crate) const MAX_EDITABLE_FILE_BYTES: usize = 256 * 1024;
const MAX_TOOL_RESULT_BYTES: usize = 256 * 1024;
const MAX_MODEL_TOOL_RESULT_BYTES: usize = 12 * 1024 * 1024;
const MAX_CONCURRENT_READ_TOOLS: usize = 8;
const MAX_ACTIVE_TOOLS: usize = 128;
const MAX_DEFERRED_TOOLS: usize = 512;
const MAX_SELECTED_TOOLS: usize = 32;
const DEFAULT_TOOL_SEARCH_RESULTS: usize = 5;
const MAX_TOOL_SEARCH_RESULTS: usize = 100;
const MAX_TOOL_NAME_BYTES: usize = 128;
const MAX_TOOL_DESCRIPTION_BYTES: usize = 8 * 1024;
const MAX_TOOL_SCHEMA_BYTES: usize = 256 * 1024;
const DEFAULT_WORKSPACE_CONTEXT_BUDGET: usize = 2 * 1024 * 1024;
const MAX_READ_CACHE_FILES: usize = 512;
const MAX_READ_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_HOT_REFRESH_TRANSACTION_FILES: usize = 64;
const MAX_HOT_REFRESH_TRANSACTION_BYTES: usize = 32 * 1024 * 1024;
const MAX_TRUSTED_WORKSPACE_ROOTS: usize = 33;
const MAX_BACKGROUND_NOTIFICATIONS: usize = 16;
const MAX_BACKGROUND_NOTIFICATION_BYTES: usize = 8 * 1024;
const MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES: usize = 64 * 1024;
const MAX_WORKSPACE_CONTEXT_CHANGE_ENTRIES: usize = 256;
const MAX_WORKSPACE_CONTEXT_CHANGED_PATHS: usize = 64;
const MAX_EXTERNAL_WATCH_SPECS: usize = 512;
const MAX_EXTERNAL_WATCH_ENTRIES: usize = 8 * 1024;
const MAX_EXTERNAL_WATCH_DEPTH: usize = 32;
const MAX_EXTERNAL_WATCH_EVENTS: usize = 256;
const MAX_EXTERNAL_WATCH_PATH_BYTES: usize = 16 * 1024;
const MAX_EXTERNAL_WATCH_PATH_TOTAL_BYTES: usize = 256 * 1024;
const MAX_EXTERNAL_WATCH_DYNAMIC_PATHS: usize = 128;
const MAX_EXTERNAL_WATCH_HASH_FILE_BYTES: u64 = 1024 * 1024;
const MAX_EXTERNAL_WATCH_HASH_TOTAL_BYTES: u64 = 32 * 1024 * 1024;
const MAX_EXTERNAL_WATCH_CONTEXTS: usize = 64;
const MAX_EXTERNAL_WATCH_CONTEXT_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ExternalWatchSpec {
    Exact(PathBuf),
    Tree(PathBuf),
    Glob { root: PathBuf, pattern: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExternalWatchFingerprint {
    kind: u8,
    length: u64,
    modified_ns: Option<u128>,
    digest: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExternalWatchEventKind {
    Add,
    Change,
    Unlink,
}

impl ExternalWatchEventKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Change => "change",
            Self::Unlink => "unlink",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExternalWatchEvent {
    path: PathBuf,
    kind: ExternalWatchEventKind,
}

#[derive(Debug, Clone, Default)]
struct ExternalFileWatchState {
    initialized: bool,
    specs: Vec<ExternalWatchSpec>,
    entries: BTreeMap<PathBuf, ExternalWatchFingerprint>,
    dynamic_paths: Vec<String>,
    dynamic_generation: u64,
    acknowledged: BTreeMap<PathBuf, Option<ExternalWatchFingerprint>>,
}

#[derive(Debug)]
pub struct BackgroundTask {
    pub child: Child,
    pub output_path: PathBuf,
    output_cleanup_armed: bool,
    pub command: String,
    pub(crate) process_tree: crate::process::ProcessTreeGuard,
    pub drains: Vec<tokio::task::JoinHandle<()>>,
    pub output_truncated: Arc<AtomicBool>,
    pub timeout_cancelled: Arc<AtomicBool>,
    pub timeout_ms: u64,
    pub timed_out: bool,
    pub notification_delivered: bool,
}

impl BackgroundTask {
    pub(crate) fn disarm_output_cleanup(&mut self) {
        self.output_cleanup_armed = false;
    }
}

impl Drop for BackgroundTask {
    fn drop(&mut self) {
        self.timeout_cancelled.store(true, Ordering::Release);
        let child_running = self.child.try_wait().ok().flatten().is_none();
        let drains_running = self.drains.iter().any(|drain| !drain.is_finished());
        self.process_tree.terminate();
        if child_running || drains_running {
            let _ = self.child.start_kill();
        }
        for drain in &self.drains {
            drain.abort();
        }
        if self.output_cleanup_armed {
            let _ = std::fs::remove_file(&self.output_path);
        }
    }
}

/// Stable ownership boundary for asynchronous work created by one query
/// context. Clones keep the same owner; agent forks append a fresh owner to
/// the lineage so ancestors may coordinate descendants without granting
/// sibling or descendant access in the opposite direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AsyncOwner {
    id: uuid::Uuid,
    lineage: Arc<Vec<uuid::Uuid>>,
}

impl AsyncOwner {
    fn root() -> Self {
        let id = uuid::Uuid::new_v4();
        Self {
            id,
            lineage: Arc::new(vec![id]),
        }
    }

    fn fork(&self) -> Self {
        let id = uuid::Uuid::new_v4();
        let mut lineage = self.lineage.as_ref().clone();
        lineage.push(id);
        Self {
            id,
            lineage: Arc::new(lineage),
        }
    }

    pub(crate) fn id(&self) -> uuid::Uuid {
        self.id
    }

    pub(crate) fn can_manage(&self, target: &Self) -> bool {
        target.lineage.starts_with(self.lineage.as_slice())
    }

    pub(crate) fn is_root(&self) -> bool {
        self.lineage.len() == 1
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TodoItem {
    pub content: String,
    pub status: String,
    #[serde(rename = "activeForm")]
    pub active_form: String,
}

#[derive(Default)]
struct WorkspaceSecurityRegistry {
    trusted_roots: Vec<PathBuf>,
    private_state_roots: Vec<PathBuf>,
}

#[derive(Clone)]
pub struct ToolContext {
    async_owner: AsyncOwner,
    location: Arc<RwLock<WorkspaceLocation>>,
    trusted_roots: Arc<RwLock<Vec<PathBuf>>>,
    workspace_security: Arc<RwLock<WorkspaceSecurityRegistry>>,
    explicit_context_roots: Arc<RwLock<HashSet<PathBuf>>>,
    pub permissions: Arc<PermissionManager>,
    read_cache: Arc<Mutex<ReadCache>>,
    pub tasks: Arc<Mutex<HashMap<String, BackgroundTask>>>,
    task_capture_root: Arc<RwLock<Option<PathBuf>>>,
    pub todos: Arc<Mutex<Vec<TodoItem>>>,
    skills: Arc<RwLock<SkillCatalog>>,
    extension_skills: Arc<RwLock<SkillCatalog>>,
    pub task_store_lock: Arc<Mutex<()>>,
    task_store_path: Arc<RwLock<PathBuf>>,
    agent_runtime: Arc<OnceLock<Arc<AgentRuntime>>>,
    execution_registry: Arc<OnceLock<ToolRegistry>>,
    workflow_runtime: WorkflowRuntime,
    agent_depth: usize,
    agent_limits: AgentLimits,
    agent_tool_policy: AgentToolPolicy,
    hooks: Arc<HookRunner>,
    bare: bool,
    workspace_context_launch_cwd: PathBuf,
    workspace_context_base: Arc<OnceLock<String>>,
    workspace_context_base_override: Arc<RwLock<Option<String>>>,
    workspace_context_overlay: Arc<RwLock<String>>,
    current_instruction_paths: Arc<RwLock<HashSet<PathBuf>>>,
    nested_instructions: Arc<RwLock<BTreeMap<PathBuf, InstructionFile>>>,
    workspace_context_budget: Arc<AtomicUsize>,
    workspace_context_refresh_lock: Arc<Mutex<()>>,
    workspace_context_changes: Arc<WorkspaceContextChanges>,
    workspace_context_parent_changes: Option<Arc<WorkspaceContextChanges>>,
    workspace_context_seen_generation: Arc<AtomicU64>,
    external_file_watch: Arc<StdMutex<ExternalFileWatchState>>,
    interaction_handler: Arc<RwLock<Option<UserInteractionHandler>>>,
    sandbox_runtime: Arc<RwLock<SandboxRuntime>>,
    file_history: Arc<RwLock<Option<FileHistory>>>,
    file_histories: Arc<RwLock<HashMap<String, FileHistory>>>,
    file_checkpoint: Arc<RwLock<Option<uuid::Uuid>>>,
    ancestor_file_checkpoints: Arc<RwLock<Vec<uuid::Uuid>>>,
    hot_refresh_transactions: Arc<RwLock<HotRefreshTransactionManager>>,
    hot_refresh_transaction: Arc<RwLock<Option<u64>>>,
    hot_refresh_parent_transaction: Option<u64>,
    team_identity: Arc<RwLock<Option<(uuid::Uuid, uuid::Uuid)>>>,
    team_mailboxes: Arc<RwLock<HashMap<uuid::Uuid, TrackedTeamMailbox>>>,
    workspace_state_recorder: Arc<RwLock<Option<WorkspaceStateRecorder>>>,
    current_cwd_state_recorder: Arc<RwLock<Option<CurrentCwdStateRecorder>>>,
    cron: CronService,
    monitor: MonitorService,
    secret_env_scrubber: SecretEnvScrubber,
}

#[derive(Debug, Clone)]
struct WorkspaceLocation {
    cwd: PathBuf,
    root: PathBuf,
}

#[derive(Clone)]
pub(crate) struct WorkspaceContextCheckpoint {
    base_override: Option<String>,
    overlay: String,
    current_instruction_paths: HashSet<PathBuf>,
    nested_instructions: BTreeMap<PathBuf, InstructionFile>,
    skills: SkillCatalog,
    workspace_deny: Vec<String>,
    seen_generation: u64,
}

#[derive(Clone)]
struct WorkspaceDiscovery {
    rendered: String,
    instruction_paths: HashSet<PathBuf>,
    skills: SkillCatalog,
    workspace_deny: Vec<String>,
}

struct WorkspaceContextCandidate {
    launch_rendered: String,
    overlay: String,
    current_instruction_paths: HashSet<PathBuf>,
    nested_instructions: BTreeMap<PathBuf, InstructionFile>,
    skills: SkillCatalog,
    workspace_deny: Vec<String>,
    instruction_hook_paths: Vec<PathBuf>,
    config_hook_paths: Vec<PathBuf>,
    changed_paths: Vec<PathBuf>,
}

#[derive(Default)]
struct WorkspaceContextChangeState {
    generation: u64,
    entries: VecDeque<(u64, Vec<PathBuf>)>,
}

#[derive(Default)]
struct WorkspaceContextChanges {
    state: RwLock<WorkspaceContextChangeState>,
}

impl WorkspaceContextChanges {
    fn publish(&self, mut paths: Vec<PathBuf>) -> u64 {
        paths.sort_unstable();
        paths.dedup();
        if paths.len() > MAX_WORKSPACE_CONTEXT_CHANGED_PATHS {
            paths.clear();
        }
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.generation = state.generation.saturating_add(1);
        let generation = state.generation;
        state.entries.push_back((generation, paths));
        while state.entries.len() > MAX_WORKSPACE_CONTEXT_CHANGE_ENTRIES {
            state.entries.pop_front();
        }
        generation
    }

    fn changes_since(&self, seen: u64) -> (u64, Option<Vec<PathBuf>>) {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if seen >= state.generation {
            return (state.generation, Some(Vec::new()));
        }
        if state
            .entries
            .front()
            .is_none_or(|(generation, _)| *generation > seen.saturating_add(1))
        {
            return (state.generation, None);
        }
        let mut paths = Vec::new();
        for (_, changed) in state
            .entries
            .iter()
            .filter(|(generation, _)| *generation > seen)
        {
            if changed.is_empty() {
                return (state.generation, None);
            }
            paths.extend(changed.iter().cloned());
            if paths.len() > MAX_WORKSPACE_CONTEXT_CHANGED_PATHS {
                return (state.generation, None);
            }
        }
        paths.sort_unstable();
        paths.dedup();
        (state.generation, Some(paths))
    }

    fn generation(&self) -> u64 {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
    }

    fn accumulated_changes(&self) -> (bool, Option<Vec<PathBuf>>) {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.entries.is_empty() {
            return (false, Some(Vec::new()));
        }
        let mut paths = Vec::new();
        for (_, changed) in &state.entries {
            if changed.is_empty() {
                return (true, None);
            }
            paths.extend(changed.iter().cloned());
            if paths.len() > MAX_WORKSPACE_CONTEXT_CHANGED_PATHS {
                return (true, None);
            }
        }
        paths.sort_unstable();
        paths.dedup();
        (true, Some(paths))
    }
}

#[derive(Clone)]
struct TrackedTeamMailbox {
    service: TeamService,
    actor: uuid::Uuid,
    delivered_through: u64,
}

pub(crate) type TeamNotificationCheckpoint = HashMap<uuid::Uuid, (uuid::Uuid, u64)>;

struct BashTaskCheckpoint {
    notification_delivered: bool,
    output_path: PathBuf,
    output_cleanup_armed: bool,
}

pub(crate) struct BackgroundNotificationCheckpoint {
    bash_tasks: HashMap<String, BashTaskCheckpoint>,
    workflow_tasks: HashMap<String, bool>,
    monitor: MonitorNotificationCheckpoint,
}

impl ToolContext {
    pub fn new(cwd: PathBuf, permissions: PermissionManager) -> Self {
        let cwd = std::fs::canonicalize(&cwd).unwrap_or(cwd);
        let task_store_path = task_store_path(&cwd);
        let workspace_root = cwd.clone();
        Self {
            async_owner: AsyncOwner::root(),
            location: Arc::new(RwLock::new(WorkspaceLocation {
                cwd: cwd.clone(),
                root: workspace_root.clone(),
            })),
            trusted_roots: Arc::new(RwLock::new(vec![cwd.clone()])),
            workspace_security: Arc::new(RwLock::new(WorkspaceSecurityRegistry {
                trusted_roots: vec![cwd.clone()],
                private_state_roots: Vec::new(),
            })),
            explicit_context_roots: Arc::new(RwLock::new(HashSet::new())),
            permissions: Arc::new(permissions),
            read_cache: Arc::new(Mutex::new(ReadCache::default())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            task_capture_root: Arc::new(RwLock::new(None)),
            todos: Arc::new(Mutex::new(Vec::new())),
            skills: Arc::new(RwLock::new(SkillCatalog::default())),
            extension_skills: Arc::new(RwLock::new(SkillCatalog::default())),
            task_store_lock: Arc::new(Mutex::new(())),
            task_store_path: Arc::new(RwLock::new(task_store_path)),
            agent_runtime: Arc::new(OnceLock::new()),
            execution_registry: Arc::new(OnceLock::new()),
            workflow_runtime: WorkflowRuntime::default(),
            agent_depth: 0,
            agent_limits: AgentLimits::default(),
            agent_tool_policy: AgentToolPolicy::default(),
            hooks: Arc::new(HookRunner::default()),
            bare: false,
            workspace_context_launch_cwd: cwd.clone(),
            workspace_context_base: Arc::new(OnceLock::new()),
            workspace_context_base_override: Arc::new(RwLock::new(None)),
            workspace_context_overlay: Arc::new(RwLock::new(String::new())),
            current_instruction_paths: Arc::new(RwLock::new(HashSet::new())),
            nested_instructions: Arc::new(RwLock::new(BTreeMap::new())),
            workspace_context_budget: Arc::new(AtomicUsize::new(DEFAULT_WORKSPACE_CONTEXT_BUDGET)),
            workspace_context_refresh_lock: Arc::new(Mutex::new(())),
            workspace_context_changes: Arc::new(WorkspaceContextChanges::default()),
            workspace_context_parent_changes: None,
            workspace_context_seen_generation: Arc::new(AtomicU64::new(0)),
            external_file_watch: Arc::new(StdMutex::new(ExternalFileWatchState::default())),
            interaction_handler: Arc::new(RwLock::new(None)),
            sandbox_runtime: Arc::new(RwLock::new(SandboxRuntime::default())),
            file_history: Arc::new(RwLock::new(None)),
            file_histories: Arc::new(RwLock::new(HashMap::new())),
            file_checkpoint: Arc::new(RwLock::new(None)),
            ancestor_file_checkpoints: Arc::new(RwLock::new(Vec::new())),
            hot_refresh_transactions: Arc::new(
                RwLock::new(HotRefreshTransactionManager::default()),
            ),
            hot_refresh_transaction: Arc::new(RwLock::new(None)),
            hot_refresh_parent_transaction: None,
            team_identity: Arc::new(RwLock::new(None)),
            team_mailboxes: Arc::new(RwLock::new(HashMap::new())),
            workspace_state_recorder: Arc::new(RwLock::new(None)),
            current_cwd_state_recorder: Arc::new(RwLock::new(None)),
            cron: CronService::for_workspace(&workspace_root),
            monitor: MonitorService::default(),
            secret_env_scrubber: SecretEnvScrubber::default(),
        }
    }

    pub(crate) fn set_secret_env_scrubber(&mut self, scrubber: SecretEnvScrubber) {
        self.secret_env_scrubber = scrubber;
    }

    pub fn configure_secret_env_scrubber(&mut self, settings: &Settings) -> Result<()> {
        self.set_secret_env_scrubber(SecretEnvScrubber::from_settings(settings)?);
        Ok(())
    }

    pub(crate) fn scrub_child_environment(&self, command: &mut tokio::process::Command) {
        self.secret_env_scrubber.scrub_tokio(command);
    }

    pub(crate) fn secret_env_scrubber(&self) -> SecretEnvScrubber {
        self.secret_env_scrubber.clone()
    }

    pub(crate) fn monitor_service(&self) -> MonitorService {
        self.monitor.clone()
    }

    /// Overrides the private task-capture directory for an embedding or test
    /// harness. Normal CLI runs leave this unset and use the private
    /// `~/.open-agent-harness/tasks` directory.
    pub fn set_task_capture_root(&self, root: PathBuf) -> Result<()> {
        ensure_private_directory(&root)?;
        let metadata = std::fs::symlink_metadata(&root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("task capture root 必须是非 symlink 目录")
        }
        let root = std::fs::canonicalize(root).context("无法解析 task capture root")?;
        *self
            .task_capture_root
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(root);
        Ok(())
    }

    pub(crate) fn task_capture_root(&self) -> Result<PathBuf> {
        if let Some(root) = self
            .task_capture_root
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            return Ok(root);
        }
        Ok(dirs::home_dir()
            .context("无法确定主目录")?
            .join(".open-agent-harness/tasks"))
    }

    pub(crate) fn cwd_marker_root(&self) -> Result<PathBuf> {
        if let Some(root) = self
            .task_capture_root
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            return Ok(root);
        }
        Ok(dirs::home_dir()
            .context("无法确定 shell cwd marker 主目录")?
            .join(".open-agent-harness/cwd-markers"))
    }

    pub(crate) fn async_owner(&self) -> AsyncOwner {
        self.async_owner.clone()
    }

    pub fn configure_plugin_monitors(&self, monitors: Vec<PluginMonitorDefinition>) {
        self.monitor.configure_plugin_monitors(monitors);
    }

    pub async fn start_always_plugin_monitors(&self) -> Vec<String> {
        self.monitor.start_always_plugin_monitors(self).await
    }

    pub async fn trigger_skill_monitors(&self, skill: &str) -> Vec<String> {
        self.monitor.trigger_skill_monitors(self, skill).await
    }

    pub async fn shutdown_monitors(&self) {
        if self.agent_depth == 0 {
            self.monitor.shutdown().await;
        }
    }

    pub fn cron_service(&self) -> CronService {
        self.cron.clone()
    }

    pub fn start_cron_scheduler(&self) -> Result<()> {
        self.cron.start()
    }

    pub fn take_scheduled_prompt(&self) -> Result<Option<String>> {
        self.cron.take_ready_prompt()
    }

    pub async fn wait_scheduled_prompt(&self) -> Result<String> {
        self.cron.wait_ready_prompt().await
    }

    pub fn stop_cron_scheduler(&self) {
        if self.agent_depth == 0 {
            self.cron.stop();
        }
    }

    pub fn set_user_interaction_handler(&self, handler: Option<UserInteractionHandler>) {
        *self
            .interaction_handler
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = handler;
    }

    pub fn set_workspace_state_recorder(&self, recorder: Option<WorkspaceStateRecorder>) {
        *self
            .workspace_state_recorder
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = recorder;
    }

    pub fn set_current_cwd_state_recorder(&self, recorder: Option<CurrentCwdStateRecorder>) {
        *self
            .current_cwd_state_recorder
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = recorder;
    }

    pub(crate) fn record_workspace_transition(&self) -> Result<()> {
        let recorder = self
            .workspace_state_recorder
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(recorder) = recorder {
            recorder(&self.cwd(), &self.workspace_root())?;
        }
        Ok(())
    }

    fn record_current_cwd_transition(&self) -> Result<()> {
        let recorder = self
            .current_cwd_state_recorder
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(recorder) = recorder {
            recorder(&self.cwd(), &self.workspace_root())?;
        }
        Ok(())
    }

    pub fn request_user_interaction(&self, tool: &str, input: Value) -> Result<Option<Value>> {
        let handler = self
            .interaction_handler
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        handler
            .map(|handler| {
                handler(&UserInteractionRequest {
                    tool: tool.to_owned(),
                    input,
                })
            })
            .transpose()
    }

    pub fn set_sandbox_runtime(&self, runtime: SandboxRuntime) {
        *self
            .sandbox_runtime
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = runtime;
    }

    pub fn sandbox_runtime(&self) -> SandboxRuntime {
        self.sandbox_runtime
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Adds directories explicitly trusted by the session (for example via
    /// `--add-dir`). Project settings never call this API, so they cannot widen
    /// filesystem scope.
    pub fn add_trusted_roots(&self, roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
        let total_path_bytes = roots
            .iter()
            .try_fold(0usize, |total, root| {
                total.checked_add(root.as_os_str().as_encoded_bytes().len())
            })
            .context("--add-dir 路径总长度溢出")?;
        if roots.len() > MAX_TRUSTED_WORKSPACE_ROOTS.saturating_sub(1)
            || total_path_bytes > 64 * 1024
        {
            bail!("--add-dir 超过 32 个目录或 64 KiB 路径总限制")
        }
        let mut accepted = Vec::new();
        let mut explicit = Vec::new();
        let mut trusted = self
            .trusted_roots
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for requested in roots {
            if requested.as_os_str().is_empty()
                || requested.as_os_str().as_encoded_bytes().len() > 4096
            {
                bail!("--add-dir 路径为空或超过 4096 字节")
            }
            let canonical = std::fs::canonicalize(requested)
                .with_context(|| format!("无法解析 --add-dir {}", requested.display()))?;
            if !canonical.is_dir() {
                bail!("--add-dir 不是目录: {}", requested.display())
            }
            self.register_security_trusted_root(&canonical)?;
            explicit.push(canonical.clone());
            if trusted.iter().any(|root| canonical.starts_with(root)) {
                continue;
            }
            if trusted.len() >= MAX_TRUSTED_WORKSPACE_ROOTS {
                bail!(
                    "可信工作区根目录超过 {} 个限制",
                    MAX_TRUSTED_WORKSPACE_ROOTS
                )
            }
            trusted.push(canonical.clone());
            accepted.push(canonical);
        }
        drop(trusted);
        self.explicit_context_roots
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(explicit);
        Ok(accepted)
    }

    pub fn trusted_roots(&self) -> Vec<PathBuf> {
        self.trusted_roots
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Removes a directory that was explicitly added to this session. The
    /// primary workspace root is immutable, and project settings cannot call
    /// this surface.
    pub fn remove_trusted_root(&self, requested: &Path) -> Result<PathBuf> {
        let canonical = std::fs::canonicalize(requested)
            .with_context(|| format!("无法解析可信工作区 {}", requested.display()))?;
        if canonical == self.workspace_root() {
            bail!("不能移除当前会话的主工作区")
        }
        let mut explicit = self
            .explicit_context_roots
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !explicit.contains(&canonical) {
            bail!("该目录不是本会话显式添加的工作区")
        }
        explicit.remove(&canonical);

        self.trusted_roots
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|root| root != &canonical);
        self.workspace_security
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .trusted_roots
            .retain(|root| root != &canonical);
        Ok(canonical)
    }

    pub fn reserve_private_state_root(&self, root: &Path) -> Result<()> {
        let root = std::fs::canonicalize(root)
            .with_context(|| format!("无法解析私有状态根目录 {}", root.display()))?;
        if !root.is_dir() {
            bail!("私有状态根目录不是目录")
        }
        let mut security = self
            .workspace_security
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for trusted in &security.trusted_roots {
            if paths_overlap(&root, trusted) {
                bail!("私有状态根目录不得与可信工作区重叠")
            }
        }
        if !security.private_state_roots.contains(&root) {
            security.private_state_roots.push(root);
        }
        Ok(())
    }

    fn register_security_trusted_root(&self, root: &Path) -> Result<()> {
        let mut security = self
            .workspace_security
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if security
            .private_state_roots
            .iter()
            .any(|private| paths_overlap(private, root))
        {
            bail!("可信工作区不得与私有状态根目录重叠")
        }
        if !security.trusted_roots.iter().any(|trusted| trusted == root) {
            security.trusted_roots.push(root.to_path_buf());
        }
        Ok(())
    }

    pub(crate) fn bind_team_identity(&self, team_id: uuid::Uuid, actor_id: uuid::Uuid) {
        *self
            .team_identity
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((team_id, actor_id));
    }

    pub(crate) fn bound_team_actor(&self, team_id: uuid::Uuid) -> Result<uuid::Uuid> {
        self.team_identity
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .filter(|(bound_team, _)| *bound_team == team_id)
            .map(|(_, actor)| actor)
            .context("当前 agent 未绑定到该 team，不能提交 actor 身份")
    }

    pub(crate) fn track_team_mailbox(&self, service: TeamService, actor: uuid::Uuid) {
        let team_id = service.id();
        let mut tracked = self
            .team_mailboxes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match tracked.get_mut(&team_id) {
            Some(mailbox) if mailbox.actor == actor => mailbox.service = service,
            _ => {
                tracked.insert(
                    team_id,
                    TrackedTeamMailbox {
                        service,
                        actor,
                        delivered_through: 0,
                    },
                );
            }
        }
    }

    pub(crate) fn untrack_team_mailbox(&self, team_id: uuid::Uuid) {
        self.team_mailboxes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&team_id);
    }

    pub(crate) fn record_team_mailbox_cursor(
        &self,
        team_id: uuid::Uuid,
        actor: uuid::Uuid,
        through_sequence: u64,
    ) {
        if let Some(mailbox) = self
            .team_mailboxes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(&team_id)
            .filter(|mailbox| mailbox.actor == actor)
        {
            mailbox.delivered_through = mailbox.delivered_through.max(through_sequence);
        }
    }

    pub(crate) fn team_notification_checkpoint(&self) -> TeamNotificationCheckpoint {
        self.team_mailboxes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(team_id, mailbox)| (*team_id, (mailbox.actor, mailbox.delivered_through)))
            .collect()
    }

    pub(crate) fn restore_team_notification_checkpoint(
        &self,
        checkpoint: &TeamNotificationCheckpoint,
    ) {
        let mut tracked = self
            .team_mailboxes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tracked.retain(|team_id, mailbox| {
            checkpoint
                .get(team_id)
                .is_some_and(|(actor, _)| *actor == mailbox.actor)
        });
        for (team_id, (actor, cursor)) in checkpoint {
            if let Some(mailbox) = tracked
                .get_mut(team_id)
                .filter(|mailbox| mailbox.actor == *actor)
            {
                mailbox.delivered_through = *cursor;
            }
        }
    }

    fn drain_team_notifications(&self, maximum: usize, maximum_bytes: usize) -> Vec<String> {
        if maximum == 0 || maximum_bytes == 0 {
            return Vec::new();
        }
        let mut teams = self
            .team_mailboxes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(team_id, mailbox)| {
                (
                    *team_id,
                    mailbox.service.clone(),
                    mailbox.actor,
                    mailbox.delivered_through,
                )
            })
            .collect::<Vec<_>>();
        teams.sort_unstable_by_key(|(team_id, _, _, _)| *team_id);
        let cwd = self.cwd();
        let mut notifications = Vec::new();
        let mut total_bytes = 0usize;
        for (team_id, service, actor, cursor) in teams {
            if notifications.len() >= maximum || total_bytes >= maximum_bytes {
                break;
            }
            let remaining = maximum - notifications.len();
            let Ok(messages) = service.read_mailbox(actor, actor, cursor, remaining.min(256))
            else {
                continue;
            };
            let mut delivered_through = cursor;
            for message in messages {
                let kind = match message.kind {
                    TeamMessageKind::Message => "message",
                    TeamMessageKind::Assignment => "assignment",
                    TeamMessageKind::Status => "status",
                };
                let mut notification = format!(
                    "Team {team_id} {kind} #{} from {}:\n{}",
                    message.sequence,
                    message.from,
                    sanitize_transport_text(&message.body, &cwd),
                );
                truncate_utf8_with_marker(
                    &mut notification,
                    MAX_BACKGROUND_NOTIFICATION_BYTES,
                    "\n[team notification truncated; use Team read for the full message]",
                );
                if total_bytes.saturating_add(notification.len()) > maximum_bytes {
                    break;
                }
                total_bytes += notification.len();
                delivered_through = message.sequence;
                notifications.push(notification);
            }
            if delivered_through > cursor {
                self.record_team_mailbox_cursor(team_id, actor, delivered_through);
            }
        }
        notifications
    }

    pub fn set_file_history(&self, history: FileHistory) {
        self.set_file_histories(vec![history])
            .expect("validated file history must install");
    }

    /// Starts a bounded, memory-only transaction for workspace context files
    /// when durable file history is disabled. This is deliberately separate
    /// from public checkpoints: it exists only so a failed AGENTS.md/SKILL.md
    /// refresh can undo the writes that made the in-memory context invalid.
    pub(crate) fn begin_hot_refresh_file_transaction(&self) -> Result<bool> {
        let transaction_ids = self.file_transaction_ids();
        if !transaction_ids.is_empty() {
            let histories = self
                .file_histories
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .values()
                .cloned()
                .collect::<Vec<_>>();
            for history in histories {
                for transaction_id in &transaction_ids {
                    if history.is_transaction_active(*transaction_id)? {
                        return Ok(false);
                    }
                }
            }
        }

        let mut local = self
            .hot_refresh_transaction
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if local.is_some() {
            bail!("已有活跃的临时 workspace context 文件事务")
        }
        let mut manager = self
            .hot_refresh_transactions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        manager.next_id = manager
            .next_id
            .checked_add(1)
            .context("临时 workspace context 文件事务 id 溢出")?;
        let id = manager.next_id;
        let parent = self
            .hot_refresh_parent_transaction
            .filter(|parent| manager.frames.contains_key(parent));
        manager.frames.insert(
            id,
            HotRefreshFileTransaction {
                parent,
                snapshots: BTreeMap::new(),
            },
        );
        *local = Some(id);
        Ok(true)
    }

    pub(crate) fn finish_hot_refresh_file_transaction(&self) -> Result<()> {
        let id = self
            .hot_refresh_transaction
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .context("找不到要结束的临时 workspace context 文件事务")?;
        let mut manager = self
            .hot_refresh_transactions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let transaction = manager
            .frames
            .remove(&id)
            .context("临时 workspace context 文件事务已丢失")?;
        let parent = transaction
            .parent
            .filter(|parent| manager.frames.contains_key(parent));

        for frame in manager.frames.values_mut() {
            if frame.parent == Some(id) {
                frame.parent = parent;
            }
        }

        if let Some(parent_id) = parent {
            for (path, child_snapshot) in transaction.snapshots {
                let owner_is_child = manager.path_owners.get(&path) == Some(&id);
                let parent_expected_bytes = manager
                    .frames
                    .get(&parent_id)
                    .and_then(|frame| frame.snapshots.get(&path))
                    .and_then(|snapshot| snapshot.expected.as_ref())
                    .map_or(0, Vec::len);
                let parent_has_snapshot = manager
                    .frames
                    .get(&parent_id)
                    .is_some_and(|frame| frame.snapshots.contains_key(&path));
                if parent_has_snapshot {
                    let child_original_bytes = child_snapshot.original.as_ref().map_or(0, Vec::len);
                    manager.retained_bytes = manager
                        .retained_bytes
                        .saturating_sub(child_original_bytes)
                        .saturating_sub(parent_expected_bytes);
                    manager
                        .frames
                        .get_mut(&parent_id)
                        .and_then(|frame| frame.snapshots.get_mut(&path))
                        .expect("validated parent snapshot must exist")
                        .expected = child_snapshot.expected;
                } else {
                    manager
                        .frames
                        .get_mut(&parent_id)
                        .expect("validated parent frame must exist")
                        .snapshots
                        .insert(path.clone(), child_snapshot);
                }
                if owner_is_child {
                    manager.path_owners.insert(path, parent_id);
                }
            }
        } else {
            for (path, snapshot) in transaction.snapshots {
                manager.retained_bytes = manager
                    .retained_bytes
                    .saturating_sub(hot_refresh_snapshot_bytes(&snapshot));
                if manager.path_owners.get(&path) == Some(&id) {
                    manager.path_owners.remove(&path);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn rollback_hot_refresh_file_transaction(&self) -> Result<()> {
        let id = self
            .hot_refresh_transaction
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .context("找不到要回滚的临时 workspace context 文件事务")?;
        let (paths, rollback) = {
            let mut manager = self
                .hot_refresh_transactions
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if manager
                .frames
                .values()
                .any(|frame| frame.parent == Some(id))
            {
                bail!("临时 workspace context 文件事务仍有活跃子事务，拒绝并发回滚")
            }
            let transaction = manager
                .frames
                .remove(&id)
                .context("临时 workspace context 文件事务已丢失")?;
            let parent = transaction
                .parent
                .filter(|parent| manager.frames.contains_key(parent));
            *self
                .hot_refresh_transaction
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            let paths = transaction.snapshots.keys().cloned().collect::<Vec<_>>();
            let rollback = rollback_hot_refresh_snapshots(&transaction.snapshots);
            for (path, snapshot) in transaction.snapshots {
                manager.retained_bytes = manager
                    .retained_bytes
                    .saturating_sub(hot_refresh_snapshot_bytes(&snapshot));
                if manager.path_owners.get(&path) == Some(&id) {
                    if let Some(parent) = parent.filter(|parent| {
                        manager
                            .frames
                            .get(parent)
                            .is_some_and(|frame| frame.snapshots.contains_key(&path))
                    }) {
                        manager.path_owners.insert(path, parent);
                    } else {
                        manager.path_owners.remove(&path);
                    }
                }
            }
            (paths, rollback)
        };

        if !paths.is_empty() {
            let mut cache = self.read_cache.lock().await;
            for path in &paths {
                if let Some(snapshot) = cache.values.remove(path) {
                    cache.bytes = cache.bytes.saturating_sub(snapshot.content.len());
                }
                cache.order.retain(|candidate| candidate != path);
            }
        }
        rollback
    }

    pub(crate) fn persistence_enabled(&self) -> bool {
        self.file_history
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(FileHistory::is_enabled)
    }

    /// Installs one durable history per trusted workspace root. Supplying every
    /// root is important for resume/fork because each root has an independent
    /// persisted manifest.
    pub fn set_file_histories(&self, histories: Vec<FileHistory>) -> Result<()> {
        let mut installed = HashMap::new();
        for history in histories {
            let root = std::fs::canonicalize(history.workspace()).with_context(|| {
                format!(
                    "无法解析 file-history 工作区 {}",
                    history.workspace().display()
                )
            })?;
            if self.root_for_resolved_path(&root).as_deref() != Some(root.as_path()) {
                bail!("file-history 工作区未被会话明确信任")
            }
            let key = workspace_key(&root);
            if installed.insert(key, history).is_some() {
                bail!("重复的 file-history 工作区")
            }
        }
        let Some(template) = installed.values().next().cloned() else {
            *self
                .file_history
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            self.file_histories
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
            return Ok(());
        };
        for root in self.trusted_roots() {
            let key = workspace_key(&root);
            if let std::collections::hash_map::Entry::Vacant(entry) = installed.entry(key) {
                entry.insert(template.relocate(&root)?);
            }
        }
        let active_key = workspace_key(&self.workspace_root());
        let active = installed
            .get(&active_key)
            .cloned()
            .context("当前工作区缺少 file-history")?;
        *self
            .file_histories
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = installed;
        *self
            .file_history
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(active);
        Ok(())
    }

    pub fn begin_file_checkpoint(
        &self,
        boundary: CheckpointBoundary,
        message_count: usize,
    ) -> Result<Option<CheckpointInfo>> {
        self.begin_file_checkpoint_with_id(uuid::Uuid::new_v4(), boundary, message_count)
    }

    pub fn begin_file_checkpoint_with_id(
        &self,
        id: uuid::Uuid,
        boundary: CheckpointBoundary,
        message_count: usize,
    ) -> Result<Option<CheckpointInfo>> {
        self.begin_file_checkpoint_with_id_and_ancestors(id, boundary, message_count, &[])
    }

    fn begin_file_checkpoint_with_id_and_ancestors(
        &self,
        id: uuid::Uuid,
        boundary: CheckpointBoundary,
        message_count: usize,
        ancestor_ids: &[uuid::Uuid],
    ) -> Result<Option<CheckpointInfo>> {
        let histories = self
            .file_histories
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if histories.is_empty() {
            return Ok(None);
        }
        if histories.iter().all(|history| !history.is_enabled()) {
            return Ok(None);
        }
        for history in &histories {
            if history.can_rewind(id)? {
                bail!("重复的 user message/checkpoint UUID")
            }
        }
        let mut info = None;
        for history in histories {
            let current =
                history.checkpoint_with_ancestors(id, boundary, message_count, ancestor_ids)?;
            info.get_or_insert(current);
        }
        *self
            .file_checkpoint
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(id);
        Ok(info)
    }

    pub fn track_before_edit(&self, path: &Path) -> Result<()> {
        let history = self.file_history_for_path(path)?;
        if let Some(history) = history {
            for checkpoint in self.file_transaction_ids() {
                if history.is_transaction_active(checkpoint)? {
                    history.track_before_edit(checkpoint, path)?;
                }
            }
        }
        self.track_hot_refresh_before_edit(path)
    }

    pub fn expect_after_edit(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let history = self.file_history_for_path(path)?;
        if let Some(history) = history {
            for checkpoint in self.file_transaction_ids() {
                if history.is_transaction_active(checkpoint)? {
                    history.expect_after_edit(checkpoint, path, bytes)?;
                }
            }
        }
        self.expect_hot_refresh_after_edit(path, bytes)
    }

    fn track_hot_refresh_before_edit(&self, path: &Path) -> Result<()> {
        let Some(id) = *self
            .hot_refresh_transaction
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        else {
            return Ok(());
        };
        let Some(resolved) = self.hot_refresh_sensitive_path(path)? else {
            return Ok(());
        };
        let mut manager = self
            .hot_refresh_transactions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let already_snapshotted = manager
            .frames
            .get(&id)
            .is_some_and(|frame| frame.snapshots.contains_key(&resolved));
        if let Some(owner) = manager.path_owners.get(&resolved).copied() {
            if owner == id {
                if already_snapshotted {
                    return Ok(());
                }
                bail!("workspace context 文件 owner 缺少事务快照")
            }
            // A child may take a fresh nested snapshot from its ancestor. The
            // reverse direction is forbidden: while the child owns the path,
            // the parent must fail before its tool can write any bytes.
            if already_snapshotted {
                bail!(
                    "workspace context 文件正被活跃子事务修改: {}",
                    self.display_path(&resolved)
                )
            }
            if !hot_refresh_transaction_is_ancestor(&manager.frames, owner, id) {
                bail!(
                    "workspace context 文件正被另一个并发事务修改: {}",
                    self.display_path(&resolved)
                )
            }
        }
        let snapshot = read_hot_refresh_snapshot(&resolved)?;
        let retained = hot_refresh_snapshot_bytes(&snapshot);
        let snapshot_count = manager
            .frames
            .values()
            .map(|frame| frame.snapshots.len())
            .sum::<usize>();
        if snapshot_count >= MAX_HOT_REFRESH_TRANSACTION_FILES {
            bail!(
                "临时 workspace context 文件事务超过 {MAX_HOT_REFRESH_TRANSACTION_FILES} 个文件限制"
            )
        }
        if manager.retained_bytes.saturating_add(retained) > MAX_HOT_REFRESH_TRANSACTION_BYTES {
            bail!(
                "临时 workspace context 文件事务超过 {MAX_HOT_REFRESH_TRANSACTION_BYTES} 字节限制"
            )
        }
        manager.retained_bytes += retained;
        manager
            .frames
            .get_mut(&id)
            .context("临时 workspace context 文件事务已丢失")?
            .snapshots
            .insert(resolved.clone(), snapshot);
        manager.path_owners.insert(resolved, id);
        Ok(())
    }

    fn expect_hot_refresh_after_edit(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        let Some(id) = *self
            .hot_refresh_transaction
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        else {
            return Ok(());
        };
        let Some(resolved) = self.hot_refresh_sensitive_path(path)? else {
            return Ok(());
        };
        if bytes.len() > MAX_EDITABLE_FILE_BYTES {
            bail!("workspace context 修改后状态超过可编辑文件限制")
        }
        let mut manager = self
            .hot_refresh_transactions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if manager.path_owners.get(&resolved) != Some(&id) {
            bail!(
                "workspace context 文件事务不再拥有修改目标: {}",
                self.display_path(&resolved)
            )
        }
        let previous = manager
            .frames
            .get(&id)
            .and_then(|frame| frame.snapshots.get(&resolved))
            .context("workspace context 修改后状态缺少修改前快照")?
            .expected
            .as_deref()
            .map_or(0, <[u8]>::len);
        let retained = manager
            .retained_bytes
            .saturating_sub(previous)
            .saturating_add(bytes.len());
        if retained > MAX_HOT_REFRESH_TRANSACTION_BYTES {
            bail!(
                "临时 workspace context 文件事务超过 {MAX_HOT_REFRESH_TRANSACTION_BYTES} 字节限制"
            )
        }
        manager
            .frames
            .get_mut(&id)
            .and_then(|frame| frame.snapshots.get_mut(&resolved))
            .expect("validated hot refresh snapshot must exist")
            .expected = Some(bytes.to_vec());
        manager.retained_bytes = retained;
        Ok(())
    }

    fn hot_refresh_sensitive_path(&self, path: &Path) -> Result<Option<PathBuf>> {
        if self.bare {
            return Ok(None);
        }
        let resolved = canonicalize_for_scope(path).with_context(|| {
            format!("无法解析临时 workspace context 事务目标 {}", path.display())
        })?;
        let Some(root) = self.root_for_resolved_path(&resolved) else {
            return Ok(None);
        };
        let relative = resolved
            .strip_prefix(&root)
            .expect("resolved trusted path must be under its selected root");
        let is_instruction = resolved.file_name().is_some_and(|name| name == "AGENTS.md");
        Ok((is_instruction || is_project_skill_path(relative)).then_some(resolved))
    }

    pub(crate) fn begin_detached_file_checkpoint(&mut self) -> Result<Option<CheckpointInfo>> {
        if let Some(parent) = *self
            .file_checkpoint
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            let mut ancestors = self
                .ancestor_file_checkpoints
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !ancestors.contains(&parent) {
                ancestors.push(parent);
            }
        }
        self.file_checkpoint = Arc::new(RwLock::new(None));
        let ancestors = self
            .ancestor_file_checkpoints
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        self.begin_file_checkpoint_with_id_and_ancestors(
            uuid::Uuid::new_v4(),
            CheckpointBoundary::Turn,
            0,
            &ancestors,
        )
    }

    fn file_transaction_ids(&self) -> Vec<uuid::Uuid> {
        let mut checkpoints = self
            .ancestor_file_checkpoints
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(current) = *self
            .file_checkpoint
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            if !checkpoints.contains(&current) {
                checkpoints.push(current);
            }
        }
        checkpoints
    }

    pub fn rewind_files(
        &self,
        checkpoint: uuid::Uuid,
        max_message_count: usize,
    ) -> Result<(RewindReport, usize)> {
        self.apply_file_checkpoint(checkpoint, max_message_count, false)
    }

    pub fn rollback_file_checkpoint(
        &self,
        checkpoint: uuid::Uuid,
        max_message_count: usize,
    ) -> Result<(RewindReport, usize)> {
        self.apply_file_checkpoint(checkpoint, max_message_count, true)
    }

    pub fn diff_file_checkpoint(
        &self,
        checkpoint: uuid::Uuid,
        max_message_count: usize,
    ) -> Result<(DiffStats, usize)> {
        let histories = self
            .file_histories
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut matching = Vec::new();
        for history in histories {
            if let Some(info) = history
                .checkpoints()?
                .into_iter()
                .find(|info| info.id == checkpoint)
            {
                if info.message_count > max_message_count {
                    bail!("checkpoint message_count 超过当前会话历史")
                }
                matching.push((history, info));
            }
        }
        let message_count = matching
            .first()
            .map(|(_, info)| info.message_count)
            .context("找不到 file history checkpoint")?;
        if matching
            .iter()
            .any(|(_, info)| info.message_count != message_count)
        {
            bail!("跨 workspace checkpoint message_count 不一致")
        }
        let mut combined = DiffStats::default();
        for (history, _) in matching {
            let stats = history.diff_stats(checkpoint)?;
            combined.insertions = combined.insertions.saturating_add(stats.insertions);
            combined.deletions = combined.deletions.saturating_add(stats.deletions);
            combined.files_changed.extend(stats.files_changed);
        }
        Ok((combined, message_count))
    }

    fn apply_file_checkpoint(
        &self,
        checkpoint: uuid::Uuid,
        max_message_count: usize,
        transaction_only: bool,
    ) -> Result<(RewindReport, usize)> {
        let histories = self
            .file_histories
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut matching = Vec::new();
        for history in histories {
            if let Some(info) = history
                .checkpoints()?
                .into_iter()
                .find(|info| info.id == checkpoint)
            {
                if info.message_count > max_message_count {
                    bail!("checkpoint message_count 超过当前会话历史")
                }
                matching.push((history, info));
            }
        }
        let message_count = matching
            .first()
            .map(|(_, info)| info.message_count)
            .context("找不到 file history checkpoint")?;
        if matching
            .iter()
            .any(|(_, info)| info.message_count != message_count)
        {
            bail!("跨 workspace checkpoint message_count 不一致")
        }
        // Two-phase cross-root operation: validate every manifest, backup,
        // destination and rollback ownership check before the first root is
        // mutated.
        let validation = matching.iter().try_for_each(|(history, _)| {
            if transaction_only {
                history.validate_rollback(checkpoint)
            } else {
                history.validate_rewind(checkpoint)
            }
        });
        if let Err(error) = validation {
            if transaction_only {
                for (history, _) in &matching {
                    if let Err(mark_error) = history.mark_rollback_conflict(checkpoint) {
                        return Err(error.context(format!(
                            "跨 workspace rollback 冲突状态持久化失败: {mark_error:#}"
                        )));
                    }
                }
            }
            return Err(error);
        }
        let mut combined = RewindReport::default();
        for (history, _) in matching {
            let report = if transaction_only {
                history.rollback_checkpoint(checkpoint)?
            } else {
                history.rewind(checkpoint)?
            };
            combined.restored = combined.restored.saturating_add(report.restored);
            combined.deleted = combined.deleted.saturating_add(report.deleted);
            combined.files_changed.extend(report.files_changed);
        }
        Ok((combined, message_count))
    }

    pub(crate) fn finish_file_checkpoint(&self, checkpoint: uuid::Uuid) -> Result<()> {
        let histories = self
            .file_histories
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut found = false;
        for history in histories {
            if history.can_rewind(checkpoint)? {
                history.finish_transaction(checkpoint)?;
                found = true;
            }
        }
        if !found {
            bail!("找不到要结束的 file history checkpoint")
        }
        let mut active = self
            .file_checkpoint
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *active == Some(checkpoint) {
            *active = None;
        }
        self.ancestor_file_checkpoints
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|ancestor| *ancestor != checkpoint);
        Ok(())
    }

    pub(crate) fn file_checkpoint_active(&self, checkpoint: uuid::Uuid) -> Result<bool> {
        let histories = self
            .file_histories
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for history in histories {
            if history.is_transaction_active(checkpoint)? {
                return Ok(true);
            }
        }
        Ok(false)
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

    pub fn set_extension_skills(&self, skills: SkillCatalog) {
        *self
            .extension_skills
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = skills;
    }

    pub fn skill_catalog(&self) -> SkillCatalog {
        self.skills
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
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

    /// Returns a bounded, read-only snapshot for local terminal rendering.
    /// This bypasses the model-facing tool dispatch path and grants no task
    /// mutation capability.
    pub async fn task_ui_snapshot(&self) -> Result<TaskUiSnapshot> {
        work_items::task_ui_snapshot(self).await
    }

    pub fn skill(&self, name: &str) -> Option<SkillDefinition> {
        self.skills
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(name)
            .cloned()
    }

    pub fn workspace_system_context(&self) -> String {
        let base = self.effective_workspace_context_base();
        let overlay = self
            .workspace_context_overlay
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let nested = self.render_nested_instruction_context();
        [base, overlay.clone(), nested]
            .into_iter()
            .filter(|section| !section.is_empty())
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    fn effective_workspace_context_base(&self) -> String {
        if let Some(override_context) = self
            .workspace_context_base_override
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            return override_context.clone();
        }
        self.workspace_context_base
            .get()
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn workspace_context_checkpoint(&self) -> WorkspaceContextCheckpoint {
        WorkspaceContextCheckpoint {
            base_override: self
                .workspace_context_base_override
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            overlay: self
                .workspace_context_overlay
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            current_instruction_paths: self
                .current_instruction_paths
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            nested_instructions: self
                .nested_instructions
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            skills: self.skill_catalog(),
            workspace_deny: self.permissions.workspace_deny_rules(),
            seen_generation: self
                .workspace_context_seen_generation
                .load(Ordering::Acquire),
        }
    }

    pub(crate) fn restore_workspace_context_checkpoint(
        &self,
        checkpoint: &WorkspaceContextCheckpoint,
    ) {
        *self
            .workspace_context_base_override
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = checkpoint.base_override.clone();
        *self
            .workspace_context_overlay
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = checkpoint.overlay.clone();
        *self
            .current_instruction_paths
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            checkpoint.current_instruction_paths.clone();
        *self
            .nested_instructions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            checkpoint.nested_instructions.clone();
        self.set_skills(checkpoint.skills.clone());
        self.permissions
            .set_workspace_deny(checkpoint.workspace_deny.clone());
        self.workspace_context_seen_generation
            .store(checkpoint.seen_generation, Ordering::Release);
    }

    async fn discover_workspace_context(&self, cwd: &Path) -> Result<WorkspaceDiscovery> {
        let instructions = discover_agent_instructions(cwd, self.bare).await?;
        let instruction_paths = instructions
            .iter()
            .map(|file| file.path.clone())
            .collect::<HashSet<_>>();
        let skill_cwd = cwd.to_owned();
        let bare = self.bare;
        let mut containing_roots = self
            .trusted_roots()
            .into_iter()
            .filter(|root| cwd.starts_with(root))
            .collect::<Vec<_>>();
        containing_roots.sort_by_key(|root| root.components().count());
        let mut deny_directories = Vec::new();
        for root in containing_roots {
            let mut directory = root.clone();
            if !deny_directories.contains(&directory) {
                deny_directories.push(directory.clone());
            }
            if let Ok(relative) = cwd.strip_prefix(&root) {
                for component in relative.components() {
                    directory.push(component.as_os_str());
                    if !deny_directories.contains(&directory) {
                        deny_directories.push(directory.clone());
                    }
                }
            }
        }
        let (mut skills, workspace_deny) = tokio::task::spawn_blocking(move || {
            let skills = discover_skills(&skill_cwd, bare)?;
            let mut deny = Vec::new();
            for directory in deny_directories {
                for rule in project_deny_rules(&directory, bare)? {
                    if !deny.contains(&rule) {
                        deny.push(rule);
                    }
                }
            }
            Ok::<_, anyhow::Error>((skills, deny))
        })
        .await
        .context("workspace discovery worker 失败")??;
        skills.merge(
            self.extension_skills
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
        )?;
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
        Ok(WorkspaceDiscovery {
            rendered,
            instruction_paths,
            skills,
            workspace_deny,
        })
    }

    pub async fn reload_workspace_context(&self) -> Result<()> {
        let _refresh_guard = self.workspace_context_refresh_lock.lock().await;
        let observed_generation = self.workspace_context_changes.generation();
        let cwd = self.cwd();
        let discovery = self.discover_workspace_context(&cwd).await?;
        let previous_instruction_paths = self
            .current_instruction_paths
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let mut newly_loaded_paths = discovery
            .instruction_paths
            .difference(&previous_instruction_paths)
            .cloned()
            .collect::<Vec<_>>();
        let mut nested_instructions = self
            .nested_instructions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let explicit_roots = self
            .explicit_context_roots
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        for root in &explicit_roots {
            for file in discover_nested_agent_instructions(root, root, false).await? {
                if !nested_instructions.contains_key(&file.path) {
                    newly_loaded_paths.push(file.path.clone());
                    nested_instructions.insert(file.path.clone(), file);
                }
            }
        }
        if nested_instructions.len() > 64 {
            bail!("nested AGENTS.md 超过 64 个会话级限制")
        }
        let budget = self.workspace_context_budget.load(Ordering::Acquire);
        let base = if self.workspace_context_base.get().is_none()
            && self
                .workspace_context_base_override
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none()
        {
            discovery.rendered.clone()
        } else {
            self.effective_workspace_context_base()
        };
        let overlay = if base == discovery.rendered {
            String::new()
        } else {
            format!(
                "# Current workspace context\n\nThe session changed working directories. The following current-workspace instructions and skills take precedence over the launch context.\n\n{}",
                discovery.rendered
            )
        };
        let nested = self.render_nested_instruction_context_from(
            &nested_instructions,
            &discovery.instruction_paths,
        );
        let effective_bytes = combined_workspace_context_bytes(&base, &overlay, &nested);
        if effective_bytes > budget {
            bail!("combined workspace system context 超过 {budget} 字节预算")
        }
        newly_loaded_paths.sort_unstable();
        newly_loaded_paths.dedup();
        for path in &newly_loaded_paths {
            let display = self.display_path(path);
            self.hooks()
                .run(
                    "InstructionsLoaded",
                    Some(&display),
                    json!({"file_path":display, "load_reason":"workspace_context"}),
                    &cwd,
                )
                .await?;
        }
        self.workspace_context_base
            .get_or_init(|| discovery.rendered.clone());
        self.permissions
            .set_workspace_deny(discovery.workspace_deny);
        self.set_skills(discovery.skills);
        *self
            .workspace_context_overlay
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = overlay;
        *self
            .current_instruction_paths
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = discovery.instruction_paths;
        *self
            .nested_instructions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = nested_instructions;
        self.workspace_context_seen_generation
            .store(observed_generation, Ordering::Release);
        Ok(())
    }

    async fn prepare_workspace_hot_refresh(
        &self,
        changed_paths: &[PathBuf],
    ) -> Result<Option<WorkspaceContextCandidate>> {
        if self.bare {
            return Ok(None);
        }
        let mut relevant = Vec::new();
        let mut seen = HashSet::new();
        for path in changed_paths {
            let resolved = canonicalize_for_scope(path).with_context(|| {
                format!("无法解析 workspace context 变更路径 {}", path.display())
            })?;
            let Some(root) = self.root_for_resolved_path(&resolved) else {
                continue;
            };
            let is_instruction = resolved.file_name().is_some_and(|name| name == "AGENTS.md");
            let is_skill = resolved
                .strip_prefix(&root)
                .is_ok_and(is_project_skill_path);
            if (is_instruction || is_skill) && seen.insert(resolved.clone()) {
                relevant.push((resolved, root, is_instruction));
            }
        }
        if relevant.is_empty() {
            return Ok(None);
        }

        let launch_cwd = self.workspace_context_launch_cwd.clone();
        let current_cwd = self.cwd();
        let launch = self.discover_workspace_context(&launch_cwd).await?;
        let current = if current_cwd == launch_cwd {
            launch.clone()
        } else {
            self.discover_workspace_context(&current_cwd).await?
        };
        let mut nested_instructions = self
            .nested_instructions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let mut instruction_hook_paths = Vec::new();
        let mut config_hook_paths = Vec::new();
        for (path, root, is_instruction) in &relevant {
            if !is_instruction {
                config_hook_paths.push(path.clone());
                continue;
            }
            nested_instructions.remove(path);
            if path.is_file() {
                for file in discover_nested_agent_instructions(root, path, false).await? {
                    nested_instructions.insert(file.path.clone(), file);
                }
                if launch.instruction_paths.contains(path)
                    || current.instruction_paths.contains(path)
                    || nested_instructions.contains_key(path)
                {
                    instruction_hook_paths.push(path.clone());
                }
            }
        }
        if nested_instructions.len() > 64 {
            bail!("nested AGENTS.md 超过 64 个会话级限制")
        }
        instruction_hook_paths.sort_unstable();
        instruction_hook_paths.dedup();
        config_hook_paths.sort_unstable();
        config_hook_paths.dedup();
        let changed_paths = relevant
            .iter()
            .map(|(path, _, _)| path.clone())
            .collect::<Vec<_>>();

        let overlay = workspace_context_overlay(&launch.rendered, &current.rendered);
        let nested = self.render_nested_instruction_context_from(
            &nested_instructions,
            &current.instruction_paths,
        );
        let budget = self.workspace_context_budget.load(Ordering::Acquire);
        if combined_workspace_context_bytes(&launch.rendered, &overlay, &nested) > budget {
            bail!("combined workspace system context 超过 {budget} 字节预算")
        }
        Ok(Some(WorkspaceContextCandidate {
            launch_rendered: launch.rendered,
            overlay,
            current_instruction_paths: current.instruction_paths,
            nested_instructions,
            skills: current.skills,
            workspace_deny: current.workspace_deny,
            instruction_hook_paths,
            config_hook_paths,
            changed_paths,
        }))
    }

    async fn run_workspace_hot_refresh_hooks(
        &self,
        candidate: &WorkspaceContextCandidate,
    ) -> Result<Vec<String>> {
        let cwd = self.cwd();
        let mut feedback = Vec::new();
        for path in &candidate.config_hook_paths {
            let display = self.display_path(path);
            let outcome = self
                .hooks()
                .run(
                    "ConfigChange",
                    Some("skills"),
                    json!({"source":"skills", "file_path":display}),
                    &cwd,
                )
                .await?;
            feedback.extend(outcome.additional_context);
        }
        for path in &candidate.instruction_hook_paths {
            let display = self.display_path(path);
            let outcome = self
                .hooks()
                .run(
                    "InstructionsLoaded",
                    Some(&display),
                    json!({"file_path":display, "load_reason":"hot_refresh"}),
                    &cwd,
                )
                .await?;
            feedback.extend(outcome.additional_context);
        }
        Ok(feedback)
    }

    fn commit_workspace_context_candidate(&self, candidate: WorkspaceContextCandidate) {
        *self
            .workspace_context_base_override
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(candidate.launch_rendered);
        *self
            .workspace_context_overlay
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = candidate.overlay;
        *self
            .current_instruction_paths
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = candidate.current_instruction_paths;
        *self
            .nested_instructions
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = candidate.nested_instructions;
        self.set_skills(candidate.skills);
        self.permissions
            .set_workspace_deny(candidate.workspace_deny);
    }

    fn commit_workspace_hot_refresh(&self, candidate: WorkspaceContextCandidate) {
        let changed_paths = candidate.changed_paths.clone();
        self.commit_workspace_context_candidate(candidate);
        let generation = self.workspace_context_changes.publish(changed_paths);
        self.workspace_context_seen_generation
            .store(generation, Ordering::Release);
    }

    fn workspace_context_full_refresh_paths(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        for root in [self.workspace_context_launch_cwd.clone(), self.cwd()] {
            paths.push(root.join("AGENTS.md"));
            paths.push(root.join(".open-agent-harness/skills/__context_refresh__/SKILL.md"));
        }
        paths.sort_unstable();
        paths.dedup();
        paths
    }

    /// Refreshes this logical context after another fork committed a
    /// project-instruction or project-skill mutation. Discovery and hooks are
    /// completed before any local state is replaced.
    pub(crate) async fn refresh_workspace_context_if_stale(&self) -> Result<bool> {
        let seen = self
            .workspace_context_seen_generation
            .load(Ordering::Acquire);
        if seen >= self.workspace_context_changes.generation() {
            return Ok(false);
        }
        let _refresh_guard = self.workspace_context_refresh_lock.lock().await;
        let mut refreshed = false;
        for _ in 0..4 {
            let seen = self
                .workspace_context_seen_generation
                .load(Ordering::Acquire);
            let (target, paths) = self.workspace_context_changes.changes_since(seen);
            if seen >= target {
                return Ok(refreshed);
            }
            let paths = paths.unwrap_or_else(|| self.workspace_context_full_refresh_paths());
            let candidate = self.prepare_workspace_hot_refresh(&paths).await?;
            if self.workspace_context_changes.generation() != target {
                continue;
            }
            if let Some(candidate) = candidate {
                let _ = self.run_workspace_hot_refresh_hooks(&candidate).await?;
                if self.workspace_context_changes.generation() != target {
                    continue;
                }
                self.commit_workspace_context_candidate(candidate);
                refreshed = true;
            }
            self.workspace_context_seen_generation
                .store(target, Ordering::Release);
            if self.workspace_context_changes.generation() == target {
                return Ok(refreshed);
            }
        }
        bail!("workspace context 持续并发变化，无法取得稳定刷新快照")
    }

    /// Announces that a completed file rollback may have reverted project
    /// context files. The restoring context keeps its checkpoint generation,
    /// so it and every sibling rediscover from disk before the next request.
    pub(crate) fn publish_workspace_context_rollback(&self) {
        self.workspace_context_changes.publish(Vec::new());
    }

    /// Commits this agent context's successful project-context mutations to
    /// its immediate parent. Nested agents publish one level at a time, so a
    /// failed outer agent never updates the root context generation.
    pub(crate) fn commit_workspace_context_changes_to_parent(&self) {
        let Some(parent) = &self.workspace_context_parent_changes else {
            return;
        };
        let (changed, paths) = self.workspace_context_changes.accumulated_changes();
        if changed {
            parent.publish(paths.unwrap_or_default());
        }
    }

    /// Loads nested `AGENTS.md` layers only after a permitted tool first
    /// reaches the corresponding directory tree. The resulting prompt blocks
    /// carry an explicit relative scope and are deduplicated for the session.
    pub async fn refresh_nested_instructions_for_path(&self, target: &Path) -> Result<()> {
        let _refresh_guard = self.workspace_context_refresh_lock.lock().await;
        let resolved = canonicalize_for_scope(target)
            .with_context(|| format!("无法解析嵌套指令目标 {}", target.display()))?;
        let Some(root) = self.root_for_resolved_path(&resolved) else {
            return Ok(());
        };
        let explicit = self
            .explicit_context_roots
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|explicit| resolved.starts_with(explicit));
        let mut files =
            discover_nested_agent_instructions(&root, &resolved, self.bare && !explicit).await?;
        let cwd = self.cwd();
        if self
            .root_for_resolved_path(&cwd)
            .is_some_and(|current_root| current_root == root)
        {
            files.retain(|file| {
                !file
                    .path
                    .parent()
                    .is_some_and(|scope| cwd.starts_with(scope))
            });
        }
        if files.is_empty() {
            return Ok(());
        }
        let newly_loaded = files
            .iter()
            .filter(|file| {
                !self
                    .nested_instructions
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains_key(&file.path)
            })
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let before = {
            let mut nested = self
                .nested_instructions
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let before = nested.clone();
            for file in files {
                nested.entry(file.path.clone()).or_insert(file);
            }
            if nested.len() > 64 {
                *nested = before;
                bail!("nested AGENTS.md 超过 64 个会话级限制")
            }
            before
        };
        if self.workspace_system_context().len()
            > self.workspace_context_budget.load(Ordering::Acquire)
        {
            *self
                .nested_instructions
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = before;
            bail!("nested workspace instructions 超过 system context 字节预算")
        }
        for path in newly_loaded {
            let display = self.display_path(&path);
            if let Err(error) = self
                .hooks()
                .run(
                    "InstructionsLoaded",
                    Some(&display),
                    json!({"file_path":display, "load_reason":"first_path_access"}),
                    &cwd,
                )
                .await
            {
                *self
                    .nested_instructions
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = before;
                return Err(error);
            }
        }
        Ok(())
    }

    fn render_nested_instruction_context(&self) -> String {
        let nested = self
            .nested_instructions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self
            .current_instruction_paths
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.render_nested_instruction_context_from(&nested, &current)
    }

    fn render_nested_instruction_context_from(
        &self,
        nested: &BTreeMap<PathBuf, InstructionFile>,
        current: &HashSet<PathBuf>,
    ) -> String {
        if nested.is_empty() {
            return String::new();
        }
        let roots = self.trusted_roots();
        let mut rendered = String::from(
            "# Scoped workspace instructions\n\nEach block below applies only to files in its declared workspace-relative directory tree. More specific scopes take precedence.\n",
        );
        for file in nested.values() {
            if current.contains(&file.path) {
                continue;
            }
            let Some(scope_dir) = file.path.parent() else {
                continue;
            };
            let Some((index, root)) = roots
                .iter()
                .enumerate()
                .filter(|(_, root)| scope_dir.starts_with(root))
                .max_by_key(|(_, root)| root.components().count())
            else {
                continue;
            };
            let relative = scope_dir.strip_prefix(root).unwrap_or(Path::new(""));
            let scope = if relative.as_os_str().is_empty() {
                ".".to_owned()
            } else {
                normalize_path_for_display(relative.display().to_string())
            };
            let scope = escape_context_attribute(&scope);
            rendered.push_str(&format!(
                "\n<agent_instructions workspace=\"{}\" scope=\"{scope}/**\">\n{}\n</agent_instructions>\n",
                index + 1,
                file.content.trim()
            ));
        }
        rendered
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

    /// Replaces the dynamic FileChanged watch list returned by a trusted hook.
    /// Static FileChanged matchers remain active and are resolved against the
    /// current cwd on every poll, matching the source watcher's restart model.
    #[doc(hidden)]
    pub fn replace_hook_watch_paths(&self, paths: &[String]) -> Result<()> {
        let paths = validate_external_dynamic_watch_paths(paths)?;
        let mut state = self
            .external_file_watch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.dynamic_paths != paths {
            state.dynamic_paths = paths;
            state.dynamic_generation = state.dynamic_generation.saturating_add(1);
        }
        Ok(())
    }

    /// Polls the bounded external watch set at a model-request boundary. The
    /// first scan and every changed watch set are baselines (`ignoreInitial`);
    /// later scans produce add/change/unlink events without a resident thread.
    pub(crate) async fn poll_external_file_changes(&self) -> Result<Vec<String>> {
        if self.bare {
            return Ok(Vec::new());
        }
        let (dynamic_paths, dynamic_generation) = {
            let state = self
                .external_file_watch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (state.dynamic_paths.clone(), state.dynamic_generation)
        };
        let (specs, hook_specs) = self.external_watch_specs(&dynamic_paths)?;
        let scan_specs = specs.clone();
        let entries = tokio::task::spawn_blocking(move || scan_external_watch_specs(&scan_specs))
            .await
            .context("external file watcher worker 失败")??;
        let events = {
            let mut state = self
                .external_file_watch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.dynamic_generation != dynamic_generation {
                return Ok(Vec::new());
            }
            reconcile_external_watch_state(&mut state, specs, entries)?
        };
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let cwd = self.cwd();
        let mut contexts = Vec::new();
        let mut context_bytes = 0usize;
        let mut changed_paths = Vec::with_capacity(events.len());
        for event in events {
            let display = self.display_path(&event.path);
            let event_name = event.kind.as_str();
            changed_paths.push(event.path.clone());
            if !external_watch_specs_cover_path(&hook_specs, &event.path) {
                continue;
            }
            match self
                .hooks()
                .run_file_changed(
                    "external",
                    &display,
                    json!({
                        "source":"watcher",
                        "file_path":display,
                        "event":event_name,
                    }),
                    &cwd,
                )
                .await
            {
                Ok(outcome) => {
                    for message in outcome.additional_context {
                        push_bounded_external_watch_context(
                            &mut contexts,
                            &mut context_bytes,
                            format!("{event_name} {display}: {message}"),
                        );
                    }
                    if !outcome.watch_paths.is_empty() {
                        if let Err(error) = self.replace_hook_watch_paths(&outcome.watch_paths) {
                            push_bounded_external_watch_context(
                                &mut contexts,
                                &mut context_bytes,
                                format!(
                                    "FileChanged hook returned invalid watchPaths for {display}: {error:#}"
                                ),
                            );
                        }
                    }
                }
                Err(error) => push_bounded_external_watch_context(
                    &mut contexts,
                    &mut context_bytes,
                    format!("FileChanged hook failed for {display}: {error:#}"),
                ),
            }
        }
        self.workspace_context_changes.publish(changed_paths);
        Ok(contexts)
    }

    async fn acknowledge_external_file_changes(&self, paths: &[PathBuf]) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        if paths.len() > MAX_EXTERNAL_WATCH_EVENTS {
            bail!("acknowledged file changes 超过 {MAX_EXTERNAL_WATCH_EVENTS} 项限制")
        }
        let paths = paths.to_vec();
        let acknowledged = tokio::task::spawn_blocking(move || {
            let mut remaining_hash_bytes = MAX_EXTERNAL_WATCH_HASH_TOTAL_BYTES;
            let mut acknowledged = BTreeMap::new();
            for path in paths {
                let mut identities = vec![normalize_lexical_path(&path)];
                if let Ok(canonical) = canonicalize_for_scope(&path) {
                    if !identities.contains(&canonical) {
                        identities.push(canonical);
                    }
                }
                for identity in identities {
                    let fingerprint =
                        external_watch_fingerprint(&identity, &mut remaining_hash_bytes)?;
                    acknowledged.insert(identity, fingerprint);
                }
            }
            Ok::<_, anyhow::Error>(acknowledged)
        })
        .await
        .context("file watcher acknowledgement worker 失败")??;
        let mut state = self
            .external_file_watch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (path, fingerprint) in acknowledged {
            state.acknowledged.insert(path, fingerprint);
        }
        while state.acknowledged.len() > MAX_EXTERNAL_WATCH_EVENTS.saturating_mul(2) {
            let Some(path) = state.acknowledged.keys().next().cloned() else {
                break;
            };
            state.acknowledged.remove(&path);
        }
        Ok(())
    }

    fn external_watch_specs(
        &self,
        dynamic_paths: &[String],
    ) -> Result<(Vec<ExternalWatchSpec>, Vec<ExternalWatchSpec>)> {
        let mut hook_specs = BTreeSet::new();
        let cwd = self.cwd();

        for pattern in self.hooks().file_watch_patterns()? {
            hook_specs.insert(external_watch_spec_from_pattern(&pattern, &cwd, false)?);
        }
        for path in dynamic_paths {
            hook_specs.insert(external_watch_spec_from_pattern(path, &cwd, true)?);
        }
        let mut specs = hook_specs.clone();

        if let Some(home) = dirs::home_dir() {
            specs.insert(ExternalWatchSpec::Exact(
                home.join(".open-agent-harness/AGENTS.md"),
            ));
            specs.insert(ExternalWatchSpec::Tree(
                home.join(".open-agent-harness/skills"),
            ));
        }
        for scope in [self.workspace_context_launch_cwd.clone(), cwd.clone()] {
            for directory in scope.ancestors() {
                specs.insert(ExternalWatchSpec::Exact(directory.join("AGENTS.md")));
                specs.insert(ExternalWatchSpec::Tree(
                    directory.join(".open-agent-harness/skills"),
                ));
            }
        }
        for root in self.trusted_roots() {
            specs.insert(ExternalWatchSpec::Exact(root.join("AGENTS.md")));
            specs.insert(ExternalWatchSpec::Tree(
                root.join(".open-agent-harness/skills"),
            ));
        }
        for path in self
            .current_instruction_paths
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            specs.insert(ExternalWatchSpec::Exact(path.clone()));
        }
        for path in self
            .nested_instructions
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
        {
            specs.insert(ExternalWatchSpec::Exact(path.clone()));
        }
        for (_, skill) in self
            .skills
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
        {
            if let Some(root) = skill.path.parent().and_then(Path::parent) {
                specs.insert(ExternalWatchSpec::Tree(root.to_path_buf()));
            } else {
                specs.insert(ExternalWatchSpec::Exact(skill.path.clone()));
            }
        }
        if specs.len() > MAX_EXTERNAL_WATCH_SPECS {
            bail!("external watch spec 超过 {MAX_EXTERNAL_WATCH_SPECS} 项限制")
        }
        Ok((
            specs.into_iter().collect(),
            hook_specs.into_iter().collect(),
        ))
    }

    pub(crate) fn agent_limits(&self) -> AgentLimits {
        self.agent_limits
    }

    pub(crate) fn agent_depth(&self) -> usize {
        self.agent_depth
    }

    pub(crate) fn set_agent_depth(&mut self, depth: usize) {
        self.agent_depth = depth;
    }

    pub(crate) fn agent_tool_policy(&self) -> &AgentToolPolicy {
        &self.agent_tool_policy
    }

    pub(crate) fn set_agent_tool_policy(&mut self, policy: AgentToolPolicy) {
        self.agent_tool_policy = policy;
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

    fn bind_execution_registry(&self, registry: ToolRegistry) -> bool {
        if let Some(existing) = self.execution_registry.get() {
            return existing.same_registry(&registry);
        }
        match self.execution_registry.set(registry) {
            Ok(()) => true,
            Err(registry) => self
                .execution_registry
                .get()
                .is_some_and(|existing| existing.same_registry(&registry)),
        }
    }

    pub(crate) fn execution_registry_has_active(&self, name: &str) -> bool {
        self.execution_registry
            .get()
            .is_some_and(|registry| registry.has_active(name))
    }

    pub(crate) fn workflow_runtime(&self) -> WorkflowRuntime {
        self.workflow_runtime.clone()
    }

    pub(crate) fn fork_for_agent(&self) -> Self {
        Self {
            async_owner: self.async_owner.fork(),
            location: Arc::new(RwLock::new(
                self.location
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            trusted_roots: Arc::new(RwLock::new(
                self.trusted_roots
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            workspace_security: Arc::clone(&self.workspace_security),
            explicit_context_roots: Arc::new(RwLock::new(
                self.explicit_context_roots
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            permissions: Arc::new(self.permissions.fork_for_context()),
            read_cache: Arc::new(Mutex::new(ReadCache::default())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            task_capture_root: Arc::clone(&self.task_capture_root),
            todos: Arc::new(Mutex::new(Vec::new())),
            skills: Arc::new(RwLock::new(
                self.skills
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            extension_skills: Arc::clone(&self.extension_skills),
            task_store_lock: Arc::clone(&self.task_store_lock),
            task_store_path: Arc::new(RwLock::new(self.task_store_path())),
            agent_runtime: Arc::clone(&self.agent_runtime),
            execution_registry: Arc::new(OnceLock::new()),
            workflow_runtime: WorkflowRuntime::default(),
            agent_depth: self.agent_depth.saturating_add(1),
            agent_limits: self.agent_limits,
            agent_tool_policy: self.agent_tool_policy.clone(),
            hooks: Arc::clone(&self.hooks),
            bare: self.bare,
            workspace_context_launch_cwd: self.workspace_context_launch_cwd.clone(),
            workspace_context_base: Arc::clone(&self.workspace_context_base),
            workspace_context_base_override: Arc::new(RwLock::new(
                self.workspace_context_base_override
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            workspace_context_overlay: Arc::new(RwLock::new(
                self.workspace_context_overlay
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            current_instruction_paths: Arc::new(RwLock::new(
                self.current_instruction_paths
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            nested_instructions: Arc::new(RwLock::new(
                self.nested_instructions
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            workspace_context_budget: Arc::clone(&self.workspace_context_budget),
            workspace_context_refresh_lock: Arc::new(Mutex::new(())),
            workspace_context_changes: Arc::new(WorkspaceContextChanges::default()),
            workspace_context_parent_changes: Some(Arc::clone(&self.workspace_context_changes)),
            workspace_context_seen_generation: Arc::new(AtomicU64::new(0)),
            external_file_watch: Arc::new(StdMutex::new(
                self.external_file_watch
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            interaction_handler: Arc::clone(&self.interaction_handler),
            sandbox_runtime: Arc::clone(&self.sandbox_runtime),
            file_history: Arc::new(RwLock::new(
                self.file_history
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            file_histories: Arc::clone(&self.file_histories),
            file_checkpoint: Arc::clone(&self.file_checkpoint),
            ancestor_file_checkpoints: Arc::new(RwLock::new(
                self.ancestor_file_checkpoints
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone(),
            )),
            hot_refresh_transactions: Arc::clone(&self.hot_refresh_transactions),
            hot_refresh_transaction: Arc::new(RwLock::new(None)),
            hot_refresh_parent_transaction: (*self
                .hot_refresh_transaction
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()))
            .or(self.hot_refresh_parent_transaction),
            team_identity: Arc::new(RwLock::new(
                *self
                    .team_identity
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            )),
            team_mailboxes: Arc::new(RwLock::new(HashMap::new())),
            workspace_state_recorder: Arc::new(RwLock::new(None)),
            current_cwd_state_recorder: Arc::new(RwLock::new(None)),
            cron: self.cron.clone(),
            monitor: self.monitor.clone(),
            secret_env_scrubber: self.secret_env_scrubber.clone(),
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

    fn root_for_resolved_path(&self, path: &Path) -> Option<PathBuf> {
        self.trusted_roots
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|root| path.starts_with(root))
            .max_by_key(|root| root.components().count())
            .cloned()
    }

    fn file_history_for_path(&self, path: &Path) -> Result<Option<FileHistory>> {
        let resolved = canonicalize_for_scope(path)
            .with_context(|| format!("无法解析 file-history 目标 {}", path.display()))?;
        let root = self
            .root_for_resolved_path(&resolved)
            .context("file-history 目标不在可信工作区内")?;
        let key = workspace_key(&root);
        if let Some(history) = self
            .file_histories
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .cloned()
        {
            return Ok(Some(history));
        }
        let Some(template) = self
            .file_history
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            return Ok(None);
        };
        let history = template.relocate(&root)?;
        for checkpoint_id in self.file_transaction_ids() {
            if let Some(info) = template
                .checkpoints()?
                .into_iter()
                .find(|info| info.id == checkpoint_id)
            {
                history.checkpoint_with_ancestors(
                    checkpoint_id,
                    info.boundary,
                    info.message_count,
                    &info.ancestor_ids,
                )?;
            }
        }
        self.file_histories
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key, history.clone());
        Ok(Some(history))
    }

    pub async fn switch_workspace(&self, cwd: PathBuf, root: PathBuf) -> Result<()> {
        let cwd = std::fs::canonicalize(&cwd)
            .with_context(|| format!("无法解析新工作目录 {}", cwd.display()))?;
        let root = std::fs::canonicalize(&root)
            .with_context(|| format!("无法解析新工作区根目录 {}", root.display()))?;
        if !cwd.is_dir() || !root.is_dir() || !cwd.starts_with(&root) {
            bail!("新工作目录必须位于有效工作区根目录内")
        }
        self.register_security_trusted_root(&root)?;
        {
            let mut trusted = self
                .trusted_roots
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !trusted.contains(&root) {
                if trusted.len() >= MAX_TRUSTED_WORKSPACE_ROOTS {
                    bail!(
                        "可信工作区根目录超过 {} 个限制",
                        MAX_TRUSTED_WORKSPACE_ROOTS
                    )
                }
                trusted.push(root.clone());
            }
        }
        let history = self
            .file_history
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let checkpoints = self.file_transaction_ids();
        let relocated_history = match history {
            Some(history) => {
                self.file_histories
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(workspace_key(history.workspace()), history.clone());
                let key = workspace_key(&root);
                let relocated = self
                    .file_histories
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(&key)
                    .cloned()
                    .map(Ok)
                    .unwrap_or_else(|| history.relocate(&root))?;
                for checkpoint_id in checkpoints {
                    if history.is_transaction_active(checkpoint_id)?
                        && !relocated.can_rewind(checkpoint_id)?
                    {
                        if let Some(info) = history
                            .checkpoints()?
                            .into_iter()
                            .find(|info| info.id == checkpoint_id)
                        {
                            relocated.checkpoint_with_ancestors(
                                checkpoint_id,
                                info.boundary,
                                info.message_count,
                                &info.ancestor_ids,
                            )?;
                        }
                    }
                }
                self.file_histories
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(key, relocated.clone());
                Some(relocated)
            }
            None => None,
        };
        let mut read_cache = self.read_cache.lock().await;
        *self
            .location
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = WorkspaceLocation { cwd, root };
        *self
            .file_history
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = relocated_history;
        self.set_task_store_path(task_store_path(&self.cwd()));
        *read_cache = ReadCache::default();
        Ok(())
    }

    /// Restores a persisted foreground-shell cwd only when its hashed root is
    /// one of this invocation's already trusted roots. The persisted path is
    /// relative and every component must still be a real directory rather
    /// than a symlink.
    pub async fn restore_persisted_cwd(&self, root_key: &str, relative: &Path) -> Result<()> {
        if root_key.len() != 32 || !root_key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("persisted current root key 必须是 32 位十六进制标识")
        }
        validate_persisted_relative_cwd(relative)?;
        let mut matches = self
            .trusted_roots()
            .into_iter()
            .filter(|root| workspace_key(root) == root_key)
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            bail!("persisted current root 不再唯一匹配本次显式信任的 workspace")
        }
        let root = matches.pop().expect("one trusted root match");
        let cwd = canonicalize_restored_cwd(&root, relative)?;
        self.switch_workspace(cwd, root).await
    }

    /// Persists the physical cwd reported by a successful foreground shell.
    /// A cwd outside every trusted root is rejected and the previous cwd stays
    /// active, even if the subprocess itself visited that directory.
    pub async fn update_cwd_from_shell(&self, cwd: &Path) -> Result<bool> {
        let cwd = std::fs::canonicalize(cwd)
            .with_context(|| format!("无法解析 shell cwd {}", cwd.display()))?;
        if !cwd.is_dir() {
            bail!("shell cwd 不是目录")
        }
        let Some(root) = self.root_for_resolved_path(&cwd) else {
            return Ok(false);
        };
        if cwd == self.cwd() && root == self.workspace_root() {
            return Ok(true);
        }
        let previous_cwd = self.cwd();
        let previous_root = self.workspace_root();
        self.switch_workspace(cwd.clone(), root).await?;
        if let Err(error) = self.reload_workspace_context().await {
            self.switch_workspace(previous_cwd, previous_root).await?;
            self.reload_workspace_context().await?;
            return Err(error).context("shell cwd 上下文刷新失败，已恢复原 cwd");
        }
        let cwd_outcome = match self
            .hooks()
            .run(
                "CwdChanged",
                Some("shell"),
                json!({
                    "source":"shell",
                    "old_cwd":self.display_path(&previous_cwd),
                    "new_cwd":self.display_path(&cwd),
                }),
                &cwd,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                self.switch_workspace(previous_cwd, previous_root).await?;
                self.reload_workspace_context().await?;
                return Err(error).context("CwdChanged hook 拒绝 shell cwd 更新，已恢复原 cwd");
            }
        };
        if let Err(error) = validate_external_dynamic_watch_paths(&cwd_outcome.watch_paths) {
            self.switch_workspace(previous_cwd, previous_root).await?;
            self.reload_workspace_context().await?;
            return Err(error).context("CwdChanged hook watchPaths 无效，已恢复原 cwd");
        }
        if let Err(error) = self.record_current_cwd_transition() {
            self.switch_workspace(previous_cwd, previous_root).await?;
            self.reload_workspace_context().await?;
            return Err(error).context("shell cwd 持久化失败，已恢复原 cwd");
        }
        self.replace_hook_watch_paths(&cwd_outcome.watch_paths)?;
        Ok(true)
    }

    pub fn resolve_path(&self, value: &str) -> Result<PathBuf> {
        if value.trim().is_empty() {
            bail!("路径不能为空");
        }
        reject_windows_network_or_device_path(value)?;
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
        Ok(self.root_for_resolved_path(&resolved).is_none())
    }

    /// Produces equivalent, normalized path identities for permission rules.
    /// The raw spelling is never authoritative: existing paths include their
    /// canonical target (closing symlink aliases), while missing write targets
    /// inherit a canonical parent. Relative identities are derived from every
    /// trusted workspace root so `./`, absolute, and `..` spellings converge.
    pub fn permission_path_candidates(&self, value: &str) -> Result<Vec<String>> {
        let path = self.resolve_path(value)?;
        self.permission_path_candidates_for_resolved(&path)
    }

    pub(crate) fn permission_path_candidates_for_resolved(
        &self,
        path: &Path,
    ) -> Result<Vec<String>> {
        // `std::fs::canonicalize` returns a verbatim local-disk path on
        // Windows (`\\?\C:\...`). Raw model input using that namespace is
        // still rejected in `resolve_path`; only this already-resolved path
        // boundary unwraps the local-disk prefix before applying the same
        // UNC/device/ADS/reserved-name checks.
        reject_windows_network_or_device_resolved_path(path)?;
        let lexical = normalize_lexical_path(path);
        let canonical = canonicalize_for_scope(&lexical)
            .with_context(|| format!("无法规范化权限路径: {}", path.display()))?;
        let mut candidates = Vec::new();
        push_permission_path_candidate(&mut candidates, &lexical);
        push_permission_path_candidate(&mut candidates, &canonical);
        for root in self.trusted_roots() {
            for candidate in [&lexical, &canonical] {
                if let Ok(relative) = candidate.strip_prefix(&root) {
                    let relative = if relative.as_os_str().is_empty() {
                        Path::new(".")
                    } else {
                        relative
                    };
                    push_permission_path_candidate(&mut candidates, relative);
                }
            }
        }
        Ok(candidates)
    }

    pub(crate) fn read_path_denied(&self, path: &Path) -> bool {
        self.permission_path_candidates_for_resolved(path)
            .map(|candidates| self.permissions.denies_read_path(&candidates))
            // Search must fail closed when a candidate cannot be normalized.
            .unwrap_or(true)
    }

    pub fn display_path(&self, path: &Path) -> String {
        let rendered = if let Ok(relative) = path.strip_prefix(self.cwd()) {
            if relative.as_os_str().is_empty() {
                ".".into()
            } else {
                relative.display().to_string()
            }
        } else if let Some(relative) =
            dirs::home_dir().and_then(|home| path.strip_prefix(home).ok().map(Path::to_path_buf))
        {
            format!("~/{}", relative.display())
        } else {
            path.display().to_string()
        };
        normalize_path_for_display(rendered)
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
        drop(tasks);
        self.workflow_runtime.shutdown().await;
    }

    pub async fn background_task_ids(&self) -> HashSet<String> {
        let mut ids = self
            .tasks
            .lock()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        ids.extend(self.workflow_runtime.task_ids().await);
        ids.extend(self.monitor.owned_task_ids(&self.async_owner).await);
        ids
    }

    pub(crate) async fn background_notification_checkpoint(
        &self,
    ) -> BackgroundNotificationCheckpoint {
        let bash_tasks = self
            .tasks
            .lock()
            .await
            .iter()
            .map(|(id, task)| {
                (
                    id.clone(),
                    BashTaskCheckpoint {
                        notification_delivered: task.notification_delivered,
                        output_path: task.output_path.clone(),
                        output_cleanup_armed: task.output_cleanup_armed,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        BackgroundNotificationCheckpoint {
            bash_tasks,
            workflow_tasks: self.workflow_runtime.notification_checkpoint().await,
            monitor: self
                .monitor
                .notification_checkpoint(&self.async_owner)
                .await,
        }
    }

    pub(crate) async fn restore_background_notification_checkpoint(
        &self,
        checkpoint: &BackgroundNotificationCheckpoint,
    ) {
        let mut tasks = self.tasks.lock().await;
        let mut orphaned_captures = Vec::new();
        for (id, snapshot) in &checkpoint.bash_tasks {
            match tasks.get_mut(id) {
                Some(task) if task.output_path == snapshot.output_path => {
                    task.notification_delivered = snapshot.notification_delivered;
                    task.output_cleanup_armed = snapshot.output_cleanup_armed;
                }
                Some(_) | None if snapshot.output_cleanup_armed => {
                    orphaned_captures.push(snapshot.output_path.clone());
                }
                Some(_) | None => {}
            }
        }
        drop(tasks);
        for path in orphaned_captures {
            let _ = std::fs::remove_file(path);
        }
        self.workflow_runtime
            .restore_notification_checkpoint(&checkpoint.workflow_tasks)
            .await;
        self.monitor
            .restore_notification_checkpoint(&checkpoint.monitor)
            .await;
    }

    /// Claims completed background work exactly once without consuming it.
    /// `TaskOutput`/`AgentOutput` remain authoritative and can still retrieve
    /// the full bounded result after this model-visible notification.
    pub(crate) async fn drain_background_notifications(&self) -> Vec<String> {
        let cwd = self.cwd();
        let mut notifications = Vec::new();
        let mut total_bytes = 0usize;
        {
            let mut tasks = self.tasks.lock().await;
            let mut ids = tasks.keys().cloned().collect::<Vec<_>>();
            ids.sort_unstable();
            for id in ids {
                if notifications.len() >= MAX_BACKGROUND_NOTIFICATIONS
                    || total_bytes >= MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
                {
                    break;
                }
                let Some(task) = tasks.get_mut(&id) else {
                    continue;
                };
                if task.notification_delivered
                    || task.child.try_wait().ok().flatten().is_none()
                    || task.drains.iter().any(|drain| !drain.is_finished())
                {
                    continue;
                }
                let status = if task.timed_out {
                    format!("timed out after {}ms", task.timeout_ms)
                } else {
                    "completed".to_owned()
                };
                let output = bash::read_output_preview(
                    &task.output_path,
                    MAX_BACKGROUND_NOTIFICATION_BYTES / 2,
                )
                .map(|(output, truncated, _)| {
                    if truncated {
                        format!(
                            "{output}\n[preview truncated; use TaskOutput for the full capture]"
                        )
                    } else {
                        output
                    }
                })
                .unwrap_or_else(|error| format!("[output preview unavailable: {error:#}]"));
                let mut notification = format!(
                    "Background Bash task {id} {status}.\nOutput preview:\n{}",
                    sanitize_transport_text(&output, &cwd)
                );
                truncate_utf8_with_marker(
                    &mut notification,
                    MAX_BACKGROUND_NOTIFICATION_BYTES,
                    "\n[background notification truncated]",
                );
                if total_bytes.saturating_add(notification.len())
                    > MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
                {
                    break;
                }
                total_bytes += notification.len();
                task.notification_delivered = true;
                notifications.push(notification);
            }
        }

        if notifications.len() < MAX_BACKGROUND_NOTIFICATIONS
            && total_bytes < MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
        {
            for mut notification in self
                .workflow_runtime
                .drain_notifications(MAX_BACKGROUND_NOTIFICATIONS - notifications.len())
                .await
            {
                truncate_utf8_with_marker(
                    &mut notification,
                    MAX_BACKGROUND_NOTIFICATION_BYTES,
                    "\n[background notification truncated]",
                );
                if total_bytes.saturating_add(notification.len())
                    > MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
                {
                    break;
                }
                total_bytes += notification.len();
                notifications.push(notification);
            }
        }

        if notifications.len() < MAX_BACKGROUND_NOTIFICATIONS
            && total_bytes < MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
        {
            for mut notification in self
                .monitor
                .drain_notifications(
                    &self.async_owner,
                    MAX_BACKGROUND_NOTIFICATIONS - notifications.len(),
                    MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES - total_bytes,
                    &cwd,
                )
                .await
            {
                truncate_utf8_with_marker(
                    &mut notification,
                    MAX_BACKGROUND_NOTIFICATION_BYTES,
                    "\n[background notification truncated; use TaskOutput for the full result]",
                );
                if total_bytes.saturating_add(notification.len())
                    > MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
                {
                    break;
                }
                total_bytes += notification.len();
                notifications.push(notification);
            }
        }

        if notifications.len() < MAX_BACKGROUND_NOTIFICATIONS
            && total_bytes < MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
        {
            if let Ok(runtime) = self.agent_runtime() {
                let remaining_count = MAX_BACKGROUND_NOTIFICATIONS - notifications.len();
                for (id, description, output) in runtime
                    .drain_ready_notifications(&self.async_owner, remaining_count)
                    .await
                {
                    let mut notification = format!(
                        "Background agent {id} completed ({description}).\nResult preview:\n{}",
                        sanitize_transport_text(&output.content, &cwd)
                    );
                    truncate_utf8_with_marker(
                        &mut notification,
                        MAX_BACKGROUND_NOTIFICATION_BYTES,
                        "\n[background notification truncated; use AgentOutput for the full result]",
                    );
                    if total_bytes.saturating_add(notification.len())
                        > MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
                    {
                        runtime
                            .restore_notification_delivery(&self.async_owner, id)
                            .await;
                        break;
                    }
                    total_bytes += notification.len();
                    notifications.push(notification);
                }
            }
        }
        if notifications.len() < MAX_BACKGROUND_NOTIFICATIONS
            && total_bytes < MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES
        {
            notifications.extend(self.drain_team_notifications(
                MAX_BACKGROUND_NOTIFICATIONS - notifications.len(),
                MAX_BACKGROUND_NOTIFICATION_TOTAL_BYTES - total_bytes,
            ));
        }
        notifications
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
        }
        self.workflow_runtime.rollback_new(keep).await;
        self.monitor
            .rollback_new_tasks(&self.async_owner, keep)
            .await;
    }
}

fn truncate_utf8_with_marker(value: &mut String, maximum: usize, marker: &str) {
    if value.len() <= maximum {
        return;
    }
    let mut end = maximum.saturating_sub(marker.len());
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value.truncate(end);
    value.push_str(marker);
}

pub(crate) fn normalize_path_for_display(value: String) -> String {
    #[cfg(windows)]
    {
        normalize_windows_local_path_text(&value)
    }
    #[cfg(not(windows))]
    {
        value
    }
}

fn normalize_windows_local_path_text(value: &str) -> String {
    let normalized = value.replace('\\', "/");
    let Some(local) = normalized.strip_prefix("//?/") else {
        return normalized;
    };
    let bytes = local.as_bytes();
    if bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && bytes[2] == b'/' {
        local.to_owned()
    } else {
        normalized
    }
}

fn escape_context_attribute(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn workspace_context_overlay(launch: &str, current: &str) -> String {
    if launch == current {
        String::new()
    } else {
        format!(
            "# Current workspace context\n\nThe session changed working directories. The following current-workspace instructions and skills take precedence over the launch context.\n\n{current}"
        )
    }
}

fn combined_workspace_context_bytes(base: &str, overlay: &str, nested: &str) -> usize {
    [base, overlay, nested]
        .into_iter()
        .filter(|section| !section.is_empty())
        .fold((0usize, false), |(bytes, has_previous), section| {
            (
                bytes
                    .saturating_add(if has_previous { 2 } else { 0 })
                    .saturating_add(section.len()),
                true,
            )
        })
        .0
}

fn hot_refresh_snapshot_bytes(snapshot: &HotRefreshFileSnapshot) -> usize {
    snapshot.original.as_deref().map_or(0, <[u8]>::len)
        + snapshot.expected.as_deref().map_or(0, <[u8]>::len)
}

fn hot_refresh_transaction_is_ancestor(
    frames: &BTreeMap<u64, HotRefreshFileTransaction>,
    candidate: u64,
    transaction: u64,
) -> bool {
    let mut current = frames.get(&transaction).and_then(|frame| frame.parent);
    let mut remaining = frames.len();
    while let Some(id) = current {
        if id == candidate {
            return true;
        }
        if remaining == 0 {
            return false;
        }
        remaining -= 1;
        current = frames.get(&id).and_then(|frame| frame.parent);
    }
    false
}

fn read_hot_refresh_snapshot(path: &Path) -> Result<HotRefreshFileSnapshot> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                bail!(
                    "临时 workspace context 事务只支持普通文件: {}",
                    path.display()
                )
            }
            let bytes = read_hot_refresh_file_state(path)?
                .context("临时 workspace context 事务读取到的文件状态与 metadata 不一致")?;
            Ok(HotRefreshFileSnapshot {
                original: Some(bytes),
                original_permissions: Some(metadata.permissions()),
                expected: None,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(HotRefreshFileSnapshot {
            original: None,
            original_permissions: None,
            expected: None,
        }),
        Err(error) => Err(error)
            .with_context(|| format!("无法读取临时 workspace context 事务目标 {}", path.display())),
    }
}

fn read_hot_refresh_file_state(path: &Path) -> Result<Option<Vec<u8>>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "临时 workspace context 事务目标不再是普通文件: {}",
            path.display()
        )
    }
    if metadata.len() > MAX_EDITABLE_FILE_BYTES as u64 {
        bail!(
            "临时 workspace context 事务文件超过 {} 字节限制: {}",
            MAX_EDITABLE_FILE_BYTES,
            path.display()
        )
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    std::fs::File::open(path)?
        .take((MAX_EDITABLE_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_EDITABLE_FILE_BYTES {
        bail!(
            "临时 workspace context 事务文件超过 {} 字节限制: {}",
            MAX_EDITABLE_FILE_BYTES,
            path.display()
        )
    }
    Ok(Some(bytes))
}

fn rollback_hot_refresh_snapshots(
    snapshots: &BTreeMap<PathBuf, HotRefreshFileSnapshot>,
) -> Result<()> {
    let mut current = Vec::with_capacity(snapshots.len());
    for (path, snapshot) in snapshots {
        let state = read_hot_refresh_file_state(path)?;
        let is_original = state == snapshot.original;
        let is_expected = snapshot
            .expected
            .as_ref()
            .is_some_and(|expected| state.as_ref() == Some(expected));
        let is_tracked_delete =
            snapshot.expected.is_none() && snapshot.original.is_some() && state.is_none();
        if !is_original && !is_expected && !is_tracked_delete {
            bail!(
                "拒绝回滚被其他进程并发修改的 workspace context 文件: {}",
                path.display()
            )
        }
        current.push((path, snapshot, state));
    }

    for (path, snapshot, validated_state) in current {
        if read_hot_refresh_file_state(path)? != validated_state {
            bail!(
                "workspace context 文件在回滚预检后再次变化: {}",
                path.display()
            )
        }
        if validated_state.as_ref() == snapshot.original.as_ref() {
            continue;
        }
        match (&snapshot.original, &snapshot.original_permissions) {
            (None, None) => std::fs::remove_file(path)
                .with_context(|| format!("无法删除本轮新建的 context 文件 {}", path.display()))?,
            (Some(bytes), Some(permissions)) => {
                atomic_write_bytes(path, bytes, Some(permissions.clone())).with_context(|| {
                    format!("无法还原 workspace context 文件 {}", path.display())
                })?;
            }
            _ => bail!("临时 workspace context 文件快照缺少权限元数据"),
        }
    }
    Ok(())
}

fn is_project_skill_path(path: &Path) -> bool {
    let components = path.components().collect::<Vec<_>>();
    components.len() >= 4
        && components[0].as_os_str() == ".open-agent-harness"
        && components[1].as_os_str() == "skills"
        && components
            .last()
            .is_some_and(|component| component.as_os_str() == "SKILL.md")
}

fn compact_value(value: &Value) -> String {
    const MAX_CHARS: usize = 160;
    let rendered = value.to_string();
    let mut compact = rendered.chars().take(MAX_CHARS).collect::<String>();
    if rendered.chars().count() > MAX_CHARS {
        compact.push('…');
    }
    compact
}

#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub content: String,
    pub model_content: Option<Value>,
    pub is_error: bool,
    pub interrupted: bool,
    pub(crate) rollback_turn: bool,
    pub(crate) skill_invocation: Option<crate::skills::SkillInvocation>,
}

impl ToolOutput {
    pub fn success(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            model_content: None,
            is_error: false,
            interrupted: false,
            rollback_turn: false,
            skill_invocation: None,
        }
    }

    pub fn success_with_model_content(content: impl Into<String>, model_content: Value) -> Self {
        Self {
            content: content.into(),
            model_content: Some(model_content),
            is_error: false,
            interrupted: false,
            rollback_turn: false,
            skill_invocation: None,
        }
    }

    pub fn error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            model_content: None,
            is_error: true,
            interrupted: false,
            rollback_turn: false,
            skill_invocation: None,
        }
    }

    fn transaction_error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            model_content: None,
            is_error: true,
            interrupted: false,
            rollback_turn: true,
            skill_invocation: None,
        }
    }

    pub fn interrupted() -> Self {
        Self {
            content: "用户中断了权限确认".to_owned(),
            model_content: None,
            is_error: true,
            interrupted: true,
            rollback_turn: false,
            skill_invocation: None,
        }
    }

    pub(crate) fn success_with_skill_invocation(
        content: impl Into<String>,
        invocation: crate::skills::SkillInvocation,
    ) -> Self {
        Self {
            content: content.into(),
            model_content: None,
            is_error: false,
            interrupted: false,
            rollback_turn: false,
            skill_invocation: Some(invocation),
        }
    }

    pub(crate) fn append_context(&mut self, label: &str, context: &str) {
        let addition = format!("\n\n[{label}]\n{context}");
        self.content.push_str(&addition);
        if let Some(model_content) = &mut self.model_content {
            match model_content {
                Value::Array(blocks) => blocks.push(json!({"type":"text", "text":addition})),
                previous => {
                    let first = std::mem::take(previous);
                    *previous = Value::Array(vec![first, json!({"type":"text", "text":addition})]);
                }
            }
        }
    }

    fn bounded(mut self) -> Self {
        if self
            .model_content
            .as_ref()
            .and_then(|content| serde_json::to_vec(content).ok())
            .is_some_and(|encoded| encoded.len() > MAX_MODEL_TOOL_RESULT_BYTES)
        {
            self.content =
                format!("结构化工具结果超过 {MAX_MODEL_TOOL_RESULT_BYTES} 字节 harness 限制");
            self.model_content = None;
            self.is_error = true;
        }
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

pub type ToolStartedObserver = Arc<dyn Fn(usize) + Send + Sync>;
pub type ToolFinishedObserver = Arc<dyn Fn(usize, &ToolOutput, Duration) + Send + Sync>;

pub struct ToolExecutionObserver {
    on_started: ToolStartedObserver,
    on_finished: ToolFinishedObserver,
}

impl ToolExecutionObserver {
    pub fn new(on_started: ToolStartedObserver, on_finished: ToolFinishedObserver) -> Self {
        Self {
            on_started,
            on_finished,
        }
    }

    fn started(&self, index: usize) {
        (self.on_started)(index);
    }

    fn finished(&self, index: usize, output: &ToolOutput, elapsed: Duration) {
        (self.on_finished)(index, output, elapsed);
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn input_schema(&self) -> Value;
    fn read_only(&self, input: &Value) -> bool;
    fn read_only_for(&self, _context: &ToolContext, input: &Value) -> bool {
        self.read_only(input)
    }
    fn destructive(&self, _input: &Value) -> bool {
        false
    }
    fn requires_permission(&self) -> bool {
        true
    }
    fn requires_permission_for(&self, _context: &ToolContext, _input: &Value) -> bool {
        self.requires_permission()
    }
    /// Read-only operations normally bypass the interactive permission prompt.
    /// Open-world reads may opt out: they remain semantically read-only and
    /// concurrency-safe, while still requiring explicit network authorization.
    fn explicit_permission_for(&self, _context: &ToolContext, _input: &Value) -> bool {
        false
    }
    fn path_fields(&self) -> &'static [&'static str] {
        &[]
    }
    fn concurrency_safe(&self, input: &Value) -> bool {
        self.read_only(input)
    }
    fn concurrency_safe_for(&self, _context: &ToolContext, input: &Value) -> bool {
        self.concurrency_safe(input)
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
    /// Notifies long-lived integrations after a harness file tool has
    /// successfully changed files. Services must treat the paths as hints and
    /// re-validate their own workspace and size boundaries before reading.
    /// Returned strings are bounded again when they are attached to the tool
    /// result; an integration failure must not silently undo a successful file
    /// mutation.
    async fn files_changed(&self, _paths: &[PathBuf]) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn shutdown(&self);
}

pub struct ToolRefresh {
    pub upsert: Vec<Arc<dyn Tool>>,
    pub remove: Vec<String>,
}

#[async_trait]
pub trait ToolDiscovery: Send + Sync {
    async fn refresh(&self) -> Result<ToolRefresh>;

    /// Names of integrations which are known but have not finished exposing
    /// their tools yet. This lets ToolSearch distinguish "no such tool" from
    /// "the configured integration is still connecting" without depending on
    /// any provider-specific discovery type.
    fn pending_names(&self) -> Vec<String> {
        Vec::new()
    }
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
    fn same_registry(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    fn builtins() -> Vec<Arc<dyn Tool>> {
        vec![
            Arc::new(AskUserQuestionTool),
            Arc::new(BashTool),
            Arc::new(RunWorkflowTool),
            Arc::new(CronCreateTool),
            Arc::new(CronDeleteTool),
            Arc::new(CronListTool),
            Arc::new(GlobTool),
            Arc::new(GrepTool),
            Arc::new(ReadTool),
            Arc::new(ScheduleWakeupTool),
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
        for tool in deferred_extensions
            .into_iter()
            .chain(std::iter::once(Arc::new(MonitorTool) as Arc<dyn Tool>))
        {
            validate_registry_tool(tool.as_ref())?;
            insert_unique_tool(&mut deferred, &active, tool)?;
        }
        let has_dynamic_discovery = !discoverers.is_empty();
        let search_slots = usize::from(!deferred.is_empty() || has_dynamic_discovery);
        if active.len().saturating_add(search_slots) > MAX_ACTIVE_TOOLS {
            bail!("active tool 数量超过 {MAX_ACTIVE_TOOLS} 个限制")
        }
        if deferred.len() > MAX_DEFERRED_TOOLS {
            bail!("deferred tool 数量超过 {MAX_DEFERRED_TOOLS} 个限制")
        }
        let state = Arc::new(RwLock::new(RegistryState { active, deferred }));
        let discoverers = Arc::new(discoverers);
        if !read_registry(&state).deferred.is_empty() || has_dynamic_discovery {
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

    pub fn restrict_to(&self, names: &[String]) -> Result<()> {
        if names.len() > MAX_ACTIVE_TOOLS {
            bail!("--tools 数量超过 {MAX_ACTIVE_TOOLS} 个限制")
        }
        let requested = names.iter().cloned().collect::<HashSet<_>>();
        if requested.len() != names.len() {
            bail!("--tools 包含重复名称")
        }
        let mut state = write_registry(&self.state);
        for name in &requested {
            if !state.active.contains_key(name) && !state.deferred.contains_key(name) {
                bail!("--tools 指定了未知工具: {name}")
            }
        }
        let preserves_structured_output = state.active.contains_key("StructuredOutput")
            && !requested.contains("StructuredOutput");
        if requested
            .len()
            .saturating_add(usize::from(preserves_structured_output))
            > MAX_ACTIVE_TOOLS
        {
            bail!("--tools 选择后 active tool 数量超过 {MAX_ACTIVE_TOOLS} 个限制")
        }
        for name in &requested {
            if let Some(tool) = state.deferred.remove(name) {
                state.active.insert(name.clone(), tool);
            }
        }
        state
            .active
            .retain(|name, _| requested.contains(name) || name == "StructuredOutput");
        if !requested.contains("ToolSearch") {
            state.deferred.clear();
        }
        Ok(())
    }

    pub(crate) fn scoped_for_agent(&self, policy: &AgentToolPolicy) -> Result<Self> {
        let state = read_registry(&self.state);
        let known = state
            .active
            .keys()
            .chain(state.deferred.keys())
            .map(String::as_str)
            .collect::<HashSet<_>>();
        if let Some(allowed) = &policy.allowed_tools {
            if allowed.contains("ToolSearch") {
                bail!("受限 agent 不得启用 ToolSearch")
            }
            if let Some(unknown) = allowed.iter().find(|name| !known.contains(name.as_str())) {
                bail!("agent allowedTools 包含未知工具: {unknown}")
            }
        }
        if let Some(unknown) = policy
            .disallowed_tools
            .iter()
            .find(|name| !known.contains(name.as_str()))
        {
            bail!("agent disallowedTools 包含未知工具: {unknown}")
        }

        let mut active = state
            .active
            .iter()
            .filter(|(name, _)| name.as_str() != "ToolSearch" && policy.allows(name))
            .map(|(name, tool)| (name.clone(), Arc::clone(tool)))
            .collect::<HashMap<_, _>>();
        match &policy.allowed_tools {
            Some(allowed) => {
                for name in allowed {
                    if let Some(tool) = state.deferred.get(name) {
                        active.insert(name.clone(), Arc::clone(tool));
                    }
                }
            }
            None => {
                // A deny-only policy means every capability inherited from the
                // parent remains available except the explicitly denied names.
                // Scoped agents do not receive ToolSearch, so eagerly promote
                // every allowed deferred tool and fail closed at the same active
                // tool ceiling instead of silently dropping capabilities.
                for (name, tool) in &state.deferred {
                    if policy.allows(name) {
                        active.insert(name.clone(), Arc::clone(tool));
                    }
                }
            }
        }
        if active.len() > MAX_ACTIVE_TOOLS {
            bail!("agent scoped active tool 数量超过 {MAX_ACTIVE_TOOLS} 个限制")
        }
        drop(state);
        Ok(Self {
            state: Arc::new(RwLock::new(RegistryState {
                active,
                deferred: HashMap::new(),
            })),
            services: Arc::clone(&self.services),
            discoverers: Arc::new(Vec::new()),
        })
    }

    pub fn summary(&self, name: &str, input: &Value) -> String {
        read_registry(&self.state)
            .active
            .get(name)
            .map_or_else(|| compact_value(input), |tool| tool.summary(input))
    }

    pub(crate) fn has_active(&self, name: &str) -> bool {
        read_registry(&self.state).active.contains_key(name)
    }

    pub async fn execute(&self, context: &ToolContext, name: &str, input: Value) -> ToolOutput {
        let tool_use_id = uuid::Uuid::new_v4().to_string();
        self.execute_with_id(context, name, input, &tool_use_id)
            .await
    }

    async fn execute_with_id(
        &self,
        context: &ToolContext,
        name: &str,
        input: Value,
        tool_use_id: &str,
    ) -> ToolOutput {
        if !context.bind_execution_registry(self.clone()) {
            return ToolOutput::error(
                "同一 ToolContext 不能跨独立 tool registry 执行；请为受限 registry 使用隔离 context",
            );
        }
        let tool = read_registry(&self.state).active.get(name).cloned();
        let Some(tool) = tool else {
            return ToolOutput::error(format!("未知工具: {name}"));
        };
        if let Err(error) = tool.validate_input(&input) {
            return ToolOutput::error(format!("工具输入校验失败: {error}"));
        }
        let hooks = context.hooks();
        let (mut input, mut pre_context) = match hooks
            .pre_tool(tool.name(), input, &context.cwd())
            .await
        {
            Ok(result) => result,
            Err(error) => return ToolOutput::error(format!("Pre-tool hook 拒绝调用: {error:#}")),
        };
        if let Err(error) = tool.validate_input(&input) {
            return ToolOutput::error(format!("hook 修改后的工具输入校验失败: {error}"));
        }
        let outside_workspace_for = |candidate: &Value| {
            tool.path_fields()
                .iter()
                .filter_map(|field| candidate.get(*field).and_then(Value::as_str))
                .try_fold(false, |outside, path| {
                    context
                        .is_outside_workspace(path)
                        .map(|current| outside || current)
                })
        };
        let outside_workspace = match outside_workspace_for(&input) {
            Ok(outside) => outside,
            Err(error) => return ToolOutput::error(format!("路径边界检查失败: {error:#}")),
        };
        let summary = tool.summary(&input);
        let permission_targets =
            match permission_targets_for(context, tool.as_ref(), &input, &summary) {
                Ok(targets) => targets,
                Err(error) => {
                    return ToolOutput::error(format!("权限目标规范化失败: {error:#}"));
                }
            };
        let mut authorized_read_only = tool.read_only_for(context, &input);
        if tool.requires_permission_for(context, &input) {
            match hooks
                .run(
                    "PermissionRequest",
                    Some(tool.name()),
                    json!({
                        "tool_name":tool.name(),
                        "tool_input":&input,
                        "tool_use_id":tool_use_id,
                        "summary":&summary,
                    }),
                    &context.cwd(),
                )
                .await
            {
                Ok(outcome) => pre_context.extend(outcome.additional_context),
                Err(error) => {
                    return ToolOutput::error(format!(
                        "PermissionRequest hook 拒绝调用: {error:#}"
                    ));
                }
            }
            match context.permissions.decide_invocation_with_targets_policy(
                tool.name(),
                &input,
                tool_use_id,
                &summary,
                authorized_read_only,
                tool.destructive(&input),
                outside_workspace,
                &permission_targets,
                tool.explicit_permission_for(context, &input),
            ) {
                Ok(PermissionDecision::Allow) => {}
                Ok(PermissionDecision::AllowWithUpdatedInput(updated)) => {
                    if let Err(error) = tool.validate_input(&updated) {
                        return ToolOutput::error(format!(
                            "权限响应修改后的工具输入校验失败: {error}"
                        ));
                    }
                    let updated_outside_workspace = match outside_workspace_for(&updated) {
                        Ok(outside) => outside,
                        Err(error) => {
                            return ToolOutput::error(format!(
                                "权限响应修改后的路径边界检查失败: {error:#}"
                            ));
                        }
                    };
                    let updated_summary = tool.summary(&updated);
                    let updated_targets = match permission_targets_for(
                        context,
                        tool.as_ref(),
                        &updated,
                        &updated_summary,
                    ) {
                        Ok(targets) => targets,
                        Err(error) => {
                            return ToolOutput::error(format!(
                                "权限响应修改后的目标规范化失败: {error:#}"
                            ));
                        }
                    };
                    if !context.permissions.permits_updated_invocation_with_targets(
                        tool.name(),
                        &updated_summary,
                        tool.read_only_for(context, &updated),
                        updated_outside_workspace,
                        &updated_targets,
                    ) {
                        let mut output =
                            ToolOutput::error("权限响应修改后的工具调用违反 deny 或 plan 规则");
                        if !pre_context.is_empty() {
                            output.append_context(
                                "PermissionRequest hook context",
                                &pre_context.join("\n"),
                            );
                        }
                        match hooks
                            .run(
                                "PermissionDenied",
                                Some(tool.name()),
                                json!({
                                    "tool_name":tool.name(),
                                    "tool_input":&updated,
                                    "tool_use_id":tool_use_id,
                                    "reason":"updated_input_rejected",
                                }),
                                &context.cwd(),
                            )
                            .await
                        {
                            Ok(outcome) if !outcome.additional_context.is_empty() => {
                                output.append_context(
                                    "PermissionDenied hook context",
                                    &outcome.additional_context.join("\n"),
                                );
                            }
                            Err(error) => output.append_context(
                                "PermissionDenied hook failed",
                                &format!("{error:#}"),
                            ),
                            _ => {}
                        }
                        return output;
                    }
                    authorized_read_only = tool.read_only_for(context, &updated);
                    input = updated;
                }
                Ok(PermissionDecision::Deny) => {
                    let mut output = ToolOutput::error("用户或权限规则拒绝了此工具调用");
                    if !pre_context.is_empty() {
                        output.append_context(
                            "PermissionRequest hook context",
                            &pre_context.join("\n"),
                        );
                    }
                    match hooks
                        .run(
                            "PermissionDenied",
                            Some(tool.name()),
                            json!({
                                "tool_name":tool.name(),
                                "tool_input":&input,
                                "tool_use_id":tool_use_id,
                                "reason":"denied",
                            }),
                            &context.cwd(),
                        )
                        .await
                    {
                        Ok(outcome) if !outcome.additional_context.is_empty() => {
                            output.append_context(
                                "PermissionDenied hook context",
                                &outcome.additional_context.join("\n"),
                            );
                        }
                        Err(error) => output
                            .append_context("PermissionDenied hook failed", &format!("{error:#}")),
                        _ => {}
                    }
                    return output;
                }
                Ok(PermissionDecision::Interrupt) => return ToolOutput::interrupted(),
                Err(error) => return ToolOutput::error(format!("权限检查失败: {error:#}")),
            }
        }
        if authorized_read_only && !tool.read_only_for(context, &input) {
            return ToolOutput::error("只读工具调用在执行前的路径或权限重检中失效，已拒绝执行");
        }
        for field in tool.path_fields() {
            let Some(value) = input.get(*field).and_then(Value::as_str) else {
                continue;
            };
            let target = match context.resolve_path(value) {
                Ok(target) => target,
                Err(error) => {
                    return ToolOutput::error(format!("嵌套指令路径解析失败: {error:#}"));
                }
            };
            if let Err(error) = context.refresh_nested_instructions_for_path(&target).await {
                return ToolOutput::error(format!("嵌套 AGENTS.md 发现失败: {error:#}"));
            }
        }
        let path_existed_before = tool
            .path_fields()
            .iter()
            .filter_map(|field| input.get(*field).and_then(Value::as_str))
            .filter_map(|path| context.resolve_path(path).ok())
            .map(|path| {
                let existed = path.exists();
                (path, existed)
            })
            .collect::<HashMap<_, _>>();
        let mut output = match tool.execute(context, input.clone()).await {
            Ok(output) => output,
            Err(error) => ToolOutput::error(format!("{error:#}")),
        };
        let inspect_context_change = !output.is_error
            && !tool.read_only_for(context, &input)
            && !tool.path_fields().is_empty();
        let _refresh_guard = if inspect_context_change {
            Some(context.workspace_context_refresh_lock.lock().await)
        } else {
            None
        };
        let mut hot_refresh_candidate = None;
        let mut relevant_context_mutation = false;
        let mut service_changed_paths = Vec::new();
        let mut pending_hook_watch_paths = None;
        if inspect_context_change {
            let paths = tool
                .path_fields()
                .iter()
                .filter_map(|field| {
                    input
                        .get(*field)
                        .and_then(Value::as_str)
                        .map(|path| (field, path))
                })
                .map(|(field, path)| {
                    context
                        .resolve_path(path)
                        .map(|resolved| ((*field).to_owned(), resolved))
                })
                .collect::<Result<Vec<_>>>();
            match paths {
                Ok(paths) => {
                    for (field, path) in &paths {
                        let display = context.display_path(path);
                        let event = if path_existed_before.get(path).copied().unwrap_or(false) {
                            "change"
                        } else {
                            "add"
                        };
                        match hooks
                            .run_file_changed(
                                tool.name(),
                                &display,
                                json!({
                                    "tool_name":tool.name(),
                                    "field":field,
                                    "file_path":display,
                                    "event":event,
                                }),
                                &context.cwd(),
                            )
                            .await
                        {
                            Ok(outcome) => {
                                if !outcome.additional_context.is_empty() {
                                    output.append_context(
                                        "FileChanged hook context",
                                        &outcome.additional_context.join("\n"),
                                    );
                                }
                                if !outcome.watch_paths.is_empty() {
                                    match validate_external_dynamic_watch_paths(
                                        &outcome.watch_paths,
                                    ) {
                                        Ok(_) => {
                                            pending_hook_watch_paths = Some(outcome.watch_paths);
                                        }
                                        Err(error) => {
                                            output.is_error = true;
                                            output.append_context(
                                                "FileChanged hook failed",
                                                &format!("hook watchPaths 无效: {error:#}"),
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(error) => {
                                output.is_error = true;
                                output.append_context(
                                    "FileChanged hook failed",
                                    &format!("{error:#}"),
                                );
                                break;
                            }
                        }
                    }
                    let changed_paths = paths.into_iter().map(|(_, path)| path).collect::<Vec<_>>();
                    service_changed_paths = changed_paths.clone();
                    match context.prepare_workspace_hot_refresh(&changed_paths).await {
                        Ok(Some(candidate)) => {
                            relevant_context_mutation = true;
                            if output.is_error {
                                output.rollback_turn = true;
                            } else {
                                match context.run_workspace_hot_refresh_hooks(&candidate).await {
                                    Ok(feedback) => {
                                        for message in feedback {
                                            output.append_context(
                                                "Workspace context refresh hook context",
                                                &message,
                                            );
                                        }
                                    }
                                    Err(error) => {
                                        output.is_error = true;
                                        output.rollback_turn = true;
                                        output.append_context(
                                            "Workspace context refresh hook failed",
                                            &format!("{error:#}"),
                                        );
                                    }
                                }
                            }
                            hot_refresh_candidate = Some(candidate);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            relevant_context_mutation = true;
                            output.is_error = true;
                            output.rollback_turn = true;
                            output.append_context(
                                "Workspace context refresh failed",
                                &format!("{error:#}"),
                            );
                        }
                    }
                }
                Err(error) => {
                    output = ToolOutput::transaction_error(format!(
                        "文件已修改，但无法解析 FileChanged 路径并安全刷新上下文: {error:#}"
                    ));
                }
            }
        }
        if !pre_context.is_empty() {
            output.append_context("Pre-tool hook context", &pre_context.join("\n"));
        }
        let mut output = hooks
            .post_tool(tool.name(), &input, output.bounded(), &context.cwd())
            .await
            .bounded();
        if relevant_context_mutation && output.is_error {
            output.rollback_turn = true;
        }
        if !output.is_error {
            if let Some(paths) = pending_hook_watch_paths {
                if let Err(error) = context.replace_hook_watch_paths(&paths) {
                    output.is_error = true;
                    output.append_context(
                        "FileChanged watch registration failed",
                        &format!("{error:#}"),
                    );
                }
            }
        }
        if !output.is_error {
            if let Some(candidate) = hot_refresh_candidate {
                context.commit_workspace_hot_refresh(candidate);
            }
            if !service_changed_paths.is_empty() {
                let mut service_contexts = 0usize;
                let mut service_context_bytes = 0usize;
                for service in self.services.iter() {
                    match service.files_changed(&service_changed_paths).await {
                        Ok(contexts) => {
                            for context_message in contexts {
                                append_bounded_file_service_context(
                                    &mut output,
                                    &mut service_contexts,
                                    &mut service_context_bytes,
                                    "File integration context",
                                    context_message,
                                );
                            }
                        }
                        Err(error) => append_bounded_file_service_context(
                            &mut output,
                            &mut service_contexts,
                            &mut service_context_bytes,
                            "File integration synchronization failed",
                            format!("{error:#}"),
                        ),
                    }
                }
                if let Err(error) = context
                    .acknowledge_external_file_changes(&service_changed_paths)
                    .await
                {
                    output.append_context(
                        "FileChanged watcher acknowledgement failed",
                        &format!("{error:#}"),
                    );
                }
            }
        }
        output.bounded()
    }

    pub async fn execute_batch(
        &self,
        context: &ToolContext,
        calls: &[(String, Value)],
    ) -> Vec<ToolOutput> {
        self.execute_batch_observed(context, calls, None).await
    }

    pub async fn execute_batch_observed(
        &self,
        context: &ToolContext,
        calls: &[(String, Value)],
        observer: Option<&ToolExecutionObserver>,
    ) -> Vec<ToolOutput> {
        self.execute_batch_observed_inner(context, calls, None, observer)
            .await
    }

    pub async fn execute_batch_observed_with_ids(
        &self,
        context: &ToolContext,
        calls: &[(String, Value)],
        tool_use_ids: &[String],
        observer: Option<&ToolExecutionObserver>,
    ) -> Vec<ToolOutput> {
        if calls.len() != tool_use_ids.len() {
            return calls
                .iter()
                .map(|_| ToolOutput::error("tool invocation id 数量与调用数量不一致"))
                .collect();
        }
        self.execute_batch_observed_inner(context, calls, Some(tool_use_ids), observer)
            .await
    }

    async fn execute_batch_observed_inner(
        &self,
        context: &ToolContext,
        calls: &[(String, Value)],
        tool_use_ids: Option<&[String]>,
        observer: Option<&ToolExecutionObserver>,
    ) -> Vec<ToolOutput> {
        let mut outputs = Vec::with_capacity(calls.len());
        let mut index = 0;
        while index < calls.len() {
            let (name, input) = &calls[index];
            let concurrency_safe = self.concurrency_safe(context, name, input);
            if !concurrency_safe {
                if let Some(observer) = observer {
                    observer.started(index);
                }
                let started = Instant::now();
                let output = match tool_use_ids.and_then(|ids| ids.get(index)) {
                    Some(tool_use_id) => {
                        self.execute_with_id(context, name, input.clone(), tool_use_id)
                            .await
                    }
                    None => self.execute(context, name, input.clone()).await,
                };
                if let Some(observer) = observer {
                    observer.finished(index, &output, started.elapsed());
                }
                let stop_batch = output.interrupted || output.rollback_turn;
                outputs.push(output);
                index += 1;
                if stop_batch {
                    break;
                }
                continue;
            }

            let end = calls[index..]
                .iter()
                .position(|(candidate_name, candidate_input)| {
                    !self.concurrency_safe(context, candidate_name, candidate_input)
                })
                .map_or(calls.len(), |offset| index + offset);
            for chunk in calls[index..end].chunks(MAX_CONCURRENT_READ_TOOLS) {
                let chunk_start = index;
                let interrupted = Arc::new(AtomicBool::new(false));
                let concurrent =
                    chunk
                        .iter()
                        .enumerate()
                        .map(|(offset, (candidate_name, candidate_input))| {
                            let interrupted = Arc::clone(&interrupted);
                            let tool_use_id =
                                tool_use_ids.and_then(|ids| ids.get(chunk_start + offset));
                            async move {
                                let call_index = chunk_start + offset;
                                if interrupted.load(Ordering::Acquire) {
                                    return ToolOutput::interrupted();
                                }
                                if let Some(observer) = observer {
                                    observer.started(call_index);
                                }
                                let started = Instant::now();
                                let output = match tool_use_id {
                                    Some(tool_use_id) => {
                                        self.execute_with_id(
                                            context,
                                            candidate_name,
                                            candidate_input.clone(),
                                            tool_use_id,
                                        )
                                        .await
                                    }
                                    None => {
                                        self.execute(
                                            context,
                                            candidate_name,
                                            candidate_input.clone(),
                                        )
                                        .await
                                    }
                                };
                                if let Some(observer) = observer {
                                    observer.finished(call_index, &output, started.elapsed());
                                }
                                if output.interrupted {
                                    interrupted.store(true, Ordering::Release);
                                }
                                output
                            }
                        });
                outputs.extend(futures_util::future::join_all(concurrent).await);
                index += chunk.len();
                if outputs
                    .iter()
                    .any(|output| output.interrupted || output.rollback_turn)
                {
                    return outputs;
                }
                if index >= end {
                    break;
                }
            }
            index = end;
        }
        outputs
    }

    fn concurrency_safe(&self, context: &ToolContext, name: &str, input: &Value) -> bool {
        read_registry(&self.state)
            .active
            .get(name)
            .is_some_and(|tool| tool.concurrency_safe_for(context, input))
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
        "Refresh and search deferred tools by keyword, or load exact tools with query `select:name1,name2`. Load all tools needed for a task in one call."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "query": {"type": "string", "minLength": 1, "maxLength": 4096},
                "max_results": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_TOOL_SEARCH_RESULTS,
                    "default": DEFAULT_TOOL_SEARCH_RESULTS
                }
            }),
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
        let max_results = input
            .get("max_results")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(DEFAULT_TOOL_SEARCH_RESULTS);
        let state = self.state.upgrade().context("工具注册表已经关闭")?;
        let refresh_errors = refresh_discovered_tools(&state, &self.discoverers).await;
        let pending_integrations = pending_discovery_names(&self.discoverers);
        if query
            .get(.."select:".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("select:"))
        {
            let selection = &query["select:".len()..];
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
                .map(|name| name.to_ascii_lowercase())
                .collect::<HashSet<_>>()
                .into_iter()
                .filter(|name| {
                    registry
                        .deferred
                        .keys()
                        .any(|candidate| candidate.eq_ignore_ascii_case(name))
                })
                .count();
            if registry.active.len().saturating_add(new_active) > MAX_ACTIVE_TOOLS {
                bail!("active tool 数量将超过 {MAX_ACTIVE_TOOLS} 个限制")
            }
            let mut loaded = Vec::new();
            let mut already_active = Vec::new();
            let mut missing = Vec::new();
            for name in requested {
                if let Some(canonical) = registry
                    .active
                    .keys()
                    .find(|candidate| candidate.eq_ignore_ascii_case(name))
                    .cloned()
                {
                    if !already_active.contains(&canonical) {
                        already_active.push(canonical);
                    }
                } else if let Some(canonical) = registry
                    .deferred
                    .keys()
                    .find(|candidate| candidate.eq_ignore_ascii_case(name))
                    .cloned()
                {
                    let tool = registry
                        .deferred
                        .remove(&canonical)
                        .expect("selected deferred tool must still exist");
                    registry.active.insert(canonical.clone(), tool);
                    if !loaded.contains(&canonical) {
                        loaded.push(canonical);
                    }
                } else {
                    missing.push(name.to_owned());
                }
            }
            let pending_integrations = if loaded.is_empty() && already_active.is_empty() {
                pending_integrations
            } else {
                Vec::new()
            };
            return Ok(ToolOutput::success(serde_json::to_string_pretty(&json!({
                "loaded": loaded,
                "already_active": already_active,
                "missing": missing,
                "remaining_deferred": registry.deferred.len(),
                "pending_integrations": pending_integrations,
                "refresh_errors": refresh_errors,
            }))?));
        }

        let query_lower = query.to_ascii_lowercase();
        let query_terms = query
            .split_whitespace()
            .map(str::to_ascii_lowercase)
            .collect::<Vec<_>>();
        let required_terms = query_terms
            .iter()
            .filter_map(|term| term.strip_prefix('+').filter(|term| !term.is_empty()))
            .collect::<Vec<_>>();
        let scoring_terms = query_terms
            .iter()
            .map(|term| term.strip_prefix('+').unwrap_or(term))
            .filter(|term| !term.is_empty())
            .collect::<Vec<_>>();
        let registry = read_registry(&state);
        if let Some(canonical) = registry
            .deferred
            .keys()
            .chain(registry.active.keys())
            .find(|name| name.to_ascii_lowercase() == query_lower)
            .cloned()
        {
            return Ok(ToolOutput::success(serde_json::to_string_pretty(&json!({
                "query": query,
                "matches": [{"name": canonical}],
                "total_deferred": registry.deferred.len(),
                "pending_integrations": [],
                "refresh_errors": refresh_errors,
            }))?));
        }
        let mut matches = registry
            .deferred
            .values()
            .filter_map(|tool| {
                let name = tool.name().to_ascii_lowercase();
                let name_parts = tool_search_name_parts(tool.name());
                let description = tool.description().to_ascii_lowercase();
                if !required_terms.iter().all(|term| {
                    name_parts.iter().any(|part| part.contains(*term))
                        || description.contains(*term)
                }) {
                    return None;
                }
                let score = scoring_terms
                    .iter()
                    .map(|term| {
                        if name_parts.iter().any(|part| part == *term) {
                            10
                        } else if name_parts.iter().any(|part| part.contains(*term)) {
                            5
                        } else if name.contains(*term) {
                            3
                        } else if description.contains(*term) {
                            2
                        } else {
                            0
                        }
                    })
                    .sum::<usize>();
                (score > 0).then(|| {
                    json!({
                        "name": tool.name(),
                        "description": truncate_utf8(tool.description(), 512),
                        "score": score,
                    })
                })
            })
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| {
            right["score"]
                .as_u64()
                .cmp(&left["score"].as_u64())
                .then_with(|| left["name"].as_str().cmp(&right["name"].as_str()))
        });
        matches.truncate(max_results);
        let pending_integrations = if matches.is_empty() {
            pending_integrations
        } else {
            Vec::new()
        };
        Ok(ToolOutput::success(serde_json::to_string_pretty(&json!({
            "query": query,
            "matches": matches,
            "total_deferred": registry.deferred.len(),
            "pending_integrations": pending_integrations,
            "refresh_errors": refresh_errors,
        }))?))
    }
}

fn tool_search_name_parts(name: &str) -> Vec<String> {
    let mut normalized = String::with_capacity(name.len().saturating_add(8));
    let mut previous_lowercase = false;
    for character in name.chars() {
        if matches!(character, '_' | '-') {
            normalized.push(' ');
            previous_lowercase = false;
            continue;
        }
        if character.is_ascii_uppercase() && previous_lowercase {
            normalized.push(' ');
        }
        normalized.push(character.to_ascii_lowercase());
        previous_lowercase = character.is_ascii_lowercase() || character.is_ascii_digit();
    }
    normalized
        .split_whitespace()
        .map(ToOwned::to_owned)
        .collect()
}

fn pending_discovery_names(discoverers: &[Arc<dyn ToolDiscovery>]) -> Vec<String> {
    let mut pending = discoverers
        .iter()
        .flat_map(|discoverer| discoverer.pending_names())
        .filter(|name| !name.is_empty() && name.len() <= MAX_TOOL_NAME_BYTES)
        .collect::<Vec<_>>();
    pending.sort_by_key(|name| name.to_ascii_lowercase());
    pending.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    pending.truncate(MAX_DEFERRED_TOOLS);
    pending
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

fn validate_persisted_relative_cwd(relative: &Path) -> Result<()> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        bail!("persisted current cwd 必须是无 parent 逃逸的相对路径")
    }
    Ok(())
}

fn canonicalize_restored_cwd(root: &Path, relative: &Path) -> Result<PathBuf> {
    let root_metadata = std::fs::symlink_metadata(root)
        .with_context(|| format!("persisted current root 已缺失: {}", root.display()))?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        bail!("persisted current root 不再是可信实体目录")
    }
    let canonical_root = std::fs::canonicalize(root)
        .with_context(|| format!("persisted current root 无法解析: {}", root.display()))?;
    if canonical_root != root {
        bail!("persisted current root 已被替换或重定向")
    }

    let mut candidate = root.to_owned();
    for component in relative.components() {
        match component {
            std::path::Component::CurDir => continue,
            std::path::Component::Normal(value) => candidate.push(value),
            _ => bail!("persisted current cwd 包含不安全路径组件"),
        }
        let metadata = std::fs::symlink_metadata(&candidate)
            .with_context(|| format!("persisted current cwd 已缺失: {}", candidate.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "persisted current cwd 拒绝 symlink: {}",
                candidate.display()
            )
        }
        if !metadata.is_dir() {
            bail!(
                "persisted current cwd 组件不是目录: {}",
                candidate.display()
            )
        }
    }

    let canonical = std::fs::canonicalize(&candidate)
        .with_context(|| format!("persisted current cwd 无法解析: {}", candidate.display()))?;
    if !canonical.is_dir() || !canonical.starts_with(root) {
        bail!("persisted current cwd 已移出 trusted root")
    }
    Ok(canonical)
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

fn permission_targets_for(
    context: &ToolContext,
    tool: &dyn Tool,
    input: &Value,
    summary: &str,
) -> Result<Vec<PermissionTarget>> {
    if tool.name() == "Monitor" {
        if let Some(command) = input.get("command").and_then(Value::as_str) {
            return Ok(vec![PermissionTarget::new(
                "Bash",
                vec![command.to_owned()],
            )]);
        }
        if let Some(ws) = input.get("ws").and_then(Value::as_str) {
            return Ok(vec![PermissionTarget::new("Monitor", vec![ws.to_owned()])]);
        }
    }
    let mut targets = Vec::new();
    let mut path_groups = Vec::new();
    for field in tool.path_fields() {
        if let Some(path) = input.get(*field).and_then(Value::as_str) {
            path_groups.push(context.permission_path_candidates(path)?);
        }
    }
    if matches!(tool.name(), "Glob" | "Grep") {
        targets.push(PermissionTarget::new(tool.name(), vec![summary.to_owned()]));
        if path_groups.is_empty() {
            path_groups.push(context.permission_path_candidates_for_resolved(&context.cwd())?);
        }
        for candidates in path_groups {
            targets.push(PermissionTarget::new("Read", candidates));
        }
    } else if path_groups.is_empty() {
        targets.push(PermissionTarget::new(tool.name(), vec![summary.to_owned()]));
    } else {
        for candidates in path_groups {
            targets.push(PermissionTarget::new(tool.name(), candidates));
        }
    }
    Ok(targets)
}

fn normalize_lexical_path(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn push_permission_path_candidate(candidates: &mut Vec<String>, path: &Path) {
    let rendered = normalize_path_for_display(path.to_string_lossy().into_owned());
    if !candidates.contains(&rendered) {
        candidates.push(rendered);
    }
}

fn validate_external_dynamic_watch_paths(paths: &[String]) -> Result<Vec<String>> {
    if paths.len() > MAX_EXTERNAL_WATCH_DYNAMIC_PATHS {
        bail!("hook watchPaths 超过 {MAX_EXTERNAL_WATCH_DYNAMIC_PATHS} 项限制")
    }
    let mut total_bytes = 0usize;
    let mut normalized = Vec::new();
    for path in paths {
        if path.is_empty() || path.len() > MAX_EXTERNAL_WATCH_PATH_BYTES || path.contains('\0') {
            bail!("hook watchPath 为空、过长或包含 NUL")
        }
        total_bytes = total_bytes
            .checked_add(path.len())
            .context("hook watchPaths 总长度溢出")?;
        if total_bytes > MAX_EXTERNAL_WATCH_PATH_TOTAL_BYTES {
            bail!("hook watchPaths 总长度超过 {MAX_EXTERNAL_WATCH_PATH_TOTAL_BYTES} 字节限制")
        }
        if !Path::new(path).is_absolute() {
            bail!("hook watchPath 必须是绝对路径: {path}")
        }
        reject_windows_network_or_device_path(path)?;
        if !normalized.contains(path) {
            normalized.push(path.clone());
        }
    }
    normalized.sort();
    Ok(normalized)
}

fn external_watch_spec_from_pattern(
    pattern: &str,
    cwd: &Path,
    require_absolute: bool,
) -> Result<ExternalWatchSpec> {
    if pattern.is_empty() || pattern.len() > MAX_EXTERNAL_WATCH_PATH_BYTES || pattern.contains('\0')
    {
        bail!("external watch pattern 为空、过长或包含 NUL")
    }
    if require_absolute && !Path::new(pattern).is_absolute() {
        bail!("hook watchPath 必须是绝对路径: {pattern}")
    }
    reject_windows_network_or_device_path(pattern)?;
    let joined = if Path::new(pattern).is_absolute() {
        PathBuf::from(pattern)
    } else {
        cwd.join(pattern)
    };
    let joined = normalize_lexical_path(&joined);
    reject_windows_network_or_device_resolved_path(&joined)?;
    if !external_pattern_has_glob(pattern) {
        return Ok(ExternalWatchSpec::Exact(joined));
    }

    let rendered = normalize_path_for_display(joined.to_string_lossy().into_owned());
    Glob::new(&rendered).with_context(|| format!("无效 external watch glob: {pattern}"))?;
    let mut root = PathBuf::new();
    for component in joined.components() {
        if external_pattern_has_glob(&component.as_os_str().to_string_lossy()) {
            break;
        }
        root.push(component.as_os_str());
    }
    if root.as_os_str().is_empty() {
        root = cwd.to_path_buf();
    }
    Ok(ExternalWatchSpec::Glob {
        root,
        pattern: rendered,
    })
}

fn external_pattern_has_glob(value: &str) -> bool {
    value.contains(['*', '?', '[', '{'])
}

fn scan_external_watch_specs(
    specs: &[ExternalWatchSpec],
) -> Result<BTreeMap<PathBuf, ExternalWatchFingerprint>> {
    let mut entries = BTreeMap::new();
    let mut visited_entries = 0usize;
    let mut remaining_hash_bytes = MAX_EXTERNAL_WATCH_HASH_TOTAL_BYTES;
    for spec in specs {
        match spec {
            ExternalWatchSpec::Exact(path) => scan_external_watch_path(
                path,
                None,
                &mut entries,
                &mut visited_entries,
                &mut remaining_hash_bytes,
            )?,
            ExternalWatchSpec::Tree(path) => scan_external_watch_tree(
                path,
                None,
                &mut entries,
                &mut visited_entries,
                &mut remaining_hash_bytes,
            )?,
            ExternalWatchSpec::Glob { root, pattern } => {
                let matcher = Glob::new(pattern)
                    .with_context(|| format!("无效 external watch glob: {pattern}"))?
                    .compile_matcher();
                scan_external_watch_tree(
                    root,
                    Some(&matcher),
                    &mut entries,
                    &mut visited_entries,
                    &mut remaining_hash_bytes,
                )?;
            }
        }
    }
    Ok(entries)
}

fn external_watch_specs_cover_path(specs: &[ExternalWatchSpec], path: &Path) -> bool {
    specs.iter().any(|spec| match spec {
        ExternalWatchSpec::Exact(watched) | ExternalWatchSpec::Tree(watched) => {
            path.starts_with(watched)
        }
        ExternalWatchSpec::Glob { pattern, .. } => Glob::new(pattern)
            .ok()
            .is_some_and(|glob| external_watch_matcher_matches(&glob.compile_matcher(), path)),
    })
}

fn scan_external_watch_path(
    path: &Path,
    matcher: Option<&globset::GlobMatcher>,
    entries: &mut BTreeMap<PathBuf, ExternalWatchFingerprint>,
    visited_entries: &mut usize,
    remaining_hash_bytes: &mut u64,
) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(());
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查 external watch path {}", path.display()));
        }
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        return scan_external_watch_tree(
            path,
            matcher,
            entries,
            visited_entries,
            remaining_hash_bytes,
        );
    }
    *visited_entries = visited_entries.saturating_add(1);
    if *visited_entries > MAX_EXTERNAL_WATCH_ENTRIES {
        bail!("external watch 扫描超过 {MAX_EXTERNAL_WATCH_ENTRIES} 个 entry 限制")
    }
    if matcher.is_none_or(|matcher| external_watch_matcher_matches(matcher, path)) {
        if let Some(fingerprint) = external_watch_fingerprint(path, remaining_hash_bytes)? {
            entries.insert(path.to_path_buf(), fingerprint);
        }
    }
    Ok(())
}

fn scan_external_watch_tree(
    root: &Path,
    matcher: Option<&globset::GlobMatcher>,
    entries: &mut BTreeMap<PathBuf, ExternalWatchFingerprint>,
    visited_entries: &mut usize,
    remaining_hash_bytes: &mut u64,
) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(());
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法检查 external watch root {}", root.display()));
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return scan_external_watch_path(
            root,
            matcher,
            entries,
            visited_entries,
            remaining_hash_bytes,
        );
    }
    for entry in WalkDir::new(root)
        .follow_links(false)
        .max_depth(MAX_EXTERNAL_WATCH_DEPTH)
        .into_iter()
    {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error)
                if error.io_error().is_some_and(|io| {
                    matches!(
                        io.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                    )
                }) =>
            {
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("external watch 遍历失败: {}", root.display()));
            }
        };
        *visited_entries = visited_entries.saturating_add(1);
        if *visited_entries > MAX_EXTERNAL_WATCH_ENTRIES {
            bail!("external watch 扫描超过 {MAX_EXTERNAL_WATCH_ENTRIES} 个 entry 限制")
        }
        if entry.file_type().is_dir() {
            continue;
        }
        let path = entry.path();
        if matcher.is_some_and(|matcher| !external_watch_matcher_matches(matcher, path)) {
            continue;
        }
        if let Some(fingerprint) = external_watch_fingerprint(path, remaining_hash_bytes)? {
            entries.insert(path.to_path_buf(), fingerprint);
        }
    }
    Ok(())
}

fn external_watch_matcher_matches(matcher: &globset::GlobMatcher, path: &Path) -> bool {
    let rendered = normalize_path_for_display(path.to_string_lossy().into_owned());
    matcher.is_match(rendered)
}

fn external_watch_fingerprint(
    path: &Path,
    remaining_hash_bytes: &mut u64,
) -> Result<Option<ExternalWatchFingerprint>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            return Ok(None);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法 fingerprint watch path {}", path.display()));
        }
    };
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos());
    if metadata.file_type().is_symlink() {
        let digest = std::fs::read_link(path).ok().map(|target| {
            let digest = Sha256::digest(target.as_os_str().as_encoded_bytes());
            <[u8; 32]>::from(digest)
        });
        return Ok(Some(ExternalWatchFingerprint {
            kind: 2,
            length: metadata.len(),
            modified_ns,
            digest,
        }));
    }
    if !metadata.is_file() {
        return Ok(None);
    }
    let digest = hash_external_regular_file(path, &metadata, remaining_hash_bytes)?;
    Ok(Some(ExternalWatchFingerprint {
        kind: 1,
        length: metadata.len(),
        modified_ns,
        digest,
    }))
}

fn hash_external_regular_file(
    path: &Path,
    expected: &std::fs::Metadata,
    remaining_hash_bytes: &mut u64,
) -> Result<Option<[u8; 32]>> {
    let length = expected.len();
    if length > MAX_EXTERNAL_WATCH_HASH_FILE_BYTES || length > *remaining_hash_bytes {
        return Ok(None);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(error) if external_watch_open_error_is_ignored(&error) => {
            return Ok(None);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("无法读取 external watch file {}", path.display()));
        }
    };
    let opened = file.metadata()?;
    if !opened.is_file()
        || opened.len() != expected.len()
        || opened.modified().ok() != expected.modified().ok()
    {
        return Ok(None);
    }
    *remaining_hash_bytes = remaining_hash_bytes.saturating_sub(length);
    let mut bytes = Vec::with_capacity(length as usize);
    std::io::Read::by_ref(&mut file)
        .take(length.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Ok(None);
    }
    let after = file.metadata()?;
    if after.len() != opened.len() || after.modified().ok() != opened.modified().ok() {
        return Ok(None);
    }
    Ok(Some(<[u8; 32]>::from(Sha256::digest(bytes))))
}

fn external_watch_open_error_is_ignored(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
    ) {
        return true;
    }
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::ELOOP)
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn reconcile_external_watch_state(
    state: &mut ExternalFileWatchState,
    specs: Vec<ExternalWatchSpec>,
    entries: BTreeMap<PathBuf, ExternalWatchFingerprint>,
) -> Result<Vec<ExternalWatchEvent>> {
    if !state.initialized || state.specs != specs {
        state.initialized = true;
        state.specs = specs;
        state.entries = entries;
        state.acknowledged.clear();
        return Ok(Vec::new());
    }
    let acknowledged = std::mem::take(&mut state.acknowledged);
    let mut paths = state
        .entries
        .keys()
        .chain(entries.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut events = Vec::new();
    for path in std::mem::take(&mut paths) {
        let previous = state.entries.get(&path);
        let current = entries.get(&path);
        if previous == current {
            continue;
        }
        if acknowledged
            .get(&path)
            .is_some_and(|expected| expected.as_ref() == current)
        {
            continue;
        }
        let kind = match (previous, current) {
            (None, Some(_)) => ExternalWatchEventKind::Add,
            (Some(_), None) => ExternalWatchEventKind::Unlink,
            (Some(_), Some(_)) => ExternalWatchEventKind::Change,
            (None, None) => continue,
        };
        events.push(ExternalWatchEvent { path, kind });
    }
    state.entries = entries;
    if events.len() > MAX_EXTERNAL_WATCH_EVENTS {
        bail!("external watch 单次变化超过 {MAX_EXTERNAL_WATCH_EVENTS} 项限制")
    }
    Ok(events)
}

fn push_bounded_external_watch_context(
    contexts: &mut Vec<String>,
    bytes: &mut usize,
    mut message: String,
) {
    if contexts.len() >= MAX_EXTERNAL_WATCH_CONTEXTS || *bytes >= MAX_EXTERNAL_WATCH_CONTEXT_BYTES {
        return;
    }
    let remaining = MAX_EXTERNAL_WATCH_CONTEXT_BYTES.saturating_sub(*bytes);
    if message.len() > remaining {
        let mut end = remaining;
        while !message.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        message.truncate(end);
    }
    *bytes = bytes.saturating_add(message.len());
    contexts.push(message);
}

pub(crate) fn reject_windows_network_or_device_path(value: &str) -> Result<()> {
    let normalized = value.replace('\\', "/");
    let namespace = normalized.to_ascii_lowercase();
    if normalized.starts_with("//")
        || matches!(
            namespace.as_str(),
            "/??" | "/device" | "/dosdevices" | "/global??"
        )
        || namespace.starts_with("/??/")
        || namespace.starts_with("/device/")
        || namespace.starts_with("/dosdevices/")
        || namespace.starts_with("/global??/")
    {
        bail!("拒绝 UNC 或 Windows device namespace 路径")
    }
    #[cfg(windows)]
    {
        let without_drive = normalized
            .get(2..)
            .filter(|_| normalized.as_bytes().get(1) == Some(&b':'))
            .unwrap_or(&normalized);
        if without_drive.contains(':') {
            bail!("拒绝 NTFS alternate data stream 路径")
        }
        for component in normalized.split('/') {
            let navigation_component = matches!(component, "." | "..");
            if !navigation_component
                && (component.ends_with(['.', ' '])
                    || (component.len() >= 3 && component.chars().all(|ch| ch == '.')))
            {
                bail!("拒绝 Windows 可疑路径规范化形式")
            }
            let component = component.trim_end_matches(['.', ' ']);
            let stem = component
                .split_once('.')
                .map_or(component, |(stem, _)| stem)
                .to_ascii_uppercase();
            if matches!(
                stem.as_str(),
                "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$" | "CONIN$" | "CONOUT$"
            ) || stem
                .strip_prefix("COM")
                .or_else(|| stem.strip_prefix("LPT"))
                .is_some_and(|number| {
                    number.len() == 1
                        && number
                            .as_bytes()
                            .first()
                            .is_some_and(|digit| matches!(digit, b'1'..=b'9'))
                })
            {
                bail!("拒绝 Windows 保留设备路径")
            }
        }
    }
    Ok(())
}

pub(crate) fn reject_windows_network_or_device_resolved_path(path: &Path) -> Result<()> {
    // Paths at this boundary were assembled by the harness (for example
    // `cwd.join(".")`), rather than supplied as an authoritative raw spelling.
    // Remove real `.`/`..` path components before applying the Windows
    // trailing-dot/device-name checks. Normal components such as `file.` and
    // `...` remain intact and are still rejected below.
    let lexical = normalize_lexical_path(path);
    reject_windows_network_or_device_resolved_text(&lexical.to_string_lossy())
}

fn reject_windows_network_or_device_resolved_text(value: &str) -> Result<()> {
    let normalized = normalize_windows_local_path_text(value);
    reject_windows_network_or_device_path(&normalized)
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

fn atomic_write_bytes(
    path: &Path,
    content: &[u8],
    permissions: Option<std::fs::Permissions>,
) -> Result<()> {
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
        file.write_all(content)?;
        file.flush()?;
        if let Some(permissions) = permissions {
            file.set_permissions(permissions)?;
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
    if let Some(home) = dirs::home_dir() {
        let harness_root = home.join(".open-agent-harness");
        if path.starts_with(&harness_root) {
            return ensure_private_managed_directory(&harness_root, path);
        }
    }
    // Explicit trusted storage overrides (primarily tests/embedding) retain
    // ordinary create_dir_all semantics. The default harness tree above is the
    // security boundary and is created one non-symlink component at a time.
    std::fs::create_dir_all(path)?;
    set_private_directory_permissions(path)?;
    Ok(())
}

fn ensure_private_managed_directory(managed_root: &Path, path: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(managed_root)
        .context("私有目录不在 managed root 内")?;
    ensure_private_directory_component(managed_root)?;
    let mut current = managed_root.to_owned();
    for component in relative.components() {
        match component {
            std::path::Component::CurDir => continue,
            std::path::Component::Normal(name) => current.push(name),
            _ => bail!("私有目录包含非法路径组件"),
        }
        ensure_private_directory_component(&current)?;
    }
    Ok(())
}

fn ensure_private_directory_component(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("私有目录组件不能是 symlink: {}", path.display())
        }
        Ok(metadata) if !metadata.is_dir() => {
            bail!("私有目录组件不是目录: {}", path.display())
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(path)
                .with_context(|| format!("无法创建私有目录组件 {}", path.display()))?;
        }
        Err(error) => return Err(error.into()),
    }
    set_private_directory_permissions(path)
}

fn set_private_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use tokio::{sync::Barrier, time::timeout};

    use super::*;
    use crate::permissions::{PermissionDecision, PermissionMode};

    #[test]
    fn windows_native_namespace_strings_are_rejected_before_path_conversion() {
        for path in [
            r"\\?\C:\secret.txt",
            r"\\?\UNC\server\share\secret.txt",
            r"\\.\PhysicalDrive0",
            r"\??\C:\secret.txt",
            r"\Device\Mup\server\share\secret.txt",
            r"\DosDevices\C:\secret.txt",
            r"\GLOBAL??\C:\secret.txt",
            r"\device\harddiskvolume1\secret.txt",
        ] {
            let error = reject_windows_network_or_device_path(path)
                .expect_err("native namespace path must be rejected on every host platform");
            assert!(error.to_string().contains("device namespace"), "{path}");
        }
        for path in [r"C:\Users\user\file.txt", "C:/Users/user/file.txt"] {
            assert!(
                reject_windows_network_or_device_path(path).is_ok(),
                "ordinary drive path was rejected: {path}"
            );
        }

        assert_eq!(
            normalize_windows_local_path_text(r"\\?\C:\Users\user\file.txt"),
            "C:/Users/user/file.txt"
        );
        assert!(
            reject_windows_network_or_device_resolved_text(r"\\?\C:\Users\user\file.txt").is_ok(),
            "trusted canonical local-disk path must normalize"
        );
        for path in [
            r"\\?\UNC\server\share\secret.txt",
            r"\\.\PhysicalDrive0",
            r"\??\C:\secret.txt",
            r"\\?\GLOBALROOT\Device\HarddiskVolume1\secret.txt",
        ] {
            assert!(
                reject_windows_network_or_device_resolved_text(path).is_err(),
                "resolved network/device namespace path was accepted: {path}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_resolved_paths_allow_harness_inserted_current_directory_components() {
        assert!(
            reject_windows_network_or_device_resolved_path(Path::new(r"C:\Users\user\workspace\."))
                .is_ok()
        );
        assert!(
            reject_windows_network_or_device_resolved_path(Path::new(
                r"C:\Users\user\workspace\nested\.."
            ))
            .is_ok()
        );
        assert!(
            reject_windows_network_or_device_resolved_path(Path::new(
                r"C:\Users\user\workspace\file."
            ))
            .is_err()
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_raw_paths_allow_navigation_but_reject_ambiguous_trailing_dots() {
        for path in [".", "..", r"C:\Users\user\workspace\.", r"nested\.."] {
            assert!(
                reject_windows_network_or_device_path(path).is_ok(),
                "ordinary navigation component was rejected: {path}"
            );
        }
        for path in ["...", r"C:\Users\user\workspace\file."] {
            assert!(
                reject_windows_network_or_device_path(path).is_err(),
                "ambiguous trailing-dot component was accepted: {path}"
            );
        }
    }

    struct BarrierTool {
        barrier: Arc<Barrier>,
    }

    struct NamedReadTool(&'static str);

    struct PendingDiscovery(&'static str);

    struct DeletePathTool;

    struct OversizedFileFeedbackService;

    struct FailingFileFeedbackService;

    #[async_trait]
    impl ToolService for OversizedFileFeedbackService {
        async fn files_changed(&self, _paths: &[PathBuf]) -> Result<Vec<String>> {
            Ok((0..100)
                .map(|_| format!("diagnostic-marker{}", "x".repeat(4096)))
                .collect())
        }

        async fn shutdown(&self) {}
    }

    #[async_trait]
    impl ToolService for FailingFileFeedbackService {
        async fn files_changed(&self, _paths: &[PathBuf]) -> Result<Vec<String>> {
            bail!("mock synchronization failure")
        }

        async fn shutdown(&self) {}
    }

    #[async_trait]
    impl Tool for NamedReadTool {
        fn name(&self) -> &str {
            self.0
        }

        fn description(&self) -> &str {
            "Test-only named deferred read"
        }

        fn input_schema(&self) -> Value {
            object_schema(json!({}), &[])
        }

        fn read_only(&self, _: &Value) -> bool {
            true
        }

        fn summary(&self, _: &Value) -> String {
            self.0.to_owned()
        }

        async fn execute(&self, _: &ToolContext, _: Value) -> Result<ToolOutput> {
            Ok(ToolOutput::success("done"))
        }
    }

    #[async_trait]
    impl ToolDiscovery for PendingDiscovery {
        async fn refresh(&self) -> Result<ToolRefresh> {
            Ok(ToolRefresh {
                upsert: Vec::new(),
                remove: Vec::new(),
            })
        }

        fn pending_names(&self) -> Vec<String> {
            vec![self.0.to_owned()]
        }
    }

    #[async_trait]
    impl Tool for DeletePathTool {
        fn name(&self) -> &str {
            "DeletePathForTest"
        }

        fn description(&self) -> &str {
            "Test-only path-aware file deletion"
        }

        fn input_schema(&self) -> Value {
            object_schema(
                json!({"file_path":{"type":"string","maxLength":4096}}),
                &["file_path"],
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
            input["file_path"].as_str().unwrap_or_default().to_owned()
        }

        async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
            let path = context.resolve_path(
                input["file_path"]
                    .as_str()
                    .context("test delete 缺少 file_path")?,
            )?;
            context.track_before_edit(&path)?;
            std::fs::remove_file(&path)?;
            Ok(ToolOutput::success(format!(
                "Deleted {}",
                context.display_path(&path)
            )))
        }
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
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let started_events = Arc::clone(&events);
        let finished_events = Arc::clone(&events);
        let observer = ToolExecutionObserver::new(
            Arc::new(move |index| {
                started_events.lock().unwrap().push(("start", index));
            }),
            Arc::new(move |index, _, _| {
                finished_events.lock().unwrap().push(("finish", index));
            }),
        );
        let outputs = timeout(
            Duration::from_secs(1),
            registry.execute_batch_observed(&context, &calls, Some(&observer)),
        )
        .await
        .expect("calls should reach the barrier together");
        assert_eq!(outputs.len(), 2);
        assert!(outputs.iter().all(|output| output.content == "done"));
        let events = events.lock().unwrap();
        assert_eq!(&events[..2], &[("start", 0), ("start", 1)]);
        assert_eq!(
            events.iter().filter(|(kind, _)| *kind == "finish").count(),
            2
        );
    }

    #[tokio::test]
    async fn permission_updates_use_the_original_id_and_recheck_deny_rules() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PermissionManager::new(
            PermissionMode::Default,
            false,
            Vec::new(),
            vec!["Write(denied.txt)".to_owned()],
        );
        manager.set_prompt_handler(Some(Arc::new(|request| {
            assert_eq!(request.tool, "Write");
            assert_eq!(request.tool_use_id, "call-1");
            assert_eq!(request.input["file_path"], "original.txt");
            Ok(PermissionDecision::AllowWithUpdatedInput(json!({
                "file_path":"updated.txt", "content":"updated"
            })))
        })));
        let context = ToolContext::new(temp.path().to_owned(), manager);
        let registry = ToolRegistry::default();
        let calls = vec![(
            "Write".to_owned(),
            json!({"file_path":"original.txt", "content":"original"}),
        )];
        let outputs = registry
            .execute_batch_observed_with_ids(&context, &calls, &["call-1".to_owned()], None)
            .await;
        assert!(!outputs[0].is_error, "{}", outputs[0].content);
        assert!(!temp.path().join("original.txt").exists());
        assert_eq!(
            std::fs::read_to_string(temp.path().join("updated.txt")).unwrap(),
            "updated"
        );

        context.permissions.set_prompt_handler(Some(Arc::new(|_| {
            Ok(PermissionDecision::AllowWithUpdatedInput(json!({
                "file_path":"denied.txt", "content":"blocked"
            })))
        })));
        let denied = registry
            .execute_batch_observed_with_ids(&context, &calls, &["call-2".to_owned()], None)
            .await;
        assert!(denied[0].is_error);
        assert!(!temp.path().join("denied.txt").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn permission_and_file_change_hooks_run_at_real_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let hooks = Arc::new(
            crate::hooks::HookRunner::from_settings(&crate::config::Settings {
                raw: json!({"hooks":{
                    "PermissionRequest":[{"matcher":"Write", "hooks":[{
                        "type":"command",
                        "command":"printf '%s' '{\"additionalContext\":\"permission-request-seen\"}'"
                    }]}],
                    "PermissionDenied":[{"matcher":"Write", "hooks":[{
                        "type":"command",
                        "command":"printf '%s' '{\"additionalContext\":\"permission-denied-seen\"}'"
                    }]}],
                    "FileChanged":[
                        {"matcher":"changed.txt", "hooks":[{
                            "type":"command",
                            "command":"printf '%s' '{\"additionalContext\":\"file-path-change-seen\"}'"
                        }]},
                        {"matcher":"Write", "hooks":[{
                            "type":"command",
                            "command":"printf '%s' '{\"additionalContext\":\"legacy-tool-change-seen\"}'"
                        }]},
                        {"matcher":"*", "hooks":[{
                            "type":"command",
                            "command":"printf '%s' '{\"additionalContext\":\"single-wildcard-change\"}'"
                        }]}
                    ]
                }}),
            })
            .unwrap(),
        );
        let mut denied_context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::Default,
                false,
                Vec::new(),
                vec!["Write(blocked.txt)".to_owned()],
            ),
        );
        denied_context.set_hooks(Arc::clone(&hooks));
        let registry = ToolRegistry::default();
        let denied = registry
            .execute(
                &denied_context,
                "Write",
                json!({"file_path":"blocked.txt", "content":"blocked"}),
            )
            .await;
        assert!(denied.is_error);
        assert!(denied.content.contains("permission-request-seen"));
        assert!(denied.content.contains("permission-denied-seen"));
        assert!(!temp.path().join("blocked.txt").exists());

        let mut allowed_context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        allowed_context.set_hooks(hooks);
        let changed = registry
            .execute(
                &allowed_context,
                "Write",
                json!({"file_path":"changed.txt", "content":"changed"}),
            )
            .await;
        assert!(!changed.is_error, "{}", changed.content);
        assert!(changed.content.contains("file-path-change-seen"));
        assert!(changed.content.contains("legacy-tool-change-seen"));
        assert_eq!(changed.content.matches("single-wildcard-change").count(), 1);
    }

    #[tokio::test]
    async fn file_service_feedback_and_failures_are_visible_and_globally_bounded() {
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
        let registry = ToolRegistry::with_services(
            Vec::new(),
            Vec::new(),
            vec![
                Arc::new(FailingFileFeedbackService),
                Arc::new(OversizedFileFeedbackService),
            ],
        )
        .unwrap();
        let output = registry
            .execute(
                &context,
                "Write",
                json!({"file_path":"changed.txt", "content":"changed"}),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("mock synchronization failure"));
        assert!(output.content.contains("diagnostic-marker"));
        assert!(output.content.len() <= MAX_TOOL_RESULT_BYTES);
    }

    #[tokio::test]
    async fn team_mailbox_notification_is_once_restorable_and_non_consuming() {
        use crate::agents::team::{MemberSpec, TeamLimits, TeamService};

        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let service = TeamService::create_in(
            workspace.path(),
            storage.path(),
            "notification-team",
            "coordinator",
            TeamLimits::default(),
        )
        .unwrap();
        let coordinator = service.coordinator_id();
        let member = service
            .add_member(
                coordinator,
                MemberSpec {
                    name: "worker".to_owned(),
                    custom_agent: None,
                    depth: 1,
                    requested_policy: AgentToolPolicy::default(),
                },
                &AgentToolPolicy::default(),
            )
            .unwrap();
        let assignment = service.assign(coordinator, member.id, "audit").unwrap();
        assert_eq!(assignment.member.id, member.id);
        service
            .mark_running(coordinator, member.id, uuid::Uuid::new_v4())
            .unwrap();

        let context = ToolContext::new(
            workspace.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.track_team_mailbox(service.clone(), coordinator);
        let checkpoint = context.team_notification_checkpoint();
        service
            .finish(coordinator, member.id, true, "team-result")
            .unwrap();

        let first = context.drain_background_notifications().await;
        assert_eq!(first.len(), 1);
        assert!(first[0].contains("team-result"));
        assert!(context.drain_background_notifications().await.is_empty());
        let still_readable = service
            .read_mailbox(coordinator, coordinator, 0, 16)
            .unwrap();
        assert_eq!(still_readable.len(), 1);

        context.restore_team_notification_checkpoint(&checkpoint);
        let retried = context.drain_background_notifications().await;
        assert_eq!(retried.len(), 1);
        assert!(retried[0].contains("team-result"));
    }

    #[tokio::test]
    async fn failed_turn_removes_consumed_preexisting_bash_capture_only_when_cleanup_was_armed() {
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
        let armed = temp.path().join("armed-capture.log");
        let retained = temp.path().join("retained-capture.log");
        std::fs::write(&armed, "discard after rollback").unwrap();
        std::fs::write(&retained, "explicitly retained before turn").unwrap();
        let mut checkpoint = context.background_notification_checkpoint().await;
        checkpoint.bash_tasks.insert(
            "consumed-armed".to_owned(),
            BashTaskCheckpoint {
                notification_delivered: false,
                output_path: armed.clone(),
                output_cleanup_armed: true,
            },
        );
        checkpoint.bash_tasks.insert(
            "consumed-retained".to_owned(),
            BashTaskCheckpoint {
                notification_delivered: false,
                output_path: retained.clone(),
                output_cleanup_armed: false,
            },
        );

        context
            .restore_background_notification_checkpoint(&checkpoint)
            .await;
        assert!(!armed.exists());
        assert!(retained.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn instructions_loaded_hook_runs_only_for_newly_discovered_files() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("AGENTS.md"), "workspace rule").unwrap();
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let hooks = crate::hooks::HookRunner::from_settings(&crate::config::Settings {
            raw: json!({"hooks":{"InstructionsLoaded":[{"matcher":"*", "hooks":[{
                "type":"command", "command":"true"
            }]}]}}),
        })
        .unwrap()
        .with_observer(Some(Arc::new(move |event| {
            captured.lock().unwrap().push(event.clone());
        })));
        let mut context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_hooks(Arc::new(hooks));
        context.reload_workspace_context().await.unwrap();
        context.reload_workspace_context().await.unwrap();

        let starts = events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    crate::hooks::HookExecutionEvent::HookStarted { event, .. }
                        if event == "InstructionsLoaded"
                )
            })
            .count();
        assert_eq!(starts, 1);
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
    async fn successful_context_file_writes_hot_refresh_instructions_and_skills() {
        let temp = tempfile::tempdir().unwrap();
        let skill = temp.path().join(".open-agent-harness/skills/hot/SKILL.md");
        std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
        std::fs::write(temp.path().join("AGENTS.md"), "agent-rule-before-refresh").unwrap();
        std::fs::write(
            &skill,
            "---\nname: hot\ndescription: skill-before-refresh\n---\nold workflow",
        )
        .unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.reload_workspace_context().await.unwrap();
        let registry = ToolRegistry::default();

        let read_agents = registry
            .execute(
                &context,
                "Read",
                json!({"file_path":temp.path().join("AGENTS.md")}),
            )
            .await;
        assert!(!read_agents.is_error, "{}", read_agents.content);
        let write_agents = registry
            .execute(
                &context,
                "Write",
                json!({
                    "file_path":temp.path().join("AGENTS.md"),
                    "content":"agent-rule-after-refresh"
                }),
            )
            .await;
        assert!(!write_agents.is_error, "{}", write_agents.content);
        assert!(!write_agents.rollback_turn);
        let refreshed_agents = context.workspace_system_context();
        assert!(refreshed_agents.contains("agent-rule-after-refresh"));
        assert!(!refreshed_agents.contains("agent-rule-before-refresh"));

        let read_skill = registry
            .execute(&context, "Read", json!({"file_path":&skill}))
            .await;
        assert!(!read_skill.is_error, "{}", read_skill.content);
        let write_skill = registry
            .execute(
                &context,
                "Write",
                json!({
                    "file_path":&skill,
                    "content":"---\nname: hot\ndescription: skill-after-refresh\n---\nnew workflow"
                }),
            )
            .await;
        assert!(!write_skill.is_error, "{}", write_skill.content);
        assert!(!write_skill.rollback_turn);
        assert_eq!(
            context.skill("hot").unwrap().description,
            "skill-after-refresh"
        );
        let refreshed_skills = context.workspace_system_context();
        assert!(refreshed_skills.contains("skill-after-refresh"));
        assert!(!refreshed_skills.contains("skill-before-refresh"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn skill_hot_refresh_emits_blockable_config_change_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let skill = temp.path().join(".open-agent-harness/skills/hot/SKILL.md");
        std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
        std::fs::write(
            &skill,
            "---\nname: hot\ndescription: before\n---\nold workflow",
        )
        .unwrap();
        let mut context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.reload_workspace_context().await.unwrap();
        context.set_hooks(Arc::new(
            crate::hooks::HookRunner::from_settings(&crate::config::Settings {
                raw: json!({"hooks":{"ConfigChange":[{"matcher":"skills","hooks":[{
                    "type":"command",
                    "command":"printf '%s' '{\"additionalContext\":\"skill-config-reloaded\"}'"
                }]}]}}),
            })
            .unwrap(),
        ));
        let registry = ToolRegistry::default();
        let read = registry
            .execute(&context, "Read", json!({"file_path":&skill}))
            .await;
        assert!(!read.is_error, "{}", read.content);
        let write = registry
            .execute(
                &context,
                "Write",
                json!({
                    "file_path":&skill,
                    "content":"---\nname: hot\ndescription: after\n---\nnew workflow"
                }),
            )
            .await;
        assert!(!write.is_error, "{}", write.content);
        assert!(write.content.contains("skill-config-reloaded"));
        assert_eq!(context.skill("hot").unwrap().description, "after");

        context.set_hooks(Arc::new(
            crate::hooks::HookRunner::from_settings(&crate::config::Settings {
                raw: json!({"hooks":{"ConfigChange":[{"matcher":"skills","hooks":[{
                    "type":"command", "command":"printf '%s' blocked >&2; exit 2"
                }]}]}}),
            })
            .unwrap(),
        ));
        let read = registry
            .execute(&context, "Read", json!({"file_path":&skill}))
            .await;
        assert!(!read.is_error, "{}", read.content);
        let blocked = registry
            .execute(
                &context,
                "Write",
                json!({
                    "file_path":&skill,
                    "content":"---\nname: hot\ndescription: blocked\n---\nblocked workflow"
                }),
            )
            .await;
        assert!(blocked.is_error);
        assert!(blocked.rollback_turn);
        assert_eq!(context.skill("hot").unwrap().description, "after");
    }

    #[tokio::test]
    async fn transient_context_transaction_rolls_back_existing_and_new_files_across_roots() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(additional.join(".open-agent-harness/skills/new")).unwrap();
        let agents = launch.join("AGENTS.md");
        let skill = additional.join(".open-agent-harness/skills/new/SKILL.md");
        std::fs::write(&agents, "original-agent-rule").unwrap();
        let context = ToolContext::new(
            launch.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .switch_workspace(additional.clone(), additional)
            .await
            .unwrap();
        assert!(context.begin_hot_refresh_file_transaction().unwrap());

        context.track_before_edit(&agents).unwrap();
        context
            .expect_after_edit(&agents, b"changed-agent-rule")
            .unwrap();
        atomic_write(&agents, "changed-agent-rule").unwrap();
        context.track_before_edit(&skill).unwrap();
        context
            .expect_after_edit(&skill, b"---\nname: new\ndescription: new\n---\nbody")
            .unwrap();
        atomic_write(&skill, "---\nname: new\ndescription: new\n---\nbody").unwrap();

        context
            .rollback_hot_refresh_file_transaction()
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&agents).unwrap(),
            "original-agent-rule"
        );
        assert!(!skill.exists());
    }

    #[tokio::test]
    async fn parent_context_transaction_cannot_write_child_owned_path_and_absorbs_child_success() {
        let temp = tempfile::tempdir().unwrap();
        let agents = temp.path().join("AGENTS.md");
        std::fs::write(&agents, "original-rule").unwrap();
        let parent = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        assert!(parent.begin_hot_refresh_file_transaction().unwrap());
        parent.track_before_edit(&agents).unwrap();
        parent.expect_after_edit(&agents, b"parent-rule").unwrap();
        atomic_write(&agents, "parent-rule").unwrap();

        let child = parent.fork_for_agent();
        assert!(child.begin_hot_refresh_file_transaction().unwrap());
        child.track_before_edit(&agents).unwrap();
        child.expect_after_edit(&agents, b"child-rule").unwrap();
        atomic_write(&agents, "child-rule").unwrap();

        let error = parent.track_before_edit(&agents).unwrap_err();
        assert!(error.to_string().contains("活跃子事务"));
        assert_eq!(std::fs::read_to_string(&agents).unwrap(), "child-rule");

        child.finish_hot_refresh_file_transaction().unwrap();
        parent
            .rollback_hot_refresh_file_transaction()
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(&agents).unwrap(), "original-rule");
    }

    #[tokio::test]
    async fn path_aware_deletes_remove_stale_instructions_and_skills() {
        let temp = tempfile::tempdir().unwrap();
        let agents = temp.path().join("AGENTS.md");
        let skill = temp
            .path()
            .join(".open-agent-harness/skills/removable/SKILL.md");
        std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
        std::fs::write(&agents, "agent-rule-to-delete").unwrap();
        std::fs::write(
            &skill,
            "---\nname: removable\ndescription: skill-to-delete\n---\nworkflow",
        )
        .unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.reload_workspace_context().await.unwrap();
        let registry =
            ToolRegistry::with_extensions(vec![Arc::new(DeletePathTool)], Vec::new()).unwrap();

        let delete_agents = registry
            .execute(&context, "DeletePathForTest", json!({"file_path":&agents}))
            .await;
        assert!(!delete_agents.is_error, "{}", delete_agents.content);
        assert!(
            !context
                .workspace_system_context()
                .contains("agent-rule-to-delete")
        );

        let delete_skill = registry
            .execute(&context, "DeletePathForTest", json!({"file_path":&skill}))
            .await;
        assert!(!delete_skill.is_error, "{}", delete_skill.content);
        assert!(context.skill("removable").is_none());
        assert!(
            !context
                .workspace_system_context()
                .contains("skill-to-delete")
        );
    }

    #[tokio::test]
    async fn external_watcher_add_change_unlink_refreshes_workspace_context() {
        let temp = tempfile::tempdir().unwrap();
        let agents = temp.path().join("AGENTS.md");
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.reload_workspace_context().await.unwrap();
        let duplicate = context.poll_external_file_changes().await.unwrap();
        assert!(duplicate.is_empty(), "duplicate contexts: {duplicate:?}");
        let baseline = context.workspace_context_changes.generation();

        std::fs::write(&agents, "alpha-rule").unwrap();
        assert!(
            context
                .poll_external_file_changes()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(context.workspace_context_changes.generation(), baseline + 1);
        assert!(context.refresh_workspace_context_if_stale().await.unwrap());
        assert!(context.workspace_system_context().contains("alpha-rule"));

        std::fs::write(&agents, "bravo-rule").unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert!(context.refresh_workspace_context_if_stale().await.unwrap());
        assert!(context.workspace_system_context().contains("bravo-rule"));
        assert!(!context.workspace_system_context().contains("alpha-rule"));

        std::fs::remove_file(&agents).unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert!(context.refresh_workspace_context_if_stale().await.unwrap());
        assert!(!context.workspace_system_context().contains("bravo-rule"));
    }

    #[tokio::test]
    async fn external_skill_changes_reload_the_catalog_before_the_next_request() {
        let temp = tempfile::tempdir().unwrap();
        let skill = temp.path().join(".open-agent-harness/skills/live/SKILL.md");
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.reload_workspace_context().await.unwrap();
        context.poll_external_file_changes().await.unwrap();

        std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
        std::fs::write(
            &skill,
            "---\nname: live\ndescription: before\n---\nworkflow-before",
        )
        .unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert!(context.refresh_workspace_context_if_stale().await.unwrap());
        assert_eq!(context.skill("live").unwrap().description, "before");

        std::fs::write(
            &skill,
            "---\nname: live\ndescription: after\n---\nworkflow-after-",
        )
        .unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert!(context.refresh_workspace_context_if_stale().await.unwrap());
        assert_eq!(context.skill("live").unwrap().description, "after");

        std::fs::remove_file(&skill).unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert!(context.refresh_workspace_context_if_stale().await.unwrap());
        assert!(context.skill("live").is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn external_watcher_runs_file_changed_hook_once_per_observed_change() {
        let temp = tempfile::tempdir().unwrap();
        let watched = temp.path().join("watched.txt");
        let mut context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_hooks(Arc::new(
            crate::hooks::HookRunner::from_settings(&crate::config::Settings {
                raw: json!({"hooks":{"FileChanged":[{
                    "matcher":"watched.txt",
                    "hooks":[{"type":"command","command":"printf '%s' '{\"additionalContext\":\"external-change-observed\"}'"}]
                }]}}),
            })
            .unwrap(),
        ));
        context.poll_external_file_changes().await.unwrap();

        std::fs::write(&watched, "first").unwrap();
        let contexts = context.poll_external_file_changes().await.unwrap();
        assert_eq!(contexts.len(), 1);
        assert!(contexts[0].contains("external-change-observed"));
        let duplicate = context.poll_external_file_changes().await.unwrap();
        assert!(duplicate.is_empty(), "duplicate contexts: {duplicate:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn automatic_context_watch_does_not_widen_file_changed_matcher_scope() {
        let temp = tempfile::tempdir().unwrap();
        let additional = tempfile::tempdir().unwrap();
        let mut context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_hooks(Arc::new(
            crate::hooks::HookRunner::from_settings(&crate::config::Settings {
                raw: json!({"hooks":{"FileChanged":[{
                    "matcher":"*",
                    "hooks":[{"type":"command","command":"printf '%s' '{\"additionalContext\":\"unexpected-auto-hook\"}'"}]
                }]}}),
            })
            .unwrap(),
        ));
        context
            .add_trusted_roots(&[additional.path().to_path_buf()])
            .unwrap();
        context.reload_workspace_context().await.unwrap();
        context.poll_external_file_changes().await.unwrap();

        std::fs::write(additional.path().join("AGENTS.md"), "new-context-rule").unwrap();
        let contexts = context.poll_external_file_changes().await.unwrap();
        assert!(
            contexts.is_empty(),
            "automatic watch leaked into hook: {contexts:?}"
        );
        assert!(context.refresh_workspace_context_if_stale().await.unwrap());
        assert!(
            context
                .workspace_system_context()
                .contains("new-context-rule")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_tool_commit_is_acknowledged_without_a_second_watcher_hook() {
        let temp = tempfile::tempdir().unwrap();
        let watched = temp.path().join("watched.txt");
        let mut context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_hooks(Arc::new(
            crate::hooks::HookRunner::from_settings(&crate::config::Settings {
                raw: json!({"hooks":{"FileChanged":[{
                    "matcher":"watched.txt",
                    "hooks":[{"type":"command","command":"printf '%s' '{\"additionalContext\":\"single-file-change-hook\"}'"}]
                }]}}),
            })
            .unwrap(),
        ));
        context.poll_external_file_changes().await.unwrap();

        let output = ToolRegistry::default()
            .execute(
                &context,
                "Write",
                json!({"file_path":&watched, "content":"created-by-tool"}),
            )
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("single-file-change-hook"));
        let duplicate = context.poll_external_file_changes().await.unwrap();
        assert!(duplicate.is_empty(), "duplicate contexts: {duplicate:?}");
    }

    #[tokio::test]
    async fn dynamic_watch_paths_rebaseline_and_tool_acknowledgement_suppresses_duplicates() {
        let temp = tempfile::tempdir().unwrap();
        let watched = temp.path().join("dynamic.env");
        std::fs::write(&watched, "before").unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        assert!(
            context
                .replace_hook_watch_paths(&["relative.env".to_owned()])
                .is_err()
        );
        context
            .replace_hook_watch_paths(&[watched.display().to_string()])
            .unwrap();
        context.poll_external_file_changes().await.unwrap();
        let baseline = context.workspace_context_changes.generation();

        std::fs::write(&watched, "direct").unwrap();
        context
            .acknowledge_external_file_changes(std::slice::from_ref(&watched))
            .await
            .unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert_eq!(context.workspace_context_changes.generation(), baseline);

        std::fs::write(&watched, "extern").unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert_eq!(context.workspace_context_changes.generation(), baseline + 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn external_watcher_never_follows_symlink_targets() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let watched = temp.path().join("watched-link");
        let target = temp.path().join("target.txt");
        std::fs::write(&target, "before").unwrap();
        symlink(&target, &watched).unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .replace_hook_watch_paths(&[watched.display().to_string()])
            .unwrap();
        context.poll_external_file_changes().await.unwrap();
        let baseline = context.workspace_context_changes.generation();

        std::fs::write(&target, "after-").unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert_eq!(context.workspace_context_changes.generation(), baseline);

        let second = temp.path().join("second.txt");
        std::fs::write(&second, "second").unwrap();
        std::fs::remove_file(&watched).unwrap();
        symlink(&second, &watched).unwrap();
        context.poll_external_file_changes().await.unwrap();
        assert_eq!(context.workspace_context_changes.generation(), baseline + 1);
    }

    #[test]
    fn external_watcher_event_fanout_is_bounded() {
        let specs = vec![ExternalWatchSpec::Exact(PathBuf::from("/tmp/watched"))];
        let mut state = ExternalFileWatchState {
            initialized: true,
            specs: specs.clone(),
            ..ExternalFileWatchState::default()
        };
        let entries = (0..=MAX_EXTERNAL_WATCH_EVENTS)
            .map(|index| {
                (
                    PathBuf::from(format!("/tmp/watched/{index}")),
                    ExternalWatchFingerprint {
                        kind: 1,
                        length: 1,
                        modified_ns: Some(index as u128),
                        digest: None,
                    },
                )
            })
            .collect();
        assert!(reconcile_external_watch_state(&mut state, specs, entries).is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn hot_refresh_hook_failure_is_transaction_fatal_and_context_is_restorable() {
        let temp = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let agents = temp.path().join("AGENTS.md");
        std::fs::write(&agents, "hook-rule-before-refresh").unwrap();
        let mut context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.reload_workspace_context().await.unwrap();
        context.set_file_history(
            FileHistory::create_in(temp.path(), uuid::Uuid::new_v4(), storage.path(), true)
                .unwrap(),
        );
        context.set_hooks(Arc::new(
            crate::hooks::HookRunner::from_settings(&crate::config::Settings {
                raw: json!({"hooks":{"InstructionsLoaded":[{"matcher":"*","hooks":[{
                    "type":"command", "command":"false"
                }]}]}}),
            })
            .unwrap(),
        ));
        let memory_checkpoint = context.workspace_context_checkpoint();
        let file_checkpoint = context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
            .unwrap()
            .unwrap();
        let registry = ToolRegistry::default();
        let read = registry
            .execute(&context, "Read", json!({"file_path":&agents}))
            .await;
        assert!(!read.is_error, "{}", read.content);
        let write = registry
            .execute(
                &context,
                "Write",
                json!({
                    "file_path":&agents,
                    "content":"hook-rule-after-refresh"
                }),
            )
            .await;
        assert!(write.is_error);
        assert!(write.rollback_turn);
        assert_eq!(
            std::fs::read_to_string(&agents).unwrap(),
            "hook-rule-after-refresh"
        );
        assert!(
            context
                .workspace_system_context()
                .contains("hook-rule-before-refresh")
        );
        assert!(
            !context
                .workspace_system_context()
                .contains("hook-rule-after-refresh")
        );

        context
            .rollback_file_checkpoint(file_checkpoint.id, 0)
            .unwrap();
        context.finish_file_checkpoint(file_checkpoint.id).unwrap();
        context.restore_workspace_context_checkpoint(&memory_checkpoint);
        assert_eq!(
            std::fs::read_to_string(&agents).unwrap(),
            "hook-rule-before-refresh"
        );
        assert!(
            context
                .workspace_system_context()
                .contains("hook-rule-before-refresh")
        );
    }

    #[tokio::test]
    async fn workspace_relocation_refreshes_instructions_and_skills() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let current = temp.path().join("current");
        let extension_root = temp.path().join("extension-skills");
        std::fs::create_dir_all(extension_root.join("persistent")).unwrap();
        std::fs::write(
            extension_root.join("persistent/SKILL.md"),
            "---\nname: persistent\ndescription: extension\n---\nextension workflow",
        )
        .unwrap();
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
        context.set_extension_skills(
            crate::skills::discover_skill_root(&extension_root, temp.path()).unwrap(),
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
        assert_eq!(
            context.skill("persistent").unwrap().description,
            "extension"
        );

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
        assert_eq!(
            context.skill("persistent").unwrap().description,
            "extension"
        );

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

    #[tokio::test]
    async fn forked_context_keeps_workspace_deny_roots_and_recorders_local() {
        let temp = tempfile::tempdir().unwrap();
        let parent_root = temp.path().join("parent");
        let child_root = temp.path().join("child");
        let child_explicit_root = temp.path().join("child-explicit");
        for (root, deny) in [(&parent_root, "ParentOnly"), (&child_root, "ChildOnly")] {
            std::fs::create_dir_all(root.join(".open-agent-harness")).unwrap();
            std::fs::write(root.join("AGENTS.md"), format!("{deny} instructions")).unwrap();
            std::fs::write(
                root.join(".open-agent-harness/settings.json"),
                format!(r#"{{"permissions":{{"deny":["{deny}"]}}}}"#),
            )
            .unwrap();
        }
        std::fs::create_dir_all(&child_explicit_root).unwrap();
        std::fs::write(
            child_explicit_root.join("AGENTS.md"),
            "child-explicit-only instructions",
        )
        .unwrap();
        let parent = ToolContext::new(
            parent_root.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        parent.reload_workspace_context().await.unwrap();
        let parent_checkpoint = parent.workspace_context_checkpoint();
        let recorder_calls = Arc::new(AtomicUsize::new(0));
        for current_cwd in [false, true] {
            let calls = Arc::clone(&recorder_calls);
            let recorder: WorkspaceStateRecorder = Arc::new(move |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            if current_cwd {
                parent.set_current_cwd_state_recorder(Some(recorder));
            } else {
                parent.set_workspace_state_recorder(Some(recorder));
            }
        }

        let child = parent.fork_for_agent();
        child
            .switch_workspace(child_root.clone(), child_root.clone())
            .await
            .unwrap();
        child
            .add_trusted_roots(std::slice::from_ref(&child_explicit_root))
            .unwrap();
        child.reload_workspace_context().await.unwrap();
        parent.reload_workspace_context().await.unwrap();
        child.record_workspace_transition().unwrap();
        child.record_current_cwd_transition().unwrap();
        let registry = ToolRegistry::default();
        let child_agents = child_root.join("AGENTS.md");
        let read = registry
            .execute(&child, "Read", json!({"file_path":&child_agents}))
            .await;
        assert!(!read.is_error, "{}", read.content);
        let write = registry
            .execute(
                &child,
                "Write",
                json!({"file_path":&child_agents,"content":"ChildOnly refreshed instructions"}),
            )
            .await;
        assert!(!write.is_error, "{}", write.content);

        assert_eq!(recorder_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            parent.trusted_roots(),
            vec![parent_root.canonicalize().unwrap()]
        );
        assert!(
            child
                .trusted_roots()
                .contains(&child_root.canonicalize().unwrap())
        );
        assert!(
            child
                .workspace_system_context()
                .contains("child-explicit-only")
        );
        assert!(
            !parent
                .workspace_system_context()
                .contains("child-explicit-only")
        );
        assert_eq!(
            parent
                .permissions
                .decide("ParentOnly", "", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );
        assert_eq!(
            parent
                .permissions
                .decide("ChildOnly", "", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(
            child
                .permissions
                .decide("ChildOnly", "", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );

        let (child_ready_tx, child_ready_rx) = tokio::sync::oneshot::channel();
        let (parent_restored_tx, parent_restored_rx) = tokio::sync::oneshot::channel();
        let child_observer = child.clone();
        let observation = tokio::spawn(async move {
            child_ready_tx.send(()).unwrap();
            parent_restored_rx.await.unwrap();
            child_observer
                .permissions
                .decide("ChildOnly", "", false, false, false)
                .unwrap()
        });
        child_ready_rx.await.unwrap();
        parent.restore_workspace_context_checkpoint(&parent_checkpoint);
        parent_restored_tx.send(()).unwrap();
        assert_eq!(
            observation.await.unwrap(),
            PermissionDecision::Deny,
            "parent rollback must not replace the child's workspace deny rules"
        );
        parent
            .permissions
            .set_session_mode(PermissionMode::DontAsk)
            .unwrap();
        assert_eq!(child.permissions.effective_mode(), PermissionMode::DontAsk);
    }

    #[tokio::test]
    async fn foreground_and_background_forks_refresh_parent_context_before_next_round() {
        let temp = tempfile::tempdir().unwrap();
        let agents = temp.path().join("AGENTS.md");
        let skill = temp
            .path()
            .join(".open-agent-harness/skills/shared/SKILL.md");
        std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
        std::fs::write(&agents, "foreground-before").unwrap();
        std::fs::write(
            &skill,
            "---\nname: shared\ndescription: background-before\n---\nold workflow",
        )
        .unwrap();
        let parent = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        parent.reload_workspace_context().await.unwrap();
        let registry = ToolRegistry::default();

        let foreground = parent.fork_for_agent();
        let read = registry
            .execute(&foreground, "Read", json!({"file_path":&agents}))
            .await;
        assert!(!read.is_error, "{}", read.content);
        let write = registry
            .execute(
                &foreground,
                "Write",
                json!({"file_path":&agents,"content":"foreground-after"}),
            )
            .await;
        assert!(!write.is_error, "{}", write.content);
        assert!(
            parent
                .workspace_system_context()
                .contains("foreground-before")
        );
        assert!(!parent.refresh_workspace_context_if_stale().await.unwrap());
        foreground.commit_workspace_context_changes_to_parent();
        assert!(parent.refresh_workspace_context_if_stale().await.unwrap());
        assert!(
            parent
                .workspace_system_context()
                .contains("foreground-after")
        );

        let background = parent.fork_for_agent();
        let background_registry = registry.clone();
        let background_skill = skill.clone();
        let completion = tokio::spawn(async move {
            let read = background_registry
                .execute(
                    &background,
                    "Read",
                    json!({"file_path":&background_skill}),
                )
                .await;
            assert!(!read.is_error, "{}", read.content);
            let output = background_registry
                .execute(
                    &background,
                    "Write",
                    json!({
                        "file_path":&background_skill,
                        "content":"---\nname: shared\ndescription: background-after\n---\nnew workflow"
                    }),
                )
                .await;
            if !output.is_error {
                background.commit_workspace_context_changes_to_parent();
            }
            output
        })
        .await
        .unwrap();
        assert!(!completion.is_error, "{}", completion.content);
        assert_eq!(
            parent.skill("shared").unwrap().description,
            "background-before"
        );
        assert!(parent.refresh_workspace_context_if_stale().await.unwrap());
        assert_eq!(
            parent.skill("shared").unwrap().description,
            "background-after"
        );
    }

    #[tokio::test]
    async fn trusted_additional_root_is_scoped_and_loads_nested_agents_on_first_touch() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        let private_state = temp.path().join("private-state");
        let private_nested = private_state.join("nested-workspace");
        let late_private_state = temp.path().join("late-private-state");
        let late_nested = late_private_state.join("child-workspace");
        let nested = additional.join("crates/core");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(&private_nested).unwrap();
        std::fs::create_dir_all(&late_nested).unwrap();
        std::fs::write(launch.join("AGENTS.md"), "launch-only").unwrap();
        std::fs::write(additional.join("AGENTS.md"), "additional-root").unwrap();
        std::fs::write(nested.join("AGENTS.md"), "core-only").unwrap();
        std::fs::write(nested.join("lib.rs"), "fn demo() {}\n").unwrap();

        let context = ToolContext::new(
            launch,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        assert_eq!(
            context
                .add_trusted_roots(std::slice::from_ref(&additional))
                .unwrap(),
            [std::fs::canonicalize(&additional).unwrap()]
        );
        context.reload_workspace_context().await.unwrap();
        let initial = context.workspace_system_context();
        assert!(initial.contains("launch-only"));
        assert!(initial.contains("additional-root"));
        assert!(!initial.contains("core-only"));
        assert!(
            !context
                .is_outside_workspace(additional.to_str().unwrap())
                .unwrap()
        );

        let read = ToolRegistry::default()
            .execute(
                &context,
                "Read",
                json!({"file_path":nested.join("lib.rs").display().to_string()}),
            )
            .await;
        assert!(!read.is_error, "{}", read.content);
        let refreshed = context.workspace_system_context();
        assert!(refreshed.contains("core-only"));
        assert!(refreshed.contains("scope=\"crates/core/**\""));
        assert_eq!(refreshed.matches("core-only").count(), 1);

        context.reserve_private_state_root(&private_state).unwrap();
        assert!(
            context
                .add_trusted_roots(std::slice::from_ref(&private_nested))
                .is_err()
        );
        assert!(
            context
                .switch_workspace(private_nested.clone(), private_state.clone())
                .await
                .is_err()
        );
        assert!(context.reserve_private_state_root(&additional).is_err());
        assert!(context.reserve_private_state_root(temp.path()).is_err());

        let child = context.fork_for_agent();
        child
            .add_trusted_roots(std::slice::from_ref(&late_nested))
            .unwrap();
        assert!(
            context
                .reserve_private_state_root(&late_private_state)
                .is_err()
        );
    }

    #[tokio::test]
    async fn bare_mode_honors_an_explicit_add_dir_context() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let explicit = launch.join("explicit");
        std::fs::create_dir_all(&explicit).unwrap();
        std::fs::write(launch.join("AGENTS.md"), "automatic-root").unwrap();
        std::fs::write(explicit.join("AGENTS.md"), "explicit-root").unwrap();
        let mut context = ToolContext::new(
            launch,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_bare(true);
        assert!(
            context
                .add_trusted_roots(std::slice::from_ref(&explicit))
                .unwrap()
                .is_empty()
        );
        context.reload_workspace_context().await.unwrap();
        let rendered = context.workspace_system_context();
        assert!(!rendered.contains("automatic-root"));
        assert!(rendered.contains("explicit-root"));
        assert!(rendered.contains("scope=\"explicit/**\""));
    }

    #[test]
    fn explicit_trusted_root_can_be_removed_but_primary_root_cannot() {
        let primary = tempfile::tempdir().unwrap();
        let additional = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            primary.path().to_owned(),
            PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
        );
        context
            .add_trusted_roots(&[additional.path().to_owned()])
            .unwrap();
        assert_eq!(context.trusted_roots().len(), 2);
        let removed = context.remove_trusted_root(additional.path()).unwrap();
        assert_eq!(removed, additional.path().canonicalize().unwrap());
        assert_eq!(
            context.trusted_roots(),
            vec![primary.path().canonicalize().unwrap()]
        );
        assert!(context.remove_trusted_root(primary.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn trusted_roots_still_reject_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        let outside = temp.path().join("outside");
        for directory in [&launch, &additional, &outside] {
            std::fs::create_dir_all(directory).unwrap();
        }
        symlink(&outside, additional.join("escape")).unwrap();
        let context = ToolContext::new(
            launch,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .add_trusted_roots(std::slice::from_ref(&additional))
            .unwrap();
        assert!(
            context
                .is_outside_workspace(additional.join("escape/file").to_str().unwrap())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn persisted_add_dir_cwd_restores_and_refreshes_nested_context() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        let nested = additional.join("nested/deep");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(additional.join("AGENTS.md"), "additional-root-rule").unwrap();
        std::fs::write(nested.join("AGENTS.md"), "resumed-nested-rule").unwrap();
        let context = ToolContext::new(
            launch,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .add_trusted_roots(std::slice::from_ref(&additional))
            .unwrap();
        let root = std::fs::canonicalize(&additional).unwrap();
        context
            .restore_persisted_cwd(&workspace_key(&root), Path::new("nested/deep"))
            .await
            .unwrap();
        context.reload_workspace_context().await.unwrap();

        assert_eq!(context.workspace_root(), root);
        assert_eq!(context.cwd(), std::fs::canonicalize(&nested).unwrap());
        let rendered = context.workspace_system_context();
        assert!(rendered.contains("additional-root-rule"));
        assert!(rendered.contains("resumed-nested-rule"));
    }

    #[tokio::test]
    async fn persisted_cwd_rejects_deleted_or_untrusted_paths() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        let nested = additional.join("nested");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        let root = std::fs::canonicalize(&additional).unwrap();
        let key = workspace_key(&root);

        let untrusted = ToolContext::new(
            launch.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        assert!(
            untrusted
                .restore_persisted_cwd(&key, Path::new("nested"))
                .await
                .is_err()
        );

        let trusted = ToolContext::new(
            launch,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        trusted
            .add_trusted_roots(std::slice::from_ref(&additional))
            .unwrap();
        std::fs::remove_dir(&nested).unwrap();
        assert!(
            trusted
                .restore_persisted_cwd(&key, Path::new("nested"))
                .await
                .is_err()
        );
        assert!(
            trusted
                .restore_persisted_cwd(&key, Path::new("../outside"))
                .await
                .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persisted_cwd_rejects_a_path_replaced_by_outside_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        let nested = additional.join("nested");
        let outside = temp.path().join("outside");
        for directory in [&launch, &nested, &outside] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let context = ToolContext::new(
            launch,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .add_trusted_roots(std::slice::from_ref(&additional))
            .unwrap();
        let root = std::fs::canonicalize(&additional).unwrap();
        std::fs::remove_dir(&nested).unwrap();
        symlink(&outside, &nested).unwrap();
        assert!(
            context
                .restore_persisted_cwd(&workspace_key(&root), Path::new("nested"))
                .await
                .is_err()
        );
    }

    #[test]
    fn one_checkpoint_tracks_and_rolls_back_every_trusted_root() {
        let temp = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(&additional).unwrap();
        let context = ToolContext::new(
            launch.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .add_trusted_roots(std::slice::from_ref(&additional))
            .unwrap();
        let session = uuid::Uuid::new_v4();
        context
            .set_file_histories(vec![
                FileHistory::create_in(&launch, session, storage.path(), true).unwrap(),
                FileHistory::create_in(&additional, session, storage.path(), true).unwrap(),
            ])
            .unwrap();
        let checkpoint = context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 4)
            .unwrap()
            .unwrap();
        for (root, after) in [
            (&launch, b"launch-after".as_slice()),
            (&additional, b"extra-after".as_slice()),
        ] {
            let path = root.join("owned.txt");
            std::fs::write(&path, "before").unwrap();
            context.track_before_edit(&path).unwrap();
            context.expect_after_edit(&path, after).unwrap();
            std::fs::write(path, after).unwrap();
        }

        let (report, message_count) = context.rollback_file_checkpoint(checkpoint.id, 4).unwrap();
        assert_eq!(message_count, 4);
        assert_eq!(report.restored, 2);
        assert_eq!(
            std::fs::read_to_string(launch.join("owned.txt")).unwrap(),
            "before"
        );
        assert_eq!(
            std::fs::read_to_string(additional.join("owned.txt")).unwrap(),
            "before"
        );
        context.finish_file_checkpoint(checkpoint.id).unwrap();
    }

    #[test]
    fn cross_root_rollback_preflights_all_roots_before_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(&additional).unwrap();
        let context = ToolContext::new(
            launch.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .add_trusted_roots(std::slice::from_ref(&additional))
            .unwrap();
        let session = uuid::Uuid::new_v4();
        context
            .set_file_histories(vec![
                FileHistory::create_in(&launch, session, storage.path(), true).unwrap(),
                FileHistory::create_in(&additional, session, storage.path(), true).unwrap(),
            ])
            .unwrap();
        let checkpoint = context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
            .unwrap()
            .unwrap();
        for root in [&launch, &additional] {
            let path = root.join("owned.txt");
            std::fs::write(&path, "before").unwrap();
            context.track_before_edit(&path).unwrap();
            context.expect_after_edit(&path, b"after").unwrap();
            std::fs::write(path, "after").unwrap();
        }
        std::fs::write(additional.join("owned.txt"), "concurrent").unwrap();

        assert!(context.rollback_file_checkpoint(checkpoint.id, 0).is_err());
        assert_eq!(
            std::fs::read_to_string(launch.join("owned.txt")).unwrap(),
            "after"
        );
        assert!(
            context
                .file_histories
                .read()
                .unwrap()
                .values()
                .all(|history| history
                    .checkpoints()
                    .unwrap()
                    .into_iter()
                    .find(|info| info.id == checkpoint.id)
                    .is_some_and(|info| info.status
                        == crate::file_history::CheckpointStatus::RollbackConflict))
        );
        context.finish_file_checkpoint(checkpoint.id).unwrap();
    }

    #[tokio::test]
    async fn file_transaction_rolls_back_every_workspace_visited_in_the_turn() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch-transaction");
        let alternate = temp.path().join("alternate-transaction");
        let storage = temp.path().join("history-storage");
        std::fs::create_dir_all(&launch).unwrap();
        std::fs::create_dir_all(&alternate).unwrap();
        std::fs::create_dir_all(&storage).unwrap();
        let context = ToolContext::new(
            launch.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_file_history(
            FileHistory::create_in(&launch, uuid::Uuid::new_v4(), &storage, true).unwrap(),
        );
        let checkpoint = context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
            .unwrap()
            .unwrap();

        let launch_file = launch.join("owned.txt");
        std::fs::write(&launch_file, "launch-before").unwrap();
        context.track_before_edit(&launch_file).unwrap();
        context
            .expect_after_edit(&launch_file, b"launch-after")
            .unwrap();
        std::fs::write(&launch_file, "launch-after").unwrap();

        context
            .switch_workspace(alternate.clone(), alternate.clone())
            .await
            .unwrap();
        let alternate_file = alternate.join("owned.txt");
        std::fs::write(&alternate_file, "alternate-before").unwrap();
        context.track_before_edit(&alternate_file).unwrap();
        context
            .expect_after_edit(&alternate_file, b"alternate-after")
            .unwrap();
        std::fs::write(&alternate_file, "alternate-after").unwrap();
        context
            .switch_workspace(launch.clone(), launch.clone())
            .await
            .unwrap();

        let (report, _) = context.rollback_file_checkpoint(checkpoint.id, 0).unwrap();
        assert_eq!(report.restored, 2);
        assert_eq!(
            std::fs::read_to_string(launch_file).unwrap(),
            "launch-before"
        );
        assert_eq!(
            std::fs::read_to_string(alternate_file).unwrap(),
            "alternate-before"
        );
        context.finish_file_checkpoint(checkpoint.id).unwrap();
    }

    #[test]
    fn detached_agent_checkpoint_rolls_back_without_touching_the_parent() {
        let temp = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_file_history(
            FileHistory::create_in(temp.path(), uuid::Uuid::new_v4(), storage.path(), true)
                .unwrap(),
        );
        let parent = context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
            .unwrap()
            .unwrap();
        let mut child = context.fork_for_agent();
        let child_checkpoint = child.begin_detached_file_checkpoint().unwrap().unwrap();
        assert_ne!(parent.id, child_checkpoint.id);
        assert_eq!(child_checkpoint.ancestor_ids, vec![parent.id]);

        let parent_file = temp.path().join("parent.txt");
        let child_file = temp.path().join("child.txt");
        std::fs::write(&parent_file, "parent-before").unwrap();
        std::fs::write(&child_file, "child-before").unwrap();
        context.track_before_edit(&parent_file).unwrap();
        context
            .expect_after_edit(&parent_file, b"parent-after")
            .unwrap();
        std::fs::write(&parent_file, "parent-after").unwrap();
        child.track_before_edit(&child_file).unwrap();
        child
            .expect_after_edit(&child_file, b"child-after")
            .unwrap();
        std::fs::write(&child_file, "child-after").unwrap();

        child
            .rollback_file_checkpoint(child_checkpoint.id, 0)
            .unwrap();
        child.finish_file_checkpoint(child_checkpoint.id).unwrap();
        assert_eq!(
            std::fs::read_to_string(&child_file).unwrap(),
            "child-before"
        );
        assert_eq!(
            std::fs::read_to_string(&parent_file).unwrap(),
            "parent-after"
        );
        context.rollback_file_checkpoint(parent.id, 0).unwrap();
        context.finish_file_checkpoint(parent.id).unwrap();
        assert_eq!(
            std::fs::read_to_string(parent_file).unwrap(),
            "parent-before"
        );
    }

    #[test]
    fn parent_transaction_captures_detached_agent_writes_to_the_same_file() {
        let temp = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_file_history(
            FileHistory::create_in(temp.path(), uuid::Uuid::new_v4(), storage.path(), true)
                .unwrap(),
        );
        let parent = context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
            .unwrap()
            .unwrap();
        let mut child = context.fork_for_agent();
        let child_checkpoint = child.begin_detached_file_checkpoint().unwrap().unwrap();
        let path = temp.path().join("shared.txt");
        std::fs::write(&path, "before").unwrap();

        child.track_before_edit(&path).unwrap();
        child.expect_after_edit(&path, b"child-write").unwrap();
        std::fs::write(&path, "child-write").unwrap();
        context.track_before_edit(&path).unwrap();
        context.expect_after_edit(&path, b"parent-write").unwrap();
        std::fs::write(&path, "parent-write").unwrap();

        assert!(
            child
                .rollback_file_checkpoint(child_checkpoint.id, 0)
                .is_err()
        );
        child.finish_file_checkpoint(child_checkpoint.id).unwrap();
        context.rollback_file_checkpoint(parent.id, 0).unwrap();
        context.finish_file_checkpoint(parent.id).unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap(), "before");
    }

    #[test]
    fn replayed_user_message_uuid_cannot_reuse_a_finished_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context.set_file_history(
            FileHistory::create_in(temp.path(), uuid::Uuid::new_v4(), storage.path(), true)
                .unwrap(),
        );
        let id = uuid::Uuid::new_v4();
        context
            .begin_file_checkpoint_with_id(id, CheckpointBoundary::UserMessage, 0)
            .unwrap();
        context.finish_file_checkpoint(id).unwrap();
        assert!(
            context
                .begin_file_checkpoint_with_id(id, CheckpointBoundary::UserMessage, 0)
                .is_err()
        );
    }

    #[test]
    fn public_tool_surface_contains_the_documented_generic_tools() {
        let names = ToolRegistry::default()
            .definitions()
            .into_iter()
            .filter_map(|definition| definition["name"].as_str().map(ToOwned::to_owned))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), 22);
        for required in [
            "AskUserQuestion",
            "CronCreate",
            "CronDelete",
            "CronList",
            "Read",
            "ScheduleWakeup",
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
            "RunWorkflow",
            "ToolSearch",
        ] {
            assert!(names.contains(required), "missing {required}");
        }
    }

    #[tokio::test]
    async fn tool_search_is_case_insensitive_bounded_and_reports_pending_discovery() {
        let registry = ToolRegistry::with_integrations(
            Vec::new(),
            vec![
                Arc::new(NamedReadTool("DeferredAlpha")),
                Arc::new(NamedReadTool("DeferredBeta")),
                Arc::new(NamedReadTool("DeferredGamma")),
                Arc::new(NamedReadTool("DeferredDelta")),
            ],
            Vec::new(),
            vec![Arc::new(PendingDiscovery("ExampleMcp"))],
        )
        .unwrap();
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

        let limited = registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"deferred", "max_results":2}),
            )
            .await;
        assert!(!limited.is_error, "{}", limited.content);
        let limited: Value = serde_json::from_str(&limited.content).unwrap();
        assert_eq!(limited["matches"].as_array().unwrap().len(), 2);
        assert_eq!(limited["pending_integrations"], json!([]));

        let optional_terms = registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"alpha gamma", "max_results":10}),
            )
            .await;
        assert!(!optional_terms.is_error, "{}", optional_terms.content);
        let optional_terms: Value = serde_json::from_str(&optional_terms.content).unwrap();
        assert_eq!(optional_terms["matches"].as_array().unwrap().len(), 2);

        let required_term = registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"+alpha gamma", "max_results":10}),
            )
            .await;
        assert!(!required_term.is_error, "{}", required_term.content);
        let required_term: Value = serde_json::from_str(&required_term.content).unwrap();
        assert_eq!(required_term["matches"].as_array().unwrap().len(), 1);
        assert_eq!(required_term["matches"][0]["name"], "DeferredAlpha");

        let selected = registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"SeLeCt:deferredalpha,DEFERREDBETA"}),
            )
            .await;
        assert!(!selected.is_error, "{}", selected.content);
        let selected: Value = serde_json::from_str(&selected.content).unwrap();
        assert_eq!(selected["loaded"], json!(["DeferredAlpha", "DeferredBeta"]));

        let already_active = registry
            .execute(&context, "ToolSearch", json!({"query":"deferredalpha"}))
            .await;
        assert!(!already_active.is_error, "{}", already_active.content);
        let already_active: Value = serde_json::from_str(&already_active.content).unwrap();
        assert_eq!(already_active["matches"], json!([{"name":"DeferredAlpha"}]));

        let missing = registry
            .execute(&context, "ToolSearch", json!({"query":"does-not-exist"}))
            .await;
        assert!(!missing.is_error, "{}", missing.content);
        let missing: Value = serde_json::from_str(&missing.content).unwrap();
        assert_eq!(missing["matches"], json!([]));
        assert_eq!(missing["pending_integrations"], json!(["ExampleMcp"]));

        let invalid_limit = registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"deferred", "max_results":0}),
            )
            .await;
        assert!(invalid_limit.is_error);
    }

    #[test]
    fn monitor_command_reuses_bash_permission_identity_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
        );
        let command = "printf ok && printf done";
        let targets = permission_targets_for(
            &context,
            &MonitorTool,
            &json!({"command":command,"description":"test"}),
            command,
        )
        .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].tool, "Bash");
        assert_eq!(targets[0].candidates, vec![command]);

        let ws = "wss://example.invalid/events";
        let targets = permission_targets_for(
            &context,
            &MonitorTool,
            &json!({"ws":ws,"description":"test"}),
            ws,
        )
        .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].tool, "Monitor");
        assert_eq!(targets[0].candidates, vec![ws]);
    }

    #[test]
    fn tool_restriction_is_exact_and_rejects_invalid_requests() {
        let registry = ToolRegistry::default();
        registry.restrict_to(&["Read".to_owned()]).unwrap();
        let names = registry
            .definitions()
            .into_iter()
            .filter_map(|definition| definition["name"].as_str().map(ToOwned::to_owned))
            .collect::<Vec<_>>();
        assert_eq!(names, ["Read"]);

        let unknown = ToolRegistry::default();
        assert!(unknown.restrict_to(&["Missing".to_owned()]).is_err());
        let duplicate = ToolRegistry::default();
        assert!(
            duplicate
                .restrict_to(&["Read".to_owned(), "Read".to_owned()])
                .is_err()
        );
    }

    #[test]
    fn scoped_agent_registry_activates_only_explicit_deferred_tools() {
        let barrier: Arc<dyn Tool> = Arc::new(BarrierTool {
            barrier: Arc::new(Barrier::new(1)),
        });
        let registry = ToolRegistry::with_extensions(Vec::new(), vec![barrier]).unwrap();
        let scoped = registry
            .scoped_for_agent(&AgentToolPolicy {
                allowed_tools: Some(BTreeSet::from([
                    "Read".to_owned(),
                    "BarrierRead".to_owned(),
                ])),
                disallowed_tools: BTreeSet::new(),
            })
            .unwrap();
        let names = scoped
            .definitions()
            .into_iter()
            .filter_map(|definition| definition["name"].as_str().map(ToOwned::to_owned))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            names,
            BTreeSet::from(["BarrierRead".to_owned(), "Read".to_owned()])
        );
        assert_eq!(scoped.deferred_count(), 0);
        assert!(
            registry
                .scoped_for_agent(&AgentToolPolicy {
                    allowed_tools: Some(BTreeSet::from(["ToolSearch".to_owned()])),
                    disallowed_tools: BTreeSet::new(),
                })
                .is_err()
        );
    }

    #[test]
    fn scoped_agent_deny_only_policy_keeps_every_other_deferred_tool() {
        let registry = ToolRegistry::with_extensions(
            Vec::new(),
            vec![
                Arc::new(NamedReadTool("AllowedDeferred")),
                Arc::new(NamedReadTool("DeniedDeferred")),
            ],
        )
        .unwrap();
        let scoped = registry
            .scoped_for_agent(&AgentToolPolicy {
                allowed_tools: None,
                disallowed_tools: BTreeSet::from(["Bash".to_owned(), "DeniedDeferred".to_owned()]),
            })
            .unwrap();
        let names = scoped
            .definitions()
            .into_iter()
            .filter_map(|definition| definition["name"].as_str().map(ToOwned::to_owned))
            .collect::<BTreeSet<_>>();

        assert!(names.contains("AllowedDeferred"));
        assert!(!names.contains("DeniedDeferred"));
        assert!(!names.contains("Bash"));
        assert!(!names.contains("ToolSearch"));
        assert_eq!(scoped.deferred_count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn managed_private_storage_rejects_every_intermediate_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        for (index, (intermediate, suffix)) in [
            ("agent-history", "workspace/agent.json"),
            ("plans", "workspace/latest.md"),
            ("worktrees", "agents/workspace/agent-id"),
        ]
        .into_iter()
        .enumerate()
        {
            let case = temp.path().join(index.to_string());
            let managed_root = case.join(".open-agent-harness");
            let outside = case.join("outside");
            std::fs::create_dir_all(&managed_root).unwrap();
            std::fs::create_dir_all(&outside).unwrap();
            symlink(&outside, managed_root.join(intermediate)).unwrap();

            let requested = managed_root.join(intermediate).join(suffix);
            let directory = requested.parent().unwrap();
            let error = ensure_private_managed_directory(&managed_root, directory).unwrap_err();
            assert!(format!("{error:#}").contains("不能是 symlink"));
            assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
        }
    }
}

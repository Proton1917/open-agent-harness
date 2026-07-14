use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::{
    tools::{
        ensure_private_directory, reject_windows_network_or_device_path,
        reject_windows_network_or_device_resolved_path, workspace_key,
    },
    types::Message,
};

const MAX_TRANSCRIPT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_TRANSCRIPT_RECORDS: usize = 100_000;
const MAX_SESSION_LIST_SCAN: usize = 10_000;
const MAX_SESSION_LIST_RESULTS: usize = 100;
const REDACTED_SECRET: &str = "[secret-redacted]";
const REDACTED_PATH: &str = "[absolute-path-redacted]";

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    session_id: Uuid,
    cwd: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    workspace_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    current_root_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    current_cwd: Option<PathBuf>,
    timestamp_ms: u128,
    #[serde(default)]
    compact_boundary: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<Message>,
}

impl Record {
    fn from_state(
        session_id: Uuid,
        workspace: &SessionWorkspaceState,
        current_cwd: Option<&SessionCurrentCwdState>,
        compact_boundary: bool,
        message: Option<Message>,
    ) -> Self {
        Self {
            session_id,
            cwd: workspace.cwd.clone(),
            workspace_key: workspace.workspace_key.clone(),
            current_root_key: current_cwd.map(|state| state.root_key.clone()),
            current_cwd: current_cwd.map(|state| state.cwd.clone()),
            timestamp_ms: now_ms(),
            compact_boundary,
            message,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionWorkspaceState {
    pub workspace_key: Option<String>,
    pub cwd: PathBuf,
}

/// A foreground shell's current directory, represented without persisting an
/// absolute local path. This is deliberately separate from the primary
/// workspace state used by worktree restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCurrentCwdState {
    pub root_key: String,
    pub cwd: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSummary {
    pub id: Uuid,
    pub modified_ms: u128,
    pub bytes: u64,
}

impl SessionWorkspaceState {
    fn launch() -> Self {
        Self {
            workspace_key: None,
            cwd: PathBuf::from("."),
        }
    }
}

struct LoadedTranscript {
    messages: Vec<Message>,
    message_workspaces: Vec<SessionWorkspaceState>,
    message_current_cwds: Vec<Option<SessionCurrentCwdState>>,
    boundary_workspace: SessionWorkspaceState,
    boundary_current_cwd: Option<SessionCurrentCwdState>,
    workspace: SessionWorkspaceState,
    current_cwd: Option<SessionCurrentCwdState>,
}

#[derive(Debug, Clone)]
pub struct SessionStore {
    pub id: Uuid,
    cwd: PathBuf,
    file: PathBuf,
    enabled: bool,
    workspace: Arc<Mutex<SessionWorkspaceState>>,
    current_cwd: Arc<Mutex<Option<SessionCurrentCwdState>>>,
    write_lock: Arc<Mutex<()>>,
}

/// A user-selected root for session transcripts and their file-history journals.
///
/// The path must already exist so selecting an override never creates an
/// unexpected ancestor tree. It is canonicalized once, and every managed child
/// is checked against that canonical boundary before use.
#[derive(Debug, Clone)]
pub struct SessionStateRoot {
    root: PathBuf,
}

impl SessionStateRoot {
    pub fn open(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            bail!("session state root 必须是绝对路径")
        }
        reject_windows_network_or_device_path(&path.to_string_lossy())?;
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("session state root 不存在或无法读取: {}", path.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("session state root 必须是非 symlink 目录")
        }
        let root = fs::canonicalize(path)
            .with_context(|| format!("无法解析 session state root: {}", path.display()))?;
        reject_windows_network_or_device_resolved_path(&root)?;
        let metadata = fs::symlink_metadata(&root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() || !root.is_absolute() {
            bail!("session state root 规范化结果无效")
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            bail!("--session-state-root 当前仅支持可强制 0700/0600 权限的 Unix 平台")
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o777 != 0o700 {
                bail!("session state root 必须预先设置为 Unix 0700 私有目录")
            }
            Ok(Self { root })
        }
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    fn project_directory(&self, cwd: &Path) -> Result<PathBuf> {
        let projects = self.private_child("projects")?;
        ensure_private_bounded_child(&self.root, &projects, &workspace_key(cwd))
    }

    pub fn file_history_root(&self) -> Result<PathBuf> {
        self.private_child("file-history")
    }

    fn private_child(&self, name: &str) -> Result<PathBuf> {
        ensure_private_bounded_child(&self.root, &self.root, name)
    }
}

impl SessionStore {
    pub fn persistence_enabled(&self) -> bool {
        self.enabled
    }

    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub fn workspace_state(&self) -> SessionWorkspaceState {
        self.workspace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn current_cwd_state(&self) -> Option<SessionCurrentCwdState> {
        self.current_cwd
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn create(cwd: &Path, enabled: bool) -> Result<Self> {
        Self::create_with_directory(cwd, enabled, || project_directory(cwd))
    }

    /// Lists recent persisted sessions for the current workspace without
    /// opening or mutating their transcripts.
    pub fn list(cwd: &Path, limit: usize) -> Result<Vec<SessionSummary>> {
        Self::list_from_directory(project_directory(cwd)?, limit)
    }

    pub fn list_in(
        cwd: &Path,
        state_root: &SessionStateRoot,
        limit: usize,
    ) -> Result<Vec<SessionSummary>> {
        Self::list_from_directory(state_root.project_directory(cwd)?, limit)
    }

    fn list_from_directory(directory: PathBuf, limit: usize) -> Result<Vec<SessionSummary>> {
        if limit == 0 || limit > MAX_SESSION_LIST_RESULTS {
            bail!("session list limit 必须在 1..={MAX_SESSION_LIST_RESULTS} 范围内")
        }
        let mut sessions = Vec::new();
        let mut scanned = 0usize;
        for entry in fs::read_dir(directory)? {
            scanned = scanned.saturating_add(1);
            if scanned > MAX_SESSION_LIST_SCAN {
                bail!("session 目录条目超过 {MAX_SESSION_LIST_SCAN} 个安全上限")
            }
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            let Some(id) = path
                .file_stem()
                .and_then(|value| value.to_str())
                .and_then(|value| value.parse::<Uuid>().ok())
            else {
                continue;
            };
            let modified_ms = metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |duration| duration.as_millis());
            sessions.push(SessionSummary {
                id,
                modified_ms,
                bytes: metadata.len(),
            });
        }
        sessions.sort_by(|left, right| {
            right
                .modified_ms
                .cmp(&left.modified_ms)
                .then_with(|| left.id.cmp(&right.id))
        });
        sessions.truncate(limit);
        Ok(sessions)
    }

    pub fn create_in(cwd: &Path, state_root: &SessionStateRoot, enabled: bool) -> Result<Self> {
        Self::create_with_directory(cwd, enabled, || state_root.project_directory(cwd))
    }

    fn create_with_directory(
        cwd: &Path,
        enabled: bool,
        directory: impl FnOnce() -> Result<PathBuf>,
    ) -> Result<Self> {
        let id = Uuid::new_v4();
        let file = if enabled {
            directory()?.join(format!("{id}.jsonl"))
        } else {
            PathBuf::new()
        };
        Ok(Self {
            id,
            cwd: cwd.to_owned(),
            file,
            enabled,
            workspace: Arc::new(Mutex::new(SessionWorkspaceState::launch())),
            current_cwd: Arc::new(Mutex::new(None)),
            write_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn resume(cwd: &Path, id: Uuid, enabled: bool) -> Result<(Self, Vec<Message>)> {
        Self::resume_from_directory(cwd, id, enabled, project_directory(cwd)?)
    }

    pub fn resume_in(
        cwd: &Path,
        id: Uuid,
        state_root: &SessionStateRoot,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        Self::resume_from_directory(cwd, id, enabled, state_root.project_directory(cwd)?)
    }

    fn resume_from_directory(
        cwd: &Path,
        id: Uuid,
        enabled: bool,
        directory: PathBuf,
    ) -> Result<(Self, Vec<Message>)> {
        let file = directory.join(format!("{id}.jsonl"));
        if !file.exists() {
            bail!("当前目录下没有会话 {id}")
        }
        let loaded = load_transcript(&file)?;
        Ok((
            Self {
                id,
                cwd: cwd.to_owned(),
                file,
                enabled,
                workspace: Arc::new(Mutex::new(loaded.workspace)),
                current_cwd: Arc::new(Mutex::new(loaded.current_cwd)),
                write_lock: Arc::new(Mutex::new(())),
            },
            loaded.messages,
        ))
    }

    pub fn continue_latest(cwd: &Path, enabled: bool) -> Result<(Self, Vec<Message>)> {
        Self::continue_latest_from_directory(cwd, enabled, project_directory(cwd)?)
    }

    pub fn continue_latest_in(
        cwd: &Path,
        state_root: &SessionStateRoot,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        Self::continue_latest_from_directory(cwd, enabled, state_root.project_directory(cwd)?)
    }

    fn continue_latest_from_directory(
        cwd: &Path,
        enabled: bool,
        directory: PathBuf,
    ) -> Result<(Self, Vec<Message>)> {
        let latest = fs::read_dir(&directory)?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("jsonl"))
            .filter_map(|entry| Some((entry.metadata().ok()?.modified().ok()?, entry.path())))
            .max_by_key(|(modified, _)| *modified)
            .map(|(_, path)| path)
            .context("当前目录没有可继续的会话")?;
        let id = latest
            .file_stem()
            .and_then(|s| s.to_str())
            .context("会话文件名无效")?
            .parse()?;
        let loaded = load_transcript(&latest)?;
        Ok((
            Self {
                id,
                cwd: cwd.to_owned(),
                file: latest,
                enabled,
                workspace: Arc::new(Mutex::new(loaded.workspace)),
                current_cwd: Arc::new(Mutex::new(loaded.current_cwd)),
                write_lock: Arc::new(Mutex::new(())),
            },
            loaded.messages,
        ))
    }

    /// Creates a new session from a bounded prefix of an existing session.
    /// The source transcript is never modified and the fork receives a fresh id.
    pub fn fork(
        cwd: &Path,
        source_id: Uuid,
        message_count: Option<usize>,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        Self::fork_from_directory(
            cwd,
            source_id,
            message_count,
            enabled,
            project_directory(cwd)?,
        )
    }

    pub fn fork_in(
        cwd: &Path,
        source_id: Uuid,
        message_count: Option<usize>,
        state_root: &SessionStateRoot,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        Self::fork_from_directory(
            cwd,
            source_id,
            message_count,
            enabled,
            state_root.project_directory(cwd)?,
        )
    }

    fn fork_from_directory(
        cwd: &Path,
        source_id: Uuid,
        message_count: Option<usize>,
        enabled: bool,
        directory: PathBuf,
    ) -> Result<(Self, Vec<Message>)> {
        let source = directory.join(format!("{source_id}.jsonl"));
        if !source.exists() {
            bail!("当前目录下没有会话 {source_id}")
        }
        let source_store = Self {
            id: source_id,
            cwd: cwd.to_owned(),
            file: source,
            enabled: true,
            workspace: Arc::new(Mutex::new(SessionWorkspaceState::launch())),
            current_cwd: Arc::new(Mutex::new(None)),
            write_lock: Arc::new(Mutex::new(())),
        };
        source_store.fork_from(message_count, enabled)
    }

    /// Forks this store without requiring another project-directory lookup.
    pub fn fork_from(
        &self,
        message_count: Option<usize>,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        let LoadedTranscript {
            mut messages,
            mut message_workspaces,
            mut message_current_cwds,
            boundary_workspace,
            boundary_current_cwd,
            workspace,
            current_cwd,
        } = load_transcript(&self.file)?;
        if let Some(count) = message_count {
            if count > messages.len() {
                bail!("fork 消息位置 {count} 超过会话长度 {}", messages.len())
            }
            messages.truncate(count);
            message_workspaces.truncate(count);
            message_current_cwds.truncate(count);
        }
        let (workspace, current_cwd) = match message_count {
            None => (workspace, current_cwd),
            Some(0) => (boundary_workspace, boundary_current_cwd),
            Some(count) => (
                message_workspaces[count - 1].clone(),
                message_current_cwds[count - 1].clone(),
            ),
        };
        let id = Uuid::new_v4();
        let file = if enabled {
            self.file
                .parent()
                .context("源 transcript 缺少父目录")?
                .join(format!("{id}.jsonl"))
        } else {
            PathBuf::new()
        };
        let destination = Self {
            id,
            cwd: self.cwd.clone(),
            file,
            enabled,
            workspace: Arc::new(Mutex::new(workspace)),
            current_cwd: Arc::new(Mutex::new(current_cwd)),
            write_lock: Arc::new(Mutex::new(())),
        };
        if enabled {
            destination.write_history_with_workspaces(
                &messages,
                &message_workspaces,
                &message_current_cwds,
            )?;
        }
        Ok((destination, messages))
    }

    /// Loads the currently effective history, after compact boundaries.
    pub fn load_history(&self) -> Result<Vec<Message>> {
        if self.file.as_os_str().is_empty() || !self.file.exists() {
            return Ok(Vec::new());
        }
        load_messages(&self.file)
    }

    /// Records a trusted workspace transition without persisting an absolute path.
    /// Callers must only invoke this after independently authorizing the target.
    pub fn record_workspace_transition(&self, cwd: &Path, root: &Path) -> Result<()> {
        let launch = fs::canonicalize(&self.cwd)
            .with_context(|| format!("无法解析 session launch cwd: {}", self.cwd.display()))?;
        let cwd = fs::canonicalize(cwd)
            .with_context(|| format!("无法解析 session transition cwd: {}", cwd.display()))?;
        let root = fs::canonicalize(root)
            .with_context(|| format!("无法解析 session transition root: {}", root.display()))?;
        let next = if cwd == launch {
            SessionWorkspaceState::launch()
        } else {
            if !cwd.is_dir() || !root.is_dir() || !cwd.starts_with(&root) {
                bail!("session transition cwd 必须位于有效 workspace root 内")
            }
            let relative = cwd
                .strip_prefix(&root)
                .context("session transition cwd 无法相对 workspace root 表示")?;
            let relative = if relative.as_os_str().is_empty() {
                PathBuf::from(".")
            } else {
                relative.to_owned()
            };
            validate_record_cwd(&relative)?;
            SessionWorkspaceState {
                workspace_key: Some(workspace_key(&root)),
                cwd: relative,
            }
        };

        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.enabled {
            let record = Record::from_state(self.id, &next, None, false, None);
            let mut file = open_private_transcript(&self.file)?;
            let mut size = file.metadata()?.len();
            append_record(&mut file, &record, &mut size)?;
            file.flush()?;
        }
        *self
            .workspace
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
        *self
            .current_cwd
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        Ok(())
    }

    /// Records only the foreground shell cwd. The primary workspace identity
    /// remains unchanged so an additional trusted root cannot be mistaken for
    /// a Git worktree during `--resume`.
    pub fn record_current_cwd_transition(&self, cwd: &Path, root: &Path) -> Result<()> {
        let launch = fs::canonicalize(&self.cwd)
            .with_context(|| format!("无法解析 session launch cwd: {}", self.cwd.display()))?;
        let cwd = fs::canonicalize(cwd)
            .with_context(|| format!("无法解析 session current cwd: {}", cwd.display()))?;
        let root = fs::canonicalize(root)
            .with_context(|| format!("无法解析 session current root: {}", root.display()))?;
        if !cwd.is_dir() || !root.is_dir() || !cwd.starts_with(&root) {
            bail!("session current cwd 必须位于有效 trusted root 内")
        }
        let relative = cwd
            .strip_prefix(&root)
            .context("session current cwd 无法相对 trusted root 表示")?;
        let relative = if relative.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            relative.to_owned()
        };
        validate_record_cwd(&relative)?;

        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let workspace = self.workspace_state();
        let root_key = workspace_key(&root);
        let matches_primary = match workspace.workspace_key.as_deref() {
            Some(primary_key) => primary_key == root_key && workspace.cwd == relative,
            None => cwd == launch,
        };
        let next = (!matches_primary).then_some(SessionCurrentCwdState {
            root_key,
            cwd: relative,
        });
        if self.enabled {
            let record = Record::from_state(self.id, &workspace, next.as_ref(), false, None);
            let mut file = open_private_transcript(&self.file)?;
            let mut size = file.metadata()?.len();
            append_record(&mut file, &record, &mut size)?;
            file.flush()?;
        }
        *self
            .current_cwd
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next;
        Ok(())
    }

    /// Atomically truncates the persisted history to `message_count` messages.
    /// This is the storage primitive used by rewind/resume-at-message flows.
    pub fn truncate_history(&self, message_count: usize) -> Result<Vec<Message>> {
        let mut messages = self.load_history()?;
        if message_count > messages.len() {
            bail!("截断位置 {message_count} 超过会话长度 {}", messages.len())
        }
        messages.truncate(message_count);
        if self.enabled {
            self.write_history(&messages)?;
        }
        Ok(messages)
    }

    pub fn append(&self, messages: &[Message]) -> Result<()> {
        if !self.enabled || messages.is_empty() {
            return Ok(());
        }
        if messages.len() > MAX_TRANSCRIPT_RECORDS {
            bail!("追加记录超过 {MAX_TRANSCRIPT_RECORDS} 条限制")
        }
        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let workspace = self.workspace_state();
        let current_cwd = self.current_cwd_state();
        let mut file = open_private_transcript(&self.file)?;
        let mut size = file.metadata()?.len();
        for message in messages {
            let record = Record::from_state(
                self.id,
                &workspace,
                current_cwd.as_ref(),
                false,
                Some(sanitize_for_storage(message, &self.cwd)),
            );
            append_record(&mut file, &record, &mut size)?;
        }
        file.flush()?;
        Ok(())
    }

    pub fn replace_history(&self, messages: &[Message]) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        self.write_history(messages)
    }

    fn write_history(&self, messages: &[Message]) -> Result<()> {
        if messages.len() > MAX_TRANSCRIPT_RECORDS {
            bail!("transcript 超过 {MAX_TRANSCRIPT_RECORDS} 条记录限制")
        }
        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let workspace = self.workspace_state();
        let current_cwd = self.current_cwd_state();
        let mut contents = Vec::new();
        if messages.is_empty() {
            let record = Record::from_state(self.id, &workspace, current_cwd.as_ref(), true, None);
            append_record_bytes(&mut contents, &record)?;
        }
        for (index, message) in messages.iter().enumerate() {
            let record = Record::from_state(
                self.id,
                &workspace,
                current_cwd.as_ref(),
                index == 0,
                Some(sanitize_for_storage(message, &self.cwd)),
            );
            append_record_bytes(&mut contents, &record)?;
        }
        replace_private_transcript(&self.file, &contents)
    }

    fn write_history_with_workspaces(
        &self,
        messages: &[Message],
        workspaces: &[SessionWorkspaceState],
        current_cwds: &[Option<SessionCurrentCwdState>],
    ) -> Result<()> {
        if messages.len() != workspaces.len()
            || messages.len() != current_cwds.len()
            || messages.len() > MAX_TRANSCRIPT_RECORDS
        {
            bail!("fork transcript 消息与 workspace 状态不一致或超过限制")
        }
        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current_workspace = self.workspace_state();
        let current_cwd = self.current_cwd_state();
        let mut contents = Vec::new();
        if messages.is_empty() {
            append_record_bytes(
                &mut contents,
                &Record::from_state(
                    self.id,
                    &current_workspace,
                    current_cwd.as_ref(),
                    true,
                    None,
                ),
            )?;
        }
        for (index, ((message, workspace), cwd)) in messages
            .iter()
            .zip(workspaces)
            .zip(current_cwds)
            .enumerate()
        {
            append_record_bytes(
                &mut contents,
                &Record::from_state(
                    self.id,
                    workspace,
                    cwd.as_ref(),
                    index == 0,
                    Some(sanitize_for_storage(message, &self.cwd)),
                ),
            )?;
        }
        if messages.last().is_some()
            && (workspaces.last() != Some(&current_workspace)
                || current_cwds.last() != Some(&current_cwd))
        {
            append_record_bytes(
                &mut contents,
                &Record::from_state(
                    self.id,
                    &current_workspace,
                    current_cwd.as_ref(),
                    false,
                    None,
                ),
            )?;
        }
        replace_private_transcript(&self.file, &contents)
    }

    pub fn clear_history(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let workspace = self.workspace_state();
        let current_cwd = self.current_cwd_state();
        let record = Record::from_state(self.id, &workspace, current_cwd.as_ref(), true, None);
        let mut contents = Vec::new();
        append_record_bytes(&mut contents, &record)?;
        replace_private_transcript(&self.file, &contents)
    }
}

fn project_directory(cwd: &Path) -> Result<PathBuf> {
    let home = dirs::home_dir().context("无法确定用户主目录")?;
    let key = workspace_key(cwd);
    let harness = home.join(".open-agent-harness");
    ensure_private_component(&harness)?;
    let projects = harness.join("projects");
    ensure_private_component(&projects)?;
    let directory = projects.join(key);
    ensure_private_component(&directory)?;
    Ok(directory)
}

fn ensure_private_component(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("拒绝使用 symlink 私有目录: {}", path.display())
        }
        Ok(metadata) if !metadata.is_dir() => {
            bail!("私有路径不是目录: {}", path.display())
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| format!("无法创建私有目录 {}", path.display()))?;
        }
        Err(error) => return Err(error.into()),
    }
    ensure_private_directory(path)
}

fn ensure_private_bounded_child(root: &Path, parent: &Path, name: &str) -> Result<PathBuf> {
    let component = Path::new(name);
    if name.is_empty()
        || component.components().count() != 1
        || !matches!(
            component.components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        bail!("session state 私有目录名称无效")
    }
    if !parent.starts_with(root) {
        bail!("session state 私有目录父路径越过存储根")
    }
    let child = parent.join(component);
    ensure_private_component(&child)?;
    let child = fs::canonicalize(&child)
        .with_context(|| format!("无法解析 session state 私有目录: {}", child.display()))?;
    if !child.starts_with(root) {
        bail!("session state 私有目录越过存储根")
    }
    Ok(child)
}

fn open_private_transcript(path: &Path) -> Result<fs::File> {
    if fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        bail!("拒绝追加 symlink transcript: {}", path.display())
    }
    if let Some(parent) = path.parent() {
        ensure_private_directory(parent)?;
    }
    let mut options = fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("无法打开 transcript {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

fn load_messages(file: &Path) -> Result<Vec<Message>> {
    Ok(load_transcript(file)?.messages)
}

fn load_transcript(file: &Path) -> Result<LoadedTranscript> {
    if fs::symlink_metadata(file)?.file_type().is_symlink() {
        bail!("拒绝从 symlink 恢复 transcript: {}", file.display())
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let input = options
        .open(file)
        .with_context(|| format!("无法打开 transcript {}", file.display()))?;
    let size = input.metadata()?.len();
    if size > MAX_TRANSCRIPT_BYTES {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    let mut bytes = Vec::new();
    input
        .take(MAX_TRANSCRIPT_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_TRANSCRIPT_BYTES as usize {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    // append_record always commits a newline-terminated JSONL record. A final
    // non-terminated fragment can only be an interrupted append; retain the
    // last durable record boundary, but continue to reject corruption in any
    // newline-terminated (middle or final) record.
    let repaired_len = if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        let complete_len = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        bytes.truncate(complete_len);
        Some(complete_len as u64)
    } else {
        None
    };
    let mut expected_session_id = file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.parse::<Uuid>().ok());
    let mut workspace_cwds = BTreeMap::<Option<String>, PathBuf>::new();
    let mut current_workspace = SessionWorkspaceState::launch();
    let mut current_cwd = None;
    let mut boundary_workspace = SessionWorkspaceState::launch();
    let mut boundary_current_cwd = None;
    let mut message_workspaces = Vec::new();
    let mut message_current_cwds = Vec::new();
    let reader = BufReader::new(bytes.as_slice());
    let messages =
        reader
            .lines()
            .enumerate()
            .try_fold(Vec::new(), |mut messages, (index, line)| {
                if index >= MAX_TRANSCRIPT_RECORDS {
                    bail!("transcript 超过 {MAX_TRANSCRIPT_RECORDS} 条记录限制")
                }
                let record: Record = serde_json::from_str(&line?)
                    .with_context(|| format!("transcript 第 {} 行损坏", index + 1))?;
                match expected_session_id {
                    Some(expected) if record.session_id != expected => {
                        bail!("transcript 第 {} 行 session id 不匹配", index + 1)
                    }
                    None => expected_session_id = Some(record.session_id),
                    _ => {}
                }
                validate_record_cwd(&record.cwd)
                    .with_context(|| format!("transcript 第 {} 行 cwd 无效", index + 1))?;
                validate_workspace_key(record.workspace_key.as_deref()).with_context(|| {
                    format!("transcript 第 {} 行 workspace key 无效", index + 1)
                })?;
                if record.workspace_key.is_none() && record.cwd != Path::new(".") {
                    bail!("transcript 第 {} 行 launch cwd 必须为 .", index + 1)
                }
                let record_current_cwd = match (&record.current_root_key, &record.current_cwd) {
                    (None, None) => None,
                    (Some(root_key), Some(cwd)) => {
                        validate_workspace_key(Some(root_key)).with_context(|| {
                            format!("transcript 第 {} 行 current root key 无效", index + 1)
                        })?;
                        validate_record_cwd(cwd).with_context(|| {
                            format!("transcript 第 {} 行 current cwd 无效", index + 1)
                        })?;
                        Some(SessionCurrentCwdState {
                            root_key: root_key.clone(),
                            cwd: cwd.clone(),
                        })
                    }
                    _ => bail!(
                        "transcript 第 {} 行 current cwd 与 root key 必须同时存在",
                        index + 1
                    ),
                };
                match workspace_cwds.get(&record.workspace_key) {
                    Some(expected) if expected != &record.cwd => {
                        bail!("transcript 第 {} 行同一 workspace 的 cwd 不匹配", index + 1)
                    }
                    None => {
                        workspace_cwds.insert(record.workspace_key.clone(), record.cwd.clone());
                    }
                    _ => {}
                }
                current_workspace = SessionWorkspaceState {
                    workspace_key: record.workspace_key.clone(),
                    cwd: record.cwd.clone(),
                };
                current_cwd = record_current_cwd;
                if record.compact_boundary {
                    messages.clear();
                    message_workspaces.clear();
                    message_current_cwds.clear();
                    boundary_workspace = current_workspace.clone();
                    boundary_current_cwd = current_cwd.clone();
                }
                if let Some(message) = record.message {
                    messages.push(message);
                    message_workspaces.push(current_workspace.clone());
                    message_current_cwds.push(current_cwd.clone());
                }
                Ok(messages)
            })?;
    let loaded = LoadedTranscript {
        messages,
        message_workspaces,
        message_current_cwds,
        boundary_workspace,
        boundary_current_cwd,
        workspace: current_workspace,
        current_cwd,
    };
    if let Some(length) = repaired_len {
        truncate_private_transcript(file, length)?;
    }
    Ok(loaded)
}

fn truncate_private_transcript(path: &Path, length: u64) -> Result<()> {
    if length > MAX_TRANSCRIPT_BYTES {
        bail!("transcript 修复边界超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        bail!("拒绝修复 symlink transcript: {}", path.display())
    }
    let mut options = fs::OpenOptions::new();
    options.write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("无法打开待修复 transcript {}", path.display()))?;
    if !file.metadata()?.is_file() {
        bail!("待修复 transcript 不是普通文件")
    }
    file.set_len(length)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn validate_workspace_key(key: Option<&str>) -> Result<()> {
    if let Some(key) = key {
        if key.len() != 32 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("workspace key 必须是 32 位十六进制标识")
        }
    }
    Ok(())
}

fn validate_record_cwd(cwd: &Path) -> Result<()> {
    let text = cwd.to_string_lossy();
    let windows_absolute = text.starts_with("\\\\")
        || text
            .as_bytes()
            .get(1..3)
            .is_some_and(|bytes| bytes[0] == b':' && matches!(bytes[1], b'/' | b'\\'));
    if cwd.as_os_str().is_empty()
        || cwd.is_absolute()
        || windows_absolute
        || cwd.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        bail!("transcript cwd 必须是无 parent 逃逸的相对路径")
    }
    Ok(())
}

fn append_record(file: &mut fs::File, record: &Record, size: &mut u64) -> Result<()> {
    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    let next = size
        .checked_add(line.len() as u64)
        .context("transcript 大小溢出")?;
    if next > MAX_TRANSCRIPT_BYTES {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    file.write_all(&line)?;
    *size = next;
    Ok(())
}

fn append_record_bytes(contents: &mut Vec<u8>, record: &Record) -> Result<()> {
    serde_json::to_writer(&mut *contents, record)?;
    contents.push(b'\n');
    if contents.len() > MAX_TRANSCRIPT_BYTES as usize {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    Ok(())
}

fn replace_private_transcript(path: &Path, contents: &[u8]) -> Result<()> {
    if fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        bail!("拒绝替换 symlink transcript: {}", path.display())
    }
    let parent = path.parent().context("transcript 路径缺少父目录")?;
    ensure_private_directory(parent)?;
    let temp = parent.join(format!(".open-agent-harness-{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(contents)?;
        file.flush()?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.with_context(|| format!("无法原子替换 transcript {}", path.display()))
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn sanitize_for_storage(message: &Message, cwd: &Path) -> Message {
    let mut sanitized = message.clone();
    if let Some(blocks) = sanitized.content.as_array_mut() {
        blocks.retain(|block| block.get("type").and_then(Value::as_str) != Some("provider_state"));
    }
    sanitize_value(&mut sanitized.content, None, cwd);
    sanitized
}

/// Redacts credentials, endpoint query secrets, and host absolute paths before
/// a value crosses a machine-facing transport boundary.
pub fn sanitize_transport_value(value: &Value, cwd: &Path) -> Value {
    let mut sanitized = value.clone();
    sanitize_value(&mut sanitized, None, cwd);
    sanitized
}

pub fn sanitize_transport_text(text: &str, cwd: &Path) -> String {
    sanitize_text(text, None, cwd)
}

fn sanitize_value(value: &mut Value, key: Option<&str>, cwd: &Path) {
    match value {
        Value::Object(object) => {
            for (child_key, child) in object {
                if is_secret_key(child_key) {
                    *child = Value::String(REDACTED_SECRET.into());
                } else {
                    sanitize_value(child, Some(child_key), cwd);
                }
            }
        }
        Value::Array(values) => {
            for child in values {
                sanitize_value(child, key, cwd);
            }
        }
        Value::String(text) => *text = sanitize_text(text, key, cwd),
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn normalized_key(key: &str) -> String {
    key.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_secret_key(key: &str) -> bool {
    matches!(
        normalized_key(key).as_str(),
        "apikey"
            | "authorization"
            | "proxyauthorization"
            | "password"
            | "passwd"
            | "secret"
            | "token"
            | "accesstoken"
            | "refreshtoken"
            | "sessiontoken"
            | "cookie"
            | "setcookie"
            | "credential"
            | "credentials"
            | "harnessapikey"
    )
}

fn is_path_key(key: Option<&str>) -> bool {
    key.is_some_and(|key| {
        matches!(
            normalized_key(key).as_str(),
            "path"
                | "filepath"
                | "notebookpath"
                | "directory"
                | "dir"
                | "cwd"
                | "workdir"
                | "workspace"
                | "workspaceroot"
        )
    })
}

fn sanitize_text(text: &str, key: Option<&str>, cwd: &Path) -> String {
    if is_path_key(key) && looks_absolute_path(text) {
        return sanitize_absolute_path(text, cwd);
    }

    let mut sanitized = sanitize_urls(text);
    sanitized = api_key_assignment_regex()
        .replace_all(&sanitized, |captures: &Captures<'_>| {
            format!("{}{}", &captures[1], REDACTED_SECRET)
        })
        .into_owned();
    sanitized = authorization_regex()
        .replace_all(&sanitized, |captures: &Captures<'_>| {
            format!("{}{}", &captures[1], REDACTED_SECRET)
        })
        .into_owned();

    if let Some(cwd) = cwd.to_str() {
        sanitized = sanitized.replace(cwd, ".");
    }
    if let Some(home) = dirs::home_dir() {
        if let Some(home) = home.to_str() {
            sanitized = sanitized.replace(home, "~");
        }
    }
    home_path_regex()
        .replace_all(&sanitized, REDACTED_PATH)
        .into_owned()
}

fn looks_absolute_path(value: &str) -> bool {
    Path::new(value).is_absolute()
        || value.starts_with("\\\\")
        || value
            .as_bytes()
            .get(1..3)
            .is_some_and(|bytes| bytes[0] == b':' && matches!(bytes[1], b'/' | b'\\'))
}

fn sanitize_absolute_path(value: &str, cwd: &Path) -> String {
    let path = Path::new(value);
    if let Ok(relative) = path.strip_prefix(cwd) {
        if relative.as_os_str().is_empty() {
            ".".into()
        } else {
            format!("./{}", relative.display())
        }
    } else if let Some(home) = dirs::home_dir() {
        if let Ok(relative) = path.strip_prefix(home) {
            format!("~/{}", relative.display())
        } else {
            REDACTED_PATH.into()
        }
    } else {
        REDACTED_PATH.into()
    }
}

fn sanitize_urls(text: &str) -> String {
    url_regex()
        .replace_all(text, |captures: &Captures<'_>| {
            let candidate = &captures[0];
            let Ok(mut url) = Url::parse(candidate) else {
                return candidate.to_owned();
            };
            let pairs = url
                .query_pairs()
                .map(|(key, value)| {
                    let value = if is_secret_key(&key) {
                        REDACTED_SECRET.to_owned()
                    } else {
                        value.into_owned()
                    };
                    (key.into_owned(), value)
                })
                .collect::<Vec<_>>();
            if !pairs.is_empty() {
                url.query_pairs_mut().clear().extend_pairs(pairs);
            }
            url.to_string()
        })
        .into_owned()
}

fn url_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| Regex::new(r#"https?://[^\s<>\"']+"#).expect("valid URL regex"))
}

fn api_key_assignment_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r#"(?i)((?:HARNESS_API_KEY|API[_-]?KEY|ACCESS[_-]?TOKEN|REFRESH[_-]?TOKEN|PASSWORD|SECRET)\s*[:=]\s*)[^\s,;&\"']+"#)
            .expect("valid secret assignment regex")
    })
}

fn authorization_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)(Authorization\s*[:=]\s*(?:(?:Bearer|Basic)\s+)?)\S+")
            .expect("valid authorization regex")
    })
}

fn home_path_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r#"/(?:Users|home)/[^/\s\"'<>]+(?:/[^\s\"'<>]*)?"#)
            .expect("valid home path regex")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn make_private_directory(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn test_store(cwd: &Path, file: PathBuf, id: Uuid) -> SessionStore {
        SessionStore {
            id,
            cwd: cwd.to_owned(),
            file,
            enabled: true,
            workspace: Arc::new(Mutex::new(SessionWorkspaceState::launch())),
            current_cwd: Arc::new(Mutex::new(None)),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    #[test]
    fn disabled_store_does_not_allocate_a_transcript_path() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStore::create(temp.path(), false).unwrap();
        assert!(store.file.as_os_str().is_empty());
        store
            .append(&[Message::user_text("not persisted")])
            .unwrap();
        assert!(store.file.as_os_str().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn explicit_state_root_keeps_the_full_session_lifecycle_in_one_boundary() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        make_private_directory(storage.path());
        let state_root = SessionStateRoot::open(storage.path()).unwrap();
        let message = Message::user_text("isolated session");

        let store = SessionStore::create_in(workspace.path(), &state_root, true).unwrap();
        store.append(std::slice::from_ref(&message)).unwrap();
        let canonical_storage = fs::canonicalize(storage.path()).unwrap();
        assert!(store.file.starts_with(canonical_storage.join("projects")));

        let (_, resumed) =
            SessionStore::resume_in(workspace.path(), store.id, &state_root, true).unwrap();
        assert_eq!(resumed, vec![message.clone()]);
        let (continued, continued_messages) =
            SessionStore::continue_latest_in(workspace.path(), &state_root, true).unwrap();
        assert_eq!(continued.id, store.id);
        assert_eq!(continued_messages, vec![message.clone()]);

        let (fork, forked) =
            SessionStore::fork_in(workspace.path(), store.id, None, &state_root, true).unwrap();
        assert_ne!(fork.id, store.id);
        assert!(fork.file.starts_with(canonical_storage.join("projects")));
        assert_eq!(forked, vec![message]);
        assert!(
            state_root
                .file_history_root()
                .unwrap()
                .starts_with(&canonical_storage)
        );
    }

    #[test]
    fn explicit_state_root_rejects_ambiguous_or_unsafe_roots() {
        let storage = tempfile::tempdir().unwrap();
        assert!(SessionStateRoot::open(Path::new("relative-state-root")).is_err());
        assert!(SessionStateRoot::open(&storage.path().join("missing")).is_err());

        let file = storage.path().join("file");
        fs::write(&file, b"not a directory").unwrap();
        assert!(SessionStateRoot::open(&file).is_err());

        #[cfg(not(unix))]
        assert!(
            SessionStateRoot::open(storage.path())
                .unwrap_err()
                .to_string()
                .contains("仅支持")
        );
    }

    #[cfg(unix)]
    #[test]
    fn explicit_state_root_rejects_symlinks_and_keeps_private_modes() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let linked_root = storage.path().join("linked-root");
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), &linked_root).unwrap();
        assert!(SessionStateRoot::open(&linked_root).is_err());

        fs::set_permissions(storage.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(SessionStateRoot::open(storage.path()).is_err());
        fs::set_permissions(storage.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let state_root = SessionStateRoot::open(storage.path()).unwrap();
        assert_eq!(
            fs::metadata(storage.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let store = SessionStore::create_in(workspace.path(), &state_root, true).unwrap();
        store.append(&[Message::user_text("private")]).unwrap();
        assert_eq!(
            fs::metadata(&store.file).unwrap().permissions().mode() & 0o777,
            0o600
        );

        let escaped_storage = tempfile::tempdir().unwrap();
        make_private_directory(escaped_storage.path());
        let escaped_root = SessionStateRoot::open(escaped_storage.path()).unwrap();
        symlink(outside.path(), escaped_storage.path().join("projects")).unwrap();
        assert!(SessionStore::create_in(workspace.path(), &escaped_root, true).is_err());
    }

    #[test]
    fn opaque_provider_state_is_never_persisted() {
        let message = Message::assistant(vec![serde_json::json!({
            "type":"provider_state",
            "format":"responses",
            "item":{"type":"reasoning","encrypted_content":"opaque-secret-state"}
        })]);
        let sanitized = sanitize_for_storage(&message, Path::new("/workspace"));
        assert_eq!(sanitized.content, serde_json::json!([]));
        assert!(
            !serde_json::to_string(&sanitized)
                .unwrap()
                .contains("opaque-secret-state")
        );
    }

    #[test]
    fn compact_boundary_replaces_prior_history_on_resume() {
        let temp = tempfile::tempdir().unwrap();
        let store = test_store(
            temp.path(),
            temp.path().join("session.jsonl"),
            Uuid::new_v4(),
        );
        store
            .append(&[
                Message::user_text("old user"),
                Message::assistant(vec![serde_json::json!({"type":"text","text":"old reply"})]),
            ])
            .unwrap();
        store
            .replace_history(&[Message::user_text("compact summary")])
            .unwrap();
        assert!(
            !fs::read_to_string(&store.file)
                .unwrap()
                .contains("old user")
        );
        let loaded = load_messages(&store.file).unwrap();
        assert_eq!(loaded, vec![Message::user_text("compact summary")]);
    }

    #[test]
    #[cfg(unix)]
    fn session_listing_is_workspace_scoped_bounded_and_read_only() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        make_private_directory(storage.path());
        let root = SessionStateRoot::open(storage.path()).unwrap();
        let first = SessionStore::create_in(workspace.path(), &root, true).unwrap();
        first.append(&[Message::user_text("first")]).unwrap();
        let second = SessionStore::create_in(workspace.path(), &root, true).unwrap();
        second.append(&[Message::user_text("second")]).unwrap();

        let listed = SessionStore::list_in(workspace.path(), &root, 20).unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().any(|session| session.id == first.id));
        assert!(listed.iter().any(|session| session.id == second.id));
        assert!(listed.iter().all(|session| session.bytes > 0));
        assert!(SessionStore::list_in(workspace.path(), &root, 0).is_err());
        assert!(SessionStore::list_in(workspace.path(), &root, 101).is_err());
    }

    #[test]
    fn clear_boundary_removes_all_prior_history() {
        let temp = tempfile::tempdir().unwrap();
        let store = test_store(
            temp.path(),
            temp.path().join("session.jsonl"),
            Uuid::new_v4(),
        );
        let sentinel = "clear-history-secret-sentinel";
        store.append(&[Message::user_text(sentinel)]).unwrap();
        store.clear_history().unwrap();
        assert!(!fs::read_to_string(&store.file).unwrap().contains(sentinel));
        assert!(load_messages(&store.file).unwrap().is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&store.file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn transcript_preserves_tool_data_while_redacting_secrets_and_paths() {
        let temp = tempfile::tempdir().unwrap();
        let store = test_store(
            temp.path(),
            temp.path().join("session.jsonl"),
            Uuid::new_v4(),
        );
        let ordinary = "ordinary-tool-result";
        let secret = "endpoint-secret-value";
        let absolute = temp.path().join("source.txt");
        store
            .append(&[
                Message::assistant(vec![serde_json::json!({
                    "type":"tool_use", "id":"read-1", "name":"Read",
                    "input":{
                        "file_path":absolute,
                        "url":format!("https://search.invalid/search?q=rust&api_key={secret}"),
                        "authorization":format!("Bearer {secret}")
                    }
                })]),
                Message::tool_results(vec![serde_json::json!({
                    "type":"tool_result", "tool_use_id":"read-1",
                    "content":format!("{ordinary}\nHARNESS_API_KEY={secret}\n{}", temp.path().display())
                })]),
            ])
            .unwrap();
        let transcript = fs::read_to_string(&store.file).unwrap();
        assert!(transcript.contains(ordinary));
        assert!(!transcript.contains(secret));
        assert!(!transcript.contains(temp.path().to_string_lossy().as_ref()));
        assert!(!transcript.contains("\"cwd\":\"/"));
        let loaded = load_messages(&store.file).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].content[0]["input"]["file_path"], "./source.txt");
        assert_eq!(
            loaded[0].content[0]["input"]["authorization"],
            REDACTED_SECRET
        );
        assert!(
            loaded[1].content[0]["content"]
                .as_str()
                .unwrap()
                .contains(ordinary)
        );
    }

    #[test]
    fn fork_and_truncate_preserve_exact_message_prefixes() {
        let temp = tempfile::tempdir().unwrap();
        let store = test_store(
            temp.path(),
            temp.path().join("source.jsonl"),
            Uuid::new_v4(),
        );
        let messages = vec![
            Message::user_text("one"),
            Message::assistant(vec![serde_json::json!({"type":"text","text":"two"})]),
            Message::user_text("three"),
        ];
        store.append(&messages).unwrap();
        let (fork, forked) = store.fork_from(Some(2), true).unwrap();
        assert_ne!(fork.id, store.id);
        assert_eq!(forked, messages[..2]);
        assert_eq!(fork.load_history().unwrap(), messages[..2]);
        assert_eq!(store.load_history().unwrap(), messages);

        assert_eq!(store.truncate_history(1).unwrap(), messages[..1]);
        assert_eq!(store.load_history().unwrap(), messages[..1]);
        assert!(store.truncate_history(2).is_err());
    }

    #[test]
    fn corrupt_and_symlink_transcripts_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("broken.jsonl");
        fs::write(&file, "not-json\n").unwrap();
        assert!(
            load_messages(&file)
                .unwrap_err()
                .to_string()
                .contains("损坏")
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let target = temp.path().join("target.jsonl");
            fs::write(&target, "").unwrap();
            let link = temp.path().join("link.jsonl");
            symlink(&target, &link).unwrap();
            assert!(load_messages(&link).is_err());
        }
    }

    #[test]
    fn transcript_repairs_only_a_nonterminated_torn_tail() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(temp.path(), temp.path().join(format!("{id}.jsonl")), id);
        let expected = vec![Message::user_text("durable")];
        store.append(&expected).unwrap();
        let durable_len = fs::metadata(&store.file).unwrap().len();
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&store.file)
            .unwrap();
        file.write_all(br#"{"session_id":"#).unwrap();
        file.sync_all().unwrap();

        assert_eq!(load_messages(&store.file).unwrap(), expected);
        assert_eq!(fs::metadata(&store.file).unwrap().len(), durable_len);
        assert!(fs::read(&store.file).unwrap().ends_with(b"\n"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(&store.file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn transcript_still_rejects_newline_terminated_middle_corruption() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(temp.path(), temp.path().join(format!("{id}.jsonl")), id);
        store.append(&[Message::user_text("durable")]).unwrap();
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&store.file)
            .unwrap();
        file.write_all(b"not-json\n{\"partial\"").unwrap();
        file.sync_all().unwrap();
        let before = fs::read(&store.file).unwrap();

        assert!(load_messages(&store.file).is_err());
        assert_eq!(fs::read(&store.file).unwrap(), before);
    }

    #[test]
    fn transcript_rejects_mixed_or_misnamed_session_ids() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(temp.path(), temp.path().join(format!("{id}.jsonl")), id);
        store
            .append(&[Message::user_text("one"), Message::user_text("two")])
            .unwrap();
        let transcript = fs::read_to_string(&store.file).unwrap();
        let mut records = transcript
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        records[1]["session_id"] = Value::String(Uuid::new_v4().to_string());
        let corrupt = records
            .into_iter()
            .map(|record| serde_json::to_string(&record).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(&store.file, corrupt).unwrap();
        assert!(load_messages(&store.file).is_err());
    }

    #[test]
    fn transcript_rejects_unsafe_or_inconsistent_record_cwd() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(temp.path(), temp.path().join(format!("{id}.jsonl")), id);
        store
            .append(&[Message::user_text("one"), Message::user_text("two")])
            .unwrap();
        let original = fs::read_to_string(&store.file).unwrap();
        let records = original
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();

        for unsafe_cwd in ["/private/outside", "../escape", r"C:\outside"] {
            let mut corrupt = records.clone();
            corrupt[0]["cwd"] = Value::String(unsafe_cwd.to_owned());
            let encoded = corrupt
                .into_iter()
                .map(|record| serde_json::to_string(&record).unwrap())
                .collect::<Vec<_>>()
                .join("\n")
                + "\n";
            fs::write(&store.file, encoded).unwrap();
            assert!(load_messages(&store.file).is_err(), "accepted {unsafe_cwd}");
        }

        let mut inconsistent = records;
        let key = "a".repeat(32);
        inconsistent[0]["workspace_key"] = Value::String(key.clone());
        inconsistent[1]["workspace_key"] = Value::String(key);
        inconsistent[0]["cwd"] = Value::String("safe-a".to_owned());
        inconsistent[1]["cwd"] = Value::String("safe-b".to_owned());
        let encoded = inconsistent
            .into_iter()
            .map(|record| serde_json::to_string(&record).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(&store.file, encoded).unwrap();
        assert!(load_messages(&store.file).is_err());
    }

    #[test]
    fn transcript_rejects_tampered_current_cwd_state() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(temp.path(), temp.path().join(format!("{id}.jsonl")), id);
        store.append(&[Message::user_text("one")]).unwrap();
        let original = fs::read_to_string(&store.file).unwrap();
        let record = serde_json::from_str::<Value>(original.lines().next().unwrap()).unwrap();

        let mut missing_pair = record.clone();
        missing_pair["current_root_key"] = Value::String("a".repeat(32));
        fs::write(
            &store.file,
            format!("{}\n", serde_json::to_string(&missing_pair).unwrap()),
        )
        .unwrap();
        assert!(load_transcript(&store.file).is_err());

        let mut escaping = record.clone();
        escaping["current_root_key"] = Value::String("a".repeat(32));
        escaping["current_cwd"] = Value::String("../outside".to_owned());
        fs::write(
            &store.file,
            format!("{}\n", serde_json::to_string(&escaping).unwrap()),
        )
        .unwrap();
        assert!(load_transcript(&store.file).is_err());

        let mut invalid_key = record;
        invalid_key["current_root_key"] = Value::String("not-a-root-key".to_owned());
        invalid_key["current_cwd"] = Value::String("safe".to_owned());
        fs::write(
            &store.file,
            format!("{}\n", serde_json::to_string(&invalid_key).unwrap()),
        )
        .unwrap();
        assert!(load_transcript(&store.file).is_err());
    }

    #[test]
    fn current_cwd_is_separate_from_primary_workspace_and_legacy_records_still_load() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let additional = temp.path().join("additional");
        let first = additional.join("first");
        let second = additional.join("second");
        fs::create_dir_all(&launch).unwrap();
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        let id = Uuid::new_v4();
        let store = test_store(&launch, temp.path().join(format!("{id}.jsonl")), id);

        store.append(&[Message::user_text("legacy")]).unwrap();
        let legacy = fs::read_to_string(&store.file).unwrap();
        assert!(!legacy.contains("current_root_key"));
        assert!(!legacy.contains("current_cwd"));
        assert_eq!(load_transcript(&store.file).unwrap().current_cwd, None);

        store
            .record_current_cwd_transition(&first, &additional)
            .unwrap();
        store.append(&[Message::user_text("first")]).unwrap();
        store
            .record_current_cwd_transition(&second, &additional)
            .unwrap();
        store.append(&[Message::user_text("second")]).unwrap();

        let loaded = load_transcript(&store.file).unwrap();
        assert_eq!(loaded.workspace, SessionWorkspaceState::launch());
        assert_eq!(
            loaded.current_cwd,
            Some(SessionCurrentCwdState {
                root_key: workspace_key(&fs::canonicalize(&additional).unwrap()),
                cwd: PathBuf::from("second"),
            })
        );
        let encoded = fs::read_to_string(&store.file).unwrap();
        assert!(!encoded.contains(additional.to_string_lossy().as_ref()));
        assert!(!encoded.contains(launch.to_string_lossy().as_ref()));

        let (fork, _) = store.fork_from(Some(2), true).unwrap();
        assert_eq!(
            fork.current_cwd_state().unwrap().cwd,
            PathBuf::from("first")
        );
        store
            .record_current_cwd_transition(&launch, &launch)
            .unwrap();
        assert_eq!(store.current_cwd_state(), None);
    }

    #[test]
    fn workspace_transitions_are_relative_and_last_record_wins() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let worktree = temp.path().join("registered-worktree");
        let nested = worktree.join("nested");
        fs::create_dir_all(&launch).unwrap();
        fs::create_dir_all(&nested).unwrap();
        let id = Uuid::new_v4();
        let store = test_store(&launch, temp.path().join(format!("{id}.jsonl")), id);
        store.append(&[Message::user_text("launch")]).unwrap();
        store
            .record_workspace_transition(&nested, &worktree)
            .unwrap();
        store.append(&[Message::user_text("worktree")]).unwrap();

        let loaded = load_transcript(&store.file).unwrap();
        assert_eq!(loaded.messages.len(), 2);
        assert_eq!(loaded.workspace.cwd, PathBuf::from("nested"));
        assert_eq!(
            loaded.workspace.workspace_key,
            Some(workspace_key(&fs::canonicalize(&worktree).unwrap()))
        );
        let encoded = fs::read_to_string(&store.file).unwrap();
        assert!(!encoded.contains(worktree.to_string_lossy().as_ref()));
        assert!(!encoded.contains(launch.to_string_lossy().as_ref()));

        store.record_workspace_transition(&launch, &launch).unwrap();
        assert_eq!(
            load_transcript(&store.file).unwrap().workspace,
            SessionWorkspaceState::launch()
        );
        let (before_transition, _) = store.fork_from(Some(1), true).unwrap();
        assert_eq!(
            before_transition.workspace_state(),
            SessionWorkspaceState::launch()
        );
        let (inside_worktree, _) = store.fork_from(Some(2), true).unwrap();
        assert_eq!(
            inside_worktree.workspace_state().cwd,
            PathBuf::from("nested")
        );
        assert_eq!(
            load_transcript(&inside_worktree.file).unwrap().workspace,
            inside_worktree.workspace_state()
        );
        let (latest, _) = store.fork_from(None, true).unwrap();
        assert_eq!(latest.workspace_state(), SessionWorkspaceState::launch());
        assert_eq!(
            load_transcript(&latest.file).unwrap().workspace,
            SessionWorkspaceState::launch()
        );
    }

    #[test]
    fn fork_zero_preserves_the_compacted_workspace_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let launch = temp.path().join("launch");
        let worktree = temp.path().join("registered-worktree");
        fs::create_dir_all(&launch).unwrap();
        fs::create_dir_all(&worktree).unwrap();
        let id = Uuid::new_v4();
        let store = test_store(&launch, temp.path().join(format!("{id}.jsonl")), id);
        store
            .record_workspace_transition(&worktree, &worktree)
            .unwrap();
        store
            .replace_history(&[Message::user_text("compacted summary")])
            .unwrap();
        let (fork, messages) = store.fork_from(Some(0), true).unwrap();
        assert!(messages.is_empty());
        assert_eq!(
            fork.workspace_state().workspace_key,
            Some(workspace_key(&fs::canonicalize(worktree).unwrap()))
        );
    }
}

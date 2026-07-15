use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
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
const MAX_SESSION_METADATA_BYTES: u64 = 16 * 1024;
const MAX_SESSION_TITLE_BYTES: usize = 512;
const MAX_SESSION_TAG_BYTES: usize = 128;
const SESSION_COLORS: &[&str] = &[
    "red", "blue", "green", "yellow", "purple", "orange", "pink", "cyan",
];
const SESSION_METADATA_VERSION: u8 = 1;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SessionMetadata {
    version: u8,
    session_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_session_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    color: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tag: Option<String>,
}

impl SessionMetadata {
    fn new(session_id: Uuid) -> Self {
        Self {
            version: SESSION_METADATA_VERSION,
            session_id,
            title: None,
            parent_session_id: None,
            color: None,
            tag: None,
        }
    }
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
    pub title: Option<String>,
    pub parent_session_id: Option<Uuid>,
    pub preview: Option<String>,
    pub color: Option<String>,
    pub tag: Option<String>,
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
            let sidecar = read_session_metadata(&path, id).ok().flatten();
            sessions.push(SessionSummary {
                id,
                modified_ms,
                bytes: metadata.len(),
                title: sidecar.as_ref().and_then(|metadata| metadata.title.clone()),
                parent_session_id: sidecar
                    .as_ref()
                    .and_then(|metadata| metadata.parent_session_id),
                preview: session_preview(&path).ok().flatten(),
                color: sidecar.as_ref().and_then(|metadata| metadata.color.clone()),
                tag: sidecar.and_then(|metadata| metadata.tag),
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
        let mut candidates = Vec::new();
        let mut scanned = 0usize;
        for entry in fs::read_dir(&directory)? {
            scanned = scanned.saturating_add(1);
            if scanned > MAX_SESSION_LIST_SCAN {
                bail!("session 目录条目超过 {MAX_SESSION_LIST_SCAN} 个安全上限")
            }
            let entry = entry?;
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
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => continue,
            };
            if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() == 0 {
                continue;
            }
            let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
            candidates.push((modified, id, path));
        }
        candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));

        let mut rejected = 0usize;
        for (_, id, path) in candidates {
            let loaded = match load_transcript(&path) {
                Ok(loaded) => loaded,
                Err(_) => {
                    rejected = rejected.saturating_add(1);
                    continue;
                }
            };
            if !fs::metadata(&path).is_ok_and(|metadata| metadata.len() > 0) {
                rejected = rejected.saturating_add(1);
                continue;
            }
            return Ok((
                Self {
                    id,
                    cwd: cwd.to_owned(),
                    file: path,
                    enabled,
                    workspace: Arc::new(Mutex::new(loaded.workspace)),
                    current_cwd: Arc::new(Mutex::new(loaded.current_cwd)),
                    write_lock: Arc::new(Mutex::new(())),
                },
                loaded.messages,
            ));
        }
        if rejected == 0 {
            bail!("当前目录没有可继续的会话")
        }
        bail!("当前目录没有可恢复的会话；已跳过 {rejected} 个损坏或不可读取的候选")
    }

    /// Creates a new session from a bounded prefix of an existing session.
    /// The source transcript is never modified and the fork receives a fresh id.
    pub fn fork(
        cwd: &Path,
        source_id: Uuid,
        message_count: Option<usize>,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        Self::fork_with_title(cwd, source_id, message_count, None, enabled)
    }

    pub fn fork_with_title(
        cwd: &Path,
        source_id: Uuid,
        message_count: Option<usize>,
        title: Option<&str>,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        Self::fork_from_directory(
            cwd,
            source_id,
            message_count,
            title,
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
        Self::fork_in_with_title(cwd, source_id, message_count, None, state_root, enabled)
    }

    pub fn fork_in_with_title(
        cwd: &Path,
        source_id: Uuid,
        message_count: Option<usize>,
        title: Option<&str>,
        state_root: &SessionStateRoot,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        Self::fork_from_directory(
            cwd,
            source_id,
            message_count,
            title,
            enabled,
            state_root.project_directory(cwd)?,
        )
    }

    fn fork_from_directory(
        cwd: &Path,
        source_id: Uuid,
        message_count: Option<usize>,
        title: Option<&str>,
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
        source_store.fork_from_with_title(message_count, title, enabled)
    }

    /// Forks this store without requiring another project-directory lookup.
    pub fn fork_from(
        &self,
        message_count: Option<usize>,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        self.fork_from_with_title(message_count, None, enabled)
    }

    pub fn fork_from_with_title(
        &self,
        message_count: Option<usize>,
        title: Option<&str>,
        enabled: bool,
    ) -> Result<(Self, Vec<Message>)> {
        let title = title
            .map(|title| validate_session_title(title, &self.cwd))
            .transpose()?;
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
            let mut metadata = SessionMetadata::new(destination.id);
            metadata.title = title;
            metadata.parent_session_id = Some(self.id);
            if let Some(source) = read_session_metadata(&self.file, self.id)? {
                metadata.color = source.color;
                metadata.tag = source.tag;
            }
            if let Err(error) = write_session_metadata(&destination.file, &metadata) {
                let _ = fs::remove_file(&destination.file);
                return Err(error);
            }
        }
        Ok((destination, messages))
    }

    pub fn rename(&self, title: &str) -> Result<()> {
        if !self.enabled || self.file.as_os_str().is_empty() {
            bail!("当前会话未启用持久化，无法保存标题")
        }
        let title = validate_session_title(title, &self.cwd)?;
        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut metadata = read_session_metadata(&self.file, self.id)?
            .unwrap_or_else(|| SessionMetadata::new(self.id));
        metadata.title = Some(title);
        write_session_metadata(&self.file, &metadata)
    }

    pub fn title(&self) -> Result<Option<String>> {
        if !self.enabled || self.file.as_os_str().is_empty() {
            return Ok(None);
        }
        Ok(read_session_metadata(&self.file, self.id)?.and_then(|metadata| metadata.title))
    }

    pub fn color(&self) -> Result<Option<String>> {
        if !self.enabled || self.file.as_os_str().is_empty() {
            return Ok(None);
        }
        Ok(read_session_metadata(&self.file, self.id)?.and_then(|metadata| metadata.color))
    }

    pub fn set_color(&self, color: Option<&str>) -> Result<()> {
        if !self.enabled || self.file.as_os_str().is_empty() {
            return Ok(());
        }
        let color = color.map(validate_session_color).transpose()?;
        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut metadata = read_session_metadata(&self.file, self.id)?
            .unwrap_or_else(|| SessionMetadata::new(self.id));
        metadata.color = color;
        write_session_metadata(&self.file, &metadata)
    }

    pub fn tag(&self) -> Result<Option<String>> {
        if !self.enabled || self.file.as_os_str().is_empty() {
            return Ok(None);
        }
        Ok(read_session_metadata(&self.file, self.id)?.and_then(|metadata| metadata.tag))
    }

    /// Stores one searchable local tag. Supplying the active tag toggles it
    /// off, matching the source-available terminal command behavior.
    pub fn toggle_tag(&self, tag: &str) -> Result<Option<String>> {
        if !self.enabled || self.file.as_os_str().is_empty() {
            bail!("当前会话未启用持久化，无法保存标签")
        }
        let tag = validate_session_tag(tag, &self.cwd)?;
        let _write = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut metadata = read_session_metadata(&self.file, self.id)?
            .unwrap_or_else(|| SessionMetadata::new(self.id));
        if metadata.tag.as_deref() == Some(tag.as_str()) {
            metadata.tag = None;
        } else {
            metadata.tag = Some(tag);
        }
        let result = metadata.tag.clone();
        write_session_metadata(&self.file, &metadata)?;
        Ok(result)
    }

    /// Removes a fork created by this process when a multi-surface rewind
    /// cannot be committed. The parent linkage is verified before either
    /// private file is removed, so this cannot target an unrelated session.
    pub fn discard_failed_fork(&self, expected_parent: Uuid) -> Result<()> {
        if !self.enabled || self.file.as_os_str().is_empty() {
            return Ok(());
        }
        let metadata = read_session_metadata(&self.file, self.id)?
            .context("failed fork is missing its private metadata")?;
        if metadata.parent_session_id != Some(expected_parent) {
            bail!("refusing to discard a session without the expected parent linkage")
        }
        let transcript = fs::symlink_metadata(&self.file).with_context(|| {
            format!(
                "failed fork transcript is unavailable: {}",
                self.file.display()
            )
        })?;
        if transcript.file_type().is_symlink() || !transcript.is_file() {
            bail!("refusing to discard a symlink or non-file failed fork")
        }
        let metadata_path = session_metadata_path(&self.file)?;
        let metadata_file = fs::symlink_metadata(&metadata_path)?;
        if metadata_file.file_type().is_symlink() || !metadata_file.is_file() {
            bail!("refusing to discard symlink or non-file session metadata")
        }
        fs::remove_file(&self.file)?;
        fs::remove_file(metadata_path)?;
        Ok(())
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

    /// Starts a genuinely new empty conversation while leaving this session untouched.
    ///
    /// `/clear` is a session boundary, not an in-place transcript rewrite. The new session keeps
    /// the trusted workspace/cwd state needed by the live terminal, receives a fresh id, and
    /// records this session as its parent so `/resume` can still identify the lineage.
    pub fn start_new_after_clear(&self) -> Result<Self> {
        let id = Uuid::new_v4();
        let workspace = self.workspace_state();
        let current_cwd = self.current_cwd_state();
        let file = if self.enabled {
            self.file
                .parent()
                .context("current transcript is missing its project directory")?
                .join(format!("{id}.jsonl"))
        } else {
            PathBuf::new()
        };
        let destination = Self {
            id,
            cwd: self.cwd.clone(),
            file,
            enabled: self.enabled,
            workspace: Arc::new(Mutex::new(workspace.clone())),
            current_cwd: Arc::new(Mutex::new(current_cwd.clone())),
            write_lock: Arc::new(Mutex::new(())),
        };
        if self.enabled {
            let record = Record::from_state(id, &workspace, current_cwd.as_ref(), true, None);
            let mut contents = Vec::new();
            append_record_bytes(&mut contents, &record)?;
            replace_private_transcript(&destination.file, &contents)?;
            let mut metadata = SessionMetadata::new(id);
            metadata.parent_session_id = Some(self.id);
            if let Err(error) = write_session_metadata(&destination.file, &metadata) {
                let _ = fs::remove_file(&destination.file);
                return Err(error);
            }
        }
        Ok(destination)
    }

    /// Preserves the current conversation as a resumable session before
    /// starting an empty history in this live runtime. The live store keeps
    /// its id so existing tool/file-history recorders remain valid; callers
    /// receive the fresh archive id to expose through `/resume`.
    pub fn archive_and_clear_history(&self) -> Result<Option<Uuid>> {
        if !self.enabled || !self.file.exists() {
            self.clear_history()?;
            return Ok(None);
        }
        let (archive, messages) = self.fork_from(None, true)?;
        if messages.is_empty() {
            let _ = fs::remove_file(&archive.file);
            if let Ok(metadata) = session_metadata_path(&archive.file) {
                let _ = fs::remove_file(metadata);
            }
            self.clear_history()?;
            return Ok(None);
        }
        self.clear_history()?;
        Ok(Some(archive.id))
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

fn session_preview(file: &Path) -> Result<Option<String>> {
    const MAX_PREVIEW_SCAN_BYTES: u64 = 512 * 1024;
    const MAX_PREVIEW_RECORDS: usize = 2_048;
    if fs::symlink_metadata(file)?.file_type().is_symlink() {
        bail!("refusing to preview a symlink transcript")
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut input = options.open(file)?;
    let size = input.metadata()?.len();
    let offset = size.saturating_sub(MAX_PREVIEW_SCAN_BYTES);
    input.seek(SeekFrom::Start(offset))?;
    let mut bytes = Vec::new();
    input
        .take(MAX_PREVIEW_SCAN_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if offset > 0 {
        let Some(first_newline) = bytes.iter().position(|byte| *byte == b'\n') else {
            return Ok(None);
        };
        bytes.drain(..=first_newline);
    }
    let mut preview = None;
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if index >= MAX_PREVIEW_RECORDS || line.is_empty() {
            break;
        }
        let Ok(record) = serde_json::from_slice::<Record>(line) else {
            continue;
        };
        if record.compact_boundary {
            preview = None;
        }
        if preview.is_none() {
            preview = record.message.as_ref().and_then(session_message_preview);
        }
    }
    Ok(preview)
}

fn session_message_preview(message: &Message) -> Option<String> {
    if message.role != crate::types::Role::User {
        return None;
    }
    let text = match &message.content {
        Value::String(text)
            if !text.starts_with("This session continues from an earlier conversation") =>
        {
            text.clone()
        }
        Value::Array(blocks)
            if !blocks
                .iter()
                .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_result")) =>
        {
            let text = blocks
                .iter()
                .filter_map(|block| {
                    (block.get("type").and_then(Value::as_str) == Some("text"))
                        .then(|| block.get("text").and_then(Value::as_str))
                        .flatten()
                })
                .collect::<Vec<_>>()
                .join(" ");
            if text.trim().is_empty() {
                "[attachment]".to_owned()
            } else {
                text
            }
        }
        _ => return None,
    };
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return None;
    }
    let mut preview = collapsed.chars().take(160).collect::<String>();
    if collapsed.chars().count() > 160 {
        preview.push('…');
    }
    Some(preview)
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

fn session_metadata_path(transcript: &Path) -> Result<PathBuf> {
    let parent = transcript.parent().context("transcript 路径缺少父目录")?;
    let id = transcript
        .file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<Uuid>().ok())
        .context("transcript 文件名不是 session UUID")?;
    Ok(parent.join(format!("{id}.meta.json")))
}

fn validate_session_title(title: &str, cwd: &Path) -> Result<String> {
    let title = title.trim();
    validate_session_title_value(title, cwd)?;
    Ok(title.to_owned())
}

fn validate_session_title_value(title: &str, cwd: &Path) -> Result<()> {
    if title.is_empty() || title.len() > MAX_SESSION_TITLE_BYTES {
        bail!("session 标题必须为 1..={MAX_SESSION_TITLE_BYTES} 字节")
    }
    if title.chars().any(char::is_control) {
        bail!("session 标题必须是单行且不能包含控制字符")
    }
    if sanitize_text(title, None, cwd) != title {
        bail!("session 标题不能包含 secret、endpoint 凭据或本机绝对路径")
    }
    if title.split_whitespace().any(|token| {
        let token = token.trim_matches(|character: char| {
            matches!(
                character,
                '(' | ')' | '[' | ']' | '{' | '}' | '<' | '>' | '"' | '\''
            )
        });
        looks_absolute_path(token)
    }) {
        bail!("session 标题不能包含本机绝对路径")
    }
    Ok(())
}

fn validate_session_tag(tag: &str, cwd: &Path) -> Result<String> {
    let tag = tag.trim().trim_start_matches('#');
    if tag.is_empty() || tag.len() > MAX_SESSION_TAG_BYTES {
        bail!("session 标签必须为 1..={MAX_SESSION_TAG_BYTES} 字节")
    }
    if tag.chars().any(char::is_control) || tag.chars().any(char::is_whitespace) {
        bail!("session 标签不能包含空白或控制字符")
    }
    if sanitize_text(tag, None, cwd) != tag || looks_absolute_path(tag) {
        bail!("session 标签不能包含 secret、endpoint 凭据或本机绝对路径")
    }
    Ok(tag.to_owned())
}

fn validate_session_metadata(metadata: &SessionMetadata, expected_id: Uuid) -> Result<()> {
    if metadata.version != SESSION_METADATA_VERSION || metadata.session_id != expected_id {
        bail!("session metadata 版本或 session id 不匹配")
    }
    if metadata.parent_session_id == Some(expected_id) {
        bail!("session metadata 不能将自身记录为父会话")
    }
    if let Some(title) = &metadata.title {
        if title.trim() != title
            || validate_session_title_value(title, Path::new("<session-metadata>")).is_err()
        {
            bail!("session metadata 标题无效")
        }
    }
    if let Some(color) = &metadata.color {
        validate_session_color(color)?;
    }
    if let Some(tag) = &metadata.tag {
        if validate_session_tag(tag, Path::new("<session-metadata>"))? != *tag {
            bail!("session metadata 标签无效")
        }
    }
    Ok(())
}

fn validate_session_color(color: &str) -> Result<String> {
    let color = color.trim().to_ascii_lowercase();
    if !SESSION_COLORS.contains(&color.as_str()) {
        bail!("session color must be one of {}", SESSION_COLORS.join(", "))
    }
    Ok(color)
}

fn read_session_metadata(transcript: &Path, expected_id: Uuid) -> Result<Option<SessionMetadata>> {
    let path = session_metadata_path(transcript)?;
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("session metadata 必须是非 symlink 普通文件")
    }
    if metadata.len() > MAX_SESSION_METADATA_BYTES {
        bail!("session metadata 超过 {MAX_SESSION_METADATA_BYTES} 字节限制")
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(&path)
        .with_context(|| format!("无法打开 session metadata {}", path.display()))?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(MAX_SESSION_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_SESSION_METADATA_BYTES {
        bail!("session metadata 超过 {MAX_SESSION_METADATA_BYTES} 字节限制")
    }
    let metadata: SessionMetadata =
        serde_json::from_slice(&bytes).context("session metadata JSON 损坏")?;
    validate_session_metadata(&metadata, expected_id)?;
    Ok(Some(metadata))
}

fn write_session_metadata(transcript: &Path, metadata: &SessionMetadata) -> Result<()> {
    let expected_id = transcript
        .file_stem()
        .and_then(|value| value.to_str())
        .and_then(|value| value.parse::<Uuid>().ok())
        .context("transcript 文件名不是 session UUID")?;
    validate_session_metadata(metadata, expected_id)?;
    let path = session_metadata_path(transcript)?;
    if fs::symlink_metadata(&path)
        .map(|metadata| metadata.file_type().is_symlink() || !metadata.is_file())
        .unwrap_or(false)
    {
        bail!(
            "拒绝替换 symlink 或非普通 session metadata: {}",
            path.display()
        )
    }
    let contents = serde_json::to_vec(metadata)?;
    if contents.len() as u64 > MAX_SESSION_METADATA_BYTES {
        bail!("session metadata 超过 {MAX_SESSION_METADATA_BYTES} 字节限制")
    }
    let parent = path.parent().context("session metadata 路径缺少父目录")?;
    ensure_private_directory(parent)?;
    let temp = parent.join(format!(".open-agent-harness-meta-{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(&contents)?;
        file.flush()?;
        file.sync_all()?;
        fs::rename(&temp, &path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.with_context(|| format!("无法原子替换 session metadata {}", path.display()))
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
    fn continue_latest_skips_empty_and_corrupt_candidates() {
        let workspace = tempfile::tempdir().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let valid_id = Uuid::parse_str("ffffffff-ffff-4fff-8fff-ffffffffffff").unwrap();
        let corrupt_id = Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();
        let empty_id = Uuid::parse_str("00000000-0000-4000-8000-000000000002").unwrap();
        let torn_id = Uuid::parse_str("00000000-0000-4000-8000-000000000003").unwrap();
        let valid = test_store(
            workspace.path(),
            directory.path().join(format!("{valid_id}.jsonl")),
            valid_id,
        );
        let expected = Message::user_text("recoverable history");
        valid.append(std::slice::from_ref(&expected)).unwrap();
        fs::write(
            directory.path().join(format!("{corrupt_id}.jsonl")),
            b"not-json\n",
        )
        .unwrap();
        fs::write(directory.path().join(format!("{empty_id}.jsonl")), b"").unwrap();
        fs::write(
            directory.path().join(format!("{torn_id}.jsonl")),
            br#"{"session_id":"#,
        )
        .unwrap();

        let (continued, messages) = SessionStore::continue_latest_from_directory(
            workspace.path(),
            true,
            directory.path().to_owned(),
        )
        .unwrap();

        assert_eq!(continued.id, valid_id);
        assert_eq!(messages, vec![expected]);
    }

    #[test]
    fn session_metadata_rename_and_fork_are_private_and_listed() {
        let workspace = tempfile::tempdir().unwrap();
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        make_private_directory(directory.path());
        let id = Uuid::new_v4();
        let store = test_store(
            workspace.path(),
            directory.path().join(format!("{id}.jsonl")),
            id,
        );
        store.append(&[Message::user_text("hello")]).unwrap();
        store.rename("  会话标题  ").unwrap();
        assert_eq!(
            store.toggle_tag("#terminal-fix").unwrap().as_deref(),
            Some("terminal-fix")
        );

        let metadata_path = session_metadata_path(&store.file).unwrap();
        let metadata = read_session_metadata(&store.file, id).unwrap().unwrap();
        assert_eq!(metadata.title.as_deref(), Some("会话标题"));
        assert_eq!(metadata.parent_session_id, None);
        assert_eq!(metadata.tag.as_deref(), Some("terminal-fix"));
        assert!(
            !fs::read_to_string(&metadata_path)
                .unwrap()
                .contains(workspace.path().to_string_lossy().as_ref())
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&metadata_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let (fork, messages) = store
            .fork_from_with_title(Some(1), Some("Branch α"), true)
            .unwrap();
        assert_eq!(messages, vec![Message::user_text("hello")]);
        let fork_metadata = read_session_metadata(&fork.file, fork.id).unwrap().unwrap();
        assert_eq!(fork_metadata.title.as_deref(), Some("Branch α"));
        assert_eq!(fork_metadata.parent_session_id, Some(id));
        assert_eq!(fork_metadata.tag.as_deref(), Some("terminal-fix"));

        let listed = SessionStore::list_from_directory(directory.path().to_owned(), 10).unwrap();
        let source_summary = listed.iter().find(|summary| summary.id == id).unwrap();
        assert_eq!(source_summary.title.as_deref(), Some("会话标题"));
        assert_eq!(source_summary.parent_session_id, None);
        assert_eq!(source_summary.tag.as_deref(), Some("terminal-fix"));
        let fork_summary = listed.iter().find(|summary| summary.id == fork.id).unwrap();
        assert_eq!(fork_summary.title.as_deref(), Some("Branch α"));
        assert_eq!(fork_summary.parent_session_id, Some(id));
        assert_eq!(fork_summary.tag.as_deref(), Some("terminal-fix"));
        assert_eq!(store.toggle_tag("terminal-fix").unwrap(), None);
        assert!(
            !directory
                .path()
                .read_dir()
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
        );
    }

    #[test]
    fn session_metadata_title_boundaries_fail_closed() {
        let workspace = tempfile::tempdir().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(
            workspace.path(),
            directory.path().join(format!("{id}.jsonl")),
            id,
        );
        store.append(&[Message::user_text("hello")]).unwrap();

        assert!(store.rename("").is_err());
        assert!(store.rename("line one\nline two").is_err());
        assert!(store.rename("control\0character").is_err());
        assert!(
            store
                .rename(&"x".repeat(MAX_SESSION_TITLE_BYTES + 1))
                .is_err()
        );
        let absolute_title = format!(
            "Inspect {}",
            workspace.path().join("private-file").display()
        );
        assert!(store.rename(&absolute_title).is_err());
        assert!(store.rename("api_key=not-a-real-key").is_err());

        assert!(store.toggle_tag("").is_err());
        assert!(store.toggle_tag("two words").is_err());
        assert!(store.toggle_tag("control\0tag").is_err());
        assert!(
            store
                .toggle_tag(&"x".repeat(MAX_SESSION_TAG_BYTES + 1))
                .is_err()
        );
        assert!(
            store
                .toggle_tag(workspace.path().to_string_lossy().as_ref())
                .is_err()
        );
        assert!(store.toggle_tag("api_key=not-a-real-key").is_err());

        let exact = "界".repeat(MAX_SESSION_TITLE_BYTES / "界".len());
        store.rename(&exact).unwrap();
        assert_eq!(
            read_session_metadata(&store.file, id)
                .unwrap()
                .unwrap()
                .title
                .as_deref(),
            Some(exact.as_str())
        );
    }

    #[test]
    fn corrupt_or_oversized_session_metadata_does_not_hide_transcript() {
        let workspace = tempfile::tempdir().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(
            workspace.path(),
            directory.path().join(format!("{id}.jsonl")),
            id,
        );
        store.append(&[Message::user_text("hello")]).unwrap();
        let metadata_path = session_metadata_path(&store.file).unwrap();

        fs::write(&metadata_path, b"not-json").unwrap();
        assert!(store.rename("replacement").is_err());
        let listed = SessionStore::list_from_directory(directory.path().to_owned(), 10).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, id);
        assert_eq!(listed[0].title, None);
        assert_eq!(listed[0].parent_session_id, None);

        fs::write(
            &metadata_path,
            vec![b'x'; MAX_SESSION_METADATA_BYTES as usize + 1],
        )
        .unwrap();
        assert!(store.rename("replacement").is_err());
        let listed = SessionStore::list_from_directory(directory.path().to_owned(), 10).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].title, None);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_session_metadata_is_ignored_by_list_and_rejected_by_rename() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(
            workspace.path(),
            directory.path().join(format!("{id}.jsonl")),
            id,
        );
        store.append(&[Message::user_text("hello")]).unwrap();
        let metadata_path = session_metadata_path(&store.file).unwrap();
        symlink(outside.path(), &metadata_path).unwrap();

        assert!(store.rename("replacement").is_err());
        let listed = SessionStore::list_from_directory(directory.path().to_owned(), 10).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].title, None);
        assert_eq!(fs::read(outside.path()).unwrap(), Vec::<u8>::new());
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
    fn archive_and_clear_keeps_previous_conversation_resumable() {
        let temp = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let store = test_store(
            temp.path(),
            temp.path().join(format!("{session_id}.jsonl")),
            session_id,
        );
        let previous = vec![
            Message::user_text("preserve me"),
            Message::assistant(vec![serde_json::json!({"type":"text","text":"kept"})]),
        ];
        store.append(&previous).unwrap();
        let archive_id = store.archive_and_clear_history().unwrap().unwrap();
        let archive_file = temp.path().join(format!("{archive_id}.jsonl"));
        assert_eq!(load_messages(&archive_file).unwrap(), previous);
        assert!(load_messages(&store.file).unwrap().is_empty());
        assert_ne!(archive_id, store.id);
    }

    #[test]
    fn clear_starts_fresh_session_without_rewriting_the_old_session() {
        let temp = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let store = test_store(
            temp.path(),
            temp.path().join(format!("{session_id}.jsonl")),
            session_id,
        );
        let previous = vec![Message::user_text("old conversation stays here")];
        store.append(&previous).unwrap();
        store.rename("Old title").unwrap();
        let old_bytes = fs::read(&store.file).unwrap();

        let next = store.start_new_after_clear().unwrap();

        assert_ne!(next.id, store.id);
        assert_eq!(fs::read(&store.file).unwrap(), old_bytes);
        assert_eq!(load_messages(&store.file).unwrap(), previous);
        assert!(load_messages(&next.file).unwrap().is_empty());
        assert_eq!(
            read_session_metadata(&store.file, store.id)
                .unwrap()
                .unwrap()
                .title
                .as_deref(),
            Some("Old title")
        );
        assert_eq!(
            read_session_metadata(&next.file, next.id)
                .unwrap()
                .unwrap()
                .parent_session_id,
            Some(store.id)
        );
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
        let id = Uuid::new_v4();
        let store = test_store(temp.path(), temp.path().join(format!("{id}.jsonl")), id);
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
    fn failed_session_fork_cleanup_requires_parent_linkage() {
        let temp = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let store = test_store(temp.path(), temp.path().join(format!("{id}.jsonl")), id);
        store.append(&[Message::user_text("one")]).unwrap();
        let (fork, _) = store.fork_from(None, true).unwrap();
        let fork_metadata = session_metadata_path(&fork.file).unwrap();

        assert!(fork.file.is_file());
        assert!(fork_metadata.is_file());
        assert!(fork.discard_failed_fork(Uuid::new_v4()).is_err());
        assert!(fork.file.is_file());
        fork.discard_failed_fork(id).unwrap();
        assert!(!fork.file.exists());
        assert!(!fork_metadata.exists());
        assert!(store.file.is_file());
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

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tools::workspace_key;

const MANIFEST_VERSION: u32 = 1;
const MAX_CHECKPOINTS: usize = 100;
const MAX_TRACKED_FILES: usize = 2_048;
const MAX_PATH_BYTES: usize = 4_096;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_BACKUP_BYTES: u64 = 256 * 1024 * 1024;
const MAX_REWIND_BYTES: u64 = 256 * 1024 * 1024;
const MAX_BACKUP_FILES: usize = 8_192;
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_POST_EDIT_STATES_PER_FILE: usize = 256;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointBoundary {
    UserMessage,
    Turn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStatus {
    Active,
    Committed,
    RolledBack,
    RollbackConflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub id: Uuid,
    pub boundary: CheckpointBoundary,
    pub message_count: usize,
    pub timestamp_ms: u128,
    pub tracked_files: usize,
    pub ancestor_ids: Vec<Uuid>,
    pub status: CheckpointStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackConflictKind {
    ConcurrentModification,
    AmbiguousTransactionOverlap,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "拒绝回滚 checkpoint {checkpoint_id} 的并发修改文件 {path}: {kind}",
    path = .path.display()
)]
pub struct RollbackConflict {
    pub checkpoint_id: Uuid,
    pub conflicting_checkpoint_id: Option<Uuid>,
    pub path: PathBuf,
    pub kind: RollbackConflictKind,
}

impl std::fmt::Display for RollbackConflictKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConcurrentModification => formatter.write_str("当前内容不属于该事务"),
            Self::AmbiguousTransactionOverlap => {
                formatter.write_str("无法证明回滚不会覆盖独立事务")
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiffStats {
    pub files_changed: Vec<PathBuf>,
    pub insertions: usize,
    pub deletions: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RewindReport {
    pub files_changed: Vec<PathBuf>,
    pub restored: usize,
    pub deleted: usize,
}

#[derive(Debug, Clone)]
pub struct FileHistory {
    session_id: Uuid,
    workspace: PathBuf,
    storage_root: PathBuf,
    directory: PathBuf,
    enabled: bool,
    transaction_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    session_id: Uuid,
    workspace_key: String,
    tracked_files: BTreeSet<String>,
    checkpoints: Vec<Checkpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    id: Uuid,
    boundary: CheckpointBoundary,
    message_count: usize,
    timestamp_ms: u128,
    files: BTreeMap<String, FileVersion>,
    #[serde(default)]
    touched_files: BTreeSet<String>,
    #[serde(default)]
    post_edit: BTreeMap<String, Vec<ExpectedFileState>>,
    #[serde(default)]
    transaction_active: bool,
    #[serde(default)]
    ancestor_ids: BTreeSet<Uuid>,
    #[serde(default)]
    rolled_back: bool,
    #[serde(default)]
    rollback_conflicted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FileVersion {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blob: Option<String>,
    bytes: u64,
    checksum: String,
    mode: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ExpectedFileState {
    exists: bool,
    bytes: u64,
    checksum: String,
}

struct PreparedChange {
    relative: String,
    path: PathBuf,
    target: Option<(Vec<u8>, u32)>,
    current: Option<Vec<u8>>,
}

impl FileHistory {
    /// Creates per-session history below `~/.open-agent-harness/file-history`.
    pub fn create(workspace: &Path, session_id: Uuid, enabled: bool) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        if !enabled {
            return Ok(Self::disabled(workspace, session_id));
        }
        let home = dirs::home_dir().context("无法确定用户主目录")?;
        let home = fs::canonicalize(&home)
            .with_context(|| format!("无法解析用户主目录 {}", home.display()))?;
        let harness = ensure_private_child(&home, ".open-agent-harness")?;
        let root = ensure_private_child(&harness, "file-history")?;
        Self::create_in_canonical(workspace, session_id, root)
    }

    /// Creates history below an explicit, already-existing storage root.
    /// This is useful for embedders and deterministic tests.
    pub fn create_in(
        workspace: &Path,
        session_id: Uuid,
        storage_root: &Path,
        enabled: bool,
    ) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        if !enabled {
            return Ok(Self::disabled(workspace, session_id));
        }
        let metadata = fs::symlink_metadata(storage_root)
            .with_context(|| format!("file-history 根目录不存在: {}", storage_root.display()))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("file-history 根目录必须是非 symlink 目录")
        }
        set_private_directory_permissions(storage_root)?;
        let root = fs::canonicalize(storage_root)
            .with_context(|| format!("无法解析 file-history 根目录 {}", storage_root.display()))?;
        Self::create_in_canonical(workspace, session_id, root)
    }

    fn create_in_canonical(
        workspace: PathBuf,
        session_id: Uuid,
        storage_root: PathBuf,
    ) -> Result<Self> {
        let project = ensure_private_child(&storage_root, &workspace_key(&workspace))?;
        let directory = ensure_private_child(&project, &session_id.to_string())?;
        ensure_private_child(&directory, "blobs")?;
        let history = Self {
            session_id,
            workspace,
            storage_root,
            directory,
            enabled: true,
            transaction_lock: Arc::new(Mutex::new(())),
        };
        let _ = history.load_manifest()?;
        history.recover_orphaned_transactions()?;
        Ok(history)
    }

    fn disabled(workspace: PathBuf, session_id: Uuid) -> Self {
        Self {
            session_id,
            workspace,
            storage_root: PathBuf::new(),
            directory: PathBuf::new(),
            enabled: false,
            transaction_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// A persisted active transaction has no owner after this history is
    /// reopened. Recover newest-first so nested/detached checkpoints restore
    /// their own writes before an ancestor is examined. Ownership validation
    /// remains fail-closed: a later external edit is never overwritten.
    fn recover_orphaned_transactions(&self) -> Result<()> {
        let active = {
            let _transaction = self
                .transaction_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.load_manifest()?
                .checkpoints
                .iter()
                .rev()
                .filter(|checkpoint| checkpoint.transaction_active)
                .map(|checkpoint| checkpoint.id)
                .collect::<Vec<_>>()
        };
        // Preflight every orphan before the first workspace mutation so a
        // later conflict cannot leave startup recovery half-applied.
        for checkpoint in &active {
            self.validate_rollback(*checkpoint).with_context(|| {
                format!("无法安全预检遗留 file-history transaction {checkpoint}")
            })?;
        }
        for checkpoint in active {
            self.rollback_checkpoint(checkpoint).with_context(|| {
                format!("无法安全恢复遗留 file-history transaction {checkpoint}")
            })?;
        }
        Ok(())
    }

    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    pub fn relocate(&self, workspace: &Path) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        if !self.enabled {
            return Ok(Self::disabled(workspace, self.session_id));
        }
        Self::create_in_canonical(workspace, self.session_id, self.storage_root.clone())
    }

    /// Starts a user-message or turn checkpoint and snapshots all files that
    /// were already tracked. Call `track_before_edit` before each first write
    /// within the boundary so newly touched files record their pre-edit state.
    pub fn checkpoint(
        &self,
        id: Uuid,
        boundary: CheckpointBoundary,
        message_count: usize,
    ) -> Result<CheckpointInfo> {
        self.checkpoint_with_ancestors(id, boundary, message_count, &[])
    }

    pub(crate) fn checkpoint_with_ancestors(
        &self,
        id: Uuid,
        boundary: CheckpointBoundary,
        message_count: usize,
        ancestor_ids: &[Uuid],
    ) -> Result<CheckpointInfo> {
        if ancestor_ids.len() > MAX_CHECKPOINTS || ancestor_ids.contains(&id) {
            bail!("file-history checkpoint 祖先链无效或超过资源限制")
        }
        let ancestor_ids = ancestor_ids.iter().copied().collect::<BTreeSet<_>>();
        if !self.enabled {
            return Ok(CheckpointInfo {
                id,
                boundary,
                message_count,
                timestamp_ms: now_ms(),
                tracked_files: 0,
                ancestor_ids: ancestor_ids.into_iter().collect(),
                status: CheckpointStatus::Active,
            });
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut manifest = self.load_manifest()?;
        if let Some(existing) = manifest.checkpoints.iter().find(|entry| entry.id == id) {
            if existing.boundary != boundary
                || existing.message_count != message_count
                || existing.ancestor_ids != ancestor_ids
            {
                bail!("重复 checkpoint id 的边界或消息位置不一致")
            }
            return Ok(checkpoint_info(existing));
        }
        self.garbage_collect(&manifest)?;
        while manifest.checkpoints.len() >= MAX_CHECKPOINTS {
            let removable = manifest
                .checkpoints
                .iter()
                .position(|checkpoint| !checkpoint.transaction_active)
                .context("file-history checkpoint 已达上限且全部仍处于活跃事务")?;
            manifest.checkpoints.remove(removable);
        }
        let mut files = BTreeMap::new();
        for relative in manifest.tracked_files.iter() {
            let path = self.resolve_stored_path(relative)?;
            let previous = manifest
                .checkpoints
                .last()
                .and_then(|checkpoint| checkpoint.files.get(relative));
            files.insert(relative.clone(), self.capture(&path, previous)?);
        }
        let checkpoint = Checkpoint {
            id,
            boundary,
            message_count,
            timestamp_ms: now_ms(),
            files,
            touched_files: BTreeSet::new(),
            post_edit: BTreeMap::new(),
            transaction_active: true,
            ancestor_ids,
            rolled_back: false,
            rollback_conflicted: false,
        };
        manifest.checkpoints.push(checkpoint);
        self.write_manifest(&manifest)?;
        self.garbage_collect(&manifest)?;
        Ok(checkpoint_info(
            manifest
                .checkpoints
                .last()
                .expect("checkpoint was inserted"),
        ))
    }

    /// Adds the pre-edit version of `path` to an active checkpoint.
    /// Repeated calls for the same file and checkpoint are idempotent.
    pub fn track_before_edit(&self, checkpoint_id: Uuid, path: &Path) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut manifest = self.load_manifest()?;
        let relative = self.relative_path(path)?;
        let checkpoint_index = manifest
            .checkpoints
            .iter()
            .position(|checkpoint| checkpoint.id == checkpoint_id)
            .context("找不到要追踪的 checkpoint")?;
        if !manifest.checkpoints[checkpoint_index].transaction_active {
            bail!("不能向已结束的 checkpoint 追加修改前快照")
        }
        manifest.checkpoints[checkpoint_index]
            .touched_files
            .insert(relative.clone());
        if manifest.checkpoints[checkpoint_index]
            .files
            .contains_key(&relative)
        {
            return self.write_manifest(&manifest);
        }
        if manifest.tracked_files.len() >= MAX_TRACKED_FILES {
            bail!("file-history 超过 {MAX_TRACKED_FILES} 个文件限制")
        }
        self.garbage_collect(&manifest)?;
        let resolved = self.resolve_stored_path(&relative)?;
        let previous = manifest.checkpoints[..checkpoint_index]
            .last()
            .and_then(|checkpoint| checkpoint.files.get(&relative));
        let version = self.capture(&resolved, previous)?;
        manifest.tracked_files.insert(relative.clone());
        manifest.checkpoints[checkpoint_index]
            .files
            .insert(relative, version);
        self.write_manifest(&manifest)
    }

    /// Records the exact state an edit is about to produce. Persisting this
    /// before the atomic file replacement lets rollback distinguish its own
    /// write from a later concurrent/background edit.
    pub fn expect_after_edit(&self, checkpoint_id: Uuid, path: &Path, bytes: &[u8]) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if bytes.len() as u64 > MAX_FILE_BYTES {
            bail!("修改后状态超过 {MAX_FILE_BYTES} 字节限制")
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut manifest = self.load_manifest()?;
        let checkpoint = manifest
            .checkpoints
            .iter_mut()
            .find(|checkpoint| checkpoint.id == checkpoint_id)
            .context("找不到要记录修改后状态的 checkpoint")?;
        if !checkpoint.transaction_active {
            bail!("不能向已结束的 checkpoint 记录修改后状态")
        }
        let relative = self.relative_path(path)?;
        if !checkpoint.touched_files.contains(&relative) {
            bail!("修改后状态必须对应本 checkpoint 已追踪的文件")
        }
        let state = ExpectedFileState {
            exists: true,
            bytes: bytes.len() as u64,
            checksum: checksum(bytes),
        };
        let states = checkpoint.post_edit.entry(relative).or_default();
        if !states.contains(&state) {
            if states.len() >= MAX_POST_EDIT_STATES_PER_FILE {
                bail!("单文件修改后状态超过 {MAX_POST_EDIT_STATES_PER_FILE} 个事务限制")
            }
            states.push(state);
        }
        self.write_manifest(&manifest)
    }

    pub fn finish_transaction(&self, checkpoint_id: Uuid) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut manifest = self.load_manifest()?;
        let checkpoint = manifest
            .checkpoints
            .iter_mut()
            .find(|checkpoint| checkpoint.id == checkpoint_id)
            .context("找不到要结束的 file-history checkpoint")?;
        checkpoint.transaction_active = false;
        self.write_manifest(&manifest)
    }

    pub fn checkpoints(&self) -> Result<Vec<CheckpointInfo>> {
        if !self.enabled {
            return Ok(Vec::new());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(self
            .load_manifest()?
            .checkpoints
            .iter()
            .map(checkpoint_info)
            .collect())
    }

    pub fn can_rewind(&self, checkpoint_id: Uuid) -> Result<bool> {
        if !self.enabled {
            return Ok(false);
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(self
            .load_manifest()?
            .checkpoints
            .iter()
            .any(|checkpoint| checkpoint.id == checkpoint_id))
    }

    pub fn is_transaction_active(&self, checkpoint_id: Uuid) -> Result<bool> {
        if !self.enabled {
            return Ok(false);
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(self
            .load_manifest()?
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.id == checkpoint_id)
            .is_some_and(|checkpoint| checkpoint.transaction_active))
    }

    /// Computes line-oriented dry-run statistics without changing the workspace.
    pub fn diff_stats(&self, checkpoint_id: Uuid) -> Result<DiffStats> {
        if !self.enabled {
            return Ok(DiffStats::default());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let manifest = self.load_manifest()?;
        let changes = self.prepare_changes(&manifest, checkpoint_id)?;
        let mut stats = DiffStats::default();
        for change in changes {
            if prepared_change_is_unchanged(&change) {
                continue;
            }
            let (insertions, deletions) = line_diff_counts(
                change.current.as_deref().unwrap_or_default(),
                change
                    .target
                    .as_ref()
                    .map(|(bytes, _)| bytes.as_slice())
                    .unwrap_or_default(),
            )?;
            stats.files_changed.push(PathBuf::from(change.relative));
            stats.insertions = stats.insertions.saturating_add(insertions);
            stats.deletions = stats.deletions.saturating_add(deletions);
        }
        Ok(stats)
    }

    /// Validates backup integrity and every destination path without mutating
    /// the workspace. Multi-root callers use this as the first phase of a
    /// cross-workspace rewind.
    pub fn validate_rewind(&self, checkpoint_id: Uuid) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let manifest = self.load_manifest()?;
        self.prepare_changes(&manifest, checkpoint_id)?;
        Ok(())
    }

    /// Restores all known files to a checkpoint. Backup integrity and every
    /// destination path are validated before the first workspace mutation.
    pub fn rewind(&self, checkpoint_id: Uuid) -> Result<RewindReport> {
        if !self.enabled {
            return Ok(RewindReport::default());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let manifest = self.load_manifest()?;
        let changes = self.prepare_changes(&manifest, checkpoint_id)?;
        let mut report = RewindReport::default();
        for change in changes {
            if prepared_change_is_unchanged(&change) {
                continue;
            }
            match change.target {
                Some((bytes, mode)) => {
                    self.atomic_restore(&change.path, &bytes, mode)?;
                    report.restored += 1;
                }
                None => {
                    remove_regular_file(&change.path)?;
                    report.deleted += 1;
                }
            }
            report.files_changed.push(PathBuf::from(change.relative));
        }
        Ok(report)
    }

    /// Rolls back only files mutated by this checkpoint. Every current file
    /// must still match the state produced by that transaction; a concurrent
    /// writer therefore causes a fail-closed error instead of being clobbered.
    pub fn rollback_checkpoint(&self, checkpoint_id: Uuid) -> Result<RewindReport> {
        if !self.enabled {
            return Ok(RewindReport::default());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut manifest = self.load_manifest()?;
        let checkpoint_index = manifest
            .checkpoints
            .iter()
            .rposition(|checkpoint| checkpoint.id == checkpoint_id)
            .context("找不到指定 file-history checkpoint")?;
        let touched_files = manifest.checkpoints[checkpoint_index].touched_files.clone();
        let changes = self.prepare_changes_for_paths(&manifest, checkpoint_id, &touched_files)?;

        if let Err(error) = validate_rollback_changes(&manifest, checkpoint_index, &changes) {
            manifest.checkpoints[checkpoint_index].rollback_conflicted = true;
            self.write_manifest(&manifest)?;
            return Err(error);
        }

        let mut report = RewindReport::default();
        for change in changes {
            if prepared_change_is_unchanged(&change) {
                continue;
            }
            match change.target {
                Some((bytes, mode)) => {
                    self.atomic_restore(&change.path, &bytes, mode)?;
                    report.restored += 1;
                }
                None => {
                    remove_regular_file(&change.path)?;
                    report.deleted += 1;
                }
            }
            report.files_changed.push(PathBuf::from(change.relative));
        }
        let checkpoint = &mut manifest.checkpoints[checkpoint_index];
        checkpoint.transaction_active = false;
        checkpoint.rolled_back = true;
        checkpoint.rollback_conflicted = false;
        self.write_manifest(&manifest)?;
        Ok(report)
    }

    /// Preflights transaction ownership for every touched file without
    /// applying the rollback.
    pub fn validate_rollback(&self, checkpoint_id: Uuid) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut manifest = self.load_manifest()?;
        let checkpoint_index = manifest
            .checkpoints
            .iter()
            .rposition(|checkpoint| checkpoint.id == checkpoint_id)
            .context("找不到指定 file-history checkpoint")?;
        let touched_files = manifest.checkpoints[checkpoint_index].touched_files.clone();
        let changes = self.prepare_changes_for_paths(&manifest, checkpoint_id, &touched_files)?;
        match validate_rollback_changes(&manifest, checkpoint_index, &changes) {
            Ok(()) => {
                if manifest.checkpoints[checkpoint_index].rollback_conflicted {
                    manifest.checkpoints[checkpoint_index].rollback_conflicted = false;
                    self.write_manifest(&manifest)?;
                }
                Ok(())
            }
            Err(error) => {
                manifest.checkpoints[checkpoint_index].rollback_conflicted = true;
                self.write_manifest(&manifest)?;
                Err(error)
            }
        }
    }

    pub(crate) fn mark_rollback_conflict(&self, checkpoint_id: Uuid) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut manifest = self.load_manifest()?;
        let checkpoint = manifest
            .checkpoints
            .iter_mut()
            .rfind(|checkpoint| checkpoint.id == checkpoint_id)
            .context("找不到指定 file-history checkpoint")?;
        if !checkpoint.rolled_back {
            checkpoint.rollback_conflicted = true;
            self.write_manifest(&manifest)?;
        }
        Ok(())
    }

    /// Copies immutable backups and the checkpoint manifest to a fresh session.
    pub fn fork(&self, new_session_id: Uuid) -> Result<Self> {
        if !self.enabled {
            return Ok(Self::disabled(self.workspace.clone(), new_session_id));
        }
        if new_session_id == self.session_id {
            bail!("file-history fork 必须使用新的 session id")
        }
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let destination = Self::create_in_canonical(
            self.workspace.clone(),
            new_session_id,
            self.storage_root.clone(),
        )?;
        if destination.manifest_path().exists() {
            bail!("目标 session 已存在 file-history")
        }
        let mut manifest = self.load_manifest()?;
        let blobs = referenced_blobs(&manifest);
        for blob in blobs {
            let source = self.read_blob_path(&blob)?;
            let target = destination.blobs_directory().join(&blob);
            match fs::hard_link(&source, &target) {
                Ok(()) => set_private_file_permissions(&target)?,
                Err(_) => {
                    let bytes = read_bounded_regular(&source, MAX_FILE_BYTES)?;
                    atomic_write_private_bytes(&target, &bytes)?;
                }
            }
        }
        manifest.session_id = new_session_id;
        // A fork copies durable rewind points, not in-flight ownership from
        // the source process. Carrying an active transaction into the new
        // session would leave it with no owner able to finish it.
        for checkpoint in &mut manifest.checkpoints {
            checkpoint.transaction_active = false;
        }
        destination.write_manifest(&manifest)?;
        Ok(destination)
    }

    fn prepare_changes(
        &self,
        manifest: &Manifest,
        checkpoint_id: Uuid,
    ) -> Result<Vec<PreparedChange>> {
        self.prepare_changes_for_paths(manifest, checkpoint_id, &manifest.tracked_files)
    }

    fn prepare_changes_for_paths(
        &self,
        manifest: &Manifest,
        checkpoint_id: Uuid,
        paths: &BTreeSet<String>,
    ) -> Result<Vec<PreparedChange>> {
        let target_index = manifest
            .checkpoints
            .iter()
            .position(|checkpoint| checkpoint.id == checkpoint_id)
            .context("找不到指定 file-history checkpoint")?;
        let target = &manifest.checkpoints[target_index];
        let mut changes = Vec::new();
        let mut prepared_bytes = 0_u64;
        for relative in paths {
            let version = target
                .files
                .get(relative)
                // A file first touched after the requested checkpoint was not
                // present in that checkpoint's sparse map. Its first later
                // pre-edit capture is the earliest state the harness can
                // restore for that boundary, whether the file already existed
                // or was created later.
                .or_else(|| first_version_after(manifest, target_index, relative));
            let Some(version) = version else {
                continue;
            };
            let path = self.resolve_stored_path(relative)?;
            let current = read_optional_workspace_file(&path)?;
            let target = match &version.blob {
                Some(blob) => Some((self.read_blob(blob, version)?, version.mode)),
                None => None,
            };
            let current_bytes = current.as_ref().map_or(0, |bytes| bytes.len() as u64);
            let target_bytes = target.as_ref().map_or(0, |(bytes, _)| bytes.len() as u64);
            prepared_bytes = prepared_bytes
                .checked_add(current_bytes)
                .and_then(|bytes| bytes.checked_add(target_bytes))
                .context("rewind 预检大小溢出")?;
            if prepared_bytes > MAX_REWIND_BYTES {
                bail!("rewind 预检超过 {MAX_REWIND_BYTES} 字节限制")
            }
            changes.push(PreparedChange {
                relative: relative.clone(),
                path,
                target,
                current,
            });
        }
        Ok(changes)
    }

    fn capture(&self, path: &Path, previous: Option<&FileVersion>) -> Result<FileVersion> {
        let Some((bytes, mode)) = read_optional_workspace_file_with_mode(path)? else {
            return Ok(FileVersion {
                blob: None,
                bytes: 0,
                checksum: checksum(&[]),
                mode: 0,
            });
        };
        let digest = checksum(&bytes);
        if let Some(previous) = previous {
            if previous.bytes == bytes.len() as u64 && previous.checksum == digest {
                if let Some(blob) = &previous.blob {
                    if self.read_blob(blob, previous)? == bytes {
                        return Ok(previous.clone());
                    }
                }
            }
        }
        self.ensure_backup_budget(bytes.len() as u64)?;
        let blob = format!("{}.blob", Uuid::new_v4());
        atomic_write_private_bytes(&self.blobs_directory().join(&blob), &bytes)?;
        Ok(FileVersion {
            blob: Some(blob),
            bytes: bytes.len() as u64,
            checksum: digest,
            mode,
        })
    }

    fn read_blob(&self, blob: &str, version: &FileVersion) -> Result<Vec<u8>> {
        if version.bytes > MAX_FILE_BYTES {
            bail!("file-history backup 超过 {MAX_FILE_BYTES} 字节限制")
        }
        let path = self.read_blob_path(blob)?;
        let bytes = read_bounded_regular(&path, MAX_FILE_BYTES)?;
        if bytes.len() as u64 != version.bytes || checksum(&bytes) != version.checksum {
            bail!("file-history backup 损坏: {blob}")
        }
        Ok(bytes)
    }

    fn read_blob_path(&self, blob: &str) -> Result<PathBuf> {
        validate_blob_name(blob)?;
        let path = self.blobs_directory().join(blob);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("file-history backup 缺失: {blob}"))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("file-history backup 不是普通文件: {blob}")
        }
        Ok(path)
    }

    fn relative_path(&self, path: &Path) -> Result<String> {
        let candidate = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.workspace.join(path)
        };
        let resolved = resolve_workspace_candidate(&self.workspace, &candidate)?;
        let relative = resolved
            .strip_prefix(&self.workspace)
            .context("file-history 路径越过工作区")?;
        validate_relative_path(relative)
    }

    fn resolve_stored_path(&self, relative: &str) -> Result<PathBuf> {
        let relative_path = Path::new(relative);
        validate_relative_path(relative_path)?;
        resolve_workspace_candidate(&self.workspace, &self.workspace.join(relative_path))
    }

    fn atomic_restore(&self, path: &Path, bytes: &[u8], mode: u32) -> Result<()> {
        if bytes.len() as u64 > MAX_FILE_BYTES {
            bail!("恢复文件超过 {MAX_FILE_BYTES} 字节限制")
        }
        let parent = path.parent().context("恢复路径缺少父目录")?;
        ensure_workspace_directory(&self.workspace, parent)?;
        reject_symlink(path)?;
        let temp = parent.join(format!(".open-agent-harness-rewind-{}.tmp", Uuid::new_v4()));
        #[cfg(not(unix))]
        let _ = mode;
        let result = (|| -> Result<()> {
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(if mode == 0 { 0o600 } else { mode & 0o777 });
            }
            let mut file = options.open(&temp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            #[cfg(unix)]
            if mode != 0 {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(fs::Permissions::from_mode(mode & 0o777))?;
            }
            fs::rename(&temp, path)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result.with_context(|| format!("无法原子恢复 {}", path.display()))
    }

    fn load_manifest(&self) -> Result<Manifest> {
        let path = self.manifest_path();
        if !path.exists() {
            return Ok(Manifest {
                version: MANIFEST_VERSION,
                session_id: self.session_id,
                workspace_key: workspace_key(&self.workspace),
                tracked_files: BTreeSet::new(),
                checkpoints: Vec::new(),
            });
        }
        reject_symlink(&path)?;
        let bytes = read_bounded_regular(&path, MAX_MANIFEST_BYTES)?;
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("file-history manifest 损坏: {}", path.display()))?;
        self.validate_manifest(&manifest)?;
        Ok(manifest)
    }

    fn validate_manifest(&self, manifest: &Manifest) -> Result<()> {
        if manifest.version != MANIFEST_VERSION
            || manifest.session_id != self.session_id
            || manifest.workspace_key != workspace_key(&self.workspace)
        {
            bail!("file-history manifest 身份或版本不匹配")
        }
        if manifest.tracked_files.len() > MAX_TRACKED_FILES
            || manifest.checkpoints.len() > MAX_CHECKPOINTS
        {
            bail!("file-history manifest 超过资源限制")
        }
        let mut ids = BTreeSet::new();
        let mut unique_blobs = BTreeMap::<String, u64>::new();
        for relative in &manifest.tracked_files {
            validate_relative_path(Path::new(relative))?;
        }
        for checkpoint in &manifest.checkpoints {
            if !ids.insert(checkpoint.id)
                || checkpoint.files.len() > MAX_TRACKED_FILES
                || checkpoint.touched_files.len() > MAX_TRACKED_FILES
                || checkpoint.post_edit.len() > MAX_TRACKED_FILES
                || checkpoint.ancestor_ids.len() > MAX_CHECKPOINTS
                || checkpoint.ancestor_ids.contains(&checkpoint.id)
                || (checkpoint.rolled_back && checkpoint.rollback_conflicted)
                || (checkpoint.rolled_back && checkpoint.transaction_active)
            {
                bail!("file-history checkpoint 重复或超过资源限制")
            }
            for (relative, version) in &checkpoint.files {
                validate_relative_path(Path::new(relative))?;
                if !manifest.tracked_files.contains(relative) {
                    bail!("file-history checkpoint 包含未追踪文件")
                }
                if version.bytes > MAX_FILE_BYTES || version.checksum.len() != 32 {
                    bail!("file-history backup 元数据超过资源限制或损坏")
                }
                if let Some(blob) = &version.blob {
                    validate_blob_name(blob)?;
                    if let Some(previous_bytes) = unique_blobs.insert(blob.clone(), version.bytes) {
                        if previous_bytes != version.bytes {
                            bail!("同一 file-history blob 的长度元数据不一致")
                        }
                    }
                } else if version.bytes != 0 {
                    bail!("缺失文件 marker 的长度必须为零")
                }
            }
            for relative in &checkpoint.touched_files {
                validate_relative_path(Path::new(relative))?;
                if !checkpoint.files.contains_key(relative) {
                    bail!("file-history touched file 缺少修改前快照")
                }
            }
            for (relative, states) in &checkpoint.post_edit {
                validate_relative_path(Path::new(relative))?;
                if !checkpoint.touched_files.contains(relative)
                    || states.is_empty()
                    || states.len() > MAX_POST_EDIT_STATES_PER_FILE
                    || states.iter().any(|state| {
                        state.bytes > MAX_FILE_BYTES
                            || state.checksum.len() != 32
                            || (!state.exists && state.bytes != 0)
                    })
                {
                    bail!("file-history 修改后状态无效或超过资源限制")
                }
            }
        }
        if unique_blobs.len() > MAX_BACKUP_FILES {
            bail!("file-history manifest 超过 backup 文件数量限制")
        }
        let total_bytes = unique_blobs.values().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .context("file-history backup 大小溢出")
        })?;
        if total_bytes > MAX_BACKUP_BYTES {
            bail!("file-history manifest 超过 backup 字节总限制")
        }
        Ok(())
    }

    fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        self.validate_manifest(manifest)?;
        let bytes = serde_json::to_vec(manifest)?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            bail!("file-history manifest 超过 {MAX_MANIFEST_BYTES} 字节限制")
        }
        atomic_write_private_bytes(&self.manifest_path(), &bytes)
    }

    fn ensure_backup_budget(&self, added: u64) -> Result<()> {
        let mut total = 0_u64;
        let mut count = 0_usize;
        for entry in fs::read_dir(self.blobs_directory())? {
            let entry = entry?;
            count += 1;
            if count >= MAX_BACKUP_FILES {
                bail!("file-history backup 超过 {MAX_BACKUP_FILES} 个文件限制")
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("file-history blob 文件名不是 UTF-8"))?;
            let path = self.read_blob_path(&name)?;
            total = total
                .checked_add(fs::metadata(path)?.len())
                .context("file-history backup 大小溢出")?;
        }
        if total
            .checked_add(added)
            .context("file-history backup 大小溢出")?
            > MAX_BACKUP_BYTES
        {
            bail!("file-history backup 超过 {MAX_BACKUP_BYTES} 字节总限制")
        }
        Ok(())
    }

    fn garbage_collect(&self, manifest: &Manifest) -> Result<()> {
        let referenced = referenced_blobs(manifest);
        let mut count = 0_usize;
        for entry in fs::read_dir(self.blobs_directory())? {
            let entry = entry?;
            count += 1;
            if count > MAX_BACKUP_FILES {
                bail!("file-history blob 目录超过 {MAX_BACKUP_FILES} 项限制")
            }
            let metadata = entry.file_type()?;
            if metadata.is_symlink() || !metadata.is_file() {
                bail!("file-history blob 目录包含非普通文件")
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("file-history blob 文件名不是 UTF-8"))?;
            validate_blob_name(&name)?;
            if !referenced.contains(&name) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn manifest_path(&self) -> PathBuf {
        self.directory.join("manifest.json")
    }

    fn blobs_directory(&self) -> PathBuf {
        self.directory.join("blobs")
    }
}

fn checkpoint_info(checkpoint: &Checkpoint) -> CheckpointInfo {
    CheckpointInfo {
        id: checkpoint.id,
        boundary: checkpoint.boundary,
        message_count: checkpoint.message_count,
        timestamp_ms: checkpoint.timestamp_ms,
        tracked_files: checkpoint.files.len(),
        ancestor_ids: checkpoint.ancestor_ids.iter().copied().collect(),
        status: checkpoint_status(checkpoint),
    }
}

fn checkpoint_status(checkpoint: &Checkpoint) -> CheckpointStatus {
    if checkpoint.rolled_back {
        CheckpointStatus::RolledBack
    } else if checkpoint.rollback_conflicted {
        CheckpointStatus::RollbackConflict
    } else if checkpoint.transaction_active {
        CheckpointStatus::Active
    } else {
        CheckpointStatus::Committed
    }
}

fn prepared_change_is_unchanged(change: &PreparedChange) -> bool {
    match (&change.current, &change.target) {
        (Some(current), Some((target, _))) => current == target,
        (None, None) => true,
        _ => false,
    }
}

fn validate_rollback_changes(
    manifest: &Manifest,
    checkpoint_index: usize,
    changes: &[PreparedChange],
) -> Result<()> {
    let checkpoint = &manifest.checkpoints[checkpoint_index];
    for change in changes {
        let expected = checkpoint.post_edit.get(&change.relative);
        let current_matches_target = match (&change.current, &change.target) {
            (None, None) => true,
            (Some(current), Some((target, _))) => current == target,
            _ => false,
        };
        let current_matches_post = expected.is_some_and(|states| {
            states.iter().any(|expected| {
                expected.exists == change.current.is_some()
                    && expected.bytes
                        == change
                            .current
                            .as_ref()
                            .map_or(0, |bytes| bytes.len() as u64)
                    && expected.checksum == checksum(change.current.as_deref().unwrap_or_default())
            })
        });
        if !current_matches_target && !current_matches_post {
            return Err(RollbackConflict {
                checkpoint_id: checkpoint.id,
                conflicting_checkpoint_id: None,
                path: PathBuf::from(&change.relative),
                kind: RollbackConflictKind::ConcurrentModification,
            }
            .into());
        }
        if current_matches_target {
            continue;
        }

        let target_state = expected_state_for_target(&change.target);
        for other in &manifest.checkpoints {
            if other.id == checkpoint.id
                || other.rolled_back
                || checkpoints_are_related(checkpoint, other)
                || !other.touched_files.contains(&change.relative)
            {
                continue;
            }
            let preserves_other = other.post_edit.get(&change.relative).is_some_and(|states| {
                !states.is_empty() && states.iter().all(|state| state == &target_state)
            });
            if !preserves_other {
                return Err(RollbackConflict {
                    checkpoint_id: checkpoint.id,
                    conflicting_checkpoint_id: Some(other.id),
                    path: PathBuf::from(&change.relative),
                    kind: RollbackConflictKind::AmbiguousTransactionOverlap,
                }
                .into());
            }
        }
    }
    Ok(())
}

fn expected_state_for_target(target: &Option<(Vec<u8>, u32)>) -> ExpectedFileState {
    let bytes = target.as_ref().map(|(bytes, _)| bytes.as_slice());
    ExpectedFileState {
        exists: bytes.is_some(),
        bytes: bytes.map_or(0, |bytes| bytes.len() as u64),
        checksum: checksum(bytes.unwrap_or_default()),
    }
}

fn checkpoints_are_related(first: &Checkpoint, second: &Checkpoint) -> bool {
    first.ancestor_ids.contains(&second.id) || second.ancestor_ids.contains(&first.id)
}

fn first_version_after<'a>(
    manifest: &'a Manifest,
    checkpoint_index: usize,
    relative: &str,
) -> Option<&'a FileVersion> {
    manifest
        .checkpoints
        .iter()
        .skip(checkpoint_index.saturating_add(1))
        .filter_map(|checkpoint| checkpoint.files.get(relative))
        .next()
}

fn referenced_blobs(manifest: &Manifest) -> BTreeSet<String> {
    manifest
        .checkpoints
        .iter()
        .flat_map(|checkpoint| checkpoint.files.values())
        .filter_map(|version| version.blob.clone())
        .collect()
}

fn canonical_workspace(workspace: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(workspace)
        .with_context(|| format!("工作区不存在: {}", workspace.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("工作区必须是非 symlink 目录")
    }
    fs::canonicalize(workspace).with_context(|| format!("无法解析工作区 {}", workspace.display()))
}

fn ensure_private_child(parent: &Path, name: &str) -> Result<PathBuf> {
    if name.is_empty()
        || Path::new(name).components().count() != 1
        || !matches!(
            Path::new(name).components().next(),
            Some(Component::Normal(_))
        )
    {
        bail!("私有目录名称无效")
    }
    let child = parent.join(name);
    match fs::symlink_metadata(&child) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("拒绝使用 symlink 私有目录: {}", child.display())
        }
        Ok(metadata) if !metadata.is_dir() => {
            bail!("私有路径不是目录: {}", child.display())
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(&child)
                .with_context(|| format!("无法创建私有目录 {}", child.display()))?;
        }
        Err(error) => return Err(error.into()),
    }
    set_private_directory_permissions(&child)?;
    let canonical = fs::canonicalize(&child)?;
    if !canonical.starts_with(parent) {
        bail!("私有目录越过存储根目录")
    }
    Ok(canonical)
}

fn set_private_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<String> {
    if path.as_os_str().is_empty() || path.as_os_str().as_encoded_bytes().len() > MAX_PATH_BYTES {
        bail!("file-history 相对路径为空或超过 {MAX_PATH_BYTES} 字节限制")
    }
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            bail!("file-history 路径必须是规范相对路径")
        }
    }
    path.to_str()
        .map(str::to_owned)
        .context("file-history 路径必须是 UTF-8")
}

fn resolve_workspace_candidate(workspace: &Path, candidate: &Path) -> Result<PathBuf> {
    if let Ok(metadata) = fs::symlink_metadata(candidate) {
        if metadata.file_type().is_symlink() {
            bail!("拒绝追踪 symlink: {}", candidate.display())
        }
        let canonical = fs::canonicalize(candidate)?;
        if !canonical.starts_with(workspace) {
            bail!("file-history 路径越过工作区")
        }
        return Ok(canonical);
    }
    let parent = candidate.parent().context("file-history 路径缺少父目录")?;
    let name = candidate
        .file_name()
        .context("file-history 路径缺少文件名")?;
    let parent = resolve_workspace_directory(workspace, parent)?;
    let resolved = parent.join(name);
    if !resolved.starts_with(workspace) {
        bail!("file-history 路径越过工作区")
    }
    Ok(resolved)
}

fn resolve_workspace_directory(workspace: &Path, directory: &Path) -> Result<PathBuf> {
    if directory.exists() {
        let metadata = fs::symlink_metadata(directory)?;
        if metadata.file_type().is_symlink() {
            bail!("file-history 父目录不能是 symlink")
        }
        let canonical = fs::canonicalize(directory)?;
        if !canonical.starts_with(workspace) || !canonical.is_dir() {
            bail!("file-history 父目录越过工作区或不是目录")
        }
        return Ok(canonical);
    }
    let parent = directory.parent().context("file-history 父目录无法解析")?;
    let name = directory
        .file_name()
        .context("file-history 父目录缺少名称")?;
    Ok(resolve_workspace_directory(workspace, parent)?.join(name))
}

fn ensure_workspace_directory(workspace: &Path, directory: &Path) -> Result<()> {
    let relative = directory
        .strip_prefix(workspace)
        .context("恢复目录越过工作区")?;
    let mut current = workspace.to_path_buf();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            bail!("恢复目录不是规范相对路径")
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("恢复目录包含 symlink: {}", current.display())
            }
            Ok(metadata) if !metadata.is_dir() => {
                bail!("恢复路径父级不是目录: {}", current.display())
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn read_optional_workspace_file(path: &Path) -> Result<Option<Vec<u8>>> {
    Ok(read_optional_workspace_file_with_mode(path)?.map(|(bytes, _)| bytes))
}

fn read_optional_workspace_file_with_mode(path: &Path) -> Result<Option<(Vec<u8>, u32)>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("file-history 只追踪非 symlink 普通文件: {}", path.display())
    }
    if metadata.len() > MAX_FILE_BYTES {
        bail!("文件超过 {MAX_FILE_BYTES} 字节 checkpoint 限制")
    }
    let bytes = read_bounded_regular(path, MAX_FILE_BYTES)?;
    #[cfg(unix)]
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o777
    };
    #[cfg(not(unix))]
    let mode = 0;
    Ok(Some((bytes, mode)))
}

fn read_bounded_regular(path: &Path, limit: u64) -> Result<Vec<u8>> {
    reject_symlink(path)?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > limit {
        bail!("文件不是普通文件或超过 {limit} 字节限制")
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!("文件超过 {limit} 字节限制")
    }
    Ok(bytes)
}

fn reject_symlink(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        bail!("拒绝 symlink 路径: {}", path.display())
    }
    Ok(())
}

fn remove_regular_file(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            bail!("拒绝删除 symlink 或非普通文件: {}", path.display())
        }
        Ok(_) => fs::remove_file(path)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn atomic_write_private_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("私有文件缺少父目录")?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        bail!("私有文件父目录无效")
    }
    reject_symlink(path)?;
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
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        set_private_file_permissions(path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.with_context(|| format!("无法原子写入私有文件 {}", path.display()))
}

fn validate_blob_name(name: &str) -> Result<()> {
    let Some(stem) = name.strip_suffix(".blob") else {
        bail!("file-history blob 名称无效")
    };
    let _: Uuid = stem.parse().context("file-history blob 名称无效")?;
    Ok(())
}

fn checksum(bytes: &[u8]) -> String {
    const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
    const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;
    let hash = bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u128::from(*byte)).wrapping_mul(PRIME)
    });
    format!("{hash:032x}")
}

fn line_diff_counts(current: &[u8], target: &[u8]) -> Result<(usize, usize)> {
    const MAX_DIFF_LINES: usize = 1_000_000;
    const MAX_DIFF_STEPS: usize = 50_000_000;
    let current = String::from_utf8_lossy(current);
    let target = String::from_utf8_lossy(target);
    let current = current.lines().collect::<Vec<_>>();
    let target = target.lines().collect::<Vec<_>>();
    if current.len().saturating_add(target.len()) > MAX_DIFF_LINES {
        bail!("file-history diff 超过 {MAX_DIFF_LINES} 行限制")
    }
    let prefix = current
        .iter()
        .zip(&target)
        .take_while(|(left, right)| left == right)
        .count();
    let suffix = current[prefix..]
        .iter()
        .rev()
        .zip(target[prefix..].iter().rev())
        .take_while(|(left, right)| left == right)
        .count();
    let current = &current[prefix..current.len().saturating_sub(suffix)];
    let target = &target[prefix..target.len().saturating_sub(suffix)];
    let maximum = current.len().saturating_add(target.len());
    if maximum == 0 {
        return Ok((0, 0));
    }
    let offset = maximum as isize;
    let mut frontier = vec![0_isize; maximum.saturating_mul(2).saturating_add(3)];
    frontier[(offset + 1) as usize] = 0;
    let mut steps = 0_usize;
    for distance in 0..=maximum {
        let distance = distance as isize;
        let mut diagonal = -distance;
        while diagonal <= distance {
            steps = steps.saturating_add(1);
            if steps > MAX_DIFF_STEPS {
                bail!("file-history diff 超过 {MAX_DIFF_STEPS} 步限制")
            }
            let index = (offset + diagonal) as usize;
            let mut x = if diagonal == -distance
                || (diagonal != distance && frontier[index - 1] < frontier[index + 1])
            {
                frontier[index + 1]
            } else {
                frontier[index - 1] + 1
            };
            let mut y = x - diagonal;
            while x < current.len() as isize
                && y < target.len() as isize
                && current[x as usize] == target[y as usize]
            {
                x += 1;
                y += 1;
                steps = steps.saturating_add(1);
                if steps > MAX_DIFF_STEPS {
                    bail!("file-history diff 超过 {MAX_DIFF_STEPS} 步限制")
                }
            }
            frontier[index] = x;
            if x >= current.len() as isize && y >= target.len() as isize {
                let distance = distance as usize;
                let common = maximum.saturating_sub(distance) / 2;
                return Ok((
                    target.len().saturating_sub(common),
                    current.len().saturating_sub(common),
                ));
            }
            diagonal += 2;
        }
    }
    bail!("file-history diff 无法收敛")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history() -> (tempfile::TempDir, tempfile::TempDir, FileHistory) {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let history =
            FileHistory::create_in(workspace.path(), Uuid::new_v4(), storage.path(), true).unwrap();
        (workspace, storage, history)
    }

    fn begin_parent_and_siblings(history: &FileHistory) -> (Uuid, Uuid, Uuid) {
        let parent = Uuid::new_v4();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        history
            .checkpoint(parent, CheckpointBoundary::UserMessage, 0)
            .unwrap();
        history
            .checkpoint_with_ancestors(first, CheckpointBoundary::Turn, 0, &[parent])
            .unwrap();
        history
            .checkpoint_with_ancestors(second, CheckpointBoundary::Turn, 0, &[parent])
            .unwrap();
        (parent, first, second)
    }

    fn prepare_edit(history: &FileHistory, checkpoints: &[Uuid], path: &Path, bytes: &[u8]) {
        for checkpoint in checkpoints {
            history.track_before_edit(*checkpoint, path).unwrap();
        }
        for checkpoint in checkpoints {
            history.expect_after_edit(*checkpoint, path, bytes).unwrap();
        }
    }

    fn checkpoint_status(history: &FileHistory, id: Uuid) -> CheckpointStatus {
        history
            .checkpoints()
            .unwrap()
            .into_iter()
            .find(|checkpoint| checkpoint.id == id)
            .unwrap()
            .status
    }

    #[test]
    fn checkpoints_diff_and_rewind_existing_file() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("src.txt");
        fs::write(&path, "one\n").unwrap();
        let first = Uuid::new_v4();
        history
            .checkpoint(first, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.track_before_edit(first, &path).unwrap();
        fs::write(&path, "two\nthree\n").unwrap();

        let stats = history.diff_stats(first).unwrap();
        assert_eq!(stats.files_changed, vec![PathBuf::from("src.txt")]);
        assert_eq!((stats.insertions, stats.deletions), (1, 2));
        let report = history.rewind(first).unwrap();
        assert_eq!(report.restored, 1);
        assert_eq!(fs::read_to_string(path).unwrap(), "one\n");
    }

    #[test]
    fn diff_stats_preserve_noncontiguous_common_lines() {
        assert_eq!(
            line_diff_counts(b"a\nx\nb\ny\nc\n", b"a\nb\nc\n").unwrap(),
            (0, 2)
        );
    }

    #[test]
    fn rewind_deletes_file_that_was_created_after_checkpoint() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("created.txt");
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::Turn, 2)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        fs::write(&path, "created").unwrap();
        assert_eq!(history.rewind(checkpoint).unwrap().deleted, 1);
        assert!(!path.exists());
    }

    #[test]
    fn reopen_rolls_back_an_orphaned_active_transaction() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let path = workspace.path().join("crash.txt");
        fs::write(&path, "before").unwrap();
        let history =
            FileHistory::create_in(workspace.path(), session_id, storage.path(), true).unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        history
            .expect_after_edit(checkpoint, &path, b"written-before-crash")
            .unwrap();
        fs::write(&path, "written-before-crash").unwrap();
        drop(history);

        let reopened =
            FileHistory::create_in(workspace.path(), session_id, storage.path(), true).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "before");
        assert_eq!(
            checkpoint_status(&reopened, checkpoint),
            CheckpointStatus::RolledBack
        );
    }

    #[test]
    fn reopen_never_rolls_back_a_committed_checkpoint() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let path = workspace.path().join("committed.txt");
        fs::write(&path, "before").unwrap();
        let history =
            FileHistory::create_in(workspace.path(), session_id, storage.path(), true).unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        history
            .expect_after_edit(checkpoint, &path, b"committed")
            .unwrap();
        fs::write(&path, "committed").unwrap();
        history.finish_transaction(checkpoint).unwrap();
        drop(history);

        let reopened =
            FileHistory::create_in(workspace.path(), session_id, storage.path(), true).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "committed");
        assert_eq!(
            checkpoint_status(&reopened, checkpoint),
            CheckpointStatus::Committed
        );
    }

    #[test]
    fn reopen_refuses_to_overwrite_an_external_edit_after_a_crash() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let session_id = Uuid::new_v4();
        let path = workspace.path().join("conflict.txt");
        fs::write(&path, "before").unwrap();
        let history =
            FileHistory::create_in(workspace.path(), session_id, storage.path(), true).unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        history
            .expect_after_edit(checkpoint, &path, b"transaction-write")
            .unwrap();
        fs::write(&path, "external-write").unwrap();

        let error =
            FileHistory::create_in(workspace.path(), session_id, storage.path(), true).unwrap_err();
        assert!(error.to_string().contains("无法安全"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "external-write");
        assert_eq!(
            checkpoint_status(&history, checkpoint),
            CheckpointStatus::RollbackConflict
        );
    }

    #[test]
    fn rewind_uses_first_later_snapshot_for_preexisting_file() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("later-tracked.txt");
        fs::write(&path, "before").unwrap();
        let early = Uuid::new_v4();
        history
            .checkpoint(early, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.finish_transaction(early).unwrap();

        let later = Uuid::new_v4();
        history
            .checkpoint(later, CheckpointBoundary::UserMessage, 2)
            .unwrap();
        history.track_before_edit(later, &path).unwrap();
        history.expect_after_edit(later, &path, b"after").unwrap();
        fs::write(&path, "after").unwrap();

        let report = history.rewind(early).unwrap();
        assert_eq!(report.restored, 1);
        assert_eq!(fs::read_to_string(path).unwrap(), "before");
    }

    #[test]
    fn transactional_rollback_only_restores_files_touched_by_its_actor() {
        let (workspace, _storage, history) = history();
        let owned = workspace.path().join("owned.txt");
        let background = workspace.path().join("background.txt");
        fs::write(&owned, "owned-before").unwrap();
        fs::write(&background, "background-before").unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 0)
            .unwrap();
        history.track_before_edit(checkpoint, &owned).unwrap();
        history
            .expect_after_edit(checkpoint, &owned, b"owned-after")
            .unwrap();
        fs::write(&owned, "owned-after").unwrap();
        fs::write(&background, "background-after").unwrap();

        let report = history.rollback_checkpoint(checkpoint).unwrap();
        assert_eq!(report.restored, 1);
        assert_eq!(fs::read_to_string(owned).unwrap(), "owned-before");
        assert_eq!(fs::read_to_string(background).unwrap(), "background-after");
    }

    #[test]
    fn transactional_rollback_refuses_to_clobber_a_later_writer() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("shared.txt");
        fs::write(&path, "before").unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 0)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        history
            .expect_after_edit(checkpoint, &path, b"root-write")
            .unwrap();
        fs::write(&path, "root-write").unwrap();
        fs::write(&path, "background-write").unwrap();

        let error = history.rollback_checkpoint(checkpoint).unwrap_err();
        assert!(format!("{error:#}").contains("并发修改"));
        assert_eq!(fs::read_to_string(path).unwrap(), "background-write");
    }

    #[test]
    fn transactional_rollback_accepts_the_last_successful_state_after_a_failed_rewrite() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("repeated.txt");
        fs::write(&path, "before").unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 0)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        history
            .expect_after_edit(checkpoint, &path, b"first-write")
            .unwrap();
        fs::write(&path, "first-write").unwrap();

        // The second atomic replacement never commits. Its prepared state must
        // not make the already successful first write look like a foreign edit.
        history
            .expect_after_edit(checkpoint, &path, b"failed-second-write")
            .unwrap();
        history.rollback_checkpoint(checkpoint).unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), "before");
    }

    #[test]
    fn concurrent_checkpoints_track_and_rollback_independent_files() {
        let (workspace, _storage, history) = history();
        let root_path = workspace.path().join("root.txt");
        let background_path = workspace.path().join("background.txt");
        fs::write(&root_path, "root-before").unwrap();
        fs::write(&background_path, "background-before").unwrap();
        let root = Uuid::new_v4();
        let background = Uuid::new_v4();
        history
            .checkpoint(root, CheckpointBoundary::UserMessage, 0)
            .unwrap();
        history
            .checkpoint(background, CheckpointBoundary::Turn, 0)
            .unwrap();

        history.track_before_edit(root, &root_path).unwrap();
        history
            .expect_after_edit(root, &root_path, b"root-after")
            .unwrap();
        fs::write(&root_path, "root-after").unwrap();
        history
            .track_before_edit(background, &background_path)
            .unwrap();
        history
            .expect_after_edit(background, &background_path, b"background-after")
            .unwrap();
        fs::write(&background_path, "background-after").unwrap();

        history.rollback_checkpoint(background).unwrap();
        assert_eq!(fs::read_to_string(&root_path).unwrap(), "root-after");
        assert_eq!(
            fs::read_to_string(&background_path).unwrap(),
            "background-before"
        );
        history.rollback_checkpoint(root).unwrap();
        assert_eq!(fs::read_to_string(root_path).unwrap(), "root-before");
    }

    #[test]
    fn sibling_rollback_refuses_ambiguous_existing_file_overlap() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("shared.txt");
        fs::write(&path, "original").unwrap();
        let (parent, first, second) = begin_parent_and_siblings(&history);

        // Both siblings capture the same pre-image before either write. There
        // is no safe way for the second rollback to know whether restoring
        // `original` would erase the first sibling's live update.
        for checkpoint in [parent, first, second] {
            history.track_before_edit(checkpoint, &path).unwrap();
        }
        for checkpoint in [parent, first] {
            history
                .expect_after_edit(checkpoint, &path, b"first")
                .unwrap();
        }
        fs::write(&path, "first").unwrap();
        for checkpoint in [parent, second] {
            history
                .expect_after_edit(checkpoint, &path, b"second")
                .unwrap();
        }
        fs::write(&path, "second").unwrap();

        let error = history.rollback_checkpoint(second).unwrap_err();
        let conflict = error.downcast_ref::<RollbackConflict>().unwrap();
        assert_eq!(
            conflict.kind,
            RollbackConflictKind::AmbiguousTransactionOverlap
        );
        assert_eq!(conflict.conflicting_checkpoint_id, Some(first));
        assert_eq!(
            checkpoint_status(&history, second),
            CheckpointStatus::RollbackConflict
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "second");

        // The common parent owns both child writes and remains able to restore
        // the turn pre-image without treating its descendants as foreign.
        history.rollback_checkpoint(parent).unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), "original");
    }

    #[test]
    fn sibling_rollbacks_can_retry_safely_in_reverse_failure_order() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("shared.txt");
        fs::write(&path, "original").unwrap();
        let (parent, first, second) = begin_parent_and_siblings(&history);

        prepare_edit(&history, &[parent, first], &path, b"first");
        fs::write(&path, "first").unwrap();
        prepare_edit(&history, &[parent, second], &path, b"second");
        fs::write(&path, "second").unwrap();

        let error = history.rollback_checkpoint(first).unwrap_err();
        assert_eq!(
            error.downcast_ref::<RollbackConflict>().unwrap().kind,
            RollbackConflictKind::ConcurrentModification
        );
        history.finish_transaction(first).unwrap();
        assert_eq!(
            checkpoint_status(&history, first),
            CheckpointStatus::RollbackConflict
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "second");

        history.rollback_checkpoint(second).unwrap();
        history.finish_transaction(second).unwrap();
        assert_eq!(
            checkpoint_status(&history, second),
            CheckpointStatus::RolledBack
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "first");

        // A conflicted checkpoint is diagnostic, not silently completed. Once
        // the later sibling has rolled back, an explicit retry is provably safe.
        history.rollback_checkpoint(first).unwrap();
        assert_eq!(
            checkpoint_status(&history, first),
            CheckpointStatus::RolledBack
        );
        assert_eq!(fs::read_to_string(path).unwrap(), "original");
    }

    #[test]
    fn sibling_rollback_restores_committed_predecessor_for_existing_file() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("shared.txt");
        fs::write(&path, "original").unwrap();
        let (parent, first, second) = begin_parent_and_siblings(&history);

        prepare_edit(&history, &[parent, first], &path, b"first");
        fs::write(&path, "first").unwrap();
        history.finish_transaction(first).unwrap();
        prepare_edit(&history, &[parent, second], &path, b"second");
        fs::write(&path, "second").unwrap();

        history.rollback_checkpoint(second).unwrap();
        assert_eq!(
            checkpoint_status(&history, first),
            CheckpointStatus::Committed
        );
        assert_eq!(
            checkpoint_status(&history, second),
            CheckpointStatus::RolledBack
        );
        assert_eq!(fs::read_to_string(path).unwrap(), "first");
    }

    #[test]
    fn sibling_rollback_never_deletes_an_ambiguous_created_file() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("created.txt");
        let (parent, first, second) = begin_parent_and_siblings(&history);

        for checkpoint in [parent, first, second] {
            history.track_before_edit(checkpoint, &path).unwrap();
        }
        for checkpoint in [parent, first] {
            history
                .expect_after_edit(checkpoint, &path, b"first")
                .unwrap();
        }
        fs::write(&path, "first").unwrap();
        for checkpoint in [parent, second] {
            history
                .expect_after_edit(checkpoint, &path, b"second")
                .unwrap();
        }
        fs::write(&path, "second").unwrap();

        let error = history.rollback_checkpoint(second).unwrap_err();
        assert_eq!(
            error.downcast_ref::<RollbackConflict>().unwrap().kind,
            RollbackConflictKind::AmbiguousTransactionOverlap
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "second");
        history.rollback_checkpoint(parent).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn sibling_rollback_restores_predecessor_for_created_file() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("created.txt");
        let (parent, first, second) = begin_parent_and_siblings(&history);

        prepare_edit(&history, &[parent, first], &path, b"first");
        fs::write(&path, "first").unwrap();
        prepare_edit(&history, &[parent, second], &path, b"second");
        fs::write(&path, "second").unwrap();

        history.rollback_checkpoint(second).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "first");
        history.rollback_checkpoint(first).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn sibling_success_finish_order_does_not_change_last_writer() {
        for reverse_finish in [false, true] {
            let (workspace, _storage, history) = history();
            let path = workspace.path().join("shared.txt");
            fs::write(&path, "original").unwrap();
            let (parent, first, second) = begin_parent_and_siblings(&history);
            prepare_edit(&history, &[parent, first], &path, b"first");
            fs::write(&path, "first").unwrap();
            prepare_edit(&history, &[parent, second], &path, b"second");
            fs::write(&path, "second").unwrap();

            let order = if reverse_finish {
                [second, first]
            } else {
                [first, second]
            };
            for checkpoint in order {
                history.finish_transaction(checkpoint).unwrap();
            }
            assert_eq!(
                checkpoint_status(&history, first),
                CheckpointStatus::Committed
            );
            assert_eq!(
                checkpoint_status(&history, second),
                CheckpointStatus::Committed
            );
            assert_eq!(fs::read_to_string(path).unwrap(), "second");
        }
    }

    #[test]
    fn transaction_ancestry_and_conflict_status_survive_failed_recovery() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let session = Uuid::new_v4();
        let history =
            FileHistory::create_in(workspace.path(), session, storage.path(), true).unwrap();
        let path = workspace.path().join("shared.txt");
        fs::write(&path, "original").unwrap();
        let (parent, first, second) = begin_parent_and_siblings(&history);
        for checkpoint in [parent, first, second] {
            history.track_before_edit(checkpoint, &path).unwrap();
        }
        for checkpoint in [parent, first] {
            history
                .expect_after_edit(checkpoint, &path, b"first")
                .unwrap();
        }
        fs::write(&path, "first").unwrap();
        for checkpoint in [parent, second] {
            history
                .expect_after_edit(checkpoint, &path, b"second")
                .unwrap();
        }
        fs::write(&path, "second").unwrap();
        history.validate_rollback(second).unwrap_err();

        assert!(FileHistory::create_in(workspace.path(), session, storage.path(), true).is_err());
        let checkpoints = history.checkpoints().unwrap();
        let second = checkpoints
            .iter()
            .find(|checkpoint| checkpoint.id == second)
            .unwrap();
        assert_eq!(second.ancestor_ids, vec![parent]);
        assert_eq!(second.status, CheckpointStatus::RollbackConflict);
        assert_eq!(fs::read_to_string(path).unwrap(), "second");
    }

    #[test]
    fn cloned_histories_serialize_manifest_updates() {
        let (workspace, _storage, history) = history();
        let first_path = workspace.path().join("first.txt");
        let second_path = workspace.path().join("second.txt");
        fs::write(&first_path, "first-before").unwrap();
        fs::write(&second_path, "second-before").unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::Turn, 1)
            .unwrap();

        let first_history = history.clone();
        let first = std::thread::spawn(move || {
            first_history
                .track_before_edit(checkpoint, &first_path)
                .unwrap();
            fs::write(first_path, "first-after").unwrap();
        });
        let second_history = history.clone();
        let second = std::thread::spawn(move || {
            second_history
                .track_before_edit(checkpoint, &second_path)
                .unwrap();
            fs::write(second_path, "second-after").unwrap();
        });
        first.join().unwrap();
        second.join().unwrap();

        assert_eq!(history.checkpoints().unwrap()[0].tracked_files, 2);
        assert_eq!(history.rewind(checkpoint).unwrap().restored, 2);
        assert_eq!(
            fs::read_to_string(workspace.path().join("first.txt")).unwrap(),
            "first-before"
        );
        assert_eq!(
            fs::read_to_string(workspace.path().join("second.txt")).unwrap(),
            "second-before"
        );
    }

    #[test]
    fn unchanged_versions_reuse_one_blob() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("same.txt");
        fs::write(&path, "same").unwrap();
        let first = Uuid::new_v4();
        history
            .checkpoint(first, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.track_before_edit(first, &path).unwrap();
        history
            .checkpoint(Uuid::new_v4(), CheckpointBoundary::Turn, 2)
            .unwrap();
        assert_eq!(fs::read_dir(history.blobs_directory()).unwrap().count(), 1);
    }

    #[test]
    fn fork_preserves_checkpoints_and_backups() {
        let (workspace, _storage, history) = history();
        let path = workspace.path().join("fork.txt");
        fs::write(&path, "before").unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        fs::write(&path, "after").unwrap();
        let fork = history.fork(Uuid::new_v4()).unwrap();
        assert!(fork.can_rewind(checkpoint).unwrap());
        assert!(
            fork.load_manifest()
                .unwrap()
                .checkpoints
                .iter()
                .all(|checkpoint| !checkpoint.transaction_active)
        );
        fork.rewind(checkpoint).unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), "before");
    }

    #[test]
    fn corrupt_manifest_and_blob_fail_closed() {
        let (workspace, _storage, history) = history();
        fs::write(history.manifest_path(), b"not-json").unwrap();
        assert!(
            history
                .checkpoints()
                .unwrap_err()
                .to_string()
                .contains("损坏")
        );

        fs::remove_file(history.manifest_path()).unwrap();
        let path = workspace.path().join("blob.txt");
        fs::write(&path, "before").unwrap();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        let blob = fs::read_dir(history.blobs_directory())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        fs::write(blob, "corrupt").unwrap();
        fs::write(&path, "after").unwrap();
        assert!(history.rewind(checkpoint).is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), "after");
    }

    #[test]
    fn rejects_outside_workspace_and_oversized_files() {
        let (workspace, storage, history) = history();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        let outside = storage.path().join("outside.txt");
        fs::write(&outside, "outside").unwrap();
        assert!(history.track_before_edit(checkpoint, &outside).is_err());

        let large = workspace.path().join("large.bin");
        fs::File::create(&large)
            .unwrap()
            .set_len(MAX_FILE_BYTES + 1)
            .unwrap();
        assert!(history.track_before_edit(checkpoint, &large).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_escape_and_uses_private_permissions() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let (workspace, storage, history) = history();
        let checkpoint = Uuid::new_v4();
        history
            .checkpoint(checkpoint, CheckpointBoundary::UserMessage, 1)
            .unwrap();
        let outside = storage.path().join("private.txt");
        fs::write(&outside, "private").unwrap();
        let link = workspace.path().join("link.txt");
        symlink(&outside, &link).unwrap();
        assert!(history.track_before_edit(checkpoint, &link).is_err());
        assert_eq!(
            fs::metadata(&history.directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );

        fs::remove_file(link).unwrap();
        let path = workspace.path().join("mode.txt");
        fs::write(&path, "mode").unwrap();
        history.track_before_edit(checkpoint, &path).unwrap();
        let blob = fs::read_dir(history.blobs_directory())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        assert_eq!(
            fs::metadata(blob).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(history.manifest_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn checkpoint_count_is_bounded() {
        let (_workspace, _storage, history) = history();
        for index in 0..=MAX_CHECKPOINTS {
            let id = Uuid::new_v4();
            history
                .checkpoint(id, CheckpointBoundary::Turn, index)
                .unwrap();
            history.finish_transaction(id).unwrap();
        }
        assert_eq!(history.checkpoints().unwrap().len(), MAX_CHECKPOINTS);
    }
}

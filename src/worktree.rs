use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    sync::Mutex,
    time::timeout,
};
use uuid::Uuid;

use crate::{
    config::Settings,
    process::{SecretEnvScrubber, spawn_managed},
    session::SessionWorkspaceState,
    tools::{
        Tool, ToolContext, ToolOutput, ensure_private_directory, object_schema, workspace_key,
    },
};

const MAX_GIT_OUTPUT_BYTES: usize = 256 * 1024;
const GIT_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_WORKTREE_NAME_BYTES: usize = 64;
const MAX_AGENT_WORKTREES_PER_REPOSITORY: usize = 64;
const MAX_AGENT_WORKTREE_STORAGE_ENTRIES: usize = 128;

/// A worktree owned by one Agent invocation.  Unlike `WorktreeSession`, this
/// never changes process-global/root-session state; only the forked
/// `ToolContext` is relocated into it.
#[derive(Debug)]
pub(crate) struct AgentWorktree {
    repo_root: PathBuf,
    root: PathBuf,
    cwd: PathBuf,
    branch: String,
    base_commit: String,
    newly_created: bool,
    cleanup_on_drop: bool,
    secret_env_scrubber: SecretEnvScrubber,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AgentWorktreeDisposition {
    Removed,
    Kept {
        branch: String,
        base_commit: String,
        display_path: String,
    },
}

impl AgentWorktree {
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub(crate) fn branch(&self) -> &str {
        &self.branch
    }

    /// Releases a worktree that was created but never handed to an agent.
    /// A restored worktree is never removed by launch-admission cleanup.
    pub(crate) async fn cleanup_unstarted(mut self) -> Result<()> {
        if !self.newly_created {
            self.cleanup_on_drop = false;
            return Ok(());
        }
        let result = remove_owned_agent_worktree(&self).await;
        // A failed exact-match cleanup is deliberately fail-closed. Do not
        // retry from Drop after the caller has received the cleanup error.
        self.cleanup_on_drop = false;
        result
    }

    /// Clean worktrees are disposable.  Changed worktrees are retained so a
    /// failed status/rollback check cannot destroy useful work; callers receive
    /// a stable branch plus a home-relative display path.
    pub(crate) async fn finish(mut self) -> Result<AgentWorktreeDisposition> {
        let changed = match agent_worktree_has_changes(&self).await {
            Ok(changed) => changed,
            Err(error) => {
                self.cleanup_on_drop = false;
                return Err(error);
            }
        };
        if changed {
            self.cleanup_on_drop = false;
            return Ok(AgentWorktreeDisposition::Kept {
                branch: self.branch.clone(),
                base_commit: self.base_commit.clone(),
                display_path: display_private_path(&self.root),
            });
        }
        let removal = remove_owned_agent_worktree(&self).await;
        self.cleanup_on_drop = false;
        removal?;
        Ok(AgentWorktreeDisposition::Removed)
    }
}

impl Drop for AgentWorktree {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }
        // Root-turn cancellation drops the in-flight Tool future. Schedule the
        // same change-aware, exact-registration cleanup so a clean isolated
        // checkout is not leaked, while dirty or unverifiable state is kept.
        let abandoned = Self {
            repo_root: self.repo_root.clone(),
            root: self.root.clone(),
            cwd: self.cwd.clone(),
            branch: self.branch.clone(),
            base_commit: self.base_commit.clone(),
            newly_created: self.newly_created,
            cleanup_on_drop: false,
            secret_env_scrubber: self.secret_env_scrubber.clone(),
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(async move {
                let _ = abandoned.finish().await;
            }));
        }
    }
}

pub struct WorktreeIntegration {
    pub deferred_tools: Vec<Arc<dyn Tool>>,
    manager: Arc<WorktreeManager>,
}

pub struct RestoredWorkspace {
    pub cwd: PathBuf,
    pub root: PathBuf,
}

impl WorktreeIntegration {
    pub async fn restore_session(
        &self,
        state: &SessionWorkspaceState,
    ) -> Result<Option<RestoredWorkspace>> {
        self.manager.restore_session(state).await
    }
}

#[derive(Debug, Clone, Copy)]
enum BaseRef {
    Fresh,
    Head,
}

#[derive(Debug, Clone)]
struct WorktreeSession {
    repo_root: PathBuf,
    worktree_root: PathBuf,
    original_cwd: PathBuf,
    branch: Option<String>,
    owned: bool,
}

struct WorktreeManager {
    original_cwd: PathBuf,
    storage_root: PathBuf,
    base_ref: BaseRef,
    state: Mutex<Option<WorktreeSession>>,
    secret_env_scrubber: SecretEnvScrubber,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnterInput {
    name: Option<String>,
    path: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExitInput {
    action: String,
    #[serde(default)]
    force: bool,
}

pub fn configure_worktree(settings: &Settings, cwd: &Path) -> Result<WorktreeIntegration> {
    let original_cwd = std::fs::canonicalize(cwd)
        .with_context(|| format!("无法解析 worktree launch cwd: {}", cwd.display()))?;
    let worktree = settings.raw.get("worktree");
    let base_ref = match worktree
        .and_then(|value| value.get("baseRef"))
        .and_then(Value::as_str)
        .unwrap_or("fresh")
    {
        "fresh" => BaseRef::Fresh,
        "head" => BaseRef::Head,
        value => bail!("worktree.baseRef 必须是 fresh 或 head，实际为 {value:?}"),
    };
    let storage_root = match worktree
        .and_then(|value| value.get("storageDirectory"))
        .and_then(Value::as_str)
    {
        Some("") => bail!("worktree.storageDirectory 不能为空"),
        Some(path) => {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                original_cwd.join(path)
            }
        }
        None => dirs::home_dir()
            .context("无法确定 worktree storage 主目录")?
            .join(".open-agent-harness/worktrees"),
    };
    let manager = Arc::new(WorktreeManager {
        original_cwd,
        storage_root,
        base_ref,
        state: Mutex::new(None),
        secret_env_scrubber: SecretEnvScrubber::from_settings(settings)?,
    });
    Ok(WorktreeIntegration {
        deferred_tools: vec![
            Arc::new(EnterWorktreeTool {
                manager: Arc::clone(&manager),
            }),
            Arc::new(ExitWorktreeTool {
                manager: Arc::clone(&manager),
            }),
        ],
        manager,
    })
}

/// Creates a bounded, harness-owned Git worktree for one subagent.  The launch
/// repository must be clean: otherwise an isolated checkout would silently
/// omit the user's uncommitted state and present a misleading workspace.
pub(crate) async fn create_agent_worktree(
    cwd: &Path,
    id: Uuid,
    secret_env_scrubber: SecretEnvScrubber,
) -> Result<AgentWorktree> {
    create_agent_worktree_with_scrubber(cwd, id, None, secret_env_scrubber).await
}

#[cfg(test)]
pub(crate) async fn create_agent_worktree_with_storage(
    cwd: &Path,
    id: Uuid,
    storage_root: Option<&Path>,
) -> Result<AgentWorktree> {
    create_agent_worktree_with_scrubber(cwd, id, storage_root, SecretEnvScrubber::default()).await
}

async fn create_agent_worktree_with_scrubber(
    cwd: &Path,
    id: Uuid,
    storage_root: Option<&Path>,
    secret_env_scrubber: SecretEnvScrubber,
) -> Result<AgentWorktree> {
    let launch_cwd = std::fs::canonicalize(cwd)
        .with_context(|| format!("无法解析 agent launch cwd: {}", cwd.display()))?;
    let repo_root = repository_root(&launch_cwd, &secret_env_scrubber).await?;
    let relative_cwd = launch_cwd
        .strip_prefix(&repo_root)
        .context("agent launch cwd 不在 Git repository root 内")?;
    let status = run_git(
        &repo_root,
        [
            OsString::from("status"),
            OsString::from("--porcelain"),
            OsString::from("--untracked-files=all"),
        ],
        &secret_env_scrubber,
    )
    .await?;
    if !status.status.success() {
        bail!(
            "无法检查 agent worktree 源仓库状态: {}",
            status.render_error()
        )
    }
    if !status.stdout.trim().is_empty() {
        bail!("agent worktree isolation 要求源仓库 clean；请先提交或暂存现有修改")
    }

    let base = run_git(
        &repo_root,
        [OsString::from("rev-parse"), OsString::from("HEAD")],
        &secret_env_scrubber,
    )
    .await?;
    if !base.status.success() {
        bail!(
            "无法解析 agent worktree base commit: {}",
            base.render_error()
        )
    }
    let base_commit = base.stdout.trim().to_owned();
    validate_git_oid(&base_commit)?;

    let parent = match storage_root {
        Some(root) => root.join(workspace_key(&repo_root)),
        None => agent_worktree_parent(&repo_root)?,
    };
    ensure_private_directory(&parent)?;
    let mut storage_entries = 0usize;
    let mut managed_count = 0usize;
    for entry in std::fs::read_dir(&parent)? {
        storage_entries = storage_entries.saturating_add(1);
        if storage_entries > MAX_AGENT_WORKTREE_STORAGE_ENTRIES {
            bail!("agent worktree storage 超过 {MAX_AGENT_WORKTREE_STORAGE_ENTRIES} 个 entry 限制")
        }
        let kind = entry?.file_type()?;
        managed_count += usize::from(kind.is_dir() || kind.is_symlink());
    }
    if managed_count >= MAX_AGENT_WORKTREES_PER_REPOSITORY {
        bail!(
            "agent worktree 达到每 repository {} 个资源上限",
            MAX_AGENT_WORKTREES_PER_REPOSITORY
        )
    }

    let suffix = id.simple().to_string();
    let target = parent.join(format!("agent-{suffix}"));
    if std::fs::symlink_metadata(&target).is_ok() {
        bail!(
            "agent worktree 目标已存在，拒绝覆盖: {}",
            display_private_path(&target)
        )
    }
    let branch = format!("open-agent/agent-{suffix}");
    let branch_check = run_git(
        &repo_root,
        [
            OsString::from("show-ref"),
            OsString::from("--verify"),
            OsString::from("--quiet"),
            OsString::from(format!("refs/heads/{branch}")),
        ],
        &secret_env_scrubber,
    )
    .await?;
    if branch_check.status.success() {
        bail!("agent worktree branch 已存在，拒绝复用: {branch}")
    }

    let created = run_git(
        &repo_root,
        [
            OsString::from("worktree"),
            OsString::from("add"),
            OsString::from("-b"),
            OsString::from(&branch),
            target.as_os_str().to_owned(),
            OsString::from(&base_commit),
        ],
        &secret_env_scrubber,
    )
    .await?;
    if !created.status.success() {
        cleanup_failed_worktree(&repo_root, &target, &branch, &secret_env_scrubber).await;
        bail!("git agent worktree add 失败: {}", created.render_error())
    }
    let root = match std::fs::canonicalize(&target) {
        Ok(root) => root,
        Err(error) => {
            cleanup_failed_worktree(&repo_root, &target, &branch, &secret_env_scrubber).await;
            return Err(error).context("无法解析新建 agent worktree");
        }
    };
    if let Err(error) = set_private_directory(&root) {
        cleanup_failed_worktree(&repo_root, &root, &branch, &secret_env_scrubber).await;
        return Err(error);
    }
    let work_cwd = corresponding_cwd(&root, relative_cwd);
    Ok(AgentWorktree {
        repo_root,
        root,
        cwd: work_cwd,
        branch,
        base_commit,
        newly_created: true,
        cleanup_on_drop: true,
        secret_env_scrubber,
    })
}

/// Restores only the exact managed worktree previously recorded for this
/// stable agent ID.  Merely finding a directory or a similarly named branch is
/// insufficient; it must still be registered by Git at the deterministic
/// harness-owned path.
pub(crate) async fn restore_agent_worktree(
    cwd: &Path,
    id: Uuid,
    branch: &str,
    base_commit: &str,
    secret_env_scrubber: SecretEnvScrubber,
) -> Result<AgentWorktree> {
    restore_agent_worktree_with_scrubber(cwd, id, branch, base_commit, None, secret_env_scrubber)
        .await
}

#[cfg(test)]
pub(crate) async fn restore_agent_worktree_with_storage(
    cwd: &Path,
    id: Uuid,
    branch: &str,
    base_commit: &str,
    storage_root: Option<&Path>,
) -> Result<AgentWorktree> {
    restore_agent_worktree_with_scrubber(
        cwd,
        id,
        branch,
        base_commit,
        storage_root,
        SecretEnvScrubber::default(),
    )
    .await
}

async fn restore_agent_worktree_with_scrubber(
    cwd: &Path,
    id: Uuid,
    branch: &str,
    base_commit: &str,
    storage_root: Option<&Path>,
    secret_env_scrubber: SecretEnvScrubber,
) -> Result<AgentWorktree> {
    let launch_cwd = std::fs::canonicalize(cwd)
        .with_context(|| format!("无法解析 agent resume cwd: {}", cwd.display()))?;
    let repo_root = repository_root(&launch_cwd, &secret_env_scrubber).await?;
    let relative_cwd = launch_cwd
        .strip_prefix(&repo_root)
        .context("agent resume cwd 不在 Git repository root 内")?;
    let suffix = id.simple().to_string();
    let expected_branch = format!("open-agent/agent-{suffix}");
    if branch != expected_branch {
        bail!("持久化 agent worktree branch 与 stable agent id 不匹配")
    }
    validate_git_oid(base_commit)?;
    let parent = match storage_root {
        Some(root) => root.join(workspace_key(&repo_root)),
        None => agent_worktree_parent(&repo_root)?,
    };
    ensure_private_directory(&parent)?;
    let expected = parent.join(format!("agent-{suffix}"));
    let records = registered_worktree_records(&repo_root, &secret_env_scrubber).await?;
    let record = records
        .into_iter()
        .find(|record| record.branch.as_deref() == Some(branch))
        .with_context(|| format!("agent {id} 的持久化 worktree 已不存在或未注册"))?;
    let expected = std::fs::canonicalize(&expected).with_context(|| {
        format!(
            "无法解析持久化 agent worktree: {}",
            display_private_path(&expected)
        )
    })?;
    if record.path != expected {
        bail!("持久化 agent worktree 不在 harness-owned 确定路径，拒绝恢复")
    }
    let work_cwd = corresponding_cwd(&record.path, relative_cwd);
    Ok(AgentWorktree {
        repo_root,
        root: record.path,
        cwd: work_cwd,
        branch: branch.to_owned(),
        base_commit: base_commit.to_owned(),
        newly_created: false,
        cleanup_on_drop: true,
        secret_env_scrubber,
    })
}

struct EnterWorktreeTool {
    manager: Arc<WorktreeManager>,
}

struct ExitWorktreeTool {
    manager: Arc<WorktreeManager>,
}

#[async_trait]
impl Tool for EnterWorktreeTool {
    fn name(&self) -> &str {
        "EnterWorktree"
    }

    fn description(&self) -> &str {
        "Creates an isolated Git worktree, or enters an existing registered worktree, and switches every local harness tool to that workspace. Use only when isolation is explicitly requested."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "name": {"type": "string", "minLength": 1, "maxLength": MAX_WORKTREE_NAME_BYTES},
                "path": {"type": "string", "minLength": 1, "maxLength": 16384}
            }),
            &[],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, _: &Value) -> bool {
        true
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("name")
            .or_else(|| input.get("path"))
            .and_then(Value::as_str)
            .unwrap_or("<worktree>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if context.agent_depth() > 0 {
            bail!("worktree transition 只能由 root agent 执行")
        }
        let input: EnterInput = serde_json::from_value(input)?;
        match (input.name, input.path) {
            (Some(name), None) => self.manager.create(context, &name).await,
            (None, Some(path)) => self.manager.enter_existing(context, &path).await,
            _ => bail!("EnterWorktree 必须且只能提供 name 或 path 之一"),
        }
    }
}

#[async_trait]
impl Tool for ExitWorktreeTool {
    fn name(&self) -> &str {
        "ExitWorktree"
    }

    fn description(&self) -> &str {
        "Returns all local harness tools to the launch workspace. It can keep a created worktree or remove a clean one; forced removal requires explicit permission."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "action": {"type": "string", "enum": ["keep", "remove"]},
                "force": {"type": "boolean"}
            }),
            &["action"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, input: &Value) -> bool {
        input.get("action").and_then(Value::as_str) == Some("remove")
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("<action>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if context.agent_depth() > 0 {
            bail!("worktree transition 只能由 root agent 执行")
        }
        let input: ExitInput = serde_json::from_value(input)?;
        self.manager.exit(context, &input.action, input.force).await
    }
}

impl WorktreeManager {
    async fn restore_session(
        &self,
        state: &SessionWorkspaceState,
    ) -> Result<Option<RestoredWorkspace>> {
        let Some(expected_key) = state.workspace_key.as_deref() else {
            return Ok(None);
        };
        validate_restored_relative_cwd(&state.cwd)?;
        let repo_root = repository_root(&self.original_cwd, &self.secret_env_scrubber).await?;
        let mut matches = registered_worktrees(&repo_root, &self.secret_env_scrubber)
            .await?
            .into_iter()
            .filter(|path| workspace_key(path) == expected_key)
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            bail!("session workspace 不再唯一匹配当前 repository 的已注册 Git worktree")
        }
        let worktree_root = matches.pop().expect("one registered worktree match");
        if worktree_root == repo_root {
            bail!("session workspace key 不能把 launch repository 冒充 worktree")
        }
        let requested_cwd = worktree_root.join(&state.cwd);
        let cwd = std::fs::canonicalize(&requested_cwd).with_context(|| {
            format!(
                "session worktree cwd 不存在或无法解析: {}",
                requested_cwd.display()
            )
        })?;
        if !cwd.is_dir() || !cwd.starts_with(&worktree_root) {
            bail!("session worktree cwd 越过已注册 worktree")
        }
        let mut active = self.state.lock().await;
        if active.is_some() {
            bail!("worktree session 已经恢复")
        }
        *active = Some(WorktreeSession {
            repo_root,
            worktree_root: worktree_root.clone(),
            original_cwd: self.original_cwd.clone(),
            branch: None,
            // Ownership is never trusted across process boundaries. A restored
            // worktree may be kept/exited, but not deleted by this session.
            owned: false,
        });
        Ok(Some(RestoredWorkspace {
            cwd,
            root: worktree_root,
        }))
    }

    async fn create(&self, context: &ToolContext, requested_name: &str) -> Result<ToolOutput> {
        context.reload_workspace_context().await?;
        let mut state = self.state.lock().await;
        if state.is_some() {
            bail!("当前 session 已经进入 worktree；请先调用 ExitWorktree")
        }
        let name = sanitize_name(requested_name)?;
        let repo_root = repository_root(&self.original_cwd, &self.secret_env_scrubber).await?;
        let relative_cwd = self
            .original_cwd
            .strip_prefix(&repo_root)
            .context("launch cwd 不在 Git repository root 内")?;
        let key = workspace_key(&repo_root);
        let parent = self.storage_root.join(key);
        ensure_private_directory(&parent)?;
        let suffix = Uuid::new_v4().simple().to_string()[..8].to_owned();
        let target = parent.join(format!("{name}-{suffix}"));
        if target.exists() {
            bail!("生成的 worktree path 已存在: {}", target.display())
        }
        let branch = format!("open-agent/{name}-{suffix}");
        let base = match self.base_ref {
            BaseRef::Head => "HEAD".to_owned(),
            BaseRef::Fresh => default_base_ref(&repo_root, &self.secret_env_scrubber).await?,
        };
        let output = run_git(
            &repo_root,
            [
                OsString::from("worktree"),
                OsString::from("add"),
                OsString::from("-b"),
                OsString::from(&branch),
                target.as_os_str().to_owned(),
                OsString::from(&base),
            ],
            &self.secret_env_scrubber,
        )
        .await?;
        if !output.status.success() {
            cleanup_failed_worktree(&repo_root, &target, &branch, &self.secret_env_scrubber).await;
            bail!("git worktree add 失败: {}", output.render_error())
        }
        let worktree_root = std::fs::canonicalize(&target)
            .with_context(|| format!("无法解析创建的 worktree: {}", target.display()))?;
        set_private_directory(&worktree_root)?;
        let work_cwd = corresponding_cwd(&worktree_root, relative_cwd);
        if let Err(error) = context
            .hooks()
            .run(
                "WorktreeCreate",
                None,
                json!({"worktree_path": &worktree_root, "branch": &branch}),
                &self.original_cwd,
            )
            .await
        {
            cleanup_failed_worktree(
                &repo_root,
                &worktree_root,
                &branch,
                &self.secret_env_scrubber,
            )
            .await;
            return Err(error);
        }
        if let Err(error) = relocate_context(context, work_cwd.clone(), worktree_root.clone()).await
        {
            let _ = relocate_context(context, self.original_cwd.clone(), repo_root.clone()).await;
            cleanup_failed_worktree(
                &repo_root,
                &worktree_root,
                &branch,
                &self.secret_env_scrubber,
            )
            .await;
            return Err(error);
        }
        if let Err(error) = context
            .hooks()
            .run(
                "CwdChanged",
                None,
                json!({"from": &self.original_cwd, "to": &work_cwd}),
                &work_cwd,
            )
            .await
        {
            let _ = relocate_context(context, self.original_cwd.clone(), repo_root.clone()).await;
            cleanup_failed_worktree(
                &repo_root,
                &worktree_root,
                &branch,
                &self.secret_env_scrubber,
            )
            .await;
            return Err(error);
        }
        *state = Some(WorktreeSession {
            repo_root,
            worktree_root: worktree_root.clone(),
            original_cwd: self.original_cwd.clone(),
            branch: Some(branch.clone()),
            owned: true,
        });
        Ok(ToolOutput::success(format!(
            "Entered isolated worktree\npath={}\nbranch={branch}\ncwd={}",
            worktree_root.display(),
            work_cwd.display()
        )))
    }

    async fn enter_existing(&self, context: &ToolContext, requested: &str) -> Result<ToolOutput> {
        context.reload_workspace_context().await?;
        let mut state = self.state.lock().await;
        if state.is_some() {
            bail!("当前 session 已经进入 worktree；请先调用 ExitWorktree")
        }
        let requested = PathBuf::from(requested);
        let requested = if requested.is_absolute() {
            requested
        } else {
            self.original_cwd.join(requested)
        };
        let requested = std::fs::canonicalize(&requested)
            .with_context(|| format!("无法解析 existing worktree: {}", requested.display()))?;
        let repo_root = repository_root(&self.original_cwd, &self.secret_env_scrubber).await?;
        let registered = registered_worktrees(&repo_root, &self.secret_env_scrubber).await?;
        if !registered.iter().any(|path| path == &requested) {
            bail!("目标 path 未注册为当前 repository 的 Git worktree")
        }
        let relative_cwd = self
            .original_cwd
            .strip_prefix(&repo_root)
            .context("launch cwd 不在 Git repository root 内")?;
        let work_cwd = corresponding_cwd(&requested, relative_cwd);
        if let Err(error) = relocate_context(context, work_cwd.clone(), requested.clone()).await {
            let _ = relocate_context(context, self.original_cwd.clone(), repo_root.clone()).await;
            return Err(error);
        }
        if let Err(error) = context
            .hooks()
            .run(
                "CwdChanged",
                None,
                json!({"from": &self.original_cwd, "to": &work_cwd}),
                &work_cwd,
            )
            .await
        {
            let _ = relocate_context(context, self.original_cwd.clone(), repo_root.clone()).await;
            return Err(error);
        }
        *state = Some(WorktreeSession {
            repo_root,
            worktree_root: requested.clone(),
            original_cwd: self.original_cwd.clone(),
            branch: None,
            owned: false,
        });
        Ok(ToolOutput::success(format!(
            "Entered existing worktree\npath={}\ncwd={}",
            requested.display(),
            work_cwd.display()
        )))
    }

    async fn exit(&self, context: &ToolContext, action: &str, force: bool) -> Result<ToolOutput> {
        if !matches!(action, "keep" | "remove") {
            bail!("ExitWorktree action 必须是 keep 或 remove")
        }
        let mut state = self.state.lock().await;
        let session = state.clone().context("当前 session 没有进入 worktree")?;
        if action == "remove" && !session.owned {
            bail!("不能通过 ExitWorktree 删除由外部创建的 existing worktree")
        }
        if action == "remove" && !force {
            let status = run_git(
                &session.worktree_root,
                [
                    OsString::from("status"),
                    OsString::from("--porcelain"),
                    OsString::from("--untracked-files=all"),
                ],
                &self.secret_env_scrubber,
            )
            .await?;
            if !status.status.success() {
                bail!("无法检查 worktree 状态: {}", status.render_error())
            }
            if !status.stdout.trim().is_empty() {
                bail!("worktree 含未提交修改；如确需删除，请设置 force=true")
            }
        }
        if let Err(error) = relocate_context(
            context,
            session.original_cwd.clone(),
            session.repo_root.clone(),
        )
        .await
        {
            let _ = relocate_context(
                context,
                session.worktree_root.clone(),
                session.worktree_root.clone(),
            )
            .await;
            return Err(error);
        }
        if let Err(error) = context
            .hooks()
            .run(
                "CwdChanged",
                None,
                json!({"from": &session.worktree_root, "to": &session.original_cwd}),
                &session.original_cwd,
            )
            .await
        {
            let _ = relocate_context(
                context,
                session.worktree_root.clone(),
                session.worktree_root.clone(),
            )
            .await;
            return Err(error);
        }
        if action == "keep" {
            *state = None;
            return Ok(ToolOutput::success(format!(
                "Returned to {} and kept worktree {}",
                session.original_cwd.display(),
                session.worktree_root.display()
            )));
        }
        let mut args = vec![OsString::from("worktree"), OsString::from("remove")];
        if force {
            args.push(OsString::from("--force"));
        }
        args.push(session.worktree_root.as_os_str().to_owned());
        let output = run_git(&session.repo_root, args, &self.secret_env_scrubber).await?;
        if !output.status.success() {
            if session.worktree_root.is_dir() {
                let _ = relocate_context(
                    context,
                    session.worktree_root.clone(),
                    session.worktree_root.clone(),
                )
                .await;
            }
            bail!("git worktree remove 失败: {}", output.render_error())
        }
        let mut branch_note = String::new();
        if let Some(branch) = &session.branch {
            let branch_output = run_git(
                &session.repo_root,
                [
                    OsString::from("branch"),
                    OsString::from("-d"),
                    OsString::from(branch),
                ],
                &self.secret_env_scrubber,
            )
            .await?;
            if !branch_output.status.success() {
                branch_note = format!("\nBranch retained: {branch}");
            }
        }
        if let Err(error) = context
            .hooks()
            .run(
                "WorktreeRemove",
                None,
                json!({"worktree_path": &session.worktree_root, "branch": &session.branch}),
                &session.original_cwd,
            )
            .await
        {
            branch_note.push_str(&format!("\nWorktreeRemove hook failed: {error:#}"));
        }
        *state = None;
        Ok(ToolOutput::success(format!(
            "Returned to {} and removed worktree {}{}",
            session.original_cwd.display(),
            session.worktree_root.display(),
            branch_note
        )))
    }
}

async fn relocate_context(context: &ToolContext, cwd: PathBuf, root: PathBuf) -> Result<()> {
    context.switch_workspace(cwd, root).await?;
    context.reload_workspace_context().await?;
    context.record_workspace_transition()
}

fn validate_restored_relative_cwd(cwd: &Path) -> Result<()> {
    if cwd.as_os_str().is_empty()
        || cwd.is_absolute()
        || cwd.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        bail!("session worktree cwd 必须是无 parent 逃逸的相对路径")
    }
    Ok(())
}

fn sanitize_name(value: &str) -> Result<String> {
    let mut output = String::new();
    let mut previous_dash = false;
    for character in value.trim().chars() {
        let mapped = if character.is_ascii_alphanumeric() {
            character.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' && previous_dash {
            continue;
        }
        output.push(mapped);
        previous_dash = mapped == '-';
        if output.len() >= MAX_WORKTREE_NAME_BYTES {
            break;
        }
    }
    let output = output.trim_matches('-').to_owned();
    if output.is_empty() {
        bail!("worktree name 规范化后为空")
    }
    Ok(output)
}

fn corresponding_cwd(root: &Path, relative: &Path) -> PathBuf {
    let candidate = root.join(relative);
    if candidate.is_dir() {
        candidate
    } else {
        root.to_owned()
    }
}

async fn repository_root(cwd: &Path, secret_env_scrubber: &SecretEnvScrubber) -> Result<PathBuf> {
    let output = run_git(
        cwd,
        [
            OsString::from("rev-parse"),
            OsString::from("--show-toplevel"),
        ],
        secret_env_scrubber,
    )
    .await?;
    if !output.status.success() {
        bail!("当前目录不在 Git repository 中: {}", output.render_error())
    }
    let path = PathBuf::from(output.stdout.trim());
    std::fs::canonicalize(&path)
        .with_context(|| format!("无法解析 Git repository root: {}", path.display()))
}

async fn default_base_ref(
    repo_root: &Path,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<String> {
    let output = run_git(
        repo_root,
        [
            OsString::from("symbolic-ref"),
            OsString::from("--quiet"),
            OsString::from("refs/remotes/origin/HEAD"),
        ],
        secret_env_scrubber,
    )
    .await?;
    if output.status.success() {
        let reference = output.stdout.trim();
        if !reference.is_empty() {
            return Ok(reference
                .strip_prefix("refs/remotes/")
                .unwrap_or(reference)
                .to_owned());
        }
    }
    Ok("HEAD".to_owned())
}

async fn registered_worktrees(
    repo_root: &Path,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<Vec<PathBuf>> {
    Ok(registered_worktree_records(repo_root, secret_env_scrubber)
        .await?
        .into_iter()
        .map(|record| record.path)
        .collect())
}

#[derive(Debug)]
struct RegisteredWorktree {
    path: PathBuf,
    branch: Option<String>,
}

async fn registered_worktree_records(
    repo_root: &Path,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<Vec<RegisteredWorktree>> {
    let output = run_git(
        repo_root,
        [
            OsString::from("worktree"),
            OsString::from("list"),
            OsString::from("--porcelain"),
        ],
        secret_env_scrubber,
    )
    .await?;
    if !output.status.success() {
        bail!("git worktree list 失败: {}", output.render_error())
    }
    let mut records = Vec::new();
    let mut path = None;
    let mut branch = None;
    for line in output.stdout.lines().chain(std::iter::once("")) {
        if line.is_empty() {
            if let Some(candidate) = path.take() {
                let candidate = PathBuf::from(candidate);
                records.push(RegisteredWorktree {
                    path: std::fs::canonicalize(&candidate).with_context(|| {
                        format!("无法解析 registered worktree: {}", candidate.display())
                    })?,
                    branch: branch.take(),
                });
            }
            continue;
        }
        if let Some(value) = line.strip_prefix("worktree ") {
            path = Some(value.to_owned());
        } else if let Some(value) = line.strip_prefix("branch refs/heads/") {
            branch = Some(value.to_owned());
        }
    }
    Ok(records)
}

fn agent_worktree_parent(repo_root: &Path) -> Result<PathBuf> {
    let home = dirs::home_dir().context("无法确定 agent worktree storage 主目录")?;
    Ok(home
        .join(".open-agent-harness/worktrees/agents")
        .join(workspace_key(repo_root)))
}

fn validate_git_oid(value: &str) -> Result<()> {
    if !(40..=64).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("agent worktree base commit 不是有效 Git object id")
    }
    Ok(())
}

async fn agent_worktree_has_changes(worktree: &AgentWorktree) -> Result<bool> {
    let status = run_git(
        &worktree.root,
        [
            OsString::from("status"),
            OsString::from("--porcelain"),
            OsString::from("--untracked-files=all"),
        ],
        &worktree.secret_env_scrubber,
    )
    .await?;
    if !status.status.success() {
        bail!("无法检查 agent worktree 状态；为防止数据丢失已保留 worktree")
    }
    if !status.stdout.trim().is_empty() {
        return Ok(true);
    }
    let commits = run_git(
        &worktree.root,
        [
            OsString::from("rev-list"),
            OsString::from("--count"),
            OsString::from(format!("{}..HEAD", worktree.base_commit)),
        ],
        &worktree.secret_env_scrubber,
    )
    .await?;
    if !commits.status.success() {
        bail!("无法检查 agent worktree commits；为防止数据丢失已保留 worktree")
    }
    let count = commits
        .stdout
        .trim()
        .parse::<u64>()
        .context("git rev-list 返回无效 commit count")?;
    Ok(count > 0)
}

async fn remove_owned_agent_worktree(worktree: &AgentWorktree) -> Result<()> {
    let registered =
        registered_worktree_records(&worktree.repo_root, &worktree.secret_env_scrubber).await?;
    if !registered.iter().any(|record| {
        record.path == worktree.root && record.branch.as_deref() == Some(&worktree.branch)
    }) {
        bail!("拒绝删除不再精确匹配注册记录的 agent worktree")
    }
    let removed = run_git(
        &worktree.repo_root,
        [
            OsString::from("worktree"),
            OsString::from("remove"),
            worktree.root.as_os_str().to_owned(),
        ],
        &worktree.secret_env_scrubber,
    )
    .await?;
    if !removed.status.success() {
        bail!("git agent worktree remove 失败: {}", removed.render_error())
    }
    let branch = run_git(
        &worktree.repo_root,
        [
            OsString::from("branch"),
            OsString::from("-D"),
            OsString::from(&worktree.branch),
        ],
        &worktree.secret_env_scrubber,
    )
    .await?;
    if !branch.status.success() {
        bail!(
            "agent worktree 已移除但临时 branch 删除失败: {}",
            branch.render_error()
        )
    }
    Ok(())
}

fn display_private_path(path: &Path) -> String {
    dirs::home_dir()
        .and_then(|home| path.strip_prefix(home).ok().map(Path::to_path_buf))
        .map(|relative| format!("~/{}", relative.display()))
        .unwrap_or_else(|| "<managed-agent-worktree>".to_owned())
}

async fn cleanup_failed_worktree(
    repo_root: &Path,
    target: &Path,
    branch: &str,
    secret_env_scrubber: &SecretEnvScrubber,
) {
    let _ = run_git(
        repo_root,
        [
            OsString::from("worktree"),
            OsString::from("remove"),
            OsString::from("--force"),
            target.as_os_str().to_owned(),
        ],
        secret_env_scrubber,
    )
    .await;
    let _ = run_git(
        repo_root,
        [
            OsString::from("branch"),
            OsString::from("-D"),
            OsString::from(branch),
        ],
        secret_env_scrubber,
    )
    .await;
    if target.exists() {
        let _ = std::fs::remove_dir_all(target);
    }
}

struct GitOutput {
    status: ExitStatus,
    stdout: String,
    stderr: String,
    truncated: bool,
}

impl GitOutput {
    fn render_error(&self) -> String {
        let detail = if self.stderr.trim().is_empty() {
            self.stdout.trim()
        } else {
            self.stderr.trim()
        };
        format!(
            "exit={}{}{}",
            self.status.code().unwrap_or(-1),
            if detail.is_empty() {
                String::new()
            } else {
                format!(" {detail}")
            },
            if self.truncated {
                " [output truncated]"
            } else {
                ""
            }
        )
    }
}

async fn run_git<I>(
    cwd: &Path,
    args: I,
    secret_env_scrubber: &SecretEnvScrubber,
) -> Result<GitOutput>
where
    I: IntoIterator<Item = OsString>,
{
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GCM_INTERACTIVE", "Never")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    secret_env_scrubber.scrub_tokio(&mut command);
    let (mut child, process_guard) = spawn_managed(&mut command).context("无法启动 git")?;
    let stdout = child.stdout.take().context("无法捕获 git stdout")?;
    let stderr = child.stderr.take().context("无法捕获 git stderr")?;
    let stdout_task = tokio::spawn(capture_stream(stdout));
    let stderr_task = tokio::spawn(capture_stream(stderr));
    let status = match timeout(GIT_TIMEOUT, child.wait()).await {
        Ok(status) => status.context("等待 git 失败")?,
        Err(_) => {
            process_guard.terminate();
            let _ = child.start_kill();
            let _ = child.wait().await;
            bail!("git command 超过 {}s timeout", GIT_TIMEOUT.as_secs())
        }
    };
    process_guard.terminate();
    let (stdout, stdout_truncated) = stdout_task.await.context("git stdout worker 失败")?;
    let (stderr, stderr_truncated) = stderr_task.await.context("git stderr worker 失败")?;
    Ok(GitOutput {
        status,
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
        truncated: stdout_truncated || stderr_truncated,
    })
}

async fn capture_stream(mut stream: impl AsyncRead + Unpin) -> (Vec<u8>, bool) {
    let mut stored = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 8192];
    loop {
        let count = match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return (stored, truncated),
            Ok(count) => count,
        };
        let keep = count.min(MAX_GIT_OUTPUT_BYTES.saturating_sub(stored.len()));
        stored.extend_from_slice(&buffer[..keep]);
        truncated |= keep < count;
    }
}

fn set_private_directory(path: &Path) -> Result<()> {
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
    use super::*;
    use crate::{
        file_history::{CheckpointBoundary, FileHistory},
        permissions::{PermissionManager, PermissionMode},
        tools::{ToolContext, ToolRegistry},
    };

    #[tokio::test]
    async fn creates_switches_and_removes_isolated_worktree() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        run_test_git(&repo, &["init"]);
        run_test_git(&repo, &["config", "user.email", "test@example.invalid"]);
        run_test_git(&repo, &["config", "user.name", "Test"]);
        std::fs::write(repo.join("tracked.txt"), "base").unwrap();
        run_test_git(&repo, &["add", "tracked.txt"]);
        run_test_git(&repo, &["commit", "-m", "base"]);

        let settings = Settings {
            raw: json!({"worktree": {
                "baseRef": "head",
                "storageDirectory": temp.path().join("worktrees")
            }}),
        };
        let integration = configure_worktree(&settings, &repo).unwrap();
        let registry =
            ToolRegistry::with_extensions(Vec::new(), integration.deferred_tools).unwrap();
        let context = ToolContext::new(
            repo.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        let history_storage = temp.path().join("history");
        std::fs::create_dir(&history_storage).unwrap();
        context.set_file_history(
            FileHistory::create_in(&repo, Uuid::new_v4(), &history_storage, true).unwrap(),
        );
        let checkpoint = context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
            .unwrap()
            .unwrap();
        registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query": "select:EnterWorktree,ExitWorktree"}),
            )
            .await;
        let entered = registry
            .execute(&context, "EnterWorktree", json!({"name": "isolated test"}))
            .await;
        assert!(!entered.is_error, "{}", entered.content);
        let worktree = context.workspace_root();
        assert_ne!(worktree, std::fs::canonicalize(&repo).unwrap());
        assert_eq!(
            std::fs::read_to_string(worktree.join("tracked.txt")).unwrap(),
            "base"
        );
        let read = registry
            .execute(&context, "Read", json!({"file_path":"tracked.txt"}))
            .await;
        assert!(!read.is_error, "{}", read.content);
        let write = registry
            .execute(
                &context,
                "Write",
                json!({"file_path":"tracked.txt", "content":"changed"}),
            )
            .await;
        assert!(!write.is_error, "{}", write.content);
        assert_eq!(
            std::fs::read_to_string(worktree.join("tracked.txt")).unwrap(),
            "changed"
        );
        let (rewound, _) = context.rewind_files(checkpoint.id, 0).unwrap();
        assert_eq!(rewound.restored, 1);
        assert_eq!(
            std::fs::read_to_string(worktree.join("tracked.txt")).unwrap(),
            "base"
        );
        let exited = registry
            .execute(&context, "ExitWorktree", json!({"action": "remove"}))
            .await;
        assert!(!exited.is_error, "{}", exited.content);
        assert_eq!(context.cwd(), std::fs::canonicalize(&repo).unwrap());
        assert!(!worktree.exists());
    }

    #[tokio::test]
    async fn restores_only_a_registered_worktree_from_hashed_session_state() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        let worktree = temp.path().join("restored-worktree");
        std::fs::create_dir(&repo).unwrap();
        run_test_git(&repo, &["init"]);
        run_test_git(&repo, &["config", "user.email", "test@example.invalid"]);
        run_test_git(&repo, &["config", "user.name", "Test"]);
        std::fs::write(repo.join("tracked.txt"), "base").unwrap();
        run_test_git(&repo, &["add", "tracked.txt"]);
        run_test_git(&repo, &["commit", "-m", "base"]);
        run_test_git(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "restored-session",
                worktree.to_str().unwrap(),
                "HEAD",
            ],
        );
        let worktree = std::fs::canonicalize(worktree).unwrap();
        let settings = Settings {
            raw: json!({"worktree": {
                "baseRef": "head",
                "storageDirectory": temp.path().join("managed-worktrees")
            }}),
        };

        let unknown = configure_worktree(&settings, &repo).unwrap();
        assert!(
            unknown
                .restore_session(&SessionWorkspaceState {
                    workspace_key: Some("0".repeat(32)),
                    cwd: PathBuf::from("."),
                })
                .await
                .is_err()
        );
        assert!(
            unknown
                .restore_session(&SessionWorkspaceState {
                    workspace_key: Some(workspace_key(&worktree)),
                    cwd: PathBuf::from("../escape"),
                })
                .await
                .is_err()
        );

        let integration = configure_worktree(&settings, &repo).unwrap();
        let restored = integration
            .restore_session(&SessionWorkspaceState {
                workspace_key: Some(workspace_key(&worktree)),
                cwd: PathBuf::from("."),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.root, worktree);
        assert_eq!(restored.cwd, worktree);

        let context = ToolContext::new(
            repo.clone(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .switch_workspace(restored.cwd, restored.root)
            .await
            .unwrap();
        let registry =
            ToolRegistry::with_extensions(Vec::new(), integration.deferred_tools).unwrap();
        registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query": "select:ExitWorktree"}),
            )
            .await;
        let rejected_remove = registry
            .execute(&context, "ExitWorktree", json!({"action":"remove"}))
            .await;
        assert!(rejected_remove.is_error);
        assert!(worktree.exists());
        let kept = registry
            .execute(&context, "ExitWorktree", json!({"action":"keep"}))
            .await;
        assert!(!kept.is_error, "{}", kept.content);
        assert_eq!(context.cwd(), std::fs::canonicalize(repo).unwrap());
        assert!(worktree.exists());
    }

    #[tokio::test]
    async fn agent_worktree_rejects_dirty_source_and_removes_clean_checkout() {
        let temp = tempfile::tempdir().unwrap();
        let non_git = temp.path().join("non-git");
        std::fs::create_dir(&non_git).unwrap();
        let non_git_error =
            create_agent_worktree_with_storage(&non_git, Uuid::new_v4(), Some(temp.path()))
                .await
                .unwrap_err();
        assert!(format!("{non_git_error:#}").contains("不在 Git repository"));

        let repo = init_test_repo(temp.path());
        let storage = temp.path().join("agent-worktrees");
        let id = Uuid::new_v4();

        std::fs::write(repo.join("untracked.txt"), "user state").unwrap();
        let error = create_agent_worktree_with_storage(&repo, id, Some(&storage))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("要求源仓库 clean"));
        std::fs::remove_file(repo.join("untracked.txt")).unwrap();

        let worktree = create_agent_worktree_with_storage(&repo, id, Some(&storage))
            .await
            .unwrap();
        let root = worktree.root().to_owned();
        let branch = worktree.branch().to_owned();
        assert!(root.is_dir());
        assert_eq!(
            std::fs::read_to_string(root.join("tracked.txt")).unwrap(),
            "base"
        );
        assert_eq!(
            worktree.finish().await.unwrap(),
            AgentWorktreeDisposition::Removed
        );
        assert!(!root.exists());
        let refs = std::process::Command::new("git")
            .args(["show-ref", "--verify", &format!("refs/heads/{branch}")])
            .current_dir(&repo)
            .status()
            .unwrap();
        assert!(!refs.success());
    }

    #[tokio::test]
    async fn changed_agent_worktree_is_kept_and_exact_metadata_can_resume_it() {
        let temp = tempfile::tempdir().unwrap();
        let repo = init_test_repo(temp.path());
        let storage = temp.path().join("agent-worktrees");
        let id = Uuid::new_v4();
        let worktree = create_agent_worktree_with_storage(&repo, id, Some(&storage))
            .await
            .unwrap();
        let root = worktree.root().to_owned();
        let branch = worktree.branch().to_owned();
        let base_commit = worktree.base_commit.clone();
        std::fs::write(root.join("tracked.txt"), "agent change").unwrap();
        let kept = worktree.finish().await.unwrap();
        assert!(matches!(kept, AgentWorktreeDisposition::Kept { .. }));
        assert!(root.is_dir());

        let restored =
            restore_agent_worktree_with_storage(&repo, id, &branch, &base_commit, Some(&storage))
                .await
                .unwrap();
        assert_eq!(restored.root(), root);
        assert!(
            restore_agent_worktree_with_storage(
                &repo,
                Uuid::new_v4(),
                &branch,
                &base_commit,
                Some(&storage),
            )
            .await
            .is_err()
        );
        std::fs::write(root.join("tracked.txt"), "base").unwrap();
        assert_eq!(
            restored.finish().await.unwrap(),
            AgentWorktreeDisposition::Removed
        );
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn dropped_clean_agent_worktree_is_reclaimed_after_cancel_style_drop() {
        let temp = tempfile::tempdir().unwrap();
        let repo = init_test_repo(temp.path());
        let storage = temp.path().join("managed");

        let clean =
            create_agent_worktree_with_storage(&repo, Uuid::new_v4(), Some(storage.as_path()))
                .await
                .unwrap();
        let clean_root = clean.root().to_owned();
        let clean_branch = clean.branch().to_owned();
        drop(clean);
        tokio::time::timeout(Duration::from_secs(5), async {
            while clean_root.exists()
                || std::process::Command::new("git")
                    .args([
                        "show-ref",
                        "--verify",
                        "--quiet",
                        &format!("refs/heads/{clean_branch}"),
                    ])
                    .current_dir(&repo)
                    .status()
                    .is_ok_and(|status| status.success())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("clean abandoned worktree should be reclaimed");
    }

    #[tokio::test]
    async fn agent_worktree_enforces_managed_repository_quota() {
        let temp = tempfile::tempdir().unwrap();
        let repo = init_test_repo(temp.path());
        let storage = temp.path().join("agent-worktrees");
        let parent = storage.join(workspace_key(&std::fs::canonicalize(&repo).unwrap()));
        std::fs::create_dir_all(&parent).unwrap();
        for index in 0..MAX_AGENT_WORKTREES_PER_REPOSITORY {
            std::fs::create_dir(parent.join(format!("occupied-{index}"))).unwrap();
        }
        let error = create_agent_worktree_with_storage(&repo, Uuid::new_v4(), Some(&storage))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("资源上限"));
    }

    fn init_test_repo(parent: &Path) -> PathBuf {
        let repo = parent.join("repo");
        std::fs::create_dir(&repo).unwrap();
        run_test_git(&repo, &["init"]);
        run_test_git(&repo, &["config", "user.email", "test@example.invalid"]);
        run_test_git(&repo, &["config", "user.name", "Test"]);
        std::fs::write(repo.join("tracked.txt"), "base").unwrap();
        run_test_git(&repo, &["add", "tracked.txt"]);
        run_test_git(&repo, &["commit", "-m", "base"]);
        repo
    }

    fn run_test_git(cwd: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }
}

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
    process::terminate_process_tree,
    tools::{
        Tool, ToolContext, ToolOutput, ensure_private_directory, object_schema, workspace_key,
    },
};

const MAX_GIT_OUTPUT_BYTES: usize = 256 * 1024;
const GIT_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_WORKTREE_NAME_BYTES: usize = 64;

pub struct WorktreeIntegration {
    pub deferred_tools: Vec<Arc<dyn Tool>>,
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
    });
    Ok(WorktreeIntegration {
        deferred_tools: vec![
            Arc::new(EnterWorktreeTool {
                manager: Arc::clone(&manager),
            }),
            Arc::new(ExitWorktreeTool { manager }),
        ],
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
    async fn create(&self, context: &ToolContext, requested_name: &str) -> Result<ToolOutput> {
        context.reload_workspace_context().await?;
        let mut state = self.state.lock().await;
        if state.is_some() {
            bail!("当前 session 已经进入 worktree；请先调用 ExitWorktree")
        }
        let name = sanitize_name(requested_name)?;
        let repo_root = repository_root(&self.original_cwd).await?;
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
            BaseRef::Fresh => default_base_ref(&repo_root).await?,
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
        )
        .await?;
        if !output.status.success() {
            cleanup_failed_worktree(&repo_root, &target, &branch).await;
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
            cleanup_failed_worktree(&repo_root, &worktree_root, &branch).await;
            return Err(error);
        }
        if let Err(error) = relocate_context(context, work_cwd.clone(), worktree_root.clone()).await
        {
            let _ = relocate_context(context, self.original_cwd.clone(), repo_root.clone()).await;
            cleanup_failed_worktree(&repo_root, &worktree_root, &branch).await;
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
            cleanup_failed_worktree(&repo_root, &worktree_root, &branch).await;
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
        let repo_root = repository_root(&self.original_cwd).await?;
        let registered = registered_worktrees(&repo_root).await?;
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
        let output = run_git(&session.repo_root, args).await?;
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
    context.reload_workspace_context().await
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

async fn repository_root(cwd: &Path) -> Result<PathBuf> {
    let output = run_git(
        cwd,
        [
            OsString::from("rev-parse"),
            OsString::from("--show-toplevel"),
        ],
    )
    .await?;
    if !output.status.success() {
        bail!("当前目录不在 Git repository 中: {}", output.render_error())
    }
    let path = PathBuf::from(output.stdout.trim());
    std::fs::canonicalize(&path)
        .with_context(|| format!("无法解析 Git repository root: {}", path.display()))
}

async fn default_base_ref(repo_root: &Path) -> Result<String> {
    let output = run_git(
        repo_root,
        [
            OsString::from("symbolic-ref"),
            OsString::from("--quiet"),
            OsString::from("refs/remotes/origin/HEAD"),
        ],
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

async fn registered_worktrees(repo_root: &Path) -> Result<Vec<PathBuf>> {
    let output = run_git(
        repo_root,
        [
            OsString::from("worktree"),
            OsString::from("list"),
            OsString::from("--porcelain"),
        ],
    )
    .await?;
    if !output.status.success() {
        bail!("git worktree list 失败: {}", output.render_error())
    }
    output
        .stdout
        .lines()
        .filter_map(|line| line.strip_prefix("worktree "))
        .map(PathBuf::from)
        .map(|path| {
            std::fs::canonicalize(&path)
                .with_context(|| format!("无法解析 registered worktree: {}", path.display()))
        })
        .collect()
}

async fn cleanup_failed_worktree(repo_root: &Path, target: &Path, branch: &str) {
    let _ = run_git(
        repo_root,
        [
            OsString::from("worktree"),
            OsString::from("remove"),
            OsString::from("--force"),
            target.as_os_str().to_owned(),
        ],
    )
    .await;
    let _ = run_git(
        repo_root,
        [
            OsString::from("branch"),
            OsString::from("-D"),
            OsString::from(branch),
        ],
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

async fn run_git<I>(cwd: &Path, args: I) -> Result<GitOutput>
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
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().context("无法启动 git")?;
    let process_group = child.id();
    let stdout = child.stdout.take().context("无法捕获 git stdout")?;
    let stderr = child.stderr.take().context("无法捕获 git stderr")?;
    let stdout_task = tokio::spawn(capture_stream(stdout));
    let stderr_task = tokio::spawn(capture_stream(stderr));
    let status = match timeout(GIT_TIMEOUT, child.wait()).await {
        Ok(status) => status.context("等待 git 失败")?,
        Err(_) => {
            terminate_process_tree(process_group);
            let _ = child.start_kill();
            let _ = child.wait().await;
            bail!("git command 超过 {}s timeout", GIT_TIMEOUT.as_secs())
        }
    };
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
        let exited = registry
            .execute(&context, "ExitWorktree", json!({"action": "remove"}))
            .await;
        assert!(!exited.is_error, "{}", exited.content);
        assert_eq!(context.cwd(), std::fs::canonicalize(&repo).unwrap());
        assert!(!worktree.exists());
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

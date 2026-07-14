use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

use crate::process::{ProcessTreeGuard, spawn_managed};

use super::{
    BackgroundTask, Tool, ToolContext, ToolOutput, ensure_private_directory, object_schema,
    parse_input,
};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
pub(crate) const MAX_OUTPUT_BYTES: usize = 30_000;
const MAX_CAPTURE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_CAPTURE_DIRECTORY_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CAPTURE_FILES: usize = 1024;
const MAX_BACKGROUND_TASKS: usize = 32;

struct CwdMarkerGuard(PathBuf);

impl Drop for CwdMarkerGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct OutputFileGuard {
    path: PathBuf,
    keep: bool,
}

impl OutputFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, keep: false }
    }

    fn keep(&mut self) {
        self.keep = true;
    }
}

impl Drop for OutputFileGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Deserialize)]
struct Input {
    command: String,
    timeout: Option<u64>,
    #[serde(default)]
    run_in_background: bool,
    description: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForegroundCapturePolicy {
    RetainLongOutput,
    DiscardAfterPreview,
}

pub struct BashTool;

impl BashTool {
    /// Workflow reports are independently bounded and cannot expose the tail marker that points
    /// at a retained foreground capture. Run those commands with an ephemeral capture so a large
    /// step cannot leave an unreachable file in the shared task-output directory.
    pub(crate) async fn execute_for_workflow(
        &self,
        context: &ToolContext,
        input: Value,
    ) -> Result<ToolOutput> {
        execute_bash(context, input, ForegroundCapturePolicy::DiscardAfterPreview).await
    }
}

#[async_trait]
impl Tool for BashTool {
    fn name(&self) -> &str {
        "Bash"
    }

    fn description(&self) -> &str {
        "Executes a shell command in the working directory with timeout support. Long commands may run in the background."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "command": {"type": "string", "maxLength": 65536},
                "timeout": {"type": "integer", "minimum": 1, "maximum": MAX_TIMEOUT_MS},
                "run_in_background": {"type": "boolean"},
                "description": {"type": "string", "maxLength": 2048}
            }),
            &["command"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, input: &Value) -> bool {
        let command = input.get("command").and_then(Value::as_str).unwrap_or("");
        command_is_destructive(command)
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        execute_bash(context, input, ForegroundCapturePolicy::RetainLongOutput).await
    }
}

async fn execute_bash(
    context: &ToolContext,
    input: Value,
    capture_policy: ForegroundCapturePolicy,
) -> Result<ToolOutput> {
    let input: Input = parse_input(input)?;
    if input.command.trim().is_empty() {
        bail!("command 不能为空")
    }
    let _description = input.description;
    let timeout_ms = input
        .timeout
        .unwrap_or(DEFAULT_TIMEOUT_MS)
        .min(MAX_TIMEOUT_MS);
    let shell = default_shell();
    if input.run_in_background {
        if capture_policy == ForegroundCapturePolicy::DiscardAfterPreview {
            bail!("workflow Bash 不允许启动独立 background command")
        }
        return spawn_background(context, &shell, input.command, timeout_ms).await;
    }
    let (cwd_path, cwd_file) = create_private_cwd_marker(context)?;
    let _cwd_marker_guard = CwdMarkerGuard(cwd_path.clone());
    drop(cwd_file);
    let (mut command, sandbox_warning) =
        match shell_command(context, &shell, &input.command, Some(&cwd_path)) {
            Ok(command) => command,
            Err(error) => {
                let _ = std::fs::remove_file(&cwd_path);
                return Err(error);
            }
        };
    let (output_path, output_file) = create_private_output(context, "foreground")?;
    let mut output_guard = OutputFileGuard::new(output_path.clone());
    let (mut child, process_guard, drains, capture_truncated) =
        match spawn_captured(&mut command, output_file).await {
            Ok(spawned) => spawned,
            Err(error) => {
                let _ = std::fs::remove_file(&output_path);
                let _ = std::fs::remove_file(&cwd_path);
                return Err(error);
            }
        };
    let status = match timeout(Duration::from_millis(timeout_ms), child.wait()).await {
        Ok(status) => Some(status.context("等待 shell 命令失败")?),
        Err(_) => {
            process_guard.terminate();
            let _ = child.start_kill();
            let _ = child.wait().await;
            None
        }
    };
    // A foreground shell can exit after detaching descendants whose stdio no longer keeps the
    // capture drains open. Reap the owned process group / Job Object on every completion path.
    process_guard.terminate();
    await_foreground_drains(drains, &process_guard).await;
    process_guard.disarm();
    let capture_was_truncated = capture_truncated.load(Ordering::Relaxed);
    let retain_long_output = capture_policy == ForegroundCapturePolicy::RetainLongOutput;
    let (mut preview, preview_truncated, size) =
        read_output_preview_with_retention(&output_path, MAX_OUTPUT_BYTES, retain_long_output)?;
    append_sandbox_warning(&mut preview, sandbox_warning.as_deref());
    if status
        .as_ref()
        .is_some_and(std::process::ExitStatus::success)
    {
        append_cwd_update(context, &cwd_path, &mut preview).await;
    }
    let _ = std::fs::remove_file(&cwd_path);
    let keep_output = retain_long_output && (preview_truncated || capture_was_truncated);
    if keep_output {
        output_guard.keep();
        if !preview.is_empty() {
            preview.push('\n');
        }
        preview.push_str(&format!(
            "[Full captured output: {} ({} bytes{})]",
            context.display_path(&output_path),
            size,
            if capture_was_truncated {
                "; additional output discarded at the 8 MiB limit"
            } else {
                ""
            }
        ));
    } else {
        let _ = std::fs::remove_file(&output_path);
    }

    let Some(status) = status else {
        if !preview.is_empty() {
            preview.push('\n');
        }
        preview.push_str(&format!(
            "Command timed out after {timeout_ms}ms and was terminated"
        ));
        return Ok(ToolOutput::error(preview));
    };
    if !status.success() {
        if !preview.is_empty() {
            preview.push('\n');
        }
        preview.push_str(&format!("Exit code {}", status.code().unwrap_or(-1)));
        return Ok(ToolOutput::error(preview));
    }
    if preview.is_empty() {
        preview = "Command completed successfully with no output".into();
    }
    Ok(ToolOutput::success(preview))
}

async fn spawn_background(
    context: &ToolContext,
    shell: &str,
    command_text: String,
    timeout_ms: u64,
) -> Result<ToolOutput> {
    if context.tasks.lock().await.len() >= MAX_BACKGROUND_TASKS {
        bail!("后台任务达到 {MAX_BACKGROUND_TASKS} 个限制；请先读取或停止已有任务")
    }
    let id = Uuid::new_v4().to_string();
    let (mut command, sandbox_warning) = shell_command(context, shell, &command_text, None)?;
    let (output_path, output_file) = create_private_output(context, &id)?;
    let (child, process_tree, drains, output_truncated) =
        match spawn_captured(&mut command, output_file).await {
            Ok(spawned) => spawned,
            Err(error) => {
                let _ = std::fs::remove_file(&output_path);
                return Err(error);
            }
        };
    let timeout_cancelled = Arc::new(AtomicBool::new(false));
    let mut task = BackgroundTask {
        child,
        output_path: output_path.clone(),
        output_cleanup_armed: true,
        command: command_text,
        process_tree,
        drains,
        output_truncated,
        timeout_cancelled: Arc::clone(&timeout_cancelled),
        timeout_ms,
        timed_out: false,
        notification_delivered: false,
    };
    let mut tasks = context.tasks.lock().await;
    if tasks.len() >= MAX_BACKGROUND_TASKS {
        terminate_task(&mut task).await;
        bail!("后台任务达到 {MAX_BACKGROUND_TASKS} 个限制；请先读取或停止已有任务")
    }
    tasks.insert(id.clone(), task);
    drop(tasks);
    let tasks = Arc::downgrade(&context.tasks);
    let timeout_id = id.clone();
    tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if timeout_cancelled.load(Ordering::Acquire) {
                return;
            }
            let Some(tasks) = tasks.upgrade() else {
                return;
            };
            let mut tasks = tasks.lock().await;
            let Some(task) = tasks.get_mut(&timeout_id) else {
                return;
            };
            match task.child.try_wait() {
                Ok(Some(_)) => {
                    task.process_tree.terminate();
                    return;
                }
                Ok(None) if started.elapsed() < Duration::from_millis(timeout_ms) => continue,
                Ok(None) | Err(_) => {
                    terminate_task(task).await;
                    task.timed_out = true;
                    return;
                }
            }
        }
    });
    let mut response = format!(
        "Command running in background with ID: {id}\nOutput: {}\nTimeout: {timeout_ms}ms",
        context.display_path(&output_path),
    );
    append_sandbox_warning(&mut response, sandbox_warning.as_deref());
    Ok(ToolOutput::success(response))
}

pub(crate) fn shell_command(
    context: &ToolContext,
    shell: &str,
    command_text: &str,
    cwd_marker: Option<&Path>,
) -> Result<(Command, Option<String>)> {
    let command_text = cwd_marker
        .map(|marker| command_with_cwd_marker(shell, command_text, marker))
        .unwrap_or_else(|| command_text.to_owned());
    let mut shell_args = Vec::<OsString>::new();
    #[cfg(windows)]
    {
        let executable = Path::new(shell)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(shell)
            .to_ascii_lowercase();
        if executable.contains("powershell") || executable == "pwsh" || executable == "pwsh.exe" {
            shell_args.extend(
                ["-NoProfile", "-NonInteractive", "-Command", &command_text]
                    .into_iter()
                    .map(OsString::from),
            );
        } else if executable == "cmd" || executable == "cmd.exe" {
            shell_args.extend(
                ["/D", "/S", "/C", &command_text]
                    .into_iter()
                    .map(OsString::from),
            );
        } else {
            shell_args.extend(["-lc", &command_text].into_iter().map(OsString::from));
        }
    }
    #[cfg(not(windows))]
    shell_args.extend(["-lc", &command_text].into_iter().map(OsString::from));
    let prepared = context.sandbox_runtime().command(
        &context.cwd(),
        std::ffi::OsStr::new(shell),
        &shell_args,
    )?;
    let (mut command, warning) = prepared.into_parts();
    context.scrub_child_environment(&mut command);
    command
        .current_dir(context.cwd())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    Ok((command, warning))
}

async fn append_cwd_update(context: &ToolContext, marker: &Path, output: &mut String) {
    let result = (|| -> Result<PathBuf> {
        let metadata = std::fs::symlink_metadata(marker)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 4096 {
            bail!("shell cwd marker 无效或超过资源限制")
        }
        let mut bytes = Vec::new();
        File::open(marker)?.take(4097).read_to_end(&mut bytes)?;
        if bytes.len() > 4096 {
            bail!("shell cwd marker 超过 4096 字节")
        }
        let cwd = String::from_utf8(bytes).context("shell cwd 不是有效 UTF-8")?;
        let cwd = cwd.trim().trim_start_matches('\u{feff}');
        if cwd.is_empty() || cwd.contains('\0') {
            bail!("shell cwd marker 为空或包含 NUL")
        }
        Ok(PathBuf::from(cwd))
    })();
    let message = match result {
        Ok(cwd) => match context.update_cwd_from_shell(&cwd).await {
            Ok(true) => return,
            Ok(false) => Some(format!(
                "Shell ended outside the trusted working directories; session cwd remains {}",
                context.display_path(&context.cwd())
            )),
            Err(error) => Some(format!("Shell cwd update was rejected: {error:#}")),
        },
        Err(error) => Some(format!("Shell cwd could not be verified: {error:#}")),
    };
    if let Some(message) = message {
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str("[Cwd warning: ");
        output.push_str(&message);
        output.push(']');
    }
}

fn command_with_cwd_marker(shell: &str, command: &str, marker: &Path) -> String {
    #[cfg(windows)]
    {
        let executable = Path::new(shell)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(shell)
            .to_ascii_lowercase();
        if executable.contains("powershell") || executable == "pwsh" || executable == "pwsh.exe" {
            let marker = marker.display().to_string().replace('\'', "''");
            format!(
                "& {{ {command} }}; $__harness_ok = $?; $__harness_code = if ($__harness_ok) {{ 0 }} elseif ($null -ne $LASTEXITCODE) {{ $LASTEXITCODE }} else {{ 1 }}; (Get-Location).ProviderPath | Set-Content -LiteralPath '{marker}' -Encoding utf8 -NoNewline; exit $__harness_code"
            )
        } else {
            let marker = marker.display().to_string().replace('"', "\"\"");
            format!(
                "{command}\r\nset \"__HARNESS_STATUS=%ERRORLEVEL%\"\r\ncd > \"{marker}\"\r\nexit /b %__HARNESS_STATUS%"
            )
        }
    }
    #[cfg(not(windows))]
    {
        let _ = shell;
        let marker = marker.display().to_string().replace('\'', "'\\''");
        format!("{command}\n__harness_status=$?\npwd -P >| '{marker}'\nexit $__harness_status")
    }
}

pub(crate) fn append_sandbox_warning(output: &mut String, warning: Option<&str>) {
    let Some(warning) = warning else {
        return;
    };
    if !output.is_empty() {
        output.push('\n');
    }
    output.push_str("[Sandbox warning: ");
    output.push_str(warning);
    output.push(']');
}

pub(crate) fn default_shell() -> String {
    #[cfg(windows)]
    {
        std::env::var("COMSPEC")
            .or_else(|_| std::env::var("SHELL"))
            .unwrap_or_else(|_| "cmd.exe".to_owned())
    }
    #[cfg(not(windows))]
    {
        std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned())
    }
}

pub(crate) fn command_is_destructive(command: &str) -> bool {
    [
        "rm ",
        "rm\t",
        "git reset",
        "git clean",
        "mkfs",
        "shutdown",
        "reboot",
    ]
    .iter()
    .any(|needle| command.contains(needle))
}

async fn spawn_captured(
    command: &mut Command,
    output_file: File,
) -> Result<(
    Child,
    ProcessTreeGuard,
    Vec<JoinHandle<()>>,
    Arc<AtomicBool>,
)> {
    let (mut child, process_tree) = spawn_managed(command).context("无法启动 shell 命令")?;
    let stdout = child.stdout.take().context("无法捕获命令 stdout")?;
    let stderr = child.stderr.take().context("无法捕获命令 stderr")?;
    let truncated = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = mpsc::channel(32);
    let drains = vec![
        tokio::spawn(drain_to_channel(stdout, sender.clone())),
        tokio::spawn(drain_to_channel(stderr, sender)),
        tokio::spawn(write_capture(
            tokio::fs::File::from_std(output_file),
            receiver,
            Arc::clone(&truncated),
        )),
    ];
    Ok((child, process_tree, drains, truncated))
}

async fn drain_to_channel(mut reader: impl AsyncRead + Unpin, sender: mpsc::Sender<Vec<u8>>) {
    let mut chunk = [0u8; 8192];
    loop {
        let count = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        if sender.send(chunk[..count].to_vec()).await.is_err() {
            break;
        }
    }
}

async fn write_capture(
    mut file: tokio::fs::File,
    mut receiver: mpsc::Receiver<Vec<u8>>,
    truncated: Arc<AtomicBool>,
) {
    let mut written = 0u64;
    while let Some(chunk) = receiver.recv().await {
        if written >= MAX_CAPTURE_FILE_BYTES {
            truncated.store(true, Ordering::Relaxed);
            continue;
        }
        let keep = chunk.len().min((MAX_CAPTURE_FILE_BYTES - written) as usize);
        if keep < chunk.len() {
            truncated.store(true, Ordering::Relaxed);
        }
        if file.write_all(&chunk[..keep]).await.is_err() {
            return;
        }
        written = written.saturating_add(keep as u64);
    }
    let _ = file.flush().await;
}

async fn await_foreground_drains(mut drains: Vec<JoinHandle<()>>, process_tree: &ProcessTreeGuard) {
    for index in 0..drains.len() {
        if timeout(Duration::from_secs(1), &mut drains[index])
            .await
            .is_err()
        {
            process_tree.terminate();
            for drain in drains.iter_mut().skip(index) {
                let _ = timeout(Duration::from_secs(1), &mut *drain).await;
                drain.abort();
            }
            return;
        }
    }
}

pub(super) async fn terminate_task(task: &mut BackgroundTask) {
    let child_running = task.child.try_wait().ok().flatten().is_none();
    task.process_tree.terminate();
    if child_running {
        let _ = task.child.start_kill();
        let _ = task.child.wait().await;
    }
    let drains = std::mem::take(&mut task.drains);
    for mut drain in drains {
        if timeout(Duration::from_millis(100), &mut drain)
            .await
            .is_err()
        {
            task.process_tree.terminate();
            let _ = task.child.start_kill();
            let _ = task.child.wait().await;
            let _ = timeout(Duration::from_secs(1), &mut drain).await;
        }
        drain.abort();
    }
}

pub(crate) fn create_private_output(
    context: &ToolContext,
    prefix: &str,
) -> Result<(PathBuf, File)> {
    let base = context.task_capture_root()?;
    ensure_private_directory(&base)?;
    let mut files = 0usize;
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(&base)? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("output") {
            continue;
        }
        let metadata = match std::fs::symlink_metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("任务输出目录包含非普通文件")
        }
        files += 1;
        bytes = bytes.saturating_add(metadata.len());
        if files >= MAX_CAPTURE_FILES || bytes >= MAX_CAPTURE_DIRECTORY_BYTES {
            bail!(
                "任务输出目录达到资源上限（{files} files, {bytes} bytes）；请清理配置的任务输出目录"
            )
        }
    }
    let output_path = base.join(format!("{prefix}-{}.output", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&output_path)
        .with_context(|| format!("无法创建输出文件 {}", output_path.display()))?;
    Ok((output_path, file))
}

fn create_private_cwd_marker(context: &ToolContext) -> Result<(PathBuf, File)> {
    // Keep markers under either the checked private harness tree or the
    // embedding/test capture root that ToolContext validated explicitly.
    let base = context.cwd_marker_root()?;
    ensure_private_directory(&base)?;
    let mut entries = 0usize;
    for entry in std::fs::read_dir(&base)? {
        let entry = entry?;
        entries = entries.saturating_add(1);
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if entries > MAX_CAPTURE_FILES || metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("shell cwd marker 目录超过限制或包含非普通文件")
        }
    }
    let path = base.join(format!("{}.cwd", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&path)
        .with_context(|| format!("无法创建 shell cwd marker {}", path.display()))?;
    Ok((path, file))
}

pub(crate) fn read_output_preview(path: &Path, max_bytes: usize) -> Result<(String, bool, u64)> {
    read_output_preview_with_retention(path, max_bytes, true)
}

fn read_output_preview_with_retention(
    path: &Path,
    max_bytes: usize,
    retained: bool,
) -> Result<(String, bool, u64)> {
    let mut file =
        File::open(path).with_context(|| format!("无法读取任务输出 {}", path.display()))?;
    let size = file.metadata()?.len();
    if size <= max_bytes as u64 {
        let mut bytes = Vec::with_capacity(size as usize);
        file.read_to_end(&mut bytes)?;
        return Ok((
            String::from_utf8_lossy(&bytes).trim_end().to_owned(),
            false,
            size,
        ));
    }

    let half = max_bytes / 2;
    let mut head = vec![0u8; half];
    file.read_exact(&mut head)?;
    file.seek(SeekFrom::End(-(half as i64)))?;
    let mut tail = vec![0u8; half];
    file.read_exact(&mut tail)?;
    let disposition = if retained {
        "full capture retained"
    } else {
        "full capture discarded after this preview"
    };
    let preview = format!(
        "{}\n... [output truncated; {disposition}] ...\n{}",
        String::from_utf8_lossy(&head).trim_end(),
        String::from_utf8_lossy(&tail).trim_start()
    );
    Ok((preview, true, size))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::TaskOutputTool,
    };

    fn test_context(workspace: &Path) -> ToolContext {
        let context = ToolContext::new(
            workspace.to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .set_task_capture_root(workspace.join(".test-task-captures"))
            .unwrap();
        context
    }

    fn background_id(output: &ToolOutput) -> String {
        output
            .content
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("Command running in background with ID: "))
            .expect("background task id")
            .to_owned()
    }

    #[tokio::test]
    async fn dropping_context_removes_unretained_background_capture() {
        let workspace = tempfile::tempdir().unwrap();
        let context = test_context(workspace.path());
        let started = BashTool
            .execute(
                &context,
                json!({"command":"sleep 30", "run_in_background":true}),
            )
            .await
            .unwrap();
        let id = background_id(&started);
        let output_path = context
            .tasks
            .lock()
            .await
            .get(&id)
            .unwrap()
            .output_path
            .clone();
        let cleanup = OutputFileGuard::new(output_path.clone());
        assert!(output_path.is_file());

        drop(context);

        timeout(Duration::from_secs(2), async {
            while output_path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background capture survived context drop");
        assert!(!output_path.exists());
        drop(cleanup);
    }

    #[tokio::test]
    async fn truncated_task_output_explicitly_retains_background_capture() {
        let workspace = tempfile::tempdir().unwrap();
        let context = test_context(workspace.path());
        let started = BashTool
            .execute(
                &context,
                json!({
                    "command":"printf '%040000d' 0",
                    "run_in_background":true
                }),
            )
            .await
            .unwrap();
        let id = background_id(&started);
        let output_path = context
            .tasks
            .lock()
            .await
            .get(&id)
            .unwrap()
            .output_path
            .clone();
        let cleanup = OutputFileGuard::new(output_path.clone());

        let output = TaskOutputTool
            .execute(
                &context,
                json!({"task_id":id, "block":true, "timeout":10_000}),
            )
            .await
            .unwrap();

        assert!(output.content.contains("Captured output retained at"));
        assert!(output_path.is_file());
        drop(cleanup);
        assert!(!output_path.exists());
    }

    #[tokio::test]
    async fn background_commands_honor_their_execution_timeout() {
        let workspace = tempfile::tempdir().unwrap();
        let context = test_context(workspace.path());
        let started = BashTool
            .execute(
                &context,
                json!({"command":"sleep 5", "timeout":50, "run_in_background":true}),
            )
            .await
            .unwrap();
        let id = started
            .content
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("Command running in background with ID: "))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let output = TaskOutputTool
            .execute(&context, json!({"task_id":id, "block":false, "timeout":0}))
            .await
            .unwrap();
        assert!(output.content.contains("timed out after 50ms"));
    }

    #[tokio::test]
    async fn completed_background_notification_is_once_restorable_and_non_consuming() {
        let workspace = tempfile::tempdir().unwrap();
        let context = test_context(workspace.path());
        let started = BashTool
            .execute(
                &context,
                json!({"command":"printf notification-result", "run_in_background":true}),
            )
            .await
            .unwrap();
        let id = started
            .content
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("Command running in background with ID: "))
            .unwrap()
            .to_owned();
        let checkpoint = context.background_notification_checkpoint().await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        let first = loop {
            let notifications = context.drain_background_notifications().await;
            if !notifications.is_empty() {
                break notifications;
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(first.len(), 1);
        assert!(first[0].contains("notification-result"));
        assert!(context.drain_background_notifications().await.is_empty());

        context
            .restore_background_notification_checkpoint(&checkpoint)
            .await;
        assert_eq!(context.drain_background_notifications().await.len(), 1);
        let polled = TaskOutputTool
            .execute(&context, json!({"task_id":id, "block":false, "timeout":0}))
            .await
            .unwrap();
        assert!(polled.content.contains("notification-result"));
    }

    #[tokio::test]
    async fn foreground_shell_cwd_persists_and_refreshes_nested_agents() {
        let workspace = tempfile::tempdir().unwrap();
        let nested = workspace.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(workspace.path().join("AGENTS.md"), "root-rule").unwrap();
        std::fs::write(nested.join("AGENTS.md"), "nested-rule").unwrap();
        let context = test_context(workspace.path());
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded_for_callback = Arc::clone(&recorded);
        context.set_current_cwd_state_recorder(Some(Arc::new(move |cwd, root| {
            recorded_for_callback
                .lock()
                .unwrap()
                .push((cwd.to_owned(), root.to_owned()));
            Ok(())
        })));
        context.reload_workspace_context().await.unwrap();
        let changed = BashTool
            .execute(&context, json!({"command":"cd nested"}))
            .await
            .unwrap();
        assert!(!changed.is_error, "{}", changed.content);
        assert_eq!(context.cwd(), std::fs::canonicalize(&nested).unwrap());
        assert!(context.workspace_system_context().contains("nested-rule"));
        {
            let recorded = recorded.lock().unwrap();
            assert_eq!(recorded.len(), 1);
            assert_eq!(recorded[0].0, std::fs::canonicalize(&nested).unwrap());
            assert_eq!(
                recorded[0].1,
                std::fs::canonicalize(workspace.path()).unwrap()
            );
        }

        let pwd = BashTool
            .execute(&context, json!({"command":"pwd -P"}))
            .await
            .unwrap();
        assert_eq!(
            pwd.content.trim(),
            std::fs::canonicalize(&nested)
                .unwrap()
                .display()
                .to_string()
        );
    }

    #[tokio::test]
    async fn shell_cwd_outside_trusted_roots_is_not_persisted() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(&outside).unwrap();
        let context = test_context(&workspace);
        let output = BashTool
            .execute(
                &context,
                json!({"command":format!("cd '{}'", outside.display())}),
            )
            .await
            .unwrap();
        assert!(!output.is_error);
        assert!(
            output
                .content
                .contains("outside the trusted working directories")
        );
        assert_eq!(context.cwd(), std::fs::canonicalize(workspace).unwrap());
    }

    #[tokio::test]
    async fn background_shell_does_not_change_session_cwd() {
        let workspace = tempfile::tempdir().unwrap();
        let nested = workspace.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        let context = test_context(workspace.path());
        BashTool
            .execute(
                &context,
                json!({"command":"cd nested", "run_in_background":true}),
            )
            .await
            .unwrap();
        assert_eq!(
            context.cwd(),
            std::fs::canonicalize(workspace.path()).unwrap()
        );
        context.shutdown_background_tasks().await;
    }
}

use std::{
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

use crate::process::{ProcessTreeGuard, terminate_process_tree};

use super::{
    BackgroundTask, Tool, ToolContext, ToolOutput, ensure_private_directory, object_schema,
    parse_input,
};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
pub(super) const MAX_OUTPUT_BYTES: usize = 30_000;
const MAX_CAPTURE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_CAPTURE_DIRECTORY_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CAPTURE_FILES: usize = 1024;
const MAX_BACKGROUND_TASKS: usize = 32;

#[derive(Deserialize)]
struct Input {
    command: String,
    timeout: Option<u64>,
    #[serde(default)]
    run_in_background: bool,
    description: Option<String>,
}

pub struct BashTool;

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

    fn summary(&self, input: &Value) -> String {
        input
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        if input.command.trim().is_empty() {
            bail!("command 不能为空")
        }
        let _description = input.description;
        let shell = default_shell();
        if input.run_in_background {
            return spawn_background(context, &shell, input.command).await;
        }

        let timeout_ms = input
            .timeout
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .min(MAX_TIMEOUT_MS);
        let (output_path, output_file) = create_private_output("foreground")?;
        let mut command = shell_command(&shell, &input.command, &context.cwd());
        let (mut child, drains, capture_truncated) =
            match spawn_captured(&mut command, output_file).await {
                Ok(spawned) => spawned,
                Err(error) => {
                    let _ = std::fs::remove_file(&output_path);
                    return Err(error);
                }
            };
        let process_group_id = child.id();
        let mut process_guard = ProcessTreeGuard::new(process_group_id);
        let status = match timeout(Duration::from_millis(timeout_ms), child.wait()).await {
            Ok(status) => Some(status.context("等待 shell 命令失败")?),
            Err(_) => {
                process_guard.terminate();
                let _ = child.start_kill();
                let _ = child.wait().await;
                None
            }
        };
        await_foreground_drains(drains, process_group_id).await;
        process_guard.disarm();
        let capture_was_truncated = capture_truncated.load(Ordering::Relaxed);
        let (mut preview, preview_truncated, size) =
            read_output_preview(&output_path, MAX_OUTPUT_BYTES)?;
        let keep_output = preview_truncated || capture_was_truncated;
        if keep_output {
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
}

async fn spawn_background(
    context: &ToolContext,
    shell: &str,
    command_text: String,
) -> Result<ToolOutput> {
    if context.tasks.lock().await.len() >= MAX_BACKGROUND_TASKS {
        bail!("后台任务达到 {MAX_BACKGROUND_TASKS} 个限制；请先读取或停止已有任务")
    }
    let id = Uuid::new_v4().to_string();
    let (output_path, output_file) = create_private_output(&id)?;
    let mut command = shell_command(shell, &command_text, &context.cwd());
    let (child, drains, output_truncated) = match spawn_captured(&mut command, output_file).await {
        Ok(spawned) => spawned,
        Err(error) => {
            let _ = std::fs::remove_file(&output_path);
            return Err(error);
        }
    };
    let process_group_id = child.id();
    let mut task = BackgroundTask {
        child,
        output_path: output_path.clone(),
        command: command_text,
        process_group_id,
        drains,
        output_truncated,
    };
    let mut tasks = context.tasks.lock().await;
    if tasks.len() >= MAX_BACKGROUND_TASKS {
        terminate_task(&mut task).await;
        let _ = std::fs::remove_file(&output_path);
        bail!("后台任务达到 {MAX_BACKGROUND_TASKS} 个限制；请先读取或停止已有任务")
    }
    tasks.insert(id.clone(), task);
    Ok(ToolOutput::success(format!(
        "Command running in background with ID: {id}\nOutput: {}",
        context.display_path(&output_path)
    )))
}

fn shell_command(shell: &str, command_text: &str, cwd: &Path) -> Command {
    let mut command = Command::new(shell);
    #[cfg(windows)]
    {
        let executable = Path::new(shell)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(shell)
            .to_ascii_lowercase();
        if executable.contains("powershell") || executable == "pwsh" || executable == "pwsh.exe" {
            command.args(["-NoProfile", "-NonInteractive", "-Command", command_text]);
        } else if executable == "cmd" || executable == "cmd.exe" {
            command.args(["/D", "/S", "/C", command_text]);
        } else {
            command.args(["-lc", command_text]);
        }
    }
    #[cfg(not(windows))]
    command.args(["-lc", command_text]);
    command
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN");
    #[cfg(unix)]
    command.process_group(0);
    command
}

fn default_shell() -> String {
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

async fn spawn_captured(
    command: &mut Command,
    output_file: File,
) -> Result<(Child, Vec<JoinHandle<()>>, Arc<AtomicBool>)> {
    let mut child = command.spawn().context("无法启动 shell 命令")?;
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
    Ok((child, drains, truncated))
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

async fn await_foreground_drains(mut drains: Vec<JoinHandle<()>>, process_group_id: Option<u32>) {
    for index in 0..drains.len() {
        if timeout(Duration::from_secs(1), &mut drains[index])
            .await
            .is_err()
        {
            terminate_process_tree(process_group_id);
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
    let mut tree_terminated = false;
    if child_running {
        terminate_process_tree(task.process_group_id);
        tree_terminated = true;
        let _ = task.child.start_kill();
        let _ = task.child.wait().await;
    }
    let drains = std::mem::take(&mut task.drains);
    for mut drain in drains {
        if timeout(Duration::from_millis(100), &mut drain)
            .await
            .is_err()
        {
            if !tree_terminated {
                terminate_process_tree(task.process_group_id);
                tree_terminated = true;
                let _ = task.child.start_kill();
                let _ = task.child.wait().await;
            }
            let _ = timeout(Duration::from_secs(1), &mut drain).await;
        }
        drain.abort();
    }
}

fn create_private_output(prefix: &str) -> Result<(PathBuf, File)> {
    let base = dirs::home_dir()
        .context("无法确定主目录")?
        .join(".open-agent-harness/tasks");
    ensure_private_directory(&base)?;
    let mut files = 0usize;
    let mut bytes = 0u64;
    for entry in std::fs::read_dir(&base)? {
        let entry = entry?;
        if entry.path().extension().and_then(|value| value.to_str()) != Some("output") {
            continue;
        }
        files += 1;
        bytes = bytes.saturating_add(entry.metadata()?.len());
        if files >= MAX_CAPTURE_FILES || bytes >= MAX_CAPTURE_DIRECTORY_BYTES {
            bail!(
                "任务输出目录达到资源上限（{files} files, {bytes} bytes）；请清理 ~/.open-agent-harness/tasks"
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

pub(super) fn read_output_preview(path: &Path, max_bytes: usize) -> Result<(String, bool, u64)> {
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
    let preview = format!(
        "{}\n... [output truncated; full capture retained] ...\n{}",
        String::from_utf8_lossy(&head).trim_end(),
        String::from_utf8_lossy(&tail).trim_start()
    );
    Ok((preview, true, size))
}

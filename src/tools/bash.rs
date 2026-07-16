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
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
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

use crate::{
    image_processing::{ProcessedImage, normalize_image},
    permissions::static_shell_pipeline,
    process::{ProcessTreeGuard, spawn_managed},
};

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

#[derive(Debug)]
struct SafeQueryPlan {
    script: String,
    operands: Vec<SafeReadOperand>,
}

#[derive(Debug)]
struct SafeReadOperand {
    path: PathBuf,
    directory: bool,
}

const MAX_SAFE_QUERY_OPERANDS: usize = 64;
#[cfg(not(windows))]
const SAFE_QUERY_FD_BASE: i32 = 64;

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

    fn read_only_for(&self, context: &ToolContext, input: &Value) -> bool {
        safe_query_plan(context, input).is_some()
    }

    fn concurrency_safe_for(&self, context: &ToolContext, input: &Value) -> bool {
        safe_query_plan(context, input).is_some()
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

fn safe_query_plan(context: &ToolContext, input: &Value) -> Option<SafeQueryPlan> {
    if input
        .get("run_in_background")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return None;
    }
    let command = input.get("command")?.as_str()?;
    let commands = static_shell_pipeline(command)?;
    let mut rendered = Vec::with_capacity(commands.len());
    let mut operands = Vec::new();
    let mut has_git = false;
    for (index, words) in commands.into_iter().enumerate() {
        has_git |= words.first().is_some_and(|word| word == "git");
        let normalized = normalize_safe_query(context, &words, index > 0, &mut operands)?;
        rendered.push(format!(
            "command {}",
            normalized
                .iter()
                .map(|word| shell_quote(word))
                .collect::<Vec<_>>()
                .join(" ")
        ));
    }
    if has_git && !operands.is_empty()
        || !operands.is_empty() && context.sandbox_runtime().enabled()
    {
        return None;
    }
    let plan = SafeQueryPlan {
        script: rendered.join(" | "),
        operands,
    };
    #[cfg(windows)]
    {
        // Windows does not yet have the inherited fixed-fd execution path used
        // below. Still parse and validate the candidate so this code remains
        // warning-clean on every target, then refuse automatic classification.
        let SafeQueryPlan { script, operands } = plan;
        let _ = script;
        for SafeReadOperand { path, directory } in operands {
            let _ = (path, directory);
        }
        None
    }
    #[cfg(not(windows))]
    {
        Some(plan)
    }
}

fn normalize_safe_query(
    context: &ToolContext,
    words: &[String],
    allow_stdin: bool,
    operands: &mut Vec<SafeReadOperand>,
) -> Option<Vec<String>> {
    let executable = words.first()?.as_str();
    match executable {
        "pwd" => {
            let arguments = words.get(1..).unwrap_or_default();
            (arguments.is_empty() || matches!(arguments, [flag] if flag == "-L" || flag == "-P"))
                .then(|| words.to_vec())
        }
        "git" => normalize_safe_git_query(context, words),
        "cat" => normalize_file_query(context, words, FileQueryKind::Cat, allow_stdin, operands),
        "head" => normalize_file_query(context, words, FileQueryKind::Head, allow_stdin, operands),
        "tail" => normalize_file_query(context, words, FileQueryKind::Tail, allow_stdin, operands),
        "wc" => normalize_file_query(context, words, FileQueryKind::Wc, allow_stdin, operands),
        "ls" => normalize_file_query(context, words, FileQueryKind::Ls, false, operands),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum FileQueryKind {
    Cat,
    Head,
    Tail,
    Wc,
    Ls,
}

fn normalize_file_query(
    context: &ToolContext,
    words: &[String],
    kind: FileQueryKind,
    allow_stdin: bool,
    operands: &mut Vec<SafeReadOperand>,
) -> Option<Vec<String>> {
    let mut normalized = vec![words.first()?.clone()];
    let mut paths = Vec::new();
    let mut index = 1;
    let mut options_done = false;
    while index < words.len() {
        let word = &words[index];
        if !options_done && word == "--" {
            options_done = true;
            normalized.push(word.clone());
            index += 1;
            continue;
        }
        if !options_done && word.starts_with('-') && word != "-" {
            let consumes_value = match kind {
                FileQueryKind::Cat => {
                    if !matches!(
                        word.as_str(),
                        "-A" | "-b"
                            | "-E"
                            | "-n"
                            | "-s"
                            | "-T"
                            | "-v"
                            | "--show-all"
                            | "--number-nonblank"
                            | "--show-ends"
                            | "--number"
                            | "--squeeze-blank"
                            | "--show-tabs"
                            | "--show-nonprinting"
                    ) {
                        return None;
                    }
                    false
                }
                FileQueryKind::Head | FileQueryKind::Tail => {
                    if matches!(
                        word.as_str(),
                        "-q" | "-v" | "--quiet" | "--silent" | "--verbose"
                    ) || is_compact_count_flag(word)
                    {
                        false
                    } else if let Some(value) = word
                        .strip_prefix("--lines=")
                        .or_else(|| word.strip_prefix("--bytes="))
                    {
                        if !safe_count(value) {
                            return None;
                        }
                        false
                    } else if matches!(word.as_str(), "-n" | "-c" | "--lines" | "--bytes") {
                        true
                    } else {
                        return None;
                    }
                }
                FileQueryKind::Wc => {
                    if !(matches!(
                        word.as_str(),
                        "-c" | "-m"
                            | "-l"
                            | "-L"
                            | "-w"
                            | "--bytes"
                            | "--chars"
                            | "--lines"
                            | "--max-line-length"
                            | "--words"
                    ) || (word.len() > 2
                        && word[1..]
                            .bytes()
                            .all(|byte| matches!(byte, b'c' | b'm' | b'l' | b'L' | b'w'))))
                    {
                        return None;
                    }
                    false
                }
                FileQueryKind::Ls => {
                    if !safe_ls_flag(word) {
                        return None;
                    }
                    false
                }
            };
            normalized.push(word.clone());
            if consumes_value {
                index += 1;
                let value = words.get(index)?;
                if !safe_count(value) {
                    return None;
                }
                normalized.push(value.clone());
            }
        } else {
            paths.push(word.as_str());
            if word == "-" && allow_stdin {
                normalized.push(word.clone());
            } else {
                let path =
                    canonical_read_operand(context, word, matches!(kind, FileQueryKind::Ls))?;
                if operands.len() >= MAX_SAFE_QUERY_OPERANDS {
                    return None;
                }
                let marker = format!("__OAH_SAFE_READ_FD_{}__", operands.len());
                operands.push(path);
                normalized.push(marker);
            }
        }
        index += 1;
    }
    if paths.is_empty() {
        if matches!(kind, FileQueryKind::Ls) {
            if context.permissions.has_read_deny_rules() || context.read_path_denied(&context.cwd())
            {
                return None;
            }
        } else if !allow_stdin {
            return None;
        }
    }
    Some(normalized)
}

fn normalize_safe_git_query(context: &ToolContext, words: &[String]) -> Option<Vec<String>> {
    if context.permissions.has_read_deny_rules() || context.read_path_denied(&context.cwd()) {
        return None;
    }
    let mut index = 1;
    let mut normalized = vec![
        "git".to_owned(),
        "-c".to_owned(),
        "core.fsmonitor=false".to_owned(),
        "--no-pager".to_owned(),
    ];
    if words.get(index).is_some_and(|word| word == "--no-pager") {
        index += 1;
    }
    let subcommand = words.get(index)?;
    if !matches!(subcommand.as_str(), "status" | "diff") {
        return None;
    }
    normalized.push(subcommand.clone());
    if subcommand == "diff" {
        normalized.extend(["--no-ext-diff".to_owned(), "--no-textconv".to_owned()]);
    }
    index += 1;
    while index < words.len() {
        let word = &words[index];
        if word == "--" {
            // Git re-resolves pathspecs and repository metadata after the
            // permission decision. Keep auto-classification to repository
            // queries without user-controlled filesystem operands.
            return None;
        } else if subcommand == "status" {
            if !safe_git_status_flag(word) {
                return None;
            }
            normalized.push(word.clone());
        } else if word.starts_with('-') {
            if !safe_git_diff_flag(word) {
                return None;
            }
            normalized.push(word.clone());
        } else if safe_git_revision(word) {
            normalized.push(word.clone());
        } else {
            return None;
        }
        index += 1;
    }
    Some(normalized)
}

fn canonical_read_operand(
    context: &ToolContext,
    value: &str,
    may_list_directory: bool,
) -> Option<SafeReadOperand> {
    if value == "-" || value.contains('\0') {
        return None;
    }
    let path = context.resolve_path(value).ok()?;
    let canonical = std::fs::canonicalize(path).ok()?;
    let metadata = std::fs::symlink_metadata(&canonical).ok()?;
    if (!metadata.is_file() && !metadata.is_dir())
        || metadata.is_dir() && !may_list_directory
        || !context
            .trusted_roots()
            .iter()
            .any(|root| canonical.starts_with(root))
        || context.read_path_denied(&canonical)
        || may_list_directory && metadata.is_dir() && context.permissions.has_read_deny_rules()
    {
        return None;
    }
    Some(SafeReadOperand {
        path: canonical,
        directory: metadata.is_dir(),
    })
}

fn safe_git_status_flag(flag: &str) -> bool {
    matches!(
        flag,
        "-s" | "--short"
            | "--porcelain"
            | "--porcelain=v1"
            | "--porcelain=v2"
            | "-b"
            | "--branch"
            | "--show-stash"
            | "--ahead-behind"
            | "--no-ahead-behind"
            | "-u"
            | "--untracked-files"
            | "-z"
            | "--null"
            | "--renames"
            | "--no-renames"
    ) || flag.starts_with("--untracked-files=")
        || flag.starts_with("--ignored=")
        || flag.starts_with("--find-renames=")
}

fn safe_git_diff_flag(flag: &str) -> bool {
    matches!(
        flag,
        "--cached"
            | "--staged"
            | "--stat"
            | "--numstat"
            | "--shortstat"
            | "--name-only"
            | "--name-status"
            | "--summary"
            | "--check"
            | "--exit-code"
            | "--quiet"
            | "--binary"
            | "--full-index"
            | "--abbrev"
            | "-p"
            | "-u"
            | "--patch"
            | "--no-patch"
            | "-U0"
            | "-U1"
            | "-U2"
            | "-U3"
    ) || flag.starts_with("--unified=")
        || flag.starts_with("--stat=")
        || flag.starts_with("--diff-filter=")
}

fn safe_git_revision(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with(['.', '/', '~'])
        && !value.contains("..")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'_' | b'-' | b'/' | b'^' | b'~' | b':' | b'.')
        })
}

fn safe_ls_flag(flag: &str) -> bool {
    matches!(
        flag,
        "-a" | "-A"
            | "-l"
            | "-h"
            | "-n"
            | "-1"
            | "-d"
            | "-F"
            | "-p"
            | "-R"
            | "-r"
            | "-S"
            | "-t"
            | "-u"
            | "-U"
            | "-x"
            | "-X"
            | "--all"
            | "--almost-all"
            | "--directory"
            | "--human-readable"
            | "--inode"
            | "--numeric-uid-gid"
            | "--reverse"
            | "--recursive"
            | "--size"
    ) || flag.len() > 2
        && flag[1..]
            .bytes()
            .all(|byte| b"aAlhn1dFprStuxX".contains(&byte))
}

fn is_compact_count_flag(flag: &str) -> bool {
    flag.strip_prefix('-').is_some_and(safe_count)
        || flag.len() > 2
            && matches!(flag.as_bytes().get(1), Some(b'n' | b'c'))
            && safe_count(&flag[2..])
}

fn safe_count(value: &str) -> bool {
    let value = value.strip_prefix(['+', '-']).unwrap_or(value);
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn shell_quote(word: &str) -> String {
    format!("'{}'", word.replace('\'', "'\\''"))
}

async fn execute_bash(
    context: &ToolContext,
    input: Value,
    capture_policy: ForegroundCapturePolicy,
) -> Result<ToolOutput> {
    let safe_query = safe_query_plan(context, &input);
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
    let cwd_marker = if safe_query.is_none() {
        let (path, file) = create_private_cwd_marker(context)?;
        drop(file);
        Some((path.clone(), CwdMarkerGuard(path)))
    } else {
        None
    };
    let command_result = match &safe_query {
        Some(plan) => safe_shell_query_command(context, plan),
        None => shell_command(
            context,
            &shell,
            &input.command,
            cwd_marker.as_ref().map(|(path, _)| path.as_path()),
        ),
    };
    let (mut command, sandbox_warning) = match command_result {
        Ok(command) => command,
        Err(error) => {
            if let Some((path, _)) = &cwd_marker {
                let _ = std::fs::remove_file(path);
            }
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
                if let Some((path, _)) = &cwd_marker {
                    let _ = std::fs::remove_file(path);
                }
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
    let shell_image = if status
        .as_ref()
        .is_some_and(std::process::ExitStatus::success)
        && !capture_was_truncated
    {
        normalize_shell_image_output(&output_path).await?
    } else {
        None
    };
    if let Some(image) = &shell_image {
        preview = shell_image_summary(image);
    }
    append_sandbox_warning(&mut preview, sandbox_warning.as_deref());
    if status
        .as_ref()
        .is_some_and(std::process::ExitStatus::success)
    {
        if let Some((path, _)) = &cwd_marker {
            append_cwd_update(context, path, &mut preview).await;
        }
    }
    if let Some((path, _)) = &cwd_marker {
        let _ = std::fs::remove_file(path);
    }
    let keep_output =
        shell_image.is_none() && retain_long_output && (preview_truncated || capture_was_truncated);
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
    if let Some(image) = shell_image {
        let model_content = json!([
            {"type":"text", "text":preview},
            {"type":"image", "source":{
                "type":"base64",
                "media_type":image.media_type,
                "data":BASE64.encode(image.bytes)
            }}
        ]);
        return Ok(ToolOutput::success_with_model_content(
            preview,
            model_content,
        ));
    }
    if preview.is_empty() {
        preview = "Command completed successfully with no output".into();
    }
    Ok(ToolOutput::success(preview))
}

async fn normalize_shell_image_output(path: &Path) -> Result<Option<ProcessedImage>> {
    let mut file = File::open(path).context("无法打开 shell image capture")?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_CAPTURE_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .context("无法读取 shell image capture")?;
    if bytes.len() as u64 > MAX_CAPTURE_FILE_BYTES {
        return Ok(None);
    }
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(None);
    };
    let text = text.trim();
    if text
        .get(.."data:image/".len())
        .is_none_or(|prefix| !prefix.eq_ignore_ascii_case("data:image/"))
    {
        return Ok(None);
    }
    let (header, encoded) = text
        .split_once(',')
        .context("shell image data URI 缺少逗号分隔符")?;
    let header = header.to_ascii_lowercase();
    let declared = header
        .strip_prefix("data:")
        .and_then(|value| value.strip_suffix(";base64"))
        .context("shell image data URI 必须使用 base64 编码且不得带额外参数")?;
    let declared = match declared {
        "image/jpg" => "image/jpeg",
        "image/png" | "image/jpeg" | "image/gif" | "image/webp" => declared,
        _ => bail!("shell image data URI 声明了不支持的 MIME {declared:?}"),
    };
    if encoded.is_empty() {
        bail!("shell image data URI 的 base64 payload 为空")
    }
    let decoded = BASE64
        .decode(encoded)
        .context("shell image data URI 包含无效 base64")?;
    if BASE64.encode(&decoded) != encoded {
        bail!("shell image data URI 不是规范的 RFC 4648 base64")
    }
    let image = tokio::task::spawn_blocking(move || normalize_image(decoded))
        .await
        .context("shell image 处理任务异常终止")?
        .context("shell image 无法归一化")?;
    if image.original_media_type != declared {
        bail!(
            "shell image 内容签名 {} 与声明的 MIME {declared:?} 不一致",
            image.original_media_type
        )
    }
    Ok(Some(image))
}

fn shell_image_summary(image: &ProcessedImage) -> String {
    if image.changed() {
        format!(
            "Shell image normalized: {} bytes, {}x{} {} -> {} bytes, {}x{} {}",
            image.original_bytes,
            image.original_width,
            image.original_height,
            image.original_media_type,
            image.bytes.len(),
            image.display_width,
            image.display_height,
            image.media_type
        )
    } else {
        format!(
            "Shell image: {} bytes, {}x{} {}",
            image.bytes.len(),
            image.display_width,
            image.display_height,
            image.media_type
        )
    }
}

async fn spawn_background(
    context: &ToolContext,
    shell: &str,
    command_text: String,
    timeout_ms: u64,
) -> Result<ToolOutput> {
    {
        let mut tasks = context.tasks.lock().await;
        reclaim_completed_tasks(&mut tasks)?;
        if tasks.len() >= MAX_BACKGROUND_TASKS {
            bail!("后台任务达到 {MAX_BACKGROUND_TASKS} 个限制；请先读取或停止已有任务")
        }
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
    reclaim_completed_tasks(&mut tasks)?;
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

fn reclaim_completed_tasks(
    tasks: &mut std::collections::HashMap<String, BackgroundTask>,
) -> Result<()> {
    if tasks.len() < MAX_BACKGROUND_TASKS {
        return Ok(());
    }
    let mut completed = Vec::new();
    for (id, task) in tasks.iter_mut() {
        if task.child.try_wait()?.is_some()
            && task.drains.iter().all(tokio::task::JoinHandle::is_finished)
        {
            completed.push(id.clone());
        }
    }
    completed.sort_unstable();
    for id in completed {
        tasks.remove(&id);
    }
    Ok(())
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

fn safe_shell_query_command(
    context: &ToolContext,
    plan: &SafeQueryPlan,
) -> Result<(Command, Option<String>)> {
    #[cfg(windows)]
    {
        let _ = (context, plan);
        bail!("Windows 尚未启用固定 argv 的 Bash 只读分类")
    }
    #[cfg(not(windows))]
    {
        use std::os::{fd::AsRawFd as _, unix::process::CommandExt as _};

        let cwd = open_safe_query_directory(context, &context.cwd())?;
        let mut operands = Vec::with_capacity(plan.operands.len());
        let mut script = plan.script.clone();
        for (index, operand) in plan.operands.iter().enumerate() {
            let file = open_safe_query_operand(context, operand)?;
            let descriptor = SAFE_QUERY_FD_BASE
                .checked_add(i32::try_from(index).ok().context("只读查询 fd 数量溢出")?)
                .context("只读查询 fd 编号溢出")?;
            let marker = format!("__OAH_SAFE_READ_FD_{index}__");
            let suffix = if operand.directory { "/" } else { "" };
            script = script.replace(&marker, &format!("/dev/fd/{descriptor}{suffix}"));
            operands.push(file);
        }
        let args = [OsString::from("-c"), OsString::from(script)];
        let prepared = context.sandbox_runtime().command(
            &context.cwd(),
            std::ffi::OsStr::new("/bin/sh"),
            &args,
        )?;
        let (mut command, warning) = prepared.into_parts();
        context.scrub_child_environment(&mut command);
        let cwd_fd = cwd.as_raw_fd();
        let operand_fds = operands
            .iter()
            .map(|file| file.as_raw_fd())
            .collect::<Vec<_>>();
        // SAFETY: only async-signal-safe descriptor operations run between
        // fork and exec. The captured File handles keep every source fd live.
        unsafe {
            command.as_std_mut().pre_exec(move || {
                let _keep_alive = (&cwd, &operands);
                if libc::fchdir(cwd_fd) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                let temporary_base = SAFE_QUERY_FD_BASE
                    .checked_add(i32::try_from(operand_fds.len()).unwrap_or(i32::MAX))
                    .and_then(|value| value.checked_add(16))
                    .ok_or_else(|| std::io::Error::other("safe query fd range overflow"))?;
                let mut temporary = Vec::with_capacity(operand_fds.len());
                for (index, source) in operand_fds.iter().copied().enumerate() {
                    let minimum = temporary_base
                        .checked_add(i32::try_from(index).unwrap_or(i32::MAX))
                        .ok_or_else(|| std::io::Error::other("safe query fd range overflow"))?;
                    let duplicated = libc::fcntl(source, libc::F_DUPFD_CLOEXEC, minimum);
                    if duplicated == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    temporary.push(duplicated);
                }
                for (index, source) in temporary.iter().copied().enumerate() {
                    let target = SAFE_QUERY_FD_BASE + i32::try_from(index).unwrap_or(i32::MAX);
                    if libc::dup2(source, target) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::fcntl(target, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                for descriptor in temporary {
                    libc::close(descriptor);
                }
                Ok(())
            });
        }
        command
            // A classified query must not inherit shell startup variables,
            // exported functions, Git repository overrides, alternate object
            // stores, config paths, pager commands, or executable search
            // paths from the harness process.
            .env_clear()
            .current_dir(context.cwd())
            .env("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
            .env("LC_ALL", "C")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_ATTR_NOSYSTEM", "1")
            .env("GIT_PAGER", "cat")
            .env("PAGER", "cat")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        Ok((command, warning))
    }
}

#[cfg(not(windows))]
fn open_safe_query_directory(context: &ToolContext, path: &Path) -> Result<File> {
    let operand = SafeReadOperand {
        path: path.to_owned(),
        directory: true,
    };
    open_safe_query_operand(context, &operand)
}

#[cfg(not(windows))]
fn open_safe_query_operand(context: &ToolContext, operand: &SafeReadOperand) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    if operand.directory {
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY);
    }
    let file = options
        .open(&operand.path)
        .with_context(|| format!("只读查询路径无法安全打开: {}", operand.path.display()))?;
    let metadata = file.metadata()?;
    if metadata.is_dir() != operand.directory || !metadata.is_file() && !metadata.is_dir() {
        bail!(
            "只读查询路径类型在授权后发生变化: {}",
            operand.path.display()
        )
    }
    let final_path = opened_safe_query_path(&file)?;
    if !context
        .trusted_roots()
        .iter()
        .any(|root| final_path.starts_with(root))
        || context.read_path_denied(&final_path)
        || operand.directory && context.permissions.has_read_deny_rules()
    {
        bail!("只读查询已打开路径越过可信或 Read 权限边界")
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
fn opened_safe_query_path(file: &File) -> Result<PathBuf> {
    use std::os::fd::AsRawFd as _;

    std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .context("无法从已打开句柄复核只读查询最终路径")
}

#[cfg(target_os = "macos")]
fn opened_safe_query_path(file: &File) -> Result<PathBuf> {
    use std::{
        ffi::CStr,
        os::{fd::AsRawFd as _, unix::ffi::OsStrExt as _},
    };

    let mut buffer = [0 as libc::c_char; libc::PATH_MAX as usize];
    // SAFETY: F_GETPATH writes a NUL-terminated path into this live buffer.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETPATH, buffer.as_mut_ptr()) } == -1 {
        return Err(std::io::Error::last_os_error())
            .context("无法从已打开句柄复核只读查询最终路径");
    }
    // SAFETY: successful F_GETPATH guarantees NUL termination.
    let bytes = unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_bytes();
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn opened_safe_query_path(_: &File) -> Result<PathBuf> {
    bail!("当前 Unix 平台不支持从句柄复核只读查询最终路径")
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
    use std::io::Cursor;

    use image::{DynamicImage, ImageBuffer, ImageFormat, Rgba};

    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        sandbox::{SandboxConfig, SandboxRuntime},
        tools::{TaskOutputTool, ToolExecutionObserver, ToolRegistry},
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

    fn png_fixture(width: u32, height: u32) -> Vec<u8> {
        let image = ImageBuffer::from_fn(width, height, |x, y| {
            Rgba([(x % 251) as u8, (y % 239) as u8, 127, 255])
        });
        let mut output = Cursor::new(Vec::new());
        DynamicImage::ImageRgba8(image)
            .write_to(&mut output, ImageFormat::Png)
            .unwrap();
        output.into_inner()
    }

    #[tokio::test]
    async fn shell_data_uri_images_are_normalized_without_base64_preview_leakage() {
        let workspace = tempfile::tempdir().unwrap();
        let context = test_context(workspace.path());
        let original = png_fixture(2_400, 2);
        let encoded = BASE64.encode(&original);
        let command = format!("printf '%s' 'data:image/png;base64,{encoded}'");
        let output = BashTool
            .execute(&context, json!({"command":command}))
            .await
            .unwrap();
        assert!(!output.is_error);
        assert!(output.content.contains("2400x2"));
        assert!(!output.content.contains(&encoded));
        let blocks = output.model_content.unwrap();
        let transported = blocks[1]["source"]["data"].as_str().unwrap();
        let decoded = BASE64.decode(transported).unwrap();
        let image = image::load_from_memory(&decoded).unwrap();
        assert!(image.width() <= crate::image_processing::MAX_IMAGE_WIDTH);
        assert!(image.height() <= crate::image_processing::MAX_IMAGE_HEIGHT);
        assert_eq!(
            blocks[1]["source"]["media_type"],
            crate::image_processing::detect_supported_image_type(&decoded).unwrap()
        );

        let malformed = BashTool
            .execute(
                &context,
                json!({"command":"printf '%s' 'data:image/png;base64,bm90LWltYWdl'"}),
            )
            .await
            .unwrap_err();
        assert!(format!("{malformed:#}").contains("无法归一化"));
    }

    fn context_with_mode(workspace: &Path, mode: PermissionMode, deny: Vec<String>) -> ToolContext {
        let context = ToolContext::new(
            workspace.to_owned(),
            PermissionManager::new(mode, false, Vec::new(), deny),
        );
        context
            .set_task_capture_root(workspace.join(".test-task-captures"))
            .unwrap();
        context
    }

    #[test]
    fn read_only_classifier_is_static_path_aware_and_fail_closed() {
        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("public.txt"), "public").unwrap();
        std::fs::write(workspace.path().join("secret.txt"), "secret").unwrap();
        std::fs::write(outside.path().join("outside.txt"), "outside").unwrap();
        let context = context_with_mode(
            workspace.path(),
            PermissionMode::Plan,
            vec!["Read(secret.txt)".to_owned()],
        );

        for command in [
            "pwd",
            "pwd -P",
            "cat public.txt",
            "head -n 1 public.txt",
            "tail -1 public.txt",
            "wc -l public.txt",
            "cat public.txt | head -n 1",
        ] {
            assert!(
                BashTool.read_only_for(&context, &json!({"command":command})),
                "expected safe: {command}"
            );
        }
        let outside_command = format!("cat {}", outside.path().join("outside.txt").display());
        for command in [
            "cat secret.txt".to_owned(),
            outside_command,
            "git status".to_owned(),
            "git -C . status".to_owned(),
            "sh -c 'pwd'".to_owned(),
            "cat $(pwd)/public.txt".to_owned(),
            "cat <(printf public)".to_owned(),
            "cat public.txt > copy.txt".to_owned(),
            "alias cat=false; cat public.txt".to_owned(),
            "cd . && pwd".to_owned(),
            "cat public.txt | tee copy.txt".to_owned(),
            "cat public.txt | rm public.txt".to_owned(),
            "git diff -- public.txt".to_owned(),
        ] {
            assert!(
                !BashTool.read_only_for(&context, &json!({"command":command})),
                "expected unsafe: {command}"
            );
        }
        assert!(!BashTool.read_only_for(
            &context,
            &json!({"command":"cat public.txt", "run_in_background":true})
        ));
    }

    #[tokio::test]
    async fn plan_and_dont_ask_execute_only_classified_queries() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("data.txt"), "safe-data\n").unwrap();
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(workspace.path())
            .status()
            .unwrap();
        let registry = ToolRegistry::default();
        let plan = context_with_mode(workspace.path(), PermissionMode::Plan, Vec::new());
        for command in [
            "pwd",
            "cat data.txt",
            "git status --short",
            "git diff --stat",
        ] {
            let output = registry
                .execute(&plan, "Bash", json!({"command":command}))
                .await;
            assert!(!output.is_error, "{command}: {}", output.content);
        }
        let denied = registry
            .execute(
                &plan,
                "Bash",
                json!({"command":"printf mutation > data.txt"}),
            )
            .await;
        assert!(denied.is_error);
        assert_eq!(
            std::fs::read_to_string(workspace.path().join("data.txt")).unwrap(),
            "safe-data\n"
        );

        let dont_ask = context_with_mode(workspace.path(), PermissionMode::DontAsk, Vec::new());
        let allowed = registry
            .execute(&dont_ask, "Bash", json!({"command":"cat data.txt"}))
            .await;
        assert!(!allowed.is_error, "{}", allowed.content);
        let denied = registry
            .execute(&dont_ask, "Bash", json!({"command":"touch denied.txt"}))
            .await;
        assert!(denied.is_error);
        assert!(!workspace.path().join("denied.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn classified_file_query_rejects_symlink_swap_before_execution() {
        use std::os::unix::fs::symlink;

        let workspace = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let nested = workspace.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        std::fs::write(nested.join("public.txt"), "public").unwrap();
        std::fs::write(outside.path().join("public.txt"), "outside-secret").unwrap();
        let context = context_with_mode(workspace.path(), PermissionMode::Plan, Vec::new());
        let plan = safe_query_plan(&context, &json!({"command":"cat nested/public.txt"}))
            .expect("initial path is classifiable");

        std::fs::rename(&nested, workspace.path().join("nested-original")).unwrap();
        symlink(outside.path(), &nested).unwrap();
        let error = safe_shell_query_command(&context, &plan).unwrap_err();
        assert!(
            format!("{error:#}").contains("越过可信") || format!("{error:#}").contains("安全打开")
        );
    }

    #[test]
    fn enabled_sandbox_fails_closed_for_fd_backed_file_queries() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("public.txt"), "public").unwrap();
        let context = context_with_mode(workspace.path(), PermissionMode::Plan, Vec::new());
        let config = SandboxConfig {
            enabled: true,
            ..SandboxConfig::default()
        };
        context.set_sandbox_runtime(SandboxRuntime::new(config).unwrap());

        assert!(!BashTool.read_only_for(&context, &json!({"command":"cat public.txt"})));
        assert!(BashTool.read_only_for(&context, &json!({"command":"pwd"})));
    }

    #[tokio::test]
    async fn classified_bash_queries_share_the_parallel_batch_lane() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("one.txt"), "one").unwrap();
        std::fs::write(workspace.path().join("two.txt"), "two").unwrap();
        let context = context_with_mode(workspace.path(), PermissionMode::Plan, Vec::new());
        let registry = ToolRegistry::default();
        let calls = vec![
            ("Bash".to_owned(), json!({"command":"cat one.txt"})),
            ("Bash".to_owned(), json!({"command":"cat two.txt"})),
        ];
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let started = Arc::clone(&events);
        let finished = Arc::clone(&events);
        let observer = ToolExecutionObserver::new(
            Arc::new(move |index| started.lock().unwrap().push(("start", index))),
            Arc::new(move |index, _, _| finished.lock().unwrap().push(("finish", index))),
        );
        let outputs = registry
            .execute_batch_observed(&context, &calls, Some(&observer))
            .await;
        assert!(outputs.iter().all(|output| !output.is_error));
        let events = events.lock().unwrap();
        assert_eq!(&events[..2], &[("start", 0), ("start", 1)]);
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
    async fn completed_background_output_can_be_read_repeatedly() {
        let workspace = tempfile::tempdir().unwrap();
        let context = test_context(workspace.path());
        let started = BashTool
            .execute(
                &context,
                json!({"command":"printf reusable-result", "run_in_background":true}),
            )
            .await
            .unwrap();
        let id = background_id(&started);

        let first = TaskOutputTool
            .execute(
                &context,
                json!({"task_id":id, "block":true, "timeout":10_000}),
            )
            .await
            .unwrap();
        let second = TaskOutputTool
            .execute(&context, json!({"task_id":id, "block":false, "timeout":0}))
            .await
            .unwrap();

        assert!(first.content.contains("reusable-result"));
        assert_eq!(second.content, first.content);
        assert!(context.tasks.lock().await.contains_key(&id));
        context.shutdown_background_tasks().await;
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

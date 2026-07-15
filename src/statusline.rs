//! Bounded, provider-neutral status-line command execution.
//!
//! Callers provide an already-public JSON value. This module never reads user
//! configuration and never performs network I/O. A command is started only
//! when the caller passes `trusted = true`.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::{mpsc, watch},
    task::JoinHandle,
    time::timeout,
};

use crate::ui_settings::StatusLineConfig;

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
#[cfg(windows)]
use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
#[cfg(windows)]
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
#[cfg(windows)]
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{
    CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
};

pub const DEFAULT_STATUS_LINE_TIMEOUT: Duration = Duration::from_secs(5);
pub const MAX_STATUS_LINE_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_STATUS_LINE_DEBOUNCE: Duration = Duration::from_millis(300);
pub const MAX_STATUS_LINE_DEBOUNCE: Duration = Duration::from_secs(5);
pub const MAX_STATUS_LINE_INPUT_BYTES: usize = 64 * 1024;
pub const MAX_STATUS_LINE_STDOUT_BYTES: usize = 8 * 1024;
pub const MAX_STATUS_LINE_STDERR_BYTES: usize = 4 * 1024;
pub const MAX_STATUS_LINE_LINES: usize = 8;
pub const MAX_STATUS_LINE_ANSI_SEQUENCES: usize = 64;
const MAX_ERROR_MESSAGE_BYTES: usize = 512;
const MAX_ANSI_SEQUENCE_BYTES: usize = 32;
const STREAM_DRAIN_GRACE: Duration = Duration::from_millis(250);

const SAFE_ENVIRONMENT_NAMES: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "LANG",
    "LC_ALL",
    "TERM",
    "TMPDIR",
    "TEMP",
    "TMP",
    "SYSTEMROOT",
    "COMSPEC",
    "PATHEXT",
];

const SECRET_ENVIRONMENT_NAMES: &[&str] = &["HARNESS_API_KEY", "HARNESS_AUTH_TOKEN", "AUTH_TOKEN"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusLineRender {
    pub text: String,
    pub line_count: usize,
    pub ansi_sequence_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatusLineOutcome {
    Rendered(StatusLineRender),
    Empty,
    /// A newer invocation or explicit cancellation invalidated this result.
    Stale,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StatusLineError {
    #[error("status-line execution requires explicit trust")]
    Untrusted,
    #[error("invalid status-line configuration: {0}")]
    InvalidConfig(String),
    #[error("status-line JSON input exceeds the {MAX_STATUS_LINE_INPUT_BYTES}-byte limit")]
    InputTooLarge,
    #[error("invalid status-line working directory: {0}")]
    InvalidWorkingDirectory(String),
    #[error("could not start status-line command: {0}")]
    Spawn(String),
    #[error("status-line command timed out after {millis}ms")]
    TimedOut { millis: u64 },
    #[error("status-line command exited unsuccessfully ({code:?}): {stderr}")]
    NonZero { code: Option<i32>, stderr: String },
    #[error("status-line {stream} exceeds its {limit}-byte limit")]
    OutputTooLarge { stream: &'static str, limit: usize },
    #[error("status-line {stream} exceeds the {MAX_STATUS_LINE_LINES}-line limit")]
    TooManyLines { stream: &'static str },
    #[error(
        "status-line {stream} exceeds the {MAX_STATUS_LINE_ANSI_SEQUENCES}-ANSI-sequence limit"
    )]
    TooManyAnsi { stream: &'static str },
    #[error("status-line {stream} contains unsafe terminal control data")]
    UnsafeTerminalData { stream: &'static str },
    #[error("status-line I/O failed: {0}")]
    Io(String),
}

#[derive(Debug, Clone)]
pub struct StatusLineRunner {
    inner: Arc<RunnerInner>,
}

#[derive(Debug)]
struct RunnerInner {
    timeout: Duration,
    generation: AtomicU64,
    active: Mutex<Option<ActiveInvocation>>,
}

#[derive(Debug)]
struct ActiveInvocation {
    generation: u64,
    cancel: watch::Sender<bool>,
}

impl Default for StatusLineRunner {
    fn default() -> Self {
        Self::with_timeout(DEFAULT_STATUS_LINE_TIMEOUT)
            .expect("the built-in status-line timeout is valid")
    }
}

impl StatusLineRunner {
    pub fn with_timeout(command_timeout: Duration) -> Result<Self, StatusLineError> {
        if command_timeout.is_zero() || command_timeout > MAX_STATUS_LINE_TIMEOUT {
            return Err(StatusLineError::InvalidConfig(format!(
                "timeout must be between 1ms and {}ms",
                MAX_STATUS_LINE_TIMEOUT.as_millis()
            )));
        }
        Ok(Self {
            inner: Arc::new(RunnerInner {
                timeout: command_timeout,
                generation: AtomicU64::new(0),
                active: Mutex::new(None),
            }),
        })
    }

    pub fn command_timeout(&self) -> Duration {
        self.inner.timeout
    }

    /// Invalidates the active invocation and kills its process tree. The
    /// corresponding future resolves to [`StatusLineOutcome::Stale`].
    pub fn cancel(&self) {
        let mut active = lock_unpoisoned(&self.inner.active);
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        if let Some(invocation) = active.take() {
            let _ = invocation.cancel.send(true);
        }
    }

    /// Runs one trusted status-line command. `input` must contain public data;
    /// this module serializes it but intentionally does not infer or redact the
    /// caller's schema.
    pub async fn run(
        &self,
        config: &StatusLineConfig,
        trusted: bool,
        input: &Value,
        working_directory: &Path,
    ) -> Result<StatusLineOutcome, StatusLineError> {
        if !trusted {
            return Err(StatusLineError::Untrusted);
        }
        config
            .validate()
            .map_err(|error| StatusLineError::InvalidConfig(bounded_message(&error.to_string())))?;
        let mut input = serde_json::to_vec(input)
            .map_err(|error| StatusLineError::Io(bounded_message(&error.to_string())))?;
        input.push(b'\n');
        if input.len() > MAX_STATUS_LINE_INPUT_BYTES {
            return Err(StatusLineError::InputTooLarge);
        }
        let working_directory = canonical_working_directory(working_directory)?;
        let (generation, cancellation) = self.begin_invocation();
        let execution = execute_command(
            config,
            &input,
            &working_directory,
            self.inner.timeout,
            cancellation,
        )
        .await;
        if !self.finish_invocation(generation) {
            return Ok(StatusLineOutcome::Stale);
        }
        match execution {
            Execution::Outcome(outcome) => Ok(outcome),
            Execution::Failure(error) => Err(error),
            Execution::Cancelled => Ok(StatusLineOutcome::Stale),
        }
    }

    fn begin_invocation(&self) -> (u64, watch::Receiver<bool>) {
        let mut active = lock_unpoisoned(&self.inner.active);
        let generation = self.inner.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let (sender, receiver) = watch::channel(false);
        if let Some(previous) = active.replace(ActiveInvocation {
            generation,
            cancel: sender,
        }) {
            let _ = previous.cancel.send(true);
        }
        (generation, receiver)
    }

    fn finish_invocation(&self, generation: u64) -> bool {
        let mut active = lock_unpoisoned(&self.inner.active);
        let current = self.inner.generation.load(Ordering::Acquire) == generation;
        if active
            .as_ref()
            .is_some_and(|invocation| invocation.generation == generation)
        {
            active.take();
        }
        current
    }
}

impl Drop for RunnerInner {
    fn drop(&mut self) {
        if let Some(active) = lock_unpoisoned(&self.active).take() {
            let _ = active.cancel.send(true);
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn canonical_working_directory(path: &Path) -> Result<PathBuf, StatusLineError> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        StatusLineError::InvalidWorkingDirectory(bounded_message(&error.to_string()))
    })?;
    if !canonical.is_dir() {
        return Err(StatusLineError::InvalidWorkingDirectory(
            "path is not a directory".to_owned(),
        ));
    }
    Ok(canonical)
}

enum Execution {
    Outcome(StatusLineOutcome),
    Failure(StatusLineError),
    Cancelled,
}

async fn execute_command(
    config: &StatusLineConfig,
    input: &[u8],
    working_directory: &Path,
    command_timeout: Duration,
    mut cancellation: watch::Receiver<bool>,
) -> Execution {
    let mut command = shell_command(&config.command);
    command
        .current_dir(working_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    apply_minimal_environment(&mut command);

    let (mut child, mut process_tree) = match spawn_managed(&mut command) {
        Ok(spawned) => spawned,
        Err(error) => return Execution::Failure(error),
    };
    let mut stdin = match child.stdin.take() {
        Some(stdin) => stdin,
        None => {
            terminate_and_reap(&mut child, &mut process_tree).await;
            return Execution::Failure(StatusLineError::Io(
                "child stdin was unavailable".to_owned(),
            ));
        }
    };
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            terminate_and_reap(&mut child, &mut process_tree).await;
            return Execution::Failure(StatusLineError::Io(
                "child stdout was unavailable".to_owned(),
            ));
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            terminate_and_reap(&mut child, &mut process_tree).await;
            return Execution::Failure(StatusLineError::Io(
                "child stderr was unavailable".to_owned(),
            ));
        }
    };

    let owned_input = input.to_vec();
    let mut stdin_task = tokio::spawn(async move {
        let result = stdin.write_all(&owned_input).await;
        let _ = stdin.shutdown().await;
        result
    });
    let (limit_sender, mut limit_receiver) = mpsc::unbounded_channel();
    let _limit_sender_guard = limit_sender.clone();
    let mut stdout_task = tokio::spawn(capture_stream(
        stdout,
        "stdout",
        MAX_STATUS_LINE_STDOUT_BYTES,
        limit_sender.clone(),
    ));
    let mut stderr_task = tokio::spawn(capture_stream(
        stderr,
        "stderr",
        MAX_STATUS_LINE_STDERR_BYTES,
        limit_sender,
    ));

    enum WaitOutcome {
        Exited(std::io::Result<ExitStatus>),
        TimedOut,
        Cancelled,
        OutputLimit(StreamLimitSignal),
    }
    let wait = tokio::select! {
        biased;
        changed = cancellation.changed() => {
            let _ = changed;
            WaitOutcome::Cancelled
        }
        limit = limit_receiver.recv() => match limit {
            Some(limit) => WaitOutcome::OutputLimit(limit),
            None => WaitOutcome::Cancelled,
        },
        result = timeout(command_timeout, child.wait()) => match result {
            Ok(status) => WaitOutcome::Exited(status),
            Err(_) => WaitOutcome::TimedOut,
        }
    };

    match wait {
        WaitOutcome::Cancelled => {
            terminate_tasks_and_reap(
                &mut child,
                &mut process_tree,
                &mut stdin_task,
                &mut stdout_task,
                &mut stderr_task,
            )
            .await;
            Execution::Cancelled
        }
        WaitOutcome::TimedOut => {
            terminate_tasks_and_reap(
                &mut child,
                &mut process_tree,
                &mut stdin_task,
                &mut stdout_task,
                &mut stderr_task,
            )
            .await;
            Execution::Failure(StatusLineError::TimedOut {
                millis: u64::try_from(command_timeout.as_millis()).unwrap_or(u64::MAX),
            })
        }
        WaitOutcome::OutputLimit(limit) => {
            terminate_tasks_and_reap(
                &mut child,
                &mut process_tree,
                &mut stdin_task,
                &mut stdout_task,
                &mut stderr_task,
            )
            .await;
            Execution::Failure(StatusLineError::OutputTooLarge {
                stream: limit.stream,
                limit: limit.limit,
            })
        }
        WaitOutcome::Exited(status) => {
            process_tree.terminate();
            let status = match status {
                Ok(status) => status,
                Err(error) => {
                    terminate_tasks_and_reap(
                        &mut child,
                        &mut process_tree,
                        &mut stdin_task,
                        &mut stdout_task,
                        &mut stderr_task,
                    )
                    .await;
                    return Execution::Failure(StatusLineError::Io(bounded_message(
                        &error.to_string(),
                    )));
                }
            };
            let streams = timeout(STREAM_DRAIN_GRACE, async {
                let _ = (&mut stdin_task).await;
                let stdout = (&mut stdout_task)
                    .await
                    .map_err(|error| bounded_message(&error.to_string()))?
                    .map_err(|error| bounded_message(&error.to_string()))?;
                let stderr = (&mut stderr_task)
                    .await
                    .map_err(|error| bounded_message(&error.to_string()))?
                    .map_err(|error| bounded_message(&error.to_string()))?;
                Ok::<_, String>((stdout, stderr))
            })
            .await;
            let (stdout, stderr) = match streams {
                Ok(Ok(streams)) => streams,
                Ok(Err(error)) => {
                    return Execution::Failure(StatusLineError::Io(error));
                }
                Err(_) => {
                    stdin_task.abort();
                    stdout_task.abort();
                    stderr_task.abort();
                    return Execution::Failure(StatusLineError::Io(
                        "stream drain exceeded its bounded grace period".to_owned(),
                    ));
                }
            };
            evaluate_process_result(status, stdout, stderr)
        }
    }
}

fn shell_command(command_text: &str) -> Command {
    #[cfg(windows)]
    {
        let mut command = Command::new("cmd.exe");
        command.args(["/D", "/S", "/C", command_text]);
        command
    }
    #[cfg(not(windows))]
    {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", command_text]);
        command
    }
}

fn apply_minimal_environment(command: &mut Command) {
    let safe_values = SAFE_ENVIRONMENT_NAMES
        .iter()
        .filter_map(|name| std::env::var_os(name).map(|value| ((*name).to_owned(), value)))
        .filter(|(_, value)| value.len() <= 32 * 1024)
        .collect::<Vec<(String, OsString)>>();
    command.env_clear();
    for (name, value) in safe_values {
        command.env(name, value);
    }
    for name in SECRET_ENVIRONMENT_NAMES {
        command.env_remove(name);
    }
}

#[derive(Debug)]
struct StreamCapture {
    bytes: Vec<u8>,
    overflow: bool,
}

#[derive(Debug, Clone, Copy)]
struct StreamLimitSignal {
    stream: &'static str,
    limit: usize,
}

async fn capture_stream(
    mut stream: impl AsyncRead + Unpin,
    stream_name: &'static str,
    limit: usize,
    limit_sender: mpsc::UnboundedSender<StreamLimitSignal>,
) -> std::io::Result<StreamCapture> {
    let mut bytes = Vec::with_capacity(limit.min(4096));
    let mut overflow = false;
    let mut chunk = [0_u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(bytes.len());
        bytes.extend_from_slice(&chunk[..read.min(remaining)]);
        if read > remaining && !overflow {
            overflow = true;
            let _ = limit_sender.send(StreamLimitSignal {
                stream: stream_name,
                limit,
            });
        }
    }
    Ok(StreamCapture { bytes, overflow })
}

fn evaluate_process_result(
    status: ExitStatus,
    stdout: StreamCapture,
    stderr: StreamCapture,
) -> Execution {
    if stdout.overflow {
        return Execution::Failure(StatusLineError::OutputTooLarge {
            stream: "stdout",
            limit: MAX_STATUS_LINE_STDOUT_BYTES,
        });
    }
    if stderr.overflow {
        return Execution::Failure(StatusLineError::OutputTooLarge {
            stream: "stderr",
            limit: MAX_STATUS_LINE_STDERR_BYTES,
        });
    }
    let stdout = match sanitize_output(&stdout.bytes, "stdout") {
        Ok(output) => output,
        Err(error) => return Execution::Failure(error),
    };
    let stderr = match sanitize_output(&stderr.bytes, "stderr") {
        Ok(output) => output,
        Err(error) => return Execution::Failure(error),
    };
    if !status.success() {
        return Execution::Failure(StatusLineError::NonZero {
            code: status.code(),
            stderr: bounded_message(&stderr.text),
        });
    }
    if stdout.text.is_empty() {
        return Execution::Outcome(StatusLineOutcome::Empty);
    }
    Execution::Outcome(StatusLineOutcome::Rendered(StatusLineRender {
        text: stdout.text,
        line_count: stdout.line_count,
        ansi_sequence_count: stdout.ansi_sequence_count,
    }))
}

#[derive(Debug)]
struct SanitizedOutput {
    text: String,
    line_count: usize,
    ansi_sequence_count: usize,
}

fn sanitize_output(bytes: &[u8], stream: &'static str) -> Result<SanitizedOutput, StatusLineError> {
    let mut sanitized = Vec::with_capacity(bytes.len());
    let mut ansi_sequence_count = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == 0xc2
            && bytes
                .get(index + 1)
                .is_some_and(|byte| (0x80..=0x9f).contains(byte))
        {
            return Err(StatusLineError::UnsafeTerminalData { stream });
        }
        match bytes[index] {
            0x1b => {
                if bytes.get(index + 1) != Some(&b'[') {
                    return Err(StatusLineError::UnsafeTerminalData { stream });
                }
                let sequence_start = index;
                index += 2;
                let parameters_start = index;
                let mut terminated = false;
                while let Some(byte) = bytes.get(index).copied() {
                    if (0x40..=0x7e).contains(&byte) {
                        if byte != b'm'
                            || bytes[parameters_start..index]
                                .iter()
                                .any(|parameter| !matches!(parameter, b'0'..=b'9' | b';' | b':'))
                            || index + 1 - sequence_start > MAX_ANSI_SEQUENCE_BYTES
                        {
                            return Err(StatusLineError::UnsafeTerminalData { stream });
                        }
                        ansi_sequence_count = ansi_sequence_count.saturating_add(1);
                        if ansi_sequence_count > MAX_STATUS_LINE_ANSI_SEQUENCES {
                            return Err(StatusLineError::TooManyAnsi { stream });
                        }
                        sanitized.extend_from_slice(&bytes[sequence_start..=index]);
                        index += 1;
                        terminated = true;
                        break;
                    }
                    if !(0x20..=0x3f).contains(&byte)
                        || index + 1 - sequence_start > MAX_ANSI_SEQUENCE_BYTES
                    {
                        return Err(StatusLineError::UnsafeTerminalData { stream });
                    }
                    index += 1;
                }
                if !terminated {
                    return Err(StatusLineError::UnsafeTerminalData { stream });
                }
            }
            b'\r' => index += 1,
            b'\t' => {
                sanitized.push(b' ');
                index += 1;
            }
            byte if byte == b'\n' || byte >= 0x20 => {
                if byte == 0x7f {
                    return Err(StatusLineError::UnsafeTerminalData { stream });
                }
                sanitized.push(byte);
                index += 1;
            }
            _ => return Err(StatusLineError::UnsafeTerminalData { stream }),
        }
    }
    let decoded =
        String::from_utf8(sanitized).map_err(|_| StatusLineError::UnsafeTerminalData { stream })?;
    let lines = decoded
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.len() > MAX_STATUS_LINE_LINES {
        return Err(StatusLineError::TooManyLines { stream });
    }
    Ok(SanitizedOutput {
        text: lines.join("\n"),
        line_count: lines.len(),
        ansi_sequence_count,
    })
}

async fn terminate_tasks_and_reap(
    child: &mut Child,
    process_tree: &mut ProcessTreeGuard,
    stdin_task: &mut JoinHandle<std::io::Result<()>>,
    stdout_task: &mut JoinHandle<std::io::Result<StreamCapture>>,
    stderr_task: &mut JoinHandle<std::io::Result<StreamCapture>>,
) {
    terminate_and_reap(child, process_tree).await;
    stdin_task.abort();
    stdout_task.abort();
    stderr_task.abort();
}

async fn terminate_and_reap(child: &mut Child, process_tree: &mut ProcessTreeGuard) {
    process_tree.terminate();
    let _ = child.start_kill();
    let _ = timeout(STREAM_DRAIN_GRACE, child.wait()).await;
}

fn bounded_message(message: &str) -> String {
    if message.len() <= MAX_ERROR_MESSAGE_BYTES {
        return message.to_owned();
    }
    let mut boundary = MAX_ERROR_MESSAGE_BYTES;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}…", &message[..boundary])
}

/// Deterministic scheduling state for event debounce and optional periodic
/// refresh. A ticket is current only until the next notification, cancellation,
/// or scheduled run. Callers should pair invalidation with
/// [`StatusLineRunner::cancel`] when a command is already running.
#[derive(Debug, Clone)]
pub struct StatusLineRefreshState {
    debounce: Duration,
    refresh_interval: Option<Duration>,
    pending_since: Option<Instant>,
    last_started: Instant,
    revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusLineRunTicket {
    revision: u64,
}

impl StatusLineRefreshState {
    pub fn from_config(
        config: &StatusLineConfig,
        now: Instant,
        debounce: Duration,
    ) -> Result<Self, StatusLineError> {
        config
            .validate()
            .map_err(|error| StatusLineError::InvalidConfig(bounded_message(&error.to_string())))?;
        if debounce > MAX_STATUS_LINE_DEBOUNCE {
            return Err(StatusLineError::InvalidConfig(format!(
                "debounce exceeds {}ms",
                MAX_STATUS_LINE_DEBOUNCE.as_millis()
            )));
        }
        Ok(Self {
            debounce,
            refresh_interval: config.refresh_interval.map(Duration::from_secs),
            pending_since: None,
            last_started: now,
            revision: 0,
        })
    }

    pub fn notify_change(&mut self, now: Instant) {
        self.revision = self.revision.wrapping_add(1);
        self.pending_since = Some(now);
    }

    pub fn cancel_pending(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        self.pending_since = None;
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        let debounced = self
            .pending_since
            .and_then(|started| started.checked_add(self.debounce));
        let periodic = self
            .refresh_interval
            .and_then(|interval| self.last_started.checked_add(interval));
        match (debounced, periodic) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    pub fn take_due(&mut self, now: Instant) -> Option<StatusLineRunTicket> {
        if self.next_deadline().is_none_or(|deadline| now < deadline) {
            return None;
        }
        self.pending_since = None;
        self.last_started = now;
        self.revision = self.revision.wrapping_add(1);
        Some(StatusLineRunTicket {
            revision: self.revision,
        })
    }

    pub fn is_current(&self, ticket: StatusLineRunTicket) -> bool {
        self.revision == ticket.revision
    }
}

fn spawn_managed(command: &mut Command) -> Result<(Child, ProcessTreeGuard), StatusLineError> {
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(CREATE_SUSPENDED);

    let mut child = command
        .spawn()
        .map_err(|error| StatusLineError::Spawn(bounded_message(&error.to_string())))?;
    let process_tree = ProcessTreeGuard::attach(&mut child)?;
    Ok((child, process_tree))
}

#[derive(Debug)]
struct ProcessTreeGuard {
    #[cfg(not(windows))]
    process_id: Option<u32>,
    armed: bool,
    #[cfg(windows)]
    windows_job: Option<WindowsJob>,
}

impl ProcessTreeGuard {
    fn attach(child: &mut Child) -> Result<Self, StatusLineError> {
        let process_id = match child.id() {
            Some(process_id) => process_id,
            None => {
                let _ = child.start_kill();
                return Err(StatusLineError::Spawn(
                    "spawned child has no process identifier".to_owned(),
                ));
            }
        };
        #[cfg(windows)]
        let windows_job = match WindowsJob::attach(child) {
            Ok(job) => Some(job),
            Err(error) => {
                let _ = child.start_kill();
                return Err(error);
            }
        };
        let guard = Self {
            #[cfg(not(windows))]
            process_id: Some(process_id),
            armed: true,
            #[cfg(windows)]
            windows_job,
        };
        #[cfg(windows)]
        if let Err(error) = resume_suspended_process(process_id) {
            let mut guard = guard;
            guard.terminate();
            let _ = child.start_kill();
            return Err(error);
        }
        Ok(guard)
    }

    fn terminate(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        #[cfg(unix)]
        if let Some(process_id) = self.process_id {
            // SAFETY: `spawn_managed` creates a fresh process group whose ID is
            // the direct child's PID. A negative PID targets only that group.
            unsafe {
                libc::kill(-(process_id as i32), libc::SIGKILL);
            }
        }
        #[cfg(windows)]
        if let Some(job) = self.windows_job.take() {
            job.terminate();
        }
        #[cfg(not(any(unix, windows)))]
        let _ = self.process_id;
    }
}

impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(windows)]
#[derive(Debug)]
struct WindowsJob {
    handle: OwnedHandle,
}

#[cfg(windows)]
impl WindowsJob {
    fn attach(child: &Child) -> Result<Self, StatusLineError> {
        let process_handle = child.raw_handle().ok_or_else(|| {
            StatusLineError::Spawn("Windows child handle is unavailable".to_owned())
        })?;
        // SAFETY: null attributes/name request an unnamed job and the returned
        // handle is checked before it becomes owned.
        let raw_job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw_job.is_null() {
            return Err(StatusLineError::Spawn(bounded_message(
                &std::io::Error::last_os_error().to_string(),
            )));
        }
        // SAFETY: `raw_job` is a new, non-null owned handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw_job.cast()) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the pointer and length describe `limits` for this information class.
        let configured = unsafe {
            SetInformationJobObject(
                handle.as_raw_handle().cast(),
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&limits).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if configured == 0 {
            return Err(StatusLineError::Spawn(bounded_message(
                &std::io::Error::last_os_error().to_string(),
            )));
        }
        // SAFETY: both handles are valid and owned for the duration of this call.
        let assigned = unsafe {
            AssignProcessToJobObject(handle.as_raw_handle().cast(), process_handle.cast())
        };
        if assigned == 0 {
            return Err(StatusLineError::Spawn(bounded_message(
                &std::io::Error::last_os_error().to_string(),
            )));
        }
        Ok(Self { handle })
    }

    fn terminate(self) {
        // SAFETY: the owned job handle is live for this call.
        let _ = unsafe { TerminateJobObject(self.handle.as_raw_handle().cast(), 1) };
    }
}

#[cfg(windows)]
fn resume_suspended_process(process_id: u32) -> Result<(), StatusLineError> {
    // SAFETY: the snapshot result is checked against the documented sentinel.
    let raw_snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if raw_snapshot == INVALID_HANDLE_VALUE {
        return Err(StatusLineError::Spawn(bounded_message(
            &std::io::Error::last_os_error().to_string(),
        )));
    }
    // SAFETY: the validated snapshot handle is owned exactly once.
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw_snapshot.cast()) };
    let mut entry = THREADENTRY32 {
        dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
        ..THREADENTRY32::default()
    };
    // SAFETY: `entry` has the required size and is writable.
    let mut found = unsafe {
        Thread32First(
            snapshot.as_raw_handle().cast(),
            std::ptr::from_mut(&mut entry),
        ) != 0
    };
    let mut resumed = false;
    while found {
        if entry.th32OwnerProcessID == process_id {
            // SAFETY: the requested access only resumes a known child thread.
            let raw_thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if raw_thread.is_null() {
                return Err(StatusLineError::Spawn(bounded_message(
                    &std::io::Error::last_os_error().to_string(),
                )));
            }
            // SAFETY: the validated thread handle is owned exactly once.
            let thread = unsafe { OwnedHandle::from_raw_handle(raw_thread.cast()) };
            // SAFETY: this thread belongs to the newly created suspended child.
            if unsafe { ResumeThread(thread.as_raw_handle().cast()) } == u32::MAX {
                return Err(StatusLineError::Spawn(bounded_message(
                    &std::io::Error::last_os_error().to_string(),
                )));
            }
            resumed = true;
        }
        // SAFETY: snapshot and entry remain valid across enumeration calls.
        found = unsafe {
            Thread32Next(
                snapshot.as_raw_handle().cast(),
                std::ptr::from_mut(&mut entry),
            ) != 0
        };
    }
    if !resumed {
        return Err(StatusLineError::Spawn(
            "no resumable thread was found for the suspended child".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(command: impl Into<String>) -> StatusLineConfig {
        StatusLineConfig {
            command: command.into(),
            padding: 0,
            refresh_interval: None,
            hide_vim_mode_indicator: false,
        }
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn success_empty_nonzero_and_ansi_are_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let runner = StatusLineRunner::default();
        let success = runner
            .run(
                &config("printf ' first \\n second \\n'"),
                true,
                &serde_json::json!({"public":"value"}),
                temp.path(),
            )
            .await
            .unwrap();
        assert_eq!(
            success,
            StatusLineOutcome::Rendered(StatusLineRender {
                text: "first\nsecond".to_owned(),
                line_count: 2,
                ansi_sequence_count: 0,
            })
        );
        let json_stdin = runner
            .run(
                &config("IFS= read -r payload; printf '%s' \"$payload\""),
                true,
                &serde_json::json!({"public":"value"}),
                temp.path(),
            )
            .await
            .unwrap();
        assert!(matches!(
            json_stdin,
            StatusLineOutcome::Rendered(StatusLineRender { ref text, .. })
                if text == r#"{"public":"value"}"#
        ));
        assert_eq!(
            runner
                .run(&config(":"), true, &Value::Null, temp.path())
                .await
                .unwrap(),
            StatusLineOutcome::Empty
        );
        let nonzero = runner
            .run(
                &config("printf 'bounded failure' >&2; exit 7"),
                true,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            nonzero,
            StatusLineError::NonZero {
                code: Some(7),
                ref stderr
            } if stderr == "bounded failure"
        ));
        let ansi = runner
            .run(
                &config("printf '\\033[31mred\\033[0m'"),
                true,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap();
        assert!(matches!(
            ansi,
            StatusLineOutcome::Rendered(StatusLineRender {
                ansi_sequence_count: 2,
                ..
            })
        ));
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn trust_size_line_and_ansi_limits_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("must-not-exist");
        let runner = StatusLineRunner::default();
        let untrusted = runner
            .run(
                &config(format!("touch {}", shell_quote(&marker))),
                false,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap_err();
        assert_eq!(untrusted, StatusLineError::Untrusted);
        assert!(!marker.exists());

        let too_long = runner
            .run(
                &config("i=0; while [ $i -lt 9000 ]; do printf x; i=$((i+1)); done"),
                true,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            too_long,
            StatusLineError::OutputTooLarge {
                stream: "stdout",
                ..
            }
        ));

        let stderr_too_long = runner
            .run(
                &config("i=0; while [ $i -lt 5000 ]; do printf x >&2; i=$((i+1)); done; exit 2"),
                true,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            stderr_too_long,
            StatusLineError::OutputTooLarge {
                stream: "stderr",
                ..
            }
        ));

        let too_many_lines = runner
            .run(
                &config("printf '1\\n2\\n3\\n4\\n5\\n6\\n7\\n8\\n9\\n'"),
                true,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            too_many_lines,
            StatusLineError::TooManyLines { stream: "stdout" }
        );

        let unsafe_ansi = runner
            .run(
                &config("printf '\\033]0;title\\007'"),
                true,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            unsafe_ansi,
            StatusLineError::UnsafeTerminalData { stream: "stdout" }
        );

        let too_many_ansi = runner
            .run(
                &config(
                    "i=0; while [ $i -lt 65 ]; do printf '\\033[31m'; i=$((i+1)); done; printf red",
                ),
                true,
                &Value::Null,
                temp.path(),
            )
            .await
            .unwrap_err();
        assert_eq!(
            too_many_ansi,
            StatusLineError::TooManyAnsi { stream: "stdout" }
        );
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn timeout_kills_the_entire_process_group() {
        let temp = tempfile::tempdir().unwrap();
        let marker = temp.path().join("descendant-survived");
        let runner = StatusLineRunner::with_timeout(Duration::from_millis(80)).unwrap();
        let command = format!("(sleep 1; printf leaked > {}) & wait", shell_quote(&marker));
        let error = runner
            .run(&config(command), true, &Value::Null, temp.path())
            .await
            .unwrap_err();
        assert!(matches!(error, StatusLineError::TimedOut { .. }));
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(!marker.exists());
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn explicitly_configured_secrets_are_scrubbed() {
        let temp = tempfile::tempdir().unwrap();
        let mut command = shell_command(
            "if [ -n \"${HARNESS_API_KEY+x}\" ] || [ -n \"${HARNESS_AUTH_TOKEN+x}\" ] || [ -n \"${AUTH_TOKEN+x}\" ]; then exit 9; fi; printf clean",
        );
        command
            .current_dir(temp.path())
            .env("HARNESS_API_KEY", "must-not-leak")
            .env("HARNESS_AUTH_TOKEN", "must-not-leak")
            .env("AUTH_TOKEN", "must-not-leak");
        apply_minimal_environment(&mut command);
        let output = command.output().await.unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"clean");
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn a_new_invocation_cancels_and_invalidates_the_old_one() {
        let temp = tempfile::tempdir().unwrap();
        let runner = Arc::new(StatusLineRunner::default());
        let old_runner = Arc::clone(&runner);
        let cwd = temp.path().to_owned();
        let old = tokio::spawn(async move {
            old_runner
                .run(&config("sleep 2; printf old"), true, &Value::Null, &cwd)
                .await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        let current = runner
            .run(&config("printf new"), true, &Value::Null, temp.path())
            .await
            .unwrap();
        assert!(matches!(
            current,
            StatusLineOutcome::Rendered(StatusLineRender { ref text, .. }) if text == "new"
        ));
        assert_eq!(old.await.unwrap().unwrap(), StatusLineOutcome::Stale);
    }

    #[test]
    fn refresh_state_debounces_refreshes_and_invalidates_tickets() {
        let mut config = config("printf ok");
        config.refresh_interval = Some(10);
        let start = Instant::now();
        let mut state =
            StatusLineRefreshState::from_config(&config, start, Duration::from_millis(300))
                .unwrap();
        state.notify_change(start + Duration::from_millis(100));
        state.notify_change(start + Duration::from_millis(200));
        assert!(state.take_due(start + Duration::from_millis(499)).is_none());
        let event_ticket = state.take_due(start + Duration::from_millis(500)).unwrap();
        assert!(state.is_current(event_ticket));
        state.notify_change(start + Duration::from_secs(1));
        assert!(!state.is_current(event_ticket));
        state.cancel_pending();
        assert!(state.take_due(start + Duration::from_secs(9)).is_none());
        let refresh_ticket = state
            .take_due(start + Duration::from_millis(10_500))
            .unwrap();
        assert!(state.is_current(refresh_ticket));
    }

    #[tokio::test]
    async fn timeout_and_input_configuration_are_bounded() {
        assert!(StatusLineRunner::with_timeout(Duration::ZERO).is_err());
        assert!(
            StatusLineRunner::with_timeout(MAX_STATUS_LINE_TIMEOUT + Duration::from_millis(1))
                .is_err()
        );
        let temp = tempfile::tempdir().unwrap();
        let oversized = serde_json::json!({
            "value": "x".repeat(MAX_STATUS_LINE_INPUT_BYTES)
        });
        let error = StatusLineRunner::default()
            .run(&config("printf unreachable"), true, &oversized, temp.path())
            .await
            .unwrap_err();
        assert_eq!(error, StatusLineError::InputTooLarge);
    }

    #[cfg(not(windows))]
    fn shell_quote(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
    }
}

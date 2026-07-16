use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    future::pending,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::Child,
    sync::{Mutex, Notify, mpsc},
    task::JoinHandle,
    time::{Instant, interval, sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::HeaderValue,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};
use url::{Host, Url};
use uuid::Uuid;

use crate::{
    network_trust::process_network_trust,
    permissions::{PermissionDecision, PermissionTarget},
    plugins::{PluginMonitorDefinition, PluginMonitorWhen},
    process::{ProcessTreeGuard, spawn_managed},
    session::sanitize_transport_text,
    tools::{
        AsyncOwner, Tool, ToolContext, ToolOutput,
        bash::{
            MAX_OUTPUT_BYTES, append_sandbox_warning, command_is_destructive,
            create_private_output, default_shell, read_output_preview, shell_command,
        },
        parse_input,
    },
    web_tools::resolve_target,
};

const DEFAULT_TIMEOUT_MS: u64 = 5 * 60 * 1000;
const MIN_TIMEOUT_MS: u64 = 1_000;
const MAX_TIMEOUT_MS: u64 = 60 * 60 * 1000;
const MAX_MONITOR_TASKS: usize = 32;
const MAX_DESCRIPTION_BYTES: usize = 2048;
const MAX_COMMAND_BYTES: usize = 64 * 1024;
const MAX_WS_URL_BYTES: usize = 16 * 1024;
const MAX_CAPTURE_BYTES: usize = 8 * 1024 * 1024;
const MAX_EVENT_BYTES: usize = 16 * 1024;
const MAX_EVENT_STREAM_BYTES: usize = 4 * 1024 * 1024;
const MAX_EVENT_COUNT: usize = 4096;
const MAX_EVENTS_PER_SECOND: usize = 200;
const MAX_BATCH_EVENTS: usize = 64;
const MAX_BATCH_BYTES: usize = 64 * 1024;
const BATCH_INTERVAL: Duration = Duration::from_millis(200);
const MAX_NOTIFICATION_QUEUE: usize = 256;
const MAX_NOTIFICATION_QUEUE_BYTES: usize = 1024 * 1024;
const MAX_WS_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_WS_FRAME_BYTES: usize = 64 * 1024;
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const TASK_JOIN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorInput {
    command: Option<String>,
    ws: Option<String>,
    description: String,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default)]
    persistent: bool,
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

pub struct MonitorTool;

#[async_trait]
impl Tool for MonitorTool {
    fn name(&self) -> &str {
        "Monitor"
    }

    fn description(&self) -> &str {
        "Streams bounded events from exactly one sandboxed command or pinned ws/wss endpoint. stdout/text events are delivered in 200ms batches; TaskOutput retains the full bounded capture."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type":"object",
            "properties":{
                "command":{"type":"string","minLength":1,"maxLength":MAX_COMMAND_BYTES},
                "ws":{"type":"string","minLength":1,"maxLength":MAX_WS_URL_BYTES},
                "description":{"type":"string","minLength":1,"maxLength":MAX_DESCRIPTION_BYTES},
                "timeout_ms":{"type":"integer","minimum":MIN_TIMEOUT_MS,"maximum":MAX_TIMEOUT_MS,"default":DEFAULT_TIMEOUT_MS},
                "persistent":{"type":"boolean","default":false}
            },
            "required":["description"],
            "oneOf":[
                {"required":["command"],"not":{"required":["ws"]}},
                {"required":["ws"],"not":{"required":["command"]}}
            ],
            "additionalProperties":false
        })
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, input: &Value) -> bool {
        input
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(command_is_destructive)
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("command")
            .or_else(|| input.get("ws"))
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: MonitorInput = parse_input(input)?;
        validate_input(&input)?;
        let service = context.monitor_service();
        let (id, output_path, warning) = match (input.command, input.ws) {
            (Some(command), None) => {
                service
                    .start_command(
                        context,
                        command,
                        input.description.clone(),
                        input.timeout_ms,
                        input.persistent,
                    )
                    .await?
            }
            (None, Some(ws)) => {
                let (id, output_path) = service
                    .start_websocket(
                        context,
                        ws,
                        input.description.clone(),
                        input.timeout_ms,
                        input.persistent,
                        false,
                    )
                    .await?;
                (id, output_path, None)
            }
            _ => unreachable!("validate_input enforces exactly one source"),
        };
        let mut response = format!(
            "Monitor running with ID: {id}\nDescription: {}\nOutput: {}\nTimeout: {}\nPersistent: {}",
            input.description,
            context.display_path(&output_path),
            if input.persistent {
                "session lifetime".to_owned()
            } else {
                format!("{}ms", input.timeout_ms)
            },
            input.persistent
        );
        append_sandbox_warning(&mut response, warning.as_deref());
        Ok(ToolOutput::success(response))
    }
}

fn validate_input(input: &MonitorInput) -> Result<()> {
    if input.description.trim().is_empty()
        || input.description.len() > MAX_DESCRIPTION_BYTES
        || input.description.chars().any(char::is_control)
    {
        bail!("Monitor description 为空、过长或包含控制字符")
    }
    if !(MIN_TIMEOUT_MS..=MAX_TIMEOUT_MS).contains(&input.timeout_ms) {
        bail!("Monitor timeout_ms 必须在 {MIN_TIMEOUT_MS}..={MAX_TIMEOUT_MS} 范围")
    }
    match (&input.command, &input.ws) {
        (Some(command), None) => {
            if command.trim().is_empty()
                || command.len() > MAX_COMMAND_BYTES
                || command.contains('\0')
            {
                bail!("Monitor command 为空、过长或包含 NUL")
            }
        }
        (None, Some(ws)) => {
            validate_ws_url(&Url::parse(ws).context("Monitor WebSocket URL 无效")?)?;
        }
        _ => bail!("Monitor 必须且只能提供 command 或 ws 之一"),
    }
    Ok(())
}

#[derive(Clone, Default)]
pub struct MonitorService {
    inner: Arc<MonitorInner>,
}

#[derive(Default)]
struct MonitorInner {
    tasks: Mutex<HashMap<String, Arc<MonitorTask>>>,
    retained_outputs: Mutex<HashSet<std::path::PathBuf>>,
    notifications: Mutex<NotificationQueue>,
    plugin_monitors: RwLock<Vec<PluginMonitorDefinition>>,
    launched_plugin_monitors: StdMutex<HashSet<(Uuid, String)>>,
    triggered_skills: StdMutex<HashSet<(Uuid, String)>>,
}

struct MonitorTask {
    id: String,
    owner: AsyncOwner,
    description: String,
    source: MonitorSource,
    timeout_ms: u64,
    persistent: bool,
    output_path: std::path::PathBuf,
    output_truncated: Arc<AtomicBool>,
    output_cleanup_armed: AtomicBool,
    cleanup_armed: AtomicBool,
    state: Mutex<MonitorStatus>,
    cancellation: Arc<MonitorCancellation>,
    process_tree: Option<ProcessTreeGuard>,
    join: Mutex<Option<JoinHandle<()>>>,
}

struct PendingMonitorOutput {
    path: std::path::PathBuf,
    file: Option<File>,
    armed: bool,
}

impl PendingMonitorOutput {
    fn new(path: std::path::PathBuf, file: File) -> Self {
        Self {
            path,
            file: Some(file),
            armed: true,
        }
    }

    fn take_file(&mut self) -> File {
        self.file
            .take()
            .expect("pending Monitor output file must be available exactly once")
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingMonitorOutput {
    fn drop(&mut self) {
        drop(self.file.take());
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[derive(Clone)]
enum MonitorSource {
    Command(String),
    WebSocket(String),
}

impl MonitorSource {
    fn label(&self) -> &str {
        match self {
            Self::Command(command) | Self::WebSocket(command) => command,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Command(_) => "command",
            Self::WebSocket(_) => "websocket",
        }
    }
}

#[derive(Debug, Clone)]
enum MonitorStatus {
    Running,
    Completed(String),
    Failed(String),
    TimedOut,
    Stopped,
    LimitExceeded(String),
}

impl MonitorStatus {
    fn running(&self) -> bool {
        matches!(self, Self::Running)
    }

    fn display(&self, timeout_ms: u64) -> String {
        match self {
            Self::Running => "running".to_owned(),
            Self::Completed(detail) => format!("completed ({detail})"),
            Self::Failed(detail) => format!("failed ({detail})"),
            Self::TimedOut => format!("timed out after {timeout_ms}ms"),
            Self::Stopped => "stopped".to_owned(),
            Self::LimitExceeded(detail) => format!("stopped at resource limit ({detail})"),
        }
    }
}

#[derive(Default)]
struct MonitorCancellation {
    cancelled: AtomicBool,
    notify: Notify,
}

impl MonitorCancellation {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn cancelled(&self) {
        loop {
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            let notified = self.notify.notified();
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Default)]
struct CaptureLimitSignal {
    reached: AtomicBool,
    notify: Notify,
}

impl CaptureLimitSignal {
    fn trigger(&self) {
        self.reached.store(true, Ordering::Release);
        // Unlike notify_waiters, notify_one retains a permit when the actor has
        // not registered its waiter yet. The atomic flag closes the remaining
        // check/register race and makes the hard limit level-triggered.
        self.notify.notify_one();
    }

    async fn reached(&self) {
        loop {
            if self.reached.load(Ordering::Acquire) {
                return;
            }
            let notified = self.notify.notified();
            if self.reached.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for MonitorTask {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if self.cleanup_armed.swap(false, Ordering::AcqRel) {
            if let Some(process_tree) = &self.process_tree {
                process_tree.terminate();
            }
        }
        if self.output_cleanup_armed.swap(false, Ordering::AcqRel) {
            let _ = std::fs::remove_file(&self.output_path);
        }
    }
}

#[derive(Default)]
struct NotificationQueue {
    next_sequence: u64,
    bytes: usize,
    entries: VecDeque<MonitorNotification>,
}

struct MonitorNotification {
    sequence: u64,
    owner: AsyncOwner,
    text: String,
    delivered: bool,
}

pub(crate) struct MonitorNotificationCheckpoint {
    owner: AsyncOwner,
    delivered: HashMap<u64, bool>,
    launched_plugin_monitors: HashSet<String>,
    triggered_skills: HashSet<String>,
}

impl MonitorService {
    pub fn configure_plugin_monitors(&self, monitors: Vec<PluginMonitorDefinition>) {
        *self
            .inner
            .plugin_monitors
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = monitors;
        self.inner
            .launched_plugin_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.inner
            .triggered_skills
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    pub async fn start_always_plugin_monitors(&self, context: &ToolContext) -> Vec<String> {
        let monitors = self
            .inner
            .plugin_monitors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|monitor| matches!(&monitor.when, PluginMonitorWhen::Always))
            .cloned()
            .collect::<Vec<_>>();
        self.start_plugin_monitors(context, monitors).await
    }

    pub async fn trigger_skill_monitors(&self, context: &ToolContext, skill: &str) -> Vec<String> {
        let owner_id = context.async_owner().id();
        {
            let mut triggered = self
                .inner
                .triggered_skills
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !triggered.insert((owner_id, skill.to_owned())) {
                return Vec::new();
            }
        }
        let monitors = self
            .inner
            .plugin_monitors
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|monitor| {
                matches!(&monitor.when, PluginMonitorWhen::OnSkillInvoke(name) if name == skill)
            })
            .cloned()
            .collect::<Vec<_>>();
        self.start_plugin_monitors(context, monitors).await
    }

    async fn start_plugin_monitors(
        &self,
        context: &ToolContext,
        monitors: Vec<PluginMonitorDefinition>,
    ) -> Vec<String> {
        let mut failures = Vec::new();
        let owner_id = context.async_owner().id();
        for monitor in monitors {
            let should_start = self
                .inner
                .launched_plugin_monitors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert((owner_id, monitor.name.clone()));
            if !should_start {
                continue;
            }
            match authorize_plugin_monitor(context, &monitor) {
                Ok(true) => {
                    if let Err(error) = self
                        .start_command(
                            context,
                            monitor.command,
                            monitor.description,
                            DEFAULT_TIMEOUT_MS,
                            true,
                        )
                        .await
                    {
                        failures.push(format!("{}: {error:#}", monitor.name));
                    }
                }
                Ok(false) => failures.push(format!("{}: permission denied", monitor.name)),
                Err(error) => failures.push(format!("{}: {error:#}", monitor.name)),
            }
        }
        failures
    }

    async fn start_command(
        &self,
        context: &ToolContext,
        command_text: String,
        description: String,
        timeout_ms: u64,
        persistent: bool,
    ) -> Result<(String, std::path::PathBuf, Option<String>)> {
        self.ensure_capacity().await?;
        let id = Uuid::new_v4().to_string();
        let (mut command, sandbox_warning) =
            shell_command(context, &default_shell(), &command_text, None)?;
        let (output_path, output_file) = create_private_output(context, &format!("monitor-{id}"))?;
        let mut pending_output = PendingMonitorOutput::new(output_path.clone(), output_file);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let (mut child, process_tree) = match spawn_managed(&mut command) {
            Ok(child) => child,
            Err(error) => {
                return Err(error).context("无法启动 Monitor command");
            }
        };
        let (stdout, stderr) = match (child.stdout.take(), child.stderr.take()) {
            (Some(stdout), Some(stderr)) => (stdout, stderr),
            _ => {
                process_tree.terminate();
                let _ = child.start_kill();
                let _ = child.wait().await;
                bail!("无法捕获 Monitor command 输出")
            }
        };
        let task = Arc::new(MonitorTask {
            id: id.clone(),
            owner: context.async_owner(),
            description,
            source: MonitorSource::Command(command_text),
            timeout_ms,
            persistent,
            output_path: output_path.clone(),
            output_truncated: Arc::new(AtomicBool::new(false)),
            output_cleanup_armed: AtomicBool::new(true),
            cleanup_armed: AtomicBool::new(true),
            state: Mutex::new(MonitorStatus::Running),
            cancellation: Arc::new(MonitorCancellation::default()),
            process_tree: Some(process_tree.clone()),
            join: Mutex::new(None),
        });
        self.insert_task(Arc::clone(&task)).await?;
        let output_file = pending_output.take_file();
        let service = self.clone();
        let task_for_actor = Arc::clone(&task);
        let handle = tokio::spawn(async move {
            run_command_monitor(
                service,
                task_for_actor,
                child,
                stdout,
                stderr,
                output_file,
                process_tree,
            )
            .await;
        });
        pending_output.disarm();
        *task.join.lock().await = Some(handle);
        Ok((id, output_path, sandbox_warning))
    }

    async fn start_websocket(
        &self,
        context: &ToolContext,
        raw_url: String,
        description: String,
        timeout_ms: u64,
        persistent: bool,
        allow_private_network: bool,
    ) -> Result<(String, std::path::PathBuf)> {
        self.ensure_capacity().await?;
        let url = Url::parse(&raw_url).context("Monitor WebSocket URL 无效")?;
        let socket = connect_monitor_websocket(&url, allow_private_network).await?;
        let id = Uuid::new_v4().to_string();
        let (output_path, output_file) = create_private_output(context, &format!("monitor-{id}"))?;
        let mut pending_output = PendingMonitorOutput::new(output_path.clone(), output_file);
        let task = Arc::new(MonitorTask {
            id: id.clone(),
            owner: context.async_owner(),
            description,
            source: MonitorSource::WebSocket(raw_url),
            timeout_ms,
            persistent,
            output_path: output_path.clone(),
            output_truncated: Arc::new(AtomicBool::new(false)),
            output_cleanup_armed: AtomicBool::new(true),
            cleanup_armed: AtomicBool::new(true),
            state: Mutex::new(MonitorStatus::Running),
            cancellation: Arc::new(MonitorCancellation::default()),
            process_tree: None,
            join: Mutex::new(None),
        });
        self.insert_task(Arc::clone(&task)).await?;
        let output_file = pending_output.take_file();
        let service = self.clone();
        let task_for_actor = Arc::clone(&task);
        let handle = tokio::spawn(async move {
            run_websocket_monitor(service, task_for_actor, socket, output_file).await;
        });
        pending_output.disarm();
        *task.join.lock().await = Some(handle);
        Ok((id, output_path))
    }

    async fn ensure_capacity(&self) -> Result<()> {
        if self.inner.tasks.lock().await.len() >= MAX_MONITOR_TASKS {
            bail!("Monitor task 达到 {MAX_MONITOR_TASKS} 个限制")
        }
        Ok(())
    }

    async fn insert_task(&self, task: Arc<MonitorTask>) -> Result<()> {
        let mut tasks = self.inner.tasks.lock().await;
        if tasks.len() >= MAX_MONITOR_TASKS {
            task.cancellation.cancel();
            if let Some(process_tree) = &task.process_tree {
                process_tree.terminate();
            }
            let _ = std::fs::remove_file(&task.output_path);
            bail!("Monitor task 达到 {MAX_MONITOR_TASKS} 个限制")
        }
        tasks.insert(task.id.clone(), task);
        Ok(())
    }

    pub async fn contains(&self, id: &str) -> bool {
        self.inner.tasks.lock().await.contains_key(id)
    }

    pub async fn task_ids(&self) -> HashSet<String> {
        self.inner.tasks.lock().await.keys().cloned().collect()
    }

    pub(crate) async fn owned_task_ids(&self, owner: &AsyncOwner) -> HashSet<String> {
        self.inner
            .tasks
            .lock()
            .await
            .iter()
            .filter(|(_, task)| task.owner == *owner)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub async fn task_output(
        &self,
        context: &ToolContext,
        id: &str,
        block: bool,
        timeout_ms: u64,
    ) -> Result<Option<ToolOutput>> {
        let Some(task) = self.inner.tasks.lock().await.get(id).cloned() else {
            return Ok(None);
        };
        ensure_monitor_access(context, &task)?;
        let started = Instant::now();
        let wait_for = Duration::from_millis(timeout_ms.min(600_000));
        loop {
            let status = task.state.lock().await.clone();
            if status.running() && block && started.elapsed() < wait_for {
                sleep(Duration::from_millis(25)).await;
                continue;
            }
            if !status.running() {
                wait_for_task(&task).await;
            }
            let (output, keep_output_file) = render_task_output(context, &task, &status)?;
            if !status.running() {
                self.remove_finished_task(id, &task).await;
                if keep_output_file {
                    self.inner
                        .retained_outputs
                        .lock()
                        .await
                        .insert(task.output_path.clone());
                    // Register ownership before disarming the task RAII guard.
                    // Cancellation while waiting for the mutex must still let
                    // `MonitorTask::drop` remove the capture.
                    task.output_cleanup_armed.store(false, Ordering::Release);
                } else {
                    let _ = std::fs::remove_file(&task.output_path);
                }
            }
            return Ok(Some(output));
        }
    }

    pub async fn task_stop(&self, context: &ToolContext, id: &str) -> Result<Option<ToolOutput>> {
        let Some(task) = self.inner.tasks.lock().await.get(id).cloned() else {
            return Ok(None);
        };
        ensure_monitor_access(context, &task)?;
        if !task.state.lock().await.running() {
            bail!("Monitor task 已经结束")
        }
        task.cancellation.cancel();
        wait_for_task(&task).await;
        let status = task.state.lock().await.clone();
        let (output, keep_output_file) = render_task_output(context, &task, &status)?;
        self.remove_finished_task(id, &task).await;
        if keep_output_file {
            self.inner
                .retained_outputs
                .lock()
                .await
                .insert(task.output_path.clone());
            task.output_cleanup_armed.store(false, Ordering::Release);
        } else {
            let _ = std::fs::remove_file(&task.output_path);
        }
        Ok(Some(ToolOutput::success(format!(
            "Stopped Monitor task {id}\n{}",
            output.content
        ))))
    }

    async fn remove_finished_task(&self, id: &str, task: &Arc<MonitorTask>) {
        let mut tasks = self.inner.tasks.lock().await;
        if tasks
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(current, task))
        {
            tasks.remove(id);
        }
    }

    pub(crate) async fn rollback_new_tasks(&self, owner: &AsyncOwner, keep: &HashSet<String>) {
        let tasks = {
            let mut current = self.inner.tasks.lock().await;
            let ids = current
                .iter()
                .filter(|(id, task)| task.owner == *owner && !keep.contains(*id))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| current.remove(&id))
                .collect::<Vec<_>>()
        };
        stop_and_cleanup(tasks).await;
    }

    pub async fn shutdown(&self) {
        let tasks = self
            .inner
            .tasks
            .lock()
            .await
            .drain()
            .map(|(_, task)| task)
            .collect::<Vec<_>>();
        stop_and_cleanup(tasks).await;
        let retained = self
            .inner
            .retained_outputs
            .lock()
            .await
            .drain()
            .collect::<Vec<_>>();
        for path in retained {
            let _ = std::fs::remove_file(path);
        }
        let mut notifications = self.inner.notifications.lock().await;
        notifications.entries.clear();
        notifications.bytes = 0;
    }

    async fn enqueue(&self, owner: &AsyncOwner, text: String) -> Result<()> {
        if text.len() > MAX_BATCH_BYTES.saturating_add(4096) {
            bail!("Monitor notification 单条超过大小限制")
        }
        let mut queue = self.inner.notifications.lock().await;
        if queue.entries.len() >= MAX_NOTIFICATION_QUEUE
            || queue.bytes.saturating_add(text.len()) > MAX_NOTIFICATION_QUEUE_BYTES
        {
            bail!("Monitor notification queue 达到资源上限")
        }
        let sequence = queue.next_sequence;
        queue.next_sequence = queue
            .next_sequence
            .checked_add(1)
            .context("Monitor notification sequence 溢出")?;
        queue.bytes += text.len();
        queue.entries.push_back(MonitorNotification {
            sequence,
            owner: owner.clone(),
            text,
            delivered: false,
        });
        Ok(())
    }

    pub(crate) async fn notification_checkpoint(
        &self,
        owner: &AsyncOwner,
    ) -> MonitorNotificationCheckpoint {
        let delivered = {
            let mut queue = self.inner.notifications.lock().await;
            queue
                .entries
                .retain(|entry| entry.owner != *owner || !entry.delivered);
            queue.bytes = queue.entries.iter().map(|entry| entry.text.len()).sum();
            queue
                .entries
                .iter()
                .filter(|entry| entry.owner == *owner)
                .map(|entry| (entry.sequence, entry.delivered))
                .collect()
        };
        let owner_id = owner.id();
        MonitorNotificationCheckpoint {
            owner: owner.clone(),
            delivered,
            launched_plugin_monitors: self
                .inner
                .launched_plugin_monitors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .filter(|(entry_owner, _)| *entry_owner == owner_id)
                .map(|(_, name)| name.clone())
                .collect(),
            triggered_skills: self
                .inner
                .triggered_skills
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .filter(|(entry_owner, _)| *entry_owner == owner_id)
                .map(|(_, name)| name.clone())
                .collect(),
        }
    }

    pub(crate) async fn restore_notification_checkpoint(
        &self,
        checkpoint: &MonitorNotificationCheckpoint,
    ) {
        let mut queue = self.inner.notifications.lock().await;
        queue.entries.retain(|entry| {
            entry.owner != checkpoint.owner || checkpoint.delivered.contains_key(&entry.sequence)
        });
        for entry in queue
            .entries
            .iter_mut()
            .filter(|entry| entry.owner == checkpoint.owner)
        {
            if let Some(delivered) = checkpoint.delivered.get(&entry.sequence) {
                entry.delivered = *delivered;
            }
        }
        queue.bytes = queue.entries.iter().map(|entry| entry.text.len()).sum();
        drop(queue);
        let owner_id = checkpoint.owner.id();
        let mut launched = self
            .inner
            .launched_plugin_monitors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        launched.retain(|(entry_owner, _)| *entry_owner != owner_id);
        launched.extend(
            checkpoint
                .launched_plugin_monitors
                .iter()
                .cloned()
                .map(|name| (owner_id, name)),
        );
        drop(launched);
        let mut triggered = self
            .inner
            .triggered_skills
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        triggered.retain(|(entry_owner, _)| *entry_owner != owner_id);
        triggered.extend(
            checkpoint
                .triggered_skills
                .iter()
                .cloned()
                .map(|name| (owner_id, name)),
        );
    }

    pub(crate) async fn drain_notifications(
        &self,
        owner: &AsyncOwner,
        maximum: usize,
        maximum_bytes: usize,
        cwd: &std::path::Path,
    ) -> Vec<String> {
        let mut queue = self.inner.notifications.lock().await;
        let mut output = Vec::new();
        let mut bytes = 0usize;
        for entry in &mut queue.entries {
            if output.len() >= maximum || bytes >= maximum_bytes {
                break;
            }
            if entry.owner != *owner || entry.delivered {
                continue;
            }
            let remaining = maximum_bytes - bytes;
            let mut rendered = sanitize_transport_text(&entry.text, cwd);
            truncate_notification(&mut rendered, remaining);
            bytes += rendered.len();
            entry.delivered = true;
            output.push(rendered);
        }
        output
    }
}

fn truncate_notification(value: &mut String, maximum: usize) {
    if value.len() <= maximum {
        return;
    }
    if maximum == 0 {
        value.clear();
        return;
    }
    const MARKER: &str =
        "\n[monitor notification truncated; use TaskOutput for the full bounded capture]";
    let marker = if MARKER.len() <= maximum { MARKER } else { "" };
    let mut end = maximum - marker.len();
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value.push_str(marker);
}

fn authorize_plugin_monitor(
    context: &ToolContext,
    monitor: &PluginMonitorDefinition,
) -> Result<bool> {
    let input = json!({"command":monitor.command});
    let target = PermissionTarget::new("Bash", vec![monitor.command.clone()]);
    match context.permissions.decide_invocation_with_targets(
        "Bash",
        &input,
        &Uuid::new_v4().to_string(),
        &monitor.command,
        false,
        command_is_destructive(&monitor.command),
        false,
        &[target],
    )? {
        PermissionDecision::Allow => Ok(true),
        PermissionDecision::AllowWithUpdatedInput(_) => {
            bail!("plugin Monitor permission 不接受隐式 input rewrite")
        }
        PermissionDecision::Deny | PermissionDecision::Interrupt => Ok(false),
    }
}

fn ensure_monitor_access(context: &ToolContext, task: &MonitorTask) -> Result<()> {
    if context.async_owner().can_manage(&task.owner) {
        Ok(())
    } else {
        bail!(
            "Monitor task 不属于当前 context 或其 descendant: {}",
            task.id
        )
    }
}

async fn wait_for_task(task: &Arc<MonitorTask>) {
    let handle = task.join.lock().await.take();
    if let Some(mut handle) = handle {
        if timeout(TASK_JOIN_TIMEOUT, &mut handle).await.is_err() {
            task.cancellation.cancel();
            if let Some(process_tree) = &task.process_tree {
                process_tree.terminate();
            }
            handle.abort();
        }
    }
}

async fn stop_and_cleanup(tasks: Vec<Arc<MonitorTask>>) {
    for task in &tasks {
        task.cancellation.cancel();
        if let Some(process_tree) = &task.process_tree {
            process_tree.terminate();
        }
    }
    for task in tasks {
        wait_for_task(&task).await;
        let _ = std::fs::remove_file(&task.output_path);
    }
}

fn render_task_output(
    context: &ToolContext,
    task: &MonitorTask,
    status: &MonitorStatus,
) -> Result<(ToolOutput, bool)> {
    let (mut output, preview_truncated, size) =
        read_output_preview(&task.output_path, MAX_OUTPUT_BYTES)?;
    let capture_truncated = task.output_truncated.load(Ordering::Relaxed);
    if preview_truncated || capture_truncated {
        output.push_str(&format!(
            "\n[Captured output: {} ({} bytes{})]",
            context.display_path(&task.output_path),
            size,
            if capture_truncated {
                "; additional output discarded at the 8 MiB limit"
            } else {
                ""
            }
        ));
    }
    Ok((
        ToolOutput::success(format!(
            "Status: {}\nMonitor: {}\nDescription: {}\nPersistent: {}\nSource: {}\nOutput:\n{}",
            status.display(task.timeout_ms),
            task.source.kind(),
            task.description,
            task.persistent,
            task.source.label(),
            output
        )),
        preview_truncated || capture_truncated,
    ))
}

enum StreamEvent {
    Text(String),
    Binary { bytes: usize },
    Limit(String),
}

struct BatchAccumulator {
    events: Vec<String>,
    bytes: usize,
    total_events: usize,
    total_bytes: usize,
    rate_started: Instant,
    rate_events: usize,
}

impl BatchAccumulator {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            bytes: 0,
            total_events: 0,
            total_bytes: 0,
            rate_started: Instant::now(),
            rate_events: 0,
        }
    }

    fn push(&mut self, event: StreamEvent) -> Result<Option<String>> {
        let rendered = match event {
            StreamEvent::Text(text) => bounded_event_text(&text)?,
            StreamEvent::Binary { bytes } => format!("[binary frame: {bytes} bytes]"),
            StreamEvent::Limit(reason) => bail!("{reason}"),
        };
        if self.rate_started.elapsed() >= Duration::from_secs(1) {
            self.rate_started = Instant::now();
            self.rate_events = 0;
        }
        self.rate_events = self.rate_events.saturating_add(1);
        self.total_events = self.total_events.saturating_add(1);
        self.total_bytes = self.total_bytes.saturating_add(rendered.len());
        if self.rate_events > MAX_EVENTS_PER_SECOND {
            bail!("event rate 超过每秒 {MAX_EVENTS_PER_SECOND} 条限制")
        }
        if self.total_events > MAX_EVENT_COUNT || self.total_bytes > MAX_EVENT_STREAM_BYTES {
            bail!("event stream 超过 count/bytes 限制")
        }
        let must_flush = !self.events.is_empty()
            && (self.events.len() >= MAX_BATCH_EVENTS
                || self.bytes.saturating_add(rendered.len()) > MAX_BATCH_BYTES);
        let prior_batch = must_flush.then(|| self.take()).flatten();
        self.bytes = self.bytes.saturating_add(rendered.len());
        self.events.push(rendered);
        Ok(prior_batch)
    }

    fn take(&mut self) -> Option<String> {
        if self.events.is_empty() {
            return None;
        }
        self.bytes = 0;
        Some(std::mem::take(&mut self.events).join("\n"))
    }
}

fn bounded_event_text(text: &str) -> Result<String> {
    if text.len() > MAX_EVENT_BYTES {
        bail!("event line 超过 {MAX_EVENT_BYTES} 字节限制")
    }
    Ok(text
        .chars()
        .map(|character| {
            if character.is_control() && !matches!(character, '\t') {
                '\u{fffd}'
            } else {
                character
            }
        })
        .collect())
}

async fn flush_batch(
    service: &MonitorService,
    task: &MonitorTask,
    batch: &mut BatchAccumulator,
) -> Result<()> {
    let Some(events) = batch.take() else {
        return Ok(());
    };
    enqueue_batch(service, task, events).await
}

async fn enqueue_batch(service: &MonitorService, task: &MonitorTask, events: String) -> Result<()> {
    service
        .enqueue(
            &task.owner,
            format!(
                "Monitor {} ({}) events:\n{}\n[Use TaskOutput for the full bounded capture]",
                task.id, task.description, events
            ),
        )
        .await
}

async fn finish_monitor(service: &MonitorService, task: &MonitorTask, status: MonitorStatus) {
    *task.state.lock().await = status.clone();
    task.cleanup_armed.store(false, Ordering::Release);
    let _ = service
        .enqueue(
            &task.owner,
            format!(
                "Monitor {} ({}) {}.",
                task.id,
                task.description,
                status.display(task.timeout_ms)
            ),
        )
        .await;
}

async fn run_command_monitor(
    service: MonitorService,
    task: Arc<MonitorTask>,
    mut child: Child,
    stdout: impl AsyncRead + Unpin + Send + 'static,
    stderr: impl AsyncRead + Unpin + Send + 'static,
    output_file: File,
    process_guard: ProcessTreeGuard,
) {
    let (capture_sender, capture_receiver) = mpsc::channel(32);
    let (event_sender, mut event_receiver) = mpsc::channel(128);
    let capture_limit = Arc::new(CaptureLimitSignal::default());
    let capture_truncated = Arc::clone(&task.output_truncated);
    let stdout_task = tokio::spawn(read_stdout(stdout, capture_sender.clone(), event_sender));
    let stderr_task = tokio::spawn(read_capture(stderr, capture_sender));
    let writer_task = tokio::spawn(write_capture(
        tokio::fs::File::from_std(output_file),
        capture_receiver,
        Arc::clone(&capture_truncated),
        Arc::clone(&capture_limit),
    ));
    let persistent = task.persistent;
    let timeout_ms = task.timeout_ms;
    let deadline = async move {
        if persistent {
            pending::<()>().await;
        } else {
            sleep(Duration::from_millis(timeout_ms)).await;
        }
    };
    tokio::pin!(deadline);
    let mut tick = interval(BATCH_INTERVAL);
    tick.tick().await;
    let mut batch = BatchAccumulator::new();
    let mut events_open = true;
    let mut natural_exit = false;
    let mut status = loop {
        tokio::select! {
            biased;
            _ = task.cancellation.cancelled() => break MonitorStatus::Stopped,
            _ = &mut deadline => break MonitorStatus::TimedOut,
            _ = capture_limit.reached() => {
                break MonitorStatus::LimitExceeded("capture 超过 8 MiB".to_owned());
            }
            event = event_receiver.recv(), if events_open => {
                match event {
                    Some(event) => {
                        match batch.push(event) {
                            Ok(Some(events)) => {
                                if let Err(error) = enqueue_batch(&service, &task, events).await {
                                    break MonitorStatus::LimitExceeded(error.to_string());
                                }
                            }
                            Ok(None) => {}
                            Err(error) => break MonitorStatus::LimitExceeded(error.to_string()),
                        }
                    }
                    None => events_open = false,
                }
            }
            _ = tick.tick() => {
                if let Err(error) = flush_batch(&service, &task, &mut batch).await {
                    break MonitorStatus::LimitExceeded(error.to_string());
                }
            }
            result = child.wait() => {
                natural_exit = true;
                break match result {
                    Ok(exit) if exit.success() => MonitorStatus::Completed(format!("exit {}", exit.code().unwrap_or(0))),
                    Ok(exit) => MonitorStatus::Failed(format!("exit {}", exit.code().unwrap_or(-1))),
                    Err(error) => MonitorStatus::Failed(format!("wait failed: {error}")),
                };
            }
        }
    };
    if !natural_exit {
        process_guard.terminate();
        let _ = child.start_kill();
        let _ = timeout(Duration::from_secs(1), child.wait()).await;
        while let Ok(Some(_)) = timeout(Duration::from_secs(1), event_receiver.recv()).await {}
    } else {
        // The shell may have spawned descendants that outlive its own exit.
        // Reap the dedicated group even on a successful natural completion.
        process_guard.terminate();
        loop {
            match timeout(Duration::from_secs(1), event_receiver.recv()).await {
                Ok(Some(event)) => match batch.push(event) {
                    Ok(Some(events)) => {
                        if let Err(error) = enqueue_batch(&service, &task, events).await {
                            status = MonitorStatus::LimitExceeded(error.to_string());
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        status = MonitorStatus::LimitExceeded(error.to_string());
                        break;
                    }
                },
                Ok(None) => break,
                Err(_) => {
                    status =
                        MonitorStatus::LimitExceeded("stdout drain 超过 1s 资源上限".to_owned());
                    break;
                }
            }
        }
    }
    if let Err(error) = flush_batch(&service, &task, &mut batch).await {
        status = MonitorStatus::LimitExceeded(error.to_string());
    }
    await_capture_tasks(vec![stdout_task, stderr_task], writer_task, &process_guard).await;
    process_guard.disarm();
    finish_monitor(&service, &task, status).await;
}

async fn read_stdout(
    mut reader: impl AsyncRead + Unpin,
    capture: mpsc::Sender<Vec<u8>>,
    events: mpsc::Sender<StreamEvent>,
) {
    let mut chunk = [0u8; 8192];
    let mut line = Vec::new();
    loop {
        let count = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        if capture.send(chunk[..count].to_vec()).await.is_err() {
            break;
        }
        for byte in &chunk[..count] {
            if *byte == b'\n' {
                if send_line(&events, &mut line).await.is_err() {
                    return;
                }
            } else {
                line.push(*byte);
                if line.len() > MAX_EVENT_BYTES {
                    let _ = events
                        .send(StreamEvent::Limit(format!(
                            "stdout line 超过 {MAX_EVENT_BYTES} 字节"
                        )))
                        .await;
                    return;
                }
            }
        }
    }
    if !line.is_empty() {
        let _ = send_line(&events, &mut line).await;
    }
}

async fn send_line(events: &mpsc::Sender<StreamEvent>, line: &mut Vec<u8>) -> Result<()> {
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    let text = String::from_utf8_lossy(line).into_owned();
    line.clear();
    events
        .send(StreamEvent::Text(text))
        .await
        .map_err(|_| anyhow::anyhow!("Monitor event receiver closed"))
}

async fn read_capture(mut reader: impl AsyncRead + Unpin, capture: mpsc::Sender<Vec<u8>>) {
    let mut chunk = [0u8; 8192];
    loop {
        let count = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(count) => count,
        };
        if capture.send(chunk[..count].to_vec()).await.is_err() {
            break;
        }
    }
}

async fn write_capture(
    mut file: tokio::fs::File,
    mut receiver: mpsc::Receiver<Vec<u8>>,
    truncated: Arc<AtomicBool>,
    limit: Arc<CaptureLimitSignal>,
) {
    let mut written = 0usize;
    while let Some(chunk) = receiver.recv().await {
        if written.saturating_add(chunk.len()) > MAX_CAPTURE_BYTES {
            let keep = MAX_CAPTURE_BYTES.saturating_sub(written);
            if keep > 0 {
                let _ = file.write_all(&chunk[..keep]).await;
            }
            truncated.store(true, Ordering::Release);
            limit.trigger();
            break;
        }
        if file.write_all(&chunk).await.is_err() {
            truncated.store(true, Ordering::Release);
            limit.trigger();
            break;
        }
        written += chunk.len();
    }
    let _ = file.flush().await;
}

async fn await_capture_tasks(
    readers: Vec<JoinHandle<()>>,
    mut writer: JoinHandle<()>,
    process_tree: &ProcessTreeGuard,
) {
    for mut reader in readers {
        if timeout(Duration::from_secs(1), &mut reader).await.is_err() {
            process_tree.terminate();
            reader.abort();
        }
    }
    if timeout(Duration::from_secs(1), &mut writer).await.is_err() {
        process_tree.terminate();
        writer.abort();
    }
}

type MonitorSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn run_websocket_monitor(
    service: MonitorService,
    task: Arc<MonitorTask>,
    mut socket: MonitorSocket,
    output_file: File,
) {
    let mut output = tokio::fs::File::from_std(output_file);
    let persistent = task.persistent;
    let timeout_ms = task.timeout_ms;
    let deadline = async move {
        if persistent {
            pending::<()>().await;
        } else {
            sleep(Duration::from_millis(timeout_ms)).await;
        }
    };
    tokio::pin!(deadline);
    let mut tick = interval(BATCH_INTERVAL);
    tick.tick().await;
    let mut batch = BatchAccumulator::new();
    let mut captured = 0usize;
    let mut status = loop {
        tokio::select! {
            biased;
            _ = task.cancellation.cancelled() => break MonitorStatus::Stopped,
            _ = &mut deadline => break MonitorStatus::TimedOut,
            _ = tick.tick() => {
                if let Err(error) = flush_batch(&service, &task, &mut batch).await {
                    break MonitorStatus::LimitExceeded(error.to_string());
                }
            }
            message = socket.next() => {
                let (event, capture) = match message {
                    Some(Ok(Message::Text(text))) => {
                        let text = text.to_string();
                        (StreamEvent::Text(text.clone()), format!("{text}\n"))
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let metadata = format!("[binary frame: {} bytes]", bytes.len());
                        (StreamEvent::Binary { bytes: bytes.len() }, format!("{metadata}\n"))
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break MonitorStatus::Failed("WebSocket pong failed".to_owned());
                        }
                        continue;
                    }
                    Some(Ok(Message::Pong(_))) => continue,
                    Some(Ok(Message::Close(_))) | None => {
                        break MonitorStatus::Completed("WebSocket stream ended".to_owned());
                    }
                    Some(Ok(Message::Frame(_))) => continue,
                    Some(Err(_)) => break MonitorStatus::Failed("WebSocket I/O failed".to_owned()),
                };
                if captured.saturating_add(capture.len()) > MAX_CAPTURE_BYTES {
                    task.output_truncated.store(true, Ordering::Release);
                    break MonitorStatus::LimitExceeded("capture 超过 8 MiB".to_owned());
                }
                if output.write_all(capture.as_bytes()).await.is_err() {
                    break MonitorStatus::Failed("capture write failed".to_owned());
                }
                captured += capture.len();
                match batch.push(event) {
                    Ok(Some(events)) => {
                        if let Err(error) = enqueue_batch(&service, &task, events).await {
                            break MonitorStatus::LimitExceeded(error.to_string());
                        }
                    }
                    Ok(None) => {}
                    Err(error) => break MonitorStatus::LimitExceeded(error.to_string()),
                }
            }
        }
    };
    if let Err(error) = flush_batch(&service, &task, &mut batch).await {
        status = MonitorStatus::LimitExceeded(error.to_string());
    }
    let _ = output.flush().await;
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "monitor stopped".into(),
        })))
        .await;
    finish_monitor(&service, &task, status).await;
}

async fn connect_monitor_websocket(
    url: &Url,
    allow_private_network: bool,
) -> Result<MonitorSocket> {
    validate_ws_url(url)?;
    let mut lookup_url = url.clone();
    lookup_url
        .set_scheme(if url.scheme() == "wss" {
            "https"
        } else {
            "http"
        })
        .map_err(|_| anyhow::anyhow!("Monitor WebSocket scheme 无效"))?;
    let target = resolve_target(&lookup_url, allow_private_network).await?;
    let stream = timeout(WS_CONNECT_TIMEOUT, TcpStream::connect(target))
        .await
        .map_err(|_| anyhow::anyhow!("Monitor WebSocket TCP connect timeout"))?
        .context("Monitor WebSocket TCP connect 失败")?;
    stream.set_nodelay(true).ok();
    let mut request = url
        .as_str()
        .into_client_request()
        .context("Monitor WebSocket handshake request 无效")?;
    request.headers_mut().insert(
        "origin",
        HeaderValue::from_str(&websocket_origin(url)).context("Monitor WebSocket Origin 无效")?,
    );
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_WS_FRAME_BYTES))
        .max_write_buffer_size(MAX_WS_MESSAGE_BYTES * 2);
    let connector = if url.scheme() == "wss" {
        process_network_trust().websocket_connector()?
    } else {
        None
    };
    let (socket, _) = timeout(
        WS_CONNECT_TIMEOUT,
        client_async_tls_with_config(request, stream, Some(config), connector),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Monitor WebSocket handshake timeout"))?
    .context("Monitor WebSocket handshake 失败")?;
    Ok(socket)
}

fn validate_ws_url(url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.as_str().len() > MAX_WS_URL_BYTES
    {
        bail!("Monitor WebSocket URL 必须是 bounded、无凭据、无 fragment 的 ws/wss URL")
    }
    for (key, _) in url.query_pairs() {
        if sensitive_query_key(&key) {
            bail!("Monitor WebSocket URL 不允许 query credential")
        }
    }
    Ok(())
}

fn websocket_origin(url: &Url) -> String {
    let scheme = if url.scheme() == "wss" {
        "https"
    } else {
        "http"
    };
    let host = match url.host() {
        Some(Host::Domain(host)) => host.to_owned(),
        Some(Host::Ipv4(address)) => address.to_string(),
        Some(Host::Ipv6(address)) => format!("[{address}]"),
        None => String::new(),
    };
    match url.port() {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}

fn sensitive_query_key(key: &str) -> bool {
    let normalized = key
        .bytes()
        .filter(|byte| byte.is_ascii_alphanumeric())
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let normalized = String::from_utf8_lossy(&normalized);
    normalized == "key"
        || normalized == "sig"
        || normalized == "jwt"
        || normalized.ends_with("key")
        || normalized.contains("auth")
        || normalized.contains("token")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("credential")
        || normalized.contains("session")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::ToolRegistry,
    };
    use tokio::net::TcpListener;
    #[cfg(unix)]
    use tokio::time::sleep;
    use tokio_tungstenite::{
        accept_hdr_async,
        tungstenite::handshake::server::{Request, Response},
    };

    fn test_context(root: &std::path::Path) -> ToolContext {
        let context = ToolContext::new(
            root.to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        context
            .set_task_capture_root(root.join(".test-task-captures"))
            .unwrap();
        context
    }

    fn synthetic_monitor_task(
        owner: &AsyncOwner,
        id: &str,
        output_path: std::path::PathBuf,
    ) -> Arc<MonitorTask> {
        std::fs::write(&output_path, format!("output for {id}\n")).unwrap();
        Arc::new(MonitorTask {
            id: id.to_owned(),
            owner: owner.clone(),
            description: id.to_owned(),
            source: MonitorSource::Command("synthetic".to_owned()),
            timeout_ms: DEFAULT_TIMEOUT_MS,
            persistent: false,
            output_path,
            output_truncated: Arc::new(AtomicBool::new(false)),
            output_cleanup_armed: AtomicBool::new(true),
            cleanup_armed: AtomicBool::new(true),
            state: Mutex::new(MonitorStatus::Running),
            cancellation: Arc::new(MonitorCancellation::default()),
            process_tree: None,
            join: Mutex::new(None),
        })
    }

    #[tokio::test]
    async fn dropping_service_removes_unretained_completed_capture() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let owner = context.async_owner();
        let service = context.monitor_service();
        drop(context);
        let path = temp.path().join("completed-unretained.output");
        let task = synthetic_monitor_task(&owner, "completed-unretained", path.clone());
        *task.state.lock().await = MonitorStatus::Completed("exit 0".to_owned());
        task.cleanup_armed.store(false, Ordering::Release);
        service.insert_task(Arc::clone(&task)).await.unwrap();
        drop(task);
        assert!(path.exists());

        drop(service);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn cancelling_output_while_registering_retention_does_not_leak_capture() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let service = context.monitor_service();
        let path = temp.path().join("cancelled-retention.output");
        let task =
            synthetic_monitor_task(&context.async_owner(), "cancelled-retention", path.clone());
        *task.state.lock().await = MonitorStatus::Completed("exit 0".to_owned());
        task.cleanup_armed.store(false, Ordering::Release);
        task.output_truncated.store(true, Ordering::Release);
        service.insert_task(Arc::clone(&task)).await.unwrap();
        drop(task);

        let retained_lock = service.inner.retained_outputs.lock().await;
        let output = tokio::spawn({
            let context = context.clone();
            let service = service.clone();
            async move {
                service
                    .task_output(&context, "cancelled-retention", false, 0)
                    .await
            }
        });
        timeout(Duration::from_secs(1), async {
            while service.contains("cancelled-retention").await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("TaskOutput did not reach retention registration");
        output.abort();
        let _ = output.await;
        drop(retained_lock);

        assert!(!path.exists());
    }

    #[tokio::test]
    async fn concurrent_owner_rollbacks_preserve_ancestors_descendants_and_siblings() {
        let temp = tempfile::tempdir().unwrap();
        let root = test_context(temp.path());
        let child = root.fork_for_agent();
        let sibling = root.fork_for_agent();
        let service = root.monitor_service();
        let root_owner = root.async_owner();
        let child_owner = child.async_owner();
        let sibling_owner = sibling.async_owner();

        service
            .insert_task(synthetic_monitor_task(
                &root_owner,
                "root-baseline",
                temp.path().join("root-baseline.log"),
            ))
            .await
            .unwrap();
        let root_keep = service.owned_task_ids(&root_owner).await;
        service
            .insert_task(synthetic_monitor_task(
                &child_owner,
                "child-after-root-checkpoint",
                temp.path().join("child-after-root-checkpoint.log"),
            ))
            .await
            .unwrap();
        let child_keep = service.owned_task_ids(&child_owner).await;
        service
            .insert_task(synthetic_monitor_task(
                &sibling_owner,
                "sibling",
                temp.path().join("sibling.log"),
            ))
            .await
            .unwrap();
        for (id, owner) in [("root-new", &root_owner), ("child-new", &child_owner)] {
            service
                .insert_task(synthetic_monitor_task(
                    owner,
                    id,
                    temp.path().join(format!("{id}.log")),
                ))
                .await
                .unwrap();
        }

        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let root_rollback = {
            let service = service.clone();
            let barrier = Arc::clone(&barrier);
            let owner = root_owner.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                service.rollback_new_tasks(&owner, &root_keep).await;
            })
        };
        let child_rollback = {
            let service = service.clone();
            let barrier = Arc::clone(&barrier);
            let owner = child_owner.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                service.rollback_new_tasks(&owner, &child_keep).await;
            })
        };
        barrier.wait().await;
        root_rollback.await.unwrap();
        child_rollback.await.unwrap();

        let ids = service.task_ids().await;
        assert!(ids.contains("root-baseline"));
        assert!(ids.contains("child-after-root-checkpoint"));
        assert!(ids.contains("sibling"));
        assert!(!ids.contains("root-new"));
        assert!(!ids.contains("child-new"));
        service.shutdown().await;
    }

    #[tokio::test]
    async fn monitor_access_allows_owner_and_ancestors_but_rejects_siblings_and_descendants() {
        let temp = tempfile::tempdir().unwrap();
        let root = test_context(temp.path());
        let child = root.fork_for_agent();
        let sibling = root.fork_for_agent();
        let service = root.monitor_service();
        let root_task = synthetic_monitor_task(
            &root.async_owner(),
            "root-task",
            temp.path().join("root-task.log"),
        );
        let child_task = synthetic_monitor_task(
            &child.async_owner(),
            "child-task",
            temp.path().join("child-task.log"),
        );
        service.insert_task(root_task).await.unwrap();
        service.insert_task(child_task).await.unwrap();

        assert!(
            service
                .task_output(&sibling, "child-task", false, 0)
                .await
                .is_err()
        );
        assert!(
            service
                .task_output(&child, "root-task", false, 0)
                .await
                .is_err()
        );
        assert!(
            service
                .task_output(&root, "child-task", false, 0)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            service
                .task_stop(&root, "child-task")
                .await
                .unwrap()
                .is_some()
        );
        assert!(service.task_stop(&sibling, "root-task").await.is_err());
        assert!(service.contains("root-task").await);
        assert!(
            service
                .task_stop(&root, "root-task")
                .await
                .unwrap()
                .is_some()
        );
        service.shutdown().await;
    }

    #[tokio::test]
    async fn notification_restore_only_rewinds_the_checkpoint_owner() {
        let temp = tempfile::tempdir().unwrap();
        let root = test_context(temp.path());
        let child = root.fork_for_agent();
        let root_owner = root.async_owner();
        let child_owner = child.async_owner();
        let service = root.monitor_service();
        service
            .enqueue(&root_owner, "root-before".to_owned())
            .await
            .unwrap();
        service
            .enqueue(&child_owner, "child-before".to_owned())
            .await
            .unwrap();
        let root_checkpoint = service.notification_checkpoint(&root_owner).await;
        let child_checkpoint = service.notification_checkpoint(&child_owner).await;
        assert_eq!(
            service
                .drain_notifications(&root_owner, 8, 4096, temp.path())
                .await,
            vec!["root-before"]
        );
        assert_eq!(
            service
                .drain_notifications(&child_owner, 8, 4096, temp.path())
                .await,
            vec!["child-before"]
        );
        service
            .enqueue(&root_owner, "root-after".to_owned())
            .await
            .unwrap();
        service
            .enqueue(&child_owner, "child-after".to_owned())
            .await
            .unwrap();

        service
            .restore_notification_checkpoint(&root_checkpoint)
            .await;
        assert_eq!(
            service
                .drain_notifications(&root_owner, 8, 4096, temp.path())
                .await,
            vec!["root-before"]
        );
        assert_eq!(
            service
                .drain_notifications(&child_owner, 8, 4096, temp.path())
                .await,
            vec!["child-after"]
        );

        service
            .restore_notification_checkpoint(&child_checkpoint)
            .await;
        assert_eq!(
            service
                .drain_notifications(&child_owner, 8, 4096, temp.path())
                .await,
            vec!["child-before"]
        );
    }

    #[tokio::test]
    async fn notification_checkpoint_restores_exact_queue_and_plugin_trigger_state() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let owner = context.async_owner();
        let service = MonitorService::default();
        service
            .inner
            .launched_plugin_monitors
            .lock()
            .unwrap()
            .insert((owner.id(), "baseline-monitor".to_owned()));
        service
            .inner
            .triggered_skills
            .lock()
            .unwrap()
            .insert((owner.id(), "baseline-skill".to_owned()));
        service
            .enqueue(&owner, "before checkpoint".to_owned())
            .await
            .unwrap();
        let checkpoint = service.notification_checkpoint(&owner).await;

        let delivered = service
            .drain_notifications(&owner, 16, 64 * 1024, temp.path())
            .await;
        assert_eq!(delivered, vec!["before checkpoint"]);
        service
            .enqueue(&owner, "after checkpoint".to_owned())
            .await
            .unwrap();
        service
            .inner
            .launched_plugin_monitors
            .lock()
            .unwrap()
            .insert((owner.id(), "rolled-back-monitor".to_owned()));
        service
            .inner
            .triggered_skills
            .lock()
            .unwrap()
            .insert((owner.id(), "rolled-back-skill".to_owned()));

        service.restore_notification_checkpoint(&checkpoint).await;

        assert_eq!(
            *service.inner.launched_plugin_monitors.lock().unwrap(),
            HashSet::from([(owner.id(), "baseline-monitor".to_owned())])
        );
        assert_eq!(
            *service.inner.triggered_skills.lock().unwrap(),
            HashSet::from([(owner.id(), "baseline-skill".to_owned())])
        );
        let restored = service
            .drain_notifications(&owner, 16, 64 * 1024, temp.path())
            .await;
        assert_eq!(restored, vec!["before checkpoint"]);
        assert!(
            service
                .drain_notifications(&owner, 16, 64 * 1024, temp.path())
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn capture_limit_signal_is_level_triggered_before_wait_registration() {
        let signal = CaptureLimitSignal::default();
        signal.trigger();
        timeout(Duration::from_millis(100), signal.reached())
            .await
            .expect("a pre-wait capture-limit signal must not be lost");
    }

    #[tokio::test]
    async fn oversized_notification_is_bounded_and_does_not_block_the_tail() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let owner = context.async_owner();
        let service = MonitorService::default();
        service
            .enqueue(&owner, "x".repeat(MAX_BATCH_BYTES + 1024))
            .await
            .unwrap();
        service
            .enqueue(&owner, "tail notification".to_owned())
            .await
            .unwrap();

        let first = service
            .drain_notifications(&owner, 16, 1024, temp.path())
            .await;
        assert_eq!(first.len(), 1);
        assert!(first[0].len() <= 1024);
        assert!(first[0].contains("monitor notification truncated"));

        let second = service
            .drain_notifications(&owner, 16, 1024, temp.path())
            .await;
        assert_eq!(second, vec!["tail notification"]);
    }

    #[tokio::test]
    async fn pending_monitor_output_removes_file_when_start_is_cancelled() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pending.output");
        let task_path = path.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let file = File::create(&task_path).unwrap();
            let _pending = PendingMonitorOutput::new(task_path, file);
            let _ = ready.send(());
            pending::<()>().await;
        });
        started.await.unwrap();
        task.abort();
        let _ = task.await;
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_monitor_captures_success_stderr_and_failure() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let service = context.monitor_service();
        let (success, _, _) = service
            .start_command(
                &context,
                "printf 'alpha\\nbeta\\n'".to_owned(),
                "success".to_owned(),
                5_000,
                false,
            )
            .await
            .unwrap();
        let success = service
            .task_output(&context, &success, true, 5_000)
            .await
            .unwrap()
            .unwrap();
        assert!(success.content.contains("completed (exit 0)"));
        assert!(success.content.contains("alpha\nbeta"));

        let (failure, _, _) = service
            .start_command(
                &context,
                "printf 'stderr-only\\n' >&2; exit 7".to_owned(),
                "failure".to_owned(),
                5_000,
                false,
            )
            .await
            .unwrap();
        let failure = service
            .task_output(&context, &failure, true, 5_000)
            .await
            .unwrap()
            .unwrap();
        assert!(failure.content.contains("failed (exit 7)"));
        assert!(failure.content.contains("stderr-only"));

        let notifications = service
            .drain_notifications(&context.async_owner(), 16, 64 * 1024, temp.path())
            .await
            .join("\n");
        assert!(notifications.contains("alpha"));
        service.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_monitor_timeout_and_task_stop_reap_process_groups() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let service = context.monitor_service();
        let (timed, _, _) = service
            .start_command(
                &context,
                "sleep 30".to_owned(),
                "timeout".to_owned(),
                MIN_TIMEOUT_MS,
                false,
            )
            .await
            .unwrap();
        let timed = service
            .task_output(&context, &timed, true, 5_000)
            .await
            .unwrap()
            .unwrap();
        assert!(timed.content.contains("timed out after 1000ms"));

        let (stopped, _, _) = service
            .start_command(
                &context,
                "sleep 30".to_owned(),
                "persistent".to_owned(),
                MIN_TIMEOUT_MS,
                true,
            )
            .await
            .unwrap();
        sleep(Duration::from_millis(50)).await;
        let stopped = service
            .task_stop(&context, &stopped)
            .await
            .unwrap()
            .unwrap();
        assert!(stopped.content.contains("Status: stopped"));
        service.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_monitor_stops_firehose_at_event_rate_limit() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let service = context.monitor_service();
        let (id, _, _) = service
            .start_command(
                &context,
                "i=0; while [ $i -lt 300 ]; do printf 'line-%s\\n' \"$i\"; i=$((i+1)); done"
                    .to_owned(),
                "firehose".to_owned(),
                5_000,
                false,
            )
            .await
            .unwrap();
        let output = service
            .task_output(&context, &id, true, 5_000)
            .await
            .unwrap()
            .unwrap();
        assert!(output.content.contains("resource limit"));
        assert!(output.content.contains("event rate"));
        service.shutdown().await;
    }

    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn websocket_monitor_pins_local_mock_origin_and_hides_binary_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
                assert_eq!(request.uri().path(), "/events");
                assert_eq!(
                    request.headers().get("origin").unwrap(),
                    &format!("http://{address}")
                );
                Ok(response)
            })
            .await
            .unwrap();
            socket.send(Message::Text("ready".into())).await.unwrap();
            socket
                .send(Message::Binary(b"secret-binary-payload".to_vec().into()))
                .await
                .unwrap();
            socket.close(None).await.unwrap();
        });
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let service = context.monitor_service();
        let (id, _) = service
            .start_websocket(
                &context,
                format!("ws://{address}/events"),
                "mock websocket".to_owned(),
                5_000,
                false,
                true,
            )
            .await
            .unwrap();
        let output = service
            .task_output(&context, &id, true, 5_000)
            .await
            .unwrap()
            .unwrap();
        assert!(output.content.contains("ready"));
        assert!(output.content.contains("[binary frame: 21 bytes]"));
        assert!(!output.content.contains("secret-binary-payload"));
        server.await.unwrap();
        service.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn trusted_plugin_monitor_triggers_are_deduplicated() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        context.configure_plugin_monitors(vec![
            PluginMonitorDefinition {
                name: "plugin:always".to_owned(),
                command: "printf 'always\\n'".to_owned(),
                description: "always".to_owned(),
                when: PluginMonitorWhen::Always,
            },
            PluginMonitorDefinition {
                name: "plugin:on-review".to_owned(),
                command: "printf 'review\\n'".to_owned(),
                description: "review".to_owned(),
                when: PluginMonitorWhen::OnSkillInvoke("plugin:review".to_owned()),
            },
        ]);
        assert!(context.start_always_plugin_monitors().await.is_empty());
        assert_eq!(context.monitor_service().task_ids().await.len(), 1);
        assert!(
            context
                .trigger_skill_monitors("plugin:review")
                .await
                .is_empty()
        );
        assert_eq!(context.monitor_service().task_ids().await.len(), 2);
        let child = context.fork_for_agent();
        assert!(
            child
                .trigger_skill_monitors("plugin:review")
                .await
                .is_empty()
        );
        assert_eq!(context.monitor_service().task_ids().await.len(), 3);
        assert_eq!(
            context
                .monitor_service()
                .owned_task_ids(&context.async_owner())
                .await
                .len(),
            2
        );
        assert_eq!(
            context
                .monitor_service()
                .owned_task_ids(&child.async_owner())
                .await
                .len(),
            1
        );
        assert!(
            context
                .trigger_skill_monitors("plugin:review")
                .await
                .is_empty()
        );
        assert_eq!(context.monitor_service().task_ids().await.len(), 3);
        context.shutdown_monitors().await;
    }

    #[tokio::test]
    async fn monitor_is_deferred_until_selected_by_tool_search() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path());
        let registry = ToolRegistry::default();
        assert!(!registry.has_active("Monitor"));
        assert!(registry.deferred_count() >= 1);
        let output = registry
            .execute(&context, "ToolSearch", json!({"query":"select:Monitor"}))
            .await;
        assert!(!output.is_error, "{}", output.content);
        assert!(registry.has_active("Monitor"));
        context.shutdown_monitors().await;
    }
}

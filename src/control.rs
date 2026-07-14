use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, BufRead, BufReader, Read, Write},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc as std_mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use crate::{
    permissions::{PermissionDecision, PermissionPromptHandler, PermissionRequest},
    protocol::validate_direct_user_content,
};

pub const MAX_CONTROL_LINE_BYTES: usize = 1024 * 1024;
const MAX_PENDING_REQUESTS: usize = 64;
const MAX_PENDING_LIFECYCLE_EVENTS: usize = 256;
const CONTROL_INBOUND_CAPACITY: usize = 16;
const NOW_INBOUND_CAPACITY: usize = 16;
const NEXT_INBOUND_CAPACITY: usize = 24;
const LATER_INBOUND_CAPACITY: usize = 8;
const CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const CONTROL_REQUEST_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, thiserror::Error)]
#[error("control request interrupted")]
pub struct ControlInterrupted;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePriority {
    Now,
    Next,
    Later,
}

impl QueuePriority {
    fn parse(value: Option<&Value>) -> Result<Self> {
        match value.and_then(Value::as_str) {
            None | Some("next") => Ok(Self::Next),
            Some("now") => Ok(Self::Now),
            Some("later") => Ok(Self::Later),
            Some(other) => bail!("user.priority 无效: {other}"),
        }
    }
}

#[derive(Debug)]
pub enum InboundMessage {
    User {
        uuid: Uuid,
        content: Value,
        priority: QueuePriority,
    },
    ControlRequest {
        request_id: String,
        request: Value,
    },
    UpdateEnvironmentVariables {
        variables: HashMap<String, String>,
    },
    ProtocolError {
        message: String,
    },
    EndOfInput,
}

type PendingSender = std_mpsc::SyncSender<Value>;

#[derive(Default)]
struct LifecycleState {
    session_id: Option<String>,
    pending: VecDeque<PendingStreamOutput>,
}

enum PendingStreamOutput {
    Lifecycle(Uuid, &'static str),
    Message(Value),
}

struct InputLoopState {
    pending: Arc<Mutex<HashMap<String, PendingSender>>>,
    cancel_tx: watch::Sender<u64>,
    request_generation: Arc<AtomicU64>,
    queued_users: Arc<Mutex<HashSet<Uuid>>>,
    cancelled_users: Arc<Mutex<HashSet<Uuid>>>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    replay_user_messages: bool,
    handle: ControlHandle,
}

struct InboundSenders {
    control: mpsc::Sender<InboundMessage>,
    now: mpsc::Sender<InboundMessage>,
    next: mpsc::Sender<InboundMessage>,
    later: mpsc::Sender<InboundMessage>,
    end: mpsc::Sender<InboundMessage>,
}

impl InboundSenders {
    fn sender_for(&self, message: &InboundMessage) -> &mpsc::Sender<InboundMessage> {
        match message {
            InboundMessage::ControlRequest { .. }
            | InboundMessage::UpdateEnvironmentVariables { .. }
            | InboundMessage::ProtocolError { .. } => &self.control,
            InboundMessage::User {
                priority: QueuePriority::Now,
                ..
            } => &self.now,
            InboundMessage::User {
                priority: QueuePriority::Next,
                ..
            } => &self.next,
            InboundMessage::User {
                priority: QueuePriority::Later,
                ..
            } => &self.later,
            InboundMessage::EndOfInput => &self.end,
        }
    }

    fn try_send(
        &self,
        message: InboundMessage,
    ) -> std::result::Result<(), mpsc::error::TrySendError<InboundMessage>> {
        self.sender_for(&message).try_send(message)
    }

    fn try_reserve(
        &self,
        message: &InboundMessage,
    ) -> std::result::Result<mpsc::Permit<'_, InboundMessage>, mpsc::error::TrySendError<()>> {
        self.sender_for(message).try_reserve()
    }
}

struct InboundReceivers {
    control: mpsc::Receiver<InboundMessage>,
    now: mpsc::Receiver<InboundMessage>,
    next: mpsc::Receiver<InboundMessage>,
    later: mpsc::Receiver<InboundMessage>,
    end: mpsc::Receiver<InboundMessage>,
}

fn inbound_channels() -> (InboundSenders, InboundReceivers) {
    let (control_tx, control) = mpsc::channel(CONTROL_INBOUND_CAPACITY);
    let (now_tx, now) = mpsc::channel(NOW_INBOUND_CAPACITY);
    let (next_tx, next) = mpsc::channel(NEXT_INBOUND_CAPACITY);
    let (later_tx, later) = mpsc::channel(LATER_INBOUND_CAPACITY);
    let (end_tx, end) = mpsc::channel(1);
    (
        InboundSenders {
            control: control_tx,
            now: now_tx,
            next: next_tx,
            later: later_tx,
            end: end_tx,
        },
        InboundReceivers {
            control,
            now,
            next,
            later,
            end,
        },
    )
}

#[derive(Clone)]
pub struct ControlHandle {
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    pending: Arc<Mutex<HashMap<String, PendingSender>>>,
    cancel_tx: watch::Sender<u64>,
    request_generation: Arc<AtomicU64>,
    request_timeout: Duration,
    lifecycle: Arc<Mutex<LifecycleState>>,
}

pub struct ControlSession {
    inbound: InboundReceivers,
    queued_users: Arc<Mutex<HashSet<Uuid>>>,
    cancelled_users: Arc<Mutex<HashSet<Uuid>>>,
    handle: ControlHandle,
}

impl ControlSession {
    pub fn stdio(replay_user_messages: bool) -> Self {
        Self::with_io_options(io::stdin(), io::stdout(), replay_user_messages)
    }

    pub fn with_io<R, W>(reader: R, writer: W) -> Self
    where
        R: Read + Send + 'static,
        W: Write + Send + 'static,
    {
        Self::with_io_options(reader, writer, false)
    }

    pub fn with_io_options<R, W>(reader: R, writer: W, replay_user_messages: bool) -> Self
    where
        R: Read + Send + 'static,
        W: Write + Send + 'static,
    {
        Self::with_io_options_and_spawner(reader, writer, replay_user_messages, |task| {
            thread::Builder::new()
                .name("harness-control-input".to_owned())
                .spawn(task)
        })
    }

    fn with_io_options_and_spawner<R, W, F>(
        reader: R,
        writer: W,
        replay_user_messages: bool,
        spawn: F,
    ) -> Self
    where
        R: Read + Send + 'static,
        W: Write + Send + 'static,
        F: FnOnce(Box<dyn FnOnce() + Send + 'static>) -> io::Result<thread::JoinHandle<()>>,
    {
        let (inbound_tx, inbound) = inbound_channels();
        let spawn_failure_control = inbound_tx.control.clone();
        let spawn_failure_end = inbound_tx.end.clone();
        let (cancel_tx, _cancel_rx) = watch::channel(0_u64);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let queued_users = Arc::new(Mutex::new(HashSet::new()));
        let cancelled_users = Arc::new(Mutex::new(HashSet::new()));
        let request_generation = Arc::new(AtomicU64::new(0));
        let lifecycle = Arc::new(Mutex::new(LifecycleState::default()));
        let writer: Arc<Mutex<Box<dyn Write + Send>>> = Arc::new(Mutex::new(Box::new(writer)));
        let handle = ControlHandle {
            writer: Arc::clone(&writer),
            pending: Arc::clone(&pending),
            cancel_tx: cancel_tx.clone(),
            request_generation: Arc::clone(&request_generation),
            request_timeout: CONTROL_REQUEST_TIMEOUT,
            lifecycle,
        };
        let reader_handle = handle.clone();
        let reader_queued_users = Arc::clone(&queued_users);
        let reader_cancelled_users = Arc::clone(&cancelled_users);
        let input_state = InputLoopState {
            pending,
            cancel_tx,
            request_generation,
            queued_users: reader_queued_users,
            cancelled_users: reader_cancelled_users,
            writer,
            replay_user_messages,
            handle: reader_handle,
        };
        let task: Box<dyn FnOnce() + Send + 'static> =
            Box::new(move || read_input_loop(BufReader::new(reader), inbound_tx, input_state));
        if let Err(error) = spawn(task) {
            let _ = spawn_failure_control.try_send(InboundMessage::ProtocolError {
                message: format!("control input reader thread could not start: {error}"),
            });
            let _ = spawn_failure_end.try_send(InboundMessage::EndOfInput);
        }
        Self {
            inbound,
            queued_users,
            cancelled_users,
            handle,
        }
    }

    pub fn handle(&self) -> ControlHandle {
        self.handle.clone()
    }

    pub async fn recv(&mut self) -> Option<InboundMessage> {
        loop {
            let message = tokio::select! {
                biased;
                message = self.inbound.control.recv(),
                    if !self.inbound.control.is_closed() || !self.inbound.control.is_empty() => message,
                message = self.inbound.now.recv(),
                    if !self.inbound.now.is_closed() || !self.inbound.now.is_empty() => message,
                message = self.inbound.next.recv(),
                    if !self.inbound.next.is_closed() || !self.inbound.next.is_empty() => message,
                message = self.inbound.later.recv(),
                    if !self.inbound.later.is_closed() || !self.inbound.later.is_empty() => message,
                message = self.inbound.end.recv(),
                    if !self.inbound.end.is_closed() || !self.inbound.end.is_empty() => message,
                else => return None,
            };
            let Some(message) = message else {
                continue;
            };
            if let InboundMessage::User { uuid, .. } = &message {
                self.queued_users
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(uuid);
                let cancelled = self
                    .cancelled_users
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(uuid);
                if cancelled {
                    continue;
                }
            }
            return Some(message);
        }
    }
}

impl ControlHandle {
    pub fn activate_command_lifecycle(&self, session_id: impl Into<String>) -> Result<()> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let session_id = session_id.into();
        lifecycle.session_id = Some(session_id.clone());
        while let Some(output) = lifecycle.pending.pop_front() {
            match output {
                PendingStreamOutput::Lifecycle(command_uuid, state) => {
                    self.emit_command_lifecycle_for(&session_id, command_uuid, state)?;
                }
                PendingStreamOutput::Message(message) => self.emit(&message)?,
            }
        }
        Ok(())
    }

    pub fn command_lifecycle(&self, command_uuid: Uuid, state: &'static str) -> Result<()> {
        let session_id = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(session_id) = &lifecycle.session_id {
                Some(session_id.clone())
            } else {
                if lifecycle.pending.len() >= MAX_PENDING_LIFECYCLE_EVENTS {
                    lifecycle.pending.pop_front();
                }
                lifecycle
                    .pending
                    .push_back(PendingStreamOutput::Lifecycle(command_uuid, state));
                None
            }
        };
        if let Some(session_id) = session_id {
            self.emit_command_lifecycle_for(&session_id, command_uuid, state)?;
        }
        Ok(())
    }

    fn replay_user_message(&self, message: Value) -> Result<()> {
        let active = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if lifecycle.session_id.is_some() {
                true
            } else {
                if lifecycle.pending.len() >= MAX_PENDING_LIFECYCLE_EVENTS {
                    lifecycle.pending.pop_front();
                }
                lifecycle
                    .pending
                    .push_back(PendingStreamOutput::Message(message.clone()));
                false
            }
        };
        if active {
            self.emit(&message)?;
        }
        Ok(())
    }

    fn emit_command_lifecycle_for(
        &self,
        session_id: &str,
        command_uuid: Uuid,
        state: &'static str,
    ) -> Result<()> {
        self.emit(&json!({
            "type":"command_lifecycle",
            "command_uuid":command_uuid,
            "state":state,
            "uuid":Uuid::new_v4(),
            "session_id":session_id,
        }))
    }

    pub fn emit(&self, message: &Value) -> Result<()> {
        let encoded = serde_json::to_vec(message)?;
        if encoded.len() > MAX_CONTROL_LINE_BYTES {
            bail!("stream-json 输出消息超过 {MAX_CONTROL_LINE_BYTES} 字节限制")
        }
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        writer.write_all(&encoded)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        Ok(())
    }

    pub fn respond_success(&self, request_id: &str, response: Value) -> Result<()> {
        self.emit(&json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": response,
            }
        }))
    }

    pub fn respond_error(&self, request_id: &str, error: impl Into<String>) -> Result<()> {
        self.emit(&json!({
            "type": "control_response",
            "response": {
                "subtype": "error",
                "request_id": request_id,
                "error": error.into(),
            }
        }))
    }

    pub fn request(&self, request: Value) -> Result<Value> {
        self.request_with_timeout(request, self.request_timeout)
    }

    fn request_with_timeout(&self, request: Value, request_timeout: Duration) -> Result<Value> {
        if !request.is_object() || request.get("subtype").and_then(Value::as_str).is_none() {
            bail!("control request 必须是带 subtype 的 object")
        }
        let generation = self.request_generation.load(Ordering::Acquire);
        let cancellation = self.cancel_tx.subscribe();
        if *cancellation.borrow() != generation {
            return Err(ControlInterrupted.into());
        }
        let request_id = Uuid::new_v4().to_string();
        let (sender, receiver) = std_mpsc::sync_channel(1);
        {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if pending.len() >= MAX_PENDING_REQUESTS {
                bail!("待处理 control request 超过 {MAX_PENDING_REQUESTS} 个限制")
            }
            pending.insert(request_id.clone(), sender);
        }
        if let Err(error) = self.emit(&json!({
            "type": "control_request",
            "request_id": request_id,
            "request": request,
        })) {
            self.remove_pending(&request_id);
            return Err(error);
        }
        let deadline = Instant::now() + request_timeout;
        let response = loop {
            if *cancellation.borrow() != generation {
                self.remove_pending(&request_id);
                return Err(ControlInterrupted.into());
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.remove_pending(&request_id);
                bail!("等待 control response 超时")
            }
            match receiver.recv_timeout(remaining.min(CONTROL_REQUEST_POLL_INTERVAL)) {
                Ok(response) => break response,
                Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                    self.remove_pending(&request_id);
                    bail!("等待 control response 失败: response channel disconnected")
                }
            }
        };
        match response.get("subtype").and_then(Value::as_str) {
            Some("success") => Ok(response
                .get("response")
                .cloned()
                .unwrap_or_else(|| json!({}))),
            Some("error") if response.get("interrupted").and_then(Value::as_bool) == Some(true) => {
                Err(ControlInterrupted.into())
            }
            Some("error") => bail!(
                "control request 被拒绝: {}",
                response
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ),
            _ => bail!("control response 缺少有效 subtype"),
        }
    }

    pub fn cancellation_since(
        &self,
        generation: u64,
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>> {
        let mut receiver = self.cancel_tx.subscribe();
        Box::pin(async move {
            if *receiver.borrow() != generation {
                return;
            }
            if receiver.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        })
    }

    pub fn acknowledge_cancellation(&self, generation: u64) {
        self.request_generation.store(generation, Ordering::Release);
    }

    pub fn current_cancellation_generation(&self) -> u64 {
        *self.cancel_tx.borrow()
    }

    pub fn permission_handler(&self) -> PermissionPromptHandler {
        let handle = self.clone();
        Arc::new(move |request| handle.request_permission(request))
    }

    pub fn ask_user(&self, input: &Value) -> Result<Value> {
        let response = self.request(json!({
            "subtype": "can_use_tool",
            "tool_name": "AskUserQuestion",
            "input": input,
            "tool_use_id": Uuid::new_v4(),
            "description": "Answer questions?",
        }))?;
        match decision_text(&response) {
            Some("deny" | "denied" | "reject" | "rejected" | "cancel") => {
                bail!("用户拒绝回答问题")
            }
            Some("interrupt") => return Err(ControlInterrupted.into()),
            _ => {}
        }
        Ok(response
            .get("updatedInput")
            .or_else(|| response.get("updated_input"))
            .cloned()
            .unwrap_or(response))
    }

    pub fn approve_plan(&self, input: &Value) -> Result<Value> {
        let response = self.request(json!({
            "subtype": "can_use_tool",
            "tool_name": "ExitPlanMode",
            "input": input,
            "tool_use_id": Uuid::new_v4(),
            "description": "Approve the saved implementation plan and leave plan mode?",
        }))?;
        match decision_text(&response) {
            Some("interrupt" | "cancel" | "cancelled") => {
                return Err(ControlInterrupted.into());
            }
            Some("deny" | "denied" | "reject" | "rejected") => {
                return Ok(json!({"approved":false}));
            }
            Some("allow" | "allowed" | "accept" | "accepted" | "approve" | "approved") => {}
            _ => bail!("plan approval control response 缺少显式 approve/reject decision"),
        }
        let plan = response
            .get("updatedInput")
            .or_else(|| response.get("updated_input"))
            .and_then(|updated| updated.get("plan"))
            .or_else(|| input.get("plan"))
            .cloned()
            .context("plan approval 缺少 plan content")?;
        Ok(json!({"approved":true, "plan":plan}))
    }

    pub fn mcp_elicitation(&self, input: &Value) -> Result<Value> {
        if input.get("subtype").and_then(Value::as_str) != Some("elicitation")
            || input
                .get("mcp_server_name")
                .and_then(Value::as_str)
                .is_none()
            || input.get("message").and_then(Value::as_str).is_none()
        {
            bail!("MCP elicitation control request shape 无效")
        }
        let request_timeout = input
            .get("interaction_timeout_ms")
            .and_then(Value::as_u64)
            .filter(|timeout| (1_000..=120_000).contains(timeout))
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(90));
        let mut request = input.clone();
        request
            .as_object_mut()
            .expect("shape checked above")
            .remove("interaction_timeout_ms");
        let response = self.request_with_timeout(request, request_timeout)?;
        let action = response
            .get("action")
            .and_then(Value::as_str)
            .context("MCP elicitation control response 缺少 action")?;
        if !matches!(action, "accept" | "decline" | "cancel") {
            bail!("MCP elicitation control action 无效")
        }
        let mut result = json!({"action":action});
        if let Some(content) = response.get("content") {
            result["content"] = content.clone();
        }
        Ok(result)
    }

    fn request_permission(&self, request: &PermissionRequest) -> Result<PermissionDecision> {
        let response = match self.request(json!({
            "subtype": "can_use_tool",
            "tool_name": request.tool,
            "input": request.input,
            "tool_use_id": request.tool_use_id,
            "description": request.summary,
            "blocked_path": request.outside_workspace.then_some("outside_workspace"),
        })) {
            Ok(response) => response,
            Err(error) if error.downcast_ref::<ControlInterrupted>().is_some() => {
                return Ok(PermissionDecision::Interrupt);
            }
            Err(error) => return Err(error),
        };
        match decision_text(&response) {
            Some("allow" | "allowed" | "accept" | "accepted") => {
                match response
                    .get("updatedInput")
                    .or_else(|| response.get("updated_input"))
                {
                    Some(updated) => Ok(PermissionDecision::AllowWithUpdatedInput(updated.clone())),
                    None => Ok(PermissionDecision::Allow),
                }
            }
            Some("interrupt" | "cancel" | "cancelled") => Ok(PermissionDecision::Interrupt),
            Some("deny" | "denied" | "reject" | "rejected") => Ok(PermissionDecision::Deny),
            _ => bail!("permission control response 缺少 allow/deny/interrupt decision"),
        }
    }

    fn remove_pending(&self, request_id: &str) {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(request_id);
    }
}

fn decision_text(response: &Value) -> Option<&str> {
    response
        .get("behavior")
        .or_else(|| response.get("decision"))
        .or_else(|| response.get("action"))
        .and_then(Value::as_str)
}

fn read_input_loop<R: BufRead>(mut reader: R, inbound: InboundSenders, state: InputLoopState) {
    let InputLoopState {
        pending,
        cancel_tx,
        request_generation,
        queued_users,
        cancelled_users,
        writer,
        replay_user_messages,
        handle,
    } = state;
    loop {
        let line = match read_bounded_line(&mut reader, MAX_CONTROL_LINE_BYTES) {
            Ok(Some(line)) => line,
            Ok(None) => {
                cancel_all_pending(&pending, "stream-json input ended");
                let _ = enqueue_inbound(&inbound, InboundMessage::EndOfInput, &writer);
                return;
            }
            Err(error) => {
                if !enqueue_inbound(
                    &inbound,
                    InboundMessage::ProtocolError {
                        message: format!("读取 stream-json 输入失败: {error:#}"),
                    },
                    &writer,
                ) {
                    return;
                }
                continue;
            }
        };
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value: Value = match serde_json::from_slice(&line) {
            Ok(value) => value,
            Err(error) => {
                if !enqueue_inbound(
                    &inbound,
                    InboundMessage::ProtocolError {
                        message: format!("无效 stream-json 消息: {error}"),
                    },
                    &writer,
                ) {
                    return;
                }
                continue;
            }
        };
        match parse_inbound(value) {
            Ok(ParsedInbound::Message(message)) => {
                if let InboundMessage::ControlRequest {
                    request_id,
                    request,
                } = &message
                {
                    if request.get("subtype").and_then(Value::as_str) == Some("interrupt") {
                        advance_cancellation(&cancel_tx);
                        cancel_all_pending(&pending, "interrupted by SDK consumer");
                        let still_queued = queued_users
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .iter()
                            .map(Uuid::to_string)
                            .collect::<Vec<_>>();
                        let response = json!({
                            "type":"control_response",
                            "response":{
                                "subtype":"success",
                                "request_id":request_id,
                                "response":{"interrupted":true, "still_queued":still_queued}
                            }
                        });
                        if write_direct(&writer, &response).is_err() {
                            return;
                        }
                        continue;
                    }
                    if request.get("subtype").and_then(Value::as_str)
                        == Some("cancel_async_message")
                    {
                        let Some(message_uuid) = request
                            .get("message_uuid")
                            .and_then(Value::as_str)
                            .and_then(|value| value.parse::<Uuid>().ok())
                        else {
                            let response = json!({
                                "type":"control_response",
                                "response":{
                                    "subtype":"error",
                                    "request_id":request_id,
                                    "error":"cancel_async_message.message_uuid 必须是 UUID"
                                }
                            });
                            if write_direct(&writer, &response).is_err() {
                                return;
                            }
                            continue;
                        };
                        let cancelled = queued_users
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(&message_uuid);
                        if cancelled {
                            cancelled_users
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .insert(message_uuid);
                            if handle.command_lifecycle(message_uuid, "cancelled").is_err() {
                                return;
                            }
                        }
                        let response = json!({
                            "type":"control_response",
                            "response":{
                                "subtype":"success",
                                "request_id":request_id,
                                "response":{"cancelled":cancelled}
                            }
                        });
                        if write_direct(&writer, &response).is_err() {
                            return;
                        }
                        continue;
                    }
                }
                let permit = match inbound.try_reserve(&message) {
                    Ok(permit) => permit,
                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        if !handle_client_overflow(&message, &handle, &writer) {
                            return;
                        }
                        continue;
                    }
                };
                if let InboundMessage::User {
                    uuid,
                    content,
                    priority,
                } = &message
                {
                    let inserted = queued_users
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .insert(*uuid);
                    if !inserted {
                        drop(permit);
                        if handle.command_lifecycle(*uuid, "discarded").is_err()
                            || write_direct(
                                &writer,
                                &json!({
                                    "type":"system",
                                    "subtype":"protocol_error",
                                    "error":"duplicate queued user UUID; message discarded",
                                    "uuid":uuid,
                                }),
                            )
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
                    if handle.command_lifecycle(*uuid, "queued").is_err()
                        || (replay_user_messages
                            && handle
                                .replay_user_message(json!({
                                    "type":"user",
                                    "uuid":uuid,
                                    "message":{"role":"user", "content":content.clone()},
                                    "priority":priority_name(*priority),
                                    "replayed":true,
                                }))
                                .is_err())
                    {
                        queued_users
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(uuid);
                        return;
                    }
                    if *priority == QueuePriority::Now
                        && *cancel_tx.borrow() == request_generation.load(Ordering::Acquire)
                    {
                        advance_cancellation(&cancel_tx);
                        cancel_all_pending(&pending, "interrupted by priority=now user message");
                    }
                }
                permit.send(message);
            }
            Ok(ParsedInbound::Response {
                request_id,
                response,
            }) => {
                let sender = pending
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&request_id);
                if let Some(sender) = sender {
                    let _ = sender.send(response);
                } else if !enqueue_inbound(
                    &inbound,
                    InboundMessage::ProtocolError {
                        message: format!("收到未知 request_id 的 control response: {request_id}"),
                    },
                    &writer,
                ) {
                    return;
                }
            }
            Ok(ParsedInbound::Ignore) => {}
            Err(error) => {
                if !enqueue_inbound(
                    &inbound,
                    InboundMessage::ProtocolError {
                        message: format!("无效 stream-json 消息: {error:#}"),
                    },
                    &writer,
                ) {
                    return;
                }
            }
        }
    }
}

fn advance_cancellation(cancel_tx: &watch::Sender<u64>) {
    let next = (*cancel_tx.borrow()).wrapping_add(1);
    cancel_tx.send_replace(next);
}

fn priority_name(priority: QueuePriority) -> &'static str {
    match priority {
        QueuePriority::Now => "now",
        QueuePriority::Next => "next",
        QueuePriority::Later => "later",
    }
}

fn cancel_all_pending(pending: &Arc<Mutex<HashMap<String, PendingSender>>>, reason: &str) {
    let senders = pending
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .drain()
        .collect::<Vec<_>>();
    for (request_id, sender) in senders {
        let _ = sender.send(json!({
            "subtype":"error",
            "request_id":request_id,
            "error":reason,
            "interrupted":true,
        }));
    }
}

fn write_direct(writer: &Arc<Mutex<Box<dyn Write + Send>>>, message: &Value) -> Result<()> {
    let encoded = serde_json::to_vec(message)?;
    if encoded.len() > MAX_CONTROL_LINE_BYTES {
        bail!("stream-json 输出消息超过 {MAX_CONTROL_LINE_BYTES} 字节限制")
    }
    let mut writer = writer
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    writer.write_all(&encoded)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn enqueue_inbound(
    inbound: &InboundSenders,
    message: InboundMessage,
    writer: &Arc<Mutex<Box<dyn Write + Send>>>,
) -> bool {
    match inbound.try_send(message) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Closed(_)) => false,
        Err(mpsc::error::TrySendError::Full(_)) => {
            let overflow = json!({
                "type":"system",
                "subtype":"protocol_error",
                "error":"stream-json inbound queue is full; message rejected"
            });
            if let Ok(encoded) = serde_json::to_vec(&overflow) {
                let mut writer = writer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let _ = writer.write_all(&encoded);
                let _ = writer.write_all(b"\n");
                let _ = writer.flush();
            }
            true
        }
    }
}

fn handle_client_overflow(
    message: &InboundMessage,
    handle: &ControlHandle,
    writer: &Arc<Mutex<Box<dyn Write + Send>>>,
) -> bool {
    match message {
        InboundMessage::User { uuid, .. } => {
            if handle.command_lifecycle(*uuid, "discarded").is_err() {
                return false;
            }
            write_direct(
                writer,
                &json!({
                    "type":"system",
                    "subtype":"protocol_error",
                    "error":"stream-json priority queue is full; user message discarded",
                    "uuid":uuid,
                }),
            )
            .is_ok()
        }
        InboundMessage::ControlRequest { request_id, .. } => write_direct(
            writer,
            &json!({
                "type":"control_response",
                "response":{
                    "subtype":"error",
                    "request_id":request_id,
                    "error":"stream-json control queue is full; request rejected"
                }
            }),
        )
        .is_ok(),
        InboundMessage::UpdateEnvironmentVariables { .. }
        | InboundMessage::ProtocolError { .. }
        | InboundMessage::EndOfInput => write_direct(
            writer,
            &json!({
                "type":"system",
                "subtype":"protocol_error",
                "error":"stream-json inbound queue is full; message rejected"
            }),
        )
        .is_ok(),
    }
}

enum ParsedInbound {
    Message(InboundMessage),
    Response { request_id: String, response: Value },
    Ignore,
}

fn parse_inbound(value: Value) -> Result<ParsedInbound> {
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .context("缺少 type")?;
    match kind {
        "user" => {
            let uuid = match value.get("uuid") {
                Some(uuid) => uuid
                    .as_str()
                    .context("user.uuid 必须是 UUID string")?
                    .parse::<Uuid>()
                    .context("user.uuid 必须是 UUID")?,
                None => Uuid::new_v4(),
            };
            let message = value.get("message").context("user 消息缺少 message")?;
            if message.get("role").and_then(Value::as_str) != Some("user") {
                bail!("user.message.role 必须是 user")
            }
            let content = message
                .get("content")
                .cloned()
                .context("user.message 缺少 content")?;
            validate_direct_user_content(&content).context("user.message.content 无效")?;
            let priority = QueuePriority::parse(value.get("priority"))?;
            Ok(ParsedInbound::Message(InboundMessage::User {
                uuid,
                content,
                priority,
            }))
        }
        "control_request" => {
            let request_id = bounded_string_alias(&value, "request_id", "requestId", 256)?;
            let request = value
                .get("request")
                .filter(|request| request.is_object())
                .cloned()
                .context("control_request.request 必须是 object")?;
            if request.get("subtype").and_then(Value::as_str).is_none() {
                bail!("control_request.request 缺少 subtype")
            }
            Ok(ParsedInbound::Message(InboundMessage::ControlRequest {
                request_id,
                request,
            }))
        }
        "control_response" => {
            let response = value
                .get("response")
                .filter(|response| response.is_object())
                .cloned()
                .context("control_response.response 必须是 object")?;
            let request_id = bounded_string_alias(&response, "request_id", "requestId", 256)?;
            Ok(ParsedInbound::Response {
                request_id,
                response,
            })
        }
        "control_cancel_request" => {
            let request_id = bounded_string_alias(&value, "request_id", "requestId", 256)?;
            Ok(ParsedInbound::Response {
                request_id: request_id.clone(),
                response: json!({
                    "subtype": "error",
                    "request_id": request_id,
                    "error": "request cancelled by SDK consumer",
                    "interrupted": true,
                }),
            })
        }
        "update_environment_variables" => {
            let variables = value
                .get("variables")
                .cloned()
                .context("update_environment_variables 缺少 variables")?;
            let variables: HashMap<String, String> =
                serde_json::from_value(variables).context("variables 必须是 string map")?;
            if variables.len() > 128
                || variables
                    .iter()
                    .any(|(key, value)| key.len() > 256 || value.len() > 64 * 1024)
            {
                bail!("environment variable 更新超过资源限制")
            }
            Ok(ParsedInbound::Message(
                InboundMessage::UpdateEnvironmentVariables { variables },
            ))
        }
        "keep_alive" => Ok(ParsedInbound::Ignore),
        other => bail!("不支持的 stream-json type: {other}"),
    }
}

fn bounded_string_alias(
    value: &Value,
    field: &str,
    alias: &str,
    max_bytes: usize,
) -> Result<String> {
    let text = value
        .get(field)
        .or_else(|| value.get(alias))
        .and_then(Value::as_str)
        .with_context(|| format!("缺少 {field}/{alias}"))?;
    if text.is_empty() || text.len() > max_bytes {
        bail!("{field}/{alias} 长度必须为 1..={max_bytes} 字节")
    }
    Ok(text.to_owned())
}

fn read_bounded_line<R: BufRead>(reader: &mut R, limit: usize) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let end = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(end) > limit.saturating_add(1) {
            let found_newline = available.get(end.saturating_sub(1)) == Some(&b'\n');
            reader.consume(end);
            if !found_newline {
                drain_to_newline(reader)?;
            }
            bail!("单行超过 {limit} 字节限制")
        }
        line.extend_from_slice(&available[..end]);
        let found_newline = available.get(end.saturating_sub(1)) == Some(&b'\n');
        reader.consume(end);
        if found_newline {
            while line
                .last()
                .is_some_and(|byte| matches!(byte, b'\n' | b'\r'))
            {
                line.pop();
            }
            return Ok(Some(line));
        }
    }
}

fn drain_to_newline<R: BufRead>(reader: &mut R) -> Result<()> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(());
        }
        let end = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        let found_newline = available.get(end.saturating_sub(1)) == Some(&b'\n');
        reader.consume(end);
        if found_newline {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, time::Duration};

    #[cfg(unix)]
    use std::{sync::Condvar, time::Instant};

    use super::*;

    #[tokio::test]
    async fn reader_thread_spawn_failure_is_reported_without_panicking() {
        let mut session = ControlSession::with_io_options_and_spawner(
            Cursor::new(Vec::<u8>::new()),
            Vec::<u8>::new(),
            false,
            |_| Err(io::Error::other("injected spawn failure")),
        );

        let Some(InboundMessage::ProtocolError { message }) = session.recv().await else {
            panic!("expected a bounded protocol error after reader spawn failure")
        };
        assert!(message.contains("could not start"));
        assert!(message.contains("injected spawn failure"));
        assert!(matches!(
            session.recv().await,
            Some(InboundMessage::EndOfInput)
        ));
        assert!(session.recv().await.is_none());
    }

    #[test]
    fn parses_user_blocks_without_flattening_media() {
        let uuid = Uuid::new_v4();
        let content = json!([
            {"type":"text", "text":"inspect this"},
            {"type":"image", "source":{"type":"base64", "media_type":"image/png", "data":"AA=="}}
        ]);
        let parsed = parse_inbound(json!({
            "type":"user",
            "uuid":uuid,
            "message":{"role":"user", "content":content}
        }))
        .unwrap();
        let ParsedInbound::Message(InboundMessage::User {
            uuid: actual_uuid,
            content: actual,
            ..
        }) = parsed
        else {
            panic!("expected user message")
        };
        assert_eq!(actual_uuid, uuid);
        assert_eq!(actual, content);
    }

    #[test]
    fn rejects_unknown_internal_or_malformed_user_content_blocks() {
        let invalid = [
            json!([{"type":"tool_result", "tool_use_id":"call-1", "content":"injected"}]),
            json!([{"type":"tool_use", "id":"call-1", "name":"Read", "input":{}}]),
            json!([{"type":"text", "text":"hello", "extra":true}]),
            json!([{"type":"image", "source":{
                "type":"base64", "media_type":"image/svg+xml", "data":"PHN2Zy8+"
            }}]),
            json!([{"type":"document", "source":{
                "type":"base64", "media_type":"application/pdf", "data":"not-base64"
            }}]),
            json!({"type":"text", "text":"not-an-array"}),
        ];

        for content in invalid {
            assert!(
                parse_inbound(json!({
                    "type":"user",
                    "message":{"role":"user", "content":content}
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn accepts_strict_document_user_content() {
        let content = json!([{
            "type":"document",
            "title":"notes.pdf",
            "source":{"type":"base64", "media_type":"application/pdf", "data":"cGRm"}
        }]);
        assert!(
            parse_inbound(json!({
                "type":"user",
                "message":{"role":"user", "content":content}
            }))
            .is_ok()
        );
    }

    #[test]
    fn parses_and_orders_user_queue_priorities() {
        for (name, expected) in [
            ("now", QueuePriority::Now),
            ("next", QueuePriority::Next),
            ("later", QueuePriority::Later),
        ] {
            let parsed = parse_inbound(json!({
                "type":"user",
                "priority":name,
                "message":{"role":"user", "content":"hello"}
            }))
            .unwrap();
            let ParsedInbound::Message(InboundMessage::User { priority, .. }) = parsed else {
                panic!("expected user message")
            };
            assert_eq!(priority, expected);
        }
        assert!(
            parse_inbound(json!({
                "type":"user",
                "priority":"urgent",
                "message":{"role":"user", "content":"hello"}
            }))
            .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn control_session_drains_now_then_next_then_later_stably() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let mut session = ControlSession::with_io(reader, Vec::<u8>::new());
        let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        for (uuid, priority) in [(ids[0], "later"), (ids[1], "next"), (ids[2], "now")] {
            writeln!(
                input,
                "{}",
                json!({
                    "type":"user", "uuid":uuid, "priority":priority,
                    "message":{"role":"user", "content":priority}
                })
            )
            .unwrap();
        }
        input.flush().unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;

        for expected in [ids[2], ids[1], ids[0]] {
            let message = session.recv().await.unwrap();
            assert!(matches!(message, InboundMessage::User { uuid, .. } if uuid == expected));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn queued_messages_start_on_the_latest_cancellation_generation() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let mut session = ControlSession::with_io(reader, Vec::<u8>::new());
        let handle = session.handle();
        let retained = Uuid::new_v4();
        let immediate = Uuid::new_v4();
        writeln!(
            input,
            "{}",
            json!({
                "type":"user", "uuid":retained, "priority":"next",
                "message":{"role":"user", "content":"retained"}
            })
        )
        .unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"user", "uuid":immediate, "priority":"now",
                "message":{"role":"user", "content":"immediate"}
            })
        )
        .unwrap();
        input.flush().unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;

        assert!(matches!(
            session.recv().await.unwrap(),
            InboundMessage::User { uuid, .. } if uuid == immediate
        ));
        let generation = handle.current_cancellation_generation();
        handle.acknowledge_cancellation(generation);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                handle.cancellation_since(generation)
            )
            .await
            .is_err()
        );

        assert!(matches!(
            session.recv().await.unwrap(),
            InboundMessage::User { uuid, .. } if uuid == retained
        ));
        let generation = handle.current_cancellation_generation();
        handle.acknowledge_cancellation(generation);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                handle.cancellation_since(generation)
            )
            .await
            .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn end_of_input_drains_queued_users_without_cancelling_them() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let mut session = ControlSession::with_io(reader, Vec::<u8>::new());
        let handle = session.handle();
        let uuid = Uuid::new_v4();
        writeln!(
            input,
            "{}",
            json!({
                "type":"user", "uuid":uuid,
                "message":{"role":"user", "content":"finish before EOF"}
            })
        )
        .unwrap();
        drop(input);

        assert!(matches!(
            session.recv().await.unwrap(),
            InboundMessage::User { uuid: actual, .. } if actual == uuid
        ));
        assert_eq!(handle.current_cancellation_generation(), 0);
        assert!(matches!(
            session.recv().await,
            Some(InboundMessage::EndOfInput)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn priority_lanes_are_bounded_and_discard_overflow_exactly_once() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let mut session = ControlSession::with_io(reader, output.clone());
        session
            .handle()
            .activate_command_lifecycle("session-1")
            .unwrap();
        let ids = (0..=LATER_INBOUND_CAPACITY)
            .map(|_| Uuid::new_v4())
            .collect::<Vec<_>>();
        for uuid in &ids {
            writeln!(
                input,
                "{}",
                json!({
                    "type":"user", "uuid":uuid, "priority":"later",
                    "message":{"role":"user", "content":"bounded"}
                })
            )
            .unwrap();
        }
        input.flush().unwrap();

        let mut states = Vec::new();
        let mut overflow_uuid = None;
        while overflow_uuid.is_none() {
            let event: Value = serde_json::from_slice(&output.wait_line()).unwrap();
            if event["type"] == "command_lifecycle" {
                states.push((
                    event["command_uuid"].as_str().unwrap().to_owned(),
                    event["state"].as_str().unwrap().to_owned(),
                ));
            } else if event["subtype"] == "protocol_error" {
                overflow_uuid = event["uuid"].as_str().map(str::to_owned);
            }
        }
        let overflow_uuid = overflow_uuid.unwrap();
        assert_eq!(overflow_uuid, ids[LATER_INBOUND_CAPACITY].to_string());
        assert!(states.contains(&(overflow_uuid, "discarded".to_owned())));

        for expected in &ids[..LATER_INBOUND_CAPACITY] {
            assert!(matches!(
                session.recv().await.unwrap(),
                InboundMessage::User { uuid, .. } if uuid == *expected
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn full_control_lane_returns_a_correlated_error() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let _session = ControlSession::with_io(reader, output.clone());
        for index in 0..=CONTROL_INBOUND_CAPACITY {
            writeln!(
                input,
                "{}",
                json!({
                    "type":"control_request",
                    "request_id":format!("request-{index}"),
                    "request":{"subtype":"custom"}
                })
            )
            .unwrap();
        }
        input.flush().unwrap();

        let response: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(response["type"], "control_response");
        assert_eq!(
            response["response"]["request_id"],
            format!("request-{CONTROL_INBOUND_CAPACITY}")
        );
        assert_eq!(response["response"]["subtype"], "error");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_async_message_drops_only_a_still_queued_uuid() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let mut session = ControlSession::with_io(reader, output.clone());
        let cancelled = Uuid::new_v4();
        let retained = Uuid::new_v4();
        writeln!(
            input,
            "{}",
            json!({
                "type":"user", "uuid":cancelled,
                "message":{"role":"user", "content":"drop me"}
            })
        )
        .unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"control_request", "request_id":"cancel-1",
                "request":{"subtype":"cancel_async_message", "message_uuid":cancelled}
            })
        )
        .unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"user", "uuid":retained,
                "message":{"role":"user", "content":"keep me"}
            })
        )
        .unwrap();
        let response: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(response["response"]["response"]["cancelled"], true);
        let message = session.recv().await.unwrap();
        assert!(matches!(message, InboundMessage::User { uuid, .. } if uuid == retained));
    }

    #[cfg(unix)]
    #[test]
    fn replay_user_messages_acknowledges_the_original_uuid_and_content() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let session = ControlSession::with_io_options(reader, output.clone(), true);
        session
            .handle()
            .activate_command_lifecycle("session-1")
            .unwrap();
        let uuid = Uuid::new_v4();
        writeln!(
            input,
            "{}",
            json!({
                "type":"user", "uuid":uuid, "priority":"later",
                "message":{"role":"user", "content":"ack me"}
            })
        )
        .unwrap();
        let queued: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(queued["type"], "command_lifecycle");
        assert_eq!(queued["state"], "queued");
        let replayed: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(replayed["type"], "user");
        assert_eq!(replayed["uuid"], uuid.to_string());
        assert_eq!(replayed["message"]["content"], "ack me");
        assert_eq!(replayed["priority"], "later");
        assert_eq!(replayed["replayed"], true);
    }

    #[cfg(unix)]
    #[test]
    fn command_lifecycle_buffers_until_stream_init_activation() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let session = ControlSession::with_io(reader, output.clone());
        let uuid = Uuid::new_v4();
        writeln!(
            input,
            "{}",
            json!({
                "type":"user", "uuid":uuid,
                "message":{"role":"user", "content":"queued"}
            })
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(25));
        session
            .handle()
            .activate_command_lifecycle("session-1")
            .unwrap();
        let event: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(event["type"], "command_lifecycle");
        assert_eq!(event["command_uuid"], uuid.to_string());
        assert_eq!(event["state"], "queued");
        assert_eq!(event["session_id"], "session-1");
    }

    #[test]
    fn parses_nested_control_response_wrapper() {
        let parsed = parse_inbound(json!({
            "type":"control_response",
            "response":{"subtype":"success", "request_id":"r1", "response":{"behavior":"allow"}}
        }))
        .unwrap();
        let ParsedInbound::Response {
            request_id,
            response,
        } = parsed
        else {
            panic!("expected response")
        };
        assert_eq!(request_id, "r1");
        assert_eq!(response["response"]["behavior"], "allow");
    }

    #[test]
    fn accepts_camel_case_request_ids_at_the_sdk_boundary() {
        let request = parse_inbound(json!({
            "type":"control_request",
            "requestId":"camel-1",
            "request":{"subtype":"get_context_usage"}
        }))
        .unwrap();
        assert!(matches!(
            request,
            ParsedInbound::Message(InboundMessage::ControlRequest { request_id, .. })
                if request_id == "camel-1"
        ));

        let response = parse_inbound(json!({
            "type":"control_response",
            "response":{"subtype":"success", "requestId":"camel-2", "response":{}}
        }))
        .unwrap();
        assert!(matches!(
            response,
            ParsedInbound::Response { request_id, .. } if request_id == "camel-2"
        ));
    }

    #[test]
    fn bounded_reader_drains_an_oversized_line_and_recovers() {
        let input = format!("{}\n{{}}\n", "x".repeat(20));
        let mut reader = Cursor::new(input.into_bytes());
        assert!(read_bounded_line(&mut reader, 8).is_err());
        assert_eq!(
            read_bounded_line(&mut reader, 8).unwrap(),
            Some(b"{}".to_vec())
        );
    }

    #[test]
    fn rejects_unknown_message_types() {
        assert!(parse_inbound(json!({"type":"mystery"})).is_err());
    }

    #[tokio::test]
    async fn cancellation_observes_an_interrupt_queued_after_the_user_message() {
        let session = ControlSession::with_io(Cursor::new(Vec::<u8>::new()), Vec::<u8>::new());
        let handle = session.handle();
        let generation = *handle.cancel_tx.borrow();
        handle.cancel_tx.send_replace(generation.wrapping_add(1));
        tokio::time::timeout(
            Duration::from_millis(100),
            handle.cancellation_since(generation),
        )
        .await
        .expect("already queued interrupt must cancel immediately");
    }

    #[cfg(unix)]
    #[test]
    fn outbound_request_is_resolved_by_a_bidirectional_control_response() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let session = ControlSession::with_io(reader, output.clone());
        let handle = session.handle();
        let waiter = std::thread::spawn(move || {
            handle.request(json!({"subtype":"can_use_tool", "tool_name":"Write"}))
        });
        let request: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        let request_id = request["request_id"].as_str().unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"control_response",
                "response":{
                    "subtype":"success",
                    "request_id":request_id,
                    "response":{"behavior":"allow"}
                }
            })
        )
        .unwrap();
        let response = waiter.join().unwrap().unwrap();
        assert_eq!(response["behavior"], "allow");
    }

    #[cfg(unix)]
    #[test]
    fn plan_approval_requires_explicit_decision_and_returns_edited_plan() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let session = ControlSession::with_io(reader, output.clone());
        let handle = session.handle();
        let waiter = std::thread::spawn(move || {
            handle.approve_plan(&json!({"plan":"original", "saved":true}))
        });
        let request: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(request["request"]["tool_name"], "ExitPlanMode");
        assert_eq!(request["request"]["input"]["plan"], "original");
        let request_id = request["request_id"].as_str().unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"control_response",
                "response":{
                    "subtype":"success", "request_id":request_id,
                    "response":{
                        "behavior":"allow",
                        "updatedInput":{"plan":"edited"}
                    }
                }
            })
        )
        .unwrap();
        assert_eq!(
            waiter.join().unwrap().unwrap(),
            json!({"approved":true, "plan":"edited"})
        );
    }

    #[cfg(unix)]
    #[test]
    fn mcp_elicitation_uses_a_dedicated_bounded_control_request() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let session = ControlSession::with_io(reader, output.clone());
        let handle = session.handle();
        let waiter = std::thread::spawn(move || {
            handle.mcp_elicitation(&json!({
                "subtype":"elicitation",
                "mcp_server_name":"calendar",
                "message":"Choose a calendar",
                "mode":"form",
                "requested_schema":{"type":"object"},
                "interaction_timeout_ms":1_000
            }))
        });
        let request: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(request["request"]["subtype"], "elicitation");
        assert_eq!(request["request"]["mcp_server_name"], "calendar");
        assert!(request["request"].get("interaction_timeout_ms").is_none());
        let request_id = request["request_id"].as_str().unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"control_response",
                "response":{
                    "subtype":"success", "request_id":request_id,
                    "response":{"action":"accept", "content":{"calendar":"work"}}
                }
            })
        )
        .unwrap();
        assert_eq!(
            waiter.join().unwrap().unwrap(),
            json!({"action":"accept", "content":{"calendar":"work"}})
        );
    }

    #[cfg(unix)]
    #[test]
    fn interrupt_immediately_wakes_a_pending_control_request() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let session = ControlSession::with_io(reader, output.clone());
        let handle = session.handle();
        handle.acknowledge_cancellation(0);
        let waiter = std::thread::spawn(move || {
            handle.request(json!({"subtype":"can_use_tool", "tool_name":"Write"}))
        });
        let _: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"control_request", "request_id":"interrupt-1",
                "request":{"subtype":"interrupt"}
            })
        )
        .unwrap();
        let response: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(response["response"]["request_id"], "interrupt-1");
        let error = waiter.join().unwrap().unwrap_err();
        assert!(error.downcast_ref::<ControlInterrupted>().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn permission_request_preserves_invocation_and_applies_updated_input() {
        use std::os::unix::net::UnixStream;

        let (mut input, reader) = UnixStream::pair().unwrap();
        let output = SharedWriter::default();
        let session = ControlSession::with_io(reader, output.clone());
        let handle = session.handle();
        let waiter = std::thread::spawn(move || {
            handle.request_permission(&PermissionRequest {
                tool: "Write".to_owned(),
                input: json!({"file_path":"original.txt", "content":"one"}),
                tool_use_id: "tool-call-7".to_owned(),
                summary: "original.txt".to_owned(),
                read_only: false,
                destructive: true,
                outside_workspace: false,
            })
        });
        let request: Value = serde_json::from_slice(&output.wait_line()).unwrap();
        assert_eq!(request["request"]["input"]["file_path"], "original.txt");
        assert_eq!(request["request"]["tool_use_id"], "tool-call-7");
        let request_id = request["request_id"].as_str().unwrap();
        writeln!(
            input,
            "{}",
            json!({
                "type":"control_response",
                "response":{
                    "subtype":"success", "request_id":request_id,
                    "response":{
                        "behavior":"allow",
                        "updatedInput":{"file_path":"updated.txt", "content":"two"}
                    }
                }
            })
        )
        .unwrap();
        assert_eq!(
            waiter.join().unwrap().unwrap(),
            PermissionDecision::AllowWithUpdatedInput(
                json!({"file_path":"updated.txt", "content":"two"})
            )
        );
    }

    #[cfg(unix)]
    #[derive(Clone, Default)]
    struct SharedWriter(Arc<(Mutex<Vec<u8>>, Condvar)>);

    #[cfg(unix)]
    impl SharedWriter {
        fn wait_line(&self) -> Vec<u8> {
            let deadline = Instant::now() + Duration::from_secs(2);
            let (lock, ready) = &*self.0;
            let mut bytes = lock.lock().unwrap();
            loop {
                if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
                    let mut line = bytes.drain(..=newline).collect::<Vec<_>>();
                    line.pop();
                    return line;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "timed out waiting for control output");
                bytes = ready.wait_timeout(bytes, remaining).unwrap().0;
            }
        }
    }

    #[cfg(unix)]
    impl Write for SharedWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let (bytes, ready) = &*self.0;
            bytes.lock().unwrap().extend_from_slice(buffer);
            ready.notify_all();
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}

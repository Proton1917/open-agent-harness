use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, broadcast, oneshot},
    task::JoinHandle,
    time::timeout,
};

use crate::process::terminate_process_tree;

const MAX_RPC_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RPC_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_RPC_HEADER_BYTES: usize = 16 * 1024;
const MAX_RPC_STDERR_BYTES: usize = 64 * 1024;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<std::result::Result<Value, String>>>>>;
type SharedWriter = Arc<Mutex<Option<ChildStdin>>>;

struct ReaderLoopState {
    framing: RpcFraming,
    writer: SharedWriter,
    pending: Pending,
    events: broadcast::Sender<Value>,
    closed: Arc<AtomicBool>,
    label: String,
    server_request_handler: Option<RpcServerRequestHandler>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcFraming {
    Newline,
    ContentLength,
}

pub type RpcServerRequestHandler = Arc<dyn Fn(&str, Option<&Value>) -> Option<Value> + Send + Sync>;

#[derive(Clone)]
pub struct StdioRpcConfig {
    pub label: String,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    pub framing: RpcFraming,
    pub request_timeout: Duration,
    pub server_request_handler: Option<RpcServerRequestHandler>,
}

pub struct StdioRpcClient {
    label: String,
    framing: RpcFraming,
    request_timeout: Duration,
    writer: SharedWriter,
    child: Mutex<Child>,
    pending: Pending,
    events: broadcast::Sender<Value>,
    stderr: Arc<Mutex<VecDeque<u8>>>,
    reader_task: Mutex<Option<JoinHandle<()>>>,
    stderr_task: Mutex<Option<JoinHandle<()>>>,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
    shutdown_started: AtomicBool,
    process_group_id: Option<u32>,
}

impl StdioRpcClient {
    pub async fn spawn(config: StdioRpcConfig) -> Result<Self> {
        if config.command.trim().is_empty() {
            bail!("{} RPC command 不能为空", config.label)
        }
        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .envs(&config.env)
            .env_remove("HARNESS_API_KEY")
            .env_remove("HARNESS_AUTH_TOKEN")
            .current_dir(&config.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);

        let mut child = command
            .spawn()
            .with_context(|| format!("无法启动 {} RPC process", config.label))?;
        let process_group_id = child.id();
        let stdin = child.stdin.take().context("无法打开 RPC stdin")?;
        let stdout = child.stdout.take().context("无法打开 RPC stdout")?;
        let stderr_pipe = child.stderr.take().context("无法打开 RPC stderr")?;
        let writer = Arc::new(Mutex::new(Some(stdin)));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let stderr = Arc::new(Mutex::new(VecDeque::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let (events, _) = broadcast::channel(128);

        let reader_task = tokio::spawn(reader_loop(
            BufReader::new(stdout),
            ReaderLoopState {
                framing: config.framing,
                writer: Arc::clone(&writer),
                pending: Arc::clone(&pending),
                events: events.clone(),
                closed: Arc::clone(&closed),
                label: config.label.clone(),
                server_request_handler: config.server_request_handler,
            },
        ));
        let stderr_task = tokio::spawn(drain_stderr(stderr_pipe, Arc::clone(&stderr)));

        Ok(Self {
            label: config.label,
            framing: config.framing,
            request_timeout: config.request_timeout,
            writer,
            child: Mutex::new(child),
            pending,
            events,
            stderr,
            reader_task: Mutex::new(Some(reader_task)),
            stderr_task: Mutex::new(Some(stderr_task)),
            next_id: AtomicU64::new(1),
            closed,
            shutdown_started: AtomicBool::new(false),
            process_group_id,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    pub async fn request(&self, method: &str, params: Option<Value>) -> Result<Value> {
        if self.closed.load(Ordering::Acquire) {
            bail!("{} RPC process 已关闭", self.label)
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            bail!("{} RPC request id 已耗尽", self.label)
        }
        let mut message = json!({"jsonrpc": "2.0", "id": id, "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .await
            .insert(id_key(&json!(id))?, sender);
        if let Err(error) = write_message(&self.writer, self.framing, &message).await {
            self.pending.lock().await.remove(&id_key(&json!(id))?);
            return Err(error).with_context(|| format!("发送 {method} RPC request 失败"));
        }

        match timeout(self.request_timeout, receiver).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(error))) => bail!("{error}"),
            Ok(Err(_)) => bail!("{} RPC process 在响应前关闭", self.label),
            Err(_) => {
                self.pending.lock().await.remove(&id_key(&json!(id))?);
                self.cancel_request(id).await;
                bail!(
                    "{} RPC request {method} 超过 {}ms timeout",
                    self.label,
                    self.request_timeout.as_millis()
                )
            }
        }
    }

    pub async fn notify(&self, method: &str, params: Option<Value>) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            bail!("{} RPC process 已关闭", self.label)
        }
        let mut message = json!({"jsonrpc": "2.0", "method": method});
        if let Some(params) = params {
            message["params"] = params;
        }
        write_message(&self.writer, self.framing, &message)
            .await
            .with_context(|| format!("发送 {method} RPC notification 失败"))
    }

    async fn cancel_request(&self, id: u64) {
        let (method, params) = match self.framing {
            RpcFraming::Newline => (
                "notifications/cancelled",
                json!({"requestId": id, "reason": "client timeout"}),
            ),
            RpcFraming::ContentLength => ("$/cancelRequest", json!({"id": id})),
        };
        let _ = self.notify(method, Some(params)).await;
    }

    pub async fn stderr_excerpt(&self) -> String {
        let bytes = self.stderr.lock().await.iter().copied().collect::<Vec<_>>();
        String::from_utf8_lossy(&bytes)
            .chars()
            .filter(|character| !character.is_control() || matches!(character, '\n' | '\r' | '\t'))
            .collect::<String>()
    }

    pub async fn shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        self.closed.store(true, Ordering::Release);
        self.writer.lock().await.take();
        fail_pending(
            &self.pending,
            format!("{} RPC process 正在关闭", self.label),
        )
        .await;

        let mut child = self.child.lock().await;
        if child.try_wait().ok().flatten().is_none() {
            match timeout(SHUTDOWN_GRACE, child.wait()).await {
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => {
                    terminate_process_tree(self.process_group_id);
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
            }
        }
        drop(child);

        if let Some(mut task) = self.reader_task.lock().await.take() {
            let _ = timeout(Duration::from_secs(1), &mut task).await;
            task.abort();
        }
        if let Some(mut task) = self.stderr_task.lock().await.take() {
            let _ = timeout(Duration::from_secs(1), &mut task).await;
            task.abort();
        }
    }
}

impl Drop for StdioRpcClient {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if let Ok(mut writer) = self.writer.try_lock() {
            writer.take();
        }
        let child = self.child.get_mut();
        if child.try_wait().ok().flatten().is_none() {
            terminate_process_tree(self.process_group_id);
            let _ = child.start_kill();
        }
        if let Some(task) = self.reader_task.get_mut().take() {
            task.abort();
        }
        if let Some(task) = self.stderr_task.get_mut().take() {
            task.abort();
        }
    }
}

async fn reader_loop<R>(mut reader: R, state: ReaderLoopState)
where
    R: AsyncBufRead + Unpin,
{
    let ReaderLoopState {
        framing,
        writer,
        pending,
        events,
        closed,
        label,
        server_request_handler,
    } = state;
    let outcome = async {
        while let Some(message) = read_message(&mut reader, framing, MAX_RPC_MESSAGE_BYTES).await? {
            if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                bail!("{label} RPC message 缺少 jsonrpc=2.0")
            }
            if let Some(method) = message.get("method").and_then(Value::as_str) {
                let _ = events.send(message.clone());
                if let Some(id) = message.get("id") {
                    let result = if method == "ping" {
                        Some(json!({}))
                    } else {
                        server_request_handler
                            .as_ref()
                            .and_then(|handler| handler(method, message.get("params")))
                    };
                    let response = result.map_or_else(
                        || {
                            json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {"code": -32601, "message": "Client method not supported"}
                            })
                        },
                        |result| json!({"jsonrpc": "2.0", "id": id, "result": result}),
                    );
                    write_message(&writer, framing, &response).await?;
                }
                continue;
            }
            let Some(id) = message.get("id") else {
                bail!("{label} RPC response 缺少 id")
            };
            let key = id_key(id)?;
            let Some(sender) = pending.lock().await.remove(&key) else {
                continue;
            };
            let result = response_result(&message, &label);
            let _ = sender.send(result);
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    closed.store(true, Ordering::Release);
    let reason = outcome.err().map_or_else(
        || format!("{label} RPC stdout 已关闭"),
        |error| format!("{error:#}"),
    );
    fail_pending(&pending, reason).await;
}

fn response_result(message: &Value, label: &str) -> std::result::Result<Value, String> {
    match (message.get("result"), message.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error)) => {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32603);
            let text = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown RPC error");
            let data = error
                .get("data")
                .map(|value| bounded_json(value, 2048))
                .unwrap_or_default();
            Err(format!(
                "{label} RPC error {code}: {text}{}",
                if data.is_empty() {
                    String::new()
                } else {
                    format!(" ({data})")
                }
            ))
        }
        _ => Err(format!(
            "{label} RPC response 必须且只能包含 result 或 error"
        )),
    }
}

async fn fail_pending(pending: &Pending, reason: String) {
    let senders = pending
        .lock()
        .await
        .drain()
        .map(|(_, sender)| sender)
        .collect::<Vec<_>>();
    for sender in senders {
        let _ = sender.send(Err(reason.clone()));
    }
}

fn id_key(id: &Value) -> Result<String> {
    match id {
        Value::String(value) => Ok(format!("s:{value}")),
        Value::Number(value) => Ok(format!("n:{value}")),
        _ => bail!("RPC id 必须是 string 或 number"),
    }
}

async fn write_message(writer: &SharedWriter, framing: RpcFraming, message: &Value) -> Result<()> {
    let encoded = encode_message(message, framing)?;
    let mut writer = writer.lock().await;
    let writer = writer.as_mut().context("RPC stdin 已关闭")?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

fn encode_message(message: &Value, framing: RpcFraming) -> Result<Vec<u8>> {
    let body = serde_json::to_vec(message).context("无法编码 RPC message")?;
    if body.len() > MAX_RPC_REQUEST_BYTES {
        bail!("RPC request 超过 {MAX_RPC_REQUEST_BYTES} 字节限制")
    }
    match framing {
        RpcFraming::Newline => {
            let mut encoded = body;
            encoded.push(b'\n');
            Ok(encoded)
        }
        RpcFraming::ContentLength => {
            let header = format!("Content-Length: {}\r\n\r\n", body.len());
            let mut encoded = Vec::with_capacity(header.len() + body.len());
            encoded.extend_from_slice(header.as_bytes());
            encoded.extend_from_slice(&body);
            Ok(encoded)
        }
    }
}

async fn read_message<R>(reader: &mut R, framing: RpcFraming, limit: usize) -> Result<Option<Value>>
where
    R: AsyncBufRead + Unpin,
{
    let bytes = match framing {
        RpcFraming::Newline => loop {
            let Some(line) = read_line_limited(reader, limit).await? else {
                return Ok(None);
            };
            if line.len() > limit {
                bail!("RPC message 超过 {limit} 字节限制")
            }
            if !line.is_empty() {
                break line;
            }
        },
        RpcFraming::ContentLength => {
            let mut header_bytes = 0usize;
            let mut content_length = None;
            loop {
                let Some(line) = read_line_limited(reader, MAX_RPC_HEADER_BYTES).await? else {
                    return if header_bytes == 0 {
                        Ok(None)
                    } else {
                        Err(anyhow::anyhow!("RPC header 在完成前结束"))
                    };
                };
                header_bytes = header_bytes
                    .checked_add(line.len() + 2)
                    .context("RPC header 大小溢出")?;
                if header_bytes > MAX_RPC_HEADER_BYTES {
                    bail!("RPC header 超过 {MAX_RPC_HEADER_BYTES} 字节限制")
                }
                if line.is_empty() {
                    break;
                }
                let text = std::str::from_utf8(&line).context("RPC header 不是 UTF-8")?;
                match text.split_once(':') {
                    Some((name, value)) if name.eq_ignore_ascii_case("content-length") => {
                        content_length = Some(
                            value
                                .trim()
                                .parse::<usize>()
                                .context("Content-Length 无效")?,
                        );
                    }
                    _ => {}
                }
            }
            let length = content_length.context("RPC header 缺少 Content-Length")?;
            if length > limit {
                bail!("RPC message 超过 {limit} 字节限制")
            }
            let mut body = vec![0; length];
            reader
                .read_exact(&mut body)
                .await
                .context("RPC body 在完成前结束")?;
            body
        }
    };
    let value = serde_json::from_slice(&bytes).context("RPC message 不是有效 JSON")?;
    Ok(Some(value))
}

async fn read_line_limited<R>(reader: &mut R, limit: usize) -> Result<Option<Vec<u8>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trim_line_end(line)))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(take) > limit.saturating_add(1) {
            bail!("RPC line 超过 {limit} 字节限制")
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(Some(trim_line_end(line)));
        }
    }
}

fn trim_line_end(mut line: Vec<u8>) -> Vec<u8> {
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    line
}

async fn drain_stderr<R>(mut reader: R, target: Arc<Mutex<VecDeque<u8>>>)
where
    R: AsyncRead + Unpin,
{
    let mut buffer = [0u8; 8192];
    loop {
        let count = match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(count) => count,
        };
        let mut target = target.lock().await;
        target.extend(&buffer[..count]);
        while target.len() > MAX_RPC_STDERR_BYTES {
            target.pop_front();
        }
    }
}

fn bounded_json(value: &Value, limit: usize) -> String {
    let mut rendered = value.to_string();
    if rendered.len() <= limit {
        return rendered;
    }
    let mut end = limit;
    while !rendered.is_char_boundary(end) {
        end -= 1;
    }
    rendered.truncate(end);
    rendered.push('…');
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn newline_framing_round_trips_without_raw_newlines() {
        let value = json!({"jsonrpc":"2.0","id":1,"result":{"text":"a\nb"}});
        let encoded = encode_message(&value, RpcFraming::Newline).unwrap();
        assert_eq!(encoded.iter().filter(|byte| **byte == b'\n').count(), 1);
        let (mut writer, reader) = tokio::io::duplex(1024);
        writer.write_all(&encoded).await.unwrap();
        drop(writer);
        let mut reader = BufReader::new(reader);
        let decoded = read_message(&mut reader, RpcFraming::Newline, 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decoded, value);
    }

    #[tokio::test]
    async fn content_length_framing_round_trips() {
        let value = json!({"jsonrpc":"2.0","id":2,"result":{}});
        let encoded = encode_message(&value, RpcFraming::ContentLength).unwrap();
        let (mut writer, reader) = tokio::io::duplex(1024);
        writer.write_all(&encoded).await.unwrap();
        drop(writer);
        let mut reader = BufReader::new(reader);
        let decoded = read_message(&mut reader, RpcFraming::ContentLength, 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decoded, value);
    }

    #[tokio::test]
    async fn oversized_newline_message_is_rejected_before_unbounded_growth() {
        let (mut writer, reader) = tokio::io::duplex(4096);
        writer.write_all(&vec![b'x'; 2048]).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
        drop(writer);
        let mut reader = BufReader::new(reader);
        let error = read_message(&mut reader, RpcFraming::Newline, 1024)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("1024"));
    }
}

use std::{
    fmt::Write as _,
    io::{Read as _, Write as _},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
};

use open_agent_harness::{
    api::ModelClient,
    config::EndpointConfig,
    permissions::{PermissionManager, PermissionMode},
    protocol::{ApiFormat, ChatTokensField},
    query::{QueryEngine, QueryOptions},
    tools::{ToolContext, ToolRegistry},
};
use serde_json::{Value, json};
use tempfile::tempdir;

const MARKER: &str = "must-not-be-written.txt";
const MARKER_CONTENT: &str = "a non-terminal response executed a tool";

#[tokio::test]
async fn chat_json_rejects_tools_with_length_finish_reason() {
    assert_chat_json_finish_rejected(Some("length")).await;
}

#[tokio::test]
async fn chat_json_rejects_tools_with_stop_finish_reason() {
    assert_chat_json_finish_rejected(Some("stop")).await;
}

#[tokio::test]
async fn chat_json_rejects_tools_without_finish_reason() {
    assert_chat_json_finish_rejected(None).await;
}

#[tokio::test]
async fn chat_sse_rejects_tools_with_length_finish_reason() {
    assert_chat_sse_finish_rejected(Some("length")).await;
}

#[tokio::test]
async fn chat_sse_rejects_tools_with_stop_finish_reason() {
    assert_chat_sse_finish_rejected(Some("stop")).await;
}

#[tokio::test]
async fn chat_sse_rejects_tools_without_finish_reason() {
    assert_chat_sse_finish_rejected(None).await;
}

#[tokio::test]
async fn responses_json_rejects_in_progress_tool_calls() {
    assert_responses_json_status_rejected(Some("in_progress")).await;
}

#[tokio::test]
async fn responses_json_rejects_queued_tool_calls() {
    assert_responses_json_status_rejected(Some("queued")).await;
}

#[tokio::test]
async fn responses_json_rejects_tool_calls_without_status() {
    assert_responses_json_status_rejected(None).await;
}

#[tokio::test]
async fn responses_stream_rejects_orphan_function_delta_before_empty_completion() {
    let events = [
        json!({
            "type": "response.created",
            "response": {"id": "response-orphan-delta", "status": "in_progress"}
        }),
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "function_call",
                "id": "fc-write",
                "call_id": "call-write",
                "name": "Write",
                "arguments": "",
                "status": "in_progress"
            }
        }),
        json!({
            "type": "response.function_call_arguments.delta",
            "output_index": 0,
            "delta": write_arguments()
        }),
        json!({"type": "response.completed"}),
    ];
    let outcome = run_case(
        ApiFormat::Responses,
        true,
        MockResponse::sse(responses_sse(events)),
        None,
    )
    .await;

    assert_rejected_without_tool(&outcome, "Responses orphan function-call delta");
}

#[tokio::test]
async fn responses_error_event_aborts_partial_tool_call_and_redacts_token() {
    assert_responses_failure_rejected("error").await;
}

#[tokio::test]
async fn responses_response_error_event_aborts_partial_tool_call_and_redacts_token() {
    assert_responses_failure_rejected("response.error").await;
}

#[tokio::test]
async fn responses_failed_event_aborts_partial_tool_call_and_redacts_token() {
    assert_responses_failure_rejected("response.failed").await;
}

#[tokio::test]
async fn chat_stream_rejects_tool_delta_after_finish_reason() {
    let finished = json!({
        "id": "chat-event-after-finish",
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
    });
    let events = [
        finished.clone(),
        json!({
            "id": "chat-event-after-finish",
            "choices": [{
                "index": 0,
                "delta": {"tool_calls": [chat_write_call()]},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}
        }),
    ];
    let outcome = run_case(
        ApiFormat::ChatCompletions,
        true,
        MockResponse::sse(chat_sse(events)),
        None,
    )
    .await;

    assert_rejected_without_tool(&outcome, "Chat tool delta after finish_reason");

    for (label, terminal) in [
        (
            "Chat terminal echo wrong empty type",
            json!({
                "id":"chat-event-after-finish",
                "choices":[{"index":0,"delta":{"role":"assistant","content":[]},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":1,"completion_tokens":1}
            }),
        ),
        (
            "Chat terminal echo changed raw finish reason",
            json!({
                "id":"chat-event-after-finish",
                "choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":"function_call"}],
                "usage":{"prompt_tokens":1,"completion_tokens":1}
            }),
        ),
        (
            "Chat terminal echo missing usage",
            json!({
                "id":"chat-event-after-finish",
                "choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":"stop"}]
            }),
        ),
    ] {
        let outcome = run_case(
            ApiFormat::ChatCompletions,
            true,
            MockResponse::sse(chat_sse([finished.clone(), terminal])),
            None,
        )
        .await;
        assert_rejected_without_tool(&outcome, label);
    }

    let valid_terminal = json!({
        "id":"chat-event-after-finish",
        "choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":1,"completion_tokens":1}
    });
    let outcome = run_case(
        ApiFormat::ChatCompletions,
        true,
        MockResponse::sse(chat_sse([
            finished,
            valid_terminal,
            json!({"id":"chat-event-after-finish","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}),
        ])),
        None,
    )
    .await;
    assert_rejected_without_tool(&outcome, "Chat JSON after terminal usage echo");
}

async fn assert_chat_json_finish_rejected(finish_reason: Option<&str>) {
    let response = json!({
        "id": "chat-json-non-tool-finish",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [chat_write_call()]
            },
            "finish_reason": finish_reason
        }]
    });
    let outcome = run_case(
        ApiFormat::ChatCompletions,
        false,
        MockResponse::json(response),
        None,
    )
    .await;

    assert_rejected_without_tool(
        &outcome,
        &format!("Chat JSON finish_reason={finish_reason:?}"),
    );
}

async fn assert_chat_sse_finish_rejected(finish_reason: Option<&str>) {
    let event = json!({
        "id": "chat-stream-non-tool-finish",
        "choices": [{
            "index": 0,
            "delta": {"tool_calls": [chat_write_call()]},
            "finish_reason": finish_reason
        }]
    });
    let outcome = run_case(
        ApiFormat::ChatCompletions,
        true,
        MockResponse::sse(chat_sse([event])),
        None,
    )
    .await;

    assert_rejected_without_tool(
        &outcome,
        &format!("Chat SSE finish_reason={finish_reason:?}"),
    );
}

async fn assert_responses_json_status_rejected(status: Option<&str>) {
    let mut response = json!({
        "id": "response-json-non-terminal",
        "output": [responses_write_call()],
        "usage": {"input_tokens": 1, "output_tokens": 1}
    });
    if let Some(status) = status {
        response["status"] = Value::String(status.to_owned());
    }
    let outcome = run_case(
        ApiFormat::Responses,
        false,
        MockResponse::json(response),
        None,
    )
    .await;

    assert_rejected_without_tool(&outcome, &format!("Responses JSON status={status:?}"));
}

async fn assert_responses_failure_rejected(failure_type: &str) {
    const SECRET: &str = "synthetic-endpoint-token-for-redaction-test";

    let failure = if failure_type == "response.failed" {
        json!({
            "type": failure_type,
            "response": {
                "id": "response-provider-failure",
                "status": "failed",
                "error": {"message": format!("provider reflected {SECRET}")}
            }
        })
    } else {
        json!({
            "type": failure_type,
            "error": {"message": format!("provider reflected {SECRET}")}
        })
    };
    let events = [
        json!({
            "type": "response.created",
            "response": {"id": "response-provider-failure", "status": "in_progress"}
        }),
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "function_call",
                "id": "fc-write",
                "call_id": "call-write",
                "name": "Write",
                "arguments": "",
                "status": "in_progress"
            }
        }),
        json!({
            "type": "response.function_call_arguments.delta",
            "output_index": 0,
            "delta": format!("{{\"file_path\":\"{MARKER}\",\"content\":\"unfinished")
        }),
        failure,
    ];
    let outcome = run_case(
        ApiFormat::Responses,
        true,
        MockResponse::sse(responses_sse(events)),
        Some(SECRET),
    )
    .await;

    assert_rejected_without_tool(&outcome, failure_type);
    let error = outcome.error.as_deref().unwrap();
    assert!(
        !error.contains(SECRET),
        "{failure_type} reflected the endpoint credential: {error}"
    );
    assert!(
        error.contains("redacted-endpoint-token"),
        "{failure_type} did not retain an explicit redaction marker: {error}"
    );
}

fn chat_write_call() -> Value {
    json!({
        "index": 0,
        "id": "call-write",
        "type": "function",
        "function": {
            "name": "Write",
            "arguments": write_arguments()
        }
    })
}

fn responses_write_call() -> Value {
    json!({
        "type": "function_call",
        "id": "fc-write",
        "call_id": "call-write",
        "name": "Write",
        "arguments": write_arguments(),
        "status": "completed"
    })
}

fn write_arguments() -> String {
    json!({"file_path": MARKER, "content": MARKER_CONTENT}).to_string()
}

fn chat_sse<const N: usize>(events: [Value; N]) -> String {
    let mut body = events.into_iter().fold(String::new(), |mut body, event| {
        write!(body, "data: {event}\n\n").expect("writing to a String cannot fail");
        body
    });
    body.push_str("data: [DONE]\n\n");
    body
}

fn responses_sse<const N: usize>(events: [Value; N]) -> String {
    let mut body = events.into_iter().fold(String::new(), |mut body, event| {
        write!(
            body,
            "event: {}\ndata: {event}\n\n",
            event["type"].as_str().unwrap()
        )
        .expect("writing to a String cannot fail");
        body
    });
    body.push_str("data: [DONE]\n\n");
    body
}

struct CaseOutcome {
    error: Option<String>,
    marker_exists: bool,
    followup_requests: usize,
}

async fn run_case(
    format: ApiFormat,
    stream: bool,
    response: MockResponse,
    token: Option<&str>,
) -> CaseOutcome {
    let fallback = match format {
        ApiFormat::ChatCompletions => MockResponse::json(json!({
            "id": "unexpected-chat-followup",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "unexpected followup"},
                "finish_reason": "stop"
            }]
        })),
        ApiFormat::Responses => MockResponse::json(json!({
            "id": "unexpected-responses-followup",
            "status": "completed",
            "output": [{
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "unexpected followup"}]
            }]
        })),
        ApiFormat::Auto | ApiFormat::Messages => unreachable!(),
    };
    let server = MockServer::spawn(response, fallback);
    let temp = tempdir().unwrap();
    let marker = temp.path().join(MARKER);
    let mut engine = query_engine(endpoint(server.address, format, stream, token), temp.path());

    let result = engine.run_turn("write the marker".into()).await;
    engine.shutdown().await;
    let marker_exists = marker.exists();
    let followup_requests = server.finish();

    CaseOutcome {
        error: result.err().map(|error| format!("{error:#}")),
        marker_exists,
        followup_requests,
    }
}

fn assert_rejected_without_tool(outcome: &CaseOutcome, label: &str) {
    assert!(
        outcome.error.is_some(),
        "{label} was accepted instead of failing closed"
    );
    assert!(
        !outcome.marker_exists,
        "{label} executed Write before protocol completion was proven"
    );
    assert_eq!(
        outcome.followup_requests, 0,
        "{label} started a tool-result round"
    );
}

fn query_engine(endpoint: EndpointConfig, cwd: &std::path::Path) -> QueryEngine {
    let client = ModelClient::new(endpoint).unwrap();
    let context = ToolContext::new(
        cwd.to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    QueryEngine::new(
        client,
        ToolRegistry::default(),
        context,
        QueryOptions {
            model: "test-model".into(),
            max_tokens: 1_024,
            system: "test system".into(),
            messages: Vec::new(),
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    )
}

fn endpoint(
    address: SocketAddr,
    api_format: ApiFormat,
    stream: bool,
    token: Option<&str>,
) -> EndpointConfig {
    EndpointConfig {
        token: token.map(ToOwned::to_owned),
        base_url: format!("http://{address}"),
        messages_path: match api_format {
            ApiFormat::ChatCompletions => "/v1/chat/completions",
            ApiFormat::Responses => "/v1/responses",
            ApiFormat::Auto | ApiFormat::Messages => unreachable!(),
        }
        .into(),
        api_format,
        stream,
        chat_tokens_field: ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
        allow_env_proxy: false,
    }
}

struct MockResponse {
    content_type: &'static str,
    body: String,
}

impl MockResponse {
    fn json(body: Value) -> Self {
        Self {
            content_type: "application/json",
            body: body.to_string(),
        }
    }

    fn sse(body: String) -> Self {
        Self {
            content_type: "text/event-stream",
            body,
        }
    }
}

struct MockServer {
    address: SocketAddr,
    stop: mpsc::Sender<()>,
    join: thread::JoinHandle<usize>,
}

impl MockServer {
    fn spawn(first: MockResponse, fallback: MockResponse) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = mpsc::channel();
        let join = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            write_response(&mut stream, &first);

            listener.set_nonblocking(true).unwrap();
            let mut followups = 0;
            loop {
                if stopped.try_recv().is_ok() {
                    return followups;
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        read_request(&mut stream);
                        write_response(&mut stream, &fallback);
                        followups += 1;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("mock server accept failed: {error}"),
                }
            }
        });
        Self {
            address,
            stop,
            join,
        }
    }

    fn finish(self) -> usize {
        let _ = self.stop.send(());
        self.join.join().unwrap()
    }
}

fn read_request(stream: &mut TcpStream) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4_096];
    let header_end = loop {
        let count = stream.read(&mut chunk).unwrap();
        assert!(count > 0, "request ended before its headers");
        buffer.extend_from_slice(&chunk[..count]);
        if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while buffer.len() < header_end + content_length {
        let count = stream.read(&mut chunk).unwrap();
        assert!(count > 0, "request ended before its body");
        buffer.extend_from_slice(&chunk[..count]);
    }
}

fn write_response(stream: &mut TcpStream, response: &MockResponse) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response.content_type,
        response.body.len()
    )
    .unwrap();
    stream.write_all(response.body.as_bytes()).unwrap();
}

use std::{
    collections::BTreeSet,
    io::{Read as _, Write as _},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use open_agent_harness::{
    api::ModelClient,
    config::EndpointConfig,
    permissions::{PermissionManager, PermissionMode},
    protocol::{ApiFormat, ChatTokensField},
    query::{QueryEngine, QueryOptions},
    tools::{ToolContext, ToolRegistry},
    types::Message,
};
use serde_json::{Value, json};
use tempfile::tempdir;

#[derive(Debug)]
struct CapturedRequest {
    target: String,
    headers: String,
    body: Value,
}

struct MockResponse {
    content_type: &'static str,
    body: String,
}

#[tokio::test]
async fn chat_completions_round_trips_tools_and_openrouter_stream_conventions() {
    let responses = vec![
        MockResponse {
            content_type: "text/event-stream",
            body: chat_tool_stream(),
        },
        MockResponse {
            content_type: "text/event-stream",
            body: chat_text_stream(),
        },
    ];
    let (address, requests, server) = spawn_server(responses);
    let temp = tempdir().unwrap();
    std::fs::write(temp.path().join("fixture.txt"), "open protocol evidence\n").unwrap();
    let mut engine = query_engine(
        endpoint(address, "/v1/chat/completions", ApiFormat::Auto, true),
        temp.path(),
    );

    let result = engine.run_turn("read fixture".into()).await.unwrap();
    engine.shutdown().await;
    server.join().unwrap();

    assert_eq!(result.text, "chat complete");
    assert!(result.streamed_text);
    assert_eq!(engine.usage.input_tokens, 11);
    assert_eq!(engine.usage.output_tokens, 7);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].target, "/v1/chat/completions");
    assert_exact_keys(
        &requests[0].body,
        &[
            "max_completion_tokens",
            "messages",
            "model",
            "stream",
            "stream_options",
            "tools",
        ],
    );
    assert_eq!(requests[0].body["messages"][0]["role"], "system");
    assert_eq!(requests[0].body["messages"][1]["role"], "user");
    assert_eq!(requests[0].body["tools"][0]["type"], "function");
    assert_eq!(requests[0].body["max_completion_tokens"], 1024);
    assert!(requests[0].body.get("max_tokens").is_none());
    assert_eq!(requests[0].body["stream_options"]["include_usage"], true);
    assert!(
        requests[0]
            .headers
            .contains("authorization: Bearer test-token")
    );

    let second_messages = requests[1].body["messages"].as_array().unwrap();
    assert!(second_messages.iter().any(|message| {
        message["role"] == "assistant" && message["tool_calls"][0]["function"]["name"] == "Read"
    }));
    assert!(second_messages.iter().any(|message| {
        message["role"] == "tool"
            && message["tool_call_id"] == "call-read"
            && message["content"]
                .as_str()
                .is_some_and(|text| text.contains("open protocol evidence"))
    }));
}

#[tokio::test]
async fn responses_round_trips_stateless_reasoning_and_function_outputs() {
    let responses = vec![
        MockResponse {
            content_type: "text/event-stream",
            body: responses_tool_stream(),
        },
        MockResponse {
            content_type: "text/event-stream",
            body: responses_text_stream(),
        },
    ];
    let (address, requests, server) = spawn_server(responses);
    let temp = tempdir().unwrap();
    std::fs::write(temp.path().join("fixture.txt"), "responses evidence\n").unwrap();
    let mut engine = query_engine(
        endpoint(address, "/v1/responses", ApiFormat::Auto, true),
        temp.path(),
    );

    let result = engine.run_turn("read fixture".into()).await.unwrap();
    engine.shutdown().await;
    server.join().unwrap();

    assert_eq!(result.text, "responses complete");
    assert!(result.streamed_text);
    assert_eq!(engine.usage.input_tokens, 13);
    assert_eq!(engine.usage.output_tokens, 8);
    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].target, "/v1/responses");
    assert_exact_keys(
        &requests[0].body,
        &[
            "include",
            "input",
            "instructions",
            "max_output_tokens",
            "model",
            "store",
            "stream",
            "tools",
        ],
    );
    assert!(
        requests[0].body["instructions"]
            .as_str()
            .is_some_and(|instructions| instructions.starts_with("test system"))
    );
    assert_eq!(requests[0].body["max_output_tokens"], 1024);
    assert_eq!(requests[0].body["store"], false);
    assert_eq!(
        requests[0].body["include"][0],
        "reasoning.encrypted_content"
    );
    assert_eq!(requests[0].body["tools"][0]["type"], "function");

    let second_input = requests[1].body["input"].as_array().unwrap();
    let reasoning = second_input
        .iter()
        .find(|item| item["type"] == "reasoning")
        .unwrap();
    assert_eq!(reasoning["encrypted_content"], "opaque-test-state");
    let function_call = second_input
        .iter()
        .find(|item| {
            item["type"] == "function_call"
                && item["call_id"] == "call-read"
                && item["name"] == "Read"
        })
        .unwrap();
    assert_eq!(function_call["id"], "fc-test");
    assert!(second_input.iter().any(|item| {
        item["type"] == "function_call_output"
            && item["call_id"] == "call-read"
            && item["output"]
                .as_str()
                .is_some_and(|text| text.contains("responses evidence"))
    }));
}

#[tokio::test]
async fn chat_and_responses_accept_complete_json_when_streaming_is_disabled() {
    let responses = vec![
        MockResponse {
            content_type: "Application/JSON; Charset=UTF-8",
            body: json!({
                "id":"chat-json",
                "choices":[{"index":0,"message":{"role":"assistant","content":"chat json"},"finish_reason":"stop"}],
                "usage":null
            })
            .to_string(),
        },
        MockResponse {
            content_type: "application/json",
            body: json!({
                "id":"responses-json","status":"completed",
                "output":[{"type":"message","id":"msg-json","status":"completed","role":"assistant","content":[{"type":"output_text","text":"responses json"}]}],
                "usage":{"input_tokens":null,"output_tokens":2}
            })
            .to_string(),
        },
    ];
    let (address, requests, server) = spawn_server(responses);
    let chat = ModelClient::new(endpoint(
        address,
        "/v1/chat/completions",
        ApiFormat::ChatCompletions,
        false,
    ))
    .unwrap();
    let responses = ModelClient::new(endpoint(
        address,
        "/v1/responses",
        ApiFormat::Responses,
        false,
    ))
    .unwrap();

    let chat_result = chat
        .messages(
            "model",
            32,
            "system",
            &[Message::user_text("hi")],
            &[],
            None,
        )
        .await
        .unwrap();
    let responses_result = responses
        .messages(
            "model",
            32,
            "system",
            &[Message::user_text("hi")],
            &[],
            None,
        )
        .await
        .unwrap();
    server.join().unwrap();

    assert_eq!(chat_result.response.content[0]["text"], "chat json");
    assert!(
        responses_result
            .response
            .content
            .iter()
            .any(|block| block["type"] == "text" && block["text"] == "responses json")
    );
    assert_eq!(responses_result.response.usage.unwrap().input_tokens, 0);
    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].body["stream"], false);
    assert!(requests[0].body.get("stream_options").is_none());
    assert_eq!(requests[1].body["stream"], false);
}

#[tokio::test]
async fn truncated_tool_stream_is_rejected_before_any_tool_executes() {
    let body = [
        json!({"id":"chat-cut","choices":[{"index":0,"delta":{"tool_calls":[{
            "index":0,"id":"call-write","type":"function","function":{
                "name":"Write","arguments":"{\"file_path\":\"must-not-exist.txt\",\"content\":\"bad\"}"
            }}]},"finish_reason":"tool_calls"}]}),
    ]
    .into_iter()
    .map(chat_event)
    .collect();
    let (address, _, server) = spawn_server(vec![MockResponse {
        content_type: "text/event-stream",
        body,
    }]);
    let temp = tempdir().unwrap();
    let mut engine = query_engine(
        endpoint(
            address,
            "/v1/chat/completions",
            ApiFormat::ChatCompletions,
            true,
        ),
        temp.path(),
    );

    let error = engine.run_turn("write".into()).await.unwrap_err();
    engine.shutdown().await;
    server.join().unwrap();

    assert!(error.to_string().contains("[DONE]"));
    assert!(!temp.path().join("must-not-exist.txt").exists());
}

#[tokio::test]
async fn openrouter_midstream_errors_are_reported_without_reflecting_credentials() {
    let secret = "local-test-token-never-log";
    let body = format!(
        ": OPENROUTER PROCESSING\n\ndata: {}\n\n",
        json!({"error":{"message":format!("reflected {secret}")}})
    );
    let (address, _, server) = spawn_server(vec![MockResponse {
        content_type: "text/event-stream",
        body,
    }]);
    let mut config = endpoint(
        address,
        "/api/v1/chat/completions",
        ApiFormat::ChatCompletions,
        true,
    );
    config.token = Some(secret.to_owned());
    let client = ModelClient::new(config).unwrap();

    let result = client
        .messages(
            "model",
            32,
            "system",
            &[Message::user_text("hi")],
            &[],
            None,
        )
        .await;
    let error = match result {
        Ok(_) => panic!("midstream endpoint error was accepted"),
        Err(error) => error,
    };
    server.join().unwrap();

    let rendered = format!("{error:#}");
    assert!(!rendered.contains(secret));
    assert!(rendered.contains("redacted-endpoint-token"));
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
            max_tokens: 1024,
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
    path: &str,
    api_format: ApiFormat,
    stream: bool,
) -> EndpointConfig {
    EndpointConfig {
        token: Some("test-token".into()),
        base_url: format!("http://{address}"),
        messages_path: path.into(),
        api_format,
        stream,
        chat_tokens_field: ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
        allow_env_proxy: false,
    }
}

fn spawn_server(
    responses: Vec<MockResponse>,
) -> (
    SocketAddr,
    Arc<Mutex<Vec<CapturedRequest>>>,
    thread::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let server = thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            captured.lock().unwrap().push(read_request(&mut stream));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                response.content_type,
                response.body.len()
            )
            .unwrap();
            stream.write_all(response.body.as_bytes()).unwrap();
        }
    });
    (address, requests, server)
}

fn read_request(stream: &mut TcpStream) -> CapturedRequest {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let count = stream.read(&mut chunk).unwrap();
        assert!(count > 0);
        buffer.extend_from_slice(&chunk[..count]);
        if let Some(index) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(buffer[..header_end].to_vec()).unwrap();
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
        assert!(count > 0);
        buffer.extend_from_slice(&chunk[..count]);
    }
    let request_line = headers.lines().next().unwrap();
    let target = request_line.split_whitespace().nth(1).unwrap().to_owned();
    CapturedRequest {
        target,
        headers,
        body: serde_json::from_slice(&buffer[header_end..header_end + content_length]).unwrap(),
    }
}

fn chat_tool_stream() -> String {
    let first = json!({"id":"chat-tool","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{
        "index":0,"id":"call-read","type":"function","function":{"name":"Read","arguments":"{\"file_path\":"}
    }]},"finish_reason":null}]});
    let second = json!({"id":"chat-tool","choices":[{"index":0,"delta":{"tool_calls":[{
        "index":0,"function":{"arguments":"\"fixture.txt\"}"}
    }]},"finish_reason":"tool_calls"}]});
    let usage =
        json!({"id":"chat-tool","choices":[],"usage":{"prompt_tokens":null,"completion_tokens":3}});
    format!(
        ": OPENROUTER PROCESSING\r\n\r\n{}{}{}data: [DONE]\n\n",
        chat_event(first),
        chat_event(second),
        chat_event(usage)
    )
}

fn chat_text_stream() -> String {
    let delta = json!({"id":"chat-text","choices":[{"index":0,"delta":{"content":"chat complete"},"finish_reason":"stop"}]});
    let usage =
        json!({"id":"chat-text","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":4}});
    format!("{}{}data: [DONE]\n\n", chat_event(delta), chat_event(usage))
}

fn chat_event(value: Value) -> String {
    format!("data: {value}\n\n")
}

fn assert_exact_keys(value: &Value, expected: &[&str]) {
    let actual = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

fn responses_tool_stream() -> String {
    let events = [
        json!({"type":"response.created","response":{"id":"resp-tool","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{
            "type":"reasoning","id":"rs-test","summary":[],"encrypted_content":"opaque-test-state"
        }}),
        json!({"type":"response.output_item.done","output_index":0,"item":{
            "type":"reasoning","id":"rs-test","summary":[],"encrypted_content":"opaque-test-state"
        }}),
        json!({"type":"response.output_item.added","output_index":1,"item":{
            "type":"function_call","id":"fc-test","call_id":"call-read","name":"Read","arguments":"","status":"in_progress"
        }}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"delta":"{\"file_path\":"}),
        json!({"type":"response.function_call_arguments.delta","output_index":1,"delta":"\"fixture.txt\"}"}),
        json!({"type":"response.function_call_arguments.done","output_index":1,"arguments":"{\"file_path\":\"fixture.txt\"}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":{
            "type":"function_call","id":"fc-test","call_id":"call-read","name":"Read",
            "arguments":"{\"file_path\":\"fixture.txt\"}","status":"completed"
        }}),
        json!({"type":"response.done","response":{
            "id":"resp-tool","status":"completed",
            "usage":{"input_tokens":5,"output_tokens":3}
        }}),
    ]
    .into_iter()
    .map(responses_event)
    .collect::<String>();
    format!("{events}data: [DONE]\n\n")
}

fn responses_text_stream() -> String {
    let events = [
        json!({"type":"response.created","response":{"id":"resp-text","status":"in_progress"}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{
            "type":"message","id":"msg-text","role":"assistant","status":"in_progress","content":[]
        }}),
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{
            "type":"output_text","text":""
        }}),
        json!({"type":"response.content_part.delta","output_index":0,"content_index":0,"delta":"responses complete"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{
            "type":"message","id":"msg-text","role":"assistant","status":"completed",
            "content":[{"type":"output_text","text":"responses complete","annotations":[]}]
        }}),
        json!({"type":"response.done","response":{
            "id":"resp-text","status":"completed","usage":{"input_tokens":8,"output_tokens":5}
        }}),
    ]
    .into_iter()
    .map(responses_event)
    .collect::<String>();
    format!("{events}data: [DONE]\n\n")
}

fn responses_event(value: Value) -> String {
    format!(
        "event: {}\ndata: {value}\n\n",
        value["type"].as_str().unwrap()
    )
}

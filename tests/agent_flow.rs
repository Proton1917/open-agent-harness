use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use open_agent_harness::{
    agents::configure_agents,
    api::ModelClient,
    config::{EndpointConfig, Settings},
    permissions::{PermissionManager, PermissionMode},
    query::{QueryEngine, QueryOptions},
    tools::{ToolContext, ToolRegistry},
};
use serde_json::Value;
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn foreground_agent_uses_independent_history_and_returns_to_parent() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = Arc::clone(&requests);
    let server = thread::spawn(move || {
        for response in [
            agent_tool_stream(),
            subagent_text_stream(),
            parent_text_stream(),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            let body = read_http_body(&mut stream);
            captured
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        }
    });

    let temp = tempdir().unwrap();
    let client = ModelClient::new(EndpointConfig {
        token: None,
        base_url: format!("http://{address}"),
        messages_path: "/v1/messages".into(),
        allow_env_proxy: false,
    })
    .unwrap();
    let integration = configure_agents(&Settings::default()).unwrap();
    let registry = ToolRegistry::with_extensions(integration.deferred_tools, Vec::new()).unwrap();
    let mut context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    context.set_agent_limits(integration.limits);
    let mut engine = QueryEngine::new(
        client,
        registry,
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
    );
    let result = engine.run_turn("delegate this".into()).await.unwrap();
    engine.shutdown().await;
    server.join().unwrap();
    assert_eq!(result.text, "parent received result");

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[1]["system"]
            .as_str()
            .unwrap()
            .contains("recursion depth 1")
    );
    assert!(
        requests[1]["messages"][0]["content"]
            .to_string()
            .contains("inspect independently")
    );
    assert!(requests[2].to_string().contains("subagent result"));
}

#[tokio::test]
async fn background_agent_is_available_through_task_output_alias() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut stream);
        let response = text_stream("background result", "background-agent");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
    });

    let temp = tempdir().unwrap();
    let client = ModelClient::new(EndpointConfig {
        token: None,
        base_url: format!("http://{address}"),
        messages_path: "/v1/messages".into(),
        allow_env_proxy: false,
    })
    .unwrap();
    let integration = configure_agents(&Settings::default()).unwrap();
    let registry = ToolRegistry::with_extensions(Vec::new(), integration.deferred_tools).unwrap();
    let mut context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    context.set_agent_limits(integration.limits);
    let tools_context = context.clone();
    let engine = QueryEngine::new(
        client,
        registry.clone(),
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
    );
    let selected = registry
        .execute(
            &tools_context,
            "ToolSearch",
            json!({"query":"select:Agent"}),
        )
        .await;
    assert!(!selected.is_error, "{}", selected.content);
    let started = registry
        .execute(
            &tools_context,
            "Agent",
            json!({
                "prompt":"return a background result",
                "description":"background test",
                "runInBackground":true
            }),
        )
        .await;
    assert!(!started.is_error, "{}", started.content);
    let agent_id = started
        .content
        .lines()
        .find_map(|line| line.strip_prefix("agent_id="))
        .unwrap();
    let output = registry
        .execute(
            &tools_context,
            "TaskOutput",
            json!({"task_id":agent_id,"block":true,"timeout":30000}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(output.content.contains("background result"));
    engine.shutdown().await;
    server.join().unwrap();
}

fn agent_tool_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"parent-tool","usage":{}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"agent-1","name":"Agent","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"prompt\":\"inspect independently\",\"description\":\"inspection\"}"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(sse_event)
    .collect()
}

fn subagent_text_stream() -> String {
    text_stream("subagent result", "subagent")
}

fn parent_text_stream() -> String {
    text_stream("parent received result", "parent-final")
}

fn text_stream(text: &str, id: &str) -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":id,"usage":{"input_tokens":1,"output_tokens":0}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(sse_event)
    .collect()
}

fn sse_event(value: Value) -> String {
    format!(
        "event: {}\ndata: {}\n\n",
        value["type"].as_str().unwrap(),
        value
    )
}

fn read_http_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
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
        assert!(count > 0);
        buffer.extend_from_slice(&chunk[..count]);
    }
    buffer[header_end..header_end + content_length].to_vec()
}

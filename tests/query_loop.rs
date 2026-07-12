use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use open_agent_harness::{
    api::ModelClient,
    config::EndpointConfig,
    permissions::{PermissionManager, PermissionMode},
    query::{QueryEngine, QueryEvent, QueryOptions},
    tools::{ToolContext, ToolRegistry},
};
use serde_json::Value;
use tempfile::tempdir;

#[tokio::test]
async fn query_engine_round_trips_tool_use_and_result() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = Arc::clone(&requests);
    let server = thread::spawn(move || {
        let responses = [tool_use_stream(), text_stream()];
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let body = read_http_body(&mut stream);
            captured
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            let body = response.into_bytes();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
        }
    });

    let temp = tempdir().unwrap();
    std::fs::write(temp.path().join("fixture.txt"), "rust migration evidence\n").unwrap();
    std::fs::write(
        temp.path().join("AGENTS.md"),
        "workspace-system-context-marker",
    )
    .unwrap();
    let client = ModelClient::new(EndpointConfig {
        token: Some("test-key".into()),
        base_url: format!("http://{address}"),
        messages_path: "/v1/messages".into(),
        allow_env_proxy: false,
    })
    .unwrap();
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    context.reload_workspace_context().await.unwrap();
    let deltas = Arc::new(Mutex::new(String::new()));
    let captured_deltas = Arc::clone(&deltas);
    let mut engine = QueryEngine::new(
        client,
        ToolRegistry::default(),
        context,
        QueryOptions {
            model: "test-model".into(),
            max_tokens: 1024,
            system: "test system".into(),
            messages: Vec::new(),
            debug: false,
            text_delta_sink: Some(Arc::new(move |delta| {
                captured_deltas.lock().unwrap().push_str(delta);
            })),
            compact_config: None,
        },
    );
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured_events = Arc::clone(&events);
    engine.set_event_sink(Some(Arc::new(move |event| {
        captured_events.lock().unwrap().push(event.clone());
    })));

    let result = engine.run_turn("read the fixture".into()).await.unwrap();
    server.join().unwrap();
    assert_eq!(result.text, "迁移链路完成");
    assert!(result.streamed_text);
    assert_eq!(&*deltas.lock().unwrap(), "迁移链路完成");
    assert_eq!(engine.usage.input_tokens, 25);
    assert_eq!(engine.usage.output_tokens, 10);
    let events = events.lock().unwrap();
    assert!(matches!(events.first(), Some(QueryEvent::TurnStarted)));
    assert!(events.iter().any(|event| matches!(
        event,
        QueryEvent::ToolStarted { name, summary, .. }
            if name == "Read" && summary.contains("fixture.txt")
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        QueryEvent::ToolFinished { name, is_error: false, .. } if name == "Read"
    )));
    assert!(matches!(events.last(), Some(QueryEvent::TurnFinished)));
    drop(events);

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["model"], "test-model");
    assert!(
        requests[0]["system"]
            .as_str()
            .unwrap()
            .contains("workspace-system-context-marker")
    );
    assert!(
        requests[0]["system"]
            .as_str()
            .unwrap()
            .contains("# Current permission mode")
    );
    let second = serde_json::to_string(&requests[1]).unwrap();
    assert!(second.contains("tool_result"));
    assert!(second.contains("rust migration evidence"));
}

#[tokio::test]
async fn failed_model_round_rolls_back_unpersisted_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut stream);
        let body = b"not-json";
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(body).unwrap();
    });
    let temp = tempdir().unwrap();
    let client = ModelClient::new(EndpointConfig {
        token: None,
        base_url: format!("http://{address}"),
        messages_path: "/v1/messages".into(),
        allow_env_proxy: false,
    })
    .unwrap();
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    let mut engine = QueryEngine::new(
        client,
        ToolRegistry::default(),
        context,
        QueryOptions {
            model: "test-model".into(),
            max_tokens: 1024,
            system: "system".into(),
            messages: Vec::new(),
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    );
    assert!(engine.run_turn("must rollback".into()).await.is_err());
    server.join().unwrap();
    assert!(engine.messages.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn failed_followup_stops_background_tasks_started_by_the_turn() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut first);
        let stream = background_tool_stream();
        write!(
            first,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            stream.len(),
            stream
        )
        .unwrap();

        let (mut second, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut second);
        let body = b"not-json";
        write!(
            second,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        second.write_all(body).unwrap();
    });
    let temp = tempdir().unwrap();
    let client = ModelClient::new(EndpointConfig {
        token: None,
        base_url: format!("http://{address}"),
        messages_path: "/v1/messages".into(),
        allow_env_proxy: false,
    })
    .unwrap();
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    let observed = context.clone();
    let mut engine = QueryEngine::new(
        client,
        ToolRegistry::default(),
        context,
        QueryOptions {
            model: "test-model".into(),
            max_tokens: 1024,
            system: "system".into(),
            messages: Vec::new(),
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    );
    assert!(
        engine
            .run_turn("start a background task".into())
            .await
            .is_err()
    );
    server.join().unwrap();
    assert!(observed.tasks.lock().await.is_empty());
    assert!(engine.messages.is_empty());
}

#[test]
fn context_estimate_includes_system_prompt_and_tool_schemas() {
    let temp = tempdir().unwrap();
    let client = ModelClient::new(EndpointConfig {
        token: None,
        base_url: "http://127.0.0.1:9".into(),
        messages_path: "/v1/messages".into(),
        allow_env_proxy: false,
    })
    .unwrap();
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    let engine = QueryEngine::new(
        client,
        ToolRegistry::default(),
        context,
        QueryOptions {
            model: "test".into(),
            max_tokens: 16,
            system: "s".repeat(4_000),
            messages: Vec::new(),
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    );
    assert!(engine.estimated_tokens() > 1_000);
}

fn tool_use_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"msg_tool","usage":{"input_tokens":10,"output_tokens":0}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_1","name":"Read","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"file_"}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"path\":\"fixture.txt\"}"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":4}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(sse_event)
    .collect()
}

fn text_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"msg_done","usage":{"input_tokens":15,"output_tokens":0}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"迁移"}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"链路完成"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":6}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(sse_event)
    .collect()
}

#[cfg(unix)]
fn background_tool_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"msg_background","usage":{}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_background","name":"Bash","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"sleep 30\",\"run_in_background\":true}"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}),
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
        buffer.extend_from_slice(&chunk[..count]);
    }
    buffer[header_end..header_end + content_length].to_vec()
}

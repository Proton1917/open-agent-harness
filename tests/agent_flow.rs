use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use open_agent_harness::{
    agents::configure_agents,
    api::ModelClient,
    config::{EndpointConfig, Settings},
    permissions::{PermissionManager, PermissionMode},
    protocol::ApiFormat,
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
        api_format: ApiFormat::Messages,
        stream: true,
        chat_tokens_field: open_agent_harness::protocol::ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
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
    engine.set_model("updated-model".into());
    let result = engine.run_turn("delegate this".into()).await.unwrap();
    engine.shutdown().await;
    server.join().unwrap();
    assert_eq!(result.text, "parent received result");

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request["model"] == "updated-model")
    );
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
        api_format: ApiFormat::Messages,
        stream: true,
        chat_tokens_field: open_agent_harness::protocol::ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
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

#[tokio::test]
async fn nested_foreground_agent_reuses_single_scheduler_slot() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = Arc::clone(&requests);
    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        for response in [
            agent_tool_stream_for("parent-outer", "outer-agent", "run the outer agent"),
            agent_tool_stream_for("outer-inner", "inner-agent", "run the inner agent"),
            text_stream("inner result", "inner-final"),
            text_stream("outer result", "outer-final"),
            text_stream("parent result", "parent-final"),
        ] {
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "mock server timed out");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("mock server accept failed: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
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
        api_format: ApiFormat::Messages,
        stream: true,
        chat_tokens_field: open_agent_harness::protocol::ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
        allow_env_proxy: false,
    })
    .unwrap();
    let integration = configure_agents(&Settings {
        raw: json!({"agents": {
            "maxConcurrent": 1,
            "defaultTimeoutMs": 3000
        }}),
    })
    .unwrap();
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
            model: "nested-model".into(),
            max_tokens: 1024,
            system: "test system".into(),
            messages: Vec::new(),
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    );

    let result = tokio::time::timeout(
        Duration::from_secs(4),
        engine.run_turn("delegate recursively".into()),
    )
    .await
    .expect("nested foreground agents must not deadlock at maxConcurrent=1")
    .unwrap();
    engine.shutdown().await;
    server.join().unwrap();

    assert_eq!(result.text, "parent result");
    assert_eq!(requests.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn cancelled_agent_rolls_back_no_persistence_hot_refresh() {
    assert_interrupted_agent_hot_refresh_rolls_back(true).await;
}

#[tokio::test]
async fn timed_out_agent_rolls_back_no_persistence_hot_refresh() {
    assert_interrupted_agent_hot_refresh_rolls_back(false).await;
}

async fn assert_interrupted_agent_hot_refresh_rolls_back(explicit_stop: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (blocked_tx, blocked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut first);
        let response = write_agents_stream("interrupted rule", "interrupted-write");
        write_sse_response(&mut first, &response);

        let (mut blocked, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut blocked);
        blocked_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(blocked);

        let (mut recovery_write, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut recovery_write);
        let response = write_agents_stream("completed rule", "recovery-write");
        write_sse_response(&mut recovery_write, &response);

        let (mut recovery_final, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut recovery_final);
        write_sse_response(
            &mut recovery_final,
            &text_stream("recovery complete", "recovery-final"),
        );
    });

    let temp = tempdir().unwrap();
    let client = ModelClient::new(EndpointConfig {
        token: None,
        base_url: format!("http://{address}"),
        messages_path: "/v1/messages".into(),
        api_format: ApiFormat::Messages,
        stream: true,
        chat_tokens_field: open_agent_harness::protocol::ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
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
                "prompt":"write the context file, then wait",
                "description":"hot refresh interruption",
                "runInBackground":true,
                "timeoutMs": if explicit_stop { 60_000 } else { 1_000 }
            }),
        )
        .await;
    assert!(!started.is_error, "{}", started.content);
    let agent_id = started
        .content
        .lines()
        .find_map(|line| line.strip_prefix("agent_id="))
        .unwrap()
        .to_owned();
    tokio::time::timeout(Duration::from_secs(3), blocked_rx)
        .await
        .expect("subagent did not reach its blocked model round")
        .unwrap();

    let interrupted = if explicit_stop {
        registry
            .execute(&tools_context, "TaskStop", json!({"task_id":agent_id}))
            .await
    } else {
        registry
            .execute(
                &tools_context,
                "TaskOutput",
                json!({"task_id":agent_id,"block":true,"timeout":5_000}),
            )
            .await
    };
    assert!(!interrupted.content.is_empty());
    assert!(
        !temp.path().join("AGENTS.md").exists(),
        "interrupted subagent left a no-persistence context edit behind"
    );
    release_tx.send(()).unwrap();

    let recovered = registry
        .execute(
            &tools_context,
            "Agent",
            json!({
                "prompt":"write the context file and complete",
                "description":"hot refresh recovery",
                "timeoutMs":5_000
            }),
        )
        .await;
    assert!(!recovered.is_error, "{}", recovered.content);
    assert!(recovered.content.contains("recovery complete"));
    assert_eq!(
        std::fs::read_to_string(temp.path().join("AGENTS.md")).unwrap(),
        "completed rule"
    );

    engine.shutdown().await;
    server.join().unwrap();
}

fn agent_tool_stream() -> String {
    agent_tool_stream_for("parent-tool", "agent-1", "inspect independently")
}

fn agent_tool_stream_for(message_id: &str, tool_id: &str, prompt: &str) -> String {
    let input = serde_json::to_string(&json!({
        "prompt": prompt,
        "description": prompt,
        "timeoutMs": 3000
    }))
    .unwrap();
    [
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":message_id,"content":[],"usage":{}
        }}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":tool_id,"name":"Agent","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":input}}),
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
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":id,"content":[],
            "usage":{"input_tokens":1,"output_tokens":0}
        }}),
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

fn write_agents_stream(content: &str, id: &str) -> String {
    let input = serde_json::to_string(&json!({
        "file_path":"AGENTS.md",
        "content":content
    }))
    .unwrap();
    [
        json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":id,"content":[],"usage":{}
        }}),
        json!({"type":"content_block_start","index":0,"content_block":{
            "type":"tool_use","id":format!("{id}-tool"),"name":"Write","input":{}
        }}),
        json!({"type":"content_block_delta","index":0,"delta":{
            "type":"input_json_delta","partial_json":input
        }}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}),
        json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(sse_event)
    .collect()
}

fn write_sse_response(stream: &mut std::net::TcpStream, response: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response.len(),
        response
    )
    .unwrap();
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

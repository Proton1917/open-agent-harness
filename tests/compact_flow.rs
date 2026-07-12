use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use open_agent_harness::{
    api::ModelClient,
    compact::CompactConfig,
    config::EndpointConfig,
    permissions::{PermissionManager, PermissionMode},
    query::{QueryEngine, QueryOptions},
    tools::{ToolContext, ToolRegistry},
    types::Message,
};
use serde_json::Value;
use tempfile::tempdir;

#[tokio::test]
async fn compact_replaces_history_with_formatted_continuation() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request: Value = serde_json::from_slice(&read_http_body(&mut stream)).unwrap();
        assert_eq!(request["tools"], serde_json::json!([]));
        assert!(request.to_string().contains("Context for Continuing Work"));

        let body = summary_stream();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(body.as_bytes()).unwrap();
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
            max_tokens: 4096,
            system: "test system".into(),
            messages: vec![
                Message::user_text("fix it"),
                Message::assistant(vec![serde_json::json!({"type":"text","text":"working"})]),
            ],
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    );

    let stats = engine.compact(Some("retain exact commands")).await.unwrap();
    server.join().unwrap();
    assert_eq!(stats.messages_before, 2);
    assert_eq!(stats.messages_after, 1);
    assert_eq!(engine.compaction_count, 1);
    let summary = engine.messages[0].content.as_str().unwrap();
    assert!(summary.contains("Summary:\nCurrent work preserved."));
    assert!(!summary.contains("drafting notes"));
    assert!(summary.contains("Continue directly"));
}

#[tokio::test]
async fn query_auto_compacts_before_normal_model_round() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for (index, body) in [summary_stream(), final_stream()].into_iter().enumerate() {
            let (mut stream, _) = listener.accept().unwrap();
            let request: Value = serde_json::from_slice(&read_http_body(&mut stream)).unwrap();
            if index == 0 {
                assert_eq!(request["tools"], serde_json::json!([]));
            } else {
                assert!(request.to_string().contains("Current work preserved"));
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(body.as_bytes()).unwrap();
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
            max_tokens: 1000,
            system: "test system".into(),
            messages: vec![
                Message::user_text("x".repeat(30_000)),
                Message::assistant(vec![serde_json::json!({"type":"text","text":"ack"})]),
            ],
            debug: false,
            text_delta_sink: None,
            compact_config: Some(CompactConfig {
                enabled: true,
                auto_enabled: true,
                context_window: 20_000,
                max_output_tokens: 1_000,
            }),
        },
    );

    let result = engine.run_turn("continue".into()).await.unwrap();
    server.join().unwrap();
    assert!(result.compacted);
    assert_eq!(result.text, "continued");
    assert_eq!(engine.compaction_count, 1);
}

fn summary_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"msg_summary","usage":{"input_tokens":30,"output_tokens":0}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"<analysis>drafting notes</analysis>"}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"<summary>Current work preserved.</summary>"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":8}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(|value| format!("event: {}\ndata: {}\n\n", value["type"].as_str().unwrap(), value))
    .collect()
}

fn final_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"msg_final","usage":{"input_tokens":12,"output_tokens":0}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"continued"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(|value| format!("event: {}\ndata: {}\n\n", value["type"].as_str().unwrap(), value))
    .collect()
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

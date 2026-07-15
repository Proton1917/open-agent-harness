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
    protocol::ApiFormat,
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
        api_format: ApiFormat::Messages,
        stream: true,
        chat_tokens_field: open_agent_harness::protocol::ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
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
async fn compact_from_preserves_the_selected_prefix_and_summarizes_only_the_suffix() {
    const PREFIX: &str = "prefix-must-remain-byte-for-byte";
    const SELECTED: &str = "selected-suffix-only";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request: Value = serde_json::from_slice(&read_http_body(&mut stream)).unwrap();
        let serialized = request.to_string();
        assert!(!serialized.contains(PREFIX));
        assert!(serialized.contains(SELECTED));
        write_sse_response(&mut stream, &summary_stream());
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
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    let prefix = vec![
        Message::user_text(PREFIX),
        Message::assistant(vec![
            serde_json::json!({"type":"text","text":"prefix reply"}),
        ]),
    ];
    let original = [
        prefix.clone(),
        vec![
            Message::user_text(SELECTED),
            Message::assistant(vec![
                serde_json::json!({"type":"text","text":"suffix reply"}),
            ]),
        ],
    ]
    .concat();
    let mut engine = QueryEngine::new(
        client,
        ToolRegistry::default(),
        context,
        QueryOptions {
            model: "test-model".into(),
            max_tokens: 4096,
            system: "test system".into(),
            messages: original.clone(),
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    );

    let stats = engine.compact_from(2, None).await.unwrap();
    server.join().unwrap();
    assert_eq!(stats.messages_before, 4);
    assert_eq!(stats.messages_after, 3);
    assert_eq!(engine.messages[..2], prefix);
    assert!(
        engine.messages[2]
            .content
            .as_str()
            .unwrap()
            .contains("Current work preserved")
    );

    let before_invalid = engine.messages.clone();
    assert!(
        engine
            .compact_from(before_invalid.len(), None)
            .await
            .is_err()
    );
    assert_eq!(engine.messages, before_invalid);
}

#[tokio::test]
async fn query_auto_compacts_before_normal_model_round() {
    const CURRENT_PROMPT: &str = "current-prompt-must-remain-verbatim";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for (index, body) in [summary_stream(), final_stream()].into_iter().enumerate() {
            let (mut stream, _) = listener.accept().unwrap();
            let request: Value = serde_json::from_slice(&read_http_body(&mut stream)).unwrap();
            if index == 0 {
                assert_eq!(request["tools"], serde_json::json!([]));
                assert!(!request.to_string().contains(CURRENT_PROMPT));
            } else {
                assert!(request.to_string().contains("Current work preserved"));
                assert!(request.to_string().contains(CURRENT_PROMPT));
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
        api_format: ApiFormat::Messages,
        stream: true,
        chat_tokens_field: open_agent_harness::protocol::ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
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

    let result = engine.run_turn(CURRENT_PROMPT.into()).await.unwrap();
    server.join().unwrap();
    assert!(result.compacted);
    assert_eq!(result.text, "continued");
    assert_eq!(engine.compaction_count, 1);
}

#[tokio::test]
async fn endpoint_size_rejection_compacts_once_and_truncates_a_rejected_summary() {
    const CURRENT_PROMPT: &str = "reactive-current-prompt";
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for index in 0..4 {
            let (mut stream, _) = listener.accept().unwrap();
            let request: Value = serde_json::from_slice(&read_http_body(&mut stream)).unwrap();
            let serialized = request.to_string();
            match index {
                0 => {
                    assert!(serialized.contains(CURRENT_PROMPT));
                    assert!(!serialized.contains("Context for Continuing Work"));
                    write_json_error(
                        &mut stream,
                        400,
                        r#"{"error":{"type":"context_length_exceeded","message":"limit"}}"#,
                    );
                }
                1 => {
                    assert!(serialized.contains("Context for Continuing Work"));
                    assert!(!serialized.contains("earlier conversation truncated"));
                    write_json_error(
                        &mut stream,
                        413,
                        r#"{"error":{"message":"request too large"}}"#,
                    );
                }
                2 => {
                    assert!(serialized.contains("Context for Continuing Work"));
                    assert!(serialized.contains("earlier conversation truncated"));
                    write_sse_response(&mut stream, &summary_stream());
                }
                3 => {
                    assert!(serialized.contains("Current work preserved"));
                    assert!(serialized.contains(CURRENT_PROMPT));
                    write_sse_response(&mut stream, &final_stream());
                }
                _ => unreachable!(),
            }
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
                Message::user_text("old request one"),
                Message::assistant(vec![
                    serde_json::json!({"type":"text","text":"old answer one"}),
                ]),
                Message::user_text("old request two"),
                Message::assistant(vec![
                    serde_json::json!({"type":"text","text":"old answer two"}),
                ]),
            ],
            debug: false,
            text_delta_sink: None,
            compact_config: Some(CompactConfig {
                enabled: true,
                auto_enabled: false,
                context_window: 20_000,
                max_output_tokens: 1_000,
            }),
        },
    );

    let result = engine.run_turn(CURRENT_PROMPT.into()).await.unwrap();
    server.join().unwrap();
    assert!(result.compacted);
    assert_eq!(result.text, "continued");
    assert_eq!(engine.compaction_count, 1);
}

#[tokio::test]
async fn repeated_size_rejection_does_not_spiral_and_restores_turn_state() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for index in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            let request: Value = serde_json::from_slice(&read_http_body(&mut stream)).unwrap();
            if index == 1 {
                assert!(request.to_string().contains("Context for Continuing Work"));
                write_sse_response(&mut stream, &summary_stream());
            } else {
                write_json_error(
                    &mut stream,
                    413,
                    r#"{"error":{"message":"payload too large"}}"#,
                );
            }
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
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    let original = vec![
        Message::user_text("old request"),
        Message::assistant(vec![serde_json::json!({"type":"text","text":"old answer"})]),
    ];
    let mut engine = QueryEngine::new(
        client,
        ToolRegistry::default(),
        context,
        QueryOptions {
            model: "test-model".into(),
            max_tokens: 1000,
            system: "test system".into(),
            messages: original.clone(),
            debug: false,
            text_delta_sink: None,
            compact_config: Some(CompactConfig {
                enabled: true,
                auto_enabled: false,
                context_window: 20_000,
                max_output_tokens: 1_000,
            }),
        },
    );

    let error = engine.run_turn("still too large".into()).await.unwrap_err();
    server.join().unwrap();
    assert!(error.to_string().contains("Model endpoint 413"));
    assert_eq!(engine.messages, original);
    assert_eq!(engine.compaction_count, 0);
}

fn write_json_error(stream: &mut std::net::TcpStream, status: u16, body: &str) {
    write!(
        stream,
        "HTTP/1.1 {status} Error\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}

fn write_sse_response(stream: &mut std::net::TcpStream, body: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
}

fn summary_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":"msg_summary","content":[],
            "usage":{"input_tokens":30,"output_tokens":0}
        }}),
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
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":"msg_final","content":[],
            "usage":{"input_tokens":12,"output_tokens":0}
        }}),
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

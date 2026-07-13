use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use open_agent_harness::{
    api::ModelClient,
    config::EndpointConfig,
    permissions::{PermissionManager, PermissionMode},
    protocol::{ApiFormat, ChatTokensField},
    query::{QueryEngine, QueryOptions},
    tools::{ToolContext, ToolRegistry},
};
use tempfile::tempdir;

#[tokio::test]
async fn duplicate_tool_ids_fail_before_any_tool_executes() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_http_body(&mut stream);
        let body = serde_json::json!({
            "type": "message",
            "id": "duplicate-tool-call-response",
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "duplicate",
                    "name": "Write",
                    "input": {"file_path": "must-not-exist.txt", "content": "executed"}
                },
                {
                    "type": "tool_use",
                    "id": "duplicate",
                    "name": "Write",
                    "input": {"file_path": "also-must-not-exist.txt", "content": "executed"}
                }
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 1, "output_tokens": 1}
        })
        .to_string();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
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
        stream: false,
        chat_tokens_field: ChatTokensField::MaxCompletionTokens,
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
            max_tokens: 1_024,
            system: "test system".into(),
            messages: Vec::new(),
            debug: false,
            text_delta_sink: None,
            compact_config: None,
        },
    );

    let error = engine
        .run_turn("write the marker".into())
        .await
        .unwrap_err();
    server.join().unwrap();

    assert_eq!(error.to_string(), "同一响应包含重复 tool_use id");
    assert!(!temp.path().join("must-not-exist.txt").exists());
    assert!(!temp.path().join("also-must-not-exist.txt").exists());
    assert!(engine.messages.is_empty());
}

fn read_http_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4_096];
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

use std::{
    collections::BTreeSet,
    fmt::Write as _,
    io::{Read, Write},
    net::TcpListener,
    thread,
};

use open_agent_harness::{
    api::ModelClient, config::EndpointConfig, protocol::ApiFormat, types::Message,
};
use serde_json::{Value, json};

#[tokio::test]
async fn model_request_contains_only_the_documented_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        let body = json!({
            "type":"message",
            "id":"response-1",
            "role":"assistant",
            "content":[{"type":"text","text":"ok"}],
            "stop_reason":"end_turn"
        })
        .to_string();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
        request
    });

    let client = ModelClient::new(endpoint(address, Some("test-token"))).unwrap();
    client
        .messages(
            "test-model",
            1024,
            "test system",
            &[Message::user_text("hello")],
            &[json!({"name":"Read","description":"read","input_schema":{"type":"object"}})],
            None,
        )
        .await
        .unwrap();
    let request = server.join().unwrap();
    let header_text = request.headers.to_ascii_lowercase();
    assert!(header_text.contains("authorization: bearer test-token"));
    assert!(!header_text.contains("x-api-key"));
    let first_removed_name = ["clau", "de"].concat();
    let second_removed_name = ["anth", "ropic"].concat();
    assert!(!header_text.contains(&first_removed_name));
    assert!(!header_text.contains(&second_removed_name));

    let body: Value = serde_json::from_slice(&request.body).unwrap();
    let keys = body
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        keys,
        [
            "max_tokens",
            "messages",
            "model",
            "stream",
            "system",
            "tools"
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
    let serialized = body.to_string().to_ascii_lowercase();
    for hidden_field in [
        "email",
        "device_id",
        "machine_id",
        "account_uuid",
        "organization_uuid",
        "telemetry",
    ] {
        assert!(!serialized.contains(hidden_field));
    }
}

#[tokio::test]
async fn model_endpoint_does_not_follow_redirects() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        write!(
            stream,
            "HTTP/1.1 307 Temporary Redirect\r\nlocation: http://127.0.0.1:9/elsewhere\r\ncontent-length: 8\r\nconnection: close\r\n\r\nredirect"
        )
        .unwrap();
    });
    let client = ModelClient::new(endpoint(address, Some("redirect-secret"))).unwrap();
    let error = match client
        .messages(
            "test",
            16,
            "system",
            &[Message::user_text("hello")],
            &[],
            None,
        )
        .await
    {
        Ok(_) => panic!("redirect unexpectedly followed"),
        Err(error) => error,
    };
    server.join().unwrap();
    assert!(error.to_string().contains("307"));
}

#[tokio::test]
async fn oversized_response_is_rejected_from_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 16777217\r\nconnection: close\r\n\r\n"
        )
        .unwrap();
    });
    let client = ModelClient::new(endpoint(address, None)).unwrap();
    let error = match client
        .messages(
            "test",
            16,
            "system",
            &[Message::user_text("hello")],
            &[],
            None,
        )
        .await
    {
        Ok(_) => panic!("oversized response unexpectedly accepted"),
        Err(error) => error,
    };
    server.join().unwrap();
    assert!(error.to_string().contains("16777216"));
}

#[tokio::test]
async fn interrupted_tool_input_json_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        let body = [
            json!({"type":"message_start","message":{
                "type":"message","role":"assistant","id":"broken","content":[],"usage":{}
            }}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool-1","name":"Read","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"file_path\":"}}),
            json!({"type":"message_stop"}),
        ]
        .into_iter()
        .fold(String::new(), |mut body, event| {
            write!(body, "data: {event}\n\n").expect("writing to a String cannot fail");
            body
        });
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    });
    let client = ModelClient::new(endpoint(address, None)).unwrap();
    let error = match client
        .messages(
            "test",
            16,
            "system",
            &[Message::user_text("hello")],
            &[],
            None,
        )
        .await
    {
        Ok(_) => panic!("interrupted tool input unexpectedly accepted"),
        Err(error) => error,
    };
    server.join().unwrap();
    assert!(
        error.to_string().contains("content block") || error.to_string().contains("JSON"),
        "unexpected error: {error:#}"
    );
}

struct CapturedRequest {
    headers: String,
    body: Vec<u8>,
}

fn read_request(stream: &mut std::net::TcpStream) -> CapturedRequest {
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
        .unwrap_or(0);
    while buffer.len() < header_end + content_length {
        let count = stream.read(&mut chunk).unwrap();
        buffer.extend_from_slice(&chunk[..count]);
    }
    CapturedRequest {
        headers,
        body: buffer[header_end..header_end + content_length].to_vec(),
    }
}

fn endpoint(address: std::net::SocketAddr, token: Option<&str>) -> EndpointConfig {
    EndpointConfig {
        token: token.map(str::to_owned),
        base_url: format!("http://{address}"),
        messages_path: "/v1/messages".into(),
        api_format: ApiFormat::Messages,
        stream: true,
        chat_tokens_field: open_agent_harness::protocol::ChatTokensField::MaxCompletionTokens,
        include_stream_usage: true,
        allow_env_proxy: false,
    }
}

use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    thread,
};

use serde_json::Value;

#[test]
fn print_text_json_and_stream_json_contracts_are_stable() {
    let text = run_cli("text", json_response("plain response"));
    assert_eq!(text.trim(), "plain response");

    let json = run_cli("json", json_response("json response"));
    let value: Value = serde_json::from_str(json.trim()).unwrap();
    assert_eq!(value["type"], "result");
    assert_eq!(value["subtype"], "success");
    assert_eq!(value["result"], "json response");

    let stream = run_cli("stream-json", sse_response("stream response"));
    let lines = stream
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0]["type"], "content_block_delta");
    assert_eq!(lines[0]["delta"]["text"], "stream response");
    assert_eq!(lines[1]["type"], "assistant");
    assert_eq!(lines[2]["type"], "result");
    assert_eq!(lines[2]["result"], "stream response");
}

fn run_cli(format: &str, response: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let content_type = if response.starts_with("data:") {
            "text/event-stream"
        } else {
            "application/json"
        };
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
    });

    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--output-format",
            format,
            "verify output",
        ])
        .env("HARNESS_BASE_URL", format!("http://{address}"))
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .output()
        .unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn json_response(text: &str) -> String {
    serde_json::json!({
        "type": "message",
        "id": "response-output",
        "role": "assistant",
        "content": [{"type": "text", "text": text}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 1, "output_tokens": 1}
    })
    .to_string()
}

fn sse_response(text: &str) -> String {
    [
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":"stream-output","content":[],
            "usage":{"input_tokens":1,"output_tokens":0}
        }}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(|value| format!("data: {value}\n\n"))
    .collect()
}

fn read_request(stream: &mut std::net::TcpStream) {
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
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while buffer.len() < header_end + length {
        let count = stream.read(&mut chunk).unwrap();
        assert!(count > 0);
        buffer.extend_from_slice(&chunk[..count]);
    }
}

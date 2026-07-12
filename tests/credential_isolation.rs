use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    sync::{Arc, Mutex},
    thread,
};

use serde_json::Value;

#[test]
fn endpoint_credential_is_not_inherited_by_shell_tools() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let bodies = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let captured = Arc::clone(&bodies);
    let server = thread::spawn(move || {
        for response in [tool_stream(), final_stream()] {
            let (mut stream, _) = listener.accept().unwrap();
            captured.lock().unwrap().push(read_body(&mut stream));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        }
    });

    let secret = "endpoint-secret-must-not-reach-tools";
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--dangerously-skip-permissions",
            "--no-session-persistence",
            "credential isolation",
        ])
        .env("HARNESS_BASE_URL", format!("http://{address}"))
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env("HARNESS_API_KEY", secret)
        .env_remove("HARNESS_AUTH_TOKEN")
        .output()
        .unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("credential isolated"));
    let requests = bodies.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let cwd = std::env::current_dir().unwrap().display().to_string();
    for body in requests.iter() {
        let body = String::from_utf8_lossy(body);
        assert!(!body.contains(secret));
        assert!(!body.contains(&cwd));
    }
    let second: Value = serde_json::from_slice(&requests[1]).unwrap();
    assert!(second.to_string().contains("credential-absent"));
}

fn tool_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"msg-tool","usage":{}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool-1","name":"Bash","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"if printenv HARNESS_API_KEY >/dev/null; then printf credential-leaked; else printf credential-absent; fi\"}"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":1}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(sse)
    .collect()
}

fn final_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{"id":"msg-final","usage":{}}}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"credential isolated"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(sse)
    .collect()
}

fn sse(value: Value) -> String {
    format!("data: {value}\n\n")
}

fn read_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
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
        buffer.extend_from_slice(&chunk[..count]);
    }
    buffer[header_end..header_end + length].to_vec()
}

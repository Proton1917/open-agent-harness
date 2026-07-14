use std::{
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Stdio},
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
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0]["type"], "system");
    assert_eq!(lines[0]["subtype"], "init");
    assert_eq!(lines[1]["type"], "content_block_delta");
    assert_eq!(lines[1]["delta"]["text"], "stream response");
    assert_eq!(lines[2]["type"], "assistant");
    assert_eq!(lines[3]["type"], "result");
    assert_eq!(lines[3]["result"], "stream response");
}

#[test]
fn validated_structured_output_is_not_mutated_by_transport_sanitizing() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for response in [
            serde_json::json!({
                "type":"message", "id":"structured", "role":"assistant",
                "content":[{"type":"tool_use", "id":"structured-1",
                    "name":"StructuredOutput",
                    "input":{"token":"abc", "filePath":"/tmp/schema-path"}}],
                "stop_reason":"tool_use",
                "usage":{"input_tokens":1,"output_tokens":1}
            })
            .to_string(),
            json_response("done"),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        }
    });
    let schema = serde_json::json!({
        "type":"object",
        "properties":{
            "token":{"const":"abc"},
            "filePath":{"const":"/tmp/schema-path"}
        },
        "required":["token","filePath"],
        "additionalProperties":false
    })
    .to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--output-format",
            "json",
            "--json-schema",
            &schema,
            "return structured data",
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
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["structured_output"]["token"], "abc");
    assert_eq!(result["structured_output"]["filePath"], "/tmp/schema-path");
}

#[test]
fn stream_json_init_precedes_buffered_session_start_hook_events() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let response = json_response("hook ordering");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
    });
    #[cfg(windows)]
    let (hook_command, hook_args) = {
        let command = std::path::PathBuf::from(
            std::env::var_os("SystemRoot").expect("SystemRoot must be defined on Windows"),
        )
        .join("System32")
        .join("cmd.exe")
        .display()
        .to_string();
        (command, vec!["/C", "exit 0"])
    };
    #[cfg(not(windows))]
    let (hook_command, hook_args) = ("/bin/sh".to_owned(), vec!["-c", "exit 0"]);
    let settings = serde_json::json!({
        "hooks": {
            "SessionStart": [{
                "hooks": [{
                    "type": "command",
                    "command": hook_command,
                    "args": hook_args
                }]
            }]
        }
    })
    .to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--output-format",
            "stream-json",
            "--include-hook-events",
            "--settings",
            &settings,
            "verify hook event ordering",
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
    let lines = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lines[0]["type"], "system");
    assert_eq!(lines[0]["subtype"], "init");
    assert!(
        lines
            .iter()
            .skip(1)
            .any(|line| line["type"] == "hook_started")
    );
    assert!(
        lines
            .iter()
            .skip(1)
            .any(|line| line["type"] == "hook_response")
    );
}

#[test]
fn session_start_failure_still_runs_session_end_cleanup() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace
        .path()
        .join("session-end-after-startup-failure.txt");
    let settings = serde_json::json!({
        "hooks": {
            "SessionStart": [{"hooks": [failing_hook()]}],
            "SessionEnd": [{"hooks": [marker_hook(&marker)]}]
        }
    })
    .to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--settings",
            &settings,
            "fail during session start",
        ])
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", "http://127.0.0.1:9")
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        marker.exists(),
        "SessionEnd hook was skipped: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn engine_setup_failure_still_runs_session_end_cleanup() {
    let workspace = tempfile::tempdir().unwrap();
    let marker = workspace
        .path()
        .join("session-end-after-engine-failure.txt");
    let settings = serde_json::json!({
        "hooks": {"SessionEnd": [{"hooks": [marker_hook(&marker)]}]}
    })
    .to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--settings",
            &settings,
            "--max-turns",
            "0",
            "fail after engine construction",
        ])
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", "http://127.0.0.1:9")
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("max turns"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        marker.exists(),
        "SessionEnd hook was skipped: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn trusted_plugin_output_style_is_selected_injected_and_advertised() {
    let workspace = tempfile::tempdir().unwrap();
    let plugin = workspace.path().join("style-plugin");
    std::fs::create_dir_all(plugin.join("output-styles")).unwrap();
    std::fs::write(plugin.join("plugin.json"), r#"{"name":"style"}"#).unwrap();
    std::fs::write(
        plugin.join("output-styles/brief.md"),
        "---\nname: brief\n---\nBRIEF_STYLE_MARKER",
    )
    .unwrap();
    std::fs::write(
        plugin.join("output-styles/verbose.md"),
        "---\nname: verbose\nforce-for-plugin: true\n---\nVERBOSE_STYLE_MARKER",
    )
    .unwrap();
    let settings = serde_json::json!({
        "plugins":{"directories":[plugin]},
        "outputStyle":"style:brief"
    })
    .to_string();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_tx, request_rx) = std::sync::mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let body = read_request_body(&mut stream);
        request_tx
            .send(serde_json::from_slice::<Value>(&body).unwrap())
            .unwrap();
        let response = sse_response("styled response");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
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
            "stream-json",
            "--settings",
            &settings,
            "--output-style",
            "style:verbose",
            "verify output style",
        ])
        .current_dir(workspace.path())
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
    let lines = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lines[0]["output_style"], "style:verbose");
    assert_eq!(
        lines[0]["available_output_styles"],
        serde_json::json!(["default", "style:brief", "style:verbose"])
    );
    let request = request_rx.recv().unwrap();
    let request = serde_json::to_string(&request).unwrap();
    assert!(request.contains("<output-style name=\\\"style:verbose\\\">"));
    assert!(request.contains("VERBOSE_STYLE_MARKER"));
    assert!(!request.contains("BRIEF_STYLE_MARKER"));

    let unknown = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--settings",
            &settings,
            "--output-style",
            "style:missing",
            "reject missing style",
        ])
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", format!("http://{address}"))
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(!unknown.status.success());
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("未知 output style: style:missing"));
}

#[test]
fn trusted_turn_end_memory_extraction_runs_after_a_completed_print_turn() {
    let workspace = tempfile::tempdir().unwrap();
    let memory_directory = workspace.path().join("memory-store");
    let settings = serde_json::json!({
        "memory": {
            "enabled": true,
            "autoExtract": true,
            "path": memory_directory,
        }
    })
    .to_string();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (requests_tx, requests_rx) = std::sync::mpsc::sync_channel(2);
    let server = thread::spawn(move || {
        let responses = [
            json_response("I will verify the real command before reporting completion."),
            serde_json::json!({
                "type":"message", "id":"memory-extraction", "role":"assistant",
                "content":[{
                    "type":"tool_use", "id":"memory-candidates-1", "name":"MemoryCandidates",
                    "input":{"entries":[{
                        "title":"Verification preference",
                        "tags":["workflow", "testing"],
                        "content":"Run the real verification command before reporting completion."
                    }]}
                }],
                "stop_reason":"tool_use",
                "usage":{"input_tokens":1,"output_tokens":1}
            })
            .to_string(),
        ];
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request_body(&mut stream);
            requests_tx
                .send(serde_json::from_slice::<Value>(&request).unwrap())
                .unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        }
    });

    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--settings",
            &settings,
            "remember that real verification is required",
        ])
        .current_dir(workspace.path())
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
    let primary = requests_rx.recv().unwrap();
    let extraction = requests_rx.recv().unwrap();
    assert!(
        primary["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "Memory")
    );
    assert_eq!(extraction["tools"].as_array().unwrap().len(), 1);
    assert_eq!(extraction["tools"][0]["name"], "MemoryCandidates");
    let persisted = std::fs::read_to_string(memory_directory.join("MEMORY.md")).unwrap();
    assert!(persisted.contains("## Verification preference"));
    assert!(persisted.contains("Run the real verification command"));
}

#[test]
fn stream_json_rewind_dry_run_does_not_modify_files() {
    let workspace = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (turn_done_tx, turn_done_rx) = std::sync::mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let mut served = 0;
        for response in [
            serde_json::json!({
                "type":"message", "id":"write", "role":"assistant",
                "content":[{"type":"tool_use", "id":"write-1", "name":"Write",
                    "input":{"file_path":"dry-run.txt", "content":"changed"}}],
                "stop_reason":"tool_use",
                "usage":{"input_tokens":1,"output_tokens":1}
            })
            .to_string(),
            json_response("done"),
        ] {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= deadline {
                            return served;
                        }
                        thread::sleep(std::time::Duration::from_millis(10));
                    }
                    Err(error) => panic!("mock server accept failed: {error}"),
                }
            };
            read_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
            served += 1;
            if served == 2 {
                let _ = turn_done_tx.send(());
            }
        }
        served
    });
    let user_id = uuid::Uuid::new_v4();
    let mut child = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--dangerously-skip-permissions",
        ])
        .current_dir(workspace.path())
        .env("HOME", home.path())
        .env("HARNESS_BASE_URL", format!("http://{address}"))
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"user", "uuid":user_id,
            "message":{"role":"user", "content":"write the file"}
        })
    )
    .unwrap();
    stdin.flush().unwrap();
    let written = workspace.path().join("dry-run.txt");
    turn_done_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("model turn did not receive its terminal response");
    let persisted_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !tree_contains(home.path(), "done") {
        assert!(
            std::time::Instant::now() < persisted_deadline,
            "model turn did not finish persisting"
        );
        thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(written.exists(), "model turn did not create dry-run.txt");
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"control_request", "request_id":"dry-run-1",
            "request":{"subtype":"rewind_files", "user_message_id":user_id, "dry_run":true}
        })
    )
    .unwrap();
    stdin.flush().unwrap();
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        server.join().unwrap(),
        2,
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read_to_string(written).unwrap(), "changed");
    let lines = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let response = lines
        .iter()
        .find(|line| {
            line["type"] == "control_response" && line["response"]["request_id"] == "dry-run-1"
        })
        .unwrap();
    assert_eq!(response["response"]["subtype"], "success");
    assert_eq!(response["response"]["response"]["canRewind"], true);
    assert_eq!(response["response"]["response"]["deletions"], 1);
}

#[test]
fn stream_json_control_accepts_dont_ask_permission_mode() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
        ])
        .env("HARNESS_BASE_URL", "http://127.0.0.1:9")
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"control_request",
            "request_id":"dont-ask-1",
            "request":{"subtype":"set_permission_mode", "mode":"dontAsk"}
        })
    )
    .unwrap();
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let response = lines
        .iter()
        .find(|line| {
            line["type"] == "control_response" && line["response"]["request_id"] == "dont-ask-1"
        })
        .unwrap();
    assert_eq!(response["response"]["subtype"], "success");
    assert_eq!(response["response"]["response"]["mode"], "dontAsk");
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
    let _ = read_request_body(stream);
}

fn read_request_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
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
    buffer[header_end..header_end + length].to_vec()
}

fn tree_contains(root: &std::path::Path, needle: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(root) else {
        return false;
    };
    entries.filter_map(Result::ok).any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            tree_contains(&path, needle)
        } else {
            std::fs::read_to_string(path)
                .map(|content| content.contains(needle))
                .unwrap_or(false)
        }
    })
}

#[cfg(not(windows))]
fn marker_hook(path: &std::path::Path) -> Value {
    serde_json::json!({
        "type":"command",
        "command":"/bin/sh",
        "args":["-c", "printf cleanup > \"$1\"", "hook", path]
    })
}

#[cfg(windows)]
fn marker_hook(path: &std::path::Path) -> Value {
    let command = std::path::PathBuf::from(
        std::env::var_os("SystemRoot").expect("SystemRoot must be defined on Windows"),
    )
    .join("System32")
    .join("cmd.exe");
    serde_json::json!({
        "type":"command",
        "command":command,
        "args":["/C", format!("echo cleanup>\"{}\"", path.display())]
    })
}

#[cfg(not(windows))]
fn failing_hook() -> Value {
    serde_json::json!({
        "type":"command",
        "command":"/bin/sh",
        "args":["-c", "exit 2"]
    })
}

#[cfg(windows)]
fn failing_hook() -> Value {
    let command = std::path::PathBuf::from(
        std::env::var_os("SystemRoot").expect("SystemRoot must be defined on Windows"),
    )
    .join("System32")
    .join("cmd.exe");
    serde_json::json!({
        "type":"command",
        "command":command,
        "args":["/C", "exit /B 2"]
    })
}

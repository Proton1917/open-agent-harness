use std::{
    fmt::Write as _,
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Stdio},
    thread,
};

#[cfg(unix)]
use std::{
    io::{BufRead, BufReader},
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant},
};

use serde_json::Value;

const SESSION_END_MARKER: &str = ".session-end-cleanup-marker";

#[test]
fn shell_completion_supports_stdout_and_create_only_output() {
    let stdout = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args(["completion", "bash"])
        .output()
        .unwrap();
    assert!(
        stdout.status.success(),
        "{}",
        String::from_utf8_lossy(&stdout.stderr)
    );
    assert!(
        String::from_utf8(stdout.stdout)
            .unwrap()
            .contains("open-agent-harness")
    );

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("completion.zsh");
    let first = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args(["completion", "zsh", "--output"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let original = std::fs::read(&path).unwrap();
    assert!(!original.is_empty());
    let second = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args(["completion", "zsh", "--output"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

#[test]
fn invalid_network_trust_fails_before_connecting() {
    let workspace = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args(["--print", "--bare", "--no-session-persistence", "hello"])
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", "http://127.0.0.1:9")
        .env("HARNESS_CA_CERT_FILE", "relative-ca.pem")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must be an absolute path"));
}

#[test]
fn valid_network_trust_material_reaches_the_model_client() {
    let workspace = tempfile::tempdir().unwrap();
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let ca = workspace.path().join("ca.pem");
    let client = workspace.path().join("client.pem");
    let key = workspace.path().join("client.key");
    std::fs::write(&ca, cert.pem()).unwrap();
    std::fs::write(&client, cert.pem()).unwrap();
    std::fs::write(&key, key_pair.serialize_pem()).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let body = serde_json::json!({
            "id":"network-trust",
            "type":"message",
            "role":"assistant",
            "model":"test-model",
            "content":[{"type":"text","text":"trusted"}],
            "stop_reason":"end_turn",
            "usage":{"input_tokens":1,"output_tokens":1}
        })
        .to_string();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    });
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--output-format",
            "json",
            "hello",
        ])
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", format!("http://{address}"))
        .env("HARNESS_API_PATH", "/v1/messages")
        .env("HARNESS_STREAM", "false")
        .env("HARNESS_CA_CERT_FILE", &ca)
        .env("HARNESS_CLIENT_CERT_FILE", &client)
        .env("HARNESS_CLIENT_KEY_FILE", &key)
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
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["result"], "trusted");
}

#[cfg(unix)]
#[test]
fn stream_json_exposes_dynamic_commands_and_runtime_status_controls() {
    let workspace = tempfile::tempdir().unwrap();
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
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", "http://127.0.0.1:9")
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    for request in [
        serde_json::json!({
            "type":"control_request", "requestId":"init-camel",
            "request":{"subtype":"initialize"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"mcp-status",
            "request":{"subtype":"mcp_status"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"settings-status",
            "request":{"subtype":"get_settings"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"context-status",
            "request":{"subtype":"get_context_usage"}
        }),
    ] {
        writeln!(input, "{request}").unwrap();
    }
    drop(input);
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
    let init = lines
        .iter()
        .find(|line| line["type"] == "system" && line["subtype"] == "init")
        .unwrap();
    for expected in ["diff", "resume", "rewind", "status"] {
        assert!(
            init["commands"]
                .as_array()
                .unwrap()
                .iter()
                .any(|name| name == expected),
            "missing {expected}"
        );
    }
    assert!(
        init["command_descriptors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| {
                command["name"] == "mcp" && command["argumentHint"].as_str().is_some()
            })
    );
    assert_eq!(init["commandDescriptors"], init["command_descriptors"]);
    assert!(
        init["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "side_question_v1")
    );
    let response = |id: &str| {
        lines
            .iter()
            .find(|line| line["type"] == "control_response" && line["response"]["request_id"] == id)
            .unwrap()
    };
    assert_eq!(response("init-camel")["response"]["subtype"], "success");
    assert_eq!(
        response("mcp-status")["response"]["response"]["mcpServers"],
        serde_json::json!([])
    );
    assert_eq!(
        response("settings-status")["response"]["response"]["effective"]["memoryEnabled"],
        false
    );
    let context = &response("context-status")["response"]["response"];
    let categories = context["categories"].as_array().unwrap();
    assert!(categories.len() >= 2);
    assert!(
        categories
            .iter()
            .any(|category| category["name"] == "Tool definitions")
    );
    assert_eq!(
        categories
            .iter()
            .map(|category| category["tokens"].as_u64().unwrap())
            .sum::<u64>(),
        context["totalTokens"].as_u64().unwrap()
    );
    assert!(context["percentage"].is_number());
}

#[cfg(unix)]
#[test]
fn stream_json_side_question_runs_while_the_main_turn_is_active() {
    let workspace = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let captured = Arc::clone(&requests);
    let server = thread::spawn(move || {
        let (mut main_stream, _) = listener.accept().unwrap();
        captured
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&read_request_body(&mut main_stream)).unwrap());
        let main_worker = thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            let response = sse_response("MAIN_CONTROL_TURN_DONE");
            write!(
                main_stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        });

        let (mut side_stream, _) = listener.accept().unwrap();
        captured
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&read_request_body(&mut side_stream)).unwrap());
        thread::sleep(Duration::from_millis(500));
        let response = sse_response("SIDE_CONTROL_ANSWER");
        write!(
            side_stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
        main_worker.join().unwrap();
    });

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
        .current_dir(workspace.path())
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
    let stdout = child.stdout.take().unwrap();
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let stdout_reader = thread::spawn(move || {
        BufReader::new(stdout)
            .lines()
            .map(|line| {
                let line = line.unwrap();
                let _ = stdout_tx.send(line.clone());
                line
            })
            .collect::<Vec<_>>()
    });
    let user_id = uuid::Uuid::new_v4();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"user", "uuid":user_id,
            "message":{"role":"user", "content":"main control objective"}
        })
    )
    .unwrap();
    stdin.flush().unwrap();
    wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "command_lifecycle"
            && line["command_uuid"] == user_id.to_string()
            && line["state"] == "started"
    });
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"control_request", "request_id":"side-active",
            "request":{
                "subtype":"side_question",
                "question":"what is the active objective?"
            }
        })
    )
    .unwrap();
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"control_request", "request_id":"side-overlap",
            "request":{
                "subtype":"side_question",
                "question":"must not start a second request"
            }
        })
    )
    .unwrap();
    stdin.flush().unwrap();
    let overlap = wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "control_response" && line["response"]["request_id"] == "side-overlap"
    });
    assert_eq!(overlap["response"]["subtype"], "error");
    assert!(
        overlap["response"]["error"]
            .as_str()
            .unwrap()
            .contains("already running")
    );
    let side = wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "control_response" && line["response"]["request_id"] == "side-active"
    });
    assert_eq!(side["response"]["subtype"], "success");
    assert_eq!(
        side["response"]["response"]["response"],
        "SIDE_CONTROL_ANSWER"
    );
    wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "result" && line["subtype"] == "success"
    });
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    let stdout = stdout_reader.join().unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        stdout.join("\n"),
        String::from_utf8_lossy(&output.stderr)
    );
    server.join().unwrap();

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].to_string().contains("main control objective"));
    assert!(
        requests[1]
            .to_string()
            .contains("what is the active objective?")
    );
    assert_eq!(requests[1]["tools"], serde_json::json!([]));
}

#[cfg(unix)]
#[test]
fn stream_json_executes_advertised_local_slash_commands_without_model_requests() {
    let workspace = tempfile::tempdir().unwrap();
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
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", "http://127.0.0.1:9")
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let status_id = uuid::Uuid::new_v4();
    let clear_id = uuid::Uuid::new_v4();
    for (uuid, content) in [(status_id, "/status"), (clear_id, "/clear")] {
        writeln!(
            input,
            "{}",
            serde_json::json!({
                "type":"user",
                "uuid":uuid,
                "message":{"role":"user","content":content}
            })
        )
        .unwrap();
    }
    drop(input);
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
    let local_results = lines
        .iter()
        .filter(|line| line["type"] == "result" && line.get("command_result").is_some())
        .collect::<Vec<_>>();
    assert_eq!(local_results.len(), 2, "{lines:#?}");
    assert_eq!(local_results[0]["command_result"]["model"], "default");
    assert_eq!(local_results[1]["command_result"]["cleared"], true);
    assert!(lines.iter().all(|line| {
        line["subtype"] != "error_during_execution" && line["type"] != "assistant"
    }));
}

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

    let chat_stream = run_cli_for_api(
        "stream-json",
        chat_reasoning_sse_response("visible chat"),
        "/v1/chat/completions",
        "chat-completions",
    );
    assert!(!chat_stream.contains("private-raw-reasoning"));
    assert!(!chat_stream.contains("private-reasoning-details"));
    assert!(!chat_stream.contains("provider_state"));
    let lines = chat_stream
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let assistant = lines
        .iter()
        .find(|line| line["type"] == "assistant")
        .unwrap();
    assert_eq!(
        assistant["message"]["content"],
        serde_json::json!([{"type":"text","text":"visible chat"}])
    );
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
    let marker = workspace.path().join(SESSION_END_MARKER);
    let settings = serde_json::json!({
        "hooks": {
            "SessionStart": [{"hooks": [failing_hook()]}],
            "SessionEnd": [{"hooks": [marker_hook()]}]
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
    let marker = workspace.path().join(SESSION_END_MARKER);
    let settings = serde_json::json!({
        "hooks": {"SessionEnd": [{"hooks": [marker_hook()]}]}
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

#[cfg(unix)]
#[test]
fn stream_json_rewind_dry_run_previews_then_rewinds_files_and_conversation() {
    let workspace = tempfile::tempdir().unwrap();
    let session_state = tempfile::tempdir().unwrap();
    make_private_directory(workspace.path());
    make_private_directory(session_state.path());
    let overlapping = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args(["--print", "--bare", "--session-state-root"])
        .arg(workspace.path())
        .arg("state root must stay outside the workspace")
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", "http://127.0.0.1:9")
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .output()
        .unwrap();
    assert!(!overlapping.status.success());
    assert!(
        String::from_utf8_lossy(&overlapping.stderr).contains("不得与可信工作区重叠"),
        "{}",
        String::from_utf8_lossy(&overlapping.stderr)
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
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
                    Ok((stream, _)) => {
                        // On macOS an accepted socket can inherit O_NONBLOCK from its listener.
                        // Request parsing below is intentionally blocking and bounded by the
                        // surrounding test deadline.
                        stream.set_nonblocking(false).unwrap();
                        break stream;
                    }
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
        }
        served
    });
    let user_id = uuid::Uuid::new_v4();
    let mut command = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"));
    command
        .args([
            "--print",
            "--bare",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
        ])
        .arg("--session-state-root")
        .arg(session_state.path())
        .arg("--dangerously-skip-permissions")
        .current_dir(workspace.path())
        .env("HARNESS_BASE_URL", format!("http://{address}"))
        .env("HARNESS_MESSAGES_PATH", "/v1/messages")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let stdout_reader = thread::spawn(move || {
        BufReader::new(stdout)
            .lines()
            .map(|line| {
                let line = line.unwrap();
                let _ = stdout_tx.send(line.clone());
                line
            })
            .collect::<Vec<_>>()
    });
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
    wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "system" && line["subtype"] == "init"
    });
    let written = workspace.path().join("dry-run.txt");
    wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "result" && line["subtype"] == "success"
    });
    assert!(written.exists(), "model turn did not create dry-run.txt");
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"control_request", "request_id":"dry-run-1",
            "request":{"subtype":"rewind_files", "userMessageId":user_id, "dryRun":true}
        })
    )
    .unwrap();
    stdin.flush().unwrap();
    wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "control_response" && line["response"]["request_id"] == "dry-run-1"
    });
    assert_eq!(std::fs::read_to_string(&written).unwrap(), "changed");
    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "type":"control_request", "request_id":"rewind-1",
            "request":{
                "subtype":"rewind", "user_message_id":user_id,
                "files":true, "conversation":true
            }
        })
    )
    .unwrap();
    stdin.flush().unwrap();
    wait_for_stream_json(&stdout_rx, Duration::from_secs(10), |line| {
        line["type"] == "control_response" && line["response"]["request_id"] == "rewind-1"
    });
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    let stdout = stdout_reader.join().unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        stdout.join("\n"),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        server.join().unwrap(),
        2,
        "stdout={} stderr={}",
        stdout.join("\n"),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !written.exists(),
        "confirmed rewind must restore file absence"
    );
    let lines = stdout
        .iter()
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
    let rewind = lines
        .iter()
        .find(|line| {
            line["type"] == "control_response" && line["response"]["request_id"] == "rewind-1"
        })
        .unwrap();
    assert_eq!(rewind["response"]["subtype"], "success");
    assert_eq!(rewind["response"]["response"]["conversationRewound"], true);
    assert_eq!(rewind["response"]["response"]["filesRewound"], true);
    assert_eq!(rewind["response"]["response"]["deleted"], 1);
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
    run_cli_for_api(format, response, "/v1/messages", "messages")
}

fn run_cli_for_api(format: &str, response: String, api_path: &str, api_format: &str) -> String {
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
        .env("HARNESS_API_PATH", api_path)
        .env("HARNESS_API_FORMAT", api_format)
        .env_remove("HARNESS_MESSAGES_PATH")
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
    .fold(String::new(), |mut body, value| {
        write!(body, "data: {value}\n\n").expect("writing to a String cannot fail");
        body
    })
}

fn chat_reasoning_sse_response(text: &str) -> String {
    let events = [
        serde_json::json!({
            "id":"chat-private","model":"router/alias",
            "choices":[{"index":0,"delta":{
                "role":"assistant","content":text,
                "reasoning":"private-raw-reasoning",
                "reasoning_details":[{"type":"reasoning.encrypted","data":"private-reasoning-details"}]
            },"finish_reason":"stop"}]
        }),
        serde_json::json!({
            "id":"chat-private","model":"provider/model",
            "choices":[{"index":0,"delta":{"role":"assistant","content":""},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":1,"completion_tokens":1}
        }),
    ]
    .into_iter()
    .fold(String::new(), |mut body, value| {
        write!(body, "data: {value}\n\n").expect("writing to a String cannot fail");
        body
    });
    format!("{events}data: [DONE]\n\n")
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

#[cfg(unix)]
fn wait_for_stream_json(
    receiver: &mpsc::Receiver<String>,
    timeout: Duration,
    matches: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "stream-json event timed out");
        let line = receiver
            .recv_timeout(remaining)
            .expect("stream-json output closed before the expected event");
        let value: Value =
            serde_json::from_str(&line).expect("stream-json line must be valid JSON");
        if matches(&value) {
            return value;
        }
    }
}

#[cfg(unix)]
fn make_private_directory(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

fn marker_hook() -> Value {
    let command = std::env::current_exe().expect("current test executable must be available");
    serde_json::json!({
        "type":"command",
        "command":command,
        "args":["--ignored", "--exact", "session_end_marker_worker", "--quiet"],
        "workspaceRelative":true
    })
}

#[test]
#[ignore = "helper process launched by SessionEnd lifecycle tests"]
fn session_end_marker_worker() {
    let Some(workspace) = std::env::var_os("HARNESS_WORKSPACE") else {
        return;
    };
    let workspace = std::fs::canonicalize(workspace).unwrap();
    let cwd = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
    assert_eq!(cwd, workspace, "marker worker must run in the workspace");
    std::fs::write(cwd.join(SESSION_END_MARKER), b"cleanup").unwrap();
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

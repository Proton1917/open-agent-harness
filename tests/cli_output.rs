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

fn mcp_stdio_exchange(workspace: &std::path::Path, tools: &str, call: Value) -> Vec<Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args(["mcp", "serve", "--bare", "--tools", tools])
        .current_dir(workspace)
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .env_remove("HARNESS_BASE_URL")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for message in [
        serde_json::json!({
            "jsonrpc":"2.0", "id":1, "method":"initialize",
            "params":{
                "protocolVersion":"2025-11-25",
                "capabilities":{},
                "clientInfo":{"name":"integration-test", "version":"1"}
            }
        }),
        serde_json::json!({"jsonrpc":"2.0", "method":"notifications/initialized"}),
        serde_json::json!({"jsonrpc":"2.0", "id":2, "method":"tools/list", "params":{}}),
        call,
    ] {
        writeln!(stdin, "{message}").unwrap();
    }
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn mcp_serve_binary_is_model_independent_and_permission_safe() {
    let workspace = tempfile::tempdir().unwrap();
    let readable = workspace.path().join("readable.txt");
    std::fs::write(&readable, "mcp-read-success").unwrap();
    let read = mcp_stdio_exchange(
        workspace.path(),
        "Read",
        serde_json::json!({
            "jsonrpc":"2.0", "id":3, "method":"tools/call",
            "params":{"name":"Read", "arguments":{"file_path":readable}}
        }),
    );
    assert_eq!(read.len(), 3);
    assert_eq!(
        read[0]["result"]["serverInfo"]["name"],
        "open-agent-harness"
    );
    assert_eq!(read[1]["result"]["tools"][0]["name"], "Read");
    assert_eq!(read[2]["result"]["isError"], false);
    assert!(
        read[2]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("mcp-read-success")
    );

    let blocked = workspace.path().join("blocked.txt");
    let write = mcp_stdio_exchange(
        workspace.path(),
        "Write",
        serde_json::json!({
            "jsonrpc":"2.0", "id":3, "method":"tools/call",
            "params":{"name":"Write", "arguments":{"file_path":blocked, "content":"no"}}
        }),
    );
    assert_eq!(write[2]["result"]["isError"], true);
    assert!(!blocked.exists());
}

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
    for capability in [
        "agent_task_events_v1",
        "dynamic_plugin_reload_v1",
        "end_session_v1",
        "generate_session_title_v1",
        "main_agent_initialize_v1",
        "mcp_sdk_transport_v1",
        "side_question_v1",
    ] {
        assert!(
            init["capabilities"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == capability),
            "missing capability {capability}"
        );
    }
    let response = |id: &str| {
        lines
            .iter()
            .find(|line| line["type"] == "control_response" && line["response"]["request_id"] == id)
            .unwrap()
    };
    let initialize = response("init-camel");
    assert_eq!(initialize["response"]["subtype"], "success");
    assert_eq!(
        initialize["response"]["response"]["capabilities"],
        init["capabilities"]
    );
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
fn stream_json_generates_a_bounded_title_and_end_session_exits_without_stdin_eof() {
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
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if tx.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    for request in [
        serde_json::json!({
            "type":"control_request", "request_id":"init",
            "request":{"subtype":"initialize"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"title",
            "request":{
                "subtype":"generate_session_title",
                "description":"  Audit   Title  ",
                "persist":false
            }
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"end",
            "request":{"subtype":"end_session", "reason":"integration complete"}
        }),
    ] {
        writeln!(input, "{request}").unwrap();
    }
    input.flush().unwrap();

    let title = wait_for_stream_json(&rx, Duration::from_secs(5), |line| {
        line["type"] == "control_response" && line["response"]["request_id"] == "title"
    });
    assert_eq!(title["response"]["subtype"], "success");
    assert_eq!(title["response"]["response"]["title"], "Audit Title");
    let end = wait_for_stream_json(&rx, Duration::from_secs(5), |line| {
        line["type"] == "control_response" && line["response"]["request_id"] == "end"
    });
    assert_eq!(end["response"]["response"]["ended"], true);

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "end_session did not terminate the process"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success());
    drop(input);
    reader.join().unwrap();
}

#[cfg(unix)]
#[test]
fn stream_json_reload_plugins_replaces_live_session_catalogs() {
    let workspace = tempfile::tempdir().unwrap();
    let home = workspace.path().join("home");
    let plugin = workspace.path().join("quality-plugin");
    let mcp_server = workspace.path().join("plugin-mcp.sh");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(plugin.join("commands")).unwrap();
    std::fs::write(
        &mcp_server,
        r##"while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"plugin-test","version":"1"}}}' ;;
  *'"method":"tools/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}' ;;
esac
done
"##,
    )
    .unwrap();
    std::fs::write(
        plugin.join("plugin.json"),
        r#"{"name":"quality","version":"1","commands":["commands"]}"#,
    )
    .unwrap();
    std::fs::write(
        plugin.join("commands/one.md"),
        "---\ndescription: First command\n---\nFirst $ARGUMENTS",
    )
    .unwrap();
    let settings = serde_json::json!({"plugins":{"directories":[plugin]}}).to_string();
    let mut child = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--no-session-persistence",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--settings",
            &settings,
        ])
        .current_dir(workspace.path())
        .env("HOME", &home)
        .env("USERPROFILE", &home)
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
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let read_response = |reader: &mut BufReader<std::process::ChildStdout>, id: &str| {
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["type"] == "control_response" && value["response"]["request_id"] == id {
                return value;
            }
        }
    };

    writeln!(
        input,
        "{}",
        serde_json::json!({
            "type":"control_request",
            "request_id":"reload-init",
            "request":{"subtype":"initialize"}
        })
    )
    .unwrap();
    input.flush().unwrap();
    let initialized = read_response(&mut output, "reload-init");
    assert!(
        initialized["response"]["response"]["commands"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command["name"] == "quality:one")
    );

    std::fs::write(
        plugin.join("commands/two.md"),
        "---\ndescription: Second command\n---\nSecond $ARGUMENTS",
    )
    .unwrap();
    let invalid_lsp_manifest = serde_json::json!({
        "name":"quality",
        "version":"1",
        "commands":["commands"],
        "mcpServers":{"plugin-dynamic":{
            "command":"/bin/sh", "args":[mcp_server.clone()], "timeoutMs":1000
        }},
        "lspServers":{
            "rust-one":{"command":"unused-language-server","extensionToLanguage":{"rs":"rust"}},
            "rust-two":{"command":"unused-language-server","extensionToLanguage":{".rs":"rust"}}
        }
    });
    std::fs::write(
        plugin.join("plugin.json"),
        serde_json::to_vec(&invalid_lsp_manifest).unwrap(),
    )
    .unwrap();
    writeln!(
        input,
        "{}",
        serde_json::json!({
            "type":"control_request",
            "request_id":"reload-partial",
            "request":{"subtype":"reload_plugins"}
        })
    )
    .unwrap();
    input.flush().unwrap();
    let partial = read_response(&mut output, "reload-partial");
    assert_eq!(partial["response"]["subtype"], "success", "{partial}");
    assert_eq!(partial["response"]["response"]["error_count"], 1);
    assert!(
        partial["response"]["response"]["commands"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command["name"] == "quality:two"),
        "{partial}"
    );
    assert!(
        partial["response"]["response"]["mcpServers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|server| {
                server["name"] == "quality:plugin-dynamic" && server["status"] == "connected"
            }),
        "{partial}"
    );

    let valid_manifest = serde_json::json!({
        "name":"quality",
        "version":"1",
        "commands":["commands"],
        "mcpServers":{"plugin-dynamic":{
            "command":"/bin/sh", "args":[mcp_server], "timeoutMs":1000
        }},
        "lspServers":{
            "rust":{"command":"unused-language-server","extensionToLanguage":{"rs":"rust"}}
        }
    });
    std::fs::write(
        plugin.join("plugin.json"),
        serde_json::to_vec(&valid_manifest).unwrap(),
    )
    .unwrap();
    writeln!(
        input,
        "{}",
        serde_json::json!({
            "type":"control_request",
            "request_id":"reload-now",
            "request":{"subtype":"reload_plugins"}
        })
    )
    .unwrap();
    input.flush().unwrap();
    let reloaded = read_response(&mut output, "reload-now");
    assert_eq!(reloaded["response"]["subtype"], "success");
    assert_eq!(reloaded["response"]["response"]["error_count"], 0);
    let commands = reloaded["response"]["response"]["commands"]
        .as_array()
        .unwrap();
    for expected in ["quality:one", "quality:two"] {
        assert!(
            commands.iter().any(|command| command["name"] == expected),
            "missing {expected}: {reloaded}"
        );
    }
    assert_eq!(
        reloaded["response"]["response"]["plugins"][0]["name"],
        "quality"
    );
    assert!(
        !reloaded
            .to_string()
            .contains(workspace.path().to_str().unwrap()),
        "reload response leaked an absolute workspace path: {reloaded}"
    );
    drop(input);
    let status = child.wait().unwrap();
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(status.success(), "{stderr}");
}

#[cfg(unix)]
#[test]
fn stream_json_flag_settings_apply_atomically_and_are_reported_redacted() {
    let workspace = tempfile::tempdir().unwrap();
    let mcp_server = workspace.path().join("flag-mcp.sh");
    std::fs::write(
        &mcp_server,
        r##"while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"flag-test","version":"1"}}}' ;;
  *'"method":"tools/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}' ;;
esac
done
"##,
    )
    .unwrap();
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
            "type":"control_request", "request_id":"flags-init",
            "request":{"subtype":"initialize"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"flags-apply",
            "request":{"subtype":"apply_flag_settings","settings":{
                "model":"provider/runtime-model",
                "reasoningEffort":"high",
                "permissions":{"defaultMode":"dontAsk","deny":["Write"]},
                "mcpServers":{"private":{
                    "command":"/bin/sh", "args":[mcp_server],
                    "env":{"PRIVATE_FLAG_SECRET":"secret"}, "timeoutMs":1000
                }}
            }}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"flags-status",
            "request":{"subtype":"mcp_status"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"flags-get",
            "request":{"subtype":"get_settings"}
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
    let applied = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "flags-apply")
        .unwrap();
    assert_eq!(applied["response"]["subtype"], "success");
    let status = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "flags-status")
        .unwrap();
    assert!(
        status["response"]["response"]["mcpServers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|server| server["name"] == "private" && server["status"] == "connected"),
        "{status}"
    );
    let settings = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "flags-get")
        .unwrap();
    assert_eq!(
        settings["response"]["response"]["effective"]["model"],
        "provider/runtime-model"
    );
    assert_eq!(
        settings["response"]["response"]["effective"]["reasoningEffort"],
        "high"
    );
    assert_eq!(
        settings["response"]["response"]["effective"]["permissionMode"],
        "dontAsk"
    );
    assert_eq!(
        settings["response"]["response"]["sources"][0]["settings"]["mcpServers"]["private"]["env"],
        "<redacted>"
    );
    assert!(!settings.to_string().contains("secret"));
}

#[cfg(unix)]
#[test]
fn stream_json_flag_settings_reject_plan_escape_before_mcp_mutation() {
    let workspace = tempfile::tempdir().unwrap();
    let mcp_server = workspace.path().join("rejected-flag-mcp.sh");
    std::fs::write(
        &mcp_server,
        r##"while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"must-not-connect","version":"1"}}}' ;;
  *'"method":"tools/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}' ;;
esac
done
"##,
    )
    .unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--permission-mode",
            "plan",
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
            "type":"control_request", "request_id":"atomic-init",
            "request":{"subtype":"initialize"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"atomic-reject",
            "request":{"subtype":"apply_flag_settings","settings":{
                "permissions":{"defaultMode":"dontAsk"},
                "mcpServers":{"must-not-connect":{
                    "command":"/bin/sh", "args":[mcp_server], "timeoutMs":1000
                }}
            }}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"atomic-status",
            "request":{"subtype":"mcp_status"}
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"atomic-settings",
            "request":{"subtype":"get_settings"}
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
    let rejected = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "atomic-reject")
        .unwrap();
    assert_eq!(rejected["response"]["subtype"], "error", "{rejected}");
    let status = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "atomic-status")
        .unwrap();
    assert!(
        status["response"]["response"]["mcpServers"]
            .as_array()
            .unwrap()
            .iter()
            .all(|server| server["name"] != "must-not-connect"),
        "{status}"
    );
    let settings = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "atomic-settings")
        .unwrap();
    assert_eq!(
        settings["response"]["response"]["effective"]["permissionMode"],
        "plan"
    );
    assert_eq!(
        settings["response"]["response"]["sources"][0]["settings"],
        serde_json::json!({})
    );
}

#[cfg(unix)]
#[test]
fn stream_json_initialize_installs_sdk_agents_and_structured_output_once() {
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
            "type":"control_request", "request_id":"sdk-init-rich",
            "request":{
                "subtype":"initialize",
                "systemPrompt":"SDK system",
                "appendSystemPrompt":"SDK append",
                "jsonSchema":{
                    "type":"object",
                    "properties":{"answer":{"type":"string"}},
                    "required":["answer"],
                    "additionalProperties":false
                },
                "agents":{"reviewer":{
                    "description":"Review changes",
                    "prompt":"Review the workspace",
                    "tools":["Read","Grep"],
                    "disallowedTools":["Bash"],
                    "maxTurns":3,
                    "background":true,
                    "effort":"high",
                    "mcpServers":[{"agent-local":{"command":"unused-agent-mcp"}}],
                    "initialPrompt":"Review the current diff first",
                    "memory":"local",
                    "permissionMode":"acceptEdits"
                }}
            }
        }),
        serde_json::json!({
            "type":"control_request", "request_id":"sdk-init-twice",
            "request":{"subtype":"initialize"}
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
    let initialized = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "sdk-init-rich")
        .unwrap();
    assert_eq!(initialized["response"]["subtype"], "success");
    assert!(
        initialized["response"]["response"]["agents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|agent| agent["name"] == "reviewer")
    );
    assert!(
        initialized["response"]["response"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool == "StructuredOutput")
    );
    let duplicate = lines
        .iter()
        .find(|line| line["response"]["request_id"] == "sdk-init-twice")
        .unwrap();
    assert_eq!(duplicate["response"]["subtype"], "error");
    assert!(
        duplicate["response"]["error"]
            .as_str()
            .unwrap()
            .contains("已初始化")
    );
}

#[cfg(unix)]
#[test]
fn trusted_default_main_agent_applies_prompt_and_tool_policy() {
    let workspace = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_tx, request_rx) = std::sync::mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let body = serde_json::from_slice::<Value>(&read_request_body(&mut stream)).unwrap();
        request_tx.send(body).unwrap();
        let response = sse_response("main-agent-policy-ok");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
    });
    let settings = serde_json::json!({
        "agent":"reader",
        "agents":{"definitions":{"reader":{
            "description":"Read-only main agent",
            "prompt":"SETTINGS_MAIN_AGENT_SYSTEM",
            "allowedTools":["Read"]
        }}}
    })
    .to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--no-session-persistence",
            "--settings",
            &settings,
            "inspect the workspace",
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
    let request = request_rx.recv().unwrap();
    assert!(
        request["system"]
            .as_str()
            .is_some_and(|system| system.starts_with("SETTINGS_MAIN_AGENT_SYSTEM")),
        "{request}"
    );
    let tools = request["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1, "{request}");
    assert_eq!(tools[0]["name"], "Read");
}

#[cfg(unix)]
#[test]
fn sdk_defined_main_agent_applies_system_initial_prompt_and_runtime_options() {
    let workspace = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_tx, request_rx) = std::sync::mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let body = serde_json::from_slice::<Value>(&read_request_body(&mut stream)).unwrap();
        request_tx.send(body).unwrap();
        let response = sse_response("main-agent-ok");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
    });
    let mut child = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args([
            "--print",
            "--bare",
            "--agent",
            "sdk-main",
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
    let mut input = child.stdin.take().unwrap();
    writeln!(
        input,
        "{}",
        serde_json::json!({
            "type":"control_request", "request_id":"main-agent-init",
            "request":{
                "subtype":"initialize",
                "appendSystemPrompt":"SDK_MAIN_APPEND",
                "agentProgressSummaries":true,
                "agents":{"sdk-main":{
                    "description":"Main SDK agent",
                    "prompt":"SDK_MAIN_SYSTEM",
                    "initialPrompt":"SDK_MAIN_INITIAL",
                    "permissionMode":"acceptEdits",
                    "memory":"local"
                }}
            }
        })
    )
    .unwrap();
    drop(input);
    let output = child.wait_with_output().unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = request_rx.recv().unwrap();
    assert!(
        request["system"]
            .as_str()
            .unwrap()
            .contains("SDK_MAIN_SYSTEM")
    );
    assert!(
        request["system"]
            .as_str()
            .unwrap()
            .contains("SDK_MAIN_APPEND")
    );
    assert!(
        request.to_string().contains("SDK_MAIN_INITIAL"),
        "{request}"
    );
    let lines = String::from_utf8(output.stdout).unwrap();
    assert!(lines.contains("main-agent-init"), "{lines}");
    assert!(lines.contains("main-agent-ok"), "{lines}");
}

#[cfg(unix)]
#[test]
fn stream_json_initialize_system_prompt_reaches_the_next_model_request() {
    let workspace = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_tx, request_rx) = std::sync::mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let body = serde_json::from_slice::<Value>(&read_request_body(&mut stream)).unwrap();
        request_tx.send(body).unwrap();
        let response = sse_response("system-ok");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
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
    let mut input = child.stdin.take().unwrap();
    for message in [
        serde_json::json!({
            "type":"control_request", "request_id":"system-init",
            "request":{
                "subtype":"initialize",
                "systemPrompt":"SDK_SYSTEM_MARKER",
                "appendSystemPrompt":"SDK_APPEND_MARKER"
            }
        }),
        serde_json::json!({
            "type":"user",
            "uuid":uuid::Uuid::new_v4(),
            "message":{"role":"user","content":"hello"}
        }),
    ] {
        writeln!(input, "{message}").unwrap();
    }
    drop(input);
    let output = child.wait_with_output().unwrap();
    server.join().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let request = request_rx.recv().unwrap();
    let system = request["system"].as_str().unwrap();
    assert!(system.contains("SDK_SYSTEM_MARKER"), "{request}");
    assert!(system.contains("SDK_APPEND_MARKER"), "{request}");
}

#[cfg(unix)]
#[test]
fn stream_json_mcp_set_servers_connects_and_removes_runtime_servers() {
    let workspace = tempfile::tempdir().unwrap();
    let server = workspace.path().join("dynamic-mcp.sh");
    std::fs::write(
        &server,
        r##"while IFS= read -r line; do
case "$line" in
  *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"dynamic-test","version":"1"}}}' ;;
  *'"method":"tools/list"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"echo","description":"Echo","inputSchema":{"type":"object","additionalProperties":false}}]}}' ;;
esac
done
"##,
    )
    .unwrap();
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
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let read_response = |reader: &mut BufReader<std::process::ChildStdout>, id: &str| {
        loop {
            let mut line = String::new();
            assert!(reader.read_line(&mut line).unwrap() > 0);
            let value: Value = serde_json::from_str(&line).unwrap();
            if value["type"] == "control_response" && value["response"]["request_id"] == id {
                return value;
            }
        }
    };
    let send = |writer: &mut std::process::ChildStdin, id: &str, request: Value| {
        writeln!(
            writer,
            "{}",
            serde_json::json!({
                "type":"control_request", "request_id":id, "request":request
            })
        )
        .unwrap();
        writer.flush().unwrap();
    };

    send(
        &mut input,
        "dynamic-add",
        serde_json::json!({
            "subtype":"mcp_set_servers",
            "servers":{"Dynamic":{
                "command":"/bin/sh", "args":[server], "timeoutMs":1000
            }}
        }),
    );
    let added = read_response(&mut output, "dynamic-add");
    assert_eq!(added["response"]["subtype"], "success");
    assert_eq!(
        added["response"]["response"]["added"],
        serde_json::json!(["Dynamic"])
    );
    assert_eq!(
        added["response"]["response"]["errors"],
        serde_json::json!({})
    );

    send(
        &mut input,
        "dynamic-status-after-add",
        serde_json::json!({"subtype":"mcp_status"}),
    );
    let status = read_response(&mut output, "dynamic-status-after-add");
    assert!(
        status["response"]["response"]["mcpServers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|server| server["name"] == "Dynamic" && server["status"] == "connected"),
        "{status}"
    );

    send(
        &mut input,
        "dynamic-remove",
        serde_json::json!({"subtype":"mcp_set_servers", "servers":{}}),
    );
    let removed = read_response(&mut output, "dynamic-remove");
    assert_eq!(
        removed["response"]["response"]["removed"],
        serde_json::json!(["Dynamic"])
    );
    send(
        &mut input,
        "dynamic-status-after-remove",
        serde_json::json!({"subtype":"mcp_status"}),
    );
    let status = read_response(&mut output, "dynamic-status-after-remove");
    assert!(
        status["response"]["response"]["mcpServers"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{status}"
    );

    drop(input);
    drop(output);
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
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
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0]["type"], "system");
    assert_eq!(lines[0]["subtype"], "init");
    assert_eq!(lines[1]["type"], "assistant");
    assert_eq!(lines[2]["type"], "result");
    assert_eq!(lines[2]["result"], "stream response");

    let partial = run_cli_for_api_with_options(
        "stream-json",
        sse_response("partial response"),
        "/v1/messages",
        "messages",
        true,
    );
    let partial = partial
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let delta = partial
        .iter()
        .find(|line| line["type"] == "stream_event")
        .expect("partial output must contain a stream_event");
    assert_eq!(delta["event"]["type"], "content_block_delta");
    assert_eq!(delta["event"]["delta"]["text"], "partial response");
    assert_eq!(delta["parent_tool_use_id"], Value::Null);
    assert!(delta["uuid"].as_str().is_some());

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
            .any(|line| line["type"] == "system" && line["subtype"] == "hook_started")
    );
    assert!(
        lines
            .iter()
            .skip(1)
            .any(|line| line["type"] == "system" && line["subtype"] == "hook_response")
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
    run_cli_for_api_with_options(format, response, api_path, api_format, false)
}

fn run_cli_for_api_with_options(
    format: &str,
    response: String,
    api_path: &str,
    api_format: &str,
    include_partial_messages: bool,
) -> String {
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

    let mut command = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"));
    command.args([
        "--print",
        "--bare",
        "--no-session-persistence",
        "--output-format",
        format,
    ]);
    if include_partial_messages {
        command.arg("--include-partial-messages");
    }
    let output = command
        .arg("verify output")
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

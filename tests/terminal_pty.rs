#![cfg(unix)]

use std::{
    fs::File,
    io::{self, Read, Write},
    net::TcpListener,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::process::CommandExt,
    },
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

#[test]
fn composer_handles_mode_help_and_double_interrupt_exit() {
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let mut output = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));
    assert!(output.contains("open-agent-harness"));
    assert!(output.contains("default"));

    terminal.write_all(b"XYZ").unwrap();
    output.push_str(&read_until(&mut terminal, "XYZ", Duration::from_secs(3)));
    terminal.write_all(b"\x7f").unwrap();
    let redraw = read_until(&mut terminal, "XY", Duration::from_secs(3));
    assert_no_bare_line_feeds(redraw.as_bytes());
    output.push_str(&redraw);

    set_terminal_size(&terminal, 40, 8);
    let resized = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));
    assert!(resized.contains("\x1b[2J"));
    assert_no_bare_line_feeds(resized.as_bytes());
    output.push_str(&resized);
    set_terminal_size(&terminal, 100, 30);
    output.push_str(&read_until(
        &mut terminal,
        "Shift+Tab mode",
        Duration::from_secs(3),
    ));

    terminal.write_all(b"\x1b[Z").unwrap();
    output.push_str(&read_until(
        &mut terminal,
        "accept edits mode",
        Duration::from_secs(3),
    ));
    terminal
        .write_all(b"\x1b[200~first line\nsecond line\x1b[201~")
        .unwrap();
    output.push_str(&read_until(
        &mut terminal,
        "second line",
        Duration::from_secs(3),
    ));
    terminal.write_all(b"\x03").unwrap();
    output.push_str(&read_until(
        &mut terminal,
        "Input cleared",
        Duration::from_secs(3),
    ));
    terminal.write_all(b"/help\r").unwrap();
    output.push_str(&read_until(
        &mut terminal,
        "/help  /init",
        Duration::from_secs(3),
    ));
    thread::sleep(Duration::from_millis(250));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
    assert!(output.contains("accept edits"));
}

#[test]
fn permission_interrupt_rolls_back_turn_and_returns_to_composer() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        let response = tool_use_stream();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
    });
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let (mut child, mut terminal) = spawn_terminal(&[&base_url]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));
    terminal.write_all(b"run command\r").unwrap();
    let mut output = read_until(&mut terminal, "Permission required", Duration::from_secs(5));
    terminal.write_all(b"\x03").unwrap();
    output.push_str(&read_until(
        &mut terminal,
        "Shift+Tab mode",
        Duration::from_secs(5),
    ));
    assert!(output.contains("Interrupted"));
    assert_eq!(output.matches("Permission required").count(), 1);
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn exact_session_permission_is_reused_without_a_second_prompt() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for response in [
            single_tool_stream("session-tool-1", "printf session-grant-ok"),
            single_tool_stream("session-tool-2", "printf session-grant-ok"),
            text_stream("SESSION_GRANT_OK"),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        }
    });
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let (mut child, mut terminal) = spawn_terminal(&[&base_url]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));
    terminal.write_all(b"repeat exact action\r").unwrap();
    let mut output = read_until(&mut terminal, "Permission required", Duration::from_secs(5));
    terminal.write_all(b"s").unwrap();
    output.push_str(&read_until(
        &mut terminal,
        "SESSION_GRANT_OK",
        Duration::from_secs(10),
    ));
    assert_eq!(output.matches("Permission required").count(), 1);
    assert!(output.contains("Allowed exact action for this session"));

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
    server.join().unwrap();
}

fn spawn_terminal(extra_env: &[&str]) -> (Child, File) {
    let mut master = -1;
    let mut slave = -1;
    let size = libc::winsize {
        ws_row: 30,
        ws_col: 100,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let size_ptr = std::ptr::addr_of!(size).cast_mut();
    let result = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            size_ptr,
        )
    };
    assert_eq!(result, 0, "{}", io::Error::last_os_error());
    let stdout = unsafe { libc::dup(slave) };
    let stderr = unsafe { libc::dup(slave) };
    assert!(stdout >= 0 && stderr >= 0);

    let mut command = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"));
    command
        .args(["--bare", "--no-session-persistence"])
        .env("NO_COLOR", "1")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(unsafe { Stdio::from_raw_fd(slave) })
        .stdout(unsafe { Stdio::from_raw_fd(stdout) })
        .stderr(unsafe { Stdio::from_raw_fd(stderr) });
    for entry in extra_env {
        let (name, value) = entry.split_once('=').unwrap();
        command.env(name, value);
    }
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY as _, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().unwrap();
    let flags = unsafe { libc::fcntl(master, libc::F_GETFL) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(master, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    (child, unsafe { File::from_raw_fd(master) })
}

fn read_until(terminal: &mut File, needle: &str, timeout: Duration) -> String {
    let started = Instant::now();
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    while started.elapsed() < timeout {
        match terminal.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                output.extend_from_slice(&buffer[..count]);
                if String::from_utf8_lossy(&output).contains(needle) {
                    return String::from_utf8_lossy(&output).into_owned();
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
            Err(error) => panic!("terminal read failed: {error}"),
        }
    }
    panic!(
        "terminal output did not contain {needle:?}: {}",
        String::from_utf8_lossy(&output)
    )
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    panic!("terminal child did not exit")
}

fn assert_no_bare_line_feeds(output: &[u8]) {
    for (index, byte) in output.iter().enumerate() {
        if *byte == b'\n' {
            assert!(
                index > 0 && output[index - 1] == b'\r',
                "raw-mode PTY output contained a bare line feed: {:?}",
                String::from_utf8_lossy(output)
            );
        }
    }
}

fn set_terminal_size(terminal: &File, columns: u16, rows: u16) {
    let size = libc::winsize {
        ws_row: rows,
        ws_col: columns,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let result = unsafe { libc::ioctl(terminal.as_raw_fd(), libc::TIOCSWINSZ as _, &size) };
    assert_eq!(result, 0, "{}", io::Error::last_os_error());
}

fn tool_use_stream() -> String {
    [
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":"permission-turn","content":[],"usage":{}
        }}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"permission-tool","name":"Bash","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"printf should-not-run\"}"}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"must-not-run","name":"Bash","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"command\":\"printf second-command-must-not-run\"}"}}),
        serde_json::json!({"type":"content_block_stop","index":1}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(|value| format!("data: {value}\n\n"))
    .collect()
}

fn single_tool_stream(id: &str, command: &str) -> String {
    [
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":format!("message-{id}"),"content":[],"usage":{}
        }}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":id,"name":"Bash","input":{}}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":serde_json::json!({"command":command}).to_string()}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}),
        serde_json::json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(|value| format!("data: {value}\n\n"))
    .collect()
}

fn text_stream(text: &str) -> String {
    [
        serde_json::json!({"type":"message_start","message":{
            "type":"message","role":"assistant","id":"session-final","content":[],"usage":{}
        }}),
        serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        serde_json::json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
        serde_json::json!({"type":"content_block_stop","index":0}),
        serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{}}),
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

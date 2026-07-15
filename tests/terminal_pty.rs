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
    sync::{Mutex, MutexGuard, OnceLock},
    thread,
    time::{Duration, Instant},
};

#[test]
fn composer_handles_mode_help_and_double_interrupt_exit() {
    let _serial = serial_terminal_test();
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
    assert!(
        !resized.contains("\x1b[2J"),
        "resize must not clear the committed transcript"
    );
    assert!(resized.contains("XY"));
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
    let help = read_until(&mut terminal, "Available commands:", Duration::from_secs(3));
    let composer_ready = help.contains("Shift+Tab mode");
    output.push_str(&help);
    if !composer_ready {
        output.push_str(&read_until(
            &mut terminal,
            "Shift+Tab mode",
            Duration::from_secs(3),
        ));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
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
fn composer_restores_terminal_around_job_control_suspend() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    let _ = read_available(&mut terminal, Duration::from_millis(100));

    let pid = child.id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(pid, libc::SIGTSTP) }, 0);
    let suspended = read_until(&mut terminal, "\u{1b}[?2004l", Duration::from_secs(3));
    assert!(
        suspended.contains("\u{1b}[?2004l"),
        "bracketed paste must be disabled before the process stops"
    );

    let mut status = 0;
    let started = Instant::now();
    loop {
        let result = unsafe { libc::waitpid(pid, &mut status, libc::WUNTRACED | libc::WNOHANG) };
        assert!(result >= 0, "{}", io::Error::last_os_error());
        if result == pid && libc::WIFSTOPPED(status) {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "child did not enter a stopped state"
        );
        thread::sleep(Duration::from_millis(20));
    }

    assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
    let resumed = read_until(&mut terminal, "\u{1b}[?2004h", Duration::from_secs(3));
    assert!(resumed.contains("\u{1b}[?2004h"));
    let ready = if resumed.contains("Shift+Tab mode") {
        resumed
    } else {
        read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3))
    };
    assert!(ready.contains("Shift+Tab mode"));
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn composer_requires_bounded_double_eof_and_preserves_forward_delete() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));

    terminal.write_all("a界b".as_bytes()).unwrap();
    let _ = read_until(&mut terminal, "a界b", Duration::from_secs(3));
    terminal.write_all(b"\x01\x06").unwrap();
    let _ = read_until(&mut terminal, "a界b", Duration::from_secs(3));
    terminal.write_all(b"\x04").unwrap();
    let deleted = read_until(&mut terminal, "› ab", Duration::from_secs(3));
    assert!(deleted.contains("› ab"));
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));
    terminal.write_all(b"\x04").unwrap();
    let first = read_until(
        &mut terminal,
        "Press Ctrl-D again to exit",
        Duration::from_secs(3),
    );
    assert!(first.contains("Press Ctrl-D again to exit"));
    assert!(child.try_wait().unwrap().is_none());

    thread::sleep(Duration::from_millis(1_700));
    // The old hint may remain painted while idle, but an EOF after the
    // reference's 800 ms window must re-arm rather than exit.
    terminal.write_all(b"\x04").unwrap();
    let rearmed = read_until(
        &mut terminal,
        "Press Ctrl-D again to exit",
        Duration::from_secs(3),
    );
    assert!(rearmed.contains("Press Ctrl-D again to exit"));
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"\x04").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn slash_palette_and_model_picker_follow_interactive_command_flow() {
    let _serial = serial_terminal_test();
    let settings = r#"{"models":[{"value":"model-a","displayName":"Model A","description":"Primary"},{"value":"model-b","displayName":"Model B","description":"Fallback"}]}"#;
    let (mut child, mut terminal) =
        spawn_terminal_with_args(&[], &["--model", "model-b", "--settings", settings]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    let palette = open_slash_palette(&mut terminal);
    assert_no_bare_line_feeds(palette.as_bytes());
    terminal.write_all(b"\x0e").unwrap();
    let next = read_until(&mut terminal, "› /agents", Duration::from_secs(3));
    assert!(next.contains("› /agents"));
    terminal.write_all(b"\x10").unwrap();
    let previous = read_until(&mut terminal, "› /add-dir", Duration::from_secs(3));
    assert!(previous.contains("› /add-dir"));

    terminal.write_all(b"mo").unwrap();
    let filtered = read_until(&mut terminal, "/model", Duration::from_secs(3));
    assert!(filtered.contains("Set the model for this session"));
    terminal.write_all(b"\t").unwrap();
    let completed = read_until(&mut terminal, "model-a", Duration::from_secs(3));
    assert!(completed.contains("/model"));
    terminal.write_all(b"\x7f\r").unwrap();
    let picker = read_until(&mut terminal, "Select model", Duration::from_secs(3));
    assert!(picker.contains("Model A"));
    assert!(picker.contains("Model B"));
    assert!(picker.contains("Enter confirm"));

    terminal.write_all(b"\x1b[A\r").unwrap();
    let selected = read_until(
        &mut terminal,
        "Set model to model-a",
        Duration::from_secs(3),
    );
    assert_no_bare_line_feeds(selected.as_bytes());
    thread::sleep(Duration::from_millis(100));

    terminal.write_all(b"/model current\r").unwrap();
    let current = read_until(
        &mut terminal,
        "Current model: model-a",
        Duration::from_secs(3),
    );
    assert!(current.contains("Current model: model-a"));

    let _ = open_slash_palette(&mut terminal);
    terminal.write_all(b"\x1b").unwrap();
    let dismissed = read_until(
        &mut terminal,
        "Suggestions dismissed",
        Duration::from_secs(3),
    );
    assert!(dismissed.contains("Suggestions dismissed"));

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn theme_picker_previews_without_persisting_on_escape() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));

    terminal.write_all(b"/theme\r").unwrap();
    let initial = read_until(&mut terminal, "Preview · demo.rs", Duration::from_secs(3));
    assert!(initial.contains("- let message = \"before\";"));
    assert!(initial.contains("+ let message = \"after\";"));

    terminal.write_all(b"\x14").unwrap();
    let syntax = read_until(&mut terminal, "syntax off", Duration::from_secs(3));
    assert!(syntax.contains("Ctrl-T toggles code syntax highlighting"));

    terminal.write_all(b"\x1b[B").unwrap();
    let focused = read_until(&mut terminal, "› 2. Dark", Duration::from_secs(3));
    assert!(focused.contains("› 2. Dark"));
    terminal.write_all(b"\x1b").unwrap();
    let cancelled = read_until(
        &mut terminal,
        "Theme unchanged: auto",
        Duration::from_secs(3),
    );
    assert!(cancelled.contains("Theme unchanged: auto"));

    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn status_line_refreshes_while_composer_is_idle_after_mode_change() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));

    terminal.write_all(b"/statusline cat\r").unwrap();
    let mut configured = read_until(
        &mut terminal,
        "Status line configured from trusted user settings.",
        Duration::from_secs(3),
    );
    assert!(configured.contains("Status line configured"));
    if !configured.contains("\"permissionMode\":\"default\"") {
        configured.push_str(&read_until(
            &mut terminal,
            "\"permissionMode\":\"default\"",
            Duration::from_secs(5),
        ));
    }
    assert!(configured.contains("\"permissionMode\":\"default\""));

    terminal.write_all(b"\x1b[Z").unwrap();
    let refreshed = read_until(
        &mut terminal,
        "\"permissionMode\":\"acceptEdits\"",
        Duration::from_secs(5),
    );
    assert!(refreshed.contains("\"permissionMode\":\"acceptEdits\""));

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn file_typeahead_accepts_selection_without_submitting_the_prompt() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));

    terminal.write_all(b"inspect @src/ter").unwrap();
    let palette = read_until(&mut terminal, "@src/terminal.rs", Duration::from_secs(5));
    assert!(palette.contains("@src/terminal.rs"));
    assert_no_bare_line_feeds(palette.as_bytes());

    terminal.write_all(b"\r").unwrap();
    let accepted = read_until(
        &mut terminal,
        "File reference inserted",
        Duration::from_secs(3),
    );
    assert!(accepted.contains("inspect @src/terminal.rs"));
    assert!(!accepted.contains("› inspect @src/terminal.rs\r\n\r\n"));
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn interactive_management_commands_open_real_dialogs_and_return_to_composer() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));

    terminal.write_all(b"/permissions\r").unwrap();
    let permissions = read_until(&mut terminal, "Permissions", Duration::from_secs(3));
    assert!(permissions.contains("Allow"));
    assert!(permissions.contains("Workspace"));
    terminal.write_all(b"\x1b").unwrap();
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));

    terminal.write_all(b"/config\r").unwrap();
    let settings = read_until(&mut terminal, "Settings", Duration::from_secs(3));
    assert!(settings.contains("Syntax highlighting"));
    terminal.write_all(b"\x1b").unwrap();
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));

    terminal.write_all(b"/tasks\r").unwrap();
    let tasks = read_until(&mut terminal, "Background tasks", Duration::from_secs(3));
    assert!(tasks.contains("No background tasks"));
    terminal.write_all(b"\x1b").unwrap();
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn composer_history_stash_multiline_and_transcript_shortcuts_are_live() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));

    terminal.write_all(b"/status\r").unwrap();
    let status = read_until(&mut terminal, "Session status:", Duration::from_secs(3));
    if !status.contains("Shift+Tab mode") {
        let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));
    }
    terminal.write_all(b"\x12").unwrap();
    let search = read_until(&mut terminal, "reverse-i-search", Duration::from_secs(3));
    assert!(search.contains("/status"));
    terminal.write_all(b"\x03").unwrap();
    let cancelled = read_until(
        &mut terminal,
        "History search cancelled",
        Duration::from_secs(3),
    );
    assert!(cancelled.contains("History search cancelled"));

    terminal.write_all(b"first\\\rsecond").unwrap();
    let multiline = read_until(&mut terminal, "second", Duration::from_secs(3));
    assert!(multiline.contains("first"));
    terminal.write_all(b"\x13").unwrap();
    let stashed = read_until(&mut terminal, "Prompt stashed", Duration::from_secs(3));
    assert!(stashed.contains("Prompt stashed"));
    terminal.write_all(b"\x13").unwrap();
    let restored = read_until(
        &mut terminal,
        "Restored stashed prompt",
        Duration::from_secs(3),
    );
    assert!(restored.contains("second"));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));

    terminal.write_all(b"\x0f").unwrap();
    let viewer = read_until(&mut terminal, "transcript", Duration::from_secs(3));
    assert!(viewer.contains("Transcript is empty."));
    terminal.write_all(b"q").unwrap();
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(3),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
}

#[test]
fn direct_shell_mode_uses_the_tool_path_and_returns_output_to_the_model() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        assert!(request.contains("&lt;shell-command&gt;") || request.contains("shell-command"));
        assert!(request.contains("pwd"));
        let response = text_stream("SHELL_MODE_OK");
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
    terminal.write_all(b"! pwd\r").unwrap();
    let output = read_until(&mut terminal, "SHELL_MODE_OK", Duration::from_secs(10));
    assert!(output.contains("$ pwd"));
    assert!(!output.contains("Permission required"));

    if !output.contains("Shift+Tab mode") {
        let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(3),
    );
    terminal.write_all(b"\x03").unwrap();
    drop(terminal);
    assert!(wait_for_exit(&mut child, Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn permission_interrupt_rolls_back_turn_and_returns_to_composer() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
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
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for response in [
            single_tool_stream("session-tool-1", "printf session-grant-ok"),
            single_tool_stream("session-tool-2", "printf session-grant-ok"),
            text_stream("SESSION_GRANT_OK"),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
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

    if !output.contains("Shift+Tab mode") {
        let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
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
    spawn_terminal_with_args(extra_env, &[])
}

fn serial_terminal_test() -> MutexGuard<'static, ()> {
    static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
    SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn spawn_terminal_with_args(extra_env: &[&str], extra_args: &[&str]) -> (Child, File) {
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
        .args(extra_args)
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
            Err(error) if error.raw_os_error() == Some(libc::EIO) => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("terminal read failed: {error}"),
        }
    }
    panic!(
        "terminal output did not contain {needle:?}: {}",
        String::from_utf8_lossy(&output)
    )
}

fn read_available(terminal: &mut File, timeout: Duration) -> String {
    let started = Instant::now();
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    while started.elapsed() < timeout {
        match terminal.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => output.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.raw_os_error() == Some(libc::EIO) => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("terminal read failed: {error}"),
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn open_slash_palette(terminal: &mut File) -> String {
    let mut output = String::new();
    for _ in 0..5 {
        // Synthetic PTYs can race an injected byte with a termios transition
        // between prompt-boundary raw-mode guards. Clear before retrying.
        terminal.write_all(b"\x15/").unwrap();
        output.push_str(&read_available(terminal, Duration::from_millis(500)));
        if output.contains("/clear") {
            return output;
        }
    }
    panic!("slash palette did not open: {output}")
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

fn wait_for_raw_mode(terminal: &File, timeout: Duration) {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let mut state = std::mem::MaybeUninit::<libc::termios>::uninit();
        let result = unsafe { libc::tcgetattr(terminal.as_raw_fd(), state.as_mut_ptr()) };
        if result == 0 {
            let state = unsafe { state.assume_init() };
            if state.c_lflag & (libc::ICANON | libc::ECHO) == 0 {
                return;
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("PTY did not enter raw mode before injected input")
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

fn read_request(stream: &mut std::net::TcpStream) -> String {
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
    String::from_utf8_lossy(&buffer).into_owned()
}

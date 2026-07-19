#![cfg(unix)]

use std::{
    fmt::Write as _,
    fs::File,
    io::{self, Read, Write},
    net::TcpListener,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::process::CommandExt,
    },
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, MutexGuard, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

use std::os::unix::fs::PermissionsExt as _;

#[test]
fn streamed_response_shows_live_tokens_and_reconciles_exact_output_usage() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (release_stream, wait_for_release) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        let events = [
            serde_json::json!({"type":"message_start","message":{
                "type":"message","role":"assistant","id":"live-token-turn","content":[],
                "usage":{"input_tokens":11,"output_tokens":0}
            }}),
            serde_json::json!({"type":"content_block_start","index":0,"content_block":{
                "type":"text","text":""
            }}),
            serde_json::json!({"type":"content_block_delta","index":0,"delta":{
                "type":"text_delta","text":"LIVE1234"
            }}),
            serde_json::json!({"type":"content_block_delta","index":0,"delta":{
                "type":"text_delta","text":"_TOKEN_COUNT_OK!"
            }}),
            serde_json::json!({"type":"content_block_stop","index":0}),
            serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},
                "usage":{"output_tokens":7}
            }),
            serde_json::json!({"type":"message_stop"}),
        ]
        .into_iter()
        .map(|value| format!("data: {value}\n\n"))
        .collect::<Vec<_>>();
        let content_length = events.iter().map(String::len).sum::<usize>();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {content_length}\r\nconnection: close\r\n\r\n"
        )
        .unwrap();
        for (index, event) in events.iter().enumerate() {
            stream.write_all(event.as_bytes()).unwrap();
            stream.flush().unwrap();
            if index == 2 {
                wait_for_release
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            } else if index == 3 {
                thread::sleep(Duration::from_millis(700));
            }
        }
    });
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let (mut child, mut terminal) = spawn_terminal(&[&base_url]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal.write_all(b"count live tokens\r").unwrap();
    let active = read_until(&mut terminal, "esc to interrupt", Duration::from_secs(3));
    assert!(active.contains("esc to interrupt"), "{active}");
    let first_delta = if active.contains("LIVE1234") {
        active.clone()
    } else {
        read_until(&mut terminal, "LIVE1234", Duration::from_secs(3))
    };
    assert!(first_delta.contains("LIVE1234"), "{first_delta}");
    assert!(!first_delta.contains("_TOKEN_COUNT_OK!"), "{first_delta}");
    let first_count = read_until(&mut terminal, "↓ 2 tokens", Duration::from_secs(5));
    assert!(first_count.contains("Working"), "{first_count}");
    assert!(
        ["·", "✢", "✳", "✶", "✻", "✽", "*"]
            .iter()
            .any(|glyph| first_count.contains(glyph)),
        "running status did not use a source-shaped spinner frame: {first_count}"
    );
    assert!(
        ["◐", "◓", "◑", "◒"]
            .iter()
            .all(|glyph| !first_count.contains(glyph)),
        "legacy quadrant spinner leaked into running status: {first_count}"
    );
    release_stream.send(()).unwrap();
    let second_count = read_until(&mut terminal, "↓ 6 tokens", Duration::from_secs(5));
    assert!(second_count.contains("↓ 6 tokens"), "{second_count}");
    let answer = read_until(
        &mut terminal,
        "LIVE1234_TOKEN_COUNT_OK!",
        Duration::from_secs(5),
    );
    let exact = if answer.contains("↓ 7 tokens") {
        answer
    } else {
        read_until(&mut terminal, "↓ 7 tokens", Duration::from_secs(3))
    };
    assert!(exact.contains("? for shortcuts"), "{exact}");
    assert!(child.try_wait().unwrap().is_none());

    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn composer_handles_mode_help_and_double_interrupt_exit() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let mut output = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    assert!(output.contains("open-agent-harness"));
    assert!(output.contains("default"));

    terminal.write_all(b"XYZ").unwrap();
    output.push_str(&read_until(&mut terminal, "XYZ", Duration::from_secs(3)));
    terminal.write_all(b"\x7f").unwrap();
    let redraw = read_until(&mut terminal, "XY", Duration::from_secs(3));
    assert_no_bare_line_feeds(redraw.as_bytes());
    output.push_str(&redraw);

    set_terminal_size(&terminal, 40, 8);
    let resized = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
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
        "? for shortcuts",
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
    let composer_ready = help.contains("accept edits on (shift+tab to cycle)");
    output.push_str(&help);
    if !composer_ready {
        output.push_str(&read_until(
            &mut terminal,
            "accept edits on (shift+tab to cycle)",
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
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    assert!(output.contains("accept edits"));
}

#[test]
fn terminal_panel_is_explicit_and_restores_the_live_composer() {
    let _serial = serial_terminal_test();
    let programs = tempfile::tempdir().unwrap();
    let shell = programs.path().join("panel-shell");
    let marker = programs.path().join("opened");
    std::fs::write(
        &shell,
        format!(
            "#!/bin/sh\nprintf 'opened' > '{}'\nprintf 'PANEL_SHELL_OPENED\\n'\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o700)).unwrap();
    let shell_env = format!("SHELL={}", shell.display());
    let path_env = format!("PATH={}", programs.path().display());
    let (mut child, mut terminal) = spawn_terminal(&[&shell_env, &path_env]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));

    terminal.write_all(b"\x1bj").unwrap();
    let disabled = read_until(
        &mut terminal,
        "Terminal panel disabled",
        Duration::from_secs(3),
    );
    assert!(disabled.contains("terminalPanelEnabled"), "{disabled}");
    assert!(!marker.exists());

    terminal
        .write_all(b"/config terminalPanelEnabled=true\r")
        .unwrap();
    let updated = read_until(
        &mut terminal,
        "Updated UI setting terminalPanelEnabled.",
        Duration::from_secs(3),
    );
    if !updated.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x1bj").unwrap();
    let opened = read_until(&mut terminal, "PANEL_SHELL_OPENED", Duration::from_secs(5));
    let restored = if opened.contains("Returned from direct terminal shell") {
        opened
    } else {
        read_until(
            &mut terminal,
            "Returned from direct terminal shell",
            Duration::from_secs(3),
        )
    };
    assert!(marker.exists());
    assert!(restored.contains("❯ "), "{restored}");
    assert!(restored.contains("Returned from direct terminal shell"));
    assert!(child.try_wait().unwrap().is_none());

    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn context_command_renders_partitioned_usage_and_memory_status() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"/context\r").unwrap();
    let report = read_until(&mut terminal, "Workspace memory", Duration::from_secs(4));
    assert!(report.contains("Context usage"), "{report}");
    assert!(report.contains("Estimated usage by category"), "{report}");
    assert!(report.contains("Base instructions"), "{report}");
    assert!(report.contains("Tool definitions"), "{report}");
    assert!(report.contains("Free before auto-compact"), "{report}");
    assert!(child.try_wait().unwrap().is_none());

    let ready = if report.contains("? for shortcuts") {
        report
    } else {
        read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3))
    };
    assert!(ready.contains("? for shortcuts"));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn workspace_quick_open_and_global_search_insert_source_style_paths() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));

    // Ctrl-X Ctrl-P is the classic-PTY fallback for Ctrl/Cmd-Shift-P.
    terminal.write_all(b"\x18\x10").unwrap();
    let quick = read_until(&mut terminal, "Quick Open", Duration::from_secs(3));
    assert!(quick.contains("Enter open"), "{quick}");
    terminal.write_all(b"Cargo.toml").unwrap();
    let quick_match = read_until(
        &mut terminal,
        "Preview · Cargo.toml",
        Duration::from_secs(3),
    );
    assert!(quick_match.contains("Cargo.toml"), "{quick_match}");
    terminal.write_all(b"\t").unwrap();
    let mentioned = read_until(&mut terminal, "@Cargo.toml", Duration::from_secs(3));
    assert!(mentioned.contains("@Cargo.toml"), "{mentioned}");

    terminal.write_all(b"\x15").unwrap();
    let _ = read_until(
        &mut terminal,
        "Ctrl+Y to paste deleted text",
        Duration::from_secs(3),
    );
    // Ctrl-X Ctrl-F is the classic-PTY fallback for Ctrl/Cmd-Shift-F.
    terminal.write_all(b"\x18\x06").unwrap();
    let global = read_until(&mut terminal, "Global Search", Duration::from_secs(3));
    assert!(global.contains("Type to search workspace text"), "{global}");
    terminal
        .write_all(b"provider-neutral Rust coding-agent harness")
        .unwrap();
    let global_match = read_until(&mut terminal, "Cargo.toml:6", Duration::from_secs(5));
    assert!(global_match.contains("provider-neutral"), "{global_match}");
    terminal.write_all(b"\x1b[Z").unwrap();
    let inserted = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    assert!(inserted.contains("Cargo.toml:6"), "{inserted}");
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));
    thread::sleep(Duration::from_millis(900));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn composer_exits_when_its_controlling_pty_hangs_up() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));

    // The child must not inherit the PTY master. Closing the parent's only
    // master descriptor should therefore deliver a terminal hangup instead of
    // leaving a detached harness process behind.
    drop(terminal);
    let _ = wait_for_exit(&mut child, None, Duration::from_secs(3));
}

#[test]
fn composer_collapses_large_paste_expands_on_submit_and_completes_mid_input_slash() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));

    terminal.write_all(b"please /mo").unwrap();
    let hinted = read_until(
        &mut terminal,
        "Tab/Right completes /model",
        Duration::from_secs(3),
    );
    assert!(hinted.contains("please /mo"));
    terminal.write_all(b"\t").unwrap();
    let completed = read_until(&mut terminal, "please /model", Duration::from_secs(3));
    assert!(completed.contains("Completed /model"));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));

    let pasted = format!("/tag {}", "x".repeat(801));
    terminal.write_all(b"\x1b[200~").unwrap();
    terminal.write_all(pasted.as_bytes()).unwrap();
    terminal.write_all(b"\x1b[201~").unwrap();
    let collapsed = read_until(&mut terminal, "[Pasted text #1]", Duration::from_secs(3));
    assert!(collapsed.contains("Large paste collapsed"));

    terminal.write_all(b"\r").unwrap();
    let rejected = read_until(
        &mut terminal,
        "Session tag unchanged:",
        Duration::from_secs(3),
    );
    assert!(
        rejected.contains("Session tag unchanged:"),
        "expanded slash command was not dispatched: {rejected}"
    );
    assert!(child.try_wait().unwrap().is_none());

    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn composer_restores_terminal_around_job_control_suspend() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
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
    let ready = if resumed.contains("? for shortcuts") {
        resumed
    } else {
        read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3))
    };
    assert!(ready.contains("? for shortcuts"));
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn external_termination_signals_restore_raw_and_fullscreen_terminal_modes() {
    let _serial = serial_terminal_test();
    for (name, signal) in [
        ("SIGHUP", libc::SIGHUP),
        ("SIGINT", libc::SIGINT),
        ("SIGQUIT", libc::SIGQUIT),
        ("SIGTERM", libc::SIGTERM),
    ] {
        let (mut child, mut terminal) = spawn_terminal(&[]);
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
        wait_for_raw_mode(&terminal, Duration::from_secs(2));

        terminal.write_all(b"/tui fullscreen\r").unwrap();
        let entered = read_until(&mut terminal, "\x1b[?1049h", Duration::from_secs(3));
        assert!(entered.contains("\x1b[?1049h"), "{name}: {entered:?}");

        // SAFETY: child.id() is the live isolated PTY child spawned above. The
        // return value is checked and each child is reaped before the next case.
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        let restored = read_until(&mut terminal, "\x1b[?1049l", Duration::from_secs(3));
        assert!(restored.contains("\x1b[?2004l"), "{name}: {restored:?}");
        assert!(restored.contains("\x1b[?25h"), "{name}: {restored:?}");
        assert_eq!(
            wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).code(),
            Some(128 + signal),
            "{name}"
        );
    }
}

#[test]
fn composer_requires_bounded_double_eof_and_preserves_forward_delete() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal.write_all("a界b".as_bytes()).unwrap();
    let _ = read_until(&mut terminal, "a界b", Duration::from_secs(3));
    terminal.write_all(b"\x01\x06").unwrap();
    let _ = read_until(&mut terminal, "a界b", Duration::from_secs(3));
    terminal.write_all(b"\x04").unwrap();
    let deleted = read_until(&mut terminal, "❯ ab", Duration::from_secs(3));
    assert!(deleted.contains("❯ ab"));
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
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn slash_palette_and_model_picker_follow_interactive_command_flow() {
    let _serial = serial_terminal_test();
    let settings = r#"{"models":[{"value":"model-a","displayName":"Model A","description":"Primary"},{"value":"model-b","displayName":"Model B","description":"Fallback"}]}"#;
    let (mut child, mut terminal) =
        spawn_terminal_with_args(&[], &["--model", "model-b", "--settings", settings]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
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
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn theme_picker_previews_without_persisting_on_escape() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

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
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn status_line_refreshes_while_composer_is_idle_after_mode_change() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal
        .write_all(b"/statusline grep -o '\"permissionMode\":\"[^\"]*\"'\r")
        .unwrap();
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
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn file_typeahead_accepts_selection_without_submitting_the_prompt() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

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
    assert!(!accepted.contains("❯ inspect @src/terminal.rs\r\n\r\n"));
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn interactive_management_commands_open_real_dialogs_and_return_to_composer() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal.write_all(b"/permissions\r").unwrap();
    let permissions = read_until(&mut terminal, "Workspace", Duration::from_secs(3));
    assert!(permissions.contains("Allow"));
    assert!(permissions.contains("Workspace"));
    terminal.write_all(b"\x1b").unwrap();
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));

    terminal.write_all(b"/config\r").unwrap();
    let settings = read_until(&mut terminal, "Syntax highlighting", Duration::from_secs(3));
    assert!(settings.contains("Syntax highlighting"));
    terminal.write_all(b"\x1b").unwrap();
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));

    terminal.write_all(b"/tasks\r").unwrap();
    let tasks = read_until(&mut terminal, "No background tasks", Duration::from_secs(3));
    assert!(tasks.contains("No background tasks"));
    terminal.write_all(b"\x1b").unwrap();
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
}

#[test]
fn composer_history_stash_multiline_and_transcript_shortcuts_are_live() {
    let _serial = serial_terminal_test();
    let (mut child, mut terminal) = spawn_terminal(&[]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal.write_all(b"/status\r").unwrap();
    let status = read_until(&mut terminal, "Session status:", Duration::from_secs(3));
    if !status.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
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
    let _ = read_until(
        &mut terminal,
        "Returned from transcript",
        Duration::from_secs(3),
    );

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(3),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
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
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    terminal.write_all(b"! pwd\r").unwrap();
    let output = read_until(&mut terminal, "SHELL_MODE_OK", Duration::from_secs(10));
    assert!(output.contains("$ pwd"));
    assert!(!output.contains("Permission required"));

    if !output.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(3),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn active_turn_btw_answers_without_interrupting_or_mutating_the_main_queue() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = Arc::clone(&requests);
    let server = thread::spawn(move || {
        let (mut main_stream, _) = listener.accept().unwrap();
        captured
            .lock()
            .unwrap()
            .push(read_request(&mut main_stream));
        let main_worker = thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            let response = text_stream("MAIN_TURN_DONE");
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
            .push(read_request(&mut side_stream));
        let response = text_stream("SIDE_QUESTION_ANSWER");
        write!(
            side_stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();

        let (mut queued_stream, _) = listener.accept().unwrap();
        captured
            .lock()
            .unwrap()
            .push(read_request(&mut queued_stream));
        let response = text_stream("QUEUED_TURN_DONE");
        write!(
            queued_stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
        main_worker.join().unwrap();
    });
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let (mut child, mut terminal) = spawn_terminal(&[&base_url]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal.write_all(b"main objective\r").unwrap();
    let active = read_until(&mut terminal, "esc to interrupt", Duration::from_secs(5));
    assert_no_bare_line_feeds(active.as_bytes());
    terminal
        .write_all(b"/btw what is the active objective?\r")
        .unwrap();
    let side_started = read_until(
        &mut terminal,
        "BTW answering separately",
        Duration::from_secs(3),
    );
    assert!(side_started.contains("main turn still running"));

    terminal.write_all(b"queued follow-up\r").unwrap();
    let queued = read_until(
        &mut terminal,
        "Queued for the next turn",
        Duration::from_secs(3),
    );
    assert!(queued.contains("1/8"));

    let mut side_answer = format!("{side_started}{queued}");
    if !side_answer.contains("SIDE_QUESTION_ANSWER") {
        side_answer.push_str(&read_until(
            &mut terminal,
            "SIDE_QUESTION_ANSWER",
            Duration::from_secs(5),
        ));
    }
    assert!(side_answer.contains("BTW"));
    let side_position = side_answer.find("SIDE_QUESTION_ANSWER").unwrap();
    if let Some(main_position) = side_answer.find("MAIN_TURN_DONE") {
        assert!(
            side_position < main_position,
            "the delayed main request completed before the independent side answer"
        );
    }
    let queued_answer = read_until(&mut terminal, "QUEUED_TURN_DONE", Duration::from_secs(10));
    assert!(queued_answer.contains("MAIN_TURN_DONE"));

    if !queued_answer.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    server.join().unwrap();

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].contains("main objective"));
    assert!(requests[1].contains("what is the active objective?"));
    assert!(requests[1].contains("\"tools\":[]"));
    assert!(requests[2].contains("queued follow-up"));
    assert!(!requests[2].contains("SIDE_QUESTION_ANSWER"));
    assert!(!requests[2].contains("what is the active objective?"));
}

#[test]
fn active_turn_terminal_panel_suspends_and_restores_without_cancelling_the_turn() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        thread::sleep(Duration::from_secs(2));
        let response = text_stream("ACTIVE_PANEL_TURN_DONE");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
    });
    let programs = tempfile::tempdir().unwrap();
    let shell = programs.path().join("active-panel-shell");
    std::fs::write(&shell, "#!/bin/sh\nprintf 'ACTIVE_PANEL_SHELL_OPENED\\n'\n").unwrap();
    std::fs::set_permissions(&shell, std::fs::Permissions::from_mode(0o700)).unwrap();
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let shell_env = format!("SHELL={}", shell.display());
    let path_env = format!("PATH={}", programs.path().display());
    let (mut child, mut terminal) = spawn_terminal(&[&base_url, &shell_env, &path_env]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    terminal
        .write_all(b"/config terminalPanelEnabled=true\r")
        .unwrap();
    let configured = read_until(
        &mut terminal,
        "Updated UI setting terminalPanelEnabled.",
        Duration::from_secs(3),
    );
    if !configured.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"keep the main turn alive\r").unwrap();
    let active = read_until(&mut terminal, "esc to interrupt", Duration::from_secs(5));
    assert!(active.contains("esc to interrupt"), "{active}");

    terminal.write_all(b"\x1bj").unwrap();
    let shell_output = read_until(
        &mut terminal,
        "ACTIVE_PANEL_SHELL_OPENED",
        Duration::from_secs(5),
    );
    let restored = if shell_output.contains("main turn still running") {
        shell_output
    } else {
        read_until(
            &mut terminal,
            "main turn still running",
            Duration::from_secs(3),
        )
    };
    assert!(restored.contains("Returned from direct terminal shell"));
    let completed = read_until(
        &mut terminal,
        "ACTIVE_PANEL_TURN_DONE",
        Duration::from_secs(6),
    );
    assert!(completed.contains("ACTIVE_PANEL_TURN_DONE"));
    assert!(child.try_wait().unwrap().is_none());

    if !completed.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn active_turn_composer_ctrl_c_interrupts_and_returns_to_idle_input() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (request_ready_tx, request_ready_rx) = std::sync::mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        request_ready_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(500));
        let response = text_stream("MUST_NOT_COMMIT");
        let _ = write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        );
    });
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let (mut child, mut terminal) = spawn_terminal(&[&base_url]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal.write_all(b"cancel this active turn\r").unwrap();
    let _ = read_until(&mut terminal, "esc to interrupt", Duration::from_secs(5));
    request_ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("active request was not fully received before interrupt");
    terminal.write_all(b"\x03").unwrap();
    let interrupted = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    assert!(interrupted.contains("Interrupted"));
    assert!(!interrupted.contains("MUST_NOT_COMMIT"));

    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn fullscreen_active_turn_keeps_btw_composer_live() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut main_stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut main_stream);
        let main_worker = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            let response = text_stream("FULLSCREEN_MAIN_DONE");
            write!(
                main_stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            )
            .unwrap();
        });
        let (mut side_stream, _) = listener.accept().unwrap();
        let request = read_request(&mut side_stream);
        assert!(request.contains("fullscreen objective"));
        assert!(request.contains("answer in fullscreen"));
        let response = text_stream("FULLSCREEN_SIDE_ANSWER");
        write!(
            side_stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response.len(),
            response
        )
        .unwrap();
        main_worker.join().unwrap();
    });
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let (mut child, mut terminal) = spawn_terminal(&[&base_url]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    terminal.write_all(b"/tui fullscreen\r").unwrap();
    let entered = read_until(
        &mut terminal,
        "TUI mode: fullscreen",
        Duration::from_secs(3),
    );
    if !entered.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));

    terminal.write_all(b"fullscreen objective").unwrap();
    let _ = read_until(
        &mut terminal,
        "fullscreen objective",
        Duration::from_secs(3),
    );
    terminal.write_all(b"\r").unwrap();
    let _ = read_until(&mut terminal, "esc to interrupt", Duration::from_secs(5));
    terminal.write_all(b"/btw answer in fullscreen").unwrap();
    let _ = read_until(
        &mut terminal,
        "/btw answer in fullscreen",
        Duration::from_secs(3),
    );
    terminal.write_all(b"\r").unwrap();
    let mut output = read_until(
        &mut terminal,
        "FULLSCREEN_SIDE_ANSWER",
        Duration::from_secs(5),
    );
    if !output.contains("FULLSCREEN_MAIN_DONE") {
        output.push_str(&read_until(
            &mut terminal,
            "FULLSCREEN_MAIN_DONE",
            Duration::from_secs(5),
        ));
    }
    assert!(output.contains("BTW"));
    assert!(child.try_wait().unwrap().is_none());

    terminal.write_all(b"/tui default\r").unwrap();
    let _ = read_until(&mut terminal, "TUI mode: default", Duration::from_secs(3));
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn idle_terminal_notification_cancels_on_activity_and_rearms_after_next_turn() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for response in [
            text_stream("FIRST_IDLE_TURN"),
            text_stream("SECOND_IDLE_TURN"),
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
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal
        .write_all(b"/config preferredNotifChannel=iterm2\r")
        .unwrap();
    let _ = read_until(
        &mut terminal,
        "Updated UI setting preferredNotifChannel.",
        Duration::from_secs(3),
    );
    terminal
        .write_all(b"/config messageIdleNotifThresholdMs=1000\r")
        .unwrap();
    let _ = read_until(
        &mut terminal,
        "Updated UI setting messageIdleNotifThresholdMs.",
        Duration::from_secs(3),
    );

    terminal.write_all(b"first idle turn\r").unwrap();
    let first = read_until(&mut terminal, "FIRST_IDLE_TURN", Duration::from_secs(10));
    if !first.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    terminal.write_all(b"x").unwrap();
    let _ = read_until(&mut terminal, "❯ x", Duration::from_secs(3));
    let cancelled_window = read_available(&mut terminal, Duration::from_millis(1_300));
    assert!(
        !cancelled_window.contains("\x1b]9;"),
        "typing after completion must cancel the idle notification: {cancelled_window:?}"
    );
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));

    terminal.write_all(b"second idle turn\r").unwrap();
    let second = read_until(&mut terminal, "SECOND_IDLE_TURN", Duration::from_secs(10));
    if !second.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    let notification = read_until(
        &mut terminal,
        "\x1b]9;\n\nOpen Agent Harness:",
        Duration::from_secs(4),
    );
    assert!(notification.contains("The agent is waiting for your input"));

    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
    server.join().unwrap();
}

#[test]
fn interactive_prompt_suggestion_cancels_stale_work_rearms_and_accepts() {
    let _serial = serial_terminal_test();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = Arc::clone(&requests);
    let server = thread::spawn(move || {
        for (delay, response) in [
            (Duration::ZERO, text_stream("FIRST_TURN_DONE")),
            (Duration::from_millis(1_500), text_stream("run the tests")),
            (Duration::ZERO, text_stream("SECOND_TURN_DONE")),
            (Duration::ZERO, text_stream("commit changes")),
            (Duration::ZERO, text_stream("SUGGESTION_ACCEPTED")),
            (Duration::ZERO, text_stream("push it")),
        ] {
            let (mut stream, _) = listener.accept().unwrap();
            captured.lock().unwrap().push(read_request(&mut stream));
            thread::sleep(delay);
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                response.len(),
                response
            );
        }
    });
    let base_url = format!("HARNESS_BASE_URL=http://{address}");
    let (mut child, mut terminal) =
        spawn_terminal_with_args(&[&base_url], &["--prompt-suggestions"]);
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));

    terminal.write_all(b"first turn\r").unwrap();
    let first = read_until(&mut terminal, "FIRST_TURN_DONE", Duration::from_secs(10));
    if !first.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    let accepted_first_suggestion_request = Instant::now();
    while requests.lock().unwrap().len() < 2 {
        assert!(
            accepted_first_suggestion_request.elapsed() < Duration::from_secs(3),
            "prompt suggestion request did not start"
        );
        thread::sleep(Duration::from_millis(10));
    }
    terminal.write_all(b"x").unwrap();
    let typed = read_until(&mut terminal, "❯ x", Duration::from_secs(3));
    assert!(!typed.contains("run the tests"));
    let stale_window = read_available(&mut terminal, Duration::from_millis(1_700));
    assert!(
        !stale_window.contains("run the tests"),
        "cancelled generation repopulated the composer: {stale_window:?}"
    );
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(&mut terminal, "Input cleared", Duration::from_secs(3));

    terminal.write_all(b"prepare next\r").unwrap();
    let second = read_until(&mut terminal, "SECOND_TURN_DONE", Duration::from_secs(10));
    let mut suggestion = if second.contains("commit changes") {
        second
    } else {
        read_until(&mut terminal, "commit changes", Duration::from_secs(5))
    };
    if !suggestion.contains("Enter send") {
        suggestion.push_str(&read_until(
            &mut terminal,
            "Enter send",
            Duration::from_secs(3),
        ));
    }
    assert!(suggestion.contains("commit changes"));
    assert!(suggestion.contains("Enter send"));
    assert!(suggestion.contains("Tab/→ edit"));
    terminal.write_all(b"\r").unwrap();
    let accepted = read_until(
        &mut terminal,
        "SUGGESTION_ACCEPTED",
        Duration::from_secs(10),
    );
    assert!(accepted.contains("commit changes"));
    if !accepted.contains("push it") {
        let _ = read_until(&mut terminal, "push it", Duration::from_secs(5));
    }

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    assert!(requests[1].contains("\"tools\":[]"));
    assert!(requests[3].contains("\"tools\":[]"));
    assert!(requests[4].contains("commit changes"));
    drop(requests);

    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
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
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
    terminal.write_all(b"run command\r").unwrap();
    let mut output = read_until(&mut terminal, "Permission required", Duration::from_secs(5));
    terminal.write_all(b"\x03").unwrap();
    output.push_str(&read_until(
        &mut terminal,
        "? for shortcuts",
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
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
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
    let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(5));
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

    if !output.contains("? for shortcuts") {
        let _ = read_until(&mut terminal, "? for shortcuts", Duration::from_secs(3));
    }
    wait_for_raw_mode(&terminal, Duration::from_secs(2));
    terminal.write_all(b"\x03").unwrap();
    let _ = read_until(
        &mut terminal,
        "Press Ctrl-C again to exit",
        Duration::from_secs(2),
    );
    terminal.write_all(b"\x03").unwrap();
    assert!(wait_for_exit(&mut child, Some(&mut terminal), Duration::from_secs(3)).success());
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
    let descriptor_flags = unsafe { libc::fcntl(master, libc::F_GETFD) };
    assert!(descriptor_flags >= 0, "{}", io::Error::last_os_error());
    assert_eq!(
        unsafe { libc::fcntl(master, libc::F_SETFD, descriptor_flags | libc::FD_CLOEXEC,) },
        0,
        "{}",
        io::Error::last_os_error()
    );
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

fn wait_for_exit(
    child: &mut Child,
    mut terminal: Option<&mut File>,
    timeout: Duration,
) -> std::process::ExitStatus {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if let Some(terminal) = terminal.as_deref_mut() {
            let mut output = [0u8; 8192];
            loop {
                match terminal.read(&mut output) {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                    Err(error) => panic!("terminal read failed while awaiting exit: {error}"),
                }
            }
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
    .fold(String::new(), |mut body, value| {
        write!(body, "data: {value}\n\n").expect("writing to a String cannot fail");
        body
    })
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
    .fold(String::new(), |mut body, value| {
        write!(body, "data: {value}\n\n").expect("writing to a String cannot fail");
        body
    })
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
    .fold(String::new(), |mut body, value| {
        write!(body, "data: {value}\n\n").expect("writing to a String cannot fail");
        body
    })
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

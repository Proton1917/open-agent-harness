#![cfg(unix)]

use std::{
    fs::{self, File},
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Mutex, MutexGuard, OnceLock},
    thread,
    time::{Duration, Instant},
};

#[test]
fn configured_unbind_command_and_chord_are_live_in_the_composer() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    write_keybindings_atomic(
        home.path(),
        r#"{
  "bindings": [
    {
      "context": "Chat",
      "bindings": {
        "alt+p": null,
        "ctrl+u": "command:status",
        "ctrl+k ctrl+s": "command:status"
      }
    }
  ]
}"#,
    );

    let mut session = spawn_terminal(home.path());
    session.read_until("Shift+Tab mode", Duration::from_secs(5));
    session.wait_for_raw_mode(Duration::from_secs(2));
    session.read_available(Duration::from_millis(100));

    // ESC+p is the terminal encoding of Alt-P. A null user binding must
    // consume the default model-picker binding without inserting text.
    session.write_all(b"\x1bp");
    let after_unbind = session.read_available(Duration::from_millis(700));
    assert!(
        !after_unbind.contains("Select model"),
        "null binding did not suppress Alt-P: {after_unbind:?}"
    );
    assert!(
        session.is_running(),
        "Alt-P unexpectedly terminated the CLI"
    );

    // A configured multi-key chord must win over the base editor's Ctrl-K
    // and Ctrl-S behavior and invoke the slash command exactly once.
    session.write_all(b"\x0b\x13");
    let chord = session.read_until("Session status:", Duration::from_secs(4));
    assert_eq!(
        chord.matches("Session status:").count(),
        1,
        "chord dispatched more than once: {chord:?}"
    );

    session.read_until("Shift+Tab mode", Duration::from_secs(4));
    session.wait_for_raw_mode(Duration::from_secs(2));
    session.write_all(b"\x15");
    let command = session.read_until("Session status:", Duration::from_secs(4));
    assert!(
        command.contains("Session status:"),
        "command binding did not execute /status: {command:?}"
    );
}

#[test]
fn keybindings_hot_reload_invalid_retention_and_unlink_restore_defaults() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    write_keybindings_atomic(
        home.path(),
        r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+u":"command:status"}}]}"#,
    );

    let mut session = spawn_terminal(home.path());
    session.read_until("Shift+Tab mode", Duration::from_secs(5));
    session.wait_for_raw_mode(Duration::from_secs(2));
    session.write_all(b"\x15");
    session.read_until("Session status:", Duration::from_secs(4));
    session.read_until("Shift+Tab mode", Duration::from_secs(4));
    session.wait_for_raw_mode(Duration::from_secs(2));

    // Replace through rename so the watcher must handle the same atomic-save
    // pattern used by editors instead of relying on an in-place write.
    write_keybindings_atomic(
        home.path(),
        r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+u":"chat:stash"}}]}"#,
    );
    thread::sleep(Duration::from_millis(900));
    session.write_all(b"hot-reload");
    session.read_until("hot-reload", Duration::from_secs(2));
    session.write_all(b"\x15");
    session.read_until("Prompt stashed", Duration::from_secs(4));

    // A partial/invalid atomic save must not discard the last valid runtime
    // map. The same key should continue to invoke chat:stash.
    write_keybindings_atomic(home.path(), "not valid json");
    thread::sleep(Duration::from_millis(900));
    session.write_all(b"invalid-keeps-old");
    session.read_until("invalid-keeps-old", Duration::from_secs(2));
    session.write_all(b"\x15");
    session.read_until("Prompt stashed", Duration::from_secs(4));

    // Removing the file restores defaults. Ctrl-U is not a default action,
    // so it falls through to the base editor kill operation; Ctrl-Y must then
    // restore the killed text. If the stale chat:stash binding survived, this
    // assertion cannot pass.
    fs::remove_file(keybindings_path(home.path())).unwrap();
    thread::sleep(Duration::from_millis(900));
    session.write_all(b"unlink-default");
    session.read_until("unlink-default", Duration::from_secs(2));
    session.write_all(b"\x15");
    session.read_until("Ctrl+Y to paste deleted text", Duration::from_secs(4));
    session.write_all(b"\x19");
    let restored = session.read_until("unlink-default", Duration::from_secs(3));
    assert!(restored.contains("unlink-default"));
}

#[test]
fn vim_command_switches_modes_and_edits_the_live_composer() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = spawn_terminal(home.path());
    session.read_until("Shift+Tab mode", Duration::from_secs(5));
    session.wait_for_raw_mode(Duration::from_secs(2));

    session.write_all(b"/vim\r");
    let enabled = session.read_until("Editor mode set to vim", Duration::from_secs(4));
    if !enabled.contains("Vim INSERT") {
        session.read_until("Vim INSERT", Duration::from_secs(4));
    }
    session.wait_for_raw_mode(Duration::from_secs(2));

    session.write_all(b"abc\x1b");
    session.read_until("Vim NORMAL", Duration::from_secs(3));
    session.write_all(b"0xiZ\r");
    let committed = session.read_until("Zbc", Duration::from_secs(3));
    assert!(
        committed.contains("Zbc"),
        "Vim edit was not reflected in the committed prompt: {committed:?}"
    );
}

#[test]
fn modal_model_picker_and_todo_toggle_preserve_the_draft() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = spawn_terminal(home.path());
    session.read_until("Shift+Tab mode", Duration::from_secs(5));
    session.wait_for_raw_mode(Duration::from_secs(2));
    session.read_available(Duration::from_millis(100));

    session.write_all(b"draft-kept");
    session.read_until("draft-kept", Duration::from_secs(2));
    session.write_all(b"\x1bp");
    session.read_until("Select model", Duration::from_secs(3));
    session.write_all(b"\x1b");
    let restored = session.read_until("Model selection cancelled", Duration::from_secs(3));
    assert!(
        restored.contains("draft-kept"),
        "Alt-P discarded the current composer draft: {restored:?}"
    );

    session.write_all(b"\x14");
    let opened = session.read_until("No todo items", Duration::from_secs(3));
    assert!(
        opened.contains("draft-kept"),
        "Ctrl-T submitted or cleared the draft: {opened:?}"
    );
    session.write_all(b"\x14");
    let closed = session.read_until("Todo list hidden", Duration::from_secs(3));
    assert!(
        closed.contains("draft-kept"),
        "closing the todo panel lost the draft: {closed:?}"
    );

    session.write_all(b"\x0f");
    session.read_until("transcript compact", Duration::from_secs(3));
    session.write_all(b"\x05");
    session.read_until("transcript all", Duration::from_secs(3));
    session.write_all(b"q");
    let transcript_closed = session.read_until("Returned from transcript", Duration::from_secs(3));
    assert!(
        transcript_closed.contains("draft-kept"),
        "Ctrl-O transcript toggle discarded the draft: {transcript_closed:?}"
    );
}

fn keybindings_path(home: &Path) -> PathBuf {
    home.join(".open-agent-harness/keybindings.json")
}

fn write_keybindings_atomic(home: &Path, contents: &str) {
    use std::os::unix::fs::PermissionsExt;

    let directory = home.join(".open-agent-harness");
    fs::create_dir_all(&directory).unwrap();
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
    let destination = keybindings_path(home);
    let temporary = directory.join(format!(
        ".keybindings-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    fs::write(&temporary, contents).unwrap();
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600)).unwrap();
    fs::rename(&temporary, &destination).unwrap();
}

struct PtySession {
    child: Child,
    terminal: File,
    pending_output: Vec<u8>,
}

impl PtySession {
    fn write_all(&mut self, bytes: &[u8]) {
        self.terminal.write_all(bytes).unwrap();
    }

    fn read_until(&mut self, needle: &str, timeout: Duration) -> String {
        read_until(
            &mut self.terminal,
            &mut self.child,
            &mut self.pending_output,
            needle,
            timeout,
        )
    }

    fn read_available(&mut self, timeout: Duration) -> String {
        read_available(&mut self.terminal, &mut self.pending_output, timeout)
    }

    fn wait_for_raw_mode(&self, timeout: Duration) {
        wait_for_raw_mode(&self.terminal, timeout);
    }

    fn is_running(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }
}

impl Drop for PtySession {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let process_group = -(self.child.id() as i32);
            unsafe {
                libc::kill(process_group, libc::SIGKILL);
            }
            let _ = self.child.kill();
        }
    }
}

fn spawn_terminal(home: &Path) -> PtySession {
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
        .env("HOME", home)
        .env("NO_COLOR", "1")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(unsafe { Stdio::from_raw_fd(slave) })
        .stdout(unsafe { Stdio::from_raw_fd(stdout) })
        .stderr(unsafe { Stdio::from_raw_fd(stderr) });
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
    PtySession {
        child,
        terminal: unsafe { File::from_raw_fd(master) },
        pending_output: Vec::new(),
    }
}

fn read_until(
    terminal: &mut File,
    child: &mut Child,
    pending_output: &mut Vec<u8>,
    needle: &str,
    timeout: Duration,
) -> String {
    let started = Instant::now();
    let mut output = std::mem::take(pending_output);
    let mut buffer = [0u8; 8192];
    while started.elapsed() < timeout {
        if let Some(found) = take_through_needle(&output, pending_output, needle.as_bytes()) {
            return found;
        }
        match terminal.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => output.extend_from_slice(&buffer[..count]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) if error.raw_os_error() == Some(libc::EIO) => {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("terminal read failed: {error}"),
        }
    }
    let status = child.try_wait().unwrap();
    panic!(
        "terminal output did not contain {needle:?}; child_status={status:?}: {}",
        String::from_utf8_lossy(&output)
    )
}

fn take_through_needle(
    output: &[u8],
    pending_output: &mut Vec<u8>,
    needle: &[u8],
) -> Option<String> {
    let position = output
        .windows(needle.len())
        .position(|window| window == needle)?;
    let end = position + needle.len();
    pending_output.extend_from_slice(&output[end..]);
    Some(String::from_utf8_lossy(output).into_owned())
}

fn read_available(terminal: &mut File, pending_output: &mut Vec<u8>, timeout: Duration) -> String {
    let started = Instant::now();
    let mut output = std::mem::take(pending_output);
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

fn serial_terminal_test() -> MutexGuard<'static, ()> {
    static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
    SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

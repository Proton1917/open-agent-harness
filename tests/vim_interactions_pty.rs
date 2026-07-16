#![cfg(unix)]

use std::{
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::process::CommandExt,
    },
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Mutex, MutexGuard, OnceLock},
    thread,
    time::{Duration, Instant},
};

const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn slash_autocomplete_keeps_vim_insert_transaction_and_escape_is_atomic() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    session.write_all(b"/he");
    session.read_until("/help", IO_TIMEOUT);
    session.drain();

    // One Escape must both dismiss the overlay and enter NORMAL. Undo then
    // removes the complete insert transaction, and dot must replay all of it.
    session.write_all(b"\x1b");
    let escaped = session.read_frame();
    assert!(
        !plain_text(&escaped).contains("/help"),
        "slash overlay survived Escape: {escaped:?}"
    );

    session.write_all(b"u");
    let undone = session.read_frame();
    assert_eq!(
        last_composer_text(&undone).as_deref(),
        Some(""),
        "single Escape did not enter NORMAL, or Vim undo missed autocomplete input: {undone:?}"
    );

    session.write_all(b".");
    let repeated = session.read_frame();
    assert_eq!(
        last_composer_text(&repeated).as_deref(),
        Some("/he"),
        "dot-repeat did not retain printable input typed while autocomplete was open: {repeated:?}"
    );
}

#[test]
fn normal_up_delegates_to_history_at_the_buffer_boundary() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    submit_status(&mut session);
    session.write_all(b"\x1b");
    session.read_frame();
    session.drain();

    session.write_all(b"\x1b[A");
    let history = session.read_frame();
    assert_eq!(
        last_composer_text(&history).as_deref(),
        Some("/status"),
        "idle NORMAL Up did not fall back to prompt history: {history:?}"
    );
}

#[test]
fn pending_operator_does_not_cross_prompt_boundaries() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    enter_status_in_normal_mode(&mut session);
    session.write_all(b"d\r");
    session.read_until("Session status:", IO_TIMEOUT);
    session.wait_for_next_prompt();

    // The previous prompt was submitted while `d` awaited a motion. The new
    // prompt may preserve NORMAL mode, but it must start with an idle parser.
    session.write_all(b"iPENDING");
    let input = session.read_frame();
    assert_eq!(
        last_composer_text(&input).as_deref(),
        Some("PENDING"),
        "a pending operator from the submitted prompt consumed new input: {input:?}"
    );
}

#[test]
fn vim_undo_history_does_not_restore_a_previous_prompt() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    enter_status_in_normal_mode(&mut session);
    // Create a non-empty Vim undo snapshot while ending with the same valid
    // local command: /status -> /statu -> /status.
    session.write_all(b"xas");
    session.read_frame();
    session.write_all(b"\x1b");
    session.read_frame();
    session.write_all(b"\r");
    session.read_until("Session status:", IO_TIMEOUT);
    session.wait_for_next_prompt();
    session.drain();

    session.write_all(b"u");
    let undone = session.read_frame();
    assert_eq!(
        last_composer_text(&undone).as_deref(),
        Some(""),
        "Vim undo restored text owned by the previous prompt: {undone:?}"
    );
}

#[test]
fn normal_mode_bracketed_paste_is_not_silently_discarded() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    session.write_all(b"seed\x1b");
    session.read_frame();
    session.drain();
    session.write_all(b"\x1b[200~PASTE\x1b[201~");
    let pasted = session.read_frame();
    let composer = last_composer_text(&pasted).unwrap_or_default();
    assert!(
        composer.contains("PASTE"),
        "NORMAL-mode bracketed paste disappeared without entering the composer: {pasted:?}"
    );
}

#[test]
fn host_undo_and_vim_undo_share_the_current_prompt_transaction() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    session.write_all(b"abc");
    session.read_frame();
    session.drain();
    session.write_all(b"\x1f"); // Ctrl-_ / chat:undo
    let host_undo = session.read_frame();

    session.write_all(b"\x1b");
    session.read_frame();
    session.drain();
    session.write_all(b"u");
    let vim_undo = session.read_frame();

    assert_eq!(
        last_composer_text(&host_undo).as_deref(),
        Some(""),
        "Ctrl-_ did not undo a Vim INSERT transaction: {host_undo:?}"
    );
    assert_eq!(
        last_composer_text(&vim_undo).as_deref(),
        Some(""),
        "Vim undo resurrected a transaction already consumed by Ctrl-_: {vim_undo:?}"
    );
}

#[test]
fn visual_selection_is_inverted_and_the_visible_range_is_edited() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    session.write_all(b"abcd\x1b");
    session.read_frame();
    session.drain();
    session.write_all(b"0vll");
    let selected = session.read_frame();

    session.drain();
    session.write_all(b"d");
    let edited = session.read_frame();

    assert!(
        contains_inverse_sgr(selected.as_bytes()),
        "Visual selection was not represented by inverse-video ANSI: {selected:?}"
    );
    assert_eq!(
        last_composer_text(&edited).as_deref(),
        Some("d"),
        "Visual delete did not edit the range that should have been highlighted: {edited:?}"
    );
}

#[test]
fn vim_mode_footer_is_not_duplicated() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    session.drain();
    session.write_all(b"\x1b");
    let frame = session.read_frame();
    let plain = plain_text(&frame);
    assert!(
        !plain.contains("Vim NORMAL · Vim NORMAL"),
        "mode transition duplicated the Vim footer: {plain:?}"
    );
}

#[test]
fn normal_slash_opens_history_search_instead_of_inserting_text() {
    let _serial = serial_terminal_test();
    let home = tempfile::tempdir().unwrap();
    let mut session = vim_session(home.path());

    submit_status(&mut session);
    session.write_all(b"\x1b");
    session.read_frame();
    session.drain();

    session.write_all(b"/");
    let opened = session.read_until("reverse-i-search", IO_TIMEOUT);
    assert!(
        last_composer_text(&opened).is_none_or(|text| text != "/"),
        "NORMAL / was inserted as ordinary input: {opened:?}"
    );
    session.write_all(b"\x13");
    assert_history_scope(&mut session, "project");
    session.write_all(b"\x13");
    assert_history_scope(&mut session, "everywhere");
    session.write_all(b"\x13");
    assert_history_scope(&mut session, "session");
    session.write_all(b"status\r");
    let executed = session.read_until("Session status:", IO_TIMEOUT);
    assert!(
        executed.contains("Session status:"),
        "history search did not execute the selected prompt: {executed:?}"
    );
}

fn assert_history_scope(session: &mut PtySession, scope: &str) {
    let mut frame = session.read_until("reverse-i-search", IO_TIMEOUT);
    frame.push_str(&session.read_available(Duration::from_millis(250)));
    let plain = plain_text(&frame);
    assert!(
        plain.contains(&format!("· {scope} ·")),
        "history search did not switch to {scope}: {plain:?}"
    );
}

fn vim_session(home: &Path) -> PtySession {
    let mut session = spawn_terminal(home);
    session.read_until("Shift+Tab mode", IO_TIMEOUT);
    session.wait_for_raw_mode(Duration::from_secs(2));
    session.drain();
    session.write_all(b"/vim\r");
    session.read_until("Editor mode set to vim", IO_TIMEOUT);
    session.wait_for_next_prompt();
    session.drain();
    session
}

fn submit_status(session: &mut PtySession) {
    session.write_all(b"/status\r");
    session.read_until("Session status:", IO_TIMEOUT);
    session.wait_for_next_prompt();
    session.drain();
}

fn enter_status_in_normal_mode(session: &mut PtySession) {
    session.write_all(b"/status");
    session.read_until("/status", IO_TIMEOUT);
    session.drain();

    // Tolerate both the desired atomic Escape behavior and the current
    // two-step overlay/mode behavior so lifecycle tests isolate their target.
    session.write_all(b"\x1b");
    session.read_frame();
    session.write_all(b"\x1b");
    session.read_frame();
    session.drain();
}

struct PtySession {
    child: Child,
    terminal: File,
    pending: Vec<u8>,
}

impl PtySession {
    fn write_all(&mut self, bytes: &[u8]) {
        self.terminal.write_all(bytes).unwrap();
        self.terminal.flush().unwrap();
    }

    fn read_until(&mut self, needle: &str, timeout: Duration) -> String {
        let started = Instant::now();
        let needle = needle.as_bytes();
        let mut buffer = [0u8; 8192];
        while started.elapsed() < timeout {
            if let Some(start) = find_bytes(&self.pending, needle) {
                let end = start + needle.len();
                let matched = self.pending.drain(..end).collect::<Vec<_>>();
                return String::from_utf8_lossy(&matched).into_owned();
            }
            match self.terminal.read(&mut buffer) {
                Ok(0) => break,
                Ok(count) => {
                    self.pending.extend_from_slice(&buffer[..count]);
                    if let Some(start) = find_bytes(&self.pending, needle) {
                        let end = start + needle.len();
                        let matched = self.pending.drain(..end).collect::<Vec<_>>();
                        return String::from_utf8_lossy(&matched).into_owned();
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
            "terminal output did not contain {:?}: {}",
            String::from_utf8_lossy(needle),
            String::from_utf8_lossy(&self.pending)
        )
    }

    fn read_available(&mut self, timeout: Duration) -> String {
        let started = Instant::now();
        let mut output = std::mem::take(&mut self.pending);
        let mut buffer = [0u8; 8192];
        while started.elapsed() < timeout {
            match self.terminal.read(&mut buffer) {
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

    fn drain(&mut self) {
        let _ = self.read_available(Duration::from_millis(120));
    }

    fn read_frame(&mut self) -> String {
        self.read_available(Duration::from_millis(700))
    }

    fn wait_for_next_prompt(&mut self) {
        self.read_until("› ", IO_TIMEOUT);
        self.wait_for_raw_mode(Duration::from_secs(2));
    }

    fn wait_for_raw_mode(&self, timeout: Duration) {
        let started = Instant::now();
        while started.elapsed() < timeout {
            let mut state = std::mem::MaybeUninit::<libc::termios>::uninit();
            let result = unsafe { libc::tcgetattr(self.terminal.as_raw_fd(), state.as_mut_ptr()) };
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
}

impl Drop for PtySession {
    fn drop(&mut self) {
        let process_group = -(self.child.id() as i32);
        unsafe {
            libc::kill(process_group, libc::SIGKILL);
        }
        let _ = self.child.kill();

        // SIGKILL is asynchronous. Polling try_wait both bounds teardown and
        // reaps the child, including when an assertion unwinds the test.
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(2) {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => return,
                Ok(None) => thread::sleep(Duration::from_millis(10)),
            }
        }
        unsafe {
            libc::kill(process_group, libc::SIGKILL);
        }
        let _ = self.child.try_wait();
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
    let result = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::addr_of!(size).cast_mut(),
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
        .env("HOME", home)
        .env("TERM", "xterm-256color")
        .env_remove("NO_COLOR")
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
        pending: Vec::new(),
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn plain_text(output: &str) -> String {
    let bytes = output.as_bytes();
    let mut plain = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != 0x1b {
            plain.push(bytes[index]);
            index += 1;
            continue;
        }

        index += 1;
        match bytes.get(index).copied() {
            Some(b'[') => {
                index += 1;
                while let Some(byte) = bytes.get(index).copied() {
                    index += 1;
                    if (0x40..=0x7e).contains(&byte) {
                        break;
                    }
                }
            }
            Some(b']') => {
                index += 1;
                while index < bytes.len() {
                    if bytes[index] == 0x07 {
                        index += 1;
                        break;
                    }
                    if bytes[index] == 0x1b && bytes.get(index + 1) == Some(&b'\\') {
                        index += 2;
                        break;
                    }
                    index += 1;
                }
            }
            Some(_) => index += 1,
            None => {}
        }
    }
    String::from_utf8_lossy(&plain).into_owned()
}

fn last_composer_text(output: &str) -> Option<String> {
    // Suggestion rows also start with `› `. Each redraw clears downward
    // before writing its top rule, composer, bottom rule, and suggestions.
    // Restrict parsing to the last redraw and take its first prompt-prefixed
    // line, which is the composer rather than the selected suggestion.
    let frame = output.rsplit("\x1b[J").next().unwrap_or(output);
    plain_text(frame)
        .lines()
        .find_map(|line| line.strip_prefix("› ").map(ToOwned::to_owned))
}

fn contains_inverse_sgr(output: &[u8]) -> bool {
    let mut index = 0;
    while index + 2 < output.len() {
        if output[index] != 0x1b || output[index + 1] != b'[' {
            index += 1;
            continue;
        }
        let parameters_start = index + 2;
        index = parameters_start;
        while index < output.len() && !(0x40..=0x7e).contains(&output[index]) {
            index += 1;
        }
        if output.get(index) == Some(&b'm') {
            let parameters = &output[parameters_start..index];
            if parameters
                .split(|byte| *byte == b';')
                .any(|parameter| parameter == b"7")
            {
                return true;
            }
        }
        index += 1;
    }
    false
}

fn serial_terminal_test() -> MutexGuard<'static, ()> {
    static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
    SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

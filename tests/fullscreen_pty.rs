#![cfg(unix)]

use std::{
    fs::{self, File},
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::PermissionsExt,
        unix::process::CommandExt,
    },
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Mutex, MutexGuard, OnceLock},
    thread,
    time::{Duration, Instant},
};

#[test]
fn fullscreen_tui_scrolls_and_restores_the_primary_screen() {
    let _serial = serial_terminal_test();
    let clipboard_bin = tempfile::tempdir().unwrap();
    for command in ["pbcopy", "wl-copy", "xclip", "xsel"] {
        let path = clipboard_bin.path().join(command);
        fs::write(&path, "#!/bin/sh\ncat >/dev/null\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let (mut child, mut terminal) = spawn_terminal(clipboard_bin.path());
    let _ = read_until(&mut terminal, "Shift+Tab mode", Duration::from_secs(5));
    wait_for_raw_mode(&mut terminal, Duration::from_secs(2));

    terminal.write_all(b"/tui fullscreen\r").unwrap();
    let entered = read_until(
        &mut terminal,
        "TUI mode: fullscreen",
        Duration::from_secs(3),
    );
    assert!(
        entered.contains("\x1b[?1049h"),
        "alternate screen was not entered"
    );
    assert!(
        entered.contains("\x1b[?1000h"),
        "mouse capture was not enabled"
    );
    wait_for_raw_mode(&mut terminal, Duration::from_secs(2));
    drain_terminal(&mut terminal, Duration::from_millis(100));

    // SGR mouse: double click a visible transcript word and bridge the
    // bounded logical selection to the native clipboard command. Probe the
    // first few content rows because terminals differ in whether the SGR row
    // is reported before or after the header offset.
    let mut clicks = Vec::new();
    for row in 2..=6 {
        clicks.extend_from_slice(format!("\x1b[<0;2;{row}M\x1b[<0;2;{row}m").as_bytes());
        clicks.extend_from_slice(format!("\x1b[<0;2;{row}M\x1b[<0;2;{row}m").as_bytes());
    }
    terminal.write_all(&clicks).unwrap();
    let copied = read_until(
        &mut terminal,
        "Selected transcript text copied",
        Duration::from_secs(3),
    );
    assert!(copied.contains("Selected transcript text copied"));

    for _ in 0..12 {
        wait_for_raw_mode(&mut terminal, Duration::from_secs(2));
        terminal.write_all(b"/tui\r").unwrap();
        let _ = read_until(
            &mut terminal,
            "TUI mode: fullscreen",
            Duration::from_secs(3),
        );
    }

    wait_for_raw_mode(&mut terminal, Duration::from_secs(2));
    terminal.write_all(b"\x1b[5~").unwrap();
    let page_up = read_until(&mut terminal, "You", Duration::from_secs(3));
    assert!(
        page_up.contains("/tui fullscreen"),
        "PageUp did not expose older transcript content: {page_up:?}"
    );

    terminal.write_all(b"/tui\r").unwrap();
    let unseen = read_until(&mut terminal, "new message", Duration::from_secs(3));
    assert!(
        unseen.contains("End to jump to bottom"),
        "scrolled transcript did not expose the unseen-message affordance"
    );

    drain_terminal(&mut terminal, Duration::from_millis(150));
    wait_for_raw_mode(&mut terminal, Duration::from_secs(2));
    terminal.write_all(b"\x1b[F").unwrap();
    let bottom = read_until(
        &mut terminal,
        "TUI mode: fullscreen",
        Duration::from_secs(3),
    );
    assert!(
        !bottom.contains("End to jump to bottom"),
        "End did not restore the sticky bottom"
    );

    terminal.write_all(b"/tui default\r").unwrap();
    let restored = read_until(&mut terminal, "TUI mode: default", Duration::from_secs(3));
    assert!(
        restored.contains("\x1b[?1049l"),
        "alternate screen was not restored"
    );
    assert!(
        restored.contains("\x1b[?1000l"),
        "mouse capture was not disabled"
    );

    wait_for_raw_mode(&mut terminal, Duration::from_secs(2));
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

fn serial_terminal_test() -> MutexGuard<'static, ()> {
    static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
    SERIAL
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn spawn_terminal(clipboard_bin: &Path) -> (Child, File) {
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
    let stdout = unsafe { libc::dup(slave) };
    let stderr = unsafe { libc::dup(slave) };
    assert!(stdout >= 0 && stderr >= 0);

    let mut command = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"));
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut combined_path = clipboard_bin.as_os_str().to_os_string();
    combined_path.push(":");
    combined_path.push(path);
    command
        .args(["--bare", "--no-session-persistence"])
        .env("PATH", combined_path)
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

fn drain_terminal(terminal: &mut File, quiet_for: Duration) {
    let mut quiet_since = Instant::now();
    let mut buffer = [0u8; 8192];
    while quiet_since.elapsed() < quiet_for {
        match terminal.read(&mut buffer) {
            Ok(0) => break,
            Ok(_) => quiet_since = Instant::now(),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
            Err(error) => panic!("terminal read failed: {error}"),
        }
    }
}

fn wait_for_raw_mode(terminal: &mut File, timeout: Duration) {
    let started = Instant::now();
    let mut drain = [0u8; 8192];
    while started.elapsed() < timeout {
        let mut state = std::mem::MaybeUninit::<libc::termios>::uninit();
        let result = unsafe { libc::tcgetattr(terminal.as_raw_fd(), state.as_mut_ptr()) };
        if result == 0 {
            let state = unsafe { state.assume_init() };
            if state.c_lflag & (libc::ICANON | libc::ECHO) == 0 {
                return;
            }
        }
        match terminal.read(&mut drain) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
            Err(error) => panic!("terminal read failed: {error}"),
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("PTY did not enter raw mode before injected input")
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

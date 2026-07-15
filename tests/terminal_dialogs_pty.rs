#![cfg(unix)]

use std::{
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
    time::{Duration, Instant},
};

use open_agent_harness::terminal_dialogs::{AlternateScreenRenderer, DialogFrame};

#[test]
fn bounded_dialog_frame_round_trips_through_a_real_pty() {
    let mut master = -1;
    let mut slave = -1;
    let size = libc::winsize {
        ws_row: 4,
        ws_col: 10,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let opened = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::addr_of!(size).cast_mut(),
        )
    };
    assert_eq!(opened, 0, "{}", std::io::Error::last_os_error());
    let master_file = unsafe { File::from_raw_fd(master) };
    let mut slave_file = unsafe { File::from_raw_fd(slave) };

    let frame = DialogFrame::new(
        vec![
            "0123456789-overflow".to_owned(),
            "second".to_owned(),
            "third".to_owned(),
            "fourth".to_owned(),
            "hidden".to_owned(),
        ],
        None,
        10,
        4,
    );
    assert_eq!(frame.lines().len(), 4);
    let mut renderer = AlternateScreenRenderer::new(10, 4);
    renderer.draw(&mut slave_file, &frame).unwrap();
    renderer.leave(&mut slave_file).unwrap();

    let output = read_pty_until(&master_file, b"\x1b[?1049l", Duration::from_secs(2));
    assert!(output.windows(8).any(|window| window == b"\x1b[?1049h"));
    assert!(output.windows(8).any(|window| window == b"\x1b[?1049l"));
    assert!(output.windows(6).any(|window| window == b"second"));
    assert!(!output.windows(6).any(|window| window == b"hidden"));
}

fn read_pty_until(file: &File, needle: &[u8], timeout: Duration) -> Vec<u8> {
    let deadline = Instant::now() + timeout;
    let mut output = Vec::new();
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout_ms = remaining.as_millis().min(i32::MAX as u128) as i32;
        let mut descriptor = libc::pollfd {
            fd: file.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(std::ptr::addr_of_mut!(descriptor), 1, timeout_ms) };
        if ready <= 0 {
            break;
        }
        let mut buffer = [0_u8; 512];
        let count =
            unsafe { libc::read(file.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if count <= 0 {
            break;
        }
        output.extend_from_slice(&buffer[..count as usize]);
        if output.windows(needle.len()).any(|window| window == needle) {
            break;
        }
    }
    output
}

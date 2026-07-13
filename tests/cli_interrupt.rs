#![cfg(unix)]

use std::{
    io::Read,
    net::TcpListener,
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[test]
fn print_mode_ctrl_c_cancels_the_request_and_exits_130() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_http_request(&mut stream);
        ready_tx.send(()).unwrap();
        let mut buffer = [0u8; 128];
        while stream.read(&mut buffer).unwrap_or(0) != 0 {}
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"))
        .args(["--print", "--no-session-persistence", "wait for model"])
        .env("HARNESS_BASE_URL", format!("http://{address}"))
        .env("HARNESS_API_PATH", "/v1/messages")
        .env("HARNESS_API_FORMAT", "messages")
        .env("HARNESS_STREAM", "1")
        .env("HARNESS_ALLOW_ENV_PROXY", "0")
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // SAFETY: child.id() is the live process spawned directly above; SIGINT is non-destructive
    // and the return value is checked.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGINT) }, 0);

    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("print mode did not exit after SIGINT");
        }
        thread::sleep(Duration::from_millis(10));
    };
    server.join().unwrap();
    assert_eq!(status.code(), Some(130));
}

fn read_http_request(stream: &mut std::net::TcpStream) {
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
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while buffer.len() < header_end + content_length {
        let count = stream.read(&mut chunk).unwrap();
        assert!(count > 0);
        buffer.extend_from_slice(&chunk[..count]);
    }
}

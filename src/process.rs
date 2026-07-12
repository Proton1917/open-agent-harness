pub(crate) fn terminate_process_tree(process_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(group) = process_id {
        // SAFETY: callers place the child in a dedicated process group before spawning it.
        unsafe {
            libc::kill(-(group as i32), libc::SIGKILL);
        }
    }

    #[cfg(windows)]
    if let Some(process_id) = process_id {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    #[cfg(not(any(unix, windows)))]
    let _ = process_id;
}

pub(crate) struct ProcessTreeGuard {
    process_id: Option<u32>,
    armed: bool,
}

impl ProcessTreeGuard {
    pub(crate) fn new(process_id: Option<u32>) -> Self {
        Self {
            process_id,
            armed: true,
        }
    }

    pub(crate) fn terminate(&mut self) {
        if self.armed {
            terminate_process_tree(self.process_id);
            self.armed = false;
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

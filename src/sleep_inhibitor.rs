//! Keeps macOS awake only while an interactive model turn is actively working.
//!
//! A bounded `caffeinate` child is restarted before its own timeout. The child
//! therefore self-expires after a hard parent crash, while normal RAII cleanup
//! stops it immediately. Blocking user interactions temporarily suspend the
//! assertion through [`InteractionWaitObserver`]. Other platforms are no-ops.

use std::{
    ffi::OsString,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant},
};

use crate::interactions::InteractionWaitObserver;

const CAFFEINATE_TIMEOUT_SECONDS: u64 = 300;
const CAFFEINATE_RESTART_AFTER: Duration = Duration::from_secs(4 * 60);
const PROCESS_PROBE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct SleepInhibitor {
    inner: Arc<Inner>,
}

struct Inner {
    enabled: bool,
    command: SleepCommand,
    state: Mutex<State>,
    changed: Condvar,
}

struct SleepCommand {
    program: PathBuf,
    args: Vec<OsString>,
    restart_after: Duration,
    probe_interval: Duration,
}

#[derive(Default)]
struct State {
    active_work: usize,
    waiting_for_user: usize,
    worker_running: bool,
    generation: u64,
}

#[must_use]
pub struct SleepWorkGuard {
    inner: Option<Arc<Inner>>,
}

impl Default for SleepInhibitor {
    fn default() -> Self {
        Self::new()
    }
}

impl SleepInhibitor {
    pub fn new() -> Self {
        Self::configured(
            cfg!(target_os = "macos"),
            PathBuf::from("/usr/bin/caffeinate"),
            vec![
                OsString::from("-i"),
                OsString::from("-t"),
                OsString::from(CAFFEINATE_TIMEOUT_SECONDS.to_string()),
            ],
            CAFFEINATE_RESTART_AFTER,
            PROCESS_PROBE_INTERVAL,
        )
    }

    fn configured(
        enabled: bool,
        program: PathBuf,
        args: Vec<OsString>,
        restart_after: Duration,
        probe_interval: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                enabled,
                command: SleepCommand {
                    program,
                    args,
                    restart_after,
                    probe_interval,
                },
                state: Mutex::new(State::default()),
                changed: Condvar::new(),
            }),
        }
    }

    /// Starts one unit of active work. Nested guards share one child process.
    pub fn start_work(&self) -> SleepWorkGuard {
        let should_spawn = {
            let mut state = lock_state(&self.inner);
            state.active_work = state.active_work.saturating_add(1);
            bump_generation(&mut state);
            if self.inner.enabled && !state.worker_running {
                state.worker_running = true;
                true
            } else {
                false
            }
        };
        self.inner.changed.notify_all();
        if should_spawn {
            spawn_worker(&self.inner);
        }
        SleepWorkGuard {
            inner: Some(Arc::clone(&self.inner)),
        }
    }

    /// Produces a generic observer used by permission and question surfaces.
    pub fn interaction_wait_observer(&self) -> InteractionWaitObserver {
        let begin = self.clone();
        let end = self.clone();
        InteractionWaitObserver::new(move || begin.begin_waiting(), move || end.end_waiting())
    }

    fn begin_waiting(&self) {
        let mut state = lock_state(&self.inner);
        state.waiting_for_user = state.waiting_for_user.saturating_add(1);
        bump_generation(&mut state);
        drop(state);
        self.inner.changed.notify_all();
    }

    fn end_waiting(&self) {
        let mut state = lock_state(&self.inner);
        state.waiting_for_user = state.waiting_for_user.saturating_sub(1);
        bump_generation(&mut state);
        drop(state);
        self.inner.changed.notify_all();
    }

    #[cfg(test)]
    fn counts(&self) -> (usize, usize, bool) {
        let state = lock_state(&self.inner);
        (
            state.active_work,
            state.waiting_for_user,
            state.worker_running,
        )
    }
}

impl Drop for SleepWorkGuard {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut state = lock_state(&inner);
        state.active_work = state.active_work.saturating_sub(1);
        bump_generation(&mut state);
        drop(state);
        inner.changed.notify_all();
    }
}

fn bump_generation(state: &mut State) {
    state.generation = state.generation.wrapping_add(1);
}

fn lock_state(inner: &Inner) -> std::sync::MutexGuard<'_, State> {
    inner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn spawn_worker(inner: &Arc<Inner>) {
    let worker_inner = Arc::clone(inner);
    if thread::Builder::new()
        .name("sleep-inhibitor".to_owned())
        .spawn(move || worker_loop(worker_inner))
        .is_err()
    {
        let mut state = lock_state(inner);
        state.worker_running = false;
        bump_generation(&mut state);
        drop(state);
        inner.changed.notify_all();
    }
}

fn worker_loop(inner: Arc<Inner>) {
    let mut child = None;
    let mut child_started = None;

    loop {
        let (active_work, waiting_for_user, observed_generation) = {
            let state = lock_state(&inner);
            (state.active_work, state.waiting_for_user, state.generation)
        };

        if active_work == 0 {
            stop_child(&mut child);
            child_started = None;
            let mut state = lock_state(&inner);
            if state.active_work == 0 {
                state.worker_running = false;
                return;
            }
            continue;
        }

        if waiting_for_user > 0 {
            stop_child(&mut child);
            child_started = None;
        } else {
            let child_exited = child
                .as_mut()
                .is_some_and(|process| match process.try_wait() {
                    Ok(Some(_)) | Err(_) => true,
                    Ok(None) => false,
                });
            let restart_due = child_started
                .is_some_and(|started: Instant| started.elapsed() >= inner.command.restart_after);
            if child_exited || restart_due {
                stop_child(&mut child);
                child_started = None;
            }
            if child.is_none() {
                child = spawn_child(&inner.command);
                child_started = child.as_ref().map(|_| Instant::now());
            }
        }

        let state = lock_state(&inner);
        if state.generation != observed_generation {
            continue;
        }
        let _ = inner
            .changed
            .wait_timeout(state, inner.command.probe_interval)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }
}

fn spawn_child(command: &SleepCommand) -> Option<Child> {
    Command::new(&command.program)
        .args(&command.args)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

fn stop_child(child: &mut Option<Child>) {
    if let Some(mut process) = child.take() {
        let _ = process.kill();
        let _ = process.wait();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[cfg(unix)]
    use std::{fs, path::Path};

    use super::*;

    #[test]
    fn disabled_runtime_balances_work_and_nested_waits() {
        let inhibitor = SleepInhibitor::configured(
            false,
            PathBuf::from("unused"),
            Vec::new(),
            Duration::from_secs(1),
            Duration::from_millis(10),
        );
        let first = inhibitor.start_work();
        let second = inhibitor.start_work();
        assert_eq!(inhibitor.counts(), (2, 0, false));

        let observer = inhibitor.interaction_wait_observer();
        let outer = observer.enter();
        let inner = observer.enter();
        assert_eq!(inhibitor.counts(), (2, 2, false));
        drop(inner);
        drop(outer);
        drop(first);
        assert_eq!(inhibitor.counts(), (1, 0, false));
        drop(second);
        assert_eq!(inhibitor.counts(), (0, 0, false));
    }

    #[cfg(unix)]
    #[test]
    fn active_process_stops_for_waiting_and_resumes_afterward() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().unwrap();
        let marker = temporary.path().join("pids");
        let script = temporary.path().join("fake-caffeinate");
        fs::write(
            &script,
            "#!/bin/sh\nprintf '%s\\n' \"$$\" >> \"$1\"\nexec /bin/sleep 30\n",
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
        let inhibitor = SleepInhibitor::configured(
            true,
            script,
            vec![marker.as_os_str().to_owned()],
            Duration::from_secs(60),
            Duration::from_millis(10),
        );

        let work = inhibitor.start_work();
        wait_until(|| marker_pids(&marker).len() == 1);
        let first_pid = marker_pids(&marker)[0];
        assert!(process_alive(first_pid));

        let waiting = inhibitor.interaction_wait_observer().enter();
        wait_until(|| !process_alive(first_pid));
        assert_eq!(marker_pids(&marker).len(), 1);

        drop(waiting);
        wait_until(|| marker_pids(&marker).len() >= 2);
        let second_pid = *marker_pids(&marker).last().unwrap();
        assert_ne!(first_pid, second_pid);
        assert!(process_alive(second_pid));

        drop(work);
        wait_until(|| !process_alive(second_pid));
        wait_until(|| !inhibitor.counts().2);
    }

    #[cfg(unix)]
    #[test]
    fn unexpectedly_exited_child_is_restarted() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().unwrap();
        let marker = temporary.path().join("starts");
        let script = temporary.path().join("short-lived-caffeinate");
        fs::write(
            &script,
            "#!/bin/sh\nprintf '%s\\n' \"$$\" >> \"$1\"\nexit 0\n",
        )
        .unwrap();
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700)).unwrap();
        let inhibitor = SleepInhibitor::configured(
            true,
            script,
            vec![marker.as_os_str().to_owned()],
            Duration::from_secs(60),
            Duration::from_millis(10),
        );

        let work = inhibitor.start_work();
        wait_until(|| marker_pids(&marker).len() >= 2);
        drop(work);
        wait_until(|| !inhibitor.counts().2);
    }

    #[cfg(unix)]
    fn marker_pids(path: &Path) -> Vec<i32> {
        fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| line.parse().ok())
            .collect()
    }

    #[cfg(unix)]
    fn process_alive(pid: i32) -> bool {
        // SAFETY: signal 0 does not mutate the target process and `pid` came
        // from the child process itself.
        unsafe { libc::kill(pid, 0) == 0 }
    }

    #[cfg(unix)]
    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(condition(), "condition did not become true before timeout");
    }
}

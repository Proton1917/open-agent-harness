//! Provider-neutral terminal notifications and one bounded idle timer.

use std::{
    io::{self, Write},
    path::Path,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::hooks::HookRunner;

pub const DEFAULT_IDLE_NOTIFICATION_THRESHOLD_MS: u64 = 60_000;
pub const MIN_IDLE_NOTIFICATION_THRESHOLD_MS: u64 = 1_000;
pub const MAX_IDLE_NOTIFICATION_THRESHOLD_MS: u64 = 24 * 60 * 60 * 1_000;
const MAX_NOTIFICATION_TITLE_BYTES: usize = 128;
const MAX_NOTIFICATION_MESSAGE_BYTES: usize = 2 * 1024;
const MAX_NOTIFICATION_TYPE_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationChannel {
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "iterm2")]
    ITerm2,
    #[serde(rename = "iterm2_with_bell")]
    ITerm2WithBell,
    #[serde(rename = "terminal_bell")]
    TerminalBell,
    #[serde(rename = "kitty")]
    Kitty,
    #[serde(rename = "ghostty")]
    Ghostty,
    #[serde(rename = "notifications_disabled")]
    Disabled,
}

impl NotificationChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::ITerm2 => "iterm2",
            Self::ITerm2WithBell => "iterm2_with_bell",
            Self::TerminalBell => "terminal_bell",
            Self::Kitty => "kitty",
            Self::Ghostty => "ghostty",
            Self::Disabled => "notifications_disabled",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "auto" => Ok(Self::Auto),
            "iterm2" => Ok(Self::ITerm2),
            "iterm2_with_bell" => Ok(Self::ITerm2WithBell),
            "terminal_bell" => Ok(Self::TerminalBell),
            "kitty" => Ok(Self::Kitty),
            "ghostty" => Ok(Self::Ghostty),
            "notifications_disabled" => Ok(Self::Disabled),
            _ => bail!(
                "preferredNotifChannel must be auto, iterm2, iterm2_with_bell, terminal_bell, kitty, ghostty, or notifications_disabled"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalNotification {
    pub title: String,
    pub message: String,
    pub notification_type: String,
}

impl TerminalNotification {
    pub fn new(title: &str, message: &str, notification_type: &str) -> Result<Self> {
        Ok(Self {
            title: bounded_text(title, MAX_NOTIFICATION_TITLE_BYTES, "notification title")?,
            message: bounded_text(
                message,
                MAX_NOTIFICATION_MESSAGE_BYTES,
                "notification message",
            )?,
            notification_type: bounded_token(
                notification_type,
                MAX_NOTIFICATION_TYPE_BYTES,
                "notification type",
            )?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationMethod {
    ITerm2,
    ITerm2WithBell,
    Kitty,
    Ghostty,
    TerminalBell,
    Disabled,
    Unavailable,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TerminalEnvironment {
    term_program: String,
    lc_terminal: String,
    term: String,
    tmux: bool,
    screen: bool,
}

impl TerminalEnvironment {
    pub fn from_process() -> Self {
        Self {
            term_program: std::env::var("TERM_PROGRAM").unwrap_or_default(),
            lc_terminal: std::env::var("LC_TERMINAL").unwrap_or_default(),
            term: std::env::var("TERM").unwrap_or_default(),
            tmux: std::env::var_os("TMUX").is_some(),
            screen: std::env::var_os("STY").is_some(),
        }
    }

    fn detected_channel(&self) -> Option<NotificationChannel> {
        let program = self.term_program.to_ascii_lowercase();
        let lc_terminal = self.lc_terminal.to_ascii_lowercase();
        let term = self.term.to_ascii_lowercase();
        if program.contains("iterm") || lc_terminal.contains("iterm") {
            Some(NotificationChannel::ITerm2)
        } else if program.contains("kitty") || term.contains("kitty") {
            Some(NotificationChannel::Kitty)
        } else if program.contains("ghostty") || term.contains("ghostty") {
            Some(NotificationChannel::Ghostty)
        } else {
            None
        }
    }
}

pub fn write_terminal_notification(
    output: &mut impl Write,
    channel: NotificationChannel,
    notification: &TerminalNotification,
    environment: &TerminalEnvironment,
) -> io::Result<NotificationMethod> {
    let (bytes, method) = render_terminal_notification(channel, notification, environment);
    if !bytes.is_empty() {
        output.write_all(&bytes)?;
        output.flush()?;
    }
    Ok(method)
}

pub fn render_terminal_notification(
    channel: NotificationChannel,
    notification: &TerminalNotification,
    environment: &TerminalEnvironment,
) -> (Vec<u8>, NotificationMethod) {
    let channel = match channel {
        NotificationChannel::Auto => match environment.detected_channel() {
            Some(channel) => channel,
            None => return (Vec::new(), NotificationMethod::Unavailable),
        },
        channel => channel,
    };
    let title = sanitize_terminal_text(&notification.title);
    let message = sanitize_terminal_text(&notification.message);
    match channel {
        NotificationChannel::Auto => unreachable!("auto was resolved above"),
        NotificationChannel::ITerm2 => {
            let display = format!("{title}:\n{message}");
            let sequence = osc_bel(&["9", &format!("\n\n{display}")]);
            (
                wrap_for_multiplexer(sequence, environment),
                NotificationMethod::ITerm2,
            )
        }
        NotificationChannel::ITerm2WithBell => {
            let display = format!("{title}:\n{message}");
            let mut sequence =
                wrap_for_multiplexer(osc_bel(&["9", &format!("\n\n{display}")]), environment);
            sequence.push(0x07);
            (sequence, NotificationMethod::ITerm2WithBell)
        }
        NotificationChannel::TerminalBell => (vec![0x07], NotificationMethod::TerminalBell),
        NotificationChannel::Kitty => {
            let id = next_notification_id();
            let mut sequence = Vec::new();
            for item in [
                osc_st(&["99", &format!("i={id}:d=0:p=title"), &title]),
                osc_st(&["99", &format!("i={id}:p=body"), &message]),
                osc_st(&["99", &format!("i={id}:d=1:a=focus"), ""]),
            ] {
                sequence.extend(wrap_for_multiplexer(item, environment));
            }
            (sequence, NotificationMethod::Kitty)
        }
        NotificationChannel::Ghostty => (
            wrap_for_multiplexer(osc_bel(&["777", "notify", &title, &message]), environment),
            NotificationMethod::Ghostty,
        ),
        NotificationChannel::Disabled => (Vec::new(), NotificationMethod::Disabled),
    }
}

fn osc_bel(parts: &[&str]) -> Vec<u8> {
    let mut sequence = format!("\x1b]{}", parts.join(";")).into_bytes();
    sequence.push(0x07);
    sequence
}

fn osc_st(parts: &[&str]) -> Vec<u8> {
    let mut sequence = format!("\x1b]{}", parts.join(";")).into_bytes();
    sequence.extend_from_slice(b"\x1b\\");
    sequence
}

fn wrap_for_multiplexer(sequence: Vec<u8>, environment: &TerminalEnvironment) -> Vec<u8> {
    if environment.tmux {
        let mut wrapped = b"\x1bPtmux;".to_vec();
        for byte in sequence {
            if byte == 0x1b {
                wrapped.push(0x1b);
            }
            wrapped.push(byte);
        }
        wrapped.extend_from_slice(b"\x1b\\");
        wrapped
    } else if environment.screen {
        let mut wrapped = b"\x1bP".to_vec();
        wrapped.extend(sequence);
        wrapped.extend_from_slice(b"\x1b\\");
        wrapped
    } else {
        sequence
    }
}

fn next_notification_id() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};

    static NEXT_ID: AtomicU32 = AtomicU32::new(0);
    NEXT_ID.fetch_add(1, Ordering::Relaxed) % 10_000
}

fn sanitize_terminal_text(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn bounded_text(value: &str, maximum: usize, label: &str) -> Result<String> {
    if value.trim().is_empty() || value.len() > maximum || value.contains('\0') {
        bail!("{label} is empty or exceeds its bounded size")
    }
    Ok(value.to_owned())
}

fn bounded_token(value: &str, maximum: usize, label: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        bail!("{label} is not a bounded identifier")
    }
    Ok(value.to_owned())
}

struct ScheduledNotification {
    deadline: Instant,
    notification: TerminalNotification,
}

#[derive(Default)]
struct SchedulerState {
    pending: Option<ScheduledNotification>,
    generation: u64,
    stopping: bool,
}

struct SchedulerInner {
    state: Mutex<SchedulerState>,
    changed: Condvar,
}

struct DueNotification {
    notification: TerminalNotification,
}

/// Maintains at most one idle deadline and at most one queued terminal event.
pub struct IdleNotificationService {
    inner: Arc<SchedulerInner>,
    due_receiver: Mutex<mpsc::Receiver<DueNotification>>,
    ready_sender: mpsc::SyncSender<TerminalNotification>,
    ready_receiver: Mutex<mpsc::Receiver<TerminalNotification>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl IdleNotificationService {
    pub fn new() -> Self {
        let inner = Arc::new(SchedulerInner {
            state: Mutex::new(SchedulerState::default()),
            changed: Condvar::new(),
        });
        let (due_sender, due_receiver) = mpsc::sync_channel(1);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let worker_inner = Arc::clone(&inner);
        let worker = thread::Builder::new()
            .name("idle-notification".to_owned())
            .spawn(move || scheduler_loop(worker_inner, due_sender))
            .ok();
        Self {
            inner,
            due_receiver: Mutex::new(due_receiver),
            ready_sender,
            ready_receiver: Mutex::new(ready_receiver),
            worker,
        }
    }

    pub fn arm(&self, threshold: Duration, notification: TerminalNotification) -> Result<()> {
        if self.worker.is_none() {
            bail!("idle notification worker is unavailable")
        }
        let threshold_ms = u64::try_from(threshold.as_millis()).unwrap_or(u64::MAX);
        if !(MIN_IDLE_NOTIFICATION_THRESHOLD_MS..=MAX_IDLE_NOTIFICATION_THRESHOLD_MS)
            .contains(&threshold_ms)
        {
            bail!(
                "idle notification threshold must be between {MIN_IDLE_NOTIFICATION_THRESHOLD_MS} and {MAX_IDLE_NOTIFICATION_THRESHOLD_MS} milliseconds"
            )
        }
        let mut state = lock_scheduler(&self.inner);
        state.pending = Some(ScheduledNotification {
            deadline: Instant::now() + threshold,
            notification,
        });
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        self.inner.changed.notify_all();
        Ok(())
    }

    /// Any user input after turn completion cancels that turn's idle notice.
    pub fn record_user_activity(&self) {
        let mut state = lock_scheduler(&self.inner);
        if state.pending.take().is_some() {
            state.generation = state.generation.wrapping_add(1);
            drop(state);
            self.inner.changed.notify_all();
        }
    }

    /// Starts due Notification hooks and returns only hook-complete events.
    pub fn poll(&self, hooks: &Arc<HookRunner>, cwd: &Path) -> Option<TerminalNotification> {
        let due = self
            .due_receiver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .try_recv()
            .ok();
        if let Some(due) = due {
            if hooks.has_event("Notification") {
                let hooks = Arc::clone(hooks);
                let cwd = cwd.to_path_buf();
                let sender = self.ready_sender.clone();
                let notification = due.notification;
                match tokio::runtime::Handle::try_current() {
                    Ok(runtime) => {
                        runtime.spawn(async move {
                            let payload = json!({
                                "title":&notification.title,
                                "message":&notification.message,
                                "notification_type":&notification.notification_type,
                            });
                            let _ = hooks
                                .run(
                                    "Notification",
                                    Some(&notification.notification_type),
                                    payload,
                                    &cwd,
                                )
                                .await;
                            let _ = sender.try_send(notification);
                        });
                    }
                    Err(_) => {
                        let _ = sender.try_send(notification);
                    }
                }
            } else {
                let _ = self.ready_sender.try_send(due.notification);
            }
        }
        self.ready_receiver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .try_recv()
            .ok()
    }

    #[cfg(test)]
    fn has_pending(&self) -> bool {
        lock_scheduler(&self.inner).pending.is_some()
    }
}

impl Default for IdleNotificationService {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for IdleNotificationService {
    fn drop(&mut self) {
        let mut state = lock_scheduler(&self.inner);
        state.stopping = true;
        state.pending = None;
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        self.inner.changed.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn lock_scheduler(inner: &SchedulerInner) -> std::sync::MutexGuard<'_, SchedulerState> {
    inner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn scheduler_loop(inner: Arc<SchedulerInner>, sender: mpsc::SyncSender<DueNotification>) {
    loop {
        let mut state = lock_scheduler(&inner);
        while !state.stopping && state.pending.is_none() {
            state = inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if state.stopping {
            return;
        }
        let Some(pending) = state.pending.as_ref() else {
            continue;
        };
        let generation = state.generation;
        let now = Instant::now();
        if pending.deadline > now {
            let wait = pending.deadline.duration_since(now);
            let _ = inner
                .changed
                .wait_timeout(state, wait)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            continue;
        }
        if state.stopping || state.generation != generation {
            continue;
        }
        let Some(pending) = state.pending.take() else {
            continue;
        };
        state.generation = state.generation.wrapping_add(1);
        drop(state);
        let _ = sender.try_send(DueNotification {
            notification: pending.notification,
        });
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs;

    use super::*;

    fn notification() -> TerminalNotification {
        TerminalNotification::new(
            "Open Agent Harness",
            "The agent is waiting for your input",
            "idle_prompt",
        )
        .unwrap()
    }

    #[test]
    fn explicit_channels_render_exact_bounded_protocols() {
        let environment = TerminalEnvironment::default();
        let (iterm, method) = render_terminal_notification(
            NotificationChannel::ITerm2,
            &notification(),
            &environment,
        );
        assert_eq!(method, NotificationMethod::ITerm2);
        assert!(iterm.starts_with(b"\x1b]9;\n\nOpen Agent Harness:"));
        assert_eq!(iterm.last(), Some(&0x07));

        let (ghostty, method) = render_terminal_notification(
            NotificationChannel::Ghostty,
            &notification(),
            &environment,
        );
        assert_eq!(method, NotificationMethod::Ghostty);
        assert!(ghostty.starts_with(b"\x1b]777;notify;Open Agent Harness;"));

        let (bell, method) = render_terminal_notification(
            NotificationChannel::TerminalBell,
            &notification(),
            &environment,
        );
        assert_eq!(method, NotificationMethod::TerminalBell);
        assert_eq!(bell, b"\x07");

        let (disabled, method) = render_terminal_notification(
            NotificationChannel::Disabled,
            &notification(),
            &environment,
        );
        assert_eq!(method, NotificationMethod::Disabled);
        assert!(disabled.is_empty());
    }

    #[test]
    fn auto_detects_supported_terminals_and_unknown_is_silent() {
        for (program, term, expected) in [
            ("iTerm.app", "xterm-256color", NotificationMethod::ITerm2),
            ("", "xterm-kitty", NotificationMethod::Kitty),
            ("ghostty", "xterm-256color", NotificationMethod::Ghostty),
        ] {
            let environment = TerminalEnvironment {
                term_program: program.to_owned(),
                term: term.to_owned(),
                ..TerminalEnvironment::default()
            };
            assert_eq!(
                render_terminal_notification(
                    NotificationChannel::Auto,
                    &notification(),
                    &environment,
                )
                .1,
                expected
            );
        }
        assert_eq!(
            render_terminal_notification(
                NotificationChannel::Auto,
                &notification(),
                &TerminalEnvironment::default(),
            ),
            (Vec::new(), NotificationMethod::Unavailable)
        );
    }

    #[test]
    fn kitty_uses_st_and_tmux_passthrough_doubles_inner_escape() {
        let environment = TerminalEnvironment {
            term: "xterm-kitty".to_owned(),
            tmux: true,
            ..TerminalEnvironment::default()
        };
        let (rendered, method) =
            render_terminal_notification(NotificationChannel::Kitty, &notification(), &environment);
        assert_eq!(method, NotificationMethod::Kitty);
        assert!(rendered.starts_with(b"\x1bPtmux;\x1b\x1b]99;"));
        assert!(rendered.windows(3).any(|window| window == b"\x1b\x1b\\"));
        assert!(rendered.ends_with(b"\x1b\\"));
    }

    #[test]
    fn terminal_control_injection_is_removed() {
        let notification = TerminalNotification::new("bad\x1b]2;x", "ring\x07now", "safe").unwrap();
        let (rendered, _) = render_terminal_notification(
            NotificationChannel::Ghostty,
            &notification,
            &TerminalEnvironment::default(),
        );
        let text = String::from_utf8(rendered).unwrap();
        assert!(!text.contains("\x1b]2"));
        assert!(!text.contains("ring\x07now"));
    }

    #[tokio::test]
    async fn idle_timer_is_replaceable_cancelled_by_activity_and_delivered_once() {
        let service = IdleNotificationService::new();
        let hooks = Arc::new(HookRunner::default());
        service
            .arm(Duration::from_millis(1_000), notification())
            .unwrap();
        assert!(service.has_pending());
        service.record_user_activity();
        assert!(!service.has_pending());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(service.poll(&hooks, Path::new(".")).is_none());

        let replacement =
            TerminalNotification::new("Open Agent Harness", "replacement", "idle_prompt").unwrap();
        service
            .arm(Duration::from_millis(1_000), notification())
            .unwrap();
        service
            .arm(Duration::from_millis(1_000), replacement.clone())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1_050)).await;
        assert_eq!(service.poll(&hooks, Path::new(".")), Some(replacement));
        assert!(service.poll(&hooks, Path::new(".")).is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn notification_hook_finishes_before_terminal_delivery() {
        use crate::config::Settings;

        let temporary = tempfile::tempdir().unwrap();
        let marker = temporary.path().join("notification-hook-ran");
        let settings = Settings {
            raw: json!({"hooks":{"Notification":[{
                "matcher":"idle_prompt",
                "hooks":[{
                    "type":"command",
                    "command":format!("printf done > '{}'", marker.display()),
                }]
            }]}}),
        };
        let hooks = Arc::new(HookRunner::from_settings(&settings).unwrap());
        let service = IdleNotificationService::new();
        service
            .arm(Duration::from_millis(1_000), notification())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1_050)).await;

        let deadline = Instant::now() + Duration::from_secs(3);
        let delivered = loop {
            if let Some(delivered) = service.poll(&hooks, temporary.path()) {
                break delivered;
            }
            assert!(Instant::now() < deadline, "notification hook timed out");
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(delivered, notification());
        assert_eq!(fs::read_to_string(marker).unwrap(), "done");
    }

    #[test]
    fn invalid_channels_thresholds_and_payloads_fail_closed() {
        assert!(NotificationChannel::parse("shell-command").is_err());
        assert!(TerminalNotification::new("", "message", "idle_prompt").is_err());
        assert!(TerminalNotification::new("title", "message", "bad type").is_err());
        let service = IdleNotificationService::new();
        assert!(
            service
                .arm(Duration::from_millis(999), notification())
                .is_err()
        );
        assert!(
            service
                .arm(
                    Duration::from_millis(MAX_IDLE_NOTIFICATION_THRESHOLD_MS + 1),
                    notification(),
                )
                .is_err()
        );
    }

    #[test]
    fn writer_flushes_only_nonempty_sequences() {
        let mut output = Vec::new();
        let method = write_terminal_notification(
            &mut output,
            NotificationChannel::Disabled,
            &notification(),
            &TerminalEnvironment::default(),
        )
        .unwrap();
        assert_eq!(method, NotificationMethod::Disabled);
        assert!(output.is_empty());
    }
}

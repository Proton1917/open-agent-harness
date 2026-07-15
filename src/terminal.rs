use std::{
    collections::VecDeque,
    io::{self, IsTerminal, Read, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Result;
use crossterm::{
    cursor,
    event::{
        self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, KeyboardEnhancementFlags,
        MouseButton, MouseEventKind, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
    },
    execute, queue,
    style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor},
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{
    clipboard::{ClipboardImage, read_clipboard_image, write_clipboard_text},
    config::ModelOption,
    fullscreen::{
        ClickKind, FrameSpec, FullscreenLimits, FullscreenState, SelectionFocusMove, ViewportPoint,
        WheelDirection,
    },
    input_history::HistoryScope,
    keybindings::{KeyResolution, KeybindingManager},
    permissions::PermissionMode,
    query::QueryEvent,
    ui_settings::ThemePreset,
    vim::{VimAction, VimEvent, VimKey, VimMode, VimState},
};

const EXIT_WINDOW: Duration = Duration::from_millis(800);
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_VISIBLE_INPUT_LINES: usize = 10;
const MAX_FILE_CANDIDATES_SCANNED: usize = 4_096;
const MAX_FILE_SUGGESTIONS: usize = 100;
const RAW_LINE_END: &str = "\r\n";
const SYNC_OUTPUT_START: &[u8] = b"\x1b[?2026h";
const SYNC_OUTPUT_END: &[u8] = b"\x1b[?2026l";
const KILL_RING_LIMIT: usize = 10;
const MAX_HISTORY_SEARCH_QUERY_BYTES: usize = 4 * 1024;
const MAX_HISTORY_SEARCH_ENTRY_BYTES: usize = 64 * 1024;
const CLEAR_INPUT_WINDOW: Duration = Duration::from_secs(2);
const MAX_CLIPBOARD_ATTACHMENTS: usize = 8;
const MAX_CLIPBOARD_ATTACHMENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_FULLSCREEN_STREAM_BYTES: usize = 64 * 1024;
const MAX_PERMISSION_PREVIEW_BYTES: usize = 64 * 1024;
const FULLSCREEN_CLICK_WINDOW: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitKey {
    CtrlC,
    CtrlD,
}

#[derive(Debug, Clone, Copy)]
struct ExitPending {
    key: ExitKey,
    armed_at: Instant,
}

impl ExitPending {
    fn new(key: ExitKey, now: Instant) -> Self {
        Self { key, armed_at: now }
    }

    fn remaining(self, now: Instant) -> Option<Duration> {
        EXIT_WINDOW
            .checked_sub(now.saturating_duration_since(self.armed_at))
            .filter(|remaining| !remaining.is_zero())
    }

    fn active_for(self, key: ExitKey, now: Instant) -> bool {
        self.key == key && self.remaining(now).is_some()
    }

    fn hint(self) -> &'static str {
        match self.key {
            ExitKey::CtrlC => "Press Ctrl-C again to exit",
            ExitKey::CtrlD => "Press Ctrl-D again to exit",
        }
    }
}

fn arm_or_confirm_exit(pending: &mut Option<ExitPending>, key: ExitKey, now: Instant) -> bool {
    if pending.is_some_and(|armed| armed.active_for(key, now)) {
        return true;
    }
    *pending = Some(ExitPending::new(key, now));
    false
}

#[derive(Clone)]
pub struct ConversationUi {
    inner: Arc<Mutex<OutputState>>,
    color: bool,
}

struct OutputState {
    assistant_open: bool,
    status_open: bool,
    fullscreen: FullscreenState,
    fullscreen_guard: Option<AlternateScreenGuard>,
    fullscreen_header: String,
    fullscreen_stream: String,
    fullscreen_composer_reserve: u16,
}

impl Default for OutputState {
    fn default() -> Self {
        Self {
            assistant_open: false,
            status_open: false,
            fullscreen: FullscreenState::new(24, 80, 1, FullscreenLimits::default()),
            fullscreen_guard: None,
            fullscreen_header: "open-agent-harness".to_owned(),
            fullscreen_stream: String::new(),
            fullscreen_composer_reserve: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuiMode {
    Default,
    Fullscreen,
}

#[derive(Debug, Clone, Copy)]
enum FullscreenScroll {
    PageUp,
    PageDown,
    Top,
    Bottom,
    WheelUp(Duration),
    WheelDown(Duration),
}

impl TuiMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Fullscreen => "fullscreen",
        }
    }
}

impl ConversationUi {
    pub fn detect() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OutputState::default())),
            color: io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none(),
        }
    }

    pub fn interactive(&self) -> bool {
        io::stdin().is_terminal() && io::stdout().is_terminal()
    }

    pub fn tui_mode(&self) -> TuiMode {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.fullscreen_guard.is_some() {
            TuiMode::Fullscreen
        } else {
            TuiMode::Default
        }
    }

    pub fn set_tui_mode(&self, mode: TuiMode) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match mode {
            TuiMode::Default => {
                state.fullscreen_composer_reserve = 1;
                state.fullscreen.set_composer_reserve(1);
                drop(state.fullscreen_guard.take());
            }
            TuiMode::Fullscreen => {
                if state.fullscreen_guard.is_none() {
                    state.fullscreen_guard = Some(AlternateScreenGuard::enter()?);
                }
                render_fullscreen_locked(&mut state, None)?;
            }
        }
        Ok(())
    }

    pub fn set_fullscreen_header(&self, header: String) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.fullscreen_header = single_line(&header, 512);
        if state.fullscreen_guard.is_some() {
            render_fullscreen_locked(&mut state, None)?;
        }
        Ok(())
    }

    pub fn replace_fullscreen_transcript(&self, lines: &[String]) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.fullscreen.clear();
        for line in lines {
            state.fullscreen.push_message(line);
        }
        if state.fullscreen_guard.is_some() {
            render_fullscreen_locked(&mut state, None)?;
        }
        Ok(())
    }

    pub fn record_user_input(&self, input: &str) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.fullscreen.push_message(&format!("You\n{input}"));
        if state.fullscreen_guard.is_some() {
            state.fullscreen_composer_reserve = 1;
            state.fullscreen.set_composer_reserve(1);
            render_fullscreen_locked(&mut state, None)?;
        }
        Ok(())
    }

    fn fullscreen_active(&self) -> bool {
        self.tui_mode() == TuiMode::Fullscreen
    }

    fn render_fullscreen_prompt(&self, composer: &[u8], reserve: u16) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.fullscreen_guard.is_none() {
            return Ok(());
        }
        state.fullscreen_composer_reserve = reserve.max(1);
        let reserve = state.fullscreen_composer_reserve;
        state.fullscreen.set_composer_reserve(reserve);
        render_fullscreen_locked(&mut state, Some(composer))
    }

    fn fullscreen_scroll(&self, direction: FullscreenScroll) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.fullscreen_guard.is_none() {
            return Ok(());
        }
        match direction {
            FullscreenScroll::PageUp => state.fullscreen.scroll_half_page(WheelDirection::Up),
            FullscreenScroll::PageDown => state.fullscreen.scroll_half_page(WheelDirection::Down),
            FullscreenScroll::Top => state.fullscreen.scroll_to_top(),
            FullscreenScroll::Bottom => state.fullscreen.scroll_to_bottom(),
            FullscreenScroll::WheelUp(at) => {
                state.fullscreen.clear_selection();
                state.fullscreen.wheel(WheelDirection::Up, at);
            }
            FullscreenScroll::WheelDown(at) => {
                state.fullscreen.clear_selection();
                state.fullscreen.wheel(WheelDirection::Down, at);
            }
        }
        Ok(())
    }

    fn fullscreen_selection_start(&self, row: u16, column: u16, kind: ClickKind) -> bool {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.fullscreen_guard.is_none() || row == 0 {
            return false;
        }
        state.fullscreen.click(
            ViewportPoint {
                row: usize::from(row - 1),
                column: usize::from(column),
            },
            kind,
        )
    }

    fn fullscreen_selection_drag(&self, row: u16, column: u16) -> bool {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.fullscreen_guard.is_none() || row == 0 {
            return false;
        }
        state.fullscreen.drag_to(ViewportPoint {
            row: usize::from(row - 1),
            column: usize::from(column),
        })
    }

    fn fullscreen_selection_finish(&self) -> Option<String> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.fullscreen.finish_selection();
        state.fullscreen.selected_text()
    }

    fn fullscreen_has_selection(&self) -> bool {
        let state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.fullscreen.has_selection()
    }

    fn fullscreen_selection_move(&self, movement: SelectionFocusMove) -> bool {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.fullscreen.move_selection_focus(movement)
    }

    fn fullscreen_selection_clear(&self) -> bool {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let had_selection = state.fullscreen.has_selection();
        state.fullscreen.clear_selection();
        had_selection
    }

    fn fullscreen_selection_take(&self) -> Option<String> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let selected = state.fullscreen.selected_text();
        state.fullscreen.clear_selection();
        selected
    }

    fn resize_fullscreen(&self, columns: u16, rows: u16) -> Result<()> {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.fullscreen_guard.is_none() {
            return Ok(());
        }
        state.fullscreen.resize(rows, columns);
        Ok(())
    }

    pub fn banner(
        &self,
        model: &str,
        cwd: &std::path::Path,
        session: uuid::Uuid,
        mode: PermissionMode,
    ) -> Result<()> {
        let width = terminal::size()
            .map(|(width, _)| usize::from(width).clamp(42, 92))
            .unwrap_or(72);
        let rule = "─".repeat(width.saturating_sub(2));
        let mut out = io::stdout().lock();
        if self.color {
            queue!(
                out,
                SetForegroundColor(Color::Cyan),
                SetAttribute(Attribute::Bold)
            )?;
        }
        queue!(out, Print(format!("╭{rule}╮\n")))?;
        write_box_line(
            &mut out,
            &format!("  open-agent-harness  v{}", env!("CARGO_PKG_VERSION")),
            width,
        )?;
        if self.color {
            queue!(out, ResetColor, SetAttribute(Attribute::Reset))?;
        }
        write_field(&mut out, "model", model, width)?;
        write_field(&mut out, "cwd", &cwd.display().to_string(), width)?;
        write_field(&mut out, "session", &session.to_string(), width)?;
        write_field(&mut out, "mode", mode_label(mode), width)?;
        if self.color {
            queue!(out, SetForegroundColor(Color::DarkGrey))?;
        }
        queue!(out, Print(format!("╰{rule}╯\n")))?;
        if self.color {
            queue!(out, ResetColor)?;
        }
        queue!(
            out,
            Print("  /help for commands · Shift+Tab changes mode\n\n")
        )?;
        out.flush()?;
        Ok(())
    }

    pub fn event(&self, event: &QueryEvent) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        apply_fullscreen_event(&mut state, event);
        if state.fullscreen_guard.is_some() {
            let _ = render_fullscreen_locked(&mut state, None);
            return;
        }
        let mut out = io::stdout().lock();
        match event {
            QueryEvent::TurnStarted => {
                clear_status(&mut out, &mut state);
            }
            QueryEvent::RequestStarted { round } => {
                clear_status(&mut out, &mut state);
                let label = if *round == 1 {
                    "Working…".to_owned()
                } else {
                    format!("Continuing · round {round}…")
                };
                let _ = styled_status(&mut out, self.color, &label);
                state.status_open = true;
            }
            QueryEvent::AssistantMessage { .. } => {}
            QueryEvent::CheckpointCreated { .. } => {}
            QueryEvent::ToolStarted { name, summary, .. } => {
                clear_status(&mut out, &mut state);
                close_assistant(&mut out, &mut state);
                let summary = single_line(summary, 120);
                if self.color {
                    let _ = queue!(out, SetForegroundColor(Color::Cyan));
                }
                let _ = queue!(out, Print("  ● "), Print(name));
                if self.color {
                    let _ = queue!(out, ResetColor, SetForegroundColor(Color::DarkGrey));
                }
                if !summary.is_empty() {
                    let _ = queue!(out, Print(" "), Print(summary));
                }
                if self.color {
                    let _ = queue!(out, ResetColor);
                }
                let _ = queue!(out, Print("\n"));
            }
            QueryEvent::ToolFinished {
                name,
                preview,
                collapsed,
                is_error,
                elapsed_ms,
                ..
            } => {
                clear_status(&mut out, &mut state);
                close_assistant(&mut out, &mut state);
                if self.color {
                    let _ = queue!(
                        out,
                        SetForegroundColor(if *is_error { Color::Red } else { Color::Green })
                    );
                }
                let symbol = if *is_error { "✗" } else { "✓" };
                let _ = queue!(out, Print(format!("    ╰─ {symbol} {name} ")));
                if self.color {
                    let _ = queue!(out, SetForegroundColor(Color::DarkGrey));
                }
                let _ = queue!(out, Print(format_duration(*elapsed_ms)));
                let preview = single_line(preview, 100);
                if !preview.is_empty() {
                    let _ = queue!(out, Print(" · "), Print(preview));
                }
                if *collapsed {
                    let _ = queue!(out, Print(" (Ctrl-O to expand)"));
                }
                if self.color {
                    let _ = queue!(out, ResetColor);
                }
                let _ = queue!(out, Print("\n"));
            }
            QueryEvent::CompactStarted => {
                clear_status(&mut out, &mut state);
                close_assistant(&mut out, &mut state);
                let _ = styled_status(&mut out, self.color, "Compressing context…");
                state.status_open = true;
            }
            QueryEvent::CompactFinished {
                before_tokens,
                after_tokens,
            } => {
                clear_status(&mut out, &mut state);
                let _ = muted_line(
                    &mut out,
                    self.color,
                    &format!("  ✓ Context {before_tokens} → {after_tokens} estimated tokens"),
                );
            }
            QueryEvent::TurnFinished => {
                clear_status(&mut out, &mut state);
                close_assistant(&mut out, &mut state);
                let _ = queue!(out, Print("\n"));
            }
            QueryEvent::TurnInterrupted => {
                clear_status(&mut out, &mut state);
                close_assistant(&mut out, &mut state);
                let _ = muted_line(&mut out, self.color, "  ■ Interrupted");
                let _ = queue!(out, Print("\n"));
            }
            QueryEvent::TurnFailed { message } => {
                clear_status(&mut out, &mut state);
                close_assistant(&mut out, &mut state);
                if self.color {
                    let _ = queue!(out, SetForegroundColor(Color::Red));
                }
                let _ = queue!(
                    out,
                    Print("  Error: "),
                    Print(single_line(message, 180)),
                    Print("\n\n")
                );
                if self.color {
                    let _ = queue!(out, ResetColor);
                }
            }
        }
        let _ = out.flush();
    }

    pub fn text_delta(&self, delta: &str) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        append_bounded_fullscreen_stream(&mut state, delta);
        if state.fullscreen_guard.is_some() {
            let _ = render_fullscreen_locked(&mut state, None);
            return;
        }
        let mut out = io::stdout().lock();
        clear_status(&mut out, &mut state);
        if !state.assistant_open {
            if self.color {
                let _ = queue!(
                    out,
                    SetForegroundColor(Color::Cyan),
                    SetAttribute(Attribute::Bold)
                );
            }
            let _ = queue!(out, Print("◆ "));
            if self.color {
                let _ = queue!(out, ResetColor, SetAttribute(Attribute::Reset));
            }
            state.assistant_open = true;
        }
        let _ = queue!(out, Print(sanitize_multiline(delta)));
        let _ = out.flush();
    }

    pub fn response(&self, text: &str) -> Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        self.text_delta(text);
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        finish_fullscreen_stream(&mut state);
        if state.fullscreen_guard.is_some() {
            render_fullscreen_locked(&mut state, None)?;
            return Ok(());
        }
        let mut out = io::stdout().lock();
        close_assistant(&mut out, &mut state);
        queue!(out, Print("\n"))?;
        out.flush()?;
        Ok(())
    }
}

fn render_fullscreen_locked(state: &mut OutputState, composer: Option<&[u8]>) -> Result<()> {
    let (columns, rows) = terminal::size().unwrap_or((80, 24));
    state.fullscreen.resize(rows, columns);
    state
        .fullscreen
        .set_composer_reserve(state.fullscreen_composer_reserve.max(1));
    let rendered = state
        .fullscreen
        .render_ansi(FrameSpec::new(&state.fullscreen_header, &[]));
    let mut frame = Vec::with_capacity(rendered.bytes.len().saturating_add(512));
    let synchronized = synchronized_output_supported();
    if synchronized {
        frame.extend_from_slice(SYNC_OUTPUT_START);
    }
    frame.extend_from_slice(rendered.bytes.as_bytes());
    if let Some(composer) = composer {
        let composer_row = rendered
            .rows
            .saturating_sub(usize::from(state.fullscreen_composer_reserve))
            .min(usize::from(u16::MAX)) as u16;
        queue!(frame, cursor::MoveTo(0, composer_row), cursor::Show)?;
        frame.extend_from_slice(composer);
    } else {
        queue!(frame, cursor::Hide)?;
    }
    if synchronized {
        frame.extend_from_slice(SYNC_OUTPUT_END);
    }
    let mut out = io::stdout().lock();
    out.write_all(&frame)?;
    out.flush()?;
    Ok(())
}

fn finish_fullscreen_stream(state: &mut OutputState) {
    state.fullscreen.commit_streaming_line();
    state.fullscreen_stream.clear();
}

fn append_bounded_fullscreen_stream(state: &mut OutputState, delta: &str) {
    let delta = sanitize_multiline(delta);
    let available = MAX_FULLSCREEN_STREAM_BYTES.saturating_sub(state.fullscreen_stream.len());
    let mut end = available.min(delta.len());
    while !delta.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    state.fullscreen_stream.push_str(&delta[..end]);
    let visible = format!("Assistant · {}", state.fullscreen_stream);
    state.fullscreen.set_streaming_line(Some(&visible));
}

fn apply_fullscreen_event(state: &mut OutputState, event: &QueryEvent) {
    match event {
        QueryEvent::TurnStarted => {
            finish_fullscreen_stream(state);
            state.fullscreen.set_status(None);
        }
        QueryEvent::RequestStarted { round } => {
            let status = if *round == 1 {
                "Working…".to_owned()
            } else {
                format!("Continuing · round {round}…")
            };
            state.fullscreen.set_status(Some(&status));
        }
        QueryEvent::AssistantMessage { content } => {
            if !state.fullscreen_stream.is_empty() {
                let complete = assistant_content_text(content);
                state.fullscreen.set_streaming_line(None);
                state.fullscreen_stream.clear();
                if !complete.is_empty() {
                    state
                        .fullscreen
                        .push_message(&format!("Assistant · {complete}"));
                }
            }
        }
        QueryEvent::CheckpointCreated { .. } => {}
        QueryEvent::ToolStarted { name, summary, .. } => {
            finish_fullscreen_stream(state);
            state.fullscreen.set_status(None);
            let summary = single_line(summary, 120);
            let line = if summary.is_empty() {
                format!("● {name}")
            } else {
                format!("● {name} · {summary}")
            };
            state.fullscreen.push_message(&line);
        }
        QueryEvent::ToolFinished {
            name,
            preview,
            collapsed,
            is_error,
            elapsed_ms,
            ..
        } => {
            let symbol = if *is_error { "✗" } else { "✓" };
            let preview = single_line(preview, 100);
            let mut suffix = if preview.is_empty() {
                String::new()
            } else {
                format!(" · {preview}")
            };
            if *collapsed {
                suffix.push_str(" (Ctrl-O to expand)");
            }
            state.fullscreen.push_message(&format!(
                "  ╰─ {symbol} {name} {}{suffix}",
                format_duration(*elapsed_ms)
            ));
        }
        QueryEvent::CompactStarted => {
            finish_fullscreen_stream(state);
            state.fullscreen.set_status(Some("Compressing context…"));
        }
        QueryEvent::CompactFinished {
            before_tokens,
            after_tokens,
        } => {
            state.fullscreen.set_status(None);
            state.fullscreen.push_message(&format!(
                "✓ Context {before_tokens} → {after_tokens} estimated tokens"
            ));
        }
        QueryEvent::TurnFinished => {
            finish_fullscreen_stream(state);
            state.fullscreen.set_status(None);
        }
        QueryEvent::TurnInterrupted => {
            finish_fullscreen_stream(state);
            state.fullscreen.set_status(None);
            state.fullscreen.push_message("■ Interrupted");
        }
        QueryEvent::TurnFailed { message } => {
            finish_fullscreen_stream(state);
            state.fullscreen.set_status(None);
            state
                .fullscreen
                .push_message(&format!("Error: {}", single_line(message, 180)));
        }
    }
}

fn assistant_content_text(content: &[serde_json::Value]) -> String {
    content
        .iter()
        .filter(|block| block.get("type").and_then(serde_json::Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join("")
}

pub struct InputEditor {
    history: Vec<String>,
    project_history: Vec<String>,
    everywhere_history: Vec<String>,
    history_limit: usize,
    stashed_prompt: Option<EditorSnapshot>,
    stashed_clipboard_images: Vec<ClipboardImage>,
    keybindings: KeybindingManager,
    vim: Option<VimState>,
    ui: Option<ConversationUi>,
    fullscreen_wheel_epoch: Instant,
}

enum BindingDispatch {
    Key(KeyEvent),
    Command(String),
    FullscreenScroll(FullscreenScroll),
    CopySelection,
    RedrawOrClear,
    ClearScreen,
    PasteImage,
    Unsupported(String),
}

fn canonical_key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
    KeyEvent::new(code, modifiers)
}

fn dispatch_binding(action: String) -> BindingDispatch {
    if let Some(command) = action.strip_prefix("command:") {
        return BindingDispatch::Command(format!("/{command}"));
    }
    let key = match action.as_str() {
        "app:interrupt" => canonical_key(KeyCode::Char('c'), KeyModifiers::CONTROL),
        "app:exit" => canonical_key(KeyCode::Char('d'), KeyModifiers::CONTROL),
        "app:toggleTodos" => canonical_key(KeyCode::Char('t'), KeyModifiers::CONTROL),
        "app:toggleTranscript" => canonical_key(KeyCode::Char('o'), KeyModifiers::CONTROL),
        "history:search" | "historySearch:next" => {
            canonical_key(KeyCode::Char('r'), KeyModifiers::CONTROL)
        }
        "history:previous" | "autocomplete:previous" => {
            canonical_key(KeyCode::Up, KeyModifiers::NONE)
        }
        "history:next" | "autocomplete:next" => canonical_key(KeyCode::Down, KeyModifiers::NONE),
        "chat:cancel" | "autocomplete:dismiss" | "historySearch:accept" => {
            canonical_key(KeyCode::Esc, KeyModifiers::NONE)
        }
        "chat:cycleMode" => canonical_key(KeyCode::BackTab, KeyModifiers::SHIFT),
        "chat:modelPicker" => canonical_key(KeyCode::Char('p'), KeyModifiers::ALT),
        "chat:submit" | "historySearch:execute" => {
            canonical_key(KeyCode::Enter, KeyModifiers::NONE)
        }
        "chat:newline" => canonical_key(KeyCode::Char('j'), KeyModifiers::CONTROL),
        "chat:undo" => canonical_key(KeyCode::Char('_'), KeyModifiers::CONTROL),
        "chat:externalEditor" => canonical_key(KeyCode::Char('g'), KeyModifiers::CONTROL),
        "chat:stash" => canonical_key(KeyCode::Char('s'), KeyModifiers::CONTROL),
        "autocomplete:accept" => canonical_key(KeyCode::Tab, KeyModifiers::NONE),
        "historySearch:cancel" => canonical_key(KeyCode::Char('c'), KeyModifiers::CONTROL),
        "historySearch:cycleScope" => canonical_key(KeyCode::Char('s'), KeyModifiers::CONTROL),
        "chat:clearInput" | "app:redraw" => return BindingDispatch::RedrawOrClear,
        "chat:clearScreen" => return BindingDispatch::ClearScreen,
        "chat:imagePaste" => return BindingDispatch::PasteImage,
        "scroll:pageUp" => return BindingDispatch::FullscreenScroll(FullscreenScroll::PageUp),
        "scroll:pageDown" => {
            return BindingDispatch::FullscreenScroll(FullscreenScroll::PageDown);
        }
        "scroll:top" => return BindingDispatch::FullscreenScroll(FullscreenScroll::Top),
        "scroll:bottom" => return BindingDispatch::FullscreenScroll(FullscreenScroll::Bottom),
        "selection:copy" => return BindingDispatch::CopySelection,
        _ => return BindingDispatch::Unsupported(action),
    };
    BindingDispatch::Key(key)
}

fn vim_event(key: KeyEvent) -> Option<VimEvent> {
    if key
        .modifiers
        .intersects(KeyModifiers::SUPER | KeyModifiers::HYPER)
    {
        return None;
    }
    let mut control = key.modifiers.contains(KeyModifiers::CONTROL);
    let mut alt = key.modifiers.contains(KeyModifiers::ALT);
    let mut shift = key.modifiers.contains(KeyModifiers::SHIFT);
    let key = match key.code {
        KeyCode::Char(character) => VimKey::Char(character),
        KeyCode::Esc => VimKey::Escape,
        KeyCode::Enter if shift || alt => {
            control = false;
            alt = false;
            shift = false;
            VimKey::Newline
        }
        KeyCode::Enter => VimKey::Enter,
        KeyCode::Backspace => VimKey::Backspace,
        KeyCode::Delete => VimKey::Delete,
        KeyCode::Left => VimKey::Left,
        KeyCode::Right => VimKey::Right,
        KeyCode::Up => VimKey::Up,
        KeyCode::Down => VimKey::Down,
        KeyCode::Home => VimKey::Home,
        KeyCode::End => VimKey::End,
        _ => return None,
    };
    Some(VimEvent {
        key,
        control,
        alt,
        shift,
    })
}

fn vim_mode_name(mode: VimMode) -> &'static str {
    match mode {
        VimMode::Insert => "INSERT",
        VimMode::Normal => "NORMAL",
        VimMode::Visual => "VISUAL",
        VimMode::VisualLine => "VISUAL LINE",
    }
}

#[derive(Debug, Clone)]
struct EditorSnapshot {
    text: String,
    cursor_byte: usize,
}

#[derive(Debug)]
struct HistorySearch {
    original: EditorSnapshot,
    query: String,
    matches: Vec<String>,
    selected: usize,
    scope: HistoryScope,
}

impl HistorySearch {
    fn new(history: &[String], scope: HistoryScope, text: String, cursor_byte: usize) -> Self {
        let mut search = Self {
            original: EditorSnapshot { text, cursor_byte },
            query: String::new(),
            matches: Vec::new(),
            selected: 0,
            scope,
        };
        search.refresh(history);
        search
    }

    fn refresh(&mut self, history: &[String]) {
        let mut seen = std::collections::HashSet::new();
        let query = self.query.to_lowercase();
        self.matches = history
            .iter()
            .rev()
            .filter(|entry| {
                entry.len() <= MAX_HISTORY_SEARCH_ENTRY_BYTES
                    && entry.to_lowercase().contains(&query)
                    && seen.insert((*entry).clone())
            })
            .take(100)
            .cloned()
            .collect();
        self.selected = self.selected.min(self.matches.len().saturating_sub(1));
    }

    fn current(&self) -> &str {
        self.matches
            .get(self.selected)
            .map_or(self.original.text.as_str(), String::as_str)
    }

    fn hint(&self) -> String {
        let scope = match self.scope {
            HistoryScope::Session => "session",
            HistoryScope::Project => "project",
            HistoryScope::Everywhere => "everywhere",
        };
        if self.matches.is_empty() {
            format!(
                "reverse-i-search `{}`: no match · {scope} · Ctrl-S scope",
                self.query
            )
        } else {
            format!(
                "reverse-i-search `{}`: {}/{} · {scope} · Ctrl-R next · Ctrl-S scope · Enter run · Esc accept",
                self.query,
                self.selected + 1,
                self.matches.len()
            )
        }
    }
}

fn push_kill(ring: &mut VecDeque<String>, text: String) {
    if text.is_empty() {
        return;
    }
    if ring.front() != Some(&text) {
        ring.push_front(text);
        ring.truncate(KILL_RING_LIMIT);
    }
}

struct TemporaryPromptFile(std::path::PathBuf);

impl Drop for TemporaryPromptFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub fn open_file_in_external_editor(path: &std::path::Path) -> Result<()> {
    let editor = std::env::var("VISUAL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("EDITOR")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| {
            if cfg!(windows) {
                "notepad".to_owned()
            } else {
                "vi".to_owned()
            }
        });
    let mut parts = editor.split_whitespace();
    let executable = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("external editor command is empty"))?;
    let mut arguments = parts.take(31).map(ToOwned::to_owned).collect::<Vec<_>>();
    let editor_name = std::path::Path::new(executable)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(executable)
        .to_ascii_lowercase();
    if (matches!(
        editor_name.as_str(),
        "code" | "code-insiders" | "codium" | "cursor" | "windsurf"
    ) || editor_name == "subl")
        && !arguments
            .iter()
            .any(|argument| argument == "-w" || argument == "--wait")
    {
        arguments.push("--wait".to_owned());
    }
    let status = std::process::Command::new(executable)
        .args(arguments)
        .arg(path)
        .status()
        .map_err(|error| anyhow::anyhow!("cannot launch external editor: {error}"))?;
    if !status.success() {
        anyhow::bail!("external editor exited with {status}")
    }
    Ok(())
}

fn edit_prompt_externally(prompt: &str) -> Result<String> {
    let path = std::env::temp_dir().join(format!(
        "open-agent-harness-prompt-{}.md",
        uuid::Uuid::new_v4()
    ));
    let cleanup = TemporaryPromptFile(path.clone());
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| anyhow::anyhow!("cannot create private prompt file: {error}"))?;
    file.write_all(prompt.as_bytes())?;
    file.flush()?;
    drop(file);

    open_file_in_external_editor(&path)?;

    let mut edited = Vec::new();
    std::fs::File::open(&path)?
        .take((MAX_INPUT_BYTES + 1) as u64)
        .read_to_end(&mut edited)?;
    if edited.len() > MAX_INPUT_BYTES {
        anyhow::bail!("external editor result exceeds the input limit")
    }
    let edited = String::from_utf8(edited)
        .map_err(|_| anyhow::anyhow!("external editor result is not valid UTF-8"))?;
    drop(cleanup);
    Ok(sanitize_paste(&edited))
}

pub struct PromptRead {
    pub text: String,
    pub permission_mode: PermissionMode,
    pub clipboard_images: Vec<ClipboardImage>,
}

pub struct InputReadContext<'a> {
    pub commands: &'a [SlashCommandSuggestion],
    pub files: &'a [FileSuggestion],
    pub todos: &'a [String],
    pub status_line: Option<&'a str>,
    pub theme: ThemePreset,
    pub copy_on_select: bool,
}

pub struct InputReadActions<'a> {
    pub scheduled_prompt: &'a mut dyn FnMut() -> Result<Option<String>>,
    pub model_picker: &'a mut dyn FnMut() -> Result<ModelPickerOutcome>,
    pub rewind_picker: &'a mut dyn FnMut() -> Result<ModelPickerOutcome>,
    pub transcript_viewer: &'a mut dyn FnMut() -> Result<()>,
    /// Returns `Some(new_value)` only when an asynchronous refresh completed.
    /// The inner `None` clears an existing status line.
    pub status_line_refresh:
        &'a mut dyn FnMut(PermissionMode, Option<VimMode>) -> Option<Option<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TranscriptMatch {
    line: usize,
    start: usize,
    end: usize,
}

fn transcript_matches(lines: &[&String], query: &str) -> Vec<TranscriptMatch> {
    if query.is_empty() {
        return Vec::new();
    }
    let Ok(pattern) = regex::RegexBuilder::new(&regex::escape(query))
        .case_insensitive(true)
        .unicode(true)
        .build()
    else {
        return Vec::new();
    };
    lines
        .iter()
        .enumerate()
        .flat_map(|(line, text)| {
            pattern.find_iter(text).map(move |found| TranscriptMatch {
                line,
                start: found.start(),
                end: found.end(),
            })
        })
        .take(1_000)
        .collect()
}

fn queue_transcript_search_line(
    out: &mut impl Write,
    line: &str,
    width: usize,
    matches: &[TranscriptMatch],
    line_index: usize,
    selected: Option<TranscriptMatch>,
) -> io::Result<()> {
    let visible = visible_line(line, width);
    let mut cursor = 0usize;
    for found in matches.iter().filter(|found| found.line == line_index) {
        let start = found.start.min(visible.len());
        let end = found.end.min(visible.len());
        if start >= end
            || start < cursor
            || !visible.is_char_boundary(start)
            || !visible.is_char_boundary(end)
        {
            continue;
        }
        queue!(out, Print(&visible[cursor..start]))?;
        if selected == Some(*found) {
            queue!(out, SetAttribute(Attribute::Reverse))?;
        } else {
            queue!(out, SetAttribute(Attribute::Underlined))?;
        }
        queue!(
            out,
            Print(&visible[start..end]),
            SetAttribute(Attribute::Reset)
        )?;
        cursor = end;
    }
    queue!(out, Print(&visible[cursor..]))
}

pub fn view_transcript(lines: &[String]) -> Result<()> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        for line in lines {
            println!("{line}");
        }
        return Ok(());
    }
    let _raw = RawModeGuard::enter()?;
    let _alternate = AlternateScreenGuard::enter()?;
    let mut out = io::stdout();
    const MAX_TRANSCRIPT_VIEW_BYTES: usize = 8 * 1024 * 1024;
    let mut bounded = VecDeque::new();
    let mut bounded_bytes = 0usize;
    for line in lines.iter().rev().take(10_000) {
        let next = bounded_bytes.saturating_add(line.len());
        if next > MAX_TRANSCRIPT_VIEW_BYTES {
            break;
        }
        bounded.push_front(line);
        bounded_bytes = next;
    }
    let full = bounded.into_iter().collect::<Vec<_>>();
    let compact = compact_transcript(&full);
    let mut show_all = false;
    let mut top = compact.len().saturating_sub(1);
    let mut search = None::<String>;
    let mut matches = Vec::<TranscriptMatch>::new();
    let mut selected_match = 0usize;
    let mut search_origin_top = 0usize;
    let mut dump_to_scrollback = false;

    loop {
        let active = if show_all { &full } else { &compact };
        let (width, height) = terminal::size()
            .map(|(width, height)| (usize::from(width).max(4), usize::from(height).max(4)))
            .unwrap_or((80, 24));
        let viewport = height.saturating_sub(2).max(1);
        top = top.min(active.len().saturating_sub(viewport));
        let mut frame = Vec::new();
        if synchronized_output_supported() {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        queue!(frame, cursor::MoveTo(0, 0), Clear(ClearType::All))?;
        let selected = matches.get(selected_match).copied();
        for (line_index, line) in active.iter().enumerate().skip(top).take(viewport) {
            queue_transcript_search_line(
                &mut frame,
                line,
                width.saturating_sub(1),
                &matches,
                line_index,
                selected,
            )?;
            queue!(frame, Print(RAW_LINE_END))?;
        }
        let footer = if let Some(query) = &search {
            format!(
                "/{query} · {}/{} · Enter accept · Esc close search",
                selected_match.saturating_add(usize::from(!matches.is_empty())),
                matches.len()
            )
        } else {
            format!(
                "transcript {} · {}/{} · Ctrl-E show all · ↑↓/PgUp/PgDn · / search · [ dump · q exit",
                if show_all { "all" } else { "compact" },
                top.saturating_add(1),
                active.len().max(1)
            )
        };
        queue!(
            frame,
            SetForegroundColor(Color::DarkGrey),
            Print(visible_line(&footer, width.saturating_sub(1))),
            ResetColor
        )?;
        if synchronized_output_supported() {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;

        let event = event::read()?;
        if let Event::Mouse(mouse) = event {
            match mouse.kind {
                MouseEventKind::ScrollUp => top = top.saturating_sub(3),
                MouseEventKind::ScrollDown => {
                    top = (top + 3).min(active.len().saturating_sub(viewport));
                }
                _ => {}
            }
            continue;
        }
        let Event::Key(key) = event else { continue };
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            continue;
        }
        if let Some(query) = search.as_mut() {
            match key {
                KeyEvent {
                    code: KeyCode::Enter,
                    ..
                } => search = None,
                KeyEvent {
                    code: KeyCode::Esc, ..
                } => {
                    search = None;
                    top = search_origin_top;
                    matches.clear();
                    selected_match = 0;
                }
                KeyEvent {
                    code: KeyCode::Backspace,
                    ..
                } => {
                    query.pop();
                }
                KeyEvent {
                    code: KeyCode::Char('c'),
                    modifiers: KeyModifiers::CONTROL,
                    ..
                } => {
                    search = None;
                    top = search_origin_top;
                    matches.clear();
                    selected_match = 0;
                }
                KeyEvent {
                    code: KeyCode::Char(character),
                    modifiers: KeyModifiers::NONE | KeyModifiers::SHIFT,
                    ..
                } if query.len().saturating_add(character.len_utf8()) <= 4 * 1024 => {
                    query.push(character);
                }
                _ => {}
            }
            if let Some(query) = &search {
                matches = transcript_matches(active, query);
                selected_match = 0;
                if let Some(found) = matches.first() {
                    top = found.line.min(active.len().saturating_sub(viewport));
                }
            }
            continue;
        }
        match key {
            KeyEvent {
                code: KeyCode::Char('q') | KeyCode::Esc,
                ..
            }
            | KeyEvent {
                code: KeyCode::Char('c'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => break,
            KeyEvent {
                code: KeyCode::Up | KeyCode::Char('k'),
                modifiers: KeyModifiers::NONE,
                ..
            } => top = top.saturating_sub(1),
            KeyEvent {
                code: KeyCode::Down | KeyCode::Char('j'),
                modifiers: KeyModifiers::NONE,
                ..
            } => top = (top + 1).min(active.len().saturating_sub(viewport)),
            KeyEvent {
                code: KeyCode::Char('p'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => top = top.saturating_sub(1),
            KeyEvent {
                code: KeyCode::Char('n'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => top = (top + 1).min(active.len().saturating_sub(viewport)),
            KeyEvent {
                code: KeyCode::PageUp,
                ..
            } => top = top.saturating_sub(viewport),
            KeyEvent {
                code: KeyCode::PageDown | KeyCode::Char(' '),
                ..
            } => top = (top + viewport).min(active.len().saturating_sub(viewport)),
            KeyEvent {
                code: KeyCode::Char('u'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => top = top.saturating_sub(viewport.div_ceil(2)),
            KeyEvent {
                code: KeyCode::Char('d'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => {
                top = (top + viewport.div_ceil(2)).min(active.len().saturating_sub(viewport));
            }
            KeyEvent {
                code: KeyCode::Char('b'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => top = top.saturating_sub(viewport),
            KeyEvent {
                code: KeyCode::Char('f'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => top = (top + viewport).min(active.len().saturating_sub(viewport)),
            KeyEvent {
                code: KeyCode::Home,
                ..
            } => top = 0,
            KeyEvent {
                code: KeyCode::Char('g'),
                modifiers: KeyModifiers::NONE,
                ..
            } => top = 0,
            KeyEvent {
                code: KeyCode::End, ..
            } => top = active.len().saturating_sub(viewport),
            KeyEvent {
                code: KeyCode::Char('G'),
                modifiers: KeyModifiers::NONE | KeyModifiers::SHIFT,
                ..
            } => top = active.len().saturating_sub(viewport),
            KeyEvent {
                code: KeyCode::Char('e'),
                modifiers: KeyModifiers::CONTROL,
                ..
            } => {
                show_all = !show_all;
                let next = if show_all { &full } else { &compact };
                top = next.len().saturating_sub(viewport);
                matches.clear();
                selected_match = 0;
            }
            KeyEvent {
                code: KeyCode::Char('/'),
                modifiers: KeyModifiers::NONE,
                ..
            } => {
                search_origin_top = top;
                search = Some(String::new());
            }
            KeyEvent {
                code: KeyCode::Char('n' | 'N'),
                modifiers,
                ..
            } if !matches.is_empty() => {
                selected_match = if modifiers.contains(KeyModifiers::SHIFT) {
                    selected_match.checked_sub(1).unwrap_or(matches.len() - 1)
                } else {
                    (selected_match + 1) % matches.len()
                };
                top = matches[selected_match]
                    .line
                    .min(active.len().saturating_sub(viewport));
            }
            KeyEvent {
                code: KeyCode::Char('['),
                ..
            } => {
                dump_to_scrollback = true;
                break;
            }
            _ => {}
        }
    }
    drop(_alternate);
    drop(_raw);
    if dump_to_scrollback {
        for line in full {
            println!("{line}");
        }
    }
    Ok(())
}

fn compact_transcript<'a>(lines: &[&'a String]) -> Vec<&'a String> {
    let mut compact = Vec::with_capacity(lines.len());
    let mut hide_tool_result = false;
    for line in lines {
        if matches!(line.as_str(), "You" | "Assistant") {
            hide_tool_result = false;
            compact.push(*line);
        } else if line.starts_with("  [tool call:") {
            continue;
        } else if line.starts_with("  [tool result]") || line.starts_with("  [tool error]") {
            hide_tool_result = true;
        } else if !hide_tool_result {
            compact.push(*line);
        }
    }
    compact
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlashCommandSuggestion {
    pub name: String,
    pub aliases: Vec<String>,
    pub description: String,
    pub argument_hint: Option<String>,
    pub execute_on_enter: bool,
    pub argument_candidates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ArgumentToken {
    start: usize,
    end: usize,
    query: String,
}

fn argument_matches<'a>(
    buffer: &str,
    cursor_byte: usize,
    commands: &'a [SlashCommandSuggestion],
) -> Option<(ArgumentToken, Vec<&'a String>)> {
    let before = buffer.get(..cursor_byte)?;
    let rest = before.strip_prefix('/')?;
    if rest.contains('\n') {
        return None;
    }
    let split = rest.find(char::is_whitespace)?;
    let command_name = &rest[..split];
    let command = commands.iter().find(|command| {
        command.name == command_name || command.aliases.iter().any(|alias| alias == command_name)
    })?;
    if command.argument_candidates.is_empty() {
        return None;
    }
    let argument_start = 1
        + split
        + rest[split..]
            .len()
            .saturating_sub(rest[split..].trim_start().len());
    let current_start = before[argument_start..]
        .rfind(char::is_whitespace)
        .map_or(argument_start, |offset| argument_start + offset + 1);
    let query = &before[current_start..];
    if before[argument_start..current_start]
        .split_whitespace()
        .count()
        > 0
    {
        return None;
    }
    let query_lower = query.to_ascii_lowercase();
    let mut matches = command
        .argument_candidates
        .iter()
        .filter(|candidate| candidate.to_ascii_lowercase().contains(&query_lower))
        .collect::<Vec<_>>();
    matches.sort_by_key(|candidate| {
        let normalized = candidate.to_ascii_lowercase();
        (
            usize::from(normalized != query_lower),
            usize::from(!normalized.starts_with(&query_lower)),
            normalized,
        )
    });
    (!matches.is_empty()).then_some((
        ArgumentToken {
            start: current_start,
            end: cursor_byte,
            query: query.to_owned(),
        },
        matches,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSuggestion {
    pub display_path: String,
    pub is_dir: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileToken {
    start: usize,
    end: usize,
    query: String,
    quoted: bool,
}

fn file_token_at_cursor(buffer: &str, cursor_byte: usize) -> Option<FileToken> {
    let before_cursor = buffer.get(..cursor_byte)?;
    for (start, _) in before_cursor.rmatch_indices("@\"") {
        if !file_reference_boundary(before_cursor[..start].chars().next_back()) {
            continue;
        }
        let raw_query = &before_cursor[start + 2..];
        if let Some(query) = unescape_open_quoted_path(raw_query) {
            return Some(FileToken {
                start,
                end: quoted_file_token_end(buffer, cursor_byte),
                query,
                quoted: true,
            });
        }
    }

    let start = before_cursor
        .char_indices()
        .rev()
        .find(|(_, character)| character.is_whitespace() || is_open_bracket(*character))
        .map_or(0, |(index, character)| index + character.len_utf8());
    let query = before_cursor[start..].strip_prefix('@')?;
    if query.contains('"') {
        return None;
    }
    Some(FileToken {
        start,
        end: plain_file_token_end(buffer, cursor_byte),
        query: query.to_owned(),
        quoted: false,
    })
}

fn quoted_file_token_end(buffer: &str, cursor_byte: usize) -> usize {
    let mut escaped = false;
    for (offset, character) in buffer[cursor_byte..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            '\\' => escaped = true,
            '"' => return cursor_byte + offset + character.len_utf8(),
            _ => {}
        }
    }
    cursor_byte
}

fn plain_file_token_end(buffer: &str, cursor_byte: usize) -> usize {
    buffer[cursor_byte..]
        .char_indices()
        .find(|(_, character)| character.is_whitespace() || matches!(character, ')' | ']' | '}'))
        .map_or(buffer.len(), |(offset, _)| cursor_byte + offset)
}

fn file_reference_boundary(previous: Option<char>) -> bool {
    previous.is_none_or(|character| character.is_whitespace() || is_open_bracket(character))
}

fn is_open_bracket(character: char) -> bool {
    matches!(character, '(' | '[' | '{')
}

fn unescape_open_quoted_path(value: &str) -> Option<String> {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        match character {
            '"' => return None,
            '\\' => match characters.next() {
                Some('"') => output.push('"'),
                Some('\\') => output.push('\\'),
                Some(next) => {
                    output.push('\\');
                    output.push(next);
                }
                None => output.push('\\'),
            },
            _ => output.push(character),
        }
    }
    Some(output)
}

fn file_matches<'a>(token: &FileToken, files: &'a [FileSuggestion]) -> Vec<&'a FileSuggestion> {
    let query = token.query.to_ascii_lowercase();
    let mut matches = files
        .iter()
        .take(MAX_FILE_CANDIDATES_SCANNED)
        .filter_map(|file| {
            if if file.is_dir {
                format!("{}/", file.display_path.trim_end_matches('/')) == token.query
            } else {
                file.display_path == token.query
            } {
                return None;
            }
            let path = file.display_path.to_ascii_lowercase();
            let basename = path.rsplit('/').next().unwrap_or(&path);
            let score = if query.is_empty() || path.starts_with(&query) {
                (0, path.len())
            } else if basename.starts_with(&query) {
                (1, path.len())
            } else if path.contains(&query) {
                (2, path.len())
            } else if fuzzy_subsequence(&path, &query) {
                (3, path.len())
            } else if edit_distance(basename, &query)
                <= usize::max(1, query.chars().count().saturating_mul(3).div_ceil(10))
            {
                (4, path.len())
            } else {
                return None;
            };
            Some((score, file))
        })
        .collect::<Vec<_>>();
    matches.sort_by(|(left_score, left), (right_score, right)| {
        left_score
            .cmp(right_score)
            .then_with(|| left.display_path.cmp(&right.display_path))
    });
    matches
        .into_iter()
        .map(|(_, file)| file)
        .take(MAX_FILE_SUGGESTIONS)
        .collect()
}

fn fuzzy_subsequence(value: &str, query: &str) -> bool {
    let mut query = query.chars();
    let mut expected = query.next();
    for character in value.chars() {
        if Some(character) == expected {
            expected = query.next();
            if expected.is_none() {
                return true;
            }
        }
    }
    expected.is_none()
}

fn common_file_prefix(files: &[&FileSuggestion]) -> String {
    let Some(first) = files.first() else {
        return String::new();
    };
    let mut common = first.display_path.clone();
    for file in &files[1..] {
        let mut bytes = 0usize;
        for (left, right) in common
            .graphemes(true)
            .zip(file.display_path.graphemes(true))
        {
            if left != right {
                break;
            }
            bytes += left.len();
        }
        common.truncate(bytes);
        if common.is_empty() {
            break;
        }
    }
    common
}

fn replace_file_token(
    buffer: &mut String,
    _cursor_byte: usize,
    token: &FileToken,
    path: &str,
    is_dir: bool,
    partial: bool,
) -> usize {
    let mut path = path.to_owned();
    if is_dir && !path.ends_with('/') {
        path.push('/');
    }
    let quote = token.quoted || path.chars().any(char::is_whitespace);
    let replacement = if quote {
        format!("@\"{}\"", escape_file_reference(&path))
    } else {
        format!("@{path}")
    };
    buffer.replace_range(token.start..token.end, &replacement);
    let end = token.start + replacement.len();
    if quote && (partial || is_dir) {
        end - 1
    } else {
        end
    }
}

fn escape_file_reference(path: &str) -> String {
    path.replace('\\', "\\\\").replace('"', "\\\"")
}

fn command_matches<'a>(
    buffer: &str,
    commands: &'a [SlashCommandSuggestion],
) -> Vec<&'a SlashCommandSuggestion> {
    let Some(rest) = buffer.strip_prefix('/') else {
        return Vec::new();
    };
    if buffer.contains('\n') {
        return Vec::new();
    }
    let Some((command_part, arguments)) = rest.split_once(char::is_whitespace) else {
        return ranked_command_matches(rest, commands);
    };
    if !arguments.trim().is_empty()
        || commands.iter().any(|command| {
            command.name == command_part
                || command.aliases.iter().any(|alias| alias == command_part)
        })
    {
        return Vec::new();
    }
    ranked_command_matches(command_part, commands)
}

fn ranked_command_matches<'a>(
    query: &str,
    commands: &'a [SlashCommandSuggestion],
) -> Vec<&'a SlashCommandSuggestion> {
    let query = query.to_ascii_lowercase();
    if query.is_empty() {
        return commands.iter().collect();
    }
    let mut matches = commands
        .iter()
        .filter_map(|command| {
            let name = command.name.to_ascii_lowercase();
            let aliases = command
                .aliases
                .iter()
                .map(|alias| alias.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let score = if name == query {
                Some((0, name.len()))
            } else if aliases.iter().any(|alias| alias == &query) {
                Some((1, name.len()))
            } else if name.starts_with(&query) {
                Some((2, name.len()))
            } else if aliases.iter().any(|alias| alias.starts_with(&query)) {
                Some((3, name.len()))
            } else if name
                .split([':', '_', '-'])
                .any(|part| part.starts_with(&query))
                || name.contains(&query)
            {
                Some((4, name.len()))
            } else if command
                .description
                .split_whitespace()
                .any(|word| word.to_ascii_lowercase().starts_with(&query))
            {
                Some((5, name.len()))
            } else if edit_distance(&name, &query)
                <= usize::max(1, query.chars().count().saturating_mul(3).div_ceil(10))
            {
                Some((6, name.len()))
            } else {
                None
            }?;
            Some((score, command))
        })
        .collect::<Vec<_>>();
    matches.sort_by(|(left_score, left), (right_score, right)| {
        left_score
            .cmp(right_score)
            .then_with(|| left.name.cmp(&right.name))
    });
    matches.into_iter().map(|(_, command)| command).collect()
}

fn command_argument_hint<'a>(
    buffer: &str,
    commands: &'a [SlashCommandSuggestion],
) -> Option<&'a str> {
    let command = buffer.strip_prefix('/')?.strip_suffix(' ')?;
    if command.contains(char::is_whitespace) {
        return None;
    }
    commands
        .iter()
        .find(|candidate| {
            candidate.name == command || candidate.aliases.iter().any(|alias| alias == command)
        })
        .and_then(|candidate| candidate.argument_hint.as_deref())
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right = right.chars().collect::<Vec<_>>();
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    for (left_index, left_character) in left.chars().enumerate() {
        let mut current = vec![left_index + 1; right.len() + 1];
        for (right_index, right_character) in right.iter().enumerate() {
            current[right_index + 1] = usize::min(
                usize::min(current[right_index] + 1, previous[right_index + 1] + 1),
                previous[right_index] + usize::from(left_character != *right_character),
            );
        }
        previous = current;
    }
    previous[right.len()]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionChoice {
    Allow,
    AllowForSession,
    Deny,
    Interrupt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelPickerOutcome {
    Selected(String),
    Cancelled,
    Exit,
}

#[derive(Debug, Clone, Copy)]
struct PickerText<'a> {
    title: &'a str,
    help: &'a str,
    preview_theme: bool,
}

#[derive(Debug, Clone)]
struct ModelPickerState {
    focused: usize,
    visible_from: usize,
    visible_count: usize,
    option_count: usize,
}

impl ModelPickerState {
    fn new(options: &[ModelOption], current: &str) -> Self {
        let option_count = options.len();
        let visible_count = option_count.min(10);
        let focused = options
            .iter()
            .position(|option| option.value == current)
            .unwrap_or(0);
        let visible_from = if focused < visible_count {
            0
        } else {
            focused + 1 - visible_count
        };
        Self {
            focused,
            visible_from,
            visible_count,
            option_count,
        }
    }

    fn next(&mut self) {
        if self.option_count == 0 {
            return;
        }
        self.focused = (self.focused + 1) % self.option_count;
        if self.focused == 0 {
            self.visible_from = 0;
        } else if self.focused >= self.visible_from + self.visible_count {
            self.visible_from = self.focused + 1 - self.visible_count;
        }
    }

    fn previous(&mut self) {
        if self.option_count == 0 {
            return;
        }
        if self.focused == 0 {
            self.focused = self.option_count - 1;
            self.visible_from = self.option_count.saturating_sub(self.visible_count);
        } else {
            self.focused -= 1;
            if self.focused < self.visible_from {
                self.visible_from = self.focused;
            }
        }
    }

    fn next_page(&mut self) {
        if self.option_count == 0 {
            return;
        }
        self.focused = (self.focused + self.visible_count).min(self.option_count - 1);
        let visible_to = (self.focused + 1).min(self.option_count);
        self.visible_from = visible_to.saturating_sub(self.visible_count);
    }

    fn previous_page(&mut self) {
        self.focused = self.focused.saturating_sub(self.visible_count);
        self.visible_from = self.focused;
    }

    fn fit_terminal(&mut self) {
        let rows = terminal::size()
            .map(|(_, rows)| usize::from(rows))
            .unwrap_or(24);
        self.visible_count = self.option_count.min(rows.saturating_sub(6).clamp(1, 10));
        if self.focused < self.visible_from {
            self.visible_from = self.focused;
        } else if self.focused >= self.visible_from + self.visible_count {
            self.visible_from = self.focused + 1 - self.visible_count;
        }
        self.visible_from = self
            .visible_from
            .min(self.option_count.saturating_sub(self.visible_count));
    }
}

pub fn select_model(options: &[ModelOption], current: &str) -> Result<ModelPickerOutcome> {
    select_option(
        options,
        current,
        "Select model",
        "Switch between models configured for this backend. Use /model <id> for another model.",
        false,
    )
}

pub fn select_rewind_checkpoint(options: &[ModelOption]) -> Result<ModelPickerOutcome> {
    let current = options.first().map_or("", |option| option.value.as_str());
    select_option(
        options,
        current,
        "Restore conversation",
        "Choose a prior user-message boundary. Enter restores it; Escape keeps the current conversation.",
        false,
    )
}

pub fn select_theme(options: &[ModelOption], current: &str) -> Result<ModelPickerOutcome> {
    select_option(
        options,
        current,
        "Select theme",
        "Choose a provider-neutral terminal color preset. Escape keeps the current preset.",
        true,
    )
}

fn select_option(
    options: &[ModelOption],
    current: &str,
    title: &str,
    help: &str,
    preview_theme: bool,
) -> Result<ModelPickerOutcome> {
    if options.is_empty() || !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Ok(ModelPickerOutcome::Cancelled);
    }
    let _raw = RawModeGuard::enter()?;
    let mut out = io::stdout();
    let mut state = ModelPickerState::new(options, current);
    let mut rendered = RenderedPicker::default();
    let mut exit_pending: Option<(KeyCode, Instant)> = None;
    let text = PickerText {
        title,
        help,
        preview_theme,
    };

    loop {
        state.fit_terminal();
        let exit_hint = exit_pending.as_ref().and_then(|(code, armed)| {
            (armed.elapsed() <= EXIT_WINDOW).then_some(match code {
                KeyCode::Char('d') => "Press Ctrl-D again to exit",
                _ => "Press Ctrl-C again to exit",
            })
        });
        if exit_hint.is_none() {
            exit_pending = None;
        }
        rendered.redraw(&mut out, options, current, &state, exit_hint, text)?;
        match event::read()? {
            Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                let exit_key = match key {
                    KeyEvent {
                        code: KeyCode::Char('c'),
                        modifiers: KeyModifiers::CONTROL,
                        ..
                    } => Some(KeyCode::Char('c')),
                    KeyEvent {
                        code: KeyCode::Char('d'),
                        modifiers: KeyModifiers::CONTROL,
                        ..
                    } => Some(KeyCode::Char('d')),
                    _ => None,
                };
                if let Some(code) = exit_key {
                    if exit_pending.as_ref().is_some_and(|(pending, armed)| {
                        *pending == code && armed.elapsed() <= EXIT_WINDOW
                    }) {
                        rendered.erase(&mut out)?;
                        return Ok(ModelPickerOutcome::Exit);
                    }
                    exit_pending = Some((code, Instant::now()));
                    continue;
                }
                exit_pending = None;
                match key {
                    KeyEvent {
                        code: KeyCode::Up | KeyCode::Char('k'),
                        modifiers: KeyModifiers::NONE,
                        ..
                    }
                    | KeyEvent {
                        code: KeyCode::Char('p'),
                        modifiers: KeyModifiers::CONTROL,
                        ..
                    } => state.previous(),
                    KeyEvent {
                        code: KeyCode::Down | KeyCode::Char('j'),
                        modifiers: KeyModifiers::NONE,
                        ..
                    }
                    | KeyEvent {
                        code: KeyCode::Char('n'),
                        modifiers: KeyModifiers::CONTROL,
                        ..
                    } => state.next(),
                    KeyEvent {
                        code: KeyCode::PageUp,
                        ..
                    } => state.previous_page(),
                    KeyEvent {
                        code: KeyCode::PageDown,
                        ..
                    } => state.next_page(),
                    KeyEvent {
                        code: KeyCode::Char(digit @ '1'..='9'),
                        modifiers: KeyModifiers::NONE,
                        ..
                    } => {
                        let index = digit.to_digit(10).unwrap_or_default() as usize - 1;
                        if let Some(option) = options.get(index) {
                            rendered.erase(&mut out)?;
                            return Ok(ModelPickerOutcome::Selected(option.value.clone()));
                        }
                    }
                    KeyEvent {
                        code: KeyCode::Enter,
                        ..
                    } => {
                        let selected = options[state.focused].value.clone();
                        rendered.erase(&mut out)?;
                        return Ok(ModelPickerOutcome::Selected(selected));
                    }
                    KeyEvent {
                        code: KeyCode::Esc, ..
                    } => {
                        rendered.erase(&mut out)?;
                        return Ok(ModelPickerOutcome::Cancelled);
                    }
                    _ => {}
                }
            }
            Event::Resize(_, _) => rendered.reset_viewport(&mut out)?,
            _ => {}
        }
    }
}

pub fn request_permission(
    tool: &str,
    input: &serde_json::Value,
    summary: &str,
    session_available: bool,
) -> Result<PermissionChoice> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Ok(PermissionChoice::Deny);
    }
    let _raw = RawModeGuard::enter()?;
    let mut out = io::stdout();
    let tool = sanitize_inline(tool);
    let summary = single_line(summary, 4 * 1024);
    let details = sanitize_multiline(&serde_json::to_string_pretty(input)?);
    if details.len() > MAX_PERMISSION_PREVIEW_BYTES {
        queue!(
            out,
            Print(RAW_LINE_END),
            Print("  Permission denied"),
            Print(RAW_LINE_END),
            Print(format!(
                "  {tool} input exceeds the {MAX_PERMISSION_PREVIEW_BYTES}-byte exact preview limit"
            )),
            Print(RAW_LINE_END),
            Print("  Refusing to authorize an action whose full input cannot be displayed."),
            Print(RAW_LINE_END),
            Print(RAW_LINE_END)
        )?;
        out.flush()?;
        return Ok(PermissionChoice::Deny);
    }
    render_permission_prompt(
        &mut out,
        &tool,
        input,
        &summary,
        &details,
        session_available,
        false,
    )?;
    loop {
        match event::read()? {
            Event::Key(KeyEvent {
                code: KeyCode::Char('y' | 'Y'),
                modifiers,
                kind: KeyEventKind::Press,
                ..
            }) if !modifiers.contains(KeyModifiers::CONTROL) => {
                queue!(
                    out,
                    Print("  Allowed"),
                    Print(RAW_LINE_END),
                    Print(RAW_LINE_END)
                )?;
                out.flush()?;
                return Ok(PermissionChoice::Allow);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('s' | 'S'),
                modifiers,
                kind: KeyEventKind::Press,
                ..
            }) if session_available && !modifiers.contains(KeyModifiers::CONTROL) => {
                queue!(
                    out,
                    Print("  Allowed exact action for this session"),
                    Print(RAW_LINE_END),
                    Print(RAW_LINE_END)
                )?;
                out.flush()?;
                return Ok(PermissionChoice::AllowForSession);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('n' | 'N') | KeyCode::Esc,
                kind: KeyEventKind::Press,
                ..
            }) => {
                queue!(
                    out,
                    Print("  Denied"),
                    Print(RAW_LINE_END),
                    Print(RAW_LINE_END)
                )?;
                out.flush()?;
                return Ok(PermissionChoice::Deny);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('c'),
                modifiers: KeyModifiers::CONTROL,
                kind: KeyEventKind::Press,
                ..
            }) => {
                queue!(
                    out,
                    Print("  Interrupted"),
                    Print(RAW_LINE_END),
                    Print(RAW_LINE_END)
                )?;
                out.flush()?;
                return Ok(PermissionChoice::Interrupt);
            }
            Event::Resize(_, _) => render_permission_prompt(
                &mut out,
                &tool,
                input,
                &summary,
                &details,
                session_available,
                true,
            )?,
            _ => {}
        }
    }
}

fn render_permission_prompt(
    out: &mut impl Write,
    tool: &str,
    input: &serde_json::Value,
    summary: &str,
    details: &str,
    session_available: bool,
    clear: bool,
) -> Result<()> {
    if clear {
        queue!(
            out,
            cursor::MoveToColumn(0),
            Clear(ClearType::FromCursorDown),
            Print(RAW_LINE_END)
        )?;
    } else {
        queue!(out, Print(RAW_LINE_END))?;
    }
    queue!(
        out,
        SetAttribute(Attribute::Bold),
        Print("  Permission required"),
        SetAttribute(Attribute::Reset),
        Print(RAW_LINE_END),
        Print(format!("  {tool}")),
        Print(if summary.is_empty() { "" } else { " · " }),
        Print(summary),
        Print(RAW_LINE_END)
    )?;
    for line in permission_action_preview(tool, input) {
        queue!(out, Print(line), Print(RAW_LINE_END))?;
    }
    queue!(
        out,
        Print("  Exact input:"),
        Print(RAW_LINE_END),
        Print(details),
        Print(RAW_LINE_END),
        Print(if session_available {
            "  [y] allow once   [s] allow exact action for session   [n] deny"
        } else {
            "  [y] allow once   [n] deny"
        }),
        Print(RAW_LINE_END),
        Print("  [Esc] deny   [Ctrl-C] interrupt"),
        Print(RAW_LINE_END)
    )?;
    out.flush()?;
    Ok(())
}

fn permission_action_preview(tool: &str, input: &serde_json::Value) -> Vec<String> {
    const MAX_PREVIEW_CHARS: usize = 240;
    let field = |name: &str| input.get(name).and_then(serde_json::Value::as_str);
    let mut lines = Vec::new();
    match tool {
        "Bash" => {
            if let Some(command) = field("command") {
                lines.push(format!("  $ {}", single_line(command, MAX_PREVIEW_CHARS)));
            }
        }
        "Edit" => {
            if let Some(path) = field("file_path").or_else(|| field("path")) {
                lines.push(format!("  Edit {}", single_line(path, MAX_PREVIEW_CHARS)));
            }
            if let Some(old) = field("old_string") {
                lines.push(format!("    - {}", single_line(old, MAX_PREVIEW_CHARS)));
            }
            if let Some(new) = field("new_string") {
                lines.push(format!("    + {}", single_line(new, MAX_PREVIEW_CHARS)));
            }
        }
        "Write" | "NotebookEdit" => {
            if let Some(path) = field("file_path").or_else(|| field("notebook_path")) {
                lines.push(format!("  {tool} {}", single_line(path, MAX_PREVIEW_CHARS)));
            }
            if let Some(content) = field("content").or_else(|| field("new_source")) {
                let line_count = content.lines().count().max(1);
                lines.push(format!("    {line_count} line(s) of proposed content"));
            }
        }
        "Read" | "Glob" | "Grep" => {
            if let Some(path) = field("file_path").or_else(|| field("path")) {
                lines.push(format!("  Access {}", single_line(path, MAX_PREVIEW_CHARS)));
            }
        }
        "WebFetch" | "WebSearch" => {
            if let Some(target) = field("url").or_else(|| field("query")) {
                lines.push(format!(
                    "  Network target {}",
                    single_line(target, MAX_PREVIEW_CHARS)
                ));
            }
        }
        _ => {}
    }
    lines
}

impl Default for InputEditor {
    fn default() -> Self {
        Self {
            history: Vec::new(),
            project_history: Vec::new(),
            everywhere_history: Vec::new(),
            history_limit: 200,
            stashed_prompt: None,
            stashed_clipboard_images: Vec::new(),
            keybindings: KeybindingManager::new(KeybindingManager::default_user_path()),
            vim: None,
            ui: None,
            fullscreen_wheel_epoch: Instant::now(),
        }
    }
}

impl InputEditor {
    pub fn attach_ui(&mut self, ui: ConversationUi) {
        self.ui = Some(ui);
    }

    pub fn toggle_vim(&mut self) -> bool {
        if self.vim.is_some() {
            self.vim = None;
            false
        } else {
            self.vim = Some(VimState::new());
            true
        }
    }

    pub fn vim_mode(&self) -> Option<VimMode> {
        self.vim.as_ref().map(VimState::mode)
    }

    pub fn finish_prompt(&mut self) {
        if let Some(vim) = self.vim.as_mut() {
            vim.reset_buffer();
        }
    }

    pub fn seed_history(&mut self, entries: impl IntoIterator<Item = String>) {
        for entry in entries {
            if !entry.trim().is_empty() && entry.len() <= MAX_INPUT_BYTES {
                self.push_history(entry);
            }
        }
    }

    pub fn seed_scoped_history(
        &mut self,
        project: impl IntoIterator<Item = String>,
        everywhere: impl IntoIterator<Item = String>,
    ) {
        self.project_history = bounded_history(project, self.history_limit);
        self.everywhere_history = bounded_history(everywhere, self.history_limit);
    }

    fn history_for_scope(&self, scope: HistoryScope) -> &[String] {
        match scope {
            HistoryScope::Session => &self.history,
            HistoryScope::Project => &self.project_history,
            HistoryScope::Everywhere => &self.everywhere_history,
        }
    }

    pub fn read(
        &mut self,
        initial_mode: PermissionMode,
        mode_locked: bool,
        context: InputReadContext<'_>,
        actions: InputReadActions<'_>,
    ) -> Result<Option<PromptRead>> {
        let InputReadContext {
            commands,
            files,
            todos,
            status_line,
            theme,
            copy_on_select,
        } = context;
        let InputReadActions {
            scheduled_prompt,
            model_picker,
            rewind_picker,
            transcript_viewer,
            status_line_refresh,
        } = actions;
        let mut raw_guard = Some(RawModeGuard::enter()?);
        #[cfg(unix)]
        let suspend_signal = SuspendSignalGuard::register()?;
        let mut out = io::stdout();
        let mut buffer = String::new();
        let mut cursor_byte = 0usize;
        let mut rendered = RenderedInput::default();
        let mut navigation_history = self.project_history.clone();
        navigation_history.retain(|entry| !self.history.contains(entry));
        navigation_history.extend(self.history.iter().cloned());
        if navigation_history.len() > self.history_limit {
            navigation_history.drain(..navigation_history.len() - self.history_limit);
        }
        let mut history_index = navigation_history.len();
        let mut draft = String::new();
        let mut mode = initial_mode;
        let mut live_status_line = status_line.map(ToOwned::to_owned);
        let mut exit_pending: Option<ExitPending> = None;
        let mut last_escape: Option<Instant> = None;
        let mut hint = String::new();
        let mut kill_ring = VecDeque::<String>::new();
        let mut last_yank: Option<(usize, usize, usize)> = None;
        let mut undo_stack = Vec::<EditorSnapshot>::new();
        let mut history_search: Option<HistorySearch> = None;
        let mut clear_input_pending: Option<Instant> = None;
        let mut show_todos = false;
        let mut selected_attachment: Option<usize> = None;
        let mut fullscreen_last_click: Option<(Instant, u16, u16, u8)> = None;
        let mut fullscreen_selecting = false;
        let mut fullscreen_composer_hit_map = FullscreenComposerHitMap::default();
        let mut clipboard_images = std::mem::take(&mut self.stashed_clipboard_images);
        let mut selected_suggestion = 0usize;
        let mut dismissed_suggestions_for: Option<String> = None;
        let mut selected_file_suggestion = 0usize;
        let mut dismissed_file_suggestions_for: Option<(String, usize)> = None;
        let mut selected_argument_suggestion = 0usize;
        let mut dismissed_argument_suggestions_for: Option<(String, usize)> = None;
        let mut needs_redraw = true;

        loop {
            #[cfg(unix)]
            if suspend_signal.take() {
                let was_fullscreen = self
                    .ui
                    .as_ref()
                    .is_some_and(ConversationUi::fullscreen_active);
                if was_fullscreen {
                    self.ui
                        .as_ref()
                        .expect("fullscreen UI was checked")
                        .set_tui_mode(TuiMode::Default)?;
                } else {
                    rendered.erase(&mut out)?;
                }
                drop(raw_guard.take());
                signal_hook::low_level::raise(signal_hook::consts::signal::SIGSTOP)?;
                raw_guard = Some(RawModeGuard::enter()?);
                if was_fullscreen {
                    self.ui
                        .as_ref()
                        .expect("fullscreen UI was checked")
                        .set_tui_mode(TuiMode::Fullscreen)?;
                }
                rendered = RenderedInput::default();
                fullscreen_composer_hit_map = FullscreenComposerHitMap::default();
                needs_redraw = true;
                continue;
            }
            if let Some(refreshed) = status_line_refresh(mode, self.vim_mode()) {
                if live_status_line != refreshed {
                    live_status_line = refreshed;
                    needs_redraw = true;
                }
            }
            if self.keybindings.reload_if_due(false) {
                needs_redraw = true;
            }
            if let Some(warning) = self.keybindings.take_warning() {
                hint = warning;
                needs_redraw = true;
            }
            if exit_pending.is_some_and(|pending| pending.remaining(Instant::now()).is_none()) {
                exit_pending = None;
                hint.clear();
                needs_redraw = true;
            }
            let suggestions = if history_search.is_some()
                || dismissed_suggestions_for.as_deref() == Some(buffer.as_str())
            {
                Vec::new()
            } else {
                command_matches(&buffer, commands)
            };
            if suggestions.is_empty() {
                selected_suggestion = 0;
            } else {
                selected_suggestion = selected_suggestion.min(suggestions.len() - 1);
            }
            let file_token = if history_search.is_some()
                || dismissed_file_suggestions_for
                    .as_ref()
                    .is_some_and(|(dismissed, cursor)| {
                        dismissed == &buffer && *cursor == cursor_byte
                    }) {
                None
            } else {
                file_token_at_cursor(&buffer, cursor_byte)
            };
            let file_suggestions = file_token
                .as_ref()
                .map_or_else(Vec::new, |token| file_matches(token, files));
            if file_suggestions.is_empty() {
                selected_file_suggestion = 0;
            } else {
                selected_file_suggestion = selected_file_suggestion.min(file_suggestions.len() - 1);
            }
            let (argument_token, argument_suggestions) = if history_search.is_some()
                || dismissed_argument_suggestions_for
                    .as_ref()
                    .is_some_and(|(dismissed, cursor)| {
                        dismissed == &buffer && *cursor == cursor_byte
                    }) {
                (None, Vec::new())
            } else {
                argument_matches(&buffer, cursor_byte, commands).map_or_else(
                    || (None, Vec::new()),
                    |(token, matches)| (Some(token), matches),
                )
            };
            if argument_suggestions.is_empty() {
                selected_argument_suggestion = 0;
            } else {
                selected_argument_suggestion =
                    selected_argument_suggestion.min(argument_suggestions.len() - 1);
            }
            let argument_hint = command_argument_hint(&buffer, commands);
            if needs_redraw {
                let vim_mode = self.vim_mode();
                let vim_selection = self
                    .vim
                    .as_ref()
                    .and_then(|vim| vim.selection(&buffer, cursor_byte));
                let mut display_hint = vim_mode.map_or_else(
                    || hint.clone(),
                    |vim_mode| {
                        if hint.is_empty() {
                            format!("Vim {}", vim_mode_name(vim_mode))
                        } else {
                            format!("{hint} · Vim {}", vim_mode_name(vim_mode))
                        }
                    },
                );
                if !clipboard_images.is_empty() {
                    let attachments = selected_attachment
                        .and_then(|selected| clipboard_images.get(selected).map(|image| (selected, image)))
                        .map_or_else(
                            || {
                                format!(
                                    "{} image attachment{}",
                                    clipboard_images.len(),
                                    if clipboard_images.len() == 1 { "" } else { "s" }
                                )
                            },
                            |(selected, image)| {
                                format!(
                                    "image {}/{} · {}×{} · Left/Right navigate · Backspace remove · Down exit",
                                    selected + 1,
                                    clipboard_images.len(),
                                    image.width,
                                    image.height
                                )
                            },
                        );
                    display_hint = if display_hint.is_empty() {
                        attachments
                    } else {
                        format!("{display_hint} · {attachments}")
                    };
                }
                let render_state = InputRenderState {
                    buffer: &buffer,
                    cursor_byte,
                    mode,
                    hint: &display_hint,
                    suggestions: &suggestions,
                    selected_suggestion,
                    file_suggestions: &file_suggestions,
                    selected_file_suggestion,
                    argument_suggestions: &argument_suggestions,
                    selected_argument_suggestion,
                    argument_hint,
                    todos: show_todos.then_some(todos),
                    status_line: live_status_line.as_deref(),
                    theme,
                    vim_mode,
                    vim_selection,
                };
                if let Some(ui) = self.ui.as_ref().filter(|ui| ui.fullscreen_active()) {
                    let mut probe = RenderedInput::default();
                    let mut composer = Vec::new();
                    probe.draw(&mut composer, render_state)?;
                    let reserve = probe.rows.saturating_add(1);
                    let terminal_rows = terminal::size().map_or(24, |(_, rows)| rows);
                    fullscreen_composer_hit_map = FullscreenComposerHitMap {
                        top_row: terminal_rows.saturating_sub(reserve),
                        rows: probe.input_rows.clone(),
                    };
                    ui.render_fullscreen_prompt(&composer, reserve)?;
                    rendered = RenderedInput::default();
                } else {
                    fullscreen_composer_hit_map = FullscreenComposerHitMap::default();
                    rendered.redraw(&mut out, render_state)?;
                }
                needs_redraw = false;
            }

            let poll_for = exit_pending
                .and_then(|pending| pending.remaining(Instant::now()))
                .map_or(Duration::from_millis(100), |remaining| {
                    remaining.min(Duration::from_millis(100))
                });
            if !event::poll(poll_for)? {
                if exit_pending.is_some_and(|pending| pending.remaining(Instant::now()).is_none()) {
                    exit_pending = None;
                    hint.clear();
                    continue;
                }
                if let Some(prompt) = scheduled_prompt()? {
                    if !buffer.trim().is_empty() {
                        self.stashed_prompt = Some(EditorSnapshot {
                            text: buffer.clone(),
                            cursor_byte,
                        });
                    }
                    self.stashed_clipboard_images = std::mem::take(&mut clipboard_images);
                    rendered.erase(&mut out)?;
                    return Ok(Some(PromptRead {
                        text: prompt,
                        permission_mode: mode,
                        clipboard_images: Vec::new(),
                    }));
                }
                continue;
            }
            let event = event::read()?;
            needs_redraw = true;
            if let Some(ui) = self.ui.as_ref().filter(|ui| ui.fullscreen_active()) {
                match &event {
                    Event::Mouse(mouse)
                        if mouse.kind == MouseEventKind::Down(MouseButton::Left) =>
                    {
                        if let Some(next_cursor) =
                            fullscreen_composer_hit_map.cursor_at(mouse.row, mouse.column, &buffer)
                        {
                            cursor_byte = next_cursor.min(buffer.len());
                            ui.fullscreen_selection_clear();
                            fullscreen_selecting = false;
                            fullscreen_last_click = None;
                            continue;
                        }
                        let now = Instant::now();
                        let count = fullscreen_last_click
                            .filter(|(at, row, column, _)| {
                                now.duration_since(*at) <= FULLSCREEN_CLICK_WINDOW
                                    && *row == mouse.row
                                    && *column == mouse.column
                            })
                            .map_or(1, |(_, _, _, count)| count.saturating_add(1).min(3));
                        fullscreen_last_click = Some((now, mouse.row, mouse.column, count));
                        let kind = match count {
                            1 => ClickKind::Single,
                            2 => ClickKind::Double,
                            _ => ClickKind::Triple,
                        };
                        fullscreen_selecting =
                            ui.fullscreen_selection_start(mouse.row, mouse.column, kind);
                        if fullscreen_selecting {
                            continue;
                        }
                    }
                    Event::Mouse(mouse)
                        if mouse.kind == MouseEventKind::Drag(MouseButton::Left)
                            && fullscreen_selecting =>
                    {
                        ui.fullscreen_selection_drag(mouse.row, mouse.column);
                        continue;
                    }
                    Event::Mouse(mouse)
                        if mouse.kind == MouseEventKind::Up(MouseButton::Left)
                            && fullscreen_selecting =>
                    {
                        fullscreen_selecting = false;
                        if let Some(selected) = ui.fullscreen_selection_finish() {
                            if copy_on_select {
                                hint = match write_clipboard_text(&selected) {
                                    Ok(()) => "Selected transcript text copied".to_owned(),
                                    Err(error) => format!("Selection copy failed: {error}"),
                                };
                            }
                        }
                        continue;
                    }
                    Event::Key(KeyEvent {
                        code: KeyCode::Esc,
                        kind: KeyEventKind::Press | KeyEventKind::Repeat,
                        ..
                    }) if ui.fullscreen_has_selection() => {
                        ui.fullscreen_selection_clear();
                        hint.clear();
                        continue;
                    }
                    Event::Key(KeyEvent {
                        code: KeyCode::Char('c'),
                        modifiers,
                        kind: KeyEventKind::Press | KeyEventKind::Repeat,
                        ..
                    }) if ui.fullscreen_has_selection()
                        && modifiers.contains(KeyModifiers::CONTROL)
                        && !modifiers.intersects(
                            KeyModifiers::SHIFT | KeyModifiers::ALT | KeyModifiers::SUPER,
                        ) =>
                    {
                        hint = match ui.fullscreen_selection_take() {
                            Some(selected) => match write_clipboard_text(&selected) {
                                Ok(()) => "Selected transcript text copied".to_owned(),
                                Err(error) => format!("Selection copy failed: {error}"),
                            },
                            None => "No transcript selection to copy".to_owned(),
                        };
                        continue;
                    }
                    Event::Key(KeyEvent {
                        code,
                        modifiers,
                        kind: KeyEventKind::Press | KeyEventKind::Repeat,
                        ..
                    }) if ui.fullscreen_has_selection()
                        && modifiers.contains(KeyModifiers::SHIFT)
                        && !modifiers.contains(KeyModifiers::ALT)
                        && matches!(
                            code,
                            KeyCode::Left
                                | KeyCode::Right
                                | KeyCode::Up
                                | KeyCode::Down
                                | KeyCode::Home
                                | KeyCode::End
                        ) =>
                    {
                        let movement = match code {
                            KeyCode::Left => SelectionFocusMove::Left,
                            KeyCode::Right => SelectionFocusMove::Right,
                            KeyCode::Up => SelectionFocusMove::Up,
                            KeyCode::Down => SelectionFocusMove::Down,
                            KeyCode::Home => SelectionFocusMove::LineStart,
                            KeyCode::End => SelectionFocusMove::LineEnd,
                            _ => unreachable!("selection movement was matched above"),
                        };
                        ui.fullscreen_selection_move(movement);
                        continue;
                    }
                    _ => {}
                }
                let wheel_up = match &event {
                    Event::Mouse(mouse) if mouse.kind == MouseEventKind::ScrollUp => Some(true),
                    Event::Mouse(mouse) if mouse.kind == MouseEventKind::ScrollDown => Some(false),
                    _ => None,
                };
                if let Some(up) = wheel_up {
                    match self.keybindings.resolve_wheel(up, &["Scroll"]) {
                        KeyResolution::Match(action)
                            if action
                                == if up {
                                    "scroll:lineUp"
                                } else {
                                    "scroll:lineDown"
                                } =>
                        {
                            let at = self.fullscreen_wheel_epoch.elapsed();
                            ui.fullscreen_scroll(if up {
                                FullscreenScroll::WheelUp(at)
                            } else {
                                FullscreenScroll::WheelDown(at)
                            })?;
                        }
                        KeyResolution::Unbound => {}
                        KeyResolution::Match(action) => {
                            hint = format!("Action {action} is unavailable for mouse wheel");
                        }
                        KeyResolution::ChordStarted => {
                            hint = "Mouse wheel chord started".to_owned();
                        }
                        KeyResolution::ChordCancelled => {
                            hint = "Mouse wheel chord cancelled".to_owned();
                        }
                        KeyResolution::None => {}
                    }
                    continue;
                }
                if let Event::Resize(columns, rows) = &event {
                    ui.resize_fullscreen(*columns, *rows)?;
                    continue;
                }
            }
            let previous_buffer = buffer.clone();
            let previous_cursor_byte = cursor_byte;
            let mut restored_undo = false;
            let mut open_external_editor = false;
            let previous_selected_name = suggestions
                .get(selected_suggestion)
                .map(|suggestion| suggestion.name.clone());
            let previous_selected_file = file_suggestions
                .get(selected_file_suggestion)
                .map(|suggestion| suggestion.display_path.clone());
            let previous_selected_argument = argument_suggestions
                .get(selected_argument_suggestion)
                .map(|suggestion| (*suggestion).clone());
            match event {
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
                    let vim_history_search = matches!(
                        key,
                        KeyEvent {
                            code: KeyCode::Char('/'),
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                    ) && self
                        .vim
                        .as_ref()
                        .is_some_and(|vim| vim.mode() == VimMode::Normal && !vim.pending_command());
                    let overlay_navigation = (!file_suggestions.is_empty()
                        || !argument_suggestions.is_empty()
                        || !suggestions.is_empty())
                        && matches!(
                            key.code,
                            KeyCode::Up
                                | KeyCode::Down
                                | KeyCode::Tab
                                | KeyCode::BackTab
                                | KeyCode::Enter
                        );
                    let key = if history_search.is_none()
                        && !overlay_navigation
                        && !vim_history_search
                    {
                        if let (Some(vim), Some(vim_event)) = (self.vim.as_mut(), vim_event(key)) {
                            let outcome =
                                vim.handle_event(&mut buffer, &mut cursor_byte, vim_event);
                            if outcome.changed {
                                dismissed_suggestions_for = None;
                                dismissed_file_suggestions_for = None;
                                dismissed_argument_suggestions_for = None;
                            }
                            match outcome.action {
                                Some(VimAction::Submit) => key,
                                Some(VimAction::LimitReached) => {
                                    hint = "Vim edit exceeds an input resource limit".to_owned();
                                    continue;
                                }
                                None if outcome.handled => {
                                    if matches!(key.code, KeyCode::Esc) {
                                        if !suggestions.is_empty() {
                                            dismissed_suggestions_for = Some(buffer.clone());
                                        }
                                        if !file_suggestions.is_empty() {
                                            dismissed_file_suggestions_for =
                                                Some((buffer.clone(), cursor_byte));
                                        }
                                        if !argument_suggestions.is_empty() {
                                            dismissed_argument_suggestions_for =
                                                Some((buffer.clone(), cursor_byte));
                                        }
                                    }
                                    continue;
                                }
                                None => key,
                            }
                        } else {
                            key
                        }
                    } else {
                        key
                    };
                    let key = if vim_history_search {
                        KeyEvent {
                            code: KeyCode::Char('r'),
                            modifiers: KeyModifiers::CONTROL,
                            ..key
                        }
                    } else {
                        key
                    };
                    let mut contexts = Vec::with_capacity(3);
                    if history_search.is_some() {
                        contexts.push("HistorySearch");
                    } else if !file_suggestions.is_empty()
                        || !argument_suggestions.is_empty()
                        || !suggestions.is_empty()
                    {
                        contexts.push("Autocomplete");
                        contexts.push("Chat");
                    } else {
                        contexts.push("Chat");
                    }
                    if self
                        .ui
                        .as_ref()
                        .is_some_and(ConversationUi::fullscreen_active)
                    {
                        contexts.push("Scroll");
                    }
                    contexts.push("Global");
                    let key = match self.keybindings.resolve(key, &contexts) {
                        KeyResolution::None => key,
                        KeyResolution::Unbound => {
                            hint = "Keybinding disabled".to_owned();
                            continue;
                        }
                        KeyResolution::ChordStarted => {
                            hint = format!(
                                "Chord {} …",
                                self.keybindings.pending_display().unwrap_or_default()
                            );
                            continue;
                        }
                        KeyResolution::ChordCancelled => {
                            hint = "Keybinding chord cancelled".to_owned();
                            continue;
                        }
                        KeyResolution::Match(action) => match dispatch_binding(action) {
                            BindingDispatch::Key(key) => key,
                            BindingDispatch::Command(command) => {
                                rendered.erase(&mut out)?;
                                print_committed_prompt(&mut out, &command)?;
                                return Ok(Some(PromptRead {
                                    text: command,
                                    permission_mode: mode,
                                    clipboard_images: std::mem::take(&mut clipboard_images),
                                }));
                            }
                            BindingDispatch::FullscreenScroll(scroll) => {
                                if let Some(ui) =
                                    self.ui.as_ref().filter(|ui| ui.fullscreen_active())
                                {
                                    ui.fullscreen_scroll(scroll)?;
                                }
                                continue;
                            }
                            BindingDispatch::CopySelection => {
                                let selected = self
                                    .ui
                                    .as_ref()
                                    .filter(|ui| ui.fullscreen_active())
                                    .and_then(ConversationUi::fullscreen_selection_take);
                                hint = match selected {
                                    Some(selected) => match write_clipboard_text(&selected) {
                                        Ok(()) => "Selected transcript text copied".to_owned(),
                                        Err(error) => {
                                            format!("Selection copy failed: {error}")
                                        }
                                    },
                                    None => "No transcript selection to copy".to_owned(),
                                };
                                continue;
                            }
                            BindingDispatch::RedrawOrClear => {
                                let now = Instant::now();
                                if clear_input_pending.is_some_and(|started| {
                                    now.duration_since(started) <= CLEAR_INPUT_WINDOW
                                }) {
                                    rendered.erase(&mut out)?;
                                    let command = "/clear".to_owned();
                                    print_committed_prompt(&mut out, &command)?;
                                    return Ok(Some(PromptRead {
                                        text: command,
                                        permission_mode: mode,
                                        clipboard_images: std::mem::take(&mut clipboard_images),
                                    }));
                                }
                                clear_input_pending = Some(now);
                                rendered.reset_viewport(&mut out)?;
                                hint = "Redrawn; press again to clear the conversation".to_owned();
                                continue;
                            }
                            BindingDispatch::ClearScreen => {
                                execute!(out, Clear(ClearType::All), cursor::MoveTo(0, 0))?;
                                rendered = RenderedInput::default();
                                continue;
                            }
                            BindingDispatch::PasteImage => {
                                let used = clipboard_images
                                    .iter()
                                    .map(|image| image.bytes.len())
                                    .sum::<usize>();
                                if clipboard_images.len() >= MAX_CLIPBOARD_ATTACHMENTS {
                                    hint = format!(
                                        "Clipboard image limit reached ({MAX_CLIPBOARD_ATTACHMENTS})"
                                    );
                                } else if let Some(image) = read_clipboard_image() {
                                    if used.saturating_add(image.bytes.len())
                                        > MAX_CLIPBOARD_ATTACHMENT_BYTES
                                    {
                                        hint = "Clipboard images exceed the 8 MiB total limit"
                                            .to_owned();
                                    } else {
                                        let width = image.width;
                                        let height = image.height;
                                        clipboard_images.push(image);
                                        selected_attachment =
                                            Some(clipboard_images.len().saturating_sub(1));
                                        hint = format!(
                                            "Attached clipboard image {width}×{height} ({}/{MAX_CLIPBOARD_ATTACHMENTS})",
                                            clipboard_images.len()
                                        );
                                    }
                                } else {
                                    hint =
                                        "Clipboard does not contain a supported image".to_owned();
                                }
                                continue;
                            }
                            BindingDispatch::Unsupported(action) => {
                                hint = format!("Action {action} is unavailable in this view");
                                continue;
                            }
                        },
                    };
                    if let Some(ui) = self
                        .ui
                        .as_ref()
                        .filter(|ui| ui.fullscreen_active() && ui.fullscreen_has_selection())
                    {
                        let navigation = matches!(
                            key.code,
                            KeyCode::Left
                                | KeyCode::Right
                                | KeyCode::Up
                                | KeyCode::Down
                                | KeyCode::Home
                                | KeyCode::End
                                | KeyCode::PageUp
                                | KeyCode::PageDown
                        );
                        let preserves_selection = navigation
                            && key.modifiers.intersects(
                                KeyModifiers::SHIFT | KeyModifiers::ALT | KeyModifiers::SUPER,
                            );
                        if !preserves_selection {
                            ui.fullscreen_selection_clear();
                        }
                    }
                    if let Some(search) = history_search.as_mut() {
                        match key {
                            KeyEvent {
                                code: KeyCode::Char('r'),
                                modifiers: KeyModifiers::CONTROL,
                                ..
                            } => {
                                if !search.matches.is_empty() {
                                    search.selected = (search.selected + 1) % search.matches.len();
                                }
                            }
                            KeyEvent {
                                code: KeyCode::Char('s'),
                                modifiers: KeyModifiers::CONTROL,
                                ..
                            } => {
                                search.scope = search.scope.next();
                                search.selected = 0;
                                let entries = match search.scope {
                                    HistoryScope::Project => navigation_history.as_slice(),
                                    scope => self.history_for_scope(scope),
                                };
                                search.refresh(entries);
                            }
                            KeyEvent {
                                code: KeyCode::Backspace,
                                ..
                            } if search.query.is_empty() => {
                                buffer.clone_from(&search.original.text);
                                cursor_byte = search.original.cursor_byte.min(buffer.len());
                                history_search = None;
                                hint = "History search cancelled".to_owned();
                                continue;
                            }
                            KeyEvent {
                                code: KeyCode::Backspace,
                                ..
                            } => {
                                search.query.pop();
                                search.selected = 0;
                                search.refresh(self.history_for_scope(search.scope));
                            }
                            KeyEvent {
                                code: KeyCode::Char('c'),
                                modifiers: KeyModifiers::CONTROL,
                                ..
                            } => {
                                buffer.clone_from(&search.original.text);
                                cursor_byte = search.original.cursor_byte.min(buffer.len());
                                history_search = None;
                                hint = "History search cancelled".to_owned();
                                continue;
                            }
                            KeyEvent {
                                code: KeyCode::Esc | KeyCode::Tab,
                                ..
                            } => {
                                buffer = search.current().to_owned();
                                cursor_byte = buffer.len();
                                history_search = None;
                                hint = "History match accepted".to_owned();
                                continue;
                            }
                            KeyEvent {
                                code: KeyCode::Enter,
                                modifiers: KeyModifiers::NONE,
                                ..
                            } => {
                                let Some(found) = search.matches.get(search.selected) else {
                                    hint = "History search has no executable match".to_owned();
                                    continue;
                                };
                                let text = found.trim_end().to_owned();
                                if text.trim().is_empty() {
                                    hint = "History search has no executable match".to_owned();
                                    continue;
                                }
                                rendered.erase(&mut out)?;
                                self.push_history(text.clone());
                                print_committed_prompt(&mut out, &text)?;
                                return Ok(Some(PromptRead {
                                    text,
                                    permission_mode: mode,
                                    clipboard_images: std::mem::take(&mut clipboard_images),
                                }));
                            }
                            KeyEvent {
                                code: KeyCode::Char(character),
                                modifiers,
                                ..
                            } if !modifiers.intersects(
                                KeyModifiers::CONTROL
                                    | KeyModifiers::ALT
                                    | KeyModifiers::SUPER
                                    | KeyModifiers::HYPER,
                            ) && search.query.len().saturating_add(character.len_utf8())
                                <= MAX_HISTORY_SEARCH_QUERY_BYTES =>
                            {
                                search.query.push(character);
                                search.selected = 0;
                                search.refresh(self.history_for_scope(search.scope));
                            }
                            _ => {}
                        }
                        buffer = search.current().to_owned();
                        cursor_byte = buffer.len();
                        hint = search.hint();
                        continue;
                    }
                    let is_yank_key = matches!(
                        key,
                        KeyEvent {
                            code: KeyCode::Char('y'),
                            modifiers: KeyModifiers::CONTROL | KeyModifiers::ALT,
                            ..
                        }
                    );
                    if !is_yank_key {
                        last_yank = None;
                    }
                    hint.clear();
                    let is_exit_key = matches!(
                        key,
                        KeyEvent {
                            code: KeyCode::Char('c'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        }
                    ) || (buffer.is_empty()
                        && matches!(
                            key,
                            KeyEvent {
                                code: KeyCode::Char('d'),
                                modifiers: KeyModifiers::CONTROL,
                                ..
                            }
                        ));
                    if !is_exit_key {
                        exit_pending = None;
                    }
                    match key {
                        KeyEvent {
                            code: KeyCode::Left,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if selected_attachment.is_some() && !clipboard_images.is_empty() => {
                            let selected = selected_attachment.unwrap_or_default();
                            selected_attachment = Some(if selected == 0 {
                                clipboard_images.len() - 1
                            } else {
                                selected - 1
                            });
                        }
                        KeyEvent {
                            code: KeyCode::Right,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if selected_attachment.is_some() && !clipboard_images.is_empty() => {
                            selected_attachment = Some(
                                (selected_attachment.unwrap_or_default() + 1)
                                    % clipboard_images.len(),
                            );
                        }
                        KeyEvent {
                            code: KeyCode::Backspace | KeyCode::Delete,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if selected_attachment.is_some() && !clipboard_images.is_empty() => {
                            let selected = selected_attachment
                                .unwrap_or_default()
                                .min(clipboard_images.len() - 1);
                            clipboard_images.remove(selected);
                            selected_attachment = if clipboard_images.is_empty() {
                                None
                            } else {
                                Some(selected.min(clipboard_images.len() - 1))
                            };
                            hint = "Removed image attachment".to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Down | KeyCode::Esc,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if selected_attachment.is_some() => {
                            selected_attachment = None;
                            hint = "Attachment navigation closed".to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Up,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('p'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !file_suggestions.is_empty() => {
                            selected_file_suggestion = if selected_file_suggestion == 0 {
                                file_suggestions.len() - 1
                            } else {
                                selected_file_suggestion - 1
                            };
                        }
                        KeyEvent {
                            code: KeyCode::Down,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('n'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !file_suggestions.is_empty() => {
                            selected_file_suggestion =
                                (selected_file_suggestion + 1) % file_suggestions.len();
                        }
                        KeyEvent {
                            code: KeyCode::Tab,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if !file_suggestions.is_empty() => {
                            let token = file_token.as_ref().expect("file suggestions have a token");
                            let common = common_file_prefix(&file_suggestions);
                            if file_suggestions.len() > 1 && common.len() > token.query.len() {
                                cursor_byte = replace_file_token(
                                    &mut buffer,
                                    cursor_byte,
                                    token,
                                    &common,
                                    false,
                                    true,
                                );
                                hint = "Completed common file prefix".to_owned();
                            } else {
                                let suggestion = file_suggestions[selected_file_suggestion];
                                cursor_byte = replace_file_token(
                                    &mut buffer,
                                    cursor_byte,
                                    token,
                                    &suggestion.display_path,
                                    suggestion.is_dir,
                                    false,
                                );
                                hint = "File reference inserted".to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Enter,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if !file_suggestions.is_empty() => {
                            let token = file_token.as_ref().expect("file suggestions have a token");
                            let suggestion = file_suggestions[selected_file_suggestion];
                            cursor_byte = replace_file_token(
                                &mut buffer,
                                cursor_byte,
                                token,
                                &suggestion.display_path,
                                suggestion.is_dir,
                                false,
                            );
                            hint = "File reference inserted".to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Esc, ..
                        } if !file_suggestions.is_empty() => {
                            dismissed_file_suggestions_for = Some((buffer.clone(), cursor_byte));
                            hint = "File suggestions dismissed".to_owned();
                            last_escape = None;
                        }
                        KeyEvent {
                            code: KeyCode::Up,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('p'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !argument_suggestions.is_empty() => {
                            selected_argument_suggestion = if selected_argument_suggestion == 0 {
                                argument_suggestions.len() - 1
                            } else {
                                selected_argument_suggestion - 1
                            };
                        }
                        KeyEvent {
                            code: KeyCode::Down,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('n'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !argument_suggestions.is_empty() => {
                            selected_argument_suggestion =
                                (selected_argument_suggestion + 1) % argument_suggestions.len();
                        }
                        KeyEvent {
                            code: KeyCode::Tab,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if !argument_suggestions.is_empty() => {
                            let token = argument_token
                                .as_ref()
                                .expect("argument suggestions have a token");
                            let selected = argument_suggestions[selected_argument_suggestion];
                            buffer.replace_range(token.start..token.end, selected);
                            cursor_byte = token.start.saturating_add(selected.len());
                            hint = "Command argument inserted".to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Enter,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if !argument_suggestions.is_empty()
                            && argument_token.as_ref().is_some_and(|token| {
                                token.query != *argument_suggestions[selected_argument_suggestion]
                            }) =>
                        {
                            let token = argument_token
                                .as_ref()
                                .expect("argument suggestions have a token");
                            let selected = argument_suggestions[selected_argument_suggestion];
                            buffer.replace_range(token.start..token.end, selected);
                            cursor_byte = token.start.saturating_add(selected.len());
                            hint = "Command argument inserted".to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Esc, ..
                        } if !argument_suggestions.is_empty() => {
                            dismissed_argument_suggestions_for =
                                Some((buffer.clone(), cursor_byte));
                            hint = "Argument suggestions dismissed".to_owned();
                            last_escape = None;
                        }
                        KeyEvent {
                            code: KeyCode::Up,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('p'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !suggestions.is_empty() => {
                            selected_suggestion = if selected_suggestion == 0 {
                                suggestions.len() - 1
                            } else {
                                selected_suggestion - 1
                            };
                        }
                        KeyEvent {
                            code: KeyCode::Down,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('n'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !suggestions.is_empty() => {
                            selected_suggestion = (selected_suggestion + 1) % suggestions.len();
                        }
                        KeyEvent {
                            code: KeyCode::Tab,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if !suggestions.is_empty() => {
                            let suggestion = suggestions[selected_suggestion];
                            buffer = format!("/{} ", suggestion.name);
                            cursor_byte = buffer.len();
                        }
                        KeyEvent {
                            code: KeyCode::Esc, ..
                        } if !suggestions.is_empty() => {
                            dismissed_suggestions_for = Some(buffer.clone());
                            hint = "Suggestions dismissed".to_owned();
                            last_escape = None;
                        }
                        KeyEvent {
                            code: KeyCode::Tab,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if buffer.starts_with('!') => {
                            if let Some(completed) = self
                                .history
                                .iter()
                                .rev()
                                .find(|entry| entry.starts_with(&buffer) && *entry != &buffer)
                            {
                                buffer.clone_from(completed);
                                cursor_byte = buffer.len();
                                hint = "Completed from shell history".to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Enter,
                            modifiers,
                            ..
                        } if modifiers.intersects(KeyModifiers::SHIFT | KeyModifiers::ALT) => {
                            if buffer.len() < MAX_INPUT_BYTES {
                                buffer.insert(cursor_byte, '\n');
                                cursor_byte += 1;
                            } else {
                                hint = "Input limit reached".to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Enter,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if buffer[..cursor_byte].ends_with('\\') => {
                            buffer.remove(cursor_byte - 1);
                            cursor_byte -= 1;
                            buffer.insert(cursor_byte, '\n');
                            cursor_byte += 1;
                        }
                        KeyEvent {
                            code: KeyCode::Enter,
                            ..
                        } => {
                            let execute_suggestion = if suggestions.is_empty() {
                                false
                            } else {
                                let suggestion = suggestions[selected_suggestion];
                                buffer = format!("/{} ", suggestion.name);
                                cursor_byte = buffer.len();
                                suggestion.execute_on_enter
                            };
                            if suggestions.is_empty() || execute_suggestion {
                                let text = buffer.trim_end().to_owned();
                                if text.trim().is_empty() {
                                    hint = "Type a message or / for commands".to_owned();
                                } else {
                                    rendered.erase(&mut out)?;
                                    self.push_history(text.clone());
                                    print_committed_prompt(&mut out, &text)?;
                                    return Ok(Some(PromptRead {
                                        text,
                                        permission_mode: mode,
                                        clipboard_images: std::mem::take(&mut clipboard_images),
                                    }));
                                }
                            }
                        }
                        KeyEvent {
                            code: KeyCode::BackTab,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Tab,
                            modifiers: KeyModifiers::SHIFT,
                            ..
                        } => {
                            if mode_locked {
                                hint =
                                    format!("{} mode is locked for this session", mode_label(mode));
                            } else {
                                mode = next_mode(mode);
                                hint = format!("{} mode", mode_label(mode));
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('j'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            if buffer.len() < MAX_INPUT_BYTES {
                                buffer.insert(cursor_byte, '\n');
                                cursor_byte += 1;
                            } else {
                                hint = "Input limit reached".to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('c'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            if !buffer.is_empty() {
                                buffer.clear();
                                cursor_byte = 0;
                                hint = "Input cleared; press Ctrl-C again to exit".to_owned();
                                exit_pending =
                                    Some(ExitPending::new(ExitKey::CtrlC, Instant::now()));
                            } else if arm_or_confirm_exit(
                                &mut exit_pending,
                                ExitKey::CtrlC,
                                Instant::now(),
                            ) {
                                rendered.erase(&mut out)?;
                                return Ok(None);
                            } else {
                                hint = exit_pending.expect("exit was armed").hint().to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('d'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if buffer.is_empty() => {
                            if arm_or_confirm_exit(
                                &mut exit_pending,
                                ExitKey::CtrlD,
                                Instant::now(),
                            ) {
                                rendered.erase(&mut out)?;
                                return Ok(None);
                            }
                            hint = exit_pending.expect("exit was armed").hint().to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Esc, ..
                        } if buffer == "!" => {
                            buffer.clear();
                            cursor_byte = 0;
                            hint = "Shell mode cancelled".to_owned();
                            last_escape = None;
                        }
                        KeyEvent {
                            code: KeyCode::Esc, ..
                        } => {
                            if last_escape.is_some_and(|at| at.elapsed() <= EXIT_WINDOW) {
                                if buffer.is_empty() {
                                    if let Some(ui) =
                                        self.ui.as_ref().filter(|ui| ui.fullscreen_active())
                                    {
                                        ui.render_fullscreen_prompt(&[], 1)?;
                                    } else {
                                        rendered.erase(&mut out)?;
                                    }
                                    drop(raw_guard.take());
                                    let outcome = rewind_picker();
                                    raw_guard = Some(RawModeGuard::enter()?);
                                    rendered = RenderedInput::default();
                                    last_escape = None;
                                    match outcome? {
                                        ModelPickerOutcome::Selected(checkpoint) => {
                                            let command = format!("/rewind {checkpoint} --confirm");
                                            print_committed_prompt(&mut out, &command)?;
                                            return Ok(Some(PromptRead {
                                                text: command,
                                                permission_mode: mode,
                                                clipboard_images: std::mem::take(
                                                    &mut clipboard_images,
                                                ),
                                            }));
                                        }
                                        ModelPickerOutcome::Cancelled => {
                                            hint = "Restore selection cancelled".to_owned();
                                            continue;
                                        }
                                        ModelPickerOutcome::Exit => return Ok(None),
                                    }
                                }
                                if !buffer.trim().is_empty() {
                                    self.push_history(buffer.clone());
                                }
                                buffer.clear();
                                cursor_byte = 0;
                                hint = "Input cleared and saved to history".to_owned();
                                last_escape = None;
                            } else {
                                hint = "Press Esc again to clear input".to_owned();
                                last_escape = Some(Instant::now());
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('_' | '-'),
                            modifiers,
                            ..
                        } if modifiers.contains(KeyModifiers::CONTROL) => {
                            if let Some(vim) = self.vim.as_mut() {
                                if vim.undo_current(&mut buffer, &mut cursor_byte) {
                                    restored_undo = true;
                                    hint = "Undid last Vim edit".to_owned();
                                } else {
                                    hint = "Nothing to undo".to_owned();
                                }
                            } else if let Some(snapshot) = undo_stack.pop() {
                                buffer = snapshot.text;
                                cursor_byte = snapshot.cursor_byte.min(buffer.len());
                                restored_undo = true;
                                hint = "Undid last edit".to_owned();
                            } else {
                                hint = "Nothing to undo".to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('s'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            if buffer.is_empty() {
                                if let Some(snapshot) = self.stashed_prompt.take() {
                                    buffer = snapshot.text;
                                    cursor_byte = snapshot.cursor_byte.min(buffer.len());
                                    hint = "Restored stashed prompt".to_owned();
                                } else {
                                    hint = "No stashed prompt".to_owned();
                                }
                            } else {
                                self.stashed_prompt = Some(EditorSnapshot {
                                    text: std::mem::take(&mut buffer),
                                    cursor_byte,
                                });
                                cursor_byte = 0;
                                hint = "Prompt stashed; Ctrl-S restores it".to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('r'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            let search = HistorySearch::new(
                                self.history_for_scope(HistoryScope::Session),
                                HistoryScope::Session,
                                buffer.clone(),
                                cursor_byte,
                            );
                            buffer = search.current().to_owned();
                            cursor_byte = buffer.len();
                            hint = search.hint();
                            history_search = Some(search);
                        }
                        KeyEvent {
                            code: KeyCode::Char('p'),
                            modifiers: KeyModifiers::ALT,
                            ..
                        } => {
                            if let Some(ui) = self.ui.as_ref().filter(|ui| ui.fullscreen_active()) {
                                ui.render_fullscreen_prompt(&[], 1)?;
                            } else {
                                rendered.erase(&mut out)?;
                            }
                            drop(raw_guard.take());
                            let outcome = model_picker();
                            raw_guard = Some(RawModeGuard::enter()?);
                            rendered = RenderedInput::default();
                            match outcome? {
                                ModelPickerOutcome::Selected(model) => {
                                    hint = format!("Model set to {}", single_line(&model, 80));
                                }
                                ModelPickerOutcome::Cancelled => {
                                    hint = "Model selection cancelled".to_owned();
                                }
                                ModelPickerOutcome::Exit => return Ok(None),
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('t'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            show_todos = !show_todos;
                            hint = if show_todos {
                                if todos.is_empty() {
                                    "Todo list is empty · Ctrl-T to close".to_owned()
                                } else {
                                    format!("{} todo item(s) · Ctrl-T to close", todos.len())
                                }
                            } else {
                                "Todo list hidden".to_owned()
                            };
                        }
                        KeyEvent {
                            code: KeyCode::Char('o'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            let fullscreen_ui = self
                                .ui
                                .as_ref()
                                .filter(|ui| ui.fullscreen_active())
                                .cloned();
                            if let Some(ui) = &fullscreen_ui {
                                ui.set_tui_mode(TuiMode::Default)?;
                            } else {
                                rendered.erase(&mut out)?;
                            }
                            drop(raw_guard.take());
                            let outcome = transcript_viewer();
                            raw_guard = Some(RawModeGuard::enter()?);
                            if let Some(ui) = &fullscreen_ui {
                                ui.set_tui_mode(TuiMode::Fullscreen)?;
                            }
                            rendered = RenderedInput::default();
                            match outcome {
                                Ok(()) => hint = "Returned from transcript".to_owned(),
                                Err(error) => hint = format!("Transcript unavailable: {error:#}"),
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('g'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => open_external_editor = true,
                        KeyEvent {
                            code: KeyCode::Char('a'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => cursor_byte = line_start(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Char('e'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => cursor_byte = line_end(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Char('b'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => cursor_byte = previous_boundary(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Char('f'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => cursor_byte = next_boundary(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Char('b') | KeyCode::Left,
                            modifiers: KeyModifiers::ALT,
                            ..
                        } => cursor_byte = previous_word_boundary(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Char('f') | KeyCode::Right,
                            modifiers: KeyModifiers::ALT,
                            ..
                        } => cursor_byte = next_word_boundary(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Left,
                            ..
                        } => cursor_byte = previous_boundary(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Right,
                            ..
                        } => cursor_byte = next_boundary(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Home,
                            ..
                        } => cursor_byte = line_start(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::End, ..
                        } => cursor_byte = line_end(&buffer, cursor_byte),
                        KeyEvent {
                            code: KeyCode::Backspace,
                            modifiers,
                            ..
                        } if modifiers.intersects(KeyModifiers::ALT | KeyModifiers::CONTROL)
                            && cursor_byte > 0 =>
                        {
                            let previous = previous_word_boundary(&buffer, cursor_byte);
                            push_kill(&mut kill_ring, buffer[previous..cursor_byte].to_owned());
                            buffer.drain(previous..cursor_byte);
                            cursor_byte = previous;
                        }
                        KeyEvent {
                            code: KeyCode::Char('w'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if cursor_byte > 0 => {
                            let previous = previous_word_boundary(&buffer, cursor_byte);
                            push_kill(&mut kill_ring, buffer[previous..cursor_byte].to_owned());
                            buffer.drain(previous..cursor_byte);
                            cursor_byte = previous;
                        }
                        KeyEvent {
                            code: KeyCode::Char('u'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if cursor_byte > 0 => {
                            let start = line_start(&buffer, cursor_byte);
                            let start = if start == cursor_byte && start > 0 {
                                start - 1
                            } else {
                                start
                            };
                            push_kill(&mut kill_ring, buffer[start..cursor_byte].to_owned());
                            buffer.drain(start..cursor_byte);
                            cursor_byte = start;
                            hint = "Ctrl+Y to paste deleted text".to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Char('k'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if cursor_byte < buffer.len() => {
                            let mut end = line_end(&buffer, cursor_byte);
                            if end == cursor_byte && buffer.as_bytes().get(end) == Some(&b'\n') {
                                end += 1;
                            }
                            push_kill(&mut kill_ring, buffer[cursor_byte..end].to_owned());
                            buffer.drain(cursor_byte..end);
                        }
                        KeyEvent {
                            code: KeyCode::Char('y'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !kill_ring.is_empty() => {
                            let killed = &kill_ring[0];
                            if buffer.len().saturating_add(killed.len()) <= MAX_INPUT_BYTES {
                                let start = cursor_byte;
                                buffer.insert_str(cursor_byte, killed);
                                cursor_byte += killed.len();
                                last_yank = Some((start, killed.len(), 0));
                            } else {
                                hint = "Input limit reached".to_owned();
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('y'),
                            modifiers: KeyModifiers::ALT,
                            ..
                        } if kill_ring.len() > 1 && last_yank.is_some() => {
                            let (start, length, index) = last_yank.expect("guarded above");
                            let next_index = (index + 1) % kill_ring.len();
                            let killed = &kill_ring[next_index];
                            let next_len = buffer
                                .len()
                                .saturating_sub(length)
                                .saturating_add(killed.len());
                            if next_len <= MAX_INPUT_BYTES
                                && start.saturating_add(length) <= buffer.len()
                            {
                                buffer.replace_range(start..start + length, killed);
                                cursor_byte = start + killed.len();
                                last_yank = Some((start, killed.len(), next_index));
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Backspace,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if buffer.is_empty() && !clipboard_images.is_empty() => {
                            clipboard_images.pop();
                            hint = "Removed the last image attachment".to_owned();
                        }
                        KeyEvent {
                            code: KeyCode::Char('h'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Backspace,
                            ..
                        } if cursor_byte > 0 => {
                            let previous = previous_boundary(&buffer, cursor_byte);
                            buffer.drain(previous..cursor_byte);
                            cursor_byte = previous;
                        }
                        KeyEvent {
                            code: KeyCode::Char('d') | KeyCode::Delete,
                            modifiers: KeyModifiers::ALT,
                            ..
                        } if cursor_byte < buffer.len() => {
                            let next = next_word_boundary(&buffer, cursor_byte);
                            push_kill(&mut kill_ring, buffer[cursor_byte..next].to_owned());
                            buffer.drain(cursor_byte..next);
                        }
                        KeyEvent {
                            code: KeyCode::Delete,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('d'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if cursor_byte < buffer.len() => {
                            let next = next_boundary(&buffer, cursor_byte);
                            buffer.drain(cursor_byte..next);
                        }
                        KeyEvent {
                            code: KeyCode::Up,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('p'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if line_start(&buffer, cursor_byte) > 0 => {
                            cursor_byte = move_vertical(&buffer, cursor_byte, -1);
                        }
                        KeyEvent {
                            code: KeyCode::Down,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('n'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if line_end(&buffer, cursor_byte) < buffer.len() => {
                            cursor_byte = move_vertical(&buffer, cursor_byte, 1);
                        }
                        KeyEvent {
                            code: KeyCode::Up,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('p'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !navigation_history.is_empty() => {
                            if history_index == navigation_history.len() {
                                draft.clone_from(&buffer);
                            }
                            history_index = history_index.saturating_sub(1);
                            buffer.clone_from(&navigation_history[history_index]);
                            cursor_byte = 0;
                        }
                        KeyEvent {
                            code: KeyCode::Down,
                            modifiers: KeyModifiers::NONE,
                            ..
                        }
                        | KeyEvent {
                            code: KeyCode::Char('n'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if history_index < navigation_history.len() => {
                            history_index += 1;
                            if history_index == navigation_history.len() {
                                buffer.clone_from(&draft);
                            } else {
                                buffer.clone_from(&navigation_history[history_index]);
                            }
                            cursor_byte = buffer.len();
                        }
                        KeyEvent {
                            code: KeyCode::Char('l'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            execute!(out, Clear(ClearType::All), cursor::MoveTo(0, 0))?;
                            rendered = RenderedInput::default();
                        }
                        KeyEvent {
                            code: KeyCode::Char(character),
                            modifiers,
                            ..
                        } if !modifiers.intersects(
                            KeyModifiers::CONTROL
                                | KeyModifiers::ALT
                                | KeyModifiers::SUPER
                                | KeyModifiers::HYPER,
                        ) =>
                        {
                            if buffer.len().saturating_add(character.len_utf8()) <= MAX_INPUT_BYTES
                            {
                                buffer.insert(cursor_byte, character);
                                cursor_byte += character.len_utf8();
                            } else {
                                hint = "Input limit reached".to_owned();
                            }
                        }
                        _ => {}
                    }
                }
                Event::Paste(text) => {
                    let text = sanitize_paste(&text);
                    if let Some(vim) = self.vim.as_mut() {
                        let outcome = vim.handle_event(
                            &mut buffer,
                            &mut cursor_byte,
                            VimEvent::key(VimKey::Text(text.clone())),
                        );
                        if outcome.action == Some(VimAction::LimitReached) {
                            hint = "Paste exceeds the Vim input limit".to_owned();
                            continue;
                        }
                        if outcome.handled {
                            continue;
                        }
                    }
                    let available = MAX_INPUT_BYTES.saturating_sub(buffer.len());
                    let mut end = available.min(text.len());
                    while !text.is_char_boundary(end) {
                        end -= 1;
                    }
                    buffer.insert_str(cursor_byte, &text[..end]);
                    cursor_byte += end;
                    if end < text.len() {
                        hint = "Paste truncated at the input limit".to_owned();
                    }
                }
                // Clear only the owned composer rows. Clearing the whole screen
                // makes a resize destroy the visible conversation and is
                // especially hostile when the terminal has no native scrollback.
                Event::Resize(_, _) => rendered.reset_viewport(&mut out)?,
                _ => {}
            }
            if open_external_editor {
                let fullscreen_ui = self
                    .ui
                    .as_ref()
                    .filter(|ui| ui.fullscreen_active())
                    .cloned();
                if let Some(ui) = &fullscreen_ui {
                    ui.set_tui_mode(TuiMode::Default)?;
                } else {
                    rendered.erase(&mut out)?;
                }
                drop(raw_guard.take());
                let edited = edit_prompt_externally(&buffer);
                raw_guard = Some(RawModeGuard::enter()?);
                if let Some(ui) = &fullscreen_ui {
                    ui.set_tui_mode(TuiMode::Fullscreen)?;
                }
                rendered = RenderedInput::default();
                match edited {
                    Ok(text) => {
                        buffer = text;
                        cursor_byte = buffer.len();
                        hint = "External editor changes loaded".to_owned();
                    }
                    Err(error) => {
                        hint = format!("External editor failed: {error:#}");
                    }
                }
            }
            if buffer != previous_buffer || cursor_byte != previous_cursor_byte {
                if buffer != previous_buffer {
                    if let Some(vim) = self.vim.as_mut() {
                        vim.reset_buffer();
                    }
                }
                if buffer != previous_buffer && !restored_undo {
                    if undo_stack.last().is_none_or(|snapshot| {
                        snapshot.text != previous_buffer
                            || snapshot.cursor_byte != previous_cursor_byte
                    }) {
                        undo_stack.push(EditorSnapshot {
                            text: previous_buffer.clone(),
                            cursor_byte: previous_cursor_byte,
                        });
                    }
                    if undo_stack.len() > 50 {
                        undo_stack.remove(0);
                    }
                }
                dismissed_suggestions_for = None;
                dismissed_file_suggestions_for = None;
                dismissed_argument_suggestions_for = None;
                if buffer != previous_buffer {
                    // Filtering establishes a new ranked list. Focus its best
                    // match; preserve an explicit keyboard selection only
                    // while the query itself is unchanged.
                    selected_suggestion = 0;
                    selected_file_suggestion = 0;
                    selected_argument_suggestion = 0;
                } else {
                    let updated_suggestions = command_matches(&buffer, commands);
                    selected_suggestion = previous_selected_name
                        .as_deref()
                        .and_then(|name| {
                            updated_suggestions
                                .iter()
                                .position(|suggestion| suggestion.name == name)
                        })
                        .unwrap_or(0);
                    let updated_file_suggestions = file_token_at_cursor(&buffer, cursor_byte)
                        .map_or_else(Vec::new, |token| file_matches(&token, files));
                    selected_file_suggestion = previous_selected_file
                        .as_deref()
                        .and_then(|path| {
                            updated_file_suggestions
                                .iter()
                                .position(|suggestion| suggestion.display_path == path)
                        })
                        .unwrap_or(0);
                    let updated_argument_suggestions =
                        argument_matches(&buffer, cursor_byte, commands)
                            .map_or_else(Vec::new, |(_, matches)| matches);
                    selected_argument_suggestion = previous_selected_argument
                        .as_deref()
                        .and_then(|value| {
                            updated_argument_suggestions
                                .iter()
                                .position(|suggestion| suggestion.as_str() == value)
                        })
                        .unwrap_or(0);
                }
            }
        }
    }

    fn push_history(&mut self, value: String) {
        if self.history.last() == Some(&value) {
            return;
        }
        self.history.push(value);
        if self.history.len() > self.history_limit {
            let excess = self.history.len() - self.history_limit;
            self.history.drain(..excess);
        }
    }
}

fn bounded_history(entries: impl IntoIterator<Item = String>, limit: usize) -> Vec<String> {
    let mut history = entries
        .into_iter()
        .filter(|entry| !entry.trim().is_empty() && entry.len() <= MAX_INPUT_BYTES)
        .collect::<Vec<_>>();
    if history.len() > limit {
        history.drain(..history.len() - limit);
    }
    history
}

#[derive(Debug, Clone)]
struct ComposerInputRow {
    local_row: u16,
    byte_start: usize,
    byte_end: usize,
}

#[derive(Debug, Clone, Default)]
struct FullscreenComposerHitMap {
    top_row: u16,
    rows: Vec<ComposerInputRow>,
}

impl FullscreenComposerHitMap {
    fn cursor_at(&self, screen_row: u16, screen_column: u16, buffer: &str) -> Option<usize> {
        let local_row = screen_row.checked_sub(self.top_row)?;
        let row = self.rows.iter().find(|row| row.local_row == local_row)?;
        let visible = buffer.get(row.byte_start..row.byte_end)?;
        let target = usize::from(screen_column.saturating_sub(2));
        let mut width = 0usize;
        let mut bytes = 0usize;
        for grapheme in visible.graphemes(true) {
            let next_width = width.saturating_add(UnicodeWidthStr::width(grapheme));
            if target < next_width {
                let choose_after = target.saturating_sub(width).saturating_mul(2)
                    >= next_width.saturating_sub(width);
                return Some(
                    row.byte_start
                        .saturating_add(bytes)
                        .saturating_add(if choose_after { grapheme.len() } else { 0 }),
                );
            }
            width = next_width;
            bytes = bytes.saturating_add(grapheme.len());
        }
        Some(row.byte_end)
    }
}

#[derive(Default)]
struct RenderedInput {
    rows: u16,
    cursor_row: u16,
    input_rows: Vec<ComposerInputRow>,
}

struct InputRenderState<'a> {
    buffer: &'a str,
    cursor_byte: usize,
    mode: PermissionMode,
    hint: &'a str,
    suggestions: &'a [&'a SlashCommandSuggestion],
    selected_suggestion: usize,
    file_suggestions: &'a [&'a FileSuggestion],
    selected_file_suggestion: usize,
    argument_suggestions: &'a [&'a String],
    selected_argument_suggestion: usize,
    argument_hint: Option<&'a str>,
    todos: Option<&'a [String]>,
    status_line: Option<&'a str>,
    theme: ThemePreset,
    vim_mode: Option<VimMode>,
    vim_selection: Option<(usize, usize, bool)>,
}

impl RenderedInput {
    fn redraw(&mut self, out: &mut impl Write, state: InputRenderState<'_>) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        self.draw(&mut frame, state)?;
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;
        Ok(())
    }

    fn erase(&mut self, out: &mut impl Write) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;
        Ok(())
    }

    fn reset_viewport(&mut self, out: &mut impl Write) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        queue!(
            frame,
            cursor::MoveToColumn(0),
            Clear(ClearType::FromCursorDown)
        )?;
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;
        Ok(())
    }

    fn clear(&mut self, out: &mut impl Write) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let below = self.rows.saturating_sub(self.cursor_row + 1);
        if below > 0 {
            queue!(out, cursor::MoveDown(below))?;
        }
        queue!(
            out,
            cursor::MoveDown(1),
            cursor::MoveUp(self.rows),
            cursor::MoveToColumn(0),
            Clear(ClearType::FromCursorDown)
        )?;
        *self = Self::default();
        Ok(())
    }

    fn draw(&mut self, out: &mut impl Write, state: InputRenderState<'_>) -> Result<()> {
        self.input_rows.clear();
        let InputRenderState {
            buffer,
            cursor_byte,
            mode,
            hint,
            suggestions,
            selected_suggestion,
            file_suggestions,
            selected_file_suggestion,
            argument_suggestions,
            selected_argument_suggestion,
            argument_hint,
            todos,
            status_line,
            theme,
            vim_mode,
            vim_selection,
        } = state;
        let (width, height) = terminal::size()
            .map(|(width, height)| (usize::from(width).max(4), usize::from(height).max(4)))
            .unwrap_or((80, 24));
        let rule = "─".repeat(width.saturating_sub(1));
        let lines = buffer.split('\n').collect::<Vec<_>>();
        let mut line_offsets = Vec::with_capacity(lines.len());
        let mut offset = 0usize;
        for line in &lines {
            line_offsets.push(offset);
            offset = offset.saturating_add(line.len()).saturating_add(1);
        }
        let active_line = buffer[..cursor_byte]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
        let active_start = line_start(buffer, cursor_byte);
        let active_column = buffer[active_start..cursor_byte].graphemes(true).count();
        let mut rendered_cursor_column = active_column;
        let color = std::env::var_os("NO_COLOR").is_none() && theme != ThemePreset::NoColor;
        let accent = match theme {
            ThemePreset::Light => Color::Blue,
            ThemePreset::Daltonized => Color::Magenta,
            ThemePreset::Auto | ThemePreset::Dark | ThemePreset::NoColor => Color::Cyan,
        };
        let muted = match theme {
            ThemePreset::Dark => Color::Grey,
            _ => Color::DarkGrey,
        };
        let shell_mode = buffer.starts_with('!');
        let suggestion_limit = if !file_suggestions.is_empty() {
            file_suggestions.len().min(6)
        } else if !argument_suggestions.is_empty() {
            argument_suggestions.len().min(6)
        } else if !suggestions.is_empty() {
            suggestions.len().min(6)
        } else if let Some(todos) = todos {
            todos.len().clamp(1, 6)
        } else {
            0
        };
        let visible_limit =
            MAX_VISIBLE_INPUT_LINES.min(height.saturating_sub(4 + suggestion_limit).max(1));
        let visible_start = if lines.len() <= visible_limit {
            0
        } else {
            active_line
                .saturating_sub(visible_limit / 2)
                .min(lines.len().saturating_sub(visible_limit))
        };
        let visible_end = lines.len().min(visible_start.saturating_add(visible_limit));

        if color {
            queue!(
                out,
                SetForegroundColor(if shell_mode { Color::Yellow } else { muted })
            )?;
        }
        queue!(out, Print(&rule), Print(RAW_LINE_END))?;
        if color {
            queue!(out, ResetColor)?;
        }
        for (index, line) in lines
            .iter()
            .enumerate()
            .skip(visible_start)
            .take(visible_end.saturating_sub(visible_start))
        {
            let prefix = if index == 0 {
                "› "
            } else if index == visible_start && visible_start > 0 {
                "⋮ "
            } else {
                "  "
            };
            if color && index == 0 {
                queue!(
                    out,
                    SetForegroundColor(if shell_mode { Color::Yellow } else { accent }),
                    SetAttribute(Attribute::Bold)
                )?;
            }
            queue!(out, Print(prefix))?;
            if color && index == 0 {
                queue!(out, ResetColor, SetAttribute(Attribute::Reset))?;
            }
            let available = width.saturating_sub(3);
            let (visible, local_start) = if index == active_line {
                let (visible, column, local_start) =
                    visible_around_cursor_window(line, active_column, available);
                rendered_cursor_column = column;
                (visible, local_start)
            } else {
                (visible_line(line, available), 0)
            };
            let visible_bytes = if index == active_line {
                visible.len()
            } else {
                visible_prefix_bytes(line, available)
            };
            self.input_rows.push(ComposerInputRow {
                local_row: u16::try_from(index.saturating_sub(visible_start).saturating_add(1))
                    .unwrap_or(u16::MAX),
                byte_start: line_offsets[index].saturating_add(local_start),
                byte_end: line_offsets[index]
                    .saturating_add(local_start)
                    .saturating_add(visible_bytes),
            });
            queue_text_with_selection(
                out,
                &visible,
                line_offsets[index].saturating_add(local_start),
                vim_selection,
            )?;
            queue!(out, Print(RAW_LINE_END))?;
        }
        if color {
            queue!(out, SetForegroundColor(muted))?;
        }
        queue!(out, Print(&rule), Print(RAW_LINE_END))?;
        let footer = if lines.len() > visible_limit {
            if hint.is_empty() {
                format!(
                    "  {} · line {}/{} · Shift+Tab mode · Ctrl+J newline",
                    mode_label(mode),
                    active_line + 1,
                    lines.len()
                )
            } else {
                format!("  {hint} · line {}/{}", active_line + 1, lines.len())
            }
        } else if !hint.is_empty() {
            if let Some(status_line) = status_line {
                format!("  {hint} · {}", plain_status_line(status_line))
            } else {
                format!("  {hint} · Shift+Tab mode · Shift+Enter/Ctrl+J newline")
            }
        } else if let Some(status_line) = status_line {
            format!("  {}", plain_status_line(status_line))
        } else if shell_mode {
            "  shell · permission checked · Enter run · Esc cancel · Tab history".to_owned()
        } else if let Some(argument_hint) = argument_hint {
            format!("  {argument_hint}")
        } else {
            format!(
                "  {} · Shift+Tab mode · Shift+Enter/Ctrl+J newline · / commands",
                mode_label(mode)
            )
        };
        let rendered_suggestions = if !file_suggestions.is_empty() {
            let count = suggestion_limit.min(height.saturating_sub(3).max(1));
            let start = selected_file_suggestion
                .saturating_sub(count / 2)
                .min(file_suggestions.len().saturating_sub(count));
            let end = (start + count).min(file_suggestions.len());
            for (index, file) in file_suggestions.iter().enumerate().take(end).skip(start) {
                let selected = index == selected_file_suggestion;
                if color && selected {
                    queue!(out, SetForegroundColor(accent))?;
                }
                let path = if file.is_dir {
                    format!("@{}/", file.display_path.trim_end_matches('/'))
                } else {
                    format!("@{}", file.display_path)
                };
                let kind = if file.is_dir { "directory" } else { "file" };
                let line = visible_line(
                    &format!("{}{}  {kind}", if selected { "› " } else { "  " }, path),
                    width.saturating_sub(1),
                );
                queue!(out, Print(line), Print(RAW_LINE_END))?;
                if color && selected {
                    queue!(out, ResetColor)?;
                }
            }
            end.saturating_sub(start)
        } else if !argument_suggestions.is_empty() {
            let count = suggestion_limit.min(height.saturating_sub(3).max(1));
            let start = selected_argument_suggestion
                .saturating_sub(count / 2)
                .min(argument_suggestions.len().saturating_sub(count));
            let end = (start + count).min(argument_suggestions.len());
            for (index, argument) in argument_suggestions
                .iter()
                .enumerate()
                .take(end)
                .skip(start)
            {
                let selected = index == selected_argument_suggestion;
                if color && selected {
                    queue!(out, SetForegroundColor(accent))?;
                }
                queue!(
                    out,
                    Print(if selected { "› " } else { "  " }),
                    Print(visible_line(argument, width.saturating_sub(3))),
                    Print(RAW_LINE_END)
                )?;
                if color && selected {
                    queue!(out, ResetColor)?;
                }
            }
            end.saturating_sub(start)
        } else if suggestions.is_empty() {
            if let Some(todos) = todos {
                if todos.is_empty() {
                    queue!(out, Print("  No todo items"), Print(RAW_LINE_END))?;
                    1usize
                } else {
                    for todo in todos.iter().take(suggestion_limit) {
                        queue!(
                            out,
                            Print(visible_line(todo, width.saturating_sub(1))),
                            Print(RAW_LINE_END)
                        )?;
                    }
                    todos.len().min(suggestion_limit)
                }
            } else {
                queue!(
                    out,
                    Print(visible_line(&footer, width.saturating_sub(1))),
                    Print(RAW_LINE_END)
                )?;
                0usize
            }
        } else {
            let count = suggestion_limit.min(height.saturating_sub(3).max(1));
            let start = selected_suggestion
                .saturating_sub(count / 2)
                .min(suggestions.len().saturating_sub(count));
            let end = (start + count).min(suggestions.len());
            let name_width = suggestions
                .iter()
                .map(|command| command.name.len() + 1)
                .max()
                .unwrap_or_default()
                .saturating_add(5)
                .min(width.saturating_mul(2) / 5);
            for (index, command) in suggestions.iter().enumerate().take(end).skip(start) {
                let selected = index == selected_suggestion;
                if color && selected {
                    queue!(out, SetForegroundColor(accent))?;
                }
                let name =
                    visible_line(&format!("/{}", command.name), name_width.saturating_sub(2));
                let padded = format!(
                    "{name}{}",
                    " ".repeat(name_width.saturating_sub(UnicodeWidthStr::width(name.as_str())))
                );
                let description_width = width.saturating_sub(name_width + 3);
                let description = visible_line(&command.description, description_width);
                queue!(
                    out,
                    Print(if selected { "› " } else { "  " }),
                    Print(padded),
                    Print(description),
                    Print(RAW_LINE_END)
                )?;
                if color && selected {
                    queue!(out, ResetColor)?;
                }
            }
            end.saturating_sub(start)
        };
        if color {
            queue!(out, ResetColor)?;
        }

        self.rows = u16::try_from(
            visible_end
                .saturating_sub(visible_start)
                .saturating_add(2)
                .saturating_add(rendered_suggestions.max(1)),
        )
        .unwrap_or(u16::MAX);
        self.cursor_row =
            u16::try_from(active_line.saturating_sub(visible_start).saturating_add(1))
                .unwrap_or(u16::MAX);
        let move_up = self.rows.saturating_sub(self.cursor_row);
        if let Some(vim_mode) = vim_mode {
            let cursor_style = if vim_mode == VimMode::Insert {
                cursor::SetCursorStyle::SteadyBar
            } else {
                cursor::SetCursorStyle::SteadyBlock
            };
            queue!(out, cursor_style)?;
        }
        queue!(
            out,
            cursor::MoveUp(move_up),
            cursor::MoveToColumn(
                u16::try_from(rendered_cursor_column.saturating_add(2)).unwrap_or(u16::MAX)
            )
        )?;
        Ok(())
    }
}

#[derive(Default)]
struct RenderedPicker {
    rows: u16,
    cursor_row: u16,
}

impl RenderedPicker {
    fn redraw(
        &mut self,
        out: &mut impl Write,
        options: &[ModelOption],
        current: &str,
        state: &ModelPickerState,
        exit_hint: Option<&str>,
        text: PickerText<'_>,
    ) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        self.draw(&mut frame, options, current, state, exit_hint, text)?;
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;
        Ok(())
    }

    fn erase(&mut self, out: &mut impl Write) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;
        Ok(())
    }

    fn reset_viewport(&mut self, out: &mut impl Write) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        queue!(
            frame,
            cursor::MoveToColumn(0),
            Clear(ClearType::FromCursorDown)
        )?;
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;
        Ok(())
    }

    fn clear(&mut self, out: &mut impl Write) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let below = self.rows.saturating_sub(self.cursor_row + 1);
        if below > 0 {
            queue!(out, cursor::MoveDown(below))?;
        }
        queue!(
            out,
            cursor::MoveDown(1),
            cursor::MoveUp(self.rows),
            cursor::MoveToColumn(0),
            Clear(ClearType::FromCursorDown)
        )?;
        *self = Self::default();
        Ok(())
    }

    fn draw(
        &mut self,
        out: &mut impl Write,
        options: &[ModelOption],
        current: &str,
        state: &ModelPickerState,
        exit_hint: Option<&str>,
        text: PickerText<'_>,
    ) -> Result<()> {
        let width = terminal::size()
            .map(|(width, _)| usize::from(width).max(20))
            .unwrap_or(80);
        let color = std::env::var_os("NO_COLOR").is_none();
        let accent = if text.preview_theme {
            picker_theme_accent(&options[state.focused].value)
        } else {
            Some(Color::Cyan)
        };
        if color {
            if let Some(accent) = accent {
                queue!(out, SetForegroundColor(accent))?;
            }
            queue!(out, SetAttribute(Attribute::Bold))?;
        }
        queue!(out, Print("  "), Print(text.title))?;
        if color {
            queue!(out, ResetColor, SetAttribute(Attribute::Reset))?;
        }
        queue!(
            out,
            Print(RAW_LINE_END),
            Print(visible_line(
                &format!("  {}", text.help),
                width.saturating_sub(1)
            )),
            Print(RAW_LINE_END),
            Print(RAW_LINE_END)
        )?;

        let visible_to = (state.visible_from + state.visible_count).min(options.len());
        let index_width = options.len().to_string().len();
        for (index, option) in options
            .iter()
            .enumerate()
            .take(visible_to)
            .skip(state.visible_from)
        {
            let focused = index == state.focused;
            let scroll_marker = if focused {
                "›"
            } else if index == state.visible_from && state.visible_from > 0 {
                "↑"
            } else if index + 1 == visible_to && visible_to < options.len() {
                "↓"
            } else {
                " "
            };
            if color && focused {
                if let Some(accent) = accent {
                    queue!(out, SetForegroundColor(accent))?;
                }
            }
            let selected = if option.value == current { " ✓" } else { "" };
            let prefix = format!(
                "  {scroll_marker} {:>index_width$}. {}{selected}",
                index + 1,
                option.display_name
            );
            let description = if option.description.is_empty() {
                String::new()
            } else {
                format!("  {}", option.description)
            };
            let line = if description.is_empty() {
                prefix
            } else {
                format!("{prefix}{description}")
            };
            queue!(
                out,
                Print(visible_line(&line, width.saturating_sub(1))),
                Print(RAW_LINE_END)
            )?;
            if color && focused {
                queue!(out, ResetColor)?;
            }
        }

        let hidden = options.len().saturating_sub(state.visible_count);
        if hidden > 0 {
            if color {
                queue!(out, SetForegroundColor(Color::DarkGrey))?;
            }
            queue!(
                out,
                Print(format!("    and {hidden} more…")),
                Print(RAW_LINE_END)
            )?;
            if color {
                queue!(out, ResetColor)?;
            }
        }
        let preview_rows = if text.preview_theme {
            queue!(
                out,
                Print(RAW_LINE_END),
                Print("  Preview · demo.rs"),
                Print(RAW_LINE_END)
            )?;
            if color && options[state.focused].value != "no-color" {
                queue!(out, SetForegroundColor(Color::Red))?;
            }
            queue!(
                out,
                Print("  - let message = \"before\";"),
                Print(RAW_LINE_END)
            )?;
            if color && options[state.focused].value != "no-color" {
                queue!(out, SetForegroundColor(Color::Green))?;
            }
            queue!(
                out,
                Print("  + let message = \"after\";"),
                Print(RAW_LINE_END)
            )?;
            if color {
                queue!(out, ResetColor)?;
            }
            4usize
        } else {
            0usize
        };
        queue!(out, Print(RAW_LINE_END))?;
        if color {
            queue!(out, SetForegroundColor(Color::DarkGrey))?;
        }
        queue!(
            out,
            Print(visible_line(
                &format!("  {}", exit_hint.unwrap_or("Enter confirm · Esc exit")),
                width.saturating_sub(1),
            )),
            Print(RAW_LINE_END)
        )?;
        if color {
            queue!(out, ResetColor)?;
        }

        let hidden_row = usize::from(hidden > 0);
        self.rows =
            u16::try_from(5 + state.visible_count + hidden_row + preview_rows).unwrap_or(u16::MAX);
        self.cursor_row =
            u16::try_from(3 + state.focused.saturating_sub(state.visible_from)).unwrap_or(u16::MAX);
        queue!(
            out,
            cursor::MoveUp(self.rows.saturating_sub(self.cursor_row)),
            cursor::MoveToColumn(2)
        )?;
        Ok(())
    }
}

fn picker_theme_accent(theme: &str) -> Option<Color> {
    match theme {
        "light" => Some(Color::Blue),
        "daltonized" => Some(Color::Magenta),
        "no-color" => None,
        _ => Some(Color::Cyan),
    }
}

struct RawModeGuard {
    bracketed_paste: bool,
    keyboard_enhancement: bool,
}

#[cfg(unix)]
struct SuspendSignalGuard {
    requested: Arc<AtomicBool>,
    registration: signal_hook::SigId,
}

#[cfg(unix)]
impl SuspendSignalGuard {
    fn register() -> Result<Self> {
        let requested = Arc::new(AtomicBool::new(false));
        let registration = signal_hook::flag::register(
            signal_hook::consts::signal::SIGTSTP,
            Arc::clone(&requested),
        )?;
        Ok(Self {
            requested,
            registration,
        })
    }

    fn take(&self) -> bool {
        self.requested.swap(false, Ordering::AcqRel)
    }
}

#[cfg(unix)]
impl Drop for SuspendSignalGuard {
    fn drop(&mut self) {
        signal_hook::low_level::unregister(self.registration);
    }
}

struct AlternateScreenGuard;

impl AlternateScreenGuard {
    fn enter() -> Result<Self> {
        execute!(
            io::stdout(),
            EnterAlternateScreen,
            EnableMouseCapture,
            cursor::Hide,
            cursor::MoveTo(0, 0),
            Clear(ClearType::All)
        )?;
        Ok(Self)
    }
}

impl Drop for AlternateScreenGuard {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            DisableMouseCapture,
            cursor::Show,
            LeaveAlternateScreen
        );
    }
}

impl RawModeGuard {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let mut out = io::stdout();
        let bracketed_paste = execute!(out, EnableBracketedPaste).is_ok();
        let keyboard_enhancement = execute!(
            out,
            PushKeyboardEnhancementFlags(
                KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                    | KeyboardEnhancementFlags::REPORT_EVENT_TYPES
                    | KeyboardEnhancementFlags::REPORT_ALTERNATE_KEYS
            )
        )
        .is_ok();
        Ok(Self {
            bracketed_paste,
            keyboard_enhancement,
        })
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let mut out = io::stdout();
        if self.keyboard_enhancement {
            let _ = execute!(out, PopKeyboardEnhancementFlags);
        }
        if self.bracketed_paste {
            let _ = execute!(out, DisableBracketedPaste);
        }
        let _ = execute!(out, cursor::SetCursorStyle::DefaultUserShape);
        let _ = terminal::disable_raw_mode();
    }
}

fn write_field(out: &mut impl Write, label: &str, value: &str, width: usize) -> io::Result<()> {
    let available = width.saturating_sub(14);
    let value = visible_line(value, available);
    write_box_line(out, &format!("  {label:<8} {value}"), width)
}

fn write_box_line(out: &mut impl Write, content: &str, width: usize) -> io::Result<()> {
    let inner = width.saturating_sub(2);
    let content = visible_line(content, inner);
    let padding = inner.saturating_sub(UnicodeWidthStr::width(content.as_str()));
    queue!(
        out,
        Print("│"),
        Print(content),
        Print(" ".repeat(padding)),
        Print("│\n")
    )
}

fn print_committed_prompt(out: &mut impl Write, text: &str) -> Result<()> {
    if std::env::var_os("NO_COLOR").is_none() {
        queue!(out, SetForegroundColor(Color::DarkGrey))?;
    }
    queue!(out, Print("› "))?;
    if std::env::var_os("NO_COLOR").is_none() {
        queue!(out, ResetColor)?;
    }
    let mut lines = text.lines();
    if let Some(first) = lines.next() {
        queue!(out, Print(sanitize_inline(first)), Print(RAW_LINE_END))?;
    }
    for line in lines {
        queue!(
            out,
            Print("  "),
            Print(sanitize_inline(line)),
            Print(RAW_LINE_END)
        )?;
    }
    queue!(out, Print(RAW_LINE_END))?;
    out.flush()?;
    Ok(())
}

fn clear_status(out: &mut impl Write, state: &mut OutputState) {
    if state.status_open {
        let _ = queue!(out, Print("\r"), Clear(ClearType::CurrentLine));
        state.status_open = false;
    }
}

fn close_assistant(out: &mut impl Write, state: &mut OutputState) {
    if state.assistant_open {
        let _ = queue!(out, Print("\n"));
        state.assistant_open = false;
    }
}

fn styled_status(out: &mut impl Write, color: bool, label: &str) -> io::Result<()> {
    if color {
        queue!(out, SetForegroundColor(Color::DarkGrey))?;
    }
    queue!(out, Print(format!("  ◐ {label}")))?;
    if color {
        queue!(out, ResetColor)?;
    }
    out.flush()
}

fn muted_line(out: &mut impl Write, color: bool, line: &str) -> io::Result<()> {
    if color {
        queue!(out, SetForegroundColor(Color::DarkGrey))?;
    }
    queue!(out, Print(line), Print("\n"))?;
    if color {
        queue!(out, ResetColor)?;
    }
    out.flush()
}

fn format_duration(elapsed_ms: u128) -> String {
    if elapsed_ms < 1_000 {
        format!("{elapsed_ms}ms")
    } else {
        format!("{:.1}s", elapsed_ms as f64 / 1_000.0)
    }
}

fn plain_status_line(value: &str) -> String {
    let mut plain = String::with_capacity(value.len());
    let mut characters = value.chars().peekable();
    while let Some(character) = characters.next() {
        if character == '\u{1b}' && characters.peek() == Some(&'[') {
            characters.next();
            for parameter in characters.by_ref() {
                if parameter == 'm' {
                    break;
                }
            }
        } else if character.is_control() {
            plain.push(' ');
        } else {
            plain.push(character);
        }
    }
    plain.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn single_line(value: &str, limit: usize) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    visible_line(&sanitize_inline(&collapsed), limit)
}

fn visible_line(value: &str, limit: usize) -> String {
    let value = sanitize_inline(value);
    if UnicodeWidthStr::width(value.as_str()) <= limit {
        return value;
    }
    if limit == 0 {
        return String::new();
    }
    let mut output = String::new();
    let mut width = 0usize;
    for grapheme in value.graphemes(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if width.saturating_add(grapheme_width).saturating_add(1) > limit {
            break;
        }
        output.push_str(grapheme);
        width = width.saturating_add(grapheme_width);
    }
    output.push('…');
    output
}

fn visible_prefix_bytes(value: &str, limit: usize) -> usize {
    if UnicodeWidthStr::width(value) <= limit {
        return value.len();
    }
    if limit == 0 {
        return 0;
    }
    let mut bytes = 0usize;
    let mut width = 0usize;
    for grapheme in value.graphemes(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if width.saturating_add(grapheme_width).saturating_add(1) > limit {
            break;
        }
        bytes = bytes.saturating_add(grapheme.len());
        width = width.saturating_add(grapheme_width);
    }
    bytes
}

#[cfg(test)]
fn visible_around_cursor(value: &str, cursor: usize, limit: usize) -> (String, usize) {
    let (visible, column, _) = visible_around_cursor_window(value, cursor, limit);
    (visible, column)
}

fn visible_around_cursor_window(
    value: &str,
    cursor: usize,
    limit: usize,
) -> (String, usize, usize) {
    if limit == 0 {
        return (String::new(), 0, 0);
    }
    let graphemes = value.graphemes(true).collect::<Vec<_>>();
    let cursor = cursor.min(graphemes.len());
    let right_reserve = graphemes
        .get(cursor)
        .map(|grapheme| UnicodeWidthStr::width(*grapheme))
        .unwrap_or_default()
        .min(limit);
    let left_limit = limit.saturating_sub(right_reserve);
    let mut start = cursor;
    let mut cursor_width = 0usize;
    while start > 0 {
        let width = UnicodeWidthStr::width(graphemes[start - 1]);
        if cursor_width.saturating_add(width) > left_limit {
            break;
        }
        start -= 1;
        cursor_width = cursor_width.saturating_add(width);
    }
    let mut end = start;
    let mut rendered_width = 0usize;
    while end < graphemes.len() {
        let width = UnicodeWidthStr::width(graphemes[end]);
        if rendered_width.saturating_add(width) > limit {
            break;
        }
        rendered_width = rendered_width.saturating_add(width);
        end += 1;
    }
    let byte_start = graphemes[..start]
        .iter()
        .map(|grapheme| grapheme.len())
        .sum();
    (graphemes[start..end].concat(), cursor_width, byte_start)
}

fn queue_text_with_selection(
    out: &mut impl Write,
    visible: &str,
    global_start: usize,
    selection: Option<(usize, usize, bool)>,
) -> io::Result<()> {
    let Some((selection_start, selection_end, _)) = selection else {
        return queue!(out, Print(visible));
    };
    let global_end = global_start.saturating_add(visible.len());
    let selected_start = selection_start.max(global_start).min(global_end);
    let selected_end = selection_end.max(global_start).min(global_end);
    if selected_start >= selected_end {
        return queue!(out, Print(visible));
    }
    let mut local_start = selected_start.saturating_sub(global_start);
    let mut local_end = selected_end.saturating_sub(global_start);
    while local_start > 0 && !visible.is_char_boundary(local_start) {
        local_start -= 1;
    }
    while local_end < visible.len() && !visible.is_char_boundary(local_end) {
        local_end += 1;
    }
    queue!(
        out,
        Print(&visible[..local_start]),
        SetAttribute(Attribute::Reverse),
        Print(&visible[local_start..local_end]),
        SetAttribute(Attribute::NoReverse),
        Print(&visible[local_end..])
    )
}

fn sanitize_inline(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character == '\t' {
                ' '
            } else if character.is_control() {
                '�'
            } else {
                character
            }
        })
        .collect()
}

fn sanitize_multiline(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\n' => '\n',
            '\t' => ' ',
            _ if character.is_control() => '�',
            _ => character,
        })
        .collect()
}

fn sanitize_paste(value: &str) -> String {
    sanitize_multiline(&value.replace("\r\n", "\n").replace('\r', "\n"))
}

fn synchronized_output_supported() -> bool {
    if std::env::var_os("TMUX").is_some() {
        return false;
    }
    let term_program = std::env::var("TERM_PROGRAM").unwrap_or_default();
    if matches!(
        term_program.as_str(),
        "iTerm.app" | "WezTerm" | "WarpTerminal" | "ghostty" | "contour" | "vscode" | "alacritty"
    ) {
        return true;
    }
    let term = std::env::var("TERM").unwrap_or_default();
    if term.contains("kitty")
        || term == "xterm-ghostty"
        || term.starts_with("foot")
        || term.contains("alacritty")
        || std::env::var_os("KITTY_WINDOW_ID").is_some()
        || std::env::var_os("ZED_TERM").is_some()
        || std::env::var_os("WT_SESSION").is_some()
    {
        return true;
    }
    std::env::var("VTE_VERSION")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .is_some_and(|version| version >= 6800)
}

fn previous_boundary(value: &str, index: usize) -> usize {
    value[..index]
        .grapheme_indices(true)
        .next_back()
        .map_or(0, |(boundary, _)| boundary)
}

fn next_boundary(value: &str, index: usize) -> usize {
    value[index..]
        .grapheme_indices(true)
        .nth(1)
        .map_or(value.len(), |(offset, _)| index + offset)
}

fn previous_word_boundary(value: &str, index: usize) -> usize {
    let mut cursor = index;
    while cursor > 0 {
        let previous = previous_boundary(value, cursor);
        if !value[previous..cursor].chars().all(char::is_whitespace) {
            break;
        }
        cursor = previous;
    }
    while cursor > 0 {
        let previous = previous_boundary(value, cursor);
        if value[previous..cursor].chars().all(char::is_whitespace) {
            break;
        }
        cursor = previous;
    }
    cursor
}

fn next_word_boundary(value: &str, index: usize) -> usize {
    let mut cursor = index;
    while cursor < value.len() {
        let next = next_boundary(value, cursor);
        if !value[cursor..next].chars().all(char::is_whitespace) {
            break;
        }
        cursor = next;
    }
    while cursor < value.len() {
        let next = next_boundary(value, cursor);
        if value[cursor..next].chars().all(char::is_whitespace) {
            break;
        }
        cursor = next;
    }
    cursor
}

fn move_vertical(value: &str, index: usize, direction: i8) -> usize {
    let current_start = line_start(value, index);
    let target = if direction < 0 {
        if current_start == 0 {
            return index;
        }
        let target_end = current_start - 1;
        (line_start(value, target_end), target_end)
    } else {
        let current_end = line_end(value, index);
        if current_end == value.len() {
            return index;
        }
        let target_start = current_end + 1;
        (target_start, line_end(value, target_start))
    };
    let desired_width = UnicodeWidthStr::width(&value[current_start..index]);
    let mut target_index = target.0;
    let mut width = 0usize;
    for (offset, grapheme) in value[target.0..target.1].grapheme_indices(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if width.saturating_add(grapheme_width) > desired_width {
            break;
        }
        width = width.saturating_add(grapheme_width);
        target_index = target.0 + offset + grapheme.len();
    }
    target_index
}

fn line_start(value: &str, index: usize) -> usize {
    value[..index]
        .rfind('\n')
        .map_or(0, |position| position + 1)
}

fn line_end(value: &str, index: usize) -> usize {
    value[index..]
        .find('\n')
        .map_or(value.len(), |offset| index + offset)
}

fn next_mode(mode: PermissionMode) -> PermissionMode {
    match mode {
        PermissionMode::Default => PermissionMode::AcceptEdits,
        PermissionMode::AcceptEdits => PermissionMode::Plan,
        PermissionMode::Plan | PermissionMode::BypassPermissions | PermissionMode::DontAsk => {
            PermissionMode::Default
        }
    }
}

fn mode_label(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::Default => "default",
        PermissionMode::AcceptEdits => "accept edits",
        PermissionMode::Plan => "plan",
        PermissionMode::BypassPermissions => "bypass permissions",
        PermissionMode::DontAsk => "don't ask",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_preview_explains_common_mutating_actions() {
        let edit = permission_action_preview(
            "Edit",
            &serde_json::json!({
                "file_path":"src/main.rs",
                "old_string":"old\nvalue",
                "new_string":"new\nvalue"
            }),
        );
        assert_eq!(
            edit,
            vec!["  Edit src/main.rs", "    - old value", "    + new value"]
        );
        let bash = permission_action_preview(
            "Bash",
            &serde_json::json!({"command":"cargo test\ncargo clippy"}),
        );
        assert_eq!(bash, vec!["  $ cargo test cargo clippy"]);
    }

    struct TestScreen {
        cells: Vec<Vec<char>>,
        row: usize,
        column: usize,
        width: usize,
    }

    impl TestScreen {
        fn new(width: usize, height: usize) -> Self {
            Self {
                cells: vec![vec![' '; width]; height],
                row: 0,
                column: 0,
                width,
            }
        }

        fn feed(&mut self, bytes: &[u8]) {
            let mut index = 0usize;
            while index < bytes.len() {
                match bytes[index] {
                    b'\r' => {
                        self.column = 0;
                        index += 1;
                    }
                    b'\n' => {
                        self.row = (self.row + 1).min(self.cells.len() - 1);
                        index += 1;
                    }
                    0x1b if bytes.get(index + 1) == Some(&b'[') => {
                        let start = index + 2;
                        let mut end = start;
                        while end < bytes.len() && !(0x40..=0x7e).contains(&bytes[end]) {
                            end += 1;
                        }
                        assert!(end < bytes.len(), "unterminated CSI sequence");
                        let command = bytes[end];
                        let parameters = std::str::from_utf8(&bytes[start..end]).unwrap();
                        let first = parameters
                            .trim_start_matches('?')
                            .split(';')
                            .next()
                            .filter(|value| !value.is_empty())
                            .and_then(|value| value.parse::<usize>().ok())
                            .unwrap_or(1);
                        match command {
                            b'A' => self.row = self.row.saturating_sub(first),
                            b'B' => {
                                self.row = (self.row + first).min(self.cells.len() - 1);
                            }
                            b'G' => self.column = first.saturating_sub(1).min(self.width - 1),
                            b'J' => {
                                for column in self.column..self.width {
                                    self.cells[self.row][column] = ' ';
                                }
                                for row in self.row + 1..self.cells.len() {
                                    self.cells[row].fill(' ');
                                }
                            }
                            b'm' | b'h' | b'l' => {}
                            _ => panic!("unsupported CSI command: {command:?}"),
                        }
                        index = end + 1;
                    }
                    _ => {
                        let character = std::str::from_utf8(&bytes[index..])
                            .unwrap()
                            .chars()
                            .next()
                            .unwrap();
                        let character_width =
                            unicode_width::UnicodeWidthChar::width(character).unwrap_or_default();
                        if character_width > 0 {
                            if self.column.saturating_add(character_width) > self.width {
                                self.row = (self.row + 1).min(self.cells.len() - 1);
                                self.column = 0;
                            }
                            self.cells[self.row][self.column] = character;
                            for offset in 1..character_width {
                                self.cells[self.row][self.column + offset] = ' ';
                            }
                            self.column += character_width;
                            if self.column == self.width {
                                self.row = (self.row + 1).min(self.cells.len() - 1);
                                self.column = 0;
                            }
                        }
                        index += character.len_utf8();
                    }
                }
            }
        }

        fn lines(&self) -> Vec<String> {
            self.cells
                .iter()
                .map(|line| line.iter().collect::<String>().trim_end().to_owned())
                .filter(|line| !line.is_empty())
                .collect()
        }
    }

    #[test]
    fn navigation_uses_grapheme_boundaries() {
        let value = "a界b";
        assert_eq!(next_boundary(value, 0), 1);
        assert_eq!(next_boundary(value, 1), 4);
        assert_eq!(previous_boundary(value, 4), 1);

        let combined = "e\u{301}x";
        assert_eq!(next_boundary(combined, 0), "e\u{301}".len());
        assert_eq!(previous_boundary(combined, "e\u{301}".len()), 0);

        let family = "👨‍👩‍👧‍👦!";
        assert_eq!(next_boundary(family, 0), "👨‍👩‍👧‍👦".len());
        assert_eq!(previous_boundary(family, "👨‍👩‍👧‍👦".len()), 0);

        let words = "one  two";
        assert_eq!(previous_word_boundary(words, words.len()), 5);
        assert_eq!(previous_word_boundary(words, 5), 0);
        assert_eq!(next_word_boundary(words, 0), 3);
        assert_eq!(next_word_boundary(words, 3), words.len());

        let multiline = "ab\n你界x\nz";
        assert_eq!(move_vertical(multiline, 2, 1), "ab\n你".len());
        assert_eq!(
            move_vertical(multiline, "ab\n你界".len(), 1),
            multiline.len()
        );
        assert_eq!(move_vertical(multiline, multiline.len(), -1), "ab\n".len());
    }

    #[test]
    fn mode_cycle_never_enters_bypass() {
        assert_eq!(
            next_mode(PermissionMode::Default),
            PermissionMode::AcceptEdits
        );
        assert_eq!(next_mode(PermissionMode::AcceptEdits), PermissionMode::Plan);
        assert_eq!(next_mode(PermissionMode::Plan), PermissionMode::Default);
        assert_eq!(next_mode(PermissionMode::DontAsk), PermissionMode::Default);
        assert_eq!(mode_label(PermissionMode::DontAsk), "don't ask");
    }

    #[test]
    fn exit_confirmation_is_key_specific_and_bounded() {
        let started = Instant::now();
        for key in [ExitKey::CtrlC, ExitKey::CtrlD] {
            let mut pending = None;
            assert!(!arm_or_confirm_exit(&mut pending, key, started));
            assert!(arm_or_confirm_exit(
                &mut pending,
                key,
                started + EXIT_WINDOW - Duration::from_millis(1)
            ));

            pending = Some(ExitPending::new(key, started));
            assert!(!arm_or_confirm_exit(
                &mut pending,
                key,
                started + EXIT_WINDOW
            ));
            assert_eq!(
                pending.expect("re-armed after timeout").armed_at,
                started + EXIT_WINDOW
            );
        }

        let mut pending = Some(ExitPending::new(ExitKey::CtrlC, started));
        assert!(!arm_or_confirm_exit(
            &mut pending,
            ExitKey::CtrlD,
            started + Duration::from_millis(1)
        ));
        assert_eq!(pending.expect("re-armed").key, ExitKey::CtrlD);
    }

    #[test]
    fn long_status_is_bounded() {
        assert_eq!(visible_line("abcdefgh", 5), "abcd…");
        assert_eq!(visible_line("你好世界", 5), "你好…");
        assert_eq!(single_line("a\n b\t c", 20), "a b c");
        assert_eq!(visible_around_cursor("abcdefgh", 7, 5), ("defgh".into(), 4));
        assert_eq!(sanitize_inline("safe\u{1b}[2Jtext"), "safe�[2Jtext");
        assert_eq!(sanitize_multiline("a\nb\u{7}"), "a\nb�");
        assert_eq!(sanitize_paste("a\r\nb\rc\u{7}"), "a\nb\nc�");
        assert_eq!(visible_around_cursor("a界b", 2, 4), ("a界b".into(), 3));
    }

    #[test]
    fn transcript_search_is_case_insensitive_and_tracks_each_occurrence() {
        let lines = ["Alpha beta ALPHA".to_owned(), "unrelated".to_owned()];
        let refs = lines.iter().collect::<Vec<_>>();
        assert_eq!(
            transcript_matches(&refs, "alpha"),
            [
                TranscriptMatch {
                    line: 0,
                    start: 0,
                    end: 5,
                },
                TranscriptMatch {
                    line: 0,
                    start: 11,
                    end: 16,
                },
            ]
        );
    }

    fn test_commands() -> Vec<SlashCommandSuggestion> {
        vec![
            SlashCommandSuggestion {
                name: "clear".into(),
                aliases: Vec::new(),
                description: "Clear conversation".into(),
                argument_hint: None,
                execute_on_enter: true,
                argument_candidates: Vec::new(),
            },
            SlashCommandSuggestion {
                name: "exit".into(),
                aliases: vec!["quit".into()],
                description: "Exit session".into(),
                argument_hint: None,
                execute_on_enter: true,
                argument_candidates: Vec::new(),
            },
            SlashCommandSuggestion {
                name: "model".into(),
                aliases: Vec::new(),
                description: "Set model".into(),
                argument_hint: Some("[model]".into()),
                execute_on_enter: true,
                argument_candidates: vec!["opus".into(), "sonnet".into()],
            },
        ]
    }

    #[test]
    fn slash_command_suggestions_filter_alias_and_arguments() {
        let commands = test_commands();
        assert_eq!(
            command_matches("/", &commands)
                .iter()
                .map(|command| command.name.as_str())
                .collect::<Vec<_>>(),
            vec!["clear", "exit", "model"]
        );
        assert_eq!(command_matches("/mo", &commands)[0].name, "model");
        assert_eq!(command_matches("/modle", &commands)[0].name, "model");
        assert_eq!(command_matches("/quit", &commands)[0].name, "exit");
        assert!(command_matches("/model ", &commands).is_empty());
        assert!(command_matches("/model custom", &commands).is_empty());
        assert_eq!(command_argument_hint("/model ", &commands), Some("[model]"));
        let (token, candidates) = argument_matches("/model so", "/model so".len(), &commands)
            .expect("model arguments complete");
        assert_eq!(token.query, "so");
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.as_str())
                .collect::<Vec<_>>(),
            ["sonnet"]
        );
    }

    #[test]
    fn file_tokens_respect_boundaries_quotes_and_unicode() {
        assert!(file_token_at_cursor("x@src", "x@src".len()).is_none());
        assert_eq!(
            file_token_at_cursor("see (@src/模", "see (@src/模".len()),
            Some(FileToken {
                start: "see (".len(),
                end: "see (@src/模".len(),
                query: "src/模".to_owned(),
                quoted: false,
            })
        );
        assert_eq!(
            file_token_at_cursor("open @\"目录/含 空", "open @\"目录/含 空".len()),
            Some(FileToken {
                start: "open ".len(),
                end: "open @\"目录/含 空".len(),
                query: "目录/含 空".to_owned(),
                quoted: true,
            })
        );
        assert_eq!(
            file_token_at_cursor("open @\"a\\\"b", "open @\"a\\\"b".len())
                .expect("escaped quote remains open")
                .query,
            "a\"b"
        );
        assert!(file_token_at_cursor("open @\"closed\"", "open @\"closed\"".len()).is_none());
        let before_closing_quote = "open @\"closed".len();
        assert_eq!(
            file_token_at_cursor("open @\"closed\"", before_closing_quote)
                .expect("cursor before closing quote")
                .query,
            "closed"
        );
    }

    #[test]
    fn file_matching_completion_and_references_are_bounded() {
        let files = (0..4_200)
            .map(|index| FileSuggestion {
                display_path: format!("src/file-{index:04}.rs"),
                is_dir: false,
            })
            .collect::<Vec<_>>();
        let token = FileToken {
            start: 0,
            end: "@src/".len(),
            query: "src/".to_owned(),
            quoted: false,
        };
        let matches = file_matches(&token, &files);
        assert_eq!(matches.len(), MAX_FILE_SUGGESTIONS);
        assert_eq!(matches.last().unwrap().display_path, "src/file-0099.rs");

        let fuzzy_files = [
            FileSuggestion {
                display_path: "src/terminal_renderer.rs".to_owned(),
                is_dir: false,
            },
            FileSuggestion {
                display_path: "docs/other.md".to_owned(),
                is_dir: false,
            },
        ];
        let fuzzy_token = FileToken {
            start: 0,
            end: "@tmrnd".len(),
            query: "tmrnd".to_owned(),
            quoted: false,
        };
        assert_eq!(
            file_matches(&fuzzy_token, &fuzzy_files)[0].display_path,
            "src/terminal_renderer.rs"
        );

        let unicode = [
            FileSuggestion {
                display_path: "目录/模型.rs".to_owned(),
                is_dir: false,
            },
            FileSuggestion {
                display_path: "目录/模块.rs".to_owned(),
                is_dir: false,
            },
        ];
        assert_eq!(
            common_file_prefix(&unicode.iter().collect::<Vec<_>>()),
            "目录/模"
        );

        let mut plain = "inspect @src/ma later".to_owned();
        let cursor = "inspect @src/ma".len();
        let token = file_token_at_cursor(&plain, cursor).unwrap();
        let cursor = replace_file_token(&mut plain, cursor, &token, "src/main.rs", false, false);
        assert_eq!(plain, "inspect @src/main.rs later");
        assert_eq!(cursor, "inspect @src/main.rs".len());

        let mut quoted_dir = "open @\"my d".to_owned();
        let cursor = quoted_dir.len();
        let token = file_token_at_cursor(&quoted_dir, cursor).unwrap();
        let cursor = replace_file_token(&mut quoted_dir, cursor, &token, "my dir", true, false);
        assert_eq!(quoted_dir, "open @\"my dir/\"");
        assert_eq!(cursor, quoted_dir.len() - 1);

        let mut spaced_file = "open @my".to_owned();
        let cursor = spaced_file.len();
        let token = file_token_at_cursor(&spaced_file, cursor).unwrap();
        let cursor = replace_file_token(
            &mut spaced_file,
            cursor,
            &token,
            "my file.txt",
            false,
            false,
        );
        assert_eq!(spaced_file, "open @\"my file.txt\"");
        assert_eq!(cursor, spaced_file.len());

        let mut middle = "inspect @README.md later".to_owned();
        let cursor = "inspect @READ".len();
        let token = file_token_at_cursor(&middle, cursor).expect("middle-of-token reference");
        assert_eq!(token.end, "inspect @README.md".len());
        let cursor = replace_file_token(&mut middle, cursor, &token, "README.md", false, false);
        assert_eq!(middle, "inspect @README.md later");
        assert_eq!(cursor, "inspect @README.md".len());

        let mut middle_quoted = "open @\"my file.txt\" later".to_owned();
        let cursor = "open @\"my fi".len();
        let token =
            file_token_at_cursor(&middle_quoted, cursor).expect("middle-of-quoted reference");
        assert_eq!(token.end, "open @\"my file.txt\"".len());
        replace_file_token(
            &mut middle_quoted,
            cursor,
            &token,
            "my file.txt",
            false,
            false,
        );
        assert_eq!(middle_quoted, "open @\"my file.txt\" later");
    }

    #[test]
    fn history_search_is_bounded_deduplicated_and_never_matches_oversized_entries() {
        let oversized = "x".repeat(MAX_HISTORY_SEARCH_ENTRY_BYTES + 1);
        let history = vec![
            "first command".to_owned(),
            "second command".to_owned(),
            "first command".to_owned(),
            oversized,
        ];
        let mut search = HistorySearch::new(&history, HistoryScope::Project, "draft".to_owned(), 5);
        search.query = "command".to_owned();
        search.refresh(&history);
        assert_eq!(search.matches, ["first command", "second command"]);
        search.query = "missing".to_owned();
        search.refresh(&history);
        assert!(search.matches.is_empty());
        assert_eq!(search.current(), "draft");
    }

    #[test]
    fn fullscreen_composer_click_maps_rows_and_wide_graphemes_to_utf8_boundaries() {
        let buffer = "a你b\nsecond";
        let map = FullscreenComposerHitMap {
            top_row: 20,
            rows: vec![
                ComposerInputRow {
                    local_row: 1,
                    byte_start: 0,
                    byte_end: "a你b".len(),
                },
                ComposerInputRow {
                    local_row: 2,
                    byte_start: "a你b\n".len(),
                    byte_end: buffer.len(),
                },
            ],
        };
        assert_eq!(map.cursor_at(21, 0, buffer), Some(0));
        assert_eq!(map.cursor_at(21, 2, buffer), Some(0));
        assert_eq!(map.cursor_at(21, 3, buffer), Some(1));
        assert_eq!(map.cursor_at(21, 4, buffer), Some("a你".len()));
        assert_eq!(map.cursor_at(21, 40, buffer), Some("a你b".len()));
        assert_eq!(map.cursor_at(22, 2, buffer), Some("a你b\n".len()));
        assert_eq!(map.cursor_at(20, 2, buffer), None);
    }

    #[test]
    fn file_suggestions_render_as_a_bounded_typeahead() {
        let files = [
            FileSuggestion {
                display_path: "src".to_owned(),
                is_dir: true,
            },
            FileSuggestion {
                display_path: "src/main.rs".to_owned(),
                is_dir: false,
            },
        ];
        let suggestions = files.iter().collect::<Vec<_>>();
        let mut frame = Vec::new();
        let mut rendered = RenderedInput::default();
        rendered
            .draw(
                &mut frame,
                InputRenderState {
                    buffer: "@s",
                    cursor_byte: 2,
                    mode: PermissionMode::Default,
                    hint: "",
                    suggestions: &[],
                    selected_suggestion: 0,
                    file_suggestions: &suggestions,
                    selected_file_suggestion: 0,
                    argument_suggestions: &[],
                    selected_argument_suggestion: 0,
                    argument_hint: None,
                    todos: None,
                    status_line: None,
                    theme: ThemePreset::Auto,
                    vim_mode: None,
                    vim_selection: None,
                },
            )
            .unwrap();
        let rendered_text = String::from_utf8_lossy(&frame);
        assert!(rendered_text.contains("› @src/  directory"));
        assert!(rendered_text.contains("@src/main.rs  file"));
        for (index, byte) in frame.iter().enumerate() {
            if *byte == b'\n' {
                assert_eq!(frame.get(index.wrapping_sub(1)), Some(&b'\r'));
            }
        }
    }

    #[test]
    fn model_picker_focuses_current_and_wraps_like_select() {
        let options = (0..12)
            .map(|index| ModelOption {
                value: format!("model-{index}"),
                display_name: format!("Model {index}"),
                description: String::new(),
            })
            .collect::<Vec<_>>();
        let mut state = ModelPickerState::new(&options, "model-11");
        assert_eq!(state.focused, 11);
        assert_eq!(state.visible_from, 2);
        state.next();
        assert_eq!((state.focused, state.visible_from), (0, 0));
        state.previous();
        assert_eq!((state.focused, state.visible_from), (11, 2));
        state.previous_page();
        assert_eq!((state.focused, state.visible_from), (1, 1));
        state.next_page();
        assert_eq!((state.focused, state.visible_from), (11, 2));
    }

    #[test]
    fn complete_assistant_event_reconciles_a_truncated_fullscreen_stream() {
        let mut state = OutputState::default();
        let complete = format!("{}TAIL", "x".repeat(MAX_FULLSCREEN_STREAM_BYTES + 4096));
        append_bounded_fullscreen_stream(&mut state, &complete);
        assert_eq!(state.fullscreen_stream.len(), MAX_FULLSCREEN_STREAM_BYTES);
        apply_fullscreen_event(
            &mut state,
            &QueryEvent::AssistantMessage {
                content: vec![serde_json::json!({"type":"text", "text":complete})],
            },
        );
        assert!(state.fullscreen_stream.is_empty());
        assert!(state.fullscreen.streaming_line().is_none());
        assert!(state.fullscreen.transcript_bytes() > MAX_FULLSCREEN_STREAM_BYTES);
        let frame = state.fullscreen.render_ansi(FrameSpec::new("session", &[]));
        assert!(frame.bytes.contains("TAIL"));
    }

    #[test]
    fn raw_mode_frames_use_carriage_return_line_feeds() {
        fn assert_no_bare_line_feeds(output: &[u8]) {
            for (index, byte) in output.iter().enumerate() {
                if *byte == b'\n' {
                    assert!(
                        index > 0 && output[index - 1] == b'\r',
                        "raw-mode output contained a bare line feed: {:?}",
                        String::from_utf8_lossy(output)
                    );
                }
            }
        }

        let mut frame = Vec::new();
        let mut rendered = RenderedInput::default();
        rendered
            .draw(
                &mut frame,
                InputRenderState {
                    buffer: "first line\nsecond line",
                    cursor_byte: "first line\nsecond".len(),
                    mode: PermissionMode::Default,
                    hint: "",
                    suggestions: &[],
                    selected_suggestion: 0,
                    file_suggestions: &[],
                    selected_file_suggestion: 0,
                    argument_suggestions: &[],
                    selected_argument_suggestion: 0,
                    argument_hint: None,
                    todos: None,
                    status_line: None,
                    theme: ThemePreset::Auto,
                    vim_mode: None,
                    vim_selection: None,
                },
            )
            .unwrap();
        assert_no_bare_line_feeds(&frame);

        let commands = test_commands();
        let suggestions = command_matches("/", &commands);
        let mut suggestion_frame = Vec::new();
        let mut suggestion_rendered = RenderedInput::default();
        suggestion_rendered
            .draw(
                &mut suggestion_frame,
                InputRenderState {
                    buffer: "/",
                    cursor_byte: 1,
                    mode: PermissionMode::Default,
                    hint: "",
                    suggestions: &suggestions,
                    selected_suggestion: 0,
                    file_suggestions: &[],
                    selected_file_suggestion: 0,
                    argument_suggestions: &[],
                    selected_argument_suggestion: 0,
                    argument_hint: None,
                    todos: None,
                    status_line: None,
                    theme: ThemePreset::Auto,
                    vim_mode: None,
                    vim_selection: None,
                },
            )
            .unwrap();
        assert_no_bare_line_feeds(&suggestion_frame);
        let suggestion_text = String::from_utf8_lossy(&suggestion_frame);
        assert!(suggestion_text.contains("/clear"));
        assert!(suggestion_text.contains("Clear conversation"));

        let mut committed = Vec::new();
        print_committed_prompt(&mut committed, "first line\nsecond line").unwrap();
        assert_no_bare_line_feeds(&committed);

        let tall_input = (1..=100)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let mut tall_frame = Vec::new();
        let mut tall_rendered = RenderedInput::default();
        tall_rendered
            .draw(
                &mut tall_frame,
                InputRenderState {
                    buffer: &tall_input,
                    cursor_byte: tall_input.len(),
                    mode: PermissionMode::Default,
                    hint: "",
                    suggestions: &[],
                    selected_suggestion: 0,
                    file_suggestions: &[],
                    selected_file_suggestion: 0,
                    argument_suggestions: &[],
                    selected_argument_suggestion: 0,
                    argument_hint: None,
                    todos: None,
                    status_line: None,
                    theme: ThemePreset::Auto,
                    vim_mode: None,
                    vim_selection: None,
                },
            )
            .unwrap();
        assert!(tall_rendered.rows <= u16::try_from(MAX_VISIBLE_INPUT_LINES + 3).unwrap());
        assert!(String::from_utf8_lossy(&tall_frame).contains("line 100/100"));
    }

    #[test]
    fn backspace_redraw_leaves_one_clean_composer_frame() {
        let mut rendered = RenderedInput::default();
        let mut output = Vec::new();
        rendered
            .draw(
                &mut output,
                InputRenderState {
                    buffer: "abc",
                    cursor_byte: 3,
                    mode: PermissionMode::Default,
                    hint: "",
                    suggestions: &[],
                    selected_suggestion: 0,
                    file_suggestions: &[],
                    selected_file_suggestion: 0,
                    argument_suggestions: &[],
                    selected_argument_suggestion: 0,
                    argument_hint: None,
                    todos: None,
                    status_line: None,
                    theme: ThemePreset::Auto,
                    vim_mode: None,
                    vim_selection: None,
                },
            )
            .unwrap();
        rendered.clear(&mut output).unwrap();
        rendered
            .draw(
                &mut output,
                InputRenderState {
                    buffer: "ab",
                    cursor_byte: 2,
                    mode: PermissionMode::Default,
                    hint: "",
                    suggestions: &[],
                    selected_suggestion: 0,
                    file_suggestions: &[],
                    selected_file_suggestion: 0,
                    argument_suggestions: &[],
                    selected_argument_suggestion: 0,
                    argument_hint: None,
                    todos: None,
                    status_line: None,
                    theme: ThemePreset::Auto,
                    vim_mode: None,
                    vim_selection: None,
                },
            )
            .unwrap();

        let screen_width = terminal::size()
            .map(|(width, _)| usize::from(width).max(4))
            .unwrap_or(80);
        let mut screen = TestScreen::new(screen_width, 24);
        screen.feed(&output);
        let lines = screen.lines();
        assert_eq!(lines.len(), 4, "final screen was {lines:#?}");
        assert_eq!(lines[1], "› ab");
        assert!(!lines.iter().any(|line| line.contains("abc")));
    }
}

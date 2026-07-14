use std::{
    io::{self, IsTerminal, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Result;
use crossterm::{
    cursor,
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags,
        PushKeyboardEnhancementFlags,
    },
    execute, queue,
    style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor},
    terminal::{self, Clear, ClearType},
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use crate::{permissions::PermissionMode, query::QueryEvent};

const EXIT_WINDOW: Duration = Duration::from_millis(1_500);
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_VISIBLE_INPUT_LINES: usize = 10;
const RAW_LINE_END: &str = "\r\n";
const SYNC_OUTPUT_START: &[u8] = b"\x1b[?2026h";
const SYNC_OUTPUT_END: &[u8] = b"\x1b[?2026l";

#[derive(Clone)]
pub struct ConversationUi {
    inner: Arc<Mutex<OutputState>>,
    color: bool,
}

#[derive(Default)]
struct OutputState {
    assistant_open: bool,
    status_open: bool,
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
        let mut out = io::stdout().lock();
        close_assistant(&mut out, &mut state);
        queue!(out, Print("\n"))?;
        out.flush()?;
        Ok(())
    }
}

pub struct InputEditor {
    history: Vec<String>,
    history_limit: usize,
}

pub struct PromptRead {
    pub text: String,
    pub permission_mode: PermissionMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionChoice {
    Allow,
    AllowForSession,
    Deny,
    Interrupt,
}

pub fn request_permission(
    tool: &str,
    summary: &str,
    session_available: bool,
) -> Result<PermissionChoice> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Ok(PermissionChoice::Deny);
    }
    let _raw = RawModeGuard::enter()?;
    let mut out = io::stdout();
    let tool = sanitize_inline(tool);
    let summary = visible_line(&single_line(summary, 160), 160);
    queue!(
        out,
        Print(RAW_LINE_END),
        Print("  Permission required"),
        Print(RAW_LINE_END),
        Print(format!("  {tool}")),
        Print(if summary.is_empty() { "" } else { " · " }),
        Print(summary),
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
            _ => {}
        }
    }
}

impl Default for InputEditor {
    fn default() -> Self {
        Self {
            history: Vec::new(),
            history_limit: 200,
        }
    }
}

impl InputEditor {
    pub fn read(
        &mut self,
        initial_mode: PermissionMode,
        mode_locked: bool,
    ) -> Result<Option<PromptRead>> {
        let _raw = RawModeGuard::enter()?;
        let mut out = io::stdout();
        let mut buffer = String::new();
        let mut cursor_byte = 0usize;
        let mut rendered = RenderedInput::default();
        let mut history_index = self.history.len();
        let mut draft = String::new();
        let mut mode = initial_mode;
        let mut exit_armed = false;
        let mut last_escape: Option<Instant> = None;
        let mut hint = String::new();
        let mut kill_buffer = String::new();

        loop {
            rendered.redraw(&mut out, &buffer, cursor_byte, mode, &hint)?;

            let event = event::read()?;
            match event {
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
                    hint.clear();
                    let is_exit_key = matches!(
                        key,
                        KeyEvent {
                            code: KeyCode::Char('c'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        }
                    );
                    if !is_exit_key {
                        exit_armed = false;
                    }
                    match key {
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
                            ..
                        } => {
                            rendered.erase(&mut out)?;
                            let text = buffer.trim_end().to_owned();
                            if !text.trim().is_empty() {
                                self.push_history(text.clone());
                                print_committed_prompt(&mut out, &text)?;
                            }
                            return Ok(Some(PromptRead {
                                text,
                                permission_mode: mode,
                            }));
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
                                exit_armed = true;
                            } else if exit_armed {
                                rendered.erase(&mut out)?;
                                return Ok(None);
                            } else {
                                hint = "Press Ctrl-C again to exit".to_owned();
                                exit_armed = true;
                            }
                        }
                        KeyEvent {
                            code: KeyCode::Char('d'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if buffer.is_empty() => {
                            rendered.erase(&mut out)?;
                            return Ok(None);
                        }
                        KeyEvent {
                            code: KeyCode::Esc, ..
                        } => {
                            if last_escape.is_some_and(|at| at.elapsed() <= EXIT_WINDOW) {
                                buffer.clear();
                                cursor_byte = 0;
                                hint = "Input cleared".to_owned();
                                last_escape = None;
                            } else {
                                hint = "Press Esc again to clear input".to_owned();
                                last_escape = Some(Instant::now());
                            }
                        }
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
                            kill_buffer = buffer[previous..cursor_byte].to_owned();
                            buffer.drain(previous..cursor_byte);
                            cursor_byte = previous;
                        }
                        KeyEvent {
                            code: KeyCode::Char('w'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if cursor_byte > 0 => {
                            let previous = previous_word_boundary(&buffer, cursor_byte);
                            kill_buffer = buffer[previous..cursor_byte].to_owned();
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
                            kill_buffer = buffer[start..cursor_byte].to_owned();
                            buffer.drain(start..cursor_byte);
                            cursor_byte = start;
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
                            kill_buffer = buffer[cursor_byte..end].to_owned();
                            buffer.drain(cursor_byte..end);
                        }
                        KeyEvent {
                            code: KeyCode::Char('y'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if !kill_buffer.is_empty() => {
                            if buffer.len().saturating_add(kill_buffer.len()) <= MAX_INPUT_BYTES {
                                buffer.insert_str(cursor_byte, &kill_buffer);
                                cursor_byte += kill_buffer.len();
                            } else {
                                hint = "Input limit reached".to_owned();
                            }
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
                            kill_buffer = buffer[cursor_byte..next].to_owned();
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
                        } if line_start(&buffer, cursor_byte) > 0 => {
                            cursor_byte = move_vertical(&buffer, cursor_byte, -1);
                        }
                        KeyEvent {
                            code: KeyCode::Down,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if line_end(&buffer, cursor_byte) < buffer.len() => {
                            cursor_byte = move_vertical(&buffer, cursor_byte, 1);
                        }
                        KeyEvent {
                            code: KeyCode::Up,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if !self.history.is_empty() && !buffer.contains('\n') => {
                            if history_index == self.history.len() {
                                draft.clone_from(&buffer);
                            }
                            history_index = history_index.saturating_sub(1);
                            buffer.clone_from(&self.history[history_index]);
                            cursor_byte = buffer.len();
                        }
                        KeyEvent {
                            code: KeyCode::Down,
                            modifiers: KeyModifiers::NONE,
                            ..
                        } if history_index < self.history.len() => {
                            history_index += 1;
                            if history_index == self.history.len() {
                                buffer.clone_from(&draft);
                            } else {
                                buffer.clone_from(&self.history[history_index]);
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
                            KeyModifiers::CONTROL | KeyModifiers::SUPER | KeyModifiers::HYPER,
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
                // A terminal may reflow already-painted rows before reporting
                // the resize, so the old relative row count is no longer a
                // safe erase anchor. Reset the visible viewport atomically;
                // the next loop iteration paints the composer at the new size.
                Event::Resize(_, _) => rendered.reset_viewport(&mut out)?,
                _ => {}
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

#[derive(Default)]
struct RenderedInput {
    rows: u16,
    cursor_row: u16,
}

impl RenderedInput {
    fn redraw(
        &mut self,
        out: &mut impl Write,
        buffer: &str,
        cursor_byte: usize,
        mode: PermissionMode,
        hint: &str,
    ) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        self.draw(&mut frame, buffer, cursor_byte, mode, hint)?;
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
        queue!(frame, Clear(ClearType::All), cursor::MoveTo(0, 0))?;
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_END);
        }
        out.write_all(&frame)?;
        out.flush()?;
        *self = Self::default();
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
        buffer: &str,
        cursor_byte: usize,
        mode: PermissionMode,
        hint: &str,
    ) -> Result<()> {
        let (width, height) = terminal::size()
            .map(|(width, height)| (usize::from(width).max(4), usize::from(height).max(4)))
            .unwrap_or((80, 24));
        let rule = "─".repeat(width.saturating_sub(1));
        let lines = buffer.split('\n').collect::<Vec<_>>();
        let active_line = buffer[..cursor_byte]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
        let active_start = line_start(buffer, cursor_byte);
        let active_column = buffer[active_start..cursor_byte].graphemes(true).count();
        let mut rendered_cursor_column = active_column;
        let color = std::env::var_os("NO_COLOR").is_none();
        let visible_limit = MAX_VISIBLE_INPUT_LINES.min(height.saturating_sub(4).max(1));
        let visible_start = if lines.len() <= visible_limit {
            0
        } else {
            active_line
                .saturating_sub(visible_limit / 2)
                .min(lines.len().saturating_sub(visible_limit))
        };
        let visible_end = lines.len().min(visible_start.saturating_add(visible_limit));

        if color {
            queue!(out, SetForegroundColor(Color::DarkGrey))?;
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
                    SetForegroundColor(Color::Cyan),
                    SetAttribute(Attribute::Bold)
                )?;
            }
            queue!(out, Print(prefix))?;
            if color && index == 0 {
                queue!(out, ResetColor, SetAttribute(Attribute::Reset))?;
            }
            let available = width.saturating_sub(3);
            let visible = if index == active_line {
                let (visible, column) = visible_around_cursor(line, active_column, available);
                rendered_cursor_column = column;
                visible
            } else {
                visible_line(line, available)
            };
            queue!(out, Print(visible), Print(RAW_LINE_END))?;
        }
        if color {
            queue!(out, SetForegroundColor(Color::DarkGrey))?;
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
        } else if hint.is_empty() {
            format!(
                "  {} · Shift+Tab mode · Shift+Enter/Ctrl+J newline · / commands",
                mode_label(mode)
            )
        } else {
            format!("  {hint}")
        };
        queue!(
            out,
            Print(visible_line(&footer, width.saturating_sub(1))),
            Print(RAW_LINE_END)
        )?;
        if color {
            queue!(out, ResetColor)?;
        }

        self.rows = u16::try_from(visible_end.saturating_sub(visible_start).saturating_add(3))
            .unwrap_or(u16::MAX);
        self.cursor_row =
            u16::try_from(active_line.saturating_sub(visible_start).saturating_add(1))
                .unwrap_or(u16::MAX);
        let move_up = self.rows.saturating_sub(self.cursor_row);
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

struct RawModeGuard {
    bracketed_paste: bool,
    keyboard_enhancement: bool,
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

fn visible_around_cursor(value: &str, cursor: usize, limit: usize) -> (String, usize) {
    if limit == 0 {
        return (String::new(), 0);
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
    (graphemes[start..end].concat(), cursor_width)
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
                "first line\nsecond line",
                "first line\nsecond".len(),
                PermissionMode::Default,
                "",
            )
            .unwrap();
        assert_no_bare_line_feeds(&frame);

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
                &tall_input,
                tall_input.len(),
                PermissionMode::Default,
                "",
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
            .draw(&mut output, "abc", 3, PermissionMode::Default, "")
            .unwrap();
        rendered.clear(&mut output).unwrap();
        rendered
            .draw(&mut output, "ab", 2, PermissionMode::Default, "")
            .unwrap();

        let mut screen = TestScreen::new(80, 24);
        screen.feed(&output);
        let lines = screen.lines();
        assert_eq!(lines.len(), 4, "final screen was {lines:#?}");
        assert_eq!(lines[1], "› ab");
        assert!(!lines.iter().any(|line| line.contains("abc")));
    }
}

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
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::{permissions::PermissionMode, query::QueryEvent};

const EXIT_WINDOW: Duration = Duration::from_millis(1_500);
const MAX_INPUT_BYTES: usize = 1024 * 1024;

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
    Deny,
    Interrupt,
}

pub fn request_permission(tool: &str, summary: &str) -> Result<PermissionChoice> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Ok(PermissionChoice::Deny);
    }
    let _raw = RawModeGuard::enter()?;
    let mut out = io::stdout();
    let tool = sanitize_inline(tool);
    let summary = visible_line(&single_line(summary, 160), 160);
    queue!(
        out,
        Print("\n  Permission required\n"),
        Print(format!("  {tool}")),
        Print(if summary.is_empty() { "" } else { " · " }),
        Print(summary),
        Print("\n  [y] allow once   [n] deny   [Esc] deny   [Ctrl-C] interrupt\n")
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
                queue!(out, Print("  Allowed\n\n"))?;
                out.flush()?;
                return Ok(PermissionChoice::Allow);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('n' | 'N') | KeyCode::Esc,
                kind: KeyEventKind::Press,
                ..
            }) => {
                queue!(out, Print("  Denied\n\n"))?;
                out.flush()?;
                return Ok(PermissionChoice::Deny);
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('c'),
                modifiers: KeyModifiers::CONTROL,
                kind: KeyEventKind::Press,
                ..
            }) => {
                queue!(out, Print("  Interrupted\n\n"))?;
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

        loop {
            rendered.clear(&mut out)?;
            rendered.draw(&mut out, &buffer, cursor_byte, mode, &hint)?;
            out.flush()?;

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
                            rendered.clear(&mut out)?;
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
                                rendered.clear(&mut out)?;
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
                            rendered.clear(&mut out)?;
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
                            ..
                        } if cursor_byte > 0 => {
                            let previous = previous_boundary(&buffer, cursor_byte);
                            buffer.drain(previous..cursor_byte);
                            cursor_byte = previous;
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
                    let text = sanitize_multiline(&text);
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
                Event::Resize(_, _) => rendered.clear(&mut out)?,
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
        let width = terminal::size()
            .map(|(width, _)| usize::from(width).max(20))
            .unwrap_or(80);
        let rule = "─".repeat(width.saturating_sub(1));
        let lines = buffer.split('\n').collect::<Vec<_>>();
        let active_line = buffer[..cursor_byte]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
        let active_start = line_start(buffer, cursor_byte);
        let active_column = buffer[active_start..cursor_byte].chars().count();
        let mut rendered_cursor_column = active_column;
        let color = std::env::var_os("NO_COLOR").is_none();

        if color {
            queue!(out, SetForegroundColor(Color::DarkGrey))?;
        }
        queue!(out, Print(&rule), Print("\n"))?;
        if color {
            queue!(out, ResetColor)?;
        }
        for (index, line) in lines.iter().enumerate() {
            let prefix = if index == 0 { "› " } else { "  " };
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
            queue!(out, Print(visible), Print("\n"))?;
        }
        if color {
            queue!(out, SetForegroundColor(Color::DarkGrey))?;
        }
        queue!(out, Print(&rule), Print("\n"))?;
        let footer = if hint.is_empty() {
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
            Print("\n")
        )?;
        if color {
            queue!(out, ResetColor)?;
        }

        self.rows = u16::try_from(lines.len().saturating_add(3)).unwrap_or(u16::MAX);
        self.cursor_row = u16::try_from(active_line.saturating_add(1)).unwrap_or(u16::MAX);
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
        queue!(out, Print(sanitize_inline(first)), Print("\n"))?;
    }
    for line in lines {
        queue!(out, Print("  "), Print(sanitize_inline(line)), Print("\n"))?;
    }
    queue!(out, Print("\n"))?;
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
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if width.saturating_add(character_width).saturating_add(1) > limit {
            break;
        }
        output.push(character);
        width = width.saturating_add(character_width);
    }
    output.push('…');
    output
}

fn visible_around_cursor(value: &str, cursor: usize, limit: usize) -> (String, usize) {
    if limit == 0 {
        return (String::new(), 0);
    }
    let characters = value.chars().collect::<Vec<_>>();
    let cursor = cursor.min(characters.len());
    let right_reserve = characters
        .get(cursor)
        .and_then(|character| UnicodeWidthChar::width(*character))
        .unwrap_or(0)
        .min(limit);
    let left_limit = limit.saturating_sub(right_reserve);
    let mut start = cursor;
    let mut cursor_width = 0usize;
    while start > 0 {
        let width = UnicodeWidthChar::width(characters[start - 1]).unwrap_or(0);
        if cursor_width.saturating_add(width) > left_limit {
            break;
        }
        start -= 1;
        cursor_width = cursor_width.saturating_add(width);
    }
    let mut end = start;
    let mut rendered_width = 0usize;
    while end < characters.len() {
        let width = UnicodeWidthChar::width(characters[end]).unwrap_or(0);
        if rendered_width.saturating_add(width) > limit {
            break;
        }
        rendered_width = rendered_width.saturating_add(width);
        end += 1;
    }
    (characters[start..end].iter().collect(), cursor_width)
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

fn previous_boundary(value: &str, index: usize) -> usize {
    value[..index]
        .char_indices()
        .next_back()
        .map_or(0, |(boundary, _)| boundary)
}

fn next_boundary(value: &str, index: usize) -> usize {
    value[index..]
        .char_indices()
        .nth(1)
        .map_or(value.len(), |(offset, _)| index + offset)
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
    fn navigation_uses_utf8_boundaries() {
        let value = "a界b";
        assert_eq!(next_boundary(value, 0), 1);
        assert_eq!(next_boundary(value, 1), 4);
        assert_eq!(previous_boundary(value, 4), 1);
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
        assert_eq!(visible_around_cursor("a界b", 2, 4), ("a界b".into(), 3));
    }
}

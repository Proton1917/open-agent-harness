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
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
        KeyModifiers, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags,
        PushKeyboardEnhancementFlags,
    },
    execute, queue,
    style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor},
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use crate::{config::ModelOption, permissions::PermissionMode, query::QueryEvent};

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
const EXTERNAL_EDITOR_CHORD_WINDOW: Duration = Duration::from_secs(3);

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
    stashed_prompt: Option<EditorSnapshot>,
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
}

impl HistorySearch {
    fn new(history: &[String], text: String, cursor_byte: usize) -> Self {
        let mut search = Self {
            original: EditorSnapshot { text, cursor_byte },
            query: String::new(),
            matches: Vec::new(),
            selected: 0,
        };
        search.refresh(history);
        search
    }

    fn refresh(&mut self, history: &[String]) {
        let mut seen = std::collections::HashSet::new();
        self.matches = history
            .iter()
            .rev()
            .filter(|entry| {
                entry.len() <= MAX_HISTORY_SEARCH_ENTRY_BYTES
                    && entry.contains(&self.query)
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
        if self.matches.is_empty() {
            format!("reverse-i-search `{}`: no match", self.query)
        } else {
            format!(
                "reverse-i-search `{}`: {}/{} · Ctrl-R next · Enter run · Esc accept · Ctrl-C cancel",
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
    let arguments = parts.take(31).collect::<Vec<_>>();
    let status = std::process::Command::new(executable)
        .args(arguments)
        .arg(&path)
        .status()
        .map_err(|error| anyhow::anyhow!("cannot launch external editor: {error}"))?;
    if !status.success() {
        anyhow::bail!("external editor exited with {status}")
    }

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
    let bounded = lines.iter().take(10_000).collect::<Vec<_>>();
    let mut top = bounded.len().saturating_sub(1);
    let mut search = None::<String>;
    let mut matches = Vec::<usize>::new();
    let mut selected_match = 0usize;
    let mut dump_to_scrollback = false;

    loop {
        let (width, height) = terminal::size()
            .map(|(width, height)| (usize::from(width).max(4), usize::from(height).max(4)))
            .unwrap_or((80, 24));
        let viewport = height.saturating_sub(2).max(1);
        top = top.min(bounded.len().saturating_sub(viewport));
        let mut frame = Vec::new();
        if synchronized_output_supported() {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        queue!(frame, cursor::MoveTo(0, 0), Clear(ClearType::All))?;
        for line in bounded.iter().skip(top).take(viewport) {
            queue!(
                frame,
                Print(visible_line(line, width.saturating_sub(1))),
                Print(RAW_LINE_END)
            )?;
        }
        let footer = if let Some(query) = &search {
            format!(
                "/{query} · {}/{} · Enter accept · Esc close search",
                selected_match.saturating_add(usize::from(!matches.is_empty())),
                matches.len()
            )
        } else {
            format!(
                "transcript · {}/{} · ↑↓/PgUp/PgDn scroll · / search · n/N match · [ dump · q exit",
                top.saturating_add(1),
                bounded.len().max(1)
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

        let Event::Key(key) = event::read()? else {
            continue;
        };
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            continue;
        }
        if let Some(query) = search.as_mut() {
            match key {
                KeyEvent {
                    code: KeyCode::Esc | KeyCode::Enter,
                    ..
                } => search = None,
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
                } => search = None,
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
                matches = if query.is_empty() {
                    Vec::new()
                } else {
                    bounded
                        .iter()
                        .enumerate()
                        .filter_map(|(index, line)| line.contains(query).then_some(index))
                        .take(1_000)
                        .collect()
                };
                selected_match = 0;
                if let Some(index) = matches.first() {
                    top = (*index).min(bounded.len().saturating_sub(viewport));
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
                ..
            } => top = top.saturating_sub(1),
            KeyEvent {
                code: KeyCode::Down | KeyCode::Char('j'),
                ..
            } => top = (top + 1).min(bounded.len().saturating_sub(viewport)),
            KeyEvent {
                code: KeyCode::PageUp,
                ..
            } => top = top.saturating_sub(viewport),
            KeyEvent {
                code: KeyCode::PageDown | KeyCode::Char(' '),
                ..
            } => top = (top + viewport).min(bounded.len().saturating_sub(viewport)),
            KeyEvent {
                code: KeyCode::Home,
                ..
            } => top = 0,
            KeyEvent {
                code: KeyCode::End, ..
            } => top = bounded.len().saturating_sub(viewport),
            KeyEvent {
                code: KeyCode::Char('/'),
                modifiers: KeyModifiers::NONE,
                ..
            } => search = Some(String::new()),
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
                top = matches[selected_match].min(bounded.len().saturating_sub(viewport));
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
        for line in bounded {
            println!("{line}");
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlashCommandSuggestion {
    pub name: String,
    pub aliases: Vec<String>,
    pub description: String,
    pub argument_hint: Option<String>,
    pub execute_on_enter: bool,
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
    files
        .iter()
        .take(MAX_FILE_CANDIDATES_SCANNED)
        .filter(|file| {
            file.display_path.starts_with(&token.query)
                && if file.is_dir {
                    format!("{}/", file.display_path.trim_end_matches('/')) != token.query
                } else {
                    file.display_path != token.query
                }
        })
        .take(MAX_FILE_SUGGESTIONS)
        .collect()
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
    if options.is_empty() || !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Ok(ModelPickerOutcome::Cancelled);
    }
    let _raw = RawModeGuard::enter()?;
    let mut out = io::stdout();
    let mut state = ModelPickerState::new(options, current);
    let mut rendered = RenderedPicker::default();
    let mut exit_pending: Option<(KeyCode, Instant)> = None;

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
        rendered.redraw(&mut out, options, current, &state, exit_hint)?;
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
            stashed_prompt: None,
        }
    }
}

impl InputEditor {
    pub fn seed_history(&mut self, entries: impl IntoIterator<Item = String>) {
        for entry in entries {
            if !entry.trim().is_empty() && entry.len() <= MAX_INPUT_BYTES {
                self.push_history(entry);
            }
        }
    }

    pub fn read(
        &mut self,
        initial_mode: PermissionMode,
        mode_locked: bool,
        commands: &[SlashCommandSuggestion],
        files: &[FileSuggestion],
        scheduled_prompt: &mut dyn FnMut() -> Result<Option<String>>,
    ) -> Result<Option<PromptRead>> {
        let mut raw_guard = Some(RawModeGuard::enter()?);
        let mut out = io::stdout();
        let mut buffer = String::new();
        let mut cursor_byte = 0usize;
        let mut rendered = RenderedInput::default();
        let mut history_index = self.history.len();
        let mut draft = String::new();
        let mut mode = initial_mode;
        let mut exit_pending: Option<ExitPending> = None;
        let mut last_escape: Option<Instant> = None;
        let mut hint = String::new();
        let mut kill_ring = VecDeque::<String>::new();
        let mut last_yank: Option<(usize, usize, usize)> = None;
        let mut undo_stack = Vec::<EditorSnapshot>::new();
        let mut history_search: Option<HistorySearch> = None;
        let mut ctrl_x_pending: Option<Instant> = None;
        let mut selected_suggestion = 0usize;
        let mut dismissed_suggestions_for: Option<String> = None;
        let mut selected_file_suggestion = 0usize;
        let mut dismissed_file_suggestions_for: Option<(String, usize)> = None;
        let mut needs_redraw = true;

        loop {
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
            let argument_hint = command_argument_hint(&buffer, commands);
            if needs_redraw {
                rendered.redraw(
                    &mut out,
                    InputRenderState {
                        buffer: &buffer,
                        cursor_byte,
                        mode,
                        hint: &hint,
                        suggestions: &suggestions,
                        selected_suggestion,
                        file_suggestions: &file_suggestions,
                        selected_file_suggestion,
                        argument_hint,
                    },
                )?;
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
                    rendered.erase(&mut out)?;
                    return Ok(Some(PromptRead {
                        text: prompt,
                        permission_mode: mode,
                    }));
                }
                continue;
            }
            let event = event::read()?;
            needs_redraw = true;
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
            match event {
                Event::Key(key)
                    if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) =>
                {
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
                                search.refresh(&self.history);
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
                                search.refresh(&self.history);
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
                    let continues_external_editor_chord = matches!(
                        key,
                        KeyEvent {
                            code: KeyCode::Char('e' | 'x'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        }
                    );
                    if !continues_external_editor_chord {
                        ctrl_x_pending = None;
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
                                    rendered.erase(&mut out)?;
                                    let command = "/rewind".to_owned();
                                    print_committed_prompt(&mut out, &command)?;
                                    return Ok(Some(PromptRead {
                                        text: command,
                                        permission_mode: mode,
                                    }));
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
                            if let Some(snapshot) = undo_stack.pop() {
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
                            let search =
                                HistorySearch::new(&self.history, buffer.clone(), cursor_byte);
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
                            rendered.erase(&mut out)?;
                            let command = "/model".to_owned();
                            print_committed_prompt(&mut out, &command)?;
                            return Ok(Some(PromptRead {
                                text: command,
                                permission_mode: mode,
                            }));
                        }
                        KeyEvent {
                            code: KeyCode::Char('t'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            rendered.erase(&mut out)?;
                            let command = "/tasks".to_owned();
                            print_committed_prompt(&mut out, &command)?;
                            return Ok(Some(PromptRead {
                                text: command,
                                permission_mode: mode,
                            }));
                        }
                        KeyEvent {
                            code: KeyCode::Char('o'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            rendered.erase(&mut out)?;
                            let command = "/transcript".to_owned();
                            return Ok(Some(PromptRead {
                                text: command,
                                permission_mode: mode,
                            }));
                        }
                        KeyEvent {
                            code: KeyCode::Char('e'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } if ctrl_x_pending.is_some_and(|started| {
                            started.elapsed() <= EXTERNAL_EDITOR_CHORD_WINDOW
                        }) =>
                        {
                            ctrl_x_pending = None;
                            open_external_editor = true;
                        }
                        KeyEvent {
                            code: KeyCode::Char('x'),
                            modifiers: KeyModifiers::CONTROL,
                            ..
                        } => {
                            ctrl_x_pending = Some(Instant::now());
                            hint = "Ctrl-X: press Ctrl-E to edit externally".to_owned();
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
                        } if !self.history.is_empty() => {
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
                        }
                        | KeyEvent {
                            code: KeyCode::Char('n'),
                            modifiers: KeyModifiers::CONTROL,
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
                rendered.erase(&mut out)?;
                drop(raw_guard.take());
                let edited = edit_prompt_externally(&buffer);
                raw_guard = Some(RawModeGuard::enter()?);
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

struct InputRenderState<'a> {
    buffer: &'a str,
    cursor_byte: usize,
    mode: PermissionMode,
    hint: &'a str,
    suggestions: &'a [&'a SlashCommandSuggestion],
    selected_suggestion: usize,
    file_suggestions: &'a [&'a FileSuggestion],
    selected_file_suggestion: usize,
    argument_hint: Option<&'a str>,
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
        let InputRenderState {
            buffer,
            cursor_byte,
            mode,
            hint,
            suggestions,
            selected_suggestion,
            file_suggestions,
            selected_file_suggestion,
            argument_hint,
        } = state;
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
        let shell_mode = buffer.starts_with('!');
        let suggestion_limit = if file_suggestions.is_empty() {
            suggestions.len().min(6)
        } else {
            file_suggestions.len().min(6)
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
                SetForegroundColor(if shell_mode {
                    Color::Yellow
                } else {
                    Color::DarkGrey
                })
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
                    SetForegroundColor(if shell_mode {
                        Color::Yellow
                    } else {
                        Color::Cyan
                    }),
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
        } else if !hint.is_empty() {
            format!("  {hint}")
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
                    queue!(out, SetForegroundColor(Color::Cyan))?;
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
        } else if suggestions.is_empty() {
            queue!(
                out,
                Print(visible_line(&footer, width.saturating_sub(1))),
                Print(RAW_LINE_END)
            )?;
            0usize
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
                    queue!(out, SetForegroundColor(Color::Cyan))?;
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
    ) -> Result<()> {
        let mut frame = Vec::new();
        let synchronized = synchronized_output_supported();
        if synchronized {
            frame.extend_from_slice(SYNC_OUTPUT_START);
        }
        self.clear(&mut frame)?;
        self.draw(&mut frame, options, current, state, exit_hint)?;
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
    ) -> Result<()> {
        let width = terminal::size()
            .map(|(width, _)| usize::from(width).max(20))
            .unwrap_or(80);
        let color = std::env::var_os("NO_COLOR").is_none();
        if color {
            queue!(
                out,
                SetForegroundColor(Color::Cyan),
                SetAttribute(Attribute::Bold)
            )?;
        }
        queue!(out, Print("  Select model"))?;
        if color {
            queue!(out, ResetColor, SetAttribute(Attribute::Reset))?;
        }
        queue!(
            out,
            Print(RAW_LINE_END),
            Print(visible_line(
                "  Switch between models configured for this backend. Use /model <id> for another model.",
                width.saturating_sub(1),
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
                queue!(out, SetForegroundColor(Color::Cyan))?;
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
        self.rows = u16::try_from(5 + state.visible_count + hidden_row).unwrap_or(u16::MAX);
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

struct RawModeGuard {
    bracketed_paste: bool,
    keyboard_enhancement: bool,
}

struct AlternateScreenGuard;

impl AlternateScreenGuard {
    fn enter() -> Result<Self> {
        execute!(io::stdout(), EnterAlternateScreen, cursor::Hide)?;
        Ok(Self)
    }
}

impl Drop for AlternateScreenGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), cursor::Show, LeaveAlternateScreen);
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

    fn test_commands() -> Vec<SlashCommandSuggestion> {
        vec![
            SlashCommandSuggestion {
                name: "clear".into(),
                aliases: Vec::new(),
                description: "Clear conversation".into(),
                argument_hint: None,
                execute_on_enter: true,
            },
            SlashCommandSuggestion {
                name: "exit".into(),
                aliases: vec!["quit".into()],
                description: "Exit session".into(),
                argument_hint: None,
                execute_on_enter: true,
            },
            SlashCommandSuggestion {
                name: "model".into(),
                aliases: Vec::new(),
                description: "Set model".into(),
                argument_hint: Some("[model]".into()),
                execute_on_enter: true,
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
        let mut search = HistorySearch::new(&history, "draft".to_owned(), 5);
        search.query = "command".to_owned();
        search.refresh(&history);
        assert_eq!(search.matches, ["first command", "second command"]);
        search.query = "missing".to_owned();
        search.refresh(&history);
        assert!(search.matches.is_empty());
        assert_eq!(search.current(), "draft");
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
                    argument_hint: None,
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
                    argument_hint: None,
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
                    argument_hint: None,
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
                    argument_hint: None,
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
                    argument_hint: None,
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
                    argument_hint: None,
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

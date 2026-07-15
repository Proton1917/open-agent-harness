//! Provider-neutral, terminal-independent Vim editing state machine.
//!
//! The state machine owns Vim-specific state only.  The caller keeps the text
//! and byte cursor and can therefore share them with any terminal renderer.
//! All public cursor offsets are UTF-8 byte offsets; every operation snaps them
//! to an extended-grapheme boundary before editing.

use std::collections::VecDeque;

use unicode_segmentation::UnicodeSegmentation;

const MAX_TEXT_BYTES: usize = 1024 * 1024;
const MAX_REGISTER_BYTES: usize = 1024 * 1024;
const MAX_COUNT: usize = 10_000;
const MAX_UNDO_ENTRIES: usize = 128;
const MAX_UNDO_BYTES: usize = 8 * 1024 * 1024;
const MAX_REPEAT_EVENTS: usize = 4096;
const INDENT: &str = "    ";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VimMode {
    Insert,
    Normal,
    Visual,
    VisualLine,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VimKey {
    Char(char),
    /// Insert a paste/IME chunk as one bounded event.
    Text(String),
    Escape,
    Enter,
    Newline,
    Backspace,
    Delete,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VimEvent {
    pub key: VimKey,
    pub control: bool,
    pub alt: bool,
    pub shift: bool,
}

impl VimEvent {
    pub fn key(key: VimKey) -> Self {
        Self {
            key,
            control: false,
            alt: false,
            shift: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VimAction {
    Submit,
    LimitReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VimOutcome {
    pub handled: bool,
    pub changed: bool,
    pub mode_changed: bool,
    pub action: Option<VimAction>,
}

impl VimOutcome {
    fn handled(changed: bool, mode_changed: bool) -> Self {
        Self {
            handled: true,
            changed,
            mode_changed,
            action: None,
        }
    }

    fn passthrough() -> Self {
        Self {
            handled: false,
            changed: false,
            mode_changed: false,
            action: None,
        }
    }

    fn action(action: VimAction) -> Self {
        Self {
            handled: true,
            changed: false,
            mode_changed: false,
            action: Some(action),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operator {
    Delete,
    Change,
    Yank,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FindKind {
    Forward,
    Backward,
    TillForward,
    TillBackward,
}

impl FindKind {
    fn reversed(self) -> Self {
        match self {
            Self::Forward => Self::Backward,
            Self::Backward => Self::Forward,
            Self::TillForward => Self::TillBackward,
            Self::TillBackward => Self::TillForward,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextObjectScope {
    Inner,
    Around,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Pending {
    Idle,
    G {
        count: Option<usize>,
    },
    Find {
        kind: FindKind,
        count: usize,
    },
    Replace {
        count: usize,
    },
    Operator {
        op: Operator,
        count: usize,
        motion_digits: String,
    },
    OperatorFind {
        op: Operator,
        count: usize,
        kind: FindKind,
    },
    TextObject {
        op: Operator,
        count: usize,
        scope: TextObjectScope,
    },
    OperatorG {
        op: Operator,
        count: usize,
    },
    Indent {
        right: bool,
        count: usize,
    },
    VisualReplace,
    VisualTextObject {
        scope: TextObjectScope,
    },
    VisualFind {
        kind: FindKind,
        count: usize,
    },
    VisualG {
        count: Option<usize>,
    },
}

#[derive(Debug, Clone)]
struct Snapshot {
    text: String,
    cursor: usize,
}

#[derive(Debug, Clone, Copy)]
struct Selection {
    start: usize,
    end: usize,
    linewise: bool,
}

#[derive(Debug, Clone, Copy)]
enum Motion {
    Position { target: usize, inclusive: bool },
    Lines { start: usize, end: usize },
}

/// Vim state which can be embedded in an existing input editor.
#[derive(Debug, Clone)]
pub struct VimState {
    mode: VimMode,
    pending: Pending,
    count_digits: String,
    visual_anchor: Option<usize>,
    register: String,
    register_linewise: bool,
    last_find: Option<(FindKind, char)>,
    undo: VecDeque<Snapshot>,
    undo_bytes: usize,
    last_change: Vec<VimKey>,
    command_keys: Vec<VimKey>,
    insert_origin: Option<Snapshot>,
    insert_undo_pushed: bool,
    replaying: bool,
}

impl Default for VimState {
    fn default() -> Self {
        Self::new()
    }
}

impl VimState {
    pub fn new() -> Self {
        Self {
            mode: VimMode::Insert,
            pending: Pending::Idle,
            count_digits: String::new(),
            visual_anchor: None,
            register: String::new(),
            register_linewise: false,
            last_find: None,
            undo: VecDeque::new(),
            undo_bytes: 0,
            last_change: Vec::new(),
            command_keys: Vec::new(),
            insert_origin: None,
            insert_undo_pushed: false,
            replaying: false,
        }
    }

    pub fn mode(&self) -> VimMode {
        self.mode
    }

    pub fn register(&self) -> (&str, bool) {
        (&self.register, self.register_linewise)
    }

    pub fn pending_command(&self) -> bool {
        !matches!(self.pending, Pending::Idle) || !self.count_digits.is_empty()
    }

    pub fn selection(&self, text: &str, cursor: usize) -> Option<(usize, usize, bool)> {
        self.selection_range(text, cursor)
            .map(|selection| (selection.start, selection.end, selection.linewise))
    }

    /// Starts a new composer buffer while preserving user-facing Vim state
    /// which is meaningful across prompts (mode, register, last find/change).
    pub fn reset_buffer(&mut self) {
        if matches!(self.mode, VimMode::Visual | VimMode::VisualLine) {
            self.mode = VimMode::Normal;
        }
        self.pending = Pending::Idle;
        self.count_digits.clear();
        self.visual_anchor = None;
        self.undo.clear();
        self.undo_bytes = 0;
        self.command_keys.clear();
        self.insert_origin = None;
        self.insert_undo_pushed = false;
        self.replaying = false;
    }

    pub fn undo_current(&mut self, text: &mut String, cursor: &mut usize) -> bool {
        self.finish_insert_recording(text, *cursor);
        self.undo(text, cursor)
    }

    /// Handle a normalized terminal event. Modified keys which are not Vim
    /// controls are deliberately returned to the caller.
    pub fn handle_event(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        event: VimEvent,
    ) -> VimOutcome {
        if (event.control || event.alt) && event.key != VimKey::Escape {
            return VimOutcome::passthrough();
        }
        self.handle_key(text, cursor, event.key)
    }

    /// Handle one Vim key. `cursor` is normalized in place even when the key
    /// is ignored, so a renderer can safely use it as a byte slice boundary.
    pub fn handle_key(&mut self, text: &mut String, cursor: &mut usize, key: VimKey) -> VimOutcome {
        *cursor = floor_boundary(text, (*cursor).min(text.len()));
        let before_mode = self.mode;
        let outcome = match self.mode {
            VimMode::Insert => self.handle_insert(text, cursor, key),
            VimMode::Normal => self.handle_normal(text, cursor, key),
            VimMode::Visual | VimMode::VisualLine => self.handle_visual(text, cursor, key),
        };
        self.normalize_cursor(text, cursor);
        VimOutcome {
            mode_changed: before_mode != self.mode || outcome.mode_changed,
            ..outcome
        }
    }

    fn handle_insert(&mut self, text: &mut String, cursor: &mut usize, key: VimKey) -> VimOutcome {
        match key {
            VimKey::Escape => {
                self.record_key(VimKey::Escape);
                self.finish_insert_recording(text, *cursor);
                self.mode = VimMode::Normal;
                self.pending = Pending::Idle;
                self.count_digits.clear();
                if *cursor > 0 {
                    let previous = prev_boundary(text, *cursor);
                    if grapheme_at(text, previous) != "\n" {
                        *cursor = previous;
                    }
                }
                VimOutcome::handled(false, true)
            }
            VimKey::Enter => VimOutcome::action(VimAction::Submit),
            VimKey::Newline => self.insert_chunk(text, cursor, "\n", VimKey::Newline),
            VimKey::Char(character) => {
                let value = character.to_string();
                self.insert_chunk(text, cursor, &value, VimKey::Char(character))
            }
            VimKey::Text(value) => {
                let event = VimKey::Text(value.clone());
                self.insert_chunk(text, cursor, &value, event)
            }
            VimKey::Backspace => {
                if *cursor == 0 {
                    return VimOutcome::handled(false, false);
                }
                self.ensure_insert_undo(text, *cursor);
                let start = prev_boundary(text, *cursor);
                text.replace_range(start..*cursor, "");
                *cursor = start;
                self.record_key(VimKey::Backspace);
                VimOutcome::handled(true, false)
            }
            VimKey::Delete => {
                if *cursor >= text.len() {
                    return VimOutcome::handled(false, false);
                }
                self.ensure_insert_undo(text, *cursor);
                let end = next_boundary(text, *cursor);
                text.replace_range(*cursor..end, "");
                self.record_key(VimKey::Delete);
                VimOutcome::handled(true, false)
            }
            VimKey::Left => {
                *cursor = prev_boundary(text, *cursor);
                VimOutcome::handled(false, false)
            }
            VimKey::Right => {
                *cursor = next_boundary(text, *cursor);
                VimOutcome::handled(false, false)
            }
            VimKey::Up => {
                *cursor = vertical_motion(text, *cursor, -1);
                VimOutcome::handled(false, false)
            }
            VimKey::Down => {
                *cursor = vertical_motion(text, *cursor, 1);
                VimOutcome::handled(false, false)
            }
            VimKey::Home => {
                *cursor = line_start(text, *cursor);
                VimOutcome::handled(false, false)
            }
            VimKey::End => {
                *cursor = line_end(text, *cursor);
                VimOutcome::handled(false, false)
            }
        }
    }

    fn insert_chunk(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        value: &str,
        event: VimKey,
    ) -> VimOutcome {
        if value.is_empty() {
            return VimOutcome::handled(false, false);
        }
        if text.len().saturating_add(value.len()) > MAX_TEXT_BYTES {
            return VimOutcome::action(VimAction::LimitReached);
        }
        self.ensure_insert_undo(text, *cursor);
        text.insert_str(*cursor, value);
        *cursor += value.len();
        self.record_key(event);
        VimOutcome::handled(true, false)
    }

    fn handle_normal(&mut self, text: &mut String, cursor: &mut usize, key: VimKey) -> VimOutcome {
        let VimKey::Char(character) = key else {
            let pending_expects_motion =
                matches!(self.pending, Pending::Idle | Pending::Operator { .. });
            return match key {
                VimKey::Escape => {
                    self.reset_command();
                    VimOutcome::handled(false, false)
                }
                VimKey::Enter => VimOutcome::action(VimAction::Submit),
                // In the reference frontend, physical arrows remain base
                // editor keys so history and wrapped-line fallback still work.
                VimKey::Left | VimKey::Right | VimKey::Up | VimKey::Down
                    if matches!(self.pending, Pending::Idle) =>
                {
                    VimOutcome::passthrough()
                }
                VimKey::Left => self.handle_normal(text, cursor, VimKey::Char('h')),
                VimKey::Right => self.handle_normal(text, cursor, VimKey::Char('l')),
                VimKey::Up => self.handle_normal(text, cursor, VimKey::Char('k')),
                VimKey::Down => self.handle_normal(text, cursor, VimKey::Char('j')),
                VimKey::Home => self.normal_motion(text, cursor, '0'),
                VimKey::End => self.normal_motion(text, cursor, '$'),
                VimKey::Backspace if pending_expects_motion => {
                    self.handle_normal(text, cursor, VimKey::Char('h'))
                }
                VimKey::Delete if pending_expects_motion && self.count_digits.is_empty() => {
                    self.handle_normal(text, cursor, VimKey::Char('x'))
                }
                VimKey::Backspace | VimKey::Delete => {
                    self.reset_command();
                    VimOutcome::handled(false, false)
                }
                _ => VimOutcome::passthrough(),
            };
        };
        if character == '/' && matches!(self.pending, Pending::Idle) && self.count_digits.is_empty()
        {
            return VimOutcome::passthrough();
        }
        if character == '?' && matches!(self.pending, Pending::Idle) && self.count_digits.is_empty()
        {
            text.clear();
            text.push('?');
            *cursor = 0;
            self.finish_nonchange();
            return VimOutcome::handled(true, false);
        }
        self.record_key(VimKey::Char(character));

        let pending = std::mem::replace(&mut self.pending, Pending::Idle);
        match pending {
            Pending::Idle => self.normal_idle(text, cursor, character),
            Pending::G { count } => {
                if character == 'g' {
                    *cursor = if let Some(line) = count {
                        nth_line_start(text, line.saturating_sub(1))
                    } else {
                        0
                    };
                    self.finish_nonchange();
                    VimOutcome::handled(false, false)
                } else if matches!(character, 'j' | 'k') {
                    let repeats = count.unwrap_or(1).clamp(1, MAX_COUNT);
                    for _ in 0..repeats {
                        *cursor =
                            vertical_motion(text, *cursor, if character == 'j' { 1 } else { -1 });
                    }
                    self.finish_nonchange();
                    VimOutcome::handled(false, false)
                } else {
                    self.finish_nonchange();
                    VimOutcome::handled(false, false)
                }
            }
            Pending::Find { kind, count } => {
                if let Some(target) = find_motion(text, *cursor, kind, character, count) {
                    *cursor = target;
                    self.last_find = Some((kind, character));
                }
                self.finish_nonchange();
                VimOutcome::handled(false, false)
            }
            Pending::Replace { count } => {
                let changed = self.replace_chars(text, cursor, character, count);
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, false)
            }
            Pending::Operator {
                op,
                count,
                mut motion_digits,
            } => {
                if character.is_ascii_digit() {
                    motion_digits.push(character);
                    if motion_digits.len() > 5 {
                        motion_digits = MAX_COUNT.to_string();
                    }
                    self.pending = Pending::Operator {
                        op,
                        count,
                        motion_digits,
                    };
                    return VimOutcome::handled(false, false);
                }
                let motion_count = parse_count(&motion_digits);
                let effective = count.saturating_mul(motion_count).min(MAX_COUNT);
                self.handle_operator_key(text, cursor, op, effective, character)
            }
            Pending::OperatorFind { op, count, kind } => {
                self.last_find = Some((kind, character));
                let motion = find_motion(text, *cursor, kind, character, count).map(|target| {
                    Motion::Position {
                        target,
                        inclusive: matches!(kind, FindKind::Forward | FindKind::Backward),
                    }
                });
                self.execute_operator(text, cursor, op, motion)
            }
            Pending::TextObject { op, count, scope } => {
                let selection = find_text_object(text, *cursor, character, scope, count);
                self.execute_operator_selection(text, cursor, op, selection)
            }
            Pending::OperatorG { op, count } => {
                if character == 'g' {
                    let target = if count > 1 {
                        nth_line_start(text, count - 1)
                    } else {
                        0
                    };
                    let start = line_start(text, (*cursor).min(target));
                    let end = line_range_end(text, (*cursor).max(target), 1);
                    self.execute_operator_selection(
                        text,
                        cursor,
                        op,
                        Some(Selection {
                            start,
                            end,
                            linewise: true,
                        }),
                    )
                } else {
                    self.finish_nonchange();
                    VimOutcome::handled(false, false)
                }
            }
            Pending::Indent { right, count } => {
                if (right && character == '>') || (!right && character == '<') {
                    let selection = Selection {
                        start: line_start(text, *cursor),
                        end: line_range_end(text, *cursor, count),
                        linewise: true,
                    };
                    let changed = self.indent_selection(text, cursor, selection, right);
                    if changed {
                        self.finish_change();
                    } else {
                        self.finish_nonchange();
                    }
                    VimOutcome::handled(changed, false)
                } else {
                    self.finish_nonchange();
                    VimOutcome::handled(false, false)
                }
            }
            Pending::VisualReplace
            | Pending::VisualTextObject { .. }
            | Pending::VisualFind { .. }
            | Pending::VisualG { .. } => {
                self.finish_nonchange();
                VimOutcome::handled(false, false)
            }
        }
    }

    fn normal_idle(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        character: char,
    ) -> VimOutcome {
        if character.is_ascii_digit() && (character != '0' || !self.count_digits.is_empty()) {
            push_count_digit(&mut self.count_digits, character);
            return VimOutcome::handled(false, false);
        }
        let count = self.take_count();
        match character {
            'h' | 'j' | 'k' | 'l' | ' ' | 'w' | 'e' | 'b' | 'W' | 'E' | 'B' | '0' | '^' | '$' => {
                let outcome = self.normal_motion_count(text, cursor, character, count);
                self.finish_nonchange();
                outcome
            }
            'g' => {
                self.pending = Pending::G {
                    count: (count > 1).then_some(count),
                };
                VimOutcome::handled(false, false)
            }
            'G' => {
                *cursor = if count > 1 {
                    nth_line_start(text, count - 1)
                } else {
                    last_line_start(text)
                };
                self.finish_nonchange();
                VimOutcome::handled(false, false)
            }
            'f' | 'F' | 't' | 'T' => {
                self.pending = Pending::Find {
                    kind: find_kind(character),
                    count,
                };
                VimOutcome::handled(false, false)
            }
            ';' | ',' => {
                if let Some((kind, target)) = self.last_find {
                    let kind = if character == ',' {
                        kind.reversed()
                    } else {
                        kind
                    };
                    if let Some(found) = find_motion(text, *cursor, kind, target, count) {
                        *cursor = found;
                    }
                }
                self.finish_nonchange();
                VimOutcome::handled(false, false)
            }
            'd' | 'c' | 'y' => {
                self.pending = Pending::Operator {
                    op: operator(character),
                    count,
                    motion_digits: String::new(),
                };
                VimOutcome::handled(false, false)
            }
            'D' | 'C' => {
                let op = if character == 'D' {
                    Operator::Delete
                } else {
                    Operator::Change
                };
                self.execute_operator(
                    text,
                    cursor,
                    op,
                    Some(Motion::Position {
                        target: line_end(text, *cursor),
                        inclusive: false,
                    }),
                )
            }
            'Y' => self.execute_operator_selection(
                text,
                cursor,
                Operator::Yank,
                Some(Selection {
                    start: line_start(text, *cursor),
                    end: line_range_end(text, *cursor, count),
                    linewise: true,
                }),
            ),
            'x' => {
                let end = advance_graphemes_within_line(text, *cursor, count);
                self.execute_operator_selection(
                    text,
                    cursor,
                    Operator::Delete,
                    (*cursor < end).then_some(Selection {
                        start: *cursor,
                        end,
                        linewise: false,
                    }),
                )
            }
            'r' => {
                self.pending = Pending::Replace { count };
                VimOutcome::handled(false, false)
            }
            'i' | 'I' | 'a' | 'A' | 'o' | 'O' => self.enter_insert_command(text, cursor, character),
            'v' | 'V' => {
                self.mode = if character == 'v' {
                    VimMode::Visual
                } else {
                    VimMode::VisualLine
                };
                self.visual_anchor = Some(*cursor);
                VimOutcome::handled(false, true)
            }
            'p' | 'P' => {
                let changed = self.paste(text, cursor, character == 'p', count);
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, false)
            }
            'u' => {
                let changed = self.undo(text, cursor);
                self.finish_nonchange();
                VimOutcome::handled(changed, false)
            }
            '.' => {
                self.finish_nonchange();
                let changed = self.dot_repeat(text, cursor, count);
                VimOutcome::handled(changed, self.mode != VimMode::Normal)
            }
            '~' => {
                let end = advance_graphemes_within_line(text, *cursor, count);
                let changed = self.transform_range(text, cursor, *cursor, end, Case::Toggle);
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, false)
            }
            'J' => {
                let changed = self.join_lines(text, cursor, count.max(2));
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, false)
            }
            '>' | '<' => {
                self.pending = Pending::Indent {
                    right: character == '>',
                    count,
                };
                VimOutcome::handled(false, false)
            }
            _ => {
                self.finish_nonchange();
                VimOutcome::handled(false, false)
            }
        }
    }

    fn handle_operator_key(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        op: Operator,
        count: usize,
        character: char,
    ) -> VimOutcome {
        if character == operator_key(op) {
            let selection = Selection {
                start: line_start(text, *cursor),
                end: line_range_end(text, *cursor, count),
                linewise: true,
            };
            return self.execute_operator_selection(text, cursor, op, Some(selection));
        }
        match character {
            'h' | 'j' | 'k' | 'l' | ' ' | 'w' | 'e' | 'b' | 'W' | 'E' | 'B' | '0' | '^' | '$'
            | 'G' => {
                // Vim's `cw` behaves like `ce` on a non-blank: it changes the
                // word but deliberately preserves the following whitespace.
                let motion_key = if op == Operator::Change && character == 'w' {
                    'e'
                } else {
                    character
                };
                let motion = resolve_motion(text, *cursor, motion_key, count);
                self.execute_operator(text, cursor, op, motion)
            }
            'f' | 'F' | 't' | 'T' => {
                self.pending = Pending::OperatorFind {
                    op,
                    count,
                    kind: find_kind(character),
                };
                VimOutcome::handled(false, false)
            }
            'i' | 'a' => {
                self.pending = Pending::TextObject {
                    op,
                    count,
                    scope: if character == 'i' {
                        TextObjectScope::Inner
                    } else {
                        TextObjectScope::Around
                    },
                };
                VimOutcome::handled(false, false)
            }
            'g' => {
                self.pending = Pending::OperatorG { op, count };
                VimOutcome::handled(false, false)
            }
            _ => {
                self.finish_nonchange();
                VimOutcome::handled(false, false)
            }
        }
    }

    fn handle_visual(&mut self, text: &mut String, cursor: &mut usize, key: VimKey) -> VimOutcome {
        let VimKey::Char(character) = key else {
            return match key {
                VimKey::Escape => {
                    self.leave_visual();
                    self.finish_nonchange();
                    VimOutcome::handled(false, true)
                }
                VimKey::Left => self.visual_motion(text, cursor, 'h'),
                VimKey::Right => self.visual_motion(text, cursor, 'l'),
                VimKey::Up => self.visual_motion(text, cursor, 'k'),
                VimKey::Down => self.visual_motion(text, cursor, 'j'),
                _ => VimOutcome::passthrough(),
            };
        };
        self.record_key(VimKey::Char(character));
        if matches!(self.pending, Pending::VisualReplace) {
            self.pending = Pending::Idle;
            let Some(selection) = self.selection_range(text, *cursor) else {
                self.finish_nonchange();
                return VimOutcome::handled(false, false);
            };
            let changed = self.replace_selection(text, cursor, selection, character);
            self.leave_visual();
            if changed {
                self.finish_change();
            } else {
                self.finish_nonchange();
            }
            return VimOutcome::handled(changed, true);
        }
        if let Pending::VisualTextObject { scope } = self.pending {
            self.pending = Pending::Idle;
            if let Some(selection) = find_text_object(text, *cursor, character, scope, 1) {
                self.mode = VimMode::Visual;
                self.visual_anchor = Some(selection.start);
                *cursor = prev_boundary(text, selection.end);
            } else {
                self.finish_nonchange();
            }
            return VimOutcome::handled(false, false);
        }
        if let Pending::VisualFind { kind, count } = self.pending {
            self.pending = Pending::Idle;
            if let Some(target) = find_motion(text, *cursor, kind, character, count) {
                *cursor = target;
                self.last_find = Some((kind, character));
            }
            return VimOutcome::handled(false, false);
        }
        if let Pending::VisualG { count } = self.pending {
            self.pending = Pending::Idle;
            if character == 'g' {
                *cursor = count.map_or(0, |line| nth_line_start(text, line.saturating_sub(1)));
            } else if matches!(character, 'j' | 'k') {
                let repeats = count.unwrap_or(1).clamp(1, MAX_COUNT);
                for _ in 0..repeats {
                    *cursor = vertical_motion(text, *cursor, if character == 'j' { 1 } else { -1 });
                }
            }
            return VimOutcome::handled(false, false);
        }
        if character.is_ascii_digit() && (character != '0' || !self.count_digits.is_empty()) {
            push_count_digit(&mut self.count_digits, character);
            return VimOutcome::handled(false, false);
        }
        let count = self.take_count();
        match character {
            'h' | 'j' | 'k' | 'l' | ' ' | 'w' | 'e' | 'b' | 'W' | 'E' | 'B' | '0' | '^' | '$' => {
                self.visual_motion_count(text, cursor, character, count)
            }
            'G' => {
                *cursor = if count > 1 {
                    nth_line_start(text, count - 1)
                } else {
                    last_line_start(text)
                };
                VimOutcome::handled(false, false)
            }
            'g' => {
                self.pending = Pending::VisualG {
                    count: (count > 1).then_some(count),
                };
                VimOutcome::handled(false, false)
            }
            'f' | 'F' | 't' | 'T' => {
                self.pending = Pending::VisualFind {
                    kind: find_kind(character),
                    count,
                };
                VimOutcome::handled(false, false)
            }
            ';' | ',' => {
                if let Some((kind, target)) = self.last_find {
                    let kind = if character == ',' {
                        kind.reversed()
                    } else {
                        kind
                    };
                    if let Some(found) = find_motion(text, *cursor, kind, target, count) {
                        *cursor = found;
                    }
                }
                VimOutcome::handled(false, false)
            }
            'v' => {
                if self.mode == VimMode::Visual {
                    self.leave_visual();
                } else {
                    self.mode = VimMode::Visual;
                }
                VimOutcome::handled(false, true)
            }
            'V' => {
                if self.mode == VimMode::VisualLine {
                    self.leave_visual();
                } else {
                    self.mode = VimMode::VisualLine;
                }
                VimOutcome::handled(false, true)
            }
            'd' | 'x' | 'y' | 'c' | 's' | 'X' | 'D' | 'C' | 'S' | 'R' | 'Y' => {
                let op = match character {
                    'y' | 'Y' => Operator::Yank,
                    'c' | 's' | 'C' | 'S' | 'R' => Operator::Change,
                    _ => Operator::Delete,
                };
                let mut selection = self.selection_range(text, *cursor);
                if matches!(character, 'X' | 'D' | 'C' | 'S' | 'R' | 'Y') {
                    selection = selection.map(|selection| Selection {
                        start: line_start(text, selection.start),
                        end: line_range_end(text, selection.end.saturating_sub(1), 1),
                        linewise: true,
                    });
                }
                self.execute_visual_operator(text, cursor, op, selection)
            }
            'o' => {
                if let Some(anchor) = self.visual_anchor.as_mut() {
                    std::mem::swap(anchor, cursor);
                }
                VimOutcome::handled(false, false)
            }
            'p' | 'P' => {
                let changed = self.visual_paste(text, cursor);
                self.leave_visual();
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, true)
            }
            'r' => {
                self.pending = Pending::VisualReplace;
                VimOutcome::handled(false, false)
            }
            'i' | 'a' => {
                self.pending = Pending::VisualTextObject {
                    scope: if character == 'i' {
                        TextObjectScope::Inner
                    } else {
                        TextObjectScope::Around
                    },
                };
                VimOutcome::handled(false, false)
            }
            '~' | 'u' | 'U' => {
                let Some(selection) = self.selection_range(text, *cursor) else {
                    return VimOutcome::handled(false, false);
                };
                let case = match character {
                    'u' => Case::Lower,
                    'U' => Case::Upper,
                    _ => Case::Toggle,
                };
                let selection_start = selection.start;
                let changed =
                    self.transform_range(text, cursor, selection.start, selection.end, case);
                *cursor = selection_start.min(text.len());
                self.leave_visual();
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, true)
            }
            '>' | '<' => {
                let selection = self.selection_range(text, *cursor);
                let changed = selection.is_some_and(|selection| {
                    self.indent_selection(text, cursor, selection, character == '>')
                });
                self.leave_visual();
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, true)
            }
            'J' => {
                let lines = self.selection_range(text, *cursor);
                let changed = if let Some(selection) = lines {
                    let selected = &text[selection.start..selection.end];
                    let mut line_count = selected.bytes().filter(|byte| *byte == b'\n').count();
                    if !selected.ends_with('\n') {
                        line_count = line_count.saturating_add(1);
                    }
                    *cursor = selection.start;
                    self.join_lines(text, cursor, line_count)
                } else {
                    false
                };
                self.leave_visual();
                if changed {
                    self.finish_change();
                } else {
                    self.finish_nonchange();
                }
                VimOutcome::handled(changed, true)
            }
            _ => VimOutcome::handled(false, false),
        }
    }

    fn execute_visual_operator(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        op: Operator,
        selection: Option<Selection>,
    ) -> VimOutcome {
        let mode_changed = true;
        let outcome = self.execute_operator_selection(text, cursor, op, selection);
        if op != Operator::Change {
            self.leave_visual();
        } else {
            self.visual_anchor = None;
        }
        VimOutcome {
            mode_changed,
            ..outcome
        }
    }

    fn execute_operator(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        op: Operator,
        motion: Option<Motion>,
    ) -> VimOutcome {
        let selection = motion.and_then(|motion| motion_selection(text, *cursor, motion));
        self.execute_operator_selection(text, cursor, op, selection)
    }

    fn execute_operator_selection(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        op: Operator,
        selection: Option<Selection>,
    ) -> VimOutcome {
        let Some(selection) = selection.filter(|selection| selection.start < selection.end) else {
            self.finish_nonchange();
            return VimOutcome::handled(false, false);
        };
        self.set_register(&text[selection.start..selection.end], selection.linewise);
        if op == Operator::Yank {
            *cursor = selection.start.min(text.len());
            self.finish_nonchange();
            return VimOutcome::handled(false, false);
        }

        let snapshot = Snapshot {
            text: text.clone(),
            cursor: *cursor,
        };
        self.push_undo(snapshot.clone());
        text.replace_range(selection.start..selection.end, "");
        *cursor = selection.start.min(text.len());
        if op == Operator::Change {
            self.mode = VimMode::Insert;
            self.pending = Pending::Idle;
            self.insert_origin = Some(snapshot);
            self.insert_undo_pushed = true;
            VimOutcome::handled(true, true)
        } else {
            self.finish_change();
            VimOutcome::handled(true, false)
        }
    }

    fn normal_motion(&mut self, text: &str, cursor: &mut usize, key: char) -> VimOutcome {
        self.normal_motion_count(text, cursor, key, 1)
    }

    fn normal_motion_count(
        &mut self,
        text: &str,
        cursor: &mut usize,
        key: char,
        count: usize,
    ) -> VimOutcome {
        if matches!(key, 'j' | 'k') {
            for _ in 0..count.clamp(1, MAX_COUNT) {
                *cursor = vertical_motion(text, *cursor, if key == 'j' { 1 } else { -1 });
            }
            return VimOutcome::handled(false, false);
        }
        if let Some(motion) = resolve_motion(text, *cursor, key, count) {
            *cursor = match motion {
                Motion::Position { target, .. } => target,
                Motion::Lines { start, .. } => start,
            };
        }
        VimOutcome::handled(false, false)
    }

    fn visual_motion(&mut self, text: &str, cursor: &mut usize, key: char) -> VimOutcome {
        self.visual_motion_count(text, cursor, key, 1)
    }

    fn visual_motion_count(
        &mut self,
        text: &str,
        cursor: &mut usize,
        key: char,
        count: usize,
    ) -> VimOutcome {
        if matches!(key, 'j' | 'k') {
            for _ in 0..count.clamp(1, MAX_COUNT) {
                *cursor = vertical_motion(text, *cursor, if key == 'j' { 1 } else { -1 });
            }
            return VimOutcome::handled(false, false);
        }
        if let Some(motion) = resolve_motion(text, *cursor, key, count) {
            *cursor = match motion {
                Motion::Position { target, .. } => target,
                Motion::Lines { start, .. } => start,
            };
        }
        VimOutcome::handled(false, false)
    }

    fn enter_insert_command(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        command: char,
    ) -> VimOutcome {
        match command {
            'I' => *cursor = first_nonblank(text, *cursor),
            'a' => {
                if *cursor < text.len() && grapheme_at(text, *cursor) != "\n" {
                    *cursor = next_boundary(text, *cursor);
                }
            }
            'A' => *cursor = line_end(text, *cursor),
            'o' | 'O' => {
                let snapshot = Snapshot {
                    text: text.clone(),
                    cursor: *cursor,
                };
                self.push_undo(snapshot.clone());
                if command == 'o' {
                    let end = line_end(text, *cursor);
                    text.insert(end, '\n');
                    *cursor = end + 1;
                } else {
                    let start = line_start(text, *cursor);
                    text.insert(start, '\n');
                    *cursor = start;
                }
                self.insert_origin = Some(snapshot);
                self.insert_undo_pushed = true;
                self.mode = VimMode::Insert;
                return VimOutcome::handled(true, true);
            }
            _ => {}
        }
        self.insert_origin = Some(Snapshot {
            text: text.clone(),
            cursor: *cursor,
        });
        self.insert_undo_pushed = false;
        self.mode = VimMode::Insert;
        VimOutcome::handled(false, true)
    }

    fn replace_chars(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        replacement: char,
        count: usize,
    ) -> bool {
        let end = advance_graphemes_within_line(text, *cursor, count);
        if end <= *cursor {
            return false;
        }
        let value = replacement
            .to_string()
            .repeat(grapheme_count(&text[*cursor..end]));
        if text.len() - (end - *cursor) + value.len() > MAX_TEXT_BYTES {
            return false;
        }
        self.push_undo(Snapshot {
            text: text.clone(),
            cursor: *cursor,
        });
        text.replace_range(*cursor..end, &value);
        true
    }

    fn replace_selection(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        selection: Selection,
        replacement: char,
    ) -> bool {
        let selected = &text[selection.start..selection.end];
        let count = grapheme_count(&selected.replace('\n', ""));
        if count == 0 {
            return false;
        }
        let replacement = replacement.to_string();
        let value = selected
            .graphemes(true)
            .map(|grapheme| {
                if grapheme == "\n" {
                    "\n"
                } else {
                    replacement.as_str()
                }
            })
            .collect::<String>();
        if text.len() - (selection.end - selection.start) + value.len() > MAX_TEXT_BYTES {
            return false;
        }
        self.push_undo(Snapshot {
            text: text.clone(),
            cursor: *cursor,
        });
        text.replace_range(selection.start..selection.end, &value);
        *cursor = selection.start;
        true
    }

    fn paste(&mut self, text: &mut String, cursor: &mut usize, after: bool, count: usize) -> bool {
        if self.register.is_empty() {
            return false;
        }
        let Some(value) = repeat_bounded(&self.register, count, MAX_TEXT_BYTES - text.len()) else {
            return false;
        };
        let at = if self.register_linewise {
            if after {
                let end = line_end(text, *cursor);
                if end < text.len() {
                    end + 1
                } else {
                    text.len()
                }
            } else {
                line_start(text, *cursor)
            }
        } else if after && *cursor < text.len() && grapheme_at(text, *cursor) != "\n" {
            next_boundary(text, *cursor)
        } else {
            *cursor
        };
        let mut inserted = value;
        if self.register_linewise && !inserted.ends_with('\n') {
            inserted.push('\n');
        }
        if self.register_linewise
            && after
            && at == text.len()
            && !text.is_empty()
            && !text.ends_with('\n')
        {
            inserted.insert(0, '\n');
        }
        if text.len().saturating_add(inserted.len()) > MAX_TEXT_BYTES {
            return false;
        }
        self.push_undo(Snapshot {
            text: text.clone(),
            cursor: *cursor,
        });
        text.insert_str(at, &inserted);
        *cursor = at;
        true
    }

    fn visual_paste(&mut self, text: &mut String, cursor: &mut usize) -> bool {
        let Some(selection) = self.selection_range(text, *cursor) else {
            return false;
        };
        if self.register.is_empty()
            || text.len() - (selection.end - selection.start) + self.register.len() > MAX_TEXT_BYTES
        {
            return false;
        }
        let old = text[selection.start..selection.end].to_owned();
        let old_linewise = selection.linewise;
        let replacement = self.register.clone();
        self.push_undo(Snapshot {
            text: text.clone(),
            cursor: *cursor,
        });
        text.replace_range(selection.start..selection.end, &replacement);
        *cursor = selection.start;
        self.set_register(&old, old_linewise);
        true
    }

    fn transform_range(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        start: usize,
        end: usize,
        case: Case,
    ) -> bool {
        if start >= end || end > text.len() {
            return false;
        }
        let transformed = text[start..end]
            .graphemes(true)
            .map(|grapheme| transform_grapheme(grapheme, case))
            .collect::<String>();
        if transformed == text[start..end]
            || text.len() - (end - start) + transformed.len() > MAX_TEXT_BYTES
        {
            return false;
        }
        self.push_undo(Snapshot {
            text: text.clone(),
            cursor: *cursor,
        });
        text.replace_range(start..end, &transformed);
        *cursor = start;
        true
    }

    fn indent_selection(
        &mut self,
        text: &mut String,
        cursor: &mut usize,
        selection: Selection,
        right: bool,
    ) -> bool {
        let starts = line_starts_in(text, selection.start, selection.end);
        if starts.is_empty() {
            return false;
        }
        let snapshot = Snapshot {
            text: text.clone(),
            cursor: *cursor,
        };
        let mut changed = false;
        if right {
            if text.len().saturating_add(starts.len() * INDENT.len()) > MAX_TEXT_BYTES {
                return false;
            }
            for start in starts.into_iter().rev() {
                text.insert_str(start, INDENT);
                changed = true;
            }
        } else {
            for start in starts.into_iter().rev() {
                let mut end = start;
                let mut spaces = 0;
                while end < text.len() && spaces < INDENT.len() {
                    match text.as_bytes()[end] {
                        b' ' => {
                            end += 1;
                            spaces += 1;
                        }
                        b'\t' if spaces == 0 => {
                            end += 1;
                            break;
                        }
                        _ => break,
                    }
                }
                if end > start {
                    text.replace_range(start..end, "");
                    changed = true;
                }
            }
        }
        if changed {
            self.push_undo(snapshot);
            // Byte offsets after reverse-order insert/delete are not stable;
            // keep the cursor on the first selected logical line instead of
            // reinterpreting the pre-edit byte offset in the new buffer.
            *cursor = selection.start.min(text.len());
        }
        changed
    }

    fn join_lines(&mut self, text: &mut String, cursor: &mut usize, count: usize) -> bool {
        let snapshot = Snapshot {
            text: text.clone(),
            cursor: *cursor,
        };
        let mut changed = false;
        let at = *cursor;
        for _ in 1..count.min(MAX_COUNT) {
            let end = line_end(text, at);
            if end >= text.len() || text.as_bytes()[end] != b'\n' {
                break;
            }
            let mut following = end + 1;
            while following < text.len() && matches!(text.as_bytes()[following], b' ' | b'\t') {
                following += 1;
            }
            let separator = if end > 0 && matches!(text.as_bytes()[end - 1], b' ' | b'\t') {
                ""
            } else {
                " "
            };
            text.replace_range(end..following, separator);
            changed = true;
        }
        if changed {
            self.push_undo(snapshot);
            *cursor = at.min(text.len());
        }
        changed
    }

    fn selection_range(&self, text: &str, cursor: usize) -> Option<Selection> {
        let anchor = self.visual_anchor?;
        match self.mode {
            VimMode::Visual => {
                let low = anchor.min(cursor);
                let high = anchor.max(cursor);
                Some(Selection {
                    start: floor_boundary(text, low),
                    end: next_boundary(text, floor_boundary(text, high)),
                    linewise: false,
                })
            }
            VimMode::VisualLine => Some(Selection {
                start: line_start(text, anchor.min(cursor)),
                end: line_range_end(text, anchor.max(cursor), 1),
                linewise: true,
            }),
            _ => None,
        }
    }

    fn leave_visual(&mut self) {
        self.mode = VimMode::Normal;
        self.visual_anchor = None;
        self.pending = Pending::Idle;
        self.count_digits.clear();
    }

    fn set_register(&mut self, value: &str, linewise: bool) {
        let end = floor_boundary(value, value.len().min(MAX_REGISTER_BYTES));
        self.register.clear();
        self.register.push_str(&value[..end]);
        self.register_linewise = linewise;
    }

    fn ensure_insert_undo(&mut self, text: &str, cursor: usize) {
        if self.insert_origin.is_none() {
            self.insert_origin = Some(Snapshot {
                text: text.to_owned(),
                cursor,
            });
            self.insert_undo_pushed = false;
        }
        if !self.insert_undo_pushed {
            if let Some(snapshot) = self.insert_origin.clone() {
                self.push_undo(snapshot);
            }
            self.insert_undo_pushed = true;
        }
    }

    fn finish_insert_recording(&mut self, text: &str, cursor: usize) {
        let changed = self
            .insert_origin
            .as_ref()
            .is_some_and(|origin| origin.text != text);
        if changed && !self.replaying {
            if self.command_keys.first().is_none_or(|key| {
                !matches!(key, VimKey::Char('i' | 'I' | 'a' | 'A' | 'o' | 'O' | 'c'))
            }) {
                self.command_keys.insert(0, VimKey::Char('i'));
            }
            self.last_change = bounded_events(&self.command_keys);
        }
        self.command_keys.clear();
        self.insert_origin = None;
        self.insert_undo_pushed = false;
        let _ = cursor;
    }

    fn push_undo(&mut self, snapshot: Snapshot) {
        if self
            .undo
            .back()
            .is_some_and(|last| last.text == snapshot.text && last.cursor == snapshot.cursor)
        {
            return;
        }
        self.undo_bytes = self.undo_bytes.saturating_add(snapshot.text.len());
        self.undo.push_back(snapshot);
        while self.undo.len() > MAX_UNDO_ENTRIES || self.undo_bytes > MAX_UNDO_BYTES {
            if let Some(removed) = self.undo.pop_front() {
                self.undo_bytes = self.undo_bytes.saturating_sub(removed.text.len());
            } else {
                break;
            }
        }
    }

    fn undo(&mut self, text: &mut String, cursor: &mut usize) -> bool {
        let Some(snapshot) = self.undo.pop_back() else {
            return false;
        };
        self.undo_bytes = self.undo_bytes.saturating_sub(snapshot.text.len());
        *text = snapshot.text;
        *cursor = snapshot.cursor.min(text.len());
        true
    }

    fn dot_repeat(&mut self, text: &mut String, cursor: &mut usize, count: usize) -> bool {
        if self.last_change.is_empty() || self.replaying {
            return false;
        }
        let events = self.last_change.clone();
        let before = text.clone();
        self.replaying = true;
        for _ in 0..count.min(MAX_COUNT) {
            for key in events.iter().cloned() {
                let _ = self.handle_key(text, cursor, key);
            }
        }
        self.replaying = false;
        self.last_change = events;
        before != *text
    }

    fn finish_change(&mut self) {
        if !self.replaying {
            self.last_change = bounded_events(&self.command_keys);
        }
        self.command_keys.clear();
        self.pending = Pending::Idle;
        self.count_digits.clear();
    }

    fn finish_nonchange(&mut self) {
        self.command_keys.clear();
        self.pending = Pending::Idle;
        self.count_digits.clear();
    }

    fn reset_command(&mut self) {
        self.finish_nonchange();
    }

    fn record_key(&mut self, key: VimKey) {
        if self.command_keys.len() < MAX_REPEAT_EVENTS {
            self.command_keys.push(key);
        }
    }

    fn take_count(&mut self) -> usize {
        let count = parse_count(&self.count_digits);
        self.count_digits.clear();
        count
    }

    fn normalize_cursor(&self, text: &str, cursor: &mut usize) {
        *cursor = floor_boundary(text, (*cursor).min(text.len()));
        if self.mode != VimMode::Insert && !text.is_empty() && *cursor == text.len() {
            *cursor = prev_boundary(text, *cursor);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Case {
    Toggle,
    Lower,
    Upper,
}

fn transform_grapheme(grapheme: &str, case: Case) -> String {
    match case {
        Case::Lower => grapheme.chars().flat_map(char::to_lowercase).collect(),
        Case::Upper => grapheme.chars().flat_map(char::to_uppercase).collect(),
        Case::Toggle => grapheme
            .chars()
            .flat_map(|character| {
                if character.is_lowercase() {
                    character.to_uppercase().collect::<Vec<_>>()
                } else {
                    character.to_lowercase().collect::<Vec<_>>()
                }
            })
            .collect(),
    }
}

fn operator(character: char) -> Operator {
    match character {
        'd' => Operator::Delete,
        'c' => Operator::Change,
        'y' => Operator::Yank,
        _ => unreachable!("operator is validated by the parser"),
    }
}

fn operator_key(operator: Operator) -> char {
    match operator {
        Operator::Delete => 'd',
        Operator::Change => 'c',
        Operator::Yank => 'y',
    }
}

fn find_kind(character: char) -> FindKind {
    match character {
        'f' => FindKind::Forward,
        'F' => FindKind::Backward,
        't' => FindKind::TillForward,
        'T' => FindKind::TillBackward,
        _ => unreachable!("find key is validated by the parser"),
    }
}

fn parse_count(digits: &str) -> usize {
    digits.parse::<usize>().unwrap_or(1).clamp(1, MAX_COUNT)
}

fn push_count_digit(digits: &mut String, digit: char) {
    if digits.len() < 5 {
        digits.push(digit);
    }
    if digits.parse::<usize>().unwrap_or(MAX_COUNT) > MAX_COUNT {
        *digits = MAX_COUNT.to_string();
    }
}

fn bounded_events(events: &[VimKey]) -> Vec<VimKey> {
    let mut bytes = 0usize;
    events
        .iter()
        .take(MAX_REPEAT_EVENTS)
        .take_while(|event| {
            bytes = bytes.saturating_add(match event {
                VimKey::Text(value) => value.len(),
                VimKey::Char(character) => character.len_utf8(),
                _ => 1,
            });
            bytes <= MAX_TEXT_BYTES
        })
        .cloned()
        .collect()
}

fn repeat_bounded(value: &str, count: usize, available: usize) -> Option<String> {
    let length = value.len().checked_mul(count)?;
    if length > available || length > MAX_TEXT_BYTES {
        return None;
    }
    Some(value.repeat(count))
}

fn floor_boundary(text: &str, byte: usize) -> usize {
    let byte = byte.min(text.len());
    if text.is_char_boundary(byte) {
        let mut last = 0;
        for (index, _) in text.grapheme_indices(true) {
            if index > byte {
                break;
            }
            last = index;
        }
        if byte == text.len() { byte } else { last }
    } else {
        let mut value = byte;
        while value > 0 && !text.is_char_boundary(value) {
            value -= 1;
        }
        floor_boundary(text, value)
    }
}

fn prev_boundary(text: &str, byte: usize) -> usize {
    let byte = floor_boundary(text, byte);
    text[..byte]
        .grapheme_indices(true)
        .next_back()
        .map_or(0, |(index, _)| index)
}

fn next_boundary(text: &str, byte: usize) -> usize {
    let byte = floor_boundary(text, byte);
    if byte >= text.len() {
        return text.len();
    }
    byte + text[byte..].graphemes(true).next().map_or(0, str::len)
}

fn grapheme_at(text: &str, byte: usize) -> &str {
    let byte = floor_boundary(text, byte);
    text.get(byte..)
        .and_then(|tail| tail.graphemes(true).next())
        .unwrap_or("")
}

fn grapheme_count(text: &str) -> usize {
    text.graphemes(true).count()
}

fn line_start(text: &str, byte: usize) -> usize {
    let byte = floor_boundary(text, byte);
    text[..byte].rfind('\n').map_or(0, |index| index + 1)
}

fn line_end(text: &str, byte: usize) -> usize {
    let byte = floor_boundary(text, byte);
    text[byte..]
        .find('\n')
        .map_or(text.len(), |relative| byte + relative)
}

fn first_nonblank(text: &str, byte: usize) -> usize {
    let start = line_start(text, byte);
    let end = line_end(text, byte);
    let mut current = start;
    while current < end && grapheme_at(text, current).chars().all(char::is_whitespace) {
        current = next_boundary(text, current);
    }
    current
}

fn last_line_start(text: &str) -> usize {
    text.rfind('\n')
        .map_or(0, |index| index + 1)
        .min(text.len())
}

fn nth_line_start(text: &str, line: usize) -> usize {
    if line == 0 {
        return 0;
    }
    let mut current = 0usize;
    for _ in 0..line.min(MAX_COUNT) {
        let Some(relative) = text[current..].find('\n') else {
            return last_line_start(text);
        };
        current += relative + 1;
    }
    current.min(text.len())
}

fn line_range_end(text: &str, byte: usize, count: usize) -> usize {
    let mut end = line_end(text, byte);
    for _ in 1..count.min(MAX_COUNT) {
        if end >= text.len() {
            break;
        }
        end = line_end(text, end + 1);
    }
    if end < text.len() { end + 1 } else { end }
}

fn vertical_motion(text: &str, byte: usize, delta: isize) -> usize {
    let start = line_start(text, byte);
    let column = grapheme_count(&text[start..byte]);
    let target_start = if delta < 0 {
        if start == 0 {
            return byte;
        }
        line_start(text, start - 1)
    } else {
        let end = line_end(text, byte);
        if end >= text.len() {
            return byte;
        }
        end + 1
    };
    let target_end = line_end(text, target_start);
    let mut target = target_start;
    for _ in 0..column {
        let next = next_boundary(text, target);
        if next > target_end || next == target {
            break;
        }
        target = next;
    }
    if target == target_end && target > target_start {
        prev_boundary(text, target)
    } else {
        target
    }
}

fn advance_graphemes_within_line(text: &str, byte: usize, count: usize) -> usize {
    let end = line_end(text, byte);
    let mut current = byte;
    for _ in 0..count.min(MAX_COUNT) {
        let next = next_boundary(text, current);
        if next > end || next == current {
            break;
        }
        current = next;
    }
    current
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WordClass {
    Space,
    Word,
    Punctuation,
}

fn word_class(grapheme: &str) -> WordClass {
    let Some(character) = grapheme.chars().next() else {
        return WordClass::Space;
    };
    if character.is_whitespace() {
        WordClass::Space
    } else if character.is_alphanumeric() || character == '_' {
        WordClass::Word
    } else {
        WordClass::Punctuation
    }
}

fn word_forward(text: &str, byte: usize, count: usize) -> usize {
    let mut current = byte;
    for _ in 0..count.min(MAX_COUNT) {
        if current >= text.len() {
            break;
        }
        let class = word_class(grapheme_at(text, current));
        while current < text.len() && word_class(grapheme_at(text, current)) == class {
            current = next_boundary(text, current);
        }
        while current < text.len() && word_class(grapheme_at(text, current)) == WordClass::Space {
            current = next_boundary(text, current);
        }
    }
    current
}

fn word_backward(text: &str, byte: usize, count: usize) -> usize {
    let mut current = byte;
    for _ in 0..count.min(MAX_COUNT) {
        if current == 0 {
            break;
        }
        current = prev_boundary(text, current);
        while current > 0 && word_class(grapheme_at(text, current)) == WordClass::Space {
            current = prev_boundary(text, current);
        }
        let class = word_class(grapheme_at(text, current));
        while current > 0 {
            let previous = prev_boundary(text, current);
            if word_class(grapheme_at(text, previous)) != class {
                break;
            }
            current = previous;
        }
    }
    current
}

fn word_end(text: &str, byte: usize, count: usize) -> usize {
    let mut current = byte;
    for iteration in 0..count.min(MAX_COUNT) {
        if iteration > 0 && current < text.len() {
            current = next_boundary(text, current);
        }
        while current < text.len() && word_class(grapheme_at(text, current)) == WordClass::Space {
            current = next_boundary(text, current);
        }
        if current >= text.len() {
            return text.len();
        }
        let class = word_class(grapheme_at(text, current));
        loop {
            let next = next_boundary(text, current);
            if next >= text.len() || word_class(grapheme_at(text, next)) != class {
                break;
            }
            current = next;
        }
    }
    current
}

fn big_word_forward(text: &str, byte: usize, count: usize) -> usize {
    let mut current = byte;
    for _ in 0..count.min(MAX_COUNT) {
        while current < text.len() && !grapheme_at(text, current).chars().all(char::is_whitespace) {
            current = next_boundary(text, current);
        }
        while current < text.len() && grapheme_at(text, current).chars().all(char::is_whitespace) {
            current = next_boundary(text, current);
        }
    }
    current
}

fn big_word_backward(text: &str, byte: usize, count: usize) -> usize {
    let mut current = byte;
    for _ in 0..count.min(MAX_COUNT) {
        if current == 0 {
            break;
        }
        current = prev_boundary(text, current);
        while current > 0 && grapheme_at(text, current).chars().all(char::is_whitespace) {
            current = prev_boundary(text, current);
        }
        while current > 0 {
            let previous = prev_boundary(text, current);
            if grapheme_at(text, previous).chars().all(char::is_whitespace) {
                break;
            }
            current = previous;
        }
    }
    current
}

fn big_word_end(text: &str, byte: usize, count: usize) -> usize {
    let mut current = byte;
    for iteration in 0..count.min(MAX_COUNT) {
        if iteration > 0 && current < text.len() {
            current = next_boundary(text, current);
        }
        while current < text.len() && grapheme_at(text, current).chars().all(char::is_whitespace) {
            current = next_boundary(text, current);
        }
        if current >= text.len() {
            return text.len();
        }
        loop {
            let next = next_boundary(text, current);
            if next >= text.len() || grapheme_at(text, next).chars().all(char::is_whitespace) {
                break;
            }
            current = next;
        }
    }
    current
}

fn resolve_motion(text: &str, cursor: usize, key: char, count: usize) -> Option<Motion> {
    let count = count.clamp(1, MAX_COUNT);
    match key {
        'h' => {
            let mut target = cursor;
            for _ in 0..count {
                let previous = prev_boundary(text, target);
                if previous < line_start(text, cursor) {
                    break;
                }
                target = previous;
            }
            Some(Motion::Position {
                target,
                inclusive: false,
            })
        }
        'l' | ' ' => {
            let mut target = cursor;
            let end = line_end(text, cursor);
            for _ in 0..count {
                let next = next_boundary(text, target);
                if next >= end {
                    break;
                }
                target = next;
            }
            Some(Motion::Position {
                target,
                inclusive: false,
            })
        }
        'j' | 'k' => {
            let mut target = cursor;
            for _ in 0..count {
                target = vertical_motion(text, target, if key == 'j' { 1 } else { -1 });
            }
            Some(Motion::Lines {
                start: line_start(text, cursor.min(target)),
                end: line_range_end(text, cursor.max(target), 1),
            })
        }
        'w' => Some(Motion::Position {
            target: word_forward(text, cursor, count),
            inclusive: false,
        }),
        'e' => Some(Motion::Position {
            target: word_end(text, cursor, count),
            inclusive: true,
        }),
        'b' => Some(Motion::Position {
            target: word_backward(text, cursor, count),
            inclusive: false,
        }),
        'W' => Some(Motion::Position {
            target: big_word_forward(text, cursor, count),
            inclusive: false,
        }),
        'E' => Some(Motion::Position {
            target: big_word_end(text, cursor, count),
            inclusive: true,
        }),
        'B' => Some(Motion::Position {
            target: big_word_backward(text, cursor, count),
            inclusive: false,
        }),
        '0' => Some(Motion::Position {
            target: line_start(text, cursor),
            inclusive: false,
        }),
        '^' => Some(Motion::Position {
            target: first_nonblank(text, cursor),
            inclusive: false,
        }),
        '$' => Some(Motion::Position {
            target: line_end(text, cursor),
            inclusive: false,
        }),
        'G' => {
            let target = if count > 1 {
                nth_line_start(text, count - 1)
            } else {
                last_line_start(text)
            };
            Some(Motion::Lines {
                start: line_start(text, cursor.min(target)),
                end: line_range_end(text, cursor.max(target), 1),
            })
        }
        _ => None,
    }
}

fn motion_selection(text: &str, cursor: usize, motion: Motion) -> Option<Selection> {
    match motion {
        Motion::Lines { start, end } => Some(Selection {
            start,
            end,
            linewise: true,
        }),
        Motion::Position { target, inclusive } => {
            let (start, end) = if target >= cursor {
                let end = if inclusive {
                    next_boundary(text, target)
                } else {
                    target
                };
                (cursor, end)
            } else {
                (
                    target,
                    if inclusive {
                        next_boundary(text, cursor)
                    } else {
                        cursor
                    },
                )
            };
            (start < end).then_some(Selection {
                start,
                end,
                linewise: false,
            })
        }
    }
}

fn find_motion(
    text: &str,
    cursor: usize,
    kind: FindKind,
    target: char,
    count: usize,
) -> Option<usize> {
    let start = line_start(text, cursor);
    let end = line_end(text, cursor);
    match kind {
        FindKind::Forward | FindKind::TillForward => {
            let mut current = next_boundary(text, cursor);
            let mut found = None;
            let mut remaining = count.clamp(1, MAX_COUNT);
            while current < end {
                if grapheme_at(text, current).starts_with(target) {
                    remaining -= 1;
                    if remaining == 0 {
                        found = Some(current);
                        break;
                    }
                }
                current = next_boundary(text, current);
            }
            found.map(|position| {
                if kind == FindKind::TillForward {
                    prev_boundary(text, position)
                } else {
                    position
                }
            })
        }
        FindKind::Backward | FindKind::TillBackward => {
            let mut current = cursor;
            let mut found = None;
            let mut remaining = count.clamp(1, MAX_COUNT);
            while current > start {
                current = prev_boundary(text, current);
                if grapheme_at(text, current).starts_with(target) {
                    remaining -= 1;
                    if remaining == 0 {
                        found = Some(current);
                        break;
                    }
                }
            }
            found.map(|position| {
                if kind == FindKind::TillBackward {
                    next_boundary(text, position)
                } else {
                    position
                }
            })
        }
    }
}

fn find_text_object(
    text: &str,
    cursor: usize,
    object: char,
    scope: TextObjectScope,
    count: usize,
) -> Option<Selection> {
    if matches!(object, 'w' | 'W') {
        return word_object(text, cursor, scope, count, object == 'W');
    }
    let (open, close) = match object {
        '"' => ('"', '"'),
        '\'' => ('\'', '\''),
        '`' => ('`', '`'),
        '(' | ')' | 'b' => ('(', ')'),
        '[' | ']' => ('[', ']'),
        '{' | '}' | 'B' => ('{', '}'),
        '<' | '>' => ('<', '>'),
        _ => return None,
    };
    delimiter_object(text, cursor, open, close, scope)
}

fn word_object(
    text: &str,
    cursor: usize,
    scope: TextObjectScope,
    count: usize,
    big: bool,
) -> Option<Selection> {
    if text.is_empty() {
        return None;
    }
    let mut at = cursor.min(prev_boundary(text, text.len()));
    let class = object_word_class(grapheme_at(text, at), big);
    while at > 0 {
        let previous = prev_boundary(text, at);
        if object_word_class(grapheme_at(text, previous), big) != class {
            break;
        }
        at = previous;
    }
    let start = at;
    let mut end = at;
    let count = count.clamp(1, MAX_COUNT);
    for iteration in 0..count {
        let current_class = object_word_class(grapheme_at(text, end), big);
        while end < text.len() && object_word_class(grapheme_at(text, end), big) == current_class {
            end = next_boundary(text, end);
        }
        if end >= text.len() {
            break;
        }
        // Inter-word whitespace belongs to a counted `iw` only when another
        // word follows.  Trailing whitespace is added separately for `aw`.
        if iteration + 1 < count {
            while end < text.len()
                && object_word_class(grapheme_at(text, end), big) == WordClass::Space
            {
                end = next_boundary(text, end);
            }
        }
    }
    let mut around_start = start;
    let mut around_end = end;
    if scope == TextObjectScope::Around {
        if around_end < text.len() {
            while around_end < text.len()
                && object_word_class(grapheme_at(text, around_end), big) == WordClass::Space
            {
                around_end = next_boundary(text, around_end);
            }
        } else {
            while around_start > 0 {
                let previous = prev_boundary(text, around_start);
                if object_word_class(grapheme_at(text, previous), big) != WordClass::Space {
                    break;
                }
                around_start = previous;
            }
        }
    }
    Some(Selection {
        start: around_start,
        end: around_end,
        linewise: false,
    })
}

fn object_word_class(grapheme: &str, big: bool) -> WordClass {
    let class = word_class(grapheme);
    if big && class == WordClass::Punctuation {
        WordClass::Word
    } else {
        class
    }
}

fn delimiter_object(
    text: &str,
    cursor: usize,
    open: char,
    close: char,
    scope: TextObjectScope,
) -> Option<Selection> {
    let (start, end) = if open == close {
        let line_start = line_start(text, cursor);
        let line_end = line_end(text, cursor);
        let positions = text[line_start..line_end]
            .char_indices()
            .filter_map(|(index, character)| (character == open).then_some(line_start + index))
            .collect::<Vec<_>>();
        positions.chunks_exact(2).find_map(|pair| {
            (pair[0] <= cursor && cursor <= pair[1]).then_some((pair[0], pair[1]))
        })?
    } else {
        let mut depth = 0usize;
        let mut start =
            (cursor < text.len() && grapheme_at(text, cursor).starts_with(open)).then_some(cursor);
        if start.is_none() {
            for (index, character) in text[..cursor.min(text.len())].char_indices().rev() {
                if character == close {
                    depth += 1;
                } else if character == open {
                    if depth == 0 {
                        start = Some(index);
                        break;
                    }
                    depth -= 1;
                }
            }
        }
        let start = start?;
        let mut depth = 0usize;
        let mut end = None;
        for (relative, character) in text[start + open.len_utf8()..].char_indices() {
            let index = start + open.len_utf8() + relative;
            if character == open {
                depth += 1;
            } else if character == close {
                if depth == 0 {
                    end = Some(index);
                    break;
                }
                depth -= 1;
            }
        }
        (start, end?)
    };
    Some(Selection {
        start: if scope == TextObjectScope::Inner {
            start + open.len_utf8()
        } else {
            start
        },
        end: if scope == TextObjectScope::Inner {
            end
        } else {
            end + close.len_utf8()
        },
        linewise: false,
    })
}

fn line_starts_in(text: &str, start: usize, end: usize) -> Vec<usize> {
    let mut starts = vec![line_start(text, start)];
    let mut current = starts[0];
    while current < end {
        let line_end = line_end(text, current);
        if line_end >= text.len() || line_end + 1 >= end {
            break;
        }
        current = line_end + 1;
        starts.push(current);
    }
    starts
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Harness {
        vim: VimState,
        text: String,
        cursor: usize,
    }

    impl Harness {
        fn normal(text: &str) -> Self {
            let mut harness = Self {
                vim: VimState::new(),
                text: text.to_owned(),
                cursor: 0,
            };
            harness.key(VimKey::Escape);
            harness
        }

        fn keys(&mut self, keys: &str) {
            for character in keys.chars() {
                self.key(if character == '\u{1b}' {
                    VimKey::Escape
                } else {
                    VimKey::Char(character)
                });
            }
        }

        fn key(&mut self, key: VimKey) -> VimOutcome {
            self.vim.handle_key(&mut self.text, &mut self.cursor, key)
        }
    }

    #[test]
    fn insert_entry_commands_and_graphemes_are_safe() {
        let mut h = Harness::normal("a👩🏽‍💻b\n  tail");
        h.keys("lxiZ\u{1b}");
        assert_eq!(h.text, "aZb\n  tail");
        assert!(h.text.is_char_boundary(h.cursor));
        h.keys("GA!\u{1b}");
        assert_eq!(h.text, "aZb\n  tail!");
        h.keys("Otop\u{1b}");
        assert_eq!(h.text, "aZb\ntop\n  tail!");
    }

    #[test]
    fn motions_counts_find_and_repeat_find_work() {
        let mut h = Harness::normal("one two three two\nlast");
        h.keys("2w");
        assert_eq!(&h.text[h.cursor..], "three two\nlast");
        h.keys("ft");
        assert_eq!(grapheme_at(&h.text, h.cursor), "t");
        h.keys(";");
        assert!(h.cursor > 8);
        h.keys(",");
        assert_eq!(grapheme_at(&h.text, h.cursor), "t");
        h.keys("ggG");
        assert_eq!(h.cursor, h.text.rfind('\n').unwrap() + 1);
        h.keys("gggjgk");
        assert_eq!(h.cursor, 0);
    }

    #[test]
    fn uppercase_word_motions_and_text_objects_use_whitespace_boundaries() {
        let mut h = Harness::normal("one,two three");
        h.keys("W");
        assert_eq!(&h.text[h.cursor..], "three");
        h.keys("B");
        assert_eq!(h.cursor, 0);
        h.keys("E");
        assert_eq!(grapheme_at(&h.text, h.cursor), "o");

        let mut object = Harness::normal("one,two three");
        object.keys("diW");
        assert_eq!(object.text, " three");
        assert_eq!(object.vim.register().0, "one,two");
    }

    #[test]
    fn operators_text_objects_and_registers_work() {
        let mut h = Harness::normal("alpha beta gamma\nsecond\nthird");
        h.keys("dw");
        assert_eq!(h.text, "beta gamma\nsecond\nthird");
        assert_eq!(h.vim.register().0, "alpha ");
        h.keys("cwX\u{1b}");
        assert_eq!(h.text, "X gamma\nsecond\nthird");
        h.keys("0yiw");
        assert_eq!(h.vim.register().0, "X");
        h.keys("jdd");
        assert_eq!(h.text, "X gamma\nthird");
        assert!(h.vim.register().1);

        let mut quoted = Harness::normal("call(\"hello world\")");
        quoted.keys("fhdi\"");
        assert_eq!(quoted.text, "call(\"\")");
        let mut bracketed = Harness::normal("a [one two] z");
        bracketed.keys("foda[");
        assert_eq!(bracketed.text, "a  z");
    }

    #[test]
    fn paste_undo_and_dot_repeat_are_grouped() {
        let mut h = Harness::normal("one two");
        h.keys("dwP");
        assert_eq!(h.text, "one two");
        h.keys("x.");
        assert_eq!(h.text, "e two");
        h.keys("u");
        assert_eq!(h.text, "ne two");
        h.keys("u");
        assert_eq!(h.text, "one two");

        let mut insertion = Harness::normal("ab");
        insertion.keys("iX\u{1b}l.");
        assert_eq!(insertion.text, "XXab");
    }

    #[test]
    fn normal_escape_motion_keys_and_question_match_reference_semantics() {
        let mut middle = Harness::normal("abc");
        middle.cursor = 2;
        middle.vim.mode = VimMode::Insert;
        middle.key(VimKey::Escape);
        assert_eq!(middle.cursor, 1);

        let mut operator_arrow = Harness::normal("abc");
        operator_arrow.keys("d");
        operator_arrow.key(VimKey::Right);
        assert_eq!(operator_arrow.text, "bc");

        let mut delete = Harness::normal("abc");
        delete.key(VimKey::Delete);
        assert_eq!(delete.text, "bc");

        let mut question = Harness::normal("old input");
        question.keys("?");
        assert_eq!(question.text, "?");
        assert_eq!(question.cursor, 0);
    }

    #[test]
    fn visual_character_and_line_operations_work() {
        let mut h = Harness::normal("abc def\nsecond\nthird");
        h.keys("vwd");
        // Characterwise `vw` includes the first grapheme of the next word,
        // matching Vim's inclusive visual endpoint.
        assert_eq!(h.text, "ef\nsecond\nthird");
        h.keys("Vjy");
        assert!(h.vim.register().1);
        h.keys("Gp");
        assert!(h.text.contains("second\nthird"));

        let mut replace = Harness::normal("abc def");
        replace.keys("vllrZ");
        assert_eq!(replace.text, "ZZZ def");
        replace.keys("vllU");
        assert_eq!(replace.text, "ZZZ def");
        replace.keys("vll~");
        assert_eq!(replace.text, "zzz def");

        let mut aliases = Harness::normal("one two\nthree");
        aliases.keys("vwX");
        assert_eq!(aliases.text, "three");
        aliases.keys("uVjY");
        assert!(aliases.vim.register().1);

        let mut substitute = Harness::normal("abc def");
        substitute.keys("vllsX\u{1b}");
        assert_eq!(substitute.text, "X def");
    }

    #[test]
    fn visual_indent_join_change_and_paste_work() {
        let mut h = Harness::normal("one\ntwo\nthree");
        h.keys("Vj>");
        assert_eq!(h.text, "    one\n    two\nthree");
        h.keys("Vj<");
        assert_eq!(h.text, "one\ntwo\nthree");
        h.keys("VjJ");
        assert_eq!(h.text, "one two\nthree");

        let mut change = Harness::normal("old tail");
        change.keys("viwcnew\u{1b}");
        assert_eq!(change.text, "new tail");
        change.keys("0yiwwviwp");
        assert_eq!(change.text, "new new");
    }

    #[test]
    fn limits_are_fail_closed() {
        let mut h = Harness {
            vim: VimState::new(),
            text: "x".repeat(MAX_TEXT_BYTES),
            cursor: MAX_TEXT_BYTES,
        };
        let outcome = h.key(VimKey::Char('x'));
        assert_eq!(outcome.action, Some(VimAction::LimitReached));
        assert_eq!(h.text.len(), MAX_TEXT_BYTES);

        let mut count = Harness::normal("abcdef");
        count.keys("999999x");
        assert!(count.text.is_empty());
        assert!(count.vim.undo.len() <= MAX_UNDO_ENTRIES);
        assert!(count.vim.undo_bytes <= MAX_UNDO_BYTES);
    }

    #[test]
    fn line_operators_changes_indents_and_find_operators_work() {
        let mut lines = Harness::normal("one\ntwo\nthree\nfour");
        lines.keys("2dd");
        assert_eq!(lines.text, "three\nfour");
        lines.keys("yyp");
        assert_eq!(lines.text, "three\nthree\nfour");
        lines.keys(">><<");
        assert_eq!(lines.text, "three\nthree\nfour");

        let mut changes = Harness::normal("abc def");
        changes.keys("wD");
        assert_eq!(changes.text, "abc ");
        changes.keys("u0wCXY\u{1b}");
        assert_eq!(changes.text, "abc XY");

        let mut find = Harness::normal("abcXdefXghi");
        find.keys("dfX");
        assert_eq!(find.text, "defXghi");
        find.keys("$dFX");
        assert_eq!(find.text, "def");
    }

    #[test]
    fn visual_find_gg_g_and_multiline_replace_work() {
        let mut find = Harness::normal("a-b-c");
        find.keys("vfcd");
        assert!(find.text.is_empty());

        let mut to_bottom = Harness::normal("one\ntwo\nthree");
        to_bottom.keys("jVGd");
        assert_eq!(to_bottom.text, "one\n");

        let mut to_top = Harness::normal("one\ntwo\nthree");
        to_top.keys("jVggd");
        assert_eq!(to_top.text, "three");

        let mut replace = Harness::normal("ab\ncd");
        replace.keys("VjrX");
        assert_eq!(replace.text, "XX\nXX");
    }

    #[test]
    fn linewise_paste_at_final_line_and_event_passthrough_are_safe() {
        let mut h = Harness::normal("one\ntwo");
        h.keys("yyGp");
        assert_eq!(h.text, "one\ntwo\none\n");

        let before = h.text.clone();
        let outcome = h.vim.handle_event(
            &mut h.text,
            &mut h.cursor,
            VimEvent {
                key: VimKey::Char('c'),
                control: true,
                alt: false,
                shift: false,
            },
        );
        assert!(!outcome.handled);
        assert_eq!(h.text, before);

        let slash = h.key(VimKey::Char('/'));
        assert!(!slash.handled);
        assert_eq!(h.text, before);
    }
}

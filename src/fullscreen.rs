//! Provider-neutral fullscreen conversation state and frame rendering.
//!
//! This module deliberately owns no terminal mode.  In particular it never
//! enters or leaves the alternate screen and never performs I/O.  A frontend
//! can feed it transcript/streaming events and input gestures, then write the
//! ANSI frame returned by [`FullscreenState::render_ansi`] using its own
//! terminal lifecycle guard.

use std::{collections::VecDeque, time::Duration};

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

const ANSI_HOME: &str = "\x1b[H";
const ANSI_CLEAR_LINE: &str = "\x1b[2K";
const ANSI_RESET: &str = "\x1b[0m";
const ANSI_HEADER: &str = "\x1b[1;36m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_SELECTION: &str = "\x1b[7m";
const ANSI_PILL: &str = "\x1b[1;30;46m";

/// Resource limits for the in-memory transcript and copied selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FullscreenLimits {
    pub max_transcript_bytes: usize,
    pub max_transcript_lines: usize,
    pub max_line_bytes: usize,
    pub max_selection_bytes: usize,
}

impl Default for FullscreenLimits {
    fn default() -> Self {
        Self {
            max_transcript_bytes: 8 * 1024 * 1024,
            max_transcript_lines: 50_000,
            max_line_bytes: 8 * 1024 * 1024,
            max_selection_bytes: 1024 * 1024,
        }
    }
}

impl FullscreenLimits {
    fn normalized(self) -> Self {
        Self {
            max_transcript_bytes: self.max_transcript_bytes.max(1),
            max_transcript_lines: self.max_transcript_lines.max(1),
            max_line_bytes: self.max_line_bytes.max(1),
            max_selection_bytes: self.max_selection_bytes.max(1),
        }
    }
}

/// Mouse-wheel direction in content coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WheelDirection {
    Up,
    Down,
}

/// Acceleration curve selected by the terminal integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WheelProfile {
    /// Native terminals normally emit multiple SGR events for a precise wheel.
    Native,
    /// Browser-backed xterm.js terminals normally emit fewer events per notch.
    XtermJs,
}

/// Deterministic wheel acceleration configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WheelConfig {
    pub profile: WheelProfile,
    pub base_rows: f32,
    pub max_rows: f32,
    pub acceleration_window: Duration,
}

impl Default for WheelConfig {
    fn default() -> Self {
        Self {
            profile: WheelProfile::Native,
            base_rows: 1.0,
            max_rows: 6.0,
            acceleration_window: Duration::from_millis(40),
        }
    }
}

impl WheelConfig {
    fn normalized(self) -> Self {
        let base_rows = if self.base_rows.is_finite() {
            self.base_rows.clamp(1.0, 20.0)
        } else {
            1.0
        };
        let max_rows = if self.max_rows.is_finite() {
            self.max_rows.clamp(base_rows, 40.0)
        } else {
            base_rows.max(6.0)
        };
        Self {
            profile: self.profile,
            base_rows,
            max_rows,
            acceleration_window: self.acceleration_window.max(Duration::from_millis(1)),
        }
    }
}

/// Stateful, timestamp-driven wheel accelerator.  Timestamps are supplied by
/// the caller so tests and replay remain deterministic.
#[derive(Debug, Clone)]
pub struct WheelAccelerator {
    config: WheelConfig,
    last_at: Option<Duration>,
    last_direction: Option<WheelDirection>,
    multiplier: f32,
    fraction: f32,
}

impl WheelAccelerator {
    pub fn new(config: WheelConfig) -> Self {
        let config = config.normalized();
        Self {
            multiplier: config.base_rows,
            config,
            last_at: None,
            last_direction: None,
            fraction: 0.0,
        }
    }

    pub fn config(&self) -> WheelConfig {
        self.config
    }

    pub fn reset(&mut self) {
        self.last_at = None;
        self.last_direction = None;
        self.multiplier = self.config.base_rows;
        self.fraction = 0.0;
    }

    /// Returns the number of visual rows to scroll for one wheel event.
    pub fn step(&mut self, direction: WheelDirection, at: Duration) -> usize {
        let same_direction = self.last_direction == Some(direction);
        let gap = self.last_at.map(|last| at.saturating_sub(last));
        match self.config.profile {
            WheelProfile::Native => {
                if same_direction && gap.is_some_and(|gap| gap <= self.config.acceleration_window) {
                    self.multiplier = (self.multiplier + 0.3).min(self.config.max_rows);
                } else {
                    self.multiplier = self.config.base_rows;
                    self.fraction = 0.0;
                }
            }
            WheelProfile::XtermJs => {
                if same_direction && gap.is_some_and(|gap| gap <= Duration::from_millis(500)) {
                    let gap_ms = gap.unwrap_or_default().as_secs_f32() * 1_000.0;
                    let momentum = 0.5_f32.powf(gap_ms / 150.0);
                    self.multiplier = (1.0 + (self.multiplier - 1.0) * momentum + 5.0 * momentum)
                        .min(self.config.max_rows);
                } else {
                    self.multiplier = self.config.base_rows.max(2.0);
                    self.fraction = 0.0;
                }
            }
        }
        self.last_at = Some(at);
        self.last_direction = Some(direction);
        let total = self.multiplier + self.fraction;
        let rows = total.floor().max(1.0) as usize;
        self.fraction = total - rows as f32;
        rows
    }
}

impl Default for WheelAccelerator {
    fn default() -> Self {
        Self::new(WheelConfig::default())
    }
}

/// A point in the scroll viewport.  Row zero is the first content row below
/// the fixed header; it does not include the header, status, or composer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewportPoint {
    pub row: usize,
    pub column: usize,
}

/// Selection gesture initiated by a mouse click.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClickKind {
    Single,
    Double,
    Triple,
}

/// Keyboard focus movement for extending an existing transcript selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionFocusMove {
    Left,
    Right,
    Up,
    Down,
    LineStart,
    LineEnd,
}

/// A complete ANSI frame.  `content_rows` excludes header/status/composer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnsiFrame {
    pub bytes: String,
    pub rows: usize,
    pub columns: usize,
    pub content_rows: usize,
    pub scroll_top: usize,
    pub scroll_max: usize,
}

/// Borrowed content rendered into fixed regions around the virtual viewport.
#[derive(Debug, Clone, Copy)]
pub struct FrameSpec<'a> {
    pub header: &'a str,
    pub composer: &'a [&'a str],
}

impl<'a> FrameSpec<'a> {
    pub fn new(header: &'a str, composer: &'a [&'a str]) -> Self {
        Self { header, composer }
    }
}

#[derive(Debug, Clone)]
struct TranscriptLine {
    id: u64,
    text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TextPoint {
    line_id: u64,
    byte: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SelectionSpan {
    anchor: TextPoint,
    focus: Option<TextPoint>,
    focus_row: Option<VisualLocation>,
    dragging: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VisualLocation {
    line_id: u64,
    logical_start: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceKey {
    line_id: u64,
}

#[derive(Debug, Clone)]
struct VisualRow {
    source: SourceKey,
    logical_start: usize,
    logical_end: usize,
    text: String,
}

/// Provider-neutral fullscreen transcript, viewport, and selection state.
#[derive(Debug, Clone)]
pub struct FullscreenState {
    limits: FullscreenLimits,
    lines: VecDeque<TranscriptLine>,
    visual_cache: Vec<VisualRow>,
    transcript_bytes: usize,
    dropped_lines: usize,
    next_line_id: u64,
    streaming: Option<TranscriptLine>,
    streaming_unseen_counted: bool,
    status: Option<String>,
    rows: usize,
    columns: usize,
    composer_reserve: usize,
    scroll_top: usize,
    sticky_bottom: bool,
    unseen_messages: usize,
    selection: Option<SelectionSpan>,
    wheel: WheelAccelerator,
}

impl FullscreenState {
    pub fn new(rows: u16, columns: u16, composer_reserve: u16, limits: FullscreenLimits) -> Self {
        Self {
            limits: limits.normalized(),
            lines: VecDeque::new(),
            visual_cache: Vec::new(),
            transcript_bytes: 0,
            dropped_lines: 0,
            next_line_id: 1,
            streaming: None,
            streaming_unseen_counted: false,
            status: None,
            rows: usize::from(rows).max(1),
            columns: usize::from(columns).max(1),
            composer_reserve: usize::from(composer_reserve),
            scroll_top: 0,
            sticky_bottom: true,
            unseen_messages: 0,
            selection: None,
            wheel: WheelAccelerator::default(),
        }
    }

    pub fn limits(&self) -> FullscreenLimits {
        self.limits
    }

    pub fn dimensions(&self) -> (usize, usize) {
        (self.rows, self.columns)
    }

    pub fn transcript_len(&self) -> usize {
        self.lines.len()
    }

    pub fn transcript_bytes(&self) -> usize {
        self.transcript_bytes
    }

    pub fn dropped_lines(&self) -> usize {
        self.dropped_lines
    }

    pub fn is_sticky_bottom(&self) -> bool {
        self.sticky_bottom
    }

    pub fn unseen_messages(&self) -> usize {
        self.unseen_messages
    }

    pub fn scroll_top(&self) -> usize {
        self.scroll_top
    }

    pub fn content_rows(&self) -> usize {
        self.layout().content_rows
    }

    pub fn has_selection(&self) -> bool {
        self.selection
            .is_some_and(|selection| selection.focus.is_some())
    }

    pub fn status(&self) -> Option<&str> {
        self.status.as_deref()
    }

    pub fn streaming_line(&self) -> Option<&str> {
        self.streaming.as_ref().map(|line| line.text.as_str())
    }

    pub fn set_wheel_config(&mut self, config: WheelConfig) {
        self.wheel = WheelAccelerator::new(config);
    }

    pub fn set_status(&mut self, status: Option<&str>) {
        self.status = status.map(|value| bounded_plain_text(value, self.limits.max_line_bytes));
        self.reconcile_scroll();
    }

    /// Appends one user-visible message.  Newline-separated logical lines are
    /// bounded independently, while unseen count increments once per message.
    pub fn push_message(&mut self, text: &str) {
        let anchor = self.top_anchor();
        let was_sticky = self.sticky_bottom;
        let mut added = 0usize;
        for line in split_logical_lines(text) {
            self.push_line_inner(line);
            added += 1;
        }
        if added == 0 {
            self.push_line_inner("");
        }
        self.trim_to_limits();
        self.rebuild_visual_cache();
        if was_sticky {
            self.scroll_to_bottom();
        } else {
            self.restore_top_anchor(anchor);
            self.unseen_messages = self.unseen_messages.saturating_add(1);
        }
        self.discard_stale_selection();
    }

    /// Updates the active streaming line without committing it to transcript
    /// storage.  A stream counts as one unseen message while scrolled away.
    pub fn set_streaming_line(&mut self, line: Option<&str>) {
        let anchor = self.top_anchor();
        let was_sticky = self.sticky_bottom;
        match line {
            Some(line) => {
                let text = bounded_plain_text(
                    line,
                    self.limits
                        .max_line_bytes
                        .min(self.limits.max_transcript_bytes),
                );
                if let Some(streaming) = self.streaming.as_mut() {
                    streaming.text = text;
                } else {
                    let id = self.take_line_id();
                    self.streaming = Some(TranscriptLine { id, text });
                }
                if !was_sticky && !self.streaming_unseen_counted {
                    self.unseen_messages = self.unseen_messages.saturating_add(1);
                    self.streaming_unseen_counted = true;
                }
            }
            None => {
                self.streaming = None;
                self.streaming_unseen_counted = false;
            }
        }
        self.rebuild_visual_cache();
        if was_sticky {
            self.scroll_to_bottom();
        } else {
            self.restore_top_anchor(anchor);
        }
    }

    /// Commits the current streaming line without double-counting unseen work.
    /// Returns false if no stream was active.
    pub fn commit_streaming_line(&mut self) -> bool {
        let Some(streaming) = self.streaming.take() else {
            return false;
        };
        let anchor = self.top_anchor();
        let was_sticky = self.sticky_bottom;
        self.transcript_bytes = self
            .transcript_bytes
            .saturating_add(transcript_line_cost(&streaming.text));
        self.lines.push_back(streaming);
        self.trim_to_limits();
        self.rebuild_visual_cache();
        self.streaming_unseen_counted = false;
        if was_sticky {
            self.scroll_to_bottom();
        } else {
            self.restore_top_anchor(anchor);
        }
        self.discard_stale_selection();
        true
    }

    pub fn clear(&mut self) {
        self.lines.clear();
        self.visual_cache.clear();
        self.streaming = None;
        self.transcript_bytes = 0;
        self.scroll_top = 0;
        self.sticky_bottom = true;
        self.unseen_messages = 0;
        self.streaming_unseen_counted = false;
        self.selection = None;
    }

    /// Resize while preserving the first visible logical grapheme when the
    /// user is scrolled away.  Sticky views remain pinned to the new bottom.
    pub fn resize(&mut self, rows: u16, columns: u16) {
        let anchor = self.top_anchor();
        let was_sticky = self.sticky_bottom;
        self.rows = usize::from(rows).max(1);
        self.columns = usize::from(columns).max(1);
        self.rebuild_visual_cache();
        if was_sticky {
            self.scroll_to_bottom();
        } else {
            self.restore_top_anchor(anchor);
        }
    }

    pub fn set_composer_reserve(&mut self, rows: u16) {
        let anchor = self.top_anchor();
        let was_sticky = self.sticky_bottom;
        self.composer_reserve = usize::from(rows);
        if was_sticky {
            self.scroll_to_bottom();
        } else {
            self.restore_top_anchor(anchor);
        }
    }

    pub fn scroll_lines(&mut self, delta: isize) {
        let max = self.scroll_max();
        let next = if delta < 0 {
            self.scroll_top.saturating_sub(delta.unsigned_abs())
        } else {
            self.scroll_top.saturating_add(delta as usize).min(max)
        };
        self.scroll_top = next.min(max);
        if self.scroll_top >= max {
            self.scroll_to_bottom();
        } else {
            self.sticky_bottom = false;
        }
    }

    pub fn scroll_half_page(&mut self, direction: WheelDirection) {
        let amount = self.content_rows().saturating_div(2).max(1);
        self.scroll_direction(direction, amount);
    }

    pub fn scroll_page(&mut self, direction: WheelDirection) {
        let amount = self.content_rows().max(1);
        self.scroll_direction(direction, amount);
    }

    pub fn scroll_to_top(&mut self) {
        self.scroll_top = 0;
        self.sticky_bottom = self.scroll_max() == 0;
        if self.sticky_bottom {
            self.unseen_messages = 0;
            self.streaming_unseen_counted = false;
        }
    }

    pub fn scroll_to_bottom(&mut self) {
        self.scroll_top = self.scroll_max();
        self.sticky_bottom = true;
        self.unseen_messages = 0;
        self.streaming_unseen_counted = false;
    }

    /// Applies one accelerated wheel event and returns the row step used.
    pub fn wheel(&mut self, direction: WheelDirection, at: Duration) -> usize {
        let rows = self.wheel.step(direction, at);
        self.scroll_direction(direction, rows);
        rows
    }

    /// Starts a single/double/triple click selection in visible content.
    /// Returns false when the point is outside selectable transcript content.
    pub fn click(&mut self, point: ViewportPoint, kind: ClickKind) -> bool {
        let Some(hit) = self.hit_test(point) else {
            self.selection = None;
            return false;
        };
        match kind {
            ClickKind::Single => {
                self.selection = Some(SelectionSpan {
                    anchor: hit.start,
                    focus: None,
                    focus_row: Some(hit.visual),
                    dragging: true,
                });
            }
            ClickKind::Double => {
                let Some((start, end)) = self.word_span(hit) else {
                    return false;
                };
                self.selection = Some(SelectionSpan {
                    anchor: start,
                    focus: Some(end),
                    focus_row: Some(hit.visual),
                    dragging: true,
                });
            }
            ClickKind::Triple => {
                self.selection = Some(SelectionSpan {
                    anchor: hit.row_start,
                    focus: Some(hit.row_end),
                    focus_row: Some(hit.visual),
                    dragging: true,
                });
            }
        }
        true
    }

    /// Extends the current selection.  A single click becomes a selection only
    /// after actual drag motion, matching native terminal behavior.
    pub fn drag_to(&mut self, point: ViewportPoint) -> bool {
        let Some(hit) = self.hit_test(point) else {
            return false;
        };
        let Some(selection) = self.selection.as_mut() else {
            return false;
        };
        if !selection.dragging {
            return false;
        }
        if selection.focus.is_none() && hit.start == selection.anchor {
            return false;
        }
        selection.focus = Some(hit.end);
        selection.focus_row = Some(hit.visual);
        true
    }

    pub fn finish_selection(&mut self) {
        if let Some(selection) = self.selection.as_mut() {
            selection.dragging = false;
            if selection.focus.is_none() {
                self.selection = None;
            }
        }
    }

    pub fn clear_selection(&mut self) {
        self.selection = None;
    }

    /// Extends an existing selection from its focus while keeping the anchor
    /// fixed.  Movement is constrained to the visible transcript viewport,
    /// matching the alternate-screen keyboard selection semantics.
    pub fn move_selection_focus(&mut self, movement: SelectionFocusMove) -> bool {
        let Some(selection) = self.selection else {
            return false;
        };
        let Some(focus) = selection.focus else {
            return false;
        };
        let Some((row_index, row)) = self.visual_row_for_focus(focus, selection.focus_row) else {
            return false;
        };
        let visible_start = self.scroll_top.min(self.scroll_max());
        let visible_end = visible_start
            .saturating_add(self.layout().content_rows)
            .min(self.visual_cache.len());
        if row_index < visible_start || row_index >= visible_end {
            return false;
        }

        let column = self.visual_column_for_point(row, focus);
        let (next, next_row) = match movement {
            SelectionFocusMove::Left => {
                let Some(next) = self.previous_text_point(focus) else {
                    return false;
                };
                let Some((index, row)) = self.visual_row_for_direction(next, false) else {
                    return false;
                };
                if index < visible_start || index >= visible_end {
                    return false;
                }
                (next, row_location(row))
            }
            SelectionFocusMove::Right => {
                let Some(next) = self.next_text_point(focus) else {
                    return false;
                };
                let Some((index, row)) = self.visual_row_for_direction(next, true) else {
                    return false;
                };
                if index < visible_start || index >= visible_end {
                    return false;
                }
                (next, row_location(row))
            }
            SelectionFocusMove::Up | SelectionFocusMove::Down => {
                let target = if movement == SelectionFocusMove::Up {
                    row_index.checked_sub(1)
                } else {
                    row_index.checked_add(1)
                };
                let Some(target) =
                    target.filter(|target| *target >= visible_start && *target < visible_end)
                else {
                    return false;
                };
                let row = &self.visual_cache[target];
                (self.point_at_visual_column(row, column), row_location(row))
            }
            SelectionFocusMove::LineStart => (
                TextPoint {
                    line_id: row.source.line_id,
                    byte: row.logical_start,
                },
                row_location(row),
            ),
            SelectionFocusMove::LineEnd => (
                TextPoint {
                    line_id: row.source.line_id,
                    byte: row.logical_end,
                },
                row_location(row),
            ),
        };
        if next == focus {
            return false;
        }
        if let Some(selection) = self.selection.as_mut() {
            selection.focus = Some(next);
            selection.focus_row = Some(next_row);
            selection.dragging = false;
        }
        true
    }

    /// Copies the selected logical text.  Soft-wrapped visual rows are joined,
    /// wide grapheme spacer cells never appear, and hard lines use `\n`.
    pub fn selected_text(&self) -> Option<String> {
        let (start, end) = self.selection_bounds()?;
        let lines = self.all_logical_lines();
        let mut output = String::new();
        let mut inside = false;
        for line in lines {
            if line.id == start.line_id {
                inside = true;
            }
            if !inside {
                continue;
            }
            let from = if line.id == start.line_id {
                start.byte.min(line.text.len())
            } else {
                0
            };
            let to = if line.id == end.line_id {
                end.byte.min(line.text.len())
            } else {
                line.text.len()
            };
            if from <= to && line.text.is_char_boundary(from) && line.text.is_char_boundary(to) {
                push_bounded(
                    &mut output,
                    &line.text[from..to],
                    self.limits.max_selection_bytes,
                );
            }
            if line.id == end.line_id || output.len() >= self.limits.max_selection_bytes {
                break;
            }
            push_bounded(&mut output, "\n", self.limits.max_selection_bytes);
        }
        (!output.is_empty()).then_some(output)
    }

    /// Renders a complete fixed-size ANSI frame.  It starts at cursor home and
    /// clears each owned row, but intentionally contains no alternate-screen,
    /// mouse-mode, synchronized-output, or cursor-visibility sequence.
    pub fn render_ansi(&self, spec: FrameSpec<'_>) -> AnsiFrame {
        let layout = self.layout();
        let visual = self.visual_rows();
        let max = visual.len().saturating_sub(layout.content_rows);
        let top = if self.sticky_bottom {
            max
        } else {
            self.scroll_top.min(max)
        };
        let mut rendered_rows = Vec::with_capacity(self.rows);
        rendered_rows.push(styled_plain_row(spec.header, self.columns, ANSI_HEADER));

        for offset in 0..layout.content_rows {
            let row = visual.get(top + offset);
            rendered_rows.push(row.map_or_else(
                || " ".repeat(self.columns),
                |row| self.render_visual_row(row),
            ));
        }

        if self.unseen_messages > 0 && layout.content_rows > 0 {
            let pill = format!(
                "{} new message{} · End to jump to bottom",
                self.unseen_messages,
                if self.unseen_messages == 1 { "" } else { "s" }
            );
            let index = layout.content_rows;
            rendered_rows[index] = centered_styled_row(&pill, self.columns, ANSI_PILL);
        }

        if layout.status_rows == 1 {
            let status = self.status.as_deref().unwrap_or_default();
            rendered_rows.push(styled_plain_row(status, self.columns, ANSI_DIM));
        }

        for index in 0..layout.composer_rows {
            let text = spec.composer.get(index).copied().unwrap_or_default();
            rendered_rows.push(plain_row(text, self.columns));
        }
        while rendered_rows.len() < self.rows {
            rendered_rows.push(" ".repeat(self.columns));
        }
        rendered_rows.truncate(self.rows);

        let mut bytes = String::new();
        bytes.push_str(ANSI_HOME);
        for (index, row) in rendered_rows.iter().enumerate() {
            bytes.push_str(ANSI_CLEAR_LINE);
            bytes.push_str(row);
            bytes.push_str(ANSI_RESET);
            if index + 1 < rendered_rows.len() {
                bytes.push_str("\r\n");
            }
        }
        AnsiFrame {
            bytes,
            rows: self.rows,
            columns: self.columns,
            content_rows: layout.content_rows,
            scroll_top: top,
            scroll_max: max,
        }
    }

    fn scroll_direction(&mut self, direction: WheelDirection, amount: usize) {
        let delta = match direction {
            WheelDirection::Up => -(amount.min(isize::MAX as usize) as isize),
            WheelDirection::Down => amount.min(isize::MAX as usize) as isize,
        };
        self.scroll_lines(delta);
    }

    fn push_line_inner(&mut self, line: &str) {
        let text = bounded_plain_text(
            line,
            self.limits
                .max_line_bytes
                .min(self.limits.max_transcript_bytes),
        );
        let id = self.take_line_id();
        self.transcript_bytes = self
            .transcript_bytes
            .saturating_add(transcript_line_cost(&text));
        self.lines.push_back(TranscriptLine { id, text });
    }

    fn take_line_id(&mut self) -> u64 {
        let id = self.next_line_id;
        self.next_line_id = self.next_line_id.saturating_add(1);
        id
    }

    fn trim_to_limits(&mut self) {
        while self.lines.len() > self.limits.max_transcript_lines
            || self.transcript_bytes > self.limits.max_transcript_bytes
        {
            let Some(line) = self.lines.pop_front() else {
                break;
            };
            self.transcript_bytes = self
                .transcript_bytes
                .saturating_sub(transcript_line_cost(&line.text));
            self.dropped_lines = self.dropped_lines.saturating_add(1);
        }
    }

    fn discard_stale_selection(&mut self) {
        let Some(selection) = self.selection else {
            return;
        };
        let anchor_live = self.line_by_id(selection.anchor.line_id).is_some();
        let focus_live = selection
            .focus
            .is_none_or(|focus| self.line_by_id(focus.line_id).is_some());
        if !anchor_live || !focus_live {
            self.selection = None;
        }
    }

    fn layout(&self) -> Layout {
        let header_rows = 1usize.min(self.rows);
        let composer_rows = self
            .composer_reserve
            .min(self.rows.saturating_sub(header_rows));
        let status_rows = usize::from(self.status.is_some()).min(
            self.rows
                .saturating_sub(header_rows)
                .saturating_sub(composer_rows),
        );
        let content_rows = self
            .rows
            .saturating_sub(header_rows + status_rows + composer_rows);
        Layout {
            content_rows,
            status_rows,
            composer_rows,
        }
    }

    fn rebuild_visual_cache(&mut self) {
        let mut output = Vec::new();
        for line in self.all_logical_lines() {
            wrap_line(line, self.columns, &mut output);
        }
        self.visual_cache = output;
    }

    fn visual_rows(&self) -> &[VisualRow] {
        &self.visual_cache
    }

    fn all_logical_lines(&self) -> Vec<&TranscriptLine> {
        self.lines.iter().chain(self.streaming.iter()).collect()
    }

    fn scroll_max(&self) -> usize {
        self.visual_cache
            .len()
            .saturating_sub(self.layout().content_rows)
    }

    fn reconcile_scroll(&mut self) {
        if self.sticky_bottom {
            self.scroll_to_bottom();
        } else {
            self.scroll_top = self.scroll_top.min(self.scroll_max());
            if self.scroll_top == self.scroll_max() {
                self.scroll_to_bottom();
            }
        }
    }

    fn top_anchor(&self) -> Option<TextPoint> {
        let visual = self.visual_rows();
        visual.get(self.scroll_top).map(|row| TextPoint {
            line_id: row.source.line_id,
            byte: row.logical_start,
        })
    }

    fn restore_top_anchor(&mut self, anchor: Option<TextPoint>) {
        let visual = self.visual_rows();
        self.scroll_top = anchor
            .and_then(|anchor| {
                visual.iter().position(|row| {
                    row.source.line_id == anchor.line_id
                        && anchor.byte >= row.logical_start
                        && anchor.byte <= row.logical_end
                })
            })
            .unwrap_or(0)
            .min(visual.len().saturating_sub(self.layout().content_rows));
        self.sticky_bottom = false;
    }

    fn line_by_id(&self, id: u64) -> Option<&TranscriptLine> {
        self.lines
            .iter()
            .chain(self.streaming.iter())
            .find(|line| line.id == id)
    }

    fn hit_test(&self, point: ViewportPoint) -> Option<Hit> {
        if point.row >= self.layout().content_rows {
            return None;
        }
        let visual = self.visual_rows();
        let row = visual.get(self.scroll_top.min(self.scroll_max()) + point.row)?;
        let line = self.line_by_id(row.source.line_id)?;
        let substituted = row.text.as_str() != &line.text[row.logical_start..row.logical_end];
        let mut width = 0usize;
        for (offset, grapheme) in row.text.grapheme_indices(true) {
            let start = if substituted {
                row.logical_start
            } else {
                row.logical_start + offset
            };
            let end = if substituted {
                row.logical_end
            } else {
                start + grapheme.len()
            };
            let next_width = width.saturating_add(display_width(grapheme));
            if point.column < next_width {
                return Some(Hit {
                    start: TextPoint {
                        line_id: line.id,
                        byte: start,
                    },
                    end: TextPoint {
                        line_id: line.id,
                        byte: end,
                    },
                    row_start: TextPoint {
                        line_id: line.id,
                        byte: row.logical_start,
                    },
                    row_end: TextPoint {
                        line_id: line.id,
                        byte: row.logical_end,
                    },
                    visual: row_location(row),
                });
            }
            width = next_width;
        }
        Some(Hit {
            start: TextPoint {
                line_id: line.id,
                byte: row.logical_end,
            },
            end: TextPoint {
                line_id: line.id,
                byte: row.logical_end,
            },
            row_start: TextPoint {
                line_id: line.id,
                byte: row.logical_start,
            },
            row_end: TextPoint {
                line_id: line.id,
                byte: row.logical_end,
            },
            visual: row_location(row),
        })
    }

    fn word_span(&self, hit: Hit) -> Option<(TextPoint, TextPoint)> {
        let line = self.line_by_id(hit.start.line_id)?;
        let row_text = line.text.get(hit.row_start.byte..hit.row_end.byte)?;
        let graphemes = row_text.grapheme_indices(true).collect::<Vec<_>>();
        let index = graphemes
            .iter()
            .position(|(offset, grapheme)| {
                let start = hit.row_start.byte + *offset;
                hit.start.byte >= start && hit.start.byte < start + grapheme.len()
            })
            .or_else(|| (!graphemes.is_empty()).then_some(graphemes.len() - 1))?;
        let class = word_class(graphemes[index].1);
        let mut lo = index;
        let mut hi = index + 1;
        while lo > 0 && word_class(graphemes[lo - 1].1) == class {
            lo -= 1;
        }
        while hi < graphemes.len() && word_class(graphemes[hi].1) == class {
            hi += 1;
        }
        let start = hit.row_start.byte + graphemes[lo].0;
        let end = graphemes
            .get(hi)
            .map_or(hit.row_end.byte, |(offset, _)| hit.row_start.byte + *offset);
        Some((
            TextPoint {
                line_id: line.id,
                byte: start,
            },
            TextPoint {
                line_id: line.id,
                byte: end,
            },
        ))
    }

    fn visual_row_for_focus(
        &self,
        focus: TextPoint,
        hint: Option<VisualLocation>,
    ) -> Option<(usize, &VisualRow)> {
        if let Some(hint) = hint {
            if let Some((index, row)) = self.visual_cache.iter().enumerate().find(|(_, row)| {
                row.source.line_id == hint.line_id && row.logical_start == hint.logical_start
            }) {
                if focus.line_id == row.source.line_id
                    && focus.byte >= row.logical_start
                    && focus.byte <= row.logical_end
                {
                    return Some((index, row));
                }
            }
        }
        self.visual_cache.iter().enumerate().find(|(_, row)| {
            row.source.line_id == focus.line_id
                && focus.byte >= row.logical_start
                && focus.byte <= row.logical_end
        })
    }

    fn visual_row_for_direction(
        &self,
        point: TextPoint,
        prefer_next_at_boundary: bool,
    ) -> Option<(usize, &VisualRow)> {
        let matches = self
            .visual_cache
            .iter()
            .enumerate()
            .filter(|(_, row)| {
                row.source.line_id == point.line_id
                    && point.byte >= row.logical_start
                    && point.byte <= row.logical_end
            })
            .collect::<Vec<_>>();
        if prefer_next_at_boundary {
            matches.last().copied()
        } else {
            matches.first().copied()
        }
    }

    fn visual_column_for_point(&self, row: &VisualRow, point: TextPoint) -> usize {
        let Some(line) = self.line_by_id(row.source.line_id) else {
            return 0;
        };
        if row.text.as_str() != &line.text[row.logical_start..row.logical_end] {
            return usize::from(point.byte >= row.logical_end).min(self.columns.saturating_sub(1));
        }
        let end = point.byte.clamp(row.logical_start, row.logical_end);
        UnicodeWidthStr::width(&line.text[row.logical_start..end])
            .min(self.columns.saturating_sub(1))
    }

    fn point_at_visual_column(&self, row: &VisualRow, column: usize) -> TextPoint {
        let Some(line) = self.line_by_id(row.source.line_id) else {
            return TextPoint {
                line_id: row.source.line_id,
                byte: row.logical_start,
            };
        };
        if row.text.as_str() != &line.text[row.logical_start..row.logical_end] {
            return TextPoint {
                line_id: row.source.line_id,
                byte: if column == 0 {
                    row.logical_start
                } else {
                    row.logical_end
                },
            };
        }
        let mut width = 0usize;
        for (offset, grapheme) in row.text.grapheme_indices(true) {
            let next = width.saturating_add(display_width(grapheme));
            if column < next {
                return TextPoint {
                    line_id: row.source.line_id,
                    byte: row.logical_start + offset,
                };
            }
            width = next;
        }
        TextPoint {
            line_id: row.source.line_id,
            byte: row.logical_end,
        }
    }

    fn previous_text_point(&self, point: TextPoint) -> Option<TextPoint> {
        let lines = self.all_logical_lines();
        let index = lines.iter().position(|line| line.id == point.line_id)?;
        let line = lines[index];
        if point.byte > 0 {
            let byte = line.text[..point.byte.min(line.text.len())]
                .grapheme_indices(true)
                .next_back()
                .map(|(offset, _)| offset)?;
            return Some(TextPoint {
                line_id: line.id,
                byte,
            });
        }
        let previous = index.checked_sub(1).and_then(|index| lines.get(index))?;
        Some(TextPoint {
            line_id: previous.id,
            byte: previous.text.len(),
        })
    }

    fn next_text_point(&self, point: TextPoint) -> Option<TextPoint> {
        let lines = self.all_logical_lines();
        let index = lines.iter().position(|line| line.id == point.line_id)?;
        let line = lines[index];
        if point.byte < line.text.len() {
            let suffix = &line.text[point.byte..];
            let grapheme = suffix.graphemes(true).next()?;
            return Some(TextPoint {
                line_id: line.id,
                byte: point.byte + grapheme.len(),
            });
        }
        let next = lines.get(index + 1)?;
        Some(TextPoint {
            line_id: next.id,
            byte: 0,
        })
    }

    fn selection_bounds(&self) -> Option<(TextPoint, TextPoint)> {
        let selection = self.selection?;
        let focus = selection.focus?;
        Some(if point_key(selection.anchor) <= point_key(focus) {
            (selection.anchor, focus)
        } else {
            (focus, selection.anchor)
        })
    }

    fn render_visual_row(&self, row: &VisualRow) -> String {
        let Some(line) = self.line_by_id(row.source.line_id) else {
            return " ".repeat(self.columns);
        };
        let mut output = String::new();
        let mut selected = false;
        let mut width = 0usize;
        let substituted = row.text.as_str() != &line.text[row.logical_start..row.logical_end];
        for (offset, grapheme) in row.text.grapheme_indices(true) {
            let start = if substituted {
                row.logical_start
            } else {
                row.logical_start + offset
            };
            let end = if substituted {
                row.logical_end
            } else {
                start + grapheme.len()
            };
            let is_selected = self.grapheme_selected(line.id, start, end);
            if is_selected != selected {
                output.push_str(if is_selected {
                    ANSI_SELECTION
                } else {
                    ANSI_RESET
                });
                selected = is_selected;
            }
            let grapheme_width = display_width(grapheme);
            if width.saturating_add(grapheme_width) > self.columns {
                break;
            }
            output.push_str(grapheme);
            width += grapheme_width;
        }
        if selected {
            output.push_str(ANSI_RESET);
        }
        output.push_str(&" ".repeat(self.columns.saturating_sub(width)));
        output
    }

    fn grapheme_selected(&self, line_id: u64, start: usize, end: usize) -> bool {
        let Some((selection_start, selection_end)) = self.selection_bounds() else {
            return false;
        };
        point_key(TextPoint { line_id, byte: end }) > point_key(selection_start)
            && point_key(TextPoint {
                line_id,
                byte: start,
            }) < point_key(selection_end)
    }
}

#[derive(Debug, Clone, Copy)]
struct Layout {
    content_rows: usize,
    status_rows: usize,
    composer_rows: usize,
}

#[derive(Debug, Clone, Copy)]
struct Hit {
    start: TextPoint,
    end: TextPoint,
    row_start: TextPoint,
    row_end: TextPoint,
    visual: VisualLocation,
}

fn row_location(row: &VisualRow) -> VisualLocation {
    VisualLocation {
        line_id: row.source.line_id,
        logical_start: row.logical_start,
    }
}

fn point_key(point: TextPoint) -> (u64, usize) {
    (point.line_id, point.byte)
}

fn split_logical_lines(text: &str) -> impl Iterator<Item = &str> {
    text.split('\n')
}

fn transcript_line_cost(text: &str) -> usize {
    text.len().saturating_add(1)
}

fn bounded_plain_text(value: &str, max_bytes: usize) -> String {
    let mut sanitized = String::new();
    for character in value.chars() {
        let replacement = match character {
            '\t' => ' ',
            '\r' | '\n' => ' ',
            character if character.is_control() => '�',
            character => character,
        };
        if sanitized.len().saturating_add(replacement.len_utf8()) > max_bytes {
            break;
        }
        sanitized.push(replacement);
    }
    if sanitized.len() < value.len() && max_bytes >= '…'.len_utf8() {
        while sanitized.len().saturating_add('…'.len_utf8()) > max_bytes {
            let Some((offset, _)) = sanitized.grapheme_indices(true).next_back() else {
                break;
            };
            sanitized.truncate(offset);
        }
        sanitized.push('…');
    }
    sanitized
}

fn wrap_line(line: &TranscriptLine, columns: usize, output: &mut Vec<VisualRow>) {
    if line.text.is_empty() {
        output.push(VisualRow {
            source: SourceKey { line_id: line.id },
            logical_start: 0,
            logical_end: 0,
            text: String::new(),
        });
        return;
    }
    let mut start = 0usize;
    let mut width = 0usize;
    for (offset, grapheme) in line.text.grapheme_indices(true) {
        let grapheme_width = display_width(grapheme);
        if width > 0 && width.saturating_add(grapheme_width) > columns {
            output.push(VisualRow {
                source: SourceKey { line_id: line.id },
                logical_start: start,
                logical_end: offset,
                text: line.text[start..offset].to_owned(),
            });
            start = offset;
            width = 0;
        }
        if grapheme_width > columns {
            if start < offset {
                output.push(VisualRow {
                    source: SourceKey { line_id: line.id },
                    logical_start: start,
                    logical_end: offset,
                    text: line.text[start..offset].to_owned(),
                });
            }
            let end = offset + grapheme.len();
            output.push(VisualRow {
                source: SourceKey { line_id: line.id },
                logical_start: offset,
                logical_end: end,
                text: "…".to_owned(),
            });
            start = end;
            width = 0;
        } else {
            width += grapheme_width;
        }
    }
    if start < line.text.len() {
        output.push(VisualRow {
            source: SourceKey { line_id: line.id },
            logical_start: start,
            logical_end: line.text.len(),
            text: line.text[start..].to_owned(),
        });
    }
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value).max(1)
}

fn word_class(grapheme: &str) -> u8 {
    if grapheme.chars().all(char::is_whitespace) {
        return 0;
    }
    if grapheme.chars().all(|character| {
        character.is_alphanumeric() || matches!(character, '_' | '/' | '.' | '-' | '+' | '~' | '\\')
    }) {
        return 1;
    }
    2
}

fn clip_plain(value: &str, columns: usize) -> (String, usize) {
    let mut output = String::new();
    let mut width = 0usize;
    for grapheme in bounded_plain_text(value, value.len().max(1)).graphemes(true) {
        let grapheme_width = display_width(grapheme);
        if width.saturating_add(grapheme_width) > columns {
            break;
        }
        output.push_str(grapheme);
        width += grapheme_width;
    }
    (output, width)
}

fn plain_row(value: &str, columns: usize) -> String {
    let (mut row, width) = clip_plain(value, columns);
    row.push_str(&" ".repeat(columns.saturating_sub(width)));
    row
}

fn styled_plain_row(value: &str, columns: usize, style: &str) -> String {
    let (plain, width) = clip_plain(value, columns);
    let mut row = String::with_capacity(style.len() + plain.len() + ANSI_RESET.len() + columns);
    row.push_str(style);
    row.push_str(&plain);
    row.push_str(ANSI_RESET);
    row.push_str(&" ".repeat(columns.saturating_sub(width)));
    row
}

fn centered_styled_row(value: &str, columns: usize, style: &str) -> String {
    let (plain, width) = clip_plain(value, columns);
    let left = columns.saturating_sub(width) / 2;
    let right = columns.saturating_sub(left + width);
    format!(
        "{}{}{}{}{}",
        " ".repeat(left),
        style,
        plain,
        ANSI_RESET,
        " ".repeat(right)
    )
}

fn push_bounded(output: &mut String, value: &str, limit: usize) {
    let available = limit.saturating_sub(output.len());
    let mut end = available.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    output.push_str(&value[..end]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(rows: u16, columns: u16) -> FullscreenState {
        FullscreenState::new(rows, columns, 2, FullscreenLimits::default())
    }

    fn add_numbered_lines(state: &mut FullscreenState, count: usize) {
        for index in 0..count {
            state.push_message(&format!("line-{index:02}"));
        }
    }

    #[test]
    fn transcript_is_bounded_by_lines_and_keeps_the_newest_tail() {
        let mut state = FullscreenState::new(
            10,
            40,
            2,
            FullscreenLimits {
                max_transcript_lines: 3,
                ..FullscreenLimits::default()
            },
        );
        add_numbered_lines(&mut state, 5);
        assert_eq!(state.transcript_len(), 3);
        assert_eq!(state.dropped_lines(), 2);
        assert_eq!(state.lines.front().unwrap().text, "line-02");
    }

    #[test]
    fn transcript_is_bounded_by_bytes_and_line_bytes() {
        let mut state = FullscreenState::new(
            8,
            20,
            1,
            FullscreenLimits {
                max_transcript_bytes: 8,
                max_line_bytes: 5,
                ..FullscreenLimits::default()
            },
        );
        state.push_message("abcdefghij");
        state.push_message("12345");
        assert!(state.transcript_bytes() <= 8);
        assert!(state.lines.iter().all(|line| line.text.len() <= 5));
        assert_eq!(state.lines.back().unwrap().text, "12345");
    }

    #[test]
    fn control_sequences_are_sanitized_before_render() {
        let mut state = state(6, 30);
        state.push_message("safe\x1b[2Jtail");
        let frame = state.render_ansi(FrameSpec::new("head", &[]));
        assert!(!frame.bytes.contains("safe\x1b[2J"));
        assert!(frame.bytes.contains("safe�[2Jtail"));
    }

    #[test]
    fn sticky_bottom_follows_new_messages() {
        let mut state = state(7, 12);
        add_numbered_lines(&mut state, 12);
        let before = state.scroll_top();
        state.push_message("last");
        assert!(state.is_sticky_bottom());
        assert!(state.scroll_top() >= before);
        assert_eq!(state.unseen_messages(), 0);
    }

    #[test]
    fn scrolling_away_freezes_anchor_and_counts_messages() {
        let mut state = state(7, 12);
        add_numbered_lines(&mut state, 12);
        state.scroll_page(WheelDirection::Up);
        let top = state.top_anchor();
        state.push_message("new-a");
        state.push_message("new-b");
        assert_eq!(state.top_anchor(), top);
        assert_eq!(state.unseen_messages(), 2);
        assert!(!state.is_sticky_bottom());
    }

    #[test]
    fn returning_to_bottom_clears_unseen() {
        let mut state = state(7, 12);
        add_numbered_lines(&mut state, 12);
        state.scroll_lines(-2);
        state.push_message("new");
        assert_eq!(state.unseen_messages(), 1);
        state.scroll_to_bottom();
        assert_eq!(state.unseen_messages(), 0);
        assert!(state.is_sticky_bottom());
    }

    #[test]
    fn streaming_updates_count_once_and_commit_does_not_double_count() {
        let mut state = state(6, 10);
        add_numbered_lines(&mut state, 10);
        state.scroll_lines(-2);
        state.set_streaming_line(Some("a"));
        state.set_streaming_line(Some("ab"));
        assert_eq!(state.unseen_messages(), 1);
        assert!(state.commit_streaming_line());
        assert_eq!(state.unseen_messages(), 1);
        assert!(!state.commit_streaming_line());
    }

    #[test]
    fn viewport_supports_line_half_page_page_and_edges() {
        let mut state = state(9, 20);
        add_numbered_lines(&mut state, 30);
        let bottom = state.scroll_top();
        state.scroll_lines(-1);
        assert_eq!(state.scroll_top(), bottom - 1);
        state.scroll_half_page(WheelDirection::Up);
        assert!(state.scroll_top() < bottom - 1);
        state.scroll_page(WheelDirection::Down);
        state.scroll_to_top();
        assert_eq!(state.scroll_top(), 0);
        state.scroll_to_bottom();
        assert_eq!(state.scroll_top(), state.scroll_max());
    }

    #[test]
    fn resize_preserves_logical_anchor_when_not_sticky() {
        let mut state = state(8, 16);
        state.push_message("one two three four five six seven eight nine");
        state.push_message("tail-a\ntail-b\ntail-c\ntail-d\ntail-e");
        state.scroll_page(WheelDirection::Up);
        let anchor = state.top_anchor().unwrap();
        state.resize(10, 9);
        let after = state.top_anchor().unwrap();
        assert_eq!(after.line_id, anchor.line_id);
        assert!(after.byte <= anchor.byte);
        assert!(!state.is_sticky_bottom());
    }

    #[test]
    fn resize_keeps_sticky_view_at_bottom() {
        let mut state = state(8, 16);
        add_numbered_lines(&mut state, 20);
        state.resize(5, 8);
        assert!(state.is_sticky_bottom());
        assert_eq!(state.scroll_top(), state.scroll_max());
    }

    #[test]
    fn native_wheel_accelerates_and_resets_after_idle_or_reversal() {
        let mut wheel = WheelAccelerator::default();
        let first = wheel.step(WheelDirection::Down, Duration::from_millis(100));
        let mut fast = first;
        for tick in 1..12 {
            fast = wheel.step(WheelDirection::Down, Duration::from_millis(100 + tick * 10));
        }
        let idle = wheel.step(WheelDirection::Down, Duration::from_secs(2));
        let reverse = wheel.step(WheelDirection::Up, Duration::from_millis(2_010));
        assert!(fast > first);
        assert_eq!(idle, 1);
        assert_eq!(reverse, 1);
    }

    #[test]
    fn xterm_wheel_uses_a_bounded_decay_curve() {
        let mut wheel = WheelAccelerator::new(WheelConfig {
            profile: WheelProfile::XtermJs,
            max_rows: 6.0,
            ..WheelConfig::default()
        });
        let mut max = 0;
        for tick in 0..20 {
            max = max.max(wheel.step(WheelDirection::Down, Duration::from_millis(100 + tick * 30)));
        }
        assert!((2..=6).contains(&max));
    }

    #[test]
    fn wide_cjk_and_emoji_wrap_without_splitting_graphemes() {
        let mut state = state(8, 4);
        state.push_message("你好👨‍👩‍👧‍👦ab");
        let rows = state.visual_rows();
        assert!(rows.len() >= 2);
        assert!(
            rows.iter()
                .all(|row| UnicodeWidthStr::width(row.text.as_str()) <= 4)
        );
        assert!(rows.iter().any(|row| row.text.contains("👨‍👩‍👧‍👦")));
    }

    #[test]
    fn too_wide_emoji_placeholder_still_maps_to_the_original_grapheme() {
        let mut state = state(5, 1);
        state.push_message("👨‍👩‍👧‍👦");
        assert!(state.click(ViewportPoint { row: 0, column: 0 }, ClickKind::Double));
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("👨‍👩‍👧‍👦"));
    }

    #[test]
    fn single_click_without_drag_does_not_select() {
        let mut state = state(7, 20);
        state.push_message("hello world");
        assert!(state.click(ViewportPoint { row: 0, column: 1 }, ClickKind::Single));
        state.finish_selection();
        assert!(!state.has_selection());
        assert_eq!(state.selected_text(), None);
    }

    #[test]
    fn dragging_within_the_same_cell_does_not_create_a_selection() {
        let mut state = state(7, 20);
        state.push_message("hello world");
        assert!(state.click(ViewportPoint { row: 0, column: 1 }, ClickKind::Single));
        assert!(!state.drag_to(ViewportPoint { row: 0, column: 1 }));
        state.finish_selection();
        assert!(!state.has_selection());
    }

    #[test]
    fn drag_selection_copies_plain_text_across_soft_wraps() {
        let mut state = state(8, 5);
        state.push_message("abcdefghij");
        assert!(state.click(ViewportPoint { row: 0, column: 1 }, ClickKind::Single));
        assert!(state.drag_to(ViewportPoint { row: 1, column: 3 }));
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("bcdefghi"));
    }

    #[test]
    fn drag_selection_across_hard_lines_inserts_newline() {
        let mut state = state(8, 20);
        state.push_message("alpha\nbeta");
        state.click(ViewportPoint { row: 0, column: 2 }, ClickKind::Single);
        state.drag_to(ViewportPoint { row: 1, column: 2 });
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("pha\nbet"));
    }

    #[test]
    fn double_click_selects_unicode_word_and_paths() {
        let mut state = state(8, 40);
        state.push_message("运行 /tmp/你好.rs now");
        assert!(state.click(ViewportPoint { row: 0, column: 7 }, ClickKind::Double));
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("/tmp/你好.rs"));
    }

    #[test]
    fn double_and_triple_click_are_bounded_to_the_visual_row() {
        let mut state = state(8, 5);
        state.push_message("abcdefghij");
        assert!(state.click(ViewportPoint { row: 1, column: 1 }, ClickKind::Double));
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("fghij"));
        assert!(state.click(ViewportPoint { row: 1, column: 1 }, ClickKind::Triple));
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("fghij"));
    }

    #[test]
    fn keyboard_selection_keeps_anchor_and_moves_by_grapheme() {
        let mut state = state(8, 40);
        state.push_message("hello world");
        assert!(state.click(ViewportPoint { row: 0, column: 1 }, ClickKind::Double));
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("hello"));
        assert!(state.move_selection_focus(SelectionFocusMove::Right));
        assert_eq!(state.selected_text().as_deref(), Some("hello "));
        assert!(state.move_selection_focus(SelectionFocusMove::Left));
        assert_eq!(state.selected_text().as_deref(), Some("hello"));
        assert!(state.move_selection_focus(SelectionFocusMove::Left));
        assert_eq!(state.selected_text().as_deref(), Some("hell"));
    }

    #[test]
    fn keyboard_selection_wraps_rows_and_supports_visual_edges() {
        let mut state = state(8, 5);
        state.push_message("abcdefghij");
        assert!(state.click(ViewportPoint { row: 1, column: 1 }, ClickKind::Triple));
        state.finish_selection();
        assert!(state.move_selection_focus(SelectionFocusMove::Left));
        assert_eq!(state.selected_text().as_deref(), Some("fghi"));
        assert!(state.move_selection_focus(SelectionFocusMove::LineStart));
        assert_eq!(state.selected_text(), None);
        assert!(state.move_selection_focus(SelectionFocusMove::Up));
        assert_eq!(state.selected_text().as_deref(), Some("abcde"));
        assert!(state.move_selection_focus(SelectionFocusMove::LineEnd));
        assert_eq!(state.selected_text(), None);
    }

    #[test]
    fn selection_limit_ends_at_a_utf8_boundary() {
        let mut state = FullscreenState::new(
            8,
            40,
            2,
            FullscreenLimits {
                max_selection_bytes: 5,
                ..FullscreenLimits::default()
            },
        );
        state.push_message("你好世界");
        state.click(ViewportPoint { row: 0, column: 0 }, ClickKind::Triple);
        state.finish_selection();
        assert_eq!(state.selected_text().as_deref(), Some("你"));
    }

    #[test]
    fn stale_selection_is_cleared_when_bounded_history_evicts_it() {
        let mut state = FullscreenState::new(
            8,
            20,
            2,
            FullscreenLimits {
                max_transcript_lines: 1,
                ..FullscreenLimits::default()
            },
        );
        state.push_message("old");
        state.click(ViewportPoint { row: 0, column: 0 }, ClickKind::Triple);
        state.finish_selection();
        assert!(state.has_selection());
        state.push_message("new");
        assert!(!state.has_selection());
    }

    #[test]
    fn frame_has_header_status_pill_and_reserved_composer() {
        let mut state = state(8, 32);
        add_numbered_lines(&mut state, 20);
        state.scroll_lines(-2);
        state.push_message("new");
        state.set_status(Some("streaming"));
        let frame = state.render_ansi(FrameSpec::new("session", &["prompt", "hint"]));
        assert_eq!(frame.rows, 8);
        assert_eq!(frame.content_rows, 4);
        assert!(frame.bytes.contains("session"));
        assert!(frame.bytes.contains("1 new message"));
        assert!(frame.bytes.contains("streaming"));
        assert!(frame.bytes.contains("prompt"));
        assert!(frame.bytes.contains("hint"));
    }

    #[test]
    fn frame_never_controls_alternate_screen_or_mouse_mode() {
        let state = state(4, 20);
        let frame = state.render_ansi(FrameSpec::new("header", &[]));
        assert!(!frame.bytes.contains("?1049"));
        assert!(!frame.bytes.contains("?1000"));
        assert!(!frame.bytes.contains("?1002"));
        assert!(!frame.bytes.contains("?1006"));
    }

    #[test]
    fn tiny_terminal_remains_bounded_and_does_not_panic() {
        let mut state = FullscreenState::new(0, 0, u16::MAX, FullscreenLimits::default());
        state.push_message("你好");
        state.set_status(Some("busy"));
        let frame = state.render_ansi(FrameSpec::new("h", &["composer"]));
        assert_eq!(frame.rows, 1);
        assert_eq!(frame.columns, 1);
        assert_eq!(frame.content_rows, 0);
    }
}

//! Provider-neutral, bounded terminal dialog state machines.
//!
//! The dialogs in this module own no process, persistence, or terminal input.
//! They consume normalized keys, produce bounded frames, and return typed
//! actions for the caller to execute. [`AlternateScreenRenderer`] is a small
//! output adapter which lets an existing terminal frontend host a dialog in
//! the alternate screen without coupling the state machines to global I/O.

use std::io::{self, Write};

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use unicode_width::UnicodeWidthChar;

const MAX_DIALOG_ITEMS: usize = 512;
const MAX_ITEM_TEXT_CHARS: usize = 2_048;
const MAX_DETAIL_CHARS: usize = 16_384;
const MAX_INPUT_CHARS: usize = 512;
const MAX_CHOICE_OPTIONS: usize = 128;
const MAX_FRAME_LINES: usize = 256;
const MAX_TERMINAL_COLUMNS: u16 = 1_000;
const EXIT_WINDOW_MILLIS: u64 = 800;

/// Input understood by every dialog state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogInput {
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
    Enter,
    Escape,
    Backspace,
    Delete,
    Tab,
    BackTab,
    Character(char),
    Save,
    CtrlC,
    CtrlD,
}

impl DialogInput {
    /// Converts a crossterm press/repeat event into a dialog input.
    pub fn from_key_event(event: KeyEvent) -> Option<Self> {
        if !matches!(event.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return None;
        }
        let control = event.modifiers.contains(KeyModifiers::CONTROL);
        if control {
            return match event.code {
                KeyCode::Char('c') => Some(Self::CtrlC),
                KeyCode::Char('d') => Some(Self::CtrlD),
                KeyCode::Char('s') => Some(Self::Save),
                KeyCode::Char('n') => Some(Self::Down),
                KeyCode::Char('p') => Some(Self::Up),
                _ => None,
            };
        }
        match event.code {
            KeyCode::Up => Some(Self::Up),
            KeyCode::Down => Some(Self::Down),
            KeyCode::Left => Some(Self::Left),
            KeyCode::Right => Some(Self::Right),
            KeyCode::PageUp => Some(Self::PageUp),
            KeyCode::PageDown => Some(Self::PageDown),
            KeyCode::Enter => Some(Self::Enter),
            KeyCode::Esc => Some(Self::Escape),
            KeyCode::Backspace => Some(Self::Backspace),
            KeyCode::Delete => Some(Self::Delete),
            KeyCode::Tab if event.modifiers.contains(KeyModifiers::SHIFT) => Some(Self::BackTab),
            KeyCode::Tab => Some(Self::Tab),
            KeyCode::Char(character) => Some(Self::Character(character)),
            _ => None,
        }
    }
}

/// Result of feeding one key into a dialog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialogUpdate<A> {
    Continue,
    /// The first Ctrl-C/Ctrl-D arms an exit for 800 ms.
    ExitHint(ExitKey),
    Action(A),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitKey {
    CtrlC,
    CtrlD,
}

impl ExitKey {
    pub fn hint(self) -> &'static str {
        match self {
            Self::CtrlC => "Press Ctrl-C again to exit",
            Self::CtrlD => "Press Ctrl-D again to exit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DialogFrame {
    lines: Vec<String>,
    cursor: Option<(u16, u16)>,
}

impl DialogFrame {
    pub fn new(lines: Vec<String>, cursor: Option<(u16, u16)>, width: u16, height: u16) -> Self {
        let width = usize::from(width.clamp(1, MAX_TERMINAL_COLUMNS));
        let height = usize::from(height.max(1)).min(MAX_FRAME_LINES);
        let lines = lines
            .into_iter()
            .take(height)
            .map(|line| fit_line(&line, width))
            .collect();
        let cursor = cursor.and_then(|(column, row)| {
            (usize::from(row) < height).then_some((column.min(width.saturating_sub(1) as u16), row))
        });
        Self { lines, cursor }
    }

    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    pub fn cursor(&self) -> Option<(u16, u16)> {
        self.cursor
    }
}

/// Writes already-bounded dialog frames to an alternate screen.
///
/// Raw mode and event polling remain the embedding frontend's responsibility.
#[derive(Debug, Clone)]
pub struct AlternateScreenRenderer {
    width: u16,
    height: u16,
    active: bool,
}

impl AlternateScreenRenderer {
    pub fn new(width: u16, height: u16) -> Self {
        Self {
            width: width.clamp(1, MAX_TERMINAL_COLUMNS),
            height: height.max(1),
            active: false,
        }
    }

    pub fn resize(&mut self, width: u16, height: u16) {
        self.width = width.clamp(1, MAX_TERMINAL_COLUMNS);
        self.height = height.max(1);
    }

    pub fn size(&self) -> (u16, u16) {
        (self.width, self.height)
    }

    pub fn enter(&mut self, output: &mut impl Write) -> io::Result<()> {
        if !self.active {
            if let Err(error) = output.write_all(b"\x1b[?1049h\x1b[?25l") {
                let _ = output.write_all(b"\x1b[0m\x1b[?25h\x1b[?1049l");
                let _ = output.flush();
                return Err(error);
            }
            self.active = true;
        }
        Ok(())
    }

    pub fn draw(&mut self, output: &mut impl Write, frame: &DialogFrame) -> io::Result<()> {
        self.enter(output)?;
        output.write_all(b"\x1b[H\x1b[2J")?;
        for (index, line) in frame
            .lines()
            .iter()
            .take(usize::from(self.height))
            .enumerate()
        {
            if index > 0 {
                output.write_all(b"\r\n")?;
            }
            output.write_all(fit_line(line, usize::from(self.width)).as_bytes())?;
        }
        if let Some((column, row)) = frame.cursor() {
            write!(output, "\x1b[{};{}H\x1b[?25h", row + 1, column + 1)?;
        }
        output.flush()
    }

    pub fn leave(&mut self, output: &mut impl Write) -> io::Result<()> {
        if self.active {
            output.write_all(b"\x1b[0m\x1b[?25h\x1b[?1049l")?;
            output.flush()?;
            self.active = false;
        }
        Ok(())
    }

    pub fn is_active(&self) -> bool {
        self.active
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ArmedExit {
    key: ExitKey,
    at_millis: u64,
}

#[derive(Debug, Clone, Default)]
struct ExitArmer {
    armed: Option<ArmedExit>,
}

impl ExitArmer {
    fn feed(&mut self, input: DialogInput, now_millis: u64) -> ExitFeed {
        let key = match input {
            DialogInput::CtrlC => ExitKey::CtrlC,
            DialogInput::CtrlD => ExitKey::CtrlD,
            _ => {
                self.armed = None;
                return ExitFeed::Ordinary;
            }
        };
        if self.armed.is_some_and(|armed| {
            armed.key == key && now_millis.saturating_sub(armed.at_millis) < EXIT_WINDOW_MILLIS
        }) {
            self.armed = None;
            ExitFeed::Exit
        } else {
            self.armed = Some(ArmedExit {
                key,
                at_millis: now_millis,
            });
            ExitFeed::Hint(key)
        }
    }
}

enum ExitFeed {
    Ordinary,
    Hint(ExitKey),
    Exit,
}

// ---- Permission manager -------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PermissionTab {
    Recent,
    Allow,
    Ask,
    Deny,
    Workspace,
}

impl PermissionTab {
    const ALL: [Self; 5] = [
        Self::Recent,
        Self::Allow,
        Self::Ask,
        Self::Deny,
        Self::Workspace,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Recent => "Recent",
            Self::Allow => "Allow",
            Self::Ask => "Ask",
            Self::Deny => "Deny",
            Self::Workspace => "Workspace",
        }
    }

    fn previous(self) -> Self {
        let index = Self::ALL
            .iter()
            .position(|tab| *tab == self)
            .unwrap_or_default();
        Self::ALL[(index + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    fn next(self) -> Self {
        let index = Self::ALL
            .iter()
            .position(|tab| *tab == self)
            .unwrap_or_default();
        Self::ALL[(index + 1) % Self::ALL.len()]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionDialogItem {
    pub id: String,
    pub label: String,
    pub detail: String,
}

impl PermissionDialogItem {
    pub fn new(id: impl AsRef<str>, label: impl AsRef<str>, detail: impl AsRef<str>) -> Self {
        Self {
            id: clean_text(id.as_ref(), MAX_ITEM_TEXT_CHARS),
            label: clean_text(label.as_ref(), MAX_ITEM_TEXT_CHARS),
            detail: clean_text(detail.as_ref(), MAX_DETAIL_CHARS),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PermissionDialogData {
    pub recent: Vec<PermissionDialogItem>,
    pub allow: Vec<PermissionDialogItem>,
    pub ask: Vec<PermissionDialogItem>,
    pub deny: Vec<PermissionDialogItem>,
    pub workspace: Vec<PermissionDialogItem>,
}

impl PermissionDialogData {
    fn bounded(mut self) -> Self {
        bound_permission_items(&mut self.recent);
        bound_permission_items(&mut self.allow);
        bound_permission_items(&mut self.ask);
        bound_permission_items(&mut self.deny);
        bound_permission_items(&mut self.workspace);
        self
    }

    fn items(&self, tab: PermissionTab) -> &[PermissionDialogItem] {
        match tab {
            PermissionTab::Recent => &self.recent,
            PermissionTab::Allow => &self.allow,
            PermissionTab::Ask => &self.ask,
            PermissionTab::Deny => &self.deny,
            PermissionTab::Workspace => &self.workspace,
        }
    }
}

fn bound_permission_items(items: &mut Vec<PermissionDialogItem>) {
    items.truncate(MAX_DIALOG_ITEMS);
    for item in items {
        *item = PermissionDialogItem::new(&item.id, &item.label, &item.detail);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionManagerAction {
    AddRule {
        tab: PermissionTab,
        rule: String,
    },
    AddWorkspace {
        path: String,
    },
    DeleteRule {
        tab: PermissionTab,
        id: String,
        rule: String,
    },
    RemoveWorkspace {
        id: String,
        path: String,
    },
    OpenRecent {
        id: String,
    },
    Cancelled,
    ExitRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PermissionMode {
    Browse,
    Search,
    Add,
    ConfirmDelete,
}

#[derive(Debug, Clone)]
pub struct PermissionManagerDialog {
    data: PermissionDialogData,
    tab: PermissionTab,
    selected: usize,
    mode: PermissionMode,
    query: String,
    input: String,
    exit: ExitArmer,
}

impl PermissionManagerDialog {
    pub fn new(data: PermissionDialogData) -> Self {
        let data = data.bounded();
        let tab = if data.recent.is_empty() {
            PermissionTab::Allow
        } else {
            PermissionTab::Recent
        };
        Self {
            data,
            tab,
            selected: 0,
            mode: PermissionMode::Browse,
            query: String::new(),
            input: String::new(),
            exit: ExitArmer::default(),
        }
    }

    pub fn tab(&self) -> PermissionTab {
        self.tab
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn selected(&self) -> usize {
        self.selected
    }

    pub fn handle(&mut self, input: DialogInput) -> DialogUpdate<PermissionManagerAction> {
        self.handle_at(input, 0)
    }

    pub fn handle_at(
        &mut self,
        input: DialogInput,
        now_millis: u64,
    ) -> DialogUpdate<PermissionManagerAction> {
        match self.exit.feed(input, now_millis) {
            ExitFeed::Hint(key) => return DialogUpdate::ExitHint(key),
            ExitFeed::Exit => return DialogUpdate::Action(PermissionManagerAction::ExitRequested),
            ExitFeed::Ordinary => {}
        }
        match self.mode {
            PermissionMode::Search => self.handle_search(input),
            PermissionMode::Add => self.handle_add(input),
            PermissionMode::ConfirmDelete => self.handle_confirm_delete(input),
            PermissionMode::Browse => self.handle_browse(input),
        }
    }

    fn handle_browse(&mut self, input: DialogInput) -> DialogUpdate<PermissionManagerAction> {
        match input {
            DialogInput::Left | DialogInput::BackTab => {
                self.tab = self.tab.previous();
                self.selected = 0;
            }
            DialogInput::Right | DialogInput::Tab => {
                self.tab = self.tab.next();
                self.selected = 0;
            }
            DialogInput::Up => self.move_selection(-1),
            DialogInput::Down => self.move_selection(1),
            DialogInput::PageUp => self.move_selection(-8),
            DialogInput::PageDown => self.move_selection(8),
            DialogInput::Character('/') => {
                self.mode = PermissionMode::Search;
                self.query.clear();
                self.selected = 0;
            }
            DialogInput::Character('a') => {
                if self.tab != PermissionTab::Recent {
                    self.mode = PermissionMode::Add;
                    self.input.clear();
                }
            }
            DialogInput::Character('x') | DialogInput::Delete => {
                if self.selected_item().is_some() && self.tab != PermissionTab::Recent {
                    self.mode = PermissionMode::ConfirmDelete;
                }
            }
            DialogInput::Enter => {
                if self.tab == PermissionTab::Recent {
                    if let Some(item) = self.selected_item() {
                        return DialogUpdate::Action(PermissionManagerAction::OpenRecent {
                            id: item.id.clone(),
                        });
                    }
                }
            }
            DialogInput::Escape => {
                return DialogUpdate::Action(PermissionManagerAction::Cancelled);
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn handle_search(&mut self, input: DialogInput) -> DialogUpdate<PermissionManagerAction> {
        match input {
            DialogInput::Escape | DialogInput::Enter => self.mode = PermissionMode::Browse,
            DialogInput::Backspace | DialogInput::Delete => {
                self.query.pop();
                self.selected = 0;
            }
            DialogInput::Character(character) => {
                push_bounded(&mut self.query, character, MAX_INPUT_CHARS);
                self.selected = 0;
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn handle_add(&mut self, input: DialogInput) -> DialogUpdate<PermissionManagerAction> {
        match input {
            DialogInput::Escape => self.mode = PermissionMode::Browse,
            DialogInput::Backspace | DialogInput::Delete => {
                self.input.pop();
            }
            DialogInput::Character(character) => {
                push_bounded(&mut self.input, character, MAX_INPUT_CHARS);
            }
            DialogInput::Enter => {
                let value = clean_text(self.input.trim(), MAX_INPUT_CHARS);
                if !value.is_empty() {
                    self.mode = PermissionMode::Browse;
                    return DialogUpdate::Action(if self.tab == PermissionTab::Workspace {
                        PermissionManagerAction::AddWorkspace { path: value }
                    } else {
                        PermissionManagerAction::AddRule {
                            tab: self.tab,
                            rule: value,
                        }
                    });
                }
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn handle_confirm_delete(
        &mut self,
        input: DialogInput,
    ) -> DialogUpdate<PermissionManagerAction> {
        match input {
            DialogInput::Escape | DialogInput::Character('n' | 'N') => {
                self.mode = PermissionMode::Browse;
            }
            DialogInput::Enter | DialogInput::Character('y' | 'Y') => {
                let Some(item) = self.selected_item().cloned() else {
                    self.mode = PermissionMode::Browse;
                    return DialogUpdate::Continue;
                };
                self.mode = PermissionMode::Browse;
                return DialogUpdate::Action(if self.tab == PermissionTab::Workspace {
                    PermissionManagerAction::RemoveWorkspace {
                        id: item.id,
                        path: item.label,
                    }
                } else {
                    PermissionManagerAction::DeleteRule {
                        tab: self.tab,
                        id: item.id,
                        rule: item.label,
                    }
                });
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn filtered_indices(&self) -> Vec<usize> {
        let needle = self.query.to_lowercase();
        self.data
            .items(self.tab)
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                (needle.is_empty()
                    || item.label.to_lowercase().contains(&needle)
                    || item.detail.to_lowercase().contains(&needle))
                .then_some(index)
            })
            .take(MAX_DIALOG_ITEMS)
            .collect()
    }

    fn selected_item(&self) -> Option<&PermissionDialogItem> {
        let indices = self.filtered_indices();
        indices
            .get(self.selected)
            .and_then(|index| self.data.items(self.tab).get(*index))
    }

    fn move_selection(&mut self, amount: isize) {
        let count = self.filtered_indices().len();
        self.selected = moved_index(self.selected, count, amount);
    }

    pub fn render(&self, width: u16, height: u16) -> DialogFrame {
        let mut lines = Vec::new();
        lines.push("Permissions".to_owned());
        let tabs = PermissionTab::ALL
            .into_iter()
            .map(|tab| {
                if tab == self.tab {
                    format!("[{}]", tab.label())
                } else {
                    tab.label().to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("  ");
        lines.push(tabs);
        match self.mode {
            PermissionMode::Search => lines.push(format!("Search: {}_", self.query)),
            PermissionMode::Add => {
                let label = if self.tab == PermissionTab::Workspace {
                    "Add workspace"
                } else {
                    "Add rule"
                };
                lines.push(format!("{label}: {}_", self.input));
            }
            PermissionMode::ConfirmDelete => {
                let label = self
                    .selected_item()
                    .map(|item| item.label.as_str())
                    .unwrap_or("selected entry");
                lines.push(format!("Delete {label:?}? y/Enter confirm · n/Esc cancel"));
            }
            PermissionMode::Browse if !self.query.is_empty() => {
                lines.push(format!("Filter: {}", self.query));
            }
            PermissionMode::Browse => lines.push(String::new()),
        }
        let indices = self.filtered_indices();
        let footer_rows = 1;
        let list_rows = usize::from(height.max(1))
            .saturating_sub(lines.len() + footer_rows)
            .max(1);
        let start = centered_start(self.selected, indices.len(), list_rows);
        if indices.is_empty() {
            lines.push("  No entries".to_owned());
        } else {
            for (visible, index) in indices.iter().skip(start).take(list_rows).enumerate() {
                let item = &self.data.items(self.tab)[*index];
                let marker = if start + visible == self.selected {
                    ">"
                } else {
                    " "
                };
                let detail = if item.detail.is_empty() {
                    String::new()
                } else {
                    format!(" — {}", item.detail)
                };
                lines.push(format!("{marker} {}{detail}", item.label));
            }
        }
        lines.push(if self.mode == PermissionMode::ConfirmDelete {
            "y/Enter confirm · n/Esc cancel".to_owned()
        } else {
            "←/→ tabs · ↑/↓ select · / search · a add · x delete · Esc close".to_owned()
        });
        DialogFrame::new(lines, None, width, height)
    }
}

// ---- Task dialog --------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskCategory {
    Agent,
    Shell,
    Other,
}

impl TaskCategory {
    pub fn label(self) -> &'static str {
        match self {
            Self::Agent => "Agent",
            Self::Shell => "Shell",
            Self::Other => "Other",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Running,
    Completed,
    Failed,
    Stopped,
}

impl TaskState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDialogItem {
    pub id: String,
    pub title: String,
    pub detail: String,
    pub category: TaskCategory,
    pub state: TaskState,
    pub can_foreground: bool,
    pub has_output: bool,
}

impl TaskDialogItem {
    pub fn bounded(mut self) -> Self {
        self.id = clean_text(&self.id, MAX_ITEM_TEXT_CHARS);
        self.title = clean_text(&self.title, MAX_ITEM_TEXT_CHARS);
        self.detail = clean_text(&self.detail, MAX_DETAIL_CHARS);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskDialogAction {
    Stop { id: String },
    Foreground { id: String },
    ShowOutput { id: String },
    Cancelled,
    ExitRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskView {
    List,
    Detail,
}

#[derive(Debug, Clone)]
pub struct TaskDialog {
    items: Vec<TaskDialogItem>,
    selected: usize,
    view: TaskView,
    exit: ExitArmer,
}

impl TaskDialog {
    pub fn new(items: Vec<TaskDialogItem>) -> Self {
        let mut items = items
            .into_iter()
            .take(MAX_DIALOG_ITEMS)
            .map(TaskDialogItem::bounded)
            .collect::<Vec<_>>();
        items.sort_by_key(|item| (item.category, item.state != TaskState::Running));
        let view = if items.len() == 1 {
            TaskView::Detail
        } else {
            TaskView::List
        };
        Self {
            items,
            selected: 0,
            view,
            exit: ExitArmer::default(),
        }
    }

    pub fn is_detail(&self) -> bool {
        self.view == TaskView::Detail
    }

    pub fn selected(&self) -> usize {
        self.selected
    }

    pub fn handle(&mut self, input: DialogInput) -> DialogUpdate<TaskDialogAction> {
        self.handle_at(input, 0)
    }

    pub fn handle_at(
        &mut self,
        input: DialogInput,
        now_millis: u64,
    ) -> DialogUpdate<TaskDialogAction> {
        match self.exit.feed(input, now_millis) {
            ExitFeed::Hint(key) => return DialogUpdate::ExitHint(key),
            ExitFeed::Exit => return DialogUpdate::Action(TaskDialogAction::ExitRequested),
            ExitFeed::Ordinary => {}
        }
        match input {
            DialogInput::Escape => return DialogUpdate::Action(TaskDialogAction::Cancelled),
            DialogInput::Left if self.view == TaskView::Detail && self.items.len() > 1 => {
                self.view = TaskView::List;
            }
            DialogInput::Right | DialogInput::Enter if self.view == TaskView::List => {
                if !self.items.is_empty() {
                    self.view = TaskView::Detail;
                }
            }
            DialogInput::Up if self.view == TaskView::List => {
                self.selected = moved_index(self.selected, self.items.len(), -1);
            }
            DialogInput::Down if self.view == TaskView::List => {
                self.selected = moved_index(self.selected, self.items.len(), 1);
            }
            DialogInput::PageUp if self.view == TaskView::List => {
                self.selected = moved_index(self.selected, self.items.len(), -8);
            }
            DialogInput::PageDown if self.view == TaskView::List => {
                self.selected = moved_index(self.selected, self.items.len(), 8);
            }
            DialogInput::Character('x') | DialogInput::Delete => {
                if let Some(task) = self.current_task() {
                    if task.state == TaskState::Running {
                        return DialogUpdate::Action(TaskDialogAction::Stop {
                            id: task.id.clone(),
                        });
                    }
                }
            }
            DialogInput::Character('f') => {
                if let Some(task) = self.current_task() {
                    if task.can_foreground {
                        return DialogUpdate::Action(TaskDialogAction::Foreground {
                            id: task.id.clone(),
                        });
                    }
                    if task.has_output {
                        return DialogUpdate::Action(TaskDialogAction::ShowOutput {
                            id: task.id.clone(),
                        });
                    }
                }
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn current_task(&self) -> Option<&TaskDialogItem> {
        self.items.get(self.selected)
    }

    pub fn render(&self, width: u16, height: u16) -> DialogFrame {
        let mut lines = vec!["Background tasks".to_owned()];
        match self.view {
            TaskView::List => self.render_list(&mut lines, height),
            TaskView::Detail => self.render_detail(&mut lines, width, height),
        }
        DialogFrame::new(lines, None, width, height)
    }

    fn render_list(&self, lines: &mut Vec<String>, height: u16) {
        let list_rows = usize::from(height.max(1)).saturating_sub(2).max(1);
        let start = centered_start(self.selected, self.items.len(), list_rows);
        if self.items.is_empty() {
            lines.push("  No background tasks".to_owned());
        } else {
            for (visible, item) in self.items.iter().skip(start).take(list_rows).enumerate() {
                let marker = if start + visible == self.selected {
                    ">"
                } else {
                    " "
                };
                lines.push(format!(
                    "{marker} [{}] {} · {}",
                    item.category.label(),
                    item.title,
                    item.state.label()
                ));
            }
        }
        lines.push(
            "↑/↓ select · Enter/→ details · x stop · f foreground/output · Esc close".to_owned(),
        );
    }

    fn render_detail(&self, lines: &mut Vec<String>, width: u16, height: u16) {
        if let Some(task) = self.current_task() {
            lines.push(format!("{} · {}", task.title, task.state.label()));
            lines.push(format!("Type: {} · ID: {}", task.category.label(), task.id));
            let available = usize::from(height.max(1)).saturating_sub(5).max(1);
            lines.extend(wrap_text(
                &task.detail,
                usize::from(width.max(1)),
                available,
            ));
            if self.items.len() > 1 {
                lines.push("← list · x stop · f foreground/output · Esc close".to_owned());
            } else {
                lines.push("x stop · f foreground/output · Esc close".to_owned());
            }
        } else {
            lines.push("No task selected".to_owned());
            lines.push("Esc close".to_owned());
        }
    }
}

// ---- Settings dialog ----------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingValue {
    Boolean(bool),
    Choice {
        selected: String,
        options: Vec<String>,
    },
}

impl SettingValue {
    fn bounded(self) -> Self {
        match self {
            Self::Boolean(value) => Self::Boolean(value),
            Self::Choice { selected, options } => {
                let mut options = options
                    .into_iter()
                    .take(MAX_CHOICE_OPTIONS)
                    .map(|option| clean_text(&option, MAX_ITEM_TEXT_CHARS))
                    .filter(|option| !option.is_empty())
                    .collect::<Vec<_>>();
                options.dedup();
                let selected = clean_text(&selected, MAX_ITEM_TEXT_CHARS);
                if !selected.is_empty() && !options.iter().any(|option| option == &selected) {
                    options.insert(0, selected.clone());
                    options.truncate(MAX_CHOICE_OPTIONS);
                }
                let selected = if options.iter().any(|option| option == &selected) {
                    selected
                } else {
                    options.first().cloned().unwrap_or_default()
                };
                Self::Choice { selected, options }
            }
        }
    }

    fn display(&self) -> String {
        match self {
            Self::Boolean(value) => if *value { "on" } else { "off" }.to_owned(),
            Self::Choice { selected, .. } => selected.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingItem {
    pub key: String,
    pub label: String,
    pub description: String,
    pub value: SettingValue,
}

impl SettingItem {
    fn bounded(mut self) -> Self {
        self.key = clean_text(&self.key, MAX_ITEM_TEXT_CHARS);
        self.label = clean_text(&self.label, MAX_ITEM_TEXT_CHARS);
        self.description = clean_text(&self.description, MAX_DETAIL_CHARS);
        self.value = self.value.bounded();
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SettingsSnapshot {
    pub items: Vec<SettingItem>,
}

impl SettingsSnapshot {
    pub fn new(items: Vec<SettingItem>) -> Self {
        Self {
            items: items
                .into_iter()
                .take(MAX_DIALOG_ITEMS)
                .map(SettingItem::bounded)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingChange {
    pub key: String,
    pub before: SettingValue,
    pub after: SettingValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettingsDialogAction {
    Save {
        snapshot: SettingsSnapshot,
        changes: Vec<SettingChange>,
    },
    Cancel {
        snapshot: SettingsSnapshot,
    },
    ExitRequested {
        snapshot: SettingsSnapshot,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingsMode {
    Browse,
    Search,
    Choice,
}

#[derive(Debug, Clone)]
pub struct SettingsDialog {
    original: SettingsSnapshot,
    working: SettingsSnapshot,
    selected: usize,
    choice_selected: usize,
    mode: SettingsMode,
    query: String,
    exit: ExitArmer,
}

impl SettingsDialog {
    pub fn new(snapshot: SettingsSnapshot) -> Self {
        let snapshot = SettingsSnapshot::new(snapshot.items);
        Self {
            original: snapshot.clone(),
            working: snapshot,
            selected: 0,
            choice_selected: 0,
            mode: SettingsMode::Browse,
            query: String::new(),
            exit: ExitArmer::default(),
        }
    }

    pub fn original_snapshot(&self) -> &SettingsSnapshot {
        &self.original
    }

    pub fn working_snapshot(&self) -> &SettingsSnapshot {
        &self.working
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn handle(&mut self, input: DialogInput) -> DialogUpdate<SettingsDialogAction> {
        self.handle_at(input, 0)
    }

    pub fn handle_at(
        &mut self,
        input: DialogInput,
        now_millis: u64,
    ) -> DialogUpdate<SettingsDialogAction> {
        match self.exit.feed(input, now_millis) {
            ExitFeed::Hint(key) => return DialogUpdate::ExitHint(key),
            ExitFeed::Exit => {
                return DialogUpdate::Action(SettingsDialogAction::ExitRequested {
                    snapshot: self.original.clone(),
                });
            }
            ExitFeed::Ordinary => {}
        }
        match self.mode {
            SettingsMode::Search => self.handle_search(input),
            SettingsMode::Choice => self.handle_choice(input),
            SettingsMode::Browse => self.handle_settings_browse(input),
        }
    }

    fn handle_search(&mut self, input: DialogInput) -> DialogUpdate<SettingsDialogAction> {
        match input {
            DialogInput::Escape | DialogInput::Enter => self.mode = SettingsMode::Browse,
            DialogInput::Backspace | DialogInput::Delete => {
                self.query.pop();
                self.selected = 0;
            }
            DialogInput::Character(character) => {
                push_bounded(&mut self.query, character, MAX_INPUT_CHARS);
                self.selected = 0;
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn handle_choice(&mut self, input: DialogInput) -> DialogUpdate<SettingsDialogAction> {
        let option_count = self.current_choice_options().map_or(0, Vec::len);
        match input {
            DialogInput::Escape | DialogInput::Left => self.mode = SettingsMode::Browse,
            DialogInput::Up => {
                self.choice_selected = moved_index(self.choice_selected, option_count, -1);
            }
            DialogInput::Down => {
                self.choice_selected = moved_index(self.choice_selected, option_count, 1);
            }
            DialogInput::PageUp => {
                self.choice_selected = moved_index(self.choice_selected, option_count, -8);
            }
            DialogInput::PageDown => {
                self.choice_selected = moved_index(self.choice_selected, option_count, 8);
            }
            DialogInput::Enter | DialogInput::Right => {
                let selected = self
                    .current_choice_options()
                    .and_then(|options| options.get(self.choice_selected))
                    .cloned();
                if let Some(value) = selected {
                    if let Some(SettingItem {
                        value: SettingValue::Choice { selected, .. },
                        ..
                    }) = self.current_setting_mut()
                    {
                        *selected = value;
                    }
                    self.mode = SettingsMode::Browse;
                }
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn handle_settings_browse(&mut self, input: DialogInput) -> DialogUpdate<SettingsDialogAction> {
        match input {
            DialogInput::Up => self.move_setting(-1),
            DialogInput::Down => self.move_setting(1),
            DialogInput::PageUp => self.move_setting(-8),
            DialogInput::PageDown => self.move_setting(8),
            DialogInput::Character('/') => {
                self.mode = SettingsMode::Search;
                self.query.clear();
                self.selected = 0;
            }
            DialogInput::Enter | DialogInput::Right | DialogInput::Character(' ') => {
                self.activate_setting();
            }
            DialogInput::Save | DialogInput::Character('s') => {
                return DialogUpdate::Action(SettingsDialogAction::Save {
                    snapshot: self.working.clone(),
                    changes: self.changes(),
                });
            }
            DialogInput::Escape => {
                return DialogUpdate::Action(SettingsDialogAction::Cancel {
                    snapshot: self.original.clone(),
                });
            }
            _ => {}
        }
        DialogUpdate::Continue
    }

    fn filtered_setting_indices(&self) -> Vec<usize> {
        let needle = self.query.to_lowercase();
        self.working
            .items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                (needle.is_empty()
                    || item.key.to_lowercase().contains(&needle)
                    || item.label.to_lowercase().contains(&needle)
                    || item.description.to_lowercase().contains(&needle))
                .then_some(index)
            })
            .take(MAX_DIALOG_ITEMS)
            .collect()
    }

    fn current_setting_index(&self) -> Option<usize> {
        self.filtered_setting_indices().get(self.selected).copied()
    }

    fn current_setting_mut(&mut self) -> Option<&mut SettingItem> {
        let index = self.current_setting_index()?;
        self.working.items.get_mut(index)
    }

    fn current_choice_options(&self) -> Option<&Vec<String>> {
        let index = self.current_setting_index()?;
        match &self.working.items.get(index)?.value {
            SettingValue::Choice { options, .. } => Some(options),
            SettingValue::Boolean(_) => None,
        }
    }

    fn activate_setting(&mut self) {
        let Some(setting) = self.current_setting_mut() else {
            return;
        };
        match &mut setting.value {
            SettingValue::Boolean(value) => *value = !*value,
            SettingValue::Choice { selected, options } => {
                self.choice_selected = options
                    .iter()
                    .position(|option| option == selected)
                    .unwrap_or_default();
                self.mode = SettingsMode::Choice;
            }
        }
    }

    fn move_setting(&mut self, amount: isize) {
        self.selected = moved_index(self.selected, self.filtered_setting_indices().len(), amount);
    }

    fn changes(&self) -> Vec<SettingChange> {
        self.original
            .items
            .iter()
            .filter_map(|original| {
                self.working
                    .items
                    .iter()
                    .find(|working| working.key == original.key)
                    .filter(|working| working.value != original.value)
                    .map(|working| SettingChange {
                        key: original.key.clone(),
                        before: original.value.clone(),
                        after: working.value.clone(),
                    })
            })
            .take(MAX_DIALOG_ITEMS)
            .collect()
    }

    pub fn render(&self, width: u16, height: u16) -> DialogFrame {
        let mut lines = vec!["Settings".to_owned()];
        match self.mode {
            SettingsMode::Search => lines.push(format!("Search: {}_", self.query)),
            SettingsMode::Browse if !self.query.is_empty() => {
                lines.push(format!("Filter: {}", self.query));
            }
            _ => lines.push(String::new()),
        }
        if self.mode == SettingsMode::Choice {
            self.render_choices(&mut lines, height);
        } else {
            self.render_settings(&mut lines, height);
        }
        DialogFrame::new(lines, None, width, height)
    }

    fn render_settings(&self, lines: &mut Vec<String>, height: u16) {
        let indices = self.filtered_setting_indices();
        let list_rows = usize::from(height.max(1)).saturating_sub(3).max(1);
        let start = centered_start(self.selected, indices.len(), list_rows);
        if indices.is_empty() {
            lines.push("  No matching settings".to_owned());
        } else {
            for (visible, index) in indices.iter().skip(start).take(list_rows).enumerate() {
                let item = &self.working.items[*index];
                let marker = if start + visible == self.selected {
                    ">"
                } else {
                    " "
                };
                lines.push(format!("{marker} {}: {}", item.label, item.value.display()));
            }
        }
        lines.push(
            "↑/↓ select · Enter toggle/choose · / search · s/Ctrl-S save · Esc cancel".to_owned(),
        );
    }

    fn render_choices(&self, lines: &mut Vec<String>, height: u16) {
        let Some(setting_index) = self.current_setting_index() else {
            lines.push("No setting selected".to_owned());
            return;
        };
        let setting = &self.working.items[setting_index];
        lines.push(format!("Choose {}", setting.label));
        let SettingValue::Choice { options, .. } = &setting.value else {
            return;
        };
        let list_rows = usize::from(height.max(1)).saturating_sub(4).max(1);
        let start = centered_start(self.choice_selected, options.len(), list_rows);
        for (visible, option) in options.iter().skip(start).take(list_rows).enumerate() {
            let marker = if start + visible == self.choice_selected {
                ">"
            } else {
                " "
            };
            lines.push(format!("{marker} {option}"));
        }
        lines.push("↑/↓ select · Enter apply · ←/Esc back".to_owned());
    }
}

// ---- Shared bounded rendering helpers ----------------------------------

fn clean_text(input: &str, max_chars: usize) -> String {
    let mut output = String::new();
    for character in input.chars() {
        if output.chars().count() >= max_chars {
            break;
        }
        match character {
            '\n' | '\r' | '\t' => output.push(' '),
            character if character.is_control() => {}
            character => output.push(character),
        }
    }
    output
}

fn push_bounded(target: &mut String, character: char, max_chars: usize) {
    if !character.is_control() && target.chars().count() < max_chars {
        target.push(character);
    }
}

fn moved_index(current: usize, count: usize, amount: isize) -> usize {
    if count == 0 {
        return 0;
    }
    if amount.is_negative() {
        current.saturating_sub(amount.unsigned_abs()).min(count - 1)
    } else {
        current.saturating_add(amount as usize).min(count - 1)
    }
}

fn centered_start(selected: usize, count: usize, visible: usize) -> usize {
    if count <= visible {
        return 0;
    }
    selected
        .saturating_sub(visible / 2)
        .min(count.saturating_sub(visible))
}

fn fit_line(input: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let clean = clean_text(input, MAX_DETAIL_CHARS);
    let mut output = String::new();
    let mut cells = 0usize;
    let mut truncated = false;
    for character in clean.chars() {
        let char_width = UnicodeWidthChar::width(character).unwrap_or_default();
        if cells.saturating_add(char_width) > width {
            truncated = true;
            break;
        }
        output.push(character);
        cells = cells.saturating_add(char_width);
    }
    if truncated && width > 0 {
        while cells >= width {
            let Some(last) = output.pop() else {
                break;
            };
            cells = cells.saturating_sub(UnicodeWidthChar::width(last).unwrap_or_default());
        }
        output.push('…');
    }
    output
}

fn wrap_text(input: &str, width: usize, max_lines: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut current = String::new();
    let mut cells = 0usize;
    for character in clean_text(input, MAX_DETAIL_CHARS).chars() {
        let char_width = UnicodeWidthChar::width(character).unwrap_or_default();
        if !current.is_empty() && cells.saturating_add(char_width) > width {
            lines.push(current);
            if lines.len() >= max_lines {
                return lines;
            }
            current = String::new();
            cells = 0;
        }
        current.push(character);
        cells = cells.saturating_add(char_width);
    }
    if !current.is_empty() && lines.len() < max_lines {
        lines.push(current);
    }
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn permission_item(id: &str, label: &str) -> PermissionDialogItem {
        PermissionDialogItem::new(id, label, "detail")
    }

    #[test]
    fn permission_tabs_search_add_delete_and_actions_are_typed() {
        let mut dialog = PermissionManagerDialog::new(PermissionDialogData {
            recent: vec![permission_item("recent-1", "Bash denied")],
            allow: vec![permission_item("allow-1", "Read(*)")],
            ask: vec![],
            deny: vec![],
            workspace: vec![permission_item("workspace-1", "/workspace")],
        });
        assert_eq!(dialog.tab(), PermissionTab::Recent);
        assert_eq!(
            dialog.handle(DialogInput::Enter),
            DialogUpdate::Action(PermissionManagerAction::OpenRecent {
                id: "recent-1".to_owned()
            })
        );
        dialog.handle(DialogInput::Right);
        assert_eq!(dialog.tab(), PermissionTab::Allow);
        dialog.handle(DialogInput::Character('/'));
        for character in "read".chars() {
            dialog.handle(DialogInput::Character(character));
        }
        assert_eq!(dialog.query(), "read");
        dialog.handle(DialogInput::Escape);
        assert_eq!(
            dialog.handle(DialogInput::Character('x')),
            DialogUpdate::Continue
        );
        assert_eq!(
            dialog.handle(DialogInput::Enter),
            DialogUpdate::Action(PermissionManagerAction::DeleteRule {
                tab: PermissionTab::Allow,
                id: "allow-1".to_owned(),
                rule: "Read(*)".to_owned(),
            })
        );
        dialog.handle(DialogInput::Character('a'));
        for character in "Bash(git status)".chars() {
            dialog.handle(DialogInput::Character(character));
        }
        assert_eq!(
            dialog.handle(DialogInput::Enter),
            DialogUpdate::Action(PermissionManagerAction::AddRule {
                tab: PermissionTab::Allow,
                rule: "Bash(git status)".to_owned(),
            })
        );
    }

    #[test]
    fn permission_workspace_actions_are_not_rule_actions() {
        let mut dialog = PermissionManagerDialog::new(PermissionDialogData {
            workspace: vec![permission_item("ws", "/safe")],
            ..PermissionDialogData::default()
        });
        for _ in 0..3 {
            dialog.handle(DialogInput::Right);
        }
        assert_eq!(dialog.tab(), PermissionTab::Workspace);
        assert_eq!(
            dialog.handle(DialogInput::Character('x')),
            DialogUpdate::Continue
        );
        assert_eq!(
            dialog.handle(DialogInput::Enter),
            DialogUpdate::Action(PermissionManagerAction::RemoveWorkspace {
                id: "ws".to_owned(),
                path: "/safe".to_owned(),
            })
        );
    }

    fn task(id: &str, category: TaskCategory, foreground: bool) -> TaskDialogItem {
        TaskDialogItem {
            id: id.to_owned(),
            title: format!("task {id}"),
            detail: "bounded detail".to_owned(),
            category,
            state: TaskState::Running,
            can_foreground: foreground,
            has_output: true,
        }
    }

    #[test]
    fn single_task_opens_detail_and_task_actions_are_typed() {
        let mut dialog = TaskDialog::new(vec![task("one", TaskCategory::Agent, true)]);
        assert!(dialog.is_detail());
        assert_eq!(
            dialog.handle(DialogInput::Character('f')),
            DialogUpdate::Action(TaskDialogAction::Foreground {
                id: "one".to_owned()
            })
        );
        assert_eq!(
            dialog.handle(DialogInput::Character('x')),
            DialogUpdate::Action(TaskDialogAction::Stop {
                id: "one".to_owned()
            })
        );
    }

    #[test]
    fn multi_task_list_navigates_into_and_back_from_detail() {
        let mut dialog = TaskDialog::new(vec![
            task("one", TaskCategory::Agent, false),
            task("two", TaskCategory::Shell, false),
        ]);
        assert!(!dialog.is_detail());
        dialog.handle(DialogInput::Down);
        dialog.handle(DialogInput::Enter);
        assert!(dialog.is_detail());
        assert_eq!(
            dialog.handle(DialogInput::Character('f')),
            DialogUpdate::Action(TaskDialogAction::ShowOutput {
                id: "two".to_owned()
            })
        );
        dialog.handle(DialogInput::Left);
        assert!(!dialog.is_detail());
    }

    fn settings() -> SettingsSnapshot {
        SettingsSnapshot::new(vec![
            SettingItem {
                key: "enabled".to_owned(),
                label: "Enabled".to_owned(),
                description: "toggle".to_owned(),
                value: SettingValue::Boolean(false),
            },
            SettingItem {
                key: "theme".to_owned(),
                label: "Theme".to_owned(),
                description: "palette".to_owned(),
                value: SettingValue::Choice {
                    selected: "dark".to_owned(),
                    options: vec!["dark".to_owned(), "light".to_owned()],
                },
            },
        ])
    }

    #[test]
    fn settings_toggle_choice_save_and_cancel_preserve_snapshots() {
        let original = settings();
        let mut dialog = SettingsDialog::new(original.clone());
        dialog.handle(DialogInput::Enter);
        dialog.handle(DialogInput::Down);
        dialog.handle(DialogInput::Enter);
        dialog.handle(DialogInput::Down);
        dialog.handle(DialogInput::Enter);
        let DialogUpdate::Action(SettingsDialogAction::Save { snapshot, changes }) =
            dialog.handle(DialogInput::Save)
        else {
            panic!("expected save action");
        };
        assert_eq!(changes.len(), 2);
        assert_ne!(snapshot, original);
        assert_eq!(
            dialog.handle(DialogInput::Escape),
            DialogUpdate::Action(SettingsDialogAction::Cancel { snapshot: original })
        );
    }

    #[test]
    fn settings_search_is_bounded_and_filters() {
        let mut dialog = SettingsDialog::new(settings());
        dialog.handle(DialogInput::Character('/'));
        for _ in 0..(MAX_INPUT_CHARS + 20) {
            dialog.handle(DialogInput::Character('x'));
        }
        assert_eq!(dialog.query().chars().count(), MAX_INPUT_CHARS);
        let frame = dialog.render(12, 4);
        assert!(frame.lines().len() <= 4);
        assert!(frame.lines().iter().all(|line| display_width(line) <= 12));
    }

    #[test]
    fn every_dialog_uses_same_bounded_double_interrupt_window() {
        let mut permission = PermissionManagerDialog::new(PermissionDialogData::default());
        assert_eq!(
            permission.handle_at(DialogInput::CtrlC, 100),
            DialogUpdate::ExitHint(ExitKey::CtrlC)
        );
        assert_eq!(
            permission.handle_at(DialogInput::CtrlC, 899),
            DialogUpdate::Action(PermissionManagerAction::ExitRequested)
        );

        let mut tasks = TaskDialog::new(Vec::new());
        assert_eq!(
            tasks.handle_at(DialogInput::CtrlD, 100),
            DialogUpdate::ExitHint(ExitKey::CtrlD)
        );
        assert_eq!(
            tasks.handle_at(DialogInput::CtrlD, 900),
            DialogUpdate::ExitHint(ExitKey::CtrlD)
        );

        let snapshot = settings();
        let mut settings = SettingsDialog::new(snapshot.clone());
        assert_eq!(
            settings.handle_at(DialogInput::CtrlD, 100),
            DialogUpdate::ExitHint(ExitKey::CtrlD)
        );
        assert_eq!(
            settings.handle_at(DialogInput::CtrlD, 899),
            DialogUpdate::Action(SettingsDialogAction::ExitRequested { snapshot })
        );
    }

    #[test]
    fn frames_strip_controls_and_are_safe_at_one_column_one_row() {
        let dialog = PermissionManagerDialog::new(PermissionDialogData {
            allow: vec![PermissionDialogItem::new("id", "bad\x1b[31m", "line\nnext")],
            ..PermissionDialogData::default()
        });
        let frame = dialog.render(1, 1);
        assert_eq!(frame.lines().len(), 1);
        assert!(
            frame.lines()[0]
                .chars()
                .all(|character| !character.is_control())
        );
        assert!(display_width(&frame.lines()[0]) <= 1);
    }

    #[test]
    fn alternate_screen_renderer_enters_draws_and_leaves_once() {
        let frame = DialogFrame::new(vec!["hello".to_owned()], None, 5, 2);
        let mut renderer = AlternateScreenRenderer::new(5, 2);
        let mut output = Vec::new();
        renderer.draw(&mut output, &frame).unwrap();
        renderer.leave(&mut output).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert_eq!(output.matches("\x1b[?1049h").count(), 1);
        assert_eq!(output.matches("\x1b[?1049l").count(), 1);
        assert!(output.contains("hello"));
    }

    fn display_width(line: &str) -> usize {
        line.chars()
            .map(|character| UnicodeWidthChar::width(character).unwrap_or_default())
            .sum()
    }
}

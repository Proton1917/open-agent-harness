use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_KEYBINDINGS_BYTES: u64 = 1024 * 1024;
const MAX_BINDING_BLOCKS: usize = 64;
const MAX_BINDINGS: usize = 1024;
const MAX_CHORD_KEYS: usize = 4;
const MAX_KEY_SPEC_BYTES: usize = 128;
const MAX_ACTION_BYTES: usize = 128;
const RELOAD_INTERVAL: Duration = Duration::from_millis(250);
const CHORD_TIMEOUT: Duration = Duration::from_secs(1);

pub const KEYBINDING_CONTEXTS: &[&str] = &[
    "Global",
    "Chat",
    "Autocomplete",
    "Confirmation",
    "Help",
    "Transcript",
    "HistorySearch",
    "Task",
    "ThemePicker",
    "Settings",
    "Tabs",
    "Attachments",
    "Footer",
    "MessageSelector",
    "DiffDialog",
    "DiffPanel",
    "ModelPicker",
    "Select",
    "Plugin",
    "Scroll",
];

const KEYBINDING_ACTIONS: &[&str] = &[
    "app:interrupt",
    "app:exit",
    "app:toggleTodos",
    "app:toggleTranscript",
    "app:toggleBrief",
    "app:toggleReplTab",
    "app:toggleDiffNoiseFilter",
    "app:diffFileListUp",
    "app:diffFileListDown",
    "app:toggleDiffPreSession",
    "app:cycleDiffBase",
    "app:toggleTerminal",
    "app:redraw",
    "app:openArtifact",
    "app:quickOpen",
    "app:globalSearch",
    "history:search",
    "history:previous",
    "history:next",
    "chat:cancel",
    "chat:killAgents",
    "chat:cycleMode",
    "chat:modelPicker",
    "chat:fastMode",
    "chat:thinkingToggle",
    "chat:workflowKeywordToggle",
    "chat:submit",
    "chat:newline",
    "chat:undo",
    "chat:externalEditor",
    "chat:stash",
    "chat:imagePaste",
    "chat:clearInput",
    "chat:clearScreen",
    "autocomplete:accept",
    "autocomplete:dismiss",
    "autocomplete:previous",
    "autocomplete:next",
    "confirm:yes",
    "confirm:no",
    "confirm:previous",
    "confirm:next",
    "confirm:nextField",
    "confirm:previousField",
    "confirm:cycleMode",
    "confirm:toggle",
    "confirm:toggleExplanation",
    "tabs:next",
    "tabs:previous",
    "transcript:toggleShowAll",
    "transcript:exit",
    "historySearch:next",
    "historySearch:accept",
    "historySearch:cancel",
    "historySearch:execute",
    "historySearch:cycleScope",
    "task:background",
    "theme:toggleSyntaxHighlighting",
    "theme:editCustom",
    "help:dismiss",
    "attachments:next",
    "attachments:previous",
    "attachments:remove",
    "attachments:exit",
    "footer:up",
    "footer:down",
    "footer:next",
    "footer:previous",
    "footer:openSelected",
    "footer:clearSelection",
    "footer:close",
    "messageSelector:up",
    "messageSelector:down",
    "messageSelector:top",
    "messageSelector:bottom",
    "messageSelector:select",
    "diff:dismiss",
    "diff:previousSource",
    "diff:nextSource",
    "diff:back",
    "diff:viewDetails",
    "diff:previousFile",
    "diff:nextFile",
    "modelPicker:decreaseEffort",
    "modelPicker:increaseEffort",
    "modelPicker:thisSessionOnly",
    "select:next",
    "select:previous",
    "select:pageUp",
    "select:pageDown",
    "select:first",
    "select:last",
    "select:accept",
    "select:cancel",
    "plugin:toggle",
    "plugin:install",
    "plugin:favorite",
    "permission:toggleDebug",
    "settings:search",
    "settings:retry",
    "settings:periodDay",
    "settings:periodWeek",
    "settings:sortByTokens",
    "voice:pushToTalk",
    "scroll:pageUp",
    "scroll:pageDown",
    "scroll:lineUp",
    "scroll:lineDown",
    "scroll:top",
    "scroll:bottom",
    "scroll:halfPageUp",
    "scroll:halfPageDown",
    "scroll:fullPageUp",
    "scroll:fullPageDown",
    "selection:copy",
    "selection:clear",
    "selection:extendLeft",
    "selection:extendRight",
    "selection:extendUp",
    "selection:extendDown",
    "selection:extendLineStart",
    "selection:extendLineEnd",
];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyName {
    Char(char),
    Enter,
    Tab,
    Escape,
    Backspace,
    Delete,
    Insert,
    Up,
    Down,
    Left,
    Right,
    Home,
    End,
    PageUp,
    PageDown,
    WheelUp,
    WheelDown,
    CapsLock,
    Function(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Keystroke {
    key: KeyName,
    control: bool,
    alt: bool,
    shift: bool,
    super_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Binding {
    context: String,
    chord: Vec<Keystroke>,
    action: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileFingerprint([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyResolution {
    Match(String),
    Unbound,
    ChordStarted,
    ChordCancelled,
    None,
}

pub struct KeybindingManager {
    path: Option<PathBuf>,
    bindings: Vec<Binding>,
    observed: Option<FileFingerprint>,
    custom_loaded: bool,
    pending: Vec<Keystroke>,
    pending_since: Option<Instant>,
    last_reload_check: Option<Instant>,
    warning: Option<String>,
}

impl Default for KeybindingManager {
    fn default() -> Self {
        Self::new(None)
    }
}

impl KeybindingManager {
    pub fn new(path: Option<PathBuf>) -> Self {
        let mut manager = Self {
            path,
            bindings: default_bindings(),
            observed: None,
            custom_loaded: false,
            pending: Vec::new(),
            pending_since: None,
            last_reload_check: None,
            warning: None,
        };
        manager.reload_if_due(true);
        manager
    }

    pub fn default_user_path() -> Option<PathBuf> {
        dirs::home_dir().map(|home| home.join(".open-agent-harness/keybindings.json"))
    }

    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn take_warning(&mut self) -> Option<String> {
        self.warning.take()
    }

    pub fn reload_if_due(&mut self, force: bool) -> bool {
        let now = Instant::now();
        if !force
            && self
                .last_reload_check
                .is_some_and(|checked| now.duration_since(checked) < RELOAD_INTERVAL)
        {
            return false;
        }
        self.last_reload_check = Some(now);
        let Some(path) = self.path.as_deref() else {
            return false;
        };

        let bytes = match read_keybindings_file(path) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                let changed = self.custom_loaded || self.observed.take().is_some();
                if changed {
                    self.bindings = default_bindings();
                    self.custom_loaded = false;
                    self.clear_chord();
                    self.warning = Some("Keybindings file removed; defaults restored".to_owned());
                }
                return changed;
            }
            Err(error) => {
                self.warning = Some(format!("Keybindings unchanged: {error:#}"));
                return false;
            }
        };
        let fingerprint = FileFingerprint(Sha256::digest(&bytes).into());
        if !force && self.observed == Some(fingerprint) {
            return false;
        }
        self.observed = Some(fingerprint);
        match parse_user_bindings(&bytes) {
            Ok(user) => {
                let mut bindings = default_bindings();
                bindings.extend(user);
                self.bindings = collapse_bindings(bindings);
                self.custom_loaded = true;
                self.clear_chord();
                if !force {
                    self.warning = Some("Keybindings reloaded".to_owned());
                }
                true
            }
            Err(error) => {
                self.warning = Some(format!("Keybindings unchanged: {error:#}"));
                false
            }
        }
    }

    pub fn resolve(&mut self, event: KeyEvent, contexts: &[&str]) -> KeyResolution {
        let Some(key) = Keystroke::from_event(event) else {
            return KeyResolution::None;
        };
        self.resolve_keystroke(key, contexts)
    }

    pub fn resolve_wheel(&mut self, up: bool, contexts: &[&str]) -> KeyResolution {
        self.resolve_keystroke(
            Keystroke {
                key: if up {
                    KeyName::WheelUp
                } else {
                    KeyName::WheelDown
                },
                control: false,
                alt: false,
                shift: false,
                super_key: false,
            },
            contexts,
        )
    }

    fn resolve_keystroke(&mut self, key: Keystroke, contexts: &[&str]) -> KeyResolution {
        if self
            .pending_since
            .is_some_and(|started| started.elapsed() > CHORD_TIMEOUT)
        {
            self.clear_chord();
        }
        if !self.pending.is_empty() && key.key == KeyName::Escape {
            self.clear_chord();
            return KeyResolution::ChordCancelled;
        }
        let had_pending = !self.pending.is_empty();
        let mut candidate = self.pending.clone();
        candidate.push(key);

        for context in contexts {
            let relevant = self
                .bindings
                .iter()
                .filter(|binding| binding.context == *context)
                .collect::<Vec<_>>();
            if relevant.iter().any(|binding| {
                binding.chord.len() > candidate.len() && binding.chord.starts_with(&candidate)
            }) {
                self.pending = candidate;
                self.pending_since = Some(Instant::now());
                return KeyResolution::ChordStarted;
            }
            if let Some(binding) = relevant
                .into_iter()
                .rev()
                .find(|binding| binding.chord == candidate)
            {
                let action = binding.action.clone();
                self.clear_chord();
                return action.map_or(KeyResolution::Unbound, KeyResolution::Match);
            }
        }
        if had_pending {
            self.clear_chord();
            KeyResolution::ChordCancelled
        } else {
            KeyResolution::None
        }
    }

    pub fn pending_display(&self) -> Option<String> {
        (!self.pending.is_empty()).then(|| {
            self.pending
                .iter()
                .map(Keystroke::display)
                .collect::<Vec<_>>()
                .join(" ")
        })
    }

    fn clear_chord(&mut self) {
        self.pending.clear();
        self.pending_since = None;
    }
}

impl Keystroke {
    fn from_event(event: KeyEvent) -> Option<Self> {
        let mut shift = event.modifiers.contains(KeyModifiers::SHIFT);
        let control = event.modifiers.contains(KeyModifiers::CONTROL);
        let key = match event.code {
            // Legacy terminal encoding cannot distinguish Ctrl-_ from Ctrl-7:
            // both arrive as byte 0x1f, which crossterm reports as Ctrl-7.
            // Canonicalize it to the documented binding spelling so the
            // default undo action also works without CSI-u/kitty keys.
            KeyCode::Char('7') if control => KeyName::Char('_'),
            KeyCode::Char(character) => KeyName::Char(character.to_ascii_lowercase()),
            KeyCode::Enter => KeyName::Enter,
            KeyCode::Tab => KeyName::Tab,
            KeyCode::BackTab => {
                shift = true;
                KeyName::Tab
            }
            KeyCode::Esc => KeyName::Escape,
            KeyCode::Backspace => KeyName::Backspace,
            KeyCode::Delete => KeyName::Delete,
            KeyCode::Insert => KeyName::Insert,
            KeyCode::Up => KeyName::Up,
            KeyCode::Down => KeyName::Down,
            KeyCode::Left => KeyName::Left,
            KeyCode::Right => KeyName::Right,
            KeyCode::Home => KeyName::Home,
            KeyCode::End => KeyName::End,
            KeyCode::PageUp => KeyName::PageUp,
            KeyCode::PageDown => KeyName::PageDown,
            KeyCode::CapsLock => KeyName::CapsLock,
            KeyCode::F(number) => KeyName::Function(number),
            _ => return None,
        };
        Some(Self {
            key,
            control,
            alt: event.modifiers.contains(KeyModifiers::ALT),
            shift,
            super_key: event.modifiers.contains(KeyModifiers::SUPER),
        })
    }

    fn display(&self) -> String {
        let mut parts = Vec::new();
        if self.control {
            parts.push("Ctrl".to_owned());
        }
        if self.alt {
            parts.push("Alt".to_owned());
        }
        if self.shift {
            parts.push("Shift".to_owned());
        }
        if self.super_key {
            parts.push("Cmd".to_owned());
        }
        parts.push(match self.key {
            KeyName::Char(character) => character.to_string(),
            KeyName::Enter => "Enter".to_owned(),
            KeyName::Tab => "Tab".to_owned(),
            KeyName::Escape => "Esc".to_owned(),
            KeyName::Backspace => "Backspace".to_owned(),
            KeyName::Delete => "Delete".to_owned(),
            KeyName::Insert => "Insert".to_owned(),
            KeyName::Up => "Up".to_owned(),
            KeyName::Down => "Down".to_owned(),
            KeyName::Left => "Left".to_owned(),
            KeyName::Right => "Right".to_owned(),
            KeyName::Home => "Home".to_owned(),
            KeyName::End => "End".to_owned(),
            KeyName::PageUp => "PageUp".to_owned(),
            KeyName::PageDown => "PageDown".to_owned(),
            KeyName::WheelUp => "WheelUp".to_owned(),
            KeyName::WheelDown => "WheelDown".to_owned(),
            KeyName::CapsLock => "CapsLock".to_owned(),
            KeyName::Function(number) => format!("F{number}"),
        });
        parts.join("+")
    }
}

fn read_keybindings_file(path: &Path) -> Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("cannot inspect {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("{} must be a regular non-symlink file", path.display())
    }
    if metadata.len() > MAX_KEYBINDINGS_BYTES {
        bail!("file exceeds {MAX_KEYBINDINGS_BYTES} bytes")
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("cannot open {}", path.display()))?;
    if !file.metadata()?.is_file() {
        bail!("{} is not a regular file", path.display())
    }
    let mut bytes = Vec::with_capacity((metadata.len() as usize).min(64 * 1024));
    file.take(MAX_KEYBINDINGS_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("cannot read {}", path.display()))?;
    if bytes.len() as u64 > MAX_KEYBINDINGS_BYTES {
        bail!("file exceeds {MAX_KEYBINDINGS_BYTES} bytes")
    }
    Ok(Some(bytes))
}

fn parse_user_bindings(bytes: &[u8]) -> Result<Vec<Binding>> {
    let root: Value = serde_json::from_slice(bytes).context("invalid JSON")?;
    let root = root.as_object().context("root must be an object")?;
    if let Some(key) = root
        .keys()
        .find(|key| !matches!(key.as_str(), "$schema" | "$docs" | "bindings"))
    {
        bail!("unknown root field {key}")
    }
    for key in ["$schema", "$docs"] {
        if root.get(key).is_some_and(|value| !value.is_string()) {
            bail!("{key} must be a string")
        }
    }
    let blocks = root
        .get("bindings")
        .and_then(Value::as_array)
        .context("bindings must be an array")?;
    if blocks.len() > MAX_BINDING_BLOCKS {
        bail!("more than {MAX_BINDING_BLOCKS} binding blocks")
    }
    let mut parsed = Vec::new();
    for block in blocks {
        let block = block
            .as_object()
            .context("binding block must be an object")?;
        if let Some(key) = block
            .keys()
            .find(|key| !matches!(key.as_str(), "context" | "bindings"))
        {
            bail!("binding block contains unknown field {key}")
        }
        let context = block
            .get("context")
            .and_then(Value::as_str)
            .context("binding context must be a string")?;
        if !KEYBINDING_CONTEXTS.contains(&context) {
            bail!("unknown binding context {context}")
        }
        let bindings = block
            .get("bindings")
            .and_then(Value::as_object)
            .context("block bindings must be an object")?;
        for (key, action) in bindings {
            if parsed.len() >= MAX_BINDINGS {
                bail!("more than {MAX_BINDINGS} bindings")
            }
            let chord = parse_chord(key)?;
            validate_reserved(&chord)?;
            let action = match action {
                Value::Null => None,
                Value::String(action) => {
                    validate_action(action, context)?;
                    Some(action.clone())
                }
                _ => bail!("binding action must be a string or null"),
            };
            parsed.push(Binding {
                context: context.to_owned(),
                chord,
                action,
            });
        }
    }
    Ok(collapse_bindings(parsed))
}

fn validate_action(action: &str, context: &str) -> Result<()> {
    if action.is_empty() || action.len() > MAX_ACTION_BYTES || action.chars().any(char::is_control)
    {
        bail!("binding action is empty, too long, or contains control characters")
    }
    if KEYBINDING_ACTIONS.contains(&action) {
        return Ok(());
    }
    if let Some(command) = action.strip_prefix("command:") {
        if context != "Chat" {
            bail!("command bindings are only valid in the Chat context")
        }
        if !command.is_empty()
            && command.len() <= 64
            && command
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "_-:".contains(character))
        {
            return Ok(());
        }
    }
    bail!("unknown binding action {action}")
}

fn parse_chord(value: &str) -> Result<Vec<Keystroke>> {
    if value.is_empty() || value.len() > MAX_KEY_SPEC_BYTES {
        bail!("key specification is empty or too long")
    }
    let parts = if value == " " {
        vec!["space"]
    } else {
        value.split_whitespace().collect::<Vec<_>>()
    };
    if parts.is_empty() || parts.len() > MAX_CHORD_KEYS {
        bail!("chord must contain 1 to {MAX_CHORD_KEYS} keys")
    }
    parts.into_iter().map(parse_keystroke).collect()
}

fn parse_keystroke(value: &str) -> Result<Keystroke> {
    let mut control = false;
    let mut alt = false;
    let mut shift = false;
    let mut super_key = false;
    let mut key = None;
    for raw in value.split('+') {
        if raw.is_empty() {
            bail!("keystroke contains an empty component")
        }
        let part = raw.to_ascii_lowercase();
        match part.as_str() {
            "ctrl" | "control" => control = true,
            "alt" | "opt" | "option" | "meta" => alt = true,
            "shift" => shift = true,
            "cmd" | "command" | "super" | "win" => super_key = true,
            _ if key.is_none() => {
                let mut parsed = parse_key_name(&part)?;
                if raw.len() == 1 && raw.as_bytes()[0].is_ascii_uppercase() {
                    shift = true;
                    parsed = KeyName::Char(
                        raw.chars()
                            .next()
                            .expect("one character")
                            .to_ascii_lowercase(),
                    );
                }
                key = Some(parsed);
            }
            _ => bail!("keystroke contains multiple keys"),
        }
    }
    let key = key.context("keystroke is missing a key")?;
    if key == KeyName::Tab && value.eq_ignore_ascii_case("backtab") {
        shift = true;
    }
    Ok(Keystroke {
        key,
        control,
        alt,
        shift,
        super_key,
    })
}

fn parse_key_name(value: &str) -> Result<KeyName> {
    let named = match value {
        "enter" | "return" => KeyName::Enter,
        "tab" | "backtab" => KeyName::Tab,
        "escape" | "esc" => KeyName::Escape,
        "backspace" => KeyName::Backspace,
        "delete" | "del" => KeyName::Delete,
        "insert" => KeyName::Insert,
        "up" | "↑" => KeyName::Up,
        "down" | "↓" => KeyName::Down,
        "left" | "←" => KeyName::Left,
        "right" | "→" => KeyName::Right,
        "home" => KeyName::Home,
        "end" => KeyName::End,
        "pageup" => KeyName::PageUp,
        "pagedown" => KeyName::PageDown,
        "wheelup" => KeyName::WheelUp,
        "wheeldown" => KeyName::WheelDown,
        "caps" | "capslock" | "caps-lock" | "caps_lock" => KeyName::CapsLock,
        "space" => KeyName::Char(' '),
        _ if value.chars().count() == 1 => {
            KeyName::Char(value.chars().next().expect("one character"))
        }
        _ if value.starts_with('f') => {
            let number = value[1..].parse::<u8>().context("invalid function key")?;
            if !(1..=24).contains(&number) {
                bail!("function key must be F1 through F24")
            }
            KeyName::Function(number)
        }
        _ => bail!("unknown key name {value}"),
    };
    Ok(named)
}

fn validate_reserved(chord: &[Keystroke]) -> Result<()> {
    if chord.len() != 1 {
        return Ok(());
    }
    let key = &chord[0];
    let plain_control = key.control && !key.alt && !key.shift && !key.super_key;
    if plain_control && matches!(key.key, KeyName::Char('c' | 'd' | 'm' | '\\')) {
        bail!("Ctrl-C, Ctrl-D, Ctrl-M, and Ctrl-\\ cannot be rebound")
    }
    if key.key == KeyName::CapsLock {
        bail!("Caps Lock is not delivered to terminal applications")
    }
    if cfg!(target_os = "macos")
        && key.super_key
        && matches!(
            key.key,
            KeyName::Char('c' | 'v' | 'x' | 'q' | 'w' | ' ') | KeyName::Tab
        )
    {
        bail!("this macOS system shortcut cannot be rebound")
    }
    Ok(())
}

fn collapse_bindings(bindings: Vec<Binding>) -> Vec<Binding> {
    let mut positions = HashMap::<(String, Vec<Keystroke>), usize>::new();
    let mut collapsed = Vec::<Binding>::new();
    for binding in bindings {
        let key = (binding.context.clone(), binding.chord.clone());
        if let Some(position) = positions.get(&key).copied() {
            collapsed[position] = binding;
        } else {
            positions.insert(key, collapsed.len());
            collapsed.push(binding);
        }
    }
    collapsed
}

fn binding(context: &str, keys: &str, action: &str) -> Binding {
    Binding {
        context: context.to_owned(),
        chord: parse_chord(keys).expect("static default keybinding"),
        action: Some(action.to_owned()),
    }
}

fn default_bindings() -> Vec<Binding> {
    let mut entries = vec![
        ("Global", "ctrl+c", "app:interrupt"),
        ("Global", "ctrl+d", "app:exit"),
        ("Global", "ctrl+t", "app:toggleTodos"),
        ("Global", "ctrl+o", "app:toggleTranscript"),
        ("Global", "ctrl+l", "app:redraw"),
        ("Global", "ctrl+r", "history:search"),
        ("Global", "ctrl+shift+p", "app:quickOpen"),
        ("Global", "cmd+shift+p", "app:quickOpen"),
        ("Global", "ctrl+shift+f", "app:globalSearch"),
        ("Global", "cmd+shift+f", "app:globalSearch"),
        ("Global", "meta+j", "app:toggleTerminal"),
        // Classic PTYs collapse Ctrl-Shift-letter into Ctrl-letter. These
        // chords keep both dialogs reachable without shadowing readline keys.
        ("Global", "ctrl+x ctrl+p", "app:quickOpen"),
        ("Global", "ctrl+x ctrl+f", "app:globalSearch"),
        ("Chat", "escape", "chat:cancel"),
        ("Chat", "cmd+k", "chat:clearScreen"),
        ("Chat", "ctrl+x ctrl+k", "chat:killAgents"),
        ("Chat", "shift+tab", "chat:cycleMode"),
        ("Chat", "meta+p", "chat:modelPicker"),
        ("Chat", "meta+o", "chat:fastMode"),
        ("Chat", "meta+t", "chat:thinkingToggle"),
        ("Chat", "enter", "chat:submit"),
        ("Chat", "ctrl+j", "chat:newline"),
        ("Chat", "up", "history:previous"),
        ("Chat", "down", "history:next"),
        ("Chat", "ctrl+_", "chat:undo"),
        ("Chat", "ctrl+-", "chat:undo"),
        ("Chat", "ctrl+shift+-", "chat:undo"),
        ("Chat", "ctrl+shift+_", "chat:undo"),
        ("Chat", "ctrl+x ctrl+e", "chat:externalEditor"),
        ("Chat", "ctrl+g", "chat:externalEditor"),
        ("Chat", "ctrl+s", "chat:stash"),
        ("Autocomplete", "tab", "autocomplete:accept"),
        ("Autocomplete", "escape", "autocomplete:dismiss"),
        ("Autocomplete", "up", "autocomplete:previous"),
        ("Autocomplete", "down", "autocomplete:next"),
        ("Settings", "escape", "confirm:no"),
        ("Settings", "up", "select:previous"),
        ("Settings", "down", "select:next"),
        ("Settings", "k", "select:previous"),
        ("Settings", "j", "select:next"),
        ("Settings", "ctrl+p", "select:previous"),
        ("Settings", "ctrl+n", "select:next"),
        ("Settings", "space", "select:accept"),
        ("Settings", "enter", "select:accept"),
        ("Settings", "/", "settings:search"),
        ("Settings", "r", "settings:retry"),
        ("Confirmation", "y", "confirm:yes"),
        ("Confirmation", "n", "confirm:no"),
        ("Confirmation", "enter", "confirm:yes"),
        ("Confirmation", "escape", "confirm:no"),
        ("Confirmation", "up", "confirm:previous"),
        ("Confirmation", "down", "confirm:next"),
        ("Confirmation", "tab", "confirm:nextField"),
        ("Confirmation", "space", "confirm:toggle"),
        ("Confirmation", "shift+tab", "confirm:cycleMode"),
        ("Confirmation", "ctrl+e", "confirm:toggleExplanation"),
        ("Tabs", "tab", "tabs:next"),
        ("Tabs", "shift+tab", "tabs:previous"),
        ("Tabs", "right", "tabs:next"),
        ("Tabs", "left", "tabs:previous"),
        ("Transcript", "ctrl+e", "transcript:toggleShowAll"),
        ("Transcript", "ctrl+c", "transcript:exit"),
        ("Transcript", "escape", "transcript:exit"),
        ("Transcript", "q", "transcript:exit"),
        ("HistorySearch", "ctrl+r", "historySearch:next"),
        ("HistorySearch", "escape", "historySearch:accept"),
        ("HistorySearch", "tab", "historySearch:accept"),
        ("HistorySearch", "ctrl+c", "historySearch:cancel"),
        ("HistorySearch", "enter", "historySearch:execute"),
        ("HistorySearch", "ctrl+s", "historySearch:cycleScope"),
        ("Task", "ctrl+x ctrl+b", "task:background"),
        ("Task", "ctrl+b", "task:background"),
        ("ThemePicker", "ctrl+t", "theme:toggleSyntaxHighlighting"),
        ("ThemePicker", "ctrl+e", "theme:editCustom"),
        ("Help", "escape", "help:dismiss"),
        ("Attachments", "right", "attachments:next"),
        ("Attachments", "left", "attachments:previous"),
        ("Attachments", "backspace", "attachments:remove"),
        ("Attachments", "delete", "attachments:remove"),
        ("Attachments", "down", "attachments:exit"),
        ("Attachments", "escape", "attachments:exit"),
        ("Footer", "up", "footer:up"),
        ("Footer", "ctrl+p", "footer:up"),
        ("Footer", "down", "footer:down"),
        ("Footer", "ctrl+n", "footer:down"),
        ("Footer", "right", "footer:next"),
        ("Footer", "left", "footer:previous"),
        ("Footer", "enter", "footer:openSelected"),
        ("Footer", "escape", "footer:clearSelection"),
        ("Footer", "x", "footer:close"),
        ("MessageSelector", "up", "messageSelector:up"),
        ("MessageSelector", "down", "messageSelector:down"),
        ("MessageSelector", "k", "messageSelector:up"),
        ("MessageSelector", "j", "messageSelector:down"),
        ("MessageSelector", "ctrl+p", "messageSelector:up"),
        ("MessageSelector", "ctrl+n", "messageSelector:down"),
        ("MessageSelector", "enter", "messageSelector:select"),
        ("DiffDialog", "escape", "diff:dismiss"),
        ("DiffDialog", "left", "diff:previousSource"),
        ("DiffDialog", "right", "diff:nextSource"),
        ("DiffDialog", "up", "diff:previousFile"),
        ("DiffDialog", "down", "diff:nextFile"),
        ("DiffDialog", "enter", "diff:viewDetails"),
        ("ModelPicker", "left", "modelPicker:decreaseEffort"),
        ("ModelPicker", "right", "modelPicker:increaseEffort"),
        ("ModelPicker", "s", "modelPicker:thisSessionOnly"),
        ("Select", "up", "select:previous"),
        ("Select", "down", "select:next"),
        ("Select", "j", "select:next"),
        ("Select", "k", "select:previous"),
        ("Select", "ctrl+n", "select:next"),
        ("Select", "ctrl+p", "select:previous"),
        ("Select", "pageup", "select:pageUp"),
        ("Select", "pagedown", "select:pageDown"),
        ("Select", "home", "select:first"),
        ("Select", "end", "select:last"),
        ("Select", "enter", "select:accept"),
        ("Select", "escape", "select:cancel"),
        ("Plugin", "space", "plugin:toggle"),
        ("Plugin", "i", "plugin:install"),
        ("Plugin", "f", "plugin:favorite"),
        ("Scroll", "pageup", "scroll:pageUp"),
        ("Scroll", "pagedown", "scroll:pageDown"),
        ("Scroll", "wheelup", "scroll:lineUp"),
        ("Scroll", "wheeldown", "scroll:lineDown"),
        ("Scroll", "ctrl+home", "scroll:top"),
        ("Scroll", "ctrl+end", "scroll:bottom"),
        ("Scroll", "ctrl+shift+c", "selection:copy"),
        ("Scroll", "cmd+c", "selection:copy"),
    ];
    if cfg!(windows) {
        entries.push(("Chat", "alt+v", "chat:imagePaste"));
    } else {
        entries.push(("Chat", "ctrl+v", "chat:imagePaste"));
        if std::env::var_os("WSL_DISTRO_NAME").is_some() {
            entries.push(("Chat", "alt+v", "chat:imagePaste"));
        }
    }
    collapse_bindings(
        entries
            .into_iter()
            .map(|(context, keys, action)| binding(context, keys, action))
            .collect(),
    )
}

pub fn template() -> &'static str {
    r#"{
  "bindings": [
    {
      "context": "Chat",
      "bindings": {
        "ctrl+x ctrl+e": "chat:externalEditor",
        "alt+p": "chat:modelPicker"
      }
    }
  ]
}
"#
}

pub fn create_default_file(path: &Path) -> Result<bool> {
    let parent = path.parent().context("keybindings path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("cannot create {}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(template().as_bytes())?;
            file.sync_all()?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                bail!("existing keybindings path is not a regular file")
            }
            Ok(false)
        }
        Err(error) => Err(error).with_context(|| format!("cannot create {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn parser_supports_aliases_chords_commands_null_and_all_contexts() {
        let parsed = parse_user_bindings(
            br#"{"bindings":[{"context":"Chat","bindings":{"control+x ctrl+k":"command:compact","opt+p":null}},{"context":"DiffPanel","bindings":{"j":"diff:nextFile"}}]}"#,
        )
        .unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].chord.len(), 2);
        assert_eq!(parsed[0].action.as_deref(), Some("command:compact"));
        assert_eq!(parsed[1].action, None);
        assert_eq!(parsed[2].context, "DiffPanel");
    }

    #[test]
    fn resolver_is_contextual_last_wins_and_supports_chords() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("keybindings.json");
        fs::write(
            &path,
            r#"{"bindings":[{"context":"Chat","bindings":{"alt+p":null,"ctrl+k ctrl+s":"chat:stash","ctrl+u":"command:status"}},{"context":"Autocomplete","bindings":{"ctrl+u":"autocomplete:next"}}]}"#,
        )
        .unwrap();
        let mut manager = KeybindingManager::new(Some(path));
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('p'), KeyModifiers::ALT),
                &["Chat", "Global"]
            ),
            KeyResolution::Unbound
        );
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('k'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::ChordStarted
        );
        assert_eq!(manager.pending_display().as_deref(), Some("Ctrl+k"));
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('s'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::Match("chat:stash".to_owned())
        );
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('u'), KeyModifiers::CONTROL),
                &["Autocomplete", "Chat", "Global"]
            ),
            KeyResolution::Match("autocomplete:next".to_owned())
        );
    }

    #[test]
    fn legacy_ctrl_underscore_byte_resolves_documented_undo_binding() {
        let mut manager = KeybindingManager::new(None);
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('7'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::Match("chat:undo".to_owned())
        );
    }

    #[test]
    fn workspace_search_defaults_include_source_shortcuts_and_pty_fallbacks() {
        let mut manager = KeybindingManager::new(None);
        assert_eq!(
            manager.resolve(
                key(
                    KeyCode::Char('p'),
                    KeyModifiers::CONTROL | KeyModifiers::SHIFT
                ),
                &["Chat", "Global"]
            ),
            KeyResolution::Match("app:quickOpen".to_owned())
        );
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('x'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::ChordStarted
        );
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('f'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::Match("app:globalSearch".to_owned())
        );
    }

    #[test]
    fn terminal_panel_uses_the_source_meta_j_shortcut() {
        let mut manager = KeybindingManager::new(None);
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('j'), KeyModifiers::ALT),
                &["Chat", "Global"]
            ),
            KeyResolution::Match("app:toggleTerminal".to_owned())
        );
    }

    #[test]
    fn invalid_second_chord_key_is_cancelled_without_replay() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("keybindings.json");
        fs::write(
            &path,
            r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+k ctrl+s":"chat:stash","ctrl+u":"command:status"}}]}"#,
        )
        .unwrap();
        let mut manager = KeybindingManager::new(Some(path));
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('k'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::ChordStarted
        );
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('u'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::ChordCancelled
        );
    }

    #[test]
    fn invalid_reload_keeps_last_valid_bindings_and_delete_restores_defaults() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("keybindings.json");
        fs::write(
            &path,
            r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+u":"command:status"}}]}"#,
        )
        .unwrap();
        let mut manager = KeybindingManager::new(Some(path.clone()));
        assert!(matches!(
            manager.resolve(
                key(KeyCode::Char('u'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::Match(_)
        ));
        fs::write(&path, "not json").unwrap();
        assert!(!manager.reload_if_due(true));
        assert!(manager.take_warning().unwrap().contains("unchanged"));
        assert!(matches!(
            manager.resolve(
                key(KeyCode::Char('u'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::Match(_)
        ));
        fs::remove_file(&path).unwrap();
        assert!(manager.reload_if_due(true));
        assert_eq!(
            manager.resolve(
                key(KeyCode::Char('u'), KeyModifiers::CONTROL),
                &["Chat", "Global"]
            ),
            KeyResolution::None
        );
    }

    #[test]
    fn reserved_context_and_action_validation_fail_closed() {
        for content in [
            r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+c":"chat:stash"}}]}"#,
            r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+m":"chat:stash"}}]}"#,
            r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+\\":"chat:stash"}}]}"#,
            r#"{"bindings":[{"context":"Chat","bindings":{"capslock":"chat:stash"}}]}"#,
            r#"{"bindings":[{"context":"Unknown","bindings":{}}]}"#,
            r#"{"bindings":[{"context":"Chat","bindings":{"ctrl+u":"unknown:action"}}]}"#,
            r#"{"bindings":[{"context":"Global","bindings":{"ctrl+u":"command:status"}}]}"#,
            r#"{"unknown":true,"bindings":[]}"#,
        ] {
            assert!(
                parse_user_bindings(content.as_bytes()).is_err(),
                "accepted invalid configuration: {content}"
            );
        }
    }

    #[test]
    fn default_file_is_private_and_never_overwritten() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("config/keybindings.json");
        assert!(create_default_file(&path).unwrap());
        assert!(!create_default_file(&path).unwrap());
        assert_eq!(fs::read_to_string(&path).unwrap(), template());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}

//! Provider-neutral terminal UI settings and private persistence.
//!
//! This module deliberately contains no renderer or process-execution code. In
//! particular, [`StatusLineConfig`] only describes and validates a command; the
//! eventual status-line runtime must apply its own trust, timeout, environment,
//! input, output, and process-tree limits before execution.

use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    permissions::UserPermissionRules,
    protocol::ReasoningEffort,
    terminal_notifications::{
        DEFAULT_IDLE_NOTIFICATION_THRESHOLD_MS, MAX_IDLE_NOTIFICATION_THRESHOLD_MS,
        MIN_IDLE_NOTIFICATION_THRESHOLD_MS, NotificationChannel,
    },
};

pub const UI_SETTINGS_FILE_NAME: &str = "ui-settings.json";
pub const MAX_UI_SETTINGS_BYTES: u64 = 64 * 1024;
pub const MAX_STATUS_LINE_COMMAND_BYTES: usize = 4 * 1024;
pub const MAX_STATUS_LINE_PADDING: u8 = 16;
pub const MAX_STATUS_LINE_REFRESH_SECONDS: u64 = 24 * 60 * 60;
pub const MAX_OUTPUT_STYLE_NAME_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EditorMode {
    #[default]
    Normal,
    Vim,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TuiMode {
    #[default]
    Default,
    Fullscreen,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ThemePreset {
    #[default]
    Auto,
    Dark,
    Light,
    #[serde(alias = "daltonized")]
    DarkDaltonized,
    LightDaltonized,
    DarkAnsi,
    LightAnsi,
    NoColor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StatusLineConfig {
    pub command: String,
    #[serde(default)]
    pub padding: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_interval: Option<u64>,
    #[serde(default)]
    pub hide_vim_mode_indicator: bool,
}

impl StatusLineConfig {
    pub fn validate(&self) -> Result<()> {
        let command = self.command.as_bytes();
        if command.is_empty() {
            bail!("statusLine.command must not be empty")
        }
        if command.len() > MAX_STATUS_LINE_COMMAND_BYTES {
            bail!("statusLine.command exceeds the {MAX_STATUS_LINE_COMMAND_BYTES}-byte limit")
        }
        if self
            .command
            .chars()
            .any(|character| character == '\0' || character == '\n' || character == '\r')
        {
            bail!("statusLine.command must be a single line without NUL bytes")
        }
        if self.padding > MAX_STATUS_LINE_PADDING {
            bail!("statusLine.padding exceeds the {MAX_STATUS_LINE_PADDING}-column limit")
        }
        if let Some(seconds) = self.refresh_interval {
            if !(1..=MAX_STATUS_LINE_REFRESH_SECONDS).contains(&seconds) {
                bail!(
                    "statusLine.refreshInterval must be between 1 and {MAX_STATUS_LINE_REFRESH_SECONDS} seconds"
                )
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UiSettings {
    #[serde(default)]
    pub editor_mode: EditorMode,
    #[serde(default)]
    pub tui_mode: TuiMode,
    #[serde(default)]
    pub theme: ThemePreset,
    #[serde(default = "default_true")]
    pub copy_on_select: bool,
    #[serde(default = "default_true")]
    pub syntax_highlighting: bool,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub prompt_suggestion_enabled: bool,
    /// Explicit local replacement for the source build's remote experiment flag.
    #[serde(default)]
    pub terminal_panel_enabled: bool,
    #[serde(default)]
    pub preferred_notif_channel: NotificationChannel,
    #[serde(default = "default_idle_notification_threshold_ms")]
    pub message_idle_notif_threshold_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_line: Option<StatusLineConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_style: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "UserPermissionRules::is_empty")]
    pub permission_rules: UserPermissionRules,
}

impl Default for UiSettings {
    fn default() -> Self {
        Self {
            editor_mode: EditorMode::default(),
            tui_mode: TuiMode::default(),
            theme: ThemePreset::default(),
            copy_on_select: true,
            syntax_highlighting: true,
            verbose: false,
            prompt_suggestion_enabled: false,
            terminal_panel_enabled: false,
            preferred_notif_channel: NotificationChannel::default(),
            message_idle_notif_threshold_ms: DEFAULT_IDLE_NOTIFICATION_THRESHOLD_MS,
            status_line: None,
            output_style: None,
            reasoning_effort: None,
            permission_rules: UserPermissionRules::default(),
        }
    }
}

impl UiSettings {
    pub fn validate(&self) -> Result<()> {
        if let Some(status_line) = &self.status_line {
            status_line.validate()?;
        }
        if !(MIN_IDLE_NOTIFICATION_THRESHOLD_MS..=MAX_IDLE_NOTIFICATION_THRESHOLD_MS)
            .contains(&self.message_idle_notif_threshold_ms)
        {
            bail!(
                "messageIdleNotifThresholdMs must be between {MIN_IDLE_NOTIFICATION_THRESHOLD_MS} and {MAX_IDLE_NOTIFICATION_THRESHOLD_MS}"
            )
        }
        if let Some(output_style) = &self.output_style {
            validate_output_style(output_style)?;
        }
        if let Some(effort) = &self.reasoning_effort {
            if ReasoningEffort::parse(effort)?.is_none() {
                bail!("reasoningEffort must be low, medium, high, xhigh, or max when persisted")
            }
        }
        self.permission_rules.validate()?;
        Ok(())
    }

    /// Applies one setting transactionally. Project-originated UI settings are
    /// rejected rather than merged, so a workspace cannot alter the user's
    /// terminal behavior or use this surface to select models or permissions.
    pub fn apply_setting(&mut self, source: UiSettingSource, key: &str, value: &str) -> Result<()> {
        if source != UiSettingSource::User {
            bail!("UI settings are user-only; project settings cannot modify them")
        }

        let mut next = self.clone();
        match key {
            "editorMode" => next.editor_mode = parse_editor_mode(value)?,
            "tuiMode" => next.tui_mode = parse_tui_mode(value)?,
            "theme" => next.theme = parse_theme(value)?,
            "copyOnSelect" => {
                next.copy_on_select = value
                    .parse::<bool>()
                    .context("copyOnSelect must be true or false")?;
            }
            "syntaxHighlighting" => {
                next.syntax_highlighting = value
                    .parse::<bool>()
                    .context("syntaxHighlighting must be true or false")?;
            }
            "verbose" => {
                next.verbose = value
                    .parse::<bool>()
                    .context("verbose must be true or false")?;
            }
            "promptSuggestionEnabled" => {
                next.prompt_suggestion_enabled = value
                    .parse::<bool>()
                    .context("promptSuggestionEnabled must be true or false")?;
            }
            "terminalPanelEnabled" => {
                next.terminal_panel_enabled = value
                    .parse::<bool>()
                    .context("terminalPanelEnabled must be true or false")?;
            }
            "preferredNotifChannel" => {
                next.preferred_notif_channel = NotificationChannel::parse(value)?;
            }
            "messageIdleNotifThresholdMs" => {
                next.message_idle_notif_threshold_ms = value
                    .parse::<u64>()
                    .context("messageIdleNotifThresholdMs must be an unsigned integer")?;
            }
            "outputStyle" => {
                next.output_style = if matches!(value.trim(), "default" | "null" | "none") {
                    None
                } else {
                    validate_output_style(value)?;
                    Some(value.to_owned())
                };
            }
            "reasoningEffort" => {
                next.reasoning_effort =
                    ReasoningEffort::parse(value)?.map(|effort| effort.as_str().to_owned());
            }
            "statusLine" => {
                next.status_line = if value.trim() == "null" {
                    None
                } else {
                    Some(
                        serde_json::from_str(value)
                            .context("statusLine must be a strict JSON object or null")?,
                    )
                };
            }
            "statusLine.command" => {
                status_line_mut(&mut next, value)?.command = value.to_owned();
            }
            "statusLine.padding" => {
                let padding = value
                    .parse::<u8>()
                    .context("statusLine.padding must be an unsigned integer")?;
                status_line_mut_existing(&mut next)?.padding = padding;
            }
            "statusLine.refreshInterval" => {
                let refresh_interval = if matches!(value.trim(), "null" | "none") {
                    None
                } else {
                    Some(
                        value
                            .parse::<u64>()
                            .context("statusLine.refreshInterval must be an integer or null")?,
                    )
                };
                status_line_mut_existing(&mut next)?.refresh_interval = refresh_interval;
            }
            "statusLine.hideVimModeIndicator" => {
                let hide = value
                    .parse::<bool>()
                    .context("statusLine.hideVimModeIndicator must be true or false")?;
                status_line_mut_existing(&mut next)?.hide_vim_mode_indicator = hide;
            }
            "permissionRules" => {
                next.permission_rules = serde_json::from_str(value)
                    .context("permissionRules must be a strict JSON object")?;
            }
            "permissionRules.allow" => {
                next.permission_rules.allow = parse_permission_rule_array(value, key)?;
            }
            "permissionRules.ask" => {
                next.permission_rules.ask = parse_permission_rule_array(value, key)?;
            }
            "permissionRules.deny" => {
                next.permission_rules.deny = parse_permission_rule_array(value, key)?;
            }
            _ => bail!("unknown or unsafe UI setting key: {key}"),
        }
        next.validate()?;
        *self = next;
        Ok(())
    }
}

fn validate_output_style(value: &str) -> Result<()> {
    if value.is_empty()
        || value != value.trim()
        || value.len() > MAX_OUTPUT_STYLE_NAME_BYTES
        || value.chars().any(char::is_control)
    {
        bail!("outputStyle is empty, too long, padded, or contains control characters")
    }
    Ok(())
}

fn parse_permission_rule_array(value: &str, key: &str) -> Result<Vec<String>> {
    serde_json::from_str(value).with_context(|| format!("{key} must be a JSON string array"))
}

fn status_line_mut<'a>(
    settings: &'a mut UiSettings,
    initial_command: &str,
) -> Result<&'a mut StatusLineConfig> {
    if settings.status_line.is_none() {
        settings.status_line = Some(StatusLineConfig {
            command: initial_command.to_owned(),
            padding: 0,
            refresh_interval: None,
            hide_vim_mode_indicator: false,
        });
    }
    status_line_mut_existing(settings)
}

fn status_line_mut_existing(settings: &mut UiSettings) -> Result<&mut StatusLineConfig> {
    settings
        .status_line
        .as_mut()
        .context("configure statusLine.command before changing status-line options")
}

fn parse_editor_mode(value: &str) -> Result<EditorMode> {
    match value {
        "normal" => Ok(EditorMode::Normal),
        "vim" => Ok(EditorMode::Vim),
        _ => bail!("editorMode must be normal or vim"),
    }
}

fn parse_tui_mode(value: &str) -> Result<TuiMode> {
    match value {
        "default" => Ok(TuiMode::Default),
        "fullscreen" => Ok(TuiMode::Fullscreen),
        _ => bail!("tuiMode must be default or fullscreen"),
    }
}

fn parse_theme(value: &str) -> Result<ThemePreset> {
    match value {
        "auto" => Ok(ThemePreset::Auto),
        "dark" => Ok(ThemePreset::Dark),
        "light" => Ok(ThemePreset::Light),
        "daltonized" | "dark-daltonized" => Ok(ThemePreset::DarkDaltonized),
        "light-daltonized" => Ok(ThemePreset::LightDaltonized),
        "dark-ansi" => Ok(ThemePreset::DarkAnsi),
        "light-ansi" => Ok(ThemePreset::LightAnsi),
        "no-color" => Ok(ThemePreset::NoColor),
        _ => bail!(
            "theme must be auto, dark, light, dark-daltonized, light-daltonized, dark-ansi, light-ansi, or no-color"
        ),
    }
}

const fn default_true() -> bool {
    true
}

const fn default_idle_notification_threshold_ms() -> u64 {
    DEFAULT_IDLE_NOTIFICATION_THRESHOLD_MS
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiSettingSource {
    User,
    Project,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiSettingValueKind {
    EditorMode,
    TuiMode,
    ThemePreset,
    NotificationChannel,
    StatusLineJson,
    Text,
    UnsignedInteger,
    OptionalUnsignedInteger,
    Boolean,
    PermissionRulesJson,
    StringArray,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UiSettingSpec {
    pub key: &'static str,
    pub value_kind: UiSettingValueKind,
}

/// The complete mutable UI surface. Absence from this registry is a hard
/// rejection. Only bounded user-authored permission rules are representable;
/// permission mode, model, endpoint, environment, and tool settings are not.
pub const UI_SETTING_REGISTRY: &[UiSettingSpec] = &[
    UiSettingSpec {
        key: "editorMode",
        value_kind: UiSettingValueKind::EditorMode,
    },
    UiSettingSpec {
        key: "tuiMode",
        value_kind: UiSettingValueKind::TuiMode,
    },
    UiSettingSpec {
        key: "theme",
        value_kind: UiSettingValueKind::ThemePreset,
    },
    UiSettingSpec {
        key: "copyOnSelect",
        value_kind: UiSettingValueKind::Boolean,
    },
    UiSettingSpec {
        key: "syntaxHighlighting",
        value_kind: UiSettingValueKind::Boolean,
    },
    UiSettingSpec {
        key: "verbose",
        value_kind: UiSettingValueKind::Boolean,
    },
    UiSettingSpec {
        key: "promptSuggestionEnabled",
        value_kind: UiSettingValueKind::Boolean,
    },
    UiSettingSpec {
        key: "terminalPanelEnabled",
        value_kind: UiSettingValueKind::Boolean,
    },
    UiSettingSpec {
        key: "preferredNotifChannel",
        value_kind: UiSettingValueKind::NotificationChannel,
    },
    UiSettingSpec {
        key: "messageIdleNotifThresholdMs",
        value_kind: UiSettingValueKind::UnsignedInteger,
    },
    UiSettingSpec {
        key: "outputStyle",
        value_kind: UiSettingValueKind::Text,
    },
    UiSettingSpec {
        key: "reasoningEffort",
        value_kind: UiSettingValueKind::Text,
    },
    UiSettingSpec {
        key: "statusLine",
        value_kind: UiSettingValueKind::StatusLineJson,
    },
    UiSettingSpec {
        key: "statusLine.command",
        value_kind: UiSettingValueKind::Text,
    },
    UiSettingSpec {
        key: "statusLine.padding",
        value_kind: UiSettingValueKind::UnsignedInteger,
    },
    UiSettingSpec {
        key: "statusLine.refreshInterval",
        value_kind: UiSettingValueKind::OptionalUnsignedInteger,
    },
    UiSettingSpec {
        key: "statusLine.hideVimModeIndicator",
        value_kind: UiSettingValueKind::Boolean,
    },
    UiSettingSpec {
        key: "permissionRules",
        value_kind: UiSettingValueKind::PermissionRulesJson,
    },
    UiSettingSpec {
        key: "permissionRules.allow",
        value_kind: UiSettingValueKind::StringArray,
    },
    UiSettingSpec {
        key: "permissionRules.ask",
        value_kind: UiSettingValueKind::StringArray,
    },
    UiSettingSpec {
        key: "permissionRules.deny",
        value_kind: UiSettingValueKind::StringArray,
    },
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiSettingsScope {
    User,
    Project,
}

/// A user-only store rooted in a dedicated private directory.
#[derive(Debug, Clone)]
pub struct UiSettingsStore {
    root: PathBuf,
    path: PathBuf,
}

impl UiSettingsStore {
    pub fn for_scope(scope: UiSettingsScope, root: impl Into<PathBuf>) -> Result<Self> {
        if scope != UiSettingsScope::User {
            bail!("project UI settings stores are forbidden")
        }
        let root = root.into();
        if root.file_name().is_none() {
            bail!("UI settings root must be a dedicated directory")
        }
        Ok(Self {
            path: root.join(UI_SETTINGS_FILE_NAME),
            root,
        })
    }

    pub fn new_user(root: impl Into<PathBuf>) -> Result<Self> {
        Self::for_scope(UiSettingsScope::User, root)
    }

    pub fn default_user() -> Result<Self> {
        let root = dirs::config_dir()
            .context("cannot determine the user configuration directory")?
            .join("open-agent-harness-ui");
        Self::new_user(root)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<UiSettings> {
        match fs::symlink_metadata(&self.root) {
            Ok(_) => validate_private_directory(&self.root)?,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(UiSettings::default());
            }
            Err(error) => return Err(error.into()),
        }
        let bytes = match read_private_bounded(&self.path, MAX_UI_SETTINGS_BYTES) {
            Ok(bytes) => bytes,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == ErrorKind::NotFound) =>
            {
                return Ok(UiSettings::default());
            }
            Err(error) => return Err(error),
        };
        let settings: UiSettings =
            serde_json::from_slice(&bytes).context("invalid strict UI settings JSON")?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn save(&self, settings: &UiSettings) -> Result<()> {
        settings.validate()?;
        ensure_private_directory(&self.root)?;
        reject_non_regular_destination(&self.path)?;

        let bytes = serde_json::to_vec_pretty(settings)?;
        let bytes_len = u64::try_from(bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        if bytes_len > MAX_UI_SETTINGS_BYTES {
            bail!("serialized UI settings exceed the {MAX_UI_SETTINGS_BYTES}-byte limit")
        }

        let temporary = self
            .root
            .join(format!(".{UI_SETTINGS_FILE_NAME}.tmp-{}", Uuid::new_v4()));
        let mut cleanup = TemporaryFile::new(temporary.clone());
        let mut file = open_private_create_new(&temporary)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.flush()?;
        file.sync_all()?;
        set_private_file_permissions(&temporary)?;
        validate_private_file_metadata(&file.metadata()?)?;
        drop(file);

        validate_private_directory(&self.root)?;
        reject_non_regular_destination(&self.path)?;
        fs::rename(&temporary, &self.path).context("cannot atomically replace UI settings")?;
        cleanup.disarm();
        validate_private_file_path(&self.path)?;
        sync_directory(&self.root)?;
        Ok(())
    }
}

fn read_private_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let before = fs::symlink_metadata(path)?;
    validate_private_file_metadata(&before)?;
    let mut file = open_nofollow_read(path)?;
    let opened = file.metadata()?;
    validate_private_file_metadata(&opened)?;
    metadata_matches(&before, &opened)?;

    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        bail!("UI settings file exceeds the {limit}-byte limit")
    }

    let after = fs::symlink_metadata(path)?;
    validate_private_file_metadata(&after)?;
    metadata_matches(&opened, &after)?;
    Ok(bytes)
}

fn open_nofollow_read(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    configure_nofollow(&mut options);
    options
        .open(path)
        .with_context(|| format!("cannot open private UI settings file {}", path.display()))
}

fn open_private_create_new(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    configure_private_create(&mut options);
    options
        .open(path)
        .with_context(|| format!("cannot create private UI settings file {}", path.display()))
}

#[cfg(unix)]
fn configure_nofollow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_nofollow(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_nofollow(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_private_create(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_private_create(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_private_create(_options: &mut OpenOptions) {}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => validate_private_directory(path),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let parent = path
                .parent()
                .context("UI settings root has no parent directory")?;
            let parent_metadata = fs::metadata(parent).with_context(|| {
                format!(
                    "UI settings root parent does not exist: {}",
                    parent.display()
                )
            })?;
            if !parent_metadata.is_dir() {
                bail!("UI settings root parent is not a directory")
            }
            create_private_directory(path)?;
            validate_private_directory(path)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> Result<()> {
    fs::create_dir(path)?;
    Ok(())
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect UI settings root {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("UI settings root must be a real directory, not a symlink")
    }
    validate_private_directory_metadata(&metadata)
}

#[cfg(unix)]
fn validate_private_directory_metadata(metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    if metadata.permissions().mode() & 0o777 != 0o700 {
        bail!("UI settings root must have 0700 permissions")
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!("UI settings root must be owned by the current user")
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_directory_metadata(_metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}

fn reject_non_regular_destination(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_file_metadata(&metadata),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_private_file_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    validate_private_file_metadata(&metadata)
}

fn validate_private_file_metadata(metadata: &fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("UI settings path must be a private regular file, not a symlink")
    }
    validate_platform_private_file_metadata(metadata)
}

#[cfg(unix)]
fn validate_platform_private_file_metadata(metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    if metadata.permissions().mode() & 0o777 != 0o600 {
        bail!("UI settings file must have 0600 permissions")
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!("UI settings file must be owned by the current user")
    }
    if metadata.nlink() != 1 {
        bail!("UI settings file must not have hard links")
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_platform_private_file_metadata(_metadata: &fs::Metadata) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn metadata_matches(left: &fs::Metadata, right: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    if left.dev() != right.dev() || left.ino() != right.ino() {
        bail!("UI settings path changed while it was being read")
    }
    Ok(())
}

#[cfg(not(unix))]
fn metadata_matches(left: &fs::Metadata, right: &fs::Metadata) -> Result<()> {
    if left.len() != right.len() || left.modified().ok() != right.modified().ok() {
        bail!("UI settings path changed while it was being read")
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    let directory = File::open(path)?;
    directory.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

struct TemporaryFile {
    path: Option<PathBuf>,
}

impl TemporaryFile {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{MAX_USER_PERMISSION_RULE_BYTES, MAX_USER_PERMISSION_RULES};

    fn store_in(temp: &tempfile::TempDir) -> UiSettingsStore {
        UiSettingsStore::new_user(temp.path().join("private-ui")).unwrap()
    }

    fn configured() -> UiSettings {
        UiSettings {
            editor_mode: EditorMode::Vim,
            tui_mode: TuiMode::Fullscreen,
            theme: ThemePreset::DarkDaltonized,
            copy_on_select: false,
            syntax_highlighting: false,
            verbose: true,
            prompt_suggestion_enabled: true,
            terminal_panel_enabled: true,
            preferred_notif_channel: NotificationChannel::Ghostty,
            message_idle_notif_threshold_ms: 90_000,
            status_line: Some(StatusLineConfig {
                command: "status-helper --json".to_owned(),
                padding: 2,
                refresh_interval: Some(30),
                hide_vim_mode_indicator: true,
            }),
            output_style: Some("plugin:brief".to_owned()),
            reasoning_effort: Some("high".to_owned()),
            permission_rules: UserPermissionRules {
                allow: vec!["Bash(cargo test:*)".to_owned()],
                ask: vec!["Write(**)".to_owned()],
                deny: vec!["Read(.env)".to_owned()],
            },
        }
    }

    #[test]
    fn private_store_round_trips_strict_settings_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let store = store_in(&temp);
        let expected = configured();
        store.save(&expected).unwrap();
        assert_eq!(store.load().unwrap(), expected);
        assert!(fs::read_dir(store.root()).unwrap().all(|entry| {
            entry.unwrap().file_name() == std::ffi::OsStr::new(UI_SETTINGS_FILE_NAME)
        }));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(store.root()).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(store.path()).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn absent_file_loads_defaults_but_corrupt_and_unknown_json_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let store = store_in(&temp);
        assert_eq!(store.load().unwrap(), UiSettings::default());
        store.save(&UiSettings::default()).unwrap();
        fs::remove_file(store.path()).unwrap();
        assert_eq!(store.load().unwrap(), UiSettings::default());

        write_private_test_file(store.path(), b"{not json");
        assert!(
            store
                .load()
                .unwrap_err()
                .to_string()
                .contains("invalid strict")
        );
        write_private_test_file(store.path(), br#"{"theme":"dark","model":"unsafe"}"#);
        assert!(
            store
                .load()
                .unwrap_err()
                .to_string()
                .contains("invalid strict")
        );
        write_private_test_file(
            store.path(),
            br#"{"statusLine":{"command":"ok","unexpected":true}}"#,
        );
        assert!(
            store
                .load()
                .unwrap_err()
                .to_string()
                .contains("invalid strict")
        );
    }

    #[test]
    fn registry_is_finite_transactional_and_user_only() {
        assert!(UI_SETTING_REGISTRY.iter().all(|spec| {
            !spec.key.contains("model")
                && (!spec.key.to_ascii_lowercase().contains("permission")
                    || spec.key.starts_with("permissionRules"))
                && !spec.key.contains("env")
                && !spec.key.contains("endpoint")
        }));
        let mut settings = UiSettings::default();
        settings
            .apply_setting(UiSettingSource::User, "theme", "dark")
            .unwrap();
        assert_eq!(settings.theme, ThemePreset::Dark);
        settings
            .apply_setting(UiSettingSource::User, "copyOnSelect", "false")
            .unwrap();
        assert!(!settings.copy_on_select);
        let before = settings.clone();
        assert!(
            settings
                .apply_setting(UiSettingSource::Project, "theme", "light")
                .is_err()
        );
        assert_eq!(settings, before);
        assert!(
            settings
                .apply_setting(UiSettingSource::User, "permissions", "bypass")
                .is_err()
        );
        assert_eq!(settings, before);
        assert!(UiSettingsStore::for_scope(UiSettingsScope::Project, "/tmp/project-ui").is_err());
    }

    #[test]
    fn notification_settings_are_typed_bounded_and_transactional() {
        let mut settings = UiSettings::default();
        settings
            .apply_setting(UiSettingSource::User, "preferredNotifChannel", "kitty")
            .unwrap();
        settings
            .apply_setting(
                UiSettingSource::User,
                "messageIdleNotifThresholdMs",
                "30000",
            )
            .unwrap();
        assert_eq!(settings.preferred_notif_channel, NotificationChannel::Kitty);
        assert_eq!(settings.message_idle_notif_threshold_ms, 30_000);

        let before = settings.clone();
        assert!(
            settings
                .apply_setting(
                    UiSettingSource::User,
                    "preferredNotifChannel",
                    "run-a-command",
                )
                .is_err()
        );
        assert_eq!(settings, before);
        assert!(
            settings
                .apply_setting(UiSettingSource::User, "messageIdleNotifThresholdMs", "999",)
                .is_err()
        );
        assert_eq!(settings, before);

        let encoded = serde_json::to_value(&settings).unwrap();
        assert_eq!(encoded["preferredNotifChannel"], "kitty");
        assert_eq!(encoded["messageIdleNotifThresholdMs"], 30_000);
        assert!(
            UI_SETTING_REGISTRY
                .iter()
                .any(|spec| spec.key == "preferredNotifChannel")
        );
        assert!(
            UI_SETTING_REGISTRY
                .iter()
                .any(|spec| spec.key == "messageIdleNotifThresholdMs")
        );
    }

    #[test]
    fn prompt_suggestions_are_explicit_typed_and_transactional() {
        let mut settings = UiSettings::default();
        assert!(!settings.prompt_suggestion_enabled);
        settings
            .apply_setting(UiSettingSource::User, "promptSuggestionEnabled", "true")
            .unwrap();
        assert!(settings.prompt_suggestion_enabled);
        assert_eq!(
            serde_json::to_value(&settings).unwrap()["promptSuggestionEnabled"],
            true
        );
        assert!(
            UI_SETTING_REGISTRY
                .iter()
                .any(|spec| spec.key == "promptSuggestionEnabled")
        );

        let before = settings.clone();
        assert!(
            settings
                .apply_setting(UiSettingSource::User, "promptSuggestionEnabled", "yes")
                .is_err()
        );
        assert_eq!(settings, before);
    }

    #[test]
    fn terminal_panel_is_explicit_local_typed_and_transactional() {
        let mut settings = UiSettings::default();
        assert!(!settings.terminal_panel_enabled);
        settings
            .apply_setting(UiSettingSource::User, "terminalPanelEnabled", "true")
            .unwrap();
        assert!(settings.terminal_panel_enabled);
        assert_eq!(
            serde_json::to_value(&settings).unwrap()["terminalPanelEnabled"],
            true
        );
        assert!(
            UI_SETTING_REGISTRY
                .iter()
                .any(|spec| spec.key == "terminalPanelEnabled")
        );
        let before = settings.clone();
        assert!(
            settings
                .apply_setting(UiSettingSource::User, "terminalPanelEnabled", "remote")
                .is_err()
        );
        assert_eq!(settings, before);
        assert!(
            settings
                .apply_setting(UiSettingSource::Project, "terminalPanelEnabled", "false")
                .is_err()
        );
        assert_eq!(settings, before);
    }

    #[test]
    fn permission_rules_round_trip_and_default_stays_absent() {
        let default_json = serde_json::to_value(UiSettings::default()).unwrap();
        assert!(default_json.get("permissionRules").is_none());

        let temp = tempfile::tempdir().unwrap();
        let store = store_in(&temp);
        let expected = configured();
        store.save(&expected).unwrap();
        assert_eq!(
            store.load().unwrap().permission_rules,
            expected.permission_rules
        );

        let mut settings = UiSettings::default();
        settings
            .apply_setting(
                UiSettingSource::User,
                "permissionRules",
                r#"{"allow":["Bash(cargo:*)"],"ask":["Write(*)"],"deny":["Read(.env)"]}"#,
            )
            .unwrap();
        assert_eq!(settings.permission_rules.ask, vec!["Write(*)"]);
        settings
            .apply_setting(
                UiSettingSource::User,
                "permissionRules.allow",
                r#"["Read(src/**)"]"#,
            )
            .unwrap();
        assert_eq!(settings.permission_rules.allow, vec!["Read(src/**)"]);
    }

    #[test]
    fn permission_rule_updates_are_strict_bounded_and_transactional() {
        let mut settings = configured();
        let before = settings.clone();
        for invalid in [
            r#"{"allow":[" Read"]}"#.to_owned(),
            r#"{"allow":["Read(foo)"],"unexpected":[]}"#.to_owned(),
            format!(
                r#"{{"deny":["{}"]}}"#,
                "X".repeat(MAX_USER_PERMISSION_RULE_BYTES + 1)
            ),
        ] {
            assert!(
                settings
                    .apply_setting(UiSettingSource::User, "permissionRules", &invalid)
                    .is_err()
            );
            assert_eq!(settings, before);
        }

        let too_many = (0..=MAX_USER_PERMISSION_RULES)
            .map(|index| format!("Read(file-{index})"))
            .collect::<Vec<_>>();
        assert!(
            settings
                .apply_setting(
                    UiSettingSource::User,
                    "permissionRules.ask",
                    &serde_json::to_string(&too_many).unwrap(),
                )
                .is_err()
        );
        assert_eq!(settings, before);
        assert!(
            settings
                .apply_setting(
                    UiSettingSource::Project,
                    "permissionRules",
                    r#"{"allow":["Bash(*)"]}"#,
                )
                .is_err()
        );
        assert_eq!(settings, before);
    }

    #[test]
    fn status_line_validation_enforces_command_and_numeric_bounds() {
        let valid = configured();
        valid.validate().unwrap();
        for invalid in [
            StatusLineConfig {
                command: String::new(),
                padding: 0,
                refresh_interval: None,
                hide_vim_mode_indicator: false,
            },
            StatusLineConfig {
                command: "bad\ncommand".to_owned(),
                padding: 0,
                refresh_interval: None,
                hide_vim_mode_indicator: false,
            },
            StatusLineConfig {
                command: "x".repeat(MAX_STATUS_LINE_COMMAND_BYTES + 1),
                padding: 0,
                refresh_interval: None,
                hide_vim_mode_indicator: false,
            },
            StatusLineConfig {
                command: "ok".to_owned(),
                padding: MAX_STATUS_LINE_PADDING + 1,
                refresh_interval: None,
                hide_vim_mode_indicator: false,
            },
            StatusLineConfig {
                command: "ok".to_owned(),
                padding: 0,
                refresh_interval: Some(0),
                hide_vim_mode_indicator: false,
            },
            StatusLineConfig {
                command: "ok".to_owned(),
                padding: 0,
                refresh_interval: Some(MAX_STATUS_LINE_REFRESH_SECONDS + 1),
                hide_vim_mode_indicator: false,
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn failed_registry_update_does_not_partially_mutate_settings() {
        let mut settings = configured();
        let before = settings.clone();
        assert!(
            settings
                .apply_setting(
                    UiSettingSource::User,
                    "statusLine.padding",
                    &(u16::from(MAX_STATUS_LINE_PADDING) + 1).to_string(),
                )
                .is_err()
        );
        assert_eq!(settings, before);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_file_and_root_are_rejected_without_following() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = tempfile::tempdir().unwrap();
        let store = store_in(&temp);
        store.save(&UiSettings::default()).unwrap();
        fs::remove_file(store.path()).unwrap();
        let target = store.root().join("target.json");
        write_private_test_file(&target, b"{}");
        symlink(&target, store.path()).unwrap();
        assert!(store.load().is_err());
        assert!(store.save(&UiSettings::default()).is_err());

        let real_root = temp.path().join("real-root");
        fs::create_dir(&real_root).unwrap();
        fs::set_permissions(&real_root, fs::Permissions::from_mode(0o700)).unwrap();
        let linked_root = temp.path().join("linked-root");
        symlink(&real_root, &linked_root).unwrap();
        let linked_store = UiSettingsStore::new_user(linked_root).unwrap();
        assert!(linked_store.save(&UiSettings::default()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn permissive_file_directory_and_hardlink_are_rejected() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().unwrap();
        let store = store_in(&temp);
        store.save(&UiSettings::default()).unwrap();

        fs::set_permissions(store.path(), fs::Permissions::from_mode(0o644)).unwrap();
        assert!(store.load().is_err());
        fs::set_permissions(store.path(), fs::Permissions::from_mode(0o600)).unwrap();
        let alias = store.root().join("alias.json");
        fs::hard_link(store.path(), &alias).unwrap();
        assert!(store.load().is_err());
        fs::remove_file(alias).unwrap();

        fs::set_permissions(store.root(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(store.load().is_err());
        assert!(store.save(&UiSettings::default()).is_err());
    }

    #[test]
    fn oversized_file_is_rejected_before_json_parsing() {
        let temp = tempfile::tempdir().unwrap();
        let store = store_in(&temp);
        store.save(&UiSettings::default()).unwrap();
        write_private_test_file(
            store.path(),
            &vec![b' '; usize::try_from(MAX_UI_SETTINGS_BYTES).unwrap() + 1],
        );
        assert!(store.load().unwrap_err().to_string().contains("exceeds"));
    }

    fn write_private_test_file(path: &Path, bytes: &[u8]) {
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
}

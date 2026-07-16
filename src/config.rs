use std::{
    env, fmt, fs,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::ValueEnum;
use serde_json::{Map, Value};

use crate::{
    permissions::PermissionMode,
    protocol::{ApiFormat, ChatTokensField},
    sandbox::SandboxRuntime,
};

pub const DEFAULT_MODEL: &str = "default";
const MAX_SETTINGS_BYTES: u64 = 1024 * 1024;
const MAX_PLUGIN_DIRECTORIES: usize = 32;
const MAX_EXTENSION_PATH_BYTES: usize = 4096;
const MAX_OUTPUT_STYLE_NAME_BYTES: usize = 128;
const MAX_MODEL_OPTIONS: usize = 64;
const MAX_MODEL_ID_BYTES: usize = 512;
const MAX_MODEL_DISPLAY_BYTES: usize = 256;
const MAX_MODEL_DESCRIPTION_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelOption {
    pub value: String,
    pub display_name: String,
    pub description: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AutoMemorySettings {
    pub enabled: bool,
    pub auto_extract: bool,
    pub auto_consolidate: bool,
    pub path: Option<PathBuf>,
}

#[derive(Clone)]
pub struct Settings {
    pub raw: Value,
}

impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut redacted = self.raw.clone();
        redact_settings_debug(&mut redacted);
        formatter
            .debug_struct("Settings")
            .field("raw", &redacted)
            .finish()
    }
}

fn redact_settings_debug(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if matches!(
                    key.to_ascii_lowercase().as_str(),
                    "env" | "headers" | "token" | "apikey" | "api_key" | "authorization"
                ) {
                    *value = Value::String("<redacted>".to_owned());
                } else {
                    redact_settings_debug(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                redact_settings_debug(value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

pub(crate) fn validate_model_id(value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value != value.trim()
        || value.len() > MAX_MODEL_ID_BYTES
        || value.chars().any(char::is_control)
        || value.chars().any(char::is_whitespace)
    {
        anyhow::bail!("model id 为空、过长或包含空白/控制字符")
    }
    Ok(())
}

fn validate_model_text(value: &str, field: &str, limit: usize, allow_empty: bool) -> Result<()> {
    if (!allow_empty && value.trim().is_empty())
        || value.len() > limit
        || value.chars().any(char::is_control)
    {
        anyhow::bail!("model {field} 为空、过长或包含控制字符")
    }
    Ok(())
}

fn parse_model_option(value: &Value) -> Result<ModelOption> {
    let (model, display_name, description) = match value {
        Value::String(model) => (model.as_str(), model.as_str(), ""),
        Value::Object(object) => {
            let allowed = ["value", "displayName", "description"];
            if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
                anyhow::bail!("models option 包含未知字段 {key}")
            }
            let model = object
                .get("value")
                .and_then(Value::as_str)
                .context("models option.value 必须是 string")?;
            let display_name = object
                .get("displayName")
                .map(|value| {
                    value
                        .as_str()
                        .context("models option.displayName 必须是 string")
                })
                .transpose()?
                .unwrap_or(model);
            let description = object
                .get("description")
                .map(|value| {
                    value
                        .as_str()
                        .context("models option.description 必须是 string")
                })
                .transpose()?
                .unwrap_or("");
            (model, display_name, description)
        }
        _ => anyhow::bail!("models 只能包含 string 或 object"),
    };
    validate_model_id(model)?;
    validate_model_text(display_name, "displayName", MAX_MODEL_DISPLAY_BYTES, false)?;
    validate_model_text(
        description,
        "description",
        MAX_MODEL_DESCRIPTION_BYTES,
        true,
    )?;
    Ok(ModelOption {
        value: model.to_owned(),
        display_name: display_name.to_owned(),
        description: description.to_owned(),
    })
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            raw: Value::Object(Map::new()),
        }
    }
}

impl Settings {
    pub fn load(cwd: &Path, explicit: Option<&str>, bare: bool) -> Result<Self> {
        let mut merged = Value::Object(Map::new());

        if !bare {
            if let Some(home) = dirs::home_dir() {
                merge_file_if_present(
                    &mut merged,
                    &home.join(".open-agent-harness/settings.json"),
                )?;
            }
            merge_project_file_if_present(
                &mut merged,
                &cwd.join(".open-agent-harness/settings.json"),
            )?;
            merge_project_file_if_present(
                &mut merged,
                &cwd.join(".open-agent-harness/settings.local.json"),
            )?;
        }

        if let Some(source) = explicit {
            let value = if source.trim_start().starts_with('{') {
                serde_json::from_str(source).context("--settings 不是有效 JSON")?
            } else {
                read_json_file(&PathBuf::from(source))?
            };
            merge_json(&mut merged, value);
        }

        if !bare {
            append_installed_plugin_directories(
                &mut merged,
                crate::plugin_manager::installed_plugin_directories_default()?,
            )?;
        }

        Ok(Self { raw: merged })
    }

    /// Remove every runtime customization while retaining only the policy surface needed by
    /// built-in tools. This is intentionally applied after all settings layers merge so an
    /// explicit `--settings` file cannot re-enable an extension in safe mode.
    pub fn retain_safe_mode_core(&mut self) {
        let Some(root) = self.raw.as_object_mut() else {
            self.raw = Value::Object(Map::new());
            return;
        };
        root.retain(|key, _| {
            matches!(key.as_str(), "model" | "models" | "permissions" | "sandbox")
        });
    }

    pub fn model(&self) -> Option<&str> {
        self.raw.get("model").and_then(Value::as_str)
    }

    /// Return the trusted model picker catalog. Provider-specific aliases are
    /// configuration data, never compiled into the provider-neutral runtime.
    /// The current model is always selectable even when it is absent from the
    /// configured catalog, matching the interactive picker contract.
    pub fn model_options(&self, current: &str) -> Result<Vec<ModelOption>> {
        validate_model_id(current)?;
        let mut options = Vec::new();
        if let Some(value) = self.raw.get("models") {
            let values = value.as_array().context("models 必须是 array")?;
            if values.len() > MAX_MODEL_OPTIONS {
                anyhow::bail!("models 超过 {MAX_MODEL_OPTIONS} 个限制")
            }
            for value in values {
                let option = parse_model_option(value)?;
                if options
                    .iter()
                    .any(|existing: &ModelOption| existing.value == option.value)
                {
                    anyhow::bail!("models 包含重复 model id: {}", option.value)
                }
                options.push(option);
            }
        }
        if !options.iter().any(|option| option.value == current) {
            if options.len() >= MAX_MODEL_OPTIONS {
                anyhow::bail!("models 已达 {MAX_MODEL_OPTIONS} 个限制，且不包含当前 model id")
            }
            options.push(ModelOption {
                value: current.to_owned(),
                display_name: current.to_owned(),
                description: "Current model".to_owned(),
            });
        }
        Ok(options)
    }

    /// Returns a statically selected output style from trusted settings.
    /// Project settings cannot contribute this key because their merge surface
    /// is restricted to permission deny rules.
    pub fn output_style(&self) -> Result<Option<&str>> {
        let Some(value) = self.raw.get("outputStyle") else {
            return Ok(None);
        };
        let name = value.as_str().context("outputStyle 必须是 string")?;
        if name.is_empty()
            || name.len() > MAX_OUTPUT_STYLE_NAME_BYTES
            || name.contains(['\0', '\n', '\r'])
        {
            anyhow::bail!("outputStyle 为空、过长或包含控制字符")
        }
        Ok(Some(name))
    }

    pub fn permission_mode(&self) -> Option<PermissionMode> {
        self.raw
            .get("permissions")
            .and_then(|v| v.get("defaultMode"))
            .and_then(Value::as_str)
            .and_then(PermissionMode::from_setting)
    }

    pub fn allow_rules(&self) -> Vec<String> {
        string_array_at(&self.raw, &["permissions", "allow"])
    }

    pub fn deny_rules(&self) -> Vec<String> {
        string_array_at(&self.raw, &["permissions", "deny"])
    }

    /// Build the command sandbox exclusively from the already merged trusted settings.
    /// Project settings cannot contribute this key because `merge_project_json` only
    /// appends permission deny rules.
    pub fn sandbox_runtime(&self) -> Result<SandboxRuntime> {
        SandboxRuntime::from_settings(&self.raw)
    }

    /// Returns explicitly trusted local plugin directories. Project settings
    /// cannot contribute this key because project merging retains deny rules only.
    pub fn plugin_directories(&self) -> Result<Vec<PathBuf>> {
        let Some(plugins) = self.raw.get("plugins") else {
            return Ok(Vec::new());
        };
        let plugins = plugins.as_object().context("plugins 必须是 object")?;
        if let Some(key) = plugins.keys().find(|key| key.as_str() != "directories") {
            anyhow::bail!("plugins 包含未知字段 {key}")
        }
        let Some(directories) = plugins.get("directories") else {
            return Ok(Vec::new());
        };
        let directories = directories
            .as_array()
            .context("plugins.directories 必须是 array")?;
        if directories.len() > MAX_PLUGIN_DIRECTORIES {
            anyhow::bail!("plugin 目录超过 {MAX_PLUGIN_DIRECTORIES} 个限制")
        }
        directories
            .iter()
            .map(|value| {
                let value = value
                    .as_str()
                    .context("plugins.directories 只能包含 string")?;
                if value.trim().is_empty()
                    || value.len() > MAX_EXTENSION_PATH_BYTES
                    || value.contains('\0')
                {
                    anyhow::bail!("plugin 目录路径为空、过长或包含 NUL")
                }
                let path = PathBuf::from(value);
                if !path.is_absolute() {
                    anyhow::bail!("plugin 目录必须使用绝对路径")
                }
                Ok(path)
            })
            .collect()
    }

    /// Auto-memory is opt-in and can only be enabled by trusted settings.
    pub fn auto_memory_settings(&self) -> Result<AutoMemorySettings> {
        let Some(memory) = self.raw.get("memory") else {
            return Ok(AutoMemorySettings::default());
        };
        let memory = memory.as_object().context("memory 必须是 object")?;
        if let Some(key) = memory.keys().find(|key| {
            !matches!(
                key.as_str(),
                "enabled" | "autoExtract" | "autoConsolidate" | "path"
            )
        }) {
            anyhow::bail!("memory 包含未知字段 {key}")
        }
        let enabled = memory
            .get("enabled")
            .map(|value| value.as_bool().context("memory.enabled 必须是 boolean"))
            .transpose()?
            .unwrap_or(false);
        let auto_extract = memory
            .get("autoExtract")
            .map(|value| value.as_bool().context("memory.autoExtract 必须是 boolean"))
            .transpose()?
            .unwrap_or(false);
        let auto_consolidate = memory
            .get("autoConsolidate")
            .map(|value| {
                value
                    .as_bool()
                    .context("memory.autoConsolidate 必须是 boolean")
            })
            .transpose()?
            .unwrap_or(false);
        if (auto_extract || auto_consolidate) && !enabled {
            anyhow::bail!("memory.autoExtract/autoConsolidate=true 要求 memory.enabled=true")
        }
        let path = memory
            .get("path")
            .map(|value| {
                let value = value.as_str().context("memory.path 必须是 string")?;
                if value.trim().is_empty()
                    || value.len() > MAX_EXTENSION_PATH_BYTES
                    || value.contains('\0')
                {
                    anyhow::bail!("memory.path 为空、过长或包含 NUL")
                }
                Ok(PathBuf::from(value))
            })
            .transpose()?;
        Ok(AutoMemorySettings {
            enabled,
            auto_extract,
            auto_consolidate,
            path,
        })
    }

    /// # Safety
    ///
    /// Call this only during single-threaded process bootstrap, before any worker threads exist.
    pub unsafe fn apply_environment(&mut self) {
        let Some(vars) = self.raw.get("env").and_then(Value::as_object) else {
            return;
        };
        let vars = vars
            .iter()
            .filter_map(|(key, value)| Some((key.clone(), value.as_str()?.to_owned())))
            .collect::<Vec<_>>();
        for (key, value) in vars {
            // SAFETY: the caller guarantees single-threaded process bootstrap.
            unsafe { env::set_var(key, value) };
        }
        if let Some(root) = self.raw.as_object_mut() {
            root.remove("env");
        }
    }
}

pub(crate) fn project_deny_rules(cwd: &Path, bare: bool) -> Result<Vec<String>> {
    if bare {
        return Ok(Vec::new());
    }
    let mut project = Value::Object(Map::new());
    merge_project_file_if_present(&mut project, &cwd.join(".open-agent-harness/settings.json"))?;
    merge_project_file_if_present(
        &mut project,
        &cwd.join(".open-agent-harness/settings.local.json"),
    )?;
    Ok(string_array_at(&project, &["permissions", "deny"]))
}

fn string_array_at(root: &Value, path: &[&str]) -> Vec<String> {
    let mut value = root;
    for key in path {
        let Some(next) = value.get(*key) else {
            return Vec::new();
        };
        value = next;
    }
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

fn read_json_file(path: &Path) -> Result<Value> {
    let size = fs::metadata(path)
        .with_context(|| format!("无法检查设置文件 {}", path.display()))?
        .len();
    if size > MAX_SETTINGS_BYTES {
        anyhow::bail!(
            "设置文件超过 {} 字节限制: {}",
            MAX_SETTINGS_BYTES,
            path.display()
        );
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .with_context(|| format!("无法打开设置文件 {}", path.display()))?
        .take(MAX_SETTINGS_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SETTINGS_BYTES as usize {
        anyhow::bail!(
            "设置文件超过 {} 字节限制: {}",
            MAX_SETTINGS_BYTES,
            path.display()
        );
    }
    let text = String::from_utf8(bytes)
        .with_context(|| format!("设置文件不是有效 UTF-8: {}", path.display()))?;
    serde_json::from_str(&text)
        .with_context(|| format!("设置文件不是有效 JSON: {}", path.display()))
}

fn merge_file_if_present(target: &mut Value, path: &Path) -> Result<()> {
    if path.exists() {
        merge_json(target, read_json_file(path)?);
    }
    Ok(())
}

fn merge_json(target: &mut Value, incoming: Value) {
    match (target, incoming) {
        (Value::Object(target), Value::Object(incoming)) => {
            for (key, value) in incoming {
                match target.get_mut(&key) {
                    Some(current) => merge_json(current, value),
                    None => {
                        target.insert(key, value);
                    }
                }
            }
        }
        (target, incoming) => *target = incoming,
    }
}

fn append_installed_plugin_directories(
    target: &mut Value,
    directories: Vec<PathBuf>,
) -> Result<()> {
    if directories.is_empty() {
        return Ok(());
    }
    let root = target
        .as_object_mut()
        .context("trusted settings 顶层必须是 object")?;
    let plugins = root
        .entry("plugins")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .context("settings.plugins 必须是 object")?;
    let configured = plugins
        .entry("directories")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .context("settings.plugins.directories 必须是 array")?;
    for directory in directories {
        let directory = directory
            .to_str()
            .context("installed plugin path 不是有效 UTF-8")?;
        if !configured
            .iter()
            .any(|value| value.as_str() == Some(directory))
        {
            configured.push(Value::String(directory.to_owned()));
        }
    }
    Ok(())
}

pub fn endpoint_config() -> Result<EndpointConfig> {
    let token = env::var("HARNESS_API_KEY")
        .or_else(|_| env::var("HARNESS_AUTH_TOKEN"))
        .ok()
        .filter(|value| !value.is_empty());
    let api_format = env::var("HARNESS_API_FORMAT")
        .ok()
        .map(|value| {
            ApiFormat::from_str(&value, true)
                .map_err(|error| anyhow::anyhow!("HARNESS_API_FORMAT 无效: {error}"))
        })
        .transpose()?
        .unwrap_or(ApiFormat::Auto);
    let chat_tokens_field = env::var("HARNESS_CHAT_TOKENS_FIELD")
        .ok()
        .map(|value| {
            ChatTokensField::from_str(&value, true)
                .map_err(|error| anyhow::anyhow!("HARNESS_CHAT_TOKENS_FIELD 无效: {error}"))
        })
        .transpose()?
        .unwrap_or(ChatTokensField::MaxCompletionTokens);
    Ok(EndpointConfig {
        token,
        base_url: env::var("HARNESS_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_owned())
            .trim_end_matches('/')
            .to_owned(),
        messages_path: env::var("HARNESS_API_PATH")
            .or_else(|_| env::var("HARNESS_MESSAGES_PATH"))
            .unwrap_or_else(|_| "/v1/messages".to_owned()),
        api_format,
        stream: env_bool("HARNESS_STREAM", true)?,
        chat_tokens_field,
        include_stream_usage: env_bool("HARNESS_INCLUDE_STREAM_USAGE", true)?,
        allow_env_proxy: env::var("HARNESS_ALLOW_ENV_PROXY")
            .ok()
            .is_some_and(|value| {
                matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
            }),
    })
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("{name} 必须是 true/false、yes/no、on/off 或 1/0"),
    }
}

#[derive(Clone)]
pub struct EndpointConfig {
    pub token: Option<String>,
    pub base_url: String,
    pub messages_path: String,
    pub api_format: ApiFormat,
    pub stream: bool,
    pub chat_tokens_field: ChatTokensField,
    pub include_stream_usage: bool,
    pub allow_env_proxy: bool,
}

impl fmt::Debug for EndpointConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointConfig")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("base_url", &self.base_url)
            .field("messages_path", &self.messages_path)
            .field("api_format", &self.api_format)
            .field("stream", &self.stream)
            .field("chat_tokens_field", &self.chat_tokens_field)
            .field("include_stream_usage", &self.include_stream_usage)
            .field("allow_env_proxy", &self.allow_env_proxy)
            .finish()
    }
}

fn merge_project_file_if_present(target: &mut Value, path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("无法检查项目设置 {}", path.display()));
        }
    }
    let project_root = path
        .parent()
        .and_then(Path::parent)
        .context("项目设置路径缺少工作区根目录")?;
    let canonical_root = fs::canonicalize(project_root)
        .with_context(|| format!("无法解析工作区根目录 {}", project_root.display()))?;
    let canonical =
        fs::canonicalize(path).with_context(|| format!("无法解析项目设置 {}", path.display()))?;
    if !canonical.starts_with(&canonical_root) {
        anyhow::bail!("项目设置 symlink 越过工作区边界: {}", path.display());
    }
    let incoming = read_json_file(&canonical)?;
    merge_project_json(target, incoming)
        .with_context(|| format!("项目设置不安全或无效: {}", path.display()))?;
    Ok(())
}

fn merge_project_json(target: &mut Value, incoming: Value) -> Result<()> {
    let deny = string_array_at(&incoming, &["permissions", "deny"]);
    incoming.as_object().context("顶层必须是 JSON object")?;
    append_project_deny_rules(target, deny);
    Ok(())
}

fn append_project_deny_rules(target: &mut Value, incoming: Vec<String>) {
    if incoming.is_empty() {
        return;
    }
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let root = target.as_object_mut().expect("target was normalized");
    let permissions = root
        .entry("permissions")
        .or_insert_with(|| Value::Object(Map::new()));
    if !permissions.is_object() {
        *permissions = Value::Object(Map::new());
    }
    let permissions = permissions
        .as_object_mut()
        .expect("permissions was normalized");
    let deny = permissions
        .entry("deny")
        .or_insert_with(|| Value::Array(Vec::new()));
    if !deny.is_array() {
        *deny = Value::Array(Vec::new());
    }
    let values = deny.as_array_mut().expect("deny was normalized");
    for rule in incoming {
        if !values.iter().any(|value| value.as_str() == Some(&rule)) {
            values.push(Value::String(rule));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_merge_preserves_siblings() {
        let mut a =
            serde_json::json!({"permissions": {"allow": ["Read"], "defaultMode": "default"}});
        merge_json(
            &mut a,
            serde_json::json!({"permissions": {"deny": ["Bash(rm *)"]}}),
        );
        assert_eq!(a["permissions"]["allow"][0], "Read");
        assert_eq!(a["permissions"]["deny"][0], "Bash(rm *)");
    }

    #[test]
    fn trusted_settings_recognize_dont_ask_mode() {
        let settings = Settings {
            raw: serde_json::json!({"permissions":{"defaultMode":"dontAsk"}}),
        };
        assert_eq!(settings.permission_mode(), Some(PermissionMode::DontAsk));
    }

    #[test]
    fn installed_plugin_directories_are_appended_as_trusted_user_state() {
        let mut settings = serde_json::json!({
            "plugins":{"directories":["/trusted/explicit"]}
        });
        append_installed_plugin_directories(
            &mut settings,
            vec![
                PathBuf::from("/trusted/explicit"),
                PathBuf::from("/trusted/installed"),
            ],
        )
        .unwrap();
        assert_eq!(
            settings["plugins"]["directories"],
            serde_json::json!(["/trusted/explicit", "/trusted/installed"])
        );

        let mut malformed = serde_json::json!({"plugins":{"directories":"not-an-array"}});
        assert!(
            append_installed_plugin_directories(
                &mut malformed,
                vec![PathBuf::from("/trusted/installed")],
            )
            .is_err()
        );
    }

    #[test]
    fn project_settings_cannot_redirect_or_elevate() {
        let mut trusted = serde_json::json!({
            "env": {"HARNESS_BASE_URL": "https://trusted.invalid"},
            "sandbox": {"enabled": false, "failIfUnavailable": true},
            "permissions": {
                "defaultMode": "default",
                "allow": ["Read"],
                "deny": ["Bash(git push *)"]
            },
            "model": "trusted-model"
        });
        merge_project_json(
            &mut trusted,
            serde_json::json!({
                "env": {"HARNESS_BASE_URL": "https://untrusted.invalid"},
                "permissions": {
                    "defaultMode": "bypassPermissions",
                    "allow": ["Bash(*)"],
                    "deny": ["Write(secrets/**)"]
                },
                "sandbox": {"enabled": true, "failIfUnavailable": false},
                "model": "project-model",
                "commands": {"unsafe":"run this"},
                "plugins": {"directories":["/tmp/untrusted"]},
                "memory": {"enabled":true},
                "mcpServers": {"untrusted":{"url":"https://untrusted.invalid/mcp"}},
                "hooks": {"PreToolUse":[]}
            }),
        )
        .unwrap();
        assert_eq!(
            trusted["env"]["HARNESS_BASE_URL"],
            "https://trusted.invalid"
        );
        assert_eq!(trusted["permissions"]["defaultMode"], "default");
        assert_eq!(trusted["sandbox"]["enabled"], false);
        assert_eq!(trusted["sandbox"]["failIfUnavailable"], true);
        assert_eq!(trusted["permissions"]["allow"], serde_json::json!(["Read"]));
        assert_eq!(
            trusted["permissions"]["deny"],
            serde_json::json!(["Bash(git push *)", "Write(secrets/**)"])
        );
        assert_eq!(trusted["model"], "trusted-model");
        assert!(trusted.get("commands").is_none());
        assert!(trusted.get("plugins").is_none());
        assert!(trusted.get("memory").is_none());
        assert!(trusted.get("mcpServers").is_none());
        assert!(trusted.get("hooks").is_none());
    }

    #[test]
    fn non_object_project_settings_fail_closed() {
        let mut trusted = serde_json::json!({"model":"safe"});
        assert!(merge_project_json(&mut trusted, Value::Null).is_err());
        assert_eq!(trusted["model"], "safe");
    }

    #[test]
    fn settings_debug_redacts_nested_secret_containers() {
        let settings = Settings {
            raw: serde_json::json!({
                "env":{"HARNESS_API_KEY":"debug-secret"},
                "mcpServers":{"private":{"headers":{"Authorization":"header-secret"}}},
                "lspServers":{"private":{"env":{"LANGUAGE_TOKEN":"nested-secret"}}}
            }),
        };
        let rendered = format!("{settings:?}");
        assert!(!rendered.contains("debug-secret"));
        assert!(!rendered.contains("header-secret"));
        assert!(!rendered.contains("nested-secret"));
        assert!(rendered.contains("redacted"));
    }

    #[cfg(unix)]
    #[test]
    fn project_settings_symlink_cannot_escape_workspace() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let settings_dir = workspace.join(".open-agent-harness");
        fs::create_dir_all(&settings_dir).unwrap();
        let private = temp.path().join("private.json");
        fs::write(&private, r#"{"model":"private"}"#).unwrap();
        let link = settings_dir.join("settings.json");
        symlink(&private, &link).unwrap();
        let mut merged = serde_json::json!({});
        let error = merge_project_file_if_present(&mut merged, &link).unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }

    #[test]
    fn trusted_extension_settings_are_typed_and_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let plugin_one = temp.path().join("one");
        let plugin_two = temp.path().join("two");
        let settings = Settings {
            raw: serde_json::json!({
                "plugins":{"directories":[&plugin_one, &plugin_two]},
                "memory":{"enabled":true, "autoExtract":true, "autoConsolidate":true, "path":"memory-root"},
                "outputStyle":"runtime:brief"
            }),
        };
        assert_eq!(
            settings.plugin_directories().unwrap(),
            vec![plugin_one, plugin_two]
        );
        assert_eq!(settings.output_style().unwrap(), Some("runtime:brief"));
        assert_eq!(
            settings.auto_memory_settings().unwrap(),
            AutoMemorySettings {
                enabled: true,
                auto_extract: true,
                auto_consolidate: true,
                path: Some(PathBuf::from("memory-root"))
            }
        );
        let invalid = Settings {
            raw: serde_json::json!({"plugins":{"directories":[], "install":true}}),
        };
        assert!(invalid.plugin_directories().is_err());
        let relative = Settings {
            raw: serde_json::json!({"plugins":{"directories":["relative/plugin"]}}),
        };
        assert!(relative.plugin_directories().is_err());
        let extraction_without_memory = Settings {
            raw: serde_json::json!({"memory":{"autoExtract":true}}),
        };
        assert!(extraction_without_memory.auto_memory_settings().is_err());
        let invalid_extraction = Settings {
            raw: serde_json::json!({"memory":{"enabled":true,"autoExtract":"yes"}}),
        };
        assert!(invalid_extraction.auto_memory_settings().is_err());
        let consolidation_without_memory = Settings {
            raw: serde_json::json!({"memory":{"autoConsolidate":true}}),
        };
        assert!(consolidation_without_memory.auto_memory_settings().is_err());
        for raw in [
            serde_json::json!({"outputStyle":false}),
            serde_json::json!({"outputStyle":""}),
            serde_json::json!({"outputStyle":"bad\nname"}),
            serde_json::json!({"outputStyle":"x".repeat(MAX_OUTPUT_STYLE_NAME_BYTES + 1)}),
        ] {
            assert!(Settings { raw }.output_style().is_err());
        }
    }

    #[test]
    fn safe_mode_retains_only_model_permissions_and_sandbox_policy() {
        let mut settings = Settings {
            raw: serde_json::json!({
                "model":"model-id",
                "models":["model-id", {"value":"other", "displayName":"Other", "description":"Fallback"}],
                "permissions":{"defaultMode":"dontAsk", "deny":["Bash(rm:*)"]},
                "sandbox":{"enabled":true, "allowedDomains":["example.com"]},
                "env":{"SECRET":"must-not-apply"},
                "plugins":{"directories":["/tmp/plugin"]},
                "commands":{"custom":"ignored"},
                "agents":{"definitions":{}},
                "hooks":{"PreToolUse":[]},
                "mcpServers":{"server":{"command":"ignored"}},
                "lspServers":{"rust":{"command":"ignored"}},
                "outputStyle":"custom",
                "memory":{"enabled":true},
                "web":{"search":{"endpoint":"https://example.invalid"}},
                "worktree":{"enabled":true},
                "workflows":{"build":{}}
            }),
        };
        settings.retain_safe_mode_core();
        let root = settings.raw.as_object().unwrap();
        assert_eq!(root.len(), 4);
        assert_eq!(settings.model(), Some("model-id"));
        assert_eq!(settings.model_options("model-id").unwrap().len(), 2);
        assert_eq!(settings.deny_rules(), vec!["Bash(rm:*)"]);
        assert!(root.contains_key("sandbox"));
        assert!(!root.contains_key("env"));
        assert!(!root.contains_key("plugins"));
        assert!(!root.contains_key("mcpServers"));
        assert!(!root.contains_key("workflows"));
    }

    #[test]
    fn model_options_are_bounded_validated_and_include_current() {
        let settings = Settings {
            raw: serde_json::json!({
                "models":[
                    "provider/model-a",
                    {"value":"provider/model-b", "displayName":"Model B", "description":"Fast"}
                ]
            }),
        };
        let options = settings.model_options("provider/current").unwrap();
        assert_eq!(options.len(), 3);
        assert_eq!(options[1].display_name, "Model B");
        assert_eq!(options[2].description, "Current model");

        for raw in [
            serde_json::json!({"models":"not-an-array"}),
            serde_json::json!({"models":["duplicate", "duplicate"]}),
            serde_json::json!({"models":[{"value":"ok", "unknown":true}]}),
            serde_json::json!({"models":["bad model"]}),
            serde_json::json!({"models":[{"value":"ok", "displayName":"bad\nname"}]}),
        ] {
            assert!(Settings { raw }.model_options("current").is_err());
        }

        let full_catalog = (0..MAX_MODEL_OPTIONS)
            .map(|index| Value::String(format!("model-{index}")))
            .collect::<Vec<_>>();
        let settings = Settings {
            raw: serde_json::json!({"models":full_catalog}),
        };
        assert_eq!(
            settings.model_options("model-0").unwrap().len(),
            MAX_MODEL_OPTIONS
        );
        assert!(settings.model_options("missing-current").is_err());
    }
}

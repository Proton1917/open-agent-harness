use std::{
    env, fmt, fs,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde_json::{Map, Value};

use crate::permissions::PermissionMode;

pub const DEFAULT_MODEL: &str = "default";
const MAX_SETTINGS_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct Settings {
    pub raw: Value,
}

impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut redacted = self.raw.clone();
        if let Some(root) = redacted.as_object_mut()
            && root.contains_key("env")
        {
            root.insert("env".into(), Value::String("<redacted>".into()));
        }
        formatter
            .debug_struct("Settings")
            .field("raw", &redacted)
            .finish()
    }
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

        Ok(Self { raw: merged })
    }

    pub fn model(&self) -> Option<&str> {
        self.raw.get("model").and_then(Value::as_str)
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

pub fn endpoint_config() -> EndpointConfig {
    let token = env::var("HARNESS_API_KEY")
        .or_else(|_| env::var("HARNESS_AUTH_TOKEN"))
        .ok()
        .filter(|value| !value.is_empty());
    EndpointConfig {
        token,
        base_url: env::var("HARNESS_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_owned())
            .trim_end_matches('/')
            .to_owned(),
        messages_path: env::var("HARNESS_MESSAGES_PATH")
            .unwrap_or_else(|_| "/v1/messages".to_owned()),
        allow_env_proxy: env::var("HARNESS_ALLOW_ENV_PROXY")
            .ok()
            .is_some_and(|value| {
                matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
            }),
    }
}

#[derive(Clone)]
pub struct EndpointConfig {
    pub token: Option<String>,
    pub base_url: String,
    pub messages_path: String,
    pub allow_env_proxy: bool,
}

impl fmt::Debug for EndpointConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointConfig")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("base_url", &self.base_url)
            .field("messages_path", &self.messages_path)
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
    fn project_settings_cannot_redirect_or_elevate() {
        let mut trusted = serde_json::json!({
            "env": {"HARNESS_BASE_URL": "https://trusted.invalid"},
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
                "model": "project-model"
            }),
        )
        .unwrap();
        assert_eq!(
            trusted["env"]["HARNESS_BASE_URL"],
            "https://trusted.invalid"
        );
        assert_eq!(trusted["permissions"]["defaultMode"], "default");
        assert_eq!(trusted["permissions"]["allow"], serde_json::json!(["Read"]));
        assert_eq!(
            trusted["permissions"]["deny"],
            serde_json::json!(["Bash(git push *)", "Write(secrets/**)"])
        );
        assert_eq!(trusted["model"], "trusted-model");
    }

    #[test]
    fn non_object_project_settings_fail_closed() {
        let mut trusted = serde_json::json!({"model":"safe"});
        assert!(merge_project_json(&mut trusted, Value::Null).is_err());
        assert_eq!(trusted["model"], "safe");
    }

    #[test]
    fn settings_debug_redacts_environment_values() {
        let settings = Settings {
            raw: serde_json::json!({"env":{"HARNESS_API_KEY":"debug-secret"}}),
        };
        let rendered = format!("{settings:?}");
        assert!(!rendered.contains("debug-secret"));
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
}

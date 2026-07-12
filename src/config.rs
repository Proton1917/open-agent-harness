use std::{
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde_json::{Map, Value};

use crate::permissions::PermissionMode;

pub const DEFAULT_MODEL: &str = "default";

#[derive(Debug, Clone)]
pub struct Settings {
    pub raw: Value,
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
            merge_file_if_present(&mut merged, &cwd.join(".open-agent-harness/settings.json"))?;
            merge_file_if_present(
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

    pub fn apply_environment(&self) {
        let Some(vars) = self.raw.get("env").and_then(Value::as_object) else {
            return;
        };
        for (key, value) in vars {
            if let Some(value) = value.as_str() {
                // SAFETY: settings are applied once, before worker threads and the HTTP client start.
                unsafe { env::set_var(key, value) };
            }
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
    let text =
        fs::read_to_string(path).with_context(|| format!("无法读取设置文件 {}", path.display()))?;
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
    EndpointConfig {
        token: env::var("HARNESS_API_KEY")
            .or_else(|_| env::var("HARNESS_AUTH_TOKEN"))
            .ok()
            .filter(|value| !value.is_empty()),
        base_url: env::var("HARNESS_BASE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_owned())
            .trim_end_matches('/')
            .to_owned(),
        messages_path: env::var("HARNESS_MESSAGES_PATH")
            .unwrap_or_else(|_| "/v1/messages".to_owned()),
    }
}

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub token: Option<String>,
    pub base_url: String,
    pub messages_path: String,
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
}

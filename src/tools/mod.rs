mod bash;
mod edit;
mod glob;
mod grep;
mod read;
mod tasks;
mod work_items;
mod write;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::{process::Child, sync::Mutex};

use crate::permissions::{PermissionDecision, PermissionManager};

pub use bash::BashTool;
pub use edit::EditTool;
pub use glob::GlobTool;
pub use grep::GrepTool;
pub use read::ReadTool;
pub use tasks::{TaskOutputTool, TaskStopTool};
pub use work_items::{TaskCreateTool, TaskGetTool, TaskListTool, TaskUpdateTool, TodoWriteTool};
pub use write::WriteTool;

#[derive(Debug, Clone)]
pub struct FileSnapshot {
    pub modified: Option<SystemTime>,
    pub content: String,
    pub partial: bool,
}

#[derive(Debug)]
pub struct BackgroundTask {
    pub child: Child,
    pub output_path: PathBuf,
    pub command: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TodoItem {
    pub content: String,
    pub status: String,
    #[serde(rename = "activeForm")]
    pub active_form: String,
}

#[derive(Clone)]
pub struct ToolContext {
    pub cwd: PathBuf,
    pub permissions: Arc<PermissionManager>,
    pub read_cache: Arc<Mutex<HashMap<PathBuf, FileSnapshot>>>,
    pub tasks: Arc<Mutex<HashMap<String, BackgroundTask>>>,
    pub todos: Arc<Mutex<Vec<TodoItem>>>,
    pub task_store_lock: Arc<Mutex<()>>,
    pub task_store_path: PathBuf,
}

impl ToolContext {
    pub fn new(cwd: PathBuf, permissions: PermissionManager) -> Self {
        let task_store_path = task_store_path(&cwd);
        Self {
            cwd,
            permissions: Arc::new(permissions),
            read_cache: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            todos: Arc::new(Mutex::new(Vec::new())),
            task_store_lock: Arc::new(Mutex::new(())),
            task_store_path,
        }
    }

    pub fn resolve_path(&self, value: &str) -> Result<PathBuf> {
        if value.trim().is_empty() {
            bail!("路径不能为空");
        }
        let expanded = if value == "~" {
            dirs::home_dir().context("无法确定用户主目录")?
        } else if let Some(rest) = value.strip_prefix("~/") {
            dirs::home_dir().context("无法确定用户主目录")?.join(rest)
        } else {
            PathBuf::from(value)
        };
        Ok(if expanded.is_absolute() {
            expanded
        } else {
            self.cwd.join(expanded)
        })
    }

    pub async fn remember_read(&self, path: PathBuf, content: String, partial: bool) -> Result<()> {
        let modified = std::fs::metadata(&path)
            .ok()
            .and_then(|m| m.modified().ok());
        self.read_cache.lock().await.insert(
            path,
            FileSnapshot {
                modified,
                content,
                partial,
            },
        );
        Ok(())
    }

    pub async fn verify_fresh_full_read(&self, path: &Path, current: &str) -> Result<()> {
        let cache = self.read_cache.lock().await;
        let snapshot = cache
            .get(path)
            .ok_or_else(|| anyhow!("文件尚未读取；请先使用 Read"))?;
        if snapshot.partial {
            bail!("此前只读取了文件的一部分；修改前必须完整读取文件");
        }
        let modified = std::fs::metadata(path).ok().and_then(|m| m.modified().ok());
        if modified != snapshot.modified && current != snapshot.content {
            bail!("文件在读取后已被用户或其他进程修改；请重新 Read 后再写入");
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ToolOutput {
    pub content: String,
    pub is_error: bool,
}

impl ToolOutput {
    pub fn success(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: false,
        }
    }

    pub fn error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: true,
        }
    }
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn input_schema(&self) -> Value;
    fn read_only(&self, input: &Value) -> bool;
    fn destructive(&self, _input: &Value) -> bool {
        false
    }
    fn requires_permission(&self) -> bool {
        true
    }
    fn summary(&self, input: &Value) -> String;
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput>;

    fn api_definition(&self) -> Value {
        json!({
            "name": self.name(),
            "description": self.description(),
            "input_schema": self.input_schema(),
        })
    }
}

#[derive(Clone)]
pub struct ToolRegistry {
    tools: Arc<HashMap<String, Arc<dyn Tool>>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        let tools: Vec<Arc<dyn Tool>> = vec![
            Arc::new(BashTool),
            Arc::new(GlobTool),
            Arc::new(GrepTool),
            Arc::new(ReadTool),
            Arc::new(EditTool),
            Arc::new(WriteTool),
            Arc::new(TaskOutputTool),
            Arc::new(TaskStopTool),
            Arc::new(TodoWriteTool),
            Arc::new(TaskCreateTool),
            Arc::new(TaskGetTool),
            Arc::new(TaskListTool),
            Arc::new(TaskUpdateTool),
        ];
        Self {
            tools: Arc::new(
                tools
                    .into_iter()
                    .map(|tool| (tool.name().to_owned(), tool))
                    .collect(),
            ),
        }
    }
}

impl ToolRegistry {
    pub fn definitions(&self) -> Vec<Value> {
        let mut tools: Vec<_> = self
            .tools
            .values()
            .map(|tool| tool.api_definition())
            .collect();
        tools.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        tools
    }

    pub async fn execute(&self, context: &ToolContext, name: &str, input: Value) -> ToolOutput {
        let Some(tool) = self.tools.get(name) else {
            return ToolOutput::error(format!("未知工具: {name}"));
        };
        let summary = tool.summary(&input);
        if tool.requires_permission() {
            match context.permissions.decide(
                tool.name(),
                &summary,
                tool.read_only(&input),
                tool.destructive(&input),
            ) {
                Ok(PermissionDecision::Allow) => {}
                Ok(PermissionDecision::Deny) => {
                    return ToolOutput::error("用户或权限规则拒绝了此工具调用");
                }
                Err(error) => return ToolOutput::error(format!("权限检查失败: {error:#}")),
            }
        }
        match tool.execute(context, input).await {
            Ok(output) => output,
            Err(error) => ToolOutput::error(format!("{error:#}")),
        }
    }
}

fn task_store_path(cwd: &Path) -> PathBuf {
    let key = cwd
        .to_string_lossy()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    dirs::home_dir()
        .unwrap_or_else(|| cwd.to_owned())
        .join(".open-agent-harness/task-lists")
        .join(format!("{key}.json"))
}

pub(crate) fn parse_input<T: serde::de::DeserializeOwned>(input: Value) -> Result<T> {
    serde_json::from_value(input).context("工具输入不符合 schema")
}

pub(crate) fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

pub(crate) fn atomic_write(path: &Path, content: &str) -> Result<()> {
    let parent = path.parent().context("目标文件没有父目录")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("无法创建目录 {}", parent.display()))?;
    let temp = parent.join(format!(".open-agent-harness-{}.tmp", uuid::Uuid::new_v4()));
    std::fs::write(&temp, content)
        .with_context(|| format!("无法写入临时文件 {}", temp.display()))?;
    if let Ok(metadata) = std::fs::metadata(path) {
        let _ = std::fs::set_permissions(&temp, metadata.permissions());
    }
    std::fs::rename(&temp, path).with_context(|| format!("无法原子替换 {}", path.display()))?;
    Ok(())
}

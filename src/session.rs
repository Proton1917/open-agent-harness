use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    tools::{ensure_private_directory, workspace_key},
    types::Message,
};

const MAX_TRANSCRIPT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    session_id: Uuid,
    cwd: PathBuf,
    timestamp_ms: u128,
    #[serde(default)]
    compact_boundary: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<Message>,
}

#[derive(Debug, Clone)]
pub struct SessionStore {
    pub id: Uuid,
    cwd: PathBuf,
    file: PathBuf,
    enabled: bool,
}

impl SessionStore {
    pub fn create(cwd: &Path, enabled: bool) -> Result<Self> {
        let id = Uuid::new_v4();
        let file = if enabled {
            project_directory(cwd)?.join(format!("{id}.jsonl"))
        } else {
            PathBuf::new()
        };
        Ok(Self {
            id,
            cwd: cwd.to_owned(),
            file,
            enabled,
        })
    }

    pub fn resume(cwd: &Path, id: Uuid, enabled: bool) -> Result<(Self, Vec<Message>)> {
        let directory = project_directory(cwd)?;
        let file = directory.join(format!("{id}.jsonl"));
        if !file.exists() {
            bail!("当前目录下没有会话 {id}")
        }
        let messages = load_messages(&file)?;
        Ok((
            Self {
                id,
                cwd: cwd.to_owned(),
                file,
                enabled,
            },
            messages,
        ))
    }

    pub fn continue_latest(cwd: &Path, enabled: bool) -> Result<(Self, Vec<Message>)> {
        let directory = project_directory(cwd)?;
        let latest = fs::read_dir(&directory)?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("jsonl"))
            .filter_map(|entry| Some((entry.metadata().ok()?.modified().ok()?, entry.path())))
            .max_by_key(|(modified, _)| *modified)
            .map(|(_, path)| path)
            .context("当前目录没有可继续的会话")?;
        let id = latest
            .file_stem()
            .and_then(|s| s.to_str())
            .context("会话文件名无效")?
            .parse()?;
        let messages = load_messages(&latest)?;
        Ok((
            Self {
                id,
                cwd: cwd.to_owned(),
                file: latest,
                enabled,
            },
            messages,
        ))
    }

    pub fn append(&self, messages: &[Message]) -> Result<()> {
        if !self.enabled || messages.is_empty() {
            return Ok(());
        }
        let mut file = open_private_transcript(&self.file)?;
        let mut size = file.metadata()?.len();
        for message in messages {
            let record = Record {
                session_id: self.id,
                cwd: self.cwd.clone(),
                timestamp_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
                compact_boundary: false,
                message: Some(sanitize_for_storage(message)),
            };
            append_record(&mut file, &record, &mut size)?;
        }
        file.flush()?;
        Ok(())
    }

    pub fn replace_history(&self, messages: &[Message]) -> Result<()> {
        if !self.enabled || messages.is_empty() {
            return Ok(());
        }
        let mut contents = Vec::new();
        for (index, message) in messages.iter().enumerate() {
            let record = Record {
                session_id: self.id,
                cwd: self.cwd.clone(),
                timestamp_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
                compact_boundary: index == 0,
                message: Some(sanitize_for_storage(message)),
            };
            append_record_bytes(&mut contents, &record)?;
        }
        replace_private_transcript(&self.file, &contents)
    }

    pub fn clear_history(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let record = Record {
            session_id: self.id,
            cwd: self.cwd.clone(),
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            compact_boundary: true,
            message: None,
        };
        let mut contents = Vec::new();
        append_record_bytes(&mut contents, &record)?;
        replace_private_transcript(&self.file, &contents)
    }
}

fn project_directory(cwd: &Path) -> Result<PathBuf> {
    let home = dirs::home_dir().context("无法确定用户主目录")?;
    let key = workspace_key(cwd);
    let directory = home.join(".open-agent-harness/projects").join(key);
    ensure_private_directory(&directory)?;
    Ok(directory)
}

fn open_private_transcript(path: &Path) -> Result<fs::File> {
    if fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        bail!("拒绝追加 symlink transcript: {}", path.display())
    }
    if let Some(parent) = path.parent() {
        ensure_private_directory(parent)?;
    }
    let mut options = fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .with_context(|| format!("无法打开 transcript {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

fn load_messages(file: &Path) -> Result<Vec<Message>> {
    if fs::symlink_metadata(file)?.file_type().is_symlink() {
        bail!("拒绝从 symlink 恢复 transcript: {}", file.display())
    }
    let size = fs::metadata(file)?.len();
    if size > MAX_TRANSCRIPT_BYTES {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    let mut bytes = Vec::new();
    fs::File::open(file)?
        .take(MAX_TRANSCRIPT_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_TRANSCRIPT_BYTES as usize {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    let reader = BufReader::new(bytes.as_slice());
    reader
        .lines()
        .enumerate()
        .try_fold(Vec::new(), |mut messages, (index, line)| {
            let record: Record = serde_json::from_str(&line?)
                .with_context(|| format!("transcript 第 {} 行损坏", index + 1))?;
            if record.compact_boundary {
                messages.clear();
            }
            if let Some(message) = record.message {
                messages.push(message);
            }
            Ok(messages)
        })
}

fn append_record(file: &mut fs::File, record: &Record, size: &mut u64) -> Result<()> {
    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    let next = size
        .checked_add(line.len() as u64)
        .context("transcript 大小溢出")?;
    if next > MAX_TRANSCRIPT_BYTES {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    file.write_all(&line)?;
    *size = next;
    Ok(())
}

fn append_record_bytes(contents: &mut Vec<u8>, record: &Record) -> Result<()> {
    serde_json::to_writer(&mut *contents, record)?;
    contents.push(b'\n');
    if contents.len() > MAX_TRANSCRIPT_BYTES as usize {
        bail!("transcript 超过 {MAX_TRANSCRIPT_BYTES} 字节限制")
    }
    Ok(())
}

fn replace_private_transcript(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path.parent().context("transcript 路径缺少父目录")?;
    ensure_private_directory(parent)?;
    let temp = parent.join(format!(".open-agent-harness-{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(contents)?;
        file.flush()?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.with_context(|| format!("无法原子替换 transcript {}", path.display()))
}

fn sanitize_for_storage(message: &Message) -> Message {
    let mut sanitized = message.clone();
    let Some(blocks) = sanitized.content.as_array_mut() else {
        return sanitized;
    };
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("tool_use") => {
                if let Some(object) = block.as_object_mut() {
                    object.insert("input".into(), serde_json::json!({}));
                }
            }
            Some("tool_result") => {
                if let Some(object) = block.as_object_mut() {
                    object.insert(
                        "content".into(),
                        Value::String(
                            "Tool result omitted from the local transcript; run the tool again if its output is needed."
                                .into(),
                        ),
                    );
                }
            }
            _ => {}
        }
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_store_does_not_allocate_a_transcript_path() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStore::create(temp.path(), false).unwrap();
        assert!(store.file.as_os_str().is_empty());
        store
            .append(&[Message::user_text("not persisted")])
            .unwrap();
        assert!(store.file.as_os_str().is_empty());
    }

    #[test]
    fn compact_boundary_replaces_prior_history_on_resume() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStore {
            id: Uuid::new_v4(),
            cwd: temp.path().to_owned(),
            file: temp.path().join("session.jsonl"),
            enabled: true,
        };
        store
            .append(&[
                Message::user_text("old user"),
                Message::assistant(vec![serde_json::json!({"type":"text","text":"old reply"})]),
            ])
            .unwrap();
        store
            .replace_history(&[Message::user_text("compact summary")])
            .unwrap();
        assert!(
            !fs::read_to_string(&store.file)
                .unwrap()
                .contains("old user")
        );
        let loaded = load_messages(&store.file).unwrap();
        assert_eq!(loaded, vec![Message::user_text("compact summary")]);
    }

    #[test]
    fn clear_boundary_removes_all_prior_history() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStore {
            id: Uuid::new_v4(),
            cwd: temp.path().to_owned(),
            file: temp.path().join("session.jsonl"),
            enabled: true,
        };
        let sentinel = "clear-history-secret-sentinel";
        store.append(&[Message::user_text(sentinel)]).unwrap();
        store.clear_history().unwrap();
        assert!(!fs::read_to_string(&store.file).unwrap().contains(sentinel));
        assert!(load_messages(&store.file).unwrap().is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&store.file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn transcript_omits_tool_inputs_and_results() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStore {
            id: Uuid::new_v4(),
            cwd: temp.path().to_owned(),
            file: temp.path().join("session.jsonl"),
            enabled: true,
        };
        let sentinel = "private-sentinel-value";
        store
            .append(&[
                Message::assistant(vec![serde_json::json!({
                    "type":"tool_use", "id":"read-1", "name":"Read",
                    "input":{"file_path":sentinel}
                })]),
                Message::tool_results(vec![serde_json::json!({
                    "type":"tool_result", "tool_use_id":"read-1", "content":sentinel
                })]),
            ])
            .unwrap();
        let transcript = fs::read_to_string(&store.file).unwrap();
        assert!(!transcript.contains(sentinel));
        let loaded = load_messages(&store.file).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].content[0]["input"], serde_json::json!({}));
        assert!(
            loaded[1].content[0]["content"]
                .as_str()
                .unwrap()
                .contains("omitted")
        );
    }
}

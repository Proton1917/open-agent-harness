use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::Message;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    session_id: Uuid,
    cwd: PathBuf,
    timestamp_ms: u128,
    message: Message,
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
        let directory = project_directory(cwd)?;
        Ok(Self {
            id,
            cwd: cwd.to_owned(),
            file: directory.join(format!("{id}.jsonl")),
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
        if let Some(parent) = self.file.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file)
            .with_context(|| format!("无法打开 transcript {}", self.file.display()))?;
        for message in messages {
            let record = Record {
                session_id: self.id,
                cwd: self.cwd.clone(),
                timestamp_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
                message: message.clone(),
            };
            serde_json::to_writer(&mut file, &record)?;
            file.write_all(b"\n")?;
        }
        file.flush()?;
        Ok(())
    }
}

fn project_directory(cwd: &Path) -> Result<PathBuf> {
    let home = dirs::home_dir().context("无法确定用户主目录")?;
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
    let directory = home.join(".agent-harness/projects").join(key);
    fs::create_dir_all(&directory)?;
    Ok(directory)
}

fn load_messages(file: &Path) -> Result<Vec<Message>> {
    let reader = BufReader::new(fs::File::open(file)?);
    reader
        .lines()
        .enumerate()
        .map(|(index, line)| {
            let record: Record = serde_json::from_str(&line?)
                .with_context(|| format!("transcript 第 {} 行损坏", index + 1))?;
            Ok(record.message)
        })
        .collect()
}

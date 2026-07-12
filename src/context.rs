use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

const MAX_INSTRUCTION_BYTES: u64 = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionFile {
    pub path: PathBuf,
    pub content: String,
}

pub async fn discover_agent_instructions(cwd: &Path, bare: bool) -> Result<Vec<InstructionFile>> {
    if bare {
        return Ok(Vec::new());
    }

    let mut candidates = Vec::new();
    if let Some(home) = dirs::home_dir() {
        candidates.push(home.join(".open-agent-harness/AGENTS.md"));
    }

    let mut ancestors = cwd.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
    ancestors.reverse();
    candidates.extend(
        ancestors
            .into_iter()
            .map(|directory| directory.join("AGENTS.md")),
    );

    let mut instructions = Vec::new();
    for path in candidates {
        if instructions
            .iter()
            .any(|entry: &InstructionFile| entry.path == path)
        {
            continue;
        }
        let Ok(metadata) = tokio::fs::metadata(&path).await else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        if metadata.len() > MAX_INSTRUCTION_BYTES {
            anyhow::bail!(
                "指令文件过大（{} 字节，限制 {} 字节）: {}",
                metadata.len(),
                MAX_INSTRUCTION_BYTES,
                path.display()
            );
        }
        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("无法读取工程指令 {}", path.display()))?;
        if !content.trim().is_empty() {
            instructions.push(InstructionFile { path, content });
        }
    }
    Ok(instructions)
}

pub fn render_agent_instructions(files: &[InstructionFile]) -> String {
    if files.is_empty() {
        return String::new();
    }
    let mut rendered = String::from(
        "Engineering instructions are listed from broadest scope to most specific scope. Later files take precedence when they conflict.\n",
    );
    for file in files {
        rendered.push_str(&format!(
            "\n<agent_instructions path=\"{}\">\n{}\n</agent_instructions>\n",
            file.path.display(),
            file.content.trim()
        ));
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn loads_agents_files_from_broad_to_specific_scope() {
        let temp = tempfile::tempdir().unwrap();
        let nested = temp.path().join("a/b");
        tokio::fs::create_dir_all(&nested).await.unwrap();
        tokio::fs::write(temp.path().join("AGENTS.md"), "root rule")
            .await
            .unwrap();
        tokio::fs::write(temp.path().join("a/AGENTS.md"), "nested rule")
            .await
            .unwrap();

        let files = discover_agent_instructions(&nested, false).await.unwrap();
        let relevant = files
            .iter()
            .filter(|file| file.path.starts_with(temp.path()))
            .collect::<Vec<_>>();
        assert_eq!(relevant.len(), 2);
        assert_eq!(relevant[0].content, "root rule");
        assert_eq!(relevant[1].content, "nested rule");
    }

    #[tokio::test]
    async fn bare_mode_skips_all_instruction_discovery() {
        let temp = tempfile::tempdir().unwrap();
        tokio::fs::write(temp.path().join("AGENTS.md"), "rule")
            .await
            .unwrap();
        assert!(
            discover_agent_instructions(temp.path(), true)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

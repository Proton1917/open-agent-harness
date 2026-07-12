use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::io::AsyncReadExt;

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
        candidates.push((
            home.join(".open-agent-harness/AGENTS.md"),
            home.join(".open-agent-harness"),
        ));
    }

    let mut ancestors = cwd.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
    ancestors.reverse();
    candidates.extend(ancestors.into_iter().map(|directory| {
        let path = directory.join("AGENTS.md");
        (path, directory)
    }));

    let mut instructions = Vec::new();
    for (path, scope_root) in candidates {
        if instructions
            .iter()
            .any(|entry: &InstructionFile| entry.path == path)
        {
            continue;
        }
        let Ok(link_metadata) = tokio::fs::symlink_metadata(&path).await else {
            continue;
        };
        let canonical = tokio::fs::canonicalize(&path)
            .await
            .with_context(|| format!("无法解析工程指令 {}", path.display()))?;
        let canonical_scope = tokio::fs::canonicalize(&scope_root)
            .await
            .with_context(|| format!("无法解析工程指令作用域 {}", scope_root.display()))?;
        if !canonical.starts_with(&canonical_scope) {
            anyhow::bail!("工程指令 symlink 越过其作用域: {}", path.display());
        }
        let metadata = if link_metadata.file_type().is_symlink() {
            tokio::fs::metadata(&canonical).await?
        } else {
            link_metadata
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
        let mut bytes = Vec::new();
        tokio::fs::File::open(&canonical)
            .await
            .with_context(|| format!("无法打开工程指令 {}", path.display()))?
            .take(MAX_INSTRUCTION_BYTES + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > MAX_INSTRUCTION_BYTES as usize {
            anyhow::bail!(
                "指令文件过大（{} 字节，限制 {} 字节）: {}",
                bytes.len(),
                MAX_INSTRUCTION_BYTES,
                path.display()
            );
        }
        let content = String::from_utf8(bytes)
            .with_context(|| format!("工程指令不是有效 UTF-8: {}", path.display()))?;
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
    for (index, file) in files.iter().enumerate() {
        rendered.push_str(&format!(
            "\n<agent_instructions precedence=\"{}\">\n{}\n</agent_instructions>\n",
            index + 1,
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

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_instruction_symlink_outside_its_scope() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        let private = temp.path().join("private.txt");
        tokio::fs::write(&private, "must not load").await.unwrap();
        symlink(&private, workspace.join("AGENTS.md")).unwrap();
        let error = discover_agent_instructions(&workspace, false)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }
}

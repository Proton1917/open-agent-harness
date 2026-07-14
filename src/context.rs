use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::io::AsyncReadExt;

const MAX_INSTRUCTION_BYTES: u64 = 256 * 1024;
const MAX_INSTRUCTION_FILES: usize = 64;
const MAX_TOTAL_INSTRUCTION_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionFile {
    pub path: PathBuf,
    pub content: String,
}

/// Discovers instruction files below one trusted workspace root for a path the
/// session is about to touch. This is deliberately separate from launch-time
/// discovery: nested `AGENTS.md` files do not become applicable until a tool
/// reaches their directory tree.
pub async fn discover_nested_agent_instructions(
    workspace_root: &Path,
    target: &Path,
    bare: bool,
) -> Result<Vec<InstructionFile>> {
    if bare {
        return Ok(Vec::new());
    }
    let root = std::fs::canonicalize(workspace_root)
        .with_context(|| format!("无法解析嵌套工程指令工作区 {}", workspace_root.display()))?;
    if !root.is_dir() {
        anyhow::bail!("嵌套工程指令工作区不是目录");
    }
    let scope = target_directory_for_discovery(target)?;
    if !scope.starts_with(&root) {
        anyhow::bail!("嵌套工程指令目标越过可信工作区");
    }
    let relative = scope
        .strip_prefix(&root)
        .context("嵌套工程指令目标无法相对工作区表示")?;
    let mut directory = root.clone();
    let mut files = Vec::new();
    if let Some(file) = load_instruction_candidate(&directory.join("AGENTS.md"), &directory).await?
    {
        files.push(file);
    }
    for component in relative.components() {
        directory.push(component.as_os_str());
        if let Some(file) =
            load_instruction_candidate(&directory.join("AGENTS.md"), &directory).await?
        {
            files.push(file);
        }
    }
    if files.len() > MAX_INSTRUCTION_FILES
        || files.iter().map(|file| file.content.len()).sum::<usize>() > MAX_TOTAL_INSTRUCTION_BYTES
    {
        anyhow::bail!(
            "嵌套工程指令超过 {MAX_INSTRUCTION_FILES} 个文件或 {MAX_TOTAL_INSTRUCTION_BYTES} 字节总限制"
        );
    }
    Ok(files)
}

fn target_directory_for_discovery(target: &Path) -> Result<PathBuf> {
    match std::fs::metadata(target) {
        Ok(metadata) if metadata.is_dir() => std::fs::canonicalize(target)
            .with_context(|| format!("无法解析工程指令目标 {}", target.display())),
        Ok(_) => {
            let parent = target.parent().context("工程指令目标缺少父目录")?;
            std::fs::canonicalize(parent)
                .with_context(|| format!("无法解析工程指令父目录 {}", parent.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut parent = target.parent().context("工程指令目标缺少父目录")?;
            loop {
                match std::fs::canonicalize(parent) {
                    Ok(canonical) => return Ok(canonical),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        parent = parent.parent().context("无法找到已存在的工程指令父目录")?;
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("无法解析工程指令父目录 {}", parent.display())
                        });
                    }
                }
            }
        }
        Err(error) => {
            Err(error).with_context(|| format!("无法检查工程指令目标 {}", target.display()))
        }
    }
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
        if let Some(file) = load_instruction_candidate(&path, &scope_root).await? {
            let next_total = instructions
                .iter()
                .map(|entry| entry.content.len())
                .sum::<usize>()
                .saturating_add(file.content.len());
            if instructions.len() >= MAX_INSTRUCTION_FILES
                || next_total > MAX_TOTAL_INSTRUCTION_BYTES
            {
                anyhow::bail!(
                    "工程指令超过 {MAX_INSTRUCTION_FILES} 个文件或 {MAX_TOTAL_INSTRUCTION_BYTES} 字节总限制"
                );
            }
            instructions.push(file);
        }
    }
    Ok(instructions)
}

async fn load_instruction_candidate(
    path: &Path,
    scope_root: &Path,
) -> Result<Option<InstructionFile>> {
    let link_metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("无法检查工程指令 {}", path.display()));
        }
    };
    let canonical = tokio::fs::canonicalize(path)
        .await
        .with_context(|| format!("无法解析工程指令 {}", path.display()))?;
    let canonical_scope = tokio::fs::canonicalize(scope_root)
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
        return Ok(None);
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
    Ok((!content.trim().is_empty()).then(|| InstructionFile {
        path: path.to_owned(),
        content,
    }))
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

    #[tokio::test]
    async fn nested_discovery_is_root_bounded_and_ordered() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        let target = root.join("crates/core/src/lib.rs");
        tokio::fs::create_dir_all(target.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(root.join("AGENTS.md"), "root")
            .await
            .unwrap();
        tokio::fs::write(root.join("crates/AGENTS.md"), "crates")
            .await
            .unwrap();
        tokio::fs::write(root.join("crates/core/AGENTS.md"), "core")
            .await
            .unwrap();
        tokio::fs::create_dir_all(root.join("sibling"))
            .await
            .unwrap();
        tokio::fs::write(root.join("sibling/AGENTS.md"), "sibling")
            .await
            .unwrap();

        let files = discover_nested_agent_instructions(&root, &target, false)
            .await
            .unwrap();
        assert_eq!(
            files
                .iter()
                .map(|file| file.content.as_str())
                .collect::<Vec<_>>(),
            ["root", "crates", "core"]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nested_discovery_rejects_a_target_symlink_escape() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::create_dir_all(&outside).await.unwrap();
        symlink(&outside, root.join("linked")).unwrap();
        assert!(
            discover_nested_agent_instructions(&root, &root.join("linked/file.rs"), false)
                .await
                .is_err()
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

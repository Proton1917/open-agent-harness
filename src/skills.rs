use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

const MAX_SKILL_BYTES: u64 = 192 * 1024;
const MAX_SKILLS: usize = 128;

#[derive(Debug, Clone, Default)]
pub struct SkillCatalog {
    entries: BTreeMap<String, SkillDefinition>,
}

#[derive(Debug, Clone)]
pub struct SkillDefinition {
    pub name: String,
    pub description: String,
    pub path: PathBuf,
    pub content: String,
}

impl SkillCatalog {
    pub fn get(&self, name: &str) -> Option<&SkillDefinition> {
        self.entries.get(name)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SkillDefinition)> {
        self.entries.iter()
    }
}

pub fn discover_skills(cwd: &Path, bare: bool) -> Result<SkillCatalog> {
    if bare {
        return Ok(SkillCatalog::default());
    }
    let mut roots = Vec::new();
    if let Some(home) = dirs::home_dir() {
        roots.push((
            home.join(".open-agent-harness/skills"),
            home.join(".open-agent-harness"),
        ));
    }
    let mut ancestors = cwd.ancestors().collect::<Vec<_>>();
    ancestors.reverse();
    roots.extend(ancestors.into_iter().map(|directory| {
        (
            directory.join(".open-agent-harness/skills"),
            directory.to_path_buf(),
        )
    }));
    discover_from_roots(&roots)
}

pub fn render_skill_index(catalog: &SkillCatalog) -> String {
    if catalog.is_empty() {
        return String::new();
    }
    let entries = catalog
        .iter()
        .map(|(name, skill)| {
            let description = skill
                .description
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            format!(
                "- {name}: {}",
                description.chars().take(240).collect::<String>()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "# Available local skills\n\nUse the Skill tool to load one of these user-provided workflows only when it is relevant:\n{entries}"
    )
}

fn discover_from_roots(roots: &[(PathBuf, PathBuf)]) -> Result<SkillCatalog> {
    let mut catalog = SkillCatalog::default();
    for (root, scope_root) in roots {
        if !root.is_dir() {
            continue;
        }
        let canonical_root = fs::canonicalize(root)
            .with_context(|| format!("无法解析 skills 目录 {}", root.display()))?;
        let canonical_scope = fs::canonicalize(scope_root)
            .with_context(|| format!("无法解析 skills 作用域 {}", scope_root.display()))?;
        if !canonical_root.starts_with(&canonical_scope) {
            bail!("skills 目录 symlink 越过作用域: {}", root.display())
        }
        let mut files = fs::read_dir(root)?
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path().join("SKILL.md"))
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        files.sort();
        for path in files {
            let canonical = fs::canonicalize(&path)
                .with_context(|| format!("无法解析 skill {}", path.display()))?;
            if !canonical.starts_with(&canonical_root) {
                bail!("skill symlink 越过发现目录: {}", path.display())
            }
            let metadata = fs::metadata(&canonical)?;
            if metadata.len() > MAX_SKILL_BYTES {
                bail!(
                    "skill {} 超过 {} 字节限制",
                    canonical.display(),
                    MAX_SKILL_BYTES
                )
            }
            let mut bytes = Vec::new();
            fs::File::open(&canonical)?
                .take(MAX_SKILL_BYTES + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > MAX_SKILL_BYTES as usize {
                bail!(
                    "skill {} 超过 {} 字节限制",
                    canonical.display(),
                    MAX_SKILL_BYTES
                )
            }
            let content = String::from_utf8(bytes)
                .with_context(|| format!("skill 不是有效 UTF-8: {}", canonical.display()))?;
            let fallback = canonical
                .parent()
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .context("skill 目录名不是有效 UTF-8")?;
            let (name, description) = parse_frontmatter(&content, fallback)?;
            catalog.entries.insert(
                name.clone(),
                SkillDefinition {
                    name,
                    description,
                    path: canonical,
                    content,
                },
            );
            if catalog.len() > MAX_SKILLS {
                bail!("skill 数量超过 {MAX_SKILLS} 个限制")
            }
        }
    }
    Ok(catalog)
}

fn parse_frontmatter(content: &str, fallback: &str) -> Result<(String, String)> {
    let mut name = fallback.to_owned();
    let mut description = format!("Local workflow from {fallback}");
    if let Some(rest) = content.strip_prefix("---\n")
        && let Some(end) = rest.find("\n---")
    {
        for line in rest[..end].lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim().trim_matches(['\'', '"']);
            match key.trim() {
                "name" if !value.is_empty() => name = value.to_owned(),
                "description" if !value.is_empty() => description = value.to_owned(),
                _ => {}
            }
        }
    }
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_-:".contains(character))
    {
        bail!("无效 skill name: {name}")
    }
    Ok((name, description))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearer_skill_roots_override_broader_roots() {
        let temp = tempfile::tempdir().unwrap();
        let broad = temp.path().join("broad");
        let near = temp.path().join("near");
        for (root, marker) in [(&broad, "broad"), (&near, "near")] {
            let skill = root.join("demo");
            fs::create_dir_all(&skill).unwrap();
            fs::write(
                skill.join("SKILL.md"),
                format!("---\nname: demo\ndescription: {marker}\n---\n{marker}"),
            )
            .unwrap();
        }
        let catalog = discover_from_roots(&[(broad.clone(), broad), (near.clone(), near)]).unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.get("demo").unwrap().description, "near");
        assert!(render_skill_index(&catalog).contains("demo: near"));
    }

    #[cfg(unix)]
    #[test]
    fn project_skill_root_cannot_escape_its_scope() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let private = temp.path().join("private-skills");
        fs::create_dir_all(private.join("secret")).unwrap();
        fs::write(private.join("secret/SKILL.md"), "secret").unwrap();
        fs::create_dir_all(workspace.join(".open-agent-harness")).unwrap();
        symlink(&private, workspace.join(".open-agent-harness/skills")).unwrap();
        let root = workspace.join(".open-agent-harness/skills");
        let error = discover_from_roots(&[(root, workspace)]).unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }
}

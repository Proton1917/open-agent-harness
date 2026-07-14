use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    path::Path,
    time::{Instant, SystemTime},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use globset::Glob;
use ignore::{DirEntry, Walk, WalkBuilder};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Tool, ToolContext, ToolOutput, object_schema, parse_input};

const MAX_VISITED_ENTRIES: usize = 200_000;
const MAX_SEARCH_TIME: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Deserialize)]
struct Input {
    pattern: String,
    path: Option<String>,
}

pub struct GlobTool;

#[async_trait]
impl Tool for GlobTool {
    fn name(&self) -> &str {
        "Glob"
    }
    fn description(&self) -> &str {
        "Finds files by glob pattern, returning up to 100 paths relative to the working directory."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "pattern": {"type": "string", "maxLength": 4096},
                "path": {"type": "string", "maxLength": 4096, "description": "Search root; defaults to cwd"}
            }),
            &["pattern"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        true
    }
    fn path_fields(&self) -> &'static [&'static str] {
        &["path"]
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("pattern")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        let root = match input.path {
            Some(path) => context.resolve_path(&path)?,
            None => context.cwd(),
        };
        if !root.is_dir() {
            bail!("搜索根目录不存在或不是目录: {}", root.display())
        }
        let matcher = Glob::new(&input.pattern)
            .context("无效 glob pattern")?
            .compile_matcher();
        let started = Instant::now();
        let mut files = BinaryHeap::new();
        let mut truncated = false;
        let mut visited = 0usize;
        for entry in search_walker(&root, context) {
            visited += 1;
            if visited > MAX_VISITED_ENTRIES || started.elapsed() > MAX_SEARCH_TIME {
                truncated = true;
                break;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    truncated = true;
                    continue;
                }
            };
            if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                continue;
            }
            let relative = entry.path().strip_prefix(&root).unwrap_or(entry.path());
            if matcher.is_match(relative) || matcher.is_match(entry.path()) {
                let modified = entry
                    .metadata()
                    .ok()
                    .and_then(|metadata| metadata.modified().ok())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let path = display_relative(context, entry.path());
                files.push(Reverse((modified, Reverse(path))));
                if files.len() > 100 {
                    files.pop();
                    truncated = true;
                }
            }
        }
        let mut files = files
            .into_iter()
            .map(|Reverse((modified, Reverse(path)))| GlobMatch { modified, path })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| {
            right
                .modified
                .cmp(&left.modified)
                .then_with(|| left.path.cmp(&right.path))
        });
        if files.is_empty() {
            return Ok(ToolOutput::success(if truncated {
                "No files found before the traversal limit was reached"
            } else {
                "No files found"
            }));
        }
        let mut output = files
            .iter()
            .map(|matched| matched.path.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if truncated {
            output.push_str("\n(Results are truncated. Use a more specific pattern.)");
        }
        output.push_str(&format!(
            "\n\nFound {} files in {}ms",
            files.len(),
            started.elapsed().as_millis()
        ));
        Ok(ToolOutput::success(output))
    }
}

struct GlobMatch {
    modified: SystemTime,
    path: String,
}

fn search_walker(root: &Path, context: &ToolContext) -> Walk {
    let safety_context = context.clone();
    let mut builder = WalkBuilder::new(root);
    builder
        .follow_links(false)
        // Hidden paths remain searchable, matching the observable reference
        // behavior. Ignore files and Git excludes are still authoritative.
        .hidden(false)
        .ignore(true)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .parents(true)
        .filter_entry(move |entry| {
            include_entry(entry) && !safety_context.read_path_denied(entry.path())
        });
    builder.build()
}

fn include_entry(entry: &DirEntry) -> bool {
    if entry.depth() == 0 {
        return true;
    }
    !matches!(
        entry.file_name().to_str(),
        Some(".git" | ".svn" | ".hg" | ".bzr" | ".jj" | ".sl")
    )
}

fn display_relative(context: &ToolContext, path: &Path) -> String {
    context.display_path(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{PermissionManager, PermissionMode};
    use std::{
        fs::{File, FileTimes},
        time::Duration,
    };

    fn test_context(root: &Path) -> ToolContext {
        ToolContext::new(
            root.to_owned(),
            PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
        )
    }

    async fn run_glob(root: &Path, pattern: &str) -> String {
        GlobTool
            .execute(&test_context(root), json!({"pattern":pattern,"path":"."}))
            .await
            .unwrap()
            .content
    }

    #[tokio::test]
    async fn glob_respects_ignore_negation_git_exclude_and_includes_hidden() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir_all(root.join(".git/info")).unwrap();
        std::fs::create_dir_all(root.join("build")).unwrap();
        std::fs::create_dir_all(root.join("cache")).unwrap();
        std::fs::create_dir_all(root.join(".hidden")).unwrap();
        std::fs::write(
            root.join(".gitignore"),
            "build/\n*.cache\n!important.cache\n",
        )
        .unwrap();
        std::fs::write(root.join(".ignore"), "cache/\n").unwrap();
        std::fs::write(root.join(".git/info/exclude"), "excluded.txt\n").unwrap();
        for path in [
            "visible.txt",
            "important.cache",
            "discard.cache",
            "build/artifact.txt",
            "cache/index.txt",
            "excluded.txt",
            ".hidden/visible.txt",
        ] {
            std::fs::write(root.join(path), "fixture\n").unwrap();
        }

        let output = run_glob(root, "**").await;
        assert!(output.contains("visible.txt"));
        assert!(output.contains("important.cache"));
        assert!(output.contains(".hidden/visible.txt"));
        assert!(!output.contains("discard.cache"));
        assert!(!output.contains("build/artifact.txt"));
        assert!(!output.contains("cache/index.txt"));
        assert!(!output.contains("excluded.txt"));
    }

    #[tokio::test]
    async fn glob_sorts_all_matches_by_newest_mtime_before_truncating() {
        let temp = tempfile::tempdir().unwrap();
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for index in 0..120u64 {
            let path = temp.path().join(format!("file-{index:03}.txt"));
            std::fs::write(&path, "fixture\n").unwrap();
            let modified_index = if index == 118 { 119 } else { index };
            File::options()
                .write(true)
                .open(path)
                .unwrap()
                .set_times(
                    FileTimes::new().set_modified(base + Duration::from_secs(modified_index)),
                )
                .unwrap();
        }

        let output = run_glob(temp.path(), "*.txt").await;
        let paths = output
            .lines()
            .take_while(|line| !line.starts_with("(Results are truncated"))
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 100);
        assert_eq!(paths[0], "file-118.txt");
        assert_eq!(paths[1], "file-119.txt");
        assert_eq!(paths[2], "file-117.txt");
        assert!(paths.contains(&"file-020.txt"));
        assert!(!paths.contains(&"file-019.txt"));
        assert!(output.contains("Results are truncated"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn glob_does_not_follow_symlinks_outside_the_search_root() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("outside.txt"), "outside\n").unwrap();
        symlink(outside.path(), root.path().join("linked-outside")).unwrap();
        let output = run_glob(root.path(), "**/*.txt").await;
        assert_eq!(output, "No files found");
    }
}

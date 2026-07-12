use std::time::Instant;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use globset::Glob;
use serde::Deserialize;
use serde_json::{Value, json};
use walkdir::{DirEntry, WalkDir};

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
        let mut files = Vec::new();
        let mut truncated = false;
        let mut visited = 0usize;
        for entry in WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_entry(include_entry)
        {
            visited += 1;
            if visited > MAX_VISITED_ENTRIES || started.elapsed() > MAX_SEARCH_TIME {
                truncated = true;
                break;
            }
            let entry = entry.with_context(|| format!("遍历 {} 失败", root.display()))?;
            if !entry.file_type().is_file() {
                continue;
            }
            let relative = entry.path().strip_prefix(&root).unwrap_or(entry.path());
            if matcher.is_match(relative) || matcher.is_match(entry.path()) {
                if files.len() == 100 {
                    truncated = true;
                    break;
                }
                files.push(display_relative(context, entry.path()));
            }
        }
        files.sort();
        if files.is_empty() {
            return Ok(ToolOutput::success(if truncated {
                "No files found before the traversal limit was reached"
            } else {
                "No files found"
            }));
        }
        let mut output = files.join("\n");
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

fn include_entry(entry: &DirEntry) -> bool {
    if entry.depth() == 0 {
        return true;
    }
    !matches!(
        entry.file_name().to_str(),
        Some(".git" | ".svn" | ".hg" | ".bzr" | ".jj" | ".sl")
    )
}

fn display_relative(context: &ToolContext, path: &std::path::Path) -> String {
    path.strip_prefix(context.cwd())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| path.display().to_string())
}

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use globset::{Glob, GlobMatcher};
use ignore::{DirEntry, Walk, WalkBuilder};
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    Tool, ToolContext, ToolOutput, normalize_path_for_display, object_schema, parse_input,
};

const MAX_RESULT_BYTES: usize = 240 * 1024;
const MAX_RECORD_BYTES: usize = 64 * 1024;
const MAX_FILE_BYTES: u64 = 1024 * 1024;
const MAX_TOTAL_SCAN_BYTES: u64 = 32 * 1024 * 1024;
const MAX_FILES: usize = 100_000;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
struct Input {
    pattern: String,
    path: Option<String>,
    glob: Option<String>,
    #[serde(default)]
    output_mode: OutputMode,
    #[serde(rename = "-B")]
    before: Option<u32>,
    #[serde(rename = "-A")]
    after: Option<u32>,
    #[serde(rename = "-C")]
    context_short: Option<u32>,
    context: Option<u32>,
    #[serde(rename = "-n", default = "default_line_numbers")]
    line_numbers: bool,
    #[serde(rename = "-i", default)]
    case_insensitive: bool,
    r#type: Option<String>,
    head_limit: Option<usize>,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    multiline: bool,
}

fn default_line_numbers() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OutputMode {
    Content,
    #[default]
    FilesWithMatches,
    Count,
}

pub struct GrepTool;

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "Grep"
    }

    fn description(&self) -> &str {
        "Searches text file contents with Rust regexes. Supports content, files_with_matches, and count modes plus pagination."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "pattern": {"type": "string", "maxLength": 65536},
                "path": {"type": "string", "maxLength": 4096},
                "glob": {"type": "string", "maxLength": 4096},
                "output_mode": {"type": "string", "enum": ["content", "files_with_matches", "count"]},
                "-B": {"type": "integer", "minimum": 0, "maximum": 10000},
                "-A": {"type": "integer", "minimum": 0, "maximum": 10000},
                "-C": {"type": "integer", "minimum": 0, "maximum": 10000},
                "context": {"type": "integer", "minimum": 0, "maximum": 10000},
                "-n": {"type": "boolean"},
                "-i": {"type": "boolean"},
                "type": {"type": "string", "maxLength": 128},
                "head_limit": {"type": "integer", "minimum": 0, "maximum": 100000},
                "offset": {"type": "integer", "minimum": 0, "maximum": 10000000},
                "multiline": {"type": "boolean"}
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
        let root = match &input.path {
            Some(path) => context.resolve_path(path)?,
            None => context.cwd(),
        };
        if !root.exists() {
            bail!("搜索路径不存在: {}", context.display_path(&root))
        }
        if !root.is_file() && !root.is_dir() {
            bail!(
                "搜索路径不是普通文件或目录: {}",
                context.display_path(&root)
            )
        }
        let cwd = context.cwd();
        let context = context.clone();
        let result = tokio::task::spawn_blocking(move || search(root, cwd, input, &context))
            .await
            .context("Rust Grep worker 失败")??;
        Ok(ToolOutput::success(result))
    }
}

fn search(root: PathBuf, cwd: PathBuf, input: Input, context: &ToolContext) -> Result<String> {
    let regex = RegexBuilder::new(&input.pattern)
        .case_insensitive(input.case_insensitive)
        .multi_line(input.multiline)
        .dot_matches_new_line(input.multiline)
        .size_limit(16 * 1024 * 1024)
        .dfa_size_limit(16 * 1024 * 1024)
        .build()
        .context("无效 regex pattern")?;
    let glob = input
        .glob
        .as_deref()
        .map(Glob::new)
        .transpose()
        .context("无效 glob filter")?
        .map(|glob| glob.compile_matcher());
    let symmetric = input.context.or(input.context_short).unwrap_or(0) as usize;
    let options = SearchOptions {
        regex,
        glob,
        kind: input.r#type.map(|kind| kind.to_ascii_lowercase()),
        output_mode: input.output_mode,
        line_numbers: input.line_numbers,
        before: input.before.map_or(symmetric, |value| value as usize),
        after: input.after.map_or(symmetric, |value| value as usize),
        multiline: input.multiline,
    };
    let limit = match input.head_limit {
        Some(0) => usize::MAX,
        Some(limit) => limit,
        None => 250,
    };
    let mut collector = Collector::new(input.offset, limit);
    let mut budget = SearchBudget::new();

    if root.is_file() {
        scan_candidate(
            &root,
            &root,
            &cwd,
            context,
            &options,
            &mut collector,
            &mut budget,
        )?;
    } else {
        for entry in search_walker(&root, context) {
            budget.check_time()?;
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    budget.limited = true;
                    continue;
                }
            };
            if !entry.file_type().is_some_and(|kind| kind.is_file()) {
                continue;
            }
            if budget.files >= MAX_FILES {
                budget.limited = true;
                break;
            }
            budget.files += 1;
            if !scan_candidate(
                entry.path(),
                &root,
                &cwd,
                context,
                &options,
                &mut collector,
                &mut budget,
            )? {
                break;
            }
        }
    }
    collector.finish(input.offset, limit, budget.limited)
}

fn search_walker(root: &Path, context: &ToolContext) -> Walk {
    let safety_context = context.clone();
    let mut builder = WalkBuilder::new(root);
    builder
        .follow_links(false)
        // Reference search includes hidden files. Ignore rules still apply to
        // hidden paths, and VCS metadata is excluded explicitly below.
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

struct SearchOptions {
    regex: Regex,
    glob: Option<GlobMatcher>,
    kind: Option<String>,
    output_mode: OutputMode,
    line_numbers: bool,
    before: usize,
    after: usize,
    multiline: bool,
}

struct SearchBudget {
    started: Instant,
    files: usize,
    scanned_bytes: u64,
    limited: bool,
}

impl SearchBudget {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            files: 0,
            scanned_bytes: 0,
            limited: false,
        }
    }

    fn check_time(&self) -> Result<()> {
        if self.started.elapsed() > SEARCH_TIMEOUT {
            bail!("Grep 搜索超过 {} 秒限制", SEARCH_TIMEOUT.as_secs())
        }
        Ok(())
    }
}

fn scan_candidate(
    path: &Path,
    root: &Path,
    cwd: &Path,
    context: &ToolContext,
    options: &SearchOptions,
    collector: &mut Collector,
    budget: &mut SearchBudget,
) -> Result<bool> {
    budget.check_time()?;
    if context.read_path_denied(path) {
        return Ok(true);
    }
    let relative = path.strip_prefix(root).unwrap_or(path);
    if !matches_filters(path, relative, options) {
        return Ok(true);
    }
    let metadata = path
        .metadata()
        .with_context(|| format!("无法检查搜索文件 {}", display_path(path, cwd)))?;
    if metadata.len() > MAX_FILE_BYTES {
        budget.limited = true;
        return Ok(true);
    }
    if budget.scanned_bytes.saturating_add(metadata.len()) > MAX_TOTAL_SCAN_BYTES {
        budget.limited = true;
        return Ok(false);
    }

    let mut bytes = Vec::new();
    File::open(path)
        .with_context(|| format!("无法打开搜索文件 {}", display_path(path, cwd)))?
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_FILE_BYTES as usize {
        budget.limited = true;
        return Ok(true);
    }
    if budget.scanned_bytes.saturating_add(bytes.len() as u64) > MAX_TOTAL_SCAN_BYTES {
        budget.limited = true;
        return Ok(false);
    }
    budget.scanned_bytes = budget.scanned_bytes.saturating_add(bytes.len() as u64);
    if bytes.contains(&0) {
        return Ok(true);
    }
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(true);
    };
    let label = display_path(path, cwd);
    if options.multiline {
        scan_multiline(text, &label, options, collector, budget)
    } else {
        scan_lines(text, &label, options, collector, budget)
    }
}

fn matches_filters(path: &Path, relative: &Path, options: &SearchOptions) -> bool {
    if let Some(glob) = &options.glob {
        let file_name_matches = path
            .file_name()
            .is_some_and(|file_name| glob.is_match(Path::new(file_name)));
        if !glob.is_match(relative) && !file_name_matches {
            return false;
        }
    }
    options
        .kind
        .as_deref()
        .is_none_or(|kind| matches_type(path, kind))
}

fn matches_type(path: &Path, kind: &str) -> bool {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let allowed: &[&str] = match kind.trim_start_matches('.') {
        "rust" => &["rs"],
        "python" => &["py", "pyi"],
        "javascript" | "js" => &["js", "jsx", "mjs", "cjs"],
        "typescript" | "ts" => &["ts", "tsx", "mts", "cts"],
        "json" => &["json", "jsonl"],
        "toml" => &["toml"],
        "yaml" => &["yaml", "yml"],
        "markdown" | "md" => &["md", "mdx", "markdown"],
        "shell" | "sh" => &["sh", "bash", "zsh", "fish"],
        "c" => &["c", "h"],
        "cpp" | "c++" => &["cc", "cpp", "cxx", "hh", "hpp", "hxx"],
        "java" => &["java"],
        "go" => &["go"],
        "ruby" => &["rb"],
        "swift" => &["swift"],
        "text" | "txt" => &["txt"],
        other => return extension == other,
    };
    allowed.contains(&extension.as_str())
}

fn scan_lines(
    text: &str,
    label: &str,
    options: &SearchOptions,
    collector: &mut Collector,
    budget: &SearchBudget,
) -> Result<bool> {
    let lines = text.split('\n').collect::<Vec<_>>();
    match options.output_mode {
        OutputMode::FilesWithMatches => {
            for (index, line) in lines.iter().enumerate() {
                if index % 4096 == 0 {
                    budget.check_time()?;
                }
                if options.regex.is_match(line.trim_end_matches('\r')) {
                    return Ok(collector.push(label.to_owned()));
                }
            }
            Ok(true)
        }
        OutputMode::Count => {
            let mut count = 0usize;
            for (index, line) in lines.iter().enumerate() {
                if index % 4096 == 0 {
                    budget.check_time()?;
                }
                if options.regex.is_match(line.trim_end_matches('\r')) {
                    count += 1;
                }
            }
            if count == 0 {
                Ok(true)
            } else {
                Ok(collector.push(format!("{label}:{count}")))
            }
        }
        OutputMode::Content => {
            for (index, line) in lines.iter().enumerate() {
                if index % 4096 == 0 {
                    budget.check_time()?;
                }
                if options.regex.is_match(line.trim_end_matches('\r')) {
                    let record = render_window(
                        &lines,
                        label,
                        index,
                        index,
                        options.before,
                        options.after,
                        options.line_numbers,
                    );
                    if !collector.push(record) {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        }
    }
}

fn scan_multiline(
    text: &str,
    label: &str,
    options: &SearchOptions,
    collector: &mut Collector,
    budget: &SearchBudget,
) -> Result<bool> {
    if !options.regex.is_match(text) {
        return Ok(true);
    }
    match options.output_mode {
        OutputMode::FilesWithMatches => Ok(collector.push(label.to_owned())),
        OutputMode::Count => {
            Ok(collector.push(format!("{label}:{}", options.regex.find_iter(text).count())))
        }
        OutputMode::Content => {
            let lines = text.split('\n').collect::<Vec<_>>();
            let starts = std::iter::once(0)
                .chain(text.match_indices('\n').map(|(index, _)| index + 1))
                .collect::<Vec<_>>();
            for (index, found) in options.regex.find_iter(text).enumerate() {
                if index % 1024 == 0 {
                    budget.check_time()?;
                }
                let start_line = line_index(&starts, found.start());
                let end_offset = found.end().saturating_sub(1).max(found.start());
                let end_line = line_index(&starts, end_offset);
                let record = render_window(
                    &lines,
                    label,
                    start_line,
                    end_line,
                    options.before,
                    options.after,
                    options.line_numbers,
                );
                if !collector.push(record) {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn line_index(starts: &[usize], offset: usize) -> usize {
    starts
        .partition_point(|start| *start <= offset)
        .saturating_sub(1)
}

fn render_window(
    lines: &[&str],
    label: &str,
    match_start: usize,
    match_end: usize,
    before: usize,
    after: usize,
    line_numbers: bool,
) -> String {
    let start = match_start.saturating_sub(before);
    let end = match_end
        .saturating_add(after)
        .saturating_add(1)
        .min(lines.len());
    let mut output = String::new();
    for (index, line) in lines.iter().enumerate().take(end).skip(start) {
        let matched = (match_start..=match_end).contains(&index);
        let separator = if matched { ':' } else { '-' };
        let line = line.trim_end_matches('\r');
        let mut rendered = if line_numbers {
            format!("{label}{separator}{}{separator}{line}", index + 1)
        } else {
            format!("{label}{separator}{line}")
        };
        let separator_bytes = usize::from(!output.is_empty());
        let remaining = MAX_RECORD_BYTES.saturating_sub(output.len() + separator_bytes);
        if rendered.len() > remaining {
            const MARKER: &str = "\n[context window truncated]";
            truncate_utf8(&mut rendered, remaining.saturating_sub(MARKER.len()));
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&rendered);
            output.push_str(MARKER);
            break;
        }
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&rendered);
    }
    output
}

struct Collector {
    offset: usize,
    limit: usize,
    seen: usize,
    bytes: usize,
    entries: Vec<String>,
    truncated: bool,
}

impl Collector {
    fn new(offset: usize, limit: usize) -> Self {
        Self {
            offset,
            limit,
            seen: 0,
            bytes: 0,
            entries: Vec::new(),
            truncated: false,
        }
    }

    fn push(&mut self, mut record: String) -> bool {
        self.seen += 1;
        if self.seen <= self.offset {
            return true;
        }
        if self.entries.len() >= self.limit {
            self.truncated = true;
            return false;
        }
        if record.len() > MAX_RECORD_BYTES {
            truncate_utf8(&mut record, MAX_RECORD_BYTES - 32);
            record.push_str("\n[match record truncated]");
            self.truncated = true;
        }
        let added = record.len() + usize::from(!self.entries.is_empty());
        if self.bytes.saturating_add(added) > MAX_RESULT_BYTES {
            self.truncated = true;
            return false;
        }
        self.bytes += added;
        self.entries.push(record);
        true
    }

    fn finish(mut self, offset: usize, limit: usize, resource_limited: bool) -> Result<String> {
        self.truncated |= resource_limited;
        if self.entries.is_empty() {
            return Ok(if self.seen > 0 {
                format!("No results returned at offset {offset}")
            } else if self.truncated {
                "No matches found before a search resource limit was reached".into()
            } else {
                "No matches found".into()
            });
        }
        let mut output = self.entries.join("\n");
        if self.truncated {
            let marker = format!(
                "\n\n[Showing bounded results with pagination = limit: {}, offset: {}]",
                if limit == usize::MAX { 0 } else { limit },
                offset
            );
            if output.len().saturating_add(marker.len()) > MAX_RESULT_BYTES {
                truncate_utf8(&mut output, MAX_RESULT_BYTES.saturating_sub(marker.len()));
            }
            output.push_str(&marker);
        }
        debug_assert!(output.len() <= MAX_RESULT_BYTES);
        Ok(output)
    }
}

fn truncate_utf8(value: &mut String, mut end: usize) {
    end = end.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
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

fn display_path(path: &Path, cwd: &Path) -> String {
    let rendered = if let Ok(relative) = path.strip_prefix(cwd) {
        if relative.as_os_str().is_empty() {
            ".".into()
        } else {
            relative.display().to_string()
        }
    } else if let Some(relative) =
        dirs::home_dir().and_then(|home| path.strip_prefix(home).ok().map(Path::to_path_buf))
    {
        format!("~/{}", relative.display())
    } else {
        path.display().to_string()
    };
    normalize_path_for_display(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{PermissionManager, PermissionMode};

    fn test_context(root: &Path) -> ToolContext {
        ToolContext::new(
            root.to_owned(),
            PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
        )
    }

    fn files_input(pattern: &str) -> Input {
        Input {
            pattern: pattern.to_owned(),
            path: None,
            glob: None,
            output_mode: OutputMode::FilesWithMatches,
            before: None,
            after: None,
            context_short: None,
            context: None,
            line_numbers: true,
            case_insensitive: false,
            r#type: None,
            head_limit: Some(0),
            offset: 0,
            multiline: false,
        }
    }

    #[test]
    fn collector_applies_offset_limit_and_utf8_safe_bounds() {
        let mut collector = Collector::new(1, 1);
        assert!(collector.push("skip".into()));
        assert!(collector.push("保留".into()));
        assert!(!collector.push("extra".into()));
        let output = collector.finish(1, 1, false).unwrap();
        assert!(output.contains("保留"));
        assert!(output.contains("bounded results"));
    }

    #[test]
    fn collector_preserves_match_order_and_total_byte_ceiling() {
        let mut ordered = Collector::new(0, usize::MAX);
        assert!(ordered.push("first-match".into()));
        assert!(ordered.push("second-match".into()));
        let output = ordered.finish(0, usize::MAX, false).unwrap();
        assert!(output.find("first-match").unwrap() < output.find("second-match").unwrap());

        let mut bounded = Collector::new(0, usize::MAX);
        let record = "界".repeat(MAX_RECORD_BYTES / '界'.len_utf8());
        while bounded.push(record.clone()) {}
        let output = bounded.finish(0, usize::MAX, false).unwrap();
        assert!(output.len() <= MAX_RESULT_BYTES);
        assert!(output.contains("Showing bounded results"));
        assert!(std::str::from_utf8(output.as_bytes()).is_ok());
    }

    #[test]
    fn type_filters_cover_common_rust_and_text_files() {
        assert!(matches_type(Path::new("src/main.rs"), "rust"));
        assert!(matches_type(Path::new("notes.txt"), "text"));
        assert!(!matches_type(Path::new("src/main.rs"), "python"));
    }

    #[test]
    fn walker_respects_ignore_negation_git_exclude_and_includes_hidden() {
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
            std::fs::write(root.join(path), "search-needle\n").unwrap();
        }
        let context = test_context(root);
        let output = search(
            root.to_owned(),
            root.to_owned(),
            files_input("search-needle"),
            &context,
        )
        .unwrap();
        assert!(output.contains("visible.txt"));
        assert!(output.contains("important.cache"));
        assert!(output.contains(".hidden/visible.txt"));
        assert!(!output.contains("discard.cache"));
        assert!(!output.contains("build/artifact.txt"));
        assert!(!output.contains("cache/index.txt"));
        assert!(!output.contains("excluded.txt"));
    }

    #[cfg(unix)]
    #[test]
    fn walker_does_not_follow_symlinks_outside_the_search_root() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(
            outside.path().join("outside.txt"),
            "outside-secret-needle\n",
        )
        .unwrap();
        symlink(outside.path(), root.path().join("linked-outside")).unwrap();
        let context = test_context(root.path());
        let output = search(
            root.path().to_owned(),
            root.path().to_owned(),
            files_input("outside-secret-needle"),
            &context,
        )
        .unwrap();
        assert_eq!(output, "No matches found");
    }
}

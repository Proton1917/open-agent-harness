//! Bounded, provider-neutral workspace file and text search for the terminal UI.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use ignore::{DirEntry, WalkBuilder};
use regex::RegexBuilder;

use crate::tools::ToolContext;

const MAX_QUERY_BYTES: usize = 256;
const MAX_QUICK_FILES: usize = 20_000;
const MAX_QUICK_PATH_BYTES: usize = 1024 * 1024;
const MAX_RESULTS: usize = 500;
const MAX_SEARCH_FILES: usize = 50_000;
const MAX_FILE_BYTES: u64 = 1024 * 1024;
const MAX_SCANNED_BYTES: u64 = 32 * 1024 * 1024;
const MAX_RETAINED_BYTES: usize = 512 * 1024;
const MAX_MATCH_TEXT_CHARS: usize = 320;
const SEARCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PREVIEW_BYTES: u64 = 256 * 1024;
const MAX_PREVIEW_LINES: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceSearchKind {
    QuickOpen,
    GlobalSearch,
}

impl WorkspaceSearchKind {
    pub fn title(self) -> &'static str {
        match self {
            Self::QuickOpen => "Quick Open",
            Self::GlobalSearch => "Global Search",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSearchItem {
    pub path: String,
    pub line: Option<usize>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePreview {
    pub title: String,
    pub lines: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceSearchUpdate {
    Ready {
        generation: u64,
        items: Vec<WorkspaceSearchItem>,
        truncated: bool,
    },
    Failed {
        generation: u64,
        message: String,
    },
}

impl WorkspaceSearchUpdate {
    pub fn generation(&self) -> u64 {
        match self {
            Self::Ready { generation, .. } | Self::Failed { generation, .. } => *generation,
        }
    }
}

pub struct WorkspaceSearchProvider {
    context: ToolContext,
    quick_paths: Vec<String>,
    generation: u64,
    immediate: Option<WorkspaceSearchUpdate>,
    worker: Option<GlobalSearchWorker>,
}

impl WorkspaceSearchProvider {
    pub fn new(context: ToolContext, quick_paths: impl IntoIterator<Item = String>) -> Self {
        let mut seen = BTreeSet::new();
        let mut retained = 0usize;
        let mut paths = Vec::new();
        for path in quick_paths {
            if paths.len() >= MAX_QUICK_FILES
                || !safe_relative_display_path(&path)
                || !seen.insert(path.clone())
            {
                continue;
            }
            let Some(next) = retained.checked_add(path.len()) else {
                break;
            };
            if next > MAX_QUICK_PATH_BYTES {
                break;
            }
            retained = next;
            paths.push(path);
        }
        paths.sort();
        Self {
            context,
            quick_paths: paths,
            generation: 0,
            immediate: None,
            worker: None,
        }
    }

    pub fn quick_open(&self, query: &str) -> Vec<WorkspaceSearchItem> {
        let query = query.to_lowercase();
        let mut matches = self
            .quick_paths
            .iter()
            .filter_map(|path| {
                quick_score(path, &query).map(|score| {
                    (
                        score,
                        WorkspaceSearchItem {
                            path: path.clone(),
                            line: None,
                            text: String::new(),
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        matches.sort_by(|(left_score, left), (right_score, right)| {
            left_score
                .cmp(right_score)
                .then_with(|| left.path.cmp(&right.path))
        });
        matches
            .into_iter()
            .take(MAX_RESULTS)
            .map(|(_, item)| item)
            .collect()
    }

    pub fn request_global(&mut self, query: &str) -> Result<u64> {
        if query.len() > MAX_QUERY_BYTES {
            bail!("workspace search query exceeds {MAX_QUERY_BYTES} bytes")
        }
        self.generation = self.generation.wrapping_add(1).max(1);
        let generation = self.generation;
        if query.is_empty() {
            if let Some(worker) = &self.worker {
                worker.cancel(generation);
            }
            self.immediate = Some(WorkspaceSearchUpdate::Ready {
                generation,
                items: Vec::new(),
                truncated: false,
            });
            return Ok(generation);
        }
        let worker = self
            .worker
            .get_or_insert_with(|| GlobalSearchWorker::start(self.context.clone()));
        worker.search(generation, query.to_owned())?;
        self.immediate = None;
        Ok(generation)
    }

    pub fn poll_global(&mut self) -> Option<WorkspaceSearchUpdate> {
        if let Some(update) = self.immediate.take() {
            return Some(update);
        }
        let mut newest = None;
        if let Some(worker) = &self.worker {
            while let Some(update) = worker.poll() {
                if update.generation() == self.generation {
                    newest = Some(update);
                }
            }
        }
        newest
    }

    pub fn resolve_item(&self, item: &WorkspaceSearchItem) -> Result<PathBuf> {
        if !safe_relative_display_path(&item.path) {
            bail!("workspace search result path is invalid")
        }
        let cwd = fs::canonicalize(self.context.cwd()).context("workspace is unavailable")?;
        let candidate = cwd.join(&item.path);
        let direct = fs::symlink_metadata(&candidate).context("selected file is unavailable")?;
        if direct.file_type().is_symlink() || !direct.is_file() {
            bail!("selected path is not a direct regular file")
        }
        validate_not_reparse_point(&direct)?;
        let resolved = fs::canonicalize(&candidate).context("selected file cannot be resolved")?;
        if !resolved.starts_with(&cwd) || self.context.read_path_denied(&resolved) {
            bail!("selected file is outside the readable workspace")
        }
        Ok(resolved)
    }

    pub fn preview(&self, item: &WorkspaceSearchItem) -> Result<WorkspacePreview> {
        let path = self.resolve_item(item)?;
        let (bytes, truncated) = read_direct_regular(&path, MAX_PREVIEW_BYTES)
            .context("selected file preview is unavailable")?;
        if bytes.contains(&0) {
            return Ok(WorkspacePreview {
                title: item_title(item),
                lines: vec!["Binary file preview unavailable".to_owned()],
                truncated,
            });
        }
        let text = std::str::from_utf8(&bytes).context("file preview is not UTF-8 text")?;
        let all = text.lines().collect::<Vec<_>>();
        let (start, end) = if let Some(line) = item.line {
            let index = line.saturating_sub(1).min(all.len().saturating_sub(1));
            (
                index.saturating_sub(4),
                (index.saturating_add(5)).min(all.len()),
            )
        } else {
            (0, all.len().min(MAX_PREVIEW_LINES))
        };
        let lines = all[start..end]
            .iter()
            .enumerate()
            .map(|(offset, line)| {
                let number = start + offset + 1;
                let marker = if item.line == Some(number) { '>' } else { ' ' };
                format!("{marker} {number:>5}  {}", sanitize_text(line))
            })
            .collect::<Vec<_>>();
        Ok(WorkspacePreview {
            title: item_title(item),
            lines: if lines.is_empty() {
                vec!["Empty text file".to_owned()]
            } else {
                lines
            },
            truncated,
        })
    }
}

fn item_title(item: &WorkspaceSearchItem) -> String {
    item.line
        .map(|line| format!("{}:{line}", item.path))
        .unwrap_or_else(|| item.path.clone())
}

fn quick_score(path: &str, query: &str) -> Option<(u8, usize, usize, usize)> {
    let lowered = path.to_lowercase();
    let basename = lowered.rsplit('/').next().unwrap_or(&lowered);
    if query.is_empty() {
        return Some((0, 0, path.matches('/').count(), path.len()));
    }
    if lowered == query {
        return Some((0, 0, 0, path.len()));
    }
    if basename == query {
        return Some((1, 0, path.matches('/').count(), path.len()));
    }
    if basename.starts_with(query) {
        return Some((2, basename.len() - query.len(), 0, path.len()));
    }
    if lowered.starts_with(query) {
        return Some((3, lowered.len() - query.len(), 0, path.len()));
    }
    if let Some(position) = basename.find(query) {
        return Some((4, position, basename.len() - query.len(), path.len()));
    }
    if let Some(position) = lowered.find(query) {
        return Some((5, position, lowered.len() - query.len(), path.len()));
    }
    fuzzy_gap(&lowered, query).map(|gap| (6, gap, lowered.len(), path.len()))
}

fn fuzzy_gap(candidate: &str, query: &str) -> Option<usize> {
    if query.is_empty() {
        return Some(0);
    }
    let mut query = query.chars();
    let mut wanted = query.next()?;
    let mut first = None;
    for (index, character) in candidate.chars().enumerate() {
        if character != wanted {
            continue;
        }
        first.get_or_insert(index);
        let Some(next) = query.next() else {
            return Some(index.saturating_sub(first.unwrap_or(index)));
        };
        wanted = next;
    }
    None
}

struct GlobalSearchWorker {
    request: Arc<(Mutex<WorkerState>, Condvar)>,
    update: Arc<Mutex<Option<WorkspaceSearchUpdate>>>,
    active_generation: Arc<AtomicU64>,
    join: Option<thread::JoinHandle<()>>,
}

impl GlobalSearchWorker {
    fn start(context: ToolContext) -> Self {
        let request = Arc::new((Mutex::new(WorkerState::default()), Condvar::new()));
        let worker_request = Arc::clone(&request);
        let update = Arc::new(Mutex::new(None));
        let worker_update = Arc::clone(&update);
        let active_generation = Arc::new(AtomicU64::new(0));
        let worker_generation = Arc::clone(&active_generation);
        let join = thread::Builder::new()
            .name("harness-workspace-search".to_owned())
            .spawn(move || {
                worker_loop(context, worker_request, worker_update, worker_generation);
            })
            .ok();
        Self {
            request,
            update,
            active_generation,
            join,
        }
    }

    fn search(&self, generation: u64, query: String) -> Result<()> {
        if self.join.is_none() {
            bail!("workspace search worker could not start")
        }
        self.active_generation.store(generation, Ordering::Release);
        let (state, ready) = &*self.request;
        let mut state = state
            .lock()
            .map_err(|_| anyhow::anyhow!("workspace search worker state is unavailable"))?;
        if state.stop {
            bail!("workspace search worker is unavailable")
        }
        state.latest = Some(SearchRequest { generation, query });
        ready.notify_one();
        Ok(())
    }

    fn cancel(&self, generation: u64) {
        self.active_generation.store(generation, Ordering::Release);
        let (state, _) = &*self.request;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.latest = None;
    }

    fn poll(&self) -> Option<WorkspaceSearchUpdate> {
        self.update
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

impl Drop for GlobalSearchWorker {
    fn drop(&mut self) {
        self.active_generation.fetch_add(1, Ordering::AcqRel);
        let (state, ready) = &*self.request;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.stop = true;
        state.latest = None;
        ready.notify_one();
        drop(state);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

#[derive(Default)]
struct WorkerState {
    latest: Option<SearchRequest>,
    stop: bool,
}

struct SearchRequest {
    generation: u64,
    query: String,
}

fn worker_loop(
    context: ToolContext,
    request: Arc<(Mutex<WorkerState>, Condvar)>,
    update: Arc<Mutex<Option<WorkspaceSearchUpdate>>>,
    active_generation: Arc<AtomicU64>,
) {
    loop {
        let next = {
            let (state, ready) = &*request;
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while state.latest.is_none() && !state.stop {
                state = ready
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            if state.stop {
                None
            } else {
                state.latest.take()
            }
        };
        let Some(SearchRequest { generation, query }) = next else {
            break;
        };
        let result = search_workspace(&context, generation, &query, &active_generation);
        if active_generation.load(Ordering::Acquire) == generation {
            *update
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(result);
        }
    }
}

fn search_workspace(
    context: &ToolContext,
    generation: u64,
    query: &str,
    active_generation: &AtomicU64,
) -> WorkspaceSearchUpdate {
    let regex = match RegexBuilder::new(&regex::escape(query))
        .case_insensitive(true)
        .unicode(true)
        .build()
    {
        Ok(regex) => regex,
        Err(_) => {
            return WorkspaceSearchUpdate::Failed {
                generation,
                message: "Search query could not be compiled".to_owned(),
            };
        }
    };
    let root = match fs::canonicalize(context.cwd()) {
        Ok(root) if root.is_dir() => root,
        _ => {
            return WorkspaceSearchUpdate::Failed {
                generation,
                message: "Workspace is unavailable".to_owned(),
            };
        }
    };
    let safety_context = context.clone();
    let mut builder = WalkBuilder::new(&root);
    builder
        .follow_links(false)
        .max_depth(Some(64))
        .hidden(false)
        .ignore(true)
        .parents(true)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(true)
        .filter_entry(move |entry| {
            searchable_entry(entry) && !safety_context.read_path_denied(entry.path())
        });
    let started = Instant::now();
    let mut files = 0usize;
    let mut scanned = 0u64;
    let mut retained = 0usize;
    let mut truncated = false;
    let mut items = Vec::new();
    'entries: for entry in builder.build().filter_map(Result::ok).skip(1) {
        if active_generation.load(Ordering::Acquire) != generation {
            return WorkspaceSearchUpdate::Ready {
                generation,
                items: Vec::new(),
                truncated: true,
            };
        }
        if started.elapsed() > SEARCH_TIMEOUT || files >= MAX_SEARCH_FILES {
            truncated = true;
            break;
        }
        let Some(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_symlink() || !kind.is_file() {
            continue;
        }
        files += 1;
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.len() > MAX_FILE_BYTES {
            continue;
        }
        if scanned.saturating_add(metadata.len()) > MAX_SCANNED_BYTES {
            truncated = true;
            break;
        }
        let Ok((bytes, changed_limit)) = read_direct_regular(entry.path(), MAX_FILE_BYTES) else {
            continue;
        };
        if changed_limit || scanned.saturating_add(bytes.len() as u64) > MAX_SCANNED_BYTES {
            truncated = true;
            break;
        }
        scanned = scanned.saturating_add(bytes.len() as u64);
        if bytes.contains(&0) {
            continue;
        }
        let Ok(text) = std::str::from_utf8(&bytes) else {
            continue;
        };
        let Ok(relative) = entry.path().strip_prefix(&root) else {
            continue;
        };
        let path = relative.to_string_lossy().replace('\\', "/");
        if !safe_relative_display_path(&path) {
            continue;
        }
        for (line_index, line) in text.lines().enumerate() {
            if line_index % 4096 == 0
                && (active_generation.load(Ordering::Acquire) != generation
                    || started.elapsed() > SEARCH_TIMEOUT)
            {
                truncated = true;
                break 'entries;
            }
            if !regex.is_match(line) {
                continue;
            }
            let excerpt = truncate_chars(sanitize_text(line.trim_start()), MAX_MATCH_TEXT_CHARS);
            let Some(next) = retained
                .checked_add(path.len())
                .and_then(|value| value.checked_add(excerpt.len()))
            else {
                truncated = true;
                break 'entries;
            };
            if next > MAX_RETAINED_BYTES || items.len() >= MAX_RESULTS {
                truncated = true;
                break 'entries;
            }
            retained = next;
            items.push(WorkspaceSearchItem {
                path: path.clone(),
                line: Some(line_index + 1),
                text: excerpt,
            });
        }
    }
    items.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.line.cmp(&right.line))
    });
    WorkspaceSearchUpdate::Ready {
        generation,
        items,
        truncated,
    }
}

fn searchable_entry(entry: &DirEntry) -> bool {
    if entry.depth() == 0 || !entry.file_type().is_some_and(|kind| kind.is_dir()) {
        return true;
    }
    !matches!(
        entry.file_name().to_str(),
        Some(
            ".git"
                | ".hg"
                | ".svn"
                | ".venv"
                | "node_modules"
                | "target"
                | "dist"
                | "build"
                | "__pycache__"
        )
    )
}

fn safe_relative_display_path(value: &str) -> bool {
    if value.is_empty()
        || value.len() > 4096
        || value.starts_with('~')
        || value.starts_with("//")
        || value.starts_with("\\\\")
        || value.chars().any(char::is_control)
    {
        return false;
    }
    let path = Path::new(value);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

fn read_direct_regular(path: &Path, limit: u64) -> Result<(Vec<u8>, bool)> {
    let before = fs::symlink_metadata(path).context("cannot inspect file")?;
    if before.file_type().is_symlink() || !before.is_file() {
        bail!("path is not a direct regular file")
    }
    validate_not_reparse_point(&before)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let mut file = options.open(path).context("cannot open file")?;
    validate_open_identity(path, &before, &file)?;
    let mut bytes = Vec::with_capacity(usize::try_from(before.len().min(limit)).unwrap_or(0));
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .context("cannot read file")?;
    let truncated = bytes.len() as u64 > limit;
    if truncated {
        bytes.truncate(usize::try_from(limit).context("file limit does not fit usize")?);
        if let Err(error) = std::str::from_utf8(&bytes) {
            if error.error_len().is_none() {
                bytes.truncate(error.valid_up_to());
            }
        }
    }
    Ok((bytes, truncated))
}

fn validate_open_identity(path: &Path, before: &fs::Metadata, file: &File) -> Result<()> {
    #[cfg(not(unix))]
    let _ = before;
    let after = fs::symlink_metadata(path).context("cannot re-check file")?;
    let opened = file.metadata().context("cannot inspect open file")?;
    if after.file_type().is_symlink() || !opened.is_file() {
        bail!("file changed while opening")
    }
    validate_not_reparse_point(&after)?;
    validate_not_reparse_point(&opened)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.dev() != opened.dev()
            || before.ino() != opened.ino()
            || after.dev() != opened.dev()
            || after.ino() != opened.ino()
        {
            bail!("file changed while opening")
        }
    }
    Ok(())
}

#[cfg(windows)]
fn validate_not_reparse_point(metadata: &fs::Metadata) -> Result<()> {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        bail!("file path is a reparse point")
    }
    Ok(())
}

#[cfg(not(windows))]
fn validate_not_reparse_point(_: &fs::Metadata) -> Result<()> {
    Ok(())
}

fn sanitize_text(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\t' => ' ',
            _ if character.is_control() => '�',
            _ => character,
        })
        .collect()
}

fn truncate_chars(value: String, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value;
    }
    let mut output = value
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>();
    output.push('…');
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{PermissionManager, PermissionMode};

    fn context(root: &Path) -> ToolContext {
        ToolContext::new(
            root.to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        )
    }

    #[test]
    fn quick_open_ranks_basename_prefix_before_path_subsequence() {
        let directory = tempfile::tempdir().unwrap();
        let provider = WorkspaceSearchProvider::new(
            context(directory.path()),
            [
                "src/search_panel.rs".to_owned(),
                "docs/panel-search.md".to_owned(),
                "src/sparse.rs".to_owned(),
            ],
        );
        let matches = provider.quick_open("search");
        assert_eq!(matches[0].path, "src/search_panel.rs");
        assert_eq!(matches[1].path, "docs/panel-search.md");
        assert!(
            provider
                .quick_open("sspr")
                .iter()
                .any(|item| item.path == "src/sparse.rs")
        );
    }

    #[test]
    fn global_search_returns_line_matches_and_bounded_preview() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join("src")).unwrap();
        fs::write(
            directory.path().join("src/example.rs"),
            "first\nUnique Search Needle\nthird\n",
        )
        .unwrap();
        fs::write(directory.path().join("binary.bin"), b"needle\0ignored").unwrap();
        let mut provider =
            WorkspaceSearchProvider::new(context(directory.path()), ["src/example.rs".to_owned()]);
        let generation = provider.request_global("search needle").unwrap();
        let started = Instant::now();
        let update = loop {
            if let Some(update) = provider.poll_global() {
                break update;
            }
            assert!(started.elapsed() < Duration::from_secs(2));
            thread::sleep(Duration::from_millis(10));
        };
        let WorkspaceSearchUpdate::Ready {
            generation: actual,
            items,
            truncated,
        } = update
        else {
            panic!("search failed")
        };
        assert_eq!(actual, generation);
        assert!(!truncated);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].path, "src/example.rs");
        assert_eq!(items[0].line, Some(2));
        let preview = provider.preview(&items[0]).unwrap();
        assert!(
            preview
                .lines
                .iter()
                .any(|line| line.contains("Unique Search Needle"))
        );
        assert!(preview.lines.iter().any(|line| line.starts_with(">")));
    }

    #[cfg(unix)]
    #[test]
    fn preview_rejects_symlink_results() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        symlink(outside.path(), directory.path().join("escape.txt")).unwrap();
        let provider =
            WorkspaceSearchProvider::new(context(directory.path()), ["escape.txt".to_owned()]);
        let item = WorkspaceSearchItem {
            path: "escape.txt".to_owned(),
            line: None,
            text: String::new(),
        };
        assert!(provider.preview(&item).is_err());
    }

    #[test]
    fn newer_generation_suppresses_stale_worker_results() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("values.txt"),
            "old value\nnew value\n",
        )
        .unwrap();
        let mut provider = WorkspaceSearchProvider::new(context(directory.path()), Vec::new());
        let old = provider.request_global("old").unwrap();
        let new = provider.request_global("new").unwrap();
        assert!(new > old);
        let started = Instant::now();
        loop {
            if let Some(update) = provider.poll_global() {
                assert_eq!(update.generation(), new);
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(2));
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn bounded_reader_trims_only_an_incomplete_utf8_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let partial = directory.path().join("partial.txt");
        fs::write(&partial, "ok😀").unwrap();
        let (bytes, truncated) = read_direct_regular(&partial, 4).unwrap();
        assert!(truncated);
        assert_eq!(bytes, b"ok");

        let invalid = directory.path().join("invalid.txt");
        fs::write(&invalid, b"ok\xfftail").unwrap();
        let (bytes, truncated) = read_direct_regular(&invalid, 4).unwrap();
        assert!(truncated);
        assert!(std::str::from_utf8(&bytes).is_err());
    }
}

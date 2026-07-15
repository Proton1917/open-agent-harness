//! Provider-neutral, bounded, persistent input history.
//!
//! The store deliberately persists only an opaque workspace key, a session ID,
//! user-entered text, and ordering metadata. Callers must not use an absolute
//! workspace path as the workspace key.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const FORMAT_VERSION: u8 = 1;
const HISTORY_FILE_NAME: &str = "input-history.v1.jsonl";
const LOCK_FILE_NAME: &str = ".input-history.lock";
const LOCK_INITIAL_BACKOFF: Duration = Duration::from_millis(2);
const LOCK_MAX_BACKOFF: Duration = Duration::from_millis(50);

const HARD_MAX_RECORDS: usize = 100_000;
const HARD_MAX_FILE_BYTES: usize = 32 * 1024 * 1024;
const HARD_MAX_ENTRY_BYTES: usize = 256 * 1024;
const HARD_MAX_QUERY_BYTES: usize = 16 * 1024;
const HARD_MAX_RESULTS: usize = 1_000;
const HARD_MAX_SCAN_RECORDS: usize = 10_000;
const HARD_MAX_FUZZY_QUERY_CHARS: usize = 512;
const HARD_MAX_FUZZY_CANDIDATE_CHARS: usize = 8_192;
const HARD_MAX_WORKSPACE_KEY_BYTES: usize = 512;
const HARD_MAX_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Resource limits applied to every read, write, and query.
#[derive(Clone, Debug)]
pub struct InputHistoryLimits {
    pub max_records: usize,
    pub max_file_bytes: usize,
    /// Maximum serialized JSONL bytes for one record, including its newline.
    pub max_entry_bytes: usize,
    pub max_query_bytes: usize,
    pub max_results: usize,
    pub max_scan_records: usize,
    pub max_fuzzy_query_chars: usize,
    pub max_fuzzy_candidate_chars: usize,
    pub lock_timeout: Duration,
}

impl Default for InputHistoryLimits {
    fn default() -> Self {
        Self {
            max_records: 4_096,
            max_file_bytes: 8 * 1024 * 1024,
            max_entry_bytes: 64 * 1024,
            max_query_bytes: 4 * 1024,
            max_results: 100,
            max_scan_records: 4_096,
            max_fuzzy_query_chars: 256,
            max_fuzzy_candidate_chars: 4_096,
            lock_timeout: Duration::from_secs(2),
        }
    }
}

impl InputHistoryLimits {
    fn validate(&self) -> Result<()> {
        validate_nonzero_limit("max_records", self.max_records, HARD_MAX_RECORDS)?;
        validate_nonzero_limit("max_file_bytes", self.max_file_bytes, HARD_MAX_FILE_BYTES)?;
        validate_nonzero_limit(
            "max_entry_bytes",
            self.max_entry_bytes,
            HARD_MAX_ENTRY_BYTES,
        )?;
        validate_nonzero_limit(
            "max_query_bytes",
            self.max_query_bytes,
            HARD_MAX_QUERY_BYTES,
        )?;
        validate_nonzero_limit("max_results", self.max_results, HARD_MAX_RESULTS)?;
        validate_nonzero_limit(
            "max_scan_records",
            self.max_scan_records,
            HARD_MAX_SCAN_RECORDS,
        )?;
        validate_nonzero_limit(
            "max_fuzzy_query_chars",
            self.max_fuzzy_query_chars,
            HARD_MAX_FUZZY_QUERY_CHARS,
        )?;
        validate_nonzero_limit(
            "max_fuzzy_candidate_chars",
            self.max_fuzzy_candidate_chars,
            HARD_MAX_FUZZY_CANDIDATE_CHARS,
        )?;
        if self.max_entry_bytes > self.max_file_bytes {
            bail!("input history max_entry_bytes exceeds max_file_bytes");
        }
        if self.lock_timeout.is_zero() || self.lock_timeout > HARD_MAX_LOCK_TIMEOUT {
            bail!("input history lock_timeout is outside the supported range");
        }
        Ok(())
    }
}

fn validate_nonzero_limit(name: &str, value: usize, maximum: usize) -> Result<()> {
    if value == 0 || value > maximum {
        bail!("input history {name} is outside the supported range");
    }
    Ok(())
}

/// The persistence partition searched by a history query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryScope {
    Session,
    Project,
    Everywhere,
}

impl HistoryScope {
    pub const ALL: [Self; 3] = [Self::Session, Self::Project, Self::Everywhere];

    #[must_use]
    pub const fn next(self) -> Self {
        match self {
            Self::Session => Self::Project,
            Self::Project => Self::Everywhere,
            Self::Everywhere => Self::Session,
        }
    }
}

/// The current workspace/session partition. `workspace_key` must be opaque.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryContext {
    pub workspace_key: String,
    pub session_id: Uuid,
}

impl HistoryContext {
    pub fn new(workspace_key: impl Into<String>, session_id: Uuid) -> Result<Self> {
        let context = Self {
            workspace_key: workspace_key.into(),
            session_id,
        };
        validate_context(&context)?;
        Ok(context)
    }
}

/// One durable history record. Unknown fields and unsupported versions are
/// rejected when reading the file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputHistoryRecord {
    pub version: u8,
    pub workspace_key: String,
    pub session_id: Uuid,
    pub text: String,
    pub timestamp_ms: u64,
    pub order: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendOutcome {
    pub record: InputHistoryRecord,
    /// False when the immediately preceding record was identical in the same
    /// workspace and session.
    pub inserted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryQuery {
    pub scope: HistoryScope,
    pub needle: String,
    pub limit: usize,
}

impl HistoryQuery {
    #[must_use]
    pub fn new(scope: HistoryScope, needle: impl Into<String>, limit: usize) -> Self {
        Self {
            scope,
            needle: needle.into(),
            limit,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryMatchKind {
    All,
    Substring,
    Fuzzy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryMatch {
    pub record: InputHistoryRecord,
    pub kind: HistoryMatchKind,
    /// Lower fuzzy scores are better. Exact substring/all matches use zero.
    pub score: u64,
}

/// A small state object suitable for Up/Down history cycling.
#[derive(Clone, Debug)]
pub struct HistoryCursor {
    matches: Vec<HistoryMatch>,
    position: Option<usize>,
}

impl HistoryCursor {
    #[must_use]
    pub fn new(matches: Vec<HistoryMatch>) -> Self {
        Self {
            matches,
            position: None,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.matches.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.matches.is_empty()
    }

    /// Move toward older entries. The first call selects the newest match.
    pub fn older(&mut self) -> Option<&HistoryMatch> {
        let next = match self.position {
            None if !self.matches.is_empty() => 0,
            Some(position) if position + 1 < self.matches.len() => position + 1,
            Some(position) => position,
            None => return None,
        };
        self.position = Some(next);
        self.current()
    }

    /// Move toward newer entries. Moving newer than the newest entry restores
    /// the caller's draft and therefore returns None.
    pub fn newer(&mut self) -> Option<&HistoryMatch> {
        match self.position {
            Some(0) => {
                self.position = None;
                None
            }
            Some(position) => {
                self.position = Some(position - 1);
                self.current()
            }
            None => None,
        }
    }

    #[must_use]
    pub fn current(&self) -> Option<&HistoryMatch> {
        self.position
            .and_then(|position| self.matches.get(position))
    }

    #[must_use]
    pub fn into_matches(self) -> Vec<HistoryMatch> {
        self.matches
    }
}

/// A private JSONL history store protected by a cross-process file lock.
#[derive(Clone, Debug)]
pub struct InputHistoryStore {
    root: PathBuf,
    history_path: PathBuf,
    lock_path: PathBuf,
    limits: InputHistoryLimits,
}

impl InputHistoryStore {
    /// Open the default per-user application data root. No home directory is
    /// read until this method is explicitly called.
    pub fn open_default() -> Result<Self> {
        let data_root =
            dirs::data_local_dir().context("user-local data directory is unavailable")?;
        Self::open(data_root.join("open-agent-harness-input-history"))
    }

    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::with_limits(root, InputHistoryLimits::default())
    }

    pub fn with_limits(root: impl Into<PathBuf>, limits: InputHistoryLimits) -> Result<Self> {
        limits.validate()?;
        let root = root.into();
        if root.as_os_str().is_empty() {
            bail!("input history root is empty");
        }
        ensure_private_directory(&root)?;
        Ok(Self {
            history_path: root.join(HISTORY_FILE_NAME),
            lock_path: root.join(LOCK_FILE_NAME),
            root,
            limits,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn history_path(&self) -> &Path {
        &self.history_path
    }

    #[must_use]
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    pub fn append(
        &self,
        context: &HistoryContext,
        text: impl Into<String>,
    ) -> Result<AppendOutcome> {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time is before the Unix epoch")?
            .as_millis()
            .try_into()
            .context("system timestamp does not fit in u64")?;
        self.append_at(context, text, timestamp_ms)
    }

    /// Deterministic append entry point for importers and tests.
    pub fn append_at(
        &self,
        context: &HistoryContext,
        text: impl Into<String>,
        timestamp_ms: u64,
    ) -> Result<AppendOutcome> {
        validate_context(context)?;
        let text = text.into();
        validate_text(&text)?;
        ensure_private_directory(&self.root)?;
        let _lock = self.acquire_lock()?;
        let mut records = self.read_records_locked()?;

        if let Some(last) = records.last() {
            if last.workspace_key == context.workspace_key
                && last.session_id == context.session_id
                && last.text == text
            {
                return Ok(AppendOutcome {
                    record: last.clone(),
                    inserted: false,
                });
            }
        }

        let order = records
            .last()
            .map_or(Some(1), |record| record.order.checked_add(1))
            .context("input history order exhausted")?;
        let record = InputHistoryRecord {
            version: FORMAT_VERSION,
            workspace_key: context.workspace_key.clone(),
            session_id: context.session_id,
            text,
            timestamp_ms,
            order,
        };
        validate_record(&record)?;
        let encoded = encode_record(&record)?;
        if encoded.len() > self.limits.max_entry_bytes {
            bail!("input history entry exceeds configured byte limit");
        }
        if encoded.len() > self.limits.max_file_bytes {
            bail!("input history entry cannot fit in the history file");
        }

        records.push(record.clone());
        let bytes = self.encode_pruned_records(&mut records)?;
        self.write_records_locked(&bytes)?;
        Ok(AppendOutcome {
            record,
            inserted: true,
        })
    }

    pub fn search(
        &self,
        context: &HistoryContext,
        query: &HistoryQuery,
    ) -> Result<Vec<HistoryMatch>> {
        validate_context(context)?;
        if query.needle.len() > self.limits.max_query_bytes {
            bail!("input history query exceeds configured byte limit");
        }
        if query.limit == 0 || query.limit > self.limits.max_results {
            bail!("input history result limit is outside the configured range");
        }
        ensure_private_directory(&self.root)?;
        let _lock = self.acquire_lock()?;
        let records = self.read_records_locked()?;
        drop(_lock);

        let needle = query.needle.to_lowercase();
        if needle.chars().count() > self.limits.max_fuzzy_query_chars {
            bail!("input history fuzzy query exceeds configured character limit");
        }

        let mut exact = Vec::new();
        let mut fuzzy = Vec::new();
        let mut seen = HashSet::new();
        for record in records
            .into_iter()
            .rev()
            .take(self.limits.max_scan_records)
            .filter(|record| record_in_scope(record, context, query.scope))
        {
            if !seen.insert(record.text.clone()) {
                continue;
            }
            if needle.is_empty() {
                exact.push(HistoryMatch {
                    record,
                    kind: HistoryMatchKind::All,
                    score: 0,
                });
                continue;
            }
            let candidate = record.text.to_lowercase();
            if candidate.contains(&needle) {
                exact.push(HistoryMatch {
                    record,
                    kind: HistoryMatchKind::Substring,
                    score: 0,
                });
                continue;
            }
            if let Some(score) =
                fuzzy_subsequence_score(&needle, &candidate, self.limits.max_fuzzy_candidate_chars)
            {
                fuzzy.push(HistoryMatch {
                    record,
                    kind: HistoryMatchKind::Fuzzy,
                    score,
                });
            }
        }

        exact.sort_by_key(|entry| std::cmp::Reverse(entry.record.order));
        fuzzy.sort_by(|left, right| {
            left.score
                .cmp(&right.score)
                .then_with(|| right.record.order.cmp(&left.record.order))
        });
        exact.extend(fuzzy);
        exact.truncate(query.limit);
        Ok(exact)
    }

    pub fn cursor(&self, context: &HistoryContext, query: &HistoryQuery) -> Result<HistoryCursor> {
        Ok(HistoryCursor::new(self.search(context, query)?))
    }

    fn acquire_lock(&self) -> Result<HistoryFileLock> {
        let file = open_lock_file(&self.lock_path)?;
        validate_open_private_file(&self.lock_path, &file, "input history lock")?;
        try_lock_exclusive_with_timeout(&file, self.limits.lock_timeout)?;
        if let Err(error) = validate_open_private_file(&self.lock_path, &file, "input history lock")
        {
            let _ = fs2::FileExt::unlock(&file);
            return Err(error);
        }
        Ok(HistoryFileLock { file })
    }

    fn read_records_locked(&self) -> Result<Vec<InputHistoryRecord>> {
        let metadata = match fs::symlink_metadata(&self.history_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error).context("cannot inspect input history file"),
        };
        validate_private_regular_metadata(&metadata, "input history file")?;
        if metadata.len() > self.limits.max_file_bytes as u64 {
            bail!("input history file exceeds configured byte limit");
        }

        let mut file = open_nofollow_read(&self.history_path)?;
        validate_open_private_file(&self.history_path, &file, "input history file")?;
        let read_limit = self
            .limits
            .max_file_bytes
            .checked_add(1)
            .context("input history read limit overflow")?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        Read::by_ref(&mut file)
            .take(read_limit as u64)
            .read_to_end(&mut bytes)
            .context("cannot read input history file")?;
        if bytes.len() > self.limits.max_file_bytes {
            bail!("input history file changed beyond configured byte limit");
        }
        parse_records(&bytes, &self.limits)
    }

    fn encode_pruned_records(&self, records: &mut Vec<InputHistoryRecord>) -> Result<Vec<u8>> {
        let encoded = records
            .iter()
            .map(encode_record)
            .collect::<Result<Vec<_>>>()?;
        if encoded
            .iter()
            .any(|line| line.len() > self.limits.max_entry_bytes)
        {
            bail!("input history contains an entry above the configured byte limit");
        }

        let mut drop_count = records.len().saturating_sub(self.limits.max_records);
        let mut byte_count = encoded[drop_count..]
            .iter()
            .try_fold(0_usize, |total, line| total.checked_add(line.len()))
            .context("input history byte count overflow")?;
        while byte_count > self.limits.max_file_bytes && drop_count < encoded.len() {
            byte_count = byte_count.saturating_sub(encoded[drop_count].len());
            drop_count += 1;
        }
        if drop_count == encoded.len() {
            bail!("input history could not retain the appended record");
        }

        let mut output = Vec::with_capacity(byte_count);
        for line in &encoded[drop_count..] {
            output.extend_from_slice(line);
        }
        if drop_count > 0 {
            records.drain(..drop_count);
        }
        Ok(output)
    }

    fn write_records_locked(&self, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() || bytes.len() > self.limits.max_file_bytes {
            bail!("refusing invalid input history rewrite size");
        }
        match fs::symlink_metadata(&self.history_path) {
            Ok(metadata) => validate_private_regular_metadata(&metadata, "input history file")?,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("cannot inspect input history target"),
        }

        let temporary = self.root.join(format!(
            ".{HISTORY_FILE_NAME}.tmp-{}",
            Uuid::new_v4().simple()
        ));
        let result = (|| -> Result<()> {
            let mut file = open_private_create_new(&temporary)?;
            validate_open_private_file(&temporary, &file, "input history temporary file")?;
            file.write_all(bytes)
                .context("cannot write input history temporary file")?;
            file.sync_all()
                .context("cannot sync input history temporary file")?;
            replace_file_atomic(&temporary, &self.history_path)?;
            let file = open_nofollow_read(&self.history_path)?;
            validate_open_private_file(&self.history_path, &file, "input history file")?;
            sync_directory(&self.root);
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
}

struct HistoryFileLock {
    file: File,
}

impl Drop for HistoryFileLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

fn validate_context(context: &HistoryContext) -> Result<()> {
    let key = &context.workspace_key;
    if key.is_empty()
        || key.len() > HARD_MAX_WORKSPACE_KEY_BYTES
        || key.chars().any(char::is_control)
    {
        bail!("input history workspace key is invalid");
    }
    if context.session_id.is_nil() {
        bail!("input history session ID must not be nil");
    }
    Ok(())
}

fn validate_text(text: &str) -> Result<()> {
    if text.trim().is_empty() {
        bail!("input history text is empty");
    }
    if text.len() > HARD_MAX_ENTRY_BYTES {
        bail!("input history text exceeds the hard byte limit");
    }
    Ok(())
}

fn validate_record(record: &InputHistoryRecord) -> Result<()> {
    if record.version != FORMAT_VERSION {
        bail!("unsupported input history record version");
    }
    validate_context(&HistoryContext {
        workspace_key: record.workspace_key.clone(),
        session_id: record.session_id,
    })?;
    validate_text(&record.text)?;
    if record.order == 0 {
        bail!("input history order must be nonzero");
    }
    Ok(())
}

fn encode_record(record: &InputHistoryRecord) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(record).context("cannot encode input history record")?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn parse_records(bytes: &[u8], limits: &InputHistoryLimits) -> Result<Vec<InputHistoryRecord>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if !bytes.ends_with(b"\n") {
        bail!("input history JSONL is truncated");
    }

    let mut records = Vec::new();
    let mut previous_order = 0_u64;
    for line in bytes[..bytes.len() - 1].split(|byte| *byte == b'\n') {
        if line.is_empty() {
            bail!("input history JSONL contains an empty record");
        }
        let line_bytes = line
            .len()
            .checked_add(1)
            .context("input history line length overflow")?;
        if line_bytes > limits.max_entry_bytes {
            bail!("input history record exceeds configured byte limit");
        }
        if records.len() >= HARD_MAX_RECORDS {
            bail!("input history record count exceeds the hard limit");
        }
        let record: InputHistoryRecord =
            serde_json::from_slice(line).context("input history JSONL is invalid")?;
        validate_record(&record)?;
        if record.order <= previous_order {
            bail!("input history order is not strictly increasing");
        }
        previous_order = record.order;
        records.push(record);
    }
    Ok(records)
}

fn record_in_scope(
    record: &InputHistoryRecord,
    context: &HistoryContext,
    scope: HistoryScope,
) -> bool {
    match scope {
        HistoryScope::Session => {
            record.workspace_key == context.workspace_key && record.session_id == context.session_id
        }
        HistoryScope::Project => record.workspace_key == context.workspace_key,
        HistoryScope::Everywhere => true,
    }
}

fn fuzzy_subsequence_score(
    query: &str,
    candidate: &str,
    max_candidate_chars: usize,
) -> Option<u64> {
    let query_chars = query.chars().collect::<Vec<_>>();
    if query_chars.is_empty() {
        return Some(0);
    }

    let mut query_index = 0_usize;
    let mut first_match = None;
    let mut gap_penalty = 0_u64;
    let mut previous_match = None;
    for (candidate_index, candidate_char) in candidate.chars().take(max_candidate_chars).enumerate()
    {
        if candidate_char != query_chars[query_index] {
            continue;
        }
        first_match.get_or_insert(candidate_index);
        if let Some(previous) = previous_match {
            gap_penalty =
                gap_penalty.saturating_add(candidate_index.saturating_sub(previous + 1) as u64);
        }
        previous_match = Some(candidate_index);
        query_index += 1;
        if query_index == query_chars.len() {
            let start = first_match.unwrap_or(0) as u64;
            let span = candidate_index
                .saturating_sub(first_match.unwrap_or(candidate_index))
                .saturating_add(1) as u64;
            return Some(
                gap_penalty
                    .saturating_mul(16)
                    .saturating_add(start.saturating_mul(4))
                    .saturating_add(span),
            );
        }
    }
    None
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_directory_metadata(&metadata),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let parent = path.parent().context("input history root has no parent")?;
            let parent_metadata = fs::symlink_metadata(parent).with_context(|| {
                format!(
                    "input history root parent does not exist: {}",
                    parent.display()
                )
            })?;
            if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
                bail!("input history root parent is not a direct directory");
            }
            create_private_directory(path)?;
            let metadata = fs::symlink_metadata(path)
                .context("cannot inspect newly created input history root")?;
            validate_private_directory_metadata(&metadata)
        }
        Err(error) => Err(error).context("cannot inspect input history root"),
    }
}

fn validate_private_directory_metadata(metadata: &fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("input history root is not a direct directory");
    }
    validate_not_reparse_point(metadata, "input history root")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("input history root permissions are not private");
        }
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    let mut builder = fs::DirBuilder::new();
    #[cfg(not(unix))]
    let builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    match builder.create(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error).context("cannot create private input history root"),
    }
}

fn open_lock_file(path: &Path) -> Result<File> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_regular_metadata(&metadata, "input history lock")?,
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("cannot inspect input history lock"),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    configure_private_create(&mut options);
    options
        .open(path)
        .context("cannot open private input history lock")
}

fn open_nofollow_read(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    configure_nofollow(&mut options);
    options
        .open(path)
        .context("cannot open private input history file")
}

fn open_private_create_new(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    configure_private_create(&mut options);
    options
        .open(path)
        .context("cannot create private input history temporary file")
}

#[cfg(unix)]
fn configure_nofollow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_nofollow(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_nofollow(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn configure_private_create(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_private_create(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_private_create(_options: &mut OpenOptions) {}

fn validate_private_regular_metadata(metadata: &fs::Metadata, label: &str) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("{label} is not a direct regular file");
    }
    validate_not_reparse_point(metadata, label)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("{label} permissions are not private");
        }
        if metadata.nlink() != 1 {
            bail!("{label} has an unsafe hard-link count");
        }
    }
    Ok(())
}

fn validate_open_private_file(path: &Path, file: &File, label: &str) -> Result<()> {
    let open_metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect open {label}"))?;
    validate_private_regular_metadata(&open_metadata, label)?;
    let path_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect path for open {label}"))?;
    validate_private_regular_metadata(&path_metadata, label)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if open_metadata.dev() != path_metadata.dev() || open_metadata.ino() != path_metadata.ino()
        {
            bail!("{label} changed while it was being opened");
        }
    }
    Ok(())
}

#[cfg(windows)]
fn validate_not_reparse_point(metadata: &fs::Metadata, label: &str) -> Result<()> {
    use std::os::windows::fs::MetadataExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        bail!("{label} is a reparse point");
    }
    Ok(())
}

#[cfg(not(windows))]
fn validate_not_reparse_point(_metadata: &fs::Metadata, _label: &str) -> Result<()> {
    Ok(())
}

fn try_lock_exclusive_with_timeout(file: &File, timeout: Duration) -> Result<()> {
    let started = Instant::now();
    let mut backoff = LOCK_INITIAL_BACKOFF;
    loop {
        match fs2::FileExt::try_lock_exclusive(file) {
            Ok(()) => return Ok(()),
            Err(error) if lock_is_contended(&error) => {
                let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                    bail!("input history lock acquisition timed out");
                };
                if remaining.is_zero() {
                    bail!("input history lock acquisition timed out");
                }
                std::thread::sleep(backoff.min(remaining));
                backoff = backoff
                    .checked_mul(2)
                    .unwrap_or(LOCK_MAX_BACKOFF)
                    .min(LOCK_MAX_BACKOFF);
            }
            Err(_) => bail!("input history lock acquisition failed"),
        }
    }
}

fn lock_is_contended(error: &std::io::Error) -> bool {
    if error.kind() == ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    if error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32) {
        return true;
    }
    false
}

#[cfg(not(windows))]
fn replace_file_atomic(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination).context("cannot atomically replace input history file")
}

#[cfg(windows)]
fn replace_file_atomic(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let moved = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(std::io::Error::last_os_error())
            .context("cannot atomically replace input history file");
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW);
    if let Ok(directory) = options.open(path) {
        let _ = directory.sync_all();
    }
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::thread;
    use tempfile::TempDir;

    fn store() -> (TempDir, InputHistoryStore) {
        let temporary = tempfile::tempdir().expect("tempdir");
        let store = InputHistoryStore::open(temporary.path().join("history")).expect("store");
        (temporary, store)
    }

    fn context(workspace: &str, session: u128) -> HistoryContext {
        HistoryContext::new(workspace, Uuid::from_u128(session)).expect("context")
    }

    #[test]
    fn scopes_case_insensitive_fuzzy_dedup_and_cursor() {
        let (_temporary, store) = store();
        let current = context("workspace-a", 1);
        let other_session = context("workspace-a", 2);
        let other_project = context("workspace-b", 3);

        assert!(
            store
                .append_at(&current, "Cargo Check", 10)
                .expect("append")
                .inserted
        );
        assert!(
            !store
                .append_at(&current, "Cargo Check", 11)
                .expect("dedup")
                .inserted
        );
        store
            .append_at(&other_session, "cargo test", 12)
            .expect("append");
        store
            .append_at(&other_project, "compile release", 13)
            .expect("append");

        let session = store
            .search(
                &current,
                &HistoryQuery::new(HistoryScope::Session, "CARGO", 10),
            )
            .expect("session query");
        assert_eq!(session.len(), 1);
        assert_eq!(session[0].record.text, "Cargo Check");
        assert_eq!(session[0].kind, HistoryMatchKind::Substring);

        let project = store
            .search(&current, &HistoryQuery::new(HistoryScope::Project, "", 10))
            .expect("project query");
        assert_eq!(
            project
                .iter()
                .map(|entry| entry.record.text.as_str())
                .collect::<Vec<_>>(),
            ["cargo test", "Cargo Check"]
        );

        let everywhere = store
            .search(
                &current,
                &HistoryQuery::new(HistoryScope::Everywhere, "cmpl", 10),
            )
            .expect("fuzzy query");
        assert_eq!(everywhere[0].record.text, "compile release");
        assert_eq!(everywhere[0].kind, HistoryMatchKind::Fuzzy);

        let mut cursor = HistoryCursor::new(project);
        assert_eq!(
            cursor.older().map(|item| item.record.text.as_str()),
            Some("cargo test")
        );
        assert_eq!(
            cursor.older().map(|item| item.record.text.as_str()),
            Some("Cargo Check")
        );
        assert_eq!(
            cursor.newer().map(|item| item.record.text.as_str()),
            Some("cargo test")
        );
        assert!(cursor.newer().is_none());
    }

    #[test]
    fn enforces_capacity_entry_query_and_scan_limits() {
        let temporary = tempfile::tempdir().expect("tempdir");
        let limits = InputHistoryLimits {
            max_records: 3,
            max_file_bytes: 1_024,
            max_entry_bytes: 300,
            max_query_bytes: 4,
            max_results: 2,
            max_scan_records: 2,
            max_fuzzy_query_chars: 4,
            max_fuzzy_candidate_chars: 16,
            lock_timeout: Duration::from_secs(1),
        };
        let store = InputHistoryStore::with_limits(temporary.path().join("history"), limits)
            .expect("store");
        let context = context("workspace", 1);
        for (index, text) in ["one", "two", "three", "four"].into_iter().enumerate() {
            store
                .append_at(&context, text, index as u64)
                .expect("append");
        }
        let entries = store
            .search(&context, &HistoryQuery::new(HistoryScope::Session, "", 2))
            .expect("query");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].record.text, "four");
        assert_eq!(entries[1].record.text, "three");
        assert!(
            store
                .search(
                    &context,
                    &HistoryQuery::new(HistoryScope::Session, "12345", 2)
                )
                .is_err()
        );
        assert!(
            store
                .search(&context, &HistoryQuery::new(HistoryScope::Session, "", 3))
                .is_err()
        );
        assert!(store.append_at(&context, "x".repeat(400), 10).is_err());
    }

    #[test]
    fn malformed_truncated_unknown_and_nonmonotonic_files_fail_closed() {
        let (_temporary, store) = store();
        let context = context("workspace", 1);
        store.append_at(&context, "valid", 1).expect("append");

        write_private_test_file(store.history_path(), b"{broken}\n");
        assert!(
            store
                .search(&context, &HistoryQuery::new(HistoryScope::Session, "", 10))
                .is_err()
        );

        let record = InputHistoryRecord {
            version: FORMAT_VERSION,
            workspace_key: "workspace".to_owned(),
            session_id: context.session_id,
            text: "valid".to_owned(),
            timestamp_ms: 1,
            order: 1,
        };
        let truncated = serde_json::to_vec(&record).expect("json");
        write_private_test_file(store.history_path(), &truncated);
        assert!(store.append_at(&context, "next", 2).is_err());

        let unknown = format!(
            "{{\"version\":1,\"workspace_key\":\"workspace\",\"session_id\":\"{}\",\"text\":\"valid\",\"timestamp_ms\":1,\"order\":1,\"unknown\":true}}\n",
            context.session_id
        );
        write_private_test_file(store.history_path(), unknown.as_bytes());
        assert!(store.append_at(&context, "next", 2).is_err());

        let mut nonmonotonic = encode_record(&record).expect("encode");
        nonmonotonic.extend_from_slice(&encode_record(&record).expect("encode"));
        write_private_test_file(store.history_path(), &nonmonotonic);
        assert!(store.append_at(&context, "next", 2).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_roots_history_and_locks() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("tempdir");
        let target = temporary.path().join("target");
        fs::create_dir(&target).expect("target");
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).expect("chmod");
        let link = temporary.path().join("root-link");
        symlink(&target, &link).expect("symlink root");
        assert!(InputHistoryStore::open(&link).is_err());

        let store = InputHistoryStore::open(temporary.path().join("history")).expect("store");
        let outside = temporary.path().join("outside");
        write_private_test_file(&outside, b"outside");
        symlink(&outside, store.history_path()).expect("symlink history");
        let context = context("workspace", 1);
        assert!(store.append_at(&context, "text", 1).is_err());
        fs::remove_file(store.history_path()).expect("remove symlink");
        fs::remove_file(store.lock_path()).expect("remove lock");
        symlink(&outside, store.lock_path()).expect("symlink lock");
        assert!(store.append_at(&context, "text", 1).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn creates_private_root_and_files() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_temporary, store) = store();
        let context = context("workspace", 1);
        store.append_at(&context, "text", 1).expect("append");
        assert_eq!(
            fs::metadata(store.root())
                .expect("root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(store.history_path())
                .expect("history metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(store.lock_path())
                .expect("lock metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn lock_wait_is_bounded() {
        let temporary = tempfile::tempdir().expect("tempdir");
        let limits = InputHistoryLimits {
            lock_timeout: Duration::from_millis(20),
            ..InputHistoryLimits::default()
        };
        let store = InputHistoryStore::with_limits(temporary.path().join("history"), limits)
            .expect("store");
        let held = store.acquire_lock().expect("first lock");
        let started = Instant::now();
        assert!(store.acquire_lock().is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(held);
    }

    #[test]
    fn concurrent_processes_preserve_all_records_and_order() {
        let temporary = tempfile::tempdir().expect("tempdir");
        let root = temporary.path().join("history");
        let store = InputHistoryStore::open(&root).expect("store");
        let executable = std::env::current_exe().expect("test executable");
        let mut children = Vec::new();
        for index in 0..6 {
            children.push(
                Command::new(&executable)
                    .arg("--exact")
                    .arg("input_history::tests::subprocess_append_helper")
                    .arg("--test-threads=1")
                    .env("OAH_HISTORY_TEST_ROOT", &root)
                    .env("OAH_HISTORY_TEST_TEXT", format!("child-{index}"))
                    .spawn()
                    .expect("spawn child"),
            );
        }
        for mut child in children {
            assert!(child.wait().expect("wait child").success());
        }

        let current = context("workspace", 1);
        let entries = store
            .search(
                &current,
                &HistoryQuery::new(HistoryScope::Everywhere, "", 10),
            )
            .expect("query");
        assert_eq!(entries.len(), 6);
        let texts = entries
            .iter()
            .map(|entry| entry.record.text.clone())
            .collect::<HashSet<_>>();
        assert_eq!(texts.len(), 6);
        let mut orders = entries
            .iter()
            .map(|entry| entry.record.order)
            .collect::<Vec<_>>();
        orders.sort_unstable();
        assert_eq!(orders, [1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn subprocess_append_helper() {
        let Ok(root) = std::env::var("OAH_HISTORY_TEST_ROOT") else {
            return;
        };
        let text = std::env::var("OAH_HISTORY_TEST_TEXT").expect("child text");
        let store = InputHistoryStore::open(root).expect("child store");
        let context = context("workspace", 1);
        store.append(&context, text).expect("child append");
    }

    #[test]
    fn parallel_threads_do_not_lose_updates() {
        let (_temporary, store) = store();
        let mut workers = Vec::new();
        for index in 0..8 {
            let store = store.clone();
            workers.push(thread::spawn(move || {
                let context = context("workspace", 1);
                store
                    .append_at(&context, format!("thread-{index}"), index)
                    .expect("thread append");
            }));
        }
        for worker in workers {
            worker.join().expect("join");
        }
        let entries = store
            .search(
                &context("workspace", 1),
                &HistoryQuery::new(HistoryScope::Session, "", 20),
            )
            .expect("query");
        assert_eq!(entries.len(), 8);
    }

    fn write_private_test_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("write fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("chmod fixture");
        }
    }

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
}

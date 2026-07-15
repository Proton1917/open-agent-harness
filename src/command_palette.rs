//! Provider-neutral command palette and argument completion primitives.
//!
//! This module owns metadata validation, deterministic identity, conflict
//! resolution, ranking, and the bounded completion-provider boundary. It does
//! not know about a terminal renderer or any model provider.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

pub const MAX_PALETTE_COMMANDS: usize = 1024;
pub const MAX_COMMAND_NAME_BYTES: usize = 128;
pub const MAX_COMMAND_ALIASES: usize = 32;
pub const MAX_COMMAND_DESCRIPTION_BYTES: usize = 8 * 1024;
pub const MAX_ARGUMENT_HINT_BYTES: usize = 1024;
pub const MAX_ARGUMENT_NAMES: usize = 32;
pub const MAX_SOURCE_BYTES: usize = 256;
pub const MAX_REPOSITORY_BYTES: usize = 1024;
pub const MAX_PALETTE_RESULTS: usize = 100;
pub const MAX_COMPLETION_INPUT_BYTES: usize = 32 * 1024;
pub const MAX_COMPLETION_CANDIDATES: usize = 256;
pub const MAX_COMPLETION_VALUE_BYTES: usize = 4096;
pub const MAX_COMPLETION_DESCRIPTION_BYTES: usize = 4096;
pub const MAX_COMPLETION_ARGUMENTS: usize = 32;
pub const EMPTY_QUERY_FREQUENT_LIMIT: usize = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandKind {
    Builtin,
    Custom,
    Skill,
    McpPrompt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SourceLane {
    Builtin,
    User,
    Project,
    Policy,
    Organization,
    External,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandSource {
    Builtin,
    UserSettings,
    LocalSettings,
    ProjectSettings,
    PolicySettings,
    Plugin { name: String },
    Mcp { server: String },
    Other { name: String, lane: SourceLane },
}

impl CommandSource {
    pub fn lane(&self) -> SourceLane {
        match self {
            Self::Builtin => SourceLane::Builtin,
            Self::UserSettings | Self::LocalSettings => SourceLane::User,
            Self::ProjectSettings => SourceLane::Project,
            Self::PolicySettings => SourceLane::Policy,
            Self::Plugin { .. } => SourceLane::Organization,
            Self::Mcp { .. } => SourceLane::External,
            Self::Other { lane, .. } => *lane,
        }
    }

    pub fn tag(&self) -> String {
        match self {
            Self::Builtin => "builtin".into(),
            Self::UserSettings => "user".into(),
            Self::LocalSettings => "local".into(),
            Self::ProjectSettings => "project".into(),
            Self::PolicySettings => "policy".into(),
            Self::Plugin { name } => format!("plugin:{name}"),
            Self::Mcp { server } => format!("mcp:{server}"),
            Self::Other { name, .. } => name.clone(),
        }
    }

    fn default_priority(&self) -> u16 {
        match self {
            Self::Builtin => 1000,
            Self::PolicySettings => 900,
            Self::ProjectSettings => 700,
            Self::LocalSettings => 600,
            Self::UserSettings => 500,
            Self::Plugin { .. } => 400,
            Self::Mcp { .. } => 300,
            Self::Other { .. } => 100,
        }
    }

    fn validate(&self) -> Result<(), PaletteError> {
        match self {
            Self::Plugin { name } => validate_field("plugin source", name, MAX_SOURCE_BYTES, false),
            Self::Mcp { server } => validate_field("MCP source", server, MAX_SOURCE_BYTES, false),
            Self::Other { name, .. } => {
                validate_field("command source", name, MAX_SOURCE_BYTES, false)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommandUsage {
    pub invocation_count: u64,
    /// Catalog-local monotonic sequence. It never contains wall-clock or user data.
    pub last_used_sequence: u64,
}

#[derive(Clone)]
pub struct CommandDescriptor {
    pub name: String,
    pub display_name: Option<String>,
    pub aliases: Vec<String>,
    pub description: String,
    pub menu_description: Option<String>,
    pub argument_hint: Option<String>,
    pub argument_names: Vec<String>,
    pub kind: CommandKind,
    pub source: CommandSource,
    pub repository: Option<String>,
    pub hidden: bool,
    pub enabled: bool,
    pub usage: CommandUsage,
    pub completion: Option<Arc<dyn ArgumentCompletionProvider>>,
}

impl fmt::Debug for CommandDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandDescriptor")
            .field("name", &self.name)
            .field("display_name", &self.display_name)
            .field("aliases", &self.aliases)
            .field("description", &self.description)
            .field("menu_description", &self.menu_description)
            .field("argument_hint", &self.argument_hint)
            .field("argument_names", &self.argument_names)
            .field("kind", &self.kind)
            .field("source", &self.source)
            .field("repository", &self.repository)
            .field("hidden", &self.hidden)
            .field("enabled", &self.enabled)
            .field("usage", &self.usage)
            .field("has_completion", &self.completion.is_some())
            .finish()
    }
}

impl CommandDescriptor {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        kind: CommandKind,
        source: CommandSource,
    ) -> Self {
        Self {
            name: name.into(),
            display_name: None,
            aliases: Vec::new(),
            description: description.into(),
            menu_description: None,
            argument_hint: None,
            argument_names: Vec::new(),
            kind,
            source,
            repository: None,
            hidden: false,
            enabled: true,
            usage: CommandUsage::default(),
            completion: None,
        }
    }

    pub fn stable_id(&self) -> String {
        stable_command_id(&self.name, &self.source, self.repository.as_deref())
    }

    fn priority(&self) -> u16 {
        self.source.default_priority()
    }

    fn validate(&self) -> Result<(), PaletteError> {
        validate_command_name(&self.name)?;
        if let Some(display_name) = &self.display_name {
            validate_field("display name", display_name, MAX_COMMAND_NAME_BYTES, false)?;
        }
        if self.aliases.len() > MAX_COMMAND_ALIASES {
            return Err(PaletteError::LimitExceeded {
                field: "command aliases",
                limit: MAX_COMMAND_ALIASES,
            });
        }
        let mut aliases = BTreeSet::new();
        for alias in &self.aliases {
            validate_command_name(alias)?;
            let normalized = normalize(alias);
            if normalized == normalize(&self.name) || !aliases.insert(normalized) {
                return Err(PaletteError::InvalidField(
                    "command aliases contain a duplicate name".into(),
                ));
            }
        }
        validate_field(
            "command description",
            &self.description,
            MAX_COMMAND_DESCRIPTION_BYTES,
            true,
        )?;
        if let Some(description) = &self.menu_description {
            validate_field(
                "menu description",
                description,
                MAX_COMMAND_DESCRIPTION_BYTES,
                true,
            )?;
        }
        if let Some(hint) = &self.argument_hint {
            validate_field("argument hint", hint, MAX_ARGUMENT_HINT_BYTES, true)?;
        }
        if self.argument_names.len() > MAX_ARGUMENT_NAMES {
            return Err(PaletteError::LimitExceeded {
                field: "argument names",
                limit: MAX_ARGUMENT_NAMES,
            });
        }
        let mut argument_names = BTreeSet::new();
        for name in &self.argument_names {
            validate_argument_name(name)?;
            if !argument_names.insert(normalize(name)) {
                return Err(PaletteError::InvalidField(
                    "argument names contain a duplicate".into(),
                ));
            }
        }
        self.source.validate()?;
        if let Some(repository) = &self.repository {
            validate_field("repository", repository, MAX_REPOSITORY_BYTES, false)?;
        }
        Ok(())
    }
}

pub fn stable_command_id(name: &str, source: &CommandSource, repository: Option<&str>) -> String {
    let source = source.tag();
    let repository = repository.unwrap_or("");
    format!(
        "cmd:v1:{}:{name}:{}:{source}:{}:{repository}",
        name.len(),
        source.len(),
        repository.len()
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaletteSuggestion {
    pub id: String,
    pub name: String,
    pub aliases: Vec<String>,
    pub display_name: String,
    pub description: String,
    pub matched_alias: Option<String>,
    pub kind: CommandKind,
    pub lane: SourceLane,
    pub source_tag: String,
    pub argument_hint: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictKind {
    CanonicalName,
    Alias,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictCandidate {
    pub id: String,
    pub source_tag: String,
    pub priority: u16,
    pub canonical: bool,
    pub enabled: bool,
    pub hidden: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandConflict {
    pub token: String,
    pub kind: ConflictKind,
    pub candidates: Vec<ConflictCandidate>,
    pub winner_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionCandidate {
    pub value: String,
    pub description: Option<String>,
    pub is_final: bool,
}

impl CompletionCandidate {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            description: None,
            is_final: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionRequest {
    pub command_id: String,
    pub command_name: String,
    pub argument_index: usize,
    pub completed_arguments: Vec<String>,
    pub current_argument: String,
    pub argument_name: Option<String>,
    pub limit: usize,
}

pub trait ArgumentCompletionProvider: Send + Sync {
    fn complete(
        &self,
        request: &CompletionRequest,
    ) -> Result<Vec<CompletionCandidate>, CompletionProviderError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionProviderError {
    message: String,
}

impl CompletionProviderError {
    pub fn new(message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > MAX_COMPLETION_DESCRIPTION_BYTES {
            message.truncate(MAX_COMPLETION_DESCRIPTION_BYTES);
        }
        Self { message }
    }
}

impl fmt::Display for CompletionProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CompletionProviderError {}

#[derive(Clone, Debug, Default)]
pub struct StaticArgumentCompletionProvider {
    candidates: BTreeMap<usize, Vec<CompletionCandidate>>,
}

impl StaticArgumentCompletionProvider {
    pub fn try_new(
        candidates: BTreeMap<usize, Vec<CompletionCandidate>>,
    ) -> Result<Self, PaletteError> {
        if candidates.len() > MAX_ARGUMENT_NAMES {
            return Err(PaletteError::LimitExceeded {
                field: "static completion stages",
                limit: MAX_ARGUMENT_NAMES,
            });
        }
        for (&index, values) in &candidates {
            if index >= MAX_COMPLETION_ARGUMENTS {
                return Err(PaletteError::InvalidField(
                    "static completion argument index is out of range".into(),
                ));
            }
            validate_completion_candidates(values)?;
        }
        Ok(Self { candidates })
    }
}

impl ArgumentCompletionProvider for StaticArgumentCompletionProvider {
    fn complete(
        &self,
        request: &CompletionRequest,
    ) -> Result<Vec<CompletionCandidate>, CompletionProviderError> {
        Ok(self
            .candidates
            .get(&request.argument_index)
            .cloned()
            .unwrap_or_default())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaletteError {
    InvalidField(String),
    LimitExceeded { field: &'static str, limit: usize },
    DuplicateStableId(String),
    UnknownCommand(String),
    CompletionProvider(String),
}

impl fmt::Display for PaletteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidField(message) => write!(formatter, "invalid command metadata: {message}"),
            Self::LimitExceeded { field, limit } => {
                write!(formatter, "{field} exceeds limit {limit}")
            }
            Self::DuplicateStableId(id) => write!(formatter, "duplicate stable command id: {id}"),
            Self::UnknownCommand(command) => write!(formatter, "unknown command: {command}"),
            Self::CompletionProvider(message) => {
                write!(formatter, "argument completion failed: {message}")
            }
        }
    }
}

impl Error for PaletteError {}

struct CatalogEntry {
    id: String,
    descriptor: CommandDescriptor,
}

pub struct CommandCatalog {
    entries: BTreeMap<String, CatalogEntry>,
    canonical_winners: BTreeMap<String, String>,
    token_winners: BTreeMap<String, String>,
    conflicts: Vec<CommandConflict>,
    usage_sequence: u64,
}

impl fmt::Debug for CommandCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandCatalog")
            .field("entries", &self.entries.len())
            .field("conflicts", &self.conflicts)
            .field("usage_sequence", &self.usage_sequence)
            .finish()
    }
}

impl CommandCatalog {
    pub fn try_new(descriptors: Vec<CommandDescriptor>) -> Result<Self, PaletteError> {
        if descriptors.len() > MAX_PALETTE_COMMANDS {
            return Err(PaletteError::LimitExceeded {
                field: "palette commands",
                limit: MAX_PALETTE_COMMANDS,
            });
        }
        let mut entries = BTreeMap::new();
        let mut usage_sequence = 0u64;
        for descriptor in descriptors {
            descriptor.validate()?;
            usage_sequence = usage_sequence.max(descriptor.usage.last_used_sequence);
            let id = descriptor.stable_id();
            if entries
                .insert(
                    id.clone(),
                    CatalogEntry {
                        id: id.clone(),
                        descriptor,
                    },
                )
                .is_some()
            {
                return Err(PaletteError::DuplicateStableId(id));
            }
        }

        let mut tokens: BTreeMap<String, Vec<(String, bool)>> = BTreeMap::new();
        for entry in entries.values() {
            tokens
                .entry(normalize(&entry.descriptor.name))
                .or_default()
                .push((entry.id.clone(), true));
            for alias in &entry.descriptor.aliases {
                tokens
                    .entry(normalize(alias))
                    .or_default()
                    .push((entry.id.clone(), false));
            }
        }

        let mut canonical_winners = BTreeMap::new();
        let mut token_winners = BTreeMap::new();
        let mut conflicts = Vec::new();
        for (token, candidates) in tokens {
            let winner = choose_winner(&entries, &candidates);
            token_winners.insert(token.clone(), winner.clone());
            let canonical = candidates
                .iter()
                .filter(|(_, canonical)| *canonical)
                .collect::<Vec<_>>();
            if !canonical.is_empty() {
                let winner = choose_winner(
                    &entries,
                    &canonical
                        .iter()
                        .map(|(id, canonical)| ((*id).clone(), *canonical))
                        .collect::<Vec<_>>(),
                );
                canonical_winners.insert(token.clone(), winner);
            }
            let unique = candidates.iter().map(|(id, _)| id).collect::<BTreeSet<_>>();
            if unique.len() > 1 {
                let kind = if canonical.len() > 1 {
                    ConflictKind::CanonicalName
                } else {
                    ConflictKind::Alias
                };
                let mut details = candidates
                    .iter()
                    .map(|(id, canonical)| {
                        let entry = &entries[id];
                        ConflictCandidate {
                            id: id.clone(),
                            source_tag: entry.descriptor.source.tag(),
                            priority: entry.descriptor.priority(),
                            canonical: *canonical,
                            enabled: entry.descriptor.enabled,
                            hidden: entry.descriptor.hidden,
                        }
                    })
                    .collect::<Vec<_>>();
                details.sort_by(conflict_candidate_order);
                conflicts.push(CommandConflict {
                    token,
                    kind,
                    candidates: details,
                    winner_id: winner,
                });
            }
        }

        Ok(Self {
            entries,
            canonical_winners,
            token_winners,
            conflicts,
            usage_sequence,
        })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn conflicts(&self) -> &[CommandConflict] {
        &self.conflicts
    }

    pub fn resolve_id(&self, command_or_alias: &str) -> Option<&str> {
        self.token_winners
            .get(&normalize(command_or_alias))
            .filter(|id| self.entries[*id].descriptor.enabled)
            .map(String::as_str)
    }

    pub fn record_usage(&mut self, id: &str) -> Result<CommandUsage, PaletteError> {
        let entry = self
            .entries
            .get_mut(id)
            .ok_or_else(|| PaletteError::UnknownCommand(id.to_owned()))?;
        self.usage_sequence = self.usage_sequence.saturating_add(1);
        entry.descriptor.usage.invocation_count =
            entry.descriptor.usage.invocation_count.saturating_add(1);
        entry.descriptor.usage.last_used_sequence = self.usage_sequence;
        Ok(entry.descriptor.usage)
    }

    pub fn suggestions(
        &self,
        input: &str,
        limit: usize,
    ) -> Result<Vec<PaletteSuggestion>, PaletteError> {
        if limit > MAX_PALETTE_RESULTS {
            return Err(PaletteError::LimitExceeded {
                field: "palette result limit",
                limit: MAX_PALETTE_RESULTS,
            });
        }
        validate_palette_input(input)?;
        let query = input.strip_prefix('/').unwrap_or(input);
        if query.contains(char::is_whitespace) {
            return Ok(Vec::new());
        }
        if query.is_empty() {
            return Ok(self.empty_query_entries(limit));
        }
        Ok(self.ranked_entries(query, limit))
    }

    pub fn complete_arguments(
        &self,
        input: &str,
        cursor: usize,
        limit: usize,
    ) -> Result<Vec<CompletionCandidate>, PaletteError> {
        if input.len() > MAX_COMPLETION_INPUT_BYTES {
            return Err(PaletteError::LimitExceeded {
                field: "completion input",
                limit: MAX_COMPLETION_INPUT_BYTES,
            });
        }
        if limit > MAX_COMPLETION_CANDIDATES {
            return Err(PaletteError::LimitExceeded {
                field: "completion result limit",
                limit: MAX_COMPLETION_CANDIDATES,
            });
        }
        let before_cursor = input.get(..cursor).ok_or_else(|| {
            PaletteError::InvalidField("completion cursor is not a UTF-8 boundary".into())
        })?;
        let parsed = parse_completion_input(before_cursor)?;
        let id = self
            .resolve_id(&parsed.command)
            .ok_or_else(|| PaletteError::UnknownCommand(parsed.command.clone()))?;
        let entry = &self.entries[id];
        if !entry.descriptor.enabled {
            return Err(PaletteError::UnknownCommand(parsed.command));
        }
        let Some(provider) = &entry.descriptor.completion else {
            return Ok(Vec::new());
        };
        let argument_index = parsed.completed.len();
        let request = CompletionRequest {
            command_id: id.to_owned(),
            command_name: entry.descriptor.name.clone(),
            argument_index,
            completed_arguments: parsed.completed,
            current_argument: parsed.current,
            argument_name: entry.descriptor.argument_names.get(argument_index).cloned(),
            limit,
        };
        let mut candidates = catch_unwind(AssertUnwindSafe(|| provider.complete(&request)))
            .map_err(|_| PaletteError::CompletionProvider("provider panicked".into()))?
            .map_err(|error| PaletteError::CompletionProvider(error.to_string()))?;
        validate_completion_candidates(&candidates)?;
        let query = normalize(&request.current_argument);
        candidates.retain(|candidate| normalize(&candidate.value).contains(&query));
        candidates.sort_by(|left, right| completion_order(left, right, &query));
        candidates.dedup_by(|left, right| left.value == right.value);
        candidates.truncate(limit);
        Ok(candidates)
    }

    pub fn argument_hint_for_input(&self, input: &str) -> Result<Option<String>, PaletteError> {
        if input.len() > MAX_COMPLETION_INPUT_BYTES {
            return Err(PaletteError::LimitExceeded {
                field: "completion input",
                limit: MAX_COMPLETION_INPUT_BYTES,
            });
        }
        let parsed = parse_completion_input(input)?;
        let Some(id) = self.resolve_id(&parsed.command) else {
            return Ok(None);
        };
        let descriptor = &self.entries[id].descriptor;
        let index = parsed.completed.len();
        if let Some(name) = descriptor.argument_names.get(index) {
            return Ok(Some(format!("<{name}>")));
        }
        Ok(descriptor.argument_hint.clone())
    }

    fn visible_winner_entries(&self) -> Vec<&CatalogEntry> {
        self.entries
            .values()
            .filter(|entry| {
                entry.descriptor.enabled
                    && !entry.descriptor.hidden
                    && self
                        .canonical_winners
                        .get(&normalize(&entry.descriptor.name))
                        .is_some_and(|id| id == &entry.id)
            })
            .collect()
    }

    fn empty_query_entries(&self, limit: usize) -> Vec<PaletteSuggestion> {
        let entries = self.visible_winner_entries();
        let mut frequent = entries
            .iter()
            .copied()
            .filter(|entry| entry.descriptor.usage.invocation_count > 0)
            .collect::<Vec<_>>();
        frequent.sort_by(usage_order);
        frequent.truncate(EMPTY_QUERY_FREQUENT_LIMIT.min(limit));
        let frequent_ids = frequent
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<BTreeSet<_>>();
        let mut remainder = entries
            .into_iter()
            .filter(|entry| !frequent_ids.contains(entry.id.as_str()))
            .collect::<Vec<_>>();
        remainder.sort_by(lane_order);
        frequent
            .into_iter()
            .chain(remainder)
            .take(limit)
            .map(|entry| suggestion(entry, None))
            .collect()
    }

    fn ranked_entries(&self, query: &str, limit: usize) -> Vec<PaletteSuggestion> {
        let normalized_query = normalize(query);
        let visible = self.visible_winner_entries();
        let visible_exact_exists = visible
            .iter()
            .any(|entry| exact_match(&entry.descriptor, &normalized_query).is_some());
        let mut ranked = self
            .entries
            .values()
            .filter(|entry| entry.descriptor.enabled)
            .filter(|entry| {
                (!entry.descriptor.hidden
                    && self
                        .canonical_winners
                        .get(&normalize(&entry.descriptor.name))
                        .is_some_and(|id| id == &entry.id))
                    || (!visible_exact_exists
                        && exact_match(&entry.descriptor, &normalized_query).is_some())
            })
            .filter_map(|entry| {
                command_match(&entry.descriptor, &normalized_query).map(|matched| (matched, entry))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_match, left), (right_match, right)| {
            left_match
                .rank
                .cmp(&right_match.rank)
                .then_with(|| left_match.distance.cmp(&right_match.distance))
                .then_with(|| usage_order(left, right))
                .then_with(|| lane_order(left, right))
        });
        ranked
            .into_iter()
            .take(limit)
            .map(|(matched, entry)| suggestion(entry, matched.alias))
            .collect()
    }
}

fn choose_winner(
    entries: &BTreeMap<String, CatalogEntry>,
    candidates: &[(String, bool)],
) -> String {
    candidates
        .iter()
        .max_by(|(left_id, left_canonical), (right_id, right_canonical)| {
            let left = &entries[left_id];
            let right = &entries[right_id];
            left.descriptor
                .enabled
                .cmp(&right.descriptor.enabled)
                .then_with(|| left_canonical.cmp(right_canonical))
                .then_with(|| right.descriptor.hidden.cmp(&left.descriptor.hidden))
                .then_with(|| left.descriptor.priority().cmp(&right.descriptor.priority()))
                // Reverse lexical tie-break under max_by so the smaller stable
                // id wins independently of registration order.
                .then_with(|| right.id.cmp(&left.id))
        })
        .map(|(id, _)| id.clone())
        .expect("token groups are non-empty")
}

fn conflict_candidate_order(left: &ConflictCandidate, right: &ConflictCandidate) -> Ordering {
    right
        .enabled
        .cmp(&left.enabled)
        .then_with(|| right.canonical.cmp(&left.canonical))
        .then_with(|| left.hidden.cmp(&right.hidden))
        .then_with(|| right.priority.cmp(&left.priority))
        .then_with(|| left.id.cmp(&right.id))
}

fn suggestion(entry: &CatalogEntry, matched_alias: Option<String>) -> PaletteSuggestion {
    let descriptor = &entry.descriptor;
    PaletteSuggestion {
        id: entry.id.clone(),
        name: descriptor.name.clone(),
        aliases: descriptor.aliases.clone(),
        display_name: descriptor
            .display_name
            .clone()
            .unwrap_or_else(|| descriptor.name.clone()),
        description: descriptor
            .menu_description
            .clone()
            .unwrap_or_else(|| descriptor.description.clone()),
        matched_alias,
        kind: descriptor.kind,
        lane: descriptor.source.lane(),
        source_tag: descriptor.source.tag(),
        argument_hint: descriptor.argument_hint.clone(),
    }
}

fn usage_order(left: &&CatalogEntry, right: &&CatalogEntry) -> Ordering {
    right
        .descriptor
        .usage
        .invocation_count
        .cmp(&left.descriptor.usage.invocation_count)
        .then_with(|| {
            right
                .descriptor
                .usage
                .last_used_sequence
                .cmp(&left.descriptor.usage.last_used_sequence)
        })
        .then_with(|| normalize(&left.descriptor.name).cmp(&normalize(&right.descriptor.name)))
        .then_with(|| left.id.cmp(&right.id))
}

fn lane_order(left: &&CatalogEntry, right: &&CatalogEntry) -> Ordering {
    left.descriptor
        .source
        .lane()
        .cmp(&right.descriptor.source.lane())
        .then_with(|| normalize(&left.descriptor.name).cmp(&normalize(&right.descriptor.name)))
        .then_with(|| left.id.cmp(&right.id))
}

struct CommandMatch {
    rank: u8,
    distance: usize,
    alias: Option<String>,
}

fn command_match(descriptor: &CommandDescriptor, query: &str) -> Option<CommandMatch> {
    if let Some(alias) = exact_match(descriptor, query) {
        return Some(CommandMatch {
            rank: if alias.is_some() { 1 } else { 0 },
            distance: 0,
            alias,
        });
    }
    let name = normalize(&descriptor.name);
    let display = normalize(
        descriptor
            .display_name
            .as_deref()
            .unwrap_or(&descriptor.name),
    );
    if name.starts_with(query) || display.starts_with(query) {
        return Some(CommandMatch {
            rank: 2,
            distance: name.len().saturating_sub(query.len()),
            alias: None,
        });
    }
    if let Some(alias) = descriptor
        .aliases
        .iter()
        .find(|alias| normalize(alias).starts_with(query))
    {
        return Some(CommandMatch {
            rank: 3,
            distance: alias.len().saturating_sub(query.len()),
            alias: Some(alias.clone()),
        });
    }
    if words(&name).any(|word| word.starts_with(query))
        || words(&display).any(|word| word.starts_with(query))
        || words(&normalize(&descriptor.description)).any(|word| word.starts_with(query))
    {
        return Some(CommandMatch {
            rank: 4,
            distance: 0,
            alias: None,
        });
    }
    if name.contains(query) || display.contains(query) {
        return Some(CommandMatch {
            rank: 5,
            distance: 0,
            alias: None,
        });
    }
    let distance = edit_distance(&name, query);
    let threshold = usize::max(1, query.chars().count().saturating_mul(3).div_ceil(10));
    (distance <= threshold).then_some(CommandMatch {
        rank: 6,
        distance,
        alias: None,
    })
}

fn exact_match(descriptor: &CommandDescriptor, query: &str) -> Option<Option<String>> {
    if normalize(&descriptor.name) == query
        || normalize(
            descriptor
                .display_name
                .as_deref()
                .unwrap_or(&descriptor.name),
        ) == query
    {
        return Some(None);
    }
    descriptor
        .aliases
        .iter()
        .find(|alias| normalize(alias) == query)
        .map(|alias| Some(alias.clone()))
}

fn words(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
}

fn completion_order(
    left: &CompletionCandidate,
    right: &CompletionCandidate,
    query: &str,
) -> Ordering {
    let left_value = normalize(&left.value);
    let right_value = normalize(&right.value);
    let rank = |value: &str| {
        if value == query {
            0
        } else if value.starts_with(query) {
            1
        } else {
            2
        }
    };
    rank(&left_value)
        .cmp(&rank(&right_value))
        .then_with(|| left_value.cmp(&right_value))
}

fn validate_completion_candidates(values: &[CompletionCandidate]) -> Result<(), PaletteError> {
    if values.len() > MAX_COMPLETION_CANDIDATES {
        return Err(PaletteError::LimitExceeded {
            field: "completion candidates",
            limit: MAX_COMPLETION_CANDIDATES,
        });
    }
    for candidate in values {
        validate_field(
            "completion value",
            &candidate.value,
            MAX_COMPLETION_VALUE_BYTES,
            false,
        )?;
        if let Some(description) = &candidate.description {
            validate_field(
                "completion description",
                description,
                MAX_COMPLETION_DESCRIPTION_BYTES,
                true,
            )?;
        }
    }
    Ok(())
}

struct ParsedCompletion {
    command: String,
    completed: Vec<String>,
    current: String,
}

fn parse_completion_input(input: &str) -> Result<ParsedCompletion, PaletteError> {
    if input.contains(['\0', '\n', '\r']) {
        return Err(PaletteError::InvalidField(
            "completion input contains a control character".into(),
        ));
    }
    let rest = input
        .strip_prefix('/')
        .ok_or_else(|| PaletteError::InvalidField("completion input must start with /".into()))?;
    let command_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let command = rest[..command_end].to_owned();
    validate_command_name(&command)?;
    let arguments = &rest[command_end..];
    let trailing_space = arguments
        .chars()
        .next_back()
        .is_some_and(char::is_whitespace);
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in arguments.trim_start().chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        match (quote, character) {
            (_, '\\') => escaped = true,
            (None, '\'' | '"') => quote = Some(character),
            (Some(active), value) if active == value => quote = None,
            (None, value) if value.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                    if tokens.len() > MAX_COMPLETION_ARGUMENTS {
                        return Err(PaletteError::LimitExceeded {
                            field: "completion arguments",
                            limit: MAX_COMPLETION_ARGUMENTS,
                        });
                    }
                }
            }
            (_, value) => current.push(value),
        }
    }
    if escaped {
        current.push('\\');
    }
    if quote.is_some() {
        // An open quote is valid while editing; its content is the current token.
    }
    if trailing_space && !current.is_empty() {
        tokens.push(std::mem::take(&mut current));
    }
    if tokens.len() >= MAX_COMPLETION_ARGUMENTS && (trailing_space || !current.is_empty()) {
        return Err(PaletteError::LimitExceeded {
            field: "completion arguments",
            limit: MAX_COMPLETION_ARGUMENTS,
        });
    }
    Ok(ParsedCompletion {
        command,
        completed: tokens,
        current,
    })
}

fn validate_palette_input(input: &str) -> Result<(), PaletteError> {
    if input.len() > MAX_COMMAND_NAME_BYTES + 1 {
        return Err(PaletteError::LimitExceeded {
            field: "palette query",
            limit: MAX_COMMAND_NAME_BYTES + 1,
        });
    }
    if input.contains(['\0', '\n', '\r']) {
        return Err(PaletteError::InvalidField(
            "palette query contains a control character".into(),
        ));
    }
    Ok(())
}

fn validate_command_name(name: &str) -> Result<(), PaletteError> {
    validate_field("command name", name, MAX_COMMAND_NAME_BYTES, false)?;
    if !name.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '.' | ':' | '_' | '-')
    }) {
        return Err(PaletteError::InvalidField(format!(
            "command name contains unsupported characters: {name}"
        )));
    }
    Ok(())
}

fn validate_argument_name(name: &str) -> Result<(), PaletteError> {
    validate_field("argument name", name, MAX_COMMAND_NAME_BYTES, false)?;
    if !name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return Err(PaletteError::InvalidField(format!(
            "argument name contains unsupported characters: {name}"
        )));
    }
    Ok(())
}

fn validate_field(
    label: &'static str,
    value: &str,
    maximum: usize,
    allow_empty: bool,
) -> Result<(), PaletteError> {
    if (!allow_empty && value.trim().is_empty())
        || value.len() > maximum
        || value.chars().any(|character| {
            character == '\0'
                || character == '\u{1b}'
                || (character.is_control() && !matches!(character, '\n' | '\t'))
        })
    {
        return Err(PaletteError::InvalidField(format!(
            "{label} is empty, oversized, or contains unsafe controls"
        )));
    }
    Ok(())
}

fn normalize(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right = right.chars().collect::<Vec<_>>();
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    for (left_index, left_character) in left.chars().enumerate() {
        let mut current = Vec::with_capacity(right.len() + 1);
        current.push(left_index + 1);
        for (right_index, right_character) in right.iter().enumerate() {
            current.push(usize::min(
                usize::min(current[right_index] + 1, previous[right_index + 1] + 1),
                previous[right_index] + usize::from(left_character != *right_character),
            ));
        }
        previous = current;
    }
    previous[right.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn command(name: &str, kind: CommandKind, source: CommandSource) -> CommandDescriptor {
        CommandDescriptor::new(name, format!("Description for {name}"), kind, source)
    }

    #[test]
    fn stable_ids_include_source_and_repository_without_delimiter_ambiguity() {
        let mut first = command("review", CommandKind::Skill, CommandSource::ProjectSettings);
        let second = command("review", CommandKind::Skill, CommandSource::UserSettings);
        first.repository = Some("org/repo:a".into());
        assert_ne!(first.stable_id(), second.stable_id());
        assert_eq!(first.stable_id(), first.stable_id());
        assert!(first.stable_id().contains("cmd:v1"));
    }

    #[test]
    fn invalid_or_oversized_metadata_fails_closed() {
        let invalid = command("bad/name", CommandKind::Custom, CommandSource::UserSettings);
        assert!(matches!(
            CommandCatalog::try_new(vec![invalid]),
            Err(PaletteError::InvalidField(_))
        ));
        let mut oversized = command("ok", CommandKind::Custom, CommandSource::UserSettings);
        oversized.description = "x".repeat(MAX_COMMAND_DESCRIPTION_BYTES + 1);
        assert!(CommandCatalog::try_new(vec![oversized]).is_err());
    }

    #[test]
    fn duplicate_stable_identity_is_rejected() {
        let value = command("same", CommandKind::Builtin, CommandSource::Builtin);
        assert!(matches!(
            CommandCatalog::try_new(vec![value.clone(), value]),
            Err(PaletteError::DuplicateStableId(_))
        ));
    }

    #[test]
    fn empty_query_uses_top_five_frequency_then_source_lanes() {
        let mut descriptors = Vec::new();
        for index in 0..7u64 {
            let mut value = command(
                &format!("used{index}"),
                CommandKind::Skill,
                CommandSource::ProjectSettings,
            );
            value.usage = CommandUsage {
                invocation_count: index + 1,
                last_used_sequence: index,
            };
            descriptors.push(value);
        }
        descriptors.push(command(
            "builtin",
            CommandKind::Builtin,
            CommandSource::Builtin,
        ));
        descriptors.push(command(
            "user",
            CommandKind::Custom,
            CommandSource::UserSettings,
        ));
        let catalog = CommandCatalog::try_new(descriptors).unwrap();
        let names = catalog
            .suggestions("/", 9)
            .unwrap()
            .into_iter()
            .map(|item| item.name)
            .collect::<Vec<_>>();
        assert_eq!(&names[..5], ["used6", "used5", "used4", "used3", "used2"]);
        assert_eq!(&names[5..7], ["builtin", "user"]);
    }

    #[test]
    fn query_ranks_exact_alias_prefix_word_and_fuzzy() {
        let mut exact = command("model", CommandKind::Builtin, CommandSource::Builtin);
        exact.aliases = vec!["engine".into()];
        let prefix = command("model-list", CommandKind::Builtin, CommandSource::Builtin);
        let mut word = command("inspect", CommandKind::Builtin, CommandSource::Builtin);
        word.description = "Model diagnostics".into();
        let fuzzy = command("modal", CommandKind::Builtin, CommandSource::Builtin);
        let catalog = CommandCatalog::try_new(vec![word, fuzzy, prefix, exact]).unwrap();
        let names = catalog
            .suggestions("/model", 10)
            .unwrap()
            .into_iter()
            .map(|item| item.name)
            .collect::<Vec<_>>();
        assert_eq!(&names[..3], ["model", "model-list", "inspect"]);
        assert!(names.contains(&"modal".into()));
        let alias_match = catalog.suggestions("/engine", 10).unwrap().remove(0);
        assert_eq!(alias_match.matched_alias, Some("engine".into()));
        assert_eq!(alias_match.aliases, ["engine"]);
    }

    #[test]
    fn usage_recency_breaks_equal_search_ranks() {
        let mut old = command(
            "alpha-one",
            CommandKind::Skill,
            CommandSource::ProjectSettings,
        );
        old.usage = CommandUsage {
            invocation_count: 3,
            last_used_sequence: 1,
        };
        let mut recent = command(
            "alpha-two",
            CommandKind::Skill,
            CommandSource::ProjectSettings,
        );
        recent.usage = CommandUsage {
            invocation_count: 3,
            last_used_sequence: 9,
        };
        let catalog = CommandCatalog::try_new(vec![old, recent]).unwrap();
        assert_eq!(
            catalog.suggestions("/alpha", 10).unwrap()[0].name,
            "alpha-two"
        );
    }

    #[test]
    fn conflicts_are_explicit_and_builtin_is_the_deterministic_winner() {
        let builtin = command("clear", CommandKind::Builtin, CommandSource::Builtin);
        let project = command("clear", CommandKind::Skill, CommandSource::ProjectSettings);
        let catalog = CommandCatalog::try_new(vec![project, builtin.clone()]).unwrap();
        assert_eq!(catalog.conflicts().len(), 1);
        assert_eq!(catalog.conflicts()[0].kind, ConflictKind::CanonicalName);
        assert_eq!(catalog.conflicts()[0].winner_id, builtin.stable_id());
        assert_eq!(
            catalog.resolve_id("clear"),
            Some(builtin.stable_id().as_str())
        );
        assert_eq!(catalog.suggestions("/", 10).unwrap().len(), 1);
    }

    #[test]
    fn alias_collisions_are_recorded_and_canonical_names_win_aliases() {
        let canonical = command("new", CommandKind::Builtin, CommandSource::Builtin);
        let mut alias = command("clear", CommandKind::Builtin, CommandSource::Builtin);
        alias.aliases = vec!["new".into()];
        let catalog = CommandCatalog::try_new(vec![alias, canonical.clone()]).unwrap();
        assert_eq!(catalog.conflicts()[0].kind, ConflictKind::Alias);
        assert_eq!(
            catalog.resolve_id("new"),
            Some(canonical.stable_id().as_str())
        );
    }

    #[test]
    fn enabled_and_visible_commands_win_otherwise_equal_conflicts() {
        let mut disabled = command("review", CommandKind::Builtin, CommandSource::Builtin);
        disabled.enabled = false;
        let enabled = command(
            "review",
            CommandKind::Custom,
            CommandSource::ProjectSettings,
        );
        let enabled_id = enabled.stable_id();
        let catalog = CommandCatalog::try_new(vec![disabled, enabled]).unwrap();
        assert_eq!(catalog.resolve_id("review"), Some(enabled_id.as_str()));
        assert!(catalog.conflicts()[0].candidates[0].enabled);

        let mut hidden = command("inspect", CommandKind::Builtin, CommandSource::Builtin);
        hidden.hidden = true;
        let visible = command(
            "inspect",
            CommandKind::Custom,
            CommandSource::ProjectSettings,
        );
        let visible_id = visible.stable_id();
        let catalog = CommandCatalog::try_new(vec![hidden, visible]).unwrap();
        assert_eq!(catalog.resolve_id("inspect"), Some(visible_id.as_str()));
        assert!(!catalog.conflicts()[0].candidates[0].hidden);
    }

    #[test]
    fn hidden_commands_only_surface_for_an_exact_query() {
        let mut hidden = command("internal", CommandKind::Builtin, CommandSource::Builtin);
        hidden.hidden = true;
        let catalog = CommandCatalog::try_new(vec![hidden]).unwrap();
        assert!(catalog.suggestions("/", 10).unwrap().is_empty());
        assert_eq!(
            catalog.suggestions("/internal", 10).unwrap()[0].name,
            "internal"
        );
    }

    #[test]
    fn static_completion_advances_one_argument_at_a_time() {
        let provider = StaticArgumentCompletionProvider::try_new(BTreeMap::from([
            (
                0,
                vec![
                    CompletionCandidate::new("enable"),
                    CompletionCandidate::new("disable"),
                ],
            ),
            (
                1,
                vec![
                    CompletionCandidate::new("server-a"),
                    CompletionCandidate::new("server-b"),
                ],
            ),
        ]))
        .unwrap();
        let mut value = command("mcp", CommandKind::Builtin, CommandSource::Builtin);
        value.argument_names = vec!["action".into(), "server".into()];
        value.completion = Some(Arc::new(provider));
        let catalog = CommandCatalog::try_new(vec![value]).unwrap();
        assert_eq!(
            catalog.complete_arguments("/mcp en", 7, 10).unwrap()[0].value,
            "enable"
        );
        let servers = catalog
            .complete_arguments("/mcp enable server-", 19, 10)
            .unwrap();
        assert_eq!(servers.len(), 2);
        assert_eq!(
            catalog.argument_hint_for_input("/mcp enable ").unwrap(),
            Some("<server>".into())
        );
    }

    struct RecordingProvider {
        request: Mutex<Option<CompletionRequest>>,
    }

    impl ArgumentCompletionProvider for RecordingProvider {
        fn complete(
            &self,
            request: &CompletionRequest,
        ) -> Result<Vec<CompletionCandidate>, CompletionProviderError> {
            *self.request.lock().unwrap() = Some(request.clone());
            Ok(vec![CompletionCandidate::new("session-1")])
        }
    }

    #[test]
    fn injected_provider_receives_bounded_parsed_context() {
        let provider = Arc::new(RecordingProvider {
            request: Mutex::new(None),
        });
        let mut value = command("resume", CommandKind::Builtin, CommandSource::Builtin);
        value.argument_names = vec!["session".into()];
        value.completion = Some(provider.clone());
        let catalog = CommandCatalog::try_new(vec![value]).unwrap();
        let result = catalog.complete_arguments("/resume sess", 12, 4).unwrap();
        assert_eq!(result[0].value, "session-1");
        let request = provider.request.lock().unwrap().clone().unwrap();
        assert_eq!(request.argument_index, 0);
        assert_eq!(request.current_argument, "sess");
        assert_eq!(request.limit, 4);
    }

    struct BadProvider;

    impl ArgumentCompletionProvider for BadProvider {
        fn complete(
            &self,
            _: &CompletionRequest,
        ) -> Result<Vec<CompletionCandidate>, CompletionProviderError> {
            Ok((0..=MAX_COMPLETION_CANDIDATES)
                .map(|index| CompletionCandidate::new(index.to_string()))
                .collect())
        }
    }

    #[test]
    fn oversized_provider_output_fails_closed() {
        let mut value = command("bad", CommandKind::Custom, CommandSource::UserSettings);
        value.completion = Some(Arc::new(BadProvider));
        let catalog = CommandCatalog::try_new(vec![value]).unwrap();
        assert!(matches!(
            catalog.complete_arguments("/bad ", 5, 10),
            Err(PaletteError::LimitExceeded { .. })
        ));
    }

    struct PanicProvider;

    impl ArgumentCompletionProvider for PanicProvider {
        fn complete(
            &self,
            _: &CompletionRequest,
        ) -> Result<Vec<CompletionCandidate>, CompletionProviderError> {
            panic!("provider bug")
        }
    }

    #[test]
    fn provider_panics_are_contained() {
        let mut value = command("panic", CommandKind::Custom, CommandSource::UserSettings);
        value.completion = Some(Arc::new(PanicProvider));
        let catalog = CommandCatalog::try_new(vec![value]).unwrap();
        assert!(matches!(
            catalog.complete_arguments("/panic ", 7, 10),
            Err(PaletteError::CompletionProvider(_))
        ));
    }

    #[test]
    fn quoted_and_escaped_arguments_are_parsed_without_shell_execution() {
        let parsed = parse_completion_input("/run 'first value' second\\ value th").unwrap();
        assert_eq!(parsed.command, "run");
        assert_eq!(parsed.completed, ["first value", "second value"]);
        assert_eq!(parsed.current, "th");
    }

    #[test]
    fn all_generic_command_kinds_and_sources_retain_palette_metadata() {
        let mut mcp = command(
            "server:review",
            CommandKind::McpPrompt,
            CommandSource::Mcp {
                server: "server".into(),
            },
        );
        mcp.display_name = Some("review (MCP)".into());
        mcp.argument_names = vec!["target".into()];
        let values = vec![
            command("help", CommandKind::Builtin, CommandSource::Builtin),
            command("audit", CommandKind::Custom, CommandSource::UserSettings),
            command("deploy", CommandKind::Skill, CommandSource::ProjectSettings),
            mcp,
        ];
        let catalog = CommandCatalog::try_new(values).unwrap();
        let mcp = catalog.suggestions("/server", 10).unwrap().remove(0);
        assert_eq!(mcp.kind, CommandKind::McpPrompt);
        assert_eq!(mcp.lane, SourceLane::External);
        assert_eq!(mcp.source_tag, "mcp:server");
    }

    #[test]
    fn usage_recording_is_monotonic_and_saturating() {
        let value = command("status", CommandKind::Builtin, CommandSource::Builtin);
        let id = value.stable_id();
        let mut catalog = CommandCatalog::try_new(vec![value]).unwrap();
        let first = catalog.record_usage(&id).unwrap();
        let second = catalog.record_usage(&id).unwrap();
        assert_eq!(second.invocation_count, 2);
        assert!(second.last_used_sequence > first.last_used_sequence);
    }
}

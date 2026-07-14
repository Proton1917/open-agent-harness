use std::{
    collections::{HashSet, VecDeque},
    fmt,
    sync::{Arc, RwLock},
};

use anyhow::{Result, bail};
use clap::ValueEnum;
use globset::GlobBuilder;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PermissionMode {
    Default,
    AcceptEdits,
    Plan,
    BypassPermissions,
    #[value(name = "dont-ask", alias = "dontAsk")]
    DontAsk,
}

impl PermissionMode {
    pub fn from_setting(value: &str) -> Option<Self> {
        match value {
            "default" => Some(Self::Default),
            "acceptEdits" => Some(Self::AcceptEdits),
            "plan" => Some(Self::Plan),
            "bypassPermissions" => Some(Self::BypassPermissions),
            "dontAsk" => Some(Self::DontAsk),
            _ => None,
        }
    }

    pub const fn as_setting(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::AcceptEdits => "acceptEdits",
            Self::Plan => "plan",
            Self::BypassPermissions => "bypassPermissions",
            Self::DontAsk => "dontAsk",
        }
    }
}

pub type PermissionPromptHandler =
    Arc<dyn Fn(&PermissionRequest) -> Result<PermissionDecision> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRequest {
    pub tool: String,
    pub input: Value,
    pub tool_use_id: String,
    pub summary: String,
    pub read_only: bool,
    pub destructive: bool,
    pub outside_workspace: bool,
}

#[derive(Clone)]
pub struct PermissionManager {
    pub mode: PermissionMode,
    pub interactive: bool,
    allow: Vec<String>,
    deny: Vec<String>,
    session_mode: Arc<RwLock<Option<PermissionMode>>>,
    workspace_deny: Arc<RwLock<Vec<String>>>,
    prompt_handler: Arc<RwLock<Option<PermissionPromptHandler>>>,
}

impl fmt::Debug for PermissionManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PermissionManager")
            .field("mode", &self.mode)
            .field("interactive", &self.interactive)
            .field("allow", &self.allow)
            .field("deny", &self.deny)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    AllowWithUpdatedInput(Value),
    Deny,
    Interrupt,
}

/// One observable identity for a tool invocation. `candidates` are equivalent
/// spellings of the same object (for example canonical absolute and
/// workspace-relative file paths). A matching deny rule on any spelling wins;
/// an allow rule may match any spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionTarget {
    pub tool: String,
    pub candidates: Vec<String>,
}

impl PermissionTarget {
    pub fn new(tool: impl Into<String>, candidates: Vec<String>) -> Self {
        Self {
            tool: tool.into(),
            candidates,
        }
    }
}

impl PermissionManager {
    pub fn new(
        mode: PermissionMode,
        interactive: bool,
        allow: Vec<String>,
        deny: Vec<String>,
    ) -> Self {
        Self {
            mode,
            interactive,
            allow,
            deny,
            session_mode: Arc::new(RwLock::new(None)),
            workspace_deny: Arc::new(RwLock::new(Vec::new())),
            prompt_handler: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_prompt_handler(&self, handler: Option<PermissionPromptHandler>) {
        *self
            .prompt_handler
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = handler;
    }

    pub fn set_workspace_deny(&self, rules: Vec<String>) {
        *self
            .workspace_deny
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = rules;
    }

    pub(crate) fn workspace_deny_rules(&self) -> Vec<String> {
        self.workspace_deny
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Creates a permission view for an independently refreshable tool
    /// context. Session-wide state remains shared, while project deny rules
    /// are copied because they are derived from that context's current cwd.
    pub(crate) fn fork_for_context(&self) -> Self {
        Self {
            mode: self.mode,
            interactive: self.interactive,
            allow: self.allow.clone(),
            deny: self.deny.clone(),
            session_mode: Arc::clone(&self.session_mode),
            workspace_deny: Arc::new(RwLock::new(self.workspace_deny_rules())),
            prompt_handler: Arc::clone(&self.prompt_handler),
        }
    }

    /// Creates an invocation-local view that adds trusted allow rules while
    /// sharing the live mode, workspace deny rules, and prompt handler. Deny
    /// evaluation and Plan mode still run before these scoped allows.
    pub(crate) fn with_scoped_allow(&self, rules: &[String]) -> Result<Self> {
        let mut scoped = self.clone();
        let mut seen = scoped.allow.iter().cloned().collect::<HashSet<_>>();
        for rule in rules {
            let rule = rule.trim();
            if rule.is_empty() || rule.len() > 512 || rule.contains(['\0', '\n', '\r']) {
                bail!("scoped permission allow rule 无效")
            }
            let parsed = parse_rule(rule);
            if parsed.tool.is_empty()
                || !parsed.tool.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':' | b'*')
                })
            {
                bail!("scoped permission allow rule tool 无效")
            }
            if seen.insert(rule.to_owned()) {
                scoped.allow.push(rule.to_owned());
            }
        }
        Ok(scoped)
    }

    pub fn effective_mode(&self) -> PermissionMode {
        self.session_mode
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unwrap_or(self.mode)
    }

    pub fn set_session_mode(&self, next: PermissionMode) -> Result<bool> {
        if self.mode == PermissionMode::Plan && next != PermissionMode::Plan {
            bail!("用户从命令行或可信设置锁定了 plan 模式，交互快捷键不能解除")
        }
        let mut mode = self
            .session_mode
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = mode.unwrap_or(self.mode);
        if current == PermissionMode::Plan && next != PermissionMode::Plan {
            bail!("必须通过 ExitPlanMode 展示计划并获得用户批准后才能退出 plan 模式")
        }
        if current == next {
            return Ok(false);
        }
        *mode = Some(next);
        Ok(true)
    }

    pub fn enter_plan_mode(&self) -> bool {
        let mut mode = self
            .session_mode
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if mode.unwrap_or(self.mode) == PermissionMode::Plan {
            return false;
        }
        *mode = Some(PermissionMode::Plan);
        true
    }

    pub fn exit_plan_mode(&self) -> Result<bool> {
        if self.mode == PermissionMode::Plan {
            bail!("用户从命令行或可信设置锁定了 plan 模式，工具不能解除")
        }
        let mut mode = self
            .session_mode
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(mode.take() == Some(PermissionMode::Plan))
    }

    pub fn decide(
        &self,
        tool: &str,
        summary: &str,
        read_only: bool,
        destructive: bool,
        outside_workspace: bool,
    ) -> Result<PermissionDecision> {
        self.decide_invocation(
            tool,
            &Value::Null,
            "",
            summary,
            read_only,
            destructive,
            outside_workspace,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn decide_invocation(
        &self,
        tool: &str,
        input: &Value,
        tool_use_id: &str,
        summary: &str,
        read_only: bool,
        destructive: bool,
        outside_workspace: bool,
    ) -> Result<PermissionDecision> {
        self.decide_invocation_with_targets(
            tool,
            input,
            tool_use_id,
            summary,
            read_only,
            destructive,
            outside_workspace,
            &[PermissionTarget::new(tool, vec![summary.to_owned()])],
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn decide_invocation_with_targets(
        &self,
        tool: &str,
        input: &Value,
        tool_use_id: &str,
        summary: &str,
        read_only: bool,
        destructive: bool,
        outside_workspace: bool,
        targets: &[PermissionTarget],
    ) -> Result<PermissionDecision> {
        let workspace_deny = self
            .workspace_deny
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if invocation_matches_rules(&self.deny, tool, summary, targets, RuleBehavior::Deny)
            || invocation_matches_rules(&workspace_deny, tool, summary, targets, RuleBehavior::Deny)
        {
            return Ok(PermissionDecision::Deny);
        }
        let mode = self.effective_mode();
        if mode == PermissionMode::Plan {
            return Ok(if read_only && !outside_workspace {
                PermissionDecision::Allow
            } else {
                PermissionDecision::Deny
            });
        }
        if invocation_matches_rules(&self.allow, tool, summary, targets, RuleBehavior::Allow) {
            return Ok(PermissionDecision::Allow);
        }
        match mode {
            PermissionMode::BypassPermissions => Ok(PermissionDecision::Allow),
            PermissionMode::Plan => unreachable!("plan mode returned before allow-rule handling"),
            PermissionMode::AcceptEdits
                if !outside_workspace && matches!(tool, "Edit" | "NotebookEdit" | "Write") =>
            {
                Ok(PermissionDecision::Allow)
            }
            _ if read_only && !destructive && !outside_workspace => Ok(PermissionDecision::Allow),
            PermissionMode::DontAsk => Ok(PermissionDecision::Deny),
            PermissionMode::Default | PermissionMode::AcceptEdits => {
                let request = PermissionRequest {
                    tool: tool.to_owned(),
                    input: input.clone(),
                    tool_use_id: tool_use_id.to_owned(),
                    summary: summary.to_owned(),
                    read_only,
                    destructive,
                    outside_workspace,
                };
                let handler = self
                    .prompt_handler
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if let Some(handler) = handler {
                    return handler(&request);
                }
                if self.interactive {
                    prompt(tool, summary)
                } else {
                    Ok(PermissionDecision::Deny)
                }
            }
        }
    }

    pub fn permits_updated_invocation(
        &self,
        tool: &str,
        summary: &str,
        read_only: bool,
        outside_workspace: bool,
    ) -> bool {
        self.permits_updated_invocation_with_targets(
            tool,
            summary,
            read_only,
            outside_workspace,
            &[PermissionTarget::new(tool, vec![summary.to_owned()])],
        )
    }

    pub fn permits_updated_invocation_with_targets(
        &self,
        tool: &str,
        summary: &str,
        read_only: bool,
        outside_workspace: bool,
        targets: &[PermissionTarget],
    ) -> bool {
        let workspace_deny = self
            .workspace_deny
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if invocation_matches_rules(&self.deny, tool, summary, targets, RuleBehavior::Deny)
            || invocation_matches_rules(&workspace_deny, tool, summary, targets, RuleBehavior::Deny)
        {
            return false;
        }
        self.effective_mode() != PermissionMode::Plan || (read_only && !outside_workspace)
    }

    /// Search tools use this before entering a directory or opening a file so
    /// `Read(...)` deny rules constrain discovery as well as direct reads.
    pub fn denies_read_path(&self, candidates: &[String]) -> bool {
        let target = PermissionTarget::new("Read", candidates.to_vec());
        let targets = [target];
        let workspace_deny = self
            .workspace_deny
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        invocation_matches_rules(&self.deny, "Read", "", &targets, RuleBehavior::Deny)
            || invocation_matches_rules(&workspace_deny, "Read", "", &targets, RuleBehavior::Deny)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuleBehavior {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy)]
struct ParsedRule<'a> {
    tool: &'a str,
    content: Option<&'a str>,
}

fn parse_rule(rule: &str) -> ParsedRule<'_> {
    let trimmed = rule.trim();
    let Some(open) = trimmed.find('(') else {
        return ParsedRule {
            tool: trimmed,
            content: None,
        };
    };
    if !trimmed.ends_with(')') || open == 0 {
        return ParsedRule {
            tool: trimmed,
            content: None,
        };
    }
    let content = &trimmed[open + 1..trimmed.len() - 1];
    ParsedRule {
        tool: &trimmed[..open],
        content: if content.is_empty() || content == "*" {
            None
        } else {
            Some(content)
        },
    }
}

fn invocation_matches_rules(
    rules: &[String],
    invocation_tool: &str,
    summary: &str,
    targets: &[PermissionTarget],
    behavior: RuleBehavior,
) -> bool {
    if invocation_tool == "Bash" {
        return shell_invocation_matches_rules(rules, summary, behavior);
    }
    if invocation_tool == "Monitor" {
        if let Some(command) = targets
            .iter()
            .find(|target| target.tool == "Bash")
            .and_then(|target| target.candidates.first())
        {
            return shell_invocation_matches_rules(rules, command, behavior);
        }
    }
    rules.iter().any(|raw_rule| {
        let rule = parse_rule(raw_rule);
        targets.iter().any(|target| {
            rule_tool_matches(rule.tool, &target.tool)
                && match rule.content {
                    None => true,
                    Some(pattern) => target.candidates.iter().any(|candidate| {
                        generic_content_matches(pattern, candidate, &target.tool, behavior)
                    }),
                }
        })
    })
}

fn rule_tool_matches(rule_tool: &str, target_tool: &str) -> bool {
    if rule_tool == target_tool {
        return true;
    }
    GlobBuilder::new(rule_tool)
        .literal_separator(true)
        .build()
        .map(|glob| glob.compile_matcher().is_match(target_tool))
        .unwrap_or(false)
}

fn generic_content_matches(
    pattern: &str,
    candidate: &str,
    tool: &str,
    behavior: RuleBehavior,
) -> bool {
    let filesystem_rule = matches!(
        tool,
        "Read" | "Write" | "Edit" | "NotebookEdit" | "Glob" | "Grep"
    );
    let pattern = if filesystem_rule {
        normalize_permission_path_pattern(pattern)
    } else {
        pattern.to_owned()
    };
    let candidate = if filesystem_rule {
        normalize_permission_path_candidate(candidate)
    } else {
        candidate.to_owned()
    };
    if pattern == candidate {
        return true;
    }
    if let Some(directory) = pattern.strip_suffix("/**") {
        if if filesystem_rule && behavior == RuleBehavior::Deny {
            directory.eq_ignore_ascii_case(&candidate)
        } else {
            directory == candidate
        } {
            return true;
        }
    }
    GlobBuilder::new(&pattern)
        .literal_separator(true)
        .case_insensitive(filesystem_rule && behavior == RuleBehavior::Deny)
        .build()
        .map(|glob| glob.compile_matcher().is_match(&candidate))
        .unwrap_or(false)
}

fn normalize_permission_path_pattern(value: &str) -> String {
    let value = value.replace('\\', "/");
    let value = if value == "~" {
        dirs::home_dir()
            .map(|home| home.to_string_lossy().replace('\\', "/"))
            .unwrap_or(value)
    } else if let Some(rest) = value.strip_prefix("~/") {
        dirs::home_dir()
            .map(|home| format!("{}/{rest}", home.to_string_lossy().replace('\\', "/")))
            .unwrap_or(value)
    } else {
        value
    };
    let value = value.strip_prefix("./").unwrap_or(&value);
    normalize_lexical_slashes(value, true)
}

fn normalize_permission_path_candidate(value: &str) -> String {
    normalize_lexical_slashes(&value.replace('\\', "/"), false)
}

fn normalize_lexical_slashes(value: &str, preserve_globs: bool) -> String {
    let absolute = value.starts_with('/');
    let mut parts = Vec::new();
    for part in value.split('/') {
        match part {
            "" | "." => {}
            ".." if parts
                .last()
                .is_some_and(|last: &&str| preserve_globs && last.contains(['*', '?', '['])) =>
            {
                parts.push(part)
            }
            ".." if parts.last().is_some_and(|last| *last != "..") => {
                parts.pop();
            }
            ".." if !absolute => parts.push(part),
            ".." => {}
            _ => parts.push(part),
        }
    }
    let joined = parts.join("/");
    if absolute {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".to_owned()
    } else {
        joined
    }
}

#[derive(Debug)]
struct ShellAnalysis {
    raw: String,
    operations: Vec<ShellOperation>,
    safe_for_pattern_allow: bool,
    deny_opaque: bool,
}

#[derive(Debug)]
struct ShellOperation {
    allow_candidates: Vec<String>,
    deny_candidates: Vec<String>,
}

fn shell_invocation_matches_rules(rules: &[String], command: &str, behavior: RuleBehavior) -> bool {
    let analysis = analyze_shell(command);
    let shell_rules = rules
        .iter()
        .map(|rule| parse_rule(rule))
        .filter(|rule| rule_tool_matches(rule.tool, "Bash"))
        .collect::<Vec<_>>();
    if shell_rules.is_empty() {
        return false;
    }

    // Tool-wide shell rules intentionally apply to the entire invocation.
    if shell_rules.iter().any(|rule| rule.content.is_none()) {
        return true;
    }

    if behavior == RuleBehavior::Deny {
        // If an execution-bearing construct exceeds the bounded static
        // analysis below, fail closed whenever any Bash deny rule exists.
        // Otherwise a long/ambiguous wrapper could hide a denied executable.
        if analysis.deny_opaque {
            return true;
        }
        return shell_rules.iter().any(|rule| {
            let pattern = rule.content.expect("tool-wide rules handled above");
            shell_rule_matches(pattern, &analysis.raw, true)
                || analysis.operations.iter().any(|operation| {
                    operation
                        .deny_candidates
                        .iter()
                        .any(|candidate| shell_rule_matches(pattern, candidate, false))
                })
        });
    }

    // Exact full-command allows remain useful for syntax that cannot be
    // analyzed safely. Prefix and wildcard rules are only evaluated against
    // atomic parsed operations.
    if shell_rules.iter().any(|rule| {
        rule.content
            .is_some_and(|pattern| shell_rule_is_exact(pattern) && pattern.trim() == analysis.raw)
    }) {
        return true;
    }
    if !analysis.safe_for_pattern_allow || analysis.operations.is_empty() {
        return false;
    }
    analysis.operations.iter().all(|operation| {
        shell_rules.iter().any(|rule| {
            let pattern = rule.content.expect("tool-wide rules handled above");
            operation
                .allow_candidates
                .iter()
                .any(|candidate| shell_rule_matches(pattern, candidate, false))
        })
    })
}

fn shell_rule_is_exact(pattern: &str) -> bool {
    !pattern.ends_with(":*") && !contains_unescaped_star(pattern)
}

fn shell_rule_matches(pattern: &str, command: &str, exact_only: bool) -> bool {
    let pattern = pattern.trim();
    let command = command.trim();
    if pattern == command {
        return true;
    }
    if exact_only {
        return false;
    }
    if let Some(prefix) = pattern.strip_suffix(":*") {
        let prefix = prefix.trim_end();
        return command == prefix
            || command
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with(char::is_whitespace))
            || command
                .strip_prefix("xargs ")
                .is_some_and(|inner| inner == prefix || inner.starts_with(&format!("{prefix} ")));
    }
    if contains_unescaped_star(pattern) {
        return wildcard_match(pattern, command);
    }
    false
}

fn contains_unescaped_star(pattern: &str) -> bool {
    let mut escaped = false;
    for ch in pattern.chars() {
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '*' {
            return true;
        }
    }
    false
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let tokens = wildcard_tokens(pattern.trim());
    if wildcard_tokens_match(&tokens, value) {
        return true;
    }
    // A sole trailing ` *` has prefix semantics and also matches the bare
    // command, matching the documented compatibility behavior.
    if tokens
        .iter()
        .filter(|token| matches!(token, WildcardToken::Any))
        .count()
        == 1
        && matches!(tokens.last(), Some(WildcardToken::Any))
        && matches!(
            tokens.get(tokens.len().saturating_sub(2)),
            Some(WildcardToken::Char(' '))
        )
    {
        let shortened = &tokens[..tokens.len() - 2];
        return wildcard_tokens_match(shortened, value);
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WildcardToken {
    Any,
    Char(char),
}

fn wildcard_tokens(pattern: &str) -> Vec<WildcardToken> {
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            tokens.push(WildcardToken::Char(chars.next().unwrap_or('\\')));
        } else if ch == '*' {
            tokens.push(WildcardToken::Any);
        } else {
            tokens.push(WildcardToken::Char(ch));
        }
    }
    tokens
}

fn wildcard_tokens_match(tokens: &[WildcardToken], value: &str) -> bool {
    let value = value.chars().collect::<Vec<_>>();
    let (mut token_index, mut value_index) = (0usize, 0usize);
    let (mut star_index, mut star_value_index) = (None, 0usize);
    while value_index < value.len() {
        match tokens.get(token_index) {
            Some(WildcardToken::Char(ch)) if *ch == value[value_index] => {
                token_index += 1;
                value_index += 1;
            }
            Some(WildcardToken::Any) => {
                star_index = Some(token_index);
                token_index += 1;
                star_value_index = value_index;
            }
            _ if star_index.is_some() => {
                star_value_index += 1;
                value_index = star_value_index;
                token_index = star_index.expect("checked") + 1;
            }
            _ => return false,
        }
    }
    while matches!(tokens.get(token_index), Some(WildcardToken::Any)) {
        token_index += 1;
    }
    token_index == tokens.len()
}

#[derive(Debug)]
struct LexedShell {
    commands: Vec<Vec<String>>,
    safe: bool,
}

fn analyze_shell(command: &str) -> ShellAnalysis {
    analyze_shell_depth(command, 0)
}

fn analyze_shell_depth(command: &str, depth: usize) -> ShellAnalysis {
    let raw = command.trim().to_owned();
    let lexed = lex_shell(command);
    let source_segments = split_atomic_shell_sources(command);
    let mut operations = Vec::new();
    let mut safe_for_pattern_allow = lexed.safe;
    let mut deny_opaque = source_segments
        .iter()
        .any(|segment| shell_segment_has_dynamic_token(segment));
    for (command_index, words) in lexed.commands.into_iter().enumerate() {
        if words.is_empty() || words.iter().all(|word| is_assignment(word)) {
            continue;
        }
        let original = words.join(" ");
        let mut allow_candidates = Vec::new();
        let mut deny_candidates = Vec::new();
        if let Some(source) = source_segments.get(command_index) {
            push_unique(&mut allow_candidates, source.clone());
            push_unique(&mut deny_candidates, source.clone());
        }
        push_unique(&mut allow_candidates, original.clone());
        push_unique(&mut deny_candidates, original);
        if let Some(stripped) = strip_safe_wrappers(&words) {
            push_unique(&mut allow_candidates, stripped.join(" "));
        }
        let expansion = expand_words_for_deny(&words);
        deny_opaque |= expansion.opaque;
        let mut nested_scripts = Vec::new();
        for expanded in expansion.words {
            push_unique(&mut deny_candidates, expanded.join(" "));
            if let Some(script) = shell_code_argument(&expanded) {
                safe_for_pattern_allow = false;
                push_unique(&mut nested_scripts, script.to_owned());
            }
        }
        operations.push(ShellOperation {
            allow_candidates,
            deny_candidates,
        });
        for script in nested_scripts {
            if depth < 8 {
                let nested = analyze_shell_depth(&script, depth + 1);
                deny_opaque |= nested.deny_opaque;
                operations.extend(nested.operations);
            } else {
                deny_opaque = true;
            }
        }
    }
    let substitutions = extract_executed_substitutions(command);
    deny_opaque |= substitutions.opaque;
    if !substitutions.commands.is_empty() {
        safe_for_pattern_allow = false;
    }
    for nested_command in substitutions.commands {
        if depth < 8 {
            let nested = analyze_shell_depth(&nested_command, depth + 1);
            deny_opaque |= nested.deny_opaque;
            operations.extend(nested.operations);
        } else {
            deny_opaque = true;
        }
    }
    ShellAnalysis {
        raw,
        operations,
        safe_for_pattern_allow,
        deny_opaque,
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.contains(&value) {
        values.push(value);
    }
}

fn lex_shell(command: &str) -> LexedShell {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let chars = command.chars().collect::<Vec<_>>();
    let mut commands = Vec::<Vec<String>>::new();
    let mut words = Vec::<String>::new();
    let mut word = String::new();
    let mut quote = Quote::None;
    let mut safe = true;
    let mut index = 0usize;
    let mut at_word_start = true;
    let flush_word = |word: &mut String, words: &mut Vec<String>| {
        if !word.is_empty() {
            words.push(std::mem::take(word));
        }
    };
    let flush_command =
        |word: &mut String, words: &mut Vec<String>, commands: &mut Vec<Vec<String>>| {
            if !word.is_empty() {
                words.push(std::mem::take(word));
            }
            if !words.is_empty() {
                commands.push(std::mem::take(words));
            }
        };

    while index < chars.len() {
        let ch = chars[index];
        match quote {
            Quote::Single => {
                if ch == '\'' {
                    quote = Quote::None;
                } else {
                    word.push(ch);
                }
                index += 1;
                at_word_start = false;
            }
            Quote::Double => {
                if ch == '"' {
                    quote = Quote::None;
                    index += 1;
                } else if ch == '\\' {
                    let Some(next) = chars.get(index + 1).copied() else {
                        safe = false;
                        break;
                    };
                    if next == '\n' {
                        index += 2;
                    } else {
                        word.push(next);
                        index += 2;
                    }
                } else {
                    if matches!(ch, '$' | '`') {
                        safe = false;
                    }
                    word.push(ch);
                    index += 1;
                }
                at_word_start = false;
            }
            Quote::None => match ch {
                '\'' => {
                    quote = Quote::Single;
                    index += 1;
                    at_word_start = false;
                }
                '"' => {
                    quote = Quote::Double;
                    index += 1;
                    at_word_start = false;
                }
                '\\' => {
                    let Some(next) = chars.get(index + 1).copied() else {
                        safe = false;
                        break;
                    };
                    if next == '\n' {
                        index += 2;
                    } else {
                        word.push(next);
                        index += 2;
                        at_word_start = false;
                    }
                }
                '#' if at_word_start => {
                    while index < chars.len() && chars[index] != '\n' {
                        index += 1;
                    }
                }
                c if c.is_whitespace() && c != '\n' => {
                    flush_word(&mut word, &mut words);
                    index += 1;
                    at_word_start = true;
                }
                '\n' | ';' | '|' | '&' => {
                    flush_command(&mut word, &mut words, &mut commands);
                    // Consume the second character in &&, ||, ;; and &>.
                    if chars.get(index + 1).copied() == Some(ch) {
                        index += 1;
                    }
                    index += 1;
                    at_word_start = true;
                }
                '>' | '<' => {
                    // Redirections are executable side effects whose target
                    // is not represented by a Bash prefix rule. Require an
                    // exact command approval instead of auto-allowing it.
                    safe = false;
                    flush_word(&mut word, &mut words);
                    index += 1;
                    while index < chars.len() && matches!(chars[index], '>' | '<' | '&') {
                        index += 1;
                    }
                    while index < chars.len() && chars[index].is_whitespace() {
                        index += 1;
                    }
                    // Skip one statically quoted/unquoted redirect target.
                    let mut target_quote = Quote::None;
                    while index < chars.len() {
                        let current = chars[index];
                        if target_quote == Quote::None
                            && (current.is_whitespace() || matches!(current, ';' | '|' | '&'))
                        {
                            break;
                        }
                        if current == '\'' && target_quote != Quote::Double {
                            target_quote = if target_quote == Quote::Single {
                                Quote::None
                            } else {
                                Quote::Single
                            };
                        } else if current == '"' && target_quote != Quote::Single {
                            target_quote = if target_quote == Quote::Double {
                                Quote::None
                            } else {
                                Quote::Double
                            };
                        } else if matches!(current, '$' | '`') && target_quote != Quote::Single {
                            safe = false;
                        }
                        index += 1;
                    }
                    if target_quote != Quote::None {
                        safe = false;
                    }
                    at_word_start = true;
                }
                '(' | ')' | '{' | '}' => {
                    safe = false;
                    flush_command(&mut word, &mut words, &mut commands);
                    index += 1;
                    at_word_start = true;
                }
                '$' | '`' => {
                    safe = false;
                    word.push(ch);
                    index += 1;
                    at_word_start = false;
                }
                _ => {
                    word.push(ch);
                    index += 1;
                    at_word_start = false;
                }
            },
        }
    }
    if quote != Quote::None {
        safe = false;
    }
    flush_command(&mut word, &mut words, &mut commands);
    if commands.iter().any(|command| {
        command.first().is_some_and(|name| {
            matches!(
                name.as_str(),
                "eval"
                    | "source"
                    | "."
                    | "exec"
                    | "command"
                    | "builtin"
                    | "fc"
                    | "coproc"
                    | "noglob"
                    | "nocorrect"
                    | "trap"
                    | "enable"
                    | "alias"
                    | "bash"
                    | "sh"
                    | "zsh"
                    | "dash"
                    | "if"
                    | "then"
                    | "elif"
                    | "else"
                    | "fi"
                    | "for"
                    | "while"
                    | "until"
                    | "do"
                    | "done"
                    | "case"
                    | "esac"
                    | "select"
                    | "function"
                    | "!"
            )
        })
    }) {
        safe = false;
    }
    LexedShell { commands, safe }
}

fn split_atomic_shell_sources(command: &str) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let chars = command.chars().collect::<Vec<_>>();
    let mut quote = Quote::None;
    let mut escaped = false;
    let mut start = 0usize;
    let mut index = 0usize;
    let mut result = Vec::new();
    while index < chars.len() {
        let ch = chars[index];
        if escaped {
            escaped = false;
            index += 1;
            continue;
        }
        if ch == '\\' && quote != Quote::Single {
            escaped = true;
            index += 1;
            continue;
        }
        match ch {
            '\'' if quote != Quote::Double => {
                quote = if quote == Quote::Single {
                    Quote::None
                } else {
                    Quote::Single
                };
            }
            '"' if quote != Quote::Single => {
                quote = if quote == Quote::Double {
                    Quote::None
                } else {
                    Quote::Double
                };
            }
            '#' if quote == Quote::None
                && (index == start
                    || chars
                        .get(index.wrapping_sub(1))
                        .is_some_and(|ch| ch.is_whitespace())) =>
            {
                while index < chars.len() && chars[index] != '\n' {
                    index += 1;
                }
                continue;
            }
            '\n' | ';' | '|' | '&' if quote == Quote::None => {
                let segment = chars[start..index]
                    .iter()
                    .collect::<String>()
                    .trim()
                    .to_owned();
                if !segment.is_empty() && !segment.starts_with('#') {
                    result.push(segment);
                }
                if chars.get(index + 1).copied() == Some(ch) {
                    index += 1;
                }
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    let segment = chars[start..].iter().collect::<String>().trim().to_owned();
    if !segment.is_empty() && !segment.starts_with('#') {
        result.push(segment);
    }
    result
}

/// Returns true only when a non-assignment shell word is produced by a
/// runtime expansion. Static shell syntax can still disable pattern-based
/// allows without making deny evaluation opaque. Single-quoted and escaped
/// metacharacters remain literal.
fn shell_segment_has_dynamic_token(segment: &str) -> bool {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let chars = segment.chars().collect::<Vec<_>>();
    let mut words = Vec::<(String, bool)>::new();
    let mut word = String::new();
    let mut dynamic = false;
    let mut quote = Quote::None;
    let mut index = 0usize;
    while index < chars.len() {
        let ch = chars[index];
        match quote {
            Quote::Single => {
                if ch == '\'' {
                    quote = Quote::None;
                } else {
                    word.push(ch);
                }
                index += 1;
            }
            Quote::Double => {
                if ch == '"' {
                    quote = Quote::None;
                    index += 1;
                } else if ch == '\\' {
                    if let Some(next) = chars.get(index + 1).copied() {
                        word.push(next);
                        index += 2;
                    } else {
                        index += 1;
                    }
                } else {
                    if ch == '`' || ch == '$' && dollar_starts_expansion(&chars, index) {
                        dynamic = true;
                    }
                    word.push(ch);
                    index += 1;
                }
            }
            Quote::None => match ch {
                '\'' => {
                    quote = Quote::Single;
                    index += 1;
                }
                '"' => {
                    quote = Quote::Double;
                    index += 1;
                }
                '\\' => {
                    if let Some(next) = chars.get(index + 1).copied() {
                        word.push(next);
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
                '#' if word.is_empty() => break,
                ch if ch.is_whitespace() => {
                    push_dynamic_word(&mut words, &mut word, &mut dynamic);
                    index += 1;
                }
                '$' => {
                    dynamic |= dollar_starts_expansion(&chars, index);
                    word.push(ch);
                    index += 1;
                }
                '`' => {
                    dynamic = true;
                    word.push(ch);
                    index += 1;
                }
                '*' | '?' => {
                    dynamic = true;
                    word.push(ch);
                    index += 1;
                }
                '[' => {
                    dynamic |= has_glob_class_end(&chars, index + 1);
                    word.push(ch);
                    index += 1;
                }
                '{' => {
                    dynamic |= starts_brace_expansion(&chars, index);
                    word.push(ch);
                    index += 1;
                }
                '~' if word.is_empty() || word.ends_with(['=', ':']) => {
                    dynamic = true;
                    word.push(ch);
                    index += 1;
                }
                _ => {
                    word.push(ch);
                    index += 1;
                }
            },
        }
    }
    push_dynamic_word(&mut words, &mut word, &mut dynamic);

    let mut command_seen = false;
    for (word, dynamic) in words {
        if !command_seen && is_assignment(&word) {
            continue;
        }
        command_seen = true;
        if dynamic {
            return true;
        }
    }
    false
}

fn push_dynamic_word(words: &mut Vec<(String, bool)>, word: &mut String, dynamic: &mut bool) {
    if !word.is_empty() {
        words.push((std::mem::take(word), *dynamic));
    }
    *dynamic = false;
}

fn dollar_starts_expansion(chars: &[char], index: usize) -> bool {
    chars.get(index + 1).is_some_and(|next| {
        next.is_ascii_alphanumeric()
            || *next == '_'
            || matches!(next, '(' | '{' | '*' | '@' | '#' | '?' | '$' | '!' | '-')
    })
}

fn has_glob_class_end(chars: &[char], mut index: usize) -> bool {
    while let Some(ch) = chars.get(index).copied() {
        if ch == '\\' {
            index += 2;
            continue;
        }
        if ch == ']' {
            return true;
        }
        if ch.is_whitespace() || matches!(ch, ';' | '|' | '&' | '<' | '>') {
            return false;
        }
        index += 1;
    }
    false
}

fn starts_brace_expansion(chars: &[char], start: usize) -> bool {
    let mut index = start + 1;
    let mut nesting = 0usize;
    let mut comma = false;
    let mut range = false;
    while let Some(ch) = chars.get(index).copied() {
        if ch == '\\' {
            index += 2;
            continue;
        }
        match ch {
            '{' => nesting += 1,
            '}' if nesting == 0 => return comma || range,
            '}' => nesting -= 1,
            ',' if nesting == 0 => comma = true,
            '.' if nesting == 0 && chars.get(index + 1) == Some(&'.') => range = true,
            ch if nesting == 0
                && (ch.is_whitespace() || matches!(ch, ';' | '|' | '&' | '<' | '>')) =>
            {
                return false;
            }
            _ => {}
        }
        index += 1;
    }
    false
}

fn is_assignment(word: &str) -> bool {
    let Some((name, _)) = word.split_once('=') else {
        return false;
    };
    let name = name.strip_suffix('+').unwrap_or(name);
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn strip_all_assignments(words: &[String]) -> Vec<String> {
    words
        .iter()
        .skip_while(|word| is_assignment(word))
        .cloned()
        .collect()
}

fn strip_safe_wrappers(words: &[String]) -> Option<Vec<String>> {
    let mut current = strip_safe_allow_assignments(words);
    let original = words.to_vec();
    for _ in 0..8 {
        let Some(name) = current.first().map(String::as_str) else {
            break;
        };
        let next = match name {
            "time" | "nohup" => 1 + usize::from(current.get(1).is_some_and(|arg| arg == "--")),
            "nice" => {
                if current.get(1).is_some_and(|arg| arg == "-n")
                    && current.get(2).is_some_and(|arg| is_signed_integer(arg))
                {
                    3 + usize::from(current.get(3).is_some_and(|arg| arg == "--"))
                } else if current
                    .get(1)
                    .is_some_and(|arg| arg.starts_with('-') && is_signed_integer(arg))
                {
                    2 + usize::from(current.get(2).is_some_and(|arg| arg == "--"))
                } else {
                    1 + usize::from(current.get(1).is_some_and(|arg| arg == "--"))
                }
            }
            "timeout" => timeout_command_index(&current)?,
            "stdbuf" => stdbuf_command_index(&current)?,
            "env" => env_command_index(&current)?,
            _ => break,
        };
        if next >= current.len() {
            break;
        }
        current = current[next..].to_vec();
    }
    (current != original && !current.is_empty()).then_some(current)
}

fn strip_safe_allow_assignments(words: &[String]) -> Vec<String> {
    let mut index = 0usize;
    while let Some(word) = words.get(index) {
        let Some((name, value)) = word.split_once('=') else {
            break;
        };
        let name = name.strip_suffix('+').unwrap_or(name);
        if !matches!(
            name,
            "GOEXPERIMENT"
                | "GOOS"
                | "GOARCH"
                | "CGO_ENABLED"
                | "GO111MODULE"
                | "RUST_BACKTRACE"
                | "RUST_LOG"
                | "NODE_ENV"
                | "PYTHONUNBUFFERED"
                | "PYTHONDONTWRITEBYTECODE"
                | "PYTEST_DISABLE_PLUGIN_AUTOLOAD"
                | "PYTEST_DEBUG"
                | "LANG"
                | "LANGUAGE"
                | "LC_ALL"
                | "LC_CTYPE"
                | "LC_TIME"
                | "CHARSET"
                | "TERM"
                | "COLORTERM"
                | "NO_COLOR"
                | "FORCE_COLOR"
                | "TZ"
                | "LS_COLORS"
                | "LSCOLORS"
                | "GREP_COLOR"
                | "GREP_COLORS"
                | "GCC_COLORS"
                | "TIME_STYLE"
                | "BLOCK_SIZE"
                | "BLOCKSIZE"
        ) || value.is_empty()
            || !value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '/' | ':' | '-'))
        {
            break;
        }
        index += 1;
    }
    words[index..].to_vec()
}

#[derive(Debug, Default)]
struct DenyWordExpansion {
    words: Vec<Vec<String>>,
    opaque: bool,
}

const MAX_DENY_EXPANSIONS: usize = 256;
const MAX_DENY_EXPANSION_DEPTH: usize = 16;
const MAX_CONSERVATIVE_SUFFIXES: usize = 64;

/// Builds deny-only executable identities. This intentionally does not feed
/// the allow candidates: wrappers and path basenames must never widen an
/// allowlist. The queue is bounded; exhaustion is recorded as opaque so deny
/// evaluation can fail closed instead of silently skipping a hidden command.
fn expand_words_for_deny(words: &[String]) -> DenyWordExpansion {
    let mut expansion = DenyWordExpansion::default();
    let mut queue = VecDeque::from([(words.to_vec(), 0usize)]);
    let mut seen = HashSet::new();
    while let Some((current, depth)) = queue.pop_front() {
        if current.is_empty() || !seen.insert(current.clone()) {
            continue;
        }
        if expansion.words.len() >= MAX_DENY_EXPANSIONS {
            expansion.opaque = true;
            break;
        }
        expansion.words.push(current.clone());

        let next = deny_word_transforms(&current);
        expansion.opaque |= next.opaque;
        if depth >= MAX_DENY_EXPANSION_DEPTH {
            if !next.words.is_empty() {
                expansion.opaque = true;
            }
            continue;
        }
        queue.extend(
            next.words
                .into_iter()
                .filter(|candidate| !candidate.is_empty())
                .map(|candidate| (candidate, depth + 1)),
        );
    }
    expansion
}

fn deny_word_transforms(words: &[String]) -> DenyWordExpansion {
    let mut transformed = DenyWordExpansion::default();
    let assignments_stripped = strip_all_assignments(words);
    if assignments_stripped != words && !assignments_stripped.is_empty() {
        push_unique_words(&mut transformed.words, assignments_stripped);
    }
    if let Some(basename) = executable_basename_identity(words) {
        push_unique_words(&mut transformed.words, basename);
    }
    if let Some(wrapper_stripped) = strip_safe_wrappers(words) {
        push_unique_words(&mut transformed.words, wrapper_stripped);
    } else if wrapper_needs_static_analysis(words) {
        let suffixes = conservative_executable_suffixes(words, 1);
        transformed.opaque |= suffixes.opaque;
        for suffix in suffixes.words {
            push_unique_words(&mut transformed.words, suffix);
        }
    }
    let wrappers = deny_execution_wrapper_inners(words);
    transformed.opaque |= wrappers.opaque;
    for inner in wrappers.words {
        push_unique_words(&mut transformed.words, inner);
    }
    let xargs = xargs_inner_candidates(words);
    transformed.opaque |= xargs.opaque;
    for inner in xargs.words {
        push_unique_words(&mut transformed.words, inner);
    }
    if let Some(inner) = find_exec_inner(words) {
        push_unique_words(&mut transformed.words, inner);
    }
    if let Some(inner) = strip_shell_control_prefix(words) {
        push_unique_words(&mut transformed.words, inner);
    }
    transformed
}

fn push_unique_words(values: &mut Vec<Vec<String>>, value: Vec<String>) {
    if !value.is_empty() && !values.contains(&value) {
        values.push(value);
    }
}

fn executable_basename_identity(words: &[String]) -> Option<Vec<String>> {
    let executable = words.first()?;
    if !executable.contains('/') {
        return None;
    }
    let basename = executable
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|basename| !basename.is_empty())?;
    if basename == executable {
        return None;
    }
    let mut normalized = words.to_vec();
    normalized[0] = basename.to_owned();
    Some(normalized)
}

fn conservative_executable_suffixes(words: &[String], start: usize) -> DenyWordExpansion {
    let mut result = DenyWordExpansion::default();
    for index in start..words.len() {
        let word = &words[index];
        if word.starts_with('-') || is_assignment(word) {
            continue;
        }
        if result.words.len() >= MAX_CONSERVATIVE_SUFFIXES {
            result.opaque = true;
            break;
        }
        result.words.push(words[index..].to_vec());
    }
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WrapperCommand {
    Found(usize),
    None,
    Ambiguous(usize),
}

fn deny_execution_wrapper_inners(words: &[String]) -> DenyWordExpansion {
    let Some(executable) = words.first().map(String::as_str) else {
        return DenyWordExpansion::default();
    };
    let parsed = match executable {
        "command" | "builtin" => parse_command_builtin_wrapper(words),
        "exec" => parse_exec_wrapper(words),
        "sudo" => parse_sudo_wrapper(words),
        "doas" => parse_doas_wrapper(words),
        // Known env forms are stripped by `strip_safe_wrappers`; unknown
        // options remain execution-bearing and need conservative suffixes.
        "env" if env_command_index(words).is_none() => WrapperCommand::Ambiguous(1),
        _ => return DenyWordExpansion::default(),
    };
    match parsed {
        WrapperCommand::Found(index) if index < words.len() => DenyWordExpansion {
            words: vec![words[index..].to_vec()],
            opaque: false,
        },
        WrapperCommand::Ambiguous(start) => conservative_executable_suffixes(words, start),
        WrapperCommand::Found(_) | WrapperCommand::None => DenyWordExpansion::default(),
    }
}

fn parse_command_builtin_wrapper(words: &[String]) -> WrapperCommand {
    let mut index = 1usize;
    while let Some(arg) = words.get(index).map(String::as_str) {
        match arg {
            "--" => return command_after(words, index + 1),
            "-p" | "-v" | "-V" => index += 1,
            _ if arg.starts_with('-') => return WrapperCommand::Ambiguous(index + 1),
            _ => return WrapperCommand::Found(index),
        }
    }
    WrapperCommand::None
}

fn parse_exec_wrapper(words: &[String]) -> WrapperCommand {
    let mut index = 1usize;
    while let Some(arg) = words.get(index).map(String::as_str) {
        if arg == "--" {
            return command_after(words, index + 1);
        }
        if arg == "-a" {
            if words.get(index + 1).is_none() {
                return WrapperCommand::None;
            }
            index += 2;
        } else if arg.starts_with("-a") && arg.len() > 2
            || arg.starts_with('-')
                && arg.len() > 1
                && arg[1..].chars().all(|flag| matches!(flag, 'c' | 'l'))
        {
            index += 1;
        } else if arg.starts_with('-') {
            return WrapperCommand::Ambiguous(index + 1);
        } else {
            return WrapperCommand::Found(index);
        }
    }
    WrapperCommand::None
}

fn parse_sudo_wrapper(words: &[String]) -> WrapperCommand {
    const VALUE_SHORT: &[&str] = &[
        "-C", "-D", "-R", "-T", "-U", "-g", "-h", "-p", "-r", "-t", "-u",
    ];
    const VALUE_LONG: &[&str] = &[
        "--chdir",
        "--chroot",
        "--close-from",
        "--command-timeout",
        "--group",
        "--host",
        "--other-user",
        "--prompt",
        "--role",
        "--type",
        "--user",
    ];
    const FLAG_LONG: &[&str] = &[
        "--askpass",
        "--background",
        "--edit",
        "--help",
        "--login",
        "--non-interactive",
        "--preserve-env",
        "--remove-timestamp",
        "--reset-timestamp",
        "--set-home",
        "--shell",
        "--stdin",
        "--validate",
        "--version",
    ];
    let mut index = 1usize;
    while let Some(arg) = words.get(index).map(String::as_str) {
        if arg == "--" {
            return command_after(words, index + 1);
        }
        if is_assignment(arg) {
            index += 1;
            continue;
        }
        if VALUE_SHORT.contains(&arg) || VALUE_LONG.contains(&arg) {
            if words.get(index + 1).is_none() {
                return WrapperCommand::None;
            }
            index += 2;
            continue;
        }
        if VALUE_SHORT
            .iter()
            .any(|option| arg.starts_with(option) && arg.len() > option.len())
            || VALUE_LONG
                .iter()
                .any(|option| arg.starts_with(&format!("{option}=")))
            || arg.starts_with("--preserve-env=")
        {
            index += 1;
            continue;
        }
        if FLAG_LONG.contains(&arg)
            || arg.starts_with('-')
                && arg.len() > 1
                && arg[1..].chars().all(|flag| {
                    matches!(
                        flag,
                        'A' | 'b'
                            | 'E'
                            | 'e'
                            | 'H'
                            | 'i'
                            | 'K'
                            | 'k'
                            | 'l'
                            | 'n'
                            | 'P'
                            | 'S'
                            | 's'
                            | 'V'
                            | 'v'
                    )
                })
        {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            return WrapperCommand::Ambiguous(index + 1);
        }
        return WrapperCommand::Found(index);
    }
    WrapperCommand::None
}

fn parse_doas_wrapper(words: &[String]) -> WrapperCommand {
    let mut index = 1usize;
    while let Some(arg) = words.get(index).map(String::as_str) {
        if arg == "--" {
            return command_after(words, index + 1);
        }
        if matches!(arg, "-a" | "-C" | "-u") {
            if words.get(index + 1).is_none() {
                return WrapperCommand::None;
            }
            index += 2;
        } else if ["-a", "-C", "-u"]
            .iter()
            .any(|option| arg.starts_with(option) && arg.len() > option.len())
            || arg.starts_with('-')
                && arg.len() > 1
                && arg[1..].chars().all(|flag| matches!(flag, 'L' | 'n' | 's'))
        {
            index += 1;
        } else if arg.starts_with('-') {
            return WrapperCommand::Ambiguous(index + 1);
        } else {
            return WrapperCommand::Found(index);
        }
    }
    WrapperCommand::None
}

fn command_after(words: &[String], index: usize) -> WrapperCommand {
    if index < words.len() {
        WrapperCommand::Found(index)
    } else {
        WrapperCommand::None
    }
}

fn wrapper_needs_static_analysis(words: &[String]) -> bool {
    words.len() > 1
        && words.first().is_some_and(|word| {
            matches!(
                word.as_str(),
                "timeout" | "time" | "nice" | "stdbuf" | "nohup" | "env"
            )
        })
}

fn timeout_command_index(words: &[String]) -> Option<usize> {
    let mut index = 1usize;
    while let Some(arg) = words.get(index) {
        if matches!(
            arg.as_str(),
            "--foreground" | "--preserve-status" | "--verbose" | "-v"
        ) || (arg.starts_with("--kill-after=") || arg.starts_with("--signal="))
            && safe_wrapper_value(arg.split_once('=').map(|(_, value)| value).unwrap_or(""))
            || (arg.starts_with("-k") || arg.starts_with("-s"))
                && arg.len() > 2
                && safe_wrapper_value(&arg[2..])
        {
            index += 1;
        } else if matches!(arg.as_str(), "--kill-after" | "--signal" | "-k" | "-s") {
            if !words
                .get(index + 1)
                .is_some_and(|value| safe_wrapper_value(value))
            {
                return None;
            }
            index += 2;
        } else if arg == "--" {
            index += 1;
            break;
        } else if arg.starts_with('-') {
            return None;
        } else {
            break;
        }
    }
    let duration = words.get(index)?;
    is_duration(duration).then_some(index + 1)
}

fn stdbuf_command_index(words: &[String]) -> Option<usize> {
    let mut index = 1usize;
    let mut consumed = false;
    while let Some(arg) = words.get(index) {
        if matches!(arg.as_str(), "-i" | "-o" | "-e") {
            words.get(index + 1)?;
            index += 2;
            consumed = true;
        } else if (arg.starts_with("-i") || arg.starts_with("-o") || arg.starts_with("-e"))
            && arg.len() > 2
            || arg.starts_with("--input=")
            || arg.starts_with("--output=")
            || arg.starts_with("--error=")
        {
            index += 1;
            consumed = true;
        } else if arg.starts_with('-') {
            return None;
        } else {
            break;
        }
    }
    (consumed && index < words.len()).then_some(index)
}

fn env_command_index(words: &[String]) -> Option<usize> {
    let mut index = 1usize;
    while let Some(arg) = words.get(index) {
        if is_assignment(arg) || matches!(arg.as_str(), "-i" | "-0" | "-v") {
            index += 1;
        } else if arg == "-u" {
            words.get(index + 1)?;
            index += 2;
        } else if arg.starts_with('-') {
            return None;
        } else {
            break;
        }
    }
    (index < words.len()).then_some(index)
}

fn xargs_inner_candidates(words: &[String]) -> DenyWordExpansion {
    if words.first().map(String::as_str) != Some("xargs") {
        return DenyWordExpansion::default();
    }
    const VALUE_SHORT: &[&str] = &["-E", "-I", "-L", "-P", "-a", "-d", "-n", "-s"];
    const VALUE_LONG: &[&str] = &[
        "--arg-file",
        "--delimiter",
        "--eof",
        "--max-args",
        "--max-chars",
        "--max-lines",
        "--max-procs",
        "--process-slot-var",
        "--replace",
    ];
    const FLAG_LONG: &[&str] = &[
        "--exit",
        "--interactive",
        "--no-run-if-empty",
        "--null",
        "--open-tty",
        "--show-limits",
        "--verbose",
    ];
    let mut index = 1usize;
    while let Some(arg) = words.get(index).map(String::as_str) {
        if arg == "--" {
            return inner_words_after(words, index + 1);
        }
        if VALUE_SHORT.contains(&arg) || VALUE_LONG.contains(&arg) {
            if words.get(index + 1).is_none() {
                return DenyWordExpansion::default();
            }
            index += 2;
            continue;
        }
        if VALUE_SHORT
            .iter()
            .any(|option| arg.starts_with(option) && arg.len() > option.len())
            || VALUE_LONG
                .iter()
                .any(|option| arg.starts_with(&format!("{option}=")))
            || matches!(arg, "-e" | "-i" | "-l")
            || ["-e", "-i", "-l"]
                .iter()
                .any(|option| arg.starts_with(option) && arg.len() > option.len())
            || FLAG_LONG.contains(&arg)
            || arg.starts_with('-')
                && arg.len() > 1
                && arg[1..]
                    .chars()
                    .all(|flag| matches!(flag, '0' | 'o' | 'p' | 'r' | 't' | 'x'))
        {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            return conservative_executable_suffixes(words, index + 1);
        }
        return inner_words_after(words, index);
    }
    DenyWordExpansion::default()
}

fn inner_words_after(words: &[String], index: usize) -> DenyWordExpansion {
    if index < words.len() {
        DenyWordExpansion {
            words: vec![words[index..].to_vec()],
            opaque: false,
        }
    } else {
        DenyWordExpansion::default()
    }
}

fn find_exec_inner(words: &[String]) -> Option<Vec<String>> {
    let start = words
        .iter()
        .position(|word| word == "-exec" || word == "-execdir")?
        + 1;
    let end = words[start..]
        .iter()
        .position(|word| word == ";" || word == "+")
        .map_or(words.len(), |offset| start + offset);
    (start < end).then(|| words[start..end].to_vec())
}

fn strip_shell_control_prefix(words: &[String]) -> Option<Vec<String>> {
    let first = words.first()?.as_str();
    let skip = match first {
        "if" | "then" | "elif" | "else" | "while" | "until" | "do" | "!" => 1,
        _ => return None,
    };
    (words.len() > skip).then(|| words[skip..].to_vec())
}

fn shell_code_argument(words: &[String]) -> Option<&str> {
    match words.first()?.as_str() {
        "bash" | "sh" | "zsh" | "dash" => words
            .iter()
            .position(|word| word == "-c")
            .and_then(|index| words.get(index + 1))
            .map(String::as_str),
        "eval" => words.get(1).map(String::as_str),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct ExecutedSubstitutions {
    commands: Vec<String>,
    opaque: bool,
}

const MAX_EXECUTED_SUBSTITUTIONS: usize = 128;
const MAX_SUBSTITUTION_NESTING: usize = 64;

/// Collects both command substitutions and the Bash process substitutions
/// introduced by less-than/greater-than followed immediately by an open
/// parenthesis. The latter execute their inner lists and therefore must be
/// visible to deny rules even though the outer lexer treats them as redirects.
fn extract_executed_substitutions(command: &str) -> ExecutedSubstitutions {
    let mut result = ExecutedSubstitutions {
        commands: extract_command_substitutions(command),
        opaque: false,
    };
    if result.commands.len() > MAX_EXECUTED_SUBSTITUTIONS {
        result.commands.truncate(MAX_EXECUTED_SUBSTITUTIONS);
        result.opaque = true;
        return result;
    }
    let process = extract_process_substitutions(command);
    result.opaque |= process.opaque;
    for nested in process.commands {
        if result.commands.len() >= MAX_EXECUTED_SUBSTITUTIONS {
            result.opaque = true;
            break;
        }
        push_unique(&mut result.commands, nested);
    }
    result
}

fn extract_process_substitutions(command: &str) -> ExecutedSubstitutions {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let chars = command.chars().collect::<Vec<_>>();
    let mut result = ExecutedSubstitutions::default();
    let mut quote = Quote::None;
    let mut index = 0usize;
    while index < chars.len() {
        match chars[index] {
            '\\' if quote != Quote::Single => {
                index = (index + 2).min(chars.len());
            }
            '\'' if quote != Quote::Double => {
                quote = if quote == Quote::Single {
                    Quote::None
                } else {
                    Quote::Single
                };
                index += 1;
            }
            '"' if quote != Quote::Single => {
                quote = if quote == Quote::Double {
                    Quote::None
                } else {
                    Quote::Double
                };
                index += 1;
            }
            '<' | '>' if quote == Quote::None && chars.get(index + 1) == Some(&'(') => {
                let start = index + 2;
                let mut cursor = start;
                let mut nesting = 1usize;
                let mut inner_quote = Quote::None;
                let mut closed = false;
                while cursor < chars.len() {
                    let current = chars[cursor];
                    if current == '\\' && inner_quote != Quote::Single {
                        cursor = (cursor + 2).min(chars.len());
                        continue;
                    }
                    if current == '\'' && inner_quote != Quote::Double {
                        inner_quote = if inner_quote == Quote::Single {
                            Quote::None
                        } else {
                            Quote::Single
                        };
                    } else if current == '"' && inner_quote != Quote::Single {
                        inner_quote = if inner_quote == Quote::Double {
                            Quote::None
                        } else {
                            Quote::Double
                        };
                    } else if inner_quote == Quote::None && current == '(' {
                        nesting += 1;
                        if nesting > MAX_SUBSTITUTION_NESTING {
                            result.opaque = true;
                            break;
                        }
                    } else if inner_quote == Quote::None && current == ')' {
                        nesting -= 1;
                        if nesting == 0 {
                            if result.commands.len() >= MAX_EXECUTED_SUBSTITUTIONS {
                                result.opaque = true;
                            } else {
                                result.commands.push(chars[start..cursor].iter().collect());
                            }
                            cursor += 1;
                            closed = true;
                            break;
                        }
                    }
                    cursor += 1;
                }
                if !closed {
                    result.opaque = true;
                    break;
                }
                index = cursor;
            }
            _ => index += 1,
        }
    }
    result
}

fn extract_command_substitutions(command: &str) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Double,
    }

    let chars = command.chars().collect::<Vec<_>>();
    let mut result = Vec::new();
    let mut quote = Quote::None;
    let mut index = 0usize;
    while index < chars.len() {
        match chars[index] {
            '\\' if quote != Quote::Single => {
                index = (index + 2).min(chars.len());
            }
            '\'' if quote != Quote::Double => {
                quote = if quote == Quote::Single {
                    Quote::None
                } else {
                    Quote::Single
                };
                index += 1;
            }
            '"' if quote != Quote::Single => {
                quote = if quote == Quote::Double {
                    Quote::None
                } else {
                    Quote::Double
                };
                index += 1;
            }
            '$' if quote != Quote::Single && chars.get(index + 1) == Some(&'(') => {
                if chars.get(index + 2) == Some(&'(') {
                    index += 2;
                    continue;
                }
                let start = index + 2;
                let mut cursor = start;
                let mut nesting = 1usize;
                let mut inner_quote = Quote::None;
                while cursor < chars.len() {
                    let current = chars[cursor];
                    if current == '\\' && inner_quote != Quote::Single {
                        cursor = (cursor + 2).min(chars.len());
                        continue;
                    }
                    if current == '\'' && inner_quote != Quote::Double {
                        inner_quote = if inner_quote == Quote::Single {
                            Quote::None
                        } else {
                            Quote::Single
                        };
                    } else if current == '"' && inner_quote != Quote::Single {
                        inner_quote = if inner_quote == Quote::Double {
                            Quote::None
                        } else {
                            Quote::Double
                        };
                    } else if inner_quote != Quote::Single
                        && current == '('
                        && chars.get(cursor.wrapping_sub(1)) == Some(&'$')
                    {
                        nesting += 1;
                    } else if inner_quote == Quote::None && current == ')' {
                        nesting -= 1;
                        if nesting == 0 {
                            result.push(chars[start..cursor].iter().collect());
                            cursor += 1;
                            break;
                        }
                    }
                    cursor += 1;
                }
                index = cursor;
            }
            '`' if quote != Quote::Single => {
                let start = index + 1;
                let mut cursor = start;
                while cursor < chars.len() {
                    if chars[cursor] == '\\' {
                        cursor = (cursor + 2).min(chars.len());
                    } else if chars[cursor] == '`' {
                        result.push(chars[start..cursor].iter().collect());
                        cursor += 1;
                        break;
                    } else {
                        cursor += 1;
                    }
                }
                index = cursor;
            }
            _ => index += 1,
        }
    }
    result
}

fn safe_wrapper_value(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '+' | '-'))
}

fn is_signed_integer(value: &str) -> bool {
    value
        .strip_prefix('-')
        .unwrap_or(value)
        .chars()
        .all(|ch| ch.is_ascii_digit())
        && value != "-"
        && !value.is_empty()
}

fn is_duration(value: &str) -> bool {
    let number = value.strip_suffix(['s', 'm', 'h', 'd']).unwrap_or(value);
    let mut dots = 0usize;
    !number.is_empty()
        && number.chars().all(|ch| {
            if ch == '.' {
                dots += 1;
                dots == 1
            } else {
                ch.is_ascii_digit()
            }
        })
}

fn prompt(tool: &str, summary: &str) -> Result<PermissionDecision> {
    match crate::terminal::request_permission(tool, summary)? {
        crate::terminal::PermissionChoice::Allow => Ok(PermissionDecision::Allow),
        crate::terminal::PermissionChoice::Deny => Ok(PermissionDecision::Deny),
        crate::terminal::PermissionChoice::Interrupt => Ok(PermissionDecision::Interrupt),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn dont_ask_setting_name_round_trips() {
        assert_eq!(
            PermissionMode::from_setting("dontAsk"),
            Some(PermissionMode::DontAsk)
        );
        assert_eq!(PermissionMode::DontAsk.as_setting(), "dontAsk");
        assert_eq!(PermissionMode::from_setting("dont-ask"), None);
    }

    #[test]
    fn dont_ask_allows_preapproved_and_safe_calls_without_prompting() {
        let prompts = Arc::new(AtomicUsize::new(0));
        let manager = PermissionManager::new(
            PermissionMode::DontAsk,
            true,
            vec!["Bash(git status)".into(), "Read(outside.txt)".into()],
            vec!["Bash(rm:*)".into()],
        );
        let prompt_count = Arc::clone(&prompts);
        manager.set_prompt_handler(Some(Arc::new(move |_| {
            prompt_count.fetch_add(1, Ordering::SeqCst);
            Ok(PermissionDecision::Allow)
        })));

        assert_eq!(
            manager
                .decide("Bash", "git status", false, false, false)
                .unwrap(),
            PermissionDecision::Allow,
            "an explicit allow rule remains effective"
        );
        assert_eq!(
            manager
                .decide("Read", "workspace.txt", true, false, false)
                .unwrap(),
            PermissionDecision::Allow,
            "safe workspace reads remain automatic"
        );
        assert_eq!(
            manager
                .decide("Read", "outside.txt", true, false, true)
                .unwrap(),
            PermissionDecision::Allow,
            "an explicit allow may preapprove an outside-workspace target"
        );
        assert_eq!(
            manager
                .decide("Bash", "rm -f output", false, true, false)
                .unwrap(),
            PermissionDecision::Deny,
            "deny rules still take precedence"
        );
        assert_eq!(prompts.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn dont_ask_converts_every_would_be_prompt_to_deny() {
        let prompts = Arc::new(AtomicUsize::new(0));
        let manager = PermissionManager::new(PermissionMode::DontAsk, true, Vec::new(), Vec::new());
        let prompt_count = Arc::clone(&prompts);
        manager.set_prompt_handler(Some(Arc::new(move |_| {
            prompt_count.fetch_add(1, Ordering::SeqCst);
            Ok(PermissionDecision::Allow)
        })));

        for (tool, summary, read_only, destructive, outside_workspace) in [
            ("Write", "workspace.txt", false, false, false),
            ("Bash", "cargo test", false, false, false),
            ("Read", "outside.txt", true, false, true),
            ("Read", "sensitive.txt", true, true, false),
        ] {
            assert_eq!(
                manager
                    .decide(tool, summary, read_only, destructive, outside_workspace,)
                    .unwrap(),
                PermissionDecision::Deny,
                "{tool} {summary:?} should fail closed"
            );
        }
        assert_eq!(
            prompts.load(Ordering::SeqCst),
            0,
            "dontAsk must never invoke a terminal or control permission handler"
        );
    }

    #[test]
    fn deny_precedes_allow() {
        let p = PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            vec!["Bash(*)".into()],
            vec!["Bash(rm *)".into()],
        );
        assert_eq!(
            p.decide("Bash", "rm x", false, true, false).unwrap(),
            PermissionDecision::Deny
        );
    }

    #[test]
    fn shell_prefix_rules_require_every_atomic_subcommand() {
        let manager = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Bash(git:*)".into()],
            Vec::new(),
        );
        assert_eq!(
            manager
                .decide("Bash", "git status", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(
            manager
                .decide("Bash", "git status && rm -rf build", false, true, false)
                .unwrap(),
            PermissionDecision::Deny
        );
        assert_eq!(
            manager
                .decide("Bash", "git status | git diff", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
    }

    #[test]
    fn shell_wildcard_and_legacy_prefix_have_word_boundaries() {
        for rule in ["Bash(git:*)", "Bash(git *)"] {
            let manager = PermissionManager::new(
                PermissionMode::Default,
                false,
                vec![rule.to_owned()],
                Vec::new(),
            );
            assert_eq!(
                manager.decide("Bash", "git", false, false, false).unwrap(),
                PermissionDecision::Allow,
                "{rule} should include the bare command"
            );
            assert_eq!(
                manager
                    .decide("Bash", "git status", false, false, false)
                    .unwrap(),
                PermissionDecision::Allow
            );
            assert_eq!(
                manager
                    .decide("Bash", "gitstatus", false, false, false)
                    .unwrap(),
                PermissionDecision::Deny
            );
        }
    }

    #[test]
    fn shell_denies_cover_compounds_wrappers_and_indirect_execution() {
        let manager = PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            vec!["Bash(rm:*)".into()],
        );
        for command in [
            "git status && rm -rf build",
            "git status | rm -f out",
            "nohup timeout 5 rm -f out",
            "FOO=a=b nice -n 5 rm -f out",
            "nohup FOO=a=b timeout 5 rm -f out",
            "xargs rm -f",
            "xargs -0 rm -f",
            r"find . -name '*.tmp' -exec rm -f {} \;",
            "echo $(rm -f out)",
            "cat <(rm -f out)",
            "tee >(rm -f out)",
            "sh -c 'rm -f out'",
            "command rm -f out",
            "exec rm -f out",
            "builtin rm -f out",
            "sudo -- rm -f out",
            "sudo -u root rm -f out",
            "sudo --future-option value rm -f out",
            "doas -u root rm -f out",
            "/bin/rm -f out",
            "command /bin/rm -f out",
            "/usr/bin/env /bin/rm -f out",
            "if true; then rm -f out; fi",
            "while false; do rm -f out; done",
        ] {
            assert_eq!(
                manager.decide("Bash", command, false, true, false).unwrap(),
                PermissionDecision::Deny,
                "deny rule was bypassed by {command:?}"
            );
        }
    }

    #[test]
    fn shell_denies_fail_closed_for_runtime_expanded_tokens_only() {
        let rm = PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            vec!["Bash(rm:*)".into()],
        );
        for command in [
            "cmd=rm; $cmd -f out",
            "$(printf rm) -f out",
            "`printf rm` -f out",
            "r* -f out",
            "{rm,echo} -f out",
            "sudo $cmd -f out",
            "cmd=rm; command $cmd -f out",
            "sh -c '$cmd -f out'",
        ] {
            assert_eq!(
                rm.decide("Bash", command, false, true, false).unwrap(),
                PermissionDecision::Deny,
                "dynamic token bypassed deny rule: {command:?}"
            );
        }

        let git_push = PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            vec!["Bash(git push:*)".into()],
        );
        for command in [
            "x=push; git $x origin",
            "git p* origin",
            "git {push,status} origin",
            "sh -c 'git $verb origin'",
        ] {
            assert_eq!(
                git_push
                    .decide("Bash", command, false, true, false)
                    .unwrap(),
                PermissionDecision::Deny,
                "dynamic subcommand bypassed deny rule: {command:?}"
            );
        }

        for literal in [
            r#"echo '$cmd $(printf rm) `printf rm` * {rm,ls}'"#,
            r"echo \$cmd \* \{rm,ls\}",
            "x=$HOME; echo ok",
            "x=$(printf rm); echo ok",
            "echo ok > static.out",
        ] {
            assert_eq!(
                rm.decide("Bash", literal, false, true, false).unwrap(),
                PermissionDecision::Allow,
                "literal/static syntax was incorrectly treated as opaque: {literal:?}"
            );
        }
        assert_eq!(
            git_push
                .decide("Bash", "git '{push,status}' origin", false, true, false,)
                .unwrap(),
            PermissionDecision::Allow
        );
    }

    #[test]
    fn shell_operators_inside_quotes_or_escapes_are_not_subcommands() {
        let manager = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Bash(printf:*)".into(), "Bash(git:*)".into()],
            Vec::new(),
        );
        assert_eq!(
            manager
                .decide("Bash", r#"printf '%s' '&& rm -rf /'"#, false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(
            manager
                .decide("Bash", r"git status \&\& rm", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );

        let exact_subcommands = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec![
                r#"Bash(printf '%s' 'a b')"#.into(),
                "Bash(git status)".into(),
            ],
            Vec::new(),
        );
        assert_eq!(
            exact_subcommands
                .decide(
                    "Bash",
                    r#"printf '%s' 'a b' && git status"#,
                    false,
                    false,
                    false,
                )
                .unwrap(),
            PermissionDecision::Allow
        );
    }

    #[test]
    fn only_non_execution_affecting_environment_prefixes_are_stripped_for_allow() {
        let manager = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Bash(git:*)".into()],
            Vec::new(),
        );
        assert_eq!(
            manager
                .decide("Bash", "RUST_LOG=debug git status", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(
            manager
                .decide(
                    "Bash",
                    "LD_PRELOAD=/tmp/inject.dylib git status",
                    false,
                    false,
                    false,
                )
                .unwrap(),
            PermissionDecision::Deny
        );
    }

    #[test]
    fn complex_shell_syntax_only_accepts_an_exact_rule() {
        let prefix = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Bash(echo:*)".into()],
            Vec::new(),
        );
        assert_eq!(
            prefix
                .decide("Bash", "echo $(git status)", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );
        let exact = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Bash(echo $(git status))".into()],
            Vec::new(),
        );
        assert_eq!(
            exact
                .decide("Bash", "echo $(git status)", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );

        let process_prefix = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Bash(cat:*)".into()],
            Vec::new(),
        );
        assert_eq!(
            process_prefix
                .decide("Bash", "cat <(echo ok)", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );
        assert_eq!(
            process_prefix
                .decide("Bash", "cat <(echo ok", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );
        let exact_process = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Bash(cat <(echo ok))".into()],
            Vec::new(),
        );
        assert_eq!(
            exact_process
                .decide("Bash", "cat <(echo ok)", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
    }

    #[test]
    fn filesystem_rules_match_normalized_case_insensitive_identities() {
        let manager = PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            vec!["Read(Secrets/**)".into()],
        );
        assert!(manager.denies_read_path(&["./secrets/../secrets/token.txt".into()]));
        assert!(manager.denies_read_path(&["SECRETS/token.txt".into()]));
        assert!(manager.denies_read_path(&["secrets".into()]));
        assert!(!manager.denies_read_path(&["public/token.txt".into()]));
    }

    #[test]
    fn session_plan_mode_cannot_override_a_user_lock() {
        let locked = PermissionManager::new(PermissionMode::Plan, false, vec![], vec![]);
        assert!(!locked.enter_plan_mode());
        assert!(locked.exit_plan_mode().is_err());
        assert_eq!(
            PermissionManager::new(
                PermissionMode::Plan,
                false,
                vec!["Bash(*)".to_owned()],
                vec![]
            )
            .decide("Bash", "echo unsafe", false, false, false)
            .unwrap(),
            PermissionDecision::Deny
        );

        let dynamic = PermissionManager::new(PermissionMode::Default, false, vec![], vec![]);
        assert!(dynamic.enter_plan_mode());
        assert_eq!(dynamic.effective_mode(), PermissionMode::Plan);
        assert!(dynamic.set_session_mode(PermissionMode::Default).is_err());
        assert!(dynamic.exit_plan_mode().unwrap());
        assert_eq!(dynamic.effective_mode(), PermissionMode::Default);
    }

    #[test]
    fn interactive_mode_changes_are_shared_across_clones() {
        let manager = PermissionManager::new(PermissionMode::Default, true, vec![], vec![]);
        let observer = manager.clone();
        assert!(
            manager
                .set_session_mode(PermissionMode::AcceptEdits)
                .unwrap()
        );
        assert_eq!(observer.effective_mode(), PermissionMode::AcceptEdits);
        assert!(
            !manager
                .set_session_mode(PermissionMode::AcceptEdits)
                .unwrap()
        );
    }

    #[test]
    fn headless_handler_is_used_but_cannot_override_deny_rules() {
        let manager = PermissionManager::new(
            PermissionMode::Default,
            false,
            Vec::new(),
            vec!["Write(blocked)".to_owned()],
        );
        manager.set_prompt_handler(Some(Arc::new(|request| {
            assert_eq!(request.tool, "Write");
            Ok(PermissionDecision::Allow)
        })));
        assert_eq!(
            manager
                .decide("Write", "allowed", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(
            manager
                .decide("Write", "blocked", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );

        let allow = PermissionManager::new(
            PermissionMode::Default,
            false,
            vec!["Read(foo)".into()],
            Vec::new(),
        );
        assert_eq!(
            allow.decide("Read", "foo", false, true, false).unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(
            allow.decide("Read", "FOO", false, true, false).unwrap(),
            PermissionDecision::Deny,
            "filesystem allow rules must not grant a differently cased identity"
        );
    }

    #[test]
    fn scoped_skill_allows_are_local_and_never_override_deny_or_plan() {
        let base = PermissionManager::new(
            PermissionMode::Default,
            false,
            Vec::new(),
            vec!["Bash(git push:*)".into()],
        );
        let scoped = base
            .with_scoped_allow(&["Bash(git:*)".into(), "Read".into()])
            .unwrap();
        assert_eq!(
            scoped
                .decide("Bash", "git status", false, false, false)
                .unwrap(),
            PermissionDecision::Allow
        );
        assert_eq!(
            scoped
                .decide("Bash", "git push origin main", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );
        assert_eq!(
            base.decide("Bash", "git status", false, false, false)
                .unwrap(),
            PermissionDecision::Deny,
            "scoped rule must not mutate the session permission manager"
        );

        let plan = PermissionManager::new(PermissionMode::Plan, false, vec![], vec![])
            .with_scoped_allow(&["Bash(git:*)".into()])
            .unwrap();
        assert_eq!(
            plan.decide("Bash", "git status", false, false, false)
                .unwrap(),
            PermissionDecision::Deny
        );

        let wildcard = base.with_scoped_allow(&["*".into()]).unwrap();
        assert_eq!(
            wildcard
                .decide("Bash", "cargo check", false, false, false)
                .unwrap(),
            PermissionDecision::Allow,
            "a trusted skill wildcard should preapprove any otherwise permitted tool"
        );
        assert_eq!(
            wildcard
                .decide("Bash", "git push origin main", false, false, false)
                .unwrap(),
            PermissionDecision::Deny,
            "a wildcard skill allow must not override a deny rule"
        );

        let plan_wildcard = PermissionManager::new(PermissionMode::Plan, false, vec![], vec![])
            .with_scoped_allow(&["*".into()])
            .unwrap();
        assert_eq!(
            plan_wildcard
                .decide("Bash", "cargo check", false, false, false)
                .unwrap(),
            PermissionDecision::Deny,
            "a wildcard skill allow must not override Plan mode"
        );
    }
}

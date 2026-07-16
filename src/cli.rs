use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

use crate::{
    permissions::PermissionMode,
    protocol::{ApiFormat, ChatTokensField},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    StreamJson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InputFormat {
    Text,
    StreamJson,
}

#[derive(Debug, Clone, Subcommand)]
pub enum HarnessCommand {
    /// Manage provider-neutral trusted plugins in the private user cache.
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
}

#[derive(Debug, Clone, Subcommand)]
pub enum PluginCommand {
    /// List plugins installed in the private user cache.
    List {
        /// Emit a stable JSON array.
        #[arg(long)]
        json: bool,
    },
    /// Validate a local directory, ZIP archive, or checksum-pinned HTTPS ZIP.
    Validate {
        source: String,
        /// Expected lowercase or uppercase SHA-256 digest.
        #[arg(long)]
        sha256: Option<String>,
    },
    /// Install a validated plugin into the private user cache.
    Install {
        source: String,
        /// Expected SHA-256 digest. Required for HTTPS sources.
        #[arg(long)]
        sha256: Option<String>,
    },
    /// Replace an installed plugin transactionally.
    Update {
        plugin_id: String,
        /// Replacement source. Defaults to the source recorded at install time.
        #[arg(long)]
        source: Option<String>,
        /// Expected SHA-256 digest. A new digest is required for changed HTTPS content.
        #[arg(long)]
        sha256: Option<String>,
    },
    /// Remove an installed plugin without following paths outside its cache slot.
    Uninstall { plugin_id: String },
}

#[derive(Debug, Parser)]
#[command(name = "open-agent-harness", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<HarnessCommand>,

    /// Prompt. If omitted in print mode, stdin is read to EOF.
    pub prompt: Option<String>,

    /// Print one response and exit.
    #[arg(short = 'p', long)]
    pub print: bool,

    /// Output format used with --print.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output_format: OutputFormat,

    /// Input format used with --print. stream-json accepts newline-delimited user and control messages.
    #[arg(long, value_enum, default_value_t = InputFormat::Text)]
    pub input_format: InputFormat,

    /// Require the final result to match this inline JSON Schema object.
    #[arg(long, requires = "print")]
    pub json_schema: Option<String>,

    /// Maximum model API round-trips for one user turn.
    #[arg(long, requires = "print")]
    pub max_turns: Option<usize>,

    /// Emit raw model stream events in stream-json mode.
    #[arg(long, requires = "print")]
    pub include_partial_messages: bool,

    /// Emit hook lifecycle events in stream-json mode.
    #[arg(long, requires = "print")]
    pub include_hook_events: bool,

    /// Re-emit accepted stream-json user messages on stdout as delivery acknowledgements.
    #[arg(long)]
    pub replay_user_messages: bool,

    /// After each completed turn, make one tool-free model request. Interactive mode displays
    /// an accept-on-Enter composer suggestion; print mode emits a stream-json event.
    #[arg(long, num_args = 0..=1, default_missing_value = "true")]
    pub prompt_suggestions: Option<bool>,

    /// Model alias or full API model identifier.
    #[arg(long)]
    pub model: Option<String>,

    /// Select the default or a namespaced trusted plugin output style.
    #[arg(long, value_name = "NAME")]
    pub output_style: Option<String>,

    /// Model endpoint wire format. Auto infers it from the API path.
    #[arg(long, value_enum)]
    pub api_format: Option<ApiFormat>,

    /// Token-limit field used by Chat Completions-compatible endpoints.
    #[arg(long, value_enum)]
    pub chat_tokens_field: Option<ChatTokensField>,

    /// Maximum output tokens for each API request.
    #[arg(long, default_value_t = 16_384)]
    pub max_tokens: u32,

    /// Replace the default system prompt.
    #[arg(long, conflicts_with = "system_prompt_file")]
    pub system_prompt: Option<String>,

    /// Read the replacement system prompt from a file.
    #[arg(long)]
    pub system_prompt_file: Option<PathBuf>,

    /// Append text to the system prompt.
    #[arg(long, conflicts_with = "append_system_prompt_file")]
    pub append_system_prompt: Option<String>,

    /// Read appended system prompt text from a file.
    #[arg(long)]
    pub append_system_prompt_file: Option<PathBuf>,

    /// Load an extra settings JSON file or inline JSON object.
    #[arg(long)]
    pub settings: Option<String>,

    /// Permission mode for tool execution.
    #[arg(long, value_enum)]
    pub permission_mode: Option<PermissionMode>,

    /// Skip interactive permission prompts. Explicit deny rules still apply.
    #[arg(long)]
    pub dangerously_skip_permissions: bool,

    /// Restrict the model-visible tool set (comma-separated, repeatable).
    #[arg(long, value_delimiter = ',')]
    pub tools: Option<Vec<String>>,

    /// Add trusted session permission allow rules (comma-separated, repeatable).
    #[arg(long, value_delimiter = ',')]
    pub allowed_tools: Vec<String>,

    /// Add trusted session permission deny rules (comma-separated, repeatable).
    #[arg(long, value_delimiter = ',')]
    pub disallowed_tools: Vec<String>,

    /// Add an existing directory to this session's trusted workspace scope.
    #[arg(long = "add-dir", value_name = "DIRECTORY")]
    pub add_dirs: Vec<PathBuf>,

    /// Continue the newest session for the current directory.
    #[arg(short = 'c', long)]
    pub r#continue: bool,

    /// Resume a session UUID.
    #[arg(short = 'r', long)]
    pub resume: Option<String>,

    /// Fork a session UUID into a new session without changing the source transcript.
    #[arg(long, conflicts_with_all = ["resume", "continue"])]
    pub fork_session: Option<String>,

    /// Resume/fork from the first N effective transcript messages. This always creates a fork.
    #[arg(long)]
    pub resume_at: Option<usize>,

    /// Do not write a session transcript.
    #[arg(long)]
    pub no_session_persistence: bool,

    /// Store session transcripts and file-history journals below this existing absolute directory.
    #[arg(
        long,
        value_name = "DIRECTORY",
        conflicts_with = "no_session_persistence"
    )]
    pub session_state_root: Option<PathBuf>,

    /// Disable project settings and automatic context discovery.
    #[arg(long)]
    pub bare: bool,

    /// Disable instructions, skills, plugins, hooks, MCP/LSP, custom commands/agents,
    /// output styles, workflows, memory, and other customizations while retaining
    /// model selection, built-in tools, permissions, and sandbox policy.
    #[arg(long)]
    pub safe_mode: bool,

    /// Enable diagnostic messages on stderr.
    #[arg(short = 'd', long)]
    pub debug: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_mode_accepts_dont_ask_cli_spellings() {
        for spelling in ["dont-ask", "dontAsk"] {
            let cli =
                Cli::try_parse_from(["open-agent-harness", "--permission-mode", spelling]).unwrap();
            assert_eq!(cli.permission_mode, Some(PermissionMode::DontAsk));
        }
    }

    #[test]
    fn parses_explicit_plugin_lifecycle_commands() {
        let cli = Cli::try_parse_from([
            "open-agent-harness",
            "plugin",
            "install",
            "plugin.zip",
            "--sha256",
            "00",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(HarnessCommand::Plugin {
                command: PluginCommand::Install { .. }
            })
        ));
    }

    #[test]
    fn parses_stream_user_replay_flag() {
        let cli = Cli::try_parse_from([
            "open-agent-harness",
            "--print",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--replay-user-messages",
        ])
        .unwrap();
        assert!(cli.replay_user_messages);
    }

    #[test]
    fn parses_safe_mode_independently_from_bare_mode() {
        let cli = Cli::try_parse_from(["open-agent-harness", "--safe-mode"]).unwrap();
        assert!(cli.safe_mode);
        assert!(!cli.bare);
    }

    #[test]
    fn parses_explicit_session_state_root() {
        let cli = Cli::try_parse_from([
            "open-agent-harness",
            "--session-state-root",
            "/isolated/session-state",
        ])
        .unwrap();
        assert_eq!(
            cli.session_state_root,
            Some(PathBuf::from("/isolated/session-state"))
        );
        assert!(
            Cli::try_parse_from([
                "open-agent-harness",
                "--no-session-persistence",
                "--session-state-root",
                "/isolated/session-state",
            ])
            .is_err()
        );
    }

    #[test]
    fn prompt_suggestions_accepts_implicit_and_explicit_boolean_values() {
        let enabled = Cli::try_parse_from(["open-agent-harness", "--prompt-suggestions"]).unwrap();
        assert_eq!(enabled.prompt_suggestions, Some(true));
        let disabled =
            Cli::try_parse_from(["open-agent-harness", "--prompt-suggestions=false"]).unwrap();
        assert_eq!(disabled.prompt_suggestions, Some(false));
    }
}

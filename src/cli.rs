use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};

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
    /// Generate a shell completion script without modifying shell configuration.
    Completion {
        #[arg(value_enum)]
        shell: CompletionShell,
        /// Write to a new file instead of stdout. Existing paths are never overwritten.
        #[arg(long, value_name = "FILE")]
        output: Option<PathBuf>,
    },
    /// Manage provider-neutral trusted plugins in the private user cache.
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Expose the provider-neutral local tool surface over MCP.
    Mcp {
        #[command(subcommand)]
        command: McpCommand,
    },
}

#[derive(Debug, Clone, Subcommand)]
pub enum McpCommand {
    /// Serve bounded newline-delimited MCP JSON-RPC on stdin/stdout.
    Serve(McpServeArgs),
}

#[derive(Debug, Clone, Args)]
pub struct McpServeArgs {
    /// Load an extra settings JSON file or inline JSON object.
    #[arg(long)]
    pub settings: Option<String>,

    /// Permission mode for tool execution. The server is always non-interactive.
    #[arg(long, value_enum)]
    pub permission_mode: Option<PermissionMode>,

    /// Allow permission-requiring calls without a prompt. Explicit deny rules still win.
    #[arg(long)]
    pub dangerously_skip_permissions: bool,

    /// Restrict the exposed tool set (comma-separated, repeatable).
    #[arg(long, value_delimiter = ',')]
    pub tools: Option<Vec<String>>,

    /// Add trusted server-scoped permission allow rules (comma-separated, repeatable).
    #[arg(long, value_delimiter = ',')]
    pub allowed_tools: Vec<String>,

    /// Add trusted server-scoped permission deny rules (comma-separated, repeatable).
    #[arg(long, value_delimiter = ',')]
    pub disallowed_tools: Vec<String>,

    /// Add an existing directory to the server's trusted workspace scope.
    #[arg(long = "add-dir", value_name = "DIRECTORY")]
    pub add_dirs: Vec<PathBuf>,

    /// Disable project settings and automatic context discovery.
    #[arg(long)]
    pub bare: bool,

    /// Disable instructions, skills, plugins, hooks, LSP, memory, and web extensions.
    #[arg(long)]
    pub safe_mode: bool,

    /// Enable bounded protocol diagnostics on stderr. Request arguments are never logged.
    #[arg(short = 'd', long)]
    pub debug: bool,
}

impl McpServeArgs {
    /// Accept the same trusted execution flags on either side of `mcp serve`.
    /// Scalar conflicts fail closed instead of silently choosing one scope.
    pub fn merge_parent_options(&mut self, parent: &Cli) -> Result<()> {
        if let Some(settings) = &parent.settings {
            if self
                .settings
                .as_ref()
                .is_some_and(|nested| nested != settings)
            {
                bail!("conflicting --settings values before and after mcp serve")
            }
            self.settings.get_or_insert_with(|| settings.clone());
        }
        if let Some(mode) = parent.permission_mode {
            if self.permission_mode.is_some_and(|nested| nested != mode) {
                bail!("conflicting --permission-mode values before and after mcp serve")
            }
            self.permission_mode.get_or_insert(mode);
        }
        if let Some(parent_tools) = &parent.tools {
            match &mut self.tools {
                Some(tools) => {
                    let mut merged = parent_tools.clone();
                    merged.append(tools);
                    *tools = merged;
                }
                None => self.tools = Some(parent_tools.clone()),
            }
        }
        prepend(&mut self.allowed_tools, &parent.allowed_tools);
        prepend(&mut self.disallowed_tools, &parent.disallowed_tools);
        prepend(&mut self.add_dirs, &parent.add_dirs);
        self.dangerously_skip_permissions |= parent.dangerously_skip_permissions;
        self.bare |= parent.bare;
        self.safe_mode |= parent.safe_mode;
        self.debug |= parent.debug;
        Ok(())
    }
}

fn prepend<T: Clone>(nested: &mut Vec<T>, parent: &[T]) {
    if parent.is_empty() {
        return;
    }
    let mut merged = parent.to_vec();
    merged.append(nested);
    *nested = merged;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Zsh,
    Fish,
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

    /// Use a trusted custom agent definition as the main conversation agent.
    #[arg(long, value_name = "NAME")]
    pub agent: Option<String>,

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
    fn parses_shell_completion_without_entering_the_harness_runtime() {
        let cli = Cli::try_parse_from([
            "open-agent-harness",
            "completion",
            "zsh",
            "--output",
            "completion.zsh",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Some(HarnessCommand::Completion {
                shell: CompletionShell::Zsh,
                output: Some(_),
            })
        ));
    }

    #[test]
    fn parses_mcp_serve_as_an_explicit_noninteractive_surface() {
        let cli = Cli::try_parse_from([
            "open-agent-harness",
            "mcp",
            "serve",
            "--bare",
            "--tools",
            "Read,Glob",
            "--allowed-tools",
            "Read(*)",
        ])
        .unwrap();
        let Some(HarnessCommand::Mcp {
            command: McpCommand::Serve(args),
        }) = cli.command
        else {
            panic!("expected mcp serve command")
        };
        assert!(args.bare);
        assert_eq!(args.tools, Some(vec!["Read".into(), "Glob".into()]));
        assert_eq!(args.allowed_tools, vec!["Read(*)"]);
    }

    #[test]
    fn mcp_serve_merges_flags_on_both_sides_of_the_subcommand() {
        let mut cli = Cli::try_parse_from([
            "open-agent-harness",
            "--debug",
            "--bare",
            "--tools",
            "Read",
            "--allowed-tools",
            "Read(*)",
            "mcp",
            "serve",
            "--safe-mode",
            "--tools",
            "Glob",
            "--disallowed-tools",
            "Write(*)",
        ])
        .unwrap();
        let Some(HarnessCommand::Mcp {
            command: McpCommand::Serve(mut args),
        }) = cli.command.take()
        else {
            panic!("expected mcp serve command")
        };
        args.merge_parent_options(&cli).unwrap();
        assert!(args.debug);
        assert!(args.bare);
        assert!(args.safe_mode);
        assert_eq!(args.tools, Some(vec!["Read".into(), "Glob".into()]));
        assert_eq!(args.allowed_tools, vec!["Read(*)"]);
        assert_eq!(args.disallowed_tools, vec!["Write(*)"]);
    }

    #[test]
    fn mcp_serve_rejects_conflicting_scalar_scopes() {
        let mut cli = Cli::try_parse_from([
            "open-agent-harness",
            "--permission-mode",
            "default",
            "mcp",
            "serve",
            "--permission-mode",
            "plan",
        ])
        .unwrap();
        let Some(HarnessCommand::Mcp {
            command: McpCommand::Serve(mut args),
        }) = cli.command.take()
        else {
            panic!("expected mcp serve command")
        };
        assert!(args.merge_parent_options(&cli).is_err());
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

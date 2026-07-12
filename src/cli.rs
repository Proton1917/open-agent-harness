use std::path::PathBuf;

use clap::{Parser, ValueEnum};

use crate::permissions::PermissionMode;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    StreamJson,
}

#[derive(Debug, Parser)]
#[command(name = "open-agent-harness", version, about)]
pub struct Cli {
    /// Prompt. If omitted in print mode, stdin is read to EOF.
    pub prompt: Option<String>,

    /// Print one response and exit.
    #[arg(short = 'p', long)]
    pub print: bool,

    /// Output format used with --print.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    pub output_format: OutputFormat,

    /// Model alias or full API model identifier.
    #[arg(long)]
    pub model: Option<String>,

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

    /// Skip all permission checks. This can modify the system without prompting.
    #[arg(long)]
    pub dangerously_skip_permissions: bool,

    /// Continue the newest session for the current directory.
    #[arg(short = 'c', long)]
    pub r#continue: bool,

    /// Resume a session UUID.
    #[arg(short = 'r', long)]
    pub resume: Option<String>,

    /// Do not write a session transcript.
    #[arg(long)]
    pub no_session_persistence: bool,

    /// Disable project settings and automatic context discovery.
    #[arg(long)]
    pub bare: bool,

    /// Enable diagnostic messages on stderr.
    #[arg(short = 'd', long)]
    pub debug: bool,
}

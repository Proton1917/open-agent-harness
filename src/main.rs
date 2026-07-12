use std::{
    io::{self, IsTerminal, Read, Write},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde_json::json;
use uuid::Uuid;

use open_agent_harness::{
    api::ModelClient,
    cli::{Cli, OutputFormat},
    commands::{self, CommandOutcome},
    config::{DEFAULT_MODEL, Settings, endpoint_config},
    context::{discover_agent_instructions, render_agent_instructions},
    permissions::{PermissionManager, PermissionMode},
    query::{QueryEngine, QueryOptions, TextDeltaSink, default_system_prompt},
    session::SessionStore,
    tools::{ToolContext, ToolRegistry},
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("Error: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let cwd = std::env::current_dir().context("无法确定当前目录")?;
    let settings = Settings::load(&cwd, cli.settings.as_deref(), cli.bare)?;
    settings.apply_environment();
    let model = cli
        .model
        .clone()
        .or_else(|| settings.model().map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_MODEL.to_owned());
    let mode = if cli.dangerously_skip_permissions {
        PermissionMode::BypassPermissions
    } else {
        cli.permission_mode
            .or_else(|| settings.permission_mode())
            .unwrap_or(PermissionMode::Default)
    };
    let permissions = PermissionManager::new(
        mode,
        !cli.print && io::stdin().is_terminal(),
        settings.allow_rules(),
        settings.deny_rules(),
    );
    let tool_context = ToolContext::new(cwd.clone(), permissions);
    let system = build_system_prompt(&cli, &cwd).await?;
    let (store, history) = open_session(&cli, &cwd)?;
    let text_delta_sink = output_sink(&cli, store.id);
    let client = ModelClient::new(endpoint_config())?;
    let mut engine = QueryEngine::new(
        client,
        ToolRegistry::default(),
        tool_context,
        QueryOptions {
            model,
            max_tokens: cli.max_tokens,
            system,
            messages: history,
            debug: cli.debug,
            text_delta_sink,
            compact_config: None,
        },
    );

    if cli.print {
        let prompt = print_prompt(&cli)?;
        let result = engine.run_turn(prompt).await?;
        persist_turn(&store, &engine, &result)?;
        print_result(&cli, &engine, &store, &result.text, result.streamed_text)?;
        return Ok(());
    }

    println!(
        "open-agent-harness · {} · session {}",
        engine.model, store.id
    );
    let mut initial = cli.prompt.clone();
    loop {
        let input = match initial.take() {
            Some(prompt) => prompt,
            None => read_prompt()?,
        };
        if input.trim().is_empty() {
            continue;
        }
        if let Some(instructions) = compact_command(input.trim()) {
            match engine.compact(instructions).await {
                Ok(stats) => {
                    store.replace_history(&engine.messages)?;
                    println!(
                        "Compacted {} messages to {} (estimated tokens: {} → {}).",
                        stats.messages_before,
                        stats.messages_after,
                        stats.before_tokens,
                        stats.after_tokens
                    );
                }
                Err(error) => eprintln!("Compact failed: {error:#}"),
            }
            continue;
        }
        match commands::handle(input.trim(), &mut engine, mode) {
            CommandOutcome::Exit => break,
            CommandOutcome::Handled => continue,
            CommandOutcome::NotCommand => {}
        }
        match engine.run_turn(input).await {
            Ok(result) => {
                persist_turn(&store, &engine, &result)?;
                if result.streamed_text {
                    println!("\n");
                } else {
                    println!("\n{}\n", result.text);
                }
            }
            Err(error) => eprintln!("Error: {error:#}"),
        }
    }
    Ok(())
}

fn persist_turn(
    store: &SessionStore,
    engine: &QueryEngine,
    result: &open_agent_harness::query::TurnResult,
) -> Result<()> {
    if result.compacted {
        store.replace_history(&engine.messages)
    } else {
        store.append(&result.new_messages)
    }
}

fn compact_command(input: &str) -> Option<Option<&str>> {
    if input == "/compact" {
        return Some(None);
    }
    input
        .strip_prefix("/compact ")
        .map(str::trim)
        .map(|instructions| (!instructions.is_empty()).then_some(instructions))
}

fn open_session(
    cli: &Cli,
    cwd: &std::path::Path,
) -> Result<(SessionStore, Vec<open_agent_harness::types::Message>)> {
    let enabled = !cli.no_session_persistence;
    if cli.r#continue && cli.resume.is_some() {
        bail!("--continue 与 --resume 不能同时使用")
    }
    if cli.r#continue {
        return SessionStore::continue_latest(cwd, enabled);
    }
    if let Some(id) = &cli.resume {
        return SessionStore::resume(
            cwd,
            id.parse::<Uuid>().context("--resume 必须是 UUID")?,
            enabled,
        );
    }
    Ok((SessionStore::create(cwd, enabled)?, Vec::new()))
}

async fn build_system_prompt(cli: &Cli, cwd: &std::path::Path) -> Result<String> {
    let mut system = if let Some(prompt) = &cli.system_prompt {
        prompt.clone()
    } else if let Some(path) = &cli.system_prompt_file {
        tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("无法读取 {}", path.display()))?
    } else {
        default_system_prompt(cwd)
    };
    let append = if let Some(prompt) = &cli.append_system_prompt {
        Some(prompt.clone())
    } else if let Some(path) = &cli.append_system_prompt_file {
        Some(
            tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("无法读取 {}", path.display()))?,
        )
    } else {
        None
    };
    if let Some(append) = append {
        system.push_str("\n\n");
        system.push_str(&append);
    }
    let instructions = discover_agent_instructions(cwd, cli.bare).await?;
    let rendered = render_agent_instructions(&instructions);
    if !rendered.is_empty() {
        system.push_str("\n\n");
        system.push_str(&rendered);
    }
    Ok(system)
}

fn print_prompt(cli: &Cli) -> Result<String> {
    if let Some(prompt) = &cli.prompt {
        return Ok(prompt.clone());
    }
    let mut prompt = String::new();
    io::stdin().read_to_string(&mut prompt)?;
    if prompt.trim().is_empty() {
        bail!("print 模式需要 positional prompt 或 stdin")
    }
    Ok(prompt)
}

fn read_prompt() -> Result<String> {
    print!("> ");
    io::stdout().flush()?;
    let mut input = String::new();
    if io::stdin().read_line(&mut input)? == 0 {
        return Ok("/exit".into());
    }
    Ok(input.trim_end().to_owned())
}

fn output_sink(cli: &Cli, session_id: Uuid) -> Option<TextDeltaSink> {
    match (cli.print, cli.output_format) {
        (true, OutputFormat::Json) => None,
        (true, OutputFormat::StreamJson) => Some(Arc::new(move |delta| {
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "type": "content_block_delta",
                    "delta": {"type": "text_delta", "text": delta},
                    "session_id": session_id,
                }))
                .expect("serializing a text delta cannot fail")
            );
        })),
        _ => Some(Arc::new(|delta| {
            print!("{delta}");
            let _ = io::stdout().flush();
        })),
    }
}

fn print_result(
    cli: &Cli,
    engine: &QueryEngine,
    store: &SessionStore,
    text: &str,
    streamed_text: bool,
) -> Result<()> {
    match cli.output_format {
        OutputFormat::Text if streamed_text => println!(),
        OutputFormat::Text => println!("{text}"),
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string(&json!({
                "type": "result", "subtype": "success", "result": text,
                "session_id": store.id, "model": engine.model, "usage": engine.usage,
            }))?
        ),
        OutputFormat::StreamJson => {
            println!(
                "{}",
                serde_json::to_string(
                    &json!({"type": "assistant", "message": {"role": "assistant", "content": text}, "session_id": store.id})
                )?
            );
            println!(
                "{}",
                serde_json::to_string(
                    &json!({"type": "result", "subtype": "success", "result": text, "session_id": store.id, "usage": engine.usage})
                )?
            );
        }
    }
    Ok(())
}

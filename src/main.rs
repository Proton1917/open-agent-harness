use std::{
    io::{self, BufRead, IsTerminal, Read, Write},
    path::PathBuf,
    sync::Arc,
};

const MAX_USER_INPUT_BYTES: usize = 1024 * 1024;
const MAX_SYSTEM_FILE_BYTES: u64 = 1024 * 1024;
const MAX_SYSTEM_CONTEXT_BYTES: usize = 4 * 1024 * 1024;

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde_json::json;
use tokio::io::AsyncReadExt as _;
use uuid::Uuid;

use open_agent_harness::{
    agents::configure_agents,
    api::ModelClient,
    cli::{Cli, OutputFormat},
    commands::{self, CommandOutcome},
    config::{DEFAULT_MODEL, EndpointConfig, Settings, endpoint_config},
    hooks::HookRunner,
    lsp::configure_lsp,
    mcp::connect_mcp,
    permissions::{PermissionManager, PermissionMode},
    plan::plan_tools,
    prompt::default_system_prompt,
    query::{QueryEngine, QueryEvent, QueryEventSink, QueryOptions, TextDeltaSink},
    session::SessionStore,
    terminal::{ConversationUi, InputEditor},
    tools::{ToolContext, ToolRegistry},
    web_tools::configure_web,
    worktree::configure_worktree,
};

fn main() {
    if let Err(error) = bootstrap() {
        if error.downcast_ref::<CliInterrupted>().is_some() {
            std::process::exit(130);
        } else {
            eprintln!("Error: {error:#}");
            std::process::exit(1);
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("turn interrupted by user")]
struct CliInterrupted;

fn bootstrap() -> Result<()> {
    let cli = Cli::parse();
    let cwd = std::env::current_dir().context("无法确定当前目录")?;
    let mut settings = Settings::load(&cwd, cli.settings.as_deref(), cli.bare)?;
    // SAFETY: bootstrap is still single-threaded; the async runtime is created below.
    unsafe { settings.apply_environment() };
    let mut endpoint = endpoint_config()?;
    if let Some(api_format) = cli.api_format {
        endpoint.api_format = api_format;
    }
    if let Some(chat_tokens_field) = cli.chat_tokens_field {
        endpoint.chat_tokens_field = chat_tokens_field;
    }
    // SAFETY: bootstrap is still single-threaded. Keep endpoint credentials only in memory so
    // subprocess tools cannot inherit them after the runtime starts.
    unsafe {
        std::env::remove_var("HARNESS_API_KEY");
        std::env::remove_var("HARNESS_AUTH_TOKEN");
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("无法创建 async runtime")?;
    runtime.block_on(run(cli, cwd, settings, endpoint))
}

async fn run(cli: Cli, cwd: PathBuf, settings: Settings, endpoint: EndpointConfig) -> Result<()> {
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
    let mut tool_context = ToolContext::new(cwd.clone(), permissions);
    tool_context.set_bare(cli.bare);
    let hooks = Arc::new(HookRunner::from_settings(&settings)?);
    tool_context.set_hooks(Arc::clone(&hooks));
    let agents = configure_agents(&settings)?;
    tool_context.set_agent_limits(agents.limits);
    let mut active_tools = Vec::new();
    let mut deferred_tools = Vec::new();
    let mut services = Vec::new();
    let mut discoveries = Vec::new();
    deferred_tools.extend(agents.deferred_tools);
    deferred_tools.extend(plan_tools());
    if let Some(integration) = connect_mcp(&settings, &cwd, cli.debug).await? {
        if cli.debug {
            eprintln!(
                "[debug] connected {} MCP server(s), {} deferred tool(s)",
                integration.server_count,
                integration.deferred_tools.len()
            );
        }
        active_tools.extend(integration.active_tools);
        deferred_tools.extend(integration.deferred_tools);
        services.push(integration.service);
        discoveries.push(integration.discovery);
    }
    if let Some(integration) = configure_lsp(&settings, &cwd, cli.debug)? {
        if cli.debug {
            eprintln!(
                "[debug] configured {} lazy LSP server(s)",
                integration.server_count
            );
        }
        deferred_tools.extend(integration.deferred_tools);
        services.push(integration.service);
    }
    deferred_tools.extend(configure_worktree(&settings, &cwd)?.deferred_tools);
    deferred_tools.extend(configure_web(&settings)?.deferred_tools);
    let registry =
        ToolRegistry::with_integrations(active_tools, deferred_tools, services, discoveries)?;
    let (store, history) = open_session(&cli, &cwd)?;
    let mut system = build_base_system_prompt(&cli).await?;
    let session_start = hooks
        .run(
            "SessionStart",
            None,
            json!({"session_id": store.id, "model": &model}),
            &cwd,
        )
        .await?;
    if !session_start.additional_context.is_empty() {
        system.push_str("\n\n<session-start-hook-context>\n");
        system.push_str(&session_start.additional_context.join("\n"));
        system.push_str("\n</session-start-hook-context>");
    }
    if system.len() > MAX_SYSTEM_CONTEXT_BYTES {
        bail!("base system context 超过 {MAX_SYSTEM_CONTEXT_BYTES} 字节限制")
    }
    tool_context.set_workspace_context_budget(MAX_SYSTEM_CONTEXT_BYTES - system.len());
    tool_context.reload_workspace_context().await?;
    let ui = ConversationUi::detect();
    let enhanced_terminal = !cli.print && ui.interactive();
    let text_delta_sink = output_sink(&cli, store.id, enhanced_terminal.then(|| ui.clone()));
    let client = ModelClient::new(endpoint)?;
    let mut engine = QueryEngine::new(
        client,
        registry,
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
    if enhanced_terminal {
        let event_ui = ui.clone();
        let event_sink: QueryEventSink = Arc::new(move |event| event_ui.event(event));
        engine.set_event_sink(Some(event_sink));
    }

    if cli.print {
        let prompt = print_prompt(&cli)?;
        let Some(result) = engine.run_turn_interruptible(prompt).await? else {
            run_session_end_hook(&hooks, store.id, &cwd, "print_interrupted", cli.debug).await;
            engine.shutdown().await;
            return Err(CliInterrupted.into());
        };
        persist_turn(&store, &engine, &result)?;
        print_result(&cli, &engine, &store, &result.text, result.streamed_text)?;
        run_session_end_hook(&hooks, store.id, &cwd, "print_complete", cli.debug).await;
        engine.shutdown().await;
        return Ok(());
    }

    if enhanced_terminal {
        ui.banner(&engine.model, &cwd, store.id, engine.permission_mode())?;
    } else {
        println!(
            "open-agent-harness · {} · session {}",
            engine.model, store.id
        );
    }
    let mut initial = cli.prompt.clone();
    let mut editor = InputEditor::default();
    loop {
        let input = match initial.take() {
            Some(prompt) => prompt,
            None if enhanced_terminal => {
                let Some(read) =
                    editor.read(engine.permission_mode(), engine.permission_mode_locked())?
                else {
                    break;
                };
                if let Err(error) = engine.set_permission_mode(read.permission_mode) {
                    eprintln!("Mode unchanged: {error:#}");
                }
                read.text
            }
            None => read_prompt()?,
        };
        if input.len() > MAX_USER_INPUT_BYTES {
            bail!("prompt 超过 {MAX_USER_INPUT_BYTES} 字节限制")
        }
        if input.trim().is_empty() {
            continue;
        }
        if let Some(instructions) = compact_command(input.trim()) {
            match engine.compact(instructions).await {
                Ok(stats) => {
                    store.replace_history(&engine.messages)?;
                    if !enhanced_terminal {
                        println!(
                            "Compacted {} messages to {} (estimated tokens: {} → {}).",
                            stats.messages_before,
                            stats.messages_after,
                            stats.before_tokens,
                            stats.after_tokens
                        );
                    }
                }
                Err(error) if enhanced_terminal => ui.event(&QueryEvent::TurnFailed {
                    message: format!("Compact failed: {error:#}"),
                }),
                Err(error) => eprintln!("Compact failed: {error:#}"),
            }
            continue;
        }
        let input = match commands::handle(input.trim(), &mut engine) {
            CommandOutcome::Exit => break,
            CommandOutcome::Cleared => {
                store.clear_history()?;
                continue;
            }
            CommandOutcome::Handled => continue,
            CommandOutcome::Submit(prompt) => prompt,
            CommandOutcome::NotCommand => input,
        };
        let turn = engine.run_turn_interruptible(input).await;
        match turn {
            Ok(Some(result)) => {
                persist_turn(&store, &engine, &result)?;
                if enhanced_terminal {
                    if !result.streamed_text {
                        ui.response(&result.text)?;
                    }
                } else if result.streamed_text {
                    println!("\n");
                } else {
                    println!("\n{}\n", result.text);
                }
            }
            Ok(None) => continue,
            Err(error) if !enhanced_terminal => eprintln!("Error: {error:#}"),
            Err(_) => {}
        }
    }
    run_session_end_hook(&hooks, store.id, &cwd, "interactive_exit", cli.debug).await;
    engine.shutdown().await;
    Ok(())
}

async fn run_session_end_hook(
    hooks: &HookRunner,
    session_id: Uuid,
    cwd: &std::path::Path,
    reason: &str,
    debug: bool,
) {
    let outcome = hooks
        .run(
            "SessionEnd",
            None,
            json!({"session_id": session_id, "reason": reason}),
            cwd,
        )
        .await;
    if let (true, Err(error)) = (debug, outcome) {
        eprintln!("[debug] SessionEnd hook failed: {error:#}");
    }
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

async fn build_base_system_prompt(cli: &Cli) -> Result<String> {
    let mut system = if let Some(prompt) = &cli.system_prompt {
        prompt.clone()
    } else if let Some(path) = &cli.system_prompt_file {
        read_system_file(path).await?
    } else {
        default_system_prompt()
    };
    let append = if let Some(prompt) = &cli.append_system_prompt {
        Some(prompt.clone())
    } else if let Some(path) = &cli.append_system_prompt_file {
        Some(read_system_file(path).await?)
    } else {
        None
    };
    if let Some(append) = append {
        system.push_str("\n\n");
        system.push_str(&append);
    }
    if system.len() > MAX_SYSTEM_CONTEXT_BYTES {
        bail!("base system context 超过 {MAX_SYSTEM_CONTEXT_BYTES} 字节限制")
    }
    Ok(system)
}

async fn read_system_file(path: &std::path::Path) -> Result<String> {
    let size = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("无法检查 {}", path.display()))?
        .len();
    if size > MAX_SYSTEM_FILE_BYTES {
        bail!("system prompt 文件超过 {MAX_SYSTEM_FILE_BYTES} 字节限制")
    }
    let mut bytes = Vec::new();
    tokio::fs::File::open(path)
        .await
        .with_context(|| format!("无法打开 {}", path.display()))?
        .take(MAX_SYSTEM_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > MAX_SYSTEM_FILE_BYTES as usize {
        bail!("system prompt 文件超过 {MAX_SYSTEM_FILE_BYTES} 字节限制")
    }
    String::from_utf8(bytes).with_context(|| format!("{} 不是有效 UTF-8", path.display()))
}

fn print_prompt(cli: &Cli) -> Result<String> {
    if let Some(prompt) = &cli.prompt {
        if prompt.len() > MAX_USER_INPUT_BYTES {
            bail!("prompt 超过 {MAX_USER_INPUT_BYTES} 字节限制")
        }
        return Ok(prompt.clone());
    }
    let mut prompt = String::new();
    io::stdin()
        .take((MAX_USER_INPUT_BYTES + 1) as u64)
        .read_to_string(&mut prompt)?;
    if prompt.len() > MAX_USER_INPUT_BYTES {
        bail!("stdin prompt 超过 {MAX_USER_INPUT_BYTES} 字节限制")
    }
    if prompt.trim().is_empty() {
        bail!("print 模式需要 positional prompt 或 stdin")
    }
    Ok(prompt)
}

fn read_prompt() -> Result<String> {
    print!("> ");
    io::stdout().flush()?;
    let mut input = String::new();
    let stdin = io::stdin();
    let mut limited = stdin.lock().take((MAX_USER_INPUT_BYTES + 1) as u64);
    if limited.read_line(&mut input)? == 0 {
        return Ok("/exit".into());
    }
    if input.len() > MAX_USER_INPUT_BYTES {
        bail!("interactive prompt 超过 {MAX_USER_INPUT_BYTES} 字节限制")
    }
    Ok(input.trim_end().to_owned())
}

fn output_sink(
    cli: &Cli,
    session_id: Uuid,
    interactive_ui: Option<ConversationUi>,
) -> Option<TextDeltaSink> {
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
        _ if interactive_ui.is_some() => {
            let ui = interactive_ui.expect("interactive UI was checked above");
            Some(Arc::new(move |delta| ui.text_delta(delta)))
        }
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

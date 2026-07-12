use crate::{permissions::PermissionMode, query::QueryEngine};

pub enum CommandOutcome {
    Handled,
    Cleared,
    Exit,
    NotCommand,
}

pub fn handle(input: &str, engine: &mut QueryEngine, mode: PermissionMode) -> CommandOutcome {
    if !input.starts_with('/') {
        return CommandOutcome::NotCommand;
    }
    let (command, argument) = input.split_once(' ').unwrap_or((input, ""));
    match command {
        "/exit" | "/quit" => CommandOutcome::Exit,
        "/clear" => {
            engine.clear();
            println!("Conversation cleared.");
            CommandOutcome::Cleared
        }
        "/model" if argument.trim().is_empty() => {
            println!("{}", engine.model);
            CommandOutcome::Handled
        }
        "/model" => {
            engine.model = argument.trim().to_owned();
            println!("Model: {}", engine.model);
            CommandOutcome::Handled
        }
        "/cost" => {
            println!(
                "input={} output={} cache_create={} cache_read={}",
                engine.usage.input_tokens,
                engine.usage.output_tokens,
                engine.usage.cache_creation_input_tokens,
                engine.usage.cache_read_input_tokens
            );
            CommandOutcome::Handled
        }
        "/permissions" => {
            println!("Permission mode: {mode:?}");
            CommandOutcome::Handled
        }
        "/context" => {
            let (used, auto_threshold, effective_window) = engine.context_status();
            println!(
                "estimated={} auto_compact={} effective_window={}",
                used, auto_threshold, effective_window
            );
            CommandOutcome::Handled
        }
        "/help" => {
            println!(
                "/help  /model [name]  /cost  /context  /compact [instructions]  /permissions  /clear  /exit"
            );
            CommandOutcome::Handled
        }
        _ => {
            eprintln!("Unknown command: {command}");
            CommandOutcome::Handled
        }
    }
}

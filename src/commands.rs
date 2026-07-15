use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::{
    config::{Settings, validate_model_id},
    prompt::init_prompt,
    query::QueryEngine,
    skills::SkillCatalog,
};

const MAX_CUSTOM_COMMANDS: usize = 128;
const MAX_COMMAND_NAME_BYTES: usize = 64;
const MAX_COMMAND_DESCRIPTION_BYTES: usize = 1024;
const MAX_COMMAND_PROMPT_BYTES: usize = 128 * 1024;
const MAX_COMMAND_ARGUMENT_BYTES: usize = 32 * 1024;
const MAX_RENDERED_COMMAND_BYTES: usize = 192 * 1024;
const RESERVED_COMMANDS: &[&str] = &[
    "exit",
    "quit",
    "clear",
    "reset",
    "new",
    "model",
    "cost",
    "permissions",
    "context",
    "compact",
    "init",
    "loop",
    "status",
    "vim",
    "keybindings",
    "config",
    "theme",
    "statusline",
    "tui",
    "copy",
    "export",
    "tasks",
    "bashes",
    "transcript",
    "diff",
    "checkpoint",
    "rewind",
    "resume",
    "continue",
    "skills",
    "hooks",
    "memory",
    "mcp",
    "sandbox",
    "plugin",
    "help",
];

const DEFAULT_LOOP_MINUTES: u64 = 10;
const MAX_LOOP_MINUTES: u64 = 30 * 24 * 60;
const MODEL_HELP_ARGS: &[&str] = &["help", "-h", "--help"];
const MODEL_INFO_ARGS: &[&str] = &[
    "list", "show", "display", "current", "view", "get", "check", "describe", "print", "version",
    "about", "status", "?",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoopRequest {
    pub cron: String,
    pub prompt: String,
    pub requested_interval: String,
    pub effective_interval: String,
    pub rounded: bool,
}

#[derive(Debug, Clone)]
pub struct CustomCommandDefinition {
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub source: String,
}

#[derive(Debug, Clone, Default)]
pub struct CustomCommandCatalog {
    entries: BTreeMap<String, CustomCommandDefinition>,
}

impl CustomCommandCatalog {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let Some(commands) = settings.raw.get("commands") else {
            return Ok(Self::default());
        };
        let commands = commands
            .as_object()
            .context("trusted settings commands 必须是 object")?;
        let mut catalog = Self::default();
        for (name, value) in commands {
            let (description, prompt) = parse_settings_command(name, value)?;
            catalog.insert(CustomCommandDefinition {
                name: name.clone(),
                description,
                prompt,
                source: "trusted settings".into(),
            })?;
        }
        Ok(catalog)
    }

    pub fn insert(&mut self, definition: CustomCommandDefinition) -> Result<()> {
        validate_command_name(&definition.name)?;
        if RESERVED_COMMANDS.contains(&definition.name.as_str()) {
            bail!("custom command 不能覆盖内置命令: /{}", definition.name)
        }
        if definition.description.len() > MAX_COMMAND_DESCRIPTION_BYTES {
            bail!(
                "custom command /{} description 超过 {MAX_COMMAND_DESCRIPTION_BYTES} 字节限制",
                definition.name
            )
        }
        if definition.prompt.trim().is_empty()
            || definition.prompt.len() > MAX_COMMAND_PROMPT_BYTES
            || definition.prompt.contains('\0')
        {
            bail!(
                "custom command /{} prompt 为空、过长或包含 NUL",
                definition.name
            )
        }
        if !self.entries.contains_key(&definition.name) && self.entries.len() >= MAX_CUSTOM_COMMANDS
        {
            bail!("custom command 数量超过 {MAX_CUSTOM_COMMANDS} 个限制")
        }
        self.entries.insert(definition.name.clone(), definition);
        Ok(())
    }

    pub fn merge(&mut self, incoming: Self) -> Result<()> {
        for (_, definition) in incoming.entries {
            self.insert(definition)?;
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&CustomCommandDefinition> {
        self.entries.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &CustomCommandDefinition)> {
        self.entries.iter()
    }

    pub fn render(&self, name: &str, arguments: &str) -> Result<String> {
        let definition = self
            .get(name)
            .with_context(|| format!("未知 custom command: /{name}"))?;
        if arguments.len() > MAX_COMMAND_ARGUMENT_BYTES {
            bail!("custom command arguments 超过 {MAX_COMMAND_ARGUMENT_BYTES} 字节限制")
        }
        let arguments = arguments.trim();
        let rendered = if definition.prompt.contains("$ARGUMENTS") {
            definition.prompt.replace("$ARGUMENTS", arguments)
        } else if arguments.is_empty() {
            definition.prompt.clone()
        } else {
            format!(
                "{}\n\n<command-arguments>\n{}\n</command-arguments>",
                definition.prompt, arguments
            )
        };
        if rendered.len() > MAX_RENDERED_COMMAND_BYTES {
            bail!("custom command 展开后超过 {MAX_RENDERED_COMMAND_BYTES} 字节限制")
        }
        Ok(rendered)
    }
}

/// Resolves only extension commands. Built-in commands remain owned by
/// `handle`; callers should invoke this before falling back to `handle` so an
/// unknown slash command can be offered to skills and trusted custom commands.
pub fn resolve_extension_submission(
    input: &str,
    skills: &SkillCatalog,
    commands: &CustomCommandCatalog,
) -> Result<Option<String>> {
    let Some(rest) = input.strip_prefix('/') else {
        return Ok(None);
    };
    let split = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let name = &rest[..split];
    if name.is_empty() || RESERVED_COMMANDS.contains(&name) {
        return Ok(None);
    }
    let arguments = rest[split..].trim_start();
    if skills.get(name).is_some() {
        return skills.render_invocation(name, arguments).map(Some);
    }
    if commands.get(name).is_some() {
        return commands.render(name, arguments).map(Some);
    }
    Ok(None)
}

fn parse_settings_command(name: &str, value: &Value) -> Result<(String, String)> {
    match value {
        Value::String(prompt) => Ok((format!("Custom command /{name}"), prompt.clone())),
        Value::Object(object) => {
            let allowed = ["description", "prompt"];
            if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
                bail!("custom command /{name} 包含未知字段 {key}")
            }
            let prompt = object
                .get("prompt")
                .and_then(Value::as_str)
                .with_context(|| format!("custom command /{name} 缺少 string prompt"))?;
            let description = object
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("Trusted custom command");
            Ok((description.to_owned(), prompt.to_owned()))
        }
        _ => bail!("custom command /{name} 必须是 string 或 object"),
    }
}

pub(crate) fn validate_command_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > MAX_COMMAND_NAME_BYTES
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_-:".contains(character))
    {
        bail!("无效 custom command name: {name}")
    }
    Ok(())
}

pub enum CommandOutcome {
    Handled,
    Clear(String),
    Exit,
    SelectModel,
    ShowHelp,
    ShowStatus,
    ToggleVim,
    ConfigureKeybindings,
    ConfigureUi(String),
    ConfigureTheme(String),
    ConfigureStatusLine(String),
    ConfigureTui(String),
    CopyResponse(String),
    ExportConversation(String),
    ShowTasks(String),
    ShowTranscript,
    ShowDiff(String),
    Rewind(String),
    Resume(String),
    ShowSkills,
    ShowHooks,
    ShowMemory,
    ManageMcp(String),
    ShowSandbox,
    ShowPlugins,
    Submit(String),
    NotCommand,
}

pub fn handle(input: &str, engine: &mut QueryEngine) -> CommandOutcome {
    if !input.starts_with('/') {
        return CommandOutcome::NotCommand;
    }
    let split = input.find(char::is_whitespace).unwrap_or(input.len());
    let command = &input[..split];
    let argument = input[split..].trim();
    match command {
        "/exit" | "/quit" => CommandOutcome::Exit,
        "/clear" | "/reset" | "/new" => CommandOutcome::Clear(argument.to_owned()),
        "/model" if argument.is_empty() => CommandOutcome::SelectModel,
        "/model" if MODEL_INFO_ARGS.contains(&argument) => {
            println!("Current model: {}", engine.model);
            CommandOutcome::Handled
        }
        "/model" if MODEL_HELP_ARGS.contains(&argument) => {
            println!(
                "Run /model to open the model selection menu, or /model [modelName] to set the model."
            );
            CommandOutcome::Handled
        }
        "/model" => match validate_model_id(argument) {
            Ok(()) => {
                engine.set_model(argument.to_owned());
                println!("Set model to {}", engine.model);
                CommandOutcome::Handled
            }
            Err(error) => {
                eprintln!("Model unchanged: {error:#}");
                CommandOutcome::Handled
            }
        },
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
            println!("Permission mode: {:?}", engine.permission_mode());
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
        "/init" => CommandOutcome::Submit(init_prompt().to_owned()),
        "/loop" => {
            eprintln!("Usage: /loop [interval] <prompt>");
            CommandOutcome::Handled
        }
        "/status" => CommandOutcome::ShowStatus,
        "/vim" => CommandOutcome::ToggleVim,
        "/keybindings" => CommandOutcome::ConfigureKeybindings,
        "/config" => CommandOutcome::ConfigureUi(argument.to_owned()),
        "/theme" => CommandOutcome::ConfigureTheme(argument.to_owned()),
        "/statusline" => CommandOutcome::ConfigureStatusLine(argument.to_owned()),
        "/tui" => CommandOutcome::ConfigureTui(argument.to_owned()),
        "/copy" => CommandOutcome::CopyResponse(argument.to_owned()),
        "/export" => CommandOutcome::ExportConversation(argument.to_owned()),
        "/tasks" | "/bashes" => CommandOutcome::ShowTasks(argument.to_owned()),
        "/transcript" => CommandOutcome::ShowTranscript,
        "/diff" => CommandOutcome::ShowDiff(argument.to_owned()),
        "/rewind" | "/checkpoint" => CommandOutcome::Rewind(argument.to_owned()),
        "/resume" | "/continue" => CommandOutcome::Resume(argument.to_owned()),
        "/skills" => CommandOutcome::ShowSkills,
        "/hooks" => CommandOutcome::ShowHooks,
        "/memory" => CommandOutcome::ShowMemory,
        "/mcp" => CommandOutcome::ManageMcp(argument.to_owned()),
        "/sandbox" => CommandOutcome::ShowSandbox,
        "/plugin" => CommandOutcome::ShowPlugins,
        "/help" => CommandOutcome::ShowHelp,
        _ => {
            eprintln!("Unknown command: {command}");
            CommandOutcome::Handled
        }
    }
}

/// Parses `/loop` without consulting the model. The returned cron uses only
/// clean divisors of the minute/hour boundary so the observable cadence does
/// not acquire a short rollover gap (for example `*/7` at `:56 -> :00`).
pub fn parse_loop_command(input: &str) -> Result<Option<LoopRequest>> {
    let Some(arguments) = input
        .strip_prefix("/loop")
        .filter(|rest| rest.is_empty() || rest.starts_with(char::is_whitespace))
    else {
        return Ok(None);
    };
    let arguments = arguments.trim();
    if arguments.is_empty() {
        bail!("Usage: /loop [interval] <prompt>")
    }

    static LEADING: OnceLock<regex::Regex> = OnceLock::new();
    static TRAILING: OnceLock<regex::Regex> = OnceLock::new();
    let leading = LEADING.get_or_init(|| {
        regex::Regex::new(r"(?i)^(\d+)([smhd])(?:\s+|$)").expect("loop leading regex")
    });
    let trailing = TRAILING.get_or_init(|| {
        regex::Regex::new(
            r"(?is)^(.*?)\s+every\s+(\d+)\s*(seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d)\s*$",
        )
        .expect("loop trailing regex")
    });

    let (requested_interval, requested_minutes, prompt) =
        if let Some(captures) = leading.captures(arguments) {
            let count = parse_loop_count(captures.get(1).expect("count capture").as_str())?;
            let unit = captures.get(2).expect("unit capture").as_str();
            let matched = captures.get(0).expect("whole capture");
            let prompt = arguments[matched.end()..].trim().to_owned();
            (
                format!("{count}{}", unit.to_ascii_lowercase()),
                interval_to_minutes(count, unit)?,
                prompt,
            )
        } else if let Some(captures) = trailing.captures(arguments) {
            let count = parse_loop_count(captures.get(2).expect("count capture").as_str())?;
            let unit = captures.get(3).expect("unit capture").as_str();
            (
                format!("{count} {}", unit.to_ascii_lowercase()),
                interval_to_minutes(count, unit)?,
                captures
                    .get(1)
                    .expect("prompt capture")
                    .as_str()
                    .trim()
                    .to_owned(),
            )
        } else {
            (
                format!("{DEFAULT_LOOP_MINUTES}m"),
                DEFAULT_LOOP_MINUTES,
                arguments.to_owned(),
            )
        };
    if prompt.trim().is_empty() {
        bail!("Usage: /loop [interval] <prompt>")
    }
    if prompt.len() > crate::cron::MAX_CRON_PROMPT_BYTES || prompt.contains('\0') {
        bail!(
            "/loop prompt 超过 {} 字节或包含 NUL",
            crate::cron::MAX_CRON_PROMPT_BYTES
        )
    }

    let effective_minutes = nearest_clean_loop_minutes(requested_minutes)?;
    let (cron, effective_interval) = minutes_to_cron(effective_minutes);
    Ok(Some(LoopRequest {
        cron,
        prompt,
        requested_interval,
        effective_interval,
        rounded: requested_minutes != effective_minutes,
    }))
}

fn parse_loop_count(source: &str) -> Result<u64> {
    let count = source.parse::<u64>().context("/loop interval 数字无效")?;
    if count == 0 {
        bail!("/loop interval 必须大于 0")
    }
    Ok(count)
}

fn interval_to_minutes(count: u64, unit: &str) -> Result<u64> {
    let unit = unit.to_ascii_lowercase();
    let minutes = match unit.as_str() {
        "s" | "sec" | "secs" | "second" | "seconds" => {
            count
                .checked_add(59)
                .context("/loop seconds interval 溢出")?
                / 60
        }
        "m" | "min" | "mins" | "minute" | "minutes" => count,
        "h" | "hr" | "hrs" | "hour" | "hours" => {
            count.checked_mul(60).context("/loop hours interval 溢出")?
        }
        "d" | "day" | "days" => count
            .checked_mul(24 * 60)
            .context("/loop days interval 溢出")?,
        _ => bail!("/loop 不支持 interval unit: {unit}"),
    };
    if minutes == 0 || minutes > MAX_LOOP_MINUTES {
        bail!("/loop interval 必须在 1 分钟到 30 天之间")
    }
    Ok(minutes)
}

fn nearest_clean_loop_minutes(requested: u64) -> Result<u64> {
    const MINUTE_CANDIDATES: &[u64] = &[1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30];
    const HOUR_CANDIDATES: &[u64] = &[1, 2, 3, 4, 6, 8, 12];
    let candidates = MINUTE_CANDIDATES
        .iter()
        .copied()
        .chain(HOUR_CANDIDATES.iter().map(|hours| hours * 60))
        .chain((1..=30).map(|days| days * 24 * 60));
    candidates
        .min_by_key(|candidate| (candidate.abs_diff(requested), *candidate))
        .context("没有可用的 /loop cron cadence")
}

fn minutes_to_cron(minutes: u64) -> (String, String) {
    if minutes < 60 {
        let cron = if minutes == 1 {
            "* * * * *".to_owned()
        } else {
            format!("*/{minutes} * * * *")
        };
        return (cron, format!("{minutes}m"));
    }
    if minutes < 24 * 60 {
        let hours = minutes / 60;
        let cron = if hours == 1 {
            "0 * * * *".to_owned()
        } else {
            format!("0 */{hours} * * *")
        };
        return (cron, format!("{hours}h"));
    }
    let days = minutes / (24 * 60);
    let cron = if days == 1 {
        "0 0 * * *".to_owned()
    } else {
        format!("0 0 */{days} * *")
    };
    (cron, format!("{days}d"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::skills::{decode_user_skill_submission, discover_skill_root};

    #[test]
    fn trusted_settings_commands_are_bounded_and_substitute_arguments() {
        let settings = Settings {
            raw: serde_json::json!({"commands":{
                "audit":{"description":"Audit target", "prompt":"Audit $ARGUMENTS carefully"},
                "explain":"Explain this code"
            }}),
        };
        let commands = CustomCommandCatalog::from_settings(&settings).unwrap();
        assert_eq!(
            commands.render("audit", "src/").unwrap(),
            "Audit src/ carefully"
        );
        assert!(
            commands
                .render("explain", "lib.rs")
                .unwrap()
                .contains("<command-arguments>\nlib.rs")
        );
        assert!(commands.get("audit").is_some());
    }

    #[test]
    fn builtins_and_invalid_settings_fail_closed() {
        for raw in [
            serde_json::json!({"commands":{"help":"replace help"}}),
            serde_json::json!({"commands":{"bad/name":"bad"}}),
            serde_json::json!({"commands":{"bad":{"prompt":"ok", "shell":"no"}}}),
        ] {
            assert!(CustomCommandCatalog::from_settings(&Settings { raw }).is_err());
        }
    }

    #[test]
    fn direct_skill_invocation_wins_and_submits_catalog_backed_marker() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("skills");
        fs::create_dir_all(root.join("audit")).unwrap();
        fs::write(
            root.join("audit/SKILL.md"),
            "---\nname: audit\ndescription: audit\n---\nFULL WORKFLOW",
        )
        .unwrap();
        let skills = discover_skill_root(&root, temp.path()).unwrap();
        let mut commands = CustomCommandCatalog::default();
        commands
            .insert(CustomCommandDefinition {
                name: "audit".into(),
                description: "shadow".into(),
                prompt: "SHADOW".into(),
                source: "test".into(),
            })
            .unwrap();
        let rendered = resolve_extension_submission("/audit target", &skills, &commands)
            .unwrap()
            .unwrap();
        assert_eq!(
            decode_user_skill_submission(&Value::String(rendered.clone())).unwrap(),
            Some(("audit".into(), "target".into()))
        );
        assert!(!rendered.contains("FULL WORKFLOW"));
        assert!(!rendered.contains("SHADOW"));
        assert!(
            resolve_extension_submission("plain", &skills, &commands)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn loop_parses_leading_trailing_and_default_intervals() {
        let leading = parse_loop_command("/loop 5m check deploy")
            .unwrap()
            .unwrap();
        assert_eq!(leading.cron, "*/5 * * * *");
        assert_eq!(leading.prompt, "check deploy");
        assert!(!leading.rounded);

        let trailing = parse_loop_command("/loop run tests every 2 hours")
            .unwrap()
            .unwrap();
        assert_eq!(trailing.cron, "0 */2 * * *");
        assert_eq!(trailing.prompt, "run tests");

        let default = parse_loop_command("/loop check every PR").unwrap().unwrap();
        assert_eq!(default.cron, "*/10 * * * *");
        assert_eq!(default.prompt, "check every PR");
    }

    #[test]
    fn loop_rounds_unclean_cadence_and_rejects_empty_or_unbounded_input() {
        let rounded = parse_loop_command("/loop 7m poll").unwrap().unwrap();
        assert_eq!(rounded.effective_interval, "6m");
        assert!(rounded.rounded);
        assert!(parse_loop_command("/loop 5m").is_err());
        assert!(parse_loop_command("/loop 0m nope").is_err());
        assert!(parse_loop_command("/loop 31d nope").is_err());
        assert!(parse_loop_command("/loopy 5m nope").unwrap().is_none());
    }
}

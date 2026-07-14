use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Map, Value};
use walkdir::WalkDir;

use crate::{
    commands::{CustomCommandCatalog, CustomCommandDefinition, validate_command_name},
    config::Settings,
    skills::{SkillCatalog, discover_skill_root, parse_markdown_frontmatter},
};

const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_PLUGIN_NAME_BYTES: usize = 48;
const MAX_PLUGIN_DESCRIPTION_BYTES: usize = 1024;
const MAX_CONTRIBUTION_PATHS: usize = 16;
const MAX_PLUGIN_MARKDOWN_FILES: usize = 256;
const MAX_PLUGIN_COMMAND_BYTES: u64 = 128 * 1024;
const MAX_PLUGIN_HOOK_BYTES: u64 = 256 * 1024;
const MAX_PLUGIN_SERVER_CONFIG_BYTES: u64 = 256 * 1024;
const MAX_PLUGIN_AGENT_BYTES: u64 = 192 * 1024;
const MAX_PLUGIN_OUTPUT_STYLE_BYTES: u64 = 192 * 1024;
const MAX_PLUGIN_CONTRIBUTION_BYTES: usize = 4 * 1024 * 1024;
const MAX_PLUGIN_WALK_DEPTH: usize = 8;
const MAX_PLUGIN_AGENTS: usize = 128;
const MAX_PLUGIN_OUTPUT_STYLES: usize = 128;
const MAX_PLUGIN_SERVERS: usize = 32;
const MAX_PLUGIN_AGENT_TOOLS: usize = 64;
const MAX_PLUGIN_AGENT_SKILLS: usize = 32;
const MAX_PLUGIN_AGENT_TURNS: usize = 256;
const MAX_SELECTED_OUTPUT_STYLE_NAME_BYTES: usize = 128;
const MAX_PLUGIN_MONITORS: usize = 64;
const MAX_PLUGIN_MONITOR_COMMAND_BYTES: usize = 64 * 1024;
const MAX_PLUGIN_MONITOR_DESCRIPTION_BYTES: usize = 2048;

#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub name: String,
    pub version: Option<String>,
    pub description: String,
    pub root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginOutputStyle {
    pub name: String,
    pub description: String,
    pub prompt: String,
    pub force_for_plugin: Option<bool>,
    pub source_path: PathBuf,
    pub source_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginMonitorWhen {
    Always,
    OnSkillInvoke(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginMonitorDefinition {
    pub name: String,
    pub command: String,
    pub description: String,
    pub when: PluginMonitorWhen,
}

impl PluginOutputStyle {
    /// Renders only the trusted presentation instructions. Selection remains
    /// explicit; `force_for_plugin` is metadata and never overrides a user.
    pub fn system_prompt_section(&self) -> String {
        format!(
            "<output-style name=\"{}\">\n{}\n</output-style>",
            self.name, self.prompt
        )
    }
}

#[derive(Debug, Clone)]
pub struct PluginCatalog {
    plugins: Vec<PluginInfo>,
    skills: SkillCatalog,
    commands: CustomCommandCatalog,
    hooks: Value,
    agents: BTreeMap<String, Value>,
    mcp_servers: BTreeMap<String, Value>,
    lsp_servers: BTreeMap<String, Value>,
    output_styles: Vec<PluginOutputStyle>,
    monitors: Vec<PluginMonitorDefinition>,
}

impl Default for PluginCatalog {
    fn default() -> Self {
        Self {
            plugins: Vec::new(),
            skills: SkillCatalog::default(),
            commands: CustomCommandCatalog::default(),
            hooks: Value::Object(Map::new()),
            agents: BTreeMap::new(),
            mcp_servers: BTreeMap::new(),
            lsp_servers: BTreeMap::new(),
            output_styles: Vec::new(),
            monitors: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    name: String,
    version: Option<String>,
    #[serde(default)]
    description: String,
    skills: Option<Value>,
    commands: Option<Value>,
    hooks: Option<Value>,
    agents: Option<Value>,
    #[serde(rename = "mcpServers")]
    mcp_servers: Option<Value>,
    #[serde(rename = "lspServers")]
    lsp_servers: Option<Value>,
    #[serde(rename = "outputStyles")]
    output_styles: Option<Value>,
    monitors: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestMonitor {
    name: String,
    command: String,
    description: String,
    when: String,
}

impl PluginCatalog {
    /// Discovers only directories explicitly named by trusted settings. This
    /// performs no installation, network access, or hook execution.
    pub fn discover(settings: &Settings, cwd: &Path, bare: bool) -> Result<Self> {
        if bare {
            return Ok(Self::default());
        }
        let installed_roots = crate::plugin_manager::installed_plugin_directories_default()?
            .into_iter()
            .map(|root| fs::canonicalize(&root))
            .collect::<std::io::Result<BTreeSet<_>>>()?;
        Self::discover_with_installed_roots(settings, cwd, installed_roots)
    }

    /// Validates a candidate package without consulting the installed-plugin
    /// registry. PluginManager already owns that registry lock while preparing
    /// or recovering a package, so re-entering the public discovery path would
    /// deadlock or fail the lifecycle operation. Monitor definitions are still
    /// parsed and validated, but an uninstalled candidate cannot activate one.
    pub(crate) fn discover_uninstalled(settings: &Settings, cwd: &Path) -> Result<Self> {
        Self::discover_with_installed_roots(settings, cwd, BTreeSet::new())
    }

    fn discover_with_installed_roots(
        settings: &Settings,
        cwd: &Path,
        installed_roots: BTreeSet<PathBuf>,
    ) -> Result<Self> {
        let directories = settings.plugin_directories()?;
        let mut catalog = Self {
            hooks: Value::Object(Map::new()),
            ..Self::default()
        };
        let mut roots = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut contribution_bytes = 0usize;
        let mut markdown_files = 0usize;
        let mut monitor_names = BTreeSet::new();
        let mut declared_monitors = Vec::new();
        for configured in directories {
            let configured = if configured.is_absolute() {
                configured
            } else {
                cwd.join(configured)
            };
            let root = fs::canonicalize(&configured)
                .with_context(|| format!("无法解析 plugin 目录 {}", configured.display()))?;
            if !root.is_dir() {
                bail!("plugin 路径不是目录: {}", root.display())
            }
            if !roots.insert(root.clone()) {
                bail!("重复 plugin 目录: {}", root.display())
            }
            let manifest_path = confined_existing_path(&root, Path::new("plugin.json"))?;
            let manifest: Manifest = serde_json::from_slice(&read_bounded(
                &manifest_path,
                MAX_MANIFEST_BYTES,
                "plugin manifest",
            )?)
            .with_context(|| format!("plugin manifest 无效: {}", manifest_path.display()))?;
            validate_manifest(&manifest)?;
            if !names.insert(manifest.name.clone()) {
                bail!("重复 plugin name: {}", manifest.name)
            }
            contribution_bytes = checked_add(
                contribution_bytes,
                fs::metadata(&manifest_path)?.len() as usize,
                "plugin contributions",
            )?;

            let plugin_monitors =
                load_plugin_monitors(&root, &manifest.name, manifest.monitors.as_ref())?;
            contribution_bytes =
                plugin_monitors
                    .iter()
                    .try_fold(contribution_bytes, |total, monitor| {
                        checked_add(
                            total,
                            monitor
                                .name
                                .len()
                                .saturating_add(monitor.command.len())
                                .saturating_add(monitor.description.len()),
                            "plugin contributions",
                        )
                    })?;
            for monitor in plugin_monitors {
                if !monitor_names.insert(monitor.name.clone()) {
                    bail!("plugin monitor contribution 冲突: {}", monitor.name)
                }
                declared_monitors.push(monitor.clone());
                if installed_roots.contains(&root) {
                    catalog.monitors.push(monitor);
                }
                if declared_monitors.len() > MAX_PLUGIN_MONITORS {
                    bail!("plugin monitor 超过 {MAX_PLUGIN_MONITORS} 个限制")
                }
            }

            let skill_paths =
                contribution_paths(&root, manifest.skills.as_ref(), "skills", Some("skills"))?;
            for path in skill_paths {
                if !path.is_dir() {
                    bail!("plugin skills contribution 必须是目录: {}", path.display())
                }
                let skills = discover_skill_root(&path, &root)?.namespaced(&manifest.name)?;
                contribution_bytes =
                    skills
                        .iter()
                        .try_fold(contribution_bytes, |total, (_, skill)| {
                            checked_add(total, skill.content.len(), "plugin contributions")
                        })?;
                catalog.skills.merge(skills)?;
            }

            let command_paths = contribution_paths(
                &root,
                manifest.commands.as_ref(),
                "commands",
                Some("commands"),
            )?;
            for path in command_paths {
                let files = collect_markdown(&path, &root)?;
                markdown_files = markdown_files
                    .checked_add(files.len())
                    .context("plugin markdown 文件数量溢出")?;
                if markdown_files > MAX_PLUGIN_MARKDOWN_FILES {
                    bail!("plugin markdown 文件超过 {MAX_PLUGIN_MARKDOWN_FILES} 个限制")
                }
                for file in files {
                    let bytes = read_bounded(&file, MAX_PLUGIN_COMMAND_BYTES, "plugin command")?;
                    contribution_bytes =
                        checked_add(contribution_bytes, bytes.len(), "plugin contributions")?;
                    let text = String::from_utf8(bytes).with_context(|| {
                        format!("plugin command 不是 UTF-8: {}", file.display())
                    })?;
                    let (frontmatter, prompt) =
                        parse_markdown_frontmatter(&text).with_context(|| {
                            format!("plugin command frontmatter 无效: {}", file.display())
                        })?;
                    let fallback = command_name_from_path(&file, &path)?;
                    let local_name = frontmatter
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or(&fallback);
                    validate_command_name(local_name)?;
                    let name = format!("{}:{local_name}", manifest.name);
                    let description = frontmatter
                        .get("description")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| format!("Command from plugin {}", manifest.name));
                    catalog.commands.insert(CustomCommandDefinition {
                        name,
                        description,
                        prompt: prompt.to_owned(),
                        source: format!("plugin {}", manifest.name),
                    })?;
                }
            }

            for (mut hooks, bytes) in load_hook_contributions(&root, manifest.hooks.as_ref())? {
                contribution_bytes =
                    checked_add(contribution_bytes, bytes, "plugin contributions")?;
                substitute_plugin_root(&mut hooks, &root)?;
                merge_hook_objects(&mut catalog.hooks, hooks)?;
            }

            for (name, definition, bytes) in
                load_plugin_agents(&root, &manifest.name, manifest.agents.as_ref())?
            {
                contribution_bytes =
                    checked_add(contribution_bytes, bytes, "plugin contributions")?;
                if catalog.agents.insert(name.clone(), definition).is_some() {
                    bail!("plugin agent contribution 冲突: {name}")
                }
                if catalog.agents.len() > MAX_PLUGIN_AGENTS {
                    bail!("plugin agent 超过 {MAX_PLUGIN_AGENTS} 个限制")
                }
            }

            for style in load_output_styles(&root, &manifest.name, manifest.output_styles.as_ref())?
            {
                contribution_bytes = checked_add(
                    contribution_bytes,
                    style.source_bytes,
                    "plugin contributions",
                )?;
                if catalog
                    .output_styles
                    .iter()
                    .any(|current| current.name == style.name)
                {
                    bail!("plugin output style contribution 冲突: {}", style.name)
                }
                catalog.output_styles.push(style);
                if catalog.output_styles.len() > MAX_PLUGIN_OUTPUT_STYLES {
                    bail!("plugin output style 超过 {MAX_PLUGIN_OUTPUT_STYLES} 个限制")
                }
            }

            let mcp_servers = load_server_contributions(
                &root,
                &manifest.name,
                manifest.mcp_servers.as_ref(),
                ".mcp.json",
                "mcpServers",
            )?;
            contribution_bytes = checked_add(
                contribution_bytes,
                serde_json::to_vec(&mcp_servers)?.len(),
                "plugin contributions",
            )?;
            merge_server_contributions(&mut catalog.mcp_servers, mcp_servers, "MCP")?;
            let lsp_servers = load_server_contributions(
                &root,
                &manifest.name,
                manifest.lsp_servers.as_ref(),
                ".lsp.json",
                "lspServers",
            )?;
            contribution_bytes = checked_add(
                contribution_bytes,
                serde_json::to_vec(&lsp_servers)?.len(),
                "plugin contributions",
            )?;
            merge_server_contributions(&mut catalog.lsp_servers, lsp_servers, "LSP")?;

            catalog.plugins.push(PluginInfo {
                name: manifest.name,
                version: manifest.version,
                description: manifest.description,
                root,
            });
        }
        for monitor in &declared_monitors {
            if let PluginMonitorWhen::OnSkillInvoke(name) = &monitor.when {
                if catalog.skills.get(name).is_none() {
                    bail!("plugin monitor {} 引用了未知 skill {name}", monitor.name)
                }
            }
        }
        Ok(catalog)
    }

    pub fn plugins(&self) -> &[PluginInfo] {
        &self.plugins
    }

    pub fn skills(&self) -> &SkillCatalog {
        &self.skills
    }

    pub fn commands(&self) -> &CustomCommandCatalog {
        &self.commands
    }

    pub fn hooks(&self) -> &Value {
        &self.hooks
    }

    pub fn agents(&self) -> &BTreeMap<String, Value> {
        &self.agents
    }

    pub fn mcp_servers(&self) -> &BTreeMap<String, Value> {
        &self.mcp_servers
    }

    pub fn lsp_servers(&self) -> &BTreeMap<String, Value> {
        &self.lsp_servers
    }

    pub fn output_styles(&self) -> &[PluginOutputStyle] {
        &self.output_styles
    }

    pub fn monitors(&self) -> &[PluginMonitorDefinition] {
        &self.monitors
    }

    pub fn available_output_style_names(&self) -> Vec<String> {
        let mut names = self
            .output_styles
            .iter()
            .map(|style| style.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names.insert(0, "default".to_owned());
        names
    }

    /// Resolves an explicit static selection. Plugins may advertise a forced
    /// style for their own future invocation scope, but cannot silently select
    /// it for the root session.
    pub fn select_output_style(&self, name: Option<&str>) -> Result<Option<&PluginOutputStyle>> {
        let name = name.unwrap_or("default");
        if name.is_empty()
            || name.len() > MAX_SELECTED_OUTPUT_STYLE_NAME_BYTES
            || name.contains(['\0', '\n', '\r'])
        {
            bail!("output style name 为空、过长或包含控制字符")
        }
        if name == "default" {
            return Ok(None);
        }
        self.output_styles
            .iter()
            .find(|style| style.name == name)
            .map(Some)
            .with_context(|| format!("未知 output style: {name}"))
    }

    /// Merges only runtime contributions into the already trusted settings
    /// object. Existing entries always win by causing an explicit conflict;
    /// plugin code can never silently override a user-selected agent/server.
    pub fn apply_runtime_contributions(&self, settings: &mut Settings) -> Result<()> {
        merge_agent_settings(&mut settings.raw, &self.agents)?;
        merge_named_settings(&mut settings.raw, "mcpServers", &self.mcp_servers)?;
        merge_named_settings(&mut settings.raw, "lspServers", &self.lsp_servers)?;
        Ok(())
    }

    pub fn into_parts(
        self,
    ) -> (
        SkillCatalog,
        CustomCommandCatalog,
        Value,
        Vec<PluginMonitorDefinition>,
    ) {
        (self.skills, self.commands, self.hooks, self.monitors)
    }
}

fn contribution_paths(
    root: &Path,
    configured: Option<&Value>,
    kind: &str,
    default: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let mut relative = Vec::new();
    if let Some(default) = default {
        let path = root.join(default);
        if fs::symlink_metadata(&path).is_ok() {
            relative.push(default.to_owned());
        }
    }
    if let Some(configured) = configured {
        relative.extend(path_strings(configured, kind)?);
    }
    if relative.len() > MAX_CONTRIBUTION_PATHS {
        bail!("plugin {kind} contribution 超过 {MAX_CONTRIBUTION_PATHS} 个限制")
    }
    let mut seen = BTreeSet::new();
    let mut paths = Vec::new();
    for value in relative {
        let path = confined_existing_path(root, Path::new(&value))?;
        if seen.insert(path.clone()) {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn path_strings(value: &Value, kind: &str) -> Result<Vec<String>> {
    match value {
        Value::String(path) => Ok(vec![validate_contribution_path_string(path, kind)?]),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                let path = value
                    .as_str()
                    .with_context(|| format!("plugin {kind} 只能包含相对路径 string"))?;
                validate_contribution_path_string(path, kind)
            })
            .collect(),
        _ => bail!("plugin {kind} 必须是相对路径或相对路径 array"),
    }
}

fn validate_contribution_path_string(path: &str, kind: &str) -> Result<String> {
    if path.trim().is_empty() || path.len() > 4096 || path.contains('\0') {
        bail!("plugin {kind} contribution 路径无效")
    }
    if path.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    }) {
        bail!("plugin {kind} 不接受网络 contribution")
    }
    Ok(path.to_owned())
}

fn load_hook_contributions(root: &Path, configured: Option<&Value>) -> Result<Vec<(Value, usize)>> {
    let mut specs = Vec::new();
    let default = root.join("hooks/hooks.json");
    if fs::symlink_metadata(&default).is_ok() {
        specs.push(Value::String("hooks/hooks.json".into()));
    }
    if let Some(configured) = configured {
        match configured {
            Value::Array(values) => specs.extend(values.iter().cloned()),
            value => specs.push(value.clone()),
        }
    }
    if specs.len() > MAX_CONTRIBUTION_PATHS {
        bail!("plugin hooks contribution 超过 {MAX_CONTRIBUTION_PATHS} 个限制")
    }
    let mut paths = BTreeSet::new();
    let mut loaded = Vec::new();
    for spec in specs {
        match spec {
            Value::String(relative) => {
                validate_contribution_path_string(&relative, "hooks")?;
                let path = confined_existing_path(root, Path::new(&relative))?;
                if !paths.insert(path.clone()) {
                    continue;
                }
                let bytes = read_bounded(&path, MAX_PLUGIN_HOOK_BYTES, "plugin hooks")?;
                let hooks: Value = serde_json::from_slice(&bytes)
                    .with_context(|| format!("plugin hooks 不是有效 JSON: {}", path.display()))?;
                loaded.push((unwrap_hooks_document(hooks)?, bytes.len()));
            }
            Value::Object(_) => {
                let size = serde_json::to_vec(&spec)?.len();
                if size > MAX_PLUGIN_HOOK_BYTES as usize {
                    bail!("inline plugin hooks 超过 {MAX_PLUGIN_HOOK_BYTES} 字节限制")
                }
                loaded.push((unwrap_hooks_document(spec)?, size));
            }
            _ => bail!("plugin hooks contribution 必须是路径、object 或其 array"),
        }
    }
    Ok(loaded)
}

fn unwrap_hooks_document(value: Value) -> Result<Value> {
    let object = value
        .as_object()
        .context("plugin hooks 顶层必须是 object")?;
    if object.len() == 1 && object.contains_key("hooks") {
        return object["hooks"]
            .as_object()
            .context("plugin hooks.hooks 必须是 object")
            .map(|object| Value::Object(object.clone()));
    }
    Ok(value)
}

fn load_plugin_agents(
    root: &Path,
    plugin: &str,
    configured: Option<&Value>,
) -> Result<Vec<(String, Value, usize)>> {
    let paths = contribution_paths(root, configured, "agents", Some("agents"))?;
    let mut loaded_paths = BTreeSet::new();
    let mut definitions = Vec::new();
    for path in paths {
        for file in collect_markdown(&path, root)? {
            if !loaded_paths.insert(file.clone()) {
                continue;
            }
            let bytes = read_bounded(&file, MAX_PLUGIN_AGENT_BYTES, "plugin agent")?;
            let text = String::from_utf8(bytes.clone())
                .with_context(|| format!("plugin agent 不是 UTF-8: {}", file.display()))?;
            let (frontmatter, prompt) = parse_markdown_frontmatter(&text)
                .with_context(|| format!("plugin agent frontmatter 无效: {}", file.display()))?;
            for forbidden in ["permissionMode", "permission-mode", "hooks", "mcpServers"] {
                if frontmatter.contains_key(forbidden) {
                    bail!(
                        "plugin agent {} 不得声明提权字段 {forbidden}",
                        file.display()
                    )
                }
            }
            let fallback = command_name_from_path(&file, &path)?;
            let local_name = optional_frontmatter_string(&frontmatter, "name")?.unwrap_or(fallback);
            validate_command_name(&local_name)?;
            let name = format!("{plugin}:{local_name}");
            let description = optional_frontmatter_string(&frontmatter, "description")?
                .or_else(|| {
                    optional_frontmatter_string(&frontmatter, "when-to-use")
                        .ok()
                        .flatten()
                })
                .unwrap_or_else(|| format!("Agent from plugin {plugin}"));
            let allowed = plugin_string_list(frontmatter.get("tools"), "agent tools")?;
            if frontmatter.contains_key("tools") && allowed.is_empty() {
                bail!("plugin agent {name} 显式空 tools 无法安全映射，已拒绝")
            }
            let disallowed = plugin_string_list(
                frontmatter
                    .get("disallowedTools")
                    .or_else(|| frontmatter.get("disallowed-tools")),
                "agent disallowed tools",
            )?;
            validate_short_list(&allowed, "plugin agent tools", MAX_PLUGIN_AGENT_TOOLS)?;
            validate_short_list(
                &disallowed,
                "plugin agent disallowed tools",
                MAX_PLUGIN_AGENT_TOOLS,
            )?;
            let mut skills = plugin_string_list(frontmatter.get("skills"), "agent skills")?;
            validate_short_list(&skills, "plugin agent skills", MAX_PLUGIN_AGENT_SKILLS)?;
            for skill in &mut skills {
                if !skill.contains(':') {
                    *skill = format!("{plugin}:{skill}");
                }
            }
            let mut allowed = allowed;
            if !skills.is_empty()
                && !allowed.is_empty()
                && !allowed.iter().any(|tool| tool == "Skill")
            {
                allowed.push("Skill".into());
            }
            let model = optional_frontmatter_string(&frontmatter, "model")?;
            let max_turns = optional_frontmatter_positive_integer(&frontmatter, "maxTurns")?
                .or(optional_frontmatter_positive_integer(
                    &frontmatter,
                    "max-turns",
                )?)
                .unwrap_or(64);
            if max_turns > MAX_PLUGIN_AGENT_TURNS {
                bail!("plugin agent {name} maxTurns 超过 {MAX_PLUGIN_AGENT_TURNS}")
            }
            let root_text = root.to_str().context("plugin root 不是有效 UTF-8")?;
            let prompt = prompt
                .replace("${PLUGIN_ROOT}", root_text)
                .trim()
                .to_owned();
            if prompt.is_empty() {
                bail!("plugin agent {name} prompt 为空")
            }
            let mut definition = Map::new();
            definition.insert("description".into(), Value::String(description));
            definition.insert("prompt".into(), Value::String(prompt));
            definition.insert("maxTurns".into(), Value::from(max_turns));
            if let Some(model) = model.filter(|model| model != "inherit") {
                definition.insert("model".into(), Value::String(model));
            }
            if !allowed.is_empty() {
                definition.insert("allowedTools".into(), serde_json::to_value(allowed)?);
            }
            if !disallowed.is_empty() {
                definition.insert("disallowedTools".into(), serde_json::to_value(disallowed)?);
            }
            if !skills.is_empty() {
                definition.insert("skills".into(), serde_json::to_value(skills)?);
            }
            definitions.push((name, Value::Object(definition), bytes.len()));
        }
    }
    Ok(definitions)
}

fn load_output_styles(
    root: &Path,
    plugin: &str,
    configured: Option<&Value>,
) -> Result<Vec<PluginOutputStyle>> {
    let paths = contribution_paths(root, configured, "outputStyles", Some("output-styles"))?;
    let mut loaded_paths = BTreeSet::new();
    let mut styles = Vec::new();
    for path in paths {
        for file in collect_markdown(&path, root)? {
            if !loaded_paths.insert(file.clone()) {
                continue;
            }
            let bytes = read_bounded(&file, MAX_PLUGIN_OUTPUT_STYLE_BYTES, "plugin output style")?;
            let source_bytes = bytes.len();
            let text = String::from_utf8(bytes)
                .with_context(|| format!("plugin output style 不是 UTF-8: {}", file.display()))?;
            let (frontmatter, prompt) = parse_markdown_frontmatter(&text).with_context(|| {
                format!("plugin output style frontmatter 无效: {}", file.display())
            })?;
            let fallback = file
                .file_stem()
                .and_then(|value| value.to_str())
                .context("plugin output style 文件名不是 UTF-8")?;
            let local_name = optional_frontmatter_string(&frontmatter, "name")?
                .unwrap_or_else(|| fallback.to_owned());
            validate_command_name(&local_name)?;
            let name = format!("{plugin}:{local_name}");
            let description = optional_frontmatter_string(&frontmatter, "description")?
                .unwrap_or_else(|| format!("Output style from plugin {plugin}"));
            let force_for_plugin = optional_frontmatter_bool(&frontmatter, "force-for-plugin")?;
            let prompt = prompt.trim().to_owned();
            if prompt.is_empty() {
                bail!("plugin output style {name} prompt 为空")
            }
            styles.push(PluginOutputStyle {
                name,
                description,
                prompt,
                force_for_plugin,
                source_path: file,
                source_bytes,
            });
        }
    }
    Ok(styles)
}

fn load_server_contributions(
    root: &Path,
    plugin: &str,
    configured: Option<&Value>,
    default_file: &str,
    wrapper: &str,
) -> Result<BTreeMap<String, Value>> {
    let mut specs = Vec::new();
    if fs::symlink_metadata(root.join(default_file)).is_ok() {
        specs.push(Value::String(default_file.to_owned()));
    }
    if let Some(configured) = configured {
        match configured {
            Value::Array(values) => specs.extend(values.iter().cloned()),
            value => specs.push(value.clone()),
        }
    }
    if specs.len() > MAX_CONTRIBUTION_PATHS {
        bail!("plugin {wrapper} contribution 超过 {MAX_CONTRIBUTION_PATHS} 个限制")
    }
    let mut loaded_paths = BTreeSet::new();
    let mut servers = BTreeMap::new();
    for spec in specs {
        let value = match spec {
            Value::String(relative) => {
                validate_contribution_path_string(&relative, wrapper)?;
                if !relative.ends_with(".json") {
                    bail!("plugin {wrapper} path 必须是本地 JSON 文件")
                }
                let path = confined_existing_path(root, Path::new(&relative))?;
                if !loaded_paths.insert(path.clone()) {
                    continue;
                }
                let bytes = read_bounded(&path, MAX_PLUGIN_SERVER_CONFIG_BYTES, wrapper)?;
                serde_json::from_slice::<Value>(&bytes)
                    .with_context(|| format!("plugin {wrapper} JSON 无效: {}", path.display()))?
            }
            Value::Object(_) => spec,
            _ => bail!("plugin {wrapper} contribution 必须是路径、object 或其 array"),
        };
        let object = value
            .as_object()
            .context("plugin server config 必须是 object")?;
        let object = if object.len() == 1 && object.contains_key(wrapper) {
            object[wrapper]
                .as_object()
                .with_context(|| format!("plugin {wrapper}.{wrapper} 必须是 object"))?
        } else {
            object
        };
        for (local_name, config) in object {
            validate_server_name(local_name)?;
            let name = format!("{plugin}:{local_name}");
            let mut config = config.clone();
            normalize_plugin_server_config(&mut config, root, wrapper)?;
            if servers.insert(name.clone(), config).is_some() {
                bail!("plugin {wrapper} server contribution 冲突: {name}")
            }
        }
    }
    if servers.len() > MAX_PLUGIN_SERVERS {
        bail!("plugin {wrapper} server 超过 {MAX_PLUGIN_SERVERS} 个限制")
    }
    Ok(servers)
}

fn normalize_plugin_server_config(value: &mut Value, root: &Path, label: &str) -> Result<()> {
    if !value.is_object() {
        bail!("plugin {label} server 必须是 object")
    }
    substitute_plugin_root(value, root)?;
    let object = value.as_object_mut().expect("object preserved");
    let root_text = root.to_str().context("plugin root 不是有效 UTF-8")?;
    if object.contains_key("command") {
        match object.get_mut("cwd") {
            None => {
                object.insert("cwd".into(), Value::String(root_text.to_owned()));
            }
            Some(Value::String(cwd)) => {
                let candidate = PathBuf::from(&*cwd);
                let candidate = if candidate.is_absolute() {
                    fs::canonicalize(&candidate)
                } else {
                    fs::canonicalize(root.join(candidate))
                }
                .with_context(|| format!("plugin {label} cwd 不存在"))?;
                if !candidate.is_dir() || !candidate.starts_with(root) {
                    bail!("plugin {label} cwd 必须位于 plugin 根目录内")
                }
                *cwd = candidate
                    .to_str()
                    .context("plugin server cwd 不是 UTF-8")?
                    .to_owned();
            }
            Some(_) => bail!("plugin {label} cwd 必须是 string"),
        }
    }
    if let Some(Value::String(command)) = object.get_mut("command") {
        if command.starts_with("./") {
            let candidate = fs::canonicalize(root.join(&*command))
                .with_context(|| format!("plugin {label} command 不存在"))?;
            if !candidate.is_file() || !candidate.starts_with(root) {
                bail!("plugin {label} command 必须是 plugin 内普通文件")
            }
            *command = candidate
                .to_str()
                .context("plugin server command 不是 UTF-8")?
                .to_owned();
        }
    }
    Ok(())
}

fn merge_server_contributions(
    target: &mut BTreeMap<String, Value>,
    incoming: BTreeMap<String, Value>,
    label: &str,
) -> Result<()> {
    for (name, config) in incoming {
        if target.insert(name.clone(), config).is_some() {
            bail!("plugin {label} server contribution 冲突: {name}")
        }
        if target.len() > MAX_PLUGIN_SERVERS {
            bail!("plugin {label} server 总数超过 {MAX_PLUGIN_SERVERS} 个限制")
        }
    }
    Ok(())
}

fn merge_named_settings(
    settings: &mut Value,
    key: &str,
    incoming: &BTreeMap<String, Value>,
) -> Result<()> {
    if incoming.is_empty() {
        return Ok(());
    }
    let root = settings
        .as_object_mut()
        .context("trusted settings 必须是 object")?;
    let destination = root
        .entry(key.to_owned())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .with_context(|| format!("settings.{key} 必须是 object"))?;
    for (name, value) in incoming {
        if destination.contains_key(name) {
            bail!("settings.{key} 与 plugin contribution 冲突: {name}")
        }
        destination.insert(name.clone(), value.clone());
    }
    Ok(())
}

fn merge_agent_settings(settings: &mut Value, incoming: &BTreeMap<String, Value>) -> Result<()> {
    if incoming.is_empty() {
        return Ok(());
    }
    let root = settings
        .as_object_mut()
        .context("trusted settings 必须是 object")?;
    let agents = root
        .entry("agents")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .context("settings.agents 必须是 object")?;
    let definitions = agents
        .entry("definitions")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .context("settings.agents.definitions 必须是 object")?;
    for (name, value) in incoming {
        if definitions.contains_key(name) {
            bail!("settings agent 与 plugin contribution 冲突: {name}")
        }
        definitions.insert(name.clone(), value.clone());
    }
    Ok(())
}

fn optional_frontmatter_string(values: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match values.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() && !value.contains('\0') => {
            Ok(Some(value.trim().to_owned()))
        }
        Some(_) => bail!("plugin frontmatter.{key} 必须是非空 string"),
    }
}

fn optional_frontmatter_bool(values: &Map<String, Value>, key: &str) -> Result<Option<bool>> {
    match values.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::String(value)) if value == "true" => Ok(Some(true)),
        Some(Value::String(value)) if value == "false" => Ok(Some(false)),
        Some(_) => bail!("plugin frontmatter.{key} 必须是 boolean"),
    }
}

fn optional_frontmatter_positive_integer(
    values: &Map<String, Value>,
    key: &str,
) -> Result<Option<usize>> {
    match values.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
            .map(Some)
            .context("plugin frontmatter integer 超出范围"),
        Some(Value::String(value)) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .map(Some)
            .with_context(|| format!("plugin frontmatter.{key} 必须是正整数")),
        Some(_) => bail!("plugin frontmatter.{key} 必须是正整数"),
    }
}

fn plugin_string_list(value: Option<&Value>, label: &str) -> Result<Vec<String>> {
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(value)) => Ok(value
            .split(|character: char| character == ',' || character.is_whitespace())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect()),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| value.trim().to_owned())
                    .with_context(|| format!("plugin {label} 只能包含非空 string"))
            })
            .collect(),
        Some(_) => bail!("plugin {label} 必须是 string 或 string array"),
    }
}

fn validate_short_list(values: &[String], label: &str, maximum: usize) -> Result<()> {
    if values.len() > maximum {
        bail!("{label} 超过 {maximum} 项限制")
    }
    let mut unique = BTreeSet::new();
    for value in values {
        if value.is_empty()
            || value.len() > 128
            || value.contains(['\0', '\n', '\r'])
            || !unique.insert(value)
        {
            bail!("{label} 包含无效或重复项")
        }
    }
    Ok(())
}

fn validate_server_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 96
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        bail!("plugin server name 无效: {name}")
    }
    Ok(())
}

fn validate_manifest(manifest: &Manifest) -> Result<()> {
    if manifest.name.is_empty()
        || manifest.name.len() > MAX_PLUGIN_NAME_BYTES
        || !manifest
            .name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_-".contains(character))
    {
        bail!("无效 plugin name: {}", manifest.name)
    }
    if manifest.description.len() > MAX_PLUGIN_DESCRIPTION_BYTES {
        bail!("plugin {} description 过长", manifest.name)
    }
    if manifest.version.as_ref().is_some_and(|version| {
        version.is_empty()
            || version.len() > 64
            || !version
                .chars()
                .all(|character| character.is_ascii_graphic())
    }) {
        bail!("plugin {} version 无效", manifest.name)
    }
    Ok(())
}

fn load_plugin_monitors(
    root: &Path,
    plugin: &str,
    configured: Option<&Value>,
) -> Result<Vec<PluginMonitorDefinition>> {
    let Some(configured) = configured else {
        return Ok(Vec::new());
    };
    let values = configured
        .as_array()
        .context("plugin monitors 必须是 array")?;
    if values.len() > MAX_PLUGIN_MONITORS {
        bail!("plugin monitors 超过 {MAX_PLUGIN_MONITORS} 个限制")
    }
    let root_text = root.to_str().context("plugin monitor root 不是 UTF-8")?;
    let mut names = BTreeSet::new();
    let mut monitors = Vec::with_capacity(values.len());
    for value in values {
        let monitor: ManifestMonitor =
            serde_json::from_value(value.clone()).context("plugin monitor definition 无效")?;
        validate_server_name(&monitor.name)?;
        let name = format!("{plugin}:{}", monitor.name);
        if !names.insert(name.clone()) {
            bail!("plugin monitor name 重复: {name}")
        }
        if monitor.command.trim().is_empty()
            || monitor.command.len() > MAX_PLUGIN_MONITOR_COMMAND_BYTES
            || monitor.command.contains('\0')
        {
            bail!("plugin monitor {name} command 为空、过长或包含 NUL")
        }
        if monitor.description.trim().is_empty()
            || monitor.description.len() > MAX_PLUGIN_MONITOR_DESCRIPTION_BYTES
            || monitor.description.chars().any(char::is_control)
        {
            bail!("plugin monitor {name} description 无效")
        }
        let when = if monitor.when == "always" {
            PluginMonitorWhen::Always
        } else if let Some(skill) = monitor.when.strip_prefix("on-skill-invoke:") {
            validate_server_name(skill)?;
            PluginMonitorWhen::OnSkillInvoke(format!("{plugin}:{skill}"))
        } else {
            bail!("plugin monitor {name} when 必须是 always 或 on-skill-invoke:<name>")
        };
        monitors.push(PluginMonitorDefinition {
            name,
            command: monitor.command.replace("${PLUGIN_ROOT}", root_text),
            description: monitor.description.trim().to_owned(),
            when,
        });
    }
    Ok(monitors)
}

fn confined_existing_path(root: &Path, relative: &Path) -> Result<PathBuf> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!(
            "plugin contribution 路径必须是安全相对路径: {}",
            relative.display()
        )
    }
    let joined = root.join(relative);
    let canonical = fs::canonicalize(&joined)
        .with_context(|| format!("无法解析 plugin contribution {}", joined.display()))?;
    if !canonical.starts_with(root) {
        bail!(
            "plugin contribution symlink 越过 plugin 根目录: {}",
            joined.display()
        )
    }
    Ok(canonical)
}

fn read_bounded(path: &Path, limit: u64, label: &str) -> Result<Vec<u8>> {
    let metadata =
        fs::metadata(path).with_context(|| format!("无法检查 {label} {}", path.display()))?;
    if !metadata.is_file() || metadata.len() > limit {
        bail!(
            "{label} 不是普通文件或超过 {limit} 字节限制: {}",
            path.display()
        )
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit as usize {
        bail!("{label} 读取时增长到超过 {limit} 字节限制")
    }
    Ok(bytes)
}

fn collect_markdown(path: &Path, root: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        if path.extension().and_then(|value| value.to_str()) != Some("md") {
            bail!("plugin command 文件必须以 .md 结尾: {}", path.display())
        }
        return Ok(vec![path.to_owned()]);
    }
    if !path.is_dir() {
        bail!("plugin command contribution 不存在: {}", path.display())
    }
    let mut files = Vec::new();
    for entry in WalkDir::new(path)
        .follow_links(false)
        .max_depth(MAX_PLUGIN_WALK_DEPTH)
    {
        let entry =
            entry.with_context(|| format!("无法遍历 plugin commands {}", path.display()))?;
        if entry.file_type().is_symlink() {
            bail!("plugin commands 不接受 symlink: {}", entry.path().display())
        }
        if !entry.file_type().is_file()
            || entry.path().extension().and_then(|value| value.to_str()) != Some("md")
        {
            continue;
        }
        let canonical = fs::canonicalize(entry.path())?;
        if !canonical.starts_with(root) {
            bail!(
                "plugin command 越过 plugin 根目录: {}",
                entry.path().display()
            )
        }
        files.push(canonical);
    }
    files.sort();
    Ok(files)
}

fn command_name_from_path(file: &Path, contribution: &Path) -> Result<String> {
    let relative = if contribution.is_file() {
        file.file_name()
            .map(PathBuf::from)
            .context("command 缺少文件名")?
    } else {
        file.strip_prefix(contribution)
            .context("command 不在 contribution 目录内")?
            .to_owned()
    };
    let mut components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str().map(str::to_owned),
            _ => None,
        })
        .collect::<Vec<_>>();
    let file = components.last_mut().context("command 路径为空")?;
    *file = file
        .strip_suffix(".md")
        .context("command 文件缺少 .md 后缀")?
        .to_owned();
    let name = components.join(":");
    validate_command_name(&name)?;
    Ok(name)
}

fn substitute_plugin_root(value: &mut Value, root: &Path) -> Result<()> {
    let root = root.to_str().context("plugin root 不是有效 UTF-8")?;
    match value {
        Value::String(text) => *text = text.replace("${PLUGIN_ROOT}", root),
        Value::Array(values) => {
            for value in values {
                substitute_plugin_root(value, Path::new(root))?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                substitute_plugin_root(value, Path::new(root))?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn merge_hook_objects(target: &mut Value, incoming: Value) -> Result<()> {
    let target = target
        .as_object_mut()
        .context("plugin hooks target 必须是 object")?;
    let incoming = incoming
        .as_object()
        .context("plugin hooks 顶层必须是 object")?;
    for (event, rules) in incoming {
        let rules = rules
            .as_array()
            .with_context(|| format!("plugin hooks.{event} 必须是 array"))?;
        let destination = target
            .entry(event.clone())
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .context("plugin hook merge target 必须是 array")?;
        destination.extend(rules.iter().cloned());
    }
    Ok(())
}

fn checked_add(current: usize, additional: usize, label: &str) -> Result<usize> {
    let total = current.checked_add(additional).context("大小计算溢出")?;
    if total > MAX_PLUGIN_CONTRIBUTION_BYTES {
        bail!("{label} 超过 {MAX_PLUGIN_CONTRIBUTION_BYTES} 字节限制")
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn settings(path: &Path) -> Settings {
        Settings {
            raw: serde_json::json!({"plugins":{"directories":[path]}}),
        }
    }

    #[test]
    fn discovers_manifest_skills_commands_and_hooks_without_executing() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("quality");
        fs::create_dir_all(plugin.join("skills/review")).unwrap();
        fs::create_dir_all(plugin.join("commands/nested")).unwrap();
        fs::write(
            plugin.join("plugin.json"),
            r#"{
                "name":"quality","version":"1.0.0","description":"Quality tools",
                "skills":["skills"],"commands":["commands"],"hooks":["hooks.json"]
            }"#,
        )
        .unwrap();
        fs::write(
            plugin.join("skills/review/SKILL.md"),
            "---\nname: review\ndescription: Review code\n---\nReview workflow",
        )
        .unwrap();
        fs::write(
            plugin.join("commands/nested/check.md"),
            "---\ndescription: Check target\n---\nCheck $ARGUMENTS",
        )
        .unwrap();
        fs::write(
            plugin.join("hooks.json"),
            r#"{"SessionStart":[{"matcher":"","hooks":[{"type":"command","command":"${PLUGIN_ROOT}/bin/check","once":true}]}]}"#,
        )
        .unwrap();

        let catalog = PluginCatalog::discover(&settings(&plugin), temp.path(), false).unwrap();
        assert_eq!(catalog.plugins().len(), 1);
        assert!(catalog.skills().get("quality:review").is_some());
        assert_eq!(
            catalog
                .commands()
                .render("quality:nested:check", "src")
                .unwrap(),
            "Check src"
        );
        let canonical_plugin = fs::canonicalize(&plugin).unwrap();
        let expected_hook_command = format!("{}/bin/check", canonical_plugin.to_str().unwrap());
        assert_eq!(
            catalog.hooks()["SessionStart"][0]["hooks"][0]["command"],
            expected_hook_command
        );
    }

    #[cfg(unix)]
    #[test]
    fn contribution_symlinks_cannot_escape_plugin_root() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("plugin");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&plugin).unwrap();
        fs::create_dir_all(outside.join("secret")).unwrap();
        fs::write(outside.join("secret/SKILL.md"), "secret").unwrap();
        symlink(&outside, plugin.join("skills")).unwrap();
        fs::write(
            plugin.join("plugin.json"),
            r#"{"name":"escape","skills":["skills"]}"#,
        )
        .unwrap();
        let error = PluginCatalog::discover(&settings(&plugin), temp.path(), false).unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }

    #[test]
    fn absolute_and_parent_contribution_paths_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("plugin");
        fs::create_dir_all(&plugin).unwrap();
        for path in ["../outside", "/tmp/outside"] {
            fs::write(
                plugin.join("plugin.json"),
                serde_json::json!({"name":"invalid", "commands":[path]}).to_string(),
            )
            .unwrap();
            assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());
        }
    }

    #[test]
    fn manifest_size_path_count_and_unknown_fields_are_bounded() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("plugin");
        fs::create_dir_all(&plugin).unwrap();

        fs::write(
            plugin.join("plugin.json"),
            vec![b'x'; MAX_MANIFEST_BYTES as usize + 1],
        )
        .unwrap();
        assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());

        let paths = (0..=MAX_CONTRIBUTION_PATHS)
            .map(|index| format!("commands/{index}.md"))
            .collect::<Vec<_>>();
        fs::write(
            plugin.join("plugin.json"),
            serde_json::json!({"name":"bounded", "commands":paths}).to_string(),
        )
        .unwrap();
        assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());

        fs::write(
            plugin.join("plugin.json"),
            r#"{"name":"bounded","install":"https://plugin.invalid/"}"#,
        )
        .unwrap();
        assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());
    }

    #[test]
    fn oversized_command_contribution_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("plugin");
        fs::create_dir_all(&plugin).unwrap();
        fs::write(
            plugin.join("plugin.json"),
            r#"{"name":"bounded","commands":["large.md"]}"#,
        )
        .unwrap();
        let file = fs::File::create(plugin.join("large.md")).unwrap();
        file.set_len(MAX_PLUGIN_COMMAND_BYTES + 1).unwrap();
        assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());
    }

    #[test]
    fn discovers_agents_mcp_lsp_and_output_styles_and_applies_runtime_settings() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("runtime");
        fs::create_dir_all(plugin.join("agents")).unwrap();
        fs::create_dir_all(plugin.join("skills/review")).unwrap();
        fs::create_dir_all(plugin.join("output-styles")).unwrap();
        fs::write(plugin.join("plugin.json"), r#"{"name":"runtime"}"#).unwrap();
        fs::write(
            plugin.join("skills/review/SKILL.md"),
            "---\nname: review\ndescription: Review\n---\nReview",
        )
        .unwrap();
        fs::write(
            plugin.join("agents/reviewer.md"),
            r#"---
name: reviewer
description: Reviews Rust
tools: ["Read"]
skills: ["review"]
model: inherit
maxTurns: 12
---
Inspect files under ${PLUGIN_ROOT}.
"#,
        )
        .unwrap();
        fs::write(
            plugin.join("output-styles/brief.md"),
            "---\nname: brief\ndescription: Brief output\nforce-for-plugin: true\n---\nRespond briefly.",
        )
        .unwrap();
        fs::write(
            plugin.join(".mcp.json"),
            r#"{"mcpServers":{"local":{"command":"echo","args":["ok"]}}}"#,
        )
        .unwrap();
        fs::write(
            plugin.join(".lsp.json"),
            r#"{"lspServers":{"rust":{"command":"rust-analyzer","extensionToLanguage":{".rs":"rust"}}}}"#,
        )
        .unwrap();

        let catalog = PluginCatalog::discover(&settings(&plugin), temp.path(), false).unwrap();
        assert!(catalog.skills().get("runtime:review").is_some());
        let agent = &catalog.agents()["runtime:reviewer"];
        assert_eq!(agent["maxTurns"], 12);
        assert_eq!(agent["skills"][0], "runtime:review");
        assert!(
            agent["allowedTools"]
                .as_array()
                .unwrap()
                .contains(&json!("Skill"))
        );
        assert_eq!(catalog.output_styles()[0].name, "runtime:brief");
        assert_eq!(catalog.output_styles()[0].force_for_plugin, Some(true));
        assert_eq!(
            catalog.available_output_style_names(),
            vec!["default", "runtime:brief"]
        );
        assert!(catalog.select_output_style(None).unwrap().is_none());
        let selected = catalog
            .select_output_style(Some("runtime:brief"))
            .unwrap()
            .unwrap();
        assert!(
            selected
                .system_prompt_section()
                .contains("Respond briefly.")
        );
        assert!(
            catalog
                .select_output_style(Some("runtime:missing"))
                .is_err()
        );
        assert!(catalog.mcp_servers().contains_key("runtime:local"));
        assert!(catalog.lsp_servers().contains_key("runtime:rust"));
        assert_eq!(
            catalog.mcp_servers()["runtime:local"]["cwd"],
            fs::canonicalize(&plugin).unwrap().to_str().unwrap()
        );

        let mut trusted = Settings {
            raw: json!({"agents":{"maxDepth":2}}),
        };
        catalog.apply_runtime_contributions(&mut trusted).unwrap();
        assert!(trusted.raw["agents"]["definitions"]["runtime:reviewer"].is_object());
        assert!(trusted.raw["mcpServers"]["runtime:local"].is_object());
        assert!(trusted.raw["lspServers"]["runtime:rust"].is_object());
    }

    #[test]
    fn output_style_conflicts_and_size_limits_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("conflict");
        fs::create_dir_all(plugin.join("output-styles")).unwrap();
        fs::write(plugin.join("plugin.json"), r#"{"name":"conflict"}"#).unwrap();
        for file in ["one.md", "two.md"] {
            fs::write(
                plugin.join("output-styles").join(file),
                "---\nname: same\n---\nStyle prompt",
            )
            .unwrap();
        }
        assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());

        let bounded = temp.path().join("bounded");
        fs::create_dir_all(bounded.join("output-styles")).unwrap();
        fs::write(bounded.join("plugin.json"), r#"{"name":"bounded"}"#).unwrap();
        let file = fs::File::create(bounded.join("output-styles/large.md")).unwrap();
        file.set_len(MAX_PLUGIN_OUTPUT_STYLE_BYTES + 1).unwrap();
        assert!(PluginCatalog::discover(&settings(&bounded), temp.path(), false).is_err());

        let aggregate = temp.path().join("aggregate");
        fs::create_dir_all(aggregate.join("output-styles")).unwrap();
        fs::write(aggregate.join("plugin.json"), r#"{"name":"aggregate"}"#).unwrap();
        let body = "x".repeat(MAX_PLUGIN_OUTPUT_STYLE_BYTES as usize - 128);
        for index in 0..22 {
            fs::write(
                aggregate
                    .join("output-styles")
                    .join(format!("style-{index}.md")),
                format!("---\nname: style-{index}\n---\n{body}"),
            )
            .unwrap();
        }
        let error = PluginCatalog::discover(&settings(&aggregate), temp.path(), false).unwrap_err();
        assert!(error.to_string().contains("plugin contributions"));
    }

    #[test]
    fn runtime_contribution_conflicts_and_remote_specs_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("runtime");
        fs::create_dir_all(&plugin).unwrap();
        fs::write(
            plugin.join("plugin.json"),
            r#"{"name":"runtime","mcpServers":{"local":{"command":"echo"}}}"#,
        )
        .unwrap();
        let catalog = PluginCatalog::discover(&settings(&plugin), temp.path(), false).unwrap();
        let mut trusted = Settings {
            raw: json!({"mcpServers":{"runtime:local":{"command":"other"}}}),
        };
        assert!(catalog.apply_runtime_contributions(&mut trusted).is_err());

        fs::write(
            plugin.join("plugin.json"),
            r#"{"name":"runtime","mcpServers":"https://plugins.invalid/server.json"}"#,
        )
        .unwrap();
        assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn default_server_config_symlink_escape_is_rejected() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("runtime");
        fs::create_dir_all(&plugin).unwrap();
        fs::write(plugin.join("plugin.json"), r#"{"name":"runtime"}"#).unwrap();
        let outside = temp.path().join("outside.json");
        fs::write(&outside, r#"{"mcpServers":{}}"#).unwrap();
        symlink(&outside, plugin.join(".mcp.json")).unwrap();
        let error = PluginCatalog::discover(&settings(&plugin), temp.path(), false).unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }

    #[test]
    fn project_plugin_monitors_are_validated_but_never_activated() {
        let temp = tempfile::tempdir().unwrap();
        let plugin = temp.path().join("project-plugin");
        fs::create_dir_all(plugin.join("skills/review")).unwrap();
        fs::write(
            plugin.join("skills/review/SKILL.md"),
            "---\nname: review\ndescription: Review\n---\nReview",
        )
        .unwrap();
        fs::write(
            plugin.join("plugin.json"),
            serde_json::json!({
                "name":"project",
                "monitors":[{
                    "name":"watch",
                    "command":"printf '${PLUGIN_ROOT}'",
                    "description":"watch project",
                    "when":"on-skill-invoke:review"
                }]
            })
            .to_string(),
        )
        .unwrap();
        let catalog = PluginCatalog::discover(&settings(&plugin), temp.path(), false).unwrap();
        assert!(catalog.monitors().is_empty());

        fs::write(
            plugin.join("plugin.json"),
            r#"{"name":"project","monitors":[{"name":"watch","command":"echo ok","description":"watch project","when":"on-skill-invoke:missing"}]}"#,
        )
        .unwrap();
        let error = PluginCatalog::discover(&settings(&plugin), temp.path(), false).unwrap_err();
        assert!(error.to_string().contains("未知 skill"));

        fs::write(
            plugin.join("plugin.json"),
            r#"{"name":"project","monitors":[{"name":"watch","command":"echo ok","description":"watch project","when":"sometimes"}]}"#,
        )
        .unwrap();
        assert!(PluginCatalog::discover(&settings(&plugin), temp.path(), false).is_err());
    }
}

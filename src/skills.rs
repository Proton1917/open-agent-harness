use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use regex::Regex;
use serde_json::{Map, Value};

const MAX_SKILL_BYTES: u64 = 192 * 1024;
const MAX_SKILLS: usize = 128;
const MAX_SKILL_ARGUMENT_BYTES: usize = 32 * 1024;
const MAX_SKILL_INVOCATION_BYTES: usize = 256 * 1024;
const MAX_SKILL_FRONTMATTER_BYTES: usize = 64 * 1024;
const MAX_SKILL_DESCRIPTION_BYTES: usize = 8 * 1024;
const MAX_SKILL_MODEL_BYTES: usize = 256;
const MAX_SKILL_ARGUMENT_HINT_BYTES: usize = 1024;
const MAX_SKILL_ARGUMENT_NAMES: usize = 32;
const MAX_SKILL_ALLOWED_TOOLS: usize = 64;
const MAX_SKILL_HOOK_BYTES: usize = 64 * 1024;
const MAX_SKILL_DIRECTORY_ENTRIES: usize = 512;
const SKILL_SUBMISSION_PREFIX: &str = "\u{1e}open-agent-harness-skill:";

#[cfg(windows)]
#[repr(C)]
struct WindowsFileTime {
    _low: u32,
    _high: u32,
}

#[cfg(windows)]
#[repr(C)]
struct WindowsFileInformation {
    attributes: u32,
    _creation_time: WindowsFileTime,
    _last_access_time: WindowsFileTime,
    _last_write_time: WindowsFileTime,
    volume_serial_number: u32,
    _file_size_high: u32,
    _file_size_low: u32,
    _number_of_links: u32,
    file_index_high: u32,
    file_index_low: u32,
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "GetFileInformationByHandle"]
    fn get_file_information_by_handle(
        file: *mut std::ffi::c_void,
        information: *mut WindowsFileInformation,
    ) -> i32;

    #[link_name = "GetFinalPathNameByHandleW"]
    fn get_final_path_name_by_handle(
        file: *mut std::ffi::c_void,
        path: *mut u16,
        path_len: u32,
        flags: u32,
    ) -> u32;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillExecutionContext {
    Inline,
    Fork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillInvocationSource {
    User,
    Model,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillTrust {
    /// User-owned skill roots and plugin roots named by trusted settings may
    /// attach invocation-local execution metadata.
    Trusted,
    /// Automatically discovered project skills are workflow text only. Their
    /// tool declarations may narrow visibility, but never grant permission.
    Project,
}

#[derive(Debug, Clone, Default)]
pub struct SkillCatalog {
    entries: BTreeMap<String, SkillDefinition>,
}

#[derive(Debug, Clone)]
pub struct SkillDefinition {
    pub name: String,
    pub description: String,
    pub path: PathBuf,
    pub content: String,
    /// Markdown body without frontmatter. Frontmatter remains available in
    /// `content` for inspection, but is never injected as executable prompt
    /// text.
    pub prompt: String,
    pub allowed_tools: BTreeSet<String>,
    pub argument_hint: Option<String>,
    pub argument_names: Vec<String>,
    pub model: Option<String>,
    pub disable_model_invocation: bool,
    pub user_invocable: bool,
    pub hooks: Option<Value>,
    pub execution_context: SkillExecutionContext,
    pub agent: Option<String>,
    pub trust: SkillTrust,
}

#[derive(Debug, Clone)]
pub struct SkillInvocation {
    pub name: String,
    pub prompt: String,
    pub allowed_tools: BTreeSet<String>,
    pub model: Option<String>,
    pub hooks: Option<Value>,
    pub execution_context: SkillExecutionContext,
    pub agent: Option<String>,
    pub trusted_execution_metadata: bool,
}

impl SkillCatalog {
    pub fn get(&self, name: &str) -> Option<&SkillDefinition> {
        self.entries.get(name)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SkillDefinition)> {
        self.entries.iter()
    }

    pub fn prepare_invocation(
        &self,
        name: &str,
        arguments: &str,
        source: SkillInvocationSource,
    ) -> Result<SkillInvocation> {
        let skill = self
            .get(name)
            .with_context(|| format!("未知 skill: {name}"))?;
        skill.prepare_invocation(arguments, source)
    }

    /// Renders an internal user-submission marker. QueryEngine resolves the
    /// metadata through this provenance-bearing catalog instead of trusting
    /// prompt text or a caller-supplied marker.
    pub fn render_invocation(&self, name: &str, arguments: &str) -> Result<String> {
        self.prepare_invocation(name, arguments, SkillInvocationSource::User)?;
        let rendered = format!(
            "{SKILL_SUBMISSION_PREFIX}{}",
            serde_json::to_string(&serde_json::json!({
                "name": name,
                "arguments": arguments,
            }))?
        );
        if rendered.len() > MAX_SKILL_INVOCATION_BYTES {
            bail!("skill invocation 超过 {MAX_SKILL_INVOCATION_BYTES} 字节限制")
        }
        Ok(rendered)
    }

    pub fn merge(&mut self, incoming: SkillCatalog) -> Result<()> {
        for (name, skill) in incoming.entries {
            if let Some(existing) = self.entries.get(&name) {
                match (existing.trust, skill.trust) {
                    (SkillTrust::Project, SkillTrust::Trusted) => {
                        self.entries.insert(name, skill);
                        continue;
                    }
                    (SkillTrust::Trusted, SkillTrust::Project) => continue,
                    _ => bail!("skill contribution 冲突: {name}"),
                }
            }
            if self.entries.len() >= MAX_SKILLS {
                bail!("skill 数量超过 {MAX_SKILLS} 个限制")
            }
            self.entries.insert(name, skill);
        }
        Ok(())
    }

    pub fn namespaced(mut self, namespace: &str) -> Result<Self> {
        validate_skill_name(namespace)?;
        let entries = self
            .entries
            .into_iter()
            .map(|(name, mut skill)| {
                let name = format!("{namespace}:{name}");
                validate_skill_name(&name)?;
                skill.name = name.clone();
                if let Some(agent) = &mut skill.agent {
                    if !agent.contains(':') {
                        *agent = format!("{namespace}:{agent}");
                        validate_skill_name(agent)?;
                    }
                }
                Ok((name, skill))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        self.entries = entries;
        Ok(self)
    }
}

pub(crate) fn decode_user_skill_submission(content: &Value) -> Result<Option<(String, String)>> {
    let Some(content) = content.as_str() else {
        return Ok(None);
    };
    let Some(encoded) = content.strip_prefix(SKILL_SUBMISSION_PREFIX) else {
        return Ok(None);
    };
    if encoded.len() > MAX_SKILL_ARGUMENT_BYTES.saturating_add(1024) {
        bail!("encoded skill submission 超过限制")
    }
    let value: Value = serde_json::from_str(encoded).context("encoded skill submission 无效")?;
    let object = value
        .as_object()
        .context("encoded skill submission 必须是 object")?;
    if object.len() != 2 || !object.contains_key("name") || !object.contains_key("arguments") {
        bail!("encoded skill submission 字段无效")
    }
    let name = object["name"]
        .as_str()
        .context("encoded skill name 必须是 string")?;
    let arguments = object["arguments"]
        .as_str()
        .context("encoded skill arguments 必须是 string")?;
    validate_skill_name(name)?;
    if arguments.len() > MAX_SKILL_ARGUMENT_BYTES {
        bail!("skill arguments 超过 {MAX_SKILL_ARGUMENT_BYTES} 字节限制")
    }
    Ok(Some((name.to_owned(), arguments.to_owned())))
}

impl SkillDefinition {
    pub fn prepare_invocation(
        &self,
        arguments: &str,
        source: SkillInvocationSource,
    ) -> Result<SkillInvocation> {
        if arguments.len() > MAX_SKILL_ARGUMENT_BYTES {
            bail!("skill arguments 超过 {MAX_SKILL_ARGUMENT_BYTES} 字节限制")
        }
        match source {
            SkillInvocationSource::User if !self.user_invocable => {
                bail!("skill {} 禁止用户直接调用", self.name)
            }
            SkillInvocationSource::Model if self.disable_model_invocation => {
                bail!("skill {} 设置了 disable-model-invocation", self.name)
            }
            SkillInvocationSource::User | SkillInvocationSource::Model => {}
        }
        if self.trust == SkillTrust::Project
            && (self.model.is_some() || self.agent.is_some() || self.hooks.is_some())
        {
            bail!(
                "project skill {} 不能声明 hooks、model 或 agent；请将需要执行元数据的 skill 放入用户目录或显式可信 plugin",
                self.name
            )
        }
        let prompt = substitute_arguments(&self.prompt, arguments, &self.argument_names)?;
        if prompt.len() > MAX_SKILL_INVOCATION_BYTES {
            bail!("skill invocation 超过 {MAX_SKILL_INVOCATION_BYTES} 字节限制")
        }
        Ok(SkillInvocation {
            name: self.name.clone(),
            prompt,
            allowed_tools: self.allowed_tools.clone(),
            model: self.model.clone(),
            hooks: self.hooks.clone(),
            execution_context: self.execution_context,
            agent: self.agent.clone(),
            trusted_execution_metadata: self.trust == SkillTrust::Trusted,
        })
    }

    /// Converts permission-like entries such as `Bash(git:*)` to registry
    /// tool names for a fail-closed forked-agent capability scope.
    pub fn allowed_tool_names(&self) -> Result<Option<BTreeSet<String>>> {
        if self.allowed_tools.is_empty() || self.allowed_tools.contains("*") {
            return Ok(None);
        }
        let mut names = BTreeSet::new();
        for entry in &self.allowed_tools {
            let name = entry
                .split_once('(')
                .map_or(entry.as_str(), |(name, _)| name);
            validate_tool_name(name)?;
            names.insert(name.to_owned());
        }
        Ok(Some(names))
    }
}

pub fn discover_skills(cwd: &Path, bare: bool) -> Result<SkillCatalog> {
    if bare {
        return Ok(SkillCatalog::default());
    }
    let mut roots = Vec::new();
    if let Some(home) = dirs::home_dir() {
        roots.push((
            home.join(".open-agent-harness/skills"),
            home.join(".open-agent-harness"),
            SkillTrust::Trusted,
        ));
    }
    let mut ancestors = cwd.ancestors().collect::<Vec<_>>();
    ancestors.reverse();
    roots.extend(ancestors.into_iter().map(|directory| {
        (
            directory.join(".open-agent-harness/skills"),
            directory.to_path_buf(),
            SkillTrust::Project,
        )
    }));
    discover_from_roots(&roots)
}

pub fn render_skill_index(catalog: &SkillCatalog) -> String {
    if catalog.is_empty() {
        return String::new();
    }
    let entries = catalog
        .iter()
        .filter(|(_, skill)| !skill.disable_model_invocation)
        .map(|(name, skill)| {
            let description = skill
                .description
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let hint = skill
                .argument_hint
                .as_deref()
                .map(|hint| format!(" {hint}"))
                .unwrap_or_default();
            format!(
                "- {name}{hint}: {}",
                description.chars().take(240).collect::<String>()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    if entries.is_empty() {
        return String::new();
    }
    format!(
        "# Available local skills\n\nUse the Skill tool to load one of these user-provided workflows only when it is relevant:\n{entries}"
    )
}

fn discover_from_roots(roots: &[(PathBuf, PathBuf, SkillTrust)]) -> Result<SkillCatalog> {
    let mut catalog = SkillCatalog::default();
    for (root, scope_root, trust) in roots {
        if !root.is_dir() {
            continue;
        }
        let canonical_root = fs::canonicalize(root)
            .with_context(|| format!("无法解析 skills 目录 {}", root.display()))?;
        let canonical_scope = fs::canonicalize(scope_root)
            .with_context(|| format!("无法解析 skills 作用域 {}", scope_root.display()))?;
        if !canonical_root.starts_with(&canonical_scope) {
            bail!("skills 目录 symlink 越过作用域: {}", root.display())
        }
        let mut files = Vec::new();
        for (index, entry) in fs::read_dir(root)?.enumerate() {
            if index >= MAX_SKILL_DIRECTORY_ENTRIES {
                bail!(
                    "skills 目录 entry 数量超过 {MAX_SKILL_DIRECTORY_ENTRIES} 个限制: {}",
                    root.display()
                )
            }
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                continue;
            }
            let path = entry.path().join("SKILL.md");
            if path.is_file() {
                files.push(path);
            }
        }
        files.sort();
        for path in files {
            let (canonical, bytes) = read_confined_skill(&path, &canonical_root)?;
            let content = String::from_utf8(bytes)
                .with_context(|| format!("skill 不是有效 UTF-8: {}", canonical.display()))?;
            let fallback = canonical
                .parent()
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .context("skill 目录名不是有效 UTF-8")?;
            let parsed = parse_frontmatter(&content, fallback)?;
            let definition = SkillDefinition {
                name: parsed.name.clone(),
                description: parsed.description,
                path: canonical,
                content,
                prompt: parsed.prompt,
                allowed_tools: parsed.allowed_tools,
                argument_hint: parsed.argument_hint,
                argument_names: parsed.argument_names,
                model: parsed.model,
                disable_model_invocation: parsed.disable_model_invocation,
                user_invocable: parsed.user_invocable,
                hooks: parsed.hooks,
                execution_context: parsed.execution_context,
                agent: parsed.agent,
                trust: *trust,
            };
            if catalog.entries.get(&parsed.name).is_some_and(|existing| {
                existing.trust == SkillTrust::Trusted && *trust == SkillTrust::Project
            }) {
                continue;
            }
            catalog.entries.insert(parsed.name.clone(), definition);
            if catalog.len() > MAX_SKILLS {
                bail!("skill 数量超过 {MAX_SKILLS} 个限制")
            }
        }
    }
    Ok(catalog)
}

fn read_confined_skill(path: &Path, canonical_root: &Path) -> Result<(PathBuf, Vec<u8>)> {
    read_confined_skill_with_hooks(path, canonical_root, || {}, || {})
}

fn read_confined_skill_with_hooks(
    path: &Path,
    canonical_root: &Path,
    after_resolve: impl FnOnce(),
    after_open: impl FnOnce(),
) -> Result<(PathBuf, Vec<u8>)> {
    let resolved =
        fs::canonicalize(path).with_context(|| format!("无法解析 skill {}", path.display()))?;
    ensure_skill_path_confined(&resolved, canonical_root, path)?;
    after_resolve();

    let file = open_skill_file_no_follow(&resolved)?;
    validate_opened_skill_file(&file, &resolved)?;
    after_open();

    // The handle is authoritative. Re-resolving the submitted path and opening a
    // second no-follow handle closes replacement of either the file or an
    // intermediate directory between canonicalization and open.
    let final_path = opened_file_final_path(&file)?;
    ensure_skill_path_confined(&final_path, canonical_root, path)?;
    let current = fs::canonicalize(path)
        .with_context(|| format!("skill 路径在打开后发生变化: {}", path.display()))?;
    ensure_skill_path_confined(&current, canonical_root, path)?;
    if !same_platform_path(&final_path, &current) {
        bail!("skill 路径在安全打开期间发生变化: {}", path.display())
    }
    let verification = open_skill_file_no_follow(&current)?;
    validate_opened_skill_file(&verification, &current)?;
    let verification_path = opened_file_final_path(&verification)?;
    ensure_skill_path_confined(&verification_path, canonical_root, path)?;
    if !same_platform_path(&final_path, &verification_path)
        || !same_open_file_identity(&file, &verification)?
    {
        bail!("skill 文件身份在安全打开期间发生变化: {}", path.display())
    }

    let mut bytes = Vec::new();
    file.take(MAX_SKILL_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SKILL_BYTES as usize {
        bail!(
            "skill {} 超过 {} 字节限制",
            final_path.display(),
            MAX_SKILL_BYTES
        )
    }
    Ok((final_path, bytes))
}

fn ensure_skill_path_confined(candidate: &Path, root: &Path, submitted: &Path) -> Result<()> {
    if !platform_path_starts_with(candidate, root) {
        bail!("skill symlink 越过发现目录: {}", submitted.display())
    }
    Ok(())
}

fn open_skill_file_no_follow(path: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    #[cfg(not(any(unix, windows)))]
    {
        bail!("当前平台不支持安全打开 skill 文件")
    }
    options
        .open(path)
        .with_context(|| format!("无法安全打开 skill {}", path.display()))
}

fn validate_opened_skill_file(file: &fs::File, path: &Path) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("无法检查已打开的 skill {}", path.display()))?;
    if !metadata.is_file() || metadata.len() > MAX_SKILL_BYTES {
        bail!(
            "skill {} 不是普通文件或超过 {} 字节限制",
            path.display(),
            MAX_SKILL_BYTES
        )
    }
    #[cfg(windows)]
    {
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        if windows_file_identity(file)?.0 & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            bail!("skill 文件不得是 reparse point: {}", path.display())
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn opened_file_final_path(file: &fs::File) -> Result<PathBuf> {
    use std::os::fd::AsRawFd as _;

    fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .context("无法从已打开句柄复核 skill 最终路径")
}

#[cfg(target_os = "macos")]
fn opened_file_final_path(file: &fs::File) -> Result<PathBuf> {
    use std::{
        ffi::CStr,
        os::{fd::AsRawFd as _, unix::ffi::OsStrExt as _},
    };

    let mut buffer = [0 as libc::c_char; libc::PATH_MAX as usize];
    // SAFETY: `buffer` is writable for PATH_MAX bytes and F_GETPATH writes a
    // NUL-terminated path for the live descriptor without retaining the pointer.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETPATH, buffer.as_mut_ptr()) } == -1 {
        return Err(std::io::Error::last_os_error()).context("无法从已打开句柄复核 skill 最终路径");
    }
    // SAFETY: successful F_GETPATH guarantees a NUL-terminated string in buffer.
    let bytes = unsafe { CStr::from_ptr(buffer.as_ptr()) }.to_bytes();
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn opened_file_final_path(_: &fs::File) -> Result<PathBuf> {
    bail!("当前 Unix 平台不支持从句柄复核 skill 最终路径")
}

#[cfg(windows)]
fn opened_file_final_path(file: &fs::File) -> Result<PathBuf> {
    use std::{os::windows::ffi::OsStringExt as _, os::windows::io::AsRawHandle as _};

    let mut capacity = 512u32;
    loop {
        let mut buffer = vec![0u16; capacity as usize];
        // SAFETY: the live file handle is borrowed for this call and buffer is
        // writable for `capacity` UTF-16 code units.
        let length = unsafe {
            get_final_path_name_by_handle(file.as_raw_handle(), buffer.as_mut_ptr(), capacity, 0)
        };
        if length == 0 {
            return Err(std::io::Error::last_os_error())
                .context("无法从已打开句柄复核 skill 最终路径");
        }
        if length < capacity {
            buffer.truncate(length as usize);
            return Ok(PathBuf::from(std::ffi::OsString::from_wide(&buffer)));
        }
        capacity = length.saturating_add(1);
        if capacity > 32_768 {
            bail!("skill 最终路径超过 Windows 长路径限制")
        }
    }
}

fn same_open_file_identity(left: &fs::File, right: &fs::File) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let left = left.metadata()?;
        let right = right.metadata()?;
        Ok(left.dev() == right.dev() && left.ino() == right.ino())
    }
    #[cfg(windows)]
    {
        let left = windows_file_identity(left)?;
        let right = windows_file_identity(right)?;
        Ok(left.1 == right.1 && left.2 == right.2)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (left, right);
        bail!("当前平台不支持复核 skill 文件身份")
    }
}

#[cfg(windows)]
fn windows_file_identity(file: &fs::File) -> Result<(u32, u32, u64)> {
    use std::os::windows::io::AsRawHandle as _;

    // SAFETY: all-zero is a valid initial byte representation for this C output struct.
    let mut information: WindowsFileInformation = unsafe { std::mem::zeroed() };
    // SAFETY: the live file handle is borrowed for the call and `information`
    // points to writable storage of the exact Windows structure layout.
    if unsafe { get_file_information_by_handle(file.as_raw_handle(), &mut information) } == 0 {
        return Err(std::io::Error::last_os_error()).context("无法复核 Windows skill 文件身份");
    }
    Ok((
        information.attributes,
        information.volume_serial_number,
        (u64::from(information.file_index_high) << 32) | u64::from(information.file_index_low),
    ))
}

#[cfg(windows)]
fn platform_path_starts_with(path: &Path, root: &Path) -> bool {
    windows_path_key(path).starts_with(windows_path_key(root))
}

#[cfg(not(windows))]
fn platform_path_starts_with(path: &Path, root: &Path) -> bool {
    path.starts_with(root)
}

#[cfg(windows)]
fn same_platform_path(left: &Path, right: &Path) -> bool {
    windows_path_key(left) == windows_path_key(right)
}

#[cfg(not(windows))]
fn same_platform_path(left: &Path, right: &Path) -> bool {
    left == right
}

#[cfg(windows)]
fn windows_path_key(path: &Path) -> PathBuf {
    let lowered = path
        .as_os_str()
        .to_string_lossy()
        .replace('/', "\\")
        .to_lowercase();
    if let Some(rest) = lowered.strip_prefix(r"\\?\unc\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = lowered.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        PathBuf::from(lowered)
    }
}

/// Discovers one explicitly trusted skill contribution root. The canonical
/// root and every `SKILL.md` must remain inside `scope_root`.
pub fn discover_skill_root(root: &Path, scope_root: &Path) -> Result<SkillCatalog> {
    discover_from_roots(&[(root.to_owned(), scope_root.to_owned(), SkillTrust::Trusted)])
}

struct ParsedSkill {
    name: String,
    description: String,
    prompt: String,
    allowed_tools: BTreeSet<String>,
    argument_hint: Option<String>,
    argument_names: Vec<String>,
    model: Option<String>,
    disable_model_invocation: bool,
    user_invocable: bool,
    hooks: Option<Value>,
    execution_context: SkillExecutionContext,
    agent: Option<String>,
}

fn parse_frontmatter(content: &str, fallback: &str) -> Result<ParsedSkill> {
    let (frontmatter, prompt) = parse_markdown_frontmatter(content)?;
    let name = optional_string(&frontmatter, "name")?.unwrap_or_else(|| fallback.to_owned());
    validate_skill_name(&name)?;
    let description = optional_string(&frontmatter, "description")?
        .unwrap_or_else(|| extract_description(prompt, fallback));
    if description.trim().is_empty() || description.len() > MAX_SKILL_DESCRIPTION_BYTES {
        bail!("skill {name} description 为空或超过 {MAX_SKILL_DESCRIPTION_BYTES} 字节限制")
    }
    let allowed_tools = parse_string_set(frontmatter.get("allowed-tools"), "allowed-tools")?;
    if allowed_tools.len() > MAX_SKILL_ALLOWED_TOOLS {
        bail!("skill {name} allowed-tools 超过 {MAX_SKILL_ALLOWED_TOOLS} 项限制")
    }
    for tool in &allowed_tools {
        validate_allowed_tool_rule(tool)?;
    }
    let argument_hint = optional_string(&frontmatter, "argument-hint")?;
    if argument_hint
        .as_ref()
        .is_some_and(|hint| hint.len() > MAX_SKILL_ARGUMENT_HINT_BYTES || hint.contains('\0'))
    {
        bail!("skill {name} argument-hint 无效")
    }
    let argument_names = parse_argument_names(frontmatter.get("arguments"))?;
    let model = optional_string(&frontmatter, "model")?
        .filter(|model| model != "inherit")
        .map(|model| validate_model(model, &name))
        .transpose()?;
    let agent = optional_string(&frontmatter, "agent")?
        .map(|agent| {
            validate_skill_name(&agent)?;
            Ok::<_, anyhow::Error>(agent)
        })
        .transpose()?;
    let execution_context = match optional_string(&frontmatter, "context")?.as_deref() {
        None | Some("inline") => SkillExecutionContext::Inline,
        Some("fork") => SkillExecutionContext::Fork,
        Some(value) => bail!("skill {name} context 无效: {value}"),
    };
    if agent.is_some() && execution_context != SkillExecutionContext::Fork {
        bail!("skill {name} 只有 context: fork 才能指定 agent")
    }
    let disable_model_invocation =
        optional_bool(&frontmatter, "disable-model-invocation")?.unwrap_or(false);
    let user_invocable = optional_bool(&frontmatter, "user-invocable")?.unwrap_or(true);
    let hooks = frontmatter
        .get("hooks")
        .cloned()
        .map(validate_skill_hooks)
        .transpose()?;
    Ok(ParsedSkill {
        name,
        description,
        prompt: prompt.to_owned(),
        allowed_tools,
        argument_hint,
        argument_names,
        model,
        disable_model_invocation,
        user_invocable,
        hooks,
        execution_context,
        agent,
    })
}

#[derive(Debug)]
struct FrontmatterLine<'a> {
    indent: usize,
    text: &'a str,
}

pub(crate) fn parse_markdown_frontmatter(content: &str) -> Result<(Map<String, Value>, &str)> {
    let content = content.strip_prefix('\u{feff}').unwrap_or(content);
    let Some(rest) = content.strip_prefix("---\n") else {
        return Ok((Map::new(), content));
    };
    let Some(end) = rest
        .match_indices('\n')
        .find_map(|(index, _)| (rest[index + 1..].starts_with("---\n")).then_some(index))
    else {
        bail!("skill frontmatter 缺少独立的结束分隔符")
    };
    let source = &rest[..end];
    if source.len() > MAX_SKILL_FRONTMATTER_BYTES {
        bail!("skill frontmatter 超过 {MAX_SKILL_FRONTMATTER_BYTES} 字节限制")
    }
    let prompt_start = end + "\n---\n".len();
    let prompt = &rest[prompt_start..];
    let lines = tokenize_frontmatter(source)?;
    if lines.is_empty() {
        return Ok((Map::new(), prompt));
    }
    if lines[0].indent != 0 || lines[0].text.starts_with('-') {
        bail!("skill frontmatter 顶层必须是 mapping")
    }
    let (value, consumed) = parse_yaml_node(&lines, 0, 0)?;
    if consumed != lines.len() {
        bail!("skill frontmatter 含无法解析的尾部内容")
    }
    let object = value
        .as_object()
        .context("skill frontmatter 顶层必须是 mapping")?
        .clone();
    Ok((object, prompt))
}

fn tokenize_frontmatter(source: &str) -> Result<Vec<FrontmatterLine<'_>>> {
    let mut lines = Vec::new();
    for raw in source.lines() {
        if raw.contains('\t') {
            bail!("skill frontmatter 不接受 tab 缩进")
        }
        let text = raw.trim_end();
        let trimmed = text.trim_start_matches(' ');
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = text.len() - trimmed.len();
        lines.push(FrontmatterLine {
            indent,
            text: trimmed,
        });
    }
    Ok(lines)
}

fn parse_yaml_node(
    lines: &[FrontmatterLine<'_>],
    start: usize,
    indent: usize,
) -> Result<(Value, usize)> {
    let line = lines.get(start).context("frontmatter node 缺失")?;
    if line.indent != indent {
        bail!("frontmatter 缩进不一致")
    }
    if line.text == "-" || line.text.starts_with("- ") {
        parse_yaml_sequence(lines, start, indent)
    } else {
        parse_yaml_mapping(lines, start, indent)
    }
}

fn parse_yaml_mapping(
    lines: &[FrontmatterLine<'_>],
    mut index: usize,
    indent: usize,
) -> Result<(Value, usize)> {
    let mut object = Map::new();
    while let Some(line) = lines.get(index) {
        if line.indent < indent {
            break;
        }
        if line.indent != indent || line.text == "-" || line.text.starts_with("- ") {
            break;
        }
        let (key, raw) = split_yaml_mapping_entry(line.text)?;
        if object.contains_key(key) {
            bail!("skill frontmatter 包含重复字段 {key}")
        }
        index += 1;
        let value = if raw.is_empty() {
            if lines.get(index).is_some_and(|next| next.indent > indent) {
                let child_indent = lines[index].indent;
                let (value, next) = parse_yaml_node(lines, index, child_indent)?;
                index = next;
                value
            } else {
                Value::Null
            }
        } else {
            parse_yaml_scalar(raw)?
        };
        object.insert(key.to_owned(), value);
    }
    Ok((Value::Object(object), index))
}

fn parse_yaml_sequence(
    lines: &[FrontmatterLine<'_>],
    mut index: usize,
    indent: usize,
) -> Result<(Value, usize)> {
    let mut values = Vec::new();
    while let Some(line) = lines.get(index) {
        if line.indent != indent || !(line.text == "-" || line.text.starts_with("- ")) {
            break;
        }
        let raw = line
            .text
            .strip_prefix('-')
            .expect("sequence marker")
            .trim_start();
        index += 1;
        if raw.is_empty() {
            if lines.get(index).is_none_or(|next| next.indent <= indent) {
                values.push(Value::Null);
                continue;
            }
            let child_indent = lines[index].indent;
            let (value, next) = parse_yaml_node(lines, index, child_indent)?;
            index = next;
            values.push(value);
            continue;
        }
        if looks_like_yaml_mapping_entry(raw) {
            let map_indent = indent.saturating_add(2);
            let (key, first_raw) = split_yaml_mapping_entry(raw)?;
            let mut object = Map::new();
            let first_value = if first_raw.is_empty() {
                if lines
                    .get(index)
                    .is_some_and(|next| next.indent > map_indent)
                {
                    let child_indent = lines[index].indent;
                    let (value, next) = parse_yaml_node(lines, index, child_indent)?;
                    index = next;
                    value
                } else {
                    Value::Null
                }
            } else {
                parse_yaml_scalar(first_raw)?
            };
            object.insert(key.to_owned(), first_value);
            while lines.get(index).is_some_and(|next| {
                next.indent == map_indent && next.text != "-" && !next.text.starts_with("- ")
            }) {
                let (tail, next) = parse_yaml_mapping(lines, index, map_indent)?;
                index = next;
                for (key, value) in tail.as_object().expect("mapping parser") {
                    if object.insert(key.clone(), value.clone()).is_some() {
                        bail!("skill frontmatter sequence mapping 包含重复字段 {key}")
                    }
                }
            }
            values.push(Value::Object(object));
        } else {
            values.push(parse_yaml_scalar(raw)?);
        }
    }
    Ok((Value::Array(values), index))
}

fn split_yaml_mapping_entry(value: &str) -> Result<(&str, &str)> {
    let mut quoted = None;
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted == Some('"') {
            escaped = true;
            continue;
        }
        if matches!(character, '\'' | '"') {
            if quoted == Some(character) {
                quoted = None;
            } else if quoted.is_none() {
                quoted = Some(character);
            }
            continue;
        }
        if character == ':' && quoted.is_none() {
            let key = value[..index].trim();
            if key.is_empty()
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            {
                bail!("skill frontmatter 字段名无效: {key:?}")
            }
            return Ok((key, value[index + 1..].trim()));
        }
    }
    bail!("skill frontmatter mapping 行缺少冒号: {value}")
}

fn looks_like_yaml_mapping_entry(value: &str) -> bool {
    let mut quoted = None;
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quoted == Some('"') {
            escaped = true;
            continue;
        }
        if matches!(character, '\'' | '"') {
            if quoted == Some(character) {
                quoted = None;
            } else if quoted.is_none() {
                quoted = Some(character);
            }
            continue;
        }
        if character == ':' && quoted.is_none() {
            let key = value[..index].trim();
            let separator = value[index + 1..]
                .chars()
                .next()
                .is_none_or(char::is_whitespace);
            return separator
                && !key.is_empty()
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
        }
    }
    false
}

fn parse_yaml_scalar(raw: &str) -> Result<Value> {
    let raw = raw.trim();
    if raw.starts_with('[') || raw.starts_with('{') || raw.starts_with('"') {
        return serde_json::from_str(raw)
            .with_context(|| format!("frontmatter inline JSON 无效: {raw}"));
    }
    if raw.starts_with('\'') {
        if raw.len() < 2 || !raw.ends_with('\'') {
            bail!("frontmatter 单引号字符串未闭合")
        }
        return Ok(Value::String(raw[1..raw.len() - 1].replace("''", "'")));
    }
    match raw {
        "true" => Ok(Value::Bool(true)),
        "false" => Ok(Value::Bool(false)),
        "null" | "~" => Ok(Value::Null),
        _ => Ok(Value::String(raw.to_owned())),
    }
}

fn optional_string(values: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match values.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() && !value.contains('\0') => {
            Ok(Some(value.trim().to_owned()))
        }
        Some(_) => bail!("skill frontmatter.{key} 必须是非空 string"),
    }
}

fn optional_bool(values: &Map<String, Value>, key: &str) -> Result<Option<bool>> {
    match values.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(Value::String(value)) if value == "true" => Ok(Some(true)),
        Some(Value::String(value)) if value == "false" => Ok(Some(false)),
        Some(_) => bail!("skill frontmatter.{key} 必须是 boolean"),
    }
}

fn parse_string_set(value: Option<&Value>, label: &str) -> Result<BTreeSet<String>> {
    let values = parse_string_list(value, label)?;
    let set = values.iter().cloned().collect::<BTreeSet<_>>();
    if set.len() != values.len() {
        bail!("skill {label} 包含重复项")
    }
    Ok(set)
}

fn parse_string_list(value: Option<&Value>, label: &str) -> Result<Vec<String>> {
    let raw = match value {
        None | Some(Value::Null) => return Ok(Vec::new()),
        Some(Value::String(value)) => value
            .split(|character: char| character == ',' || character.is_whitespace())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.trim().is_empty())
                    .map(|value| value.trim().to_owned())
                    .with_context(|| format!("skill {label} 只能包含非空 string"))
            })
            .collect::<Result<Vec<_>>>()?,
        Some(_) => bail!("skill {label} 必须是 string 或 string array"),
    };
    Ok(raw)
}

fn parse_argument_names(value: Option<&Value>) -> Result<Vec<String>> {
    let names = parse_string_list(value, "arguments")?;
    if names.len() > MAX_SKILL_ARGUMENT_NAMES {
        bail!("skill arguments 超过 {MAX_SKILL_ARGUMENT_NAMES} 项限制")
    }
    let mut unique = BTreeSet::new();
    for name in &names {
        if name.len() > 64
            || !name.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_alphanumeric() || byte == b'_' || (index > 0 && byte == b'-')
            })
            || name.bytes().all(|byte| byte.is_ascii_digit())
            || !unique.insert(name)
        {
            bail!("skill argument name 无效或重复: {name}")
        }
    }
    Ok(names)
}

fn validate_model(model: String, skill: &str) -> Result<String> {
    if model.len() > MAX_SKILL_MODEL_BYTES || model.contains('\0') {
        bail!("skill {skill} model 无效")
    }
    Ok(model)
}

fn validate_skill_hooks(value: Value) -> Result<Value> {
    if !value.is_object() {
        bail!("skill hooks 必须是 object")
    }
    if serde_json::to_vec(&value)?.len() > MAX_SKILL_HOOK_BYTES {
        bail!("skill hooks 超过 {MAX_SKILL_HOOK_BYTES} 字节限制")
    }
    for (event, rules) in value.as_object().expect("object checked") {
        if event.is_empty() || event.len() > 128 || !rules.is_array() {
            bail!("skill hooks.{event} 必须是 bounded array")
        }
    }
    if contains_async_hook(&value) {
        bail!("skill scoped hooks 不接受 async 命令，以保证取消时作用域可回收")
    }
    Ok(value)
}

fn contains_async_hook(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.get("async").and_then(Value::as_bool) == Some(true)
                || object.values().any(contains_async_hook)
        }
        Value::Array(values) => values.iter().any(contains_async_hook),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn extract_description(prompt: &str, fallback: &str) -> String {
    prompt
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| line.trim_start_matches('#').trim())
        .filter(|line| !line.is_empty())
        .map(|line| line.chars().take(240).collect())
        .unwrap_or_else(|| format!("Local workflow from {fallback}"))
}

fn validate_allowed_tool_rule(rule: &str) -> Result<()> {
    if rule == "*" {
        return Ok(());
    }
    if rule.len() > 256 || rule.contains(['\0', '\n', '\r']) {
        bail!("skill allowed-tools rule 无效")
    }
    let tool = rule.split_once('(').map_or(rule, |(tool, suffix)| {
        if !suffix.ends_with(')') || suffix[..suffix.len() - 1].contains(['\n', '\r']) {
            return "";
        }
        tool
    });
    validate_tool_name(tool)
}

fn validate_tool_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b':'))
    {
        bail!("skill tool name 无效: {name:?}")
    }
    Ok(())
}

fn substitute_arguments(content: &str, arguments: &str, names: &[String]) -> Result<String> {
    if arguments.len() > MAX_SKILL_ARGUMENT_BYTES {
        bail!("skill arguments 超过 {MAX_SKILL_ARGUMENT_BYTES} 字节限制")
    }
    let parsed = parse_arguments(arguments)?;
    let mut rendered = content.to_owned();
    for (index, name) in names.iter().enumerate() {
        rendered = replace_named_argument(
            &rendered,
            name,
            parsed.get(index).map_or("", String::as_str),
        );
    }
    let indexed = Regex::new(r"\$ARGUMENTS\[(\d+)\]")?;
    rendered = indexed
        .replace_all(&rendered, |captures: &regex::Captures<'_>| {
            captures[1]
                .parse::<usize>()
                .ok()
                .and_then(|index| parsed.get(index))
                .map_or("", String::as_str)
                .to_owned()
        })
        .into_owned();
    let shorthand = Regex::new(r"\$(\d+)(?:\b|$)")?;
    rendered = shorthand
        .replace_all(&rendered, |captures: &regex::Captures<'_>| {
            captures[1]
                .parse::<usize>()
                .ok()
                .and_then(|index| parsed.get(index))
                .map_or("", String::as_str)
                .to_owned()
        })
        .into_owned();
    let had_placeholder = rendered != content || rendered.contains("$ARGUMENTS");
    rendered = rendered.replace("$ARGUMENTS", arguments);
    if !had_placeholder && !arguments.trim().is_empty() {
        rendered.push_str("\n\nARGUMENTS: ");
        rendered.push_str(arguments);
    }
    Ok(rendered)
}

fn replace_named_argument(content: &str, name: &str, replacement: &str) -> String {
    let needle = format!("${name}");
    let mut rendered = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(index) = rest.find(&needle) {
        rendered.push_str(&rest[..index]);
        let after = &rest[index + needle.len()..];
        if after.chars().next().is_some_and(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '['
        }) {
            rendered.push_str(&needle);
        } else {
            rendered.push_str(replacement);
        }
        rest = after;
    }
    rendered.push_str(rest);
    rendered
}

fn parse_arguments(arguments: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    for character in arguments.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' && quote != Some('\'') {
            escaped = true;
            continue;
        }
        if matches!(character, '\'' | '"') {
            if quote == Some(character) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(character);
            } else {
                current.push(character);
            }
            continue;
        }
        if character.is_whitespace() && quote.is_none() {
            if !current.is_empty() {
                values.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
        }
    }
    if escaped || quote.is_some() {
        bail!("skill arguments 含未闭合的 quote 或 escape")
    }
    if !current.is_empty() {
        values.push(current);
    }
    Ok(values)
}

fn validate_skill_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_-:".contains(character))
    {
        bail!("无效 skill name: {name}")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearer_skill_roots_override_broader_roots() {
        let temp = tempfile::tempdir().unwrap();
        let broad = temp.path().join("broad");
        let near = temp.path().join("near");
        for (root, marker) in [(&broad, "broad"), (&near, "near")] {
            let skill = root.join("demo");
            fs::create_dir_all(&skill).unwrap();
            fs::write(
                skill.join("SKILL.md"),
                format!("---\nname: demo\ndescription: {marker}\n---\n{marker}"),
            )
            .unwrap();
        }
        let catalog = discover_from_roots(&[
            (broad.clone(), broad, SkillTrust::Project),
            (near.clone(), near, SkillTrust::Project),
        ])
        .unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.get("demo").unwrap().description, "near");
        assert!(render_skill_index(&catalog).contains("demo: near"));
    }

    #[test]
    fn confined_skill_reader_accepts_stable_regular_file() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("skills");
        let skill = root.join("demo/SKILL.md");
        fs::create_dir_all(skill.parent().unwrap()).unwrap();
        fs::write(&skill, "---\nname: demo\n---\nStable workflow").unwrap();

        let canonical_root = fs::canonicalize(&root).unwrap();
        let (resolved, bytes) = read_confined_skill(&skill, &canonical_root).unwrap();
        assert!(resolved.starts_with(&canonical_root));
        assert_eq!(bytes, fs::read(&skill).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn project_skill_root_cannot_escape_its_scope() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let private = temp.path().join("private-skills");
        fs::create_dir_all(private.join("secret")).unwrap();
        fs::write(private.join("secret/SKILL.md"), "secret").unwrap();
        fs::create_dir_all(workspace.join(".open-agent-harness")).unwrap();
        symlink(&private, workspace.join(".open-agent-harness/skills")).unwrap();
        let root = workspace.join(".open-agent-harness/skills");
        let error = discover_from_roots(&[(root, workspace, SkillTrust::Project)]).unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn skill_file_replacement_with_symlink_before_open_fails_closed() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("skills");
        let path = root.join("demo/SKILL.md");
        let backup = root.join("demo/original.md");
        let outside = temp.path().join("outside.md");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "safe workflow").unwrap();
        fs::write(&outside, "outside secret").unwrap();
        let canonical_root = fs::canonicalize(&root).unwrap();
        let raced_path = path.clone();

        let error = read_confined_skill_with_hooks(
            &path,
            &canonical_root,
            move || {
                fs::rename(&raced_path, &backup).unwrap();
                symlink(&outside, &raced_path).unwrap();
            },
            || {},
        )
        .unwrap_err();
        assert!(error.to_string().contains("安全打开"));
    }

    #[cfg(unix)]
    #[test]
    fn skill_file_replacement_after_open_fails_identity_recheck() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("skills");
        let path = root.join("demo/SKILL.md");
        let backup = root.join("demo/original.md");
        let outside = temp.path().join("outside.md");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "safe workflow").unwrap();
        fs::write(&outside, "outside secret").unwrap();
        let canonical_root = fs::canonicalize(&root).unwrap();
        let raced_path = path.clone();

        let error = read_confined_skill_with_hooks(
            &path,
            &canonical_root,
            || {},
            move || {
                fs::rename(&raced_path, &backup).unwrap();
                symlink(&outside, &raced_path).unwrap();
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("symlink") || error.to_string().contains("发生变化"));
    }

    #[test]
    fn direct_invocation_uses_catalog_backed_internal_marker() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("skills");
        fs::create_dir_all(root.join("audit")).unwrap();
        let source = "---\nname: audit\ndescription: Audit workflow\narguments: target mode\n---\nCheck $target using $1 and $ARGUMENTS.\n";
        fs::write(root.join("audit/SKILL.md"), source).unwrap();
        let catalog = discover_skill_root(&root, temp.path()).unwrap();
        let rendered = catalog
            .render_invocation("audit", "'target src/' strict")
            .unwrap();
        assert!(!rendered.contains("description:"));
        let (name, arguments) = decode_user_skill_submission(&Value::String(rendered))
            .unwrap()
            .unwrap();
        assert_eq!(name, "audit");
        assert_eq!(arguments, "'target src/' strict");
        let invocation = catalog
            .prepare_invocation(&name, &arguments, SkillInvocationSource::User)
            .unwrap();
        assert!(invocation.prompt.contains("Check target src/ using strict"));
        assert!(
            catalog
                .render_invocation("audit", &"x".repeat(MAX_SKILL_ARGUMENT_BYTES + 1))
                .is_err()
        );
    }

    #[test]
    fn plugin_skill_namespaces_are_valid_and_bounded() {
        let mut catalog = SkillCatalog::default();
        catalog.entries.insert(
            "review".into(),
            SkillDefinition {
                name: "review".into(),
                description: "Review".into(),
                path: PathBuf::from("review/SKILL.md"),
                content: "review".into(),
                prompt: "review".into(),
                allowed_tools: BTreeSet::new(),
                argument_hint: None,
                argument_names: Vec::new(),
                model: None,
                disable_model_invocation: false,
                user_invocable: true,
                hooks: None,
                execution_context: SkillExecutionContext::Fork,
                agent: Some("reviewer".into()),
                trust: SkillTrust::Trusted,
            },
        );
        let catalog = catalog.namespaced("quality").unwrap();
        assert_eq!(
            catalog.get("quality:review").unwrap().agent.as_deref(),
            Some("quality:reviewer")
        );
        assert!(catalog.namespaced("bad/name").is_err());
    }

    #[test]
    fn parses_full_provider_neutral_skill_metadata_and_yaml_hooks() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("skills");
        fs::create_dir_all(root.join("audit")).unwrap();
        fs::write(
            root.join("audit/SKILL.md"),
            r#"---
name: audit
description: Audits a target
allowed-tools:
  - Read
  - Bash(git:*)
argument-hint: "[target] [mode]"
arguments: ["target", "mode"]
model: local-review-model
context: fork
agent: reviewer
user-invocable: false
disable-model-invocation: false
hooks:
  PreToolUse:
    - matcher: Bash
      hooks:
        - type: command
          command: ./check
---
Audit $target in $mode.
"#,
        )
        .unwrap();
        let catalog = discover_skill_root(&root, temp.path()).unwrap();
        let skill = catalog.get("audit").unwrap();
        assert_eq!(skill.execution_context, SkillExecutionContext::Fork);
        assert_eq!(skill.model.as_deref(), Some("local-review-model"));
        assert_eq!(skill.agent.as_deref(), Some("reviewer"));
        assert!(skill.hooks.as_ref().unwrap()["PreToolUse"].is_array());
        assert_eq!(
            skill.allowed_tool_names().unwrap().unwrap(),
            BTreeSet::from(["Bash".to_owned(), "Read".to_owned()])
        );
        assert!(catalog.render_invocation("audit", "src strict").is_err());
        let invocation = catalog
            .prepare_invocation("audit", "src strict", SkillInvocationSource::Model)
            .unwrap();
        assert_eq!(invocation.prompt.trim(), "Audit src in strict.");
    }

    #[test]
    fn invocation_flags_are_enforced_by_source_and_hidden_from_model_index() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("skills");
        fs::create_dir_all(root.join("manual")).unwrap();
        fs::write(
            root.join("manual/SKILL.md"),
            "---\nname: manual\ndisable-model-invocation: true\n---\nManual only",
        )
        .unwrap();
        let catalog = discover_skill_root(&root, temp.path()).unwrap();
        assert!(render_skill_index(&catalog).is_empty());
        assert!(catalog.render_invocation("manual", "").is_ok());
        assert!(
            catalog
                .prepare_invocation("manual", "", SkillInvocationSource::Model)
                .is_err()
        );
    }

    #[test]
    fn project_skills_cannot_attach_executable_or_routing_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project-skills");
        fs::create_dir_all(root.join("unsafe")).unwrap();
        fs::write(
            root.join("unsafe/SKILL.md"),
            r#"---
name: unsafe
allowed-tools: "*"
model: expensive-model
context: fork
agent: privileged-agent
hooks:
  UserPromptSubmit:
    - hooks:
        - type: command
          command: /bin/false
---
Never reached.
"#,
        )
        .unwrap();
        let catalog = discover_from_roots(&[(root.clone(), root, SkillTrust::Project)]).unwrap();
        let skill = catalog.get("unsafe").unwrap();
        assert_eq!(skill.trust, SkillTrust::Project);
        for source in [SkillInvocationSource::User, SkillInvocationSource::Model] {
            let error = skill.prepare_invocation("", source).unwrap_err();
            assert!(error.to_string().contains("project skill"));
        }
        assert!(catalog.render_invocation("unsafe", "").is_err());
    }

    #[test]
    fn project_allowed_tools_only_narrow_and_trusted_skill_names_cannot_be_shadowed() {
        let temp = tempfile::tempdir().unwrap();
        let trusted = temp.path().join("trusted");
        let project = temp.path().join("project");
        fs::create_dir_all(trusted.join("audit")).unwrap();
        fs::create_dir_all(project.join("audit")).unwrap();
        fs::create_dir_all(project.join("local")).unwrap();
        fs::write(
            trusted.join("audit/SKILL.md"),
            "---\nname: audit\ndescription: Trusted audit\n---\nTrusted workflow",
        )
        .unwrap();
        fs::write(
            project.join("audit/SKILL.md"),
            "---\nname: audit\ndescription: Shadow\n---\nProject shadow",
        )
        .unwrap();
        fs::write(
            project.join("local/SKILL.md"),
            "---\nname: local\nallowed-tools: Bash(git:*)\n---\nLocal workflow",
        )
        .unwrap();
        let catalog = discover_from_roots(&[
            (trusted.clone(), trusted.clone(), SkillTrust::Trusted),
            (project.clone(), project.clone(), SkillTrust::Project),
        ])
        .unwrap();
        assert_eq!(catalog.get("audit").unwrap().description, "Trusted audit");
        let invocation = catalog
            .prepare_invocation("local", "", SkillInvocationSource::User)
            .unwrap();
        assert!(!invocation.trusted_execution_metadata);
        assert_eq!(
            invocation.allowed_tools,
            BTreeSet::from(["Bash(git:*)".to_owned()])
        );

        let mut merged =
            discover_from_roots(&[(project.clone(), project, SkillTrust::Project)]).unwrap();
        merged
            .merge(discover_from_roots(&[(trusted.clone(), trusted, SkillTrust::Trusted)]).unwrap())
            .unwrap();
        assert_eq!(merged.get("audit").unwrap().description, "Trusted audit");
    }

    #[test]
    fn malformed_frontmatter_and_unclosed_arguments_fail_closed() {
        assert!(parse_markdown_frontmatter("---\nname: demo\nbody").is_err());
        assert!(parse_arguments("one 'two").is_err());
        assert!(parse_argument_names(Some(&serde_json::json!(["ok", "1"]))).is_err());
    }
}

use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
};

#[cfg(any(target_os = "macos", target_os = "linux"))]
use std::process::{Command as StdCommand, Stdio};
#[cfg(any(target_os = "macos", target_os = "linux", test))]
use std::{collections::HashSet, path::Component};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::Value;
use tokio::process::Command;

#[cfg(target_os = "linux")]
use crate::sandbox_proxy::BRIDGE_PORT;
#[cfg(any(target_os = "macos", target_os = "linux"))]
use crate::sandbox_proxy::DomainProxy;

#[cfg(unix)]
pub use crate::sandbox_proxy::maybe_run_proxy_bridge;

#[cfg(not(unix))]
pub fn maybe_run_proxy_bridge() -> Option<Result<i32>> {
    let arguments = std::env::args_os().collect::<Vec<_>>();
    (arguments.get(1).and_then(|value| value.to_str())
        == Some("__open_agent_harness_sandbox_proxy_bridge"))
    .then(|| Err(anyhow::anyhow!("sandbox proxy bridge 只支持 Unix")))
}

const MAX_POLICY_PATHS: usize = 64;
const MAX_POLICY_PATH_BYTES: usize = 4096;
const MAX_POLICY_TOTAL_BYTES: usize = 64 * 1024;
const MAX_ALLOWED_DOMAINS: usize = 64;
const MAX_DOMAIN_BYTES: usize = 253;

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SandboxConfig {
    pub enabled: bool,
    pub fail_if_unavailable: bool,
    pub filesystem: SandboxFilesystemConfig,
    pub network: SandboxNetworkConfig,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SandboxFilesystemConfig {
    pub allow_write: Vec<String>,
    pub deny_read: Vec<String>,
    pub deny_write: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct SandboxNetworkConfig {
    /// When non-empty, direct IP networking is denied and commands receive only an
    /// authenticated, DNS-pinning proxy constrained to these exact or `*.` domains.
    pub allowed_domains: Vec<String>,
    /// Private/reserved destinations remain denied after DNS resolution unless the
    /// trusted user settings explicitly opt in here.
    pub allow_private_network: bool,
    /// Deny all network access. Both supported backends can enforce this mode.
    pub deny: bool,
}

#[derive(Debug, Clone)]
enum Backend {
    Disabled,
    #[cfg(target_os = "macos")]
    MacOs(PathBuf),
    #[cfg(target_os = "linux")]
    Linux(PathBuf),
    Unavailable(String),
}

#[derive(Debug, Clone)]
pub struct SandboxRuntime {
    config: SandboxConfig,
    backend: Backend,
    #[cfg(any(target_os = "macos", target_os = "linux"))]
    proxy: Option<DomainProxy>,
}

pub struct PreparedCommand {
    command: Command,
    warning: Option<String>,
    sandboxed: bool,
}

impl PreparedCommand {
    pub fn into_parts(self) -> (Command, Option<String>) {
        (self.command, self.warning)
    }

    pub fn is_sandboxed(&self) -> bool {
        self.sandboxed
    }
}

impl Default for SandboxRuntime {
    fn default() -> Self {
        Self {
            config: SandboxConfig::default(),
            backend: Backend::Disabled,
            #[cfg(any(target_os = "macos", target_os = "linux"))]
            proxy: None,
        }
    }
}

impl SandboxRuntime {
    pub fn from_settings(settings: &Value) -> Result<Self> {
        let config = match settings.get("sandbox") {
            None | Some(Value::Null) => SandboxConfig::default(),
            Some(value) => {
                serde_json::from_value(value.clone()).context("sandbox settings 不是有效配置")?
            }
        };
        Self::new(config)
    }

    pub fn new(config: SandboxConfig) -> Result<Self> {
        validate_config(&config)?;
        if !config.enabled {
            return Ok(Self {
                config,
                backend: Backend::Disabled,
                #[cfg(any(target_os = "macos", target_os = "linux"))]
                proxy: None,
            });
        }

        let mut backend = detect_backend(&config);
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        let proxy = if config.network.allowed_domains.is_empty()
            || matches!(backend, Backend::Unavailable(_))
        {
            None
        } else {
            match DomainProxy::start(
                &config.network.allowed_domains,
                config.network.allow_private_network,
            ) {
                Ok(proxy) => Some(proxy),
                Err(error) => {
                    backend =
                        Backend::Unavailable(format!("trusted domain proxy 启动失败: {error:#}"));
                    None
                }
            }
        };
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let _ = &mut backend;
        if config.fail_if_unavailable {
            if let Backend::Unavailable(reason) = &backend {
                bail!("sandbox 是必需的，但当前不可用: {reason}")
            }
        }
        Ok(Self {
            config,
            backend,
            #[cfg(any(target_os = "macos", target_os = "linux"))]
            proxy,
        })
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn available(&self) -> bool {
        !matches!(self.backend, Backend::Disabled | Backend::Unavailable(_))
    }

    pub fn unavailable_reason(&self) -> Option<&str> {
        match &self.backend {
            Backend::Unavailable(reason) => Some(reason),
            _ => None,
        }
    }

    /// Extends an already trusted sandbox configuration with session-only
    /// workspace roots. This is called only for explicit CLI directories;
    /// project settings cannot reach it or widen sandbox scope.
    pub fn with_session_workspaces(mut self, roots: &[PathBuf]) -> Result<Self> {
        for root in roots {
            let canonical = fs::canonicalize(root)
                .with_context(|| format!("无法解析 sandbox 工作区 {}", root.display()))?;
            if !canonical.is_dir() {
                bail!("sandbox 工作区不是目录: {}", root.display())
            }
            let rendered = canonical
                .to_str()
                .context("sandbox 工作区路径必须是 UTF-8")?
                .to_owned();
            if !self.config.filesystem.allow_write.contains(&rendered) {
                self.config.filesystem.allow_write.push(rendered);
            }
            let denied = canonical.join(".open-agent-harness");
            let rendered = denied
                .to_str()
                .context("sandbox 保护路径必须是 UTF-8")?
                .to_owned();
            if !self.config.filesystem.deny_write.contains(&rendered) {
                self.config.filesystem.deny_write.push(rendered);
            }
        }
        validate_config(&self.config)?;
        Ok(self)
    }

    /// Build an OS-sandboxed invocation around an already selected shell and argument list.
    /// The caller remains responsible for cwd, stdio, environment scrubbing, process groups,
    /// timeouts, and tree cleanup.
    pub fn command(
        &self,
        cwd: &Path,
        shell: &OsStr,
        shell_args: &[OsString],
    ) -> Result<PreparedCommand> {
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        let _ = cwd;
        match &self.backend {
            Backend::Disabled => Ok(direct_command(shell, shell_args, None)),
            Backend::Unavailable(reason) => self.unavailable_command(shell, shell_args, reason),
            #[cfg(target_os = "macos")]
            Backend::MacOs(program) => match macos_profile(
                &self.config,
                cwd,
                self.proxy.as_ref().map(DomainProxy::tcp_port),
            ) {
                Ok(profile) => {
                    let mut command = Command::new(program);
                    command.arg("-p").arg(profile).arg("--").arg(shell);
                    command.args(shell_args);
                    if let Some(proxy) = &self.proxy {
                        configure_proxy_environment(
                            &mut command,
                            proxy.http_url(),
                            proxy.socks_url(),
                        );
                    }
                    Ok(PreparedCommand {
                        command,
                        warning: None,
                        sandboxed: true,
                    })
                }
                Err(_) => self.unavailable_command(
                    shell,
                    shell_args,
                    "filesystem policy could not be represented by sandbox-exec",
                ),
            },
            #[cfg(target_os = "linux")]
            Backend::Linux(program) => match linux_arguments(&self.config, cwd) {
                Ok(arguments) => {
                    let mut command = Command::new(program);
                    command.args(arguments).arg("--");
                    if let Some(proxy) = &self.proxy {
                        let executable = env::current_exe()
                            .context("无法定位 sandbox proxy bridge executable")?;
                        let socket = proxy.unix_socket();
                        command
                            .arg(executable)
                            .arg("__open_agent_harness_sandbox_proxy_bridge")
                            .arg(socket)
                            .arg(BRIDGE_PORT.to_string())
                            .arg(proxy.token())
                            .arg("--")
                            .arg(shell)
                            .args(shell_args);
                    } else {
                        command.arg(shell).args(shell_args);
                    }
                    Ok(PreparedCommand {
                        command,
                        warning: None,
                        sandboxed: true,
                    })
                }
                Err(_) => self.unavailable_command(
                    shell,
                    shell_args,
                    "filesystem policy could not be represented by bubblewrap",
                ),
            },
        }
    }

    fn unavailable_command(
        &self,
        shell: &OsStr,
        shell_args: &[OsString],
        reason: &str,
    ) -> Result<PreparedCommand> {
        if self.config.fail_if_unavailable
            || self.config.network.deny
            || !self.config.network.allowed_domains.is_empty()
        {
            bail!("sandbox 是必需的，但当前命令无法隔离: {reason}")
        }
        Ok(direct_command(
            shell,
            shell_args,
            Some(format!(
                "requested isolation is unavailable; command ran unsandboxed: {reason}"
            )),
        ))
    }

    #[cfg(test)]
    fn with_backend(config: SandboxConfig, backend: Backend) -> Result<Self> {
        validate_config(&config)?;
        if config.fail_if_unavailable {
            if let Backend::Unavailable(reason) = &backend {
                bail!("sandbox 是必需的，但当前不可用: {reason}")
            }
        }
        Ok(Self {
            config,
            backend,
            #[cfg(any(target_os = "macos", target_os = "linux"))]
            proxy: None,
        })
    }
}

fn direct_command(
    shell: &OsStr,
    shell_args: &[OsString],
    warning: Option<String>,
) -> PreparedCommand {
    let mut command = Command::new(shell);
    command.args(shell_args);
    PreparedCommand {
        command,
        warning,
        sandboxed: false,
    }
}

fn validate_config(config: &SandboxConfig) -> Result<()> {
    let lists = [
        ("filesystem.allowWrite", &config.filesystem.allow_write),
        ("filesystem.denyRead", &config.filesystem.deny_read),
        ("filesystem.denyWrite", &config.filesystem.deny_write),
    ];
    let mut total = 0usize;
    for (name, paths) in lists {
        if paths.len() > MAX_POLICY_PATHS {
            bail!("sandbox.{name} 超过 {MAX_POLICY_PATHS} 项限制")
        }
        for path in paths {
            if path.is_empty() || path.len() > MAX_POLICY_PATH_BYTES {
                bail!("sandbox.{name} 包含空路径或超长路径")
            }
            if path.contains(['\0', '\n', '\r']) {
                bail!("sandbox.{name} 路径包含不支持的控制字符")
            }
            total = total.saturating_add(path.len());
        }
    }
    if total > MAX_POLICY_TOTAL_BYTES {
        bail!("sandbox filesystem policy 超过 {MAX_POLICY_TOTAL_BYTES} 字节限制")
    }

    if config.network.allowed_domains.len() > MAX_ALLOWED_DOMAINS {
        bail!("sandbox.network.allowedDomains 超过 {MAX_ALLOWED_DOMAINS} 项限制")
    }
    for domain in &config.network.allowed_domains {
        if domain.is_empty()
            || domain.len() > MAX_DOMAIN_BYTES
            || domain
                .contains(|character: char| character.is_control() || character.is_whitespace())
        {
            bail!("sandbox.network.allowedDomains 包含无效域名")
        }
    }
    if config.network.deny && !config.network.allowed_domains.is_empty() {
        bail!("sandbox.network.deny 与 allowedDomains 不能同时设置")
    }
    Ok(())
}

fn detect_backend(config: &SandboxConfig) -> Backend {
    let isolate_network = config.network.deny || !config.network.allowed_domains.is_empty();

    #[cfg(target_os = "macos")]
    {
        let program = PathBuf::from("/usr/bin/sandbox-exec");
        if !program.is_file() {
            return Backend::Unavailable("缺少 /usr/bin/sandbox-exec".to_owned());
        }
        if let Err(error) = probe_macos(&program, isolate_network) {
            return Backend::Unavailable(format!("sandbox-exec 能力探测失败: {error:#}"));
        }
        Backend::MacOs(program)
    }

    #[cfg(target_os = "linux")]
    {
        let Some(program) = find_executable("bwrap") else {
            return Backend::Unavailable("缺少 bubblewrap (bwrap)".to_owned());
        };
        if let Err(error) = probe_linux(&program, isolate_network) {
            return Backend::Unavailable(format!("bubblewrap 能力探测失败: {error:#}"));
        }
        Backend::Linux(program)
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (config, isolate_network);
        Backend::Unavailable(format!("{} 尚无可靠的 OS sandbox backend", env::consts::OS))
    }
}

#[cfg(target_os = "macos")]
fn probe_macos(program: &Path, isolate_network: bool) -> Result<()> {
    let network = if isolate_network {
        ""
    } else {
        " (allow network*)"
    };
    let profile = format!(
        "(version 1) (deny default) (allow process*) (allow file-read*) (allow sysctl-read) (allow mach-lookup){network}"
    );
    let status = StdCommand::new(program)
        .args(["-p", &profile, "--", "/usr/bin/true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("无法启动 sandbox-exec probe")?;
    if !status.success() {
        bail!("probe exit status {status}")
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn probe_linux(program: &Path, isolate_network: bool) -> Result<()> {
    let mut command = StdCommand::new(program);
    command.args([
        "--die-with-parent",
        "--unshare-pid",
        "--ro-bind",
        "/",
        "/",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
    ]);
    if isolate_network {
        command.arg("--unshare-net");
    }
    let status = command
        .args(["--", "/bin/true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .context("无法启动 bubblewrap probe")?;
    if !status.success() {
        bail!("probe exit status {status}")
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn find_executable(name: &str) -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("/usr/bin").join(name),
        PathBuf::from("/bin").join(name),
        PathBuf::from("/usr/local/bin").join(name),
    ];
    for candidate in candidates {
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    env::var_os("PATH").and_then(|path| {
        env::split_paths(&path)
            .filter(|directory| directory.is_absolute())
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
    })
}

#[cfg(any(target_os = "macos", test))]
fn macos_profile(config: &SandboxConfig, cwd: &Path, proxy_port: Option<u16>) -> Result<String> {
    let paths = effective_paths(config, cwd)?;
    let mut profile = String::from(
        "(version 1)\n(deny default)\n(allow process*)\n(allow file-read*)\n(allow file-write* (literal \"/dev/null\") (literal \"/dev/zero\") (literal \"/dev/tty\"))\n(allow sysctl-read)\n(allow mach-lookup)\n",
    );
    if !config.network.allowed_domains.is_empty() {
        let port = proxy_port.context("allowedDomains 缺少 active trusted proxy")?;
        profile.push_str(&format!(
            "(allow network-outbound (remote ip \"localhost:{port}\"))\n"
        ));
    } else if !config.network.deny {
        profile.push_str("(allow network*)\n");
    }
    for path in &paths.allow_write {
        profile.push_str(&format!(
            "(allow file-write* (literal \"{}\") (subpath \"{}\"))\n",
            scheme_escape(path)?,
            scheme_escape(path)?
        ));
    }
    for path in &paths.deny_write {
        profile.push_str(&format!(
            "(deny file-write* (literal \"{}\") (subpath \"{}\"))\n",
            scheme_escape(path)?,
            scheme_escape(path)?
        ));
    }
    for path in &paths.deny_read {
        profile.push_str(&format!(
            "(deny file-read* (literal \"{}\") (subpath \"{}\"))\n",
            scheme_escape(path)?,
            scheme_escape(path)?
        ));
    }
    Ok(profile)
}

#[cfg(any(target_os = "linux", test))]
fn linux_arguments(config: &SandboxConfig, cwd: &Path) -> Result<Vec<OsString>> {
    let paths = effective_paths(config, cwd)?;
    let mut arguments = strings([
        "--die-with-parent",
        "--unshare-pid",
        "--ro-bind",
        "/",
        "/",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
    ]);
    if config.network.deny || !config.network.allowed_domains.is_empty() {
        arguments.push("--unshare-net".into());
    }
    for path in paths.allow_write.iter().filter(|path| path.exists()) {
        arguments.push("--bind".into());
        arguments.push(path.as_os_str().to_owned());
        arguments.push(path.as_os_str().to_owned());
    }
    for path in &paths.deny_write {
        if path.exists() {
            arguments.push("--ro-bind".into());
            arguments.push(path.as_os_str().to_owned());
            arguments.push(path.as_os_str().to_owned());
        } else {
            // Mount an inaccessible placeholder so a path created outside the
            // namespace after launch cannot bypass the deny rule.
            arguments.push("--tmpfs".into());
            arguments.push(path.as_os_str().to_owned());
            arguments.push("--chmod".into());
            arguments.push("000".into());
            arguments.push(path.as_os_str().to_owned());
        }
    }
    for path in &paths.deny_read {
        if path.is_dir() {
            arguments.push("--tmpfs".into());
            arguments.push(path.as_os_str().to_owned());
        } else if path.exists() {
            arguments.push("--ro-bind".into());
            arguments.push(OsString::from("/dev/null"));
            arguments.push(path.as_os_str().to_owned());
        } else {
            arguments.push("--tmpfs".into());
            arguments.push(path.as_os_str().to_owned());
        }
        arguments.push("--chmod".into());
        arguments.push("000".into());
        arguments.push(path.as_os_str().to_owned());
    }
    Ok(arguments)
}

#[cfg(any(target_os = "linux", test))]
fn strings<const N: usize>(values: [&str; N]) -> Vec<OsString> {
    values.into_iter().map(OsString::from).collect()
}

#[cfg(any(target_os = "macos", target_os = "linux", test))]
struct EffectivePaths {
    allow_write: Vec<PathBuf>,
    deny_read: Vec<PathBuf>,
    deny_write: Vec<PathBuf>,
}

#[cfg(any(target_os = "macos", target_os = "linux", test))]
fn effective_paths(config: &SandboxConfig, cwd: &Path) -> Result<EffectivePaths> {
    let cwd = resolve_policy_path(cwd, cwd.as_os_str())?;
    let mut allow_write = vec![cwd.clone()];
    if let Ok(temp) = resolve_policy_path(&cwd, env::temp_dir().as_os_str()) {
        allow_write.push(temp);
    }
    allow_write.extend(resolve_paths(&cwd, &config.filesystem.allow_write)?);

    let mut deny_write = resolve_paths(&cwd, &config.filesystem.deny_write)?;
    deny_write.extend([
        cwd.join(".open-agent-harness/settings.json"),
        cwd.join(".open-agent-harness/settings.local.json"),
        cwd.join(".open-agent-harness/skills"),
    ]);
    if let Some(home) = dirs::home_dir() {
        deny_write.extend([
            home.join(".open-agent-harness/settings.json"),
            home.join(".open-agent-harness/skills"),
        ]);
    }
    let deny_read = resolve_paths(&cwd, &config.filesystem.deny_read)?;
    deduplicate(&mut allow_write);
    deduplicate(&mut deny_write);
    Ok(EffectivePaths {
        allow_write,
        deny_read,
        deny_write,
    })
}

#[cfg(any(target_os = "macos", target_os = "linux", test))]
fn resolve_paths(cwd: &Path, values: &[String]) -> Result<Vec<PathBuf>> {
    values
        .iter()
        .map(|value| resolve_policy_path(cwd, OsStr::new(value)))
        .collect()
}

#[cfg(any(target_os = "macos", target_os = "linux", test))]
fn resolve_policy_path(cwd: &Path, value: &OsStr) -> Result<PathBuf> {
    if value.is_empty() {
        bail!("sandbox policy 路径不能为空")
    }
    let raw = PathBuf::from(value);
    let expanded = if raw == Path::new("~") {
        dirs::home_dir().context("无法确定主目录")?
    } else if let Ok(rest) = raw.strip_prefix("~/") {
        dirs::home_dir().context("无法确定主目录")?.join(rest)
    } else if raw.is_absolute() {
        raw
    } else {
        cwd.join(raw)
    };
    let normalized = lexical_normalize(&expanded)?;
    canonicalize_with_missing(&normalized)
}

#[cfg(any(target_os = "macos", target_os = "linux", test))]
fn lexical_normalize(path: &Path) -> Result<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    bail!("sandbox policy 路径越过文件系统根目录")
                }
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    if !normalized.is_absolute() {
        bail!("sandbox policy 路径未解析为绝对路径")
    }
    Ok(normalized)
}

#[cfg(any(target_os = "macos", target_os = "linux", test))]
fn canonicalize_with_missing(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path)
            .with_context(|| format!("无法解析 sandbox policy 路径 {}", path.display()));
    }
    let mut existing = path;
    let mut suffix = Vec::new();
    while !existing.exists() {
        let name = existing
            .file_name()
            .context("sandbox policy 路径没有可解析的父目录")?;
        suffix.push(name.to_owned());
        existing = existing
            .parent()
            .context("sandbox policy 路径没有可解析的父目录")?;
    }
    let mut canonical = fs::canonicalize(existing)
        .with_context(|| format!("无法解析 sandbox policy 父目录 {}", existing.display()))?;
    for component in suffix.into_iter().rev() {
        canonical.push(component);
    }
    Ok(canonical)
}

#[cfg(any(target_os = "macos", test))]
fn scheme_escape(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .with_context(|| format!("sandbox policy 路径不是有效 UTF-8: {}", path.display()))?;
    if value.contains(['\0', '\n', '\r']) {
        bail!("sandbox policy 路径包含不支持的控制字符")
    }
    Ok(value.replace('\\', "\\\\").replace('"', "\\\""))
}

#[cfg(any(target_os = "macos", target_os = "linux", test))]
fn deduplicate(paths: &mut Vec<PathBuf>) {
    let mut seen = HashSet::new();
    paths.retain(|path| seen.insert(path.clone()));
}

#[cfg(target_os = "macos")]
fn configure_proxy_environment(command: &mut Command, http_proxy: String, socks_proxy: String) {
    for name in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ] {
        command.env_remove(name);
    }
    command
        .env("HTTP_PROXY", &http_proxy)
        .env("HTTPS_PROXY", &http_proxy)
        .env("ALL_PROXY", &socks_proxy)
        .env("http_proxy", &http_proxy)
        .env("https_proxy", &http_proxy)
        .env("all_proxy", &socks_proxy)
        .env("NO_PROXY", "")
        .env("no_proxy", "");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> SandboxConfig {
        SandboxConfig {
            enabled: true,
            fail_if_unavailable: true,
            ..SandboxConfig::default()
        }
    }

    #[test]
    fn explicit_session_workspaces_extend_write_scope_and_protect_settings() {
        let root = tempfile::tempdir().unwrap();
        let runtime = SandboxRuntime::new(SandboxConfig::default())
            .unwrap()
            .with_session_workspaces(&[root.path().to_owned()])
            .unwrap();
        let canonical = std::fs::canonicalize(root.path()).unwrap();
        let rendered = canonical.display().to_string();
        assert!(runtime.config.filesystem.allow_write.contains(&rendered));
        assert!(
            runtime
                .config
                .filesystem
                .deny_write
                .contains(&canonical.join(".open-agent-harness").display().to_string())
        );
    }

    #[test]
    fn parses_provider_neutral_settings() {
        let settings = serde_json::json!({
            "sandbox": {
                "enabled": true,
                "failIfUnavailable": false,
                "filesystem": {
                    "allowWrite": ["build"],
                    "denyRead": ["secrets"],
                    "denyWrite": ["locked"]
                },
                "network": {"deny": true}
            }
        });
        let runtime = SandboxRuntime::from_settings(&settings).unwrap();
        assert!(runtime.enabled());
        assert_eq!(runtime.config.filesystem.allow_write, ["build"]);
        assert_eq!(runtime.config.filesystem.deny_read, ["secrets"]);
        assert_eq!(runtime.config.filesystem.deny_write, ["locked"]);
        assert!(runtime.config.network.deny);
    }

    #[test]
    fn sandbox_settings_reject_unknown_fields() {
        for settings in [
            serde_json::json!({"sandbox":{"enabled":true,"enabledd":true}}),
            serde_json::json!({"sandbox":{"filesystem":{"denyReads":["private"]}}}),
            serde_json::json!({"sandbox":{"network":{"denyy":true}}}),
        ] {
            assert!(SandboxRuntime::from_settings(&settings).is_err());
        }
    }

    #[test]
    fn required_missing_backend_fails_closed() {
        let error = SandboxRuntime::with_backend(
            enabled_config(),
            Backend::Unavailable("test dependency is missing".to_owned()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("test dependency is missing"));
    }

    #[test]
    fn optional_missing_backend_is_explicitly_unsandboxed() {
        let mut config = enabled_config();
        config.fail_if_unavailable = false;
        let runtime = SandboxRuntime::with_backend(
            config,
            Backend::Unavailable("test dependency is missing".to_owned()),
        )
        .unwrap();
        let prepared = runtime
            .command(
                Path::new("/"),
                OsStr::new("/bin/sh"),
                &["-c".into(), "true".into()],
            )
            .unwrap();
        assert!(!prepared.is_sandboxed());
        assert!(
            prepared
                .warning
                .as_deref()
                .is_some_and(|warning| warning.contains("test dependency is missing"))
        );
    }

    #[test]
    fn macos_profile_contains_write_and_read_boundaries() {
        let temp = tempfile::tempdir().unwrap();
        let allowed = temp.path().join("allowed");
        let deny_read = temp.path().join("private");
        let deny_write = allowed.join("locked");
        fs::create_dir_all(&allowed).unwrap();
        fs::create_dir_all(&deny_read).unwrap();
        fs::create_dir_all(&deny_write).unwrap();
        let mut config = enabled_config();
        config.filesystem.allow_write = vec![allowed.display().to_string()];
        config.filesystem.deny_read = vec![deny_read.display().to_string()];
        config.filesystem.deny_write = vec![deny_write.display().to_string()];
        config.network.deny = true;
        let profile = macos_profile(&config, temp.path(), None).unwrap();
        assert!(profile.contains("(allow file-write*"));
        assert!(profile.contains("(deny file-read*"));
        assert!(profile.contains("(deny file-write*"));
        assert!(!profile.contains("(allow network*)"));
        let canonical_allowed = fs::canonicalize(allowed).unwrap();
        assert!(profile.contains(&scheme_escape(&canonical_allowed).unwrap()));
    }

    #[test]
    fn macos_domain_proxy_profile_allows_only_the_authenticated_proxy_port() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = enabled_config();
        config.network.allowed_domains = vec!["example.com".to_owned()];
        let profile = macos_profile(&config, temp.path(), Some(43123)).unwrap();
        assert!(profile.contains("localhost:43123"));
        assert!(!profile.contains("(allow network*)"));
    }

    #[test]
    fn network_deny_and_allowlist_are_mutually_exclusive() {
        let mut config = enabled_config();
        config.network.deny = true;
        config.network.allowed_domains = vec!["example.com".to_owned()];
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn unavailable_explicit_network_policy_never_falls_back_unsandboxed() {
        let mut allowlisted = enabled_config();
        allowlisted.network.allowed_domains = vec!["example.com".to_owned()];
        let mut denied = enabled_config();
        denied.network.deny = true;
        for mut config in [allowlisted, denied] {
            config.fail_if_unavailable = false;
            let runtime = SandboxRuntime::with_backend(
                config,
                Backend::Unavailable("test backend missing".to_owned()),
            )
            .unwrap();
            assert!(
                runtime
                    .command(
                        Path::new("/"),
                        OsStr::new("/bin/sh"),
                        &["-c".into(), "true".into()],
                    )
                    .is_err()
            );
        }
    }

    #[test]
    fn linux_plan_uses_read_only_root_and_network_namespace() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = enabled_config();
        config.network.deny = true;
        let missing_read = temp.path().join("future-secret");
        let missing_write = temp.path().join("future-locked");
        config.filesystem.deny_read = vec![missing_read.display().to_string()];
        config.filesystem.deny_write = vec![missing_write.display().to_string()];
        let arguments = linux_arguments(&config, temp.path()).unwrap();
        assert!(arguments.windows(3).any(|window| {
            window
                == [
                    OsString::from("--ro-bind"),
                    OsString::from("/"),
                    OsString::from("/"),
                ]
        }));
        assert!(arguments.contains(&OsString::from("--unshare-net")));
        assert!(arguments.contains(&OsString::from("--bind")));
        for path in [missing_read, missing_write] {
            let path = resolve_policy_path(temp.path(), path.as_os_str()).unwrap();
            assert!(arguments.windows(2).any(|window| {
                window == [OsString::from("--tmpfs"), path.as_os_str().to_owned()]
            }));
        }
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_backend_allows_workspace_and_rejects_denied_paths() {
        let temp = tempfile::tempdir().unwrap();
        let private = temp.path().join("private");
        let locked = temp.path().join("locked");
        fs::create_dir_all(&private).unwrap();
        fs::create_dir_all(&locked).unwrap();
        fs::write(private.join("secret.txt"), "secret").unwrap();
        let mut config = enabled_config();
        config.filesystem.deny_read = vec![private.display().to_string()];
        config.filesystem.deny_write = vec![locked.display().to_string()];
        let runtime = SandboxRuntime::new(config).unwrap();

        let script = format!(
            "printf ok > allowed.txt; cat '{}'; printf denied > '{}/no.txt'",
            private.join("secret.txt").display(),
            locked.display()
        );
        let prepared = runtime
            .command(
                temp.path(),
                OsStr::new("/bin/sh"),
                &["-c".into(), script.into()],
            )
            .unwrap();
        assert!(prepared.is_sandboxed());
        let (mut command, warning) = prepared.into_parts();
        assert!(warning.is_none());
        let output = command.current_dir(temp.path()).output().await.unwrap();
        assert!(!output.status.success());
        assert_eq!(
            fs::read_to_string(temp.path().join("allowed.txt")).unwrap(),
            "ok"
        );
        assert!(!locked.join("no.txt").exists());
        let rendered = String::from_utf8_lossy(&output.stderr);
        assert!(rendered.contains("Operation not permitted"));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_backend_denies_network_when_requested() {
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut config = enabled_config();
        config.network.deny = true;
        let runtime = SandboxRuntime::new(config).unwrap();
        let script = format!("/usr/bin/nc -z 127.0.0.1 {port}");
        let prepared = runtime
            .command(
                temp.path(),
                OsStr::new("/bin/sh"),
                &["-c".into(), script.into()],
            )
            .unwrap();
        let (mut command, warning) = prepared.into_parts();
        assert!(warning.is_none());
        let output = command.current_dir(temp.path()).output().await.unwrap();
        assert!(!output.status.success());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn macos_allowed_domain_uses_proxy_but_direct_socket_stays_denied() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let count = stream.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..count]).starts_with("GET /allowed"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\nallowed",
                )
                .await
                .unwrap();
        });
        let mut config = enabled_config();
        config.network.allowed_domains = vec!["127.0.0.1".to_owned()];
        config.network.allow_private_network = true;
        let runtime = SandboxRuntime::new(config).unwrap();

        let prepared = runtime
            .command(
                temp.path(),
                OsStr::new("/bin/sh"),
                &[
                    "-c".into(),
                    format!(
                        "/usr/bin/curl --silent --show-error --max-time 5 http://127.0.0.1:{port}/allowed"
                    )
                    .into(),
                ],
            )
            .unwrap();
        let (mut command, warning) = prepared.into_parts();
        assert!(warning.is_none());
        let output = command.current_dir(temp.path()).output().await.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&output.stdout), "allowed");
        server.await.unwrap();

        let direct_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let direct_port = direct_listener.local_addr().unwrap().port();
        let prepared = runtime
            .command(
                temp.path(),
                OsStr::new("/bin/sh"),
                &[
                    "-c".into(),
                    format!("/usr/bin/nc -z 127.0.0.1 {direct_port}").into(),
                ],
            )
            .unwrap();
        let (mut command, warning) = prepared.into_parts();
        assert!(warning.is_none());
        let output = command.current_dir(temp.path()).output().await.unwrap();
        assert!(!output.status.success());
    }
}

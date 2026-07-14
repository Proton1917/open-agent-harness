use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
#[cfg(windows)]
use std::sync::Mutex;

use anyhow::{Context, Result, bail};
use tokio::process::{Child, Command};

#[cfg(windows)]
use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
#[cfg(windows)]
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
#[cfg(windows)]
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject,
};
#[cfg(windows)]
use windows_sys::Win32::System::Threading::{
    CREATE_SUSPENDED, OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
};

use crate::config::Settings;

const MAX_SECRET_ENV_NAMES: usize = 128;
const MAX_SECRET_ENV_NAME_BYTES: usize = 128;
const MAX_SECRET_ENV_TOTAL_BYTES: usize = 16 * 1024;
const MAX_MCP_SERVERS_FOR_SECRET_SCAN: usize = 32;
const BUILTIN_SECRET_ENV_NAMES: &[&str] = &["HARNESS_API_KEY", "HARNESS_AUTH_TOKEN"];

/// Names of ambient credential variables that must never cross into a child
/// process. Values stay in the parent so configured transports can read them
/// lazily; only inherited child environments are scrubbed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SecretEnvScrubber {
    names: Arc<[String]>,
}

impl Default for SecretEnvScrubber {
    fn default() -> Self {
        Self {
            names: BUILTIN_SECRET_ENV_NAMES
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>()
                .into(),
        }
    }
}

impl SecretEnvScrubber {
    pub(crate) fn from_settings(settings: &Settings) -> Result<Self> {
        let mut names = BUILTIN_SECRET_ENV_NAMES
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        if let Some(servers) = settings.raw.get("mcpServers") {
            let servers = servers
                .as_object()
                .context("mcpServers 必须是 JSON object")?;
            if servers.len() > MAX_MCP_SERVERS_FOR_SECRET_SCAN {
                bail!("mcpServers 超过 {MAX_MCP_SERVERS_FOR_SECRET_SCAN} 个 secret-env 扫描限制")
            }
            for (server, config) in servers {
                let config = config
                    .as_object()
                    .with_context(|| format!("MCP server {server} 配置必须是 object"))?;
                let Some(auth) = config.get("auth") else {
                    continue;
                };
                if auth.is_null() {
                    continue;
                }
                let auth = auth
                    .as_object()
                    .with_context(|| format!("MCP server {server} auth 必须是 object"))?;
                for field in ["env", "clientSecretEnv", "callbackEnv"] {
                    let Some(value) = auth.get(field) else {
                        continue;
                    };
                    let name = value.as_str().with_context(|| {
                        format!("MCP server {server} auth.{field} 必须是 env name string")
                    })?;
                    validate_secret_env_name(name).with_context(|| {
                        format!("MCP server {server} auth.{field} env name 无效")
                    })?;
                    names.insert(name.to_owned());
                }
            }
        }
        if names.len() > MAX_SECRET_ENV_NAMES {
            bail!("MCP secret env 名称超过 {MAX_SECRET_ENV_NAMES} 项限制")
        }
        let total = names.iter().map(String::len).sum::<usize>();
        if total > MAX_SECRET_ENV_TOTAL_BYTES {
            bail!("MCP secret env 名称超过 {MAX_SECRET_ENV_TOTAL_BYTES} 字节限制")
        }
        Ok(Self {
            names: names.into_iter().collect::<Vec<_>>().into(),
        })
    }

    pub(crate) fn scrub_tokio(&self, command: &mut Command) {
        for name in self.names.iter() {
            command.env_remove(name);
        }
    }

    #[cfg(test)]
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }
}

fn validate_secret_env_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_SECRET_ENV_NAME_BYTES {
        bail!("env name 为空或超过 {MAX_SECRET_ENV_NAME_BYTES} 字节")
    }
    if !name.bytes().enumerate().all(|(index, byte)| {
        matches!(
            (index, byte),
            (0, b'A'..=b'Z' | b'a'..=b'z' | b'_')
                | (_, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
        )
    }) {
        bail!("env name 语法无效")
    }
    Ok(())
}

pub(crate) fn resolve_trusted_executable(command: &str, workspace: &Path) -> Result<PathBuf> {
    if command.trim().is_empty() || command.contains('\0') {
        bail!("executable command 为空或包含 NUL")
    }
    let command_path = Path::new(command);
    let candidate = if command_path.is_absolute() {
        command_path.to_owned()
    } else if command_path.components().count() == 1
        && matches!(command_path.components().next(), Some(Component::Normal(_)))
    {
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .filter(|directory| directory.is_absolute())
            .map(|directory| directory.join(command_path))
            .find(|candidate| candidate.is_file())
            .with_context(|| format!("PATH 中找不到 trusted executable: {command}"))?
    } else {
        bail!("executable command 必须是绝对路径或 PATH 中的单一名称: {command}")
    };
    let executable = std::fs::canonicalize(&candidate)
        .with_context(|| format!("无法解析 executable: {}", candidate.display()))?;
    if !executable.is_file() {
        bail!("executable 不是普通文件: {}", executable.display())
    }
    let workspace = std::fs::canonicalize(workspace)
        .with_context(|| format!("无法解析 executable workspace: {}", workspace.display()))?;
    if workspace.parent().is_some() && executable.starts_with(&workspace) {
        bail!("拒绝从当前 workspace 启动持久化 trusted executable")
    }
    Ok(executable)
}

pub(crate) fn terminate_process_tree(process_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(group) = process_id {
        // SAFETY: callers place the child in a dedicated process group before spawning it.
        unsafe {
            libc::kill(-(group as i32), libc::SIGKILL);
        }
    }

    #[cfg(windows)]
    if let Some(process_id) = process_id {
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    #[cfg(not(any(unix, windows)))]
    let _ = process_id;
}

/// Spawns a child inside a platform process-tree boundary. Windows children
/// start suspended, are assigned to a Job Object, and are resumed only after
/// the assignment succeeds; Unix children start as leaders of a fresh process
/// group. Callers must retain the returned guard for the child lifecycle.
pub(crate) fn spawn_managed(command: &mut Command) -> Result<(Child, ProcessTreeGuard)> {
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(CREATE_SUSPENDED);
    let mut child = command.spawn().context("无法启动受控 child process")?;
    let guard = ProcessTreeGuard::attach(&mut child)?;
    Ok((child, guard))
}

/// Owns one platform process-tree boundary. On Unix the child must be spawned
/// in its own process group. On Windows the child is assigned immediately to a
/// Job Object configured with `KILL_ON_JOB_CLOSE`, so descendants remain
/// addressable even after the direct child exits.
#[derive(Clone, Debug)]
pub(crate) struct ProcessTreeGuard {
    inner: Arc<ProcessTreeInner>,
}

#[derive(Debug)]
struct ProcessTreeInner {
    process_id: Option<u32>,
    armed: AtomicBool,
    #[cfg(windows)]
    windows_job: Mutex<Option<WindowsJob>>,
}

#[cfg(windows)]
#[derive(Debug)]
struct WindowsJob {
    handle: OwnedHandle,
}

impl ProcessTreeGuard {
    /// Attaches the freshly spawned child before callers take its stdio or
    /// perform any await. Failure is fail-closed: the PID fallback and direct
    /// child kill are both attempted before returning the error.
    fn attach(child: &mut Child) -> Result<Self> {
        let process_id = child.id().context("spawned child has no process id")?;
        #[cfg(windows)]
        let windows_job = match WindowsJob::attach(child) {
            Ok(job) => Mutex::new(Some(job)),
            Err(error) => {
                terminate_process_tree(Some(process_id));
                let _ = child.start_kill();
                return Err(error).context("无法将 Windows child 绑定到受控 Job Object");
            }
        };
        let guard = Self {
            inner: Arc::new(ProcessTreeInner {
                process_id: Some(process_id),
                armed: AtomicBool::new(true),
                #[cfg(windows)]
                windows_job,
            }),
        };
        #[cfg(windows)]
        if let Err(error) = resume_suspended_process(process_id) {
            guard.terminate();
            let _ = child.start_kill();
            return Err(error).context("无法恢复已绑定 Job Object 的 Windows child");
        }
        Ok(guard)
    }

    pub(crate) fn terminate(&self) {
        self.inner.terminate();
    }

    /// Marks cleanup complete. Closing a Windows KILL_ON_JOB_CLOSE handle is
    /// intentionally still a final descendant barrier; on Unix this only
    /// suppresses the already-completed process-group kill.
    pub(crate) fn disarm(&self) {
        if self.inner.armed.swap(false, Ordering::AcqRel) {
            #[cfg(windows)]
            self.inner.close_windows_job();
        }
    }
}

impl ProcessTreeInner {
    fn terminate(&self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        #[cfg(windows)]
        if let Some(job) = self.take_windows_job() {
            job.terminate();
            return;
        }
        terminate_process_tree(self.process_id);
    }

    #[cfg(windows)]
    fn take_windows_job(&self) -> Option<WindowsJob> {
        self.windows_job
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }

    #[cfg(windows)]
    fn close_windows_job(&self) {
        drop(self.take_windows_job());
    }
}

impl Drop for ProcessTreeInner {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(windows)]
impl WindowsJob {
    fn attach(child: &Child) -> Result<Self> {
        let process_handle = child
            .raw_handle()
            .context("Windows child process handle 不可用")?;
        // SAFETY: null security attributes/name request an unnamed job. The
        // returned owned handle is closed exactly once by `OwnedHandle`.
        let raw_job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw_job.is_null() {
            return Err(std::io::Error::last_os_error()).context("CreateJobObjectW 失败");
        }
        // SAFETY: `raw_job` is a newly created, non-null owned HANDLE.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw_job.cast()) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the information pointer and byte count describe `limits` for
        // the documented extended-limit information class.
        let configured = unsafe {
            SetInformationJobObject(
                handle.as_raw_handle().cast(),
                JobObjectExtendedLimitInformation,
                std::ptr::from_ref(&limits).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if configured == 0 {
            return Err(std::io::Error::last_os_error())
                .context("SetInformationJobObject(KILL_ON_JOB_CLOSE) 失败");
        }
        // SAFETY: both handles are valid for this call and remain owned by the
        // job wrapper / tokio child respectively.
        let assigned = unsafe {
            AssignProcessToJobObject(handle.as_raw_handle().cast(), process_handle.cast())
        };
        if assigned == 0 {
            return Err(std::io::Error::last_os_error()).context("AssignProcessToJobObject 失败");
        }
        Ok(Self { handle })
    }

    fn terminate(&self) {
        // SAFETY: the job handle stays valid for the duration of the call.
        let _ = unsafe { TerminateJobObject(self.handle.as_raw_handle().cast(), 1) };
    }
}

#[cfg(windows)]
fn resume_suspended_process(process_id: u32) -> Result<()> {
    // SAFETY: the returned snapshot handle is validated against the documented
    // INVALID_HANDLE_VALUE sentinel and then owned exactly once.
    let raw_snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if raw_snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error()).context("无法枚举 Windows child threads");
    }
    // SAFETY: `raw_snapshot` is a newly returned valid owned HANDLE.
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw_snapshot.cast()) };
    let mut entry = THREADENTRY32 {
        dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
        ..THREADENTRY32::default()
    };
    // SAFETY: `entry` has the required size and remains writable for the call.
    let mut found = unsafe {
        Thread32First(
            snapshot.as_raw_handle().cast(),
            std::ptr::from_mut(&mut entry),
        ) != 0
    };
    while found {
        if entry.th32OwnerProcessID == process_id {
            // SAFETY: access is limited to resuming this known suspended child
            // thread; the returned handle is validated and owned exactly once.
            let raw_thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if raw_thread.is_null() {
                return Err(std::io::Error::last_os_error())
                    .context("无法打开 Windows child primary thread");
            }
            // SAFETY: `raw_thread` is a newly returned valid owned HANDLE.
            let thread = unsafe { OwnedHandle::from_raw_handle(raw_thread.cast()) };
            // SAFETY: the handle grants THREAD_SUSPEND_RESUME and remains valid.
            let mut previous = unsafe { ResumeThread(thread.as_raw_handle().cast()) };
            if previous == u32::MAX {
                return Err(std::io::Error::last_os_error())
                    .context("ResumeThread Windows child 失败");
            }
            if previous == 0 {
                bail!("Windows child 未保持 CREATE_SUSPENDED 状态")
            }
            for _ in 1..64 {
                if previous <= 1 {
                    return Ok(());
                }
                // SAFETY: same valid thread handle; this only drains an
                // unexpected additional suspend count before returning.
                previous = unsafe { ResumeThread(thread.as_raw_handle().cast()) };
                if previous == u32::MAX {
                    return Err(std::io::Error::last_os_error())
                        .context("ResumeThread Windows child 失败");
                }
            }
            if previous > 1 {
                bail!("Windows child suspend count 超过安全恢复上限")
            }
            return Ok(());
        }
        // SAFETY: same initialized writable entry and valid snapshot as above.
        found = unsafe {
            Thread32Next(
                snapshot.as_raw_handle().cast(),
                std::ptr::from_mut(&mut entry),
            ) != 0
        };
    }
    bail!("找不到 suspended Windows child primary thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn process_tree_guard_is_send_sync_and_cloneable() {
        fn assert_traits<T: Send + Sync + Clone>() {}
        assert_traits::<ProcessTreeGuard>();
    }

    #[test]
    fn secret_env_scrubber_collects_only_declared_credentials() {
        let scrubber = SecretEnvScrubber::from_settings(&Settings {
            raw: json!({"mcpServers":{
                "bearer":{"url":"https://example.invalid", "auth":{
                    "type":"bearer-env", "env":"MCP_BEARER_TOKEN"
                }},
                "oauth":{"url":"https://oauth.invalid", "auth":{
                    "type":"oauth",
                    "clientSecretEnv":"MCP_CLIENT_SECRET",
                    "callbackEnv":"MCP_CALLBACK",
                    "clientId":"client"
                }},
                "stdio":{"command":"server", "env":{
                    "ORDINARY_CHILD_ENV":"not-a-credential-name-declaration"
                }}
            }}),
        })
        .unwrap();
        assert!(
            scrubber
                .names()
                .iter()
                .any(|name| name == "HARNESS_API_KEY")
        );
        assert!(
            scrubber
                .names()
                .iter()
                .any(|name| name == "MCP_BEARER_TOKEN")
        );
        assert!(
            scrubber
                .names()
                .iter()
                .any(|name| name == "MCP_CLIENT_SECRET")
        );
        assert!(scrubber.names().iter().any(|name| name == "MCP_CALLBACK"));
        assert!(
            !scrubber
                .names()
                .iter()
                .any(|name| name == "ORDINARY_CHILD_ENV")
        );
    }

    #[test]
    fn secret_env_scrubber_rejects_invalid_or_unbounded_names() {
        let invalid = Settings {
            raw: json!({"mcpServers":{"bad":{"auth":{
                "type":"bearer-env", "env":"BAD-NAME"
            }}}}),
        };
        assert!(SecretEnvScrubber::from_settings(&invalid).is_err());

        let mut servers = serde_json::Map::new();
        for index in 0..=MAX_MCP_SERVERS_FOR_SECRET_SCAN {
            servers.insert(
                format!("server-{index}"),
                json!({"auth":{"type":"bearer-env","env":format!("TOKEN_{index}")}}),
            );
        }
        assert!(
            SecretEnvScrubber::from_settings(&Settings {
                raw: json!({"mcpServers":servers})
            })
            .is_err()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn secret_env_scrubber_removes_values_from_child_environment() {
        let scrubber = SecretEnvScrubber::from_settings(&Settings {
            raw: json!({"mcpServers":{"bearer":{"auth":{
                "type":"bearer-env", "env":"MCP_CHILD_SECRET_TEST"
            }}}}),
        })
        .unwrap();
        let mut command = Command::new("/usr/bin/env");
        command
            .env("MCP_CHILD_SECRET_TEST", "must-not-leak")
            .env("ORDINARY_CHILD_ENV_TEST", "kept");
        scrubber.scrub_tokio(&mut command);
        let output = command.output().await.unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(!stdout.contains("MCP_CHILD_SECRET_TEST"));
        assert!(!stdout.contains("must-not-leak"));
        assert!(stdout.contains("ORDINARY_CHILD_ENV_TEST=kept"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_tree_reaps_descendant_after_parent_natural_exit() {
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader};
        use tokio::time::{Duration, sleep, timeout};

        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                "sleep 30 & descendant=$!; printf '%s\\n' \"$descendant\"",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let (mut child, process_tree) = spawn_managed(&mut command).unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut stdout = BufReader::new(stdout);
        let mut line = String::new();
        timeout(Duration::from_secs(2), stdout.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let descendant = line.trim().parse::<i32>().unwrap();
        let status = timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("parent shell should exit while descendant remains")
            .unwrap();
        assert!(status.success());

        process_tree.terminate();
        let mut trailing = Vec::new();
        timeout(Duration::from_secs(2), stdout.read_to_end(&mut trailing))
            .await
            .expect("owned descendant must not keep redirected stdout open")
            .unwrap();
        for _ in 0..100 {
            // SAFETY: signal zero performs an existence check without sending a
            // signal. The PID came from the child process itself.
            if unsafe { libc::kill(descendant, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!("managed Unix descendant remained alive after tree termination");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_job_reaps_descendant_after_parent_natural_exit() {
        use std::process::Stdio;
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader};
        use tokio::time::{Duration, sleep, timeout};

        let script = concat!(
            "$child = Start-Process -FilePath powershell.exe ",
            "-ArgumentList '-NoProfile','-NonInteractive','-Command',",
            "'Start-Sleep -Seconds 30' -NoNewWindow -PassThru; ",
            "[Console]::Out.WriteLine($child.Id)"
        );
        let mut command = Command::new("powershell.exe");
        command
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let (mut child, process_tree) = spawn_managed(&mut command).unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut stdout = BufReader::new(stdout);
        let mut line = String::new();
        timeout(Duration::from_secs(5), stdout.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let descendant = line.trim().parse::<u32>().unwrap();
        timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("PowerShell parent should exit")
            .unwrap();
        assert!(windows_process_exists(descendant));

        process_tree.terminate();
        let mut trailing = Vec::new();
        timeout(Duration::from_secs(2), stdout.read_to_end(&mut trailing))
            .await
            .expect("Job descendant must not keep redirected stdout open")
            .unwrap();
        for _ in 0..100 {
            if !windows_process_exists(descendant) {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
        panic!("Windows Job Object descendant remained alive after termination");
    }

    #[cfg(windows)]
    fn windows_process_exists(process_id: u32) -> bool {
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };

        // SAFETY: this opens a query-only handle for the supplied test PID.
        let raw = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) };
        if raw.is_null() {
            return false;
        }
        // SAFETY: `raw` is a newly opened valid owned HANDLE.
        drop(unsafe { OwnedHandle::from_raw_handle(raw.cast()) });
        true
    }
}

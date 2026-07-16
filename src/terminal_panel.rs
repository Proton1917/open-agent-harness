//! User-controlled persistent terminal panel.
//!
//! A private tmux server keeps one shell alive while the harness owns the
//! process. If tmux is unavailable, an interactive login shell is launched
//! directly. No shell command is assembled as text: every argument is passed
//! separately, child credentials are scrubbed, helper calls are bounded, and
//! cleanup targets only the private socket created for this process instance.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use uuid::Uuid;

use crate::{
    process::{SecretEnvScrubber, resolve_trusted_executable, spawn_managed},
    tools::ToolContext,
};

const TMUX_SESSION: &str = "panel";
const HELPER_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalPanelOutcome {
    Persistent,
    Direct,
}

impl TerminalPanelOutcome {
    pub const fn message(self) -> &'static str {
        match self {
            Self::Persistent => "Returned from persistent terminal panel",
            Self::Direct => "Returned from direct terminal shell (tmux unavailable)",
        }
    }
}

#[derive(Debug)]
pub struct TerminalPanel {
    socket: String,
    tmux_checked: bool,
    tmux: Option<PathBuf>,
    shell_override: Option<PathBuf>,
    owned_server: bool,
    helper_timeout: Duration,
    cleanup_cwd: Option<PathBuf>,
    cleanup_scrubber: Option<SecretEnvScrubber>,
}

impl TerminalPanel {
    pub fn new(instance_id: Uuid) -> Self {
        let compact = instance_id.simple().to_string();
        Self {
            // The full process-instance UUID keeps ownership exact even when
            // abandoned tmux sockets from an earlier process still exist.
            socket: format!("oah-panel-{compact}"),
            tmux_checked: false,
            tmux: None,
            shell_override: None,
            owned_server: false,
            helper_timeout: HELPER_TIMEOUT,
            cleanup_cwd: None,
            cleanup_scrubber: None,
        }
    }

    pub fn show(&mut self, context: &ToolContext) -> Result<TerminalPanelOutcome> {
        let context = context.clone();
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .context("cannot create terminal-panel runtime")?;
                    runtime.block_on(self.show_async(&context))
                })
                .join()
                .map_err(|_| anyhow::anyhow!("terminal-panel worker panicked"))?
        })
    }

    pub fn shutdown(&mut self, context: &ToolContext) -> Result<()> {
        if !self.owned_server {
            return Ok(());
        }
        self.remember_cleanup(&context.cwd(), &context.secret_env_scrubber());
        self.shutdown_stored()
    }

    fn shutdown_stored(&mut self) -> Result<()> {
        if !self.owned_server {
            return Ok(());
        }
        let cwd = self
            .cleanup_cwd
            .clone()
            .context("terminal-panel cleanup cwd is unavailable")?;
        let scrubber = self
            .cleanup_scrubber
            .clone()
            .context("terminal-panel cleanup scrubber is unavailable")?;
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .context("cannot create terminal-panel cleanup runtime")?;
                    runtime.block_on(self.shutdown_async(&cwd, &scrubber))
                })
                .join()
                .map_err(|_| anyhow::anyhow!("terminal-panel cleanup worker panicked"))?
        })
    }

    async fn show_async(&mut self, context: &ToolContext) -> Result<TerminalPanelOutcome> {
        let cwd = context.cwd();
        let scrubber = context.secret_env_scrubber();
        if let Some(tmux) = self.tmux_executable(&cwd, &scrubber).await {
            if self.ensure_tmux_session(&tmux, &cwd, &scrubber).await? {
                let status = run_child(
                    &tmux,
                    tmux_args(&self.socket, ["attach-session", "-t", TMUX_SESSION]),
                    &cwd,
                    &scrubber,
                    ChildMode::Interactive,
                )
                .await?;
                if !status.success() {
                    bail!("terminal-panel tmux client exited with {status}")
                }
                return Ok(TerminalPanelOutcome::Persistent);
            }
        }

        let shell = self.shell_executable(&cwd)?;
        let arguments = direct_shell_arguments(&shell);
        let status = run_child(&shell, arguments, &cwd, &scrubber, ChildMode::Interactive).await?;
        if !status.success() {
            bail!("terminal-panel shell exited with {status}")
        }
        Ok(TerminalPanelOutcome::Direct)
    }

    async fn tmux_executable(
        &mut self,
        cwd: &Path,
        scrubber: &SecretEnvScrubber,
    ) -> Option<PathBuf> {
        if cfg!(windows) {
            let _ = (cwd, scrubber);
            self.tmux_checked = true;
            self.tmux = None;
            return None;
        }
        if self.tmux_checked {
            return self.tmux.clone();
        }
        self.tmux_checked = true;
        let executable = resolve_trusted_executable("tmux", cwd).ok()?;
        let status = run_child(
            &executable,
            vec![OsString::from("-V")],
            cwd,
            scrubber,
            ChildMode::Helper {
                timeout: self.helper_timeout,
                preserve_descendants: false,
            },
        )
        .await
        .ok()?;
        if status.success() {
            self.tmux = Some(executable.clone());
            Some(executable)
        } else {
            None
        }
    }

    async fn ensure_tmux_session(
        &mut self,
        tmux: &Path,
        cwd: &Path,
        scrubber: &SecretEnvScrubber,
    ) -> Result<bool> {
        let exists = run_child(
            tmux,
            tmux_args(&self.socket, ["has-session", "-t", TMUX_SESSION]),
            cwd,
            scrubber,
            ChildMode::Helper {
                timeout: self.helper_timeout,
                preserve_descendants: false,
            },
        )
        .await?;
        if exists.success() {
            if !self.owned_server {
                bail!("terminal-panel private tmux socket is already in use")
            }
            self.owned_server = true;
            self.remember_cleanup(cwd, scrubber);
            return Ok(true);
        }

        let shell = self.shell_executable(cwd)?;
        let mut create = tmux_args(
            &self.socket,
            ["new-session", "-d", "-s", TMUX_SESSION, "-c"],
        );
        create.push(cwd.as_os_str().to_owned());
        create.push(shell.as_os_str().to_owned());
        #[cfg(unix)]
        create.push(OsString::from("-l"));
        let created = run_child(
            tmux,
            create,
            cwd,
            scrubber,
            ChildMode::Helper {
                timeout: self.helper_timeout,
                preserve_descendants: true,
            },
        )
        .await?;
        if !created.success() {
            return Ok(false);
        }
        self.owned_server = true;
        self.remember_cleanup(cwd, scrubber);

        let configured = run_child(
            tmux,
            tmux_args(
                &self.socket,
                [
                    "bind-key",
                    "-n",
                    "M-j",
                    "detach-client",
                    ";",
                    "set-option",
                    "-g",
                    "status-style",
                    "bg=default",
                    ";",
                    "set-option",
                    "-g",
                    "status-left",
                    "",
                    ";",
                    "set-option",
                    "-g",
                    "status-right",
                    " Alt+J to return to harness ",
                    ";",
                    "set-option",
                    "-g",
                    "status-right-style",
                    "fg=brightblack",
                ],
            ),
            cwd,
            scrubber,
            ChildMode::Helper {
                timeout: self.helper_timeout,
                preserve_descendants: true,
            },
        )
        .await?;
        if configured.success() {
            Ok(true)
        } else {
            let _ = self.shutdown_async(cwd, scrubber).await;
            Ok(false)
        }
    }

    fn shell_executable(&self, cwd: &Path) -> Result<PathBuf> {
        if let Some(shell) = &self.shell_override {
            return resolve_trusted_executable(
                shell
                    .to_str()
                    .context("terminal-panel shell path is not UTF-8")?,
                cwd,
            );
        }
        let configured = if cfg!(windows) {
            std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".to_owned())
        } else {
            std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_owned())
        };
        resolve_trusted_executable(&configured, cwd).context("terminal-panel shell is not trusted")
    }

    fn remember_cleanup(&mut self, cwd: &Path, scrubber: &SecretEnvScrubber) {
        self.cleanup_cwd = Some(cwd.to_owned());
        self.cleanup_scrubber = Some(scrubber.clone());
    }

    async fn shutdown_async(&mut self, cwd: &Path, scrubber: &SecretEnvScrubber) -> Result<()> {
        let Some(tmux) = self.tmux.clone() else {
            self.owned_server = false;
            self.cleanup_cwd = None;
            self.cleanup_scrubber = None;
            return Ok(());
        };
        let status = run_child(
            &tmux,
            tmux_args(&self.socket, ["kill-server"]),
            cwd,
            scrubber,
            ChildMode::Helper {
                timeout: self.helper_timeout,
                preserve_descendants: false,
            },
        )
        .await?;
        if !status.success() {
            bail!("terminal-panel tmux cleanup exited with {status}")
        }
        self.owned_server = false;
        self.cleanup_cwd = None;
        self.cleanup_scrubber = None;
        Ok(())
    }

    #[cfg(all(test, unix))]
    fn with_executables(
        instance_id: Uuid,
        tmux: Option<PathBuf>,
        shell: PathBuf,
        helper_timeout: Duration,
    ) -> Self {
        let mut panel = Self::new(instance_id);
        panel.tmux_checked = true;
        panel.tmux = tmux;
        panel.shell_override = Some(shell);
        panel.helper_timeout = helper_timeout;
        panel
    }
}

impl Drop for TerminalPanel {
    fn drop(&mut self) {
        let _ = self.shutdown_stored();
    }
}

#[derive(Debug, Clone, Copy)]
enum ChildMode {
    Helper {
        timeout: Duration,
        preserve_descendants: bool,
    },
    Interactive,
}

async fn run_child(
    executable: &Path,
    arguments: Vec<OsString>,
    cwd: &Path,
    scrubber: &SecretEnvScrubber,
    mode: ChildMode,
) -> Result<ExitStatus> {
    let mut command = Command::new(executable);
    command.args(arguments).current_dir(cwd).kill_on_drop(true);
    match mode {
        ChildMode::Helper { .. } => {
            command
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
        }
        ChildMode::Interactive => {
            command
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit());
        }
    }
    scrubber.scrub_tokio(&mut command);

    if matches!(mode, ChildMode::Interactive) {
        // Interactive children must stay in the inherited foreground process
        // group on Unix. A managed fresh group would be stopped by terminal
        // job control before it could read or redraw the TTY.
        #[cfg(not(windows))]
        {
            return command
                .spawn()
                .with_context(|| {
                    format!(
                        "cannot launch terminal-panel process {}",
                        executable.display()
                    )
                })?
                .wait()
                .await
                .context("cannot wait for terminal-panel interactive process");
        }
    }

    let (mut child, process_tree) = spawn_managed(&mut command).with_context(|| {
        format!(
            "cannot launch terminal-panel process {}",
            executable.display()
        )
    })?;
    let status = match mode {
        ChildMode::Helper { timeout, .. } => {
            match tokio::time::timeout(timeout, child.wait()).await {
                Ok(status) => status.context("cannot wait for terminal-panel helper")?,
                Err(_) => {
                    process_tree.terminate();
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    bail!(
                        "terminal-panel helper {} exceeded {}ms",
                        executable.display(),
                        timeout.as_millis()
                    )
                }
            }
        }
        ChildMode::Interactive => child
            .wait()
            .await
            .context("cannot wait for terminal-panel interactive process")?,
    };
    match mode {
        ChildMode::Helper {
            preserve_descendants: true,
            ..
        } => process_tree.disarm(),
        ChildMode::Helper {
            preserve_descendants: false,
            ..
        }
        | ChildMode::Interactive => process_tree.terminate(),
    }
    Ok(status)
}

fn tmux_args<const N: usize>(socket: &str, tail: [&str; N]) -> Vec<OsString> {
    std::iter::once(OsString::from("-L"))
        .chain(std::iter::once(OsString::from(socket)))
        .chain(tail.into_iter().map(OsString::from))
        .collect()
}

fn direct_shell_arguments(shell: &Path) -> Vec<OsString> {
    if cfg!(windows) {
        return Vec::new();
    }
    let _ = shell;
    vec![OsString::from("-i"), OsString::from("-l")]
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    mod unix {
        use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

        use super::super::*;
        use crate::{
            config::Settings,
            permissions::{PermissionManager, PermissionMode},
        };

        fn executable(path: &Path, source: &str) {
            fs::write(path, source).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
        }

        fn context(workspace: &Path) -> ToolContext {
            ToolContext::new(
                workspace.to_owned(),
                PermissionManager::new(
                    PermissionMode::BypassPermissions,
                    false,
                    Vec::new(),
                    Vec::new(),
                ),
            )
        }

        #[test]
        fn private_tmux_session_persists_and_is_killed_explicitly() {
            let root = tempfile::tempdir().unwrap();
            let workspace = root.path().join("workspace");
            let bin = root.path().join("bin");
            fs::create_dir_all(&workspace).unwrap();
            fs::create_dir_all(&bin).unwrap();
            let tmux = bin.join("tmux");
            let shell = bin.join("shell");
            executable(
                &tmux,
                r##"#!/bin/sh
base=$(dirname "$0")
printf '%s\n' "$*" >> "$base/log"
case "$*" in
  *"has-session"*) test -f "$base/state" ;;
  *"new-session"*) touch "$base/state" ;;
  *"kill-server"*) rm -f "$base/state" ;;
  *) exit 0 ;;
esac
"##,
            );
            executable(&shell, "#!/bin/sh\nexit 0\n");
            let mut panel = TerminalPanel::with_executables(
                Uuid::nil(),
                Some(tmux),
                shell,
                Duration::from_secs(5),
            );
            let context = context(&workspace);
            assert_eq!(
                panel.show(&context).unwrap(),
                TerminalPanelOutcome::Persistent
            );
            assert_eq!(
                panel.show(&context).unwrap(),
                TerminalPanelOutcome::Persistent
            );
            panel.shutdown(&context).unwrap();
            assert!(!bin.join("state").exists());
            let log = fs::read_to_string(bin.join("log")).unwrap();
            assert_eq!(log.matches("new-session").count(), 1);
            assert_eq!(log.matches("attach-session").count(), 2);
            assert_eq!(log.matches("kill-server").count(), 1);
            assert!(log.contains("bind-key -n M-j detach-client"));
        }

        #[test]
        fn preexisting_private_socket_is_never_adopted_or_killed() {
            let root = tempfile::tempdir().unwrap();
            let workspace = root.path().join("workspace");
            let bin = root.path().join("bin");
            fs::create_dir_all(&workspace).unwrap();
            fs::create_dir_all(&bin).unwrap();
            let tmux = bin.join("tmux");
            let shell = bin.join("shell");
            executable(
                &tmux,
                r##"#!/bin/sh
base=$(dirname "$0")
printf '%s\n' "$*" >> "$base/log"
case "$*" in
  *"has-session"*) test -f "$base/state" ;;
  *"kill-server"*) rm -f "$base/state" ;;
  *) exit 0 ;;
esac
"##,
            );
            executable(&shell, "#!/bin/sh\nexit 0\n");
            fs::write(bin.join("state"), "preexisting").unwrap();
            let mut panel = TerminalPanel::with_executables(
                Uuid::nil(),
                Some(tmux),
                shell,
                Duration::from_secs(5),
            );
            let error = panel.show(&context(&workspace)).unwrap_err();
            assert!(error.to_string().contains("already in use"));
            drop(panel);
            assert_eq!(
                fs::read_to_string(bin.join("state")).unwrap(),
                "preexisting"
            );
            let log = fs::read_to_string(bin.join("log")).unwrap();
            assert!(!log.contains("kill-server"));
        }

        #[test]
        fn direct_shell_fallback_is_bounded_to_the_selected_executable() {
            let root = tempfile::tempdir().unwrap();
            let workspace = root.path().join("workspace");
            let bin = root.path().join("bin");
            fs::create_dir_all(&workspace).unwrap();
            fs::create_dir_all(&bin).unwrap();
            let shell = bin.join("shell");
            executable(
                &shell,
                "#!/bin/sh\nbase=$(dirname \"$0\")\ntouch \"$base/direct\"\n",
            );
            let mut panel = TerminalPanel::with_executables(
                Uuid::nil(),
                None,
                shell,
                Duration::from_millis(100),
            );
            let context = context(&workspace);
            assert_eq!(panel.show(&context).unwrap(), TerminalPanelOutcome::Direct);
            assert!(bin.join("direct").exists());
            panel.shutdown(&context).unwrap();
        }

        #[test]
        fn helper_timeout_reaps_the_control_process() {
            let root = tempfile::tempdir().unwrap();
            let workspace = root.path().join("workspace");
            let bin = root.path().join("bin");
            fs::create_dir_all(&workspace).unwrap();
            fs::create_dir_all(&bin).unwrap();
            let tmux = bin.join("tmux");
            let shell = bin.join("shell");
            executable(&tmux, "#!/bin/sh\nsleep 5\n");
            executable(&shell, "#!/bin/sh\nexit 0\n");
            let mut panel = TerminalPanel::with_executables(
                Uuid::nil(),
                Some(tmux),
                shell,
                Duration::from_millis(40),
            );
            let started = std::time::Instant::now();
            let error = panel.show(&context(&workspace)).unwrap_err();
            assert!(error.to_string().contains("exceeded"));
            assert!(started.elapsed() < Duration::from_secs(2));
        }

        #[test]
        fn direct_shell_does_not_inherit_configured_credentials() {
            const SECRET: &str = "OAH_TERMINAL_PANEL_SECRET_TEST";
            let root = tempfile::tempdir().unwrap();
            let workspace = root.path().join("workspace");
            let bin = root.path().join("bin");
            fs::create_dir_all(&workspace).unwrap();
            fs::create_dir_all(&bin).unwrap();
            let shell = bin.join("shell");
            executable(
                &shell,
                "#!/bin/sh\nbase=$(dirname \"$0\")\nprintf '%s' \"${OAH_TERMINAL_PANEL_SECRET_TEST-unset}\" > \"$base/secret\"\n",
            );
            let context = context(&workspace);
            context
                .configure_secret_env_scrubber(&Settings {
                    raw: serde_json::json!({"mcpServers":{"private":{"auth":{
                        "type":"bearer-env", "env":SECRET
                    }}}}),
                })
                .unwrap();
            // SAFETY: the unique test variable is restored before the test returns.
            unsafe { std::env::set_var(SECRET, "must-not-leak") };
            let mut panel = TerminalPanel::with_executables(
                Uuid::nil(),
                None,
                shell,
                Duration::from_millis(100),
            );
            let result = panel.show(&context);
            unsafe { std::env::remove_var(SECRET) };
            assert_eq!(result.unwrap(), TerminalPanelOutcome::Direct);
            assert_eq!(fs::read_to_string(bin.join("secret")).unwrap(), "unset");
        }
    }
}

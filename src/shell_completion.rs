use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::CommandFactory;
use clap_complete::{Shell, generate};
use uuid::Uuid;

use crate::cli::{Cli, CompletionShell};

const MAX_COMPLETION_BYTES: usize = 1024 * 1024;

impl From<CompletionShell> for Shell {
    fn from(value: CompletionShell) -> Self {
        match value {
            CompletionShell::Bash => Self::Bash,
            CompletionShell::Zsh => Self::Zsh,
            CompletionShell::Fish => Self::Fish,
        }
    }
}

pub fn generate_completion(shell: CompletionShell) -> Result<Vec<u8>> {
    let mut command = Cli::command();
    let binary_name = command.get_name().to_owned();
    let mut output = Vec::new();
    generate(Shell::from(shell), &mut command, binary_name, &mut output);
    if output.is_empty() || output.len() > MAX_COMPLETION_BYTES {
        bail!("generated completion script is empty or exceeds {MAX_COMPLETION_BYTES} bytes")
    }
    Ok(output)
}

pub fn run_completion(shell: CompletionShell, output: Option<&Path>) -> Result<()> {
    let bytes = generate_completion(shell)?;
    if let Some(path) = output {
        write_new_private_file(path, &bytes)
    } else {
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        lock.write_all(&bytes)
            .context("cannot write shell completion to stdout")?;
        lock.flush().context("cannot flush shell completion stdout")
    }
}

fn write_new_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.as_os_str().is_empty() {
        bail!("completion output path is empty")
    }
    match fs::symlink_metadata(path) {
        Ok(_) => bail!("refusing to overwrite an existing completion output path"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("cannot inspect completion output path"),
    }
    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let metadata = fs::metadata(parent).context("completion output parent does not exist")?;
    if !metadata.is_dir() {
        bail!("completion output parent is not a directory")
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .context("completion output must have a UTF-8 file name")?;
    let temporary = temporary_path(parent, file_name);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let write_result = (|| -> Result<()> {
        let mut file = options
            .open(&temporary)
            .context("cannot create completion temporary file")?;
        file.write_all(bytes)
            .context("cannot write completion temporary file")?;
        file.sync_all()
            .context("cannot sync completion temporary file")?;
        fs::hard_link(&temporary, path)
            .context("cannot publish completion output without overwriting")?;
        Ok(())
    })();
    let cleanup_result = fs::remove_file(&temporary);
    write_result?;
    if let Err(error) = cleanup_result {
        return Err(error).context("completion output was published but temporary cleanup failed");
    }
    Ok(())
}

fn temporary_path(parent: &Path, file_name: &str) -> PathBuf {
    parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_supported_shells_generate_bounded_scripts() {
        for shell in [
            CompletionShell::Bash,
            CompletionShell::Zsh,
            CompletionShell::Fish,
        ] {
            let output = generate_completion(shell).unwrap();
            assert!(output.len() < MAX_COMPLETION_BYTES);
            assert!(
                String::from_utf8(output)
                    .unwrap()
                    .contains("open-agent-harness")
            );
        }
    }

    #[test]
    fn output_is_create_only_and_does_not_replace_existing_files() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("completion.zsh");
        write_new_private_file(&output, b"first").unwrap();
        assert_eq!(fs::read(&output).unwrap(), b"first");
        assert!(write_new_private_file(&output, b"second").is_err());
        assert_eq!(fs::read(&output).unwrap(), b"first");
    }

    #[cfg(unix)]
    #[test]
    fn output_rejects_symlink_targets() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        fs::write(&target, b"keep").unwrap();
        let output = directory.path().join("completion.zsh");
        symlink(&target, &output).unwrap();
        assert!(write_new_private_file(&output, b"replace").is_err());
        assert_eq!(fs::read(&target).unwrap(), b"keep");
    }
}

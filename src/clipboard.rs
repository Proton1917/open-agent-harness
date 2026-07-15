//! Provider-neutral clipboard image ingestion.
//!
//! The module deliberately has no UI, model-provider, network, or secret
//! dependencies. Platform commands are hidden behind [`ClipboardCommandRunner`]
//! so callers can test every path without touching the real clipboard.

use std::{
    env,
    error::Error,
    ffi::OsString,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::{fs::OpenOptionsExt, process::CommandExt};

/// Maximum accepted encoded image size. This matches the harness media budget.
pub const MAX_CLIPBOARD_IMAGE_BYTES: usize = 12 * 1024 * 1024;
/// Maximum width or height accepted from an image header.
pub const MAX_CLIPBOARD_IMAGE_DIMENSION: u32 = 16_384;
/// Maximum decoded pixel count accepted from an image header.
pub const MAX_CLIPBOARD_IMAGE_PIXELS: u64 = 100_000_000;
/// Wall-clock budget for one platform clipboard command.
pub const CLIPBOARD_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
pub const MAX_CLIPBOARD_TEXT_BYTES: usize = 1024 * 1024;

const MAX_COMMAND_STDERR_BYTES: usize = 16 * 1024;
const BASE64_OUTPUT_LIMIT: usize = (MAX_CLIPBOARD_IMAGE_BYTES / 3 + 1) * 4 + 4096;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A validated image ready to be encoded as a model media block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClipboardImage {
    pub bytes: Vec<u8>,
    pub media_type: &'static str,
    pub width: u32,
    pub height: u32,
}

/// Platform selector exposed for deterministic tests and nonstandard hosts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClipboardPlatform {
    MacOs,
    Linux,
    Windows,
    Wsl,
    Unsupported,
}

impl ClipboardPlatform {
    pub fn current() -> Self {
        if cfg!(target_os = "macos") {
            Self::MacOs
        } else if cfg!(target_os = "windows") {
            Self::Windows
        } else if cfg!(target_os = "linux") {
            if env::var_os("WSL_INTEROP").is_some() || env::var_os("WSL_DISTRO_NAME").is_some() {
                Self::Wsl
            } else {
                Self::Linux
            }
        } else {
            Self::Unsupported
        }
    }
}

/// Bounded command request. `output_file` is always a pre-created private file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClipboardCommand {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub timeout: Duration,
    pub max_stdout_bytes: usize,
    pub output_file: Option<PathBuf>,
}

/// Result returned by a command runner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClipboardCommandOutput {
    pub success: bool,
    pub status_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Injectable boundary for all operating-system clipboard commands.
pub trait ClipboardCommandRunner {
    fn run(&self, command: &ClipboardCommand) -> Result<ClipboardCommandOutput, ClipboardError>;
}

/// Production runner with timeout, bounded pipes, and process-tree cleanup.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClipboardCommandRunner;

impl ClipboardCommandRunner for SystemClipboardCommandRunner {
    fn run(&self, request: &ClipboardCommand) -> Result<ClipboardCommandOutput, ClipboardError> {
        let mut command = Command::new(&request.program);
        command
            .args(&request.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        apply_sanitized_environment(&mut command);
        #[cfg(unix)]
        command.process_group(0);

        let mut child = command.spawn().map_err(|error| {
            ClipboardError::Command(format!(
                "failed to start {}: {error}",
                request.program.to_string_lossy()
            ))
        })?;
        let process_id = child.id();
        let Some(stdout) = child.stdout.take() else {
            terminate_and_reap(&mut child, process_id);
            return Err(ClipboardError::Command(
                "clipboard stdout pipe missing".into(),
            ));
        };
        let Some(stderr) = child.stderr.take() else {
            terminate_and_reap(&mut child, process_id);
            return Err(ClipboardError::Command(
                "clipboard stderr pipe missing".into(),
            ));
        };
        let stdout_limit = request.max_stdout_bytes;
        let stdout_reader = thread::spawn(move || read_pipe_bounded(stdout, stdout_limit));
        let stderr_reader =
            thread::spawn(move || read_pipe_bounded(stderr, MAX_COMMAND_STDERR_BYTES));

        let deadline = Instant::now() + request.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => thread::sleep(POLL_INTERVAL),
                Ok(None) => {
                    terminate_and_reap(&mut child, process_id);
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    return Err(ClipboardError::Timeout(request.timeout));
                }
                Err(error) => {
                    terminate_and_reap(&mut child, process_id);
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    return Err(ClipboardError::Command(format!(
                        "failed to poll clipboard command: {error}"
                    )));
                }
            }
        };

        let stdout = join_pipe(stdout_reader, "stdout")?;
        let stderr = join_pipe(stderr_reader, "stderr")?;
        Ok(ClipboardCommandOutput {
            success: status.success(),
            status_code: status.code(),
            stdout,
            stderr,
        })
    }
}

/// Clipboard failures that are useful for diagnostics. The safe public entry
/// point intentionally converts all of these into `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClipboardError {
    Command(String),
    Timeout(Duration),
    OutputTooLarge { limit: usize },
    InvalidImage(String),
    TemporaryFile(String),
}

impl fmt::Display for ClipboardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Command(message) => write!(formatter, "clipboard command failed: {message}"),
            Self::Timeout(timeout) => {
                write!(formatter, "clipboard command timed out after {timeout:?}")
            }
            Self::OutputTooLarge { limit } => {
                write!(formatter, "clipboard image exceeds {limit} bytes")
            }
            Self::InvalidImage(message) => write!(formatter, "invalid clipboard image: {message}"),
            Self::TemporaryFile(message) => {
                write!(formatter, "clipboard temporary file failed: {message}")
            }
        }
    }
}

impl Error for ClipboardError {}

/// Reads the current clipboard and safely falls back to `None` for unavailable,
/// empty, malformed, oversized, or timed-out platform backends.
pub fn read_clipboard_image() -> Option<ClipboardImage> {
    try_read_clipboard_image_with(&SystemClipboardCommandRunner, ClipboardPlatform::current())
        .ok()
        .flatten()
}

/// Copies bounded plain text through the native clipboard command without a
/// shell. The child receives a scrubbed environment and a fixed timeout.
pub fn write_clipboard_text(text: &str) -> Result<(), ClipboardError> {
    if text.is_empty() {
        return Ok(());
    }
    if text.len() > MAX_CLIPBOARD_TEXT_BYTES {
        return Err(ClipboardError::OutputTooLarge {
            limit: MAX_CLIPBOARD_TEXT_BYTES,
        });
    }
    let commands: &[(&str, &[&str])] = match ClipboardPlatform::current() {
        ClipboardPlatform::MacOs => &[("pbcopy", &[])],
        ClipboardPlatform::Linux => &[
            ("wl-copy", &[]),
            ("xclip", &["-selection", "clipboard"]),
            ("xsel", &["--clipboard", "--input"]),
        ],
        ClipboardPlatform::Windows | ClipboardPlatform::Wsl => &[("clip.exe", &[])],
        ClipboardPlatform::Unsupported => {
            return Err(ClipboardError::Command(
                "clipboard text writing is unsupported on this platform".into(),
            ));
        }
    };
    let mut errors = Vec::new();
    for (program, arguments) in commands {
        match write_clipboard_text_command(program, arguments, text.as_bytes()) {
            Ok(()) => return Ok(()),
            Err(error) => errors.push(error.to_string()),
        }
    }
    Err(ClipboardError::Command(errors.join("; ")))
}

fn write_clipboard_text_command(
    program: &str,
    arguments: &[&str],
    bytes: &[u8],
) -> Result<(), ClipboardError> {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    apply_sanitized_environment(&mut command);
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .map_err(|error| ClipboardError::Command(format!("failed to start {program}: {error}")))?;
    let process_id = child.id();
    let Some(mut stdin) = child.stdin.take() else {
        terminate_and_reap(&mut child, process_id);
        return Err(ClipboardError::Command(format!(
            "clipboard stdin pipe missing for {program}"
        )));
    };
    let bytes = bytes.to_vec();
    let writer = thread::spawn(move || stdin.write_all(&bytes));
    let deadline = Instant::now() + CLIPBOARD_COMMAND_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(POLL_INTERVAL),
            Ok(None) => {
                terminate_and_reap(&mut child, process_id);
                let _ = writer.join();
                return Err(ClipboardError::Timeout(CLIPBOARD_COMMAND_TIMEOUT));
            }
            Err(error) => {
                terminate_and_reap(&mut child, process_id);
                let _ = writer.join();
                return Err(ClipboardError::Command(format!(
                    "failed to poll {program}: {error}"
                )));
            }
        }
    };
    writer
        .join()
        .map_err(|_| ClipboardError::Command("clipboard writer panicked".into()))?
        .map_err(|error| ClipboardError::Command(format!("clipboard write failed: {error}")))?;
    if !status.success() {
        return Err(ClipboardError::Command(format!(
            "{program} exited with {:?}",
            status.code()
        )));
    }
    Ok(())
}

/// Diagnostic/injectable variant. Missing commands and ordinary "no image"
/// responses return `Ok(None)`; safety-limit and malformed-image failures are
/// returned only after all compatible fallbacks have been exhausted.
pub fn try_read_clipboard_image_with<R: ClipboardCommandRunner + ?Sized>(
    runner: &R,
    platform: ClipboardPlatform,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    let mut deferred_error = None;
    let result = match platform {
        ClipboardPlatform::MacOs => read_macos(runner, &mut deferred_error),
        ClipboardPlatform::Linux => read_linux(runner, &mut deferred_error),
        ClipboardPlatform::Windows => read_windows(runner, false, &mut deferred_error),
        ClipboardPlatform::Wsl => read_windows(runner, true, &mut deferred_error),
        ClipboardPlatform::Unsupported => return Ok(None),
    };
    match result? {
        Some(image) => Ok(Some(image)),
        None => match deferred_error {
            Some(error) => Err(error),
            None => Ok(None),
        },
    }
}

fn read_macos<R: ClipboardCommandRunner + ?Sized>(
    runner: &R,
    deferred_error: &mut Option<ClipboardError>,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    // pbpaste is cheap and present on every supported macOS installation. Some
    // releases do not expose image flavors through it, so native JXA follows.
    if let Some(image) = attempt_stdout(
        runner,
        "/usr/bin/pbpaste",
        &["-Prefer", "png"],
        OutputEncoding::Raw,
        deferred_error,
    )? {
        return Ok(Some(image));
    }

    for pasteboard_type in [
        "public.png",
        "public.jpeg",
        "com.compuserve.gif",
        "org.webmproject.webp",
    ] {
        let temporary = PrivateTempOutput::new()?;
        let script = format!(
            "ObjC.import('AppKit'); ObjC.import('Foundation'); function run(argv) {{ \
             var d=$.NSPasteboard.generalPasteboard.dataForType('{pasteboard_type}'); \
             if (!d) return ''; var h=$.NSFileHandle.fileHandleForWritingAtPath($(argv[0])); \
             if (!h) return ''; h.truncateFileAtOffset(0); h.writeData(d); h.closeFile(); return ''; }}"
        );
        if let Some(image) = attempt_private_file(
            runner,
            "/usr/bin/osascript",
            &[
                OsString::from("-l"),
                OsString::from("JavaScript"),
                OsString::from("-e"),
                OsString::from(script),
                OsString::from("--"),
                temporary.path().as_os_str().to_owned(),
            ],
            &temporary,
            deferred_error,
        )? {
            return Ok(Some(image));
        }
    }
    Ok(None)
}

fn read_linux<R: ClipboardCommandRunner + ?Sized>(
    runner: &R,
    deferred_error: &mut Option<ClipboardError>,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    for media_type in ["image/png", "image/jpeg", "image/gif", "image/webp"] {
        if let Some(image) = attempt_stdout(
            runner,
            "wl-paste",
            &["--no-newline", "--type", media_type],
            OutputEncoding::Raw,
            deferred_error,
        )? {
            return Ok(Some(image));
        }
        if let Some(image) = attempt_stdout(
            runner,
            "xclip",
            &["-selection", "clipboard", "-t", media_type, "-o"],
            OutputEncoding::Raw,
            deferred_error,
        )? {
            return Ok(Some(image));
        }
    }
    Ok(None)
}

fn read_windows<R: ClipboardCommandRunner + ?Sized>(
    runner: &R,
    wsl: bool,
    deferred_error: &mut Option<ClipboardError>,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    const IMAGE_SCRIPT_PREFIX: &str = "Add-Type -AssemblyName System.Windows.Forms; Add-Type -AssemblyName System.Drawing; $i=[Windows.Forms.Clipboard]::GetImage(); if ($null -eq $i) { exit 3 }; $m=New-Object IO.MemoryStream; try { $i.Save($m,[Drawing.Imaging.ImageFormat]::Png);";
    for program in ["powershell.exe", "pwsh"] {
        if wsl {
            let script = format!(
                "{IMAGE_SCRIPT_PREFIX} [Convert]::ToBase64String($m.ToArray()) }} finally {{ $m.Dispose(); $i.Dispose() }}"
            );
            if let Some(image) = attempt_stdout_os(
                runner,
                program,
                &powershell_args(script, None),
                OutputEncoding::Base64,
                deferred_error,
            )? {
                return Ok(Some(image));
            }
        } else {
            let temporary = PrivateTempOutput::new()?;
            let script = format!(
                "param([string]$OutputPath); {IMAGE_SCRIPT_PREFIX} [IO.File]::WriteAllBytes($OutputPath,$m.ToArray()) }} finally {{ $m.Dispose(); $i.Dispose() }}"
            );
            if let Some(image) = attempt_private_file(
                runner,
                program,
                &powershell_args(script, Some(temporary.path())),
                &temporary,
                deferred_error,
            )? {
                return Ok(Some(image));
            }
        }
    }
    Ok(None)
}

fn powershell_args(script: String, output_path: Option<&Path>) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("-NoLogo"),
        OsString::from("-NoProfile"),
        OsString::from("-NonInteractive"),
        OsString::from("-Sta"),
        OsString::from("-Command"),
        OsString::from(script),
    ];
    if let Some(path) = output_path {
        args.push(path.as_os_str().to_owned());
    }
    args
}

#[derive(Clone, Copy)]
enum OutputEncoding {
    Raw,
    Base64,
}

fn attempt_stdout<R: ClipboardCommandRunner + ?Sized>(
    runner: &R,
    program: &str,
    args: &[&str],
    encoding: OutputEncoding,
    deferred_error: &mut Option<ClipboardError>,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    let args = args.iter().map(OsString::from).collect::<Vec<_>>();
    attempt_stdout_os(runner, program, &args, encoding, deferred_error)
}

fn attempt_stdout_os<R: ClipboardCommandRunner + ?Sized>(
    runner: &R,
    program: &str,
    args: &[OsString],
    encoding: OutputEncoding,
    deferred_error: &mut Option<ClipboardError>,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    let output_limit = match encoding {
        OutputEncoding::Raw => MAX_CLIPBOARD_IMAGE_BYTES + 1,
        OutputEncoding::Base64 => BASE64_OUTPUT_LIMIT,
    };
    let command = ClipboardCommand {
        program: OsString::from(program),
        args: args.to_vec(),
        timeout: CLIPBOARD_COMMAND_TIMEOUT,
        max_stdout_bytes: output_limit,
        output_file: None,
    };
    let output = match runner.run(&command) {
        Ok(output) => output,
        Err(error) => {
            remember_safety_error(deferred_error, error);
            return Ok(None);
        }
    };
    if !output.success || output.stdout.is_empty() {
        return Ok(None);
    }
    let bytes = match encoding {
        OutputEncoding::Raw => output.stdout,
        OutputEncoding::Base64 => match decode_base64(&output.stdout) {
            Ok(bytes) => bytes,
            Err(error) => {
                remember_safety_error(deferred_error, error);
                return Ok(None);
            }
        },
    };
    validate_attempt(bytes, deferred_error)
}

fn attempt_private_file<R: ClipboardCommandRunner + ?Sized>(
    runner: &R,
    program: &str,
    args: &[OsString],
    temporary: &PrivateTempOutput,
    deferred_error: &mut Option<ClipboardError>,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    let command = ClipboardCommand {
        program: OsString::from(program),
        args: args.to_vec(),
        timeout: CLIPBOARD_COMMAND_TIMEOUT,
        max_stdout_bytes: MAX_COMMAND_STDERR_BYTES,
        output_file: Some(temporary.path().to_owned()),
    };
    let output = match runner.run(&command) {
        Ok(output) => output,
        Err(error) => {
            remember_safety_error(deferred_error, error);
            return Ok(None);
        }
    };
    if !output.success {
        return Ok(None);
    }
    let bytes = match temporary.read_bounded(MAX_CLIPBOARD_IMAGE_BYTES) {
        Ok(bytes) => bytes,
        Err(error) => {
            remember_safety_error(deferred_error, error);
            return Ok(None);
        }
    };
    if bytes.is_empty() {
        return Ok(None);
    }
    validate_attempt(bytes, deferred_error)
}

fn validate_attempt(
    bytes: Vec<u8>,
    deferred_error: &mut Option<ClipboardError>,
) -> Result<Option<ClipboardImage>, ClipboardError> {
    match validate_image(bytes) {
        Ok(image) => Ok(Some(image)),
        // Clipboard text or a backend that ignores its requested MIME is an
        // ordinary no-image response, not a malformed image error.
        Err(ClipboardError::InvalidImage(message)) if message == "unsupported format" => Ok(None),
        Err(error) => {
            remember_safety_error(deferred_error, error);
            Ok(None)
        }
    }
}

fn remember_safety_error(slot: &mut Option<ClipboardError>, error: ClipboardError) {
    if matches!(
        error,
        ClipboardError::OutputTooLarge { .. }
            | ClipboardError::InvalidImage(_)
            | ClipboardError::TemporaryFile(_)
    ) {
        *slot = Some(error);
    }
}

fn validate_image(bytes: Vec<u8>) -> Result<ClipboardImage, ClipboardError> {
    if bytes.len() > MAX_CLIPBOARD_IMAGE_BYTES {
        return Err(ClipboardError::OutputTooLarge {
            limit: MAX_CLIPBOARD_IMAGE_BYTES,
        });
    }
    let (media_type, width, height) = image_dimensions(&bytes)?;
    if width == 0 || height == 0 {
        return Err(ClipboardError::InvalidImage("zero-sized image".into()));
    }
    if width > MAX_CLIPBOARD_IMAGE_DIMENSION || height > MAX_CLIPBOARD_IMAGE_DIMENSION {
        return Err(ClipboardError::InvalidImage(format!(
            "dimensions {width}x{height} exceed {MAX_CLIPBOARD_IMAGE_DIMENSION}"
        )));
    }
    let pixels = u64::from(width) * u64::from(height);
    if pixels > MAX_CLIPBOARD_IMAGE_PIXELS {
        return Err(ClipboardError::InvalidImage(format!(
            "pixel count {pixels} exceeds {MAX_CLIPBOARD_IMAGE_PIXELS}"
        )));
    }
    Ok(ClipboardImage {
        bytes,
        media_type,
        width,
        height,
    })
}

fn image_dimensions(bytes: &[u8]) -> Result<(&'static str, u32, u32), ClipboardError> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        if bytes.len() < 24 || &bytes[12..16] != b"IHDR" {
            return Err(ClipboardError::InvalidImage("truncated PNG header".into()));
        }
        return Ok((
            "image/png",
            u32::from_be_bytes(bytes[16..20].try_into().expect("fixed slice")),
            u32::from_be_bytes(bytes[20..24].try_into().expect("fixed slice")),
        ));
    }
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        let (width, height) = jpeg_dimensions(bytes)?;
        return Ok(("image/jpeg", width, height));
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        if bytes.len() < 10 {
            return Err(ClipboardError::InvalidImage("truncated GIF header".into()));
        }
        return Ok((
            "image/gif",
            u16::from_le_bytes([bytes[6], bytes[7]]).into(),
            u16::from_le_bytes([bytes[8], bytes[9]]).into(),
        ));
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        let (width, height) = webp_dimensions(bytes)?;
        return Ok(("image/webp", width, height));
    }
    Err(ClipboardError::InvalidImage("unsupported format".into()))
}

fn jpeg_dimensions(bytes: &[u8]) -> Result<(u32, u32), ClipboardError> {
    let mut offset = 2usize;
    let mut segments = 0usize;
    while offset < bytes.len() && segments < 4096 {
        if bytes[offset] != 0xff {
            offset += 1;
            continue;
        }
        while offset < bytes.len() && bytes[offset] == 0xff {
            offset += 1;
        }
        if offset >= bytes.len() {
            break;
        }
        let marker = bytes[offset];
        offset += 1;
        segments += 1;
        if marker == 0xd9 || marker == 0xda {
            break;
        }
        if marker == 0x01 || marker == 0xd8 || (0xd0..=0xd7).contains(&marker) {
            continue;
        }
        if offset + 2 > bytes.len() {
            break;
        }
        let length = usize::from(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]));
        if length < 2
            || offset
                .checked_add(length)
                .is_none_or(|end| end > bytes.len())
        {
            return Err(ClipboardError::InvalidImage("invalid JPEG segment".into()));
        }
        if matches!(
            marker,
            0xc0 | 0xc1
                | 0xc2
                | 0xc3
                | 0xc5
                | 0xc6
                | 0xc7
                | 0xc9
                | 0xca
                | 0xcb
                | 0xcd
                | 0xce
                | 0xcf
        ) {
            if length < 7 {
                return Err(ClipboardError::InvalidImage("truncated JPEG SOF".into()));
            }
            let height = u16::from_be_bytes([bytes[offset + 3], bytes[offset + 4]]).into();
            let width = u16::from_be_bytes([bytes[offset + 5], bytes[offset + 6]]).into();
            return Ok((width, height));
        }
        offset += length;
    }
    Err(ClipboardError::InvalidImage(
        "JPEG dimensions not found".into(),
    ))
}

fn webp_dimensions(bytes: &[u8]) -> Result<(u32, u32), ClipboardError> {
    let mut offset = 12usize;
    let mut chunks = 0usize;
    while offset + 8 <= bytes.len() && chunks < 1024 {
        let kind = &bytes[offset..offset + 4];
        let length = u32::from_le_bytes(
            bytes[offset + 4..offset + 8]
                .try_into()
                .expect("fixed slice"),
        ) as usize;
        let data = offset + 8;
        let end = data
            .checked_add(length)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| ClipboardError::InvalidImage("invalid WebP chunk".into()))?;
        match kind {
            b"VP8X" if length >= 10 => {
                let width = 1 + read_u24_le(&bytes[data + 4..data + 7]);
                let height = 1 + read_u24_le(&bytes[data + 7..data + 10]);
                return Ok((width, height));
            }
            b"VP8 " if length >= 10 && &bytes[data + 3..data + 6] == b"\x9d\x01\x2a" => {
                let width =
                    u32::from(u16::from_le_bytes([bytes[data + 6], bytes[data + 7]]) & 0x3fff);
                let height =
                    u32::from(u16::from_le_bytes([bytes[data + 8], bytes[data + 9]]) & 0x3fff);
                return Ok((width, height));
            }
            b"VP8L" if length >= 5 && bytes[data] == 0x2f => {
                let bits =
                    u32::from_le_bytes(bytes[data + 1..data + 5].try_into().expect("fixed slice"));
                return Ok(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1));
            }
            _ => {}
        }
        offset = end + (length & 1);
        chunks += 1;
    }
    Err(ClipboardError::InvalidImage(
        "WebP dimensions not found".into(),
    ))
}

fn read_u24_le(bytes: &[u8]) -> u32 {
    u32::from(bytes[0]) | (u32::from(bytes[1]) << 8) | (u32::from(bytes[2]) << 16)
}

fn decode_base64(input: &[u8]) -> Result<Vec<u8>, ClipboardError> {
    let mut output = Vec::with_capacity(input.len().saturating_mul(3) / 4);
    let mut quartet = [0u8; 4];
    let mut used = 0usize;
    let mut finished = false;
    for &byte in input {
        if byte.is_ascii_whitespace() {
            continue;
        }
        if finished {
            return Err(ClipboardError::InvalidImage("invalid base64 suffix".into()));
        }
        quartet[used] = byte;
        used += 1;
        if used == 4 {
            let a = base64_value(quartet[0])?;
            let b = base64_value(quartet[1])?;
            output.push((a << 2) | (b >> 4));
            if quartet[2] == b'=' {
                if quartet[3] != b'=' {
                    return Err(ClipboardError::InvalidImage(
                        "invalid base64 padding".into(),
                    ));
                }
                finished = true;
            } else {
                let c = base64_value(quartet[2])?;
                output.push((b << 4) | (c >> 2));
                if quartet[3] == b'=' {
                    finished = true;
                } else {
                    let d = base64_value(quartet[3])?;
                    output.push((c << 6) | d);
                }
            }
            if output.len() > MAX_CLIPBOARD_IMAGE_BYTES {
                return Err(ClipboardError::OutputTooLarge {
                    limit: MAX_CLIPBOARD_IMAGE_BYTES,
                });
            }
            used = 0;
        }
    }
    if used != 0 || output.is_empty() {
        return Err(ClipboardError::InvalidImage(
            "incomplete base64 data".into(),
        ));
    }
    Ok(output)
}

fn base64_value(byte: u8) -> Result<u8, ClipboardError> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(ClipboardError::InvalidImage(
            "invalid base64 character".into(),
        )),
    }
}

fn apply_sanitized_environment(command: &mut Command) {
    const ALLOWED: &[&str] = &[
        "PATH",
        "HOME",
        "TMPDIR",
        "TMP",
        "TEMP",
        "DISPLAY",
        "WAYLAND_DISPLAY",
        "XDG_RUNTIME_DIR",
        "XAUTHORITY",
        "WSL_INTEROP",
        "WSL_DISTRO_NAME",
        "SystemRoot",
        "WINDIR",
    ];
    let values = ALLOWED
        .iter()
        .filter_map(|name| env::var_os(name).map(|value| (*name, value)))
        .collect::<Vec<_>>();
    command.env_clear();
    command.envs(values);
}

fn read_pipe_bounded<R: Read>(reader: R, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::FileTooLarge,
            "pipe limit exceeded",
        ));
    }
    Ok(bytes)
}

fn join_pipe(
    reader: thread::JoinHandle<io::Result<Vec<u8>>>,
    name: &str,
) -> Result<Vec<u8>, ClipboardError> {
    match reader.join() {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(error)) if error.kind() == io::ErrorKind::FileTooLarge => {
            Err(ClipboardError::OutputTooLarge {
                limit: MAX_CLIPBOARD_IMAGE_BYTES,
            })
        }
        Ok(Err(error)) => Err(ClipboardError::Command(format!(
            "failed to read clipboard {name}: {error}"
        ))),
        Err(_) => Err(ClipboardError::Command(format!(
            "clipboard {name} reader panicked"
        ))),
    }
}

fn terminate_and_reap(child: &mut Child, process_id: u32) {
    #[cfg(unix)]
    {
        const SIGKILL: i32 = 9;
        unsafe extern "C" {
            fn kill(process_id: i32, signal: i32) -> i32;
        }
        // SAFETY: the child was placed in a new process group whose id equals
        // its pid. A negative id addresses only that dedicated group.
        unsafe {
            let _ = kill(-(process_id as i32), SIGKILL);
        }
    }
    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    let _ = child.kill();
    let _ = child.wait();
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct PrivateTempOutput {
    directory: PathBuf,
    path: PathBuf,
    file: Option<File>,
}

impl PrivateTempOutput {
    fn new() -> Result<Self, ClipboardError> {
        for _ in 0..32 {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let directory = env::temp_dir().join(format!(
                "oah-clipboard-{}-{nonce:x}-{counter:x}",
                std::process::id()
            ));
            match fs::create_dir(&directory) {
                Ok(()) => {
                    #[cfg(unix)]
                    fs::set_permissions(
                        &directory,
                        std::os::unix::fs::PermissionsExt::from_mode(0o700),
                    )
                    .map_err(|error| ClipboardError::TemporaryFile(error.to_string()))?;
                    let path = directory.join("image.bin");
                    let mut options = OpenOptions::new();
                    options.read(true).write(true).create_new(true);
                    #[cfg(unix)]
                    options.mode(0o600);
                    let file = match options.open(&path) {
                        Ok(file) => file,
                        Err(error) => {
                            let _ = fs::remove_dir(&directory);
                            return Err(ClipboardError::TemporaryFile(error.to_string()));
                        }
                    };
                    return Ok(Self {
                        directory,
                        path,
                        file: Some(file),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(ClipboardError::TemporaryFile(error.to_string())),
            }
        }
        Err(ClipboardError::TemporaryFile(
            "cannot allocate collision-free path".into(),
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn read_bounded(&self, limit: usize) -> Result<Vec<u8>, ClipboardError> {
        // Read from the original create_new handle rather than reopening the
        // pathname. Even if a backend unexpectedly replaces the directory
        // entry, this cannot follow a substituted symlink or another file.
        let mut file = self
            .file
            .as_ref()
            .expect("private output handle remains open until drop")
            .try_clone()
            .map_err(|error| {
                ClipboardError::TemporaryFile(format!(
                    "cannot clone {} handle: {error}",
                    self.path.display()
                ))
            })?;
        let length = file
            .metadata()
            .map_err(|error| {
                ClipboardError::TemporaryFile(format!(
                    "cannot inspect {}: {error}",
                    self.path.display()
                ))
            })?
            .len();
        if length > limit as u64 {
            return Err(ClipboardError::OutputTooLarge { limit });
        }
        file.seek(SeekFrom::Start(0)).map_err(|error| {
            ClipboardError::TemporaryFile(format!("cannot seek {}: {error}", self.path.display()))
        })?;
        let mut bytes = Vec::new();
        file.take(limit.saturating_add(1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|error| {
                ClipboardError::TemporaryFile(format!(
                    "cannot read {}: {error}",
                    self.path.display()
                ))
            })?;
        if bytes.len() > limit {
            return Err(ClipboardError::OutputTooLarge { limit });
        }
        Ok(bytes)
    }
}

impl Drop for PrivateTempOutput {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_dir(&self.directory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::VecDeque, sync::Mutex};

    #[derive(Clone)]
    enum MockReply {
        Stdout(Vec<u8>),
        PrivateFile(Vec<u8>),
        NoImage,
        Error(ClipboardError),
    }

    struct MockRunner {
        replies: Mutex<VecDeque<MockReply>>,
        calls: Mutex<Vec<ClipboardCommand>>,
        private_paths: Mutex<Vec<PathBuf>>,
    }

    impl MockRunner {
        fn new(replies: impl IntoIterator<Item = MockReply>) -> Self {
            Self {
                replies: Mutex::new(replies.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
                private_paths: Mutex::new(Vec::new()),
            }
        }
    }

    impl ClipboardCommandRunner for MockRunner {
        fn run(
            &self,
            command: &ClipboardCommand,
        ) -> Result<ClipboardCommandOutput, ClipboardError> {
            self.calls.lock().unwrap().push(command.clone());
            match self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(MockReply::NoImage)
            {
                MockReply::Stdout(stdout) => Ok(ClipboardCommandOutput {
                    success: true,
                    status_code: Some(0),
                    stdout,
                    stderr: Vec::new(),
                }),
                MockReply::PrivateFile(bytes) => {
                    let path = command.output_file.as_ref().expect("private output path");
                    self.private_paths.lock().unwrap().push(path.clone());
                    fs::write(path, bytes).unwrap();
                    Ok(ClipboardCommandOutput {
                        success: true,
                        status_code: Some(0),
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    })
                }
                MockReply::NoImage => Ok(ClipboardCommandOutput {
                    success: false,
                    status_code: Some(3),
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                }),
                MockReply::Error(error) => Err(error),
            }
        }
    }

    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes
    }

    fn jpeg(width: u16, height: u16) -> Vec<u8> {
        let mut bytes = vec![0xff, 0xd8, 0xff, 0xc0, 0, 7, 8];
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&[0xff, 0xd9]);
        bytes
    }

    fn base64_encode(bytes: &[u8]) -> Vec<u8> {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut output = Vec::new();
        for chunk in bytes.chunks(3) {
            let a = chunk[0];
            let b = *chunk.get(1).unwrap_or(&0);
            let c = *chunk.get(2).unwrap_or(&0);
            output.push(TABLE[(a >> 2) as usize]);
            output.push(TABLE[(((a & 3) << 4) | (b >> 4)) as usize]);
            output.push(if chunk.len() > 1 {
                TABLE[(((b & 15) << 2) | (c >> 6)) as usize]
            } else {
                b'='
            });
            output.push(if chunk.len() > 2 {
                TABLE[(c & 63) as usize]
            } else {
                b'='
            });
        }
        output
    }

    #[test]
    fn linux_prefers_png_and_stops_after_first_valid_image() {
        let runner = MockRunner::new([MockReply::Stdout(png(640, 480))]);
        let image = try_read_clipboard_image_with(&runner, ClipboardPlatform::Linux)
            .unwrap()
            .unwrap();
        assert_eq!(image.media_type, "image/png");
        assert_eq!((image.width, image.height), (640, 480));
        let calls = runner.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].program, "wl-paste");
    }

    #[test]
    fn linux_falls_back_from_wayland_to_xclip() {
        let runner = MockRunner::new([
            MockReply::Error(ClipboardError::Command("missing wl-paste".into())),
            MockReply::Stdout(jpeg(320, 200)),
        ]);
        let image = try_read_clipboard_image_with(&runner, ClipboardPlatform::Linux)
            .unwrap()
            .unwrap();
        assert_eq!(image.media_type, "image/jpeg");
        let calls = runner.calls.lock().unwrap();
        assert_eq!(calls[0].program, "wl-paste");
        assert_eq!(calls[1].program, "xclip");
    }

    #[test]
    fn text_clipboard_and_command_errors_safely_return_none() {
        let mut replies = vec![MockReply::Stdout(b"plain text".to_vec())];
        replies.extend((0..7).map(|_| MockReply::NoImage));
        let runner = MockRunner::new(replies);
        assert_eq!(
            try_read_clipboard_image_with(&runner, ClipboardPlatform::Linux).unwrap(),
            None
        );
    }

    #[test]
    fn malformed_dimensions_are_reported_after_fallbacks() {
        let mut malformed = png(0, 10);
        malformed.extend_from_slice(b"ignored");
        let mut replies = vec![MockReply::Stdout(malformed)];
        replies.extend((0..7).map(|_| MockReply::NoImage));
        let runner = MockRunner::new(replies);
        assert!(matches!(
            try_read_clipboard_image_with(&runner, ClipboardPlatform::Linux),
            Err(ClipboardError::InvalidImage(_))
        ));
    }

    #[test]
    fn wsl_decodes_bounded_base64_png() {
        let runner = MockRunner::new([MockReply::Stdout(base64_encode(&png(80, 60)))]);
        let image = try_read_clipboard_image_with(&runner, ClipboardPlatform::Wsl)
            .unwrap()
            .unwrap();
        assert_eq!((image.width, image.height), (80, 60));
        assert_eq!(runner.calls.lock().unwrap()[0].program, "powershell.exe");
    }

    #[test]
    fn windows_private_output_is_removed_after_read() {
        let runner = MockRunner::new([MockReply::PrivateFile(png(10, 20))]);
        let image = try_read_clipboard_image_with(&runner, ClipboardPlatform::Windows)
            .unwrap()
            .unwrap();
        assert_eq!((image.width, image.height), (10, 20));
        let paths = runner.private_paths.lock().unwrap();
        assert_eq!(paths.len(), 1);
        assert!(!paths[0].exists());
        assert!(!paths[0].parent().unwrap().exists());
    }

    #[cfg(unix)]
    #[test]
    fn temporary_output_has_private_unix_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let temporary = PrivateTempOutput::new().unwrap();
        assert_eq!(
            fs::metadata(temporary.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(temporary.path().parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    #[test]
    fn gif_and_webp_headers_are_validated() {
        let mut gif = b"GIF89a".to_vec();
        gif.extend_from_slice(&12u16.to_le_bytes());
        gif.extend_from_slice(&34u16.to_le_bytes());
        let gif = validate_image(gif).unwrap();
        assert_eq!(
            (gif.media_type, gif.width, gif.height),
            ("image/gif", 12, 34)
        );

        let mut webp = b"RIFF\x12\0\0\0WEBPVP8X\x0a\0\0\0\0\0\0\0".to_vec();
        webp.extend_from_slice(&[1, 0, 0, 2, 0, 0]);
        let webp = validate_image(webp).unwrap();
        assert_eq!(
            (webp.media_type, webp.width, webp.height),
            ("image/webp", 2, 3)
        );
    }

    #[test]
    fn dimension_and_byte_limits_fail_closed() {
        assert!(matches!(
            validate_image(png(MAX_CLIPBOARD_IMAGE_DIMENSION + 1, 1)),
            Err(ClipboardError::InvalidImage(_))
        ));
        assert!(matches!(
            validate_image(vec![0; MAX_CLIPBOARD_IMAGE_BYTES + 1]),
            Err(ClipboardError::OutputTooLarge { .. })
        ));
    }

    #[test]
    fn timeout_is_an_unavailable_backend_not_a_panic() {
        let mut replies = vec![MockReply::Error(ClipboardError::Timeout(
            CLIPBOARD_COMMAND_TIMEOUT,
        ))];
        replies.extend((0..7).map(|_| MockReply::NoImage));
        let runner = MockRunner::new(replies);
        assert_eq!(
            try_read_clipboard_image_with(&runner, ClipboardPlatform::Linux).unwrap(),
            None
        );
    }
}

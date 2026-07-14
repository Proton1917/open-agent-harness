use std::{
    ffi::OsString,
    fmt,
    io::{self, Read, Write},
    net::{
        IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs,
    },
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

#[cfg(unix)]
use std::{
    path::PathBuf,
    process::{Command, ExitStatus},
};

#[cfg(unix)]
use std::os::unix::{
    fs::{FileTypeExt, PermissionsExt},
    net::{UnixListener, UnixStream},
    process::ExitStatusExt,
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use url::{Host, Url};
use uuid::Uuid;

const INTERNAL_BRIDGE_MODE: &str = "__open_agent_harness_sandbox_proxy_bridge";
#[cfg(target_os = "linux")]
pub(crate) const BRIDGE_PORT: u16 = 39_091;
const MAX_PROXY_CONNECTIONS: usize = 32;
const MAX_PROXY_HEADER_BYTES: usize = 64 * 1024;
const MAX_PROXY_TRANSFER_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RESOLVED_ADDRESSES: usize = 16;
const PROXY_IO_TIMEOUT: Duration = Duration::from_secs(60);
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct DomainProxy {
    inner: Arc<DomainProxyInner>,
}

struct DomainProxyInner {
    tcp_addr: SocketAddr,
    #[cfg(any(target_os = "macos", target_os = "linux", test))]
    token: String,
    #[cfg(unix)]
    unix_socket: PathBuf,
    #[cfg(unix)]
    unix_directory: PathBuf,
    stop: Arc<AtomicBool>,
}

impl fmt::Debug for DomainProxy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DomainProxy")
            .field("tcp_addr", &self.inner.tcp_addr)
            .field("authenticated", &true)
            .finish_non_exhaustive()
    }
}

impl Drop for DomainProxyInner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.tcp_addr, Duration::from_millis(100));
        #[cfg(unix)]
        {
            let _ = UnixStream::connect(&self.unix_socket);
            let _ = std::fs::remove_file(&self.unix_socket);
            let _ = std::fs::remove_dir(&self.unix_directory);
        }
    }
}

#[derive(Clone)]
struct ProxyPolicy {
    allowed: Arc<Vec<DomainPattern>>,
    allow_private_network: bool,
    expected_http_authorization: Arc<String>,
    expected_socks_user: Arc<Vec<u8>>,
    expected_socks_password: Arc<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DomainPattern {
    Exact(String),
    Subdomains(String),
}

impl DomainProxy {
    pub(crate) fn start(domains: &[String], allow_private_network: bool) -> Result<Self> {
        let allowed = domains
            .iter()
            .map(|domain| parse_domain_pattern(domain))
            .collect::<Result<Vec<_>>>()?;
        if allowed.is_empty() {
            bail!("sandbox domain proxy 至少需要一个 allowedDomains 条目")
        }

        let token = Uuid::new_v4().simple().to_string();
        let expected = BASE64_STANDARD.encode(format!("oah:{token}"));
        let policy = ProxyPolicy {
            allowed: Arc::new(allowed),
            allow_private_network,
            expected_http_authorization: Arc::new(format!("Basic {expected}")),
            expected_socks_user: Arc::new(b"oah".to_vec()),
            expected_socks_password: Arc::new(token.as_bytes().to_vec()),
        };
        let stop = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));

        let tcp =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).context("无法启动 sandbox domain proxy")?;
        tcp.set_nonblocking(true)
            .context("无法配置 sandbox domain proxy")?;
        let tcp_addr = tcp.local_addr().context("无法读取 sandbox proxy 地址")?;
        spawn_tcp_accept_loop(tcp, policy.clone(), Arc::clone(&stop), Arc::clone(&active))?;

        #[cfg(unix)]
        let unix_setup = (|| -> Result<(PathBuf, PathBuf)> {
            // AF_UNIX paths are capped at roughly 104 bytes on macOS. `/tmp`
            // keeps the broker address short; the random 0700 directory and
            // authenticated protocol keep it private.
            let directory = PathBuf::from("/tmp").join(format!("oahp-{}", Uuid::new_v4().simple()));
            std::fs::create_dir(&directory)
                .with_context(|| format!("无法创建 sandbox proxy 目录 {}", directory.display()))?;
            if let Err(error) =
                std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
            {
                let _ = std::fs::remove_dir(&directory);
                return Err(error).context("无法保护 sandbox proxy 目录");
            }
            let socket = directory.join("broker.sock");
            let listener = match UnixListener::bind(&socket) {
                Ok(listener) => listener,
                Err(error) => {
                    let _ = std::fs::remove_dir(&directory);
                    return Err(error).context("无法启动 sandbox Unix proxy broker");
                }
            };
            if let Err(error) =
                std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
            {
                let _ = std::fs::remove_file(&socket);
                let _ = std::fs::remove_dir(&directory);
                return Err(error).context("无法保护 sandbox Unix proxy socket");
            }
            if let Err(error) = listener.set_nonblocking(true) {
                let _ = std::fs::remove_file(&socket);
                let _ = std::fs::remove_dir(&directory);
                return Err(error).context("无法配置 sandbox Unix proxy broker");
            }
            if let Err(error) =
                spawn_unix_accept_loop(listener, policy, Arc::clone(&stop), Arc::clone(&active))
            {
                let _ = std::fs::remove_file(&socket);
                let _ = std::fs::remove_dir(&directory);
                return Err(error);
            }
            Ok((socket, directory))
        })();
        #[cfg(unix)]
        let (unix_socket, unix_directory) = match unix_setup {
            Ok(paths) => paths,
            Err(error) => {
                stop.store(true, Ordering::Release);
                let _ = TcpStream::connect_timeout(&tcp_addr, Duration::from_millis(100));
                return Err(error);
            }
        };

        Ok(Self {
            inner: Arc::new(DomainProxyInner {
                tcp_addr,
                #[cfg(any(target_os = "macos", target_os = "linux", test))]
                token,
                #[cfg(unix)]
                unix_socket,
                #[cfg(unix)]
                unix_directory,
                stop,
            }),
        })
    }

    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn tcp_port(&self) -> u16 {
        self.inner.tcp_addr.port()
    }

    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn http_url(&self) -> String {
        format!(
            "http://oah:{}@127.0.0.1:{}",
            self.inner.token,
            self.tcp_port()
        )
    }

    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn socks_url(&self) -> String {
        format!(
            "socks5h://oah:{}@127.0.0.1:{}",
            self.inner.token,
            self.tcp_port()
        )
    }

    #[cfg(any(target_os = "linux", test))]
    pub(crate) fn token(&self) -> &str {
        &self.inner.token
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn unix_socket(&self) -> &std::path::Path {
        &self.inner.unix_socket
    }
}

fn spawn_tcp_accept_loop(
    listener: TcpListener,
    policy: ProxyPolicy,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) -> Result<()> {
    thread::Builder::new()
        .name("oah-domain-proxy-tcp".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, address)) => {
                        if !address.ip().is_loopback() {
                            let _ = stream.shutdown(Shutdown::Both);
                            continue;
                        }
                        dispatch_client(
                            ClientStream::Tcp(stream),
                            policy.clone(),
                            Arc::clone(&active),
                        );
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => break,
                }
            }
        })
        .context("无法创建 sandbox proxy listener thread")?;
    Ok(())
}

#[cfg(unix)]
fn spawn_unix_accept_loop(
    listener: UnixListener,
    policy: ProxyPolicy,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) -> Result<()> {
    thread::Builder::new()
        .name("oah-domain-proxy-unix".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => dispatch_client(
                        ClientStream::Unix(stream),
                        policy.clone(),
                        Arc::clone(&active),
                    ),
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => break,
                }
            }
        })
        .context("无法创建 sandbox Unix proxy listener thread")?;
    Ok(())
}

fn dispatch_client(client: ClientStream, policy: ProxyPolicy, active: Arc<AtomicUsize>) {
    if active
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < MAX_PROXY_CONNECTIONS).then_some(count + 1)
        })
        .is_err()
    {
        let _ = client.shutdown();
        return;
    }
    let thread_active = Arc::clone(&active);
    let spawned = thread::Builder::new()
        .name("oah-domain-proxy-client".to_owned())
        .spawn(move || {
            let _guard = ActiveConnectionGuard(thread_active);
            let _ = handle_client(client, &policy);
        });
    if spawned.is_err() {
        active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct ActiveConnectionGuard(Arc<AtomicUsize>);

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

enum ClientStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl ClientStream {
    fn try_clone(&self) -> io::Result<Self> {
        match self {
            Self::Tcp(stream) => stream.try_clone().map(Self::Tcp),
            #[cfg(unix)]
            Self::Unix(stream) => stream.try_clone().map(Self::Unix),
        }
    }

    fn configure(&self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => {
                stream.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
                stream.set_write_timeout(Some(PROXY_IO_TIMEOUT))?;
                stream.set_nodelay(true)
            }
            #[cfg(unix)]
            Self::Unix(stream) => {
                stream.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
                stream.set_write_timeout(Some(PROXY_IO_TIMEOUT))
            }
        }
    }

    fn shutdown(&self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.shutdown(Shutdown::Both),
            #[cfg(unix)]
            Self::Unix(stream) => stream.shutdown(Shutdown::Both),
        }
    }
}

impl Read for ClientStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buffer),
            #[cfg(unix)]
            Self::Unix(stream) => stream.read(buffer),
        }
    }
}

impl Write for ClientStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buffer),
            #[cfg(unix)]
            Self::Unix(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            #[cfg(unix)]
            Self::Unix(stream) => stream.flush(),
        }
    }
}

fn handle_client(mut client: ClientStream, policy: &ProxyPolicy) -> Result<()> {
    client
        .configure()
        .context("无法配置 sandbox proxy client")?;
    let mut first = [0u8; 1];
    client
        .read_exact(&mut first)
        .context("sandbox proxy client 在协议握手前关闭")?;
    if first[0] == 0x05 {
        handle_socks5(client, policy)
    } else {
        handle_http(client, first[0], policy)
    }
}

fn handle_socks5(mut client: ClientStream, policy: &ProxyPolicy) -> Result<()> {
    let method_count = read_u8(&mut client)? as usize;
    if method_count == 0 || method_count > 16 {
        bail!("SOCKS5 authentication method count 无效")
    }
    let mut methods = vec![0u8; method_count];
    client.read_exact(&mut methods)?;
    if !methods.contains(&0x02) {
        client.write_all(&[0x05, 0xff])?;
        bail!("SOCKS5 client 未提供 username/password authentication")
    }
    client.write_all(&[0x05, 0x02])?;

    if read_u8(&mut client)? != 0x01 {
        bail!("SOCKS5 username/password version 无效")
    }
    let user_length = read_u8(&mut client)? as usize;
    if user_length == 0 || user_length > 64 {
        bail!("SOCKS5 username 长度无效")
    }
    let mut user = vec![0u8; user_length];
    client.read_exact(&mut user)?;
    let password_length = read_u8(&mut client)? as usize;
    if password_length == 0 || password_length > 128 {
        bail!("SOCKS5 password 长度无效")
    }
    let mut password = vec![0u8; password_length];
    client.read_exact(&mut password)?;
    if !constant_time_eq(&user, policy.expected_socks_user.as_slice())
        || !constant_time_eq(&password, policy.expected_socks_password.as_slice())
    {
        client.write_all(&[0x01, 0x01])?;
        bail!("SOCKS5 proxy authentication failed")
    }
    client.write_all(&[0x01, 0x00])?;

    let mut request = [0u8; 4];
    client.read_exact(&mut request)?;
    if request[0] != 0x05 || request[1] != 0x01 || request[2] != 0x00 {
        write_socks_failure(&mut client, 0x07)?;
        bail!("SOCKS5 只支持 CONNECT")
    }
    let host = match request[3] {
        0x01 => {
            let mut address = [0u8; 4];
            client.read_exact(&mut address)?;
            Ipv4Addr::from(address).to_string()
        }
        0x03 => {
            let length = read_u8(&mut client)? as usize;
            if length == 0 || length > 253 {
                write_socks_failure(&mut client, 0x08)?;
                bail!("SOCKS5 domain 长度无效")
            }
            let mut domain = vec![0u8; length];
            client.read_exact(&mut domain)?;
            String::from_utf8(domain).context("SOCKS5 domain 不是 UTF-8")?
        }
        0x04 => {
            let mut address = [0u8; 16];
            client.read_exact(&mut address)?;
            Ipv6Addr::from(address).to_string()
        }
        _ => {
            write_socks_failure(&mut client, 0x08)?;
            bail!("SOCKS5 address type 无效")
        }
    };
    let mut port = [0u8; 2];
    client.read_exact(&mut port)?;
    let port = u16::from_be_bytes(port);
    if port == 0 {
        write_socks_failure(&mut client, 0x01)?;
        bail!("SOCKS5 target port 无效")
    }
    let remote = match connect_authorized(policy, &host, port) {
        Ok(remote) => remote,
        Err(error) => {
            write_socks_failure(&mut client, 0x02)?;
            return Err(error);
        }
    };
    client.write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])?;
    relay_limited(client, remote)
}

fn write_socks_failure(client: &mut ClientStream, code: u8) -> io::Result<()> {
    client.write_all(&[0x05, code, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
}

fn handle_http(mut client: ClientStream, first: u8, policy: &ProxyPolicy) -> Result<()> {
    let mut buffer = Vec::with_capacity(1024);
    buffer.push(first);
    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        if buffer.len() >= MAX_PROXY_HEADER_BYTES {
            write_http_error(&mut client, 431, "Request Header Fields Too Large")?;
            bail!("HTTP proxy header 超过资源限制")
        }
        let mut chunk = [0u8; 2048];
        let count = client.read(&mut chunk)?;
        if count == 0 {
            bail!("HTTP proxy client 在 header 完成前关闭")
        }
        buffer.extend_from_slice(&chunk[..count]);
    }
    let header_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("HTTP proxy header boundary missing")?
        + 4;
    let header =
        std::str::from_utf8(&buffer[..header_end]).context("HTTP proxy header 不是 UTF-8")?;
    let mut lines = header[..header.len() - 4].split("\r\n");
    let request_line = lines.next().context("HTTP proxy request line 缺失")?;
    let mut request = request_line.split_whitespace();
    let method = request.next().context("HTTP proxy method 缺失")?;
    let target = request.next().context("HTTP proxy target 缺失")?;
    let version = request.next().context("HTTP proxy version 缺失")?;
    if request.next().is_some()
        || !version.starts_with("HTTP/1.")
        || !valid_http_token(method.as_bytes())
    {
        write_http_error(&mut client, 400, "Bad Request")?;
        bail!("HTTP proxy request line 无效")
    }

    let headers = lines
        .map(parse_header)
        .collect::<Result<Vec<(String, String)>>>()?;
    let authorized = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("proxy-authorization"))
        .is_some_and(|(_, value)| {
            constant_time_eq(
                value.as_bytes(),
                policy.expected_http_authorization.as_bytes(),
            )
        });
    if !authorized {
        client.write_all(
            b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"open-agent-harness\"\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        )?;
        bail!("HTTP proxy authentication failed")
    }

    if method.eq_ignore_ascii_case("CONNECT") {
        let (host, port) = parse_authority(target, 443)?;
        let remote = match connect_authorized(policy, &host, port) {
            Ok(remote) => remote,
            Err(error) => {
                write_http_error(&mut client, 403, "Forbidden")?;
                return Err(error);
            }
        };
        client.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")?;
        relay_limited(client, remote)
    } else {
        let (url, origin_target) = parse_http_target(target, &headers)?;
        if url.scheme() != "http" || !url.username().is_empty() || url.password().is_some() {
            write_http_error(&mut client, 400, "Bad Request")?;
            bail!("HTTP proxy 只接受无凭据的 http URL；https 必须使用 CONNECT")
        }
        let host = url.host_str().context("HTTP proxy URL 缺少 host")?;
        let port = url
            .port_or_known_default()
            .context("HTTP proxy URL 缺少 port")?;
        let mut remote = match connect_authorized(policy, host, port) {
            Ok(remote) => remote,
            Err(error) => {
                write_http_error(&mut client, 403, "Forbidden")?;
                return Err(error);
            }
        };
        let host_header = if url.port().is_some() {
            format_authority(host, port)
        } else {
            host.to_owned()
        };
        let mut rewritten = format!("{method} {origin_target} {version}\r\n").into_bytes();
        for (name, value) in &headers {
            if name.eq_ignore_ascii_case("proxy-authorization")
                || name.eq_ignore_ascii_case("proxy-connection")
                || name.eq_ignore_ascii_case("host")
            {
                continue;
            }
            rewritten.extend_from_slice(name.as_bytes());
            rewritten.extend_from_slice(b": ");
            rewritten.extend_from_slice(value.as_bytes());
            rewritten.extend_from_slice(b"\r\n");
        }
        rewritten.extend_from_slice(format!("Host: {host_header}\r\n\r\n").as_bytes());
        rewritten.extend_from_slice(&buffer[header_end..]);
        remote.write_all(&rewritten)?;
        relay_limited(client, remote)
    }
}

fn parse_header(line: &str) -> Result<(String, String)> {
    let (name, value) = line.split_once(':').context("HTTP proxy header 缺少冒号")?;
    if name.is_empty()
        || name.len() > 128
        || !valid_http_token(name.as_bytes())
        || value.len() > 16 * 1024
        || value.bytes().any(|byte| byte < 0x20 && byte != b'\t')
    {
        bail!("HTTP proxy header 无效")
    }
    Ok((name.to_owned(), value.trim().to_owned()))
}

fn valid_http_token(value: &[u8]) -> bool {
    !value.is_empty()
        && value.iter().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    *byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn parse_http_target(target: &str, headers: &[(String, String)]) -> Result<(Url, String)> {
    if let Ok(url) = Url::parse(target) {
        let mut origin = url.path().to_owned();
        if origin.is_empty() {
            origin.push('/');
        }
        if let Some(query) = url.query() {
            origin.push('?');
            origin.push_str(query);
        }
        return Ok((url, origin));
    }
    if !target.starts_with('/') {
        bail!("HTTP proxy target 不是 absolute URI 或 origin-form")
    }
    let host = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("host"))
        .map(|(_, value)| value.as_str())
        .context("HTTP proxy origin-form request 缺少 Host")?;
    let url =
        Url::parse(&format!("http://{host}{target}")).context("HTTP proxy Host/target 无效")?;
    Ok((url, target.to_owned()))
}

fn write_http_error(client: &mut ClientStream, code: u16, reason: &str) -> io::Result<()> {
    write!(
        client,
        "HTTP/1.1 {code} {reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    )
}

fn parse_authority(value: &str, default_port: u16) -> Result<(String, u16)> {
    if value.starts_with('[') {
        let end = value.find(']').context("IPv6 authority 缺少 ]")?;
        let host = &value[1..end];
        let port = value
            .get(end + 1..)
            .and_then(|suffix| suffix.strip_prefix(':'))
            .map(u16::from_str)
            .transpose()
            .context("authority port 无效")?
            .unwrap_or(default_port);
        if port == 0 {
            bail!("authority port 不能为 0")
        }
        return Ok((host.to_owned(), port));
    }
    if let Some((host, port)) = value.rsplit_once(':') {
        if !host.contains(':') {
            let port = port.parse::<u16>().context("authority port 无效")?;
            if host.is_empty() || port == 0 {
                bail!("authority host/port 无效")
            }
            return Ok((host.to_owned(), port));
        }
    }
    if value.is_empty() || default_port == 0 {
        bail!("authority host/port 无效")
    }
    Ok((value.to_owned(), default_port))
}

fn format_authority(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn connect_authorized(policy: &ProxyPolicy, host: &str, port: u16) -> Result<TcpStream> {
    let normalized = normalize_host(host)?;
    if !policy
        .allowed
        .iter()
        .any(|pattern| pattern.matches(&normalized))
    {
        bail!("sandbox proxy 拒绝未授权域名")
    }
    let addresses = (normalized.as_str(), port)
        .to_socket_addrs()
        .context("sandbox proxy DNS 解析失败")?
        .take(MAX_RESOLVED_ADDRESSES)
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        bail!("sandbox proxy DNS 没有返回地址")
    }
    let addresses = addresses
        .into_iter()
        .filter(|address| policy.allow_private_network || is_public_ip(address.ip()))
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        bail!("sandbox proxy 拒绝 private/reserved destination")
    }
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, PROXY_CONNECT_TIMEOUT) {
            Ok(stream) => {
                stream.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
                stream.set_write_timeout(Some(PROXY_IO_TIMEOUT))?;
                stream.set_nodelay(true)?;
                return Ok(stream);
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .context("sandbox proxy 无法连接已授权 destination")?
        .into())
}

impl DomainPattern {
    fn matches(&self, host: &str) -> bool {
        match self {
            Self::Exact(expected) => host == expected,
            Self::Subdomains(suffix) => {
                host.len() > suffix.len()
                    && host.ends_with(suffix)
                    && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
            }
        }
    }
}

fn parse_domain_pattern(value: &str) -> Result<DomainPattern> {
    if let Some(suffix) = value.strip_prefix("*.") {
        if suffix.contains('*') {
            bail!("sandbox allowedDomains wildcard 只能位于最左侧")
        }
        let suffix = normalize_host(suffix)?;
        if !suffix.contains('.') {
            bail!("sandbox allowedDomains wildcard 不能覆盖顶级域")
        }
        Ok(DomainPattern::Subdomains(suffix))
    } else {
        if value.contains('*') {
            bail!("sandbox allowedDomains wildcard 只能使用 *.example.com")
        }
        normalize_host(value).map(DomainPattern::Exact)
    }
}

fn normalize_host(value: &str) -> Result<String> {
    let value = value.strip_suffix('.').unwrap_or(value);
    if value.is_empty()
        || value.len() > 253
        || value.contains(['/', '\\', '@', '#', '?'])
        || value.chars().any(char::is_whitespace)
    {
        bail!("sandbox domain 无效")
    }
    match Host::parse(value).context("sandbox domain 无效")? {
        Host::Domain(domain) => {
            let domain = domain.to_ascii_lowercase();
            if domain
                .split('.')
                .any(|label| label.is_empty() || label.len() > 63)
            {
                bail!("sandbox domain label 无效")
            }
            Ok(domain)
        }
        Host::Ipv4(address) => Ok(address.to_string()),
        Host::Ipv6(address) => Ok(address.to_string()),
    }
}

fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4_mapped() {
                return is_public_ipv4(mapped);
            }
            !(address.is_unspecified()
                || address.is_loopback()
                || address.is_multicast()
                || (address.segments()[0] & 0xfe00) == 0xfc00
                || (address.segments()[0] & 0xffc0) == 0xfe80
                || (address.segments()[0] == 0x2001 && address.segments()[1] == 0x0db8))
        }
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !(address.is_unspecified()
        || address.is_loopback()
        || address.is_private()
        || address.is_link_local()
        || address.is_multicast()
        || address.is_broadcast()
        || a == 0
        || a >= 240
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113))
}

fn relay_limited(client: ClientStream, remote: TcpStream) -> Result<()> {
    let mut client_reader = client.try_clone()?;
    let mut remote_writer = remote.try_clone()?;
    let outbound = thread::Builder::new()
        .name("oah-domain-proxy-upload".to_owned())
        .spawn(move || copy_limited(&mut client_reader, &mut remote_writer))
        .context("无法创建 sandbox proxy relay thread")?;
    let mut remote_reader = remote;
    let mut client_writer = client;
    let inbound = copy_limited(&mut remote_reader, &mut client_writer);
    let _ = remote_reader.shutdown(Shutdown::Both);
    let _ = client_writer.shutdown();
    let outbound = outbound
        .join()
        .map_err(|_| anyhow::anyhow!("sandbox proxy relay thread panic"))?;
    inbound?;
    outbound?;
    Ok(())
}

fn copy_limited(reader: &mut impl Read, writer: &mut impl Write) -> io::Result<()> {
    let mut total = 0u64;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            writer.flush()?;
            return Ok(());
        }
        total = total.saturating_add(count as u64);
        if total > MAX_PROXY_TRANSFER_BYTES {
            return Err(io::Error::other("sandbox proxy transfer limit exceeded"));
        }
        writer.write_all(&buffer[..count])?;
    }
}

fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut value = [0u8; 1];
    reader.read_exact(&mut value)?;
    Ok(value[0])
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left ^ right);
    }
    difference == 0
}

pub fn maybe_run_proxy_bridge() -> Option<Result<i32>> {
    let arguments = std::env::args_os().collect::<Vec<_>>();
    (arguments.get(1).and_then(|value| value.to_str()) == Some(INTERNAL_BRIDGE_MODE))
        .then(|| run_proxy_bridge(&arguments[2..]))
}

fn run_proxy_bridge(arguments: &[OsString]) -> Result<i32> {
    #[cfg(not(unix))]
    {
        let _ = arguments;
        bail!("sandbox proxy bridge 只支持 Unix")
    }
    #[cfg(unix)]
    {
        if arguments.len() < 6 || arguments.get(3).and_then(|value| value.to_str()) != Some("--") {
            bail!("sandbox proxy bridge 参数无效")
        }
        let socket = PathBuf::from(&arguments[0]);
        if !socket.is_absolute() {
            bail!("sandbox proxy bridge socket 必须是绝对路径")
        }
        let metadata =
            std::fs::symlink_metadata(&socket).context("sandbox proxy bridge socket 不存在")?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_socket() {
            bail!("sandbox proxy bridge path 不是可信 Unix socket")
        }
        let port = arguments[1]
            .to_str()
            .context("sandbox proxy bridge port 不是 UTF-8")?
            .parse::<u16>()
            .context("sandbox proxy bridge port 无效")?;
        if port == 0 {
            bail!("sandbox proxy bridge port 不能为 0")
        }
        let token = arguments[2]
            .to_str()
            .context("sandbox proxy bridge token 不是 UTF-8")?;
        if token.len() != 32 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("sandbox proxy bridge token 无效")
        }
        let shell = &arguments[4];
        let shell_args = &arguments[5..];
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
            .context("无法启动 sandbox network-namespace proxy bridge")?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let accept_stop = Arc::clone(&stop);
        let accept_active = Arc::clone(&active);
        let accept_socket = socket.clone();
        let accept_thread = thread::Builder::new()
            .name("oah-sandbox-proxy-bridge".to_owned())
            .spawn(move || {
                while !accept_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((client, address)) => {
                            if !address.ip().is_loopback()
                                || accept_active
                                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                                        (count < MAX_PROXY_CONNECTIONS).then_some(count + 1)
                                    })
                                    .is_err()
                            {
                                let _ = client.shutdown(Shutdown::Both);
                                continue;
                            }
                            let socket = accept_socket.clone();
                            let active = Arc::clone(&accept_active);
                            let _ = thread::Builder::new()
                                .name("oah-sandbox-proxy-bridge-client".to_owned())
                                .spawn(move || {
                                    let _guard = ActiveConnectionGuard(active);
                                    let Ok(broker) = UnixStream::connect(socket) else {
                                        let _ = client.shutdown(Shutdown::Both);
                                        return;
                                    };
                                    let _ = relay_local_streams(client, broker);
                                });
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(25));
                        }
                        Err(_) => break,
                    }
                }
            })
            .context("无法创建 sandbox proxy bridge thread")?;

        let http_proxy = format!("http://oah:{token}@127.0.0.1:{port}");
        let socks_proxy = format!("socks5h://oah:{token}@127.0.0.1:{port}");
        let mut child = Command::new(shell);
        child.args(shell_args);
        clear_proxy_environment(&mut child);
        child
            .env("HTTP_PROXY", &http_proxy)
            .env("HTTPS_PROXY", &http_proxy)
            .env("ALL_PROXY", &socks_proxy)
            .env("http_proxy", &http_proxy)
            .env("https_proxy", &http_proxy)
            .env("all_proxy", &socks_proxy)
            .env("NO_PROXY", "")
            .env("no_proxy", "");
        let status = child
            .status()
            .context("sandbox proxy bridge 无法启动 shell")?;
        stop.store(true, Ordering::Release);
        let _ = TcpStream::connect((Ipv4Addr::LOCALHOST, port));
        let _ = accept_thread.join();
        Ok(exit_status_code(status))
    }
}

#[cfg(unix)]
fn relay_local_streams(client: TcpStream, broker: UnixStream) -> Result<()> {
    client.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
    client.set_write_timeout(Some(PROXY_IO_TIMEOUT))?;
    broker.set_read_timeout(Some(PROXY_IO_TIMEOUT))?;
    broker.set_write_timeout(Some(PROXY_IO_TIMEOUT))?;
    let mut client_reader = client.try_clone()?;
    let mut broker_writer = broker.try_clone()?;
    let upload = thread::spawn(move || copy_limited(&mut client_reader, &mut broker_writer));
    let mut broker_reader = broker;
    let mut client_writer = client;
    let download = copy_limited(&mut broker_reader, &mut client_writer);
    let _ = broker_reader.shutdown(Shutdown::Both);
    let _ = client_writer.shutdown(Shutdown::Both);
    upload
        .join()
        .map_err(|_| anyhow::anyhow!("sandbox proxy local relay thread panic"))??;
    download?;
    Ok(())
}

#[cfg(unix)]
fn clear_proxy_environment(command: &mut Command) {
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
}

#[cfg(unix)]
fn exit_status_code(status: ExitStatus) -> i32 {
    status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_patterns_are_normalized_and_bounded() {
        assert_eq!(
            parse_domain_pattern("Example.COM.").unwrap(),
            DomainPattern::Exact("example.com".to_owned())
        );
        let wildcard = parse_domain_pattern("*.example.com").unwrap();
        assert!(wildcard.matches("api.example.com"));
        assert!(!wildcard.matches("example.com"));
        assert!(!wildcard.matches("badexample.com"));
        for invalid in [
            "*",
            "*.*.example.com",
            "https://example.invalid",
            "exa mple.com",
        ] {
            assert!(parse_domain_pattern(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn private_and_reserved_addresses_fail_closed() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.1.1",
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            assert!(!is_public_ip(address.parse().unwrap()), "{address}");
        }
        assert!(is_public_ip("1.1.1.1".parse().unwrap()));
        assert!(is_public_ip("2606:4700:4700::1111".parse().unwrap()));
    }

    #[test]
    fn proxy_requires_auth_and_enforces_domain_before_connect() {
        let destination = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let destination_port = destination.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = destination.accept().unwrap();
            let mut request = [0u8; 1024];
            let count = stream.read(&mut request).unwrap();
            assert!(String::from_utf8_lossy(&request[..count]).starts_with("GET /ok HTTP/1.1"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .unwrap();
        });
        let proxy = DomainProxy::start(&["127.0.0.1".to_owned()], true).unwrap();

        let mut unauthenticated = TcpStream::connect(proxy.inner.tcp_addr).unwrap();
        write!(
            unauthenticated,
            "GET http://127.0.0.1:{destination_port}/ok HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
        )
        .unwrap();
        let mut response = [0u8; 256];
        let count = unauthenticated.read(&mut response).unwrap();
        assert!(String::from_utf8_lossy(&response[..count]).contains("407"));

        let authorization = BASE64_STANDARD.encode(format!("oah:{}", proxy.token()));
        let mut client = TcpStream::connect(proxy.inner.tcp_addr).unwrap();
        write!(
            client,
            "GET http://127.0.0.1:{destination_port}/ok HTTP/1.1\r\nHost: ignored\r\nProxy-Authorization: Basic {authorization}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut output = Vec::new();
        client.read_to_end(&mut output).unwrap();
        assert!(String::from_utf8_lossy(&output).contains("200 OK"));
        assert!(String::from_utf8_lossy(&output).ends_with("ok"));
        server.join().unwrap();
    }

    #[test]
    fn proxy_rejects_private_addresses_without_explicit_trust() {
        let proxy = DomainProxy::start(&["127.0.0.1".to_owned()], false).unwrap();
        let policy = ProxyPolicy {
            allowed: Arc::new(vec![DomainPattern::Exact("127.0.0.1".to_owned())]),
            allow_private_network: false,
            expected_http_authorization: Arc::new(String::new()),
            expected_socks_user: Arc::new(Vec::new()),
            expected_socks_password: Arc::new(Vec::new()),
        };
        assert!(connect_authorized(&policy, "127.0.0.1", 80).is_err());
        drop(proxy);
    }
}

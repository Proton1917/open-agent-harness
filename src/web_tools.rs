use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::{
    Client,
    header::{HeaderMap, HeaderName, HeaderValue, LOCATION},
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::timeout;
use url::Url;

use crate::{
    config::Settings,
    tools::{Tool, ToolContext, ToolOutput, object_schema},
};

const DEFAULT_MAX_BYTES: usize = 2 * 1024 * 1024;
const MIN_MAX_BYTES: usize = 1024;
const MAX_MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
const MAX_REDIRECTS: usize = 5;
const MAX_HEADERS: usize = 64;
const MAX_HEADER_VALUE_BYTES: usize = 16 * 1024;
const MAX_URL_BYTES: usize = 16 * 1024;
const MAX_QUERY_BYTES: usize = 16 * 1024;
const DNS_TIMEOUT: Duration = Duration::from_secs(15);
const FETCH_TIMEOUT: Duration = Duration::from_secs(120);

pub struct WebIntegration {
    pub deferred_tools: Vec<Arc<dyn Tool>>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawWebConfig {
    #[serde(default)]
    allow_private_network: bool,
    max_bytes: Option<usize>,
    search: Option<RawSearchConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawSearchConfig {
    endpoint: String,
    query_parameter: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

struct SearchConfig {
    endpoint: Url,
    query_parameter: String,
    headers: HeaderMap,
    secrets: Vec<String>,
}

struct WebRuntime {
    allow_private_network: bool,
    max_bytes: usize,
    search: Option<SearchConfig>,
}

pub fn configure_web(settings: &Settings) -> Result<WebIntegration> {
    let raw: RawWebConfig = settings
        .raw
        .get("web")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("web settings 无效")?
        .unwrap_or_default();
    let search = raw.search.map(parse_search_config).transpose()?;
    let runtime = Arc::new(WebRuntime {
        allow_private_network: raw.allow_private_network,
        max_bytes: raw
            .max_bytes
            .unwrap_or(DEFAULT_MAX_BYTES)
            .clamp(MIN_MAX_BYTES, MAX_MAX_BYTES),
        search,
    });
    let mut tools: Vec<Arc<dyn Tool>> = vec![Arc::new(WebFetchTool {
        runtime: Arc::clone(&runtime),
    })];
    if runtime.search.is_some() {
        tools.push(Arc::new(WebSearchTool { runtime }));
    }
    Ok(WebIntegration {
        deferred_tools: tools,
    })
}

fn parse_search_config(raw: RawSearchConfig) -> Result<SearchConfig> {
    let endpoint = parse_url(&raw.endpoint)?;
    let query_parameter = raw.query_parameter.unwrap_or_else(|| "q".to_owned());
    if query_parameter.is_empty()
        || query_parameter.len() > 128
        || !query_parameter
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        bail!("web.search.queryParameter 无效")
    }
    if raw.headers.len() > MAX_HEADERS {
        bail!("web.search.headers 超过 {MAX_HEADERS} 项限制")
    }
    let mut headers = HeaderMap::new();
    let mut secrets = Vec::new();
    for (name, value) in raw.headers {
        if value.len() > MAX_HEADER_VALUE_BYTES {
            bail!("web.search header value 过长")
        }
        let name =
            HeaderName::from_bytes(name.as_bytes()).context("web.search header name 无效")?;
        if matches!(
            name.as_str(),
            "host" | "content-length" | "connection" | "transfer-encoding"
        ) {
            bail!("web.search 不允许覆盖 header {name}")
        }
        let value = HeaderValue::from_str(&value).context("web.search header value 无效")?;
        if !value.is_sensitive() && !value.as_bytes().is_empty() {
            secrets.push(String::from_utf8_lossy(value.as_bytes()).into_owned());
        }
        headers.insert(name, value);
    }
    Ok(SearchConfig {
        endpoint,
        query_parameter,
        headers,
        secrets,
    })
}

struct WebFetchTool {
    runtime: Arc<WebRuntime>,
}

struct WebSearchTool {
    runtime: Arc<WebRuntime>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchInput {
    url: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchInput {
    query: String,
}

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &str {
        "WebFetch"
    }

    fn description(&self) -> &str {
        "Fetches textual HTTP(S) content with DNS pinning, redirect revalidation, private-network denial, timeouts, and response-size limits."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({"url": {"type": "string", "minLength": 1, "maxLength": MAX_URL_BYTES}}),
            &["url"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or("<url>")
            .to_owned()
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: FetchInput = serde_json::from_value(input)?;
        let url = parse_url(&input.url)?;
        let response = self
            .runtime
            .fetch(url, HeaderMap::new(), Vec::new())
            .await?;
        Ok(ToolOutput::success(response))
    }
}

#[async_trait]
impl Tool for WebSearchTool {
    fn name(&self) -> &str {
        "WebSearch"
    }

    fn description(&self) -> &str {
        "Queries the user-configured provider-neutral HTTP search endpoint. No search service is built in or contacted unless explicitly configured."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({"query": {"type": "string", "minLength": 1, "maxLength": MAX_QUERY_BYTES}}),
            &["query"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or("<query>")
            .to_owned()
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: SearchInput = serde_json::from_value(input)?;
        let search = self
            .runtime
            .search
            .as_ref()
            .context("WebSearch endpoint 未配置")?;
        let mut url = search.endpoint.clone();
        url.query_pairs_mut()
            .append_pair(&search.query_parameter, &input.query);
        let response = self
            .runtime
            .fetch(url, search.headers.clone(), search.secrets.clone())
            .await?;
        Ok(ToolOutput::success(response))
    }
}

impl WebRuntime {
    async fn fetch(&self, url: Url, headers: HeaderMap, secrets: Vec<String>) -> Result<String> {
        timeout(FETCH_TIMEOUT, self.fetch_inner(url, headers, secrets))
            .await
            .map_err(|_| anyhow::anyhow!("HTTP fetch 超过 {}s timeout", FETCH_TIMEOUT.as_secs()))?
    }

    async fn fetch_inner(
        &self,
        mut url: Url,
        mut headers: HeaderMap,
        secrets: Vec<String>,
    ) -> Result<String> {
        let mut previous_origin = origin(&url);
        for redirect in 0..=MAX_REDIRECTS {
            let resolved = resolve_target(&url, self.allow_private_network).await?;
            let client = client_for_target(&url, resolved)?;
            let response = client
                .get(url.clone())
                .headers(headers.clone())
                .header("accept", "text/html, text/plain, application/json, application/xml, text/xml, text/markdown")
                .header(
                    "user-agent",
                    concat!("open-agent-harness/", env!("CARGO_PKG_VERSION")),
                )
                .send()
                .await
                .with_context(|| format!("HTTP request 失败: {}", display_url(&url)))?;
            if response.status().is_redirection() {
                if redirect == MAX_REDIRECTS {
                    bail!("HTTP redirect 超过 {MAX_REDIRECTS} 次限制")
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .context("HTTP redirect 缺少 Location")?
                    .to_str()
                    .context("HTTP redirect Location 不是有效文本")?;
                let next = url.join(location).context("HTTP redirect Location 无效")?;
                validate_url(&next)?;
                if url.scheme() == "https" && next.scheme() == "http" {
                    bail!("拒绝 HTTPS 到 HTTP 的降级 redirect")
                }
                let next_origin = origin(&next);
                if next_origin != previous_origin {
                    headers.clear();
                }
                previous_origin = next_origin;
                url = next;
                continue;
            }
            let status = response.status();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("application/octet-stream")
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            let limit = if status.is_success() {
                self.max_bytes
            } else {
                MAX_ERROR_BYTES
            };
            if response
                .content_length()
                .is_some_and(|length| length > limit as u64)
            {
                bail!("HTTP response 超过 {limit} 字节限制")
            }
            let body = read_limited(response, limit).await?;
            let text = String::from_utf8(body).context("HTTP response 不是有效 UTF-8 文本")?;
            let text = redact(&text, &secrets);
            if !status.is_success() {
                bail!(
                    "HTTP {}: {}",
                    status.as_u16(),
                    truncate_text(&text, MAX_ERROR_BYTES)
                )
            }
            if !textual_content_type(&content_type) {
                bail!("拒绝非文本 Content-Type: {content_type}")
            }
            return Ok(format!(
                "URL: {}\nStatus: {}\nContent-Type: {}\n\n{}",
                display_url(&url),
                status.as_u16(),
                content_type,
                text
            ));
        }
        bail!("HTTP fetch 未产生结果")
    }
}

fn parse_url(value: &str) -> Result<Url> {
    if value.len() > MAX_URL_BYTES {
        bail!("URL 超过 {MAX_URL_BYTES} 字节限制")
    }
    let url = Url::parse(value).context("URL 无效")?;
    validate_url(&url)?;
    Ok(url)
}

fn validate_url(url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https") {
        bail!("只允许 http 或 https URL")
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("URL 不得包含 username 或 password")
    }
    if url.host_str().is_none() {
        bail!("URL 缺少 host")
    }
    Ok(())
}

async fn resolve_target(url: &Url, allow_private: bool) -> Result<SocketAddr> {
    validate_url(url)?;
    let host = url.host_str().context("URL 缺少 host")?;
    let port = url.port_or_known_default().context("URL 缺少可识别端口")?;
    let addresses = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![SocketAddr::new(ip, port)]
    } else {
        timeout(DNS_TIMEOUT, tokio::net::lookup_host((host, port)))
            .await
            .map_err(|_| anyhow::anyhow!("DNS resolution 超过 {}s timeout", DNS_TIMEOUT.as_secs()))?
            .with_context(|| format!("DNS resolution 失败: {host}"))?
            .collect::<Vec<_>>()
    };
    if addresses.is_empty() {
        bail!("DNS resolution 没有返回地址: {host}")
    }
    if !allow_private && addresses.iter().any(|address| !is_public_ip(address.ip())) {
        bail!("目标 host 解析到本地、私有、保留或不可路由地址")
    }
    Ok(addresses[0])
}

fn client_for_target(url: &Url, address: SocketAddr) -> Result<Client> {
    let host = url.host_str().context("URL 缺少 host")?;
    Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .resolve(host, address)
        .build()
        .context("无法创建 Web HTTP client")
}

pub(crate) async fn secure_client_for_url(url: &Url, allow_private: bool) -> Result<Client> {
    let address = resolve_target(url, allow_private).await?;
    client_for_target(url, address)
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("读取 HTTP response 失败")?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            bail!("HTTP response 超过 {limit} 字节限制")
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn textual_content_type(content_type: &str) -> bool {
    content_type.starts_with("text/")
        || matches!(
            content_type,
            "application/json"
                | "application/ld+json"
                | "application/xml"
                | "application/xhtml+xml"
                | "application/javascript"
                | "application/x-ndjson"
        )
        || content_type.ends_with("+json")
        || content_type.ends_with("+xml")
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || ip.is_unspecified()
        || a == 0
        || a >= 240
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && matches!(b, 18 | 19)))
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    if let Some(embedded) = ip.to_ipv4() {
        return is_public_ipv4(embedded);
    }
    if segments[0] == 0x0064 && segments[1] == 0xff9b {
        if segments[2..6].iter().all(|segment| *segment == 0) {
            let embedded = Ipv4Addr::new(
                (segments[6] >> 8) as u8,
                segments[6] as u8,
                (segments[7] >> 8) as u8,
                segments[7] as u8,
            );
            return is_public_ipv4(embedded);
        }
        if segments[2] == 1 {
            return false;
        }
    }
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xffc0) == 0xfec0
        || (segments[0] == 0x0100 && segments[1..4].iter().all(|segment| *segment == 0))
        || (segments[0] == 0x2001 && segments[1] == 0x0002)
        || (segments[0] == 0x2001 && (segments[1] & 0xfff0) == 0x0010)
        || (segments[0] == 0x2001 && (segments[1] & 0xfff0) == 0x0020)
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x3fff && (segments[1] & 0xf000) == 0))
}

fn origin(url: &Url) -> (String, String, Option<u16>) {
    (
        url.scheme().to_owned(),
        url.host_str().unwrap_or_default().to_owned(),
        url.port_or_known_default(),
    )
}

fn display_url(url: &Url) -> String {
    let mut clean = url.clone();
    clean.set_fragment(None);
    clean.to_string()
}

fn redact(value: &str, secrets: &[String]) -> String {
    secrets
        .iter()
        .filter(|secret| !secret.is_empty())
        .fold(value.to_owned(), |text, secret| {
            text.replace(secret, "[REDACTED]")
        })
}

fn truncate_text(value: &str, maximum: usize) -> &str {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::{ToolContext, ToolRegistry},
    };

    #[tokio::test]
    async fn private_network_is_denied_by_default() {
        let url = Url::parse("http://127.0.0.1:8080/test").unwrap();
        let error = resolve_target(&url, false).await.unwrap_err();
        assert!(error.to_string().contains("私有"));
    }

    #[tokio::test]
    async fn explicitly_allowed_local_text_response_is_bounded() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = stream.read(&mut chunk).unwrap();
                assert!(count > 0);
                request.extend_from_slice(&chunk[..count]);
            }
            let body = "local response";
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let settings = Settings {
            raw: json!({"web": {"allowPrivateNetwork": true, "maxBytes": 4096}}),
        };
        let integration = configure_web(&settings).unwrap();
        let registry =
            ToolRegistry::with_extensions(integration.deferred_tools, Vec::new()).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        let output = registry
            .execute(
                &context,
                "WebFetch",
                json!({"url": format!("http://{address}/text")}),
            )
            .await;
        server.join().unwrap();
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("local response"));
    }

    #[test]
    fn search_headers_and_endpoint_are_only_loaded_from_settings() {
        let settings = Settings {
            raw: json!({"web": {"search": {
                "endpoint": "https://search.example.invalid/query",
                "queryParameter": "query",
                "headers": {"authorization": "test-token"}
            }}}),
        };
        let integration = configure_web(&settings).unwrap();
        assert_eq!(integration.deferred_tools.len(), 2);
    }

    #[test]
    fn public_ip_classification_rejects_reserved_ranges() {
        assert!(!is_public_ipv4(Ipv4Addr::new(10, 0, 0, 1)));
        assert!(!is_public_ipv4(Ipv4Addr::new(100, 64, 0, 1)));
        assert!(!is_public_ipv4(Ipv4Addr::new(192, 0, 2, 1)));
        assert!(!is_public_ipv4(Ipv4Addr::new(192, 88, 99, 1)));
        assert!(!is_public_ipv6(Ipv6Addr::LOCALHOST));
        for address in [
            "2001:db8::1",
            "3fff::1",
            "2001:20::1",
            "fec0::1",
            "64:ff9b::7f00:1",
        ] {
            assert!(!is_public_ipv6(address.parse().unwrap()), "{address}");
        }
        assert!(is_public_ipv4(Ipv4Addr::new(8, 8, 8, 8)));
        assert!(is_public_ipv6("2606:4700:4700::1111".parse().unwrap()));
    }
}

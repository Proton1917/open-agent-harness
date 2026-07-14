use std::{
    collections::{BTreeMap, BTreeSet},
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
    tools::{Tool, ToolContext, ToolOutput, object_schema, schema},
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
const MAX_PROMPT_BYTES: usize = 16 * 1024;
const MAX_DOMAIN_FILTERS: usize = 32;
const MAX_DOMAIN_BYTES: usize = 253;
const MAX_PROCESSED_BYTES: usize = 128 * 1024;
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
    for (key, _) in endpoint.query_pairs() {
        if is_sensitive_query_key(&key) {
            bail!("web.search.endpoint 不允许在 query 中携带凭据参数 {key:?}；请改用 headers")
        }
    }
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
    let mut secrets = url_query_secrets(&endpoint);
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
        let mut value = HeaderValue::from_str(&value).context("web.search header value 无效")?;
        if !value.is_sensitive() && !value.as_bytes().is_empty() {
            secrets.push(String::from_utf8_lossy(value.as_bytes()).into_owned());
        }
        value.set_sensitive(true);
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
    prompt: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SearchInput {
    query: String,
    #[serde(default, alias = "allowed_domains")]
    allowed_domains: Vec<String>,
    #[serde(default, alias = "blocked_domains")]
    blocked_domains: Vec<String>,
}

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &str {
        "WebFetch"
    }

    fn description(&self) -> &str {
        "Fetches textual HTTP(S) content and makes a bounded local prompt-guided extract, with DNS pinning, redirect revalidation, private-network denial, timeouts, and response-size limits."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "url": {"type": "string", "minLength": 1, "maxLength": MAX_URL_BYTES},
                "prompt": {"type": "string", "minLength": 1, "maxLength": MAX_PROMPT_BYTES}
            }),
            &["url", "prompt"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        let value = input.get("url").and_then(Value::as_str).unwrap_or("<url>");
        Url::parse(value)
            .ok()
            .and_then(|url| url.host_str().map(|host| format!("domain:{host}")))
            .unwrap_or_else(|| "domain:<invalid>".to_owned())
    }

    async fn execute(&self, _: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: FetchInput = serde_json::from_value(input)?;
        let url = parse_url(&input.url)?;
        let response = self
            .runtime
            .fetch(url, HeaderMap::new(), Vec::new())
            .await?;
        Ok(ToolOutput::success(apply_prompt_to_response(
            &response,
            &input.prompt,
        )?))
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
            json!({
                "query": {"type": "string", "minLength": 1, "maxLength": MAX_QUERY_BYTES},
                "allowedDomains": {
                    "type":"array", "maxItems":MAX_DOMAIN_FILTERS,
                    "items":{"type":"string", "minLength":1, "maxLength":MAX_DOMAIN_BYTES}
                },
                "blockedDomains": {
                    "type":"array", "maxItems":MAX_DOMAIN_FILTERS,
                    "items":{"type":"string", "minLength":1, "maxLength":MAX_DOMAIN_BYTES}
                },
                "allowed_domains": {
                    "type":"array", "maxItems":MAX_DOMAIN_FILTERS,
                    "items":{"type":"string", "minLength":1, "maxLength":MAX_DOMAIN_BYTES}
                },
                "blocked_domains": {
                    "type":"array", "maxItems":MAX_DOMAIN_FILTERS,
                    "items":{"type":"string", "minLength":1, "maxLength":MAX_DOMAIN_BYTES}
                }
            }),
            &["query"],
        )
    }

    fn validate_input(&self, input: &Value) -> std::result::Result<(), String> {
        schema::validate(&self.input_schema(), input)?;
        let input: SearchInput =
            serde_json::from_value(input.clone()).map_err(|error| error.to_string())?;
        normalize_domain_filters(&input.allowed_domains, &input.blocked_domains)
            .map(|_| ())
            .map_err(|error| format!("{error:#}"))
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
        let filters = normalize_domain_filters(&input.allowed_domains, &input.blocked_domains)?;
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
        let response = if filters.is_empty() {
            response
        } else {
            apply_search_domain_filters(&response, &filters)?
        };
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
                // reqwest errors may include their source URL. Discard that source here so a
                // configured query credential or user search query cannot escape redaction.
                .map_err(|_| anyhow::anyhow!("HTTP request 失败: {}", display_url(&url)))?;
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
            let mut response_secrets = secrets.clone();
            response_secrets.extend(url.query_pairs().filter_map(|(key, value)| {
                (is_sensitive_query_key(&key) && !value.is_empty()).then(|| value.into_owned())
            }));
            let text = redact(&text, &response_secrets);
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DomainFilters {
    allowed: BTreeSet<String>,
    blocked: BTreeSet<String>,
}

impl DomainFilters {
    fn is_empty(&self) -> bool {
        self.allowed.is_empty() && self.blocked.is_empty()
    }

    fn permits(&self, url: &str) -> bool {
        let Ok(url) = Url::parse(url) else {
            return false;
        };
        if !matches!(url.scheme(), "http" | "https") {
            return false;
        }
        let Some(host) = url.host_str() else {
            return false;
        };
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        let matches = |domain: &str| host == domain || host.ends_with(&format!(".{domain}"));
        (self.allowed.is_empty() || self.allowed.iter().any(|domain| matches(domain)))
            && !self.blocked.iter().any(|domain| matches(domain))
    }
}

fn normalize_domain_filters(allowed: &[String], blocked: &[String]) -> Result<DomainFilters> {
    if !allowed.is_empty() && !blocked.is_empty() {
        bail!("allowedDomains 与 blockedDomains 不能同时设置")
    }
    if allowed.len() > MAX_DOMAIN_FILTERS || blocked.len() > MAX_DOMAIN_FILTERS {
        bail!("search domain filters 超过 {MAX_DOMAIN_FILTERS} 项限制")
    }
    Ok(DomainFilters {
        allowed: allowed
            .iter()
            .map(|domain| normalize_domain(domain))
            .collect::<Result<_>>()?,
        blocked: blocked
            .iter()
            .map(|domain| normalize_domain(domain))
            .collect::<Result<_>>()?,
    })
}

fn normalize_domain(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > MAX_DOMAIN_BYTES {
        bail!("search domain 为空或超过 {MAX_DOMAIN_BYTES} 字节")
    }
    let value = value
        .strip_prefix("*.")
        .unwrap_or(value)
        .trim_start_matches('.')
        .trim_end_matches('.');
    if value.is_empty() || value.contains('*') {
        bail!("search domain wildcard 只允许最前面的 *. 前缀")
    }
    let url = Url::parse(&format!("{}://{value}/", "https")).context("search domain 无效")?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        bail!("search domain 只能包含 hostname")
    }
    let host = url.host_str().context("search domain 缺少 hostname")?;
    if host.len() > MAX_DOMAIN_BYTES {
        bail!("search domain 规范化后过长")
    }
    Ok(host.trim_end_matches('.').to_ascii_lowercase())
}

fn apply_search_domain_filters(response: &str, filters: &DomainFilters) -> Result<String> {
    let (metadata, body) = split_fetch_response(response)?;
    let value: Value = serde_json::from_str(body).context(
        "配置 domain filters 时，provider-neutral search endpoint 必须返回含 url/link 字段的 JSON",
    )?;
    let mut saw_url = false;
    let filtered = filter_search_json(value, filters, &mut saw_url)
        .unwrap_or_else(|| Value::Array(Vec::new()));
    if !saw_url {
        bail!("search endpoint JSON 不含可验证的 url/link 结果字段")
    }
    Ok(format!(
        "{metadata}\n\n{}",
        serde_json::to_string_pretty(&filtered)?
    ))
}

fn filter_search_json(value: Value, filters: &DomainFilters, saw_url: &mut bool) -> Option<Value> {
    match value {
        Value::Array(values) => Some(Value::Array(
            values
                .into_iter()
                .filter_map(|value| filter_search_json(value, filters, saw_url))
                .collect(),
        )),
        Value::Object(mut object) => {
            let direct_urls = object
                .iter()
                .filter(|(key, value)| {
                    matches!(key.to_ascii_lowercase().as_str(), "url" | "link") && value.is_string()
                })
                .filter_map(|(_, value)| value.as_str())
                .collect::<Vec<_>>();
            if !direct_urls.is_empty() {
                *saw_url = true;
                if direct_urls.iter().any(|url| !filters.permits(url)) {
                    return None;
                }
            }
            for value in object.values_mut() {
                let current = std::mem::take(value);
                *value = filter_search_json(current, filters, saw_url)?;
            }
            Some(Value::Object(object))
        }
        other => Some(other),
    }
}

fn apply_prompt_to_response(response: &str, prompt: &str) -> Result<String> {
    if prompt.trim().is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        bail!("WebFetch prompt 为空或超过 {MAX_PROMPT_BYTES} 字节限制")
    }
    let (metadata, body) = split_fetch_response(response)?;
    let content_type = metadata
        .lines()
        .find_map(|line| line.strip_prefix("Content-Type: "))
        .unwrap_or("text/plain");
    let text = if matches!(content_type, "text/html" | "application/xhtml+xml") {
        html_to_text(body)
    } else if content_type.ends_with("json") || content_type.ends_with("+json") {
        serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|value| serde_json::to_string_pretty(&value).ok())
            .unwrap_or_else(|| body.to_owned())
    } else {
        body.to_owned()
    };
    let extract = prompt_guided_extract(&text, prompt);
    Ok(format!(
        "{metadata}\n\nPrompt: {}\n\nPrompt-guided extract:\n{}",
        prompt.trim(),
        extract
    ))
}

fn split_fetch_response(response: &str) -> Result<(&str, &str)> {
    response
        .split_once("\n\n")
        .context("内部 Web response envelope 损坏")
}

fn prompt_guided_extract(content: &str, prompt: &str) -> String {
    let terms = prompt
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| term.chars().count() >= 3)
        .map(str::to_lowercase)
        .filter(|term| {
            !matches!(
                term.as_str(),
                "the"
                    | "and"
                    | "for"
                    | "from"
                    | "this"
                    | "that"
                    | "with"
                    | "return"
                    | "show"
                    | "page"
                    | "content"
                    | "please"
                    | "summarize"
            )
        })
        .take(64)
        .collect::<BTreeSet<_>>();
    let mut candidates = content
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let lowercase = line.to_lowercase();
            let score = terms
                .iter()
                .filter(|term| lowercase.contains(term.as_str()))
                .count();
            Some((index, score, line))
        })
        .collect::<Vec<_>>();
    let has_match = candidates.iter().any(|(_, score, _)| *score > 0);
    if has_match {
        candidates.retain(|(_, score, _)| *score > 0);
        candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        candidates.truncate(128);
        candidates.sort_by_key(|(index, _, _)| *index);
    }
    let mut output = String::new();
    for (_, _, line) in candidates {
        if output.len().saturating_add(line.len()).saturating_add(1) > MAX_PROCESSED_BYTES {
            break;
        }
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(line);
    }
    if output.is_empty() {
        truncate_text(content.trim(), MAX_PROCESSED_BYTES).to_owned()
    } else {
        output
    }
}

fn html_to_text(html: &str) -> String {
    let mut output = String::with_capacity(html.len().min(MAX_PROCESSED_BYTES * 2));
    let mut tag = String::new();
    let mut in_tag = false;
    let mut suppressed: Option<&'static str> = None;
    for character in html.chars() {
        if in_tag {
            if character == '>' {
                let normalized = tag.trim().to_ascii_lowercase();
                let closing = normalized.starts_with('/');
                let name = normalized
                    .trim_start_matches('/')
                    .split(|character: char| character.is_ascii_whitespace() || character == '/')
                    .next()
                    .unwrap_or("");
                if closing && suppressed == Some(name) {
                    suppressed = None;
                } else if !closing && matches!(name, "script" | "style" | "noscript") {
                    suppressed = Some(match name {
                        "script" => "script",
                        "style" => "style",
                        _ => "noscript",
                    });
                }
                if suppressed.is_none()
                    && matches!(
                        name,
                        "address"
                            | "article"
                            | "aside"
                            | "blockquote"
                            | "br"
                            | "div"
                            | "footer"
                            | "h1"
                            | "h2"
                            | "h3"
                            | "h4"
                            | "h5"
                            | "h6"
                            | "header"
                            | "li"
                            | "main"
                            | "nav"
                            | "p"
                            | "pre"
                            | "section"
                            | "table"
                            | "tr"
                    )
                {
                    output.push('\n');
                }
                tag.clear();
                in_tag = false;
            } else if tag.len() < 512 {
                tag.push(character);
            }
        } else if character == '<' {
            in_tag = true;
            tag.clear();
        } else if suppressed.is_none() {
            output.push(character);
        }
    }
    let decoded = output
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");
    decoded
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
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

pub(crate) async fn resolve_target(url: &Url, allow_private: bool) -> Result<SocketAddr> {
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
    clean.set_query(None);
    clean.set_fragment(None);
    clean.to_string()
}

fn url_query_secrets(url: &Url) -> Vec<String> {
    url.query_pairs()
        .filter_map(|(_, value)| (!value.is_empty()).then(|| value.into_owned()))
        .collect()
}

fn is_sensitive_query_key(key: &str) -> bool {
    let normalized = key
        .bytes()
        .filter(|byte| byte.is_ascii_alphanumeric())
        .map(|byte| byte.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let normalized = String::from_utf8_lossy(&normalized);
    normalized == "key"
        || normalized == "sig"
        || normalized == "jwt"
        || normalized == "code"
        || normalized.ends_with("key")
        || normalized.contains("auth")
        || normalized.contains("apikey")
        || normalized.contains("token")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("credential")
        || normalized.contains("signature")
        || normalized.contains("session")
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
                json!({
                    "url": format!("http://{address}/text"),
                    "prompt":"Return the local response"
                }),
            )
            .await;
        server.join().unwrap();
        assert!(!output.is_error, "{}", output.content);
        assert!(output.content.contains("local response"));
        assert!(output.content.contains("Prompt-guided extract"));
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
    fn search_endpoint_rejects_query_credentials_without_echoing_them() {
        let configured_secret = "unit-test-query-credential";
        let settings = Settings {
            raw: json!({"web": {"search": {
                "endpoint": format!(
                    "https://search.example.invalid/query?api_key={configured_secret}"
                )
            }}}),
        };
        let error = configure_web(&settings).err().unwrap();
        let rendered = format!("{error:#}");
        assert!(rendered.contains("请改用 headers"));
        assert!(!rendered.contains(configured_secret));
    }

    #[tokio::test]
    async fn search_query_and_configured_secrets_are_absent_from_results() {
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
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /query?fixed=unit-test-filter&query=rust+agents "));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: unit-test-header-credential")
            );
            let body = "unit-test-header-credential unit-test-filter";
            write!(
                stream,
                "HTTP/1.1 500 Internal Server Error\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        });
        let settings = Settings {
            raw: json!({"web": {
                "allowPrivateNetwork": true,
                "search": {
                    "endpoint": format!("http://{address}/query?fixed=unit-test-filter"),
                    "queryParameter": "query",
                    "headers": {"authorization": "unit-test-header-credential"}
                }
            }}),
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
            .execute(&context, "WebSearch", json!({"query": "rust agents"}))
            .await;
        server.join().unwrap();
        assert!(output.is_error);
        assert!(!output.content.contains("unit-test-header-credential"));
        assert!(!output.content.contains("unit-test-filter"));
        assert!(!output.content.contains("rust+agents"));
        assert!(output.content.contains("[REDACTED]"));
    }

    #[test]
    fn displayed_urls_never_include_query_or_fragment() {
        let url = Url::parse(
            "https://search.example.invalid/query?token=unit-test-query-credential#fragment",
        )
        .unwrap();
        assert_eq!(display_url(&url), "https://search.example.invalid/query");
    }

    #[test]
    fn search_domain_filters_are_normalized_and_enforced_on_json_results() {
        let filters = normalize_domain_filters(&["*.Example.INVALID.".to_owned()], &[]).unwrap();
        assert!(filters.permits("https://docs.example.invalid/a"));
        assert!(filters.permits("https://example.invalid/root"));
        assert!(!filters.permits("https://other.invalid/no"));

        let response = concat!(
            "URL: https://search.invalid/query\nStatus: 200\nContent-Type: application/json\n\n",
            r#"{"results":[{"title":"keep","url":"https://docs.example.invalid/a"},{"title":"drop","url":"https://other.invalid/b"}],"metadata":{"count":2}}"#
        );
        let filtered = apply_search_domain_filters(response, &filters).unwrap();
        assert!(filtered.contains("keep"));
        assert!(!filtered.contains("drop"));
        assert!(filtered.contains("metadata"));

        let blocked = normalize_domain_filters(&[], &["other.invalid".to_owned()]).unwrap();
        let filtered = apply_search_domain_filters(response, &blocked).unwrap();
        assert!(filtered.contains("keep"));
        assert!(!filtered.contains("drop"));
        assert!(apply_search_domain_filters("metadata\n\nplain text", &filters).is_err());
    }

    #[test]
    fn nested_blocked_urls_remove_the_result_without_removing_allowed_siblings() {
        let filters = normalize_domain_filters(&[], &["blocked.invalid".to_owned()]).unwrap();
        let response = concat!(
            "URL: https://search.invalid/query\nStatus: 200\nContent-Type: application/json\n\n",
            r#"{"results":[{"title":"blocked snippet","source":{"details":{"url":"https://blocked.invalid/private"}}},{"title":"allowed sibling","source":{"details":{"url":"https://allowed.invalid/public"}}}],"metadata":{"count":2}}"#
        );
        let filtered = apply_search_domain_filters(response, &filters).unwrap();
        assert!(!filtered.contains("blocked snippet"));
        assert!(!filtered.contains("blocked.invalid"));
        assert!(!filtered.contains("\"source\": null"));
        assert!(filtered.contains("allowed sibling"));
        assert!(filtered.contains("allowed.invalid"));
        assert!(filtered.contains("metadata"));
    }

    #[test]
    fn search_domain_filter_schema_rejects_conflicts_and_invalid_hosts() {
        let tool = WebSearchTool {
            runtime: Arc::new(WebRuntime {
                allow_private_network: false,
                max_bytes: DEFAULT_MAX_BYTES,
                search: None,
            }),
        };
        assert!(
            tool.validate_input(&json!({
                "query":"rust",
                "allowedDomains":["example.com"],
                "blockedDomains":["example.net"]
            }))
            .is_err()
        );
        assert!(
            tool.validate_input(&json!({
                "query":"rust", "allowed_domains":["https://example.invalid/path"]
            }))
            .is_err()
        );
        assert!(
            tool.validate_input(&json!({
                "query":"rust", "blocked_domains":["*.example.com"]
            }))
            .is_ok()
        );
    }

    #[test]
    fn prompt_processing_removes_active_html_and_selects_relevant_lines() {
        let response = concat!(
            "URL: https://example.invalid/\nStatus: 200\nContent-Type: text/html\n\n",
            "<html><style>.secret{}</style><script>alert('hidden')</script>",
            "<p>Rust ownership keeps memory safe.</p><p>Unrelated weather text.</p></html>"
        );
        let processed = apply_prompt_to_response(response, "Explain Rust ownership").unwrap();
        assert!(processed.contains("Rust ownership keeps memory safe"));
        assert!(!processed.contains("Unrelated weather"));
        assert!(!processed.contains("hidden"));
        assert!(!processed.contains(".secret"));
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

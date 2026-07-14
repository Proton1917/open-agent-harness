use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use reqwest::{Response, header::HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{
    sync::Mutex,
    time::{Instant, sleep, timeout},
};
use url::Url;
use uuid::Uuid;

use crate::web_tools::secure_client_for_url;

const OAUTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OAUTH_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_OAUTH_STORE_BYTES: usize = 256 * 1024;
const MAX_OAUTH_URL_BYTES: usize = 16 * 1024;
const MAX_OAUTH_TOKEN_BYTES: usize = 64 * 1024;
const MAX_OAUTH_CLIENT_BYTES: usize = 4096;
const MAX_OAUTH_SCOPES: usize = 64;
const MAX_OAUTH_REDIRECTS: usize = 3;
const ACCESS_TOKEN_REFRESH_SKEW_SECS: u64 = 30;
const OAUTH_STORE_LOCK_TIMEOUT: Duration = Duration::from_secs(2);
const OAUTH_STORE_LOCK_INITIAL_BACKOFF: Duration = Duration::from_millis(10);
const OAUTH_STORE_LOCK_MAX_BACKOFF: Duration = Duration::from_millis(100);
const MAX_STALE_TEMP_SCAN_ENTRIES: usize = 256;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawOAuthConfig {
    #[serde(rename = "clientId")]
    client_id: Option<String>,
    #[serde(rename = "clientSecretEnv")]
    client_secret_env: Option<String>,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(rename = "tokenPath")]
    token_path: Option<String>,
    #[serde(rename = "authorizationUrlPath")]
    authorization_url_path: String,
    #[serde(rename = "callbackPath")]
    callback_path: Option<String>,
    #[serde(rename = "callbackEnv")]
    callback_env: Option<String>,
    #[serde(rename = "redirectUri")]
    redirect_uri: String,
    #[serde(rename = "resourceMetadataUrl")]
    resource_metadata_url: Option<String>,
    #[serde(rename = "authServerMetadataUrl")]
    auth_server_metadata_url: Option<String>,
    #[serde(rename = "allowDynamicClientRegistration", default)]
    allow_dynamic_client_registration: bool,
}

pub(crate) struct OAuthCredentialProvider {
    inner: Arc<OAuthInner>,
}

impl Clone for OAuthCredentialProvider {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for OAuthCredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OAuthCredentialProvider")
            .field("server", &self.inner.server_name)
            .field("server_origin", &origin_text(&self.inner.server_url))
            .field("allow_private_network", &self.inner.allow_private_network)
            .field("credentials", &"<redacted>")
            .finish()
    }
}

struct OAuthInner {
    server_name: String,
    server_url: Url,
    allow_private_network: bool,
    fingerprint: String,
    client_id: Option<String>,
    client_secret_env: Option<String>,
    scopes: Vec<String>,
    token_path: PathBuf,
    authorization_url_path: PathBuf,
    callback: CallbackSource,
    redirect_uri: Url,
    resource_metadata_url: Option<Url>,
    auth_server_metadata_url: Option<Url>,
    allow_dynamic_client_registration: bool,
    lock: Mutex<()>,
}

#[derive(Debug)]
struct OAuthStoreLock {
    file: File,
}

impl Drop for OAuthStoreLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

enum CallbackSource {
    File(PathBuf),
    Env(String),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OAuthStore {
    version: u8,
    fingerprint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    client: Option<StoredClient>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tokens: Option<StoredTokens>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending: Option<StoredPending>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredClient {
    client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_secret: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredTokens {
    access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredPending {
    state: String,
    code_verifier: String,
    authorization_url: String,
    redirect_uri: String,
    token_endpoint: String,
    client_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ProtectedResourceMetadata {
    resource: Option<String>,
    authorization_servers: Option<Vec<String>>,
    scopes_supported: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
struct AuthorizationServerMetadata {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    registration_endpoint: Option<String>,
    scopes_supported: Option<Vec<String>>,
    code_challenge_methods_supported: Option<Vec<String>>,
    token_endpoint_auth_methods_supported: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct RegistrationResponse {
    client_id: String,
    client_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    refresh_token: Option<String>,
    expires_in: Option<u64>,
    scope: Option<String>,
}

struct Discovery {
    authorization_endpoint: Url,
    token_endpoint: Url,
    registration_endpoint: Option<Url>,
    scopes: Vec<String>,
}

impl OAuthCredentialProvider {
    pub(crate) fn from_raw(
        server_name: &str,
        server_url: &Url,
        allow_private_network: bool,
        raw: RawOAuthConfig,
    ) -> Result<Self> {
        validate_bounded_identifier(raw.client_id.as_deref(), "OAuth clientId")?;
        if let Some(name) = &raw.client_secret_env {
            validate_env_name(name).context("OAuth clientSecretEnv 无效")?;
        }
        validate_scopes(&raw.scopes)?;
        let redirect_uri = parse_redirect_uri(&raw.redirect_uri)?;
        let resource_metadata_url = raw
            .resource_metadata_url
            .as_deref()
            .map(|value| {
                parse_oauth_network_url(value, allow_private_network, "resourceMetadataUrl")
            })
            .transpose()?;
        let auth_server_metadata_url = raw
            .auth_server_metadata_url
            .as_deref()
            .map(|value| {
                parse_oauth_network_url(value, allow_private_network, "authServerMetadataUrl")
            })
            .transpose()?;
        let fingerprint = oauth_fingerprint(server_name, server_url);
        let token_path = match raw.token_path {
            Some(path) => absolute_private_path(&path, "OAuth tokenPath")?,
            None => default_token_path(&fingerprint)?,
        };
        let authorization_url_path =
            absolute_private_path(&raw.authorization_url_path, "OAuth authorizationUrlPath")?;
        let callback = match (raw.callback_path, raw.callback_env) {
            (Some(path), None) => {
                CallbackSource::File(absolute_private_path(&path, "OAuth callbackPath")?)
            }
            (None, Some(name)) => {
                validate_env_name(&name).context("OAuth callbackEnv 无效")?;
                CallbackSource::Env(name)
            }
            _ => bail!("OAuth 必须且只能配置 callbackPath 或 callbackEnv 之一"),
        };
        if token_path == authorization_url_path
            || matches!(&callback, CallbackSource::File(path) if path == &token_path || path == &authorization_url_path)
        {
            bail!("OAuth token、authorization URL 与 callback file 必须使用不同路径")
        }
        Ok(Self {
            inner: Arc::new(OAuthInner {
                server_name: server_name.to_owned(),
                server_url: server_url.clone(),
                allow_private_network,
                fingerprint,
                client_id: raw.client_id,
                client_secret_env: raw.client_secret_env,
                scopes: raw.scopes,
                token_path,
                authorization_url_path,
                callback,
                redirect_uri,
                resource_metadata_url,
                auth_server_metadata_url,
                allow_dynamic_client_registration: raw.allow_dynamic_client_registration,
                lock: Mutex::new(()),
            }),
        })
    }

    pub(crate) async fn bearer_header(&self) -> Result<(HeaderValue, String)> {
        self.bearer_header_inner(false).await
    }

    pub(crate) async fn force_refresh_bearer_header(&self) -> Result<(HeaderValue, String)> {
        self.bearer_header_inner(true).await
    }

    async fn bearer_header_inner(&self, force_refresh: bool) -> Result<(HeaderValue, String)> {
        let token = self.bearer_token(force_refresh).await?;
        let mut header = HeaderValue::from_str(&format!("Bearer {token}"))
            .context("OAuth access token 不能编码为 Authorization header")?;
        header.set_sensitive(true);
        Ok((header, token))
    }

    async fn bearer_token(&self, force_refresh: bool) -> Result<String> {
        let _guard = self.inner.lock.lock().await;
        let _store_lock = acquire_oauth_store_lock(&self.inner.token_path).await?;
        cleanup_stale_atomic_temps(&self.inner.authorization_url_path)
            .map_err(|_| anyhow::anyhow!("OAuth authorization URL stale temp cleanup failed"))?;
        let mut store = load_store(&self.inner.token_path, &self.inner.fingerprint)?;

        if store.pending.is_none() && store.tokens.is_some() {
            self.cleanup_completed_authorization_inputs()?;
        }

        if let Some(pending) = store.pending.clone() {
            if let Some(callback) = self.read_callback_response()? {
                self.complete_authorization(&mut store, &pending, &callback)
                    .await?;
            }
        }

        if !force_refresh {
            if let Some(tokens) = store.tokens.as_ref() {
                if access_token_is_fresh(tokens)? {
                    return validate_token(&tokens.access_token, "OAuth access token");
                }
            }
        }

        if let Some(refresh_token) = store
            .tokens
            .as_ref()
            .and_then(|tokens| tokens.refresh_token.clone())
        {
            match self.refresh_tokens(&store, &refresh_token).await {
                Ok(tokens) => {
                    let access = validate_token(&tokens.access_token, "OAuth access token")?;
                    store.tokens = Some(tokens);
                    store.pending = None;
                    save_store(&self.inner.token_path, &store)?;
                    return Ok(access);
                }
                Err(error) if error.to_string().contains("invalid_grant") => {
                    store.tokens = None;
                    save_store(&self.inner.token_path, &store)?;
                }
                Err(error) => return Err(error),
            }
        }

        if store.pending.is_some() {
            if let Some(pending) = &store.pending {
                write_private_atomic(
                    &self.inner.authorization_url_path,
                    format!("{}\n", pending.authorization_url).as_bytes(),
                )?;
            }
            bail!(
                "MCP OAuth authorization 尚未完成；请将完整 callback URL 写入显式 callback 输入后重试"
            )
        }
        self.start_authorization(&mut store).await?;
        bail!("MCP OAuth authorization required；授权 URL 已写入配置的私有 authorizationUrlPath")
    }

    fn read_callback_response(&self) -> Result<Option<String>> {
        let value = match &self.inner.callback {
            CallbackSource::File(path) => match std::fs::symlink_metadata(path) {
                Ok(_) => Some(read_private_text(path, MAX_OAUTH_URL_BYTES)?),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error).context("无法检查 OAuth callback file"),
            },
            CallbackSource::Env(name) => std::env::var(name).ok(),
        };
        value
            .map(|value| {
                let value = value.trim().to_owned();
                if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES || value.contains('\0') {
                    bail!("OAuth callback URL 为空、过长或包含 NUL")
                }
                Ok(value)
            })
            .transpose()
    }

    async fn start_authorization(&self, store: &mut OAuthStore) -> Result<()> {
        let discovery = self.discover().await?;
        let client = self.ensure_client(store, &discovery).await?;
        let verifier = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let state = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let scopes = if self.inner.scopes.is_empty() {
            discovery.scopes.clone()
        } else {
            self.inner.scopes.clone()
        };
        let scope = (!scopes.is_empty()).then(|| scopes.join(" "));
        let mut authorization_url = discovery.authorization_endpoint.clone();
        {
            let mut query = authorization_url.query_pairs_mut();
            query.append_pair("response_type", "code");
            query.append_pair("client_id", &client.client_id);
            query.append_pair("redirect_uri", self.inner.redirect_uri.as_str());
            query.append_pair("state", &state);
            query.append_pair("code_challenge", &challenge);
            query.append_pair("code_challenge_method", "S256");
            query.append_pair("resource", self.inner.server_url.as_str());
            if let Some(scope) = &scope {
                query.append_pair("scope", scope);
            }
        }
        if authorization_url.as_str().len() > MAX_OAUTH_URL_BYTES {
            bail!("OAuth authorization URL 超过限制")
        }
        store.pending = Some(StoredPending {
            state,
            code_verifier: verifier,
            authorization_url: authorization_url.to_string(),
            redirect_uri: self.inner.redirect_uri.to_string(),
            token_endpoint: discovery.token_endpoint.to_string(),
            client_id: client.client_id.clone(),
            scope,
        });
        save_store(&self.inner.token_path, store)?;
        write_private_atomic(
            &self.inner.authorization_url_path,
            format!("{authorization_url}\n").as_bytes(),
        )?;
        Ok(())
    }

    async fn complete_authorization(
        &self,
        store: &mut OAuthStore,
        pending: &StoredPending,
        callback_text: &str,
    ) -> Result<()> {
        let callback = Url::parse(callback_text).context("OAuth callback URL 无效")?;
        let expected =
            Url::parse(&pending.redirect_uri).context("OAuth pending redirect URI 无效")?;
        if url_endpoint_identity(&callback) != url_endpoint_identity(&expected) {
            bail!("OAuth callback URL 与 redirectUri 不匹配")
        }
        if callback.fragment().is_some()
            || !callback.username().is_empty()
            || callback.password().is_some()
        {
            bail!("OAuth callback URL 不得包含 fragment 或凭据")
        }
        let state = unique_query_value(&callback, "state")?.context("OAuth callback 缺少 state")?;
        if !constant_time_equal(state.as_bytes(), pending.state.as_bytes()) {
            bail!("OAuth callback state 校验失败")
        }
        if let Some(error) = unique_query_value(&callback, "error")? {
            bail!(
                "OAuth authorization server 拒绝请求: {}",
                safe_oauth_error_code(&error)
            )
        }
        let code = unique_query_value(&callback, "code")?.context("OAuth callback 缺少 code")?;
        validate_authorization_code(&code)?;
        let endpoint = parse_oauth_network_url(
            &pending.token_endpoint,
            self.inner.allow_private_network,
            "OAuth token endpoint",
        )?;
        let client = effective_client(
            store,
            &pending.client_id,
            self.inner.client_secret_env.as_deref(),
        )?;
        let mut fields = vec![
            ("grant_type", "authorization_code".to_owned()),
            ("code", code),
            ("redirect_uri", pending.redirect_uri.clone()),
            ("client_id", pending.client_id.clone()),
            ("code_verifier", pending.code_verifier.clone()),
            ("resource", self.inner.server_url.to_string()),
        ];
        if let Some(secret) = client.client_secret {
            fields.push(("client_secret", secret));
        }
        let response: TokenResponse = post_form_json(
            &endpoint,
            self.inner.allow_private_network,
            &fields,
            "OAuth token exchange",
        )
        .await?;
        let tokens = validate_token_response(response)?;
        store.tokens = Some(tokens);
        store.pending = None;
        save_store(&self.inner.token_path, store)?;
        self.cleanup_completed_authorization_inputs()
    }

    fn cleanup_completed_authorization_inputs(&self) -> Result<()> {
        if let CallbackSource::File(path) = &self.inner.callback {
            remove_private_file_if_present(path, "OAuth callback file")?;
        }
        remove_private_file_if_present(
            &self.inner.authorization_url_path,
            "OAuth authorization URL file",
        )
    }

    async fn refresh_tokens(
        &self,
        store: &OAuthStore,
        refresh_token: &str,
    ) -> Result<StoredTokens> {
        let discovery = self.discover().await?;
        let client_id = configured_or_stored_client_id(self.inner.client_id.as_deref(), store)?;
        let client = effective_client(store, &client_id, self.inner.client_secret_env.as_deref())?;
        let refresh_token = validate_token(refresh_token, "OAuth refresh token")?;
        let mut fields = vec![
            ("grant_type", "refresh_token".to_owned()),
            ("refresh_token", refresh_token.clone()),
            ("client_id", client_id),
            ("resource", self.inner.server_url.to_string()),
        ];
        if let Some(scope) = store
            .tokens
            .as_ref()
            .and_then(|tokens| tokens.scope.clone())
        {
            fields.push(("scope", scope));
        }
        if let Some(secret) = client.client_secret {
            fields.push(("client_secret", secret));
        }
        let response: TokenResponse = post_form_json(
            &discovery.token_endpoint,
            self.inner.allow_private_network,
            &fields,
            "OAuth token refresh",
        )
        .await?;
        let mut refreshed = validate_token_response(response)?;
        if refreshed.refresh_token.is_none() {
            refreshed.refresh_token = Some(refresh_token);
        }
        Ok(refreshed)
    }

    async fn ensure_client(
        &self,
        store: &mut OAuthStore,
        discovery: &Discovery,
    ) -> Result<StoredClient> {
        if let Some(client_id) = &self.inner.client_id {
            let client = StoredClient {
                client_id: client_id.clone(),
                client_secret: None,
            };
            store.client = Some(client.clone());
            return Ok(client);
        }
        if let Some(client) = &store.client {
            validate_bounded_identifier(Some(&client.client_id), "stored OAuth client_id")?;
            return Ok(client.clone());
        }
        if !self.inner.allow_dynamic_client_registration {
            bail!("OAuth clientId 未配置，且 dynamic client registration 未显式启用")
        }
        let endpoint = discovery
            .registration_endpoint
            .as_ref()
            .context("OAuth metadata 未提供 registration_endpoint")?;
        let body = serde_json::json!({
            "client_name": format!("open-agent-harness ({})", self.inner.server_name),
            "redirect_uris": [self.inner.redirect_uri.as_str()],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none"
        });
        let response: RegistrationResponse = post_json(
            endpoint,
            self.inner.allow_private_network,
            &body,
            "OAuth dynamic client registration",
        )
        .await?;
        validate_bounded_identifier(Some(&response.client_id), "OAuth registered client_id")?;
        if let Some(secret) = &response.client_secret {
            validate_token(secret, "OAuth registered client_secret")?;
        }
        let client = StoredClient {
            client_id: response.client_id,
            client_secret: response.client_secret,
        };
        store.client = Some(client.clone());
        save_store(&self.inner.token_path, store)?;
        Ok(client)
    }

    async fn discover(&self) -> Result<Discovery> {
        let mut protected_scopes = Vec::new();
        let authorization_server = if self.inner.auth_server_metadata_url.is_some() {
            None
        } else {
            let configured_resource_metadata = self.inner.resource_metadata_url.is_some();
            let protected_url = self.inner.resource_metadata_url.clone().unwrap_or_else(|| {
                well_known_url(&self.inner.server_url, "oauth-protected-resource")
            });
            let discovered = get_json::<ProtectedResourceMetadata>(
                &protected_url,
                self.inner.allow_private_network,
                "OAuth protected-resource discovery",
            )
            .await;
            match discovered {
                Ok(metadata) => {
                    if let Some(resource) = metadata.resource.as_deref() {
                        let resource = Url::parse(resource)
                            .context("OAuth resource metadata.resource 无效")?;
                        if normalized_resource(&resource)
                            != normalized_resource(&self.inner.server_url)
                        {
                            bail!("OAuth protected-resource metadata 与 MCP resource 不匹配")
                        }
                    }
                    protected_scopes = validated_metadata_scopes(metadata.scopes_supported)?;
                    let servers = metadata.authorization_servers.unwrap_or_default();
                    if servers.len() > 8 {
                        bail!("OAuth protected-resource authorization_servers 超过限制")
                    }
                    servers.into_iter().next()
                }
                Err(error) if configured_resource_metadata => return Err(error),
                Err(_) => None,
            }
        };
        let (metadata_url, expected_issuer) =
            if let Some(url) = &self.inner.auth_server_metadata_url {
                (url.clone(), None)
            } else {
                let issuer = authorization_server
                    .as_deref()
                    .map(|value| {
                        parse_oauth_network_url(
                            value,
                            self.inner.allow_private_network,
                            "authorization server",
                        )
                    })
                    .transpose()?
                    .unwrap_or_else(|| self.inner.server_url.clone());
                (
                    well_known_url(&issuer, "oauth-authorization-server"),
                    Some(issuer),
                )
            };
        let metadata: AuthorizationServerMetadata = get_json(
            &metadata_url,
            self.inner.allow_private_network,
            "OAuth authorization-server discovery",
        )
        .await?;
        let issuer = parse_oauth_network_url(
            &metadata.issuer,
            self.inner.allow_private_network,
            "OAuth metadata issuer",
        )?;
        if let Some(expected) = expected_issuer {
            if normalized_resource(&issuer) != normalized_resource(&expected) {
                bail!("OAuth metadata issuer 与发现的 authorization server 不匹配")
            }
        }
        if let Some(methods) = &metadata.code_challenge_methods_supported {
            if !methods.iter().any(|method| method == "S256") {
                bail!("OAuth authorization server 不支持 PKCE S256")
            }
        }
        if let Some(methods) = &metadata.token_endpoint_auth_methods_supported {
            let required = if self.inner.client_secret_env.is_some() {
                "client_secret_post"
            } else {
                "none"
            };
            if !(methods.iter().any(|method| method == required)
                || required == "none" && self.inner.client_id.is_none())
            {
                bail!("OAuth authorization server 不支持配置的 token endpoint auth method")
            }
        }
        let authorization_endpoint = parse_oauth_network_url(
            &metadata.authorization_endpoint,
            self.inner.allow_private_network,
            "OAuth authorization endpoint",
        )?;
        let token_endpoint = parse_oauth_network_url(
            &metadata.token_endpoint,
            self.inner.allow_private_network,
            "OAuth token endpoint",
        )?;
        let registration_endpoint = metadata
            .registration_endpoint
            .as_deref()
            .map(|value| {
                parse_oauth_network_url(
                    value,
                    self.inner.allow_private_network,
                    "OAuth registration endpoint",
                )
            })
            .transpose()?;
        let metadata_scopes = validated_metadata_scopes(metadata.scopes_supported.clone())?;
        let scopes = if protected_scopes.is_empty() {
            metadata_scopes
        } else {
            protected_scopes
        };
        Ok(Discovery {
            authorization_endpoint,
            token_endpoint,
            registration_endpoint,
            scopes,
        })
    }
}

fn configured_or_stored_client_id(configured: Option<&str>, store: &OAuthStore) -> Result<String> {
    if let Some(client) = configured {
        return Ok(client.to_owned());
    }
    store
        .client
        .as_ref()
        .map(|client| client.client_id.clone())
        .context("OAuth refresh 缺少 client information")
}

fn effective_client(
    store: &OAuthStore,
    client_id: &str,
    client_secret_env: Option<&str>,
) -> Result<StoredClient> {
    let client_secret = if let Some(name) = client_secret_env {
        Some(
            std::env::var(name)
                .map_err(|_| anyhow::anyhow!("OAuth client secret environment variable 未设置"))?,
        )
    } else {
        store
            .client
            .as_ref()
            .and_then(|client| client.client_secret.clone())
    };
    if let Some(secret) = &client_secret {
        validate_token(secret, "OAuth client secret")?;
    }
    Ok(StoredClient {
        client_id: client_id.to_owned(),
        client_secret,
    })
}

fn validate_token_response(response: TokenResponse) -> Result<StoredTokens> {
    if !response.token_type.eq_ignore_ascii_case("bearer") {
        bail!("OAuth token response token_type 必须是 Bearer")
    }
    let access_token = validate_token(&response.access_token, "OAuth access token")?;
    let refresh_token = response
        .refresh_token
        .map(|token| validate_token(&token, "OAuth refresh token"))
        .transpose()?;
    let expires_at = response
        .expires_in
        .map(|seconds| {
            if seconds > 365 * 24 * 60 * 60 {
                bail!("OAuth expires_in 超过一年限制")
            }
            now_unix()
                .checked_add(seconds)
                .context("OAuth expires_at 溢出")
        })
        .transpose()?;
    if let Some(scope) = &response.scope {
        if scope.len() > 16 * 1024 || scope.contains(['\0', '\r', '\n']) {
            bail!("OAuth token scope 无效或超过限制")
        }
    }
    Ok(StoredTokens {
        access_token,
        refresh_token,
        expires_at,
        scope: response.scope,
    })
}

fn access_token_is_fresh(tokens: &StoredTokens) -> Result<bool> {
    validate_token(&tokens.access_token, "OAuth access token")?;
    Ok(tokens
        .expires_at
        .is_none_or(|expires| expires > now_unix().saturating_add(ACCESS_TOKEN_REFRESH_SKEW_SECS)))
}

async fn get_json<T: for<'de> Deserialize<'de>>(
    initial: &Url,
    allow_private: bool,
    label: &str,
) -> Result<T> {
    let mut current = initial.clone();
    for redirects in 0..=MAX_OAUTH_REDIRECTS {
        let client = secure_client_for_url(&current, allow_private).await?;
        let response = timeout(
            OAUTH_REQUEST_TIMEOUT,
            client
                .get(current.clone())
                .header("accept", "application/json")
                .send(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("{label} timeout"))?
        .map_err(|_| anyhow::anyhow!("{label} request 失败"))?;
        if response.status().is_redirection() {
            if redirects == MAX_OAUTH_REDIRECTS {
                bail!("{label} redirect 超过限制")
            }
            let location = response
                .headers()
                .get("location")
                .context(format!("{label} redirect 缺少 Location"))?
                .to_str()
                .context(format!("{label} redirect Location 无效"))?;
            let next = current
                .join(location)
                .context(format!("{label} redirect URL 无效"))?;
            validate_oauth_url(&next, allow_private, label)?;
            if current.scheme() == "https" && next.scheme() != "https" {
                bail!("{label} 拒绝 HTTPS 降级 redirect")
            }
            current = next;
            continue;
        }
        return decode_json_response(response, label).await;
    }
    unreachable!()
}

async fn post_json<T: for<'de> Deserialize<'de>>(
    url: &Url,
    allow_private: bool,
    body: &Value,
    label: &str,
) -> Result<T> {
    let bytes = serde_json::to_vec(body)?;
    if bytes.len() > MAX_OAUTH_RESPONSE_BYTES {
        bail!("{label} request 超过限制")
    }
    let client = secure_client_for_url(url, allow_private).await?;
    let response = timeout(
        OAUTH_REQUEST_TIMEOUT,
        client
            .post(url.clone())
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .body(bytes)
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("{label} timeout"))?
    .map_err(|_| anyhow::anyhow!("{label} request 失败"))?;
    if response.status().is_redirection() {
        bail!("{label} 拒绝 redirect")
    }
    decode_json_response(response, label).await
}

async fn post_form_json<T: for<'de> Deserialize<'de>>(
    url: &Url,
    allow_private: bool,
    fields: &[(&str, String)],
    label: &str,
) -> Result<T> {
    let body = {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in fields {
            serializer.append_pair(key, value);
        }
        serializer.finish()
    };
    if body.len() > MAX_OAUTH_RESPONSE_BYTES {
        bail!("{label} request 超过限制")
    }
    let client = secure_client_for_url(url, allow_private).await?;
    let response = timeout(
        OAUTH_REQUEST_TIMEOUT,
        client
            .post(url.clone())
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(body)
            .send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("{label} timeout"))?
    .map_err(|_| anyhow::anyhow!("{label} request 失败"))?;
    if response.status().is_redirection() {
        bail!("{label} 拒绝 redirect")
    }
    decode_json_response(response, label).await
}

async fn decode_json_response<T: for<'de> Deserialize<'de>>(
    response: Response,
    label: &str,
) -> Result<T> {
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let bytes = read_response_limited(response).await?;
    if !status.is_success() {
        let error_code = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(safe_oauth_error_code)
            });
        if let Some(code) = error_code {
            bail!("{label} HTTP {}: {code}", status.as_u16())
        }
        bail!("{label} HTTP {}", status.as_u16())
    }
    if !content_type.is_empty()
        && !content_type.contains("application/json")
        && !content_type.contains("+json")
    {
        bail!("{label} response Content-Type 不是 JSON")
    }
    serde_json::from_slice(&bytes).with_context(|| format!("{label} response 不是有效 JSON"))
}

async fn read_response_limited(response: Response) -> Result<Vec<u8>> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("读取 OAuth response 失败")?;
        if bytes.len().saturating_add(chunk.len()) > MAX_OAUTH_RESPONSE_BYTES {
            bail!("OAuth response 超过 {MAX_OAUTH_RESPONSE_BYTES} 字节限制")
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn parse_oauth_network_url(value: &str, allow_private: bool, label: &str) -> Result<Url> {
    if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES {
        bail!("{label} 为空或超过限制")
    }
    let url = Url::parse(value).with_context(|| format!("{label} 无效"))?;
    validate_oauth_url(&url, allow_private, label)?;
    Ok(url)
}

fn validate_oauth_url(url: &Url, allow_private: bool, label: &str) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        bail!("{label} 必须是无凭据、无 fragment 的 http(s) URL")
    }
    if url.scheme() == "http" && !allow_private {
        bail!("{label} 必须使用 HTTPS；只有显式 allowPrivateNetwork 才允许 HTTP")
    }
    if url.query_pairs().any(|(key, _)| sensitive_query_key(&key)) {
        bail!("{label} 不允许在 query 中携带 credential 参数")
    }
    Ok(())
}

fn parse_redirect_uri(value: &str) -> Result<Url> {
    if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES {
        bail!("OAuth redirectUri 为空或超过限制")
    }
    let url = Url::parse(value).context("OAuth redirectUri 无效")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("OAuth redirectUri 必须是无凭据/query/fragment 的 http(s) URL")
    }
    if url.scheme() == "http" && !url.host_str().is_some_and(is_loopback_host) {
        bail!("OAuth HTTP redirectUri 只允许 loopback host")
    }
    Ok(url)
}

fn well_known_url(base: &Url, kind: &str) -> Url {
    let mut url = base.clone();
    let path = base.path().trim_matches('/');
    url.set_path(&format!(
        "/.well-known/{kind}{}",
        if path.is_empty() {
            String::new()
        } else {
            format!("/{path}")
        }
    ));
    url.set_query(None);
    url.set_fragment(None);
    url
}

fn unique_query_value(url: &Url, key: &str) -> Result<Option<String>> {
    let values = url
        .query_pairs()
        .filter(|(name, _)| name == key)
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    if values.len() > 1 {
        bail!("OAuth callback 包含重复 {key}")
    }
    Ok(values.first().map(|value| value.to_string()))
}

fn validate_authorization_code(code: &str) -> Result<()> {
    if code.is_empty()
        || code.len() > MAX_OAUTH_TOKEN_BYTES
        || code.chars().any(char::is_whitespace)
        || code.chars().any(char::is_control)
    {
        bail!("OAuth authorization code 无效或超过限制")
    }
    Ok(())
}

fn validate_token(value: &str, label: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > MAX_OAUTH_TOKEN_BYTES
        || value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
    {
        bail!("{label} 无效或超过限制")
    }
    Ok(value.to_owned())
}

fn validate_bounded_identifier(value: Option<&str>, label: &str) -> Result<()> {
    if value.is_some_and(|value| {
        value.is_empty()
            || value.len() > MAX_OAUTH_CLIENT_BYTES
            || value.contains(['\0', '\r', '\n'])
    }) {
        bail!("{label} 无效或超过限制")
    }
    Ok(())
}

fn validate_scopes(scopes: &[String]) -> Result<()> {
    if scopes.len() > MAX_OAUTH_SCOPES {
        bail!("OAuth scopes 超过 {MAX_OAUTH_SCOPES} 项限制")
    }
    if scopes.iter().any(|scope| {
        scope.is_empty()
            || scope.len() > 1024
            || scope.chars().any(char::is_whitespace)
            || scope.chars().any(char::is_control)
    }) {
        bail!("OAuth scope 为空、过长或包含空白/控制字符")
    }
    Ok(())
}

fn validated_metadata_scopes(scopes: Option<Vec<String>>) -> Result<Vec<String>> {
    let scopes = scopes.unwrap_or_default();
    validate_scopes(&scopes)?;
    Ok(scopes)
}

fn validate_env_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 256
        || !name.bytes().enumerate().all(|(index, byte)| {
            matches!(
                (index, byte),
                (0, b'A'..=b'Z' | b'a'..=b'z' | b'_')
                    | (_, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_')
            )
        })
    {
        bail!("environment variable name 必须是有效 identifier")
    }
    Ok(())
}

fn default_token_path(fingerprint: &str) -> Result<PathBuf> {
    let home = dirs::home_dir().context("OAuth tokenPath 未配置且无法确定 home directory")?;
    Ok(home
        .join(".open-agent-harness")
        .join("mcp-oauth")
        .join(format!("{}.json", &fingerprint[..32])))
}

fn absolute_private_path(value: &str, label: &str) -> Result<PathBuf> {
    if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES || value.contains('\0') {
        bail!("{label} 为空、过长或包含 NUL")
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() || path.file_name().is_none() {
        bail!("{label} 必须是绝对路径")
    }
    if path.components().any(|component| {
        matches!(
            component,
            std::path::Component::CurDir | std::path::Component::ParentDir
        )
    }) {
        bail!("{label} 不得包含 . 或 .. 路径组件")
    }
    Ok(path)
}

fn oauth_fingerprint(server_name: &str, server_url: &Url) -> String {
    let mut hash = Sha256::new();
    hash.update(server_name.as_bytes());
    hash.update([0]);
    hash.update(server_url.as_str().as_bytes());
    format!("{:x}", hash.finalize())
}

fn load_store(path: &Path, fingerprint: &str) -> Result<OAuthStore> {
    let mut store = match std::fs::symlink_metadata(path) {
        Ok(_) => {
            let text = read_private_text(path, MAX_OAUTH_STORE_BYTES)?;
            serde_json::from_str::<OAuthStore>(&text).context("OAuth token store 不是有效 JSON")?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => OAuthStore {
            version: 1,
            fingerprint: fingerprint.to_owned(),
            ..OAuthStore::default()
        },
        Err(error) => return Err(error).context("无法检查 OAuth token store"),
    };
    if store.version != 1 || store.fingerprint != fingerprint {
        bail!("OAuth token store 与当前 MCP server 配置不匹配")
    }
    if let Some(client) = &store.client {
        validate_bounded_identifier(Some(&client.client_id), "stored OAuth client_id")?;
        if let Some(secret) = &client.client_secret {
            validate_token(secret, "stored OAuth client_secret")?;
        }
    }
    if let Some(tokens) = &store.tokens {
        validate_token(&tokens.access_token, "stored OAuth access token")?;
        if let Some(refresh) = &tokens.refresh_token {
            validate_token(refresh, "stored OAuth refresh token")?;
        }
    }
    if let Some(pending) = &store.pending {
        validate_token(&pending.state, "stored OAuth state")?;
        validate_token(&pending.code_verifier, "stored OAuth code_verifier")?;
        validate_authorization_code(&pending.client_id)?;
        if pending.authorization_url.is_empty()
            || pending.authorization_url.len() > MAX_OAUTH_URL_BYTES
            || pending.authorization_url.contains(['\0', '\r', '\n'])
        {
            bail!("stored OAuth authorization URL 无效或超过限制")
        }
        Url::parse(&pending.authorization_url).context("stored OAuth authorization URL 无效")?;
        Url::parse(&pending.token_endpoint).context("stored OAuth token endpoint 无效")?;
        Url::parse(&pending.redirect_uri).context("stored OAuth redirect URI 无效")?;
    }
    store.fingerprint = fingerprint.to_owned();
    Ok(store)
}

fn save_store(path: &Path, store: &OAuthStore) -> Result<()> {
    let bytes = serde_json::to_vec(store)?;
    if bytes.len() > MAX_OAUTH_STORE_BYTES {
        bail!("OAuth token store 超过 {MAX_OAUTH_STORE_BYTES} 字节限制")
    }
    write_private_atomic(path, &bytes)
}

async fn acquire_oauth_store_lock(path: &Path) -> Result<OAuthStoreLock> {
    acquire_oauth_store_lock_with_timeout(path, OAUTH_STORE_LOCK_TIMEOUT).await
}

async fn acquire_oauth_store_lock_with_timeout(
    path: &Path,
    maximum_wait: Duration,
) -> Result<OAuthStoreLock> {
    let parent = path
        .parent()
        .context("OAuth token store path 缺少 parent")?;
    ensure_private_directory_tree(parent)
        .map_err(|_| anyhow::anyhow!("OAuth token store lock directory validation failed"))?;
    let lock_path = oauth_store_lock_path(path)
        .map_err(|_| anyhow::anyhow!("OAuth token store lock path validation failed"))?;
    let (file, created) = match open_oauth_lock_file(&lock_path, true) {
        Ok(file) => (file, true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (
            open_oauth_lock_file(&lock_path, false)
                .map_err(|_| anyhow::anyhow!("OAuth token store lock open failed"))?,
            false,
        ),
        Err(_) => bail!("OAuth token store lock creation failed"),
    };
    if created {
        set_open_file_private_permissions(&file)
            .map_err(|_| anyhow::anyhow!("OAuth token store lock permission setup failed"))?;
        file.sync_all()
            .map_err(|_| anyhow::anyhow!("OAuth token store lock sync failed"))?;
        sync_directory(parent);
    }
    validate_open_private_file(&lock_path, &file)
        .map_err(|_| anyhow::anyhow!("OAuth token store lock validation failed"))?;

    let started = Instant::now();
    let mut backoff = OAUTH_STORE_LOCK_INITIAL_BACKOFF;
    loop {
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => break,
            Err(error) if oauth_lock_is_contended(&error) => {
                let Some(remaining) = maximum_wait.checked_sub(started.elapsed()) else {
                    bail!("OAuth token store lock acquisition timed out")
                };
                if remaining.is_zero() {
                    bail!("OAuth token store lock acquisition timed out")
                }
                sleep(backoff.min(remaining)).await;
                backoff = backoff
                    .checked_mul(2)
                    .unwrap_or(OAUTH_STORE_LOCK_MAX_BACKOFF)
                    .min(OAUTH_STORE_LOCK_MAX_BACKOFF);
            }
            Err(_) => bail!("OAuth token store lock acquisition failed"),
        }
    }
    if validate_open_private_file(&lock_path, &file).is_err() {
        let _ = fs2::FileExt::unlock(&file);
        bail!("OAuth token store lock validation failed")
    }
    if cleanup_stale_atomic_temps(path).is_err() {
        let _ = fs2::FileExt::unlock(&file);
        bail!("OAuth token store stale temp cleanup failed")
    }
    Ok(OAuthStoreLock { file })
}

fn oauth_store_lock_path(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .context("OAuth token store path 缺少 parent")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("OAuth token store filename 无效")?;
    Ok(parent.join(format!(".{name}.lock")))
}

fn open_oauth_lock_file(path: &Path, create_new: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if create_new {
        options.create_new(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options.open(path)
}

fn oauth_lock_is_contended(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    if error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32) {
        return true;
    }
    false
}

fn cleanup_stale_atomic_temps(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("OAuth token store path 缺少 parent")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("OAuth token store filename 无效")?;
    match std::fs::symlink_metadata(parent) {
        Ok(_) => ensure_private_directory_tree(parent)
            .map_err(|_| anyhow::anyhow!("OAuth private temp directory validation failed"))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => bail!("OAuth private temp directory validation failed"),
    }
    let entries =
        std::fs::read_dir(parent).map_err(|_| anyhow::anyhow!("OAuth private temp scan failed"))?;
    let mut candidates = Vec::new();
    for (index, entry) in entries.enumerate() {
        if index >= MAX_STALE_TEMP_SCAN_ENTRIES {
            bail!("OAuth private temp scan entry limit exceeded")
        }
        let entry = entry.map_err(|_| anyhow::anyhow!("OAuth token store temp scan failed"))?;
        let Some(candidate) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_exact_atomic_temp_name(&candidate, name) {
            continue;
        }
        let metadata = match std::fs::symlink_metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => bail!("OAuth token store temp validation failed"),
        };
        validate_private_regular_metadata(&metadata)
            .map_err(|_| anyhow::anyhow!("OAuth atomic temp validation failed"))?;
        let file = open_private_file_for_validation(&entry.path())
            .map_err(|_| anyhow::anyhow!("OAuth atomic temp validation failed"))?;
        drop(file);
        candidates.push(entry.path());
    }
    for candidate in candidates {
        let file = open_private_file_for_validation(&candidate)
            .map_err(|_| anyhow::anyhow!("OAuth atomic temp validation failed"))?;
        drop(file);
        std::fs::remove_file(candidate)
            .map_err(|_| anyhow::anyhow!("OAuth token store stale temp removal failed"))?;
    }
    Ok(())
}

fn is_exact_atomic_temp_name(candidate: &str, target_name: &str) -> bool {
    let prefix = format!(".{target_name}.tmp-");
    let Some(suffix) = candidate.strip_prefix(&prefix) else {
        return false;
    };
    suffix.len() == 32
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn read_private_text(path: &Path, maximum: usize) -> Result<String> {
    let file = open_private_file_for_validation(path)?;
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        bail!("OAuth private file 超过 {maximum} 字节限制")
    }
    String::from_utf8(bytes).context("OAuth private file 不是 UTF-8")
}

fn validate_private_regular_metadata(metadata: &std::fs::Metadata) -> Result<()> {
    if metadata_is_symlink_or_reparse(metadata) || !metadata.is_file() {
        bail!("OAuth private file 必须是 regular file")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.permissions().mode() & 0o777 != 0o600 || metadata.nlink() != 1 {
            bail!("OAuth private file 必须使用 0600 权限")
        }
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!("OAuth private file owner 与当前用户不匹配")
        }
    }
    Ok(())
}

fn validate_open_private_file(path: &Path, file: &File) -> Result<()> {
    let opened = file.metadata().context("无法检查 OAuth private file")?;
    validate_private_regular_metadata(&opened)?;
    let current = std::fs::symlink_metadata(path).context("无法检查 OAuth private file path")?;
    validate_private_regular_metadata(&current)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if opened.dev() != current.dev() || opened.ino() != current.ino() {
            bail!("OAuth private file 在打开期间被替换")
        }
    }
    #[cfg(windows)]
    {
        let opened_info = windows_file_info(file)?;
        validate_windows_private_file_info(&opened_info)?;
        let current_file = open_windows_path_without_following_reparse_points(path)?;
        let current_info = windows_file_info(&current_file)?;
        validate_windows_private_file_info(&current_info)?;
        if windows_file_identity(&opened_info) != windows_file_identity(&current_info) {
            bail!("OAuth private file 在打开期间被替换")
        }
    }
    Ok(())
}

fn open_private_file_for_validation(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options
        .open(path)
        .context("无法安全打开 OAuth private file")?;
    validate_open_private_file(path, &file)?;
    Ok(file)
}

fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("OAuth private path 缺少 parent")?;
    ensure_private_directory_tree(parent)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_regular_metadata(&metadata)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("无法检查 OAuth private file"),
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("OAuth private path filename 无效")?;
    let temporary = parent.join(format!(".{name}.tmp-{}", Uuid::new_v4().simple()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let result = (|| -> Result<()> {
        let mut file = options
            .open(&temporary)
            .context("无法创建 OAuth private temp file")?;
        set_open_file_private_permissions(&file)?;
        validate_open_private_file(&temporary, &file)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path).context("无法原子替换 OAuth private file")?;
        validate_private_regular_metadata(&std::fs::symlink_metadata(path)?)?;
        sync_directory(parent);
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn remove_private_file_if_present(path: &Path, label: &str) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => bail!("无法检查 {label}"),
    };
    validate_private_regular_metadata(&metadata)
        .map_err(|_| anyhow::anyhow!("{label} 不是安全的私有普通文件"))?;
    let parent = path.parent().context("OAuth private path 缺少 parent")?;
    ensure_private_directory_tree(parent)
        .map_err(|_| anyhow::anyhow!("{label} parent validation failed"))?;
    let file = open_private_file_for_validation(path)
        .map_err(|_| anyhow::anyhow!("{label} validation failed"))?;
    drop(file);
    std::fs::remove_file(path).map_err(|_| anyhow::anyhow!("无法安全删除 {label}"))?;
    sync_directory(parent);
    Ok(())
}

fn ensure_private_directory_tree(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        bail!("OAuth private directory 必须是绝对路径")
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                current.push(component.as_os_str());
            }
            std::path::Component::Normal(name) => {
                current.push(name);
                ensure_private_directory_component(&current)?;
            }
            std::path::Component::CurDir | std::path::Component::ParentDir => {
                bail!("OAuth private directory 包含非法路径组件")
            }
        }
    }
    validate_private_directory(path)
}

fn ensure_private_directory_component(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => validate_directory_component_metadata(&metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                let mut builder = std::fs::DirBuilder::new();
                builder.mode(0o700);
                match builder.create(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(_) => bail!("无法创建 OAuth private directory component"),
                }
            }
            #[cfg(not(unix))]
            match std::fs::create_dir(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => bail!("无法创建 OAuth private directory component"),
            }
            let metadata = std::fs::symlink_metadata(path)
                .map_err(|_| anyhow::anyhow!("无法验证 OAuth private directory component"))?;
            validate_directory_component_metadata(&metadata)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
                    .map_err(|_| anyhow::anyhow!("无法设置 OAuth private directory permissions"))?;
            }
            Ok(())
        }
        Err(_) => bail!("无法检查 OAuth private directory component"),
    }
}

fn validate_directory_component_metadata(metadata: &std::fs::Metadata) -> Result<()> {
    if metadata_is_symlink_or_reparse(metadata) || !metadata.is_dir() {
        bail!("OAuth private directory component 必须是非 symlink directory")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let effective_user = unsafe { libc::geteuid() };
        if metadata.uid() != 0 && metadata.uid() != effective_user {
            bail!("OAuth private directory component owner 不可信")
        }
        let mode = metadata.permissions().mode();
        if mode & 0o022 != 0 && mode & 0o1000 == 0 {
            bail!("OAuth private directory component 可被其他用户替换")
        }
    }
    Ok(())
}

fn validate_private_directory(path: &Path) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path).context("无法检查 OAuth private directory")?;
    validate_directory_component_metadata(&metadata)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.permissions().mode() & 0o777 != 0o700 {
            bail!("OAuth private directory 必须使用 0700 权限")
        }
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!("OAuth private directory owner 与当前用户不匹配")
        }
    }
    Ok(())
}

fn set_open_file_private_permissions(file: &File) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = file;
    Ok(())
}

fn sync_directory(path: &Path) {
    if let Ok(directory) = File::open(path) {
        let _ = directory.sync_all();
    }
}

#[cfg(not(windows))]
fn metadata_is_symlink_or_reparse(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn metadata_is_symlink_or_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(windows)]
fn open_windows_path_without_following_reparse_points(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    options.open(path).context("无法复查 OAuth private file")
}

#[cfg(windows)]
fn windows_file_info(
    file: &File,
) -> Result<windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle},
    };

    let mut info = unsafe { std::mem::zeroed::<BY_HANDLE_FILE_INFORMATION>() };
    let result = unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, std::ptr::addr_of_mut!(info))
    };
    if result == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(info)
}

#[cfg(windows)]
fn validate_windows_private_file_info(
    info: &windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION,
) -> Result<()> {
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 || info.nNumberOfLinks != 1 {
        bail!("OAuth private file 必须是非 reparse、单 hard-link 文件")
    }
    Ok(())
}

#[cfg(windows)]
fn windows_file_identity(
    info: &windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION,
) -> (u32, u64) {
    (
        info.dwVolumeSerialNumber,
        (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    )
}

fn unique_normalized_url_text(url: &Url) -> String {
    let mut url = url.clone();
    url.set_fragment(None);
    url.to_string().trim_end_matches('/').to_owned()
}

fn normalized_resource(url: &Url) -> String {
    unique_normalized_url_text(url)
}

fn url_endpoint_identity(url: &Url) -> (String, String, Option<u16>, String) {
    (
        url.scheme().to_owned(),
        url.host_str().unwrap_or_default().to_ascii_lowercase(),
        url.port_or_known_default(),
        url.path().to_owned(),
    )
}

fn origin_text(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn sensitive_query_key(key: &str) -> bool {
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
        || normalized.contains("token")
        || normalized.contains("secret")
        || normalized.contains("password")
        || normalized.contains("credential")
        || normalized.contains("session")
}

fn safe_oauth_error_code(value: &str) -> String {
    let cleaned = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .take(64)
        .collect::<String>();
    if cleaned.is_empty() {
        "oauth_error".to_owned()
    } else {
        cleaned
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::process::{Command, Stdio};
    #[cfg(unix)]
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::{TcpListener, TcpStream},
    };

    #[cfg(unix)]
    fn private_temp_root(temp: &tempfile::TempDir) -> PathBuf {
        use std::os::unix::fs::PermissionsExt as _;

        let root = std::fs::canonicalize(temp.path()).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        root
    }

    #[test]
    fn pkce_state_callback_and_secret_redaction_helpers_are_strict() {
        let verifier = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(verifier.len(), 64);
        assert_eq!(challenge.len(), 43);
        assert!(constant_time_equal(b"same", b"same"));
        assert!(!constant_time_equal(b"same", b"diff"));
        assert_eq!(
            safe_oauth_error_code("invalid_grant?code=secret"),
            "invalid_grantcodesecret"
        );
        let callback = Url::parse("http://127.0.0.1/callback?state=a&code=b").unwrap();
        assert_eq!(
            unique_query_value(&callback, "state").unwrap(),
            Some("a".to_owned())
        );
        let duplicate = Url::parse("http://127.0.0.1/callback?state=a&state=b").unwrap();
        assert!(unique_query_value(&duplicate, "state").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn oauth_store_is_atomic_private_and_fingerprint_bound() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().unwrap();
        let root = private_temp_root(&temp);
        let path = root.join("oauth/store.json");
        let store = OAuthStore {
            version: 1,
            fingerprint: "fingerprint".to_owned(),
            tokens: Some(StoredTokens {
                access_token: "private-access-token".to_owned(),
                refresh_token: Some("private-refresh-token".to_owned()),
                expires_at: Some(now_unix() + 60),
                scope: Some("read".to_owned()),
            }),
            ..OAuthStore::default()
        };
        save_store(&path, &store).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            load_store(&path, "fingerprint")
                .unwrap()
                .tokens
                .unwrap()
                .access_token,
            "private-access-token"
        );
        assert!(load_store(&path, "other").is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_store(&path, "fingerprint").is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oauth_store_lock_cleans_only_exact_private_stale_temps() {
        use std::os::unix::fs::OpenOptionsExt as _;

        let temp = tempfile::tempdir().unwrap();
        let root = private_temp_root(&temp);
        let token_path = root.join("nested/store.json");
        ensure_private_directory_tree(token_path.parent().unwrap()).unwrap();
        let exact = token_path
            .parent()
            .unwrap()
            .join(".store.json.tmp-0123456789abcdef0123456789abcdef");
        let wrong_suffix = token_path
            .parent()
            .unwrap()
            .join(".store.json.tmp-0123456789abcdef0123456789abcdeg");
        for path in [&exact, &wrong_suffix] {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .unwrap();
            file.write_all(b"crash residue").unwrap();
        }

        let _guard = acquire_oauth_store_lock(&token_path).await.unwrap();
        assert!(!exact.exists());
        assert!(wrong_suffix.exists());
    }

    #[cfg(unix)]
    #[test]
    fn oauth_stale_temp_validation_is_fail_closed_before_deletion() {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

        let temp = tempfile::tempdir().unwrap();
        let root = private_temp_root(&temp);
        let token_path = root.join("store.json");
        let valid = root.join(".store.json.tmp-0123456789abcdef0123456789abcdef");
        let invalid = root.join(".store.json.tmp-fedcba9876543210fedcba9876543210");
        for path in [&valid, &invalid] {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)
                .unwrap();
        }
        std::fs::set_permissions(&invalid, std::fs::Permissions::from_mode(0o644)).unwrap();

        let error = cleanup_stale_atomic_temps(&token_path).unwrap_err();
        assert!(error.to_string().contains("validation failed"));
        assert!(valid.exists());
        assert!(invalid.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oauth_private_directory_creation_rejects_symlink_component_without_path_leak() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = tempfile::tempdir().unwrap();
        let root = private_temp_root(&temp);
        let real = root.join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
        let link = root.join("private-link");
        symlink(&real, &link).unwrap();
        let token_path = link.join("nested/super-secret-store.json");

        let error = acquire_oauth_store_lock_with_timeout(&token_path, Duration::from_millis(20))
            .await
            .unwrap_err();
        let rendered = error.to_string();
        assert!(rendered.contains("lock directory validation failed"));
        assert!(!rendered.contains("super-secret-store"));
        assert!(!rendered.contains(&root.to_string_lossy().into_owned()));
        assert!(!real.join("nested").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oauth_store_lock_timeout_is_bounded_cross_process_and_redacted() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_temp_root(&temp);
        let token_path = root.join("super-secret-token-store.json");
        let ready_path = root.join("holder-ready");
        let executable = std::env::current_exe().unwrap();
        let mut child = Command::new(executable)
            .arg("--exact")
            .arg("mcp_oauth::tests::oauth_store_lock_holder_process")
            .arg("--nocapture")
            .env("OAH_TEST_OAUTH_LOCK_PATH", &token_path)
            .env("OAH_TEST_OAUTH_LOCK_READY", &ready_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let ready_deadline = Instant::now() + Duration::from_secs(5);
        while !ready_path.exists() && Instant::now() < ready_deadline {
            sleep(Duration::from_millis(10)).await;
        }
        assert!(ready_path.exists(), "lock holder did not become ready");

        let started = Instant::now();
        let error = acquire_oauth_store_lock_with_timeout(&token_path, Duration::from_millis(80))
            .await
            .unwrap_err();
        assert!(started.elapsed() >= Duration::from_millis(70));
        assert!(started.elapsed() < Duration::from_secs(1));
        let rendered = error.to_string();
        assert_eq!(rendered, "OAuth token store lock acquisition timed out");
        assert!(!rendered.contains("super-secret-token-store"));
        assert!(!rendered.contains(&root.to_string_lossy().into_owned()));
        let _ = child.kill();
        child.wait().unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oauth_store_lock_holder_process() {
        let Some(token_path) = std::env::var_os("OAH_TEST_OAUTH_LOCK_PATH") else {
            return;
        };
        let ready_path = std::env::var_os("OAH_TEST_OAUTH_LOCK_READY").unwrap();
        let _guard = acquire_oauth_store_lock(Path::new(&token_path))
            .await
            .unwrap();
        write_private_atomic(Path::new(&ready_path), b"ready").unwrap();
        std::thread::sleep(Duration::from_secs(5));
    }

    #[test]
    fn oauth_config_requires_explicit_private_callback_channel() {
        let temp = tempfile::tempdir().unwrap();
        let server = Url::parse("http://127.0.0.1:1/mcp").unwrap();
        let raw: RawOAuthConfig = serde_json::from_value(serde_json::json!({
            "clientId":"client",
            "authorizationUrlPath":temp.path().join("authorize").to_string_lossy(),
            "redirectUri":"http://127.0.0.1/callback"
        }))
        .unwrap();
        let error = OAuthCredentialProvider::from_raw("mock", &server, true, raw).unwrap_err();
        assert!(error.to_string().contains("callbackPath"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_authorization_keeps_callback_and_authorization_url_for_retry() {
        let temp = tempfile::tempdir().unwrap();
        let root = private_temp_root(&temp);
        let token_path = root.join("store.json");
        let authorization_path = root.join("authorization-url");
        let callback_path = root.join("callback-url");
        let raw: RawOAuthConfig = serde_json::from_value(serde_json::json!({
            "clientId":"client",
            "tokenPath":token_path,
            "authorizationUrlPath":authorization_path,
            "callbackPath":callback_path,
            "redirectUri":"http://127.0.0.1/callback"
        }))
        .unwrap();
        let provider = OAuthCredentialProvider::from_raw(
            "mock",
            &Url::parse("http://127.0.0.1:1/mcp").unwrap(),
            true,
            raw,
        )
        .unwrap();
        write_private_atomic(&authorization_path, b"http://example.invalid/authorize\n").unwrap();
        write_private_atomic(
            &callback_path,
            b"http://127.0.0.1/callback?state=wrong&code=authorization-code",
        )
        .unwrap();
        let pending = StoredPending {
            state: "expected-state".to_owned(),
            code_verifier: "verifier".to_owned(),
            authorization_url: "http://example.invalid/authorize".to_owned(),
            redirect_uri: "http://127.0.0.1/callback".to_owned(),
            token_endpoint: "http://127.0.0.1:1/token".to_owned(),
            client_id: "client".to_owned(),
            scope: None,
        };
        let mut store = OAuthStore {
            version: 1,
            fingerprint: provider.inner.fingerprint.clone(),
            pending: Some(pending.clone()),
            ..OAuthStore::default()
        };
        let error = provider
            .complete_authorization(
                &mut store,
                &pending,
                "http://127.0.0.1/callback?state=wrong&code=authorization-code",
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("state 校验失败"));
        assert!(callback_path.exists());
        assert!(authorization_path.exists());
        assert!(store.pending.is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn oauth_mock_server_runs_discovery_dcr_pkce_exchange_and_refresh() {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let issuer = format!("http://{address}/auth");
        let server_url = format!("http://{address}/mcp");
        let mock_server_url = server_url.clone();
        let mock = tokio::spawn(async move {
            let mut registration_seen = false;
            let mut exchange_seen = false;
            let mut refresh_count = 0usize;
            for _ in 0..10 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let (request_line, body) = read_mock_request(&mut stream).await;
                let path = request_line.split_whitespace().nth(1).unwrap();
                let response = match path {
                    "/.well-known/oauth-protected-resource/mcp" => serde_json::json!({
                        "resource":mock_server_url,
                        "authorization_servers":[issuer],
                        "scopes_supported":["read", "write"]
                    }),
                    "/.well-known/oauth-authorization-server/auth" => serde_json::json!({
                        "issuer":issuer,
                        "authorization_endpoint":format!("http://{address}/authorize"),
                        "token_endpoint":format!("http://{address}/token"),
                        "registration_endpoint":format!("http://{address}/register"),
                        "code_challenge_methods_supported":["S256"],
                        "token_endpoint_auth_methods_supported":["none"]
                    }),
                    "/register" => {
                        assert!(body.contains("authorization_code"));
                        assert!(body.contains("http://127.0.0.1/callback"));
                        registration_seen = true;
                        serde_json::json!({"client_id":"registered-client"})
                    }
                    "/token" if body.contains("grant_type=authorization_code") => {
                        assert!(body.contains("code_verifier="));
                        assert!(body.contains("code=authorization-code"));
                        assert!(body.contains("client_id=registered-client"));
                        exchange_seen = true;
                        serde_json::json!({
                            "access_token":"first-access",
                            "token_type":"Bearer",
                            "refresh_token":"refresh-value",
                            "expires_in":0,
                            "scope":"read write"
                        })
                    }
                    "/token" if body.contains("grant_type=refresh_token") => {
                        assert!(body.contains("refresh_token=refresh-value"));
                        assert!(body.contains("client_id=registered-client"));
                        refresh_count += 1;
                        serde_json::json!({
                            "access_token":"refreshed-access",
                            "token_type":"Bearer",
                            "expires_in":3600,
                            "scope":"read write"
                        })
                    }
                    other => panic!("unexpected mock OAuth path {other}; body={body}"),
                };
                write_mock_json(&mut stream, &response).await;
            }
            assert!(registration_seen);
            assert!(exchange_seen);
            assert_eq!(refresh_count, 2);
        });

        let temp = tempfile::tempdir().unwrap();
        let root = private_temp_root(&temp);
        let token_path = root.join("oauth-store.json");
        let authorization_path = root.join("authorization-url");
        let callback_path = root.join("callback-url");
        let stale_authorization =
            root.join(".authorization-url.tmp-0123456789abcdef0123456789abcdef");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&stale_authorization)
            .unwrap();
        let raw: RawOAuthConfig = serde_json::from_value(serde_json::json!({
            "tokenPath":token_path,
            "authorizationUrlPath":authorization_path,
            "callbackPath":callback_path,
            "redirectUri":"http://127.0.0.1/callback",
            "allowDynamicClientRegistration":true
        }))
        .unwrap();
        let provider =
            OAuthCredentialProvider::from_raw("mock", &Url::parse(&server_url).unwrap(), true, raw)
                .unwrap();
        let first = provider.bearer_header().await.unwrap_err();
        assert!(first.to_string().contains("authorization required"));
        assert!(!stale_authorization.exists());
        let authorization = std::fs::read_to_string(&authorization_path).unwrap();
        let authorization = Url::parse(authorization.trim()).unwrap();
        assert_eq!(
            authorization
                .query_pairs()
                .find(|(key, _)| key == "code_challenge_method")
                .unwrap()
                .1,
            "S256"
        );
        assert_eq!(
            authorization
                .query_pairs()
                .find(|(key, _)| key == "client_id")
                .unwrap()
                .1,
            "registered-client"
        );
        assert_eq!(
            authorization
                .query_pairs()
                .find(|(key, _)| key == "code_challenge")
                .unwrap()
                .1
                .len(),
            43
        );
        let state = authorization
            .query_pairs()
            .find(|(key, _)| key == "state")
            .unwrap()
            .1
            .into_owned();
        std::fs::write(
            &callback_path,
            format!("http://127.0.0.1/callback?state={state}&code=authorization-code"),
        )
        .unwrap();
        std::fs::set_permissions(&callback_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let (header, secret) = provider.bearer_header().await.unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer refreshed-access");
        assert_eq!(secret, "refreshed-access");
        assert!(!callback_path.exists());
        assert!(!authorization_path.exists());
        let (header, secret) = provider.force_refresh_bearer_header().await.unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer refreshed-access");
        assert_eq!(secret, "refreshed-access");
        assert_eq!(
            std::fs::metadata(&token_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let stored = std::fs::read_to_string(&token_path).unwrap();
        assert!(stored.contains("refreshed-access"));
        assert!(!stored.contains("first-access"));
        mock.await.unwrap();
    }

    #[cfg(unix)]
    async fn read_mock_request(stream: &mut TcpStream) -> (String, String) {
        let mut bytes = Vec::new();
        let header_end = loop {
            let mut chunk = [0u8; 2048];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&chunk[..read]);
            assert!(bytes.len() <= 128 * 1024);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = std::str::from_utf8(&bytes[..header_end])
            .unwrap()
            .to_owned();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("content-length: ")
                    .or_else(|| line.strip_prefix("Content-Length: "))
            })
            .map(|value| value.trim().parse::<usize>().unwrap())
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let mut chunk = [0u8; 2048];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&chunk[..read]);
        }
        (
            headers.lines().next().unwrap().to_owned(),
            String::from_utf8(bytes[header_end..header_end + content_length].to_vec()).unwrap(),
        )
    }

    #[cfg(unix)]
    async fn write_mock_json(stream: &mut TcpStream, value: &Value) {
        let body = serde_json::to_vec(value).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
        stream.shutdown().await.unwrap();
    }
}

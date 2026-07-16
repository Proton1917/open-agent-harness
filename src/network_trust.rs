use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, bail};
use reqwest::{Certificate, ClientBuilder, Identity};
use rustls::{ClientConfig, RootCertStore};
use tokio_tungstenite::Connector;

const MAX_TRUST_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_TRUST_PATH_BYTES: usize = 16 * 1024;
const CA_ENV: &str = "HARNESS_CA_CERT_FILE";
const CLIENT_CERT_ENV: &str = "HARNESS_CLIENT_CERT_FILE";
const CLIENT_KEY_ENV: &str = "HARNESS_CLIENT_KEY_FILE";

static PROCESS_NETWORK_TRUST: OnceLock<NetworkTrust> = OnceLock::new();

#[derive(Clone, Default)]
pub struct NetworkTrust {
    ca_bundle: Option<Arc<[u8]>>,
    client_identity: Option<Arc<[u8]>>,
}

impl std::fmt::Debug for NetworkTrust {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetworkTrust")
            .field("custom_ca", &self.ca_bundle.is_some())
            .field("client_identity", &self.client_identity.is_some())
            .finish()
    }
}

impl NetworkTrust {
    pub fn from_files(
        ca_path: Option<&Path>,
        client_cert_path: Option<&Path>,
        client_key_path: Option<&Path>,
    ) -> Result<Self> {
        if client_cert_path.is_some() != client_key_path.is_some() {
            bail!(
                "HARNESS_CLIENT_CERT_FILE and HARNESS_CLIENT_KEY_FILE must be configured together"
            )
        }
        let ca_bundle = ca_path
            .map(|path| read_trust_file(path, false, "CA certificate"))
            .transpose()?
            .map(Arc::from);
        let client_identity = match (client_cert_path, client_key_path) {
            (Some(cert_path), Some(key_path)) => {
                let cert = read_trust_file(cert_path, false, "client certificate")?;
                let key = read_trust_file(key_path, true, "client private key")?;
                let mut identity = Vec::with_capacity(cert.len().saturating_add(key.len() + 1));
                identity.extend_from_slice(&cert);
                if !identity.ends_with(b"\n") {
                    identity.push(b'\n');
                }
                identity.extend_from_slice(&key);
                if identity.len() as u64 > MAX_TRUST_FILE_BYTES.saturating_mul(2) {
                    bail!("combined client identity exceeds the configured byte limit")
                }
                Some(Arc::from(identity))
            }
            (None, None) => None,
            _ => unreachable!("paired client identity paths were checked"),
        };
        let trust = Self {
            ca_bundle,
            client_identity,
        };
        trust.validate_material()?;
        Ok(trust)
    }

    pub fn is_configured(&self) -> bool {
        self.ca_bundle.is_some() || self.client_identity.is_some()
    }

    pub fn apply_reqwest(&self, mut builder: ClientBuilder) -> Result<ClientBuilder> {
        if let Some(bundle) = &self.ca_bundle {
            let certificates = Certificate::from_pem_bundle(bundle)
                .context("HARNESS_CA_CERT_FILE is not a valid PEM certificate bundle")?;
            if certificates.is_empty() {
                bail!("HARNESS_CA_CERT_FILE does not contain a certificate")
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if let Some(identity) = &self.client_identity {
            let identity = Identity::from_pem(identity)
                .context("client certificate and key are not a valid unencrypted PEM identity")?;
            builder = builder.identity(identity);
        }
        Ok(builder)
    }

    pub fn websocket_connector(&self) -> Result<Option<Connector>> {
        if !self.is_configured() {
            return Ok(None);
        }
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(bundle) = &self.ca_bundle {
            let mut reader = BufReader::new(bundle.as_ref());
            let certificates = rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("HARNESS_CA_CERT_FILE is not a valid PEM certificate bundle")?;
            if certificates.is_empty() {
                bail!("HARNESS_CA_CERT_FILE does not contain a certificate")
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .context("HARNESS_CA_CERT_FILE contains an invalid certificate")?;
            }
        }
        let builder = ClientConfig::builder().with_root_certificates(roots);
        let config = if let Some(identity) = &self.client_identity {
            let mut cert_reader = BufReader::new(identity.as_ref());
            let certificates = rustls_pemfile::certs(&mut cert_reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("client certificate PEM is invalid")?;
            let mut key_reader = BufReader::new(identity.as_ref());
            let key = rustls_pemfile::private_key(&mut key_reader)
                .context("client private key PEM is invalid")?
                .context("client private key PEM is missing")?;
            builder
                .with_client_auth_cert(certificates, key)
                .context("client certificate and private key do not form a valid identity")?
        } else {
            builder.with_no_client_auth()
        };
        Ok(Some(Connector::Rustls(Arc::new(config))))
    }

    fn validate_material(&self) -> Result<()> {
        self.apply_reqwest(ClientBuilder::new())?.build().context(
            "custom CA or client identity could not be installed in the HTTP TLS client",
        )?;
        self.websocket_connector()?;
        Ok(())
    }
}

pub fn initialize_process_network_trust_from_env() -> Result<()> {
    if PROCESS_NETWORK_TRUST.get().is_some() {
        return Ok(());
    }
    let ca = env_path(CA_ENV)?;
    let cert = env_path(CLIENT_CERT_ENV)?;
    let key = env_path(CLIENT_KEY_ENV)?;
    let trust = NetworkTrust::from_files(ca.as_deref(), cert.as_deref(), key.as_deref())?;
    PROCESS_NETWORK_TRUST
        .set(trust)
        .map_err(|_| anyhow::anyhow!("process network trust was initialized concurrently"))
}

pub fn process_network_trust() -> NetworkTrust {
    PROCESS_NETWORK_TRUST.get().cloned().unwrap_or_default()
}

fn env_path(name: &str) -> Result<Option<PathBuf>> {
    let Some(value) = env::var_os(name) else {
        return Ok(None);
    };
    if value.is_empty() {
        bail!("{name} cannot be empty")
    }
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("{name} must be an absolute path")
    }
    if path.as_os_str().as_encoded_bytes().len() > MAX_TRUST_PATH_BYTES {
        bail!("{name} path exceeds {MAX_TRUST_PATH_BYTES} bytes")
    }
    Ok(Some(path))
}

fn read_trust_file(path: &Path, private: bool, label: &str) -> Result<Vec<u8>> {
    if !path.is_absolute() {
        bail!("{label} path must be absolute")
    }
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("cannot inspect {label} file"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("{label} path must be a direct regular file")
    }
    validate_not_reparse_point(&metadata, label)?;
    if metadata.len() == 0 || metadata.len() > MAX_TRUST_FILE_BYTES {
        bail!("{label} file must contain 1..={MAX_TRUST_FILE_BYTES} bytes")
    }
    #[cfg(unix)]
    if private {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("{label} file permissions must not grant group or other access")
        }
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("cannot open {label} file"))?;
    validate_open_file(path, &file, private, label)?;
    let capacity = usize::try_from(metadata.len()).context("trust file size does not fit usize")?;
    let mut bytes = Vec::with_capacity(capacity);
    file.by_ref()
        .take(MAX_TRUST_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("cannot read {label} file"))?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_TRUST_FILE_BYTES {
        bail!("{label} file changed beyond the configured byte limit")
    }
    Ok(bytes)
}

fn validate_open_file(path: &Path, file: &File, private: bool, label: &str) -> Result<()> {
    #[cfg(not(unix))]
    let _ = private;
    let path_metadata =
        fs::symlink_metadata(path).with_context(|| format!("cannot re-check {label} path"))?;
    let open_metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect open {label} file"))?;
    if path_metadata.file_type().is_symlink() || !open_metadata.is_file() {
        bail!("{label} file changed while opening")
    }
    validate_not_reparse_point(&open_metadata, label)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path_metadata.dev() != open_metadata.dev() || path_metadata.ino() != open_metadata.ino()
        {
            bail!("{label} file changed while opening")
        }
        if private && open_metadata.mode() & 0o077 != 0 {
            bail!("{label} file permissions changed while opening")
        }
    }
    Ok(())
}

#[cfg(windows)]
fn validate_not_reparse_point(metadata: &fs::Metadata, label: &str) -> Result<()> {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        bail!("{label} path must not be a reparse point")
    }
    Ok(())
}

#[cfg(not(windows))]
fn validate_not_reparse_point(_: &fs::Metadata, _: &str) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_trust_keeps_default_http_and_websocket_configuration() {
        let trust = NetworkTrust::default();
        assert!(!trust.is_configured());
        trust
            .apply_reqwest(ClientBuilder::new())
            .unwrap()
            .build()
            .unwrap();
        assert!(trust.websocket_connector().unwrap().is_none());
    }

    #[test]
    fn client_identity_paths_must_be_paired() {
        let directory = tempfile::tempdir().unwrap();
        let cert = directory.path().join("client.pem");
        fs::write(&cert, b"invalid").unwrap();
        assert!(NetworkTrust::from_files(None, Some(&cert), None).is_err());
    }

    #[test]
    fn invalid_custom_ca_fails_before_any_network_request() {
        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("ca.pem");
        fs::write(&ca, b"not a certificate").unwrap();
        assert!(NetworkTrust::from_files(Some(&ca), None, None).is_err());
    }

    #[test]
    fn valid_custom_ca_and_client_identity_configure_http_and_websocket_tls() {
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let ca = directory.path().join("ca.pem");
        let client = directory.path().join("client.pem");
        let key = directory.path().join("client.key");
        fs::write(&ca, cert.pem()).unwrap();
        fs::write(&client, cert.pem()).unwrap();
        fs::write(&key, key_pair.serialize_pem()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let trust = NetworkTrust::from_files(Some(&ca), Some(&client), Some(&key)).unwrap();
        assert!(trust.is_configured());
        trust
            .apply_reqwest(ClientBuilder::new())
            .unwrap()
            .build()
            .unwrap();
        assert!(trust.websocket_connector().unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn private_key_rejects_broad_permissions_and_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().unwrap();
        let cert = directory.path().join("client.pem");
        let key = directory.path().join("client.key");
        fs::write(&cert, b"invalid certificate").unwrap();
        fs::write(&key, b"invalid key").unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(NetworkTrust::from_files(None, Some(&cert), Some(&key)).is_err());
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let linked = directory.path().join("linked.key");
        symlink(&key, &linked).unwrap();
        assert!(NetworkTrust::from_files(None, Some(&cert), Some(&linked)).is_err());
    }
}

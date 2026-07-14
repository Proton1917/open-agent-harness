use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Cursor, Read, Write},
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use fs2::FileExt as _;
use futures_util::StreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;
use walkdir::WalkDir;
use zip::ZipArchive;

use crate::{
    cli::PluginCommand, config::Settings, plugins::PluginCatalog, tools::ensure_private_directory,
    web_tools::secure_client_for_url,
};

const REGISTRY_SCHEMA_VERSION: u32 = 1;
const MAX_REGISTRY_BYTES: u64 = 1024 * 1024;
const MAX_INSTALLED_PLUGINS: usize = 32;
const MAX_SOURCE_BYTES: usize = 4096;
const MAX_ARCHIVE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PACKAGE_FILES: usize = 2048;
const MAX_PACKAGE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_PACKAGE_TOTAL_BYTES: u64 = 32 * 1024 * 1024;
const MAX_PACKAGE_DEPTH: usize = 32;
const MAX_REDIRECTS: usize = 3;
const TRANSACTION_VERSION: u32 = 1;
const TRANSACTION_FILE: &str = "transaction.json";
const MAX_TRANSACTION_BYTES: u64 = 64 * 1024;
const MAX_TRANSIENT_ENTRIES: usize = MAX_PACKAGE_FILES * 4 + 64;
const MAX_TRANSIENT_BYTES: u64 = MAX_PACKAGE_TOTAL_BYTES * 4;
const REGISTRY_TEMP_PREFIX: &str = ".plugin-manager-registry-";
const JOURNAL_TEMP_PREFIX: &str = ".plugin-manager-journal-";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ValidatedPlugin {
    pub id: String,
    pub version: String,
    pub source: String,
    pub sha256: String,
    pub files: usize,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstalledPlugin {
    pub id: String,
    pub version: String,
    pub source: String,
    pub sha256: String,
    pub install_path: PathBuf,
    pub installed_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledRecord {
    id: String,
    version: String,
    source: String,
    sha256: String,
    installed_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    schema_version: u32,
    plugins: BTreeMap<String, InstalledRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum TransactionOperation {
    Install {
        plugin_id: String,
        stage: String,
        record: InstalledRecord,
    },
    Update {
        plugin_id: String,
        stage: String,
        backup: String,
        old_record: InstalledRecord,
        new_record: InstalledRecord,
    },
    Uninstall {
        plugin_id: String,
        quarantine: String,
        old_record: InstalledRecord,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransactionJournal {
    version: u32,
    operation: TransactionOperation,
}

#[derive(Default)]
struct LayoutResidues {
    stages: Vec<PathBuf>,
    backups: Vec<(String, PathBuf)>,
    quarantines: Vec<(String, PathBuf)>,
    atomic_temps: Vec<PathBuf>,
}

#[derive(Default)]
struct TreeUsage {
    entries: usize,
    bytes: u64,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            schema_version: REGISTRY_SCHEMA_VERSION,
            plugins: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PluginManager {
    root: PathBuf,
}

impl PluginManager {
    pub fn user_default() -> Result<Self> {
        let home = dirs::home_dir().context("无法确定 plugin cache 用户主目录")?;
        Ok(Self::new(home.join(".open-agent-harness/plugins")))
    }

    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn list(&self) -> Result<Vec<InstalledPlugin>> {
        let _lock = self.acquire_lock()?;
        let registry = self.load_registry()?;
        registry
            .plugins
            .values()
            .map(|record| self.public_record(record))
            .collect()
    }

    pub async fn validate(
        &self,
        source: &str,
        expected_sha256: Option<&str>,
    ) -> Result<ValidatedPlugin> {
        let _lock = self.acquire_lock()?;
        let prepared = self.prepare(source, expected_sha256).await?;
        Ok(prepared.descriptor())
    }

    pub async fn install(
        &self,
        source: &str,
        expected_sha256: Option<&str>,
    ) -> Result<InstalledPlugin> {
        let _lock = self.acquire_lock()?;
        let mut prepared = self.prepare(source, expected_sha256).await?;
        let mut registry = self.load_registry()?;
        if registry.plugins.contains_key(&prepared.id) {
            bail!("plugin {} 已安装；请使用 plugin update", prepared.id)
        }
        if registry.plugins.len() >= MAX_INSTALLED_PLUGINS {
            bail!("已安装 plugin 超过 {MAX_INSTALLED_PLUGINS} 个限制")
        }
        let now = now_ms()?;
        let target = self.package_path(&prepared.id)?;
        reject_existing_path(&target, "plugin install target")?;
        let record = InstalledRecord {
            id: prepared.id.clone(),
            version: prepared.version.clone(),
            source: prepared.source.clone(),
            sha256: prepared.sha256.clone(),
            installed_at_ms: now,
            updated_at_ms: now,
        };
        let journal = TransactionJournal {
            version: TRANSACTION_VERSION,
            operation: TransactionOperation::Install {
                plugin_id: prepared.id.clone(),
                stage: prepared.stage_name()?,
                record: record.clone(),
            },
        };
        self.write_journal(&journal)?;
        prepared.transfer_cleanup_to_journal();
        self.recover_transaction(&journal, &mut registry)?;
        self.public_record(&record)
    }

    pub async fn update(
        &self,
        plugin_id: &str,
        source: Option<&str>,
        expected_sha256: Option<&str>,
    ) -> Result<InstalledPlugin> {
        validate_plugin_id(plugin_id)?;
        let _lock = self.acquire_lock()?;
        let existing = self
            .load_registry()?
            .plugins
            .get(plugin_id)
            .cloned()
            .with_context(|| format!("plugin {plugin_id} 未安装"))?;
        let source = source.unwrap_or(&existing.source);
        let expected = expected_sha256.or_else(|| {
            (source == existing.source && source.starts_with("https://"))
                .then_some(existing.sha256.as_str())
        });
        let mut prepared = self.prepare(source, expected).await?;
        if prepared.id != plugin_id {
            bail!(
                "update source manifest id {} 与已安装 id {plugin_id} 不一致",
                prepared.id
            )
        }
        let mut registry = self.load_registry()?;
        let current = registry
            .plugins
            .get(plugin_id)
            .cloned()
            .with_context(|| format!("plugin {plugin_id} 在 update 期间被卸载"))?;
        if current != existing {
            bail!("plugin {plugin_id} 在 update 期间发生并发变化，请重试")
        }
        let updated_at_ms = now_ms()?;
        let target = self.package_path(plugin_id)?;
        validate_installed_directory(&target, &self.packages_directory())?;
        let backup = self
            .root
            .join(format!(".backup-{plugin_id}-{}", Uuid::new_v4()));
        reject_existing_path(&backup, "plugin update backup")?;
        let record = InstalledRecord {
            id: plugin_id.to_owned(),
            version: prepared.version.clone(),
            source: prepared.source.clone(),
            sha256: prepared.sha256.clone(),
            installed_at_ms: current.installed_at_ms,
            updated_at_ms,
        };
        let journal = TransactionJournal {
            version: TRANSACTION_VERSION,
            operation: TransactionOperation::Update {
                plugin_id: plugin_id.to_owned(),
                stage: prepared.stage_name()?,
                backup: backup
                    .file_name()
                    .and_then(|name| name.to_str())
                    .context("plugin update backup 名称无效")?
                    .to_owned(),
                old_record: current,
                new_record: record.clone(),
            },
        };
        self.write_journal(&journal)?;
        prepared.transfer_cleanup_to_journal();
        self.recover_transaction(&journal, &mut registry)?;
        self.public_record(&record)
    }

    pub fn uninstall(&self, plugin_id: &str) -> Result<InstalledPlugin> {
        validate_plugin_id(plugin_id)?;
        let _lock = self.acquire_lock()?;
        let mut registry = self.load_registry()?;
        let record = registry
            .plugins
            .get(plugin_id)
            .cloned()
            .with_context(|| format!("plugin {plugin_id} 未安装"))?;
        let public = self.public_record(&record)?;
        let target = self.package_path(plugin_id)?;
        validate_installed_directory(&target, &self.packages_directory())?;
        let quarantine = self
            .root
            .join(format!(".uninstall-{plugin_id}-{}", Uuid::new_v4()));
        reject_existing_path(&quarantine, "plugin uninstall quarantine")?;
        let journal = TransactionJournal {
            version: TRANSACTION_VERSION,
            operation: TransactionOperation::Uninstall {
                plugin_id: plugin_id.to_owned(),
                quarantine: quarantine
                    .file_name()
                    .and_then(|name| name.to_str())
                    .context("plugin uninstall quarantine 名称无效")?
                    .to_owned(),
                old_record: record,
            },
        };
        self.write_journal(&journal)?;
        self.recover_transaction(&journal, &mut registry)?;
        Ok(public)
    }

    fn ensure_layout(&self) -> Result<()> {
        ensure_private_directory(&self.root)?;
        reject_symlink_or_non_directory(&self.root, "plugin cache root")?;
        ensure_private_directory(&self.packages_directory())?;
        reject_symlink_or_non_directory(&self.packages_directory(), "plugin packages directory")?;
        ensure_private_directory(&self.staging_directory())?;
        reject_symlink_or_non_directory(&self.staging_directory(), "plugin staging directory")?;
        Ok(())
    }

    fn packages_directory(&self) -> PathBuf {
        self.root.join("packages")
    }

    fn staging_directory(&self) -> PathBuf {
        self.root.join("staging")
    }

    fn registry_path(&self) -> PathBuf {
        self.root.join("installed.json")
    }

    fn transaction_path(&self) -> PathBuf {
        self.root.join(TRANSACTION_FILE)
    }

    fn package_path(&self, plugin_id: &str) -> Result<PathBuf> {
        validate_plugin_id(plugin_id)?;
        Ok(self.packages_directory().join(plugin_id))
    }

    fn public_record(&self, record: &InstalledRecord) -> Result<InstalledPlugin> {
        validate_record(record)?;
        let install_path = self.package_path(&record.id)?;
        validate_installed_directory(&install_path, &self.packages_directory())?;
        Ok(InstalledPlugin {
            id: record.id.clone(),
            version: record.version.clone(),
            source: record.source.clone(),
            sha256: record.sha256.clone(),
            install_path,
            installed_at_ms: record.installed_at_ms,
            updated_at_ms: record.updated_at_ms,
        })
    }

    fn load_registry(&self) -> Result<Registry> {
        if fs::symlink_metadata(&self.root).is_ok() {
            reject_symlink_or_non_directory(&self.root, "plugin cache root")?;
        }
        let path = self.registry_path();
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Registry::default());
            }
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_REGISTRY_BYTES
        {
            bail!("plugin registry 不是普通文件或超过大小限制")
        }
        validate_private_file_mode(&metadata, "plugin registry")?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        let file = open_regular_nofollow(&path)?;
        validate_open_private_file(&path, &file, "plugin registry", false)?;
        file.take(MAX_REGISTRY_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() > MAX_REGISTRY_BYTES as usize {
            bail!("plugin registry 读取时增长到超过限制")
        }
        let registry: Registry =
            serde_json::from_slice(&bytes).context("plugin registry JSON 无效")?;
        validate_registry(&registry)?;
        Ok(registry)
    }

    fn write_registry(&self, registry: &Registry) -> Result<()> {
        validate_registry(registry)?;
        let encoded = serde_json::to_string_pretty(registry)? + "\n";
        if encoded.len() > MAX_REGISTRY_BYTES as usize {
            bail!("plugin registry 超过 {MAX_REGISTRY_BYTES} 字节限制")
        }
        atomic_write_manager_file(
            &self.registry_path(),
            encoded.as_bytes(),
            MAX_REGISTRY_BYTES,
            REGISTRY_TEMP_PREFIX,
        )
    }

    fn acquire_lock(&self) -> Result<PluginLock> {
        self.ensure_layout()?;
        let path = self.root.join("manager.lock");
        let (file, created) = match open_manager_lock(&path, true) {
            Ok(file) => (file, true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                (open_manager_lock(&path, false)?, false)
            }
            Err(error) => return Err(error).context("无法打开 plugin manager lock"),
        };
        if created {
            set_private_file_mode(&file, false)?;
            file.sync_all()?;
            sync_directory(&self.root)?;
        }
        validate_open_private_file(&path, &file, "plugin manager lock", false)?;
        file.try_lock_exclusive()
            .context("另一个 plugin 管理操作正在进行")?;
        if let Err(error) = validate_open_private_file(&path, &file, "plugin manager lock", false) {
            let _ = fs2::FileExt::unlock(&file);
            return Err(error);
        }
        let lock = PluginLock { file };
        if let Err(error) = self.recover_locked() {
            drop(lock);
            return Err(error.context("plugin manager 恢复未完成事务失败"));
        }
        Ok(lock)
    }

    fn write_journal(&self, journal: &TransactionJournal) -> Result<()> {
        validate_journal(journal)?;
        if fs::symlink_metadata(self.transaction_path()).is_ok() {
            bail!("plugin transaction journal 已存在")
        }
        let bytes = serde_json::to_vec(journal)?;
        atomic_write_manager_file(
            &self.transaction_path(),
            &bytes,
            MAX_TRANSACTION_BYTES,
            JOURNAL_TEMP_PREFIX,
        )
    }

    fn load_journal(&self) -> Result<Option<TransactionJournal>> {
        let path = self.transaction_path();
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_TRANSACTION_BYTES
        {
            bail!("plugin transaction journal 不是普通文件或超过大小限制")
        }
        let file = open_regular_nofollow(&path)?;
        validate_open_private_file(&path, &file, "plugin transaction journal", false)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.take(MAX_TRANSACTION_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_TRANSACTION_BYTES as usize {
            bail!("plugin transaction journal 读取时增长到超过限制")
        }
        let journal: TransactionJournal =
            serde_json::from_slice(&bytes).context("plugin transaction journal JSON 无效")?;
        validate_journal(&journal)?;
        Ok(Some(journal))
    }

    fn remove_journal(&self) -> Result<()> {
        let path = self.transaction_path();
        validate_private_regular_path(&path, "plugin transaction journal", false)?;
        fs::remove_file(&path).context("无法删除 plugin transaction journal")?;
        sync_directory(&self.root)
    }

    fn recover_locked(&self) -> Result<()> {
        let residues = self.inspect_layout()?;
        let mut registry = self.load_registry()?;
        if let Some(journal) = self.load_journal()? {
            self.recover_transaction(&journal, &mut registry)?;
        }
        self.reconcile_orphan_residues(&registry, residues)?;
        self.validate_cache(&registry)
    }

    fn recover_transaction(
        &self,
        journal: &TransactionJournal,
        registry: &mut Registry,
    ) -> Result<()> {
        validate_journal(journal)?;
        match &journal.operation {
            TransactionOperation::Install {
                plugin_id,
                stage,
                record,
            } => self.recover_install(plugin_id, stage, record, registry)?,
            TransactionOperation::Update {
                plugin_id,
                stage,
                backup,
                old_record,
                new_record,
            } => self.recover_update(plugin_id, stage, backup, old_record, new_record, registry)?,
            TransactionOperation::Uninstall {
                plugin_id,
                quarantine,
                old_record,
            } => self.recover_uninstall(plugin_id, quarantine, old_record, registry)?,
        }
        self.remove_journal()
    }

    fn recover_install(
        &self,
        plugin_id: &str,
        stage_name: &str,
        record: &InstalledRecord,
        registry: &mut Registry,
    ) -> Result<()> {
        let stage = self.staging_directory().join(stage_name);
        let payload = stage.join("payload");
        let target = self.package_path(plugin_id)?;
        let target_exists = path_exists(&target)?;
        let payload_exists = path_exists(&payload)?;
        match registry.plugins.get(plugin_id) {
            None => match (target_exists, payload_exists) {
                (false, true) => {
                    maybe_fail("install-before-target-rename")?;
                    durable_rename(&payload, &target)?;
                    maybe_crash("install-after-target-rename");
                }
                (true, false) => {}
                _ => bail!("plugin install journal 的 target/payload 状态不一致"),
            },
            Some(current) if current == record => {
                if !target_exists || payload_exists {
                    bail!("已提交 plugin install 的 target/payload 状态不一致")
                }
            }
            Some(_) => bail!("plugin install journal 与 registry record 冲突"),
        }
        self.validate_record_package(&target, record)?;
        if registry.plugins.get(plugin_id) != Some(record) {
            registry
                .plugins
                .insert(plugin_id.to_owned(), record.clone());
            self.write_registry(registry)?;
            maybe_crash("install-after-registry");
        }
        remove_tree_if_present(&stage, &self.staging_directory())?;
        Ok(())
    }

    fn recover_update(
        &self,
        plugin_id: &str,
        stage_name: &str,
        backup_name: &str,
        old_record: &InstalledRecord,
        new_record: &InstalledRecord,
        registry: &mut Registry,
    ) -> Result<()> {
        let stage = self.staging_directory().join(stage_name);
        let payload = stage.join("payload");
        let target = self.package_path(plugin_id)?;
        let backup = self.root.join(backup_name);
        let mut target_exists = path_exists(&target)?;
        let mut payload_exists = path_exists(&payload)?;
        let mut backup_exists = path_exists(&backup)?;
        match registry.plugins.get(plugin_id) {
            Some(current) if current == old_record => {
                match (target_exists, payload_exists, backup_exists) {
                    (true, true, false) => {
                        self.validate_record_package(&target, old_record)?;
                        maybe_fail("update-before-backup-rename")?;
                        durable_rename(&target, &backup)?;
                        target_exists = false;
                        backup_exists = true;
                    }
                    (false, true, true) | (true, false, true) => {}
                    _ => bail!("plugin update journal 的 target/payload/backup 状态不一致"),
                }
                if payload_exists {
                    if target_exists {
                        bail!("plugin update 新 payload 无法覆盖已有 target")
                    }
                    durable_rename(&payload, &target)?;
                    target_exists = true;
                    payload_exists = false;
                    maybe_crash("update-after-target-rename");
                }
                if !target_exists || payload_exists || !backup_exists {
                    bail!("plugin update 前滚后的目录状态不一致")
                }
                self.validate_record_package(&target, new_record)?;
                registry
                    .plugins
                    .insert(plugin_id.to_owned(), new_record.clone());
                self.write_registry(registry)?;
                maybe_crash("update-after-registry");
            }
            Some(current) if current == new_record => {
                if !target_exists || payload_exists {
                    bail!("已提交 plugin update 的 target/payload 状态不一致")
                }
                self.validate_record_package(&target, new_record)?;
            }
            Some(_) => bail!("plugin update journal 与 registry record 冲突"),
            None => bail!("plugin update journal 对应的 registry record 缺失"),
        }
        remove_tree_if_present(&backup, &self.root)?;
        remove_tree_if_present(&stage, &self.staging_directory())?;
        Ok(())
    }

    fn recover_uninstall(
        &self,
        plugin_id: &str,
        quarantine_name: &str,
        old_record: &InstalledRecord,
        registry: &mut Registry,
    ) -> Result<()> {
        let target = self.package_path(plugin_id)?;
        let quarantine = self.root.join(quarantine_name);
        let target_exists = path_exists(&target)?;
        let quarantine_exists = path_exists(&quarantine)?;
        match registry.plugins.get(plugin_id) {
            Some(current) if current == old_record => {
                match (target_exists, quarantine_exists) {
                    (true, false) => {
                        self.validate_record_package(&target, old_record)?;
                        durable_rename(&target, &quarantine)?;
                        maybe_crash("uninstall-after-quarantine-rename");
                    }
                    (false, true) => {}
                    _ => bail!("plugin uninstall journal 的 target/quarantine 状态不一致"),
                }
                registry.plugins.remove(plugin_id);
                self.write_registry(registry)?;
                maybe_crash("uninstall-after-registry");
            }
            None => {
                if target_exists {
                    bail!("已提交 plugin uninstall 仍存在 target")
                }
            }
            Some(_) => bail!("plugin uninstall journal 与 registry record 冲突"),
        }
        remove_tree_if_present(&quarantine, &self.root)?;
        Ok(())
    }

    fn inspect_layout(&self) -> Result<LayoutResidues> {
        reject_symlink_or_non_directory(&self.root, "plugin cache root")?;
        reject_symlink_or_non_directory(&self.packages_directory(), "plugin packages directory")?;
        reject_symlink_or_non_directory(&self.staging_directory(), "plugin staging directory")?;
        let mut residues = LayoutResidues::default();
        let mut usage = TreeUsage::default();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry
                .file_name()
                .to_str()
                .context("plugin cache root 含非 UTF-8 文件名")?
                .to_owned();
            match name.as_str() {
                "packages" | "staging" => {
                    validate_private_directory_exact(&path, "plugin cache directory")?;
                }
                "installed.json" => {
                    validate_private_regular_path(&path, "plugin registry", false)?;
                    if fs::metadata(&path)?.len() > MAX_REGISTRY_BYTES {
                        bail!("plugin registry 超过大小限制")
                    }
                }
                "manager.lock" => {
                    validate_private_regular_path(&path, "plugin manager lock", false)?;
                }
                TRANSACTION_FILE => {
                    validate_private_regular_path(&path, "plugin transaction journal", false)?;
                    if fs::metadata(&path)?.len() > MAX_TRANSACTION_BYTES {
                        bail!("plugin transaction journal 超过大小限制")
                    }
                }
                _ if parse_backup_name(&name).is_some() => {
                    let plugin_id = parse_backup_name(&name).expect("guarded parse");
                    validate_transient_tree(&path, &mut usage)?;
                    residues.backups.push((plugin_id, path));
                }
                _ if parse_quarantine_name(&name).is_some() => {
                    let plugin_id = parse_quarantine_name(&name).expect("guarded parse");
                    validate_transient_tree(&path, &mut usage)?;
                    residues.quarantines.push((plugin_id, path));
                }
                _ if valid_atomic_temp_name(&name) => {
                    validate_private_regular_path(&path, "plugin atomic temp", false)?;
                    let maximum = if name.starts_with(REGISTRY_TEMP_PREFIX) {
                        MAX_REGISTRY_BYTES
                    } else {
                        MAX_TRANSACTION_BYTES
                    };
                    if fs::metadata(&path)?.len() > maximum {
                        bail!("plugin atomic temp 超过对应文件大小限制")
                    }
                    account_transient_file(&path, &mut usage)?;
                    residues.atomic_temps.push(path);
                }
                _ => bail!("plugin cache root 含未知条目: {name}"),
            }
        }
        for entry in fs::read_dir(self.staging_directory())? {
            let entry = entry?;
            let path = entry.path();
            let name = entry
                .file_name()
                .to_str()
                .context("plugin staging directory 含非 UTF-8 文件名")?
                .to_owned();
            if !valid_stage_name(&name) {
                bail!("plugin staging directory 含未知条目: {name}")
            }
            validate_transient_tree(&path, &mut usage)?;
            residues.stages.push(path);
        }
        Ok(residues)
    }

    fn reconcile_orphan_residues(
        &self,
        registry: &Registry,
        residues: LayoutResidues,
    ) -> Result<()> {
        for path in residues.atomic_temps {
            remove_private_file_if_present(&path)?;
        }
        for stage in residues.stages {
            remove_tree_if_present(&stage, &self.staging_directory())?;
        }
        for (plugin_id, backup) in residues.backups {
            if !path_exists(&backup)? {
                continue;
            }
            let record = registry.plugins.get(&plugin_id).with_context(|| {
                format!("orphan plugin backup {plugin_id} 没有对应 registry record")
            })?;
            let target = self.package_path(&plugin_id)?;
            if path_exists(&target)? {
                self.validate_record_package(&target, record)?;
                remove_tree_if_present(&backup, &self.root)?;
            } else {
                self.validate_record_package_at_boundary(&backup, record, &self.root)?;
                durable_rename(&backup, &target)?;
            }
        }
        for (plugin_id, quarantine) in residues.quarantines {
            if !path_exists(&quarantine)? {
                continue;
            }
            let target = self.package_path(&plugin_id)?;
            match registry.plugins.get(&plugin_id) {
                None => remove_tree_if_present(&quarantine, &self.root)?,
                Some(record) if path_exists(&target)? => {
                    self.validate_record_package(&target, record)?;
                    remove_tree_if_present(&quarantine, &self.root)?;
                }
                Some(record) => {
                    self.validate_record_package_at_boundary(&quarantine, record, &self.root)?;
                    durable_rename(&quarantine, &target)?;
                }
            }
        }
        Ok(())
    }

    fn validate_cache(&self, registry: &Registry) -> Result<()> {
        validate_registry(registry)?;
        let packages = self.packages_directory();
        let mut seen = BTreeSet::new();
        let mut total_bytes = 0u64;
        for entry in fs::read_dir(&packages)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .to_str()
                .context("plugin packages directory 含非 UTF-8 文件名")?
                .to_owned();
            validate_plugin_id(&name)?;
            if !seen.insert(name.clone()) {
                bail!("plugin packages directory 含重复 id")
            }
            let record = registry
                .plugins
                .get(&name)
                .with_context(|| format!("plugin package {name} 没有 registry record"))?;
            total_bytes = total_bytes
                .checked_add(self.validate_record_package(&entry.path(), record)?)
                .context("plugin cache 总字节数溢出")?;
            if total_bytes > MAX_PACKAGE_TOTAL_BYTES.saturating_mul(MAX_INSTALLED_PLUGINS as u64) {
                bail!("plugin cache 总字节数超过硬限制")
            }
        }
        if seen.len() != registry.plugins.len() {
            let missing = registry
                .plugins
                .keys()
                .find(|id| !seen.contains(*id))
                .context("plugin registry/package 数量不一致")?;
            bail!("plugin registry record 缺少 package: {missing}")
        }
        Ok(())
    }

    fn validate_record_package(&self, path: &Path, record: &InstalledRecord) -> Result<u64> {
        self.validate_record_package_at_boundary(path, record, &self.packages_directory())
    }

    fn validate_record_package_at_boundary(
        &self,
        path: &Path,
        record: &InstalledRecord,
        boundary: &Path,
    ) -> Result<u64> {
        if path.parent() != Some(boundary) {
            bail!("plugin package 不在预期 cache boundary")
        }
        let bytes = validate_private_package_tree(path)?;
        let (id, version) = validate_payload(path)?;
        if id != record.id || version != record.version {
            bail!("plugin package manifest 与 registry record 不一致")
        }
        Ok(bytes)
    }

    async fn prepare(&self, source: &str, expected_sha256: Option<&str>) -> Result<PreparedPlugin> {
        let source = SourceSpec::parse(source)?;
        let expected = expected_sha256.map(validate_sha256).transpose()?;
        if matches!(source, SourceSpec::Https(_)) && expected.is_none() {
            bail!("HTTPS plugin source 必须提供 --sha256")
        }
        let stage = self
            .staging_directory()
            .join(format!("stage-{}", Uuid::new_v4()));
        ensure_private_directory(&stage)?;
        let payload = stage.join("payload");
        let result = match &source {
            SourceSpec::Directory(path) => {
                let (sha256, stats) = copy_directory_package(path, &payload)?;
                verify_sha256(expected.as_deref(), &sha256)?;
                Ok((sha256, stats))
            }
            SourceSpec::Archive(path) => {
                let bytes = read_local_archive(path)?;
                let sha256 = sha256_hex(&bytes);
                verify_sha256(expected.as_deref(), &sha256)?;
                let stats = extract_zip_package(&bytes, &stage, &payload)?;
                Ok((sha256, stats))
            }
            SourceSpec::Https(url) => {
                let bytes = download_https_archive(url).await?;
                let sha256 = sha256_hex(&bytes);
                verify_sha256(expected.as_deref(), &sha256)?;
                let stats = extract_zip_package(&bytes, &stage, &payload)?;
                Ok((sha256, stats))
            }
        };
        let (sha256, stats) = match result {
            Ok(result) => result,
            Err(error) => {
                if remove_tree_nofollow(&stage).is_ok() {
                    let _ = sync_directory(&self.staging_directory());
                }
                return Err(error);
            }
        };
        if let Err(error) = normalize_private_tree_directories(&payload) {
            if remove_tree_nofollow(&stage).is_ok() {
                let _ = sync_directory(&self.staging_directory());
            }
            return Err(error);
        }
        let identity = match validate_payload(&payload) {
            Ok(identity) => identity,
            Err(error) => {
                if remove_tree_nofollow(&stage).is_ok() {
                    let _ = sync_directory(&self.staging_directory());
                }
                return Err(error);
            }
        };
        sync_tree(&payload)?;
        sync_directory(&stage)?;
        sync_directory(&self.staging_directory())?;
        Ok(PreparedPlugin {
            id: identity.0,
            version: identity.1,
            source: source.persisted(),
            sha256,
            stats,
            stage,
            cleanup_stage: true,
        })
    }
}

struct PluginLock {
    file: File,
}

impl Drop for PluginLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

struct PreparedPlugin {
    id: String,
    version: String,
    source: String,
    sha256: String,
    stats: PackageStats,
    stage: PathBuf,
    cleanup_stage: bool,
}

impl PreparedPlugin {
    fn descriptor(&self) -> ValidatedPlugin {
        ValidatedPlugin {
            id: self.id.clone(),
            version: self.version.clone(),
            source: self.source.clone(),
            sha256: self.sha256.clone(),
            files: self.stats.files,
            bytes: self.stats.bytes,
        }
    }

    fn stage_name(&self) -> Result<String> {
        let name = self
            .stage
            .file_name()
            .and_then(|name| name.to_str())
            .context("plugin staging path 缺少有效名称")?;
        if !valid_stage_name(name) {
            bail!("plugin staging path 名称无效")
        }
        Ok(name.to_owned())
    }

    fn transfer_cleanup_to_journal(&mut self) {
        self.cleanup_stage = false;
    }
}

impl Drop for PreparedPlugin {
    fn drop(&mut self) {
        if !self.cleanup_stage {
            return;
        }
        if remove_tree_nofollow(&self.stage).is_ok() {
            if let Some(parent) = self.stage.parent() {
                let _ = sync_directory(parent);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PackageStats {
    files: usize,
    bytes: u64,
}

enum SourceSpec {
    Directory(PathBuf),
    Archive(PathBuf),
    Https(Url),
}

impl SourceSpec {
    fn parse(value: &str) -> Result<Self> {
        if value.trim().is_empty()
            || value.len() > MAX_SOURCE_BYTES
            || value.contains(['\0', '\n', '\r'])
        {
            bail!("plugin source 为空、过长或包含控制字符")
        }
        if value.contains("://") {
            let url = Url::parse(value).context("plugin source URL 无效")?;
            validate_remote_url(&url, false)?;
            return Ok(Self::Https(url));
        }
        let path = PathBuf::from(value);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("plugin source 不存在: {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("plugin source 不接受 symlink: {}", path.display())
        }
        let canonical = fs::canonicalize(&path)
            .with_context(|| format!("无法解析 plugin source: {}", path.display()))?;
        canonical
            .to_str()
            .context("plugin source canonical path 必须是 UTF-8")?;
        if metadata.is_dir() {
            Ok(Self::Directory(canonical))
        } else if metadata.is_file() {
            Ok(Self::Archive(canonical))
        } else {
            bail!("plugin source 必须是目录、ZIP 普通文件或 HTTPS URL")
        }
    }

    fn persisted(&self) -> String {
        match self {
            Self::Directory(path) | Self::Archive(path) => path.to_string_lossy().into_owned(),
            Self::Https(url) => url.to_string(),
        }
    }
}

pub async fn run_plugin_command(command: PluginCommand) -> Result<()> {
    let manager = PluginManager::user_default()?;
    match command {
        PluginCommand::List { json: output_json } => {
            let installed = manager.list()?;
            if output_json {
                println!("{}", serde_json::to_string_pretty(&installed)?);
            } else if installed.is_empty() {
                println!("No trusted plugins installed.");
            } else {
                for plugin in installed {
                    println!(
                        "{} {} {}",
                        plugin.id,
                        plugin.version,
                        plugin.install_path.display()
                    );
                }
            }
        }
        PluginCommand::Validate { source, sha256 } => {
            let plugin = manager.validate(&source, sha256.as_deref()).await?;
            println!("{}", serde_json::to_string_pretty(&plugin)?);
        }
        PluginCommand::Install { source, sha256 } => {
            let plugin = manager.install(&source, sha256.as_deref()).await?;
            println!("Installed {} {}", plugin.id, plugin.version);
        }
        PluginCommand::Update {
            plugin_id,
            source,
            sha256,
        } => {
            let plugin = manager
                .update(&plugin_id, source.as_deref(), sha256.as_deref())
                .await?;
            println!("Updated {} {}", plugin.id, plugin.version);
        }
        PluginCommand::Uninstall { plugin_id } => {
            let plugin = manager.uninstall(&plugin_id)?;
            println!("Uninstalled {} {}", plugin.id, plugin.version);
        }
    }
    Ok(())
}

pub(crate) fn installed_plugin_directories_default() -> Result<Vec<PathBuf>> {
    let Some(home) = dirs::home_dir() else {
        return Ok(Vec::new());
    };
    let manager = PluginManager::new(home.join(".open-agent-harness/plugins"));
    if fs::symlink_metadata(&manager.root).is_err() {
        return Ok(Vec::new());
    }
    manager
        .list()?
        .into_iter()
        .map(|plugin| Ok(plugin.install_path))
        .collect()
}

fn validate_payload(payload: &Path) -> Result<(String, String)> {
    let settings = Settings {
        raw: json!({"plugins":{"directories":[payload]}}),
    };
    let catalog = PluginCatalog::discover_uninstalled(&settings, payload)
        .context("plugin payload 通过 package 检查但 runtime contribution 校验失败")?;
    let info = catalog
        .plugins()
        .first()
        .context("plugin payload 没有 manifest")?;
    if catalog.plugins().len() != 1 {
        bail!("plugin payload 必须恰好包含一个 plugin")
    }
    validate_plugin_id(&info.name)?;
    let version = info
        .version
        .as_deref()
        .context("可安装 plugin manifest 必须声明 version")?;
    validate_plugin_version(version)?;
    Ok((info.name.clone(), version.to_owned()))
}

fn validate_registry(registry: &Registry) -> Result<()> {
    if registry.schema_version != REGISTRY_SCHEMA_VERSION {
        bail!("不支持的 plugin registry schema version")
    }
    if registry.plugins.len() > MAX_INSTALLED_PLUGINS {
        bail!("plugin registry 超过 {MAX_INSTALLED_PLUGINS} 项限制")
    }
    for (id, record) in &registry.plugins {
        validate_plugin_id(id)?;
        validate_record(record)?;
        if id != &record.id {
            bail!("plugin registry key 与 record id 不一致")
        }
    }
    Ok(())
}

fn validate_record(record: &InstalledRecord) -> Result<()> {
    validate_plugin_id(&record.id)?;
    validate_plugin_version(&record.version)?;
    validate_sha256(&record.sha256)?;
    if record.source.is_empty()
        || record.source.len() > MAX_SOURCE_BYTES
        || record.source.contains(['\0', '\n', '\r'])
    {
        bail!("plugin registry source 无效")
    }
    if record.source.contains("://") {
        let url = Url::parse(&record.source).context("plugin registry source URL 无效")?;
        validate_remote_url(&url, false)?;
    } else if !Path::new(&record.source).is_absolute() {
        bail!("plugin registry local source 必须是绝对路径")
    }
    if record.installed_at_ms == 0
        || record.updated_at_ms == 0
        || record.updated_at_ms < record.installed_at_ms
    {
        bail!("plugin registry timestamp 无效")
    }
    Ok(())
}

fn validate_journal(journal: &TransactionJournal) -> Result<()> {
    if journal.version != TRANSACTION_VERSION {
        bail!("plugin transaction journal version 不受支持")
    }
    match &journal.operation {
        TransactionOperation::Install {
            plugin_id,
            stage,
            record,
        } => {
            validate_plugin_id(plugin_id)?;
            validate_record(record)?;
            if record.id != *plugin_id || !valid_stage_name(stage) {
                bail!("plugin install journal identity 无效")
            }
        }
        TransactionOperation::Update {
            plugin_id,
            stage,
            backup,
            old_record,
            new_record,
        } => {
            validate_plugin_id(plugin_id)?;
            validate_record(old_record)?;
            validate_record(new_record)?;
            if old_record.id != *plugin_id
                || new_record.id != *plugin_id
                || !valid_stage_name(stage)
                || parse_backup_name(backup).as_deref() != Some(plugin_id)
            {
                bail!("plugin update journal identity 无效")
            }
        }
        TransactionOperation::Uninstall {
            plugin_id,
            quarantine,
            old_record,
        } => {
            validate_plugin_id(plugin_id)?;
            validate_record(old_record)?;
            if old_record.id != *plugin_id
                || parse_quarantine_name(quarantine).as_deref() != Some(plugin_id)
            {
                bail!("plugin uninstall journal identity 无效")
            }
        }
    }
    Ok(())
}

fn valid_stage_name(value: &str) -> bool {
    value
        .strip_prefix("stage-")
        .and_then(|suffix| Uuid::parse_str(suffix).ok())
        .is_some()
}

fn parse_backup_name(value: &str) -> Option<String> {
    parse_plugin_uuid_name(value, ".backup-")
}

fn parse_quarantine_name(value: &str) -> Option<String> {
    parse_plugin_uuid_name(value, ".uninstall-")
}

fn parse_plugin_uuid_name(value: &str, prefix: &str) -> Option<String> {
    let suffix = value.strip_prefix(prefix)?;
    let split = suffix.len().checked_sub(37)?;
    if suffix.as_bytes().get(split) != Some(&b'-') {
        return None;
    }
    let (plugin_id, uuid) = suffix.split_at(split);
    let uuid = uuid.strip_prefix('-')?;
    validate_plugin_id(plugin_id).ok()?;
    Uuid::parse_str(uuid).ok()?;
    Some(plugin_id.to_owned())
}

fn valid_atomic_temp_name(value: &str) -> bool {
    [REGISTRY_TEMP_PREFIX, JOURNAL_TEMP_PREFIX]
        .into_iter()
        .any(|prefix| {
            value
                .strip_prefix(prefix)
                .and_then(|suffix| suffix.strip_suffix(".tmp"))
                .and_then(|uuid| Uuid::parse_str(uuid).ok())
                .is_some()
        })
}

fn validate_plugin_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 48
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!("plugin id 无效: {value}")
    }
    Ok(())
}

fn validate_plugin_version(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-'))
    {
        bail!("plugin version 无效: {value}")
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<String> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("SHA-256 必须是 64 位十六进制字符串")
    }
    Ok(value.to_ascii_lowercase())
}

fn verify_sha256(expected: Option<&str>, actual: &str) -> Result<()> {
    if expected.is_some_and(|expected| expected != actual) {
        bail!("plugin archive SHA-256 校验失败")
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("系统时间超过 u64 millisecond 范围")
}

fn copy_directory_package(source: &Path, destination: &Path) -> Result<(String, PackageStats)> {
    let source_metadata = fs::symlink_metadata(source)?;
    if source_metadata.file_type().is_symlink() || !source_metadata.is_dir() {
        bail!("plugin directory source 必须是非 symlink 目录")
    }
    let source = fs::canonicalize(source)?;
    if let Some(parent) = destination
        .parent()
        .and_then(|path| fs::canonicalize(path).ok())
    {
        if parent.starts_with(&source) {
            bail!("plugin staging directory 不能位于 source 内部")
        }
    }
    let mut entries = Vec::new();
    for entry in WalkDir::new(&source).follow_links(false) {
        let entry =
            entry.with_context(|| format!("无法遍历 plugin source {}", source.display()))?;
        if entry.path() == source {
            continue;
        }
        let relative = entry.path().strip_prefix(&source)?;
        validate_relative_package_path(relative)?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            bail!("plugin package 不接受 symlink: {}", relative.display())
        }
        if !metadata.is_dir() && !metadata.is_file() {
            bail!(
                "plugin package 不接受设备、socket 或其他特殊文件: {}",
                relative.display()
            )
        }
        if metadata.is_file() {
            validate_regular_source_metadata(&metadata, relative)?;
        }
        entries.push((relative.to_owned(), metadata.is_dir()));
        if entries.len() > MAX_PACKAGE_FILES.saturating_mul(2) {
            bail!("plugin package entry 数量超过限制")
        }
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    ensure_private_directory(destination)?;
    let mut stats = PackageStats::default();
    let mut digest = Sha256::new();
    for (relative, directory) in entries {
        let target = destination.join(&relative);
        if directory {
            ensure_private_directory(&target)?;
            digest.update(b"D\0");
            update_digest_path(&mut digest, &relative)?;
            continue;
        }
        stats.files = stats
            .files
            .checked_add(1)
            .context("plugin file count 溢出")?;
        if stats.files > MAX_PACKAGE_FILES {
            bail!("plugin package 文件超过 {MAX_PACKAGE_FILES} 个限制")
        }
        let source_file = source.join(&relative);
        let mut input = open_regular_nofollow(&source_file)?;
        let metadata = input.metadata()?;
        validate_regular_source_metadata(&metadata, &relative)?;
        let executable = regular_file_is_executable(&metadata);
        stats.bytes = stats
            .bytes
            .checked_add(metadata.len())
            .context("plugin bytes 溢出")?;
        if stats.bytes > MAX_PACKAGE_TOTAL_BYTES {
            bail!("plugin package 总大小超过 {MAX_PACKAGE_TOTAL_BYTES} 字节限制")
        }
        let mut output = create_private_file(&target)?;
        digest.update(b"F\0");
        update_digest_path(&mut digest, &relative)?;
        digest.update([u8::from(executable)]);
        digest.update(metadata.len().to_le_bytes());
        let mut remaining = metadata.len();
        let mut buffer = [0u8; 64 * 1024];
        while remaining > 0 {
            let maximum = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
            let read = input.read(&mut buffer[..maximum])?;
            if read == 0 {
                bail!("plugin source 文件读取时缩短: {}", relative.display())
            }
            output.write_all(&buffer[..read])?;
            digest.update(&buffer[..read]);
            remaining -= read as u64;
        }
        let mut extra = [0u8; 1];
        if input.read(&mut extra)? != 0 {
            bail!("plugin source 文件读取时增长: {}", relative.display())
        }
        set_private_file_mode(&output, executable)?;
        output.sync_all()?;
    }
    Ok((format!("{:x}", digest.finalize()), stats))
}

fn validate_regular_source_metadata(metadata: &fs::Metadata, relative: &Path) -> Result<()> {
    if !metadata.is_file() || metadata.len() > MAX_PACKAGE_FILE_BYTES {
        bail!(
            "plugin file 不是普通文件或超过 {MAX_PACKAGE_FILE_BYTES} 字节: {}",
            relative.display()
        )
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            bail!("plugin package 不接受 hardlink: {}", relative.display())
        }
    }
    Ok(())
}

fn update_digest_path(digest: &mut Sha256, relative: &Path) -> Result<()> {
    let path = relative
        .to_str()
        .context("plugin package 路径必须是 UTF-8")?;
    digest.update((path.len() as u64).to_le_bytes());
    digest.update(path.as_bytes());
    Ok(())
}

fn read_local_archive(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_ARCHIVE_BYTES as u64
    {
        bail!("plugin ZIP 不是普通文件或超过 {MAX_ARCHIVE_BYTES} 字节限制")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            bail!("plugin ZIP source 不接受 hardlink")
        }
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    open_regular_nofollow(path)?
        .take(MAX_ARCHIVE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_ARCHIVE_BYTES {
        bail!("plugin ZIP 读取时增长到超过限制")
    }
    Ok(bytes)
}

fn extract_zip_package(bytes: &[u8], stage: &Path, payload: &Path) -> Result<PackageStats> {
    let extracted = stage.join("extracted");
    ensure_private_directory(&extracted)?;
    let mut archive = ZipArchive::new(Cursor::new(bytes)).context("plugin archive 不是有效 ZIP")?;
    if archive.len() > MAX_PACKAGE_FILES.saturating_mul(2) {
        bail!("plugin ZIP entry 数量超过限制")
    }
    let mut seen = BTreeSet::new();
    let mut stats = PackageStats::default();
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .context("无法读取 plugin ZIP entry")?;
        let name = entry.name();
        if name.is_empty() || name.contains(['\0', '\\']) {
            bail!("plugin ZIP entry 名称无效")
        }
        let relative = entry
            .enclosed_name()
            .context("plugin ZIP entry 使用绝对路径或 ..")?;
        validate_relative_package_path(&relative)?;
        if !seen.insert(relative.clone()) {
            bail!("plugin ZIP 包含重复路径: {}", relative.display())
        }
        let directory = entry.is_dir();
        if entry.is_symlink() {
            bail!(
                "plugin ZIP 不接受 symlink、hardlink 或特殊文件: {}",
                relative.display()
            )
        }
        if let Some(mode) = entry.unix_mode() {
            let kind = mode & 0o170000;
            let allowed = if directory {
                kind == 0 || kind == 0o040000
            } else {
                kind == 0 || kind == 0o100000
            };
            if !allowed {
                bail!(
                    "plugin ZIP 不接受 symlink、hardlink 或特殊文件: {}",
                    relative.display()
                )
            }
        }
        let target = extracted.join(&relative);
        if directory {
            ensure_private_directory(&target)?;
            continue;
        }
        let executable = entry.unix_mode().is_some_and(|mode| mode & 0o111 != 0);
        if entry.size() > MAX_PACKAGE_FILE_BYTES {
            bail!("plugin ZIP 单文件超过 {MAX_PACKAGE_FILE_BYTES} 字节限制")
        }
        stats.files = stats
            .files
            .checked_add(1)
            .context("plugin ZIP file count 溢出")?;
        if stats.files > MAX_PACKAGE_FILES {
            bail!("plugin ZIP 文件超过 {MAX_PACKAGE_FILES} 个限制")
        }
        let mut output = create_private_file(&target)?;
        let mut written = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = entry.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            written = written
                .checked_add(read as u64)
                .context("plugin ZIP bytes 溢出")?;
            if written > MAX_PACKAGE_FILE_BYTES {
                bail!("plugin ZIP 解压文件增长到超过单文件限制")
            }
            stats.bytes = stats
                .bytes
                .checked_add(read as u64)
                .context("plugin ZIP bytes 溢出")?;
            if stats.bytes > MAX_PACKAGE_TOTAL_BYTES {
                bail!("plugin ZIP 解压总大小超过 {MAX_PACKAGE_TOTAL_BYTES} 字节限制")
            }
            output.write_all(&buffer[..read])?;
        }
        if written != entry.size() {
            bail!("plugin ZIP entry 声明大小与解压结果不一致")
        }
        set_private_file_mode(&output, executable)?;
        output.sync_all()?;
    }
    normalize_extracted_root(&extracted, payload)?;
    Ok(stats)
}

fn normalize_extracted_root(extracted: &Path, payload: &Path) -> Result<()> {
    if extracted.join("plugin.json").is_file() {
        fs::rename(extracted, payload)?;
        return Ok(());
    }
    let mut entries = fs::read_dir(extracted)?.collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    if entries.len() != 1 {
        bail!("plugin ZIP 必须在根目录或唯一顶层目录中包含 plugin.json")
    }
    let root = entries.remove(0).path();
    let metadata = fs::symlink_metadata(&root)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || !root.join("plugin.json").is_file()
    {
        bail!("plugin ZIP 唯一顶层目录缺少 plugin.json")
    }
    fs::rename(&root, payload)?;
    fs::remove_dir(extracted)?;
    Ok(())
}

fn validate_relative_package_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().count() > MAX_PACKAGE_DEPTH
        || path.components().any(|component| {
            matches!(
                component,
                Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        bail!(
            "plugin package path 必须是有界安全相对路径: {}",
            path.display()
        )
    }
    for component in path.components() {
        let Component::Normal(value) = component else {
            continue;
        };
        let value = value.to_str().context("plugin package path 必须是 UTF-8")?;
        if value.is_empty()
            || value == "."
            || value.ends_with(['.', ' '])
            || value.contains(['\0', '\\'])
            || is_windows_reserved_name(value)
        {
            bail!("plugin package path component 无效: {value}")
        }
    }
    Ok(())
}

fn is_windows_reserved_name(value: &str) -> bool {
    let stem = value
        .split('.')
        .next()
        .unwrap_or(value)
        .to_ascii_uppercase();
    matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$" | "CONIN$" | "CONOUT$"
    ) || stem
        .strip_prefix("COM")
        .or_else(|| stem.strip_prefix("LPT"))
        .is_some_and(|suffix| {
            suffix.len() == 1 && suffix.as_bytes()[0].is_ascii_digit() && suffix != "0"
        })
}

async fn download_https_archive(url: &Url) -> Result<Vec<u8>> {
    download_archive_with_policy(url, false, false).await
}

async fn download_archive_with_policy(
    initial: &Url,
    allow_private_network: bool,
    allow_http_for_test: bool,
) -> Result<Vec<u8>> {
    let mut current = initial.clone();
    for redirect in 0..=MAX_REDIRECTS {
        validate_remote_url(&current, allow_http_for_test)?;
        let client = secure_client_for_url(&current, allow_private_network).await?;
        let response = client
            .get(current.clone())
            .send()
            .await
            .context("plugin HTTPS download 失败")?;
        if response.status().is_redirection() {
            if redirect == MAX_REDIRECTS {
                bail!("plugin HTTPS redirect 超过 {MAX_REDIRECTS} 次限制")
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .context("plugin HTTPS redirect 缺少 Location")?
                .to_str()
                .context("plugin HTTPS redirect Location 无效")?;
            let next = current
                .join(location)
                .context("plugin HTTPS redirect URL 无效")?;
            validate_remote_url(&next, allow_http_for_test)?;
            if current.scheme() == "https" && next.scheme() != "https" {
                bail!("plugin download 拒绝 HTTPS 降级 redirect")
            }
            current = next;
            continue;
        }
        if !response.status().is_success() {
            bail!("plugin HTTPS download HTTP {}", response.status().as_u16())
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_ARCHIVE_BYTES as u64)
        {
            bail!("plugin HTTPS archive 超过 {MAX_ARCHIVE_BYTES} 字节限制")
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("读取 plugin HTTPS archive 失败")?;
            if bytes.len().saturating_add(chunk.len()) > MAX_ARCHIVE_BYTES {
                bail!("plugin HTTPS archive 超过 {MAX_ARCHIVE_BYTES} 字节限制")
            }
            bytes.extend_from_slice(&chunk);
        }
        return Ok(bytes);
    }
    unreachable!("redirect loop either returns or fails")
}

fn validate_remote_url(url: &Url, allow_http_for_test: bool) -> Result<()> {
    if url.scheme() != "https" && !(allow_http_for_test && url.scheme() == "http") {
        bail!("远程 plugin source 只允许无凭据 HTTPS URL")
    }
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str().len() > MAX_SOURCE_BYTES
    {
        bail!("远程 plugin URL 缺少 host、包含凭据/query/fragment 或过长")
    }
    Ok(())
}

fn open_manager_lock(path: &Path, create_new: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn atomic_write_manager_file(
    path: &Path,
    bytes: &[u8],
    maximum: u64,
    temp_prefix: &str,
) -> Result<()> {
    if bytes.len() as u64 > maximum {
        bail!("plugin manager private file 超过大小限制")
    }
    let parent = path
        .parent()
        .context("plugin manager private file 缺少父目录")?;
    validate_private_directory_exact(parent, "plugin manager private file parent")?;
    match fs::symlink_metadata(path) {
        Ok(_) => validate_private_regular_path(path, "plugin manager private file", false)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let temporary = parent.join(format!("{temp_prefix}{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut output = create_private_file(&temporary)?;
        output.write_all(bytes)?;
        set_private_file_mode(&output, false)?;
        output.sync_all()?;
        fs::rename(&temporary, path).with_context(|| {
            format!(
                "无法原子替换 plugin manager private file {}",
                path.display()
            )
        })?;
        validate_open_private_file(path, &output, "plugin manager private file", false)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn durable_rename(source: &Path, target: &Path) -> Result<()> {
    let source_parent = source.parent().context("rename source 缺少父目录")?;
    let target_parent = target.parent().context("rename target 缺少父目录")?;
    reject_existing_path(target, "durable rename target")?;
    fs::rename(source, target).with_context(|| {
        format!(
            "无法持久 rename {} -> {}",
            source.display(),
            target.display()
        )
    })?;
    sync_directory(source_parent)?;
    if source_parent != target_parent {
        sync_directory(target_parent)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .with_context(|| format!("无法打开目录进行 fsync: {}", path.display()))?
            .sync_all()
            .with_context(|| format!("无法 fsync 目录: {}", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn sync_tree(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("拒绝 fsync symlink plugin tree")
    }
    if metadata.is_file() {
        open_regular_nofollow(path)?.sync_all()?;
        return Ok(());
    }
    if !metadata.is_dir() {
        bail!("plugin tree 含特殊文件")
    }
    for entry in fs::read_dir(path)? {
        sync_tree(&entry?.path())?;
    }
    sync_directory(path)
}

fn normalize_private_tree_directories(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("plugin payload root 必须是非 symlink 目录")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            bail!("plugin payload 不接受 symlink")
        }
        if metadata.is_dir() {
            normalize_private_tree_directories(&entry.path())?;
        } else if !metadata.is_file() {
            bail!("plugin payload 不接受特殊文件")
        }
    }
    Ok(())
}

fn path_exists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("plugin transaction path 不能是 symlink: {}", path.display())
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn remove_private_file_if_present(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            validate_private_regular_path(path, "plugin private temporary file", false)?;
            let parent = path.parent().context("plugin private temp 缺少父目录")?;
            fs::remove_file(path)?;
            sync_directory(parent)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_tree_if_present(path: &Path, boundary: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            validate_transient_tree(path, &mut TreeUsage::default())?;
            remove_validated_tree(path, boundary)?;
            sync_directory(boundary)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_private_package_tree(path: &Path) -> Result<u64> {
    let mut usage = TreeUsage::default();
    inspect_private_tree(
        path,
        path,
        0,
        MAX_PACKAGE_FILES.saturating_mul(2),
        MAX_PACKAGE_TOTAL_BYTES,
        &mut usage,
    )?;
    Ok(usage.bytes)
}

fn validate_transient_tree(path: &Path, aggregate: &mut TreeUsage) -> Result<()> {
    let mut usage = TreeUsage::default();
    inspect_private_tree(
        path,
        path,
        0,
        MAX_TRANSIENT_ENTRIES,
        MAX_TRANSIENT_BYTES,
        &mut usage,
    )?;
    aggregate.entries = aggregate
        .entries
        .checked_add(usage.entries)
        .context("plugin transient entry count 溢出")?;
    aggregate.bytes = aggregate
        .bytes
        .checked_add(usage.bytes)
        .context("plugin transient bytes 溢出")?;
    if aggregate.entries > MAX_TRANSIENT_ENTRIES || aggregate.bytes > MAX_TRANSIENT_BYTES {
        bail!("plugin transient storage 超过 entry/byte 硬限制")
    }
    Ok(())
}

fn inspect_private_tree(
    root: &Path,
    path: &Path,
    depth: usize,
    max_entries: usize,
    max_bytes: u64,
    usage: &mut TreeUsage,
) -> Result<()> {
    if depth > MAX_PACKAGE_DEPTH.saturating_add(4) {
        bail!("plugin private tree 深度超过限制")
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("plugin private tree 不接受 symlink: {}", path.display())
    }
    let canonical_root = fs::canonicalize(root)?;
    let canonical_path = fs::canonicalize(path)?;
    if !canonical_path.starts_with(&canonical_root) {
        bail!("plugin private tree 越过 storage boundary")
    }
    usage.entries = usage
        .entries
        .checked_add(1)
        .context("plugin private tree entry count 溢出")?;
    if usage.entries > max_entries {
        bail!("plugin private tree entry count 超过限制")
    }
    if metadata.is_dir() {
        validate_private_directory_exact(path, "plugin private tree directory")?;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let relative = entry.path().strip_prefix(root)?.to_owned();
            if !relative.as_os_str().is_empty() {
                validate_relative_package_path(&relative)?;
            }
            inspect_private_tree(
                root,
                &entry.path(),
                depth + 1,
                max_entries,
                max_bytes,
                usage,
            )?;
        }
        return Ok(());
    }
    if !metadata.is_file() || metadata.len() > MAX_PACKAGE_FILE_BYTES {
        bail!("plugin private tree 含特殊文件或超大文件")
    }
    validate_private_regular_metadata(&metadata, "plugin private tree file", true)?;
    usage.bytes = usage
        .bytes
        .checked_add(metadata.len())
        .context("plugin private tree bytes 溢出")?;
    if usage.bytes > max_bytes {
        bail!("plugin private tree bytes 超过限制")
    }
    Ok(())
}

fn account_transient_file(path: &Path, usage: &mut TreeUsage) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    validate_private_regular_metadata(&metadata, "plugin atomic temp", false)?;
    usage.entries = usage
        .entries
        .checked_add(1)
        .context("plugin transient entry count 溢出")?;
    usage.bytes = usage
        .bytes
        .checked_add(metadata.len())
        .context("plugin transient bytes 溢出")?;
    if usage.entries > MAX_TRANSIENT_ENTRIES || usage.bytes > MAX_TRANSIENT_BYTES {
        bail!("plugin transient storage 超过 entry/byte 硬限制")
    }
    Ok(())
}

fn open_regular_nofollow(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("无法安全打开普通文件 {}", path.display()))?;
    if !file.metadata()?.is_file() {
        bail!("路径不是普通文件: {}", path.display())
    }
    Ok(file)
}

fn create_private_file(path: &Path) -> Result<File> {
    let parent = path.parent().context("plugin file 缺少父目录")?;
    ensure_private_directory(parent)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("无法创建私有 plugin file {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

#[cfg(unix)]
fn regular_file_is_executable(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn regular_file_is_executable(_metadata: &fs::Metadata) -> bool {
    false
}

fn set_private_file_mode(file: &File, executable: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        let mode = if executable { 0o700 } else { 0o600 };
        file.set_permissions(fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    let _ = (file, executable);
    Ok(())
}

fn reject_existing_path(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => bail!("{label} 已存在: {}", path.display()),
        Err(error) => Err(error.into()),
    }
}

fn reject_symlink_or_non_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} 必须是非 symlink 目录")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

fn validate_private_file_mode(metadata: &fs::Metadata, label: &str) -> Result<()> {
    #[cfg(not(unix))]
    let _ = (metadata, label);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("{label} 权限必须不宽于 0600")
        }
    }
    Ok(())
}

fn validate_private_directory_exact(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} 不存在: {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} 必须是非 symlink 目录")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        if metadata.permissions().mode() & 0o777 != 0o700 {
            bail!("{label} 权限必须为 0700")
        }
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!("{label} owner 与当前用户不匹配")
        }
    }
    Ok(())
}

fn validate_private_regular_metadata(
    metadata: &fs::Metadata,
    label: &str,
    executable_allowed: bool,
) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("{label} 必须是非 symlink 普通文件")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 && !(executable_allowed && mode == 0o700) {
            bail!("{label} 权限必须为 0600 或受控的 0700 executable")
        }
        if metadata.nlink() != 1 {
            bail!("{label} 不接受 hardlink")
        }
        // SAFETY: geteuid has no preconditions.
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!("{label} owner 与当前用户不匹配")
        }
    }
    #[cfg(not(unix))]
    let _ = executable_allowed;
    Ok(())
}

fn validate_private_regular_path(path: &Path, label: &str, executable_allowed: bool) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    validate_private_regular_metadata(&metadata, label, executable_allowed)?;
    let file = open_regular_nofollow(path)?;
    validate_open_private_file(path, &file, label, executable_allowed)
}

fn validate_open_private_file(
    path: &Path,
    file: &File,
    label: &str,
    executable_allowed: bool,
) -> Result<()> {
    let opened = file.metadata()?;
    validate_private_regular_metadata(&opened, label, executable_allowed)?;
    let current = fs::symlink_metadata(path)?;
    validate_private_regular_metadata(&current, label, executable_allowed)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if opened.dev() != current.dev() || opened.ino() != current.ino() {
            bail!("{label} path 在打开期间被替换")
        }
    }
    Ok(())
}

fn validate_installed_directory(path: &Path, packages: &Path) -> Result<()> {
    validate_plugin_id(
        path.file_name()
            .and_then(|name| name.to_str())
            .context("plugin install path 缺少有效 id")?,
    )?;
    if path.parent() != Some(packages) {
        bail!("plugin install path 不在 packages 直接子目录")
    }
    validate_private_directory_exact(packages, "plugin packages directory")?;
    validate_private_directory_exact(path, "installed plugin directory")?;
    let packages = fs::canonicalize(packages)?;
    let canonical = fs::canonicalize(path)?;
    if canonical.parent() != Some(packages.as_path()) {
        bail!("installed plugin directory 越过 cache boundary")
    }
    Ok(())
}

fn remove_validated_tree(path: &Path, boundary: &Path) -> Result<()> {
    if path.parent() != Some(boundary) {
        bail!("拒绝删除 cache boundary 外路径")
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("拒绝删除 symlink 或非目录 plugin tree")
    }
    remove_tree_nofollow(path)
}

fn remove_tree_nofollow(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        let metadata = fs::symlink_metadata(&child)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            remove_tree_nofollow(&child)?;
        } else {
            fs::remove_file(&child)?;
        }
    }
    fs::remove_dir(path)?;
    Ok(())
}

fn maybe_crash(point: &str) {
    #[cfg(test)]
    if std::env::var("OPEN_AGENT_HARNESS_PLUGIN_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(86);
    }
    #[cfg(not(test))]
    let _ = point;
}

fn maybe_fail(point: &str) -> Result<()> {
    #[cfg(test)]
    if std::env::var("OPEN_AGENT_HARNESS_PLUGIN_FAIL_POINT").as_deref() == Ok(point) {
        bail!("injected plugin transaction failure at {point}")
    }
    #[cfg(not(test))]
    let _ = point;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read as _, Write as _},
        net::TcpListener,
        process::Command,
        thread,
    };

    use super::*;
    use zip::write::SimpleFileOptions;

    fn write_plugin(root: &Path, id: &str, version: &str, marker: &str) {
        fs::create_dir_all(root.join("commands")).unwrap();
        fs::write(
            root.join("plugin.json"),
            serde_json::json!({
                "name":id,
                "version":version,
                "description":"test plugin",
                "commands":["commands"]
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            root.join("commands/check.md"),
            format!("---\ndescription: Check\n---\n{marker}"),
        )
        .unwrap();
    }

    fn zip_bytes(entries: &[(&str, &[u8], Option<u32>)]) -> Vec<u8> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        for (name, content, mode) in entries {
            let mut options = SimpleFileOptions::default();
            if let Some(mode) = mode {
                options = options.unix_permissions(*mode);
            }
            writer.start_file(*name, options).unwrap();
            writer.write_all(content).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    #[ignore = "subprocess crash-point worker"]
    fn plugin_crash_worker() {
        let Ok(operation) = std::env::var("OPEN_AGENT_HARNESS_PLUGIN_WORKER") else {
            return;
        };
        let root = PathBuf::from(std::env::var("OPEN_AGENT_HARNESS_PLUGIN_ROOT").unwrap());
        let source = PathBuf::from(std::env::var("OPEN_AGENT_HARNESS_PLUGIN_SOURCE").unwrap());
        let manager = PluginManager::new(root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        match operation.as_str() {
            "install" => runtime
                .block_on(manager.install(source.to_str().unwrap(), None))
                .unwrap(),
            "update" => runtime
                .block_on(manager.update("quality", Some(source.to_str().unwrap()), None))
                .unwrap(),
            "uninstall" => manager.uninstall("quality").unwrap(),
            other => panic!("unknown plugin crash worker operation: {other}"),
        };
        panic!("plugin crash point was not reached")
    }

    #[test]
    #[ignore = "subprocess recoverable-failure worker"]
    fn plugin_recoverable_failure_worker() {
        let root = PathBuf::from(std::env::var("OPEN_AGENT_HARNESS_PLUGIN_ROOT").unwrap());
        let source = PathBuf::from(std::env::var("OPEN_AGENT_HARNESS_PLUGIN_SOURCE").unwrap());
        let operation = std::env::var("OPEN_AGENT_HARNESS_PLUGIN_WORKER").unwrap();
        let manager = PluginManager::new(root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = match operation.as_str() {
            "install" => runtime
                .block_on(manager.install(source.to_str().unwrap(), None))
                .map(|_| ()),
            "update" => runtime
                .block_on(manager.update("quality", Some(source.to_str().unwrap()), None))
                .map(|_| ()),
            other => panic!("unknown plugin failure worker operation: {other}"),
        };
        let error = result.unwrap_err().to_string();
        assert!(error.contains("injected plugin transaction failure"));
        assert!(manager.transaction_path().is_file());
        assert_eq!(
            fs::read_dir(manager.staging_directory()).unwrap().count(),
            1,
            "PreparedPlugin::Drop must not remove journal-owned payload"
        );
    }

    fn run_crash_worker(operation: &str, point: &str, root: &Path, source: &Path) {
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("plugin_manager::tests::plugin_crash_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env("OPEN_AGENT_HARNESS_PLUGIN_WORKER", operation)
            .env("OPEN_AGENT_HARNESS_PLUGIN_CRASH_POINT", point)
            .env("OPEN_AGENT_HARNESS_PLUGIN_ROOT", root)
            .env("OPEN_AGENT_HARNESS_PLUGIN_SOURCE", source)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "worker did not crash at {point}");
    }

    fn run_failure_worker(operation: &str, point: &str, root: &Path, source: &Path) {
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("plugin_manager::tests::plugin_recoverable_failure_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env("OPEN_AGENT_HARNESS_PLUGIN_WORKER", operation)
            .env("OPEN_AGENT_HARNESS_PLUGIN_FAIL_POINT", point)
            .env("OPEN_AGENT_HARNESS_PLUGIN_ROOT", root)
            .env("OPEN_AGENT_HARNESS_PLUGIN_SOURCE", source)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "failure worker did not stop cleanly at {point}"
        );
    }

    fn assert_no_transaction_residue(manager: &PluginManager) {
        assert!(!manager.transaction_path().exists());
        assert_eq!(
            fs::read_dir(manager.staging_directory()).unwrap().count(),
            0
        );
        for entry in fs::read_dir(&manager.root).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().into_owned();
            assert!(!name.starts_with(".backup-"), "backup residue: {name}");
            assert!(
                !name.starts_with(".uninstall-"),
                "quarantine residue: {name}"
            );
            assert!(
                !name.starts_with(REGISTRY_TEMP_PREFIX) && !name.starts_with(JOURNAL_TEMP_PREFIX),
                "atomic temp residue: {name}"
            );
        }
    }

    fn write_private_test_file(path: &Path, bytes: &[u8]) {
        let mut file = create_private_file(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

    #[tokio::test]
    async fn subprocess_crash_points_reconcile_install_update_and_uninstall() {
        let temp = tempfile::tempdir().unwrap();
        for point in ["install-after-target-rename", "install-after-registry"] {
            let case = temp.path().join(point);
            let source = case.join("source");
            let cache = case.join("cache");
            write_plugin(&source, "quality", "1.0.0", "INSTALL_RECOVERED");
            run_crash_worker("install", point, &cache, &source);
            let manager = PluginManager::new(cache);
            let installed = manager.list().unwrap();
            assert_eq!(installed.len(), 1);
            assert_eq!(installed[0].version, "1.0.0");
            assert!(
                fs::read_to_string(installed[0].install_path.join("commands/check.md"))
                    .unwrap()
                    .contains("INSTALL_RECOVERED")
            );
            assert_no_transaction_residue(&manager);
        }

        for point in ["update-after-target-rename", "update-after-registry"] {
            let case = temp.path().join(point);
            let source = case.join("source");
            let cache = case.join("cache");
            write_plugin(&source, "quality", "1.0.0", "VERSION_ONE");
            let manager = PluginManager::new(cache.clone());
            manager
                .install(source.to_str().unwrap(), None)
                .await
                .unwrap();
            write_plugin(&source, "quality", "2.0.0", "VERSION_TWO");
            run_crash_worker("update", point, &cache, &source);
            let installed = manager.list().unwrap();
            assert_eq!(installed.len(), 1);
            assert_eq!(installed[0].version, "2.0.0");
            assert!(
                fs::read_to_string(installed[0].install_path.join("commands/check.md"))
                    .unwrap()
                    .contains("VERSION_TWO")
            );
            assert_no_transaction_residue(&manager);
        }

        for point in [
            "uninstall-after-quarantine-rename",
            "uninstall-after-registry",
        ] {
            let case = temp.path().join(point);
            let source = case.join("source");
            let cache = case.join("cache");
            write_plugin(&source, "quality", "1.0.0", "REMOVE_ME");
            let manager = PluginManager::new(cache.clone());
            manager
                .install(source.to_str().unwrap(), None)
                .await
                .unwrap();
            run_crash_worker("uninstall", point, &cache, &source);
            assert!(manager.list().unwrap().is_empty());
            assert_eq!(
                fs::read_dir(manager.packages_directory()).unwrap().count(),
                0
            );
            assert_no_transaction_residue(&manager);
        }
    }

    #[tokio::test]
    async fn journal_owns_staging_after_recoverable_install_and_update_failures() {
        let temp = tempfile::tempdir().unwrap();

        let install_case = temp.path().join("install");
        let install_source = install_case.join("source");
        let install_cache = install_case.join("cache");
        write_plugin(&install_source, "quality", "1.0.0", "INSTALL_AFTER_ERROR");
        run_failure_worker(
            "install",
            "install-before-target-rename",
            &install_cache,
            &install_source,
        );
        let install_manager = PluginManager::new(install_cache);
        let installed = install_manager.list().unwrap();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].version, "1.0.0");
        assert_no_transaction_residue(&install_manager);

        let update_case = temp.path().join("update");
        let update_source = update_case.join("source");
        let update_cache = update_case.join("cache");
        write_plugin(&update_source, "quality", "1.0.0", "VERSION_ONE");
        let update_manager = PluginManager::new(update_cache.clone());
        update_manager
            .install(update_source.to_str().unwrap(), None)
            .await
            .unwrap();
        write_plugin(&update_source, "quality", "2.0.0", "VERSION_TWO");
        run_failure_worker(
            "update",
            "update-before-backup-rename",
            &update_cache,
            &update_source,
        );
        let installed = update_manager.list().unwrap();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].version, "2.0.0");
        assert_no_transaction_residue(&update_manager);
    }

    #[tokio::test]
    async fn bounded_orphan_residues_are_reconciled_without_deleting_unknown_state() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let cache = temp.path().join("cache");
        write_plugin(&source, "quality", "1.0.0", "ORPHAN_RECOVERY");
        let manager = PluginManager::new(cache);
        manager
            .install(source.to_str().unwrap(), None)
            .await
            .unwrap();

        let stage = manager
            .staging_directory()
            .join(format!("stage-{}", Uuid::new_v4()));
        ensure_private_directory(&stage).unwrap();
        write_private_test_file(&stage.join("partial"), b"partial");
        let atomic_temp = manager
            .root
            .join(format!("{REGISTRY_TEMP_PREFIX}{}.tmp", Uuid::new_v4()));
        write_private_test_file(&atomic_temp, b"temporary");

        let target = manager.package_path("quality").unwrap();
        let backup = manager
            .root
            .join(format!(".backup-quality-{}", Uuid::new_v4()));
        durable_rename(&target, &backup).unwrap();
        let installed = manager.list().unwrap();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].version, "1.0.0");
        assert!(target.is_dir());
        assert!(!backup.exists());
        assert!(!stage.exists());
        assert!(!atomic_temp.exists());

        let quarantine = manager
            .root
            .join(format!(".uninstall-quality-{}", Uuid::new_v4()));
        durable_rename(&target, &quarantine).unwrap();
        let mut registry = manager.load_registry().unwrap();
        registry.plugins.remove("quality").unwrap();
        manager.write_registry(&registry).unwrap();
        assert!(manager.list().unwrap().is_empty());
        assert!(!quarantine.exists());
        assert_no_transaction_residue(&manager);

        let unknown_manager = PluginManager::new(temp.path().join("unknown-cache"));
        unknown_manager.ensure_layout().unwrap();
        let unknown = unknown_manager.root.join("unexpected-private-state");
        write_private_test_file(&unknown, b"do not delete");
        let error = format!("{:#}", unknown_manager.list().unwrap_err());
        assert!(error.contains("未知条目"), "unexpected error: {error}");
        assert!(unknown.exists(), "unknown state must be retained");

        let backup_manager = PluginManager::new(temp.path().join("unowned-backup-cache"));
        backup_manager.ensure_layout().unwrap();
        let unowned_backup = backup_manager
            .root
            .join(format!(".backup-orphan-{}", Uuid::new_v4()));
        ensure_private_directory(&unowned_backup).unwrap();
        write_private_test_file(&unowned_backup.join("partial"), b"do not delete");
        let error = format!("{:#}", backup_manager.list().unwrap_err());
        assert!(
            error.contains("没有对应 registry record"),
            "unexpected error: {error}"
        );
        assert!(unowned_backup.exists(), "unowned backup must be retained");
    }

    #[cfg(unix)]
    #[test]
    fn transient_residue_rejects_symlinks_hardlinks_and_oversized_files() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();

        let symlink_manager = PluginManager::new(temp.path().join("symlink-cache"));
        symlink_manager.ensure_layout().unwrap();
        let symlink_stage = symlink_manager
            .staging_directory()
            .join(format!("stage-{}", Uuid::new_v4()));
        ensure_private_directory(&symlink_stage).unwrap();
        let outside = temp.path().join("outside");
        write_private_test_file(&outside, b"outside");
        symlink(&outside, symlink_stage.join("escape")).unwrap();
        let error = format!("{:#}", symlink_manager.list().unwrap_err());
        assert!(error.contains("symlink"), "unexpected error: {error}");
        assert!(symlink_stage.exists());

        let hardlink_manager = PluginManager::new(temp.path().join("hardlink-cache"));
        hardlink_manager.ensure_layout().unwrap();
        let hardlink_stage = hardlink_manager
            .staging_directory()
            .join(format!("stage-{}", Uuid::new_v4()));
        ensure_private_directory(&hardlink_stage).unwrap();
        let first = hardlink_stage.join("first");
        write_private_test_file(&first, b"linked");
        fs::hard_link(&first, hardlink_stage.join("second")).unwrap();
        let error = format!("{:#}", hardlink_manager.list().unwrap_err());
        assert!(error.contains("hardlink"), "unexpected error: {error}");
        assert!(hardlink_stage.exists());

        let oversized_manager = PluginManager::new(temp.path().join("oversized-cache"));
        oversized_manager.ensure_layout().unwrap();
        let oversized_stage = oversized_manager
            .staging_directory()
            .join(format!("stage-{}", Uuid::new_v4()));
        ensure_private_directory(&oversized_stage).unwrap();
        let oversized = oversized_stage.join("oversized");
        let file = create_private_file(&oversized).unwrap();
        file.set_len(MAX_PACKAGE_FILE_BYTES + 1).unwrap();
        file.sync_all().unwrap();
        let error = format!("{:#}", oversized_manager.list().unwrap_err());
        assert!(error.contains("超大文件"), "unexpected error: {error}");
        assert!(oversized_stage.exists());

        let temp_manager = PluginManager::new(temp.path().join("temp-symlink-cache"));
        temp_manager.ensure_layout().unwrap();
        let temp_name = temp_manager
            .root
            .join(format!("{JOURNAL_TEMP_PREFIX}{}.tmp", Uuid::new_v4()));
        symlink(&outside, &temp_name).unwrap();
        let error = format!("{:#}", temp_manager.list().unwrap_err());
        assert!(
            error.contains("普通文件") || error.contains("symlink"),
            "unexpected error: {error}"
        );
        assert!(temp_name.exists());
    }

    #[test]
    fn transient_aggregate_byte_limit_fails_before_any_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PluginManager::new(temp.path().join("cache"));
        manager.ensure_layout().unwrap();
        let stage_count = (MAX_TRANSIENT_BYTES / MAX_PACKAGE_FILE_BYTES) as usize + 1;
        let mut stages = Vec::new();
        for _ in 0..stage_count {
            let stage = manager
                .staging_directory()
                .join(format!("stage-{}", Uuid::new_v4()));
            ensure_private_directory(&stage).unwrap();
            let file = create_private_file(&stage.join("sparse")).unwrap();
            file.set_len(MAX_PACKAGE_FILE_BYTES).unwrap();
            file.sync_all().unwrap();
            stages.push(stage);
        }
        let error = format!("{:#}", manager.list().unwrap_err());
        assert!(
            error.contains("bytes 超过限制") || error.contains("entry/byte 硬限制"),
            "unexpected error: {error}"
        );
        assert!(
            stages.iter().all(|stage| stage.exists()),
            "layout inspection must complete before deleting any residue"
        );
    }

    #[tokio::test]
    async fn local_install_update_rollback_uninstall_and_runtime_discovery() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        write_plugin(&source, "quality", "1.0.0", "VERSION_ONE");
        let manager = PluginManager::new(temp.path().join("cache"));

        let installed = manager
            .install(source.to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(installed.id, "quality");
        assert_eq!(installed.version, "1.0.0");
        assert!(installed.install_path.join("plugin.json").is_file());
        assert!(
            fs::read_to_string(installed.install_path.join("commands/check.md"))
                .unwrap()
                .contains("VERSION_ONE")
        );
        let settings = Settings {
            raw: json!({"plugins":{"directories":[installed.install_path]}}),
        };
        let catalog = PluginCatalog::discover(&settings, temp.path(), false).unwrap();
        assert_eq!(catalog.plugins()[0].name, "quality");

        let invalid = temp.path().join("wrong-id");
        write_plugin(&invalid, "other", "2.0.0", "INVALID_UPDATE");
        assert!(
            manager
                .update("quality", Some(invalid.to_str().unwrap()), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("不一致")
        );
        assert!(
            fs::read_to_string(
                manager
                    .package_path("quality")
                    .unwrap()
                    .join("commands/check.md")
            )
            .unwrap()
            .contains("VERSION_ONE"),
            "a rejected update must leave the old plugin intact"
        );

        write_plugin(&source, "quality", "2.0.0", "VERSION_TWO");
        let updated = manager.update("quality", None, None).await.unwrap();
        assert_eq!(updated.version, "2.0.0");
        assert!(
            fs::read_to_string(updated.install_path.join("commands/check.md"))
                .unwrap()
                .contains("VERSION_TWO")
        );
        assert_eq!(manager.list().unwrap().len(), 1);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(manager.root.join("installed.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(&updated.install_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(updated.install_path.join("plugin.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }

        let removed = manager.uninstall("quality").unwrap();
        assert_eq!(removed.id, "quality");
        assert!(!removed.install_path.exists());
        assert!(manager.list().unwrap().is_empty());
        assert!(manager.uninstall("../escape").is_err());
    }

    #[tokio::test]
    async fn zip_install_checks_digest_and_rejects_traversal_or_special_entries() {
        let temp = tempfile::tempdir().unwrap();
        let archive = temp.path().join("plugin.zip");
        let manifest = br#"{"name":"zipper","version":"1.2.3"}"#;
        let bytes = zip_bytes(&[
            ("bundle/plugin.json", manifest, Some(0o100600)),
            ("bundle/commands/check.md", b"zip command", Some(0o100600)),
            ("bundle/bin/run", b"#!/bin/sh\nexit 0\n", Some(0o100711)),
            ("bundle/data.txt", b"private data", Some(0o100666)),
        ]);
        fs::write(&archive, &bytes).unwrap();
        let digest = sha256_hex(&bytes);
        let manager = PluginManager::new(temp.path().join("cache"));
        assert!(
            manager
                .validate(archive.to_str().unwrap(), Some(&"0".repeat(64)))
                .await
                .unwrap_err()
                .to_string()
                .contains("SHA-256")
        );
        let installed = manager
            .install(archive.to_str().unwrap(), Some(&digest))
            .await
            .unwrap();
        assert_eq!(installed.id, "zipper");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            assert_eq!(
                fs::metadata(installed.install_path.join("bin/run"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(installed.install_path.join("data.txt"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(installed.install_path.join("bin"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
            assert!(
                std::process::Command::new(installed.install_path.join("bin/run"))
                    .status()
                    .unwrap()
                    .success(),
                "ZIP executable must remain directly runnable after private-mode normalization"
            );
        }
        manager.uninstall("zipper").unwrap();

        let traversal = temp.path().join("traversal.zip");
        fs::write(
            &traversal,
            zip_bytes(&[
                ("../escape", b"escape", Some(0o100600)),
                ("plugin.json", manifest, Some(0o100600)),
            ]),
        )
        .unwrap();
        assert!(
            manager
                .validate(traversal.to_str().unwrap(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("绝对路径或 ..")
        );

        let symlink = temp.path().join("symlink.zip");
        let cursor = Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(cursor);
        writer
            .start_file("plugin.json", SimpleFileOptions::default())
            .unwrap();
        writer.write_all(manifest).unwrap();
        writer
            .add_symlink("link", "outside", SimpleFileOptions::default())
            .unwrap();
        fs::write(&symlink, writer.finish().unwrap().into_inner()).unwrap();
        let error = manager
            .validate(symlink.to_str().unwrap(), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("symlink"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn directory_copy_normalizes_private_modes_and_digests_executable_state() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source-modes");
        write_plugin(&source, "modes", "1.0.0", "test");
        fs::create_dir_all(source.join("bin")).unwrap();
        let executable = source.join("bin/run");
        let data = source.join("data.txt");
        fs::write(&executable, "#!/bin/sh\nexit 0\n").unwrap();
        fs::write(&data, "private data").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o751)).unwrap();
        fs::set_permissions(&data, fs::Permissions::from_mode(0o666)).unwrap();

        let manager = PluginManager::new(temp.path().join("cache"));
        let executable_digest = manager
            .validate(source.to_str().unwrap(), None)
            .await
            .unwrap()
            .sha256;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o600)).unwrap();
        let non_executable_digest = manager
            .validate(source.to_str().unwrap(), None)
            .await
            .unwrap()
            .sha256;
        assert_ne!(
            executable_digest, non_executable_digest,
            "directory package digest must authenticate normalized executable state"
        );

        fs::set_permissions(&executable, fs::Permissions::from_mode(0o751)).unwrap();
        let installed = manager
            .install(source.to_str().unwrap(), None)
            .await
            .unwrap();
        assert_eq!(
            fs::metadata(installed.install_path.join("bin/run"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(installed.install_path.join("data.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(installed.install_path.join("bin"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert!(
            std::process::Command::new(installed.install_path.join("bin/run"))
                .status()
                .unwrap()
                .success(),
            "directory executable must remain directly runnable after private-mode normalization"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn directory_sources_reject_symlinks_hardlinks_and_oversized_files() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let manager = PluginManager::new(temp.path().join("cache"));

        let symlinked = temp.path().join("symlinked");
        write_plugin(&symlinked, "links", "1.0.0", "test");
        symlink("../outside", symlinked.join("escape")).unwrap();
        assert!(
            manager
                .validate(symlinked.to_str().unwrap(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );

        let hardlinked = temp.path().join("hardlinked");
        write_plugin(&hardlinked, "hard", "1.0.0", "test");
        fs::write(hardlinked.join("data"), "data").unwrap();
        fs::hard_link(hardlinked.join("data"), hardlinked.join("data-copy")).unwrap();
        assert!(
            manager
                .validate(hardlinked.to_str().unwrap(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("hardlink")
        );

        let oversized = temp.path().join("oversized");
        write_plugin(&oversized, "large", "1.0.0", "test");
        File::create(oversized.join("large.bin"))
            .unwrap()
            .set_len(MAX_PACKAGE_FILE_BYTES + 1)
            .unwrap();
        assert!(
            manager
                .validate(oversized.to_str().unwrap(), None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn manifests_and_remote_sources_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PluginManager::new(temp.path().join("cache"));
        let missing_version = temp.path().join("missing-version");
        fs::create_dir_all(&missing_version).unwrap();
        fs::write(missing_version.join("plugin.json"), r#"{"name":"missing"}"#).unwrap();
        assert!(
            manager
                .validate(missing_version.to_str().unwrap(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("version")
        );
        assert_eq!(
            fs::read_dir(manager.staging_directory()).unwrap().count(),
            0,
            "failed validation must not leave a staging tree"
        );

        assert!(SourceSpec::parse("https://user:secret@example.invalid/plugin.zip").is_err());
        assert!(SourceSpec::parse("https://example.invalid/plugin.zip?token=secret").is_err());
        assert!(
            manager
                .validate("https://example.invalid/plugin.zip", None)
                .await
                .unwrap_err()
                .to_string()
                .contains("--sha256")
        );
    }

    #[tokio::test]
    async fn bounded_local_mock_download_revalidates_redirects() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for (status, extra_headers, body) in [
                ("302 Found", "Location: /final\r\n", Vec::new()),
                ("200 OK", "", b"bounded archive".to_vec()),
            ] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        let url = Url::parse(&format!("http://{address}/start")).unwrap();
        let bytes = download_archive_with_policy(&url, true, true)
            .await
            .unwrap();
        server.join().unwrap();
        assert_eq!(bytes, b"bounded archive");
    }

    #[test]
    fn registry_schema_and_concurrent_lock_are_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let manager = PluginManager::new(temp.path().join("cache"));
        manager.ensure_layout().unwrap();
        let _lock = manager.acquire_lock().unwrap();
        assert!(
            manager
                .uninstall("missing")
                .unwrap_err()
                .to_string()
                .contains("正在进行")
        );
        drop(_lock);

        atomic_write_manager_file(
            &manager.registry_path(),
            r#"{"schema_version":1,"plugins":{},"unexpected":true}"#.as_bytes(),
            MAX_REGISTRY_BYTES,
            REGISTRY_TEMP_PREFIX,
        )
        .unwrap();
        assert!(manager.list().is_err());
    }
}

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    agents::{AgentToolPolicy, CustomAgentCatalog},
    tools::workspace_key,
};

const TEAM_STATE_VERSION: u32 = 1;
const MAX_TEAM_NAME_BYTES: usize = 128;
const MAX_MEMBER_NAME_BYTES: usize = 64;
const MAX_AGENT_NAME_BYTES: usize = 64;
const MAX_TASK_BYTES: usize = 256 * 1024;
const MAX_MESSAGE_READ: usize = 256;

const HARD_MAX_MEMBERS: usize = 64;
const HARD_MAX_RUNNING: usize = 16;
const HARD_MAX_DEPTH: usize = 8;
const HARD_MAX_ASSIGNMENTS: usize = 1_024;
const HARD_MAX_MESSAGES: usize = 8_192;
const HARD_MAX_MESSAGE_BYTES: usize = 256 * 1024;
const HARD_MAX_MAILBOX_BYTES: usize = 16 * 1024 * 1024;
const HARD_MAX_STATE_BYTES: usize = 32 * 1024 * 1024;
const HARD_MAX_PERSISTENT_TEAMS: usize = 256;
const HARD_MAX_WORKSPACE_TEAM_BYTES: usize = 256 * 1024 * 1024;
const HARD_MAX_WORKSPACE_TEAM_ENTRIES: usize = 1_024;
const MAX_STALE_TEAM_TEMPS_PER_SCAN: usize = 64;
const HARD_MAX_WORKSPACE_TEAM_SCAN_ENTRIES: usize =
    HARD_MAX_WORKSPACE_TEAM_ENTRIES + MAX_STALE_TEAM_TEMPS_PER_SCAN;
const PROJECT_LOCK_FILE: &str = ".open-agent-harness-team.lock";
const TEAM_TEMP_PREFIX: &str = ".open-agent-harness-team-";
const TEAM_TEMP_SUFFIX: &str = ".tmp";
const PROJECT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const PROJECT_LOCK_INITIAL_BACKOFF: Duration = Duration::from_millis(2);
const PROJECT_LOCK_MAX_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamStorageLimits {
    pub max_persistent_teams: usize,
    pub max_total_state_bytes: usize,
}

impl Default for TeamStorageLimits {
    fn default() -> Self {
        Self {
            max_persistent_teams: 32,
            max_total_state_bytes: 64 * 1024 * 1024,
        }
    }
}

impl TeamStorageLimits {
    pub fn validate(self) -> Result<Self> {
        if !(1..=HARD_MAX_PERSISTENT_TEAMS).contains(&self.max_persistent_teams)
            || !(1..=HARD_MAX_WORKSPACE_TEAM_BYTES).contains(&self.max_total_state_bytes)
        {
            bail!("team workspace storage limits 超过硬上限")
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamGcResult {
    pub deleted_team_ids: Vec<Uuid>,
    pub remaining_teams: usize,
    pub remaining_state_bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamLimits {
    pub max_members: usize,
    pub max_running: usize,
    pub max_depth: usize,
    pub max_total_assignments: usize,
    pub max_messages: usize,
    pub max_message_bytes: usize,
    pub max_mailbox_bytes: usize,
    pub max_state_bytes: usize,
}

impl Default for TeamLimits {
    fn default() -> Self {
        Self {
            max_members: 16,
            max_running: 4,
            max_depth: 3,
            max_total_assignments: 128,
            max_messages: 2_048,
            max_message_bytes: 64 * 1024,
            max_mailbox_bytes: 2 * 1024 * 1024,
            max_state_bytes: 8 * 1024 * 1024,
        }
    }
}

impl TeamLimits {
    pub fn validate(self) -> Result<Self> {
        if !(1..=HARD_MAX_MEMBERS).contains(&self.max_members)
            || !(1..=HARD_MAX_RUNNING).contains(&self.max_running)
            || self.max_running > self.max_members
            || !(1..=HARD_MAX_DEPTH).contains(&self.max_depth)
            || !(1..=HARD_MAX_ASSIGNMENTS).contains(&self.max_total_assignments)
            || !(1..=HARD_MAX_MESSAGES).contains(&self.max_messages)
            || !(1..=HARD_MAX_MESSAGE_BYTES).contains(&self.max_message_bytes)
            || !(1..=HARD_MAX_MAILBOX_BYTES).contains(&self.max_mailbox_bytes)
            || !(1..=HARD_MAX_STATE_BYTES).contains(&self.max_state_bytes)
        {
            bail!("team limits 超过硬上限或内部不一致")
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemberStatus {
    Idle,
    Assigned,
    Running,
    Completed,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TeamMessageKind {
    Message,
    Assignment,
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_agent: Option<String>,
    pub depth: usize,
    #[serde(default)]
    pub requested_policy: AgentToolPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMemberView {
    pub id: Uuid,
    pub name: String,
    pub custom_agent: Option<String>,
    pub depth: usize,
    pub status: MemberStatus,
    pub assignment: Option<String>,
    pub runtime_agent_id: Option<Uuid>,
    pub tool_policy: AgentToolPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamMessage {
    pub sequence: u64,
    pub id: Uuid,
    pub from: Uuid,
    pub to: Uuid,
    pub kind: TeamMessageKind,
    pub body: String,
    pub timestamp_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TeamSnapshot {
    pub id: Uuid,
    pub name: String,
    pub coordinator_id: Uuid,
    pub closed: bool,
    pub members: Vec<TeamMemberView>,
    pub next_sequence: u64,
    pub total_assignments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MemberAssignment {
    pub member: TeamMemberView,
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TeamState {
    version: u32,
    id: Uuid,
    workspace_key: String,
    name: String,
    coordinator_id: Uuid,
    coordinator_name: String,
    limits: TeamLimits,
    storage_limits: TeamStorageLimits,
    closed: bool,
    members: BTreeMap<Uuid, TeamMember>,
    messages: VecDeque<TeamMessage>,
    next_sequence: u64,
    total_assignments: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TeamMember {
    id: Uuid,
    name: String,
    custom_agent: Option<String>,
    depth: usize,
    status: MemberStatus,
    assignment: Option<String>,
    runtime_agent_id: Option<Uuid>,
    tool_policy: AgentToolPolicy,
}

#[derive(Clone)]
pub struct TeamService {
    workspace: PathBuf,
    storage_root: PathBuf,
    file: PathBuf,
    state: Arc<Mutex<TeamState>>,
    transaction_lock: Arc<Mutex<()>>,
}

/// The file descriptor, rather than the pathname, owns the advisory lock. Closing it (including
/// during process teardown) releases the lock, so a crashed harness cannot leave a stale mutex.
struct ProjectFileLock {
    file: File,
}

impl Drop for ProjectFileLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

impl TeamService {
    pub fn create(
        workspace: &Path,
        name: &str,
        coordinator_name: &str,
        limits: TeamLimits,
    ) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        let home = dirs::home_dir().context("无法确定用户主目录")?;
        let home = fs::canonicalize(home)?;
        let harness = ensure_private_child(&home, ".open-agent-harness")?;
        let root = ensure_private_child(&harness, "teams")?;
        Self::create_in_canonical(
            workspace,
            root,
            name,
            coordinator_name,
            limits,
            TeamStorageLimits::default(),
        )
    }

    pub fn create_in(
        workspace: &Path,
        storage_root: &Path,
        name: &str,
        coordinator_name: &str,
        limits: TeamLimits,
    ) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        let root = canonical_private_root(storage_root)?;
        Self::create_in_canonical(
            workspace,
            root,
            name,
            coordinator_name,
            limits,
            TeamStorageLimits::default(),
        )
    }

    pub fn create_in_with_storage_limits(
        workspace: &Path,
        storage_root: &Path,
        name: &str,
        coordinator_name: &str,
        limits: TeamLimits,
        storage_limits: TeamStorageLimits,
    ) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        let root = canonical_private_root(storage_root)?;
        Self::create_in_canonical(
            workspace,
            root,
            name,
            coordinator_name,
            limits,
            storage_limits,
        )
    }

    fn create_in_canonical(
        workspace: PathBuf,
        storage_root: PathBuf,
        name: &str,
        coordinator_name: &str,
        limits: TeamLimits,
        storage_limits: TeamStorageLimits,
    ) -> Result<Self> {
        validate_text("team name", name, MAX_TEAM_NAME_BYTES)?;
        validate_identifier("coordinator name", coordinator_name, MAX_MEMBER_NAME_BYTES)?;
        let limits = limits.validate()?;
        let storage_limits = storage_limits.validate()?;
        let project = ensure_private_child(&storage_root, &workspace_key(&workspace))?;
        let project_lock = team_lock_for(&project);
        let _project_transaction = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _project_file_lock = lock_project_file(&project)?;
        let id = Uuid::new_v4();
        let file = project.join(format!("{id}.json"));
        let transaction_lock = team_lock_for(&file);
        let transaction = transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if file.exists() || fs::symlink_metadata(&file).is_ok() {
            bail!("team state 已存在")
        }
        let state = TeamState {
            version: TEAM_STATE_VERSION,
            id,
            workspace_key: workspace_key(&workspace),
            name: name.to_owned(),
            coordinator_id: Uuid::new_v4(),
            coordinator_name: coordinator_name.to_owned(),
            limits,
            storage_limits,
            closed: false,
            members: BTreeMap::new(),
            messages: VecDeque::new(),
            next_sequence: 1,
            total_assignments: 0,
        };
        validate_state(&state, &workspace)?;
        let state_bytes = serde_json::to_vec(&state)?.len();
        let usage = workspace_team_usage(&project)?;
        if usage.team_count >= storage_limits.max_persistent_teams {
            bail!(
                "workspace persistent team 达到 {} 个限制；请先 delete 或 gc",
                storage_limits.max_persistent_teams
            )
        }
        if usage
            .state_bytes
            .checked_add(state_bytes)
            .context("workspace team state 字节数溢出")?
            > storage_limits.max_total_state_bytes
        {
            bail!(
                "workspace persistent team state 超过 {} 字节限制；请先 delete 或 gc",
                storage_limits.max_total_state_bytes
            )
        }
        write_state(&file, &state)?;
        drop(transaction);
        Ok(Self {
            workspace,
            storage_root,
            file,
            state: Arc::new(Mutex::new(state)),
            transaction_lock,
        })
    }

    pub fn open(workspace: &Path, team_id: Uuid) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        let home = dirs::home_dir().context("无法确定用户主目录")?;
        let home = fs::canonicalize(home)?;
        let harness = ensure_private_child(&home, ".open-agent-harness")?;
        let root = ensure_private_child(&harness, "teams")?;
        Self::open_in_canonical(workspace, root, team_id)
    }

    pub fn open_in(workspace: &Path, storage_root: &Path, team_id: Uuid) -> Result<Self> {
        let workspace = canonical_workspace(workspace)?;
        let root = canonical_private_root(storage_root)?;
        Self::open_in_canonical(workspace, root, team_id)
    }

    fn open_in_canonical(workspace: PathBuf, storage_root: PathBuf, team_id: Uuid) -> Result<Self> {
        let project = ensure_private_child(&storage_root, &workspace_key(&workspace))?;
        let project_lock = team_lock_for(&project);
        let _project_transaction = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _project_file_lock = lock_project_file(&project)?;
        let file = project.join(format!("{team_id}.json"));
        let transaction_lock = team_lock_for(&file);
        let transaction = transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = read_state(&file)?;
        if state.id != team_id {
            bail!("team state id 不匹配")
        }
        validate_state(&state, &workspace)?;
        drop(transaction);
        Ok(Self {
            workspace,
            storage_root,
            file,
            state: Arc::new(Mutex::new(state)),
            transaction_lock,
        })
    }

    pub fn id(&self) -> Uuid {
        self.lock_state().id
    }

    pub fn coordinator_id(&self) -> Uuid {
        self.lock_state().coordinator_id
    }

    pub fn snapshot(&self, actor: Uuid) -> Result<TeamSnapshot> {
        let state = self.current_state()?;
        require_coordinator(&state, actor)?;
        Ok(snapshot(&state))
    }

    pub fn list_members(&self, actor: Uuid) -> Result<Vec<TeamMemberView>> {
        let state = self.current_state()?;
        require_coordinator(&state, actor)?;
        Ok(state.members.values().map(member_view).collect())
    }

    pub fn member(&self, actor: Uuid, member_id: Uuid) -> Result<TeamMemberView> {
        let state = self.current_state()?;
        if actor != member_id && actor != state.coordinator_id {
            bail!("不能查看其他 member 状态")
        }
        state
            .members
            .get(&member_id)
            .map(member_view)
            .context("team member 不存在")
    }

    pub fn add_member(
        &self,
        actor: Uuid,
        spec: MemberSpec,
        parent_policy: &AgentToolPolicy,
    ) -> Result<TeamMemberView> {
        self.mutate(|state| {
            require_open_coordinator(state, actor)?;
            validate_identifier("member name", &spec.name, MAX_MEMBER_NAME_BYTES)?;
            if let Some(custom_agent) = &spec.custom_agent {
                validate_identifier("custom agent", custom_agent, MAX_AGENT_NAME_BYTES)?;
            }
            if spec.depth == 0 || spec.depth > state.limits.max_depth {
                bail!("member depth 必须在 1..={} 之间", state.limits.max_depth)
            }
            if state.members.len() >= state.limits.max_members {
                bail!("team member 达到 {} 个限制", state.limits.max_members)
            }
            if state
                .members
                .values()
                .any(|member| member.name == spec.name)
                || state.coordinator_name == spec.name
            {
                bail!("team participant name 重复")
            }
            validate_policy(&spec.requested_policy)?;
            let member = TeamMember {
                id: Uuid::new_v4(),
                name: spec.name,
                custom_agent: spec.custom_agent,
                depth: spec.depth,
                status: MemberStatus::Idle,
                assignment: None,
                runtime_agent_id: None,
                tool_policy: AgentToolPolicy::narrow(parent_policy, &spec.requested_policy),
            };
            let view = member_view(&member);
            state.members.insert(member.id, member);
            Ok(view)
        })
    }

    pub fn add_custom_member(
        &self,
        actor: Uuid,
        member_name: &str,
        custom_agent: &str,
        depth: usize,
        catalog: &CustomAgentCatalog,
        parent_policy: &AgentToolPolicy,
    ) -> Result<TeamMemberView> {
        let definition = catalog
            .get(custom_agent)
            .with_context(|| format!("custom agent 不存在: {custom_agent}"))?;
        self.add_member(
            actor,
            MemberSpec {
                name: member_name.to_owned(),
                custom_agent: Some(custom_agent.to_owned()),
                depth,
                requested_policy: definition.tool_policy(),
            },
            parent_policy,
        )
    }

    pub fn assign(&self, actor: Uuid, member_id: Uuid, task: &str) -> Result<MemberAssignment> {
        self.mutate(|state| {
            require_open_coordinator(state, actor)?;
            validate_text("assignment", task, MAX_TASK_BYTES)?;
            if state.total_assignments >= state.limits.max_total_assignments {
                bail!(
                    "team assignment 达到 {} 次限制",
                    state.limits.max_total_assignments
                )
            }
            let member = state
                .members
                .get_mut(&member_id)
                .context("team member 不存在")?;
            if matches!(member.status, MemberStatus::Running | MemberStatus::Stopped) {
                bail!("running 或 stopped member 不能接收新任务")
            }
            member.assignment = Some(task.to_owned());
            member.runtime_agent_id = None;
            member.status = MemberStatus::Assigned;
            state.total_assignments += 1;
            push_message(state, actor, member_id, TeamMessageKind::Assignment, task)?;
            let member = state
                .members
                .get(&member_id)
                .expect("member remains present");
            Ok(MemberAssignment {
                member: member_view(member),
                prompt: task.to_owned(),
            })
        })
    }

    pub fn mark_running(
        &self,
        actor: Uuid,
        member_id: Uuid,
        runtime_agent_id: Uuid,
    ) -> Result<TeamMemberView> {
        self.mutate(|state| {
            require_open_coordinator(state, actor)?;
            let running = state
                .members
                .values()
                .filter(|member| member.status == MemberStatus::Running)
                .count();
            if running >= state.limits.max_running {
                bail!(
                    "team running member 达到 {} 个限制",
                    state.limits.max_running
                )
            }
            if state
                .members
                .values()
                .any(|member| member.runtime_agent_id == Some(runtime_agent_id))
            {
                bail!("runtime agent id 已绑定到其他 member")
            }
            let member = state
                .members
                .get_mut(&member_id)
                .context("team member 不存在")?;
            if member.status != MemberStatus::Assigned {
                bail!("只有 assigned member 可以进入 running")
            }
            member.status = MemberStatus::Running;
            member.runtime_agent_id = Some(runtime_agent_id);
            Ok(member_view(member))
        })
    }

    pub fn mark_start_failed(&self, actor: Uuid, member_id: Uuid) -> Result<TeamMemberView> {
        self.mutate(|state| {
            require_open_coordinator(state, actor)?;
            let member = state
                .members
                .get_mut(&member_id)
                .context("team member 不存在")?;
            if member.status != MemberStatus::Assigned {
                bail!("只有 assigned member 可以标记启动失败")
            }
            member.status = MemberStatus::Failed;
            member.runtime_agent_id = None;
            Ok(member_view(member))
        })
    }

    pub fn finish(
        &self,
        actor: Uuid,
        member_id: Uuid,
        succeeded: bool,
        summary: &str,
    ) -> Result<TeamMemberView> {
        self.mutate(|state| {
            require_participant(state, actor)?;
            if actor != member_id && actor != state.coordinator_id {
                bail!("member 只能结束自己的任务")
            }
            validate_text(
                "member status summary",
                summary,
                state.limits.max_message_bytes,
            )?;
            let member = state
                .members
                .get_mut(&member_id)
                .context("team member 不存在")?;
            if member.status != MemberStatus::Running {
                bail!("只有 running member 可以结束任务")
            }
            member.status = if succeeded {
                MemberStatus::Completed
            } else {
                MemberStatus::Failed
            };
            member.runtime_agent_id = None;
            push_message(
                state,
                member_id,
                state.coordinator_id,
                TeamMessageKind::Status,
                summary,
            )?;
            Ok(member_view(
                state
                    .members
                    .get(&member_id)
                    .expect("member remains present"),
            ))
        })
    }

    pub fn send(&self, from: Uuid, to: Uuid, body: &str) -> Result<TeamMessage> {
        self.mutate(|state| {
            if state.closed {
                bail!("team 已关闭")
            }
            require_active_participant(state, from)?;
            require_participant(state, to)?;
            push_message(state, from, to, TeamMessageKind::Message, body)
        })
    }

    pub fn read_mailbox(
        &self,
        actor: Uuid,
        mailbox: Uuid,
        after_sequence: u64,
        maximum: usize,
    ) -> Result<Vec<TeamMessage>> {
        let state = self.current_state()?;
        require_mailbox_access(&state, actor, mailbox)?;
        if maximum == 0 || maximum > MAX_MESSAGE_READ {
            bail!("mailbox read maximum 必须在 1..={MAX_MESSAGE_READ}")
        }
        Ok(state
            .messages
            .iter()
            .filter(|message| message.to == mailbox && message.sequence > after_sequence)
            .take(maximum)
            .cloned()
            .collect())
    }

    pub fn acknowledge(&self, actor: Uuid, mailbox: Uuid, through_sequence: u64) -> Result<usize> {
        self.mutate(|state| {
            require_mailbox_access(state, actor, mailbox)?;
            let before = state.messages.len();
            state
                .messages
                .retain(|message| message.to != mailbox || message.sequence > through_sequence);
            Ok(before - state.messages.len())
        })
    }

    pub fn stop_member(&self, actor: Uuid, member_id: Uuid) -> Result<Option<Uuid>> {
        self.mutate(|state| {
            require_open_coordinator(state, actor)?;
            let member = state
                .members
                .get_mut(&member_id)
                .context("team member 不存在")?;
            let runtime = member.runtime_agent_id.take();
            member.status = MemberStatus::Stopped;
            Ok(runtime)
        })
    }

    pub fn shutdown(&self, actor: Uuid) -> Result<Vec<Uuid>> {
        self.mutate(|state| {
            require_coordinator(state, actor)?;
            if state.closed {
                return Ok(Vec::new());
            }
            state.closed = true;
            let mut runtime_ids = Vec::new();
            for member in state.members.values_mut() {
                if let Some(runtime) = member.runtime_agent_id.take() {
                    runtime_ids.push(runtime);
                }
                member.status = MemberStatus::Stopped;
            }
            Ok(runtime_ids)
        })
    }

    pub fn delete(&self, actor: Uuid) -> Result<()> {
        let project = self
            .file
            .parent()
            .context("team state 路径缺少 project 目录")?;
        let project_lock = team_lock_for(project);
        let _project_transaction = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _project_file_lock = lock_project_file(project)?;
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = read_state(&self.file)?;
        validate_state(&state, &self.workspace)?;
        require_coordinator(&state, actor)?;
        if !state.closed {
            bail!("team 仍处于打开状态；请先 shutdown 再 delete")
        }
        if state
            .members
            .values()
            .any(|member| member.runtime_agent_id.is_some())
        {
            bail!("team 仍绑定运行中的 agent，拒绝删除")
        }
        remove_state_file(&self.file)
    }

    pub fn gc_closed(workspace: &Path, maximum: usize) -> Result<TeamGcResult> {
        let workspace = canonical_workspace(workspace)?;
        let home = dirs::home_dir().context("无法确定用户主目录")?;
        let home = fs::canonicalize(home)?;
        let harness = ensure_private_child(&home, ".open-agent-harness")?;
        let root = ensure_private_child(&harness, "teams")?;
        Self::gc_closed_in_canonical(workspace, root, maximum)
    }

    pub fn gc_closed_in(
        workspace: &Path,
        storage_root: &Path,
        maximum: usize,
    ) -> Result<TeamGcResult> {
        let workspace = canonical_workspace(workspace)?;
        let root = canonical_private_root(storage_root)?;
        Self::gc_closed_in_canonical(workspace, root, maximum)
    }

    fn gc_closed_in_canonical(
        workspace: PathBuf,
        storage_root: PathBuf,
        maximum: usize,
    ) -> Result<TeamGcResult> {
        if maximum == 0 || maximum > HARD_MAX_PERSISTENT_TEAMS {
            bail!("team gc maximum 必须在 1..={HARD_MAX_PERSISTENT_TEAMS}")
        }
        let project = ensure_private_child(&storage_root, &workspace_key(&workspace))?;
        let project_lock = team_lock_for(&project);
        let _project_transaction = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _project_file_lock = lock_project_file(&project)?;
        let paths = workspace_team_state_paths(&project)?;
        let file_locks = paths
            .iter()
            .map(|path| team_lock_for(path))
            .collect::<Vec<_>>();
        let _file_transactions = file_locks
            .iter()
            .map(|lock| lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
            .collect::<Vec<_>>();
        let mut states = Vec::with_capacity(paths.len());
        for path in &paths {
            let state = read_state(path)?;
            validate_state(&state, &workspace)?;
            states.push(state);
        }
        let mut deleted_team_ids = Vec::new();
        for (path, state) in paths.iter().zip(states).filter(|(_, state)| state.closed) {
            if deleted_team_ids.len() >= maximum {
                break;
            }
            remove_state_file(path)?;
            deleted_team_ids.push(state.id);
        }
        let usage = workspace_team_usage(&project)?;
        Ok(TeamGcResult {
            deleted_team_ids,
            remaining_teams: usage.team_count,
            remaining_state_bytes: usage.state_bytes,
        })
    }

    pub fn storage_path(&self) -> &Path {
        &self.file
    }

    pub fn storage_root(&self) -> &Path {
        &self.storage_root
    }

    fn mutate<T>(&self, operation: impl FnOnce(&mut TeamState) -> Result<T>) -> Result<T> {
        let project = self
            .file
            .parent()
            .context("team state 路径缺少 project 目录")?;
        let project_lock = team_lock_for(project);
        let _project_transaction = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _project_file_lock = lock_project_file(project)?;
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut next = read_state(&self.file)?;
        validate_state(&next, &self.workspace)?;
        let result = operation(&mut next)?;
        validate_state(&next, &self.workspace)?;
        validate_workspace_state_replacement(
            project,
            &self.file,
            serde_json::to_vec(&next)?.len(),
            next.storage_limits,
        )?;
        write_state(&self.file, &next)?;
        *self.lock_state() = next;
        Ok(result)
    }

    fn current_state(&self) -> Result<TeamState> {
        let project = self
            .file
            .parent()
            .context("team state 路径缺少 project 目录")?;
        let project_lock = team_lock_for(project);
        let _project_transaction = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _project_file_lock = lock_project_file(project)?;
        let _transaction = self
            .transaction_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = read_state(&self.file)?;
        validate_state(&state, &self.workspace)?;
        *self.lock_state() = state.clone();
        Ok(state)
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TeamState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn lock_project_file(project: &Path) -> Result<ProjectFileLock> {
    validate_private_directory(project, "team project storage")
        .map_err(|_| anyhow::anyhow!("team project lock directory validation failed"))?;
    let path = project.join(PROJECT_LOCK_FILE);
    let (file, created) = match open_project_lock_file(&path, true) {
        Ok(file) => (file, true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (
            open_project_lock_file(&path, false)
                .map_err(|_| anyhow::anyhow!("team project lockfile open failed"))?,
            false,
        ),
        Err(_) => bail!("team project lockfile creation failed"),
    };
    if created {
        set_private_open_file_permissions(&file)
            .map_err(|_| anyhow::anyhow!("team project lockfile permission setup failed"))?;
    }
    try_lock_project_file_with_timeout(&file, PROJECT_LOCK_TIMEOUT)?;
    if validate_open_private_file(&path, &file, "team project lockfile").is_err() {
        let _ = fs2::FileExt::unlock(&file);
        bail!("team project lockfile validation failed")
    }
    Ok(ProjectFileLock { file })
}

fn try_lock_project_file_with_timeout(file: &File, timeout: Duration) -> Result<()> {
    let started = Instant::now();
    let mut backoff = PROJECT_LOCK_INITIAL_BACKOFF;
    loop {
        match fs2::FileExt::try_lock_exclusive(file) {
            Ok(()) => return Ok(()),
            Err(error) if project_lock_is_contended(&error) => {
                let elapsed = started.elapsed();
                let Some(remaining) = timeout.checked_sub(elapsed) else {
                    bail!("team project lock acquisition timed out")
                };
                if remaining.is_zero() {
                    bail!("team project lock acquisition timed out")
                }
                std::thread::sleep(backoff.min(remaining));
                backoff = backoff
                    .checked_mul(2)
                    .unwrap_or(PROJECT_LOCK_MAX_BACKOFF)
                    .min(PROJECT_LOCK_MAX_BACKOFF);
            }
            Err(_) => bail!("team project lock acquisition failed"),
        }
    }
}

fn project_lock_is_contended(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::WouldBlock {
        return true;
    }
    #[cfg(windows)]
    if error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION as i32) {
        return true;
    }
    false
}

fn open_project_lock_file(path: &Path, create_new: bool) -> std::io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn team_lock_for(file: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(file).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(file.to_path_buf(), Arc::downgrade(&lock));
    lock
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkspaceTeamUsage {
    team_count: usize,
    state_bytes: usize,
}

fn workspace_team_usage(project: &Path) -> Result<WorkspaceTeamUsage> {
    let paths = workspace_team_state_paths(project)?;
    let mut state_bytes = 0_usize;
    for path in &paths {
        let metadata = fs::symlink_metadata(path)?;
        state_bytes = state_bytes
            .checked_add(usize::try_from(metadata.len()).context("team state 文件过大")?)
            .context("workspace team state 字节数溢出")?;
        if state_bytes > HARD_MAX_WORKSPACE_TEAM_BYTES {
            bail!("workspace team state 超过 {HARD_MAX_WORKSPACE_TEAM_BYTES} 字节硬限制")
        }
    }
    Ok(WorkspaceTeamUsage {
        team_count: paths.len(),
        state_bytes,
    })
}

fn validate_workspace_state_replacement(
    project: &Path,
    current_file: &Path,
    next_bytes: usize,
    limits: TeamStorageLimits,
) -> Result<()> {
    let limits = limits.validate()?;
    let usage = workspace_team_usage(project)?;
    let current_bytes = usize::try_from(fs::symlink_metadata(current_file)?.len())
        .context("team state 文件过大")?;
    let replaced = usage
        .state_bytes
        .checked_sub(current_bytes)
        .and_then(|bytes| bytes.checked_add(next_bytes))
        .context("workspace team state 字节数溢出")?;
    if replaced > limits.max_total_state_bytes {
        bail!(
            "workspace persistent team state 超过 {} 字节限制；请先 acknowledge、delete 或 gc",
            limits.max_total_state_bytes
        )
    }
    Ok(())
}

fn workspace_team_state_paths(project: &Path) -> Result<Vec<PathBuf>> {
    validate_private_directory(project, "team project storage")?;
    let mut paths = Vec::new();
    let mut stale_temps = Vec::new();
    let mut found_lockfile = false;
    let mut persistent_entries = 0_usize;
    for (index, entry) in fs::read_dir(project)?.enumerate() {
        if index >= HARD_MAX_WORKSPACE_TEAM_SCAN_ENTRIES {
            bail!(
                "workspace team storage 扫描条目超过 {HARD_MAX_WORKSPACE_TEAM_SCAN_ENTRIES} 硬限制"
            )
        }
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            bail!("workspace team storage 含非 UTF-8 文件名")
        };
        if file_name == PROJECT_LOCK_FILE {
            if found_lockfile {
                bail!("workspace team storage 含重复 project lockfile")
            }
            validate_private_regular_path(&path, "team project lockfile")?;
            found_lockfile = true;
            persistent_entries += 1;
            continue;
        }
        if parse_team_temp_name(file_name).is_some() {
            validate_private_regular_path(&path, "team temporary state")?;
            stale_temps.push(path);
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            bail!("workspace team storage 含未知条目: {file_name}")
        }
        let stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .context("team state 文件名无效")?;
        let _: Uuid = stem.parse().context("team state 文件名不是 UUID")?;
        validate_private_regular_path(&path, "team state")?;
        paths.push(path);
        persistent_entries += 1;
        if persistent_entries > HARD_MAX_WORKSPACE_TEAM_ENTRIES {
            bail!("workspace team storage 持久条目超过 {HARD_MAX_WORKSPACE_TEAM_ENTRIES} 硬限制")
        }
    }
    if !found_lockfile {
        bail!("workspace team storage 缺少 project lockfile")
    }
    paths.sort();
    stale_temps.sort();
    cleanup_stale_team_temps(project, &stale_temps)?;
    Ok(paths)
}

fn parse_team_temp_name(file_name: &str) -> Option<Uuid> {
    file_name
        .strip_prefix(TEAM_TEMP_PREFIX)?
        .strip_suffix(TEAM_TEMP_SUFFIX)?
        .parse()
        .ok()
}

fn cleanup_stale_team_temps(project: &Path, paths: &[PathBuf]) -> Result<()> {
    let mut removed_any = false;
    let result = (|| -> Result<()> {
        for path in paths.iter().take(MAX_STALE_TEAM_TEMPS_PER_SCAN) {
            // Revalidate immediately before unlinking. Legitimate writers cannot coexist because
            // callers hold the project OS lock, so every valid temp observed here is crash debris.
            validate_private_regular_path(path, "team temporary state")?;
            fs::remove_file(path).context("无法删除 stale team temporary state")?;
            removed_any = true;
        }
        Ok(())
    })();
    if removed_any {
        #[cfg(unix)]
        fs::File::open(project)?.sync_all()?;
        #[cfg(not(unix))]
        let _ = project;
    }
    result
}

fn remove_state_file(path: &Path) -> Result<()> {
    validate_private_regular_path(path, "team state")?;
    let parent = path.parent().context("team state 路径缺少父目录")?;
    fs::remove_file(path).with_context(|| format!("无法删除 team state {}", path.display()))?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

fn snapshot(state: &TeamState) -> TeamSnapshot {
    TeamSnapshot {
        id: state.id,
        name: state.name.clone(),
        coordinator_id: state.coordinator_id,
        closed: state.closed,
        members: state.members.values().map(member_view).collect(),
        next_sequence: state.next_sequence,
        total_assignments: state.total_assignments,
    }
}

fn member_view(member: &TeamMember) -> TeamMemberView {
    TeamMemberView {
        id: member.id,
        name: member.name.clone(),
        custom_agent: member.custom_agent.clone(),
        depth: member.depth,
        status: member.status,
        assignment: member.assignment.clone(),
        runtime_agent_id: member.runtime_agent_id,
        tool_policy: member.tool_policy.clone(),
    }
}

fn push_message(
    state: &mut TeamState,
    from: Uuid,
    to: Uuid,
    kind: TeamMessageKind,
    body: &str,
) -> Result<TeamMessage> {
    validate_text("team message", body, state.limits.max_message_bytes)?;
    if state.messages.len() >= state.limits.max_messages {
        bail!("team mailbox 达到 {} 条消息限制", state.limits.max_messages)
    }
    let current_bytes = state
        .messages
        .iter()
        .try_fold(0_usize, |bytes, message| {
            bytes.checked_add(message.body.len())
        })
        .context("team mailbox 字节数溢出")?;
    if current_bytes
        .checked_add(body.len())
        .context("team mailbox 字节数溢出")?
        > state.limits.max_mailbox_bytes
    {
        bail!(
            "team mailbox 超过 {} 字节限制",
            state.limits.max_mailbox_bytes
        )
    }
    let sequence = state.next_sequence;
    state.next_sequence = state
        .next_sequence
        .checked_add(1)
        .context("team message sequence 溢出")?;
    let message = TeamMessage {
        sequence,
        id: Uuid::new_v4(),
        from,
        to,
        kind,
        body: body.to_owned(),
        timestamp_ms: now_ms(),
    };
    state.messages.push_back(message.clone());
    Ok(message)
}

fn require_coordinator(state: &TeamState, actor: Uuid) -> Result<()> {
    if actor != state.coordinator_id {
        bail!("此操作只允许 team coordinator")
    }
    Ok(())
}

fn require_open_coordinator(state: &TeamState, actor: Uuid) -> Result<()> {
    require_coordinator(state, actor)?;
    if state.closed {
        bail!("team 已关闭")
    }
    Ok(())
}

fn require_participant(state: &TeamState, participant: Uuid) -> Result<()> {
    if participant != state.coordinator_id && !state.members.contains_key(&participant) {
        bail!("team participant 不存在")
    }
    Ok(())
}

fn require_active_participant(state: &TeamState, participant: Uuid) -> Result<()> {
    require_participant(state, participant)?;
    if state
        .members
        .get(&participant)
        .is_some_and(|member| member.status == MemberStatus::Stopped)
    {
        bail!("stopped member 不能发送消息")
    }
    Ok(())
}

fn require_mailbox_access(state: &TeamState, actor: Uuid, mailbox: Uuid) -> Result<()> {
    require_participant(state, mailbox)?;
    if actor != mailbox && actor != state.coordinator_id {
        bail!("不能读取其他 member 的 mailbox")
    }
    Ok(())
}

fn validate_policy(policy: &AgentToolPolicy) -> Result<()> {
    let allowed_count = policy.allowed_tools.as_ref().map_or(0, BTreeSet::len);
    if allowed_count > 128 || policy.disallowed_tools.len() > 128 {
        bail!("member tool policy 超过 128 项限制")
    }
    for name in policy
        .allowed_tools
        .iter()
        .flatten()
        .chain(&policy.disallowed_tools)
    {
        validate_identifier("tool name", name, 128)?;
    }
    Ok(())
}

fn validate_state(state: &TeamState, workspace: &Path) -> Result<()> {
    state.limits.validate()?;
    state.storage_limits.validate()?;
    if state.version != TEAM_STATE_VERSION || state.workspace_key != workspace_key(workspace) {
        bail!("team state 版本或工作区不匹配")
    }
    validate_text("team name", &state.name, MAX_TEAM_NAME_BYTES)?;
    validate_identifier(
        "coordinator name",
        &state.coordinator_name,
        MAX_MEMBER_NAME_BYTES,
    )?;
    if state.members.len() > state.limits.max_members
        || state.messages.len() > state.limits.max_messages
        || state.total_assignments > state.limits.max_total_assignments
    {
        bail!("team state 超过资源上限")
    }
    let running = state
        .members
        .values()
        .filter(|member| member.status == MemberStatus::Running)
        .count();
    if running > state.limits.max_running {
        bail!("team state running member 超过限制")
    }
    let mut names = BTreeSet::new();
    names.insert(state.coordinator_name.as_str());
    let mut runtime_ids = BTreeSet::new();
    for (id, member) in &state.members {
        if id != &member.id
            || !names.insert(member.name.as_str())
            || member.depth == 0
            || member.depth > state.limits.max_depth
        {
            bail!("team member state 损坏")
        }
        validate_identifier("member name", &member.name, MAX_MEMBER_NAME_BYTES)?;
        if let Some(custom_agent) = &member.custom_agent {
            validate_identifier("custom agent", custom_agent, MAX_AGENT_NAME_BYTES)?;
        }
        if let Some(task) = &member.assignment {
            validate_text("assignment", task, MAX_TASK_BYTES)?;
        }
        validate_policy(&member.tool_policy)?;
        if let Some(runtime_id) = member.runtime_agent_id {
            if member.status != MemberStatus::Running || !runtime_ids.insert(runtime_id) {
                bail!("team runtime agent binding 损坏")
            }
        } else if member.status == MemberStatus::Running {
            bail!("running member 缺少 runtime agent id")
        }
        if state.closed && member.status != MemberStatus::Stopped {
            bail!("closed team 含未停止 member")
        }
    }
    let mut mailbox_bytes = 0_usize;
    let mut previous_sequence = 0_u64;
    let mut message_ids = BTreeSet::new();
    for message in &state.messages {
        require_participant(state, message.from)?;
        require_participant(state, message.to)?;
        validate_text(
            "team message",
            &message.body,
            state.limits.max_message_bytes,
        )?;
        mailbox_bytes = mailbox_bytes
            .checked_add(message.body.len())
            .context("team mailbox 字节数溢出")?;
        if message.sequence <= previous_sequence || !message_ids.insert(message.id) {
            bail!("team message sequence 或 id 损坏")
        }
        previous_sequence = message.sequence;
    }
    if mailbox_bytes > state.limits.max_mailbox_bytes
        || state.next_sequence <= previous_sequence
        || state.next_sequence == 0
    {
        bail!("team mailbox 状态损坏或超过限制")
    }
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() > state.limits.max_state_bytes || bytes.len() > HARD_MAX_STATE_BYTES {
        bail!("team state 超过持久化字节限制")
    }
    Ok(())
}

fn read_state(path: &Path) -> Result<TeamState> {
    validate_private_regular_path(path, "team state")?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .with_context(|| format!("无法打开 team state {}", path.display()))?;
    validate_open_private_file(path, &file, "team state")?;
    let size = file.metadata()?.len();
    if size > HARD_MAX_STATE_BYTES as u64 {
        bail!("team state 超过 {HARD_MAX_STATE_BYTES} 字节硬限制")
    }
    let mut bytes = Vec::new();
    file.take(HARD_MAX_STATE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > HARD_MAX_STATE_BYTES {
        bail!("team state 超过 {HARD_MAX_STATE_BYTES} 字节硬限制")
    }
    serde_json::from_slice(&bytes).context("team state JSON 损坏")
}

fn write_state(path: &Path, state: &TeamState) -> Result<()> {
    let bytes = serde_json::to_vec(state)?;
    if bytes.len() > state.limits.max_state_bytes || bytes.len() > HARD_MAX_STATE_BYTES {
        bail!("team state 超过持久化字节限制")
    }
    atomic_write_private(path, &bytes)
}

fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            validate_private_regular_path(path, "team state")?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let parent = path.parent().context("team state 路径缺少父目录")?;
    validate_private_directory(parent, "team state parent")?;
    let temp = parent.join(format!(".open-agent-harness-team-{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        validate_open_private_file(&temp, &file, "team temporary state")?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        set_private_file_permissions(path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result.with_context(|| format!("无法原子写入 team state {}", path.display()))
}

fn canonical_workspace(workspace: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(workspace)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("team workspace 必须是非 symlink 目录")
    }
    fs::canonicalize(workspace).context("无法解析 team workspace")
}

fn validate_private_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} 不存在: {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("{label} 必须是非 symlink 目录")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("{label} 权限必须为私有目录权限")
        }
    }
    Ok(())
}

fn validate_private_regular_path(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} 不存在: {}", path.display()))?;
    validate_private_regular_metadata(&metadata, label)?;
    #[cfg(windows)]
    {
        let file = open_windows_path_without_following_reparse_points(path)?;
        validate_windows_file_info(&windows_file_info(&file)?, label)?;
    }
    Ok(())
}

fn validate_open_private_file(path: &Path, file: &File, label: &str) -> Result<()> {
    let opened = file.metadata()?;
    validate_private_regular_metadata(&opened, label)?;
    let path_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} 路径在打开后消失: {}", path.display()))?;
    validate_private_regular_metadata(&path_metadata, label)?;
    #[cfg(unix)]
    if !same_unix_file_identity(&opened, &path_metadata) {
        bail!("{label} 路径在打开期间被替换")
    }
    #[cfg(windows)]
    {
        let opened_info = windows_file_info(file)?;
        validate_windows_file_info(&opened_info, label)?;
        let path_file = open_windows_path_without_following_reparse_points(path)?;
        let path_info = windows_file_info(&path_file)?;
        validate_windows_file_info(&path_info, label)?;
        if windows_file_identity(&opened_info) != windows_file_identity(&path_info) {
            bail!("{label} 路径在打开期间被替换")
        }
    }
    Ok(())
}

fn validate_private_regular_metadata(metadata: &fs::Metadata, label: &str) -> Result<()> {
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("{label} 必须是非 symlink 普通文件")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.nlink() != 1 {
            bail!("{label} 必须只有一个 hard link")
        }
        if metadata.permissions().mode() & 0o777 != 0o600 {
            bail!("{label} 权限必须为 0600")
        }
    }
    Ok(())
}

#[cfg(unix)]
fn same_unix_file_identity(first: &fs::Metadata, second: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    first.dev() == second.dev() && first.ino() == second.ino()
}

#[cfg(windows)]
fn open_windows_path_without_following_reparse_points(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;

    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    options
        .open(path)
        .with_context(|| format!("无法安全打开 team 文件 {}", path.display()))
}

#[cfg(windows)]
fn windows_file_info(
    file: &File,
) -> Result<windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION> {
    use std::os::windows::io::AsRawHandle;
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
fn validate_windows_file_info(
    info: &windows_sys::Win32::Storage::FileSystem::BY_HANDLE_FILE_INFORMATION,
    label: &str,
) -> Result<()> {
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        bail!("{label} 不能是 reparse point")
    }
    if info.nNumberOfLinks != 1 {
        bail!("{label} 必须只有一个 hard link")
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

fn canonical_private_root(root: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(root)
        .with_context(|| format!("team storage root 不存在: {}", root.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("team storage root 必须是非 symlink 目录")
    }
    set_private_directory_permissions(root)?;
    fs::canonicalize(root).context("无法解析 team storage root")
}

fn ensure_private_child(parent: &Path, name: &str) -> Result<PathBuf> {
    if name.is_empty()
        || Path::new(name).components().count() != 1
        || !matches!(
            Path::new(name).components().next(),
            Some(Component::Normal(_))
        )
    {
        bail!("私有 team 目录名称无效")
    }
    let child = parent.join(name);
    match fs::symlink_metadata(&child) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("拒绝使用 symlink team 目录")
        }
        Ok(metadata) if !metadata.is_dir() => bail!("team 私有路径不是目录"),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::create_dir(&child) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(&child)?;
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        bail!("并发创建的 team 私有路径不是安全目录")
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(error) => return Err(error.into()),
    }
    set_private_directory_permissions(&child)?;
    let canonical = fs::canonicalize(&child)?;
    if !canonical.starts_with(parent) {
        bail!("team 私有目录越过 storage root")
    }
    Ok(canonical)
}

fn set_private_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn set_private_open_file_permissions(file: &File) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = file;
    Ok(())
}

fn validate_text(label: &str, value: &str, maximum: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > maximum {
        bail!("{label} 为空或超过 {maximum} 字节限制")
    }
    Ok(())
}

fn validate_identifier(label: &str, value: &str, maximum: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        bail!("{label} 不是有效标识符或超过 {maximum} 字节限制")
    }
    Ok(())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKER_MODE_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_WORKER";
    const WORKER_WORKSPACE_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_WORKSPACE";
    const WORKER_STORAGE_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_STORAGE";
    const WORKER_READY_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_READY";
    const WORKER_GO_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_GO";
    const WORKER_OUTCOME_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_OUTCOME";
    const WORKER_TEAM_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_TEAM";
    const WORKER_MEMBER_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_MEMBER";
    const WORKER_LABEL_ENV: &str = "OPEN_AGENT_HARNESS_TEAM_TEST_LABEL";

    fn service_with_limits(
        limits: TeamLimits,
    ) -> (tempfile::TempDir, tempfile::TempDir, TeamService) {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let service = TeamService::create_in(
            workspace.path(),
            storage.path(),
            "audit-team",
            "coordinator",
            limits,
        )
        .unwrap();
        (workspace, storage, service)
    }

    fn service() -> (tempfile::TempDir, tempfile::TempDir, TeamService) {
        service_with_limits(TeamLimits::default())
    }

    fn add_member(service: &TeamService, name: &str) -> TeamMemberView {
        service
            .add_member(
                service.coordinator_id(),
                MemberSpec {
                    name: name.to_owned(),
                    custom_agent: Some("reviewer".to_owned()),
                    depth: 1,
                    requested_policy: AgentToolPolicy {
                        allowed_tools: Some(
                            ["Read".to_owned(), "Grep".to_owned()].into_iter().collect(),
                        ),
                        disallowed_tools: BTreeSet::new(),
                    },
                },
                &AgentToolPolicy {
                    allowed_tools: Some(
                        ["Read".to_owned(), "Grep".to_owned(), "Bash".to_owned()]
                            .into_iter()
                            .collect(),
                    ),
                    disallowed_tools: ["Bash".to_owned()].into_iter().collect(),
                },
            )
            .unwrap()
    }

    fn write_private_test_file(path: &Path, bytes: &[u8]) {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        set_private_open_file_permissions(&file).unwrap();
    }

    fn create_stale_team_temp(project: &Path) -> PathBuf {
        let path = project.join(format!(
            "{TEAM_TEMP_PREFIX}{}{TEAM_TEMP_SUFFIX}",
            Uuid::new_v4()
        ));
        write_private_test_file(&path, b"crash-debris");
        path
    }

    fn stale_team_temp_count(project: &Path) -> usize {
        fs::read_dir(project)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(parse_team_temp_name)
                    .is_some()
            })
            .count()
    }

    fn spawn_multiprocess_worker(
        mode: &str,
        workspace: &Path,
        storage: &Path,
        ready: &Path,
        go: &Path,
        outcome: &Path,
        extra_env: &[(&str, String)],
    ) -> std::process::Child {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .arg("--exact")
            .arg("agents::team::tests::multiprocess_worker")
            .arg("--ignored")
            .arg("--nocapture")
            .env(WORKER_MODE_ENV, mode)
            .env(WORKER_WORKSPACE_ENV, workspace)
            .env(WORKER_STORAGE_ENV, storage)
            .env(WORKER_READY_ENV, ready)
            .env(WORKER_GO_ENV, go)
            .env(WORKER_OUTCOME_ENV, outcome)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (name, value) in extra_env {
            command.env(name, value);
        }
        command.spawn().unwrap()
    }

    fn wait_for_workers_ready(ready: &[&Path]) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !ready.iter().all(|path| path.exists()) {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for team worker barrier"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn release_workers_when_ready(ready: &[&Path], go: &Path) {
        wait_for_workers_ready(ready);
        fs::write(go, b"go").unwrap();
    }

    fn assert_worker_success(child: std::process::Child) {
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "team worker failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn send_with_bounded_lock_retry(service: &TeamService, from: Uuid, to: Uuid, body: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match service.send(from, to, body) {
                Ok(_) => return,
                Err(error)
                    if error.to_string() == "team project lock acquisition timed out"
                        && Instant::now() < deadline =>
                {
                    // A timeout happens before the state is read or written, so retrying cannot
                    // duplicate a message. Windows file locks are not fair and one worker may
                    // otherwise reacquire the lock for all 64 sends before its peer gets a turn.
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("team worker send failed: {error:#}"),
            }
        }
    }

    #[test]
    #[ignore = "helper process launched by multiprocess team tests"]
    fn multiprocess_worker() {
        let Ok(mode) = std::env::var(WORKER_MODE_ENV) else {
            return;
        };
        let workspace = PathBuf::from(std::env::var_os(WORKER_WORKSPACE_ENV).unwrap());
        let storage = PathBuf::from(std::env::var_os(WORKER_STORAGE_ENV).unwrap());
        let ready = PathBuf::from(std::env::var_os(WORKER_READY_ENV).unwrap());
        let go = PathBuf::from(std::env::var_os(WORKER_GO_ENV).unwrap());
        let outcome = PathBuf::from(std::env::var_os(WORKER_OUTCOME_ENV).unwrap());
        if mode == "hold_lock" {
            let workspace = canonical_workspace(&workspace).unwrap();
            let storage = canonical_private_root(&storage).unwrap();
            let project = ensure_private_child(&storage, &workspace_key(&workspace)).unwrap();
            let project_lock = team_lock_for(&project);
            let _local = project_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let _file = lock_project_file(&project).unwrap();
            fs::write(&ready, b"ready").unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while !go.exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting to release team project lock"
                );
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            fs::write(outcome, b"ok").unwrap();
            return;
        }
        fs::write(&ready, b"ready").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !go.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for parent barrier"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        match mode.as_str() {
            "send" => {
                let team_id = std::env::var(WORKER_TEAM_ENV)
                    .unwrap()
                    .parse::<Uuid>()
                    .unwrap();
                let member_id = std::env::var(WORKER_MEMBER_ENV)
                    .unwrap()
                    .parse::<Uuid>()
                    .unwrap();
                let label = std::env::var(WORKER_LABEL_ENV).unwrap();
                let service = TeamService::open_in(&workspace, &storage, team_id).unwrap();
                let coordinator = service.coordinator_id();
                for index in 0..64 {
                    send_with_bounded_lock_retry(
                        &service,
                        coordinator,
                        member_id,
                        &format!("{label}-{index}"),
                    );
                    std::thread::yield_now();
                }
                fs::write(outcome, b"ok").unwrap();
            }
            "create" => {
                let result = TeamService::create_in_with_storage_limits(
                    &workspace,
                    &storage,
                    "concurrent",
                    "coordinator",
                    TeamLimits::default(),
                    TeamStorageLimits {
                        max_persistent_teams: 1,
                        max_total_state_bytes: HARD_MAX_WORKSPACE_TEAM_BYTES,
                    },
                );
                let value = match result {
                    Ok(_) => "ok".to_owned(),
                    Err(error) => format!("err:{error:#}"),
                };
                fs::write(outcome, value).unwrap();
            }
            other => panic!("unknown team worker mode: {other}"),
        }
    }

    #[test]
    fn coordinator_lifecycle_and_mailbox_persist() {
        let (workspace, storage, service) = service();
        let coordinator = service.coordinator_id();
        let member = add_member(&service, "reader");
        assert!(member.tool_policy.allows("Read"));
        assert!(!member.tool_policy.allows("Bash"));
        service.assign(coordinator, member.id, "Audit src").unwrap();
        let runtime_id = Uuid::new_v4();
        service
            .mark_running(coordinator, member.id, runtime_id)
            .unwrap();
        service
            .send(member.id, coordinator, "Found one issue")
            .unwrap();
        service
            .finish(member.id, member.id, true, "Audit complete")
            .unwrap();

        let mailbox = service
            .read_mailbox(coordinator, coordinator, 0, 10)
            .unwrap();
        assert_eq!(mailbox.len(), 2);
        let reopened =
            TeamService::open_in(workspace.path(), storage.path(), service.id()).unwrap();
        assert_eq!(reopened.snapshot(coordinator).unwrap().members.len(), 1);
        assert_eq!(
            reopened
                .read_mailbox(coordinator, coordinator, 0, 10)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn independently_opened_services_do_not_lose_updates() {
        let (workspace, storage, service) = service();
        let second_handle =
            TeamService::open_in(workspace.path(), storage.path(), service.id()).unwrap();
        let coordinator = service.coordinator_id();

        add_member(&service, "first");
        add_member(&second_handle, "second");

        let reopened =
            TeamService::open_in(workspace.path(), storage.path(), service.id()).unwrap();
        let names = reopened
            .list_members(coordinator)
            .unwrap()
            .into_iter()
            .map(|member| member.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            names,
            ["first".to_owned(), "second".to_owned()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn concurrent_processes_do_not_lose_mailbox_updates() {
        let (workspace, storage, service) = service();
        let member = add_member(&service, "receiver");
        let barrier = tempfile::tempdir().unwrap();
        let go = barrier.path().join("go");
        let left_ready = barrier.path().join("left-ready");
        let right_ready = barrier.path().join("right-ready");
        let left_outcome = barrier.path().join("left-outcome");
        let right_outcome = barrier.path().join("right-outcome");
        let common = [
            (WORKER_TEAM_ENV, service.id().to_string()),
            (WORKER_MEMBER_ENV, member.id.to_string()),
        ];
        let mut left_env = common.clone().to_vec();
        left_env.push((WORKER_LABEL_ENV, "left".to_owned()));
        let mut right_env = common.to_vec();
        right_env.push((WORKER_LABEL_ENV, "right".to_owned()));
        let left = spawn_multiprocess_worker(
            "send",
            workspace.path(),
            storage.path(),
            &left_ready,
            &go,
            &left_outcome,
            &left_env,
        );
        let right = spawn_multiprocess_worker(
            "send",
            workspace.path(),
            storage.path(),
            &right_ready,
            &go,
            &right_outcome,
            &right_env,
        );
        release_workers_when_ready(&[&left_ready, &right_ready], &go);
        assert_worker_success(left);
        assert_worker_success(right);
        assert_eq!(fs::read_to_string(left_outcome).unwrap(), "ok");
        assert_eq!(fs::read_to_string(right_outcome).unwrap(), "ok");

        let reopened =
            TeamService::open_in(workspace.path(), storage.path(), service.id()).unwrap();
        let messages = reopened
            .read_mailbox(member.id, member.id, 0, MAX_MESSAGE_READ)
            .unwrap();
        assert_eq!(messages.len(), 128);
        let bodies = messages
            .into_iter()
            .map(|message| message.body)
            .collect::<BTreeSet<_>>();
        let expected = (0..64)
            .flat_map(|index| [format!("left-{index}"), format!("right-{index}")])
            .collect::<BTreeSet<_>>();
        assert_eq!(bodies, expected);
    }

    #[test]
    fn concurrent_processes_cannot_bypass_workspace_team_quota() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let barrier = tempfile::tempdir().unwrap();
        let go = barrier.path().join("go");
        let left_ready = barrier.path().join("left-ready");
        let right_ready = barrier.path().join("right-ready");
        let left_outcome = barrier.path().join("left-outcome");
        let right_outcome = barrier.path().join("right-outcome");
        let left = spawn_multiprocess_worker(
            "create",
            workspace.path(),
            storage.path(),
            &left_ready,
            &go,
            &left_outcome,
            &[],
        );
        let right = spawn_multiprocess_worker(
            "create",
            workspace.path(),
            storage.path(),
            &right_ready,
            &go,
            &right_outcome,
            &[],
        );
        release_workers_when_ready(&[&left_ready, &right_ready], &go);
        assert_worker_success(left);
        assert_worker_success(right);

        let outcomes = [left_outcome, right_outcome].map(|path| fs::read_to_string(path).unwrap());
        assert_eq!(outcomes.iter().filter(|value| *value == "ok").count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|value| value.starts_with("err:") && value.contains("达到 1 个限制"))
                .count(),
            1
        );

        let workspace = canonical_workspace(workspace.path()).unwrap();
        let storage = canonical_private_root(storage.path()).unwrap();
        let project = ensure_private_child(&storage, &workspace_key(&workspace)).unwrap();
        let project_lock = team_lock_for(&project);
        let _local = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file = lock_project_file(&project).unwrap();
        assert_eq!(workspace_team_state_paths(&project).unwrap().len(), 1);
    }

    #[test]
    fn held_project_lock_times_out_without_path_leak_and_recovers() {
        let (workspace, storage, service) = service();
        let barrier = tempfile::tempdir().unwrap();
        let ready = barrier.path().join("holder-ready");
        let release = barrier.path().join("release-holder");
        let outcome = barrier.path().join("holder-outcome");
        let holder = spawn_multiprocess_worker(
            "hold_lock",
            workspace.path(),
            storage.path(),
            &ready,
            &release,
            &outcome,
            &[],
        );
        wait_for_workers_ready(&[&ready]);

        let started = Instant::now();
        let result = service.snapshot(service.coordinator_id());
        let elapsed = started.elapsed();

        // Always release and reap the child before asserting, so a failed assertion cannot leave a
        // helper process or its temporary workspace behind.
        fs::write(&release, b"release").unwrap();
        assert_worker_success(holder);
        assert_eq!(fs::read_to_string(outcome).unwrap(), "ok");

        let error = format!("{:#}", result.unwrap_err());
        assert!(error.contains("team project lock acquisition timed out"));
        assert!(!error.contains(&workspace.path().to_string_lossy().to_string()));
        assert!(elapsed >= PROJECT_LOCK_TIMEOUT);
        assert!(elapsed < PROJECT_LOCK_TIMEOUT + Duration::from_secs(3));
        assert!(service.snapshot(service.coordinator_id()).is_ok());
    }

    #[test]
    fn non_coordinator_cannot_manage_team_or_other_mailboxes() {
        let (_workspace, _storage, service) = service();
        let first = add_member(&service, "first");
        let second = add_member(&service, "second");
        assert!(service.assign(first.id, second.id, "unauthorized").is_err());
        assert!(service.read_mailbox(first.id, second.id, 0, 10).is_err());
        assert!(service.stop_member(first.id, second.id).is_err());
        assert!(service.shutdown(first.id).is_err());
    }

    #[test]
    fn limits_and_duplicate_runtime_ids_fail_closed() {
        let limits = TeamLimits {
            max_members: 2,
            max_running: 1,
            max_total_assignments: 2,
            ..TeamLimits::default()
        };
        let (_workspace, _storage, service) = service_with_limits(limits);
        let coordinator = service.coordinator_id();
        let first = add_member(&service, "first");
        let second = add_member(&service, "second");
        assert!(
            service
                .add_member(
                    coordinator,
                    MemberSpec {
                        name: "third".to_owned(),
                        custom_agent: None,
                        depth: 1,
                        requested_policy: AgentToolPolicy::default(),
                    },
                    &AgentToolPolicy::default(),
                )
                .is_err()
        );
        service.assign(coordinator, first.id, "first task").unwrap();
        service
            .assign(coordinator, second.id, "second task")
            .unwrap();
        let runtime = Uuid::new_v4();
        service
            .mark_running(coordinator, first.id, runtime)
            .unwrap();
        assert!(
            service
                .mark_running(coordinator, second.id, runtime)
                .is_err()
        );
        assert!(
            service
                .assign(coordinator, second.id, "third task")
                .is_err()
        );
    }

    #[test]
    fn start_failure_is_failed_and_member_can_be_reassigned() {
        let (_workspace, _storage, service) = service();
        let coordinator = service.coordinator_id();
        let member = add_member(&service, "retryable");
        service
            .assign(coordinator, member.id, "first task")
            .unwrap();
        let failed = service.mark_start_failed(coordinator, member.id).unwrap();
        assert_eq!(failed.status, MemberStatus::Failed);
        assert_eq!(failed.runtime_agent_id, None);

        let retried = service
            .assign(coordinator, member.id, "retry task")
            .unwrap();
        assert_eq!(retried.member.status, MemberStatus::Assigned);
        assert_eq!(retried.prompt, "retry task");
    }

    #[test]
    fn mailbox_bounds_and_acknowledgement() {
        let limits = TeamLimits {
            max_messages: 2,
            max_message_bytes: 8,
            max_mailbox_bytes: 12,
            ..TeamLimits::default()
        };
        let (_workspace, _storage, service) = service_with_limits(limits);
        let coordinator = service.coordinator_id();
        let member = add_member(&service, "reader");
        let first = service.send(coordinator, member.id, "123456").unwrap();
        service.send(coordinator, member.id, "abcdef").unwrap();
        assert!(service.send(coordinator, member.id, "x").is_err());
        assert_eq!(
            service
                .acknowledge(member.id, member.id, first.sequence)
                .unwrap(),
            1
        );
        service.send(coordinator, member.id, "x").unwrap();
    }

    #[test]
    fn corrupt_and_symlink_state_are_rejected() {
        let (workspace, storage, service) = service();
        fs::write(service.storage_path(), b"not-json").unwrap();
        assert!(TeamService::open_in(workspace.path(), storage.path(), service.id()).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let target = storage.path().join("target.json");
            fs::write(&target, b"{}").unwrap();
            fs::remove_file(service.storage_path()).unwrap();
            symlink(target, service.storage_path()).unwrap();
            assert!(TeamService::open_in(workspace.path(), storage.path(), service.id()).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn hardlinked_state_and_project_lock_are_rejected() {
        use std::os::unix::fs::OpenOptionsExt;

        let (_workspace, storage, service) = service();
        let coordinator = service.coordinator_id();
        let project = service.storage_path().parent().unwrap();
        let state_alias = project.join(format!("{}.json", Uuid::new_v4()));
        fs::hard_link(service.storage_path(), &state_alias).unwrap();
        assert!(service.snapshot(coordinator).is_err());
        fs::remove_file(state_alias).unwrap();
        assert!(service.snapshot(coordinator).is_ok());

        let lockfile = project.join(PROJECT_LOCK_FILE);
        fs::remove_file(&lockfile).unwrap();
        let outside = storage.path().join("outside-lock");
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&outside)
            .unwrap();
        fs::hard_link(&outside, &lockfile).unwrap();
        assert!(service.snapshot(coordinator).is_err());
    }

    #[test]
    fn workspace_storage_rejects_unknown_or_inexact_temporary_entries() {
        let (_workspace, _storage, service) = service();
        let project = service.storage_path().parent().unwrap();
        let project_lock = team_lock_for(project);
        let _local = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file = lock_project_file(project).unwrap();

        let unknown = project.join("unexpected.entry");
        fs::write(&unknown, b"unexpected").unwrap();
        assert!(workspace_team_state_paths(project).is_err());
        fs::remove_file(&unknown).unwrap();

        let inexact_temp = project.join(".open-agent-harness-team-not-a-uuid.tmp");
        fs::write(&inexact_temp, b"temporary").unwrap();
        assert!(workspace_team_state_paths(project).is_err());
    }

    #[test]
    fn workspace_scan_cleans_valid_stale_temporary_state() {
        let (_workspace, _storage, service) = service();
        let project = service.storage_path().parent().unwrap();
        let stale = create_stale_team_temp(project);
        let project_lock = team_lock_for(project);
        let _local = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file = lock_project_file(project).unwrap();

        let paths = workspace_team_state_paths(project).unwrap();
        assert_eq!(paths, vec![service.storage_path().to_path_buf()]);
        assert!(!stale.exists());
    }

    #[test]
    fn stale_temporary_cleanup_is_bounded_per_scan() {
        let (_workspace, _storage, service) = service();
        let project = service.storage_path().parent().unwrap();
        for _ in 0..=MAX_STALE_TEAM_TEMPS_PER_SCAN {
            create_stale_team_temp(project);
        }
        let project_lock = team_lock_for(project);
        let _local = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file = lock_project_file(project).unwrap();

        assert_eq!(workspace_team_state_paths(project).unwrap().len(), 1);
        assert_eq!(stale_team_temp_count(project), 1);
        assert_eq!(workspace_team_state_paths(project).unwrap().len(), 1);
        assert_eq!(stale_team_temp_count(project), 0);
    }

    #[cfg(unix)]
    #[test]
    fn unsafe_stale_temporary_state_prevents_partial_cleanup() {
        let (_workspace, storage, service) = service();
        let project = service.storage_path().parent().unwrap();
        let safe_stale = create_stale_team_temp(project);
        let source = storage.path().join("hardlink-source");
        write_private_test_file(&source, b"unsafe");
        let hardlinked_stale = project.join(format!(
            "{TEAM_TEMP_PREFIX}{}{TEAM_TEMP_SUFFIX}",
            Uuid::new_v4()
        ));
        fs::hard_link(source, hardlinked_stale).unwrap();
        let project_lock = team_lock_for(project);
        let _local = project_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _file = lock_project_file(project).unwrap();

        assert!(workspace_team_state_paths(project).is_err());
        assert!(safe_stale.exists());
    }

    #[cfg(unix)]
    #[test]
    fn storage_permissions_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let (_workspace, _storage, service) = service();
        assert_eq!(
            fs::metadata(service.storage_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(service.storage_path().parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(
                service
                    .storage_path()
                    .parent()
                    .unwrap()
                    .join(PROJECT_LOCK_FILE)
            )
            .unwrap()
            .permissions()
            .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn shutdown_returns_running_agent_ids_and_is_idempotent() {
        let (_workspace, _storage, service) = service();
        let coordinator = service.coordinator_id();
        let member = add_member(&service, "reader");
        service.assign(coordinator, member.id, "task").unwrap();
        let runtime = Uuid::new_v4();
        service
            .mark_running(coordinator, member.id, runtime)
            .unwrap();
        assert_eq!(service.shutdown(coordinator).unwrap(), vec![runtime]);
        assert!(service.shutdown(coordinator).unwrap().is_empty());
        assert!(service.send(coordinator, member.id, "closed").is_err());
    }

    #[test]
    fn delete_requires_coordinator_and_closed_team_then_removes_state() {
        let (workspace, storage, service) = service();
        let coordinator = service.coordinator_id();
        let member = add_member(&service, "reader");

        assert!(service.delete(member.id).is_err());
        assert!(service.delete(coordinator).is_err());
        assert!(service.storage_path().exists());

        service.shutdown(coordinator).unwrap();
        service.delete(coordinator).unwrap();
        assert!(!service.storage_path().exists());
        assert!(TeamService::open_in(workspace.path(), storage.path(), service.id()).is_err());
    }

    #[test]
    fn workspace_team_count_and_byte_quotas_fail_closed() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let one_team = TeamStorageLimits {
            max_persistent_teams: 1,
            max_total_state_bytes: HARD_MAX_WORKSPACE_TEAM_BYTES,
        };
        let first = TeamService::create_in_with_storage_limits(
            workspace.path(),
            storage.path(),
            "first",
            "coordinator",
            TeamLimits::default(),
            one_team,
        )
        .unwrap();
        assert!(
            TeamService::create_in_with_storage_limits(
                workspace.path(),
                storage.path(),
                "second",
                "coordinator",
                TeamLimits::default(),
                one_team,
            )
            .is_err()
        );

        first.shutdown(first.coordinator_id()).unwrap();
        first.delete(first.coordinator_id()).unwrap();
        TeamService::create_in_with_storage_limits(
            workspace.path(),
            storage.path(),
            "replacement",
            "coordinator",
            TeamLimits::default(),
            one_team,
        )
        .unwrap();

        let other_workspace = tempfile::tempdir().unwrap();
        assert!(
            TeamService::create_in_with_storage_limits(
                other_workspace.path(),
                storage.path(),
                "too-large",
                "coordinator",
                TeamLimits::default(),
                TeamStorageLimits {
                    max_persistent_teams: 1,
                    max_total_state_bytes: 1,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn workspace_byte_quota_is_rechecked_on_every_state_growth() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let service = TeamService::create_in_with_storage_limits(
            workspace.path(),
            storage.path(),
            "bounded",
            "coordinator",
            TeamLimits::default(),
            TeamStorageLimits {
                max_persistent_teams: 4,
                max_total_state_bytes: 4 * 1024,
            },
        )
        .unwrap();
        let member = add_member(&service, "reader");
        assert!(
            service
                .send(service.coordinator_id(), member.id, &"x".repeat(4 * 1024))
                .is_err()
        );
        assert!(
            service
                .read_mailbox(member.id, member.id, 0, 10)
                .unwrap()
                .is_empty()
        );
        let reopened =
            TeamService::open_in(workspace.path(), storage.path(), service.id()).unwrap();
        assert!(
            reopened
                .read_mailbox(member.id, member.id, 0, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn gc_removes_only_closed_teams_with_a_bounded_batch() {
        let workspace = tempfile::tempdir().unwrap();
        let storage = tempfile::tempdir().unwrap();
        let limits = TeamStorageLimits {
            max_persistent_teams: 4,
            max_total_state_bytes: HARD_MAX_WORKSPACE_TEAM_BYTES,
        };
        let closed = TeamService::create_in_with_storage_limits(
            workspace.path(),
            storage.path(),
            "closed",
            "coordinator",
            TeamLimits::default(),
            limits,
        )
        .unwrap();
        let open = TeamService::create_in_with_storage_limits(
            workspace.path(),
            storage.path(),
            "open",
            "coordinator",
            TeamLimits::default(),
            limits,
        )
        .unwrap();
        closed.shutdown(closed.coordinator_id()).unwrap();

        let result = TeamService::gc_closed_in(workspace.path(), storage.path(), 1).unwrap();
        assert_eq!(result.deleted_team_ids, vec![closed.id()]);
        assert_eq!(result.remaining_teams, 1);
        assert!(result.remaining_state_bytes > 0);
        assert!(TeamService::open_in(workspace.path(), storage.path(), closed.id()).is_err());
        assert!(TeamService::open_in(workspace.path(), storage.path(), open.id()).is_ok());
    }
}

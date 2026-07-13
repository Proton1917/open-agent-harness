use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex as StdMutex, RwLock, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot, watch},
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};
use uuid::Uuid;

use crate::{
    api::ModelClient,
    config::Settings,
    query::{QueryEngine, QueryOptions},
    tools::{Tool, ToolContext, ToolOutput, ToolRegistry, object_schema},
    types::{Message, SessionUsage},
};

const MAX_AGENT_PROMPT_BYTES: usize = 1024 * 1024;
const MAX_AGENT_DESCRIPTION_BYTES: usize = 2048;
const MAX_AGENT_MODEL_BYTES: usize = 256;
const MAX_AGENT_HISTORY_BYTES: usize = 2 * 1024 * 1024;
const MAX_AGENT_HISTORIES: usize = 32;
const MIN_AGENT_TIMEOUT_MS: u64 = 1_000;
const MAX_AGENT_TIMEOUT_MS: u64 = 3_600_000;
const AGENT_CANCEL_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
pub struct AgentLimits {
    max_depth: usize,
    max_concurrent: usize,
    max_total: usize,
    max_background: usize,
    default_timeout_ms: u64,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            max_depth: 3,
            max_concurrent: 4,
            max_total: 64,
            max_background: 16,
            default_timeout_ms: 900_000,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawAgentLimits {
    max_depth: Option<usize>,
    max_concurrent: Option<usize>,
    max_total: Option<usize>,
    max_background: Option<usize>,
    default_timeout_ms: Option<u64>,
}

impl AgentLimits {
    pub fn from_settings(settings: &Settings) -> Result<Self> {
        let Some(raw) = settings.raw.get("agents") else {
            return Ok(Self::default());
        };
        let raw: RawAgentLimits =
            serde_json::from_value(raw.clone()).context("agents settings 无效")?;
        let defaults = Self::default();
        Ok(Self {
            max_depth: raw.max_depth.unwrap_or(defaults.max_depth).clamp(1, 8),
            max_concurrent: raw
                .max_concurrent
                .unwrap_or(defaults.max_concurrent)
                .clamp(1, 16),
            max_total: raw.max_total.unwrap_or(defaults.max_total).clamp(1, 256),
            max_background: raw
                .max_background
                .unwrap_or(defaults.max_background)
                .clamp(1, 64),
            default_timeout_ms: raw
                .default_timeout_ms
                .unwrap_or(defaults.default_timeout_ms)
                .clamp(MIN_AGENT_TIMEOUT_MS, MAX_AGENT_TIMEOUT_MS),
        })
    }
}

pub struct AgentIntegration {
    pub deferred_tools: Vec<Arc<dyn Tool>>,
    pub limits: AgentLimits,
}

pub fn configure_agents(settings: &Settings) -> Result<AgentIntegration> {
    Ok(AgentIntegration {
        deferred_tools: vec![
            Arc::new(AgentTool),
            Arc::new(AgentOutputTool),
            Arc::new(AgentStopTool),
        ],
        limits: AgentLimits::from_settings(settings)?,
    })
}

pub(crate) struct AgentRuntime {
    client: ModelClient,
    registry: ToolRegistry,
    model: RwLock<String>,
    max_tokens: u32,
    system: String,
    debug: bool,
    limits: AgentLimits,
    slots: Arc<Semaphore>,
    total_started: AtomicUsize,
    active_ids: StdMutex<HashSet<Uuid>>,
    jobs: Mutex<HashMap<Uuid, BackgroundAgent>>,
    histories: Mutex<HistoryStore>,
}

struct BackgroundAgent {
    description: String,
    launch_token: Uuid,
    cancel: Option<oneshot::Sender<()>>,
    result: watch::Receiver<Option<Arc<ToolOutput>>>,
    handle: JoinHandle<()>,
    _reservation: Arc<ActiveAgentReservation>,
}

struct ActiveAgentReservation {
    runtime: Weak<AgentRuntime>,
    id: Uuid,
}

impl Drop for ActiveAgentReservation {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.upgrade() else {
            return;
        };
        runtime
            .active_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.id);
    }
}

struct ForegroundAgentRun {
    cancel: Option<oneshot::Sender<()>>,
    handle: JoinHandle<Result<AgentRun>>,
}

enum Controlled<T> {
    Completed(T),
    Cancelled,
    TimedOut,
}

impl ForegroundAgentRun {
    async fn wait(mut self) -> Result<AgentRun> {
        let result = (&mut self.handle)
            .await
            .context("foreground agent task failed")?;
        self.cancel.take();
        result
    }
}

impl Drop for ForegroundAgentRun {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

#[derive(Clone)]
struct AgentSnapshot {
    messages: Vec<Message>,
}

#[derive(Default)]
struct HistoryStore {
    values: HashMap<Uuid, AgentSnapshot>,
    order: VecDeque<Uuid>,
}

struct AgentRun {
    id: Uuid,
    text: String,
    messages: Vec<Message>,
    usage: SessionUsage,
}

struct AgentRunRequest {
    id: Uuid,
    context: ToolContext,
    prompt: String,
    history: Vec<Message>,
    model: String,
    max_tokens: u32,
    depth: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentInput {
    prompt: String,
    description: Option<String>,
    model: Option<String>,
    #[serde(default)]
    run_in_background: bool,
    resume: Option<String>,
    timeout_ms: Option<u64>,
    max_tokens: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentOutputInput {
    agent_id: String,
    #[serde(default)]
    wait: bool,
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentStopInput {
    agent_id: String,
}

impl AgentRuntime {
    pub(crate) fn new(
        client: ModelClient,
        registry: ToolRegistry,
        model: String,
        max_tokens: u32,
        system: String,
        debug: bool,
        limits: AgentLimits,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            registry,
            model: RwLock::new(model),
            max_tokens,
            system,
            debug,
            limits,
            slots: Arc::new(Semaphore::new(limits.max_concurrent)),
            total_started: AtomicUsize::new(0),
            active_ids: StdMutex::new(HashSet::new()),
            jobs: Mutex::new(HashMap::new()),
            histories: Mutex::new(HistoryStore::default()),
        })
    }

    pub(crate) fn set_default_model(&self, model: String) {
        *self
            .model
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = model;
    }

    fn default_model(&self) -> String {
        self.model
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn reserve_active(self: &Arc<Self>, id: Uuid) -> Result<Arc<ActiveAgentReservation>> {
        let mut active = self
            .active_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(id) {
            bail!("agent 已经在运行或结果尚未读取: {id}")
        }
        Ok(Arc::new(ActiveAgentReservation {
            runtime: Arc::downgrade(self),
            id,
        }))
    }

    async fn start(
        self: &Arc<Self>,
        parent: &ToolContext,
        input: AgentInput,
    ) -> Result<ToolOutput> {
        self.validate_start(parent, &input)?;
        let id = input
            .resume
            .as_deref()
            .map(parse_agent_id)
            .transpose()?
            .unwrap_or_else(Uuid::new_v4);
        let history = if input.resume.is_some() {
            self.histories
                .lock()
                .await
                .values
                .get(&id)
                .cloned()
                .with_context(|| format!("agent history 不存在: {id}"))?
                .messages
        } else {
            Vec::new()
        };
        let model = input.model.unwrap_or_else(|| self.default_model());
        let max_tokens = input
            .max_tokens
            .unwrap_or(self.max_tokens)
            .min(self.max_tokens);
        if max_tokens == 0 {
            bail!("agent maxTokens 必须大于 0")
        }
        let description = input
            .description
            .unwrap_or_else(|| truncate_text(&input.prompt, 120).to_owned());
        let timeout_ms = input
            .timeout_ms
            .unwrap_or(self.limits.default_timeout_ms)
            .clamp(MIN_AGENT_TIMEOUT_MS, MAX_AGENT_TIMEOUT_MS);
        let context = parent.fork_for_agent();
        let prompt = input.prompt;
        let depth = context.agent_depth();
        let acquire_slot = input.run_in_background || parent.agent_depth() == 0;

        if input.run_in_background {
            let mut jobs = self.jobs.lock().await;
            if jobs.len() >= self.limits.max_background {
                bail!(
                    "background agent 达到 {} 个限制",
                    self.limits.max_background
                )
            }
            let reservation = self.reserve_active(id)?;
            self.reserve_start()?;
            let request = AgentRunRequest {
                id,
                context,
                prompt,
                history,
                model,
                max_tokens,
                depth,
            };
            let (cancel, cancel_rx) = oneshot::channel();
            let (result_tx, result) = watch::channel(None);
            let runtime = Arc::clone(self);
            let task_reservation = Arc::clone(&reservation);
            let handle = tokio::spawn(async move {
                let output = match runtime
                    .run_controlled(
                        request,
                        timeout_ms,
                        acquire_slot,
                        cancel_rx,
                        task_reservation,
                    )
                    .await
                {
                    Ok(run) => {
                        runtime.store_snapshot(&run).await;
                        render_agent_run(&run)
                    }
                    Err(error) => ToolOutput::error(format!("Agent {id} failed: {error:#}")),
                };
                let _ = result_tx.send(Some(Arc::new(output)));
            });
            jobs.insert(
                id,
                BackgroundAgent {
                    description: description.clone(),
                    launch_token: Uuid::new_v4(),
                    cancel: Some(cancel),
                    result,
                    handle,
                    _reservation: reservation,
                },
            );
            return Ok(ToolOutput::success(format!(
                "Agent running in background\nagent_id={id}\ndescription={description}"
            )));
        }

        let reservation = self.reserve_active(id)?;
        self.reserve_start()?;
        let request = AgentRunRequest {
            id,
            context,
            prompt,
            history,
            model,
            max_tokens,
            depth,
        };
        let (cancel, cancel_rx) = oneshot::channel();
        let runtime = Arc::clone(self);
        let handle = tokio::spawn(async move {
            runtime
                .run_controlled(request, timeout_ms, acquire_slot, cancel_rx, reservation)
                .await
        });
        let result = ForegroundAgentRun {
            cancel: Some(cancel),
            handle,
        }
        .wait()
        .await?;
        self.store_snapshot(&result).await;
        Ok(render_agent_run(&result))
    }

    fn validate_start(&self, parent: &ToolContext, input: &AgentInput) -> Result<()> {
        if input.prompt.trim().is_empty() || input.prompt.len() > MAX_AGENT_PROMPT_BYTES {
            bail!("agent prompt 为空或超过 {MAX_AGENT_PROMPT_BYTES} 字节限制")
        }
        if input
            .description
            .as_ref()
            .is_some_and(|value| value.len() > MAX_AGENT_DESCRIPTION_BYTES)
        {
            bail!("agent description 超过 {MAX_AGENT_DESCRIPTION_BYTES} 字节限制")
        }
        if input
            .model
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_AGENT_MODEL_BYTES)
        {
            bail!("agent model 为空或过长")
        }
        if parent.agent_depth() >= self.limits.max_depth {
            bail!("agent recursion 达到 {} 层限制", self.limits.max_depth)
        }
        if self.total_started.load(Ordering::Acquire) >= self.limits.max_total {
            bail!("agent session 达到 {} 次启动限制", self.limits.max_total)
        }
        Ok(())
    }

    async fn acquire_slot(&self) -> Result<OwnedSemaphorePermit> {
        Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .context("agent scheduler 已关闭")
    }

    async fn run_controlled(
        self: &Arc<Self>,
        request: AgentRunRequest,
        timeout_ms: u64,
        acquire_slot: bool,
        mut cancel: oneshot::Receiver<()>,
        _reservation: Arc<ActiveAgentReservation>,
    ) -> Result<AgentRun> {
        let id = request.id;
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        let _permit = if acquire_slot {
            Some(tokio::select! {
                result = self.acquire_slot() => result?,
                _ = &mut cancel => bail!("agent {id} 已取消"),
                _ = sleep_until(deadline) => {
                    bail!("agent {id} 超过 {timeout_ms}ms timeout（包含调度等待）")
                }
            })
        } else {
            None
        };
        self.run_once(request, deadline, timeout_ms, &mut cancel)
            .await
    }

    fn reserve_start(&self) -> Result<()> {
        let mut current = self.total_started.load(Ordering::Acquire);
        loop {
            if current >= self.limits.max_total {
                bail!("agent session 达到 {} 次启动限制", self.limits.max_total)
            }
            match self.total_started.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    async fn run_once(
        &self,
        request: AgentRunRequest,
        deadline: Instant,
        timeout_ms: u64,
        cancel: &mut oneshot::Receiver<()>,
    ) -> Result<AgentRun> {
        let AgentRunRequest {
            id,
            context,
            prompt,
            history,
            model,
            max_tokens,
            depth,
        } = request;
        let mut system = self.system.clone();
        let hooks = context.hooks();
        let hook_cwd = context.cwd();
        let start_hook = tokio::select! {
            result = hooks.run(
                "SubagentStart",
                None,
                json!({"agent_id": id, "depth": depth, "prompt": &prompt}),
                &hook_cwd,
            ) => result?,
            _ = &mut *cancel => bail!("agent {id} 已取消"),
            _ = sleep_until(deadline) => {
                bail!("agent {id} 超过 {timeout_ms}ms timeout")
            }
        };
        system.push_str(&format!(
            "\n\nYou are a delegated local coding agent at recursion depth {depth}. Work only on the assigned prompt, preserve the shared workspace, and return a concrete result to the parent agent."
        ));
        if !start_hook.additional_context.is_empty() {
            system.push_str("\n\n<subagent-start-hook-context>\n");
            system.push_str(&start_hook.additional_context.join("\n"));
            system.push_str("\n</subagent-start-hook-context>");
        }
        let mut engine = QueryEngine::new(
            self.client.clone(),
            self.registry.clone(),
            context,
            QueryOptions {
                model,
                max_tokens,
                system,
                messages: history,
                debug: self.debug,
                text_delta_sink: None,
                compact_config: None,
            },
        );
        let descendant_checkpoint = self.background_checkpoint().await;
        let outcome = {
            let turn = engine.run_turn(prompt);
            tokio::pin!(turn);
            tokio::select! {
                result = &mut turn => Controlled::Completed(result),
                _ = &mut *cancel => Controlled::Cancelled,
                _ = sleep_until(deadline) => Controlled::TimedOut,
            }
        };
        let (mut result, forced_cleanup) = match outcome {
            Controlled::Completed(Ok(turn)) => (
                Ok(AgentRun {
                    id,
                    text: turn.text,
                    messages: engine.messages.clone(),
                    usage: engine.usage.clone(),
                }),
                false,
            ),
            Controlled::Completed(Err(error)) => (Err(error), false),
            Controlled::Cancelled => (Err(anyhow::anyhow!("agent {id} 已取消")), true),
            Controlled::TimedOut => (
                Err(anyhow::anyhow!("agent {id} 超过 {timeout_ms}ms timeout")),
                true,
            ),
        };
        if forced_cleanup {
            self.rollback_new_background(&descendant_checkpoint).await;
        }
        engine.shutdown().await;
        match &mut result {
            Ok(run) => {
                let stop_hook = hooks
                    .run(
                        "SubagentStop",
                        None,
                        json!({"agent_id": id, "depth": depth, "success": true}),
                        &hook_cwd,
                    )
                    .await;
                if let Ok(outcome) = stop_hook {
                    if !outcome.additional_context.is_empty() {
                        run.text.push_str("\n\n[Subagent stop hook context]\n");
                        run.text.push_str(&outcome.additional_context.join("\n"));
                    }
                }
            }
            Err(error) => {
                let _ = hooks
                    .run(
                        "SubagentStop",
                        None,
                        json!({"agent_id": id, "depth": depth, "success": false, "error": format!("{error:#}")}),
                        &hook_cwd,
                    )
                    .await;
            }
        }
        result
    }

    async fn store_snapshot(&self, run: &AgentRun) {
        let Ok(encoded) = serde_json::to_vec(&run.messages) else {
            return;
        };
        if encoded.len() > MAX_AGENT_HISTORY_BYTES {
            return;
        }
        let mut histories = self.histories.lock().await;
        if !histories.values.contains_key(&run.id) {
            histories.order.push_back(run.id);
        }
        histories.values.insert(
            run.id,
            AgentSnapshot {
                messages: run.messages.clone(),
            },
        );
        while histories.order.len() > MAX_AGENT_HISTORIES {
            if let Some(id) = histories.order.pop_front() {
                histories.values.remove(&id);
            }
        }
    }

    async fn output(&self, input: AgentOutputInput) -> Result<ToolOutput> {
        let id = parse_agent_id(&input.agent_id)?;
        let jobs = self.jobs.lock().await;
        let Some(job) = jobs.get(&id) else {
            drop(jobs);
            if self.histories.lock().await.values.contains_key(&id) {
                return Ok(ToolOutput::success(format!(
                    "Agent {id} completed earlier; use Agent with resume={id} to continue it"
                )));
            }
            bail!("background agent 不存在: {id}")
        };
        let mut result = job.result.clone();
        let description = job.description.clone();
        let launch_token = job.launch_token;
        let handle_finished = job.handle.is_finished();
        drop(jobs);

        let current = result.borrow().clone();
        if !input.wait && current.is_none() && !handle_finished {
            return Ok(ToolOutput::success(format!(
                "Agent still running\nagent_id={id}\ndescription={description}"
            )));
        }
        let output = if let Some(output) = current {
            (*output).clone()
        } else if input.wait {
            let wait_ms = input
                .timeout_ms
                .unwrap_or(30_000)
                .clamp(1, MAX_AGENT_TIMEOUT_MS);
            match timeout(
                Duration::from_millis(wait_ms),
                wait_for_background_result(&mut result, id),
            )
            .await
            {
                Ok(output) => output,
                Err(_) => {
                    return Ok(ToolOutput::success(format!(
                        "Agent still running after {wait_ms}ms\nagent_id={id}"
                    )));
                }
            }
        } else {
            wait_for_background_result(&mut result, id).await
        };

        let completed = {
            let mut jobs = self.jobs.lock().await;
            if jobs
                .get(&id)
                .is_some_and(|job| job.launch_token == launch_token)
            {
                jobs.remove(&id)
            } else {
                None
            }
        };
        if let Some(job) = completed {
            let _ = job.handle.await;
        }
        Ok(output)
    }

    async fn stop(&self, id: Uuid) -> Result<ToolOutput> {
        let Some(job) = self.jobs.lock().await.remove(&id) else {
            bail!("background agent 不存在: {id}")
        };
        cancel_background_job(job).await;
        Ok(ToolOutput::success(format!("Stopped agent {id}")))
    }

    pub(crate) async fn task_output_alias(
        &self,
        agent_id: &str,
        wait: bool,
        timeout_ms: u64,
    ) -> Result<ToolOutput> {
        self.output(AgentOutputInput {
            agent_id: agent_id.to_owned(),
            wait,
            timeout_ms: Some(timeout_ms),
        })
        .await
    }

    pub(crate) async fn task_stop_alias(&self, agent_id: &str) -> Result<ToolOutput> {
        self.stop(parse_agent_id(agent_id)?).await
    }

    pub(crate) async fn background_checkpoint(&self) -> HashMap<Uuid, Uuid> {
        self.jobs
            .lock()
            .await
            .iter()
            .map(|(id, job)| (*id, job.launch_token))
            .collect()
    }

    pub(crate) async fn rollback_new_background(&self, keep: &HashMap<Uuid, Uuid>) {
        let jobs = {
            let mut jobs = self.jobs.lock().await;
            let ids = jobs
                .iter()
                .filter(|(id, job)| keep.get(id) != Some(&job.launch_token))
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| jobs.remove(&id))
                .collect::<Vec<_>>()
        };
        for job in jobs {
            cancel_background_job(job).await;
        }
    }

    pub(crate) async fn shutdown_all(&self) {
        let jobs = self
            .jobs
            .lock()
            .await
            .drain()
            .map(|(_, job)| job)
            .collect::<Vec<_>>();
        for job in jobs {
            cancel_background_job(job).await;
        }
    }
}

async fn wait_for_background_result(
    result: &mut watch::Receiver<Option<Arc<ToolOutput>>>,
    id: Uuid,
) -> ToolOutput {
    loop {
        if let Some(output) = result.borrow().clone() {
            return (*output).clone();
        }
        if result.changed().await.is_err() {
            return ToolOutput::error(format!("Agent {id} task ended before publishing a result"));
        }
    }
}

async fn cancel_background_job(mut job: BackgroundAgent) {
    if let Some(cancel) = job.cancel.take() {
        let _ = cancel.send(());
    }
    if timeout(AGENT_CANCEL_GRACE, &mut job.handle).await.is_err() {
        job.handle.abort();
        let _ = job.handle.await;
    }
}

struct AgentTool;
struct AgentOutputTool;
struct AgentStopTool;

#[async_trait]
impl Tool for AgentTool {
    fn name(&self) -> &str {
        "Agent"
    }

    fn description(&self) -> &str {
        "Delegates a bounded task to a local subagent with an independent message history and the same audited tool and permission boundaries. Supports foreground, background, and resume."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "prompt": {"type": "string", "minLength": 1, "maxLength": MAX_AGENT_PROMPT_BYTES},
                "description": {"type": "string", "maxLength": MAX_AGENT_DESCRIPTION_BYTES},
                "model": {"type": "string", "minLength": 1, "maxLength": MAX_AGENT_MODEL_BYTES},
                "runInBackground": {"type": "boolean"},
                "resume": {"type": "string", "maxLength": 64},
                "timeoutMs": {"type": "integer", "minimum": MIN_AGENT_TIMEOUT_MS, "maximum": MAX_AGENT_TIMEOUT_MS},
                "maxTokens": {"type": "integer", "minimum": 1}
            }),
            &["prompt"],
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
            .get("description")
            .or_else(|| input.get("prompt"))
            .and_then(Value::as_str)
            .map(|value| truncate_text(value, 200).to_owned())
            .unwrap_or_else(|| "<agent>".to_owned())
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: AgentInput = serde_json::from_value(input)?;
        context.agent_runtime()?.start(context, input).await
    }
}

#[async_trait]
impl Tool for AgentOutputTool {
    fn name(&self) -> &str {
        "AgentOutput"
    }

    fn description(&self) -> &str {
        "Reads the status or final result of a background local subagent."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "agentId": {"type": "string", "minLength": 1, "maxLength": 64},
                "wait": {"type": "boolean"},
                "timeoutMs": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_TIMEOUT_MS}
            }),
            &["agentId"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        true
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("agentId")
            .and_then(Value::as_str)
            .unwrap_or("<agent>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: AgentOutputInput = serde_json::from_value(input)?;
        context.agent_runtime()?.output(input).await
    }
}

#[async_trait]
impl Tool for AgentStopTool {
    fn name(&self) -> &str {
        "AgentStop"
    }

    fn description(&self) -> &str {
        "Cancels a running background local subagent and its in-flight work."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({"agentId": {"type": "string", "minLength": 1, "maxLength": 64}}),
            &["agentId"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn concurrency_safe(&self, _: &Value) -> bool {
        false
    }

    fn summary(&self, input: &Value) -> String {
        input
            .get("agentId")
            .and_then(Value::as_str)
            .unwrap_or("<agent>")
            .to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: AgentStopInput = serde_json::from_value(input)?;
        context
            .agent_runtime()?
            .stop(parse_agent_id(&input.agent_id)?)
            .await
    }
}

fn render_agent_run(run: &AgentRun) -> ToolOutput {
    ToolOutput::success(
        serde_json::to_string_pretty(&json!({
            "agent_id": run.id,
            "result": run.text,
            "usage": run.usage,
        }))
        .unwrap_or_else(|error| {
            format!(
                "Agent {} completed but result encoding failed: {error}",
                run.id
            )
        }),
    )
}

fn parse_agent_id(value: &str) -> Result<Uuid> {
    value.parse().context("agent id 必须是 UUID")
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
    use super::*;
    use crate::config::EndpointConfig;
    use crate::protocol::{ApiFormat, ChatTokensField};

    fn pending_background_agent(
        runtime: &Arc<AgentRuntime>,
        id: Uuid,
        description: &str,
    ) -> BackgroundAgent {
        let reservation = runtime.reserve_active(id).unwrap();
        let (cancel, cancel_rx) = oneshot::channel();
        let (result_tx, result) = watch::channel(None);
        let handle = tokio::spawn(async move {
            let _result_tx = result_tx;
            let _ = cancel_rx.await;
        });
        BackgroundAgent {
            description: description.to_owned(),
            launch_token: Uuid::new_v4(),
            cancel: Some(cancel),
            result,
            handle,
            _reservation: reservation,
        }
    }

    fn test_runtime(limits: AgentLimits) -> Arc<AgentRuntime> {
        let client = ModelClient::new(EndpointConfig {
            token: None,
            base_url: "http://127.0.0.1:9".to_owned(),
            messages_path: "/v1/messages".to_owned(),
            api_format: ApiFormat::Messages,
            stream: true,
            chat_tokens_field: ChatTokensField::MaxCompletionTokens,
            include_stream_usage: true,
            allow_env_proxy: false,
        })
        .unwrap();
        AgentRuntime::new(
            client,
            ToolRegistry::default(),
            "test".to_owned(),
            128,
            "test".to_owned(),
            false,
            limits,
        )
    }

    #[test]
    fn limits_from_settings_are_clamped() {
        let settings = Settings {
            raw: json!({"agents": {
                "maxDepth": 100,
                "maxConcurrent": 0,
                "maxTotal": 1000,
                "maxBackground": 1000,
                "defaultTimeoutMs": 1
            }}),
        };
        let limits = AgentLimits::from_settings(&settings).unwrap();
        assert_eq!(limits.max_depth, 8);
        assert_eq!(limits.max_concurrent, 1);
        assert_eq!(limits.max_total, 256);
        assert_eq!(limits.max_background, 64);
        assert_eq!(limits.default_timeout_ms, MIN_AGENT_TIMEOUT_MS);
    }

    #[tokio::test]
    async fn transaction_cleanup_removes_new_descendant_jobs_only() {
        let client = ModelClient::new(EndpointConfig {
            token: None,
            base_url: "http://127.0.0.1:9".to_owned(),
            messages_path: "/v1/messages".to_owned(),
            api_format: ApiFormat::Messages,
            stream: true,
            chat_tokens_field: ChatTokensField::MaxCompletionTokens,
            include_stream_usage: true,
            allow_env_proxy: false,
        })
        .unwrap();
        let runtime = AgentRuntime::new(
            client,
            ToolRegistry::default(),
            "test".to_owned(),
            128,
            "test".to_owned(),
            false,
            AgentLimits::default(),
        );
        let existing = Uuid::new_v4();
        let descendant = Uuid::new_v4();
        runtime.jobs.lock().await.insert(
            existing,
            pending_background_agent(&runtime, existing, "existing"),
        );
        let checkpoint = runtime.background_checkpoint().await;
        runtime.jobs.lock().await.insert(
            descendant,
            pending_background_agent(&runtime, descendant, "descendant"),
        );

        runtime.rollback_new_background(&checkpoint).await;
        let jobs = runtime.jobs.lock().await;
        assert!(jobs.contains_key(&existing));
        assert!(!jobs.contains_key(&descendant));
        drop(jobs);
        runtime.shutdown_all().await;
    }

    #[tokio::test]
    async fn transaction_cleanup_removes_relaunched_job_with_reused_agent_id() {
        let client = ModelClient::new(EndpointConfig {
            token: None,
            base_url: "http://127.0.0.1:9".to_owned(),
            messages_path: "/v1/messages".to_owned(),
            api_format: ApiFormat::Messages,
            stream: true,
            chat_tokens_field: ChatTokensField::MaxCompletionTokens,
            include_stream_usage: true,
            allow_env_proxy: false,
        })
        .unwrap();
        let runtime = AgentRuntime::new(
            client,
            ToolRegistry::default(),
            "test".to_owned(),
            128,
            "test".to_owned(),
            false,
            AgentLimits::default(),
        );
        let reused_id = Uuid::new_v4();
        runtime.jobs.lock().await.insert(
            reused_id,
            pending_background_agent(&runtime, reused_id, "before checkpoint"),
        );
        let checkpoint = runtime.background_checkpoint().await;
        let old = runtime.jobs.lock().await.remove(&reused_id).unwrap();
        cancel_background_job(old).await;
        runtime.jobs.lock().await.insert(
            reused_id,
            pending_background_agent(&runtime, reused_id, "relaunched during turn"),
        );

        runtime.rollback_new_background(&checkpoint).await;
        assert!(!runtime.jobs.lock().await.contains_key(&reused_id));
    }

    #[tokio::test]
    async fn cancelling_output_wait_keeps_background_agent_tracked() {
        let runtime = test_runtime(AgentLimits::default());
        let id = Uuid::new_v4();
        runtime.jobs.lock().await.insert(
            id,
            pending_background_agent(&runtime, id, "wait cancellation"),
        );

        let waiter_runtime = Arc::clone(&runtime);
        let waiter = tokio::spawn(async move {
            waiter_runtime
                .output(AgentOutputInput {
                    agent_id: id.to_string(),
                    wait: true,
                    timeout_ms: Some(60_000),
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        waiter.abort();
        let _ = waiter.await;

        assert!(runtime.jobs.lock().await.contains_key(&id));
        let stopped = runtime.stop(id).await.unwrap();
        assert!(!stopped.is_error, "{}", stopped.content);
    }

    #[tokio::test]
    async fn scheduler_wait_is_covered_by_agent_timeout() {
        let limits = AgentLimits {
            max_concurrent: 1,
            default_timeout_ms: MIN_AGENT_TIMEOUT_MS,
            ..AgentLimits::default()
        };
        let runtime = test_runtime(limits);
        let permit = runtime.acquire_slot().await.unwrap();
        let context = ToolContext::new(
            std::env::current_dir().unwrap(),
            crate::permissions::PermissionManager::new(
                crate::permissions::PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            runtime.start(
                &context,
                AgentInput {
                    prompt: "will wait for the scheduler".to_owned(),
                    description: None,
                    model: None,
                    run_in_background: false,
                    resume: None,
                    timeout_ms: Some(MIN_AGENT_TIMEOUT_MS),
                    max_tokens: None,
                },
            ),
        )
        .await
        .expect("agent timeout must include semaphore queue time")
        .unwrap_err();
        assert!(format!("{result:#}").contains("包含调度等待"));
        assert!(
            runtime
                .active_ids
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
        drop(permit);
    }

    #[tokio::test]
    async fn resume_rejects_an_id_reserved_by_background_agent() {
        let runtime = test_runtime(AgentLimits::default());
        let id = Uuid::new_v4();
        runtime.histories.lock().await.values.insert(
            id,
            AgentSnapshot {
                messages: vec![Message::user_text("previous run")],
            },
        );
        runtime
            .jobs
            .lock()
            .await
            .insert(id, pending_background_agent(&runtime, id, "active resume"));
        let context = ToolContext::new(
            std::env::current_dir().unwrap(),
            crate::permissions::PermissionManager::new(
                crate::permissions::PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );

        let error = runtime
            .start(
                &context,
                AgentInput {
                    prompt: "resume concurrently".to_owned(),
                    description: None,
                    model: None,
                    run_in_background: false,
                    resume: Some(id.to_string()),
                    timeout_ms: Some(MIN_AGENT_TIMEOUT_MS),
                    max_tokens: None,
                },
            )
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("已经在运行或结果尚未读取"));
        runtime.shutdown_all().await;
    }
}

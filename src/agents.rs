use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::timeout,
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
    model: String,
    max_tokens: u32,
    system: String,
    debug: bool,
    limits: AgentLimits,
    slots: Arc<Semaphore>,
    total_started: AtomicUsize,
    jobs: Mutex<HashMap<Uuid, BackgroundAgent>>,
    histories: Mutex<HistoryStore>,
}

struct BackgroundAgent {
    owner: Uuid,
    description: String,
    handle: JoinHandle<Result<AgentRun>>,
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
            model,
            max_tokens,
            system,
            debug,
            limits,
            slots: Arc::new(Semaphore::new(limits.max_concurrent)),
            total_started: AtomicUsize::new(0),
            jobs: Mutex::new(HashMap::new()),
            histories: Mutex::new(HistoryStore::default()),
        })
    }

    async fn start(
        self: &Arc<Self>,
        parent: &ToolContext,
        input: AgentInput,
    ) -> Result<ToolOutput> {
        self.validate_start(parent, &input).await?;
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
        let model = input.model.unwrap_or_else(|| self.model.clone());
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

        if input.run_in_background {
            let mut jobs = self.jobs.lock().await;
            if jobs.len() >= self.limits.max_background {
                bail!(
                    "background agent 达到 {} 个限制",
                    self.limits.max_background
                )
            }
            if jobs.contains_key(&id) {
                bail!("agent 已经在运行: {id}")
            }
            self.reserve_start()?;
            let runtime = Arc::clone(self);
            let handle = tokio::spawn(async move {
                let permit = runtime.acquire_slot().await?;
                let result = timeout(
                    Duration::from_millis(timeout_ms),
                    runtime.run_once(AgentRunRequest {
                        id,
                        context,
                        prompt,
                        history,
                        model,
                        max_tokens,
                        depth,
                    }),
                )
                .await
                .map_err(|_| anyhow::anyhow!("agent {id} 超过 {timeout_ms}ms timeout"))??;
                drop(permit);
                runtime.store_snapshot(&result).await;
                Ok(result)
            });
            jobs.insert(
                id,
                BackgroundAgent {
                    owner: parent.agent_scope(),
                    description: description.clone(),
                    handle,
                },
            );
            return Ok(ToolOutput::success(format!(
                "Agent running in background\nagent_id={id}\ndescription={description}"
            )));
        }

        self.reserve_start()?;
        let permit = self.acquire_slot().await?;
        let result = timeout(
            Duration::from_millis(timeout_ms),
            self.run_once(AgentRunRequest {
                id,
                context,
                prompt,
                history,
                model,
                max_tokens,
                depth,
            }),
        )
        .await
        .map_err(|_| anyhow::anyhow!("agent {id} 超过 {timeout_ms}ms timeout"))??;
        drop(permit);
        self.store_snapshot(&result).await;
        Ok(render_agent_run(&result))
    }

    async fn validate_start(&self, parent: &ToolContext, input: &AgentInput) -> Result<()> {
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
        if let Some(resume) = &input.resume {
            let id = parse_agent_id(resume)?;
            if self.jobs.lock().await.contains_key(&id) {
                bail!("不能 resume 正在运行的 agent: {id}")
            }
        }
        Ok(())
    }

    async fn acquire_slot(&self) -> Result<OwnedSemaphorePermit> {
        Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .context("agent scheduler 已关闭")
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

    async fn run_once(&self, request: AgentRunRequest) -> Result<AgentRun> {
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
        let start_hook = context
            .hooks()
            .run(
                "SubagentStart",
                None,
                json!({"agent_id": id, "depth": depth, "prompt": &prompt}),
                &context.cwd(),
            )
            .await?;
        system.push_str(&format!(
            "\n\nYou are a delegated local coding agent at recursion depth {depth}. Work only on the assigned prompt, preserve the shared workspace, and return a concrete result to the parent agent."
        ));
        if !start_hook.additional_context.is_empty() {
            system.push_str("\n\n<subagent-start-hook-context>\n");
            system.push_str(&start_hook.additional_context.join("\n"));
            system.push_str("\n</subagent-start-hook-context>");
        }
        let hooks = context.hooks();
        let hook_cwd = context.cwd();
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
        let turn = engine.run_turn(prompt).await;
        let result = match turn {
            Ok(turn) => AgentRun {
                id,
                text: turn.text,
                messages: engine.messages.clone(),
                usage: engine.usage.clone(),
            },
            Err(error) => {
                engine.shutdown().await;
                let _ = hooks
                    .run(
                        "SubagentStop",
                        None,
                        json!({"agent_id": id, "depth": depth, "success": false, "error": format!("{error:#}")}),
                        &hook_cwd,
                    )
                    .await;
                return Err(error);
            }
        };
        engine.shutdown().await;
        let stop_hook = hooks
            .run(
                "SubagentStop",
                None,
                json!({"agent_id": id, "depth": depth, "success": true}),
                &hook_cwd,
            )
            .await;
        let mut result = result;
        match stop_hook {
            Ok(outcome) if !outcome.additional_context.is_empty() => {
                result.text.push_str("\n\n[Subagent stop hook context]\n");
                result.text.push_str(&outcome.additional_context.join("\n"));
            }
            _ => {}
        }
        Ok(result)
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
        let mut jobs = self.jobs.lock().await;
        let Some(mut job) = jobs.remove(&id) else {
            if self.histories.lock().await.values.contains_key(&id) {
                return Ok(ToolOutput::success(format!(
                    "Agent {id} completed earlier; use Agent with resume={id} to continue it"
                )));
            }
            bail!("background agent 不存在: {id}")
        };
        if !input.wait && !job.handle.is_finished() {
            let description = job.description.clone();
            jobs.insert(id, job);
            return Ok(ToolOutput::success(format!(
                "Agent still running\nagent_id={id}\ndescription={description}"
            )));
        }
        drop(jobs);
        let result = if input.wait {
            let wait_ms = input
                .timeout_ms
                .unwrap_or(30_000)
                .clamp(1, MAX_AGENT_TIMEOUT_MS);
            match timeout(Duration::from_millis(wait_ms), &mut job.handle).await {
                Ok(result) => result,
                Err(_) => {
                    self.jobs.lock().await.insert(id, job);
                    return Ok(ToolOutput::success(format!(
                        "Agent still running after {wait_ms}ms\nagent_id={id}"
                    )));
                }
            }
        } else {
            job.handle.await
        };
        match result {
            Ok(Ok(run)) => Ok(render_agent_run(&run)),
            Ok(Err(error)) => Ok(ToolOutput::error(format!("Agent {id} failed: {error:#}"))),
            Err(error) => Ok(ToolOutput::error(format!(
                "Agent {id} task failed: {error}"
            ))),
        }
    }

    async fn stop(&self, id: Uuid) -> Result<ToolOutput> {
        let Some(job) = self.jobs.lock().await.remove(&id) else {
            bail!("background agent 不存在: {id}")
        };
        job.handle.abort();
        let _ = job.handle.await;
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

    pub(crate) async fn background_ids(&self, owner: Uuid) -> HashSet<Uuid> {
        self.jobs
            .lock()
            .await
            .iter()
            .filter_map(|(id, job)| (job.owner == owner).then_some(*id))
            .collect()
    }

    pub(crate) async fn rollback_background(&self, owner: Uuid, keep: &HashSet<Uuid>) {
        let jobs = {
            let mut jobs = self.jobs.lock().await;
            let ids = jobs
                .iter()
                .filter_map(|(id, job)| (job.owner == owner && !keep.contains(id)).then_some(*id))
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| jobs.remove(&id).map(|job| job.handle))
                .collect::<Vec<_>>()
        };
        for handle in jobs {
            handle.abort();
            let _ = handle.await;
        }
    }

    pub(crate) async fn shutdown_all(&self) {
        let jobs = self
            .jobs
            .lock()
            .await
            .drain()
            .map(|(_, job)| job.handle)
            .collect::<Vec<_>>();
        for handle in jobs {
            handle.abort();
            let _ = handle.await;
        }
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
    async fn failed_turn_cleanup_is_scoped_to_its_agent_owner() {
        let client = ModelClient::new(EndpointConfig {
            token: None,
            base_url: "http://127.0.0.1:9".to_owned(),
            messages_path: "/v1/messages".to_owned(),
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
        let first_owner = Uuid::new_v4();
        let second_owner = Uuid::new_v4();
        let first_job = Uuid::new_v4();
        let second_job = Uuid::new_v4();
        let pending = || tokio::spawn(async { std::future::pending::<Result<AgentRun>>().await });
        runtime.jobs.lock().await.insert(
            first_job,
            BackgroundAgent {
                owner: first_owner,
                description: "first".to_owned(),
                handle: pending(),
            },
        );
        runtime.jobs.lock().await.insert(
            second_job,
            BackgroundAgent {
                owner: second_owner,
                description: "second".to_owned(),
                handle: pending(),
            },
        );

        runtime
            .rollback_background(first_owner, &HashSet::new())
            .await;
        let jobs = runtime.jobs.lock().await;
        assert!(!jobs.contains_key(&first_job));
        assert!(jobs.contains_key(&second_job));
        drop(jobs);
        runtime.shutdown_all().await;
    }
}

use std::{
    io::{self, IsTerminal, Write},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    control::ControlInterrupted,
    permissions::PermissionMode,
    tools::{
        Tool, ToolContext, ToolOutput, atomic_write_private, ensure_private_directory,
        object_schema, workspace_key,
    },
};

const MAX_PLAN_BYTES: usize = 256 * 1024;

pub fn plan_tools() -> Vec<Arc<dyn Tool>> {
    plan_tools_with_storage(None)
}

struct EnterPlanModeTool;
struct ExitPlanModeTool {
    storage_root: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExitPlanInput {
    plan: String,
}

fn plan_tools_with_storage(storage_root: Option<PathBuf>) -> Vec<Arc<dyn Tool>> {
    vec![
        Arc::new(EnterPlanModeTool),
        Arc::new(ExitPlanModeTool { storage_root }),
    ]
}

#[async_trait]
impl Tool for EnterPlanModeTool {
    fn name(&self) -> &str {
        "EnterPlanMode"
    }

    fn description(&self) -> &str {
        "Switches this session into a read-only planning state until ExitPlanMode is called."
    }

    fn input_schema(&self) -> Value {
        object_schema(json!({}), &[])
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

    fn summary(&self, _: &Value) -> String {
        "enter plan mode".to_owned()
    }

    async fn execute(&self, context: &ToolContext, _: Value) -> Result<ToolOutput> {
        if context.agent_depth() != 0 {
            bail!("plan mode 只能由 root agent 控制")
        }
        let changed = context.permissions.enter_plan_mode();
        Ok(ToolOutput::success(if changed {
            "Entered plan mode. Mutating tools are now denied."
        } else {
            "Already in plan mode."
        }))
    }
}

#[async_trait]
impl Tool for ExitPlanModeTool {
    fn name(&self) -> &str {
        "ExitPlanMode"
    }

    fn description(&self) -> &str {
        "Presents and privately saves a completed implementation plan, then asks the root user for explicit approval before leaving plan mode."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "plan": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_PLAN_BYTES
                }
            }),
            &["plan"],
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

    fn summary(&self, _: &Value) -> String {
        "present plan for root-user approval".to_owned()
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        if context.agent_depth() != 0 {
            bail!("ExitPlanMode 只能由 root agent 请求，subagent 不能代替用户批准计划")
        }
        if context.permissions.effective_mode() != PermissionMode::Plan {
            bail!("当前不在 plan mode，不能提交 ExitPlanMode")
        }
        if context.permissions.mode == PermissionMode::Plan {
            bail!("用户从命令行或可信设置锁定了 plan 模式，工具不能解除")
        }
        let parsed: ExitPlanInput = serde_json::from_value(input)?;
        validate_plan(&parsed.plan)?;
        self.save_plan(context, &parsed.plan)?;
        let approved_plan = match request_plan_approval(context, &parsed.plan) {
            Ok(plan) => plan,
            Err(error) if error.downcast_ref::<ControlInterrupted>().is_some() => {
                return Ok(ToolOutput::interrupted());
            }
            Err(error) => return Err(error),
        };
        validate_plan(&approved_plan)?;
        if approved_plan != parsed.plan {
            self.save_plan(context, &approved_plan)?;
        }
        let changed = context.permissions.exit_plan_mode()?;
        Ok(ToolOutput::success(if changed {
            "Root user approved the saved plan. Exited plan mode and restored the user's original permission mode."
        } else {
            "Plan approval was recorded, but no session-entered plan mode was active."
        }))
    }
}

impl ExitPlanModeTool {
    fn save_plan(&self, context: &ToolContext, plan: &str) -> Result<()> {
        let root = self.storage_root.clone().map(Ok).unwrap_or_else(|| {
            Ok::<_, anyhow::Error>(
                dirs::home_dir()
                    .context("无法确定 plan storage 主目录")?
                    .join(".open-agent-harness/plans"),
            )
        })?;
        let directory = root.join(workspace_key(&context.workspace_root()));
        if std::fs::symlink_metadata(&directory)
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            bail!("plan storage 目录不能是 symlink")
        }
        ensure_private_directory(&directory)?;
        atomic_write_private(&directory.join("latest.md"), plan)
    }
}

fn validate_plan(plan: &str) -> Result<()> {
    if plan.trim().is_empty() || plan.len() > MAX_PLAN_BYTES {
        bail!("plan 为空或超过 {MAX_PLAN_BYTES} 字节限制")
    }
    Ok(())
}

fn request_plan_approval(context: &ToolContext, plan: &str) -> Result<String> {
    let interaction = context.request_user_interaction(
        "ExitPlanMode",
        json!({
            "plan": plan,
            "saved": true,
            "question": "Approve this implementation plan and leave plan mode?"
        }),
    )?;
    if let Some(response) = interaction {
        return parse_plan_approval(response, plan);
    }
    if !context.permissions.interactive || !io::stdin().is_terminal() {
        bail!("ExitPlanMode 需要交互式 root user 或 stream-json control approval")
    }
    eprintln!("\n--- Saved implementation plan ---\n{plan}\n--- End plan ---");
    eprint!("Approve this plan and leave plan mode? [y/N]: ");
    io::stderr().flush()?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .context("读取 plan approval 失败")?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(plan.to_owned())
    } else {
        bail!("root user rejected the plan; remaining in plan mode")
    }
}

fn parse_plan_approval(response: Value, original: &str) -> Result<String> {
    let approved = response
        .get("approved")
        .and_then(Value::as_bool)
        .or_else(|| {
            response
                .get("behavior")
                .or_else(|| response.get("decision"))
                .or_else(|| response.get("action"))
                .and_then(Value::as_str)
                .map(|decision| {
                    matches!(
                        decision,
                        "allow" | "allowed" | "accept" | "accepted" | "approve" | "approved"
                    )
                })
        })
        .context("plan approval response 缺少显式 approve/reject decision")?;
    if !approved {
        bail!("root user rejected the plan; remaining in plan mode")
    }
    let edited = response
        .get("updatedInput")
        .or_else(|| response.get("updated_input"))
        .and_then(|input| input.get("plan"))
        .or_else(|| response.get("plan"))
        .and_then(Value::as_str)
        .unwrap_or(original);
    Ok(edited.to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::ToolRegistry,
    };

    #[tokio::test]
    async fn exit_saves_presents_and_requires_explicit_root_approval() {
        let temp = tempfile::tempdir().unwrap();
        let storage = temp.path().join("plans");
        let registry = ToolRegistry::with_extensions(
            Vec::new(),
            plan_tools_with_storage(Some(storage.clone())),
        )
        .unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Default, false, vec![], vec![]),
        );
        context.set_user_interaction_handler(Some(Arc::new(|request| {
            assert_eq!(request.tool, "ExitPlanMode");
            assert_eq!(request.input["plan"], "original plan");
            Ok(json!({
                "approved": true,
                "updatedInput": {"plan": "edited and approved plan"}
            }))
        })));
        registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"select:EnterPlanMode,ExitPlanMode"}),
            )
            .await;
        let entered = registry.execute(&context, "EnterPlanMode", json!({})).await;
        assert!(!entered.is_error, "{}", entered.content);
        assert_eq!(context.permissions.effective_mode(), PermissionMode::Plan);
        let exited = registry
            .execute(&context, "ExitPlanMode", json!({"plan":"original plan"}))
            .await;
        assert!(!exited.is_error, "{}", exited.content);
        assert_eq!(
            context.permissions.effective_mode(),
            PermissionMode::Default
        );
        let saved = storage
            .join(workspace_key(&std::fs::canonicalize(temp.path()).unwrap()))
            .join("latest.md");
        assert_eq!(
            std::fs::read_to_string(saved).unwrap(),
            "edited and approved plan"
        );
    }

    #[tokio::test]
    async fn rejection_or_implicit_response_never_exits_plan_mode() {
        let temp = tempfile::tempdir().unwrap();
        let storage = temp.path().join("plans");
        let registry = ToolRegistry::with_extensions(
            Vec::new(),
            plan_tools_with_storage(Some(storage.clone())),
        )
        .unwrap();
        let context = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Default, false, vec![], vec![]),
        );
        registry
            .execute(
                &context,
                "ToolSearch",
                json!({"query":"select:EnterPlanMode,ExitPlanMode"}),
            )
            .await;
        context.permissions.enter_plan_mode();
        context.set_user_interaction_handler(Some(Arc::new(|_| Ok(json!({"approved": false})))));
        let rejected = registry
            .execute(&context, "ExitPlanMode", json!({"plan":"safe plan"}))
            .await;
        assert!(rejected.is_error);
        assert_eq!(context.permissions.effective_mode(), PermissionMode::Plan);
        let saved = storage
            .join(workspace_key(&std::fs::canonicalize(temp.path()).unwrap()))
            .join("latest.md");
        assert_eq!(std::fs::read_to_string(saved).unwrap(), "safe plan");

        context.set_user_interaction_handler(Some(Arc::new(|_| Ok(json!({"plan":"no decision"})))));
        let implicit = registry
            .execute(&context, "ExitPlanMode", json!({"plan":"second plan"}))
            .await;
        assert!(implicit.is_error);
        assert_eq!(context.permissions.effective_mode(), PermissionMode::Plan);
    }

    #[tokio::test]
    async fn user_lock_and_subagent_cannot_bypass_root_approval() {
        let temp = tempfile::tempdir().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = ToolRegistry::with_extensions(
            Vec::new(),
            plan_tools_with_storage(Some(temp.path().join("plans"))),
        )
        .unwrap();

        let locked = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Plan, false, vec![], vec![]),
        );
        registry
            .execute(
                &locked,
                "ToolSearch",
                json!({"query":"select:EnterPlanMode,ExitPlanMode"}),
            )
            .await;
        let observed = Arc::clone(&calls);
        locked.set_user_interaction_handler(Some(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"approved":true}))
        })));
        let denied = registry
            .execute(&locked, "ExitPlanMode", json!({"plan":"locked plan"}))
            .await;
        assert!(denied.is_error);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let root = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Default, false, vec![], vec![]),
        );
        root.permissions.enter_plan_mode();
        let child = root.fork_for_agent();
        let child_enter = registry.execute(&child, "EnterPlanMode", json!({})).await;
        assert!(child_enter.is_error);
        let child_exit = registry
            .execute(&child, "ExitPlanMode", json!({"plan":"child plan"}))
            .await;
        assert!(child_exit.is_error);
        assert_eq!(root.permissions.effective_mode(), PermissionMode::Plan);
    }
}

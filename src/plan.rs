use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::tools::{Tool, ToolContext, ToolOutput, object_schema};

pub fn plan_tools() -> Vec<Arc<dyn Tool>> {
    vec![Arc::new(EnterPlanModeTool), Arc::new(ExitPlanModeTool)]
}

struct EnterPlanModeTool;
struct ExitPlanModeTool;

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
        "Leaves a plan state entered by EnterPlanMode; it cannot override a user-locked plan mode."
    }

    fn input_schema(&self) -> Value {
        object_schema(json!({}), &[])
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
        "exit plan mode".to_owned()
    }

    async fn execute(&self, context: &ToolContext, _: Value) -> Result<ToolOutput> {
        let changed = context.permissions.exit_plan_mode()?;
        Ok(ToolOutput::success(if changed {
            "Exited plan mode and restored the user's original permission mode."
        } else {
            "No session-entered plan mode was active."
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        permissions::{PermissionManager, PermissionMode},
        tools::ToolRegistry,
    };

    #[tokio::test]
    async fn tools_toggle_dynamic_mode_but_respect_user_lock() {
        let temp = tempfile::tempdir().unwrap();
        let registry = ToolRegistry::with_extensions(Vec::new(), plan_tools()).unwrap();
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
        let entered = registry.execute(&context, "EnterPlanMode", json!({})).await;
        assert!(!entered.is_error, "{}", entered.content);
        assert_eq!(context.permissions.effective_mode(), PermissionMode::Plan);
        let exited = registry.execute(&context, "ExitPlanMode", json!({})).await;
        assert!(!exited.is_error, "{}", exited.content);
        assert_eq!(
            context.permissions.effective_mode(),
            PermissionMode::Default
        );

        let locked = ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(PermissionMode::Plan, false, vec![], vec![]),
        );
        let denied = registry.execute(&locked, "ExitPlanMode", json!({})).await;
        assert!(denied.is_error);
    }
}

use std::{
    io::{self, Write},
    sync::{Arc, RwLock},
};

use anyhow::{Result, bail};
use clap::ValueEnum;
use globset::Glob;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum PermissionMode {
    Default,
    AcceptEdits,
    Plan,
    BypassPermissions,
}

impl PermissionMode {
    pub fn from_setting(value: &str) -> Option<Self> {
        match value {
            "default" => Some(Self::Default),
            "acceptEdits" => Some(Self::AcceptEdits),
            "plan" => Some(Self::Plan),
            "bypassPermissions" => Some(Self::BypassPermissions),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PermissionManager {
    pub mode: PermissionMode,
    pub interactive: bool,
    allow: Vec<String>,
    deny: Vec<String>,
    session_mode: Arc<RwLock<Option<PermissionMode>>>,
    workspace_deny: Arc<RwLock<Vec<String>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny,
}

impl PermissionManager {
    pub fn new(
        mode: PermissionMode,
        interactive: bool,
        allow: Vec<String>,
        deny: Vec<String>,
    ) -> Self {
        Self {
            mode,
            interactive,
            allow,
            deny,
            session_mode: Arc::new(RwLock::new(None)),
            workspace_deny: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn set_workspace_deny(&self, rules: Vec<String>) {
        *self
            .workspace_deny
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = rules;
    }

    pub fn effective_mode(&self) -> PermissionMode {
        self.session_mode
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unwrap_or(self.mode)
    }

    pub fn enter_plan_mode(&self) -> bool {
        let mut mode = self
            .session_mode
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if mode.unwrap_or(self.mode) == PermissionMode::Plan {
            return false;
        }
        *mode = Some(PermissionMode::Plan);
        true
    }

    pub fn exit_plan_mode(&self) -> Result<bool> {
        if self.mode == PermissionMode::Plan {
            bail!("用户从命令行或可信设置锁定了 plan 模式，工具不能解除")
        }
        let mut mode = self
            .session_mode
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(mode.take() == Some(PermissionMode::Plan))
    }

    pub fn decide(
        &self,
        tool: &str,
        summary: &str,
        read_only: bool,
        destructive: bool,
        outside_workspace: bool,
    ) -> Result<PermissionDecision> {
        let target = format!("{tool}({summary})");
        let workspace_denied = matches_any(
            &self
                .workspace_deny
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            tool,
            &target,
        );
        if matches_any(&self.deny, tool, &target) || workspace_denied {
            return Ok(PermissionDecision::Deny);
        }
        if self.effective_mode() == PermissionMode::Plan {
            return Ok(if read_only && !outside_workspace {
                PermissionDecision::Allow
            } else {
                PermissionDecision::Deny
            });
        }
        if matches_any(&self.allow, tool, &target) {
            return Ok(PermissionDecision::Allow);
        }
        match self.effective_mode() {
            PermissionMode::BypassPermissions => Ok(PermissionDecision::Allow),
            PermissionMode::Plan => unreachable!("plan mode returned before allow-rule handling"),
            PermissionMode::AcceptEdits
                if !outside_workspace && matches!(tool, "Edit" | "NotebookEdit" | "Write") =>
            {
                Ok(PermissionDecision::Allow)
            }
            _ if read_only && !destructive && !outside_workspace => Ok(PermissionDecision::Allow),
            _ if !self.interactive => Ok(PermissionDecision::Deny),
            _ => prompt(tool, summary),
        }
    }
}

fn matches_any(rules: &[String], tool: &str, target: &str) -> bool {
    rules.iter().any(|rule| {
        if rule == tool || rule == target {
            return true;
        }
        Glob::new(rule)
            .map(|glob| glob.compile_matcher().is_match(target))
            .unwrap_or(false)
    })
}

fn prompt(tool: &str, summary: &str) -> Result<PermissionDecision> {
    eprint!("允许 {tool} 执行 `{summary}`？[y/N] ");
    io::stderr().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(
        if matches!(input.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            PermissionDecision::Allow
        } else {
            PermissionDecision::Deny
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deny_precedes_allow() {
        let p = PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            vec!["Bash(*)".into()],
            vec!["Bash(rm *)".into()],
        );
        assert_eq!(
            p.decide("Bash", "rm x", false, true, false).unwrap(),
            PermissionDecision::Deny
        );
    }

    #[test]
    fn session_plan_mode_cannot_override_a_user_lock() {
        let locked = PermissionManager::new(PermissionMode::Plan, false, vec![], vec![]);
        assert!(!locked.enter_plan_mode());
        assert!(locked.exit_plan_mode().is_err());
        assert_eq!(
            PermissionManager::new(
                PermissionMode::Plan,
                false,
                vec!["Bash(*)".to_owned()],
                vec![]
            )
            .decide("Bash", "echo unsafe", false, false, false)
            .unwrap(),
            PermissionDecision::Deny
        );

        let dynamic = PermissionManager::new(PermissionMode::Default, false, vec![], vec![]);
        assert!(dynamic.enter_plan_mode());
        assert_eq!(dynamic.effective_mode(), PermissionMode::Plan);
        assert!(dynamic.exit_plan_mode().unwrap());
        assert_eq!(dynamic.effective_mode(), PermissionMode::Default);
    }
}

use std::io::{self, Write};

use anyhow::Result;
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
        }
    }

    pub fn decide(
        &self,
        tool: &str,
        summary: &str,
        read_only: bool,
        destructive: bool,
    ) -> Result<PermissionDecision> {
        let target = format!("{tool}({summary})");
        if matches_any(&self.deny, tool, &target) {
            return Ok(PermissionDecision::Deny);
        }
        if matches_any(&self.allow, tool, &target) {
            return Ok(PermissionDecision::Allow);
        }
        match self.mode {
            PermissionMode::BypassPermissions => Ok(PermissionDecision::Allow),
            PermissionMode::Plan => {
                if read_only {
                    Ok(PermissionDecision::Allow)
                } else {
                    Ok(PermissionDecision::Deny)
                }
            }
            PermissionMode::AcceptEdits if matches!(tool, "Edit" | "Write") => {
                Ok(PermissionDecision::Allow)
            }
            _ if read_only && !destructive => Ok(PermissionDecision::Allow),
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
            p.decide("Bash", "rm x", false, true).unwrap(),
            PermissionDecision::Deny
        );
    }
}

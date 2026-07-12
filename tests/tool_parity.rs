use std::sync::Arc;

use agent_harness::{
    permissions::{PermissionManager, PermissionMode},
    tools::{ToolContext, ToolRegistry},
};
use serde_json::json;
use tempfile::tempdir;

fn context(root: &std::path::Path) -> ToolContext {
    ToolContext::new(
        root.to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    )
}

#[tokio::test]
async fn read_then_edit_preserves_reference_guard() {
    let temp = tempdir().unwrap();
    let file = temp.path().join("sample.txt");
    std::fs::write(&file, "one\ntwo\n").unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();

    let read = registry
        .execute(&context, "Read", json!({"file_path": "sample.txt"}))
        .await;
    assert!(!read.is_error, "{}", read.content);
    assert!(read.content.contains("     1→one"));

    let edit = registry
        .execute(
            &context,
            "Edit",
            json!({"file_path": "sample.txt", "old_string": "two", "new_string": "second"}),
        )
        .await;
    assert!(!edit.is_error, "{}", edit.content);
    assert_eq!(std::fs::read_to_string(file).unwrap(), "one\nsecond\n");
}

#[tokio::test]
async fn edit_rejects_external_change_after_read() {
    let temp = tempdir().unwrap();
    let file = temp.path().join("sample.txt");
    std::fs::write(&file, "before\n").unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    registry
        .execute(&context, "Read", json!({"file_path": "sample.txt"}))
        .await;
    std::fs::write(&file, "changed outside\n").unwrap();

    let edit = registry
        .execute(
            &context,
            "Edit",
            json!({"file_path": "sample.txt", "old_string": "before", "new_string": "after"}),
        )
        .await;
    assert!(edit.is_error);
    assert!(edit.content.contains("读取后已被"));
    assert_eq!(std::fs::read_to_string(file).unwrap(), "changed outside\n");
}

#[tokio::test]
async fn partial_read_cannot_authorize_edit() {
    let temp = tempdir().unwrap();
    let file = temp.path().join("sample.txt");
    std::fs::write(&file, "one\ntwo\nthree\n").unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    registry
        .execute(
            &context,
            "Read",
            json!({"file_path": "sample.txt", "offset": 2, "limit": 1}),
        )
        .await;

    let edit = registry
        .execute(
            &context,
            "Edit",
            json!({"file_path": "sample.txt", "old_string": "two", "new_string": "second"}),
        )
        .await;
    assert!(edit.is_error);
    assert!(edit.content.contains("完整读取"));
}

#[tokio::test]
async fn glob_and_grep_return_real_matches() {
    let temp = tempdir().unwrap();
    std::fs::create_dir(temp.path().join("src")).unwrap();
    std::fs::write(temp.path().join("src/lib.rs"), "fn migrated_marker() {}\n").unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();

    let glob = registry
        .execute(&context, "Glob", json!({"pattern": "**/*.rs"}))
        .await;
    assert!(!glob.is_error, "{}", glob.content);
    assert!(glob.content.contains("src/lib.rs"));

    let grep = registry
        .execute(
            &context,
            "Grep",
            json!({"pattern": "migrated_marker", "path": "src", "output_mode": "content"}),
        )
        .await;
    assert!(!grep.is_error, "{}", grep.content);
    assert!(grep.content.contains("migrated_marker"));
}

#[tokio::test]
async fn bash_captures_stdout_stderr_and_exit_status() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    let output = registry
        .execute(
            &context,
            "Bash",
            json!({"command": "printf ok; printf problem >&2; exit 7"}),
        )
        .await;
    assert!(output.is_error);
    assert!(output.content.contains("ok"));
    assert!(output.content.contains("problem"));
    assert!(output.content.contains("Exit code 7"));
}

#[tokio::test]
async fn default_noninteractive_permissions_deny_mutation() {
    let temp = tempdir().unwrap();
    let context = ToolContext {
        permissions: Arc::new(PermissionManager::new(
            PermissionMode::Default,
            false,
            Vec::new(),
            Vec::new(),
        )),
        ..context(temp.path())
    };
    let output = ToolRegistry::default()
        .execute(
            &context,
            "Write",
            json!({"file_path": "blocked.txt", "content": "no"}),
        )
        .await;
    assert!(output.is_error);
    assert!(!temp.path().join("blocked.txt").exists());
}

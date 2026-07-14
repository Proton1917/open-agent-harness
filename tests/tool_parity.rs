use open_agent_harness::{
    permissions::{PermissionManager, PermissionMode},
    skills::discover_skills,
    tools::{ToolContext, ToolRegistry},
};
use serde_json::json;
use tempfile::tempdir;

fn context(root: &std::path::Path) -> ToolContext {
    let context = ToolContext::new(
        root.to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            Vec::new(),
        ),
    );
    context
        .set_task_capture_root(root.join(".test-task-captures"))
        .unwrap();
    context
}

#[cfg(unix)]
fn process_is_running(pid: i32) -> bool {
    // SAFETY: signal 0 only probes whether the process exists.
    if unsafe { libc::kill(pid, 0) } != 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return false;
        };
        let state = stat
            .rsplit_once(") ")
            .and_then(|(_, fields)| fields.chars().next());
        if matches!(state, Some('Z' | 'X')) {
            return false;
        }
    }
    true
}

#[cfg(unix)]
async fn wait_for_process_exit(pid: i32) -> bool {
    for _ in 0..100 {
        if !process_is_running(pid) {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    false
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
async fn read_deny_rules_filter_glob_and_grep_results() {
    let temp = tempdir().unwrap();
    std::fs::create_dir(temp.path().join("secret")).unwrap();
    std::fs::create_dir(temp.path().join("public")).unwrap();
    std::fs::write(temp.path().join("secret/token.txt"), "needle secret\n").unwrap();
    std::fs::write(temp.path().join("public/readme.txt"), "needle public\n").unwrap();
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(
            PermissionMode::BypassPermissions,
            false,
            Vec::new(),
            vec!["Read(secret/**)".into()],
        ),
    );
    let registry = ToolRegistry::default();

    let glob = registry
        .execute(&context, "Glob", json!({"pattern":"**/*.txt"}))
        .await;
    assert!(!glob.is_error, "{}", glob.content);
    assert!(glob.content.contains("public/readme.txt"));
    assert!(!glob.content.contains("secret/token.txt"));

    let grep = registry
        .execute(
            &context,
            "Grep",
            json!({"pattern":"needle","output_mode":"content"}),
        )
        .await;
    assert!(!grep.is_error, "{}", grep.content);
    assert!(grep.content.contains("public/readme.txt"));
    assert!(!grep.content.contains("secret/token.txt"));

    for path in [
        "./secret/../secret/token.txt".to_owned(),
        temp.path()
            .join("secret/token.txt")
            .to_string_lossy()
            .into_owned(),
    ] {
        let direct = registry
            .execute(&context, "Read", json!({"file_path":path}))
            .await;
        assert!(direct.is_error);
        assert!(!direct.content.contains("needle secret"));
    }

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            temp.path().join("secret/token.txt"),
            temp.path().join("token-alias.txt"),
        )
        .unwrap();
        let alias = registry
            .execute(&context, "Read", json!({"file_path":"token-alias.txt"}))
            .await;
        assert!(alias.is_error);
        assert!(!alias.content.contains("needle secret"));
    }
}

#[tokio::test]
async fn unc_and_windows_device_namespace_paths_fail_before_filesystem_access() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    for path in [
        r"\\server\share\secret.txt",
        "//server/share/secret.txt",
        r"\\?\C:\secret.txt",
        "//./C:/secret.txt",
        r"\??\C:\secret.txt",
        r"\Device\Mup\server\share\secret.txt",
        r"\GLOBAL??\C:\secret.txt",
    ] {
        let output = registry
            .execute(&context, "Read", json!({"file_path":path}))
            .await;
        assert!(output.is_error, "path unexpectedly accepted: {path}");
        assert!(
            output.content.contains("UNC") || output.content.contains("device namespace"),
            "unexpected preflight error for {path}: {}",
            output.content
        );
    }
}

#[cfg(windows)]
#[tokio::test]
async fn reserved_windows_device_names_are_rejected() {
    let temp = tempdir().unwrap();
    let output = ToolRegistry::default()
        .execute(
            &context(temp.path()),
            "Read",
            json!({"file_path":"NUL.txt"}),
        )
        .await;
    assert!(output.is_error);
    assert!(output.content.contains("保留设备"));
}

#[tokio::test]
async fn rust_grep_supports_filters_context_counts_and_multiline() {
    let temp = tempdir().unwrap();
    std::fs::write(temp.path().join("code.rs"), "before\nNeedle\nafter\n").unwrap();
    std::fs::write(temp.path().join("notes.txt"), "needle\nsecond line\n").unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();

    let content = registry
        .execute(
            &context,
            "Grep",
            json!({"pattern":"needle","output_mode":"content","type":"rust","-i":true,"-C":1}),
        )
        .await;
    assert!(!content.is_error, "{}", content.content);
    assert!(content.content.contains("code.rs:2:Needle"));
    assert!(content.content.contains("code.rs-1-before"));
    assert!(!content.content.contains("notes.txt"));

    let count = registry
        .execute(
            &context,
            "Grep",
            json!({"pattern":"needle","output_mode":"count","-i":true}),
        )
        .await;
    assert!(!count.is_error, "{}", count.content);
    assert!(count.content.contains("code.rs:1"));
    assert!(count.content.contains("notes.txt:1"));

    let multiline = registry
        .execute(
            &context,
            "Grep",
            json!({"pattern":"needle\\nsecond","glob":"*.txt","output_mode":"files_with_matches","multiline":true}),
        )
        .await;
    assert!(!multiline.is_error, "{}", multiline.content);
    assert_eq!(multiline.content, "notes.txt");
}

#[tokio::test]
async fn bash_captures_stdout_stderr_and_exit_status() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    #[cfg(windows)]
    let command = "echo ok & echo problem 1>&2 & exit /b 7";
    #[cfg(not(windows))]
    let command = "printf ok; printf problem >&2; exit 7";
    let output = registry
        .execute(&context, "Bash", json!({"command": command}))
        .await;
    assert!(output.is_error);
    assert!(output.content.contains("ok"));
    assert!(output.content.contains("problem"));
    assert!(output.content.contains("Exit code 7"));
}

#[tokio::test]
async fn default_noninteractive_permissions_deny_mutation() {
    let temp = tempdir().unwrap();
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
    );
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

#[tokio::test]
async fn todo_and_persistent_tasks_work_without_permission_prompts() {
    let temp = tempdir().unwrap();
    let context = ToolContext::new(
        temp.path().to_owned(),
        PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
    );
    context.set_task_store_path(temp.path().join("tasks.json"));
    let registry = ToolRegistry::default();

    let todo = registry
        .execute(
            &context,
            "TodoWrite",
            json!({"todos":[
                {"content":"design","status":"in_progress","activeForm":"Designing"},
                {"content":"verify","status":"pending","activeForm":"Verifying"}
            ]}),
        )
        .await;
    assert!(!todo.is_error, "{}", todo.content);
    assert_eq!(context.todos.lock().await.len(), 2);

    for (subject, description) in [("first", "first task"), ("second", "second task")] {
        let created = registry
            .execute(
                &context,
                "TaskCreate",
                json!({"subject":subject,"description":description}),
            )
            .await;
        assert!(!created.is_error, "{}", created.content);
    }
    let linked = registry
        .execute(
            &context,
            "TaskUpdate",
            json!({"taskId":"1","status":"in_progress","addBlocks":["2"]}),
        )
        .await;
    assert!(!linked.is_error, "{}", linked.content);

    let listed = registry.execute(&context, "TaskList", json!({})).await;
    assert!(listed.content.contains("#1 [in_progress] first"));
    assert!(
        listed
            .content
            .contains("#2 [pending] second [blocked by #1]")
    );

    let fetched = registry
        .execute(&context, "TaskGet", json!({"taskId":"2"}))
        .await;
    assert!(fetched.content.contains("Blocked by: #1"));
    assert!(context.task_store_path().exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(context.task_store_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(temp.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }
}

#[tokio::test]
async fn task_relations_reject_missing_or_self_targets() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    context.set_task_store_path(temp.path().join("tasks.json"));
    let registry = ToolRegistry::default();
    registry
        .execute(
            &context,
            "TaskCreate",
            json!({"subject":"one","description":"task"}),
        )
        .await;

    let missing = registry
        .execute(
            &context,
            "TaskUpdate",
            json!({"taskId":"1","addBlocks":["99"]}),
        )
        .await;
    assert!(missing.is_error);
    assert!(missing.content.contains("关联任务不存在"));

    let self_block = registry
        .execute(
            &context,
            "TaskUpdate",
            json!({"taskId":"1","addBlockedBy":["1"]}),
        )
        .await;
    assert!(self_block.is_error);
    assert!(self_block.content.contains("不能阻塞自身"));
}

#[tokio::test]
async fn strict_schema_rejects_unknown_fields_before_execution() {
    let temp = tempdir().unwrap();
    let output = ToolRegistry::default()
        .execute(
            &context(temp.path()),
            "Bash",
            json!({"command":"touch should-not-exist", "unexpected":true}),
        )
        .await;
    assert!(output.is_error);
    assert!(output.content.contains("不允许额外字段"));
    assert!(!temp.path().join("should-not-exist").exists());
}

#[tokio::test]
async fn outside_workspace_paths_require_explicit_permission() {
    let workspace = tempdir().unwrap();
    let outside = tempdir().unwrap();
    let outside_file = outside.path().join("outside.txt");
    std::fs::write(&outside_file, "outside evidence\n").unwrap();
    let registry = ToolRegistry::default();
    let guarded = ToolContext::new(
        workspace.path().to_owned(),
        PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
    );
    let denied = registry
        .execute(
            &guarded,
            "Read",
            json!({"file_path":outside_file.to_string_lossy()}),
        )
        .await;
    assert!(denied.is_error);
    assert!(denied.content.contains("拒绝"));

    let target = format!("Read({})", outside_file.display());
    let allowed = ToolContext::new(
        workspace.path().to_owned(),
        PermissionManager::new(PermissionMode::Default, false, vec![target], Vec::new()),
    );
    let output = registry
        .execute(
            &allowed,
            "Read",
            json!({"file_path":outside_file.to_string_lossy()}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(output.content.contains("outside evidence"));

    let accept_edits = ToolContext::new(
        workspace.path().to_owned(),
        PermissionManager::new(PermissionMode::AcceptEdits, false, Vec::new(), Vec::new()),
    );
    let write = registry
        .execute(
            &accept_edits,
            "Write",
            json!({"file_path":outside.path().join("new.txt").to_string_lossy(),"content":"no"}),
        )
        .await;
    assert!(write.is_error);
    assert!(!outside.path().join("new.txt").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_escape_is_outside_the_workspace() {
    use std::os::unix::fs::symlink;

    let workspace = tempdir().unwrap();
    let outside = tempdir().unwrap();
    std::fs::write(outside.path().join("secret.txt"), "not authorized\n").unwrap();
    symlink(outside.path(), workspace.path().join("escape")).unwrap();
    let guarded = ToolContext::new(
        workspace.path().to_owned(),
        PermissionManager::new(PermissionMode::Default, false, Vec::new(), Vec::new()),
    );
    let output = ToolRegistry::default()
        .execute(&guarded, "Read", json!({"file_path":"escape/secret.txt"}))
        .await;
    assert!(output.is_error);
    assert!(!output.content.contains("not authorized"));
}

#[cfg(unix)]
#[tokio::test]
async fn edit_refuses_to_replace_an_in_workspace_symlink() {
    use std::os::unix::fs::symlink;

    let workspace = tempdir().unwrap();
    let target = workspace.path().join("target.txt");
    let link = workspace.path().join("link.txt");
    std::fs::write(&target, "original\n").unwrap();
    symlink(&target, &link).unwrap();
    let context = context(workspace.path());
    let registry = ToolRegistry::default();
    let read = registry
        .execute(&context, "Read", json!({"file_path":"link.txt"}))
        .await;
    assert!(!read.is_error, "{}", read.content);
    let edit = registry
        .execute(
            &context,
            "Edit",
            json!({"file_path":"link.txt","old_string":"original","new_string":"changed"}),
        )
        .await;
    assert!(edit.is_error);
    assert!(edit.content.contains("symlink"));
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "original\n");
    assert!(link.is_symlink());
}

#[tokio::test]
async fn partial_read_of_sparse_large_file_stays_bounded() {
    use std::io::Write;

    let temp = tempdir().unwrap();
    let path = temp.path().join("large.txt");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(b"first line\n").unwrap();
    file.set_len(512 * 1024 * 1024).unwrap();
    let output = ToolRegistry::default()
        .execute(
            &context(temp.path()),
            "Read",
            json!({"file_path":"large.txt","limit":1}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert_eq!(output.content, "     1→first line");
}

#[tokio::test]
async fn notebook_edit_replaces_inserts_and_deletes_cells() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("analysis.ipynb");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&json!({
            "cells": [
                {"cell_type":"code","id":"cell-a","metadata":{},"source":"print(1)","execution_count":7,"outputs":[{"output_type":"stream","text":"1"}]},
                {"cell_type":"markdown","id":"cell-b","metadata":{},"source":"old"}
            ],
            "metadata":{"language_info":{"name":"python"}},
            "nbformat":4,
            "nbformat_minor":5
        }))
        .unwrap(),
    )
    .unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    let read = registry
        .execute(&context, "Read", json!({"file_path":"analysis.ipynb"}))
        .await;
    assert!(!read.is_error, "{}", read.content);

    let replaced = registry
        .execute(
            &context,
            "NotebookEdit",
            json!({"notebook_path":"analysis.ipynb","cell_id":"cell-a","new_source":"print(2)"}),
        )
        .await;
    assert!(!replaced.is_error, "{}", replaced.content);
    let inserted = registry
        .execute(
            &context,
            "NotebookEdit",
            json!({"notebook_path":"analysis.ipynb","cell_id":"cell-a","new_source":"new note","cell_type":"markdown","edit_mode":"insert"}),
        )
        .await;
    assert!(!inserted.is_error, "{}", inserted.content);
    let deleted = registry
        .execute(
            &context,
            "NotebookEdit",
            json!({"notebook_path":"analysis.ipynb","cell_id":"cell-b","new_source":"","edit_mode":"delete"}),
        )
        .await;
    assert!(!deleted.is_error, "{}", deleted.content);

    let notebook: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let cells = notebook["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 2);
    assert_eq!(cells[0]["source"], "print(2)");
    assert_eq!(cells[0]["execution_count"], serde_json::Value::Null);
    assert_eq!(cells[0]["outputs"], json!([]));
    assert_eq!(cells[1]["cell_type"], "markdown");
    assert_eq!(cells[1]["source"], "new note");
    assert!(cells[1]["id"].as_str().is_some());
}

#[tokio::test]
async fn bash_large_output_is_bounded_and_retained_privately() {
    let temp = tempdir().unwrap();
    #[cfg(windows)]
    let command = "for /L %i in (1,1,10000) do @echo 0123456789";
    #[cfg(not(windows))]
    let command = "yes 0123456789 | head -c 100000";
    let context = context(temp.path());
    let output = ToolRegistry::default()
        .execute(&context, "Bash", json!({"command":command}))
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(output.content.len() < 32_000);
    let marker = "[Full captured output: ";
    let retained = output
        .content
        .split_once(marker)
        .and_then(|(_, tail)| tail.split_once(" (").map(|(path, _)| path))
        .expect("large output path");
    let retained = temp.path().join(retained);
    let metadata = std::fs::metadata(&retained).unwrap();
    assert!((100_000..=8 * 1024 * 1024).contains(&metadata.len()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
    std::fs::remove_file(retained).unwrap();
}

#[tokio::test]
async fn workflow_large_step_output_does_not_leave_an_unreachable_capture() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    let marker = format!("workflow-capture-{}", uuid::Uuid::new_v4());
    let task_directory = temp.path().join(".test-task-captures");
    let before = std::fs::read_dir(&task_directory)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<std::collections::HashSet<_>>();
    #[cfg(windows)]
    let command = format!("for /L %i in (1,1,10000) do @echo {marker}");
    #[cfg(not(windows))]
    let command = format!("yes {marker} | head -c 100000");

    let launched = registry
        .execute(
            &context,
            "RunWorkflow",
            json!({
                "name":"large-output",
                "steps":[{"id":"emit", "command":command}]
            }),
        )
        .await;
    assert!(!launched.is_error, "{}", launched.content);
    let task_id = launched
        .content
        .lines()
        .find_map(|line| line.strip_prefix("task_id="))
        .expect("workflow task id");
    let output = registry
        .execute(
            &context,
            "TaskOutput",
            json!({"task_id":task_id, "block":true, "timeout":10_000}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(!output.content.contains("Full captured output:"));

    let leaked = std::fs::read_dir(&task_directory)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| !before.contains(path))
        .filter(|path| {
            std::fs::read(path).ok().is_some_and(|bytes| {
                bytes
                    .windows(marker.len())
                    .any(|part| part == marker.as_bytes())
            })
        })
        .collect::<Vec<_>>();
    for path in &leaked {
        let _ = std::fs::remove_file(path);
    }
    assert!(
        leaked.is_empty(),
        "workflow left an unreachable foreground capture: {leaked:?}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn bash_timeout_terminates_descendant_processes() {
    let temp = tempdir().unwrap();
    let output = ToolRegistry::default()
        .execute(
            &context(temp.path()),
            "Bash",
            json!({
                "command":"sh -c 'sleep 30 & echo $! > descendant.pid; wait'",
                "timeout":500
            }),
        )
        .await;
    assert!(output.is_error);
    assert!(output.content.contains("timed out"));
    let pid = std::fs::read_to_string(temp.path().join("descendant.pid"))
        .unwrap()
        .trim()
        .to_owned();
    let pid = pid.parse::<i32>().unwrap();
    assert!(
        wait_for_process_exit(pid).await,
        "descendant process {pid} survived timeout"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn foreground_bash_reaps_detached_descendants_after_shell_exit() {
    let temp = tempdir().unwrap();
    let output = ToolRegistry::default()
        .execute(
            &context(temp.path()),
            "Bash",
            json!({
                "command":"sh -c 'sleep 30 >/dev/null 2>&1 & echo $! > detached.pid'"
            }),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    let pid = std::fs::read_to_string(temp.path().join("detached.pid"))
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    let exited = wait_for_process_exit(pid).await;
    if !exited {
        // SAFETY: this PID came from the test-owned child process.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
    assert!(
        exited,
        "detached foreground descendant process {pid} survived normal shell exit"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_foreground_bash_terminates_descendant_processes() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    let execution = tokio::spawn({
        let context = context.clone();
        let registry = registry.clone();
        async move {
            registry
                .execute(
                    &context,
                    "Bash",
                    json!({
                        "command":"sh -c 'sleep 30 & echo $! > cancelled.pid; wait'"
                    }),
                )
                .await
        }
    });
    let pid_path = temp.path().join("cancelled.pid");
    for _ in 0..100 {
        if pid_path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let pid = std::fs::read_to_string(&pid_path)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    execution.abort();
    let _ = execution.await;
    assert!(
        wait_for_process_exit(pid).await,
        "foreground descendant process {pid} survived task cancellation"
    );
}

#[tokio::test]
async fn task_output_blocks_until_background_command_finishes() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    let started = registry
        .execute(
            &context,
            "Bash",
            json!({"command":"echo task-output-ready","run_in_background":true}),
        )
        .await;
    assert!(!started.is_error, "{}", started.content);
    let task_id = started
        .content
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("Command running in background with ID: "))
        .unwrap();
    let output = registry
        .execute(
            &context,
            "TaskOutput",
            json!({"task_id":task_id,"block":true,"timeout":5000}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(output.content.contains("completed"));
    assert!(output.content.contains("task-output-ready"));
}

#[tokio::test]
async fn task_stop_does_not_discard_a_naturally_completed_task_result() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    let started = registry
        .execute(
            &context,
            "Bash",
            json!({"command":"printf natural-completion","run_in_background":true}),
        )
        .await;
    assert!(!started.is_error, "{}", started.content);
    let task_id = started
        .content
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("Command running in background with ID: "))
        .unwrap()
        .to_owned();

    let mut naturally_finished = false;
    for _ in 0..200 {
        let finished = {
            let mut tasks = context.tasks.lock().await;
            let task = tasks.get_mut(&task_id).unwrap();
            task.child.try_wait().unwrap().is_some()
                && task.drains.iter().all(tokio::task::JoinHandle::is_finished)
        };
        if finished {
            naturally_finished = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        naturally_finished,
        "background command did not finish in time"
    );

    let stopped = registry
        .execute(&context, "TaskStop", json!({"task_id":task_id}))
        .await;
    assert!(stopped.is_error);
    assert!(
        stopped.content.contains("TaskOutput"),
        "{}",
        stopped.content
    );

    let output = registry
        .execute(
            &context,
            "TaskOutput",
            json!({"task_id":task_id,"block":true,"timeout":5000}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(output.content.contains("natural-completion"));
}

#[cfg(unix)]
#[tokio::test]
async fn task_stop_terminates_background_process_group() {
    let temp = tempdir().unwrap();
    let context = context(temp.path());
    let registry = ToolRegistry::default();
    let started = registry
        .execute(
            &context,
            "Bash",
            json!({
                "command":"sh -c 'sleep 30 & echo $! > background.pid; wait'",
                "run_in_background":true
            }),
        )
        .await;
    assert!(!started.is_error, "{}", started.content);
    let task_id = started
        .content
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("Command running in background with ID: "))
        .unwrap()
        .to_owned();
    let pid_path = temp.path().join("background.pid");
    for _ in 0..50 {
        if pid_path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let pid = std::fs::read_to_string(&pid_path)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    let stopped = registry
        .execute(&context, "TaskStop", json!({"task_id":task_id}))
        .await;
    assert!(!stopped.is_error, "{}", stopped.content);
    assert!(
        wait_for_process_exit(pid).await,
        "background descendant process {pid} survived TaskStop"
    );
}

#[tokio::test]
async fn local_skill_loads_text_without_executing_bundled_files() {
    let temp = tempdir().unwrap();
    let skill_root = temp.path().join(".open-agent-harness/skills/review");
    std::fs::create_dir_all(&skill_root).unwrap();
    std::fs::write(
        skill_root.join("SKILL.md"),
        "---\nname: review\ndescription: Review a patch\n---\nRead the diff and report evidence.",
    )
    .unwrap();
    std::fs::write(skill_root.join("must-not-run.sh"), "touch executed").unwrap();
    let context = context(temp.path());
    context.set_skills(discover_skills(temp.path(), false).unwrap());
    let output = ToolRegistry::default()
        .execute(
            &context,
            "Skill",
            json!({"name":"review","arguments":"focus on safety"}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(output.content.contains("Read the diff"));
    assert!(output.content.contains("focus on safety"));
    assert!(!temp.path().join("executed").exists());
}

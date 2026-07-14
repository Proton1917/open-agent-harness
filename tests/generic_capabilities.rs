use std::sync::Arc;

use open_agent_harness::{
    file_history::{CheckpointBoundary, FileHistory},
    interactions::UserInteractionHandler,
    permissions::{PermissionManager, PermissionMode},
    tools::{ToolContext, ToolRegistry},
};
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

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
async fn ask_user_question_uses_the_headless_interaction_handler() {
    let workspace = tempdir().unwrap();
    let context = context(workspace.path());
    let handler: UserInteractionHandler = Arc::new(|request| {
        assert_eq!(request.tool, "AskUserQuestion");
        Ok(json!({"answers":{"Which path?":"Safe"}}))
    });
    context.set_user_interaction_handler(Some(handler));
    let output = ToolRegistry::default()
        .execute(
            &context,
            "AskUserQuestion",
            json!({
                "questions":[{
                    "question":"Which path?",
                    "header":"Path",
                    "options":[
                        {"label":"Safe", "description":"Use the safe path"},
                        {"label":"Fast", "description":"Use the fast path"}
                    ],
                    "multiSelect":false
                }]
            }),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(output.content.contains("Which path?"));
    assert!(output.content.contains("Safe"));
}

#[tokio::test]
async fn ask_user_question_rejects_model_supplied_answers_at_the_public_schema() {
    let workspace = tempdir().unwrap();
    let context = context(workspace.path());
    let handler: UserInteractionHandler = Arc::new(|_| {
        panic!("schema-invalid model input must not reach the trusted interaction handler")
    });
    context.set_user_interaction_handler(Some(handler));
    let output = ToolRegistry::default()
        .execute(
            &context,
            "AskUserQuestion",
            json!({
                "questions":[{
                    "question":"Which path?",
                    "header":"Path",
                    "options":[
                        {"label":"Safe", "description":"Use the safe path"},
                        {"label":"Fast", "description":"Use the fast path"}
                    ]
                }],
                "answers":{"Which path?":"Fast"}
            }),
        )
        .await;
    assert!(output.is_error);
    assert!(output.content.contains("工具输入校验失败"));
}

#[tokio::test]
async fn ask_user_question_fails_closed_without_tty_or_handler() {
    let workspace = tempdir().unwrap();
    let output = ToolRegistry::default()
        .execute(
            &context(workspace.path()),
            "AskUserQuestion",
            json!({
                "questions":[{
                    "question":"Continue?",
                    "header":"Continue",
                    "options":[
                        {"label":"Yes", "description":"Continue"},
                        {"label":"No", "description":"Stop"}
                    ]
                }]
            }),
        )
        .await;
    assert!(output.is_error);
    assert!(output.content.contains("交互式终端"));
}

#[tokio::test]
async fn write_is_tracked_and_can_be_rewound_to_a_checkpoint() {
    let workspace = tempdir().unwrap();
    let storage = tempdir().unwrap();
    let context = context(workspace.path());
    let history =
        FileHistory::create_in(workspace.path(), Uuid::new_v4(), storage.path(), true).unwrap();
    context.set_file_history(history);
    let checkpoint = context
        .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
        .unwrap()
        .unwrap();
    let output = ToolRegistry::default()
        .execute(
            &context,
            "Write",
            json!({"file_path":"created.txt", "content":"temporary\n"}),
        )
        .await;
    assert!(!output.is_error, "{}", output.content);
    assert!(workspace.path().join("created.txt").exists());
    let (report, message_count) = context.rewind_files(checkpoint.id, 0).unwrap();
    assert_eq!(message_count, 0);
    assert_eq!(report.deleted, 1);
    assert!(!workspace.path().join("created.txt").exists());
}

#[test]
fn disabled_file_history_does_not_publish_fake_checkpoints() {
    let workspace = tempdir().unwrap();
    let context = context(workspace.path());
    context.set_file_history(FileHistory::create(workspace.path(), Uuid::new_v4(), false).unwrap());
    assert!(
        context
            .begin_file_checkpoint(CheckpointBoundary::UserMessage, 0)
            .unwrap()
            .is_none()
    );
}

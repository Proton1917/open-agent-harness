use open_agent_harness::terminal_dialogs::{
    DialogInput, DialogUpdate, PermissionDialogData, PermissionDialogItem, PermissionManagerAction,
    PermissionManagerDialog, PermissionTab, SettingItem, SettingValue, SettingsDialog,
    SettingsDialogAction, SettingsSnapshot, TaskCategory, TaskDialog, TaskDialogAction,
    TaskDialogItem, TaskState,
};

#[test]
fn permission_manager_emits_actions_without_mutating_input_data() {
    let data = PermissionDialogData {
        allow: vec![PermissionDialogItem::new("read", "Read(*)", "user rule")],
        ..PermissionDialogData::default()
    };
    let mut dialog = PermissionManagerDialog::new(data);
    assert_eq!(dialog.tab(), PermissionTab::Allow);
    assert_eq!(
        dialog.handle(DialogInput::Character('x')),
        DialogUpdate::Continue
    );
    assert_eq!(
        dialog.handle(DialogInput::Character('n')),
        DialogUpdate::Continue
    );
    assert_eq!(
        dialog.handle(DialogInput::Character('x')),
        DialogUpdate::Continue
    );
    assert_eq!(
        dialog.handle(DialogInput::Enter),
        DialogUpdate::Action(PermissionManagerAction::DeleteRule {
            tab: PermissionTab::Allow,
            id: "read".to_owned(),
            rule: "Read(*)".to_owned(),
        })
    );
    dialog.handle(DialogInput::Character('a'));
    for character in "Bash(git status)".chars() {
        dialog.handle(DialogInput::Character(character));
    }
    assert_eq!(
        dialog.handle(DialogInput::Enter),
        DialogUpdate::Action(PermissionManagerAction::AddRule {
            tab: PermissionTab::Allow,
            rule: "Bash(git status)".to_owned(),
        })
    );
}

#[test]
fn task_dialog_auto_details_and_routes_foreground_or_output() {
    let mut dialog = TaskDialog::new(vec![TaskDialogItem {
        id: "agent-1".to_owned(),
        title: "review".to_owned(),
        detail: "reviewing files".to_owned(),
        category: TaskCategory::Agent,
        state: TaskState::Running,
        can_foreground: true,
        has_output: true,
    }]);
    assert!(dialog.is_detail());
    assert_eq!(
        dialog.handle(DialogInput::Character('f')),
        DialogUpdate::Action(TaskDialogAction::Foreground {
            id: "agent-1".to_owned()
        })
    );
}

#[test]
fn settings_save_returns_changes_and_escape_returns_original_snapshot() {
    let original = SettingsSnapshot::new(vec![SettingItem {
        key: "streaming".to_owned(),
        label: "Streaming".to_owned(),
        description: "Render incremental output".to_owned(),
        value: SettingValue::Boolean(false),
    }]);
    let mut dialog = SettingsDialog::new(original.clone());
    dialog.handle(DialogInput::Enter);
    let DialogUpdate::Action(SettingsDialogAction::Save { snapshot, changes }) =
        dialog.handle(DialogInput::Save)
    else {
        panic!("save action expected");
    };
    assert_eq!(changes.len(), 1);
    assert_ne!(snapshot, original);
    assert_eq!(
        dialog.handle(DialogInput::Escape),
        DialogUpdate::Action(SettingsDialogAction::Cancel { snapshot: original })
    );

    let frame = dialog.render(8, 3);
    assert!(frame.lines().len() <= 3);
}

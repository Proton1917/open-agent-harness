use std::{fs, path::Path, process::Command};

use serde_json::Value;

fn write_plugin(root: &Path, version: &str, marker: &str) {
    fs::create_dir_all(root.join("commands")).unwrap();
    fs::write(
        root.join("plugin.json"),
        serde_json::json!({
            "name": "cli-lifecycle",
            "version": version,
            "description": "CLI lifecycle integration test",
            "commands": ["commands"]
        })
        .to_string(),
    )
    .unwrap();
    fs::write(root.join("commands/check.md"), marker).unwrap();
}

fn plugin_command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_open-agent-harness"));
    command
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("HARNESS_API_KEY")
        .env_remove("HARNESS_AUTH_TOKEN")
        .env_remove("HARNESS_BASE_URL")
        .env_remove("HARNESS_API_PATH")
        .env_remove("HARNESS_MESSAGES_PATH");
    command
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn plugin_cli_runs_the_full_local_lifecycle_without_a_model_endpoint() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let source = temp.path().join("source");
    fs::create_dir_all(&home).unwrap();
    write_plugin(&source, "1.0.0", "VERSION_ONE");
    let source = source.to_str().unwrap();

    let validate = plugin_command(&home)
        .args(["plugin", "validate", source])
        .output()
        .unwrap();
    assert_success(&validate);
    let validated: Value = serde_json::from_slice(&validate.stdout).unwrap();
    assert_eq!(validated["id"], "cli-lifecycle");
    assert_eq!(validated["version"], "1.0.0");

    let install = plugin_command(&home)
        .args(["plugin", "install", source])
        .output()
        .unwrap();
    assert_success(&install);

    let list = plugin_command(&home)
        .args(["plugin", "list", "--json"])
        .output()
        .unwrap();
    assert_success(&list);
    let installed: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(installed.as_array().unwrap().len(), 1);
    assert_eq!(installed[0]["id"], "cli-lifecycle");
    assert_eq!(installed[0]["version"], "1.0.0");

    write_plugin(Path::new(source), "2.0.0", "VERSION_TWO");
    let update = plugin_command(&home)
        .args(["plugin", "update", "cli-lifecycle"])
        .output()
        .unwrap();
    assert_success(&update);

    let list = plugin_command(&home)
        .args(["plugin", "list", "--json"])
        .output()
        .unwrap();
    assert_success(&list);
    let installed: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(installed[0]["version"], "2.0.0");

    let uninstall = plugin_command(&home)
        .args(["plugin", "uninstall", "cli-lifecycle"])
        .output()
        .unwrap();
    assert_success(&uninstall);

    let list = plugin_command(&home)
        .args(["plugin", "list", "--json"])
        .output()
        .unwrap();
    assert_success(&list);
    let installed: Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(installed, serde_json::json!([]));
}

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::{
    MAX_EDITABLE_FILE_BYTES, Tool, ToolContext, ToolOutput, atomic_write, object_schema,
    parse_input, read_text_bounded, reject_direct_symlink_write,
};

#[derive(Deserialize)]
struct Input {
    notebook_path: String,
    cell_id: Option<String>,
    new_source: String,
    cell_type: Option<String>,
    #[serde(default = "default_edit_mode")]
    edit_mode: String,
}

fn default_edit_mode() -> String {
    "replace".into()
}

pub struct NotebookEditTool;

#[async_trait]
impl Tool for NotebookEditTool {
    fn name(&self) -> &'static str {
        "NotebookEdit"
    }

    fn description(&self) -> &'static str {
        "Replaces, inserts, or deletes one Jupyter notebook cell. Read the complete notebook first; cells without IDs can be addressed as cell-<zero-based-index>."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "notebook_path": {"type": "string", "maxLength": 4096},
                "cell_id": {"type": "string", "maxLength": 256},
                "new_source": {"type": "string", "maxLength": MAX_EDITABLE_FILE_BYTES},
                "cell_type": {"type": "string", "enum": ["code", "markdown"]},
                "edit_mode": {"type": "string", "enum": ["replace", "insert", "delete"]}
            }),
            &["notebook_path", "new_source"],
        )
    }

    fn read_only(&self, _: &Value) -> bool {
        false
    }

    fn destructive(&self, input: &Value) -> bool {
        input.get("edit_mode").and_then(Value::as_str) == Some("delete")
    }

    fn path_fields(&self) -> &'static [&'static str] {
        &["notebook_path"]
    }

    fn summary(&self, input: &Value) -> String {
        let path = input
            .get("notebook_path")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        let mode = input
            .get("edit_mode")
            .and_then(Value::as_str)
            .unwrap_or("replace");
        let cell = input
            .get("cell_id")
            .and_then(Value::as_str)
            .unwrap_or("beginning");
        format!("{path}:{cell} ({mode})")
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        let path = context.resolve_path(&input.notebook_path)?;
        if path.extension().and_then(|value| value.to_str()) != Some("ipynb") {
            bail!("NotebookEdit 只接受 .ipynb 文件")
        }

        reject_direct_symlink_write(&path)?;
        context.require_full_read(&path).await?;
        let original = read_text_bounded(&path)
            .with_context(|| format!("无法读取 notebook {}", path.display()))?;
        context.verify_fresh_full_read(&path, &original).await?;
        let mut notebook: Value = serde_json::from_str(&original)
            .with_context(|| format!("notebook 不是有效 JSON: {}", path.display()))?;
        let object = notebook
            .as_object_mut()
            .context("notebook 顶层必须是 JSON object")?;
        let supports_cell_ids = notebook_supports_cell_ids(object);
        let cells = object
            .get_mut("cells")
            .and_then(Value::as_array_mut)
            .context("notebook 的 cells 必须是 array")?;
        if cells.iter().any(|cell| !cell.is_object()) {
            bail!("notebook cells 只能包含 object")
        }

        let result = match input.edit_mode.as_str() {
            "insert" => insert_cell(cells, &input, supports_cell_ids)?,
            "replace" => replace_cell(cells, &input)?,
            "delete" => delete_cell(cells, &input)?,
            _ => bail!("不支持的 edit_mode: {}", input.edit_mode),
        };

        let updated = serde_json::to_string_pretty(&notebook)? + "\n";
        if updated.len() > MAX_EDITABLE_FILE_BYTES {
            bail!("更新后的 notebook 超过 {MAX_EDITABLE_FILE_BYTES} 字节限制")
        }
        atomic_write(&path, &updated)?;
        context.remember_read(path, updated, false).await?;
        Ok(ToolOutput::success(result))
    }
}

fn notebook_supports_cell_ids(notebook: &Map<String, Value>) -> bool {
    let major = notebook
        .get("nbformat")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let minor = notebook
        .get("nbformat_minor")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    major > 4 || (major == 4 && minor >= 5)
}

fn insert_cell(cells: &mut Vec<Value>, input: &Input, with_id: bool) -> Result<String> {
    let cell_type = input
        .cell_type
        .as_deref()
        .context("insert 模式必须提供 cell_type")?;
    let position = match input.cell_id.as_deref() {
        Some(id) => find_cell(cells, id)? + 1,
        None => 0,
    };
    let generated_id = with_id.then(|| {
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(8)
            .collect::<String>()
    });
    let mut cell = Map::new();
    cell.insert("cell_type".into(), Value::String(cell_type.into()));
    if let Some(id) = &generated_id {
        cell.insert("id".into(), Value::String(id.clone()));
    }
    cell.insert("metadata".into(), json!({}));
    cell.insert("source".into(), Value::String(input.new_source.clone()));
    normalize_cell_kind(&mut cell, cell_type);
    cells.insert(position, Value::Object(cell));
    let id = generated_id.unwrap_or_else(|| format!("cell-{position}"));
    Ok(format!("Inserted {cell_type} cell {id}"))
}

fn replace_cell(cells: &mut [Value], input: &Input) -> Result<String> {
    let requested = input
        .cell_id
        .as_deref()
        .context("replace 模式必须提供 cell_id")?;
    let index = find_cell(cells, requested)?;
    let cell = cells[index]
        .as_object_mut()
        .context("目标 cell 不是 object")?;
    cell.insert("source".into(), Value::String(input.new_source.clone()));
    if let Some(cell_type) = input.cell_type.as_deref() {
        cell.insert("cell_type".into(), Value::String(cell_type.into()));
    }
    let final_type = cell
        .get("cell_type")
        .and_then(Value::as_str)
        .unwrap_or("code")
        .to_owned();
    normalize_cell_kind(cell, &final_type);
    let id = cell
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("cell-{index}"));
    Ok(format!("Updated {final_type} cell {id}"))
}

fn delete_cell(cells: &mut Vec<Value>, input: &Input) -> Result<String> {
    let requested = input
        .cell_id
        .as_deref()
        .context("delete 模式必须提供 cell_id")?;
    let index = find_cell(cells, requested)?;
    let removed = cells.remove(index);
    let id = removed
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(requested);
    Ok(format!("Deleted cell {id}"))
}

fn find_cell(cells: &[Value], requested: &str) -> Result<usize> {
    if let Some(index) = cells.iter().position(|cell| {
        cell.get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| id == requested)
    }) {
        return Ok(index);
    }
    if let Some(index) = requested
        .strip_prefix("cell-")
        .and_then(|value| value.parse::<usize>().ok())
        && index < cells.len()
    {
        return Ok(index);
    }
    bail!("notebook 中找不到 cell_id `{requested}`")
}

fn normalize_cell_kind(cell: &mut Map<String, Value>, cell_type: &str) {
    match cell_type {
        "code" => {
            cell.insert("execution_count".into(), Value::Null);
            cell.insert("outputs".into(), json!([]));
        }
        "markdown" => {
            cell.remove("execution_count");
            cell.remove("outputs");
        }
        _ => {}
    }
}

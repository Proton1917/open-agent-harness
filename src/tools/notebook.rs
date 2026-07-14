use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use super::{
    MAX_EDITABLE_FILE_BYTES, Tool, ToolContext, ToolOutput, atomic_write, object_schema,
    parse_input, read_text_bounded, reject_direct_symlink_write,
};

const MAX_PATH_BYTES: usize = 4096;
const MAX_CELL_ID_BYTES: usize = 256;
const UTF8_BOM: &str = "\u{feff}";

#[derive(Clone, Copy, PartialEq, Eq)]
enum LineEnding {
    Lf,
    CrLf,
}

impl LineEnding {
    fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
        }
    }
}

enum JsonIndent {
    Compact,
    Pretty(Vec<u8>),
}

struct NotebookFormat {
    bom: bool,
    line_ending: LineEnding,
    trailing_newline: bool,
    indent: JsonIndent,
}

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
    fn name(&self) -> &str {
        "NotebookEdit"
    }

    fn description(&self) -> &str {
        "Replaces, inserts, or deletes one Jupyter notebook cell. Read the complete notebook first; cells without IDs can be addressed as cell-<zero-based-index>."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "notebook_path": {"type": "string", "maxLength": MAX_PATH_BYTES},
                "cell_id": {"type": "string", "maxLength": MAX_CELL_ID_BYTES},
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
        validate_input_bytes(&input)?;
        let path = context.resolve_path(&input.notebook_path)?;
        if path.extension().and_then(|value| value.to_str()) != Some("ipynb") {
            bail!("NotebookEdit 只接受 .ipynb 文件")
        }

        reject_direct_symlink_write(&path)?;
        context.require_full_read(&path).await?;
        let original = read_text_bounded(&path)
            .with_context(|| format!("无法读取 notebook {}", path.display()))?;
        context.verify_fresh_full_read(&path, &original).await?;
        let (json_text, format) = detect_notebook_format(&original);
        let mut notebook: Value = serde_json::from_str(json_text)
            .with_context(|| format!("notebook 不是有效 JSON: {}", path.display()))?;
        let notebook_before_edit = notebook.clone();
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

        if notebook == notebook_before_edit {
            return Ok(ToolOutput::success(result));
        }

        let updated = serialize_notebook(&notebook, &format)?;
        if updated.len() > MAX_EDITABLE_FILE_BYTES {
            bail!("更新后的 notebook 超过 {MAX_EDITABLE_FILE_BYTES} 字节限制")
        }
        context.track_before_edit(&path)?;
        context.expect_after_edit(&path, updated.as_bytes())?;
        atomic_write(&path, &updated)?;
        context.remember_read(path, updated, false).await?;
        Ok(ToolOutput::success(result))
    }
}

fn detect_notebook_format(original: &str) -> (&str, NotebookFormat) {
    let (bom, json_text) = match original.strip_prefix(UTF8_BOM) {
        Some(text) => (true, text),
        None => (false, original),
    };
    let line_ending = json_text
        .find('\n')
        .map(|index| {
            if json_text[..index].ends_with('\r') {
                LineEnding::CrLf
            } else {
                LineEnding::Lf
            }
        })
        .unwrap_or(LineEnding::Lf);
    let trailing_newline = json_text.ends_with('\n');
    let indent = detect_json_indent(json_text);
    (
        json_text,
        NotebookFormat {
            bom,
            line_ending,
            trailing_newline,
            indent,
        },
    )
}

fn detect_json_indent(json_text: &str) -> JsonIndent {
    if !json_text.contains('\n') {
        return JsonIndent::Compact;
    }
    let indent = json_text
        .split('\n')
        .filter_map(|line| {
            let line = line.strip_suffix('\r').unwrap_or(line);
            let width = line
                .bytes()
                .take_while(|byte| matches!(byte, b' ' | b'\t'))
                .count();
            (width > 0).then(|| line.as_bytes()[..width].to_vec())
        })
        .min_by_key(Vec::len)
        .unwrap_or_default();
    JsonIndent::Pretty(indent)
}

fn serialize_notebook(notebook: &Value, format: &NotebookFormat) -> Result<String> {
    let mut bytes = Vec::new();
    match &format.indent {
        JsonIndent::Compact => serde_json::to_writer(&mut bytes, notebook)?,
        JsonIndent::Pretty(indent) => {
            let formatter = serde_json::ser::PrettyFormatter::with_indent(indent);
            let mut serializer = serde_json::Serializer::with_formatter(&mut bytes, formatter);
            notebook.serialize(&mut serializer)?;
        }
    }
    let mut rendered = String::from_utf8(bytes).context("notebook JSON 序列化产生非 UTF-8")?;
    if format.line_ending == LineEnding::CrLf {
        rendered = rendered.replace('\n', "\r\n");
    }
    if format.trailing_newline {
        rendered.push_str(format.line_ending.as_str());
    }
    if format.bom {
        rendered.insert_str(0, UTF8_BOM);
    }
    Ok(rendered)
}

fn validate_input_bytes(input: &Input) -> Result<()> {
    ensure_utf8_bytes("notebook_path", &input.notebook_path, MAX_PATH_BYTES)?;
    if let Some(cell_id) = &input.cell_id {
        ensure_utf8_bytes("cell_id", cell_id, MAX_CELL_ID_BYTES)?;
    }
    ensure_utf8_bytes("new_source", &input.new_source, MAX_EDITABLE_FILE_BYTES)
}

fn ensure_utf8_bytes(field: &str, value: &str, limit: usize) -> Result<()> {
    if value.len() > limit {
        bail!("{field} 超过 {limit} 字节限制")
    }
    Ok(())
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
    let source_changed = cell.get("source") != Some(&Value::String(input.new_source.clone()));
    let type_changed = input
        .cell_type
        .as_deref()
        .is_some_and(|cell_type| cell.get("cell_type").and_then(Value::as_str) != Some(cell_type));
    cell.insert("source".into(), Value::String(input.new_source.clone()));
    if let Some(cell_type) = input.cell_type.as_deref() {
        cell.insert("cell_type".into(), Value::String(cell_type.into()));
    }
    let final_type = cell
        .get("cell_type")
        .and_then(Value::as_str)
        .unwrap_or("code")
        .to_owned();
    if source_changed || type_changed {
        normalize_cell_kind(cell, &final_type);
    }
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
        .filter(|index| *index < cells.len())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{PermissionManager, PermissionMode};

    fn context(path: &std::path::Path) -> ToolContext {
        ToolContext::new(
            path.to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        )
    }

    fn fixture_value() -> Value {
        json!({
            "cells": [
                {
                    "cell_type":"code",
                    "id":"cell-a",
                    "metadata":{},
                    "source":"print(1)",
                    "execution_count":7,
                    "outputs":[{"output_type":"stream", "text":"1"}]
                },
                {
                    "cell_type":"markdown",
                    "id":"cell-b",
                    "metadata":{"trusted":true},
                    "source":"unchanged"
                }
            ],
            "metadata":{"language_info":{"name":"python"}},
            "nbformat":4,
            "nbformat_minor":5
        })
    }

    fn formatted_value(value: &Value, indent: &[u8], line_ending: LineEnding, bom: bool) -> String {
        let mut bytes = Vec::new();
        let formatter = serde_json::ser::PrettyFormatter::with_indent(indent);
        let mut serializer = serde_json::Serializer::with_formatter(&mut bytes, formatter);
        value.serialize(&mut serializer).unwrap();
        let mut rendered = String::from_utf8(bytes).unwrap();
        if line_ending == LineEnding::CrLf {
            rendered = rendered.replace('\n', "\r\n");
        }
        rendered.push_str(line_ending.as_str());
        if bom {
            rendered.insert_str(0, UTF8_BOM);
        }
        rendered
    }

    fn formatted_fixture(indent: &[u8], line_ending: LineEnding, bom: bool) -> String {
        formatted_value(&fixture_value(), indent, line_ending, bom)
    }

    #[tokio::test]
    async fn edit_preserves_bom_line_endings_and_common_indentation() {
        for (indent, line_ending, bom) in [
            (&b" "[..], LineEnding::Lf, false),
            (&b"  "[..], LineEnding::CrLf, true),
            (&b"    "[..], LineEnding::Lf, true),
            (&b"\t"[..], LineEnding::CrLf, false),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("format.ipynb");
            let original = formatted_fixture(indent, line_ending, bom);
            std::fs::write(&path, &original).unwrap();
            let context = context(temp.path());
            let resolved = context.resolve_path("format.ipynb").unwrap();
            context
                .remember_read(resolved, original, false)
                .await
                .unwrap();

            let output = NotebookEditTool
                .execute(
                    &context,
                    json!({
                        "notebook_path":"format.ipynb",
                        "cell_id":"cell-a",
                        "new_source":"print(2)"
                    }),
                )
                .await
                .unwrap();
            assert!(!output.is_error, "{}", output.content);
            let updated = std::fs::read_to_string(&path).unwrap();
            assert_eq!(updated.starts_with(UTF8_BOM), bom);
            let body = updated.strip_prefix(UTF8_BOM).unwrap_or(&updated);
            let indent = std::str::from_utf8(indent).unwrap();
            assert!(body.contains(&format!("{}{}\"cells\"", line_ending.as_str(), indent)));
            assert!(body.ends_with(line_ending.as_str()));
            match line_ending {
                LineEnding::Lf => assert!(!body.contains("\r\n")),
                LineEnding::CrLf => assert!(!body.replace("\r\n", "").contains('\n')),
            }
            let parsed: Value = serde_json::from_str(body).unwrap();
            assert_eq!(parsed["cells"][0]["source"], "print(2)");
            assert_eq!(parsed["cells"][1], fixture_value()["cells"][1]);
            assert_eq!(parsed["metadata"], fixture_value()["metadata"]);
            let mut expected = fixture_value();
            expected["cells"][0]["source"] = Value::String("print(2)".to_owned());
            expected["cells"][0]["execution_count"] = Value::Null;
            expected["cells"][0]["outputs"] = json!([]);
            assert_eq!(
                updated,
                formatted_value(&expected, indent.as_bytes(), line_ending, bom),
                "editing one cell must not churn unrelated notebook bytes"
            );
        }
    }

    #[tokio::test]
    async fn replacing_with_identical_content_is_byte_stable() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("stable.ipynb");
        let original = formatted_fixture(b"    ", LineEnding::CrLf, true);
        std::fs::write(&path, &original).unwrap();
        let context = context(temp.path());
        let resolved = context.resolve_path("stable.ipynb").unwrap();
        context
            .remember_read(resolved, original.clone(), false)
            .await
            .unwrap();

        NotebookEditTool
            .execute(
                &context,
                json!({
                    "notebook_path":"stable.ipynb",
                    "cell_id":"cell-a",
                    "new_source":"print(1)"
                }),
            )
            .await
            .unwrap();
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn non_utf8_notebook_remains_explicitly_unsupported() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("invalid.ipynb");
        std::fs::write(&path, [0xff, 0xfe, b'{', b'}']).unwrap();
        let error = read_text_bounded(&path).unwrap_err();
        assert!(error.to_string().contains("UTF-8"), "{error:#}");
    }

    #[test]
    fn four_byte_unicode_respects_source_byte_boundaries() {
        let at_limit = "🦀".repeat(MAX_EDITABLE_FILE_BYTES / 4);
        let over_limit = format!("{at_limit}🦀");
        let valid: Input = serde_json::from_value(json!({
            "notebook_path": "analysis.ipynb",
            "cell_id": "cell-a",
            "new_source": at_limit,
        }))
        .unwrap();
        let oversized: Input = serde_json::from_value(json!({
            "notebook_path": "analysis.ipynb",
            "cell_id": "cell-a",
            "new_source": over_limit,
        }))
        .unwrap();

        assert!(validate_input_bytes(&valid).is_ok());
        assert!(validate_input_bytes(&oversized).is_err());
    }

    #[test]
    fn path_and_cell_id_limits_are_checked_in_utf8_bytes() {
        let path_at_limit = "🦀".repeat(MAX_PATH_BYTES / 4);
        let path_over_limit = format!("{path_at_limit}🦀");
        let cell_at_limit = "🦀".repeat(MAX_CELL_ID_BYTES / 4);
        let cell_over_limit = format!("{cell_at_limit}🦀");
        let input = |notebook_path, cell_id| Input {
            notebook_path,
            cell_id: Some(cell_id),
            new_source: String::new(),
            cell_type: None,
            edit_mode: default_edit_mode(),
        };

        assert!(validate_input_bytes(&input(path_at_limit.clone(), cell_at_limit.clone())).is_ok());
        assert!(validate_input_bytes(&input(path_over_limit, cell_at_limit)).is_err());
        assert!(validate_input_bytes(&input(path_at_limit, cell_over_limit)).is_err());
    }
}

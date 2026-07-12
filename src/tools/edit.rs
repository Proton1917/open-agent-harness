use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{Tool, ToolContext, ToolOutput, atomic_write, object_schema, parse_input};

#[derive(Deserialize)]
struct Input {
    file_path: String,
    old_string: String,
    new_string: String,
    #[serde(default)]
    replace_all: bool,
}

pub struct EditTool;

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &'static str {
        "Edit"
    }
    fn description(&self) -> &'static str {
        "Performs an exact string replacement in a fully-read file, with uniqueness and stale-read checks."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "file_path": {"type": "string"},
                "old_string": {"type": "string"},
                "new_string": {"type": "string"},
                "replace_all": {"type": "boolean", "default": false}
            }),
            &["file_path", "old_string", "new_string"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
        false
    }
    fn destructive(&self, _: &Value) -> bool {
        true
    }
    fn summary(&self, input: &Value) -> String {
        input
            .get("file_path")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned()
    }
    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let input: Input = parse_input(input)?;
        if input.old_string == input.new_string {
            bail!("old_string 与 new_string 相同")
        }
        let path = context.resolve_path(&input.file_path)?;
        if path.extension().and_then(|s| s.to_str()) == Some("ipynb") {
            bail!("Jupyter Notebook 不能通过 Edit 修改")
        }
        let original = std::fs::read_to_string(&path)
            .with_context(|| format!("无法读取 {}", path.display()))?;
        context.verify_fresh_full_read(&path, &original).await?;
        let actual_old = find_actual_string(&original, &input.old_string)
            .context("String to replace not found in file")?;
        let matches = original.matches(&actual_old).count();
        if matches > 1 && !input.replace_all {
            bail!(
                "找到 {matches} 处匹配，但 replace_all=false；请提供更多上下文或设置 replace_all=true"
            )
        }
        let replacement = preserve_quote_style(&input.old_string, &actual_old, &input.new_string);
        let updated = if input.replace_all {
            original.replace(&actual_old, &replacement)
        } else {
            original.replacen(&actual_old, &replacement, 1)
        };
        atomic_write(&path, &updated)?;
        context.remember_read(path.clone(), updated, false).await?;
        Ok(ToolOutput::success(format!("Updated {}", path.display())))
    }
}

fn normalized_quote(ch: char) -> char {
    match ch {
        '‘' | '’' => '\'',
        '“' | '”' => '"',
        other => other,
    }
}

fn find_actual_string(file: &str, search: &str) -> Option<String> {
    if file.contains(search) {
        return Some(search.to_owned());
    }
    let file_chars = file.char_indices().collect::<Vec<_>>();
    let search_chars = search.chars().map(normalized_quote).collect::<Vec<_>>();
    if search_chars.is_empty() {
        return Some(String::new());
    }
    let start_char = file_chars.windows(search_chars.len()).position(|window| {
        window
            .iter()
            .map(|(_, ch)| normalized_quote(*ch))
            .eq(search_chars.iter().copied())
    })?;
    let start_byte = file_chars[start_char].0;
    let end_char = start_char + search_chars.len();
    let end_byte = file_chars
        .get(end_char)
        .map(|(offset, _)| *offset)
        .unwrap_or(file.len());
    Some(file[start_byte..end_byte].to_owned())
}

fn preserve_quote_style(old: &str, actual: &str, new: &str) -> String {
    if old == actual {
        return new.to_owned();
    }
    let curly_double = actual.contains(['“', '”']);
    let curly_single = actual.contains(['‘', '’']);
    let mut result = String::with_capacity(new.len());
    let mut previous = None;
    for ch in new.chars() {
        let opening = previous.is_none_or(|c: char| c.is_whitespace() || "([{<".contains(c));
        match ch {
            '"' if curly_double => result.push(if opening { '“' } else { '”' }),
            '\'' if curly_single => result.push(if opening { '‘' } else { '’' }),
            _ => result.push(ch),
        }
        previous = Some(ch);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_curly_quote_variant() {
        assert_eq!(
            find_actual_string("say “hello”", "say \"hello\""),
            Some("say “hello”".into())
        );
    }
}

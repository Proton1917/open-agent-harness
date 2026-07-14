use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    MAX_EDITABLE_FILE_BYTES, Tool, ToolContext, ToolOutput, atomic_write, object_schema,
    parse_input, read_text_bounded, reject_direct_symlink_write,
};

const MAX_PATH_BYTES: usize = 4096;

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
    fn name(&self) -> &str {
        "Edit"
    }
    fn description(&self) -> &str {
        "Performs an exact string replacement in a fully-read file, with uniqueness and stale-read checks."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "file_path": {"type": "string", "maxLength": MAX_PATH_BYTES},
                "old_string": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_EDITABLE_FILE_BYTES
                },
                "new_string": {"type": "string", "maxLength": MAX_EDITABLE_FILE_BYTES},
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
    fn path_fields(&self) -> &'static [&'static str] {
        &["file_path"]
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
        validate_input(&input)?;
        if input.old_string == input.new_string {
            bail!("old_string 与 new_string 相同")
        }
        let path = context.resolve_path(&input.file_path)?;
        if path.extension().and_then(|s| s.to_str()) == Some("ipynb") {
            bail!("Jupyter Notebook 不能通过 Edit 修改")
        }
        reject_direct_symlink_write(&path)?;
        context.require_full_read(&path).await?;
        let original =
            read_text_bounded(&path).with_context(|| format!("无法读取 {}", path.display()))?;
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
        let replacement_count = if input.replace_all { matches } else { 1 };
        let updated_bytes = replacement_output_bytes(
            original.len(),
            actual_old.len(),
            replacement.len(),
            replacement_count,
        )?;
        let updated = if input.replace_all {
            original.replace(&actual_old, &replacement)
        } else {
            original.replacen(&actual_old, &replacement, 1)
        };
        debug_assert_eq!(updated.len(), updated_bytes);
        context.track_before_edit(&path)?;
        context.expect_after_edit(&path, updated.as_bytes())?;
        atomic_write(&path, &updated)?;
        context.remember_read(path.clone(), updated, false).await?;
        Ok(ToolOutput::success(format!(
            "Updated {}",
            context.display_path(&path)
        )))
    }
}

fn validate_input(input: &Input) -> Result<()> {
    ensure_utf8_bytes("file_path", &input.file_path, MAX_PATH_BYTES)?;
    ensure_utf8_bytes("old_string", &input.old_string, MAX_EDITABLE_FILE_BYTES)?;
    ensure_utf8_bytes("new_string", &input.new_string, MAX_EDITABLE_FILE_BYTES)?;
    if input.old_string.is_empty() {
        bail!("old_string 不能为空")
    }
    Ok(())
}

fn ensure_utf8_bytes(field: &str, value: &str, limit: usize) -> Result<()> {
    if value.len() > limit {
        bail!("{field} 超过 {limit} 字节限制")
    }
    Ok(())
}

fn replacement_output_bytes(
    original_bytes: usize,
    old_bytes: usize,
    new_bytes: usize,
    replacements: usize,
) -> Result<usize> {
    if old_bytes == 0 {
        bail!("old_string 不能为空")
    }
    let removed = old_bytes
        .checked_mul(replacements)
        .context("计算替换后的文件大小时溢出")?;
    let added = new_bytes
        .checked_mul(replacements)
        .context("计算替换后的文件大小时溢出")?;
    let updated = original_bytes
        .checked_sub(removed)
        .and_then(|remaining| remaining.checked_add(added))
        .context("计算替换后的文件大小时溢出")?;
    if updated > MAX_EDITABLE_FILE_BYTES {
        bail!("更新后的文件超过 {MAX_EDITABLE_FILE_BYTES} 字节限制")
    }
    Ok(updated)
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

    #[test]
    fn rejects_empty_old_string() {
        let input = Input {
            file_path: "sample.txt".into(),
            old_string: String::new(),
            new_string: "replacement".into(),
            replace_all: true,
        };

        assert!(validate_input(&input).is_err());
    }

    #[test]
    fn replace_all_rejects_output_amplification_before_allocation() {
        let original = "x".repeat(MAX_EDITABLE_FILE_BYTES);
        let matches = original.matches('x').count();
        assert!(replacement_output_bytes(original.len(), 1, 2, matches).is_err());
    }

    #[test]
    fn four_byte_unicode_respects_utf8_byte_boundaries() {
        let at_limit = "🦀".repeat(MAX_EDITABLE_FILE_BYTES / 4);
        let over_limit = format!("{at_limit}🦀");
        let path_at_limit = "🦀".repeat(MAX_PATH_BYTES / 4);
        let path_over_limit = format!("{path_at_limit}🦀");
        let input = |file_path, old_string, new_string| Input {
            file_path,
            old_string,
            new_string,
            replace_all: true,
        };

        assert!(
            validate_input(&input(
                path_at_limit.clone(),
                at_limit.clone(),
                at_limit.clone(),
            ))
            .is_ok()
        );
        assert!(
            validate_input(&input(
                "sample.txt".into(),
                over_limit.clone(),
                at_limit.clone(),
            ))
            .is_err()
        );
        assert!(
            validate_input(&input("sample.txt".into(), at_limit.clone(), over_limit,)).is_err()
        );
        assert!(validate_input(&input(path_over_limit, at_limit, "replacement".into())).is_err());
    }
}

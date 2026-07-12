use serde_json::Value;

pub fn validate(schema: &Value, input: &Value) -> Result<(), String> {
    validate_at(schema, input, "$".to_owned())
}

fn validate_at(schema: &Value, input: &Value, path: String) -> Result<(), String> {
    if let Some(variants) = schema.get("anyOf").and_then(Value::as_array) {
        if variants
            .iter()
            .any(|variant| validate_at(variant, input, path.clone()).is_ok())
        {
            return Ok(());
        }
        return Err(format!("{path}: 不匹配 anyOf 中的任何 schema"));
    }

    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.iter().any(|value| value == input)
    {
        return Err(format!("{path}: 值不在允许的枚举中"));
    }

    if let Some(expected) = schema.get("type")
        && !matches_type(expected, input)
    {
        return Err(format!(
            "{path}: 期望 {}，实际为 {}",
            render_expected_type(expected),
            actual_type(input)
        ));
    }

    match input {
        Value::Object(object) => validate_object(schema, object, &path)?,
        Value::Array(items) => validate_array(schema, items, &path)?,
        Value::String(value) => validate_string(schema, value, &path)?,
        Value::Number(value) => {
            let number = value
                .as_f64()
                .ok_or_else(|| format!("{path}: 数字无法表示"))?;
            if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64)
                && number < minimum
            {
                return Err(format!("{path}: 必须大于或等于 {minimum}"));
            }
            if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64)
                && number > maximum
            {
                return Err(format!("{path}: 必须小于或等于 {maximum}"));
            }
        }
        Value::Bool(_) | Value::Null => {}
    }
    Ok(())
}

fn validate_object(
    schema: &Value,
    object: &serde_json::Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    let properties = schema.get("properties").and_then(Value::as_object);
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(name) {
                return Err(format!("{path}.{name}: 缺少必填字段"));
            }
        }
    }

    for (name, value) in object {
        let child_path = format!("{path}.{name}");
        if let Some(property_schema) = properties.and_then(|values| values.get(name)) {
            validate_at(property_schema, value, child_path)?;
            continue;
        }
        match schema.get("additionalProperties") {
            Some(Value::Bool(false)) => return Err(format!("{child_path}: 不允许额外字段")),
            Some(additional @ Value::Object(_)) => validate_at(additional, value, child_path)?,
            _ => {}
        }
    }
    Ok(())
}

fn validate_array(schema: &Value, items: &[Value], path: &str) -> Result<(), String> {
    if let Some(minimum) = schema.get("minItems").and_then(Value::as_u64)
        && items.len() < minimum as usize
    {
        return Err(format!("{path}: 至少需要 {minimum} 项"));
    }
    if let Some(maximum) = schema.get("maxItems").and_then(Value::as_u64)
        && items.len() > maximum as usize
    {
        return Err(format!("{path}: 最多允许 {maximum} 项"));
    }
    if let Some(item_schema) = schema.get("items") {
        for (index, item) in items.iter().enumerate() {
            validate_at(item_schema, item, format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn validate_string(schema: &Value, value: &str, path: &str) -> Result<(), String> {
    let length = value.chars().count();
    if let Some(minimum) = schema.get("minLength").and_then(Value::as_u64)
        && length < minimum as usize
    {
        return Err(format!("{path}: 长度至少为 {minimum}"));
    }
    if let Some(maximum) = schema.get("maxLength").and_then(Value::as_u64)
        && length > maximum as usize
    {
        return Err(format!("{path}: 长度最多为 {maximum}"));
    }
    Ok(())
}

fn matches_type(expected: &Value, input: &Value) -> bool {
    match expected {
        Value::String(kind) => matches_single_type(kind, input),
        Value::Array(kinds) => kinds
            .iter()
            .filter_map(Value::as_str)
            .any(|kind| matches_single_type(kind, input)),
        _ => false,
    }
}

fn matches_single_type(expected: &str, input: &Value) -> bool {
    match expected {
        "object" => input.is_object(),
        "array" => input.is_array(),
        "string" => input.is_string(),
        "integer" => input.as_i64().is_some() || input.as_u64().is_some(),
        "number" => input.is_number(),
        "boolean" => input.is_boolean(),
        "null" => input.is_null(),
        _ => false,
    }
}

fn render_expected_type(expected: &Value) -> String {
    match expected {
        Value::String(kind) => kind.clone(),
        Value::Array(kinds) => kinds
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" 或 "),
        _ => "有效类型".into(),
    }
}

fn actual_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn validates_nested_objects_and_constraints() {
        let schema = json!({
            "type": "object",
            "properties": {
                "mode": {"type": "string", "enum": ["read", "write"]},
                "items": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "properties": {"count": {"type": "integer", "minimum": 1}},
                        "required": ["count"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["mode", "items"],
            "additionalProperties": false
        });
        assert!(validate(&schema, &json!({"mode":"read","items":[{"count":1}]})).is_ok());
        assert!(validate(&schema, &json!({"mode":"other","items":[{"count":1}]})).is_err());
        assert!(validate(&schema, &json!({"mode":"read","items":[{"count":0}]})).is_err());
        assert!(validate(&schema, &json!({"mode":"read","items":[],"extra":true})).is_err());
    }
}

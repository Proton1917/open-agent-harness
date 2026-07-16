use std::{
    collections::{HashMap, HashSet},
    io::{self, IsTerminal, Write},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::control::ControlInterrupted;

use super::{Tool, ToolContext, ToolOutput, object_schema, schema};

const MAX_RESPONSE_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Deserialize)]
struct QuestionOption {
    label: String,
    description: String,
    #[serde(default, rename = "preview")]
    _preview: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct Question {
    question: String,
    header: String,
    options: Vec<QuestionOption>,
    #[serde(default, rename = "multiSelect")]
    multi_select: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    questions: Vec<Question>,
}

pub struct AskUserQuestionTool;

#[async_trait]
impl Tool for AskUserQuestionTool {
    fn name(&self) -> &str {
        "AskUserQuestion"
    }

    fn description(&self) -> &str {
        "Ask the user 1-4 focused multiple-choice questions when information is required to continue. Each question includes 2-4 choices; free-text Other is added automatically."
    }

    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "questions": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 4,
                    "items": {
                        "type": "object",
                        "properties": {
                            "question": {"type":"string", "minLength":1, "maxLength":2048},
                            "header": {"type":"string", "minLength":1, "maxLength":64},
                            "options": {
                                "type":"array", "minItems":2, "maxItems":4,
                                "items": {
                                    "type":"object",
                                    "properties": {
                                        "label":{"type":"string", "minLength":1, "maxLength":128},
                                        "description":{"type":"string", "minLength":1, "maxLength":2048},
                                        "preview":{"type":"string", "maxLength":65536}
                                    },
                                    "required":["label", "description"],
                                    "additionalProperties":false
                                }
                            },
                            "multiSelect":{"type":"boolean"}
                        },
                        "required":["question", "header", "options"],
                        "additionalProperties":false
                    }
                }
            }),
            &["questions"],
        )
    }

    fn read_only(&self, _input: &Value) -> bool {
        true
    }

    fn requires_permission(&self) -> bool {
        false
    }

    fn concurrency_safe(&self, _input: &Value) -> bool {
        false
    }

    fn validate_input(&self, input: &Value) -> std::result::Result<(), String> {
        schema::validate(&self.input_schema(), input)?;
        let input: Input =
            serde_json::from_value(input.clone()).map_err(|error| error.to_string())?;
        validate_questions(&input.questions).map_err(|error| format!("{error:#}"))
    }

    fn summary(&self, input: &Value) -> String {
        let count = input
            .get("questions")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        format!("{count} question(s)")
    }

    async fn execute(&self, context: &ToolContext, input: Value) -> Result<ToolOutput> {
        let parsed: Input = serde_json::from_value(input.clone())?;
        let interaction = match context.request_user_interaction(self.name(), input) {
            Ok(interaction) => interaction,
            Err(error) if error.downcast_ref::<ControlInterrupted>().is_some() => {
                return Ok(ToolOutput::interrupted());
            }
            Err(error) => return Err(error),
        };
        let answers = if let Some(updated) = interaction {
            parse_interaction_answers(updated)
                .context("交互响应不是有效 AskUserQuestion answers")?
        } else if context.agent_depth() != 0 || !context.permissions.interactive {
            bail!("AskUserQuestion 需要交互式终端或 stream-json control handler")
        } else {
            let _waiting = context.begin_user_interaction();
            prompt_interactively(&parsed.questions)?
        };
        validate_answers_complete(&parsed.questions, &answers)?;
        let rendered = parsed
            .questions
            .iter()
            .map(|question| {
                format!(
                    "\"{}\"=\"{}\"",
                    question.question,
                    answers
                        .get(&question.question)
                        .expect("answers were validated")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(ToolOutput::success(format!(
            "User has answered your questions: {rendered}. You can now continue with the user's answers in mind."
        )))
    }
}

fn parse_interaction_answers(updated: Value) -> Result<HashMap<String, String>> {
    let answers = updated
        .get("answers")
        .cloned()
        .unwrap_or_else(|| updated.clone());
    serde_json::from_value(answers).context("answers 必须是 question 到 answer 的字符串映射")
}

fn validate_questions(questions: &[Question]) -> Result<()> {
    let mut question_texts = HashSet::new();
    for question in questions {
        if question.header.chars().count() > 12 {
            bail!("question header 最多 12 个字符: {}", question.header)
        }
        if !question_texts.insert(question.question.as_str()) {
            bail!("question 文本必须唯一")
        }
        let mut labels = HashSet::new();
        for option in &question.options {
            if option.label.eq_ignore_ascii_case("other") || option.label == "其他" {
                bail!("不要显式提供 Other/其他选项；运行时会自动提供")
            }
            if !labels.insert(option.label.as_str()) {
                bail!("每个 question 内的 option label 必须唯一")
            }
        }
    }
    Ok(())
}

fn validate_answers(questions: &[Question], answers: &HashMap<String, String>) -> Result<()> {
    let known = questions
        .iter()
        .map(|question| question.question.as_str())
        .collect::<HashSet<_>>();
    if answers.iter().any(|(question, answer)| {
        !known.contains(question.as_str())
            || answer.trim().is_empty()
            || answer.len() > MAX_RESPONSE_BYTES
    }) {
        bail!("answers 只能包含已提问的问题且答案不能为空")
    }
    Ok(())
}

fn validate_answers_complete(
    questions: &[Question],
    answers: &HashMap<String, String>,
) -> Result<()> {
    validate_answers(questions, answers)?;
    if questions
        .iter()
        .any(|question| !answers.contains_key(&question.question))
    {
        bail!("用户交互响应没有回答所有问题")
    }
    Ok(())
}

fn prompt_interactively(questions: &[Question]) -> Result<HashMap<String, String>> {
    if !io::stdin().is_terminal() {
        bail!("AskUserQuestion 需要交互式终端或 stream-json control handler")
    }
    let mut answers = HashMap::new();
    for question in questions {
        eprintln!("\n[{}] {}", question.header, question.question);
        for (index, option) in question.options.iter().enumerate() {
            eprintln!("  {}. {} — {}", index + 1, option.label, option.description);
        }
        eprintln!("  {}. Other / 自定义", question.options.len() + 1);
        if question.multi_select {
            eprint!("选择一个或多个编号（逗号分隔），或输入自定义答案: ");
        } else {
            eprint!("选择编号，或输入自定义答案: ");
        }
        io::stderr().flush()?;
        let mut response = String::new();
        io::stdin()
            .read_line(&mut response)
            .context("读取用户回答失败")?;
        if response.len() > MAX_RESPONSE_BYTES {
            bail!("单个用户回答超过 {MAX_RESPONSE_BYTES} 字节限制")
        }
        let response = response.trim();
        if response.is_empty() {
            bail!("用户回答不能为空")
        }
        let answer = selected_labels(question, response).unwrap_or_else(|| response.to_owned());
        answers.insert(question.question.clone(), answer);
    }
    Ok(answers)
}

fn selected_labels(question: &Question, response: &str) -> Option<String> {
    let indexes = response
        .split(',')
        .map(str::trim)
        .map(str::parse::<usize>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()?;
    if indexes.is_empty() || (!question.multi_select && indexes.len() != 1) {
        return None;
    }
    let labels = indexes
        .into_iter()
        .map(|index| {
            question
                .options
                .get(index.checked_sub(1)?)
                .map(|option| option.label.clone())
        })
        .collect::<Option<Vec<_>>>()?;
    Some(labels.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_questions_options_and_explicit_other() {
        let duplicate = vec![
            Question {
                question: "Same?".to_owned(),
                header: "One".to_owned(),
                options: vec![option("A"), option("B")],
                multi_select: false,
            },
            Question {
                question: "Same?".to_owned(),
                header: "Two".to_owned(),
                options: vec![option("A"), option("B")],
                multi_select: false,
            },
        ];
        assert!(validate_questions(&duplicate).is_err());
        let explicit_other = vec![Question {
            question: "Pick?".to_owned(),
            header: "Pick".to_owned(),
            options: vec![option("A"), option("Other")],
            multi_select: false,
        }];
        assert!(validate_questions(&explicit_other).is_err());
    }

    #[test]
    fn parses_single_and_multi_selection() {
        let single = Question {
            question: "Pick?".to_owned(),
            header: "Pick".to_owned(),
            options: vec![option("A"), option("B")],
            multi_select: false,
        };
        assert_eq!(selected_labels(&single, "2").as_deref(), Some("B"));
        assert_eq!(selected_labels(&single, "1,2"), None);
        let multi = Question {
            multi_select: true,
            ..single
        };
        assert_eq!(selected_labels(&multi, "1, 2").as_deref(), Some("A, B"));
    }

    #[test]
    fn model_answers_are_rejected_and_control_updates_cannot_replace_questions() {
        let tool = AskUserQuestionTool;
        assert!(
            tool.validate_input(&json!({
                "questions":[{
                    "question":"Original?", "header":"Original",
                    "options":[
                        {"label":"A", "description":"description"},
                        {"label":"B", "description":"description"}
                    ]
                }],
                "answers":{"Original?":"A"}
            }))
            .is_err()
        );

        let questions = vec![Question {
            question: "Original?".to_owned(),
            header: "Original".to_owned(),
            options: vec![option("A"), option("B")],
            multi_select: false,
        }];
        let answers = parse_interaction_answers(json!({
            "questions":[{
                "question":"Injected?", "header":"Injected",
                "options":[{"label":"X","description":"x"},{"label":"Y","description":"y"}]
            }],
            "answers":{"Original?":"A"}
        }))
        .unwrap();
        assert_eq!(answers.get("Original?").map(String::as_str), Some("A"));
        validate_answers_complete(&questions, &answers).unwrap();
        let oversized =
            HashMap::from([("Original?".to_owned(), "x".repeat(MAX_RESPONSE_BYTES + 1))]);
        assert!(validate_answers(&questions, &oversized).is_err());
    }

    fn option(label: &str) -> QuestionOption {
        QuestionOption {
            label: label.to_owned(),
            description: "description".to_owned(),
            _preview: None,
        }
    }
}

//! Provider-neutral, terminal-safe Markdown parsing and layout.
//!
//! Model text never carries terminal control sequences through this module.
//! Rendering produces plain UTF-8 plus trusted style and link ranges; terminal
//! adapters may translate those ranges into ANSI, OSC 8, HTML, or plain text.

use std::{cmp::max, ops::Range};

use pulldown_cmark::{Alignment, CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;
use url::Url;

const DEFAULT_MAX_SOURCE_BYTES: usize = 8 * 1024 * 1024;
const TABLE_SAFETY_MARGIN: usize = 4;
const MIN_TABLE_COLUMN_WIDTH: usize = 3;
const MAX_HORIZONTAL_TABLE_ROW_LINES: usize = 4;
const MAX_LINK_TARGET_BYTES: usize = 2 * 1024;

/// A style that a trusted terminal adapter may translate to presentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TextStyle {
    Bold,
    Italic,
    Underline,
    Quote,
    InlineCode,
    Code,
    Heading(u8),
    Syntax(SyntaxClass),
}

/// Coarse, provider-neutral syntax classes. They deliberately carry no color.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyntaxClass {
    Keyword,
    String,
    Number,
    Comment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StyleSpan {
    pub range: Range<usize>,
    pub style: TextStyle,
}

/// A validated link destination. Only `http` and `https` targets are retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkSpan {
    pub range: Range<usize>,
    pub target: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RenderedLine {
    pub plain: String,
    pub styles: Vec<StyleSpan>,
    pub links: Vec<LinkSpan>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RenderedMarkdown {
    pub lines: Vec<RenderedLine>,
}

impl RenderedMarkdown {
    pub fn plain_text(&self) -> String {
        self.lines
            .iter()
            .map(|line| line.plain.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn append(&mut self, mut other: Self) {
        if other.lines.is_empty() {
            return;
        }
        if !self.lines.is_empty() {
            let self_has_separator = self.lines.last().is_some_and(|line| line.plain.is_empty());
            let other_has_separator = other
                .lines
                .first()
                .is_some_and(|line| line.plain.is_empty());
            match (self_has_separator, other_has_separator) {
                (true, true) => {
                    other.lines.remove(0);
                }
                (false, false) => self.lines.push(RenderedLine::default()),
                _ => {}
            }
        }
        self.lines.extend(other.lines);
    }

    fn trim_trailing_blank_lines(&mut self) {
        while self.lines.last().is_some_and(|line| line.plain.is_empty()) {
            self.lines.pop();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkdownRenderOptions {
    pub columns: usize,
    pub syntax_highlighting: bool,
}

impl Default for MarkdownRenderOptions {
    fn default() -> Self {
        Self {
            columns: 80,
            syntax_highlighting: true,
        }
    }
}

/// Replaces terminal controls without allowing model text to become terminal
/// syntax. Newlines and tabs remain available to the Markdown parser.
pub fn sanitize_markdown_source(input: &str) -> String {
    let normalized = input.replace("\r\n", "\n").replace('\r', "\n");
    normalized
        .chars()
        .map(|character| match character {
            '\n' | '\t' => character,
            _ if character.is_control() => '�',
            _ => character,
        })
        .collect()
}

/// Parses and lays out one complete Markdown document.
pub fn render_markdown(source: &str, options: MarkdownRenderOptions) -> RenderedMarkdown {
    let source = sanitize_markdown_source(source);
    render_sanitized_markdown(&source, normalized_options(options))
}

fn normalized_options(mut options: MarkdownRenderOptions) -> MarkdownRenderOptions {
    options.columns = options.columns.max(1);
    options
}

fn parser(source: &str) -> Parser<'_> {
    // Tables are useful in coding-agent output. Strikethrough is intentionally
    // not enabled: model text commonly uses `~100` to mean approximately 100.
    Parser::new_ext(source, Options::ENABLE_TABLES)
}

fn render_sanitized_markdown(source: &str, options: MarkdownRenderOptions) -> RenderedMarkdown {
    let mut builder = DocumentBuilder::new(options);
    for event in parser(source) {
        builder.event(event);
    }
    builder.finish()
}

#[derive(Default)]
struct LineBuilder {
    line: RenderedLine,
}

impl LineBuilder {
    fn append(&mut self, text: &str, styles: &[TextStyle], link: Option<&str>) {
        if text.is_empty() {
            return;
        }
        let start = self.line.plain.len();
        self.line.plain.push_str(text);
        let end = self.line.plain.len();
        for style in styles {
            push_style(&mut self.line.styles, start..end, *style);
        }
        if let Some(target) = link {
            push_link(&mut self.line.links, start..end, target);
        }
    }

    fn take(&mut self) -> RenderedLine {
        std::mem::take(&mut self.line)
    }

    fn is_empty(&self) -> bool {
        self.line.plain.is_empty()
    }
}

fn push_style(spans: &mut Vec<StyleSpan>, range: Range<usize>, style: TextStyle) {
    if range.is_empty() {
        return;
    }
    if let Some(last) = spans.last_mut() {
        if last.style == style && last.range.end == range.start {
            last.range.end = range.end;
            return;
        }
    }
    spans.push(StyleSpan { range, style });
}

fn push_link(spans: &mut Vec<LinkSpan>, range: Range<usize>, target: &str) {
    if range.is_empty() {
        return;
    }
    if let Some(last) = spans.last_mut() {
        if last.target == target && last.range.end == range.start {
            last.range.end = range.end;
            return;
        }
    }
    spans.push(LinkSpan {
        range,
        target: target.to_owned(),
    });
}

#[derive(Debug)]
struct ListState {
    next: Option<u64>,
}

struct DocumentBuilder {
    options: MarkdownRenderOptions,
    output: RenderedMarkdown,
    current: LineBuilder,
    styles: Vec<TextStyle>,
    lists: Vec<ListState>,
    item_depth: usize,
    quote_depth: usize,
    pending_prefix: Option<String>,
    continuation_indent: usize,
    code_language: Option<String>,
    table: Option<TableBuilder>,
    active_link: Option<String>,
    image_depth: usize,
}

impl DocumentBuilder {
    fn new(options: MarkdownRenderOptions) -> Self {
        Self {
            options,
            output: RenderedMarkdown::default(),
            current: LineBuilder::default(),
            styles: Vec::new(),
            lists: Vec::new(),
            item_depth: 0,
            quote_depth: 0,
            pending_prefix: None,
            continuation_indent: 0,
            code_language: None,
            table: None,
            active_link: None,
            image_depth: 0,
        }
    }

    fn event(&mut self, event: Event<'_>) {
        if self.table.is_some() {
            let ends_table = matches!(event, Event::End(TagEnd::Table));
            if !ends_table {
                self.table.as_mut().expect("checked above").event(event);
                return;
            }
            let table = self.table.take().expect("checked above");
            self.finish_line(false);
            self.output.append(table.render(self.options.columns));
            self.blank_line();
            return;
        }

        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(text) => {
                if self.image_depth == 0 {
                    if self.code_language.is_some() {
                        self.append_code(&text);
                    } else {
                        self.append_text(&text);
                    }
                }
            }
            Event::Code(text) => {
                if self.image_depth == 0 {
                    self.ensure_prefix();
                    let mut styles = self.styles.clone();
                    styles.push(TextStyle::InlineCode);
                    self.current
                        .append(&expand_tabs(&text), &styles, self.active_link.as_deref());
                }
            }
            Event::Html(_) | Event::InlineHtml(_) => {}
            Event::FootnoteReference(label) => self.append_text(&format!("[{label}]")),
            Event::SoftBreak => self.append_text(" "),
            Event::HardBreak => self.finish_line(true),
            Event::Rule => {
                self.finish_line(false);
                self.current.append("---", &[], None);
                self.finish_line(true);
            }
            Event::TaskListMarker(checked) => {
                self.append_text(if checked { "[x] " } else { "[ ] " });
            }
        }
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph => {}
            Tag::Heading { level, .. } => {
                self.finish_line(false);
                let depth = heading_depth(level);
                self.styles.push(TextStyle::Heading(depth));
                self.styles.push(TextStyle::Bold);
                if depth == 1 {
                    self.styles.push(TextStyle::Italic);
                    self.styles.push(TextStyle::Underline);
                }
            }
            Tag::BlockQuote => {
                self.finish_line(false);
                self.quote_depth = self.quote_depth.saturating_add(1);
            }
            Tag::CodeBlock(kind) => {
                self.finish_line(false);
                self.code_language = Some(match kind {
                    CodeBlockKind::Indented => String::new(),
                    CodeBlockKind::Fenced(language) => language
                        .split_whitespace()
                        .next()
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                });
            }
            Tag::HtmlBlock | Tag::MetadataBlock(_) | Tag::FootnoteDefinition(_) => {}
            Tag::List(start) => {
                if !self.current.is_empty() {
                    self.finish_line(false);
                }
                self.lists.push(ListState { next: start });
            }
            Tag::Item => {
                if !self.current.is_empty() {
                    self.finish_line(false);
                }
                self.item_depth = self.item_depth.saturating_add(1);
                let indent = self.lists.len().saturating_sub(1).saturating_mul(2);
                let marker = match self.lists.last_mut().and_then(|list| list.next.as_mut()) {
                    Some(next) => {
                        let marker = format!("{next}. ");
                        *next = next.saturating_add(1);
                        marker
                    }
                    None => "- ".to_owned(),
                };
                self.continuation_indent = indent.saturating_add(display_width(&marker));
                self.pending_prefix = Some(format!("{}{marker}", " ".repeat(indent)));
            }
            Tag::Table(alignments) => self.table = Some(TableBuilder::new(alignments)),
            Tag::TableHead | Tag::TableRow | Tag::TableCell => {}
            Tag::Emphasis => self.styles.push(TextStyle::Italic),
            Tag::Strong => self.styles.push(TextStyle::Bold),
            Tag::Strikethrough => {}
            Tag::Link { dest_url, .. } => {
                self.active_link = safe_link_target(&dest_url);
            }
            Tag::Image { dest_url, .. } => {
                self.image_depth = self.image_depth.saturating_add(1);
                if let Some(target) = safe_link_target(&dest_url) {
                    self.ensure_prefix();
                    self.current.append(&target, &self.styles, Some(&target));
                } else {
                    self.append_text(&dest_url);
                }
            }
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Paragraph => {
                self.finish_line(true);
                if self.item_depth == 0 && self.quote_depth == 0 {
                    self.blank_line();
                }
            }
            TagEnd::Heading(_) => {
                self.finish_line(true);
                self.blank_line();
                while matches!(
                    self.styles.last(),
                    Some(
                        TextStyle::Heading(_)
                            | TextStyle::Bold
                            | TextStyle::Italic
                            | TextStyle::Underline
                    )
                ) {
                    self.styles.pop();
                }
            }
            TagEnd::BlockQuote => {
                self.finish_line(false);
                self.quote_depth = self.quote_depth.saturating_sub(1);
                if self.quote_depth == 0 {
                    self.blank_line();
                }
            }
            TagEnd::CodeBlock => {
                self.finish_line(true);
                self.code_language = None;
                self.blank_line();
            }
            TagEnd::List(_) => {
                self.lists.pop();
                if self.lists.is_empty() {
                    self.blank_line();
                }
            }
            TagEnd::Item => {
                self.finish_line(false);
                self.item_depth = self.item_depth.saturating_sub(1);
                self.pending_prefix = None;
                self.continuation_indent = 0;
            }
            TagEnd::Emphasis => pop_last_style(&mut self.styles, TextStyle::Italic),
            TagEnd::Strong => pop_last_style(&mut self.styles, TextStyle::Bold),
            TagEnd::Link => self.active_link = None,
            TagEnd::Image => self.image_depth = self.image_depth.saturating_sub(1),
            TagEnd::HtmlBlock
            | TagEnd::FootnoteDefinition
            | TagEnd::Table
            | TagEnd::TableHead
            | TagEnd::TableRow
            | TagEnd::TableCell
            | TagEnd::Strikethrough
            | TagEnd::MetadataBlock(_) => {}
        }
    }

    fn ensure_prefix(&mut self) {
        if !self.current.is_empty() {
            return;
        }
        if self.quote_depth > 0 {
            let prefix = "│ ".repeat(self.quote_depth);
            self.current.append(&prefix, &[TextStyle::Quote], None);
        }
        if let Some(prefix) = self.pending_prefix.take() {
            self.current.append(&prefix, &[], None);
        } else if self.item_depth > 0 && self.continuation_indent > 0 {
            self.current
                .append(&" ".repeat(self.continuation_indent), &[], None);
        }
    }

    fn append_text(&mut self, text: &str) {
        let expanded = expand_tabs(text);
        let mut parts = expanded.split('\n').peekable();
        while let Some(part) = parts.next() {
            self.ensure_prefix();
            self.current
                .append(part, &self.styles, self.active_link.as_deref());
            if parts.peek().is_some() {
                self.finish_line(true);
            }
        }
    }

    fn append_code(&mut self, text: &str) {
        let expanded = expand_tabs(text);
        let language = self.code_language.clone().unwrap_or_default();
        let mut parts = expanded.split('\n').peekable();
        while let Some(part) = parts.next() {
            self.ensure_prefix();
            let start = self.current.line.plain.len();
            self.current.append(part, &[TextStyle::Code], None);
            if self.options.syntax_highlighting {
                for span in syntax_spans(part, &language, start) {
                    push_style(&mut self.current.line.styles, span.range, span.style);
                }
            }
            if parts.peek().is_some() {
                self.finish_line(true);
            }
        }
    }

    fn finish_line(&mut self, force: bool) {
        if force || !self.current.is_empty() {
            self.output.lines.push(self.current.take());
        }
    }

    fn blank_line(&mut self) {
        self.finish_line(false);
        if !self
            .output
            .lines
            .last()
            .is_some_and(|line| line.plain.is_empty())
        {
            self.output.lines.push(RenderedLine::default());
        }
    }

    fn finish(mut self) -> RenderedMarkdown {
        self.finish_line(false);
        self.output.trim_trailing_blank_lines();
        self.output
    }
}

fn pop_last_style(styles: &mut Vec<TextStyle>, target: TextStyle) {
    if let Some(index) = styles.iter().rposition(|style| *style == target) {
        styles.remove(index);
    }
}

fn heading_depth(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn expand_tabs(value: &str) -> String {
    value.replace('\t', "    ")
}

fn safe_link_target(value: &str) -> Option<String> {
    if value.len() > MAX_LINK_TARGET_BYTES || value.chars().any(char::is_control) {
        return None;
    }
    let parsed = Url::parse(value).ok()?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.host_str().is_none()
    {
        return None;
    }
    Some(parsed.to_string())
}

#[derive(Default)]
struct TableCellBuilder {
    line: LineBuilder,
    styles: Vec<TextStyle>,
    active_link: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct TableCell {
    line: RenderedLine,
}

impl TableCell {
    fn plain(&self) -> &str {
        &self.line.plain
    }
}

struct TableBuilder {
    alignments: Vec<Alignment>,
    header: Vec<TableCell>,
    rows: Vec<Vec<TableCell>>,
    current_row: Vec<TableCell>,
    current_cell: Option<TableCellBuilder>,
    in_header: bool,
}

impl TableBuilder {
    fn new(alignments: Vec<Alignment>) -> Self {
        Self {
            alignments,
            header: Vec::new(),
            rows: Vec::new(),
            current_row: Vec::new(),
            current_cell: None,
            in_header: false,
        }
    }

    fn event(&mut self, event: Event<'_>) {
        match event {
            Event::Start(Tag::TableHead) => {
                self.in_header = true;
                self.current_row.clear();
            }
            Event::Start(Tag::TableRow) => self.current_row.clear(),
            Event::Start(Tag::TableCell) => self.current_cell = Some(TableCellBuilder::default()),
            Event::Start(Tag::Strong) => self.cell_style(TextStyle::Bold, true),
            Event::Start(Tag::Emphasis) => self.cell_style(TextStyle::Italic, true),
            Event::Start(Tag::Link { dest_url, .. }) => {
                if let Some(cell) = self.current_cell.as_mut() {
                    cell.active_link = safe_link_target(&dest_url);
                }
            }
            Event::Start(Tag::Image { dest_url, .. }) => self.cell_text(&dest_url),
            Event::End(TagEnd::TableCell) => {
                if let Some(cell) = self.current_cell.take() {
                    self.current_row.push(TableCell {
                        line: cell.line.line,
                    });
                }
            }
            Event::End(TagEnd::TableHead) => {
                self.header = std::mem::take(&mut self.current_row);
                self.in_header = false;
            }
            Event::End(TagEnd::TableRow) if !self.in_header => {
                self.rows.push(std::mem::take(&mut self.current_row));
            }
            Event::End(TagEnd::Strong) => self.cell_style(TextStyle::Bold, false),
            Event::End(TagEnd::Emphasis) => self.cell_style(TextStyle::Italic, false),
            Event::End(TagEnd::Link) => {
                if let Some(cell) = self.current_cell.as_mut() {
                    cell.active_link = None;
                }
            }
            Event::Text(text) | Event::Code(text) => self.cell_text(&text),
            Event::SoftBreak | Event::HardBreak => self.cell_text(" "),
            Event::TaskListMarker(checked) => self.cell_text(if checked { "[x] " } else { "[ ] " }),
            Event::FootnoteReference(label) => self.cell_text(&format!("[{label}]")),
            Event::Html(_) | Event::InlineHtml(_) | Event::Rule => {}
            Event::Start(_) | Event::End(_) => {}
        }
    }

    fn cell_text(&mut self, text: &str) {
        if let Some(cell) = self.current_cell.as_mut() {
            cell.line.append(
                &collapse_cell_whitespace(text),
                &cell.styles,
                cell.active_link.as_deref(),
            );
        }
    }

    fn cell_style(&mut self, style: TextStyle, start: bool) {
        if let Some(cell) = self.current_cell.as_mut() {
            if start {
                cell.styles.push(style);
            } else {
                pop_last_style(&mut cell.styles, style);
            }
        }
    }

    fn render(mut self, columns: usize) -> RenderedMarkdown {
        let column_count = max(
            self.header.len(),
            self.rows.iter().map(Vec::len).max().unwrap_or(0),
        );
        if column_count == 0 {
            return RenderedMarkdown::default();
        }
        self.header.resize_with(column_count, TableCell::default);
        for row in &mut self.rows {
            row.resize_with(column_count, TableCell::default);
        }
        self.alignments.resize(column_count, Alignment::None);

        let min_widths = (0..column_count)
            .map(|column| {
                std::iter::once(&self.header[column])
                    .chain(self.rows.iter().map(|row| &row[column]))
                    .map(|cell| longest_word_width(cell.plain()).max(MIN_TABLE_COLUMN_WIDTH))
                    .max()
                    .unwrap_or(MIN_TABLE_COLUMN_WIDTH)
            })
            .collect::<Vec<_>>();
        let ideal_widths = (0..column_count)
            .map(|column| {
                std::iter::once(&self.header[column])
                    .chain(self.rows.iter().map(|row| &row[column]))
                    .map(|cell| display_width(cell.plain()).max(MIN_TABLE_COLUMN_WIDTH))
                    .max()
                    .unwrap_or(MIN_TABLE_COLUMN_WIDTH)
            })
            .collect::<Vec<_>>();
        let overhead = 1usize.saturating_add(column_count.saturating_mul(3));
        let available = columns.saturating_sub(overhead + TABLE_SAFETY_MARGIN);
        let total_min = min_widths.iter().sum::<usize>();
        if available < column_count.saturating_mul(MIN_TABLE_COLUMN_WIDTH) || total_min > available
        {
            return self.render_vertical(columns);
        }

        let total_ideal = ideal_widths.iter().sum::<usize>();
        let widths = if total_ideal <= available {
            ideal_widths
        } else {
            distribute_widths(&min_widths, &ideal_widths, available)
        };
        if self.max_wrapped_row_lines(&widths) > MAX_HORIZONTAL_TABLE_ROW_LINES {
            return self.render_vertical(columns);
        }
        let horizontal = self.render_horizontal(&widths);
        if horizontal
            .lines
            .iter()
            .any(|line| display_width(&line.plain) > columns.saturating_sub(TABLE_SAFETY_MARGIN))
        {
            self.render_vertical(columns)
        } else {
            horizontal
        }
    }

    fn max_wrapped_row_lines(&self, widths: &[usize]) -> usize {
        std::iter::once(&self.header)
            .chain(self.rows.iter())
            .map(|row| {
                row.iter()
                    .zip(widths)
                    .map(|(cell, width)| wrap_plain(cell.plain(), *width).len())
                    .max()
                    .unwrap_or(1)
            })
            .max()
            .unwrap_or(1)
    }

    fn render_horizontal(&self, widths: &[usize]) -> RenderedMarkdown {
        let mut output = RenderedMarkdown::default();
        output.lines.push(RenderedLine {
            plain: table_border(widths, '┌', '┬', '┐'),
            ..RenderedLine::default()
        });
        append_table_row(&mut output, &self.header, widths, &self.alignments, true);
        output.lines.push(RenderedLine {
            plain: table_border(widths, '├', '┼', '┤'),
            ..RenderedLine::default()
        });
        for (index, row) in self.rows.iter().enumerate() {
            append_table_row(&mut output, row, widths, &self.alignments, false);
            if index + 1 < self.rows.len() {
                output.lines.push(RenderedLine {
                    plain: table_border(widths, '├', '┼', '┤'),
                    ..RenderedLine::default()
                });
            }
        }
        output.lines.push(RenderedLine {
            plain: table_border(widths, '└', '┴', '┘'),
            ..RenderedLine::default()
        });
        output
    }

    fn render_vertical(&self, columns: usize) -> RenderedMarkdown {
        let width = columns.max(1);
        let mut output = RenderedMarkdown::default();
        for (row_index, row) in self.rows.iter().enumerate() {
            if row_index > 0 {
                output.lines.push(RenderedLine {
                    plain: "─".repeat(width.saturating_sub(1).min(40)),
                    ..RenderedLine::default()
                });
            }
            for (column, cell) in row.iter().enumerate() {
                let label = self
                    .header
                    .get(column)
                    .map(|cell| cell.plain().trim())
                    .filter(|label| !label.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("Column {}", column + 1));
                let prefix = format!("{label}: ");
                let first_width = width.saturating_sub(display_width(&prefix)).max(1);
                let wrapped = wrap_rendered_line(&cell.line, first_width);
                let mut first = RenderedLine::default();
                first.plain.push_str(&prefix);
                if let Some(fragment) = wrapped.first() {
                    let start = first.plain.len();
                    first.plain.push_str(&fragment.plain);
                    append_line_spans(&mut first, fragment, start);
                }
                push_style(&mut first.styles, 0..label.len(), TextStyle::Bold);
                output.lines.push(first);
                for continuation in wrapped.iter().skip(1) {
                    let mut line = RenderedLine {
                        plain: "  ".to_owned(),
                        ..RenderedLine::default()
                    };
                    let start = line.plain.len();
                    line.plain.push_str(&continuation.plain);
                    append_line_spans(&mut line, continuation, start);
                    output.lines.push(line);
                }
            }
        }
        output
    }
}

fn collapse_cell_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn longest_word_width(value: &str) -> usize {
    value
        .split_whitespace()
        .map(display_width)
        .max()
        .unwrap_or(0)
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn distribute_widths(minimum: &[usize], ideal: &[usize], available: usize) -> Vec<usize> {
    let total_min = minimum.iter().sum::<usize>();
    let extra = available.saturating_sub(total_min);
    let overflow = ideal
        .iter()
        .zip(minimum)
        .map(|(ideal, minimum)| ideal.saturating_sub(*minimum))
        .collect::<Vec<_>>();
    let total_overflow = overflow.iter().sum::<usize>();
    let mut widths = minimum.to_vec();
    if total_overflow == 0 {
        return widths;
    }
    let mut allocated = 0usize;
    for index in 0..widths.len() {
        let share = extra.saturating_mul(overflow[index]) / total_overflow;
        widths[index] = widths[index].saturating_add(share);
        allocated = allocated.saturating_add(share);
    }
    let mut remainder = extra.saturating_sub(allocated);
    let mut index = 0usize;
    while remainder > 0 && !widths.is_empty() {
        let slot = index % widths.len();
        widths[slot] = widths[slot].saturating_add(1);
        remainder -= 1;
        index += 1;
    }
    widths
}

fn wrap_plain(value: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    if value.is_empty() {
        return vec![String::new()];
    }
    let mut lines = Vec::new();
    let mut current = String::new();
    let mut current_width = 0usize;
    for word in value.split_whitespace() {
        let word_width = display_width(word);
        if current_width > 0 && current_width.saturating_add(1 + word_width) <= width {
            current.push(' ');
            current.push_str(word);
            current_width += 1 + word_width;
            continue;
        }
        if current_width > 0 {
            lines.push(std::mem::take(&mut current));
            current_width = 0;
        }
        if word_width <= width {
            current.push_str(word);
            current_width = word_width;
            continue;
        }
        for grapheme in word.graphemes(true) {
            let grapheme_width = display_width(grapheme).max(1);
            if current_width > 0 && current_width.saturating_add(grapheme_width) > width {
                lines.push(std::mem::take(&mut current));
                current_width = 0;
            }
            current.push_str(grapheme);
            current_width = current_width.saturating_add(grapheme_width);
        }
    }
    if !current.is_empty() || lines.is_empty() {
        lines.push(current);
    }
    lines
}

fn wrap_rendered_line(line: &RenderedLine, width: usize) -> Vec<RenderedLine> {
    let fragments = wrap_plain(&line.plain, width);
    let mut cursor = 0usize;
    fragments
        .into_iter()
        .map(|plain| {
            let relative = line.plain[cursor..].find(&plain).unwrap_or(0);
            let source_start = cursor.saturating_add(relative);
            let source_end = source_start.saturating_add(plain.len());
            cursor = source_end;
            let mut rendered = RenderedLine {
                plain,
                ..RenderedLine::default()
            };
            copy_intersecting_spans(line, source_start..source_end, &mut rendered, 0);
            rendered
        })
        .collect()
}

fn copy_intersecting_spans(
    source: &RenderedLine,
    source_range: Range<usize>,
    target: &mut RenderedLine,
    target_start: usize,
) {
    for span in &source.styles {
        let start = span.range.start.max(source_range.start);
        let end = span.range.end.min(source_range.end);
        if start < end {
            push_style(
                &mut target.styles,
                target_start + start - source_range.start..target_start + end - source_range.start,
                span.style,
            );
        }
    }
    for span in &source.links {
        let start = span.range.start.max(source_range.start);
        let end = span.range.end.min(source_range.end);
        if start < end {
            push_link(
                &mut target.links,
                target_start + start - source_range.start..target_start + end - source_range.start,
                &span.target,
            );
        }
    }
}

fn append_line_spans(target: &mut RenderedLine, source: &RenderedLine, target_start: usize) {
    copy_intersecting_spans(source, 0..source.plain.len(), target, target_start);
}

fn table_border(widths: &[usize], left: char, middle: char, right: char) -> String {
    let mut line = String::new();
    line.push(left);
    for (index, width) in widths.iter().enumerate() {
        line.push_str(&"─".repeat(width.saturating_add(2)));
        line.push(if index + 1 == widths.len() {
            right
        } else {
            middle
        });
    }
    line
}

fn append_table_row(
    output: &mut RenderedMarkdown,
    row: &[TableCell],
    widths: &[usize],
    alignments: &[Alignment],
    header: bool,
) {
    let wrapped = row
        .iter()
        .zip(widths)
        .map(|(cell, width)| wrap_rendered_line(&cell.line, *width))
        .collect::<Vec<_>>();
    let height = wrapped.iter().map(Vec::len).max().unwrap_or(1);
    for visual_row in 0..height {
        let mut line = RenderedLine::default();
        line.plain.push('│');
        for column in 0..widths.len() {
            let fragment = wrapped[column].get(visual_row).cloned().unwrap_or_default();
            let alignment = if header {
                Alignment::Center
            } else {
                alignments.get(column).copied().unwrap_or(Alignment::Left)
            };
            line.plain.push(' ');
            let padded = pad_to_width(&fragment.plain, widths[column], alignment);
            let text_offset = padded.find(&fragment.plain).unwrap_or(0);
            let start = line.plain.len().saturating_add(text_offset);
            line.plain.push_str(&padded);
            append_line_spans(&mut line, &fragment, start);
            if header {
                push_style(
                    &mut line.styles,
                    start..start.saturating_add(fragment.plain.len()),
                    TextStyle::Bold,
                );
            }
            line.plain.push_str(" │");
        }
        output.lines.push(line);
    }
}

fn pad_to_width(value: &str, width: usize, alignment: Alignment) -> String {
    let remaining = width.saturating_sub(display_width(value));
    let (left, right) = match alignment {
        Alignment::Right => (remaining, 0),
        Alignment::Center => (remaining / 2, remaining.saturating_sub(remaining / 2)),
        Alignment::None | Alignment::Left => (0, remaining),
    };
    format!("{}{}{}", " ".repeat(left), value, " ".repeat(right))
}

fn syntax_spans(line: &str, language: &str, base: usize) -> Vec<StyleSpan> {
    let family = language_family(language);
    if family == LanguageFamily::Plain {
        return Vec::new();
    }
    let bytes = line.as_bytes();
    let mut output = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        if comment_marker(family).is_some_and(|marker| line[index..].starts_with(marker)) {
            output.push(StyleSpan {
                range: base + index..base + line.len(),
                style: TextStyle::Syntax(SyntaxClass::Comment),
            });
            break;
        }
        let character = line[index..].chars().next().expect("valid UTF-8 boundary");
        if matches!(character, '\'' | '"' | '`') {
            let start = index;
            index += character.len_utf8();
            let mut escaped = false;
            while index < bytes.len() {
                let current = line[index..].chars().next().expect("valid UTF-8 boundary");
                index += current.len_utf8();
                if escaped {
                    escaped = false;
                } else if current == '\\' {
                    escaped = true;
                } else if current == character {
                    break;
                }
            }
            output.push(StyleSpan {
                range: base + start..base + index,
                style: TextStyle::Syntax(SyntaxClass::String),
            });
            continue;
        }
        if character.is_ascii_digit() {
            let start = index;
            index += character.len_utf8();
            while index < bytes.len() {
                let current = line[index..].chars().next().expect("valid UTF-8 boundary");
                if !(current.is_ascii_alphanumeric() || matches!(current, '.' | '_')) {
                    break;
                }
                index += current.len_utf8();
            }
            output.push(StyleSpan {
                range: base + start..base + index,
                style: TextStyle::Syntax(SyntaxClass::Number),
            });
            continue;
        }
        if character.is_alphabetic() || character == '_' {
            let start = index;
            index += character.len_utf8();
            while index < bytes.len() {
                let current = line[index..].chars().next().expect("valid UTF-8 boundary");
                if !(current.is_alphanumeric() || current == '_') {
                    break;
                }
                index += current.len_utf8();
            }
            if is_keyword(family, &line[start..index]) {
                output.push(StyleSpan {
                    range: base + start..base + index,
                    style: TextStyle::Syntax(SyntaxClass::Keyword),
                });
            }
            continue;
        }
        index += character.len_utf8();
    }
    output
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LanguageFamily {
    Plain,
    Rust,
    Shell,
    Python,
    JavaScript,
    Json,
}

fn language_family(language: &str) -> LanguageFamily {
    match language {
        "rust" | "rs" => LanguageFamily::Rust,
        "bash" | "sh" | "shell" | "zsh" => LanguageFamily::Shell,
        "python" | "py" => LanguageFamily::Python,
        "javascript" | "js" | "jsx" | "typescript" | "ts" | "tsx" => LanguageFamily::JavaScript,
        "json" | "jsonc" => LanguageFamily::Json,
        _ => LanguageFamily::Plain,
    }
}

fn comment_marker(family: LanguageFamily) -> Option<&'static str> {
    match family {
        LanguageFamily::Rust | LanguageFamily::JavaScript => Some("//"),
        LanguageFamily::Shell | LanguageFamily::Python => Some("#"),
        LanguageFamily::Plain | LanguageFamily::Json => None,
    }
}

fn is_keyword(family: LanguageFamily, word: &str) -> bool {
    match family {
        LanguageFamily::Rust => matches!(
            word,
            "as" | "async"
                | "await"
                | "break"
                | "const"
                | "continue"
                | "crate"
                | "else"
                | "enum"
                | "extern"
                | "false"
                | "fn"
                | "for"
                | "if"
                | "impl"
                | "in"
                | "let"
                | "loop"
                | "match"
                | "mod"
                | "move"
                | "mut"
                | "pub"
                | "ref"
                | "return"
                | "self"
                | "Self"
                | "static"
                | "struct"
                | "super"
                | "trait"
                | "true"
                | "type"
                | "unsafe"
                | "use"
                | "where"
                | "while"
        ),
        LanguageFamily::Shell => matches!(
            word,
            "case"
                | "do"
                | "done"
                | "elif"
                | "else"
                | "esac"
                | "fi"
                | "for"
                | "function"
                | "if"
                | "in"
                | "select"
                | "then"
                | "time"
                | "until"
                | "while"
        ),
        LanguageFamily::Python => matches!(
            word,
            "and"
                | "as"
                | "assert"
                | "async"
                | "await"
                | "break"
                | "class"
                | "continue"
                | "def"
                | "del"
                | "elif"
                | "else"
                | "except"
                | "False"
                | "finally"
                | "for"
                | "from"
                | "global"
                | "if"
                | "import"
                | "in"
                | "is"
                | "lambda"
                | "None"
                | "nonlocal"
                | "not"
                | "or"
                | "pass"
                | "raise"
                | "return"
                | "True"
                | "try"
                | "while"
                | "with"
                | "yield"
        ),
        LanguageFamily::JavaScript => matches!(
            word,
            "async"
                | "await"
                | "break"
                | "case"
                | "catch"
                | "class"
                | "const"
                | "continue"
                | "debugger"
                | "default"
                | "delete"
                | "do"
                | "else"
                | "export"
                | "extends"
                | "false"
                | "finally"
                | "for"
                | "function"
                | "if"
                | "import"
                | "in"
                | "instanceof"
                | "let"
                | "new"
                | "null"
                | "return"
                | "static"
                | "super"
                | "switch"
                | "this"
                | "throw"
                | "true"
                | "try"
                | "typeof"
                | "var"
                | "void"
                | "while"
                | "with"
                | "yield"
        ),
        LanguageFamily::Json => matches!(word, "true" | "false" | "null"),
        LanguageFamily::Plain => false,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingMarkdownFrame {
    pub stable: RenderedMarkdown,
    pub unstable: RenderedMarkdown,
    pub stable_prefix_end: usize,
    pub truncated: bool,
}

impl StreamingMarkdownFrame {
    pub fn combined(&self) -> RenderedMarkdown {
        let mut combined = self.stable.clone();
        combined.append(self.unstable.clone());
        combined.trim_trailing_blank_lines();
        combined
    }
}

/// Incremental Markdown state. Only the last top-level content block remains
/// unstable; prior blocks are parsed and rendered once, then retained.
#[derive(Debug, Clone)]
pub struct StreamingMarkdown {
    raw_source: String,
    sanitized_source: String,
    stable_prefix_end: usize,
    stable: RenderedMarkdown,
    unstable: RenderedMarkdown,
    options: MarkdownRenderOptions,
    max_source_bytes: usize,
    truncated: bool,
}

impl StreamingMarkdown {
    pub fn new(options: MarkdownRenderOptions) -> Self {
        Self::with_max_source_bytes(options, DEFAULT_MAX_SOURCE_BYTES)
    }

    pub fn with_max_source_bytes(options: MarkdownRenderOptions, max_source_bytes: usize) -> Self {
        Self {
            raw_source: String::new(),
            sanitized_source: String::new(),
            stable_prefix_end: 0,
            stable: RenderedMarkdown::default(),
            unstable: RenderedMarkdown::default(),
            options: normalized_options(options),
            max_source_bytes: max_source_bytes.max(1),
            truncated: false,
        }
    }

    pub fn append(&mut self, delta: &str) -> StreamingMarkdownFrame {
        let available = self.max_source_bytes.saturating_sub(self.raw_source.len());
        let mut end = available.min(delta.len());
        while end > 0 && !delta.is_char_boundary(end) {
            end -= 1;
        }
        self.raw_source.push_str(&delta[..end]);
        self.truncated |= end < delta.len();
        self.refresh();
        self.frame()
    }

    pub fn replace(&mut self, source: &str) -> StreamingMarkdownFrame {
        self.raw_source.clear();
        self.sanitized_source.clear();
        self.stable_prefix_end = 0;
        self.stable = RenderedMarkdown::default();
        self.unstable = RenderedMarkdown::default();
        self.truncated = false;
        self.append(source)
    }

    pub fn frame(&self) -> StreamingMarkdownFrame {
        StreamingMarkdownFrame {
            stable: self.stable.clone(),
            unstable: self.unstable.clone(),
            stable_prefix_end: self.stable_prefix_end,
            truncated: self.truncated,
        }
    }

    pub fn source(&self) -> &str {
        &self.sanitized_source
    }

    pub fn finish(&self) -> RenderedMarkdown {
        render_sanitized_markdown(&self.sanitized_source, self.options)
    }

    fn refresh(&mut self) {
        let mut sanitized = sanitize_markdown_source(&self.raw_source);
        truncate_utf8(&mut sanitized, self.max_source_bytes);
        if !sanitized.starts_with(
            self.sanitized_source
                .get(..self.stable_prefix_end)
                .unwrap_or_default(),
        ) {
            self.stable_prefix_end = 0;
            self.stable = RenderedMarkdown::default();
        }
        self.sanitized_source = sanitized;

        let suffix = &self.sanitized_source[self.stable_prefix_end..];
        let blocks = top_level_block_ranges(suffix);
        if blocks.len() > 1 {
            let advance = blocks.last().expect("len checked").start;
            if advance > 0 {
                let old_end = self.stable_prefix_end;
                let new_end = old_end.saturating_add(advance);
                let newly_stable = render_sanitized_markdown(
                    &self.sanitized_source[old_end..new_end],
                    self.options,
                );
                self.stable.append(newly_stable);
                self.stable_prefix_end = new_end;
            }
        }
        self.unstable = render_sanitized_markdown(
            &self.sanitized_source[self.stable_prefix_end..],
            self.options,
        );
    }
}

fn truncate_utf8(value: &mut String, limit: usize) {
    if value.len() <= limit {
        return;
    }
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

fn top_level_block_ranges(source: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut depth = 0usize;
    let mut start = None;
    for (event, range) in parser(source).into_offset_iter() {
        match event {
            Event::Start(_) => {
                if depth == 0 {
                    start = Some(range.start);
                }
                depth = depth.saturating_add(1);
            }
            Event::End(_) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    ranges.push(start.take().unwrap_or(range.start)..range.end);
                }
            }
            _ if depth == 0 => ranges.push(range),
            _ => {}
        }
    }
    if let Some(start) = start {
        ranges.push(start..source.len());
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(columns: usize) -> MarkdownRenderOptions {
        MarkdownRenderOptions {
            columns,
            syntax_highlighting: true,
        }
    }

    #[test]
    fn renders_core_markdown_to_plain_text_and_trusted_styles() {
        let rendered = render_markdown(
            "# Title\n\n> quoted **strong**\n\n1. first\n2. *second*\n\n`inline` and ~100",
            options(80),
        );
        let plain = rendered.plain_text();
        assert!(plain.contains("Title"));
        assert!(plain.contains("│ quoted strong"));
        assert!(plain.contains("1. first\n2. second"));
        assert!(plain.contains("inline and ~100"));
        assert!(!plain.contains("# Title"));
        assert!(!plain.contains("**"));
        assert!(rendered.lines.iter().any(|line| {
            line.styles
                .iter()
                .any(|span| span.style == TextStyle::Heading(1))
        }));
        assert!(rendered.lines.iter().any(|line| {
            line.styles
                .iter()
                .any(|span| span.style == TextStyle::InlineCode)
        }));
    }

    #[test]
    fn strips_raw_html_and_neutralizes_terminal_controls() {
        let rendered = render_markdown(
            "safe\u{1b}[2J<script>alert(1)</script>tail\u{7}",
            options(80),
        );
        let plain = rendered.plain_text();
        assert_eq!(plain, "safe�[2Jalert(1)tail�");
        assert!(!plain.contains('\u{1b}'));
        assert!(!plain.contains("<script>"));
    }

    #[test]
    fn retains_only_safe_http_link_targets() {
        let rendered = render_markdown(
            "[safe](http://127.0.0.1:9/a?q=1) [bad](javascript:alert(1)) [auth](http://u:p@127.0.0.1:9/)",
            options(100),
        );
        let links = rendered
            .lines
            .iter()
            .flat_map(|line| &line.links)
            .collect::<Vec<_>>();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "http://127.0.0.1:9/a?q=1");
        assert_eq!(&rendered.lines[0].plain[links[0].range.clone()], "safe");
    }

    #[test]
    fn fenced_code_preserves_lines_and_highlighting_can_be_disabled() {
        let source = "```rust\nfn main() {\n    let n = 42; // value\n}\n```";
        let highlighted = render_markdown(source, options(80));
        assert_eq!(
            highlighted.plain_text(),
            "fn main() {\n    let n = 42; // value\n}"
        );
        assert!(highlighted.lines.iter().any(|line| {
            line.styles
                .iter()
                .any(|span| matches!(span.style, TextStyle::Syntax(SyntaxClass::Keyword)))
        }));

        let plain = render_markdown(
            source,
            MarkdownRenderOptions {
                columns: 80,
                syntax_highlighting: false,
            },
        );
        assert!(plain.lines.iter().all(|line| {
            !line
                .styles
                .iter()
                .any(|span| matches!(span.style, TextStyle::Syntax(_)))
        }));
        assert!(
            plain
                .lines
                .iter()
                .all(|line| line.styles.iter().any(|span| span.style == TextStyle::Code))
        );
    }

    #[test]
    fn unknown_code_language_falls_back_without_syntax_spans() {
        let rendered = render_markdown("```madeup\nlet x = 1\n```", options(80));
        assert!(rendered.lines.iter().all(|line| {
            !line
                .styles
                .iter()
                .any(|span| matches!(span.style, TextStyle::Syntax(_)))
        }));
        assert_eq!(rendered.plain_text(), "let x = 1");
    }

    #[test]
    fn table_is_width_safe_and_uses_vertical_fallback_when_narrow() {
        let source = "| Name | Meaning |\n|:--|--:|\n| α | a short value |\n| 中文 | a-very-long-unbroken-value |";
        let wide = render_markdown(source, options(80));
        assert!(wide.plain_text().contains('┌'));
        assert!(
            wide.lines
                .iter()
                .all(|line| display_width(&line.plain) <= 76)
        );

        let narrow = render_markdown(source, options(20));
        assert!(!narrow.plain_text().contains('┌'));
        assert!(narrow.plain_text().contains("Name: α"));
        assert!(
            narrow
                .lines
                .iter()
                .all(|line| display_width(&line.plain) <= 20)
        );
    }

    #[test]
    fn table_handles_empty_cells_unicode_and_alignment_without_overflow() {
        let source = "| 左 | emoji | empty |\n|:--|:--:|--:|\n| 中文 | 👨‍👩‍👧‍👦 | |";
        for width in [20, 39, 40, 79, 80, 120] {
            let rendered = render_markdown(source, options(width));
            assert!(
                rendered
                    .lines
                    .iter()
                    .all(|line| display_width(&line.plain) <= width)
            );
        }
    }

    #[test]
    fn table_links_remain_structured_after_horizontal_or_vertical_wrapping() {
        let source = "| kind | target |\n|---|---|\n| docs | [documentation](http://127.0.0.1:9/docs) and [bad](file:///tmp/no) |";
        for width in [16, 80] {
            let rendered = render_markdown(source, options(width));
            let links = rendered
                .lines
                .iter()
                .flat_map(|line| {
                    line.links
                        .iter()
                        .map(|link| (&line.plain[link.range.clone()], link.target.as_str()))
                })
                .collect::<Vec<_>>();
            assert!(!links.is_empty());
            assert!(
                links
                    .iter()
                    .all(|(_, target)| *target == "http://127.0.0.1:9/docs")
            );
            assert_eq!(
                links.iter().map(|(text, _)| *text).collect::<String>(),
                "documentation"
            );
            assert!(rendered.plain_text().contains("bad"));
        }
    }

    #[test]
    fn streaming_keeps_only_last_top_level_block_unstable() {
        let mut stream = StreamingMarkdown::new(options(80));
        let first = stream.append("First paragraph");
        assert_eq!(first.stable_prefix_end, 0);
        assert_eq!(first.unstable.plain_text(), "First paragraph");

        let second = stream.append("\n\nSecond **partial");
        assert!(second.stable_prefix_end > 0);
        assert_eq!(second.stable.plain_text(), "First paragraph");
        // An unmatched emphasis delimiter remains visible until its closing
        // delimiter arrives in a later delta.
        assert!(second.unstable.plain_text().contains("Second **partial"));
        let stable_end = second.stable_prefix_end;

        let third = stream.append("** tail");
        assert_eq!(third.stable_prefix_end, stable_end);
        assert_eq!(third.combined(), stream.finish());
    }

    #[test]
    fn unclosed_fence_remains_unstable_across_deltas() {
        let mut stream = StreamingMarkdown::new(options(80));
        stream.append("Before\n\n```ru");
        let open = stream.append("st\nfn main()");
        assert_eq!(open.stable.plain_text(), "Before");
        let boundary = open.stable_prefix_end;
        let closed = stream.append(" {}\n```\n\nAfter");
        assert!(closed.stable_prefix_end > boundary);
        assert_eq!(closed.combined(), stream.finish());
    }

    #[test]
    fn streaming_final_output_matches_one_shot_for_every_utf8_split() {
        let source = "# 标题\n\n- α\n- 👨‍👩‍👧‍👦\n\n```rust\nfn main() {}\n```\n\n| k | v |\n|---|---|\n| 中 | 文 |";
        let expected = render_markdown(source, options(32));
        let mut boundaries = source
            .char_indices()
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        boundaries.push(source.len());
        for window in boundaries.windows(2) {
            let split = window[1];
            let mut stream = StreamingMarkdown::new(options(32));
            stream.append(&source[..split]);
            stream.append(&source[split..]);
            assert_eq!(stream.finish(), expected, "split at byte {split}");
        }
    }

    #[test]
    fn streaming_source_is_bounded_at_utf8_boundary() {
        let mut stream = StreamingMarkdown::with_max_source_bytes(options(20), 7);
        let frame = stream.append("你好世界");
        assert!(frame.truncated);
        assert!(stream.source().is_char_boundary(stream.source().len()));
        assert!(stream.source().len() <= 7);
    }

    #[test]
    fn replacing_non_prefix_text_resets_stable_boundary() {
        let mut stream = StreamingMarkdown::new(options(80));
        let old = stream.append("one\n\ntwo");
        assert!(old.stable_prefix_end > 0);
        let new = stream.replace("replacement");
        assert_eq!(new.stable_prefix_end, 0);
        assert_eq!(new.combined().plain_text(), "replacement");
    }
}

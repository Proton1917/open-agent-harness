use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, BufReader};

use super::{MAX_EDITABLE_FILE_BYTES, Tool, ToolContext, ToolOutput, object_schema, parse_input};

const MAX_SIZE: u64 = MAX_EDITABLE_FILE_BYTES as u64;
const MAX_APPROX_TOKENS: usize = 25_000;
const MAX_LINE_BYTES: usize = 1024 * 1024;
const MAX_PARTIAL_SCAN_BYTES: usize = 16 * 1024 * 1024;
const MAX_IMAGE_RAW_BYTES: usize = 3 * 1024 * 1024;
const MAX_PDF_RAW_BYTES: usize = 8 * 1024 * 1024;
const MAX_PDF_PAGES_PER_READ: usize = 20;
const MAX_NOTEBOOK_CELLS: usize = 256;
const MAX_NOTEBOOK_OUTPUTS: usize = 1_024;
const MAX_NOTEBOOK_MEDIA: usize = 50;
const MAX_NOTEBOOK_MEDIA_RAW_BYTES: usize = 4 * 1024 * 1024;
const MAX_NOTEBOOK_TEXT_BYTES: usize = 100 * 1024;
const BLOCKED_DEVICES: &[&str] = &[
    "/dev/zero",
    "/dev/random",
    "/dev/urandom",
    "/dev/full",
    "/dev/stdin",
    "/dev/tty",
    "/dev/console",
    "/dev/stdout",
    "/dev/stderr",
    "/dev/fd/0",
    "/dev/fd/1",
    "/dev/fd/2",
];

#[derive(Deserialize)]
struct Input {
    file_path: String,
    #[serde(default = "default_offset")]
    offset: usize,
    limit: Option<usize>,
    pages: Option<String>,
}

fn default_offset() -> usize {
    1
}

pub struct ReadTool;

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "Read"
    }
    fn description(&self) -> &str {
        "Reads text with 1-based line numbers, images as model-visible media, PDFs (optionally with pages), and Jupyter notebooks as structured cells and outputs."
    }
    fn input_schema(&self) -> Value {
        object_schema(
            json!({
                "file_path": {"type": "string", "maxLength": 4096, "description": "Absolute or working-directory-relative file path"},
                "offset": {"type": "integer", "minimum": 1, "maximum": 10000000},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000000},
                "pages": {"type": "string", "maxLength": 64, "pattern": "^[0-9]+(?:-[0-9]*)?$", "description": "1-based PDF page or inclusive range, for example 3, 1-5, or 10-"}
            }),
            &["file_path"],
        )
    }
    fn read_only(&self, _: &Value) -> bool {
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
        if input.offset == 0 {
            bail!("offset 必须从 1 开始")
        }
        let path = context.resolve_path(&input.file_path)?;
        block_device(&path)?;
        if let Ok(canonical) = std::fs::canonicalize(&path) {
            block_device(&canonical)?;
        }
        let metadata = std::fs::metadata(&path)
            .with_context(|| format!("文件不存在或不可访问: {}", path.display()))?;
        if !metadata.is_file() {
            bail!("路径不是普通文件: {}", path.display())
        }
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if matches!(extension.as_str(), "png" | "jpg" | "jpeg" | "gif" | "webp") {
            reject_text_ranges(&input, "图片")?;
            return read_image(context, &path, metadata.len()).await;
        }
        if extension == "pdf" {
            reject_line_ranges(&input, "PDF")?;
            return read_pdf(context, &path, metadata.len(), input.pages.as_deref()).await;
        }
        if extension == "ipynb" {
            reject_text_ranges(&input, "Jupyter notebook")?;
            return read_notebook(context, &path, metadata.len()).await;
        }
        if input.pages.is_some() {
            bail!("pages 只适用于 PDF 文件")
        }
        let partial = input.limit.is_some() || input.offset != 1;
        if !partial && metadata.len() > MAX_SIZE {
            bail!(
                "文件大小 {} 字节，超过 {} 字节限制；请使用 offset/limit 分段读取",
                metadata.len(),
                MAX_SIZE
            )
        }
        if partial {
            let (rendered, observed, lines_seen) =
                read_partial(&path, input.offset, input.limit).await?;
            let result = if rendered.is_empty() {
                format!(
                    "Warning: file has only {lines_seen} lines; offset was {}",
                    input.offset
                )
            } else {
                rendered
            };
            context.remember_read(path, observed, true).await?;
            return Ok(ToolOutput::success(result));
        }
        let mut bytes = Vec::new();
        tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("无法打开 {}", path.display()))?
            .take(MAX_SIZE + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > MAX_SIZE as usize {
            bail!(
                "文件读取时增长到超过 {} 字节限制；请使用 offset/limit 分段读取",
                MAX_SIZE
            )
        }
        if bytes.contains(&0) {
            bail!("文件看起来是二进制文件，当前 Read 文本路径不支持")
        }
        let content = String::from_utf8(bytes).context("文件不是有效 UTF-8 文本")?;
        let lines: Vec<&str> = content.split('\n').collect();
        let start = input.offset.saturating_sub(1);
        let end = input
            .limit
            .map(|n| start.saturating_add(n))
            .unwrap_or(lines.len())
            .min(lines.len());
        let selected = if start >= lines.len() {
            &[][..]
        } else {
            &lines[start..end]
        };
        let rendered = selected
            .iter()
            .enumerate()
            .map(|(index, line)| {
                format!(
                    "{:>6}→{}",
                    input.offset + index,
                    line.strip_suffix('\r').unwrap_or(line)
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        if rendered.len() / 4 > MAX_APPROX_TOKENS {
            bail!("读取结果估算超过 {MAX_APPROX_TOKENS} tokens；请缩小 offset/limit 范围")
        }
        let result = if rendered.is_empty() {
            if lines.len() == 1 && lines[0].is_empty() {
                "Warning: file exists but is empty".to_owned()
            } else {
                format!(
                    "Warning: file has only {} lines; offset was {}",
                    lines.len(),
                    input.offset
                )
            }
        } else {
            rendered
        };
        context.remember_read(path, content, partial).await?;
        Ok(ToolOutput::success(result))
    }
}

fn reject_text_ranges(input: &Input, kind: &str) -> Result<()> {
    reject_line_ranges(input, kind)?;
    if input.pages.is_some() {
        bail!("pages 只适用于 PDF 文件")
    }
    Ok(())
}

fn reject_line_ranges(input: &Input, kind: &str) -> Result<()> {
    if input.offset != 1 || input.limit.is_some() {
        bail!("{kind} 不支持 offset/limit；请移除文本行范围参数")
    }
    Ok(())
}

async fn read_binary_bounded(
    path: &Path,
    metadata_len: u64,
    limit: usize,
    kind: &str,
) -> Result<Vec<u8>> {
    if metadata_len == 0 {
        bail!("{kind} 文件为空")
    }
    if metadata_len > limit as u64 {
        bail!("{kind} 文件大小 {metadata_len} 字节，超过 {limit} 字节限制")
    }
    let mut bytes = Vec::with_capacity(metadata_len as usize);
    tokio::fs::File::open(path)
        .await
        .with_context(|| format!("无法打开 {}", path.display()))?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > limit {
        bail!("{kind} 文件读取时增长到超过 {limit} 字节限制")
    }
    if bytes.is_empty() {
        bail!("{kind} 文件为空")
    }
    Ok(bytes)
}

async fn read_image(context: &ToolContext, path: &Path, metadata_len: u64) -> Result<ToolOutput> {
    let bytes = read_binary_bounded(path, metadata_len, MAX_IMAGE_RAW_BYTES, "图片").await?;
    let media_type = detect_image_media_type(&bytes)
        .context("图片内容不是受支持的 PNG、JPEG、GIF 或 WebP；不会仅凭扩展名发送二进制内容")?;
    let display = context.display_path(path);
    let preview = format!("Read image {display} ({} bytes, {media_type})", bytes.len());
    let content = json!([
        {"type":"text", "text":preview},
        {"type":"image", "source":{
            "type":"base64", "media_type":media_type, "data":BASE64.encode(bytes)
        }}
    ]);
    Ok(ToolOutput::success_with_model_content(preview, content))
}

fn detect_image_media_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

struct PdfPayload {
    bytes: Vec<u8>,
    total_pages: usize,
    selected: Option<(usize, usize)>,
}

async fn read_pdf(
    context: &ToolContext,
    path: &Path,
    metadata_len: u64,
    pages: Option<&str>,
) -> Result<ToolOutput> {
    let bytes = read_binary_bounded(path, metadata_len, MAX_PDF_RAW_BYTES, "PDF").await?;
    if !bytes.starts_with(b"%PDF-") {
        bail!("文件缺少 %PDF- 标头，不是有效 PDF")
    }
    let requested = pages.map(str::to_owned);
    let payload = tokio::task::spawn_blocking(move || prepare_pdf(bytes, requested.as_deref()))
        .await
        .context("PDF 解析任务意外终止")??;
    let display = context.display_path(path);
    let selection = payload.selected.map_or_else(
        || format!("all {} pages", payload.total_pages),
        |(first, last)| format!("pages {first}-{last} of {}", payload.total_pages),
    );
    let preview = format!(
        "Read PDF {display} ({selection}, {} bytes)",
        payload.bytes.len()
    );
    let title = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("document.pdf");
    let content = json!([
        {"type":"text", "text":preview},
        {"type":"document", "title":title, "source":{
            "type":"base64", "media_type":"application/pdf", "data":BASE64.encode(payload.bytes)
        }}
    ]);
    Ok(ToolOutput::success_with_model_content(preview, content))
}

fn prepare_pdf(bytes: Vec<u8>, pages: Option<&str>) -> Result<PdfPayload> {
    let mut document =
        lopdf::Document::load_mem(&bytes).context("PDF 无法解析、已损坏或受密码保护")?;
    if document.is_encrypted() {
        bail!("PDF 受密码保护，无法读取")
    }
    let total_pages = document.get_pages().len();
    if total_pages == 0 {
        bail!("PDF 不包含页面")
    }
    let Some(pages) = pages else {
        if total_pages > MAX_PDF_PAGES_PER_READ {
            bail!(
                "PDF 有 {total_pages} 页，整份读取超过每次 {MAX_PDF_PAGES_PER_READ} 页限制；请使用 pages 分段读取"
            )
        }
        return Ok(PdfPayload {
            bytes,
            total_pages,
            selected: None,
        });
    };
    let (first, requested_last) = parse_pdf_page_range(pages)?;
    if first > total_pages {
        bail!("PDF 只有 {total_pages} 页，pages 从 {first} 开始越界")
    }
    let last = requested_last.unwrap_or(total_pages);
    if last > total_pages {
        bail!("PDF 只有 {total_pages} 页，pages 结束于 {last}")
    }
    let count = last - first + 1;
    if count > MAX_PDF_PAGES_PER_READ {
        bail!("PDF pages 范围包含 {count} 页，超过每次 {MAX_PDF_PAGES_PER_READ} 页限制")
    }
    let deleted = (1..=total_pages)
        .filter(|page| *page < first || *page > last)
        .map(|page| u32::try_from(page).context("PDF 页码超出 u32 范围"))
        .collect::<Result<Vec<_>>>()?;
    document.delete_pages(&deleted);
    document.prune_objects();
    document.renumber_objects();
    document.compress();
    let mut selected_bytes = Vec::new();
    document
        .save_to(&mut selected_bytes)
        .context("无法序列化选定 PDF 页面")?;
    if selected_bytes.len() > MAX_PDF_RAW_BYTES {
        bail!(
            "选定 PDF 页面序列化后为 {} 字节，超过 {MAX_PDF_RAW_BYTES} 字节限制",
            selected_bytes.len()
        )
    }
    let reparsed =
        lopdf::Document::load_mem(&selected_bytes).context("选定 PDF 页面序列化后校验失败")?;
    if reparsed.get_pages().len() != count {
        bail!("选定 PDF 页面数量校验失败")
    }
    Ok(PdfPayload {
        bytes: selected_bytes,
        total_pages,
        selected: Some((first, last)),
    })
}

fn parse_pdf_page_range(value: &str) -> Result<(usize, Option<usize>)> {
    let value = value.trim();
    if value.is_empty() {
        bail!("pages 不能为空")
    }
    if let Some((first, last)) = value.split_once('-') {
        if last.contains('-') {
            bail!("pages 必须是单页、闭区间或开放区间，例如 3、1-5、10-")
        }
        let first = parse_positive_page(first)?;
        if last.is_empty() {
            return Ok((first, None));
        }
        let last = parse_positive_page(last)?;
        if last < first {
            bail!("pages 结束页不能小于开始页")
        }
        Ok((first, Some(last)))
    } else {
        let page = parse_positive_page(value)?;
        Ok((page, Some(page)))
    }
}

fn parse_positive_page(value: &str) -> Result<usize> {
    let page = value
        .parse::<usize>()
        .with_context(|| format!("无效 PDF 页码 `{value}`"))?;
    if page == 0 {
        bail!("PDF 页码从 1 开始")
    }
    Ok(page)
}

async fn read_notebook(
    context: &ToolContext,
    path: &Path,
    metadata_len: u64,
) -> Result<ToolOutput> {
    let bytes =
        read_binary_bounded(path, metadata_len, MAX_SIZE as usize, "Jupyter notebook").await?;
    if bytes.contains(&0) {
        bail!("Jupyter notebook 包含 NUL 字节")
    }
    let raw = String::from_utf8(bytes).context("Jupyter notebook 不是有效 UTF-8")?;
    let notebook: Value = serde_json::from_str(&raw).context("Jupyter notebook 不是有效 JSON")?;
    let cells = notebook
        .get("cells")
        .and_then(Value::as_array)
        .context("Jupyter notebook 缺少 cells array")?;
    if cells.len() > MAX_NOTEBOOK_CELLS {
        bail!(
            "Jupyter notebook 有 {} 个 cells，超过 {MAX_NOTEBOOK_CELLS} 个限制",
            cells.len()
        )
    }
    let display = context.display_path(path);
    let (content, output_count, media_count) = render_notebook(&display, cells)?;
    context.remember_read(path.to_owned(), raw, false).await?;
    let preview = format!(
        "Read notebook {display} ({} cells, {output_count} outputs, {media_count} visualizations)",
        cells.len()
    );
    Ok(ToolOutput::success_with_model_content(preview, content))
}

struct NotebookRenderer {
    blocks: Vec<Value>,
    text: String,
    text_bytes: usize,
    output_count: usize,
    media_count: usize,
    media_raw_bytes: usize,
}

impl NotebookRenderer {
    fn new(display: &str, cell_count: usize) -> Self {
        let text = format!("Notebook {display}: {cell_count} cells\n");
        let text_bytes = text.len();
        Self {
            blocks: Vec::new(),
            text,
            text_bytes,
            output_count: 0,
            media_count: 0,
            media_raw_bytes: 0,
        }
    }

    fn push_text(&mut self, value: &str) -> Result<()> {
        self.text_bytes = self
            .text_bytes
            .checked_add(value.len())
            .context("notebook 文本大小溢出")?;
        if self.text_bytes > MAX_NOTEBOOK_TEXT_BYTES {
            bail!(
                "notebook 结构化文本超过 {MAX_NOTEBOOK_TEXT_BYTES} 字节限制；请用 jq 或脚本读取特定 cells"
            )
        }
        self.text.push_str(value);
        Ok(())
    }

    fn flush_text(&mut self) {
        if !self.text.is_empty() {
            self.blocks
                .push(json!({"type":"text", "text":std::mem::take(&mut self.text)}));
        }
    }

    fn push_image(&mut self, media_type: &str, encoded: &str) -> Result<()> {
        if self.media_count >= MAX_NOTEBOOK_MEDIA {
            bail!("notebook 可视化超过 {MAX_NOTEBOOK_MEDIA} 个限制")
        }
        let decoded = BASE64
            .decode(encoded.as_bytes())
            .context("notebook 可视化包含无效 base64")?;
        if detect_image_media_type(&decoded) != Some(media_type) {
            bail!("notebook 可视化的 MIME 类型与文件内容不一致: {media_type}")
        }
        self.media_raw_bytes = self
            .media_raw_bytes
            .checked_add(decoded.len())
            .context("notebook 可视化大小溢出")?;
        if self.media_raw_bytes > MAX_NOTEBOOK_MEDIA_RAW_BYTES {
            bail!("notebook 可视化总大小超过 {MAX_NOTEBOOK_MEDIA_RAW_BYTES} 字节限制")
        }
        self.flush_text();
        self.blocks.push(json!({"type":"image", "source":{
            "type":"base64", "media_type":media_type, "data":encoded
        }}));
        self.media_count += 1;
        Ok(())
    }

    fn finish(mut self) -> (Value, usize, usize) {
        self.flush_text();
        (
            Value::Array(self.blocks),
            self.output_count,
            self.media_count,
        )
    }
}

fn render_notebook(display: &str, cells: &[Value]) -> Result<(Value, usize, usize)> {
    let mut renderer = NotebookRenderer::new(display, cells.len());
    for (index, cell) in cells.iter().enumerate() {
        let object = cell
            .as_object()
            .with_context(|| format!("notebook cell {index} 不是 object"))?;
        let cell_type = object
            .get("cell_type")
            .and_then(Value::as_str)
            .context("notebook cell 缺少 cell_type")?;
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .map_or_else(|| format!("cell-{index}"), str::to_owned);
        let source = notebook_text(object.get("source"), "cell source")?;
        renderer.push_text(&format!(
            "\n## Cell {index} [{cell_type}] id={id}\n{source}\n"
        ))?;
        if cell_type != "code" {
            continue;
        }
        let outputs = object
            .get("outputs")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        renderer.output_count = renderer
            .output_count
            .checked_add(outputs.len())
            .context("notebook output 数量溢出")?;
        if renderer.output_count > MAX_NOTEBOOK_OUTPUTS {
            bail!("notebook outputs 超过 {MAX_NOTEBOOK_OUTPUTS} 个限制")
        }
        for (output_index, output) in outputs.iter().enumerate() {
            render_notebook_output(&mut renderer, index, output_index, output)?;
        }
    }
    Ok(renderer.finish())
}

fn render_notebook_output(
    renderer: &mut NotebookRenderer,
    cell_index: usize,
    output_index: usize,
    output: &Value,
) -> Result<()> {
    let object = output
        .as_object()
        .with_context(|| format!("notebook cell {cell_index} output {output_index} 不是 object"))?;
    let output_type = object
        .get("output_type")
        .and_then(Value::as_str)
        .context("notebook output 缺少 output_type")?;
    renderer.push_text(&format!("### Output {output_index} [{output_type}]\n"))?;
    match output_type {
        "stream" => renderer.push_text(&notebook_text(object.get("text"), "stream text")?),
        "error" => {
            let name = object
                .get("ename")
                .and_then(Value::as_str)
                .unwrap_or("Error");
            let value = object.get("evalue").and_then(Value::as_str).unwrap_or("");
            renderer.push_text(&format!("{name}: {value}\n"))?;
            renderer.push_text(&notebook_text(object.get("traceback"), "traceback")?)
        }
        "display_data" | "execute_result" => {
            let data = object
                .get("data")
                .and_then(Value::as_object)
                .context("notebook display output 缺少 data object")?;
            for (mime, value) in data {
                match mime.as_str() {
                    "image/png" | "image/jpeg" | "image/gif" | "image/webp" => {
                        let encoded = value.as_str().with_context(|| {
                            format!("{mime} notebook output 必须是 base64 string")
                        })?;
                        renderer.push_text(&format!("[{mime} visualization]\n"))?;
                        renderer.push_image(mime, encoded)?;
                    }
                    "text/plain" | "text/markdown" | "text/html" | "image/svg+xml" => {
                        renderer.push_text(&format!("[{mime}]\n"))?;
                        let rendered = notebook_text(Some(value), mime)?;
                        renderer.push_text(&rendered)?;
                        renderer.push_text("\n")?;
                    }
                    "application/json" | "application/vnd.plotly.v1+json" => {
                        renderer.push_text(&format!("[{mime}]\n{}\n", value))?;
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        other => renderer.push_text(&format!("[unsupported output type {other}]\n")),
    }
}

fn notebook_text(value: Option<&Value>, label: &str) -> Result<String> {
    match value {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(text)) => Ok(text.clone()),
        Some(Value::Array(lines)) => lines
            .iter()
            .map(|line| {
                line.as_str()
                    .map(str::to_owned)
                    .with_context(|| format!("notebook {label} array 只能包含 string"))
            })
            .collect::<Result<String>>(),
        Some(_) => bail!("notebook {label} 必须是 string 或 string array"),
    }
}

async fn read_partial(
    path: &Path,
    offset: usize,
    limit: Option<usize>,
) -> Result<(String, String, usize)> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("无法读取 {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut raw_line = Vec::new();
    let mut rendered = String::new();
    let mut observed = String::new();
    let mut line_number = 0usize;
    let mut selected = 0usize;
    let mut scanned = 0usize;
    let limit = limit.unwrap_or(usize::MAX);

    loop {
        if selected >= limit {
            break;
        }
        if !read_line_limited(&mut reader, &mut raw_line).await? {
            break;
        }
        scanned = scanned
            .checked_add(raw_line.len())
            .context("partial Read 扫描大小溢出")?;
        if scanned > MAX_PARTIAL_SCAN_BYTES {
            bail!("partial Read 扫描超过 {MAX_PARTIAL_SCAN_BYTES} 字节限制；请缩小 offset")
        }
        line_number += 1;
        if raw_line.contains(&0) {
            bail!("文件看起来是二进制文件，当前 Read 文本路径不支持")
        }
        if line_number < offset {
            continue;
        }
        let line = std::str::from_utf8(&raw_line).context("文件不是有效 UTF-8 文本")?;
        let display = line.trim_end_matches(['\r', '\n']);
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(&format!("{line_number:>6}→{display}"));
        observed.push_str(line);
        selected += 1;
        if rendered.len() / 4 > MAX_APPROX_TOKENS {
            bail!("读取结果估算超过 {MAX_APPROX_TOKENS} tokens；请缩小 offset/limit 范围")
        }
    }
    Ok((rendered, observed, line_number))
}

async fn read_line_limited<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    output: &mut Vec<u8>,
) -> Result<bool> {
    output.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(!output.is_empty());
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if output.len().saturating_add(take) > MAX_LINE_BYTES {
            bail!("单行超过 {MAX_LINE_BYTES} 字节限制")
        }
        output.extend_from_slice(&available[..take]);
        let found_newline = take < available.len() || available[take - 1] == b'\n';
        reader.consume(take);
        if found_newline {
            return Ok(true);
        }
    }
}

fn block_device(path: &Path) -> Result<()> {
    let text = path.to_string_lossy();
    if BLOCKED_DEVICES.iter().any(|blocked| text == *blocked)
        || (text.starts_with("/proc/")
            && ["/fd/0", "/fd/1", "/fd/2"]
                .iter()
                .any(|suffix| text.ends_with(suffix)))
    {
        bail!("拒绝读取会阻塞或产生无限输出的设备文件: {text}")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use lopdf::{Document, Object, dictionary};
    use tempfile::TempDir;

    use super::*;
    use crate::permissions::{PermissionManager, PermissionMode};

    fn test_context(temp: &TempDir) -> ToolContext {
        ToolContext::new(
            temp.path().to_owned(),
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        )
    }

    fn minimal_pdf(page_count: usize) -> Vec<u8> {
        let mut document = Document::with_version("1.5");
        let pages_id = document.new_object_id();
        let page_ids = (0..page_count)
            .map(|_| {
                document.add_object(dictionary! {
                    "Type" => "Page",
                    "Parent" => pages_id,
                    "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
                })
            })
            .collect::<Vec<_>>();
        document.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => page_ids.iter().copied().map(Object::Reference).collect::<Vec<_>>(),
                "Count" => page_count as i64,
            }),
        );
        let catalog_id = document.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        document.trailer.set("Root", catalog_id);
        let mut bytes = Vec::new();
        document.save_to(&mut bytes).unwrap();
        bytes
    }

    #[tokio::test]
    async fn image_read_returns_model_facing_base64_without_polluting_preview() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pixel.png");
        let bytes = b"\x89PNG\r\n\x1a\nfixture";
        fs::write(&path, bytes).unwrap();

        let output = ReadTool
            .execute(&test_context(&temp), json!({"file_path":"pixel.png"}))
            .await
            .unwrap();

        assert!(!output.is_error);
        assert!(!output.content.contains(&BASE64.encode(bytes)));
        let blocks = output.model_content.unwrap();
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["source"]["media_type"], "image/png");
        assert_eq!(blocks[1]["source"]["data"], BASE64.encode(bytes));
    }

    #[tokio::test]
    async fn image_extension_does_not_bypass_content_validation_or_size_limit() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("fake.png"), b"not an image").unwrap();
        let error = ReadTool
            .execute(&test_context(&temp), json!({"file_path":"fake.png"}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("不是受支持"));

        let oversized = temp.path().join("large.png");
        let file = fs::File::create(&oversized).unwrap();
        file.set_len((MAX_IMAGE_RAW_BYTES + 1) as u64).unwrap();
        let error = ReadTool
            .execute(&test_context(&temp), json!({"file_path":"large.png"}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("超过"));
    }

    #[test]
    fn pdf_page_ranges_are_strict_and_selection_is_revalidated() {
        assert_eq!(parse_pdf_page_range("3").unwrap(), (3, Some(3)));
        assert_eq!(parse_pdf_page_range("2-5").unwrap(), (2, Some(5)));
        assert_eq!(parse_pdf_page_range("7-").unwrap(), (7, None));
        for invalid in ["", "0", "4-2", "a", "1-2-3"] {
            assert!(parse_pdf_page_range(invalid).is_err(), "{invalid}");
        }

        let payload = prepare_pdf(minimal_pdf(25), Some("3-7")).unwrap();
        assert_eq!(payload.total_pages, 25);
        assert_eq!(payload.selected, Some((3, 7)));
        assert_eq!(
            Document::load_mem(&payload.bytes)
                .unwrap()
                .get_pages()
                .len(),
            5
        );
        assert!(prepare_pdf(minimal_pdf(25), None).is_err());
        assert!(prepare_pdf(minimal_pdf(25), Some("1-21")).is_err());
        assert!(prepare_pdf(minimal_pdf(5), Some("6")).is_err());
    }

    #[tokio::test]
    async fn pdf_read_emits_document_block_for_whole_and_selected_ranges() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("demo.pdf"), minimal_pdf(3)).unwrap();
        let context = test_context(&temp);

        let whole = ReadTool
            .execute(&context, json!({"file_path":"demo.pdf"}))
            .await
            .unwrap();
        let whole_blocks = whole.model_content.unwrap();
        assert_eq!(whole_blocks[1]["type"], "document");
        assert_eq!(whole_blocks[1]["source"]["media_type"], "application/pdf");

        let selected = ReadTool
            .execute(&context, json!({"file_path":"demo.pdf", "pages":"2-3"}))
            .await
            .unwrap();
        assert!(selected.content.contains("pages 2-3 of 3"));
        let encoded = selected.model_content.unwrap()[1]["source"]["data"]
            .as_str()
            .unwrap()
            .to_owned();
        let selected_pdf = BASE64.decode(encoded).unwrap();
        assert_eq!(
            Document::load_mem(&selected_pdf).unwrap().get_pages().len(),
            2
        );
    }

    #[tokio::test]
    async fn notebook_read_renders_cells_outputs_and_visualizations_not_raw_json() {
        let temp = tempfile::tempdir().unwrap();
        let image = BASE64.encode(b"\x89PNG\r\n\x1a\nfixture");
        let notebook = json!({
            "nbformat":4,
            "nbformat_minor":5,
            "cells":[
                {"cell_type":"markdown", "id":"intro", "source":["# Demo\n", "hello"]},
                {"cell_type":"code", "id":"plot", "source":"print('ok')", "outputs":[
                    {"output_type":"stream", "name":"stdout", "text":"ok\n"},
                    {"output_type":"display_data", "data":{
                        "text/plain":"<figure>", "image/png":image
                    }, "metadata":{}}
                ], "execution_count":1}
            ],
            "metadata":{}
        });
        fs::write(
            temp.path().join("demo.ipynb"),
            serde_json::to_vec(&notebook).unwrap(),
        )
        .unwrap();

        let context = test_context(&temp);
        let output = ReadTool
            .execute(&context, json!({"file_path":"demo.ipynb"}))
            .await
            .unwrap();
        assert!(
            output
                .content
                .contains("2 cells, 2 outputs, 1 visualizations")
        );
        let blocks = output.model_content.unwrap();
        let serialized = blocks.to_string();
        assert!(serialized.contains("Cell 0 [markdown] id=intro"));
        assert!(serialized.contains("Output 1 [display_data]"));
        assert!(
            blocks
                .as_array()
                .unwrap()
                .iter()
                .any(|block| block["type"] == "image")
        );
        assert!(!serialized.contains("nbformat_minor"));
        let resolved = context.resolve_path("demo.ipynb").unwrap();
        context.require_full_read(&resolved).await.unwrap();
    }

    #[tokio::test]
    async fn media_parameters_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("image.png"), b"\x89PNG\r\n\x1a\nfixture").unwrap();
        fs::write(temp.path().join("text.txt"), b"hello").unwrap();
        let context = test_context(&temp);

        assert!(
            ReadTool
                .execute(&context, json!({"file_path":"image.png", "offset":2}),)
                .await
                .is_err()
        );
        assert!(
            ReadTool
                .execute(&context, json!({"file_path":"text.txt", "pages":"1"}),)
                .await
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn media_symlink_scope_uses_canonical_target() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let outside = root.path().join("outside.png");
        fs::write(&outside, b"\x89PNG\r\n\x1a\nfixture").unwrap();
        symlink(&outside, workspace.join("linked.png")).unwrap();
        let context = ToolContext::new(
            workspace,
            PermissionManager::new(
                PermissionMode::BypassPermissions,
                false,
                Vec::new(),
                Vec::new(),
            ),
        );
        assert!(context.is_outside_workspace("linked.png").unwrap());
    }
}

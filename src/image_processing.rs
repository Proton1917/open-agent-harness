//! Bounded provider-neutral image normalization.
//!
//! The source snapshot routes file reads, pasted images, and MCP image blocks
//! through one image processor before model transport. This Rust module owns
//! the equivalent resource and media invariants without depending on bundled
//! native modules or provider-specific endpoints.

use std::{collections::BTreeSet, io::Cursor};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use image::{
    ColorType, DynamicImage, GenericImageView, ImageEncoder, ImageFormat, ImageReader, RgbImage,
    codecs::{
        jpeg::JpegEncoder,
        png::{CompressionType, FilterType as PngFilterType, PngEncoder},
        webp::WebPEncoder,
    },
    imageops::FilterType,
};
use serde_json::Value;

/// Maximum compressed input accepted for one normalization operation.
pub const MAX_IMAGE_INPUT_BYTES: usize = 20 * 1024 * 1024;
/// Raw output target that remains below a 5 MiB base64 transport ceiling.
pub const TARGET_IMAGE_RAW_BYTES: usize = 15 * 1024 * 1024 / 4;
/// Images larger than these dimensions are downsampled before model transport.
pub const MAX_IMAGE_WIDTH: u32 = 2_000;
pub const MAX_IMAGE_HEIGHT: u32 = 2_000;

const MAX_DECODE_DIMENSION: u32 = 16_384;
const MAX_DECODE_PIXELS: u64 = 100_000_000;
const MAX_DECODE_ALLOC_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessedImage {
    pub bytes: Vec<u8>,
    pub media_type: &'static str,
    pub original_media_type: &'static str,
    pub original_bytes: usize,
    pub original_width: u32,
    pub original_height: u32,
    pub display_width: u32,
    pub display_height: u32,
}

impl ProcessedImage {
    pub fn changed(&self) -> bool {
        self.original_media_type != self.media_type
            || self.original_bytes != self.bytes.len()
            || self.original_width != self.display_width
            || self.original_height != self.display_height
    }
}

pub fn normalize_image(bytes: Vec<u8>) -> Result<ProcessedImage> {
    normalize_image_to_limits(
        bytes,
        MAX_IMAGE_WIDTH,
        MAX_IMAGE_HEIGHT,
        TARGET_IMAGE_RAW_BYTES,
    )
}

pub fn detect_supported_image_type(bytes: &[u8]) -> Option<&'static str> {
    detect_supported_format(bytes).map(|(_, media_type)| media_type)
}

/// Normalizes direct user/SDK image blocks before they enter a query
/// transaction. Tool-result media is normalized at its owning tool boundary.
pub async fn normalize_user_content_images(content: Value) -> Result<Value> {
    let has_images = content.as_array().is_some_and(|blocks| {
        blocks
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("image"))
    });
    if !has_images {
        return Ok(content);
    }
    tokio::task::spawn_blocking(move || normalize_user_content_images_sync(content))
        .await
        .context("用户图片处理任务异常终止")?
}

fn normalize_user_content_images_sync(mut content: Value) -> Result<Value> {
    let blocks = content
        .as_array_mut()
        .context("用户富媒体 content 必须是 array")?;
    for (index, block) in blocks.iter_mut().enumerate() {
        if block.get("type").and_then(Value::as_str) != Some("image") {
            continue;
        }
        let source = block
            .get_mut("source")
            .and_then(Value::as_object_mut)
            .with_context(|| format!("用户图片 block {index}.source 必须是 object"))?;
        if source.get("type").and_then(Value::as_str) != Some("base64") {
            bail!("用户图片 block {index} 只支持 base64 source")
        }
        let declared = source
            .get("media_type")
            .and_then(Value::as_str)
            .with_context(|| format!("用户图片 block {index} 缺少 media_type"))?;
        if !matches!(
            declared,
            "image/png" | "image/jpeg" | "image/gif" | "image/webp"
        ) {
            bail!("用户图片 block {index} media_type {declared:?} 不受支持")
        }
        let encoded = source
            .get("data")
            .and_then(Value::as_str)
            .with_context(|| format!("用户图片 block {index} 缺少 base64 data"))?;
        let max_base64 = MAX_IMAGE_INPUT_BYTES.div_ceil(3).saturating_mul(4);
        if encoded.is_empty() || encoded.len() > max_base64 {
            bail!("用户图片 block {index} base64 为空或超过 {max_base64} 字节限制")
        }
        let decoded = BASE64
            .decode(encoded)
            .with_context(|| format!("用户图片 block {index} 包含无效 base64"))?;
        if BASE64.encode(&decoded) != encoded {
            bail!("用户图片 block {index} 不是规范的 RFC 4648 base64")
        }
        let image = normalize_image(decoded)
            .with_context(|| format!("用户图片 block {index} 无法归一化"))?;
        if image.original_media_type != declared {
            bail!(
                "用户图片 block {index} 内容签名 {} 与声明 MIME {declared:?} 不一致",
                image.original_media_type
            )
        }
        source.insert(
            "media_type".to_owned(),
            Value::String(image.media_type.to_owned()),
        );
        source.insert("data".to_owned(), Value::String(BASE64.encode(image.bytes)));
    }
    Ok(content)
}

fn normalize_image_to_limits(
    bytes: Vec<u8>,
    max_width: u32,
    max_height: u32,
    target_bytes: usize,
) -> Result<ProcessedImage> {
    if bytes.is_empty() {
        bail!("图片为空")
    }
    if bytes.len() > MAX_IMAGE_INPUT_BYTES {
        bail!("图片超过 {MAX_IMAGE_INPUT_BYTES} 字节输入限制")
    }
    if max_width == 0 || max_height == 0 || target_bytes == 0 {
        bail!("图片归一化限制必须大于 0")
    }
    let original_bytes = bytes.len();
    let (format, original_media_type) =
        detect_supported_format(&bytes).context("图片内容不是受支持的 PNG、JPEG、GIF 或 WebP")?;
    let decoded = decode_bounded(&bytes, format)?;
    let (original_width, original_height) = decoded.dimensions();
    validate_dimensions(original_width, original_height)?;

    if original_bytes <= target_bytes
        && original_width <= max_width
        && original_height <= max_height
    {
        return Ok(ProcessedImage {
            bytes,
            media_type: original_media_type,
            original_media_type,
            original_bytes,
            original_width,
            original_height,
            display_width: original_width,
            display_height: original_height,
        });
    }

    let mut dimensions_seen = BTreeSet::new();
    let maximums = [
        (max_width, max_height),
        scale_box(max_width, max_height, 3, 4),
        scale_box(max_width, max_height, 1, 2),
        scale_box(max_width, max_height, 1, 4),
        (800, 800),
        (600, 600),
        (400, 400),
    ];
    for (candidate_width, candidate_height) in maximums {
        let candidate = resize_inside_without_enlargement(
            &decoded,
            candidate_width.min(max_width).max(1),
            candidate_height.min(max_height).max(1),
        );
        let dimensions = candidate.dimensions();
        if !dimensions_seen.insert(dimensions) {
            continue;
        }

        if let Some((encoded, media_type)) = encode_preferred(&candidate, format)?
            .filter(|(encoded, _)| encoded.len() <= target_bytes)
        {
            return Ok(processed(
                encoded,
                media_type,
                original_media_type,
                original_bytes,
                original_width,
                original_height,
                dimensions,
            ));
        }
        for quality in [80, 60, 40, 20] {
            let encoded = encode_jpeg(&candidate, quality)?;
            if encoded.len() <= target_bytes {
                return Ok(processed(
                    encoded,
                    "image/jpeg",
                    original_media_type,
                    original_bytes,
                    original_width,
                    original_height,
                    dimensions,
                ));
            }
        }
    }

    bail!("图片在有界缩放和压缩后仍超过 {target_bytes} 字节；请使用更小的图片")
}

fn resize_inside_without_enlargement(
    image: &DynamicImage,
    max_width: u32,
    max_height: u32,
) -> DynamicImage {
    let (width, height) = image.dimensions();
    if width <= max_width && height <= max_height {
        image.clone()
    } else {
        image.resize(max_width, max_height, FilterType::Lanczos3)
    }
}

fn processed(
    bytes: Vec<u8>,
    media_type: &'static str,
    original_media_type: &'static str,
    original_bytes: usize,
    original_width: u32,
    original_height: u32,
    (display_width, display_height): (u32, u32),
) -> ProcessedImage {
    ProcessedImage {
        bytes,
        media_type,
        original_media_type,
        original_bytes,
        original_width,
        original_height,
        display_width,
        display_height,
    }
}

fn scale_box(width: u32, height: u32, numerator: u32, denominator: u32) -> (u32, u32) {
    (
        width.saturating_mul(numerator).div_ceil(denominator).max(1),
        height
            .saturating_mul(numerator)
            .div_ceil(denominator)
            .max(1),
    )
}

fn validate_dimensions(width: u32, height: u32) -> Result<()> {
    if width == 0 || height == 0 {
        bail!("图片尺寸不能为 0")
    }
    if width > MAX_DECODE_DIMENSION || height > MAX_DECODE_DIMENSION {
        bail!("图片尺寸 {width}x{height} 超过 {MAX_DECODE_DIMENSION} 解码边界")
    }
    let pixels = u64::from(width)
        .checked_mul(u64::from(height))
        .context("图片像素数溢出")?;
    if pixels > MAX_DECODE_PIXELS {
        bail!("图片像素数 {pixels} 超过 {MAX_DECODE_PIXELS} 解码边界")
    }
    Ok(())
}

fn decode_bounded(bytes: &[u8], format: ImageFormat) -> Result<DynamicImage> {
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_DECODE_DIMENSION);
    limits.max_image_height = Some(MAX_DECODE_DIMENSION);
    limits.max_alloc = Some(MAX_DECODE_ALLOC_BYTES);
    let mut reader = ImageReader::with_format(Cursor::new(bytes), format);
    reader.limits(limits);
    reader.decode().context("图片解码失败")
}

fn detect_supported_format(bytes: &[u8]) -> Option<(ImageFormat, &'static str)> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some((ImageFormat::Png, "image/png"))
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some((ImageFormat::Jpeg, "image/jpeg"))
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some((ImageFormat::Gif, "image/gif"))
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some((ImageFormat::WebP, "image/webp"))
    } else {
        None
    }
}

fn encode_preferred(
    image: &DynamicImage,
    original_format: ImageFormat,
) -> Result<Option<(Vec<u8>, &'static str)>> {
    match original_format {
        ImageFormat::Png | ImageFormat::Gif => Ok(Some((encode_png(image)?, "image/png"))),
        ImageFormat::WebP => Ok(Some((encode_webp(image)?, "image/webp"))),
        ImageFormat::Jpeg => Ok(Some((encode_jpeg(image, 90)?, "image/jpeg"))),
        _ => Ok(None),
    }
}

fn encode_png(image: &DynamicImage) -> Result<Vec<u8>> {
    let rgba = image.to_rgba8();
    let mut output = Vec::new();
    PngEncoder::new_with_quality(&mut output, CompressionType::Best, PngFilterType::Adaptive)
        .write_image(
            rgba.as_raw(),
            rgba.width(),
            rgba.height(),
            ColorType::Rgba8.into(),
        )
        .context("PNG 编码失败")?;
    Ok(output)
}

fn encode_webp(image: &DynamicImage) -> Result<Vec<u8>> {
    let rgba = image.to_rgba8();
    let mut output = Vec::new();
    WebPEncoder::new_lossless(&mut output)
        .write_image(
            rgba.as_raw(),
            rgba.width(),
            rgba.height(),
            ColorType::Rgba8.into(),
        )
        .context("WebP 编码失败")?;
    Ok(output)
}

fn encode_jpeg(image: &DynamicImage, quality: u8) -> Result<Vec<u8>> {
    let rgb = flatten_to_rgb(image);
    let mut output = Vec::new();
    JpegEncoder::new_with_quality(&mut output, quality)
        .encode(
            rgb.as_raw(),
            rgb.width(),
            rgb.height(),
            ColorType::Rgb8.into(),
        )
        .context("JPEG 编码失败")?;
    Ok(output)
}

fn flatten_to_rgb(image: &DynamicImage) -> RgbImage {
    let rgba = image.to_rgba8();
    let mut rgb = RgbImage::new(rgba.width(), rgba.height());
    for (source, target) in rgba.pixels().zip(rgb.pixels_mut()) {
        let alpha = u16::from(source[3]);
        for channel in 0..3 {
            let foreground = u16::from(source[channel]) * alpha;
            let background = 255 * (255 - alpha);
            target[channel] = ((foreground + background + 127) / 255) as u8;
        }
    }
    rgb
}

#[cfg(test)]
mod tests {
    use image::{ImageBuffer, Rgba};

    use super::*;

    fn fixture_png(width: u32, height: u32, noisy: bool) -> Vec<u8> {
        let mut state = 0x1234_5678u32;
        let image = ImageBuffer::from_fn(width, height, |x, y| {
            if noisy {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                Rgba([state as u8, (state >> 8) as u8, (state >> 16) as u8, 255])
            } else {
                Rgba([(x % 255) as u8, (y % 255) as u8, 127, 255])
            }
        });
        encode_png(&DynamicImage::ImageRgba8(image)).unwrap()
    }

    #[test]
    fn valid_small_image_is_preserved_after_bounded_decode() {
        let source = fixture_png(32, 24, false);
        let result = normalize_image(source.clone()).unwrap();
        assert_eq!(result.bytes, source);
        assert_eq!(result.media_type, "image/png");
        assert_eq!((result.display_width, result.display_height), (32, 24));
        assert!(!result.changed());
    }

    #[test]
    fn oversized_dimensions_are_resized_with_aspect_ratio() {
        let source = fixture_png(2_400, 12, false);
        let result = normalize_image(source).unwrap();
        assert_eq!(result.original_width, 2_400);
        assert!(result.display_width <= MAX_IMAGE_WIDTH);
        assert!(result.display_height <= MAX_IMAGE_HEIGHT);
        assert!(result.changed());
        let decoded = decode_bounded(
            &result.bytes,
            detect_supported_format(&result.bytes).unwrap().0,
        )
        .unwrap();
        assert_eq!(
            decoded.dimensions(),
            (result.display_width, result.display_height)
        );
    }

    #[test]
    fn byte_budget_uses_progressive_lossy_fallback() {
        let source = fixture_png(160, 160, true);
        let result = normalize_image_to_limits(source, 160, 160, 12 * 1024).unwrap();
        assert!(result.bytes.len() <= 12 * 1024);
        assert_eq!(result.media_type, "image/jpeg");
        assert!(result.display_width <= 160);
        assert!(result.display_height <= 160);
    }

    #[test]
    fn malformed_and_unbounded_inputs_fail_closed() {
        assert!(normalize_image(b"\x89PNG\r\n\x1a\nnot-an-image".to_vec()).is_err());
        assert!(normalize_image(vec![0; MAX_IMAGE_INPUT_BYTES + 1]).is_err());
    }

    #[tokio::test]
    async fn direct_user_image_blocks_are_normalized_and_signature_checked() {
        let original = fixture_png(2_400, 2, false);
        let content = serde_json::json!([
            {"type":"text", "text":"inspect"},
            {"type":"image", "source":{
                "type":"base64", "media_type":"image/png", "data":BASE64.encode(&original)
            }}
        ]);
        let normalized = normalize_user_content_images(content).await.unwrap();
        let encoded = normalized[1]["source"]["data"].as_str().unwrap();
        let decoded = BASE64.decode(encoded).unwrap();
        let image = decode_bounded(&decoded, detect_supported_format(&decoded).unwrap().0).unwrap();
        assert!(image.width() <= MAX_IMAGE_WIDTH);
        assert!(image.height() <= MAX_IMAGE_HEIGHT);

        let wrong_mime = serde_json::json!([{"type":"image", "source":{
            "type":"base64", "media_type":"image/jpeg", "data":BASE64.encode(original)
        }}]);
        assert!(
            normalize_user_content_images(wrong_mime)
                .await
                .unwrap_err()
                .to_string()
                .contains("内容签名")
        );
    }
}

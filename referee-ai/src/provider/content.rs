//! 多模态内容数据模型（纯数据，无行为）
//!
//! 定义会话消息中的多模态内容片段。它们是**纯数据载体**，只描述「内容是什么、
//! 数据在哪」，不包含任何厂商逻辑句柄。厂商格式（OpenAI 数组 / `ms://` 文件引用
//! 等）的转译发生在适配器层（`openai_compat` 与各厂商适配器），以恪守
//! 「数据与行为分离」约束。
//!
//! ## 模型来源
//! 对齐 OpenAI Chat Completions 多模态标准（`content` 数组），并统一覆盖
//! MiMo 与 Kimi 的差异：
//! - 图片：`image_url`（URL 或 base64 `data:`；Kimi 仅 base64 / 文件 ID）
//! - 音频：`input_audio`（MiMo）
//! - 视频：`video_url` + `fps` / `media_resolution`（MiMo）；Kimi 用 `ms://` 文件引用
//!
//! 各厂商是否支持某模态 / 是否支持 URL，由 [`crate::provider::ProviderCapabilities`]
//! 的能力声明驱动上层降级，本模块不写厂商分支。

use serde::{Deserialize, Serialize};

/// 媒体数据来源 — 只描述「数据在哪」，不执行上传 / 转码等动作
///
/// 内部序列化采用 internally-tagged 枚举（故用结构体变体，保证无损往返）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum MediaSource {
    /// 公网可访问 URL
    Url { url: String },
    /// Base64 编码数据（携带 MIME；序列化时拼为 `data:{mime};base64,{data}`）
    Base64 { mime: String, data: String },
    /// 厂商文件 ID 引用（如 Kimi 的 `ms://<id>`）
    FileId { file_id: String },
}

/// 视频帧解析分辨率档次（MiMo `media_resolution`）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MediaResolution {
    /// 默认档次：平衡识别效果与处理效率
    #[default]
    Default,
    /// 最高分辨率档次：提升对小物体、细节纹理的识别
    Max,
}

/// 图片处理细节级别（OpenAI 标准 `image_url.detail`，DeepSeek / OpenAI 等支持）
///
/// 控制图片输入的处理方式：不追求精细视觉细节时可降档以更快、更省 token。
/// 不支持的厂商会忽略该字段，或由上层依据能力声明决定是否透传。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageDetail {
    /// 推理前缩放至 512×512：更快、更省 token
    Low,
    /// 保留原图（DeepSeek 语义等价于 original，为兼容提供）
    High,
    /// 保留原图
    Original,
    /// 自动选择（DeepSeek 当前等价于 original）
    #[default]
    Auto,
}

/// 视频理解参数（MiMo `fps` + `media_resolution`）
///
/// 含浮点 `fps`，故仅实现 `PartialEq`（不实现 `Eq`）。
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct VideoParams {
    /// 每秒抽帧数；默认 2，范围 [0.1, 10]。越高时序越精细，Token 越多
    pub fps: Option<f32>,
    /// 单帧解析分辨率档次
    pub media_resolution: Option<MediaResolution>,
}

impl VideoParams {
    /// 默认视频参数（不指定 fps / 分辨率，由厂商取默认值）
    pub const fn default_() -> Self {
        Self {
            fps: None,
            media_resolution: None,
        }
    }
}

/// 多模态内容片段 — 一条消息可由多个片段组成（文本 + 图片 + 音频 + 视频）
///
/// 含浮点 `fps`（经 [`VideoParams`]），故仅实现 `PartialEq`（不实现 `Eq`）。
/// 内部序列化采用 internally-tagged 枚举（故用结构体变体，保证无损往返）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    /// 文本片段
    Text { text: String },
    /// 图片（可选 `detail` 控制处理细节级别，OpenAI 标准 `image_url.detail`）
    Image {
        source: MediaSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    /// 音频
    Audio { source: MediaSource },
    /// 视频（MiMo 支持 `fps` / `media_resolution` 精调）
    Video {
        source: MediaSource,
        #[serde(default)]
        params: VideoParams,
    },
}

impl ContentPart {
    /// 文本片段
    pub fn text(s: impl Into<String>) -> Self {
        Self::Text { text: s.into() }
    }

    /// 图片片段（URL 或 base64），处理细节级别取厂商默认
    pub fn image(src: MediaSource) -> Self {
        Self::Image {
            source: src,
            detail: None,
        }
    }

    /// 图片片段（URL 或 base64），可指定处理细节级别（`low` 更省 token）
    pub fn image_with_detail(src: MediaSource, detail: ImageDetail) -> Self {
        Self::Image {
            source: src,
            detail: Some(detail),
        }
    }

    /// 音频片段（URL 或 base64）
    pub fn audio(src: MediaSource) -> Self {
        Self::Audio { source: src }
    }

    /// 视频片段（URL 或 base64），可指定抽帧 / 分辨率参数
    pub fn video(src: MediaSource, params: VideoParams) -> Self {
        Self::Video { source: src, params }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn content_part_serde_roundtrip() {
        let parts = vec![
            ContentPart::text("describe this"),
            ContentPart::image(MediaSource::Url {
                url: "https://example.png".into(),
            }),
            ContentPart::image_with_detail(
                MediaSource::Base64 {
                    mime: "image/png".into(),
                    data: "aGVsbG8=".into(),
                },
                ImageDetail::Low,
            ),
            ContentPart::video(
                MediaSource::Base64 {
                    mime: "video/mp4".into(),
                    data: "AAAA".into(),
                },
                VideoParams {
                    fps: Some(2.0),
                    media_resolution: Some(MediaResolution::Max),
                },
            ),
        ];
        let json = serde_json::to_value(&parts).unwrap();
        let back: Vec<ContentPart> = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(back, parts);
        // detail=Some 时序列化带出；detail=None 时省略
        let arr = json.as_array().unwrap();
        assert!(arr[1].get("detail").is_none());
        assert_eq!(arr[2]["detail"], json!("low"));
    }

    #[test]
    fn video_params_default_roundtrip() {
        let part = ContentPart::video(
            MediaSource::Url {
                url: "https://v.mp4".into(),
            },
            VideoParams::default_(),
        );
        let json = serde_json::to_value(&part).unwrap();
        let back: ContentPart = serde_json::from_value(json).unwrap();
        assert_eq!(back, part);
    }
}
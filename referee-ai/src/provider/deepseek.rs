//! DeepSeek 适配器（OpenAI 兼容协议）
//!
//! - BASE_URL: `https://api.deepseek.com`
//! - 模型：`deepseek-v4-flash` / `deepseek-v4-pro` / `deepseek-v4-flash-vision-exp`
//! - 思考开关：`extra_body.thinking.type = enabled | disabled`（默认开启）
//! - 思考强度：`reasoning_effort = low | high | max`（默认 high）
//! - 推理输出：`message.reasoning_content` / `delta.reasoning_content`
//! - usage 扩展：`prompt_cache_hit_tokens` / `prompt_cache_miss_tokens`（硬盘缓存）
//! - 多轮工具调用：assistant 消息必须完整回传 `reasoning_content`
//! - 多模态：`deepseek-v4-flash-vision-exp`（实验性）额外支持图片输入，
//!   `image_url` URL / base64 直传，可选 `detail`（low/high/original/auto）控制处理级别
//!
//! 错误码扩展（已纳入 [`crate::provider::LlmError`] 归一，与 MiMo 共享底座）：
//! - 402 余额不足 → `InsufficientBalance`
//! - 422 参数错误 → `BadRequest`（DeepSeek 特有码；MiMo 用 400 涵盖）

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{json, Value};

use crate::provider::openai_compat::{build_common_body, OpenAiCompatClient, OpenAiCompatConfig};
use crate::provider::{
    ChatRequest, ChatResponse, LLMProvider, LlmError, ModelSpec, ProviderCapabilities, ProviderId,
    ReasoningEffort, RetryPolicy, StreamChunk,
};

/// DeepSeek 厂商标识前缀
pub const DEEPSEEK_VENDOR: &str = "deepseek";

/// 预定义 ProviderId
pub mod ids {
    use crate::provider::ProviderId;

    /// `deepseek-v4-flash`：低成本快速模型
    pub const DEEPSEEK_V4_FLASH: ProviderId = ProviderId::new("deepseek/deepseek-v4-flash");
    /// `deepseek-v4-pro`：高能力模型
    pub const DEEPSEEK_V4_PRO: ProviderId = ProviderId::new("deepseek/deepseek-v4-pro");
    /// `deepseek-v4-flash-vision-exp`：实验性视觉模型（额外支持图片输入）
    pub const DEEPSEEK_V4_FLASH_VISION_EXP: ProviderId =
        ProviderId::new("deepseek/deepseek-v4-flash-vision-exp");
}

/// DeepSeek 默认 BASE_URL
pub const DEFAULT_BASE_URL: &str = "https://api.deepseek.com";

/// DeepSeek 上下文窗口 token 数（1M）
pub const CONTEXT_WINDOW_TOKENS: usize = 1024 * 1024;

/// DeepSeek 单次响应最大输出 token 数（384K）
pub const MAX_OUTPUT_TOKENS: usize = 384 * 1024;

/// DeepSeek 模型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeepSeekModel {
    /// `deepseek-v4-flash`：低成本快速
    V4Flash,
    /// `deepseek-v4-pro`：高能力
    V4Pro,
    /// `deepseek-v4-flash-vision-exp`：实验性视觉模型（flash 能力 + 图片输入）
    V4FlashVisionExp,
}

impl DeepSeekModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::V4Flash => "deepseek-v4-flash",
            Self::V4Pro => "deepseek-v4-pro",
            Self::V4FlashVisionExp => "deepseek-v4-flash-vision-exp",
        }
    }

    pub fn provider_id(&self) -> ProviderId {
        match self {
            Self::V4Flash => ids::DEEPSEEK_V4_FLASH,
            Self::V4Pro => ids::DEEPSEEK_V4_PRO,
            Self::V4FlashVisionExp => ids::DEEPSEEK_V4_FLASH_VISION_EXP,
        }
    }

    /// 模型规模规格（上下文窗口 1M / 最大输出 384K；三模型当前一致）
    pub fn spec(&self) -> ModelSpec {
        match self {
            Self::V4Flash | Self::V4Pro | Self::V4FlashVisionExp => ModelSpec {
                context_window_tokens: CONTEXT_WINDOW_TOKENS,
                max_output_tokens: MAX_OUTPUT_TOKENS,
            },
        }
    }
}

/// DeepSeek 适配器构造配置
pub struct DeepSeekConfig {
    /// API Key（环境变量 `DEEPSEEK_API_KEY`）
    pub api_key: String,
    /// 覆盖默认 BASE_URL
    pub base_url: Option<String>,
    /// 单次请求总超时（含重试）
    pub timeout: Duration,
    /// 重试策略
    pub retry: RetryPolicy,
}

impl DeepSeekConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: None,
            timeout: Duration::from_secs(120),
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    pub fn with_timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }

    pub fn with_retry(mut self, r: RetryPolicy) -> Self {
        self.retry = r;
        self
    }
}

/// DeepSeek 适配器
pub struct DeepSeekProvider {
    client: OpenAiCompatClient,
    model: DeepSeekModel,
    capabilities: ProviderCapabilities,
}

impl DeepSeekProvider {
    pub fn new(model: DeepSeekModel, cfg: DeepSeekConfig) -> Result<Self, LlmError> {
        let client = OpenAiCompatClient::new(OpenAiCompatConfig {
            base_url: cfg.base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
            api_key: cfg.api_key,
            timeout: cfg.timeout,
            retry: cfg.retry,
            extra_headers: Vec::new(),
        })?;
        Ok(Self {
            client,
            model,
            capabilities: ProviderCapabilities {
                parallel_tool_calls: true,
                system_role: true,
                streaming: true,
                usage_reported: true,
                multimodal: match model {
                    // 视觉实验模型：额外支持图片输入（URL / base64，可选 detail）
                    DeepSeekModel::V4FlashVisionExp => {
                        crate::provider::MultimodalCapabilities {
                            image: true,
                            audio: false,
                            video: false,
                            file_upload: false,
                        }
                    }
                    // 纯文本模型：无多模态
                    DeepSeekModel::V4Flash | DeepSeekModel::V4Pro => {
                        crate::provider::MultimodalCapabilities::NONE
                    }
                },
            },
        })
    }

    /// 构造 DeepSeek 请求 body：公共字段 + thinking + reasoning_effort + extras
    fn build_body(&self, req: &ChatRequest) -> Value {
        let mut body = build_common_body(
            &req.messages,
            &req.tools,
            req.tool_choice,
            req.temperature,
            req.max_tokens,
            self.model.as_str(),
        );
        // DeepSeek 思考开关
        body["thinking"] = json!({
            "type": if req.thinking.enabled { "enabled" } else { "disabled" }
        });
        // DeepSeek 思考强度（仅 DeepSeek 支持；MiMo 收到会忽略）
        if let Some(effort) = req.thinking.effort {
            body["reasoning_effort"] = json!(match effort {
                ReasoningEffort::Low => "low",
                ReasoningEffort::High => "high",
                ReasoningEffort::Max => "max",
            });
        }
        // 调用方 extras（最后写入，可覆盖厂商默认）
        for (k, v) in &req.extra {
            body[k] = v.clone();
        }
        body
    }
}

#[async_trait]
impl LLMProvider for DeepSeekProvider {
    fn id(&self) -> ProviderId {
        self.model.provider_id()
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }

    fn model_spec(&self) -> ModelSpec {
        self.model.spec()
    }

    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        let body = self.build_body(&req);
        self.client.chat(body).await
    }

    async fn chat_stream(
        &self,
        req: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        let body = self.build_body(&req);
        self.client.chat_stream(body).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{ContentPart, MediaSource, MessageContent, ThinkingConfig};

    /// 视觉实验模型：模型字段 / ProviderId / image 能力声明 / 多模态请求序列化。
    #[test]
    fn vision_model_maps_identity_and_capabilities() {
        let provider = DeepSeekProvider::new(
            DeepSeekModel::V4FlashVisionExp,
            DeepSeekConfig::new("test-key"),
        )
        .expect("create provider");

        assert_eq!(provider.model.as_str(), "deepseek-v4-flash-vision-exp");
        assert_eq!(
            provider.id(),
            ids::DEEPSEEK_V4_FLASH_VISION_EXP
        );
        assert!(provider.capabilities().multimodal.image);
        assert!(!provider.capabilities().multimodal.audio);

        // 多模态图片请求 body：image_url + 可选 detail 透传
        let mut req = ChatRequest::simple(MessageContent::multimodal(vec![
            ContentPart::text("这张图片里有什么？"),
            ContentPart::image_with_detail(
                MediaSource::Base64 {
                    mime: "image/jpeg".into(),
                    data: "aGVsbG8=".into(),
                },
                crate::provider::ImageDetail::High,
            ),
        ]));
        req.thinking = ThinkingConfig {
            enabled: true,
            effort: Some(ReasoningEffort::High),
        };

        let body = provider.build_body(&req);

        assert_eq!(body["model"], json!("deepseek-v4-flash-vision-exp"));
        assert_eq!(body["reasoning_effort"], json!("high"));
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[0], json!({"type": "text", "text": "这张图片里有什么？"}));
        assert_eq!(
            content[1],
            json!({
                "type": "image_url",
                "image_url": {
                    "url": "data:image/jpeg;base64,aGVsbG8=",
                    "detail": "high"
                }
            })
        );
    }

    /// 纯文本模型：不声明图片能力，且不因多模态内容误声明。
    #[test]
    fn text_models_have_no_image_capability() {
        for model in [DeepSeekModel::V4Flash, DeepSeekModel::V4Pro] {
            let provider =
                DeepSeekProvider::new(model, DeepSeekConfig::new("test-key")).expect("create");
            assert!(!provider.capabilities().multimodal.image);
            assert_eq!(provider.capabilities().multimodal, crate::provider::MultimodalCapabilities::NONE);
        }
    }
}

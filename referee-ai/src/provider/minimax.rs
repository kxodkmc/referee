//! MiniMax 适配器（Anthropic Messages 兼容协议）
//!
//! - BASE_URL: `https://api.minimaxi.com/anthropic`
//! - 模型：`MiniMax-M3`（原生多模态，文本 / 图片 / 视频输入）
//! - 上下文窗口：1M（1048576）；最大输出见 [`MAX_OUTPUT_TOKENS`]
//! - 思考开关：`thinking.type = adaptive | disabled`（MiniMax 特有取值；
//!   Anthropic 标准为 `enabled`，语义近似，统一映射为 `adaptive`）
//! - 推理输出：`content[].type = thinking` / 流式 `thinking_delta`
//! - usage 扩展：`input_tokens` / `output_tokens` / `cache_creation_input_tokens`
//!   / `cache_read_input_tokens`（归一为 [`crate::provider::TokenUsage`]）
//! - 鉴权：`Authorization: Bearer <API_KEY>`（官方文档：与 `x-api-key` 并存时
//!   以 `Authorization` 优先）
//!
//! 本适配器复用 [`crate::provider::anthropic_compat::AnthropicClient`] 共享底座，
//! 仅在 build_body 组装 MiniMax 特有字段（`thinking.type = adaptive|disabled`）。
//! 多模态：图片 `image.source.url`、视频 `video.source.url` 原生支持。

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{json, Value};

use crate::provider::anthropic_compat::{build_common_body, AnthropicClient, AnthropicConfig};
use crate::provider::{
    ChatRequest, ChatResponse, LLMProvider, LlmError, ModelSpec, ProviderCapabilities, ProviderId,
    RetryPolicy, StreamChunk,
};

/// MiniMax 厂商标识前缀
pub const MINIMAX_VENDOR: &str = "minimax";

/// 预定义 ProviderId
pub mod ids {
    use crate::provider::ProviderId;

    /// `MiniMax-M3`：原生多模态（文本/图片/视频）Frontier 模型
    pub const MINIMAX_M3: ProviderId = ProviderId::new("minimax/minimax-m3");
}

/// MiniMax 默认 BASE_URL（Anthropic Messages 兼容协议）
pub const DEFAULT_BASE_URL: &str = "https://api.minimaxi.com/anthropic";

/// MiniMax-M3 上下文窗口 token 数（1M）
pub const CONTEXT_WINDOW_TOKENS: usize = 1_048_576;

/// MiniMax-M3 单次响应最大输出 token 数
///
/// 官方未公开硬上限；取 64K 作为预算护栏，实际操作由请求 `max_tokens` 控制。
pub const MAX_OUTPUT_TOKENS: usize = 64 * 1024;

/// MiniMax 模型枚举（决定 model 字段与 ProviderId）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiniMaxModel {
    /// `MiniMax-M3`：多模态 + 思考 + 工具调用
    M3,
}

impl MiniMaxModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::M3 => "MiniMax-M3",
        }
    }

    pub fn provider_id(&self) -> ProviderId {
        match self {
            Self::M3 => ids::MINIMAX_M3,
        }
    }

    /// 模型规模规格（上下文窗口 1M / 最大输出 64K）
    pub fn spec(&self) -> ModelSpec {
        match self {
            Self::M3 => ModelSpec {
                context_window_tokens: CONTEXT_WINDOW_TOKENS,
                max_output_tokens: MAX_OUTPUT_TOKENS,
            },
        }
    }
}

/// MiniMax 适配器构造配置
pub struct MiniMaxConfig {
    /// API Key（环境变量 `MINIMAX_API_KEY`）
    pub api_key: String,
    /// 覆盖默认 BASE_URL（自部署 / 代理）
    pub base_url: Option<String>,
    /// 单次请求总超时（含重试）
    pub timeout: Duration,
    /// 重试策略
    pub retry: RetryPolicy,
}

impl MiniMaxConfig {
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

/// MiniMax 适配器
pub struct MiniMaxProvider {
    client: AnthropicClient,
    model: MiniMaxModel,
    capabilities: ProviderCapabilities,
}

impl MiniMaxProvider {
    pub fn new(model: MiniMaxModel, cfg: MiniMaxConfig) -> Result<Self, LlmError> {
        let client = AnthropicClient::new(AnthropicConfig {
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
                // MiniMax-M3：图片 / 视频原生多模态（URL 直传），音频不在 Anthropic 协议内
                multimodal: crate::provider::MultimodalCapabilities {
                    image: true,
                    audio: false,
                    video: true,
                    file_upload: false,
                },
            },
        })
    }

    /// 构造 MiniMax 请求 body：公共字段 + `thinking` 开关 + 调用方 extras
    ///
    /// `max_tokens` 为 Anthropic 协议必填：调用方未指定时回退模型输出上限。
    /// MiniMax M3 思考取值 `adaptive`（开）/ `disabled`（关）。
    fn build_body(&self, req: &ChatRequest) -> Value {
        let max_tokens = req
            .max_tokens
            .unwrap_or(self.model.spec().max_output_tokens);
        let mut body = build_common_body(
            &req.messages,
            &req.tools,
            req.tool_choice,
            req.temperature,
            max_tokens,
            self.model.as_str(),
        );
        body["thinking"] = json!({
            "type": if req.thinking.enabled { "adaptive" } else { "disabled" }
        });
        // 调用方 extras（最后写入，可覆盖厂商默认）
        for (k, v) in &req.extra {
            body[k] = v.clone();
        }
        body
    }
}

#[async_trait]
impl LLMProvider for MiniMaxProvider {
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

    /// 验证 MiniMax 特有字段映射：model / thinking.adaptive / max_tokens 必填回退。
    #[test]
    fn build_body_maps_minimax_specific_fields() {
        let provider = MiniMaxProvider::new(MiniMaxModel::M3, MiniMaxConfig::new("test-key"))
            .expect("create provider");

        let mut req = ChatRequest::simple("hi");
        req.thinking = ThinkingConfig {
            enabled: true,
            effort: None,
        };
        req.max_tokens = Some(500);

        let body = provider.build_body(&req);
        assert_eq!(body["model"], json!("MiniMax-M3"));
        assert_eq!(body["max_tokens"], json!(500));
        assert_eq!(body["thinking"], json!({"type": "adaptive"}));
    }

    /// thinking 关闭 → `disabled`；max_tokens 缺失 → 回退模型输出上限。
    #[test]
    fn build_body_disabled_thinking_and_default_max_tokens() {
        let provider = MiniMaxProvider::new(MiniMaxModel::M3, MiniMaxConfig::new("test-key"))
            .expect("create provider");

        let mut req = ChatRequest::simple("hi");
        req.thinking.enabled = false;
        req.max_tokens = None;

        let body = provider.build_body(&req);
        assert_eq!(body["thinking"], json!({"type": "disabled"}));
        assert_eq!(body["max_tokens"], json!(MAX_OUTPUT_TOKENS));
    }

    /// 多模态（图片/视频 URL）序列化为 Anthropic 内容块。
    #[test]
    fn build_body_maps_multimodal_blocks() {
        let provider = MiniMaxProvider::new(MiniMaxModel::M3, MiniMaxConfig::new("test-key"))
            .expect("create provider");

        let mut req = ChatRequest::simple(MessageContent::multimodal(vec![
            ContentPart::text("图中是什么？"),
            ContentPart::image(MediaSource::Url {
                url: "https://filecdn.minimax.chat/public/a.jpeg".into(),
            }),
            ContentPart::video(
                MediaSource::Url {
                    url: "https://filecdn.minimax.chat/public/b.mp4".into(),
                },
                crate::provider::VideoParams::default_(),
            ),
        ]));
        req.max_tokens = Some(1024);

        let body = provider.build_body(&req);
        assert_eq!(body["model"], json!("MiniMax-M3"));
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[0], json!({"type": "text", "text": "图中是什么？"}));
        assert_eq!(
            content[1],
            json!({"type": "image", "source": {"type": "url", "url": "https://filecdn.minimax.chat/public/a.jpeg"}})
        );
        assert_eq!(
            content[2],
            json!({"type": "video", "source": {"type": "url", "url": "https://filecdn.minimax.chat/public/b.mp4"}})
        );
    }
}
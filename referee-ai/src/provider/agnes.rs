//! Agnes AI 适配器（OpenAI 兼容协议）
//!
//! - BASE_URL: `https://apihub.agnes-ai.com/v1`
//! - 模型：`agnes-2.5-flash`
//! - 上下文窗口：512K / 最大输出：65.5K（≈ 65536 token）
//! - 多模态：同一 messages 请求中文本 + 图片 URL（`image_url`）混传
//! - Thinking 模式：`chat_template_kwargs.enable_thinking = true | false`
//! - 输出长度：标准 OpenAI `max_tokens` 字段（非 `max_completion_tokens`）
//!
//! 本适配器复用 [`crate::provider::openai_compat::OpenAiCompatClient`] 共享底座，
//! 仅在 build_body 中组装 Agnes 特有字段（Thinking 开关 + `max_tokens` 字段名校正）。

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{json, Value};

use crate::provider::openai_compat::{build_common_body, OpenAiCompatClient, OpenAiCompatConfig};
use crate::provider::{
    ChatRequest, ChatResponse, LLMProvider, LlmError, ProviderCapabilities, ProviderId,
    RetryPolicy, StreamChunk,
};

/// Agnes 厂商标识前缀
pub const AGNES_VENDOR: &str = "agnes";

/// 预定义 ProviderId
pub mod ids {
    use crate::provider::ProviderId;

    /// `agnes-2.5-flash`：文本 + 图片 URL + Thinking 模式
    pub const AGNES_2_5_FLASH: ProviderId = ProviderId::new("agnes/agnes-2.5-flash");
}

/// Agnes 默认 BASE_URL（OpenAI 兼容协议）
pub const DEFAULT_BASE_URL: &str = "https://apihub.agnes-ai.com/v1";

/// Agnes 上下文窗口 token 数（512K = 512 × 1024）
pub const CONTEXT_WINDOW_TOKENS: usize = 512 * 1024;

/// Agnes 单次响应最大输出 token 数（65.5K ≈ 65536）
pub const MAX_OUTPUT_TOKENS: usize = 64 * 1024;

/// Agnes 模型枚举（决定 model 字段与 ProviderId）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgnesModel {
    /// `agnes-2.5-flash`：文本 + 图片 URL + Thinking 模式
    V25Flash,
}

impl AgnesModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::V25Flash => "agnes-2.5-flash",
        }
    }

    pub fn provider_id(&self) -> ProviderId {
        match self {
            Self::V25Flash => ids::AGNES_2_5_FLASH,
        }
    }
}

/// Agnes 适配器构造配置
pub struct AgnesConfig {
    /// API Key（环境变量 `AGNES_API_KEY`）
    pub api_key: String,
    /// 覆盖默认 BASE_URL（自部署 / 代理）
    pub base_url: Option<String>,
    /// 单次请求总超时（含重试）
    pub timeout: Duration,
    /// 重试策略
    pub retry: RetryPolicy,
}

impl AgnesConfig {
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

/// Agnes 适配器
pub struct AgnesProvider {
    client: OpenAiCompatClient,
    model: AgnesModel,
    capabilities: ProviderCapabilities,
}

impl AgnesProvider {
    pub fn new(model: AgnesModel, cfg: AgnesConfig) -> Result<Self, LlmError> {
        let client = OpenAiCompatClient::new(OpenAiCompatConfig {
            base_url: cfg.base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
            api_key: cfg.api_key,
            timeout: cfg.timeout,
            retry: cfg.retry,
        })?;
        Ok(Self {
            client,
            model,
            capabilities: ProviderCapabilities {
                parallel_tool_calls: true,
                system_role: true,
                streaming: true,
                usage_reported: true,
                max_output_tokens: MAX_OUTPUT_TOKENS,
                context_window_tokens: CONTEXT_WINDOW_TOKENS,
                // `agnes-2.5-flash`：图片 URL 多模态；不支持音频/视频
                multimodal: crate::provider::MultimodalCapabilities {
                    image: true,
                    audio: false,
                    video: false,
                    file_upload: false,
                },
            },
        })
    }

    /// 构造 Agnes 请求 body：公共字段 + Thinking 开关 + `max_tokens` 字段名校正 + extras
    fn build_body(&self, req: &ChatRequest) -> Value {
        let mut body = build_common_body(
            &req.messages,
            &req.tools,
            req.tool_choice,
            req.temperature,
            req.max_tokens,
            self.model.as_str(),
        );
        // Agnes 使用标准 OpenAI `max_tokens`（非共享底座写入的 `max_completion_tokens`）
        if let Some(m) = req.max_tokens {
            if let Some(obj) = body.as_object_mut() {
                obj.remove("max_completion_tokens");
                obj.insert("max_tokens".to_string(), json!(m));
            }
        }
        // Thinking 模式：chat_template_kwargs.enable_thinking（effort 不受支持，忽略）
        body["chat_template_kwargs"] = json!({
            "enable_thinking": req.thinking.enabled
        });
        // 调用方 extras（最后写入，可覆盖厂商默认）
        for (k, v) in &req.extra {
            body[k] = v.clone();
        }
        body
    }
}

#[async_trait]
impl LLMProvider for AgnesProvider {
    fn id(&self) -> ProviderId {
        self.model.provider_id()
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
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

    /// 验证 Agnes 特有字段映射：model / max_tokens（非 max_completion_tokens）/
    /// chat_template_kwargs.enable_thinking / 多模态图片序列化。
    #[test]
    fn build_body_maps_agnes_specific_fields() {
        let provider = AgnesProvider::new(
            AgnesModel::V25Flash,
            AgnesConfig::new("test-key"),
        )
        .expect("create provider");

        let mut req = ChatRequest::simple(
            crate::provider::MessageContent::multimodal(vec![
                crate::provider::ContentPart::text("描述这张图片"),
                crate::provider::ContentPart::image(crate::provider::MediaSource::Url {
                    url: "https://example.com/image.jpg".into(),
                }),
            ]),
        );
        req.max_tokens = Some(2048);

        let body = provider.build_body(&req);

        assert_eq!(body["model"], json!("agnes-2.5-flash"));
        // 使用标准 max_tokens，而非共享底座的 max_completion_tokens
        assert_eq!(body["max_tokens"], json!(2048));
        assert!(body.get("max_completion_tokens").is_none());
        // Thinking 模式默认开启（ThinkingConfig 默认 enabled=true）
        assert_eq!(
            body["chat_template_kwargs"],
            json!({ "enable_thinking": true })
        );
        // 多模态内容：文本 + image_url 数组合并
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[0], json!({"type": "text", "text": "描述这张图片"}));
        assert_eq!(
            content[1],
            json!({"type": "image_url", "image_url": {"url": "https://example.com/image.jpg"}})
        );
    }

    /// 关闭 Thinking 时透传 enable_thinking=false
    #[test]
    fn build_body_disables_thinking() {
        let provider = AgnesProvider::new(
            AgnesModel::V25Flash,
            AgnesConfig::new("test-key"),
        )
        .expect("create provider");

        let mut req = ChatRequest::simple("hi");
        req.thinking.enabled = false;

        let body = provider.build_body(&req);
        assert_eq!(
            body["chat_template_kwargs"],
            json!({ "enable_thinking": false })
        );
    }
}
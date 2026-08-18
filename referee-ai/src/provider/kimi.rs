//! Moonshot Kimi 适配器（OpenAI 兼容协议）
//!
//! - BASE_URL: `https://api.moonshot.cn/v1`
//! - 模型：`kimi-k3`（旗舰多模态推理模型）
//! - 上下文窗口：1M（1048576）/ 最大输出：1M（思考模式下）
//! - 常驻思考（Preserved Thinking 始终开启，无开关）：请求顶层
//!   `reasoning_effort = low | high | max`（默认 max，仅工作量档位）
//! - 推理输出：`message.reasoning_content` / `delta.reasoning_content`
//!   流式分别返回 `reasoning_content` 与 `content` 增量
//! - 多模态：视觉消息 content 为对象数组，图片走 `image_url`（URL 或 base64 `data:`）
//! - 错误：JSON `{error:{type,message}}`；400/401/429/500 等码归一即既有底座语义
//!
//! 本适配器复用 [`crate::provider::openai_compat::OpenAiCompatClient`] 共享底座，
//! 仅在 build_body 组装 Kimi 特有字段（`reasoning_effort`）。输出长度沿用共享底座
//! 的 `max_completion_tokens`（官方字段，默认 131072）。

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{json, Value};

use crate::provider::openai_compat::{build_common_body, OpenAiCompatClient, OpenAiCompatConfig};
use crate::provider::{
    ChatRequest, ChatResponse, LLMProvider, LlmError, ModelSpec, ProviderCapabilities, ProviderId,
    ReasoningEffort, RetryPolicy, StreamChunk,
};

/// Moonshot 厂商标识前缀
pub const MOONSHOT_VENDOR: &str = "moonshot";

/// 预定义 ProviderId
pub mod ids {
    use crate::provider::ProviderId;

    /// `kimi-k3`：旗舰多模态推理模型
    pub const MOONSHOT_KIMI_K3: ProviderId = ProviderId::new("moonshot/kimi-k3");
}

/// Kimi 默认 BASE_URL（OpenAI 兼容协议）
pub const DEFAULT_BASE_URL: &str = "https://api.moonshot.cn/v1";

/// Kimi 上下文窗口 token 数（1M）
pub const CONTEXT_WINDOW_TOKENS: usize = 1_048_576;

/// Kimi 单次响应最大输出 token 数（思考模式下 1M）
pub const MAX_OUTPUT_TOKENS: usize = 1_048_576;

/// Moonshot Kimi 模型枚举（决定 model 字段与 ProviderId）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KimiModel {
    /// `kimi-k3`：文本 + 图片（URL/base64）+ 常驻思考 + 工具调用
    K3,
}

impl KimiModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::K3 => "kimi-k3",
        }
    }

    pub fn provider_id(&self) -> ProviderId {
        match self {
            Self::K3 => ids::MOONSHOT_KIMI_K3,
        }
    }

    /// 模型规模规格（上下文窗口 1M / 最大输出 1M）
    pub fn spec(&self) -> ModelSpec {
        match self {
            Self::K3 => ModelSpec {
                context_window_tokens: CONTEXT_WINDOW_TOKENS,
                max_output_tokens: MAX_OUTPUT_TOKENS,
            },
        }
    }
}

/// Kimi 适配器构造配置
pub struct KimiConfig {
    /// API Key（环境变量 `MOONSHOT_API_KEY`）
    pub api_key: String,
    /// 覆盖默认 BASE_URL（自部署 / 代理）
    pub base_url: Option<String>,
    /// 单次请求总超时（含重试）
    pub timeout: Duration,
    /// 重试策略
    pub retry: RetryPolicy,
}

impl KimiConfig {
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

/// Kimi 适配器
pub struct KimiProvider {
    client: OpenAiCompatClient,
    model: KimiModel,
    capabilities: ProviderCapabilities,
}

impl KimiProvider {
    pub fn new(model: KimiModel, cfg: KimiConfig) -> Result<Self, LlmError> {
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
                // `kimi-k3`：图片 URL/base64 多模态；原生支持视频但文件上传流程按需拓展
                multimodal: crate::provider::MultimodalCapabilities {
                    image: true,
                    audio: false,
                    video: false,
                    file_upload: false,
                },
            },
        })
    }

    /// 构造 Kimi 请求 body：公共字段 + `reasoning_effort`（顶层）+ 调用方 extras
    ///
    /// Kimi 常驻思考（无开关），`thinking.enabled` 不参与；仅透传 `thinking.effort`
    /// 到 `reasoning_effort`（low/high/max）。effort 为 None 时不发送（厂商默认 max）。
    fn build_body(&self, req: &ChatRequest) -> Value {
        let mut body = build_common_body(
            &req.messages,
            &req.tools,
            req.tool_choice,
            req.temperature,
            req.max_tokens,
            self.model.as_str(),
        );
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
impl LLMProvider for KimiProvider {
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

    /// 验证 Kimi 特有字段映射：model / reasoning_effort 透传 / 多模态图片数组。
    #[test]
    fn build_body_maps_kimi_specific_fields() {
        let provider = KimiProvider::new(KimiModel::K3, KimiConfig::new("test-key"))
            .expect("create provider");

        let mut req = ChatRequest::simple(MessageContent::multimodal(vec![
            ContentPart::image(MediaSource::Url {
                url: "https://example.com/image.jpg".into(),
            }),
            ContentPart::text("描述这张图片"),
        ]));
        req.thinking = ThinkingConfig {
            enabled: true,
            effort: Some(ReasoningEffort::Max),
        };

        let body = provider.build_body(&req);

        assert_eq!(body["model"], json!("kimi-k3"));
        assert_eq!(body["reasoning_effort"], json!("max"));
        // 多模态内容：image_url + text 数组合并
        let content = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(
            content[0],
            json!({"type": "image_url", "image_url": {"url": "https://example.com/image.jpg"}})
        );
        assert_eq!(content[1], json!({"type": "text", "text": "描述这张图片"}));
    }

    /// effort 为 None 时不发送 reasoning_effort（由厂商取默认 max）
    #[test]
    fn build_body_omits_effort_when_none() {
        let provider = KimiProvider::new(KimiModel::K3, KimiConfig::new("test-key"))
            .expect("create provider");

        let mut req = ChatRequest::simple("hi");
        req.thinking.effort = None;

        let body = provider.build_body(&req);
        assert!(body.get("reasoning_effort").is_none());
    }
}
//! 通用 OpenAI 兼容厂商适配器 — 用户自填 base_url / api_key / model
//!
//! 适用于任何适配 **OpenAI Chat Completions API** 风格的服务：各云厂商中转、
//! 本地推理网关（vLLM / ollama / LM Studio）、自部署模型等。用户只需提供：
//!
//! - `base_url`：服务根地址（如 `https://api.example.com/v1`，请求打到 `{base_url}/chat/completions`）
//! - `api_key`：服务密钥（无需鉴权的本地服务可填空串）
//! - `model`：模型 ID（如 `gpt-4o-mini` / `deepseek-chat` / 本地模型名）
//!
//! 复用 [`crate::provider::openai_compat`] 共享底座（HTTP / 错误归一 / 重试 /
//! 流式 / SSE），**不发送厂商特殊字段**（如 `thinking` / `reasoning_effort`），
//! 仅输出公共兼容字段 + 调用方 `req.extra` 透传，最大程度兼容任意 OpenAI 风格服务。
//!
//! 能力声明与模型规格按**通用安全默认值**给出（并行工具 / system / 流式 / usage
//! 均开启；纯文本多模态关闭；上下文 128K / 输出 8K），可用 builder 方法按需覆盖——
//! 因为通用厂商无法获知模型真实参数，上层预算治理依赖此处值的准确性，务必核对。

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::Value;

use crate::provider::openai_compat::{build_common_body, OpenAiCompatClient, OpenAiCompatConfig};
use crate::provider::{
    ChatRequest, ChatResponse, LLMProvider, LlmError, ModelSpec, ProviderCapabilities, ProviderId,
    RetryPolicy, StreamChunk,
};

/// 通用 OpenAI 兼容厂商标识前缀
pub const VENDOR: &str = "openai";

/// 通用安全默认：上下文窗口 128K
pub const DEFAULT_CONTEXT_WINDOW_TOKENS: usize = 128 * 1024;

/// 通用安全默认：单次响应最大输出 8K
pub const DEFAULT_MAX_OUTPUT_TOKENS: usize = 8 * 1024;

/// 构造通用安全默认模型规格（通用厂商无法获知真实参数，务必按实际模型覆盖）
fn default_model_spec() -> ModelSpec {
    ModelSpec {
        context_window_tokens: DEFAULT_CONTEXT_WINDOW_TOKENS,
        max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
    }
}

/// 通用 OpenAI 兼容厂商构造配置
pub struct OpenAiConfig {
    /// 服务根地址（请求打到 `{base_url}/chat/completions`）
    pub base_url: String,
    /// 鉴权密钥（无需鉴权的本地服务可填空串）
    pub api_key: String,
    /// 模型 ID（服务端实际接受的模型名）
    pub model: String,
    /// 单次请求总超时（含重试）
    pub timeout: Duration,
    /// 重试策略
    pub retry: RetryPolicy,
    /// 能力声明（默认通用安全值，可按厂商能力覆盖）
    pub capabilities: ProviderCapabilities,
    /// 模型规模规格（默认通用安全值，按实际模型覆盖）
    pub model_spec: ModelSpec,
}

impl OpenAiConfig {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            model: model.into(),
            timeout: Duration::from_secs(120),
            retry: RetryPolicy::default(),
            capabilities: ProviderCapabilities {
                parallel_tool_calls: true,
                system_role: true,
                streaming: true,
                usage_reported: true,
                multimodal: crate::provider::MultimodalCapabilities::NONE,
            },
            model_spec: default_model_spec(),
        }
    }

    pub fn with_timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }

    pub fn with_retry(mut self, r: RetryPolicy) -> Self {
        self.retry = r;
        self
    }

    pub fn with_capabilities(mut self, c: ProviderCapabilities) -> Self {
        self.capabilities = c;
        self
    }

    pub fn with_model_spec(mut self, s: ModelSpec) -> Self {
        self.model_spec = s;
        self
    }
}

/// 通用 OpenAI 兼容厂商适配器
pub struct OpenAiProvider {
    client: OpenAiCompatClient,
    model: String,
    capabilities: ProviderCapabilities,
    model_spec: ModelSpec,
}

impl OpenAiProvider {
    pub fn new(cfg: OpenAiConfig) -> Result<Self, LlmError> {
        let client = OpenAiCompatClient::new(OpenAiCompatConfig {
            base_url: cfg.base_url,
            api_key: cfg.api_key,
            timeout: cfg.timeout,
            retry: cfg.retry,
            extra_headers: Vec::new(),
        })?;
        Ok(Self {
            client,
            model: cfg.model,
            capabilities: cfg.capabilities,
            model_spec: cfg.model_spec,
        })
    }

    /// 构造请求 body：仅公共兼容字段 + 调用方 extras（不发送厂商特殊字段）
    fn build_body(&self, req: &ChatRequest) -> Value {
        let mut body = build_common_body(
            &req.messages,
            &req.tools,
            req.tool_choice,
            req.temperature,
            req.max_tokens,
            &self.model,
        );
        // 调用方 extras（最后写入，可覆盖默认）
        for (k, v) in &req.extra {
            body[k] = v.clone();
        }
        body
    }
}

#[async_trait]
impl LLMProvider for OpenAiProvider {
    fn id(&self) -> ProviderId {
        ProviderId::owned(format!("{VENDOR}/{}", self.model))
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }

    fn model_spec(&self) -> ModelSpec {
        self.model_spec
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

    /// ProviderId 由模型名推导（`openai/{model}`），可注册进 ProviderRegistry。
    #[test]
    fn provider_id_derives_from_model() {
        let p = OpenAiProvider::new(OpenAiConfig::new(
            "https://api.example.com/v1",
            "key",
            "my-model",
        ))
        .expect("create");
        assert_eq!(p.id().as_str(), "openai/my-model");
        assert_eq!(p.model_spec().context_window_tokens, DEFAULT_CONTEXT_WINDOW_TOKENS);
    }

    /// 通用厂商不发送厂商特殊字段（thinking），仅输出公共兼容字段。
    #[test]
    fn build_body_is_generic_openai_compatible() {
        let p = OpenAiProvider::new(OpenAiConfig::new(
            "https://api.example.com/v1",
            "key",
            "gpt-4o-mini",
        ))
        .expect("create");

        let mut req = ChatRequest::simple("hi");
        req.temperature = Some(0.7);
        req.max_tokens = Some(1024);
        let body = p.build_body(&req);

        assert_eq!(body["model"], serde_json::json!("gpt-4o-mini"));
        assert_eq!(body["messages"][0]["content"], serde_json::json!("hi"));
        let t = body["temperature"].as_f64().unwrap();
        assert!((t - 0.7f64).abs() < 1e-4, "temperature={t}");
        assert_eq!(body["max_completion_tokens"], serde_json::json!(1024));
        // 通用厂商不写 thinking（避免部分 OpenAI 兼容服务 400）
        assert!(body.get("thinking").is_none());
        // stream 字段由客户端在发送前填入，build 阶段不含
        assert!(body.get("stream").is_none());
    }

    /// 调用方 extras 透传至 body 顶层。
    #[test]
    fn build_body_passes_through_extras() {
        let p = OpenAiProvider::new(OpenAiConfig::new(
            "https://api.example.com/v1",
            "key",
            "m",
        ))
        .expect("create");

        let mut req = ChatRequest::simple("hi");
        req.extra.insert("max_completion_tokens".into(), 99.into());
        let body = p.build_body(&req);
        assert_eq!(body["max_completion_tokens"], serde_json::json!(99));
    }
}
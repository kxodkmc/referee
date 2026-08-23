//! OpenRouter 适配器 — OpenAI 兼容协议的深度适配
//!
//! [OpenRouter](https://openrouter.ai) 是聚合多家上游模型提供商的 OpenAI 兼容网关。
//! 用户**自行填写** base_url / api_key / model ID（无预设模型），模型 ID 形如
//! `openai/gpt-4o`、`~openai/gpt-latest`、`org/model:free`（含 `/` 等字符，原样透传）。
//!
//! 本适配器基于共享底座 [`crate::provider::openai_compat::OpenAiCompatClient`]，仅在
//! **请求头**上深度适配 OpenRouter：
//!
//! - `HTTP-Referer`（站点 URL）与 `X-OpenRouter-Title`（站点名）：OpenRouter 登录排名
//!   用，可选。构造时传入 `site_url` / `app_title` 即注入，缺省则不发送（保持普通
//!   OpenAI 用法兼容）。
//!
//! 错误归一复用底座既有语义，覆盖 OpenRouter 文档常见状态码：
//! - 401 → `Auth`（API Key 无效）
//! - 402 → `InsufficientBalance`（额度不足，确定性错误，不重试）
//! - 429 → `RateLimited`（尊重 `Retry-After` 头）
//! - 502 / 503 → `Server`（上游 provider 不可用，可重试，受 [`RetryPolicy`] 约束）
//! - 400 → `BadRequest`（参数错误）
//! - 403 归 `Auth` 兜底（OpenRouter 的 guardrail/moderation 拦截也走此分支，见注释）
//!
//! ## 注意
//! 能力声明与模型规格按**通用安全默认**给出：OpenRouter 网关背后模型千差万别，
//! 上层预算治理依赖 [`ModelSpec`] 精度，务必用 `with_model_spec` 按实际模型覆盖。

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::Value;

use crate::provider::openai_compat::{build_common_body, OpenAiCompatClient, OpenAiCompatConfig};
use crate::provider::{
    ChatRequest, ChatResponse, LLMProvider, LlmError, ModelSpec, ProviderCapabilities, ProviderId,
    RetryPolicy, StreamChunk,
};

/// OpenRouter 厂商标识前缀
pub const VENDOR: &str = "openrouter";

/// OpenRouter 默认 BASE_URL
pub const DEFAULT_BASE_URL: &str = "https://openrouter.ai/api/v1";

/// OpenRouter 适配器构造配置
///
/// 用户只需提供 `api_key` 与 `model`；`base_url` 默认 OpenRouter 官方网关，
/// 可覆盖为自建中转。站点头项为可选。
pub struct OpenRouterConfig {
    /// 服务根地址（默认 `https://openrouter.ai/api/v1`，请求打到 `{base_url}/chat/completions`）
    pub base_url: String,
    /// OpenRouter API Key
    pub api_key: String,
    /// 模型 ID（用户自行填写，如 `openai/gpt-4o` / `~openai/gpt-latest` / 本地模型名，原样透传）
    pub model: String,
    /// 站点 URL（可选，注入 `HTTP-Referer` 请求头，用于 OpenRouter 登录排名）
    pub site_url: Option<String>,
    /// 站点名（可选，注入 `X-OpenRouter-Title` 请求头，用于 OpenRouter 登录排名）
    pub app_title: Option<String>,
    /// 单次请求总超时（含重试）
    pub timeout: Duration,
    /// 重试策略
    pub retry: RetryPolicy,
    /// 能力声明（默认通用安全值，可按厂商能力覆盖）
    pub capabilities: ProviderCapabilities,
    /// 模型规模规格（默认通用安全值，按实际模型覆盖）
    pub model_spec: ModelSpec,
}

/// 构造通用安全默认模型规格（OpenRouter 网关背后模型各异，务必按实际模型覆盖）
fn default_model_spec() -> ModelSpec {
    ModelSpec {
        context_window_tokens: 128 * 1024,
        max_output_tokens: 8 * 1024,
    }
}

impl OpenRouterConfig {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: api_key.into(),
            model: model.into(),
            site_url: None,
            app_title: None,
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

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// 站点 URL → `HTTP-Referer`（OpenRouter 登录排名用）
    pub fn with_site_url(mut self, url: impl Into<String>) -> Self {
        self.site_url = Some(url.into());
        self
    }

    /// 站点名 → `X-OpenRouter-Title`（OpenRouter 登录排名用）
    pub fn with_app_title(mut self, title: impl Into<String>) -> Self {
        self.app_title = Some(title.into());
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

    pub fn with_capabilities(mut self, c: ProviderCapabilities) -> Self {
        self.capabilities = c;
        self
    }

    pub fn with_model_spec(mut self, s: ModelSpec) -> Self {
        self.model_spec = s;
        self
    }
}

/// OpenRouter 适配器
pub struct OpenRouterProvider {
    client: OpenAiCompatClient,
    model: String,
    capabilities: ProviderCapabilities,
    model_spec: ModelSpec,
}

impl OpenRouterProvider {
    pub fn new(cfg: OpenRouterConfig) -> Result<Self, LlmError> {
        let client = OpenAiCompatClient::new(OpenAiCompatConfig {
            base_url: cfg.base_url,
            api_key: cfg.api_key,
            timeout: cfg.timeout,
            retry: cfg.retry,
            extra_headers: extra_headers(cfg.site_url.as_deref(), cfg.app_title.as_deref()),
        })?;
        Ok(Self {
            client,
            model: cfg.model,
            capabilities: cfg.capabilities,
            model_spec: cfg.model_spec,
        })
    }

    /// 构造请求 body：仅公共 OpenAI 兼容字段 + 调用方 extras（不发送厂商特殊字段，
    /// 因网关背后模型各异，`thinking` 等字段对部分模型会 400）
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

/// 由站点头构造 OpenRouter 附加请求头（仅注入非空项）
fn extra_headers(site_url: Option<&str>, app_title: Option<&str>) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> = Vec::new();
    if let Some(url) = site_url.filter(|s| !s.trim().is_empty()) {
        headers.push(("HTTP-Referer".to_string(), url.to_string()));
    }
    if let Some(title) = app_title.filter(|s| !s.trim().is_empty()) {
        headers.push(("X-OpenRouter-Title".to_string(), title.to_string()));
    }
    if headers.is_empty() {
        Vec::new()
    } else {
        headers
    }
}

#[async_trait]
impl LLMProvider for OpenRouterProvider {
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

    /// ProviderId 由模型名推导（`openrouter/{model}`），可注册进 ProviderRegistry。
    #[test]
    fn provider_id_derives_from_model() {
        let p = OpenRouterProvider::new(OpenRouterConfig::new("key", "openai/gpt-4o"))
            .expect("create");
        assert_eq!(p.id().as_str(), "openrouter/openai/gpt-4o");
    }

    /// 请求 body 为通用 OpenAI 兼容：仅公共字段，不发送 thinking 等厂商特殊字段。
    #[test]
    fn build_body_is_generic_openai_compatible() {
        let p = OpenRouterProvider::new(OpenRouterConfig::new("key", "~openai/gpt-latest"))
            .expect("create");

        let mut req = ChatRequest::simple("hi");
        req.max_tokens = Some(1024);
        let body = p.build_body(&req);

        assert_eq!(body["model"], serde_json::json!("~openai/gpt-latest"));
        assert_eq!(body["messages"][0]["content"], serde_json::json!("hi"));
        assert_eq!(body["max_completion_tokens"], serde_json::json!(1024));
        // 通用网关不写 thinking（避免部分模型 400）
        assert!(body.get("thinking").is_none());
        // stream 字段由客户端在发送前填入，build 阶段不含
        assert!(body.get("stream").is_none());
    }

    /// 站点头映射为 OpenRouter 附加请求头；缺省为空。
    #[test]
    fn headers_map_site_url_and_app_title() {
        // 两者都配置
        let h = extra_headers(Some("https://example.com"), Some("Referee"));
        assert!(h.contains(&("HTTP-Referer".to_string(), "https://example.com".to_string())));
        assert!(h.contains(&("X-OpenRouter-Title".to_string(), "Referee".to_string())));

        // 缺省 / 空串 → 空
        assert!(extra_headers(None, None).is_empty());
        assert!(extra_headers(Some(""), Some("  ")).is_empty());
    }

    /// 未配站点头时客户端附加头为空（构造原样透传，保持普通 OpenAI 用法兼容）。
    #[test]
    fn no_site_headers_by_default() {
        let client = OpenAiCompatClient::new(OpenAiCompatConfig {
            base_url: DEFAULT_BASE_URL.to_string(),
            api_key: "key".into(),
            timeout: Duration::from_secs(120),
            retry: RetryPolicy::default(),
            extra_headers: extra_headers(None, None),
        })
        .expect("create");
        assert!(client.header_map().is_empty());
    }
}
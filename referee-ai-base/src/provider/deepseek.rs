//! DeepSeek 适配器（OpenAI 兼容协议）
//!
//! - BASE_URL: `https://api.deepseek.com`
//! - 模型：`deepseek-v4-flash` / `deepseek-v4-pro`
//! - 思考开关：`extra_body.thinking.type = enabled | disabled`（默认开启）
//! - 思考强度：`reasoning_effort = low | high | max`（默认 high）
//! - 推理输出：`message.reasoning_content` / `delta.reasoning_content`
//! - usage 扩展：`prompt_cache_hit_tokens` / `prompt_cache_miss_tokens`（硬盘缓存）
//! - 多轮工具调用：assistant 消息必须完整回传 `reasoning_content`
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
    ChatRequest, ChatResponse, LLMProvider, LlmError, ProviderCapabilities, ProviderId,
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
}

/// DeepSeek 默认 BASE_URL
pub const DEFAULT_BASE_URL: &str = "https://api.deepseek.com";

/// DeepSeek 单次响应最大输出 token 数
pub const MAX_OUTPUT_TOKENS: usize = 384 * 1024;

/// DeepSeek 模型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeepSeekModel {
    /// `deepseek-v4-flash`：低成本快速
    V4Flash,
    /// `deepseek-v4-pro`：高能力
    V4Pro,
}

impl DeepSeekModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::V4Flash => "deepseek-v4-flash",
            Self::V4Pro => "deepseek-v4-pro",
        }
    }

    pub fn provider_id(&self) -> ProviderId {
        match self {
            Self::V4Flash => ids::DEEPSEEK_V4_FLASH,
            Self::V4Pro => ids::DEEPSEEK_V4_PRO,
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

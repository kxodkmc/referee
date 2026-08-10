//! Xiaomi MiMo 适配器（OpenAI 兼容协议）
//!
//! - BASE_URL: `https://api.xiaomimimo.com/v1`
//! - 模型：`mimo-v2.5-pro` / `mimo-v2.5`
//! - 思考开关：`extra_body.thinking.type = enabled | disabled`（默认开启）
//! - 推理输出：`message.reasoning_content` / `delta.reasoning_content`
//! - usage 扩展：`completion_tokens_details.reasoning_tokens`
//! - 多轮工具调用：assistant 消息必须完整回传 `reasoning_content`
//!
//! 多模态（音频/视频/图片，仅 `mimo-v2.5`）在后续 Phase 通过扩展
//! [`crate::provider::MessageContent`] 启用，本适配器无需改动。

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::{json, Value};

use crate::provider::openai_compat::{build_common_body, OpenAiCompatClient, OpenAiCompatConfig};
use crate::provider::{
    ChatRequest, ChatResponse, LLMProvider, LlmError, ProviderCapabilities, ProviderId,
    RetryPolicy, StreamChunk,
};

/// MiMo 厂商标识前缀（与 model 拼接成完整 ProviderId）
pub const XIAOMI_VENDOR: &str = "xiaomi";

/// 预定义 ProviderId
pub mod ids {
    use crate::provider::ProviderId;

    /// `mimo-v2.5-pro`：文本生成 / 深度思考 / 函数调用 / 结构化输出 / 联网搜索
    pub const MIMO_V25_PRO: ProviderId = ProviderId::new("xiaomi/mimo-v2.5-pro");
    /// `mimo-v2.5`：在 pro 基础上增加全模态理解（多模态在后续 Phase 启用）
    pub const MIMO_V25: ProviderId = ProviderId::new("xiaomi/mimo-v2.5");
}

/// MiMo 默认 BASE_URL（OpenAI 兼容协议）
pub const DEFAULT_BASE_URL: &str = "https://api.xiaomimimo.com/v1";

/// MiMo 单次响应最大输出 token 数（pro 与 v2.5 相同）
pub const MAX_OUTPUT_TOKENS: usize = 128 * 1024;

/// MiMo 模型枚举（决定 model 字段与 ProviderId）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XiaomiModel {
    /// `mimo-v2.5-pro`：纯文本 + 深度思考 + 工具调用
    MimoV25Pro,
    /// `mimo-v2.5`：pro 能力 + 全模态（多模态后续 Phase 启用）
    MimoV25,
}

impl XiaomiModel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MimoV25Pro => "mimo-v2.5-pro",
            Self::MimoV25 => "mimo-v2.5",
        }
    }

    pub fn provider_id(&self) -> ProviderId {
        match self {
            Self::MimoV25Pro => ids::MIMO_V25_PRO,
            Self::MimoV25 => ids::MIMO_V25,
        }
    }
}

/// MiMo 适配器构造配置
pub struct XiaomiConfig {
    /// API Key（环境变量 `MIMO_API_KEY`）
    pub api_key: String,
    /// 覆盖默认 BASE_URL（自部署 / 代理场景）
    pub base_url: Option<String>,
    /// 单次请求总超时（含重试）
    pub timeout: Duration,
    /// 重试策略（默认 3 次）
    pub retry: RetryPolicy,
}

impl XiaomiConfig {
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

/// Xiaomi MiMo 适配器
pub struct XiaomiProvider {
    client: OpenAiCompatClient,
    model: XiaomiModel,
    capabilities: ProviderCapabilities,
}

impl XiaomiProvider {
    pub fn new(model: XiaomiModel, cfg: XiaomiConfig) -> Result<Self, LlmError> {
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

    /// 构造 MiMo 请求 body：公共字段 + `thinking` 开关 + 调用方 extras
    fn build_body(&self, req: &ChatRequest) -> Value {
        let mut body = build_common_body(
            &req.messages,
            &req.tools,
            req.tool_choice,
            req.temperature,
            req.max_tokens,
            self.model.as_str(),
        );
        // MiMo 思考开关：thinking.type = enabled | disabled
        body["thinking"] = json!({
            "type": if req.thinking.enabled { "enabled" } else { "disabled" }
        });
        // 调用方 extras（最后写入，可覆盖厂商默认）
        for (k, v) in &req.extra {
            body[k] = v.clone();
        }
        body
    }
}

#[async_trait]
impl LLMProvider for XiaomiProvider {
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

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
//!
//! ## 订阅计划接入（Token Plan 等）
//! Token Plan 是订阅制**独立计费身份**，协议行为与按量付费 MiMo 完全一致，
//! 仅接入端点与 API Key 不同 —— 属于配置差异而非新提供商，无需新增适配器：
//!
//! ```no_run
//! use referee_agent::provider::xiaomi::{
//!     XiaomiConfig, XiaomiModel, XiaomiProvider, TOKEN_PLAN_BASE_URL_CN,
//! };
//! let provider = XiaomiProvider::new(
//!     XiaomiModel::MimoV25Pro,
//!     XiaomiConfig::new("tp-xxxxx") // Token Plan 专属 Key（tp- 前缀）
//!         .with_plan("tokenplan") // 计费身份前缀（任意名称，如 "codeplan"）
//!         .with_base_url(TOKEN_PLAN_BASE_URL_CN), // 切换订阅计划集群
//! )
//! .expect("provider creation");
//! ```
//!
//! 401 认证失败可能因混用订阅计划与按量付费的 API Key 导致，请核对专属 Key。

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

/// MiMo 默认 BASE_URL（OpenAI 兼容协议，按量付费）
pub const DEFAULT_BASE_URL: &str = "https://api.xiaomimimo.com/v1";

/// Token Plan 订阅计划接入端点（中国集群）
pub const TOKEN_PLAN_BASE_URL_CN: &str = "https://token-plan-cn.xiaomimimo.com/v1";
/// Token Plan 订阅计划接入端点（新加坡集群）
pub const TOKEN_PLAN_BASE_URL_SGP: &str = "https://token-plan-sgp.xiaomimimo.com/v1";
/// Token Plan 订阅计划接入端点（欧洲集群）
pub const TOKEN_PLAN_BASE_URL_AMS: &str = "https://token-plan-ams.xiaomimimo.com/v1";

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
    /// 覆盖默认 BASE_URL（自部署 / 代理 / 订阅计划集群）
    pub base_url: Option<String>,
    /// 订阅计划计费身份（如 `"tokenplan"` / `"codeplan"`），决定 ProviderId 前缀；
    /// `None` 时使用默认按量付费身份 `"xiaomi"`。协议行为不受影响。
    pub plan: Option<String>,
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
            plan: None,
            timeout: Duration::from_secs(120),
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }

    /// 设置订阅计划计费身份（ProviderId 前缀，如 `"tokenplan"`）
    pub fn with_plan(mut self, plan: impl Into<String>) -> Self {
        self.plan = Some(plan.into());
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
    /// 最终身份：默认 `xiaomi/{model}`；配置订阅计划时为 `{plan}/{model}`
    provider_id: ProviderId,
}

impl XiaomiProvider {
    pub fn new(model: XiaomiModel, cfg: XiaomiConfig) -> Result<Self, LlmError> {
        let client = OpenAiCompatClient::new(OpenAiCompatConfig {
            base_url: cfg.base_url.unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
            api_key: cfg.api_key,
            timeout: cfg.timeout,
            retry: cfg.retry,
        })?;
        // 订阅计划身份：有 plan 时 ProviderId = "{plan}/{model}"（动态），否则默认静态标识
        let provider_id = match &cfg.plan {
            Some(plan) => ProviderId::owned(format!("{plan}/{}", model.as_str())),
            None => model.provider_id(),
        };
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
            provider_id,
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
        self.provider_id.clone()
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

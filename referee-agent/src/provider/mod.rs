//! 厂商抽象层 — Agent Runtime 的唯一 I/O 边界
//!
//! 所有上层模块（session / tool / memory / prompt）只面对 [`LLMProvider`] trait
//! 与本模块定义的纯数据类型，**不写厂商分支**。厂商差异通过：
//! - [`ProviderCapabilities`]：能力声明驱动上层自动降级（如不支持并行工具 → 串行调度）
//! - [`ChatRequest::extra`] / [`Message::reasoning_content`] / [`ThinkingConfig`]：
//!   厂商特殊参数经类型化扩展点透传，避免散落字符串键
//!
//! ## 设计约束（继承 AGENT_RUNTIME_PLAN §2 横切约束）
//! - **数据/行为分离**：本模块仅定义纯数据载体（请求/响应/错误），不持逻辑句柄
//! - **背压**：SSE 流式增量处理，不缓存整段响应；上层调用方自行决定是否背压
//! - **错误归一**：超时/限流/4xx/5xx/网络 全部映射 [`LlmError`]；重试仅对
//!   `RateLimited / Server / Network` 三类，指数退避且受 [`RetryPolicy`] 上限约束
//! - **可观测**：`tracing` 全链路 span；上层 P6 注入 metrics（调用数/延迟/token）
//!
//! ## 可拓展性
//! - 新增 OpenAI 兼容厂商 = 新增 `provider/<vendor>.rs` 配置 [`crate::provider::openai_compat::OpenAiCompatClient`]
//! - 新增非兼容厂商（Anthropic / Responses）= 新增独立 HTTP/JSON 映射实现
//! - 多模态：[`MessageContent`] 当前仅 `Text` 变体；后续 Phase 增加 `Multimodal(Vec<ContentPart>)`
//!   变体即可，不破坏既有调用方（[`MessageContent::as_text`] 优雅降级）

pub mod openai_compat;

#[cfg(feature = "deepseek")]
pub mod deepseek;
#[cfg(feature = "xiaomi")]
pub mod xiaomi;

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ───────────────────────────────────────────────
// 厂商标识与能力声明
// ───────────────────────────────────────────────

/// 厂商 + 模型的唯一标识（路由 / 日志 / 缓存键）
///
/// 设计为 `&'static str` 包装而非 enum：新增厂商无需修改本类型，
/// 各适配器以 `const` 形式导出自己的标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProviderId(&'static str);

impl ProviderId {
    pub const fn new(v: &'static str) -> Self {
        Self(v)
    }
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for ProviderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// 厂商能力声明 — 上层据此自动降级，不写死厂商分支
///
/// 例：`parallel_tool_calls=false` → 调度层（P2）将工具调用串行化；
/// `system_role=false` → 提示词层（P5）将 system 前缀到首条 user。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// 是否支持并行工具调用；不支持时调度层自动串行
    pub parallel_tool_calls: bool,
    /// 是否原生支持 system role；不支持时由提示词层前缀到首条 user
    pub system_role: bool,
    /// 是否支持流式输出
    pub streaming: bool,
    /// 厂商是否在响应中返回 usage 字段；不返回时走 P6 估算
    pub usage_reported: bool,
    /// 单次响应最大输出 token 数
    pub max_output_tokens: usize,
}

// ───────────────────────────────────────────────
// 请求 / 响应数据类型（纯数据，无逻辑句柄）
// ───────────────────────────────────────────────

/// 角色 — 与 OpenAI Chat 协议对齐
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

/// 消息内容 — 当前仅文本；多模态在后续 Phase 通过新增变体扩展
///
/// 序列化规则：`Text` → JSON 字符串（OpenAI 简写形式）；
/// 未来 `Multimodal` → JSON 数组形式（OpenAI 多模态标准）。
/// 既有调用方通过 [`MessageContent::as_text`] 优雅处理新增变体。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// 纯文本（OpenAI `content: "..."` 简写形式）
    Text(String),
    // 预留：Multimodal(Vec<ContentPart>) — 后续 Phase 启用图像/音频/视频
}

impl MessageContent {
    pub fn text(s: impl Into<String>) -> Self {
        Self::Text(s.into())
    }

    /// 取文本内容；非文本变体返回 None（多模态扩展后仍向后兼容）
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            // 后续变体在此匹配返回 None，保证既有调用方不破坏
        }
    }
}

impl From<String> for MessageContent {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<&str> for MessageContent {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

/// 消息 — 一轮对话的最小单元
///
/// `reasoning_content` 字段对深度思考厂商（MiMo / DeepSeek）必需：
/// 多轮对话中带工具调用时，回传的 assistant 消息必须完整保留
/// `reasoning_content`，否则 API 返回 400。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: MessageContent,
    /// 助手消息的推理内容（深度思考厂商多轮工具调用回传必需）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    /// 助手消息发起的工具调用列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    /// 工具结果消息对应的工具调用 ID（role=Tool 时必需）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

impl Message {
    pub fn user(content: impl Into<MessageContent>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn assistant(content: impl Into<MessageContent>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }

    pub fn system(content: impl Into<MessageContent>) -> Self {
        Self {
            role: Role::System,
            content: content.into(),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
        }
    }
}

/// 工具声明 — JSON Schema 描述，各厂商适配器负责转译为本厂商格式
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDeclaration {
    pub name: String,
    pub description: String,
    /// JSON Schema 描述的参数结构
    pub parameters: serde_json::Value,
}

/// 工具调用选择策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ToolChoice {
    /// 由模型决定
    #[default]
    Auto,
    /// 强制不调用工具
    None,
    /// 强制调用工具（任一）
    Required,
}

/// 模型发起的工具调用
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    /// 调用 ID（多轮工具调用回传时匹配 tool_call_id）
    pub id: String,
    /// 工具调用详情
    pub function: ToolCallFunction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallFunction {
    pub name: String,
    /// JSON 字符串形式的参数（厂商协议如此，解析由上层负责）
    pub arguments: String,
}

/// 流式增量中的工具调用片段（按 index 累积）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallDelta {
    /// 在 tool_calls 数组中的索引（流式累积依据）
    pub index: u32,
    pub id: Option<String>,
    pub function: Option<ToolCallFunctionDelta>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolCallFunctionDelta {
    pub name: Option<String>,
    pub arguments: Option<String>,
}

/// 深度思考配置 — MiMo / DeepSeek 共同支持
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThinkingConfig {
    /// 是否开启思考模式（两家厂商默认均开启）
    pub enabled: bool,
    /// 思考强度（仅 DeepSeek 支持；MiMo 收到此字段会被忽略）
    pub effort: Option<ReasoningEffort>,
}

impl Default for ThinkingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            effort: None,
        }
    }
}

/// 推理强度 — 仅 DeepSeek 协议支持
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReasoningEffort {
    Low,
    High,
    Max,
}

/// 调用结束原因
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// 自然停止（达到 EOS 或完成回答）
    Stop,
    /// 达到 max_tokens 上限
    Length,
    /// 模型发起工具调用
    ToolCalls,
    /// 内容过滤
    ContentFilter,
    /// 厂商特定原因（保留原始字符串以便诊断）
    Other(String),
}

impl FinishReason {
    /// 从厂商返回的字符串归一化
    pub fn from_vendor_str(s: &str) -> Self {
        match s {
            "stop" => Self::Stop,
            "length" => Self::Length,
            "tool_calls" | "function_call" => Self::ToolCalls,
            "content_filter" => Self::ContentFilter,
            other => Self::Other(other.to_string()),
        }
    }
}

/// Token 用量 — 各厂商 usage 字段的并集
///
/// - `reasoning_tokens`：MiMo 通过 `completion_tokens_details.reasoning_tokens` 报告
/// - `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens`：DeepSeek 缓存命中指标
///
/// 不支持的厂商对应字段为 None，上层不写厂商分支。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
    pub total_tokens: usize,
    /// 推理 token 数（深度思考厂商）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<usize>,
    /// 输入缓存命中 token 数（DeepSeek）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_hit_tokens: Option<usize>,
    /// 输入缓存未命中 token 数（DeepSeek）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_cache_miss_tokens: Option<usize>,
}

/// 一次性（非流式）响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatResponse {
    /// 厂商返回的响应 ID（用于日志关联）
    pub id: String,
    pub model: String,
    /// 助手的最终消息（含 content / reasoning_content / tool_calls）
    pub message: Message,
    pub finish_reason: FinishReason,
    /// 厂商 usage；`capabilities.usage_reported=false` 时为 None
    pub usage: Option<TokenUsage>,
}

/// 流式增量块
#[derive(Debug, Clone)]
pub enum StreamChunk {
    /// 增量内容（content / reasoning_content / tool_calls 累积）
    Delta {
        #[allow(dead_code)] // 流式累积语义保留；P1 状态机会消费
        content: Option<String>,
        reasoning_content: Option<String>,
        tool_calls: Vec<ToolCallDelta>,
        role: Option<Role>,
    },
    /// 流终止：最终 finish_reason + 累积 usage（若厂商报告）
    Finish {
        finish_reason: FinishReason,
        usage: Option<TokenUsage>,
    },
}

/// 统一调用请求 — 所有适配器面对同一结构
#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDeclaration>,
    pub tool_choice: ToolChoice,
    pub temperature: Option<f32>,
    /// 输出长度上限（OpenAI 协议 `max_completion_tokens`）
    pub max_tokens: Option<usize>,
    pub thinking: ThinkingConfig,
    /// 厂商特定扩展点（透传至请求 body 顶层）
    ///
    /// 例：MiMo 的 `extra_body: {"thinking": {"type": "enabled"}}` 在适配器
    /// 内部填入，调用方一般不直接使用；保留此字段作为逃生通道。
    pub extra: HashMap<String, serde_json::Value>,
}

impl ChatRequest {
    /// 最小请求：单条 user 消息
    pub fn simple(user_content: impl Into<MessageContent>) -> Self {
        Self {
            messages: vec![Message::user(user_content)],
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            temperature: None,
            max_tokens: None,
            thinking: ThinkingConfig::default(),
            extra: HashMap::new(),
        }
    }
}

impl Default for ChatRequest {
    fn default() -> Self {
        Self::simple("")
    }
}

// ───────────────────────────────────────────────
// 错误归一
// ───────────────────────────────────────────────

/// 统一错误类型 — 各厂商 HTTP/协议错误全部归一为此枚举
///
/// 重试策略：仅 `Network / Server / RateLimited` 三类触发重试，
/// 指数退避且受 [`RetryPolicy::max_retries`] 上限约束。
#[derive(Debug, Clone, PartialEq, Error)]
pub enum LlmError {
    #[error("network error: {0}")]
    Network(String),
    #[error("request timed out")]
    Timeout,
    #[error("rate limited; retry after {retry_after:?}")]
    RateLimited { retry_after: Option<Duration> },
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("server error ({status}): {body}")]
    Server { status: u16, body: String },
    #[error("authentication failed")]
    Auth,
    #[error("operation cancelled")]
    Cancelled,
    /// 响应体无法解析为厂商协议结构（JSON 错误或字段缺失）
    #[error("protocol parse error: {0}")]
    Protocol(String),
}

// ───────────────────────────────────────────────
// 重试策略
// ───────────────────────────────────────────────

/// 重试策略 — 仅对可恢复错误（Network / Server / RateLimited）生效
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// 禁用重试（测试用，避免 flaky）
    pub const fn no_retry() -> Self {
        Self {
            max_retries: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    /// 当前错误是否应触发重试
    pub fn is_retryable(err: &LlmError) -> bool {
        matches!(
            err,
            LlmError::Network(_) | LlmError::Server { .. } | LlmError::RateLimited { .. }
        )
    }

    /// 第 `attempt` 次失败后的退避时长（指数退避：initial × 2^attempt，封顶 max）
    pub fn backoff_for(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return self.initial_backoff;
        }
        // 防溢出：限制指数为 10 位
        let exp = attempt.min(10);
        let scale = 1u64 << exp;
        let initial_ms = self.initial_backoff.as_millis() as u64;
        let backoff_ms = initial_ms.saturating_mul(scale);
        let capped = Duration::from_millis(backoff_ms);
        if capped > self.max_backoff {
            self.max_backoff
        } else {
            capped
        }
    }
}

// ───────────────────────────────────────────────
// 厂商 Trait
// ───────────────────────────────────────────────

/// LLM 厂商统一接口 — Agent Runtime 的唯一 I/O 边界
///
/// 实现要求：
/// - `chat` 与 `chat_stream` 在同一输入下语义等价（流式收敛 == 一次性响应）
/// - 厂商错误全部归一为 [`LlmError`]；可恢复错误由内部按 [`RetryPolicy`] 重试
/// - 流式返回 `BoxStream<'static, ...>`：流所有权独立，不借用 `&self`
/// - 实现需 `Send + Sync`：可在多 task 间共享（HTTP client 内部连接池线程安全）
#[async_trait]
pub trait LLMProvider: Send + Sync {
    /// 厂商 + 模型标识
    fn id(&self) -> ProviderId;

    /// 能力声明（决定上层降级行为）
    fn capabilities(&self) -> &ProviderCapabilities;

    /// 一次性调用
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, LlmError>;

    /// 流式调用 — chunk 收敛后必须与 `chat()` 语义等价
    async fn chat_stream(
        &self,
        req: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError>;
}

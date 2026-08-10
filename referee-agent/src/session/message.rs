//! 会话消息类型与 Envelope 编解码
//!
//! 内核不持有业务 payload —— `Envelope.metadata: HashMap<String, String>`
//! 是专为扩展留的数据出口（经 grep 确认内核内部零 `metadata` 调用）。
//! 本模块约定 metadata 键名，将类型化 [`SessionMessage`] 序列化为 JSON
//! 字符串塞入单一键 `"_msg"`，解码时反序列化回来。
//!
//! ## 优先级约定（对应内核三分桶）
//! - `Interrupt`: `priority = 0`（High 桶，保证及时打断 Thinking）
//! - 其他: `priority = 100`（Normal 桶）
//!
//! ## 可拓展性
//! `SessionMessage` 是 `#[serde(tag = "kind")]` enum —— 新增消息类型只需
//! 新增变体 + 对应编解码分支，不破坏既有调用方（`#[serde(default)]` 兜底）。

use referee_core::Envelope;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::provider::{
    ChatResponse, FinishReason, Message, ThinkingConfig, TokenUsage, ToolDeclaration,
};

/// 会话标识（UUID 包装，`Copy + Eq + Hash`，可直接做 DashMap key）
pub type SessionId = Uuid;

/// metadata 键名：完整消息 JSON
const META_MSG: &str = "_msg";

/// 优先级常量（对应内核三分桶：0..=49 High / 50..=149 Normal / >=150 Low）
pub const PRIORITY_INTERRUPT: u8 = 0;
pub const PRIORITY_NORMAL: u8 = 100;

/// 会话消息 — 驱动状态机流转的唯一入参
///
/// Phase 1 实现 `Chat` / `Interrupt`；
/// `ToolResult` / `Resume` / `SubagentDone` 为 P2/P3 预留（编解码已就绪，
/// 状态机暂不处理，收到时返回 `Unhandled` 由上层决定）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionMessage {
    /// 用户发起对话（触发 Idle → Thinking）
    Chat {
        session_id: SessionId,
        #[serde(flatten)]
        payload: ChatPayload,
    },
    /// 中断当前思考（协作取消，High 优先级投递）
    Interrupt { session_id: SessionId },
    /// 工具结果回写（P2：触发 AwaitingCalls → Thinking）
    ToolResult {
        session_id: SessionId,
        turn_id: u64,
        tool_call_id: String,
        /// 工具执行结果（JSON 字符串）
        result: String,
    },
    /// 等待项全部完成，进入下一轮思考（P2/P3 resume 循环）
    Resume { session_id: SessionId, turn_id: u64 },
    /// 子 Agent 完成（P3：写入 Artifact + 通知主 Agent）
    SubagentDone {
        session_id: SessionId,
        turn_id: u64,
        subagent_id: SessionId,
        /// 子 Agent 产出的 artifact ID 列表
        artifact_ids: Vec<String>,
    },
}

/// Chat 消息负载
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatPayload {
    /// 用户消息（将追加到会话 history）
    pub message: Message,
    /// 可选参数覆盖（None 时用 Session 默认配置）
    #[serde(default)]
    pub options: ChatOptions,
}

/// Chat 可选参数 — 覆盖会话级默认值
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatOptions {
    /// 本轮可用的工具声明（空 = 无工具）
    #[serde(default)]
    pub tools: Vec<ToolDeclaration>,
    /// 采样温度
    #[serde(default)]
    pub temperature: Option<f32>,
    /// 输出长度上限
    #[serde(default)]
    pub max_tokens: Option<usize>,
    /// 深度思考配置
    #[serde(default)]
    pub thinking: ThinkingConfig,
}

/// 消息编解码错误
#[derive(Debug, thiserror::Error)]
pub enum MessageError {
    #[error("missing metadata key: {0}")]
    MissingKey(&'static str),
    #[error("payload decode error: {0}")]
    Decode(#[from] serde_json::Error),
}

/// 会话回信 — `ctx.reply()` 的载荷格式
///
/// 通过 `Envelope.metadata["_reply"]` 传递（JSON 字符串）。
/// `emit` 路径下 `ctx.reply()` 是 no-op，不影响调用方。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "reply", rename_all = "snake_case")]
pub enum SessionReply {
    /// LLM 调用成功
    Success {
        id: String,
        model: String,
        message: Box<Message>,
        finish_reason: FinishReason,
        usage: Option<TokenUsage>,
    },
    /// 会话忙碌（正在 Thinking 或 AwaitingCalls），拒绝新 Chat
    Busy { turn_id: u64 },
    /// 调用失败（错误/超时/panic）
    Error { message: String },
    /// 已取消（Interrupt 生效）
    Cancelled,
    /// 消息无法处理（如 P2 消息在 P1 阶段收到）
    Unhandled { reason: String },
}

impl SessionReply {
    /// 从 ChatResponse 构造成功回信
    pub fn from_response(resp: ChatResponse) -> Self {
        Self::Success {
            id: resp.id,
            model: resp.model,
            message: Box::new(resp.message),
            finish_reason: resp.finish_reason,
            usage: resp.usage,
        }
    }

    /// 编码到 Envelope（用于 `ctx.reply()`）
    pub fn to_envelope(&self) -> Envelope {
        let mut env = Envelope::new();
        let json = serde_json::to_string(self).expect("SessionReply always serializable");
        env.metadata.insert(META_REPLY.to_string(), json);
        env
    }

    /// 从 Envelope 解码回信
    pub fn from_envelope(env: &Envelope) -> Result<Self, MessageError> {
        let json = env
            .metadata
            .get(META_REPLY)
            .ok_or(MessageError::MissingKey(META_REPLY))?;
        Ok(serde_json::from_str(json)?)
    }
}

/// metadata 键名：回信 JSON
const META_REPLY: &str = "_reply";

impl SessionMessage {
    /// 编码到 Envelope（自动设置 priority）
    pub fn to_envelope(&self) -> Envelope {
        let mut env = Envelope::new();
        let json = serde_json::to_string(self).expect("SessionMessage always serializable");
        env.metadata.insert(META_MSG.to_string(), json);
        env.priority = self.priority();
        env
    }

    /// 从 Envelope 解码消息
    pub fn from_envelope(env: &Envelope) -> Result<Self, MessageError> {
        let json = env
            .metadata
            .get(META_MSG)
            .ok_or(MessageError::MissingKey(META_MSG))?;
        Ok(serde_json::from_str(json)?)
    }

    /// 消息对应的会话标识
    pub fn session_id(&self) -> SessionId {
        match self {
            Self::Chat { session_id, .. }
            | Self::Interrupt { session_id }
            | Self::ToolResult { session_id, .. }
            | Self::Resume { session_id, .. }
            | Self::SubagentDone { session_id, .. } => *session_id,
        }
    }

    /// 消息优先级（Interrupt 走 High 桶，其余 Normal）
    pub fn priority(&self) -> u8 {
        match self {
            Self::Interrupt { .. } => PRIORITY_INTERRUPT,
            _ => PRIORITY_NORMAL,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_message_roundtrip() {
        let msg = SessionMessage::Chat {
            session_id: Uuid::new_v4(),
            payload: ChatPayload {
                message: Message::user("hello"),
                options: ChatOptions::default(),
            },
        };
        let env = msg.to_envelope();
        let decoded = SessionMessage::from_envelope(&env).unwrap();
        assert_eq!(msg.session_id(), decoded.session_id());
        assert_eq!(env.priority, PRIORITY_NORMAL);
    }

    #[test]
    fn interrupt_uses_high_priority() {
        let msg = SessionMessage::Interrupt {
            session_id: Uuid::new_v4(),
        };
        let env = msg.to_envelope();
        assert_eq!(env.priority, PRIORITY_INTERRUPT);
    }
}

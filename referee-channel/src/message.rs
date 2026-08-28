//! 统一消息模型与 Envelope 编解码 — 设计文档 `docs/channel-execution.md` §4.1/§4.3
//!
//! 载荷约定：消息体走 `Envelope.payload`（JSON → Bytes），`metadata["kind"]` 区分
//! 类型，回合归因走 `metadata["session_id"]` / `["turn_id"]`。与会话协议的
//! `metadata["_msg"]` / `["_reply"]` 惯例分属不同键位，互不干扰。

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use referee_core::Envelope;

use crate::error::ChannelError;

/// 消息 kind 常量（写入 `metadata["kind"]`）
pub mod kind {
    /// 入站消息（host → router，emit）
    pub const INBOUND: &str = "im.inbound";
    /// 发送命令（任意 → host，invoke；metadata 附回合归因）
    pub const SEND: &str = "im.send";
    /// im.send 的受理回信
    pub const RECEIPT: &str = "im.receipt";
    /// 受理后通知（host → router，emit；仅观测归因，不参与控制流）
    pub const SENT: &str = "im.sent";
    /// 系统提示（router → host，invoke）
    pub const SYSTEM: &str = "im.system";
    /// 线级投递结果（Phase 2）
    pub const DELIVERY: &str = "im.delivery";
}

/// metadata 键名
pub mod meta {
    pub const KIND: &str = "kind";
    pub const SESSION_ID: &str = "session_id";
    pub const TURN_ID: &str = "turn_id";
    /// 拒绝回执的错误说明（SendReceipt{accepted:false} 时附带）
    pub const ERROR: &str = "error";
}

/// 通道消息统一优先级：内核 Normal 桶（50..=149，与会话消息同级）
const PRIORITY_NORMAL: u8 = 100;

/// 会话对方键 = (通道账号, 对端)。批次/调度/会话映射统一用它，
/// 避免同 peer 标识跨通道账号混淆。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerKey {
    pub endpoint: String,
    pub peer: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub enum ChannelContent {
    Text(String),
    /// 媒体引用（Phase 2 启用：CDN ref + 可选 AES 密钥）
    Media {
        media_kind: String,
        cdn_ref: String,
        aes_key: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboundMessage {
    /// 通道账号标识，如 "wechat/<ilink_bot_id>"
    pub endpoint: String,
    pub peer: String,
    pub message_id: String,
    pub content: ChannelContent,
    /// 通道级会话句柄（微信 = context_token），对上层 opaque，由 adapter 保管
    pub session_ctx: String,
    /// 毫秒时间戳
    pub occurred_at: i64,
    /// 通道特有字段逃生口
    pub raw: Option<serde_json::Value>,
}

impl InboundMessage {
    pub fn peer_key(&self) -> PeerKey {
        PeerKey {
            endpoint: self.endpoint.clone(),
            peer: self.peer.clone(),
        }
    }

    pub fn to_envelope(&self) -> Envelope {
        encode(kind::INBOUND, self)
    }

    pub fn from_envelope(env: &Envelope) -> Result<Self, ChannelError> {
        expect_kind(env, &[kind::INBOUND])?;
        decode_payload(env)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboundCommand {
    pub endpoint: String,
    pub peer: String,
    pub content: ChannelContent,
}

impl OutboundCommand {
    pub fn peer_key(&self) -> PeerKey {
        PeerKey {
            endpoint: self.endpoint.clone(),
            peer: self.peer.clone(),
        }
    }

    /// im.send：模型回执/主动汇报（附回合归因，host 受理后转入 im.sent 观测）。
    /// `turn_id` 未知时传 None（如 router 兜底交付）——host 将跳过 im.sent 归因。
    pub fn to_send_envelope(&self, session_id: Uuid, turn_id: Option<u64>) -> Envelope {
        let mut env = encode(kind::SEND, self);
        env.metadata
            .insert(meta::SESSION_ID.to_owned(), session_id.to_string());
        if let Some(turn) = turn_id {
            env.metadata
                .insert(meta::TURN_ID.to_owned(), turn.to_string());
        }
        env
    }

    /// im.system：基座自身的系统提示（拒绝/失败通知），无回合归因
    pub fn to_system_envelope(&self) -> Envelope {
        encode(kind::SYSTEM, self)
    }

    /// im.send 与 im.system 载荷同形，按任一 kind 解码
    pub fn from_envelope(env: &Envelope) -> Result<Self, ChannelError> {
        expect_kind(env, &[kind::SEND, kind::SYSTEM])?;
        decode_payload(env)
    }
}

/// im.send 的受理回信
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendReceipt {
    pub accepted: bool,
    /// 受理时的出站队列水位（观测用）
    pub queue_depth: usize,
}

impl SendReceipt {
    pub fn to_envelope(&self) -> Envelope {
        encode(kind::RECEIPT, self)
    }

    pub fn from_envelope(env: &Envelope) -> Result<Self, ChannelError> {
        expect_kind(env, &[kind::RECEIPT])?;
        decode_payload(env)
    }
}

/// im.sent 事件载荷——仅观测归因/指标，不参与交付判断
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SentNotice {
    pub endpoint: String,
    pub peer: String,
    pub session_id: Uuid,
    /// 来自工具写入的 metadata，作 tracing/指标维度
    pub turn_id: u64,
}

impl SentNotice {
    pub fn to_envelope(&self) -> Envelope {
        encode(kind::SENT, self)
    }

    pub fn from_envelope(env: &Envelope) -> Result<Self, ChannelError> {
        expect_kind(env, &[kind::SENT])?;
        decode_payload(env)
    }
}

/// 通道能力声明——批次/分段参数的事实来源（adapter 提供，基座消费）。
/// 具体数值由各 adapter 按自身通道声明，基座不固化任何通道假设。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelCapabilities {
    /// 单条文本上限
    pub max_text_len: usize,
    /// 批次静默闭合窗口（毫秒）
    pub batch_idle_window_ms: u64,
    /// 批次条数上限
    pub max_batch_messages: usize,
    /// 批次总窗上限（毫秒）
    pub max_batch_window_ms: u64,
}

fn encode<T: Serialize + ?Sized>(kind: &str, msg: &T) -> Envelope {
    // 消息体仅含 String/数值/bool/Value/Uuid，序列化不存在失败路径
    let mut env = Envelope::with_payload(serde_json::to_vec(msg).expect("infallible serialize"));
    env.metadata.insert(meta::KIND.to_owned(), kind.to_owned());
    env.priority = PRIORITY_NORMAL;
    env
}

fn expect_kind(env: &Envelope, expected: &[&str]) -> Result<(), ChannelError> {
    let actual = env
        .metadata
        .get(meta::KIND)
        .map(String::as_str)
        .ok_or_else(|| ChannelError::Decode(format!("missing metadata[{}]", meta::KIND)))?;
    if expected.contains(&actual) {
        Ok(())
    } else {
        Err(ChannelError::Decode(format!(
            "kind mismatch: expect one of {expected:?}, got {actual:?}"
        )))
    }
}

fn decode_payload<T: DeserializeOwned>(env: &Envelope) -> Result<T, ChannelError> {
    let payload = env
        .payload
        .as_deref()
        .ok_or_else(|| ChannelError::Decode("missing payload".into()))?;
    serde_json::from_slice(payload).map_err(|e| ChannelError::Decode(e.to_string()))
}

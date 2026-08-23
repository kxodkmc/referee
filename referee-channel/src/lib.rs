//! IM 通道接入基座（referee-channel）。
//!
//! 提供「消息进来 → 任务化 → 智能体处理 → 结果确定交付」的通用闭环：
//! 统一消息模型（`message`）、适配器契约（`adapter`）、通道宿主与路由扩展
//! （`host` / `router`）、批次与调度（`batch` / `dispatch`）、交付契约（`policy`）。
//! 通道差异全部锁进各通道适配器 crate（如 `referee-channel-wechat`），基座零通道知识。
//!
//! 设计与验收标准：`docs/channel-execution.md`。

pub mod adapter;
pub mod batch;
pub mod dispatch;
pub mod error;
pub mod host;
pub mod message;
pub mod policy;
pub mod router;
pub mod tools;

pub use adapter::{AdapterError, AdapterState, ChannelAdapter, ChannelIo};
pub use batch::{BatchAccumulator, BatchConfig, ClosedBatch};
pub use error::ChannelError;
pub use host::ChannelHost;
pub use message::{
    ChannelCapabilities, ChannelContent, InboundMessage, OutboundCommand, PeerKey, SendReceipt,
    SentNotice,
};
pub use router::{ImRouter, ImRouterConfig, SessionMap};
pub use tools::ImSendText;

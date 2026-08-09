//! 消息上下文 — 解耦数据传递与回复行为

use tokio::sync::oneshot;

use crate::common::{Envelope, KernelError, KernelResult};

/// 封装 Envelope 与可选的回复通道
pub struct MessageContext {
    pub envelope: Envelope,
    /// `emit` 时为 None；`invoke` 时为 Some（Phase 2 注入）
    reply_to: Option<oneshot::Sender<Envelope>>,
}

impl MessageContext {
    /// 用于 `emit`（fire-and-forget）— 无回复通道
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            reply_to: None,
        }
    }

    /// 用于 `invoke`（request-response）— 注入回信通道
    pub fn with_reply(envelope: Envelope, tx: oneshot::Sender<Envelope>) -> Self {
        Self {
            envelope,
            reply_to: Some(tx),
        }
    }

    /// 扩展处理完毕后调用，自动路由回调用方
    /// 消费 self，防止重复回复
    pub fn reply(self, resp: Envelope) -> KernelResult<()> {
        if let Some(tx) = self.reply_to {
            tx.send(resp).map_err(|_| KernelError::TargetUnreachable)?;
        }
        Ok(())
    }
}

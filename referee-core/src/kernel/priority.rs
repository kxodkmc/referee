//! 严格优先级通道 — 三分桶有界队列
//!
//! 发送端按 `Envelope.priority` 分桶（High / Normal / Low），
//! 接收端 `biased` 轮询强制按优先级消费，杜绝优先级反转。

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};

use crate::extension::MessageContext;

/// 优先级分桶：
/// - `0..=49`   → High
/// - `50..=149` → Normal
/// - `>=150`    → Low
#[derive(Clone)]
pub struct PrioritySender {
    high: mpsc::Sender<MessageContext>,
    norm: mpsc::Sender<MessageContext>,
    low: mpsc::Sender<MessageContext>,
}

pub struct PriorityReceiver {
    high: mpsc::Receiver<MessageContext>,
    norm: mpsc::Receiver<MessageContext>,
    low: mpsc::Receiver<MessageContext>,
}

impl PrioritySender {
    /// 创建三分桶有界通道，每个桶容量均为 `queue_size`
    pub fn new(queue_size: usize) -> (PrioritySender, PriorityReceiver) {
        let (htx, hrx) = mpsc::channel(queue_size);
        let (ntx, nrx) = mpsc::channel(queue_size);
        let (ltx, lrx) = mpsc::channel(queue_size);
        (
            PrioritySender {
                high: htx,
                norm: ntx,
                low: ltx,
            },
            PriorityReceiver {
                high: hrx,
                norm: nrx,
                low: lrx,
            },
        )
    }

    /// 按优先级分桶投递；缓冲区满 / 通道关闭均即时返回，不阻塞
    #[allow(clippy::result_large_err)] // tokio 错误类型固定携带 MessageContext，无法缩小
    pub fn try_send(&self, ctx: MessageContext) -> Result<(), TrySendError<MessageContext>> {
        match ctx.envelope.priority {
            0..=49 => self.high.try_send(ctx),
            50..=149 => self.norm.try_send(ctx),
            _ => self.low.try_send(ctx),
        }
    }
}

impl PriorityReceiver {
    /// 严格按优先级拉取消息，杜绝优先级反转
    ///
    /// 快速路径 `try_recv` 处理已有消息；全空时进入 `biased` 阻塞等待，
    /// 保证后到的高优先级消息可立即插队。所有 Sender drop 后返回 `None`。
    pub async fn recv(&mut self) -> Option<MessageContext> {
        if let Ok(ctx) = self.high.try_recv() {
            return Some(ctx);
        }
        if let Ok(ctx) = self.norm.try_recv() {
            return Some(ctx);
        }
        if let Ok(ctx) = self.low.try_recv() {
            return Some(ctx);
        }
        tokio::select! {
            biased;
            Some(ctx) = self.high.recv() => Some(ctx),
            Some(ctx) = self.norm.recv() => Some(ctx),
            Some(ctx) = self.low.recv() => Some(ctx),
            else => None,
        }
    }

    /// 非阻塞拉取：优先 High，其次 Normal，最后 Low
    pub fn try_recv(&mut self) -> Result<MessageContext, TryRecvError> {
        if let Ok(ctx) = self.high.try_recv() {
            return Ok(ctx);
        }
        if let Ok(ctx) = self.norm.try_recv() {
            return Ok(ctx);
        }
        self.low.try_recv()
    }
}

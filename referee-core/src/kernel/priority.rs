//! 严格优先级通道 — 三分桶有界队列 + 老化防饥饿
//!
//! 自定义实现（废弃 `tokio::mpsc`）：`parking_lot::Mutex<VecDeque>` 提供
//! 头部探测能力（老化检查需要 peek，`tokio::mpsc` 无此能力），
//! `tokio::sync::Notify` 提供异步唤醒；`try_send` 手动容量检查保持有界背压。
//!
//! 消费顺序：Low 头部老化（超过阈值）→ 优先消费（杜绝 High 持续负载下的
//! Low 永久饥饿）；否则 High → Normal → Low。
//!
//! 可观测性：Sender / Receiver 共享 `Arc<AtomicUsize>` 深度计数器，每次
//! 入队 / 出队后以 `Relaxed` 内存序更新并回写 `referee_queue_depth` gauge。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use metrics::gauge;
use parking_lot::Mutex;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::Notify;

use crate::extension::{CapabilityId, MessageContext};

/// Low 队列头部老化阈值：超过则抢在 High / Normal 之前消费
const LOW_PRIORITY_AGING_THRESHOLD: Duration = Duration::from_secs(1);

/// 优先级分桶：
/// - `0..=49`   → High
/// - `50..=149` → Normal
/// - `>=150`    → Low
#[derive(Clone)]
pub struct PrioritySender {
    inner: Arc<Mutex<QueueState>>,
    notify: Arc<Notify>,
    /// 当前在途消息数（三桶合计）— 与 Receiver 共享
    depth: Arc<AtomicUsize>,
    ext_id: CapabilityId,
}

pub struct PriorityReceiver {
    inner: Arc<Mutex<QueueState>>,
    notify: Arc<Notify>,
    /// 当前在途消息数（三桶合计）— 与 Sender 共享
    depth: Arc<AtomicUsize>,
    ext_id: CapabilityId,
}

struct QueueState {
    high: VecDeque<MessageContext>,
    norm: VecDeque<MessageContext>,
    low: VecDeque<MessageContext>,
    /// 每桶容量上限（背压硬约束）
    capacity: usize,
    /// 最后一个 Sender drop 后置位（通道关闭语义）
    closed: bool,
}

/// 消费优先级：Low 头部老化优先（防饥饿），否则 High → Normal → Low
fn pop_front_priority(q: &mut QueueState) -> Option<MessageContext> {
    // 老化优先：Low 头部超时则抢在 High 之前消费，杜绝持续 High 负载下的饥饿
    if let Some(ctx) = q.low.front() {
        if ctx.envelope.queued_at.elapsed() > LOW_PRIORITY_AGING_THRESHOLD {
            return q.low.pop_front();
        }
    }
    if let Some(ctx) = q.high.pop_front() {
        return Some(ctx);
    }
    if let Some(ctx) = q.norm.pop_front() {
        return Some(ctx);
    }
    // High / Normal 空：Low 无竞争者，未老化也直接消费，不滞留
    q.low.pop_front()
}

impl PrioritySender {
    /// 创建三分桶有界通道，每个桶容量均为 `queue_size`
    pub fn new(queue_size: usize, ext_id: CapabilityId) -> (PrioritySender, PriorityReceiver) {
        let inner = Arc::new(Mutex::new(QueueState {
            high: VecDeque::new(),
            norm: VecDeque::new(),
            low: VecDeque::new(),
            capacity: queue_size,
            closed: false,
        }));
        let notify = Arc::new(Notify::new());
        let depth = Arc::new(AtomicUsize::new(0));
        (
            PrioritySender {
                inner: inner.clone(),
                notify: notify.clone(),
                depth: depth.clone(),
                ext_id,
            },
            PriorityReceiver {
                inner,
                notify,
                depth,
                ext_id,
            },
        )
    }

    /// 按优先级分桶投递；缓冲区满 / 通道关闭均即时返回，不阻塞
    #[allow(clippy::result_large_err)] // 错误类型固定携带 MessageContext，无法缩小
    pub fn try_send(&self, ctx: MessageContext) -> Result<(), TrySendError<MessageContext>> {
        let mut q = self.inner.lock();
        if q.closed {
            return Err(TrySendError::Closed(ctx));
        }
        let capacity = q.capacity;
        let bucket = match ctx.envelope.priority {
            0..=49 => &mut q.high,
            50..=149 => &mut q.norm,
            _ => &mut q.low,
        };
        if bucket.len() >= capacity {
            return Err(TrySendError::Full(ctx));
        }
        let mut ctx = ctx;
        // 入队点刷新老化时钟（WAL 恢复重投也会重置计时）
        ctx.envelope.queued_at = Instant::now();
        bucket.push_back(ctx);
        // 先释放锁再唤醒，避免接收端拿锁自旋
        drop(q);
        let current = self.depth.fetch_add(1, Ordering::Relaxed) + 1;
        gauge!("referee_queue_depth", "ext_id" => self.ext_id.to_string()).set(current as f64);
        self.notify.notify_one();
        Ok(())
    }

    /// 当前在途消息数（三桶合计）— 只读快照，供自省接口观测
    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::Relaxed)
    }
}

impl Drop for PrioritySender {
    fn drop(&mut self) {
        // strong_count == 2：此刻仍持有 self，仅剩 self 与 Receiver —— 即最后一个 Sender
        if Arc::strong_count(&self.inner) == 2 {
            self.inner.lock().closed = true;
            self.notify.notify_one();
        }
    }
}

impl PriorityReceiver {
    /// 严格按优先级拉取消息，杜绝优先级反转与 Low 饥饿
    ///
    /// 队列非空即返回（老化优先）；全空时挂起等待 `Notify`（`notify_one`
    /// 存储 permit，无丢失唤醒竞态）。所有 Sender drop 后返回 `None`。
    pub async fn recv(&self) -> Option<MessageContext> {
        loop {
            let (popped, closed) = {
                let mut q = self.inner.lock();
                (pop_front_priority(&mut q), q.closed)
            };
            if let Some(ctx) = popped {
                self.update_depth();
                return Some(ctx);
            }
            if closed {
                return None;
            }
            self.notify.notified().await;
        }
    }

    /// 非阻塞拉取：优先 High，其次 Normal，最后 Low
    pub fn try_recv(&self) -> Result<MessageContext, TryRecvError> {
        let mut q = self.inner.lock();
        if let Some(ctx) = pop_front_priority(&mut q) {
            self.update_depth();
            return Ok(ctx);
        }
        if q.closed {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// 出队后递减深度并回写 gauge
    ///
    /// 用 `checked_sub` 原子防下溢：depth=0 时保持 0，绝不回绕成天文数字
    /// （正常协议下 recv 必先于 send 成功，此处为防御性兜底）。
    #[inline]
    fn update_depth(&self) {
        let current = self
            .depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
            .map(|prev| prev - 1)
            .unwrap_or(0);
        gauge!("referee_queue_depth", "ext_id" => self.ext_id.to_string()).set(current as f64);
    }
}

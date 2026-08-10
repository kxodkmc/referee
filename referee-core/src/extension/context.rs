//! 扩展侧执行上下文 — 受限通信能力注入
//!
//! `KernelContext` 取代完整 `Kernel` 句柄注入 `handle`：仅暴露
//! `emit`（即发即弃）与 `spawn_blocking`（阻塞出口），**不暴露 `invoke`**，
//! 从编译期切断 `handle` 内的嵌套请求响应链（A→invoke B→invoke A 耗尽线程池
//! 死锁），贯彻「阻塞即违规」。
//!
//! `MessageContext` 退化为内核内部队列元素（Envelope + 可选回信通道 + WAL ID）。

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::common::{Envelope, KernelError, KernelResult};
use crate::extension::CapabilityId;
use crate::kernel::KernelView;

/// 受限的内核上下文 — 注入 `handle`，杜绝嵌套 invoke 死锁
pub struct KernelContext {
    self_id: CapabilityId,
    view: KernelView,
    /// `invoke` 注入的回信通道（emit 时为 None）
    reply_to: Option<oneshot::Sender<Envelope>>,
}

impl KernelContext {
    /// 仅供内核（supervisor）构造
    pub(crate) fn new(
        self_id: CapabilityId,
        view: KernelView,
        reply_to: Option<oneshot::Sender<Envelope>>,
    ) -> Self {
        Self {
            self_id,
            view,
            reply_to,
        }
    }

    /// 当前扩展的能力标识
    pub fn self_id(&self) -> CapabilityId {
        self.self_id
    }

    /// 唯一允许的通信原语：即发即弃（非阻塞，无响应等待）
    pub async fn emit(&self, target: CapabilityId, env: Envelope) -> KernelResult<()> {
        self.view.dispatch(target, MessageContext::new(env)).await
    }

    /// 唯一允许的阻塞出口：强制转移至独立线程池（`spawn_blocking`）
    pub fn spawn_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        tokio::task::spawn_blocking(f)
    }

    /// 消费式回信（仅 `invoke` 请求携带回信通道；emit 调用为 no-op）
    /// 消费 `self`，从结构上杜绝重复回复
    pub fn reply(self, resp: Envelope) -> KernelResult<()> {
        if let Some(tx) = self.reply_to {
            tx.send(resp).map_err(|_| KernelError::TargetUnreachable)?;
        }
        Ok(())
    }
}

/// 封装 Envelope 与可选回复通道的队列元素（内核内部使用）
pub struct MessageContext {
    pub envelope: Envelope,
    /// `emit` 时为 None；`invoke` 时为 Some（Phase 2 注入）
    reply_to: Option<oneshot::Sender<Envelope>>,
    /// WAL 追加 ID — handle 成功后由 supervisor ACK 清理（未启用 WAL 时为 None）
    pub(crate) wal_id: Option<Uuid>,
}

impl MessageContext {
    /// 用于 `emit`（fire-and-forget）— 无回复通道
    pub fn new(envelope: Envelope) -> Self {
        Self {
            envelope,
            reply_to: None,
            wal_id: None,
        }
    }

    /// 用于 `invoke`（request-response）— 注入回信通道
    pub fn with_reply(envelope: Envelope, tx: oneshot::Sender<Envelope>) -> Self {
        Self {
            envelope,
            reply_to: Some(tx),
            wal_id: None,
        }
    }

    /// 用于 WAL 恢复（`start_with_recovery`）— 无回信通道、无 WAL 追加
    pub fn from_recovered(envelope: Envelope) -> Self {
        Self {
            envelope,
            reply_to: None,
            wal_id: None,
        }
    }

    /// 提取回信通道（supervisor 组装 KernelContext 时使用）
    pub(crate) fn take_reply(&mut self) -> Option<oneshot::Sender<Envelope>> {
        self.reply_to.take()
    }
}

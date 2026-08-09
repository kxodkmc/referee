//! 路由与背压层 — DashMap + 优先级通道强制背压

use dashmap::DashMap;

use crate::common::{Envelope, KernelError};
use crate::extension::{CapabilityId, MessageContext};
use crate::kernel::priority::PrioritySender;

/// 分发失败：错误码 + 被拒的 Envelope（供死信队列捕获）
pub type DispatchError = (KernelError, Envelope);

struct RouterInner {
    /// CapabilityId → 优先级发送端
    routes: DashMap<CapabilityId, PrioritySender>,
}

/// 并发路由表
/// 通过 `Arc` 实现 `Clone`（廉价），可在 spawned task 间共享
#[derive(Clone)]
pub struct Router {
    inner: std::sync::Arc<RouterInner>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(RouterInner {
                routes: DashMap::new(),
            }),
        }
    }

    /// 注册路由条目
    pub fn insert(&self, id: CapabilityId, sender: PrioritySender) {
        self.inner.routes.insert(id, sender);
    }

    /// 移除路由条目（同时丢弃 Sender，触发通道关闭）
    pub fn remove(&self, id: &CapabilityId) {
        self.inner.routes.remove(id);
    }

    pub fn contains(&self, id: &CapabilityId) -> bool {
        self.inner.routes.contains_key(id)
    }

    /// 统一的路由分发入口 — 接受已组装好的 MessageContext
    ///
    /// 使用 try_send 实现即时背压：
    /// 缓冲区满 → `ResourceExhausted`
    /// 通道关闭 / 目标不存在 → `TargetUnreachable`
    /// 失败时返回被拒的 Envelope，供上层写入死信队列
    #[allow(clippy::result_large_err)] // 需回传被拒 Envelope 供 DLQ 捕获，无法缩小
    pub fn dispatch(
        &self,
        target: &CapabilityId,
        ctx: MessageContext,
    ) -> Result<(), DispatchError> {
        let Some(route) = self.inner.routes.get(target) else {
            return Err((KernelError::TargetUnreachable, ctx.envelope));
        };
        route.try_send(ctx).map_err(|e| match e {
            tokio::sync::mpsc::error::TrySendError::Full(ctx) => {
                (KernelError::ResourceExhausted, ctx.envelope)
            }
            tokio::sync::mpsc::error::TrySendError::Closed(ctx) => {
                (KernelError::TargetUnreachable, ctx.envelope)
            }
        })
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

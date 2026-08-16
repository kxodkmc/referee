//! 路由与背压层 — 状态 + 路由原子合并（零竞态）
//!
//! `DashMap<CapabilityId, RouteEntry>`：扩展状态与发送端在同一条目内，
//! `dispatch` 通过 `get` 在读锁内一次性完成状态校验与路由投递，
//! 消除「路由已移除但状态仍为 Running」的盲区窗口（消息静默丢失）。

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use crate::common::{Envelope, KernelError};
use crate::extension::{CapabilityId, MessageContext};
use crate::kernel::priority::PrioritySender;

/// 分发失败：错误码 + 被拒的 Envelope（供死信队列捕获）
pub type DispatchError = (KernelError, Envelope);

/// 扩展生命周期状态（并入路由表，与发送端同条目原子可见）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionState {
    Running,
    Crashed,
    Stopped,
}

/// 扩展运行时信息快照 — 自省 / 运维观测（`Kernel::extensions`）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtensionInfo {
    pub id: CapabilityId,
    pub state: ExtensionState,
    /// 在途消息数（三优先级桶合计）
    pub queue_depth: usize,
    /// 当前重启窗口内的累计重启次数
    pub restarts: u32,
}

struct RouteEntry {
    sender: PrioritySender,
    state: ExtensionState,
    /// 注册代际：同 id 重注册后旧监督任务的退出收敛不覆盖新条目
    gen: u64,
    /// 重启计数 — 与监督运行时共享（运行时写入，快照读取）
    restarts: Arc<AtomicU32>,
}

struct RouterInner {
    /// CapabilityId → 路由条目（发送端 + 状态）
    routes: DashMap<CapabilityId, RouteEntry>,
    /// 单调递增的注册代际
    next_gen: AtomicU64,
}

/// 并发路由表
/// 通过 `Arc` 实现 `Clone`（廉价），可在 spawned task 间共享
#[derive(Clone)]
pub struct Router {
    inner: Arc<RouterInner>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RouterInner {
                routes: DashMap::new(),
                next_gen: AtomicU64::new(0),
            }),
        }
    }

    /// 分配一个新的注册代际（每次 register 递增）
    pub fn next_generation(&self) -> u64 {
        self.inner.next_gen.fetch_add(1, Ordering::Relaxed)
    }

    /// 注册路由条目（初始状态由调用方给定，通常为 `Running`）；
    /// `restarts` 为与监督运行时共享的重启计数器（初始 0）
    pub fn insert(
        &self,
        id: CapabilityId,
        sender: PrioritySender,
        state: ExtensionState,
        gen: u64,
        restarts: Arc<AtomicU32>,
    ) {
        self.inner.routes.insert(
            id,
            RouteEntry {
                sender,
                state,
                gen,
                restarts,
            },
        );
    }

    /// 移除路由条目（同时丢弃 Sender，触发通道关闭）
    pub fn remove(&self, id: &CapabilityId) {
        self.inner.routes.remove(id);
    }

    pub fn contains(&self, id: &CapabilityId) -> bool {
        self.inner.routes.contains_key(id)
    }

    /// 全量路由快照 — id / 状态 / 队列深度 / 重启计数的一次性只读视图
    ///
    /// 逐条目读取：单条目内各字段取自同一路由条目（状态与深度一致），
    /// 跨条目不保证同一瞬时；仅供运维观测，不作决策依据。
    pub fn snapshot(&self) -> Vec<ExtensionInfo> {
        self.inner
            .routes
            .iter()
            .map(|e| ExtensionInfo {
                id: *e.key(),
                state: e.value().state,
                queue_depth: e.value().sender.depth(),
                restarts: e.value().restarts.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// 当前条目是否属于指定注册代际（防旧任务误改新条目）
    pub fn matches_generation(&self, id: &CapabilityId, gen: u64) -> bool {
        self.inner.routes.get(id).is_some_and(|e| e.gen == gen)
    }

    /// 读取扩展状态（supervisor 退出收敛 / 测试断言）
    pub fn get_state(&self, id: &CapabilityId) -> Option<ExtensionState> {
        self.inner.routes.get(id).map(|e| e.state)
    }

    /// 原子修改扩展状态（崩溃标记 / 重启恢复 / 停机收敛）
    pub fn set_state(&self, id: CapabilityId, state: ExtensionState) {
        if let Some(mut entry) = self.inner.routes.get_mut(&id) {
            entry.state = state;
        }
    }

    /// 统一的路由分发入口 — 状态校验与背压投递在同一锁内完成
    ///
    /// 使用 try_send 实现即时背压：
    /// 缓冲区满 → `ResourceExhausted`
    /// 通道关闭 / 目标不存在 → `TargetUnreachable`
    /// 状态拦截 → `ExtensionCrashed` / `TargetUnreachable`
    /// 失败时返回被拒的 Envelope，供上层写入死信队列
    #[allow(clippy::result_large_err)] // 需回传被拒 Envelope 供 DLQ 捕获，无法缩小
    pub fn dispatch(
        &self,
        target: &CapabilityId,
        ctx: MessageContext,
    ) -> Result<(), DispatchError> {
        let Some(entry) = self.inner.routes.get(target) else {
            return Err((KernelError::TargetUnreachable, ctx.envelope));
        };
        // 状态拦截（与路由查找同一读锁，无并发窗口）
        match entry.state {
            ExtensionState::Crashed => {
                return Err((KernelError::ExtensionCrashed, ctx.envelope));
            }
            ExtensionState::Stopped => {
                return Err((KernelError::TargetUnreachable, ctx.envelope));
            }
            ExtensionState::Running => {}
        }
        entry.sender.try_send(ctx).map_err(|e| match e {
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

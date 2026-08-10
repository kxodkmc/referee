//! 全局治理状态 — 停机拦截
//!
//! 扩展状态已并入路由表（见 `kernel/router.rs`，与发送端同条目原子可见）；
//! 本模块仅保留内核全局治理状态（Running / Stopping）。

use std::sync::Arc;

use parking_lot::RwLock;

/// 内核全局治理状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalState {
    Running,
    /// 优雅停机进行中：拒绝新消息，允许扩展 drain 积压
    Stopping,
}

struct MonitorInner {
    global: RwLock<GlobalState>,
}

/// 全局治理状态持有者
/// 通过 `Arc` 实现 `Clone`（廉价），可在 spawned task 间共享
#[derive(Clone)]
pub struct Monitor {
    inner: Arc<MonitorInner>,
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MonitorInner {
                global: RwLock::new(GlobalState::Running),
            }),
        }
    }

    /// 设置全局治理状态（如进入优雅停机）
    pub fn set_global_state(&self, state: GlobalState) {
        *self.inner.global.write() = state;
    }

    /// 是否处于停机拦截态
    pub fn is_stopping(&self) -> bool {
        *self.inner.global.read() == GlobalState::Stopping
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}

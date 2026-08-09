//! 治理监控层 — 扩展状态机 + 全局治理状态 + 路由拦截

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::extension::CapabilityId;

/// 扩展生命周期状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtensionState {
    Running,
    Crashed,
    Stopped,
}

/// 内核全局治理状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalState {
    Running,
    /// 优雅停机进行中：拒绝新消息，允许扩展 drain 积压
    Stopping,
}

struct MonitorInner {
    states: RwLock<HashMap<CapabilityId, ExtensionState>>,
    global: RwLock<GlobalState>,
}

/// 并发安全的状态注册表
/// 通过 `Arc` 实现 `Clone`（廉价），可在 spawned task 间共享
#[derive(Clone)]
pub struct Monitor {
    inner: Arc<MonitorInner>,
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MonitorInner {
                states: RwLock::new(HashMap::new()),
                global: RwLock::new(GlobalState::Running),
            }),
        }
    }

    pub fn set_state(&self, id: CapabilityId, state: ExtensionState) {
        self.inner.states.write().insert(id, state);
    }

    pub fn get_state(&self, id: &CapabilityId) -> Option<ExtensionState> {
        self.inner.states.read().get(id).copied()
    }

    pub fn remove(&self, id: &CapabilityId) {
        self.inner.states.write().remove(id);
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

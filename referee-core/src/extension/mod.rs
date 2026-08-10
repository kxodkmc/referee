//! 扩展 SDK 侧接口定义

pub mod context;
pub mod dlq;

use async_trait::async_trait;
use uuid::Uuid;

use crate::common::{Envelope, KernelResult};
pub use context::{KernelContext, MessageContext};

/// 扩展能力标识 — 路由主键
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct CapabilityId(Uuid);

impl CapabilityId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl std::fmt::Display for CapabilityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for CapabilityId {
    fn default() -> Self {
        Self::new()
    }
}

/// 扩展 Trait — SDK 侧唯一契约
///
/// # Safety
/// 实现必须保证 `handle` 非阻塞；重计算逻辑必须移交 `ctx.spawn_blocking`。
/// 与上游通信仅允许 `ctx.emit`（即发即弃）；**不得**在 `handle` 内等待
/// 其他扩展的响应（`invoke` 未注入，编译期即被禁止），防止嵌套调用
/// 耗尽线程池导致死锁。
#[async_trait]
pub trait Extension: Send + Sync {
    /// 返回该扩展的能力标识（注册时使用）
    fn id(&self) -> CapabilityId;

    /// 核心处理逻辑 — 注入受限上下文 + 消息载荷
    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()>;
}

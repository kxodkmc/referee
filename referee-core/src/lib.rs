//! Referee Core — 工业级微内核
//!
//! 当前版本: Phase 4 — 治理与生命周期闭环（优先级路由 / 监督自愈 / 优雅停机 / 死信降级）

pub mod common;
pub mod extension;
pub mod kernel;

// 顶层快捷重导出
pub use common::{Envelope, KernelError, KernelResult};
pub use extension::dlq::{DlqSink, InMemoryDlq};
pub use extension::{CapabilityId, Extension, KernelContext, MessageContext};
pub use kernel::wal::{InMemoryWal, WalSink};
pub use kernel::{Kernel, SupervisionPolicy};

//! Referee Core — 工业级微内核
//!
//! 当前版本: Phase 1 ~ 6 全部完成 + 监督治理加固——路由 / 原语 / 治理
//! （背压、严格优先级、监督自愈、挂起治理、优雅停机、死信降级、WAL 崩溃兜底）

pub mod common;
pub mod extension;
pub mod kernel;

// 顶层快捷重导出
pub use common::{Envelope, KernelError, KernelResult};
pub use extension::dlq::{DlqSink, InMemoryDlq};
pub use extension::{CapabilityId, Extension, KernelContext, MessageContext};
pub use kernel::wal::{InMemoryWal, WalSink};
pub use kernel::{
    ExtensionInfo, ExtensionState, Kernel, RegisterOptions, SupervisionPolicy,
};

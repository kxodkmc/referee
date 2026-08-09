//! 内核错误码 — 覆盖传输、状态与资源异常

use thiserror::Error;

/// 所有 Kernel API 返回的统一错误类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum KernelError {
    #[error("target unreachable: extension not registered or invalidated")]
    TargetUnreachable,
    #[error("extension crashed")]
    ExtensionCrashed,
    #[error("resource exhausted: channel buffer full (backpressure triggered)")]
    ResourceExhausted,
    #[error("operation timed out")]
    Timeout,
    #[error("invalid response data")]
    InvalidResponse,
    #[error("system is shutting down")]
    SystemShuttingDown,
}

pub type KernelResult<T> = Result<T, KernelError>;

//! 通道层错误分类 — 设计文档 `docs/channel-execution.md` §4.4

/// 通道层统一错误。与内核错误语义对齐：`Rejected` ↔ `ResourceExhausted`，
/// `TargetUnreachable` ↔ 内核同名错误。
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    /// 载荷编解码失败——丢弃该消息并记 warn 日志，绝不打断收发循环
    #[error("channel decode: {0}")]
    Decode(String),
    /// 队列满，显式拒绝（对应内核 ResourceExhausted 语义）
    #[error("channel queue full (rejected)")]
    Rejected,
    /// 路由目标不可达（对齐内核 TargetUnreachable）
    #[error("channel target unreachable")]
    TargetUnreachable,
    /// 通道会话句柄失效（微信 errcode=-14）
    #[error("channel session token expired")]
    TokenExpired,
    /// 适配器内部错误（重试后仍失败 / adapter 已降级）
    #[error("channel adapter: {0}")]
    Adapter(String),
}

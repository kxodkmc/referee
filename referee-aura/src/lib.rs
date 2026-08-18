//! # Referee Aura — 智能体运行气场（常驻 daemon）
//!
//! 把 referee（Rust 智能体库）做成可被 TUI / Web / CLI 调用的智能体服务，
//! 支持**多个独立实例并行运行与管理、非正常中断可恢复**。
//!
//! 分层（职责边界，严格不越界）：
//! - [`protocol`]：纯数据 serde 类型 + 错误码（与传输解耦，零业务逻辑）。
//! - [`instance`]：实例生命周期 + 多实例有界管理 + 请求路由（transport-agnostic）。
//! - [`persist`]：文件 IO + 崩溃恢复（依赖 `protocol::InstanceSpec`）。
//! - [`chat`]：对话公共助手（载荷构造 / 流收敛 / 帧映射，TCP 与 HTTP 复用）。
//! - [`transport`]（feature `tcp`）：TCP JSON-RPC 2.0 网络 IO（仅调用 instance/persist）。
//! - [`http`]（feature `http`）：HTTP + SSE 网络 IO（仅调用 instance/chat）。
//! - [`tui`]（feature `tui`）：官方 TUI 客户端（JSON-RPC 客户端，连接 daemon）。
//!
//! 硬约束（继承 referee 内核哲学）：零新增依赖；不吞异常；背压有界。

pub mod chat;
pub mod instance;
pub mod persist;
pub mod protocol;
pub mod server;
#[cfg(feature = "tcp")]
pub mod transport;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "tui")]
pub mod tui;

pub use instance::{Instance, InstanceManager, InstanceManagerConfig, InstanceStatus};
pub use persist::{BrokenEntry, PersistError, PersistStore, RecoveryResult};
pub use protocol::{
    ChatReply, ChatRequest, InstanceId, InstanceInfo, InstanceSpec, InstanceState, InstanceTools,
    FsToolConfig, ProviderConfig, ServerError, StreamFrame, TokenUsageData,
};
#[cfg(feature = "tcp")]
pub use transport::{dispatch, serve_tcp};
#[cfg(feature = "http")]
pub use http::serve_http;
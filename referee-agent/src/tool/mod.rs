//! 工具调用模块 — Phase 2 核心交付
//!
//! ## 模块结构
//! - [`trait`]: `Tool` trait + `ToolContext` + `ToolOutput` + `ToolError`
//! - [`registry`]: `ToolRegistry` — 有界注册表
//! - [`executor`]: `ToolExecutor` — 并行执行 + 截断 + panic 隔离
//! - `bridge`（预留）: MCP / Skills 桥接（Phase 7）
//!
//! ## 分层依赖
//! ```text
//!  provider ──▶ tool/trait ──▶ tool/registry ──▶ tool/executor
//!                                                        │
//!                                           session ◀───┘（通过 emit 回写）
//! ```
//!
//! `tool` 模块不依赖 `session`，反向依赖通过 `emit_callback` 闭包注入。

pub mod agent_tool;
pub mod definition;
pub mod executor;
pub mod registry;

pub use agent_tool::AgentTool;
pub use definition::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
pub use executor::{ExecutedTool, ExecutorConfig, ToolExecutor};
pub use registry::{RegistryConfig, RegistryError, ToolRegistry};

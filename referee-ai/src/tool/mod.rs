//! 工具调用模块 — 抽象与执行机制（地基）
//!
//! 仅保留「工具机制」本身：`Tool` trait + 有界注册表 + 并行/截断/隔离/超时执行器。
//! 具体的业务能力（MCP 适配、Skills、对等 Agent）由上层（referee-agent）基于
//! `Tool` trait 自行接入，本模块不预置任何协议桥接。
//!
//! ## 模块结构
//! - [`definition`]: `Tool` trait + `ToolContext` + `ToolOutput` + `ToolError`
//! - [`registry`]: `ToolRegistry` — 有界注册表
//! - [`executor`]: `ToolExecutor` — 并行执行 + 截断 + panic 隔离 + 超时
//!
//! ## 分层依赖
//! ```text
//!  provider ──▶ tool/definition ──▶ tool/registry ──▶ tool/executor
//!                                                        │
//!                                      （执行结果由调用方/engine 收敛回写）
//! ```
//! `tool` 模块不依赖 `session`，结果回写由上层编排负责。

pub mod definition;
pub mod executor;
pub mod registry;

pub use definition::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
pub use executor::{ExecutedTool, ExecutorConfig, ToolOutcome, ToolExecutor};
pub use registry::{RegistryConfig, RegistryError, ToolRegistry};

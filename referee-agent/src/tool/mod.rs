//! 对等协作与工具（业务层扩展）
//! 将 `agent_tool`（Agent as Tool）、成果板读取工具挂载到 base 的 `Tool` 抽象上。
//! MCP 客户端桥（`mcp`）为按需拓展，需启用 `mcp-stdio` feature 才加载。

pub mod agent_tool;
pub mod artifact_reader;
pub mod edit;
pub mod fs_common;
pub mod read;
pub mod write;
#[cfg(feature = "mcp-stdio")]
pub mod mcp;

pub use agent_tool::AgentTool;
pub use artifact_reader::{ArtifactReader, ListMyBoard};
pub use edit::EditTool;
pub use fs_common::FsConfig;
pub use read::{ReadTool, ReadToolConfig};
pub use write::WriteTool;
#[cfg(feature = "mcp-stdio")]
pub use mcp::{McpClient, McpError, McpServer, McpServerConfig, McpToolClient, MrtrStrategy};

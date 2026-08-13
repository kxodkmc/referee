//! 对等协作与工具（业务层扩展）
//! 将 `agent_tool`（Agent as Tool）、成果板读取工具挂载到 base 的 `Tool` 抽象上。

pub mod agent_tool;
pub mod artifact_reader;

pub use agent_tool::AgentTool;
pub use artifact_reader::{ArtifactReader, ListMyBoard};

//! 对等协作与工具（业务层扩展）
//! 将 `agent_tool`（Agent as Tool）等业务工具能力挂载到 base 的 `Tool` 抽象上。

pub mod agent_tool;

pub use agent_tool::AgentTool;

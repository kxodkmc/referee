//! Agent 定义与装配 — 业务层能力声明
//!
//! 模型：`AgentDefinition` 是**纯数据**（声明式，可来自 JSON/TOML/builder），
//! `AgentBuilder` 链式构造，`bind` 解析白名单并渲染模板为 [`BoundAgent`]。
//! 白名单**封闭默认**：`["*"]`=全部，`["a"]`=仅 a，`[]`=无该能力（空则不进提示词）。
//!
//! 依赖 base 的分段编排器（`SystemSection` + `assemble`）与白名单过滤
//! （`ToolRegistry::declarations_visible`），实现"没启用的能力就不注入"。

pub mod builder;
pub mod builtin;
pub mod definition;
pub mod id;
pub mod registry;
pub mod template;

pub use builder::{AgentBuilder, BoundAgent, BuilderError};
pub use builtin::{general, GENERAL_ID};
pub use definition::{AgentDefinition, ChatParams, TemplateRef};
pub use id::{AgentId, AgentIdError};
pub use registry::{AgentRegistry, RegistryConfig, RegistryError};
pub use template::{interpolate, TemplateConfig, TemplateError, TemplateRegistry};
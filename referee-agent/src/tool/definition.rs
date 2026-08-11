//! Tool trait — 工具统一抽象
//!
//! 所有工具（内置 / 用户自定义 / 未来 MCP 代理）实现此 trait，
//! 上层面对统一接口，不写工具分支。
//!
//! ## 设计约束
//! - **数据/行为分离**：`ToolDeclaration` 是纯数据（供 LLM 调用方拼装请求），
//!   `Tool` 是行为（实际执行）。声明从实现中提取，不混用。
//! - **panic 隔离**：`execute` 内部的 panic 由 `ToolExecutor` 在调用边界
//!   `catch_unwind` 捕获，转为 `ToolError::Panic`，不影响其他工具与会话。
//! - **输入/输出均 `Send + 'static`**：工具可在独立 task 中并行执行。

//! ## 信任边界（安全声明）
//! 工具注册（`ToolRegistry::register` / `AgentRuntime::register_peer_tool`）是
//! 对等能力的唯一信任边界：注入 `ToolContext` 的 `Kernel` 与 `ArtifactStore`
//! 句柄仅授予**可信注册**的工具。当前 Phase 无用户自定义 / MCP 工具（Phase 7
//! 预留），引入不可信工具前须先将 `kernel` 收窄为受限句柄（固定目标白名单），
//! 否则任何持有句柄的工具均可 invoke 任意能力 / 伪造会话消息。

use std::sync::Arc;

use async_trait::async_trait;
use referee_core::Kernel;
use serde_json::Value;

use crate::artifact::ArtifactStore;
use crate::provider::ToolDeclaration;

/// 工具分类 — 决定执行策略
///
/// - `Local`：内部调用（如对等 Agent RPC），不占用外部 IO 并发槽位；
///   仅受内核背压与目标扩展自身容量约束。
/// - `Remote`：外部 IO（HTTP / 文件等），受 `ToolExecutor` 的 Semaphore 限流。
///
/// 分类解决「AgentTool 占用槽位等待目标 Agent 完成，而目标 Agent 又需要
/// 槽位执行自身工具」的资源池死锁。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCategory {
    /// 内部调用（如 Agent 调用），不占用外部 IO 槽位
    Local,
    /// 外部 IO（如 HTTP），受 Semaphore 限制
    Remote,
}

/// 工具执行上下文 — 由 `ToolExecutor` 注入
///
/// Phase 3 注入受限的对等能力：`kernel`（RPC invoke）与 `artifact_store`
/// （大结果落库）。两者均为 `Option`：未启用对等能力时保持 `None`，
/// 对等工具（如 [`crate::tool::AgentTool`]）在缺失时返回明确错误。
/// 既有本地工具无感知（字段为 pub，忽略即可）。
#[derive(Clone)]
pub struct ToolContext {
    /// 触发此次工具调用的 `ToolCall.id`（LLM 生成的，用于结果回传匹配）
    pub tool_call_id: String,
    /// 所属会话 ID（用于 tracing 关联与工件 ACL 授权）
    pub session_id: uuid::Uuid,
    /// 所属轮次 ID（用于 tracing 关联）
    pub turn_id: u64,
    /// 内核句柄（对等 RPC 用；未注入为 None）
    ///
    /// **信任边界**：完整 `Kernel` 仅授予可信注册工具（见模块文档）；
    /// 工具可经它 invoke 任意目标、emit 任意消息，不可信工具接入前必须收窄。
    pub kernel: Option<Kernel>,
    /// 工件存储句柄（大结果落库用；未注入为 None）
    ///
    /// **信任边界**：仅授予可信注册工具；工具可凭 owner 身份写工件。
    pub artifact_store: Option<Arc<dyn ArtifactStore>>,
}

impl std::fmt::Debug for ToolContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolContext")
            .field("tool_call_id", &self.tool_call_id)
            .field("session_id", &self.session_id)
            .field("turn_id", &self.turn_id)
            .field("kernel", &self.kernel.is_some())
            .field("artifact_store", &self.artifact_store.is_some())
            .finish()
    }
}

/// 工具输出 — execute 的成功返回值
///
/// `content` 为 JSON 字符串形式（厂商协议如此：tool_result message 的
/// content 字段是字符串）。调用方直接写入 `SessionMessage::ToolResult.result`。
#[derive(Debug, Clone)]
pub struct ToolOutput {
    /// 工具执行结果（JSON 字符串或纯文本，由工具自行决定）
    pub content: String,
}

impl ToolOutput {
    /// 从 JSON value 构造（常用快捷方式）
    pub fn from_json(value: &Value) -> Self {
        Self {
            content: value.to_string(),
        }
    }

    /// 从纯文本构造
    pub fn text(s: impl Into<String>) -> Self {
        Self { content: s.into() }
    }
}

/// 工具执行错误
#[derive(Debug, Clone, thiserror::Error)]
pub enum ToolError {
    /// 参数解析失败（JSON schema 不匹配等）
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),
    /// 工具内部逻辑错误
    #[error("execution error: {0}")]
    Execution(String),
    /// 执行超时
    #[error("tool execution timed out")]
    Timeout,
    /// 工具 panic（由 catch_unwind 捕获）
    #[error("tool panicked: {0}")]
    Panic(String),
}

/// 工具统一接口 — Agent Runtime 的能力扩展点
///
/// 实现要求：
/// - `execute` 必须 `Send + 'static`：可在独立 task 中并行执行
/// - `execute` 内部允许自管耗时操作（HTTP 调用、文件 I/O 等），无需额外 spawn
/// - `execute` 的 panic 由 `ToolExecutor` 在调用边界捕获，实现方无需自行 catch
/// - `input_schema` 返回 JSON Schema，由适配器转换为厂商工具声明格式
#[async_trait]
pub trait Tool: Send + Sync {
    /// 工具名称（唯一标识，注册时校验冲突）
    fn name(&self) -> &str;

    /// 工具描述（供 LLM 理解工具用途）
    fn description(&self) -> &str;

    /// 输入参数 JSON Schema
    fn input_schema(&self) -> Value;

    /// 执行工具
    ///
    /// `args` 为 LLM 生成的参数 JSON 字符串（`ToolCallFunction.arguments`）。
    /// 实现方自行解析与校验。
    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError>;

    /// 工具分类，决定执行策略（默认 Remote，保证向后兼容）
    fn category(&self) -> ToolCategory {
        ToolCategory::Remote
    }

    /// 导出为 `ToolDeclaration`（供 LLM 请求时拼装 `tools` 字段）
    fn to_declaration(&self) -> ToolDeclaration {
        ToolDeclaration {
            name: self.name().to_string(),
            description: self.description().to_string(),
            parameters: self.input_schema(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct EchoTool;

    #[async_trait]
    impl Tool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "Echoes the input"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object", "properties": {"text": {"type": "string"}}})
        }
        async fn execute(&self, _ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
            let text = args.get("text").and_then(|v| v.as_str()).unwrap_or("");
            Ok(ToolOutput::text(text.to_string()))
        }
    }

    #[tokio::test]
    async fn echo_tool_basic() {
        let tool = EchoTool;
        let ctx = ToolContext {
            tool_call_id: "tc_1".into(),
            session_id: uuid::Uuid::new_v4(),
            turn_id: 0,
            kernel: None,
            artifact_store: None,
        };
        let result = tool.execute(ctx, json!({"text": "hello"})).await.unwrap();
        assert_eq!(result.content, "hello");
    }

    #[test]
    fn to_declaration_format() {
        let tool = EchoTool;
        let decl = tool.to_declaration();
        assert_eq!(decl.name, "echo");
        assert_eq!(decl.description, "Echoes the input");
    }

    #[tokio::test]
    async fn tool_output_from_json() {
        let out = ToolOutput::from_json(&json!({"key": "value"}));
        assert!(out.content.contains("key"));
        assert!(out.content.contains("value"));
    }
}

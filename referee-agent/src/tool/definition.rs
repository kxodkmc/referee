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

use async_trait::async_trait;
use serde_json::Value;

use crate::provider::ToolDeclaration;

/// 工具执行上下文 — 预留扩展点
///
/// Phase 2 仅携带 `tool_call_id` 与 `session_id`，供工具日志关联。
/// 后续 Phase 可注入 `KernelView`（emit 能力）、ArtifactStore 句柄等，
/// 但 `Tool` trait 不感知具体注入内容，保持解耦。
#[derive(Debug, Clone)]
pub struct ToolContext {
    /// 触发此次工具调用的 `ToolCall.id`（LLM 生成的，用于结果回传匹配）
    pub tool_call_id: String,
    /// 所属会话 ID（用于 tracing 关联）
    pub session_id: uuid::Uuid,
    /// 所属轮次 ID（用于 tracing 关联）
    pub turn_id: u64,
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

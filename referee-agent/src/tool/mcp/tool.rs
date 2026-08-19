//! MCP 工具适配 — 将 MCP 服务器工具映射为 base 的 `Tool` trait
//!
//! ## 职责
//! - [`McpToolClient`]：单个 MCP 工具的 `Tool` 适配（Remote 分类，默认等待）
//! - MRTR 处理：`InputRequiredResult` 三种策略（拒绝 / 自动填充 / 上抛）
//! - 发现映射：`McpServer` 拉取 `tools/list` 声明并批量构造 `McpToolClient`
//!
//! ## 信任边界
//! MCP 工具声明（名称/描述/schema）来自不可信服务器，仅作 `Tool` 元数据透传，
//! 不作为授权依据；执行经 `ToolExecutor` 的 `catch_unwind` + 超时兜底隔离。

use std::sync::Arc;

use async_trait::async_trait;
use referee_ai::tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
use serde_json::Value;

use crate::tool::mcp::client::McpClient;
use crate::tool::mcp::protocol::{McpToolSchema, ToolCallResult};

/// MRTR 重试上限（防止输入循环失控）
const MAX_INPUT_ROUNDS: usize = 3;

/// MRTR 处理策略 — 服务器返回 `InputRequiredResult` 时的行为
#[derive(Clone)]
pub enum MrtrStrategy {
    /// 拒绝：直接返回执行错误（自动化场景，无人工介入）
    Reject,
    /// 自动填充：用回调从上下文/记忆提取答案；提取不到则退回拒绝
    Autofill(Arc<dyn Fn(&Value) -> Option<Value> + Send + Sync>),
    /// 上抛：返回带 `mcp_input_required` 哨兵前缀的错误，交由上层决策
    /// （`mcp_input_required:<tool>:<summary>`，供 Human-in-the-loop 预留）
    Escalate,
}

impl Default for MrtrStrategy {
    fn default() -> Self {
        MrtrStrategy::Reject
    }
}

impl std::fmt::Debug for MrtrStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MrtrStrategy::Reject => f.write_str("Reject"),
            MrtrStrategy::Autofill(_) => f.write_str("Autofill"),
            MrtrStrategy::Escalate => f.write_str("Escalate"),
        }
    }
}

/// 单个 MCP 工具 — `Tool` trait 适配
pub struct McpToolClient {
    client: McpClient,
    name: String,
    description: String,
    input_schema: Value,
    mrtr: MrtrStrategy,
}

impl std::fmt::Debug for McpToolClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpToolClient")
            .field("name", &self.name)
            .field("mrtr", &self.mrtr)
            .finish()
    }
}

impl McpToolClient {
    /// 从 MCP 工具声明构造（共享同一 `McpClient`）
    pub fn new(client: McpClient, schema: McpToolSchema, mrtr: MrtrStrategy) -> Self {
        Self {
            client,
            name: schema.name,
            description: schema.description,
            input_schema: schema.input_schema,
            mrtr,
        }
    }

    /// 覆盖 MRTR 策略
    pub fn with_mrtr(mut self, mrtr: MrtrStrategy) -> Self {
        self.mrtr = mrtr;
        self
    }

    /// 优雅停机：关闭共享下的子进程传输（多个 MCP 工具共享同一 `McpClient`，
    /// 底层 `StdioTransport::shutdown` 以 `child.take()` 收敛为一次有效关闭，幂等）。
    pub async fn shutdown(&self) {
        self.client.shutdown().await;
    }
}

#[async_trait]
impl Tool for McpToolClient {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> &str {
        &self.description
    }

    fn input_schema(&self) -> Value {
        self.input_schema.clone()
    }

    /// 外部 IO（子进程传输），受 `ToolExecutor` Semaphore 限流
    fn category(&self) -> ToolCategory {
        ToolCategory::Remote
    }

    /// MCP 工具为外部调用，默认同步等待结果
    fn default_wait(&self) -> bool {
        true
    }

    async fn execute(&self, _ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        let arguments = args;
        let mut pending_input: Option<(Value, String)> = None;

        for round in 0..=MAX_INPUT_ROUNDS {
            let outcome = match &pending_input {
                Some((responses, state)) => {
                    self.client
                        .call_tool_with_inputs(&self.name, arguments.clone(), responses.clone(), state)
                        .await
                }
                None => self.client.call_tool(&self.name, arguments.clone()).await,
            };

            match outcome {
                Ok(ToolCallResult::Complete { content, structured }) => {
                    let text = crate::tool::mcp::protocol::render_content(&content, structured.as_ref());
                    return Ok(ToolOutput::text(text));
                }
                Ok(ToolCallResult::Error { message }) => {
                    return Err(ToolError::Execution(format!(
                        "mcp tool '{}' error: {message}",
                        self.name
                    )));
                }
                Ok(ToolCallResult::InputRequired { input_requests, request_state }) => {
                    match &self.mrtr {
                        MrtrStrategy::Reject => {
                            return Err(ToolError::Execution(format!(
                                "mcp tool '{}' requires input: {}",
                                self.name,
                                summarize_inputs(&input_requests)
                            )));
                        }
                        MrtrStrategy::Escalate => {
                            return Err(ToolError::Execution(format!(
                                "mcp_input_required:{}:{}",
                                self.name,
                                summarize_inputs(&input_requests)
                            )));
                        }
                        MrtrStrategy::Autofill(fill) => {
                            if round >= MAX_INPUT_ROUNDS {
                                return Err(ToolError::Execution(format!(
                                    "mcp tool '{}' input rounds exhausted",
                                    self.name
                                )));
                            }
                            match fill(&input_requests) {
                                Some(responses) => {
                                    pending_input = Some((responses, request_state));
                                }
                                None => {
                                    return Err(ToolError::Execution(format!(
                                        "mcp tool '{}' requires input: {}",
                                        self.name,
                                        summarize_inputs(&input_requests)
                                    )));
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(ToolError::Execution(format!(
                        "mcp tool '{}' failed: {e}",
                        self.name
                    )));
                }
            }
        }

        Err(ToolError::Execution(format!(
            "mcp tool '{}' input rounds exhausted",
            self.name
        )))
    }
}

/// 将 `inputRequests` 摘要为可读文本（错误信息用）
fn summarize_inputs(input_requests: &Value) -> String {
    match input_requests {
        Value::Object(map) => {
            let keys: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
            if keys.is_empty() {
                "no input requests".to_string()
            } else {
                keys.join(", ")
            }
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn summarize_requests() {
        let v = json!({"github_login": {}, "api_key": {}});
        let s = summarize_inputs(&v);
        assert!(s.contains("github_login"));
        assert!(s.contains("api_key"));
    }

    #[test]
    fn summarize_empty() {
        assert_eq!(summarize_inputs(&json!({})), "no input requests");
    }
}
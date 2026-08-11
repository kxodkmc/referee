//! 工具执行器 — 并行执行 + 截断 + panic 隔离 + 超时
//!
//! ## 设计约束
//! - **并行有上限**：`Semaphore` 限制并发数；`max_per_turn` 限制每轮工具数
//! - **截断策略**：前 N 个执行，多余的发引导错误消息（引导 LLM 下轮分批）
//! - **panic 隔离**：每个 `execute` 调用包 `catch_unwind`，panic 转为 `ToolError::Panic`
//! - **超时治理**：每个工具独立 `tokio::time::timeout`
//! - **结果返回**：`execute_batch` 返回 `Vec<ExecutedTool>`，调用方异步 emit

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::FutureExt;
use serde_json::Value;
use tokio::sync::Semaphore;
use tracing::{debug, instrument, warn};

use crate::provider::ToolCall;
use crate::tool::{Tool, ToolContext, ToolError};

/// 执行器配置
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// 每轮最大工具调用数（截断阈值）
    pub max_per_turn: usize,
    /// 单个工具执行超时
    pub tool_timeout: Duration,
    /// 并行执行并发上限
    pub max_concurrency: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            max_per_turn: 10,
            tool_timeout: Duration::from_secs(30),
            max_concurrency: 5,
        }
    }
}

/// 工具执行结果（回写给会话的消息载荷）
#[derive(Debug, Clone)]
pub struct ExecutedTool {
    pub tool_call_id: String,
    pub result: String,
}

/// 工具执行器 — 无状态，可跨 Session 共享
///
/// 持有配置和 Semaphore，不持有任何可变状态。
/// 每次 `execute_batch` 创建独立的 permit 上下文。
#[derive(Clone)]
pub struct ToolExecutor {
    config: ExecutorConfig,
    semaphore: Arc<Semaphore>,
}

impl std::fmt::Debug for ToolExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolExecutor")
            .field("max_per_turn", &self.config.max_per_turn)
            .field("tool_timeout", &self.config.tool_timeout)
            .field("max_concurrency", &self.config.max_concurrency)
            .finish()
    }
}

impl ToolExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
        Self { config, semaphore }
    }

    /// 从默认配置构造
    pub fn with_defaults() -> Self {
        Self::new(ExecutorConfig::default())
    }

    /// 配置引用
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// 批量执行工具调用，返回所有结果（成功的 + 截断的）
    ///
    /// - 前 `max_per_turn` 个调用并行执行
    /// - 超出部分返回截断错误消息（引导 LLM 下轮重发）
    /// - 每个调用独立 `catch_unwind` + `timeout`
    pub async fn execute_batch(
        &self,
        tool_calls: Vec<ToolCall>,
        registry: &crate::tool::ToolRegistry,
        session_id: uuid::Uuid,
        turn_id: u64,
    ) -> Vec<ExecutedTool> {
        if tool_calls.is_empty() {
            return Vec::new();
        }

        let max = self.config.max_per_turn;
        let (to_execute, truncated) = if tool_calls.len() > max {
            warn!(
                count = tool_calls.len(),
                max, "tool calls exceed per-turn limit, truncating"
            );
            let (head, tail) = tool_calls.split_at(max);
            (head.to_vec(), tail.to_vec())
        } else {
            (tool_calls, vec![])
        };

        // 截断项：生成引导消息
        let mut results: Vec<ExecutedTool> = truncated
            .iter()
            .map(|tc| ExecutedTool {
                tool_call_id: tc.id.clone(),
                result: format!(
                    "Exceeds max_tools_per_turn limit ({}). \
                     Please re-issue this tool call in the next turn.",
                    max
                ),
            })
            .collect();

        // 并行执行
        let futures: Vec<_> = to_execute
            .into_iter()
            .map(|tc| {
                let sem = self.semaphore.clone();
                let registry = registry.clone();
                let timeout = self.config.tool_timeout;
                async move {
                    execute_single(&registry, tc, session_id, turn_id, timeout, sem).await
                }
            })
            .collect();

        let executed = join_all(futures).await;
        results.extend(executed);
        results
    }
}

/// 执行单个工具调用（含 permit 获取 + panic 隔离 + 超时）
#[instrument(skip(registry, tc, sem), fields(tool_call_id = %tc.id, tool_name = %tc.function.name))]
async fn execute_single(
    registry: &crate::tool::ToolRegistry,
    tc: ToolCall,
    session_id: uuid::Uuid,
    turn_id: u64,
    timeout: Duration,
    sem: Arc<Semaphore>,
) -> ExecutedTool {
    let tool_call_id = tc.id.clone();
    let tool_name = tc.function.name.clone();

    // 解析参数
    let args: Value = serde_json::from_str(&tc.function.arguments).unwrap_or_else(|e| {
        warn!(error = %e, tool = %tool_name, "failed to parse tool arguments");
        Value::Null
    });

    // 查找工具
    let tool: Arc<dyn Tool> = match registry.get(&tool_name) {
        Some(t) => t,
        None => {
            return ExecutedTool {
                tool_call_id,
                result: format!("Tool '{}' not found", tool_name),
            };
        }
    };

    // 获取并发 permit
    let _permit = match sem.acquire_owned().await {
        Ok(p) => p,
        Err(_) => {
            return ExecutedTool {
                tool_call_id,
                result: "Failed to acquire concurrency permit".to_string(),
            };
        }
    };

    let ctx = ToolContext {
        tool_call_id: tool_call_id.clone(),
        session_id,
        turn_id,
    };

    // 执行（panic 隔离 + 超时）
    let result =
        AssertUnwindSafe(async { tokio::time::timeout(timeout, tool.execute(ctx, args)).await })
            .catch_unwind()
            .await;

    let content = match result {
        Ok(Ok(Ok(output))) => output.content,
        Ok(Ok(Err(e))) => {
            warn!(error = %e, tool = %tool_name, "tool execution failed");
            format!("{}", e)
        }
        Ok(Err(_)) => {
            warn!(tool = %tool_name, "tool execution timed out");
            format!("{}", ToolError::Timeout)
        }
        Err(panic_payload) => {
            let msg = panic_message(panic_payload);
            warn!(tool = %tool_name, panic = %msg, "tool execution panicked");
            format!("{}", ToolError::Panic(msg))
        }
    };

    debug!(tool = %tool_name, content_len = content.len(), "tool executed");

    ExecutedTool {
        tool_call_id,
        result: content,
    }
}

/// 提取 panic 消息
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::registry::{RegistryConfig, ToolRegistry};
    use crate::tool::ToolOutput;
    use async_trait::async_trait;
    use serde_json::json;

    struct SlowTool {
        name: String,
        delay_ms: u64,
        result: String,
    }

    #[async_trait]
    impl Tool for SlowTool {
        fn name(&self) -> &str {
            &self.name
        }
        fn description(&self) -> &str {
            "slow tool"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        async fn execute(&self, _ctx: ToolContext, _args: Value) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            Ok(ToolOutput::text(self.result.clone()))
        }
    }

    struct PanicTool;

    #[async_trait]
    impl Tool for PanicTool {
        fn name(&self) -> &str {
            "panic"
        }
        fn description(&self) -> &str {
            "panics"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        async fn execute(&self, _ctx: ToolContext, _args: Value) -> Result<ToolOutput, ToolError> {
            panic!("boom");
        }
    }

    fn make_registry(tools: Vec<Arc<dyn Tool>>) -> ToolRegistry {
        let reg = ToolRegistry::new(RegistryConfig { max_tools: 64 });
        for t in tools {
            reg.register(t).unwrap();
        }
        reg
    }

    fn make_call(name: &str, args: &str) -> ToolCall {
        ToolCall {
            id: format!("tc_{}", name),
            function: crate::provider::ToolCallFunction {
                name: name.to_string(),
                arguments: args.to_string(),
            },
        }
    }

    #[tokio::test]
    async fn parallel_execution() {
        let reg = make_registry(vec![
            Arc::new(SlowTool {
                name: "a".into(),
                delay_ms: 50,
                result: "a_done".into(),
            }),
            Arc::new(SlowTool {
                name: "b".into(),
                delay_ms: 50,
                result: "b_done".into(),
            }),
        ]);
        let exec = ToolExecutor::with_defaults();
        let calls = vec![make_call("a", "{}"), make_call("b", "{}")];
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0)
            .await;

        assert_eq!(results.len(), 2);
        let ids: Vec<_> = results.iter().map(|r| r.tool_call_id.as_str()).collect();
        assert!(ids.contains(&"tc_a"));
        assert!(ids.contains(&"tc_b"));
    }

    #[tokio::test]
    async fn truncation() {
        let reg = make_registry(vec![Arc::new(SlowTool {
            name: "a".into(),
            delay_ms: 10,
            result: "ok".into(),
        })]);
        let exec = ToolExecutor::new(ExecutorConfig {
            max_per_turn: 2,
            tool_timeout: Duration::from_secs(5),
            max_concurrency: 5,
        });
        let calls: Vec<ToolCall> = (0..5)
            .map(|i| make_call("a", &format!("{{\"i\":{}}}", i)))
            .collect();
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0)
            .await;

        assert_eq!(results.len(), 5); // 2 executed + 3 truncated
        let truncated: Vec<_> = results
            .iter()
            .filter(|r| r.result.contains("Exceeds max_tools_per_turn"))
            .collect();
        assert_eq!(truncated.len(), 3);
    }

    #[tokio::test]
    async fn panic_isolation() {
        let reg = make_registry(vec![
            Arc::new(PanicTool),
            Arc::new(SlowTool {
                name: "ok".into(),
                delay_ms: 10,
                result: "ok_result".into(),
            }),
        ]);
        let exec = ToolExecutor::with_defaults();
        let calls = vec![make_call("panic", "{}"), make_call("ok", "{}")];
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0)
            .await;

        assert_eq!(results.len(), 2);
        let panic_result = results
            .iter()
            .find(|r| r.tool_call_id == "tc_panic")
            .unwrap();
        assert!(panic_result.result.contains("panicked"));
        let ok_result = results.iter().find(|r| r.tool_call_id == "tc_ok").unwrap();
        assert_eq!(ok_result.result, "ok_result");
    }

    #[tokio::test]
    async fn timeout_isolation() {
        let reg = make_registry(vec![Arc::new(SlowTool {
            name: "slow".into(),
            delay_ms: 200,
            result: "ok".into(),
        })]);
        let exec = ToolExecutor::new(ExecutorConfig {
            max_per_turn: 10,
            tool_timeout: Duration::from_millis(50),
            max_concurrency: 5,
        });
        let calls = vec![make_call("slow", "{}")];
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0)
            .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].result.contains("timed out"));
    }

    #[tokio::test]
    async fn tool_not_found() {
        let reg = ToolRegistry::with_defaults();
        let exec = ToolExecutor::with_defaults();
        let calls = vec![make_call("nonexistent", "{}")];
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0)
            .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].result.contains("not found"));
    }
}

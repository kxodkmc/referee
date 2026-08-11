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
use referee_core::Kernel;
use serde_json::Value;
use tokio::sync::Semaphore;
use tracing::{debug, instrument, warn};

use crate::artifact::ArtifactStore;
use crate::provider::ToolCall;
use crate::tool::{Tool, ToolCategory, ToolContext, ToolError};

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
///
/// Phase 3：可注入 `kernel` / `artifact_store` 以启用对等能力
/// （构造 `ToolContext` 时透传给所有工具，本地工具无感知）。
#[derive(Clone)]
pub struct ToolExecutor {
    config: ExecutorConfig,
    semaphore: Arc<Semaphore>,
    /// 对等 RPC 能力（`AgentTool` 经 `kernel.invoke` 调用目标 Agent）
    kernel: Option<Kernel>,
    /// 工件存储能力（大结果落库 + ACL）
    artifact_store: Option<Arc<dyn ArtifactStore>>,
}

impl std::fmt::Debug for ToolExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolExecutor")
            .field("max_per_turn", &self.config.max_per_turn)
            .field("tool_timeout", &self.config.tool_timeout)
            .field("max_concurrency", &self.config.max_concurrency)
            .field("kernel", &self.kernel.is_some())
            .field("artifact_store", &self.artifact_store.is_some())
            .finish()
    }
}

impl ToolExecutor {
    pub fn new(config: ExecutorConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrency));
        Self {
            config,
            semaphore,
            kernel: None,
            artifact_store: None,
        }
    }

    /// 从默认配置构造
    pub fn with_defaults() -> Self {
        Self::new(ExecutorConfig::default())
    }

    /// 注入内核句柄（启用对等 RPC 能力）
    pub fn with_kernel(mut self, kernel: Kernel) -> Self {
        self.kernel = Some(kernel);
        self
    }

    /// 注入工件存储（启用大结果落库能力）
    pub fn with_artifact_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self
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
        let kernel = self.kernel.clone();
        let artifact_store = self.artifact_store.clone();
        let futures: Vec<_> = to_execute
            .into_iter()
            .map(|tc| {
                let sem = self.semaphore.clone();
                let registry = registry.clone();
                let timeout = self.config.tool_timeout;
                let kernel = kernel.clone();
                let artifact_store = artifact_store.clone();
                async move {
                    execute_single(
                        &registry,
                        tc,
                        session_id,
                        turn_id,
                        timeout,
                        sem,
                        kernel,
                        artifact_store,
                    )
                    .await
                }
            })
            .collect();

        let executed = join_all(futures).await;
        results.extend(executed);
        results
    }
}

/// 执行单个工具调用（分类限流 + panic 隔离 + 超时）
///
/// 限流策略：`Remote` 工具获取 Semaphore permit（外部 IO 并发上限）；
/// `Local` 工具（如 AgentTool）不占槽位，直接执行——避免对等调用
/// 相互等待外部 IO 槽位的资源池死锁。
#[instrument(skip(registry, tc, sem, kernel, artifact_store), fields(tool_call_id = %tc.id, tool_name = %tc.function.name))]
#[allow(clippy::too_many_arguments)]
async fn execute_single(
    registry: &crate::tool::ToolRegistry,
    tc: ToolCall,
    session_id: uuid::Uuid,
    turn_id: u64,
    timeout: Duration,
    sem: Arc<Semaphore>,
    kernel: Option<Kernel>,
    artifact_store: Option<Arc<dyn ArtifactStore>>,
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

    // 按分类获取并发 permit：Remote 受限流，Local 不占槽位
    let _permit = if tool.category() == ToolCategory::Remote {
        match sem.acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => {
                return ExecutedTool {
                    tool_call_id,
                    result: "Failed to acquire concurrency permit".to_string(),
                };
            }
        }
    } else {
        None
    };

    let ctx = ToolContext {
        tool_call_id: tool_call_id.clone(),
        session_id,
        turn_id,
        kernel,
        artifact_store,
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

    /// Local 工具 — 声明为 Local 分类，不占 Semaphore 槽位
    struct LocalSlowTool {
        name: String,
        delay_ms: u64,
    }

    #[async_trait]
    impl Tool for LocalSlowTool {
        fn name(&self) -> &str {
            &self.name
        }
        fn description(&self) -> &str {
            "local slow tool"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        fn category(&self) -> ToolCategory {
            ToolCategory::Local
        }
        async fn execute(&self, _ctx: ToolContext, _args: Value) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
            Ok(ToolOutput::text(self.name.clone()))
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

    #[tokio::test]
    async fn local_tools_bypass_semaphore() {
        // max_concurrency=1：两个 Local 工具仍应并行（不占槽位）
        let reg = make_registry(vec![
            Arc::new(LocalSlowTool {
                name: "local_a".into(),
                delay_ms: 50,
            }),
            Arc::new(LocalSlowTool {
                name: "local_b".into(),
                delay_ms: 50,
            }),
        ]);
        let exec = ToolExecutor::new(ExecutorConfig {
            max_per_turn: 10,
            tool_timeout: Duration::from_secs(5),
            max_concurrency: 1,
        });
        let calls = vec![make_call("local_a", "{}"), make_call("local_b", "{}")];
        let start = std::time::Instant::now();
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0)
            .await;
        let elapsed = start.elapsed();

        assert_eq!(results.len(), 2);
        // 并行：50ms 双任务应远小于串行 100ms
        assert!(
            elapsed < Duration::from_millis(95),
            "local tools should run in parallel, took {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn remote_tools_respect_semaphore() {
        // max_concurrency=1：两个 Remote 工具串行执行（受槽位限制）
        let reg = make_registry(vec![
            Arc::new(SlowTool {
                name: "r_a".into(),
                delay_ms: 50,
                result: "a".into(),
            }),
            Arc::new(SlowTool {
                name: "r_b".into(),
                delay_ms: 50,
                result: "b".into(),
            }),
        ]);
        let exec = ToolExecutor::new(ExecutorConfig {
            max_per_turn: 10,
            tool_timeout: Duration::from_secs(5),
            max_concurrency: 1,
        });
        let calls = vec![make_call("r_a", "{}"), make_call("r_b", "{}")];
        let start = std::time::Instant::now();
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0)
            .await;
        let elapsed = start.elapsed();

        assert_eq!(results.len(), 2);
        // 串行：两个 50ms 任务至少 ~100ms
        assert!(
            elapsed >= Duration::from_millis(90),
            "remote tools should be serialized by semaphore, took {:?}",
            elapsed
        );
    }
}

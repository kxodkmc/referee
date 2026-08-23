//! 工具执行器 — 并行执行 + 截断 + panic 隔离 + 超时 + 等待/派发分流
//!
//! ## 设计约束
//! - **并行有上限**：`Semaphore` 限制并发数；`max_per_turn` 限制每轮工具数
//! - **截断策略**：调用方先 `truncate`，多余的发引导错误消息（引导 LLM 下轮分批）
//! - **panic 隔离**：每个 `execute` 调用包 `catch_unwind`，panic 转为 `ToolError::Panic`
//! - **超时治理**：每个工具独立 `tokio::time::timeout`；等待类批次另有总 deadline
//!   （`execute_batch` 的 `batch_deadline`，未完成项超时收敛，绝不无限等待）
//! - **等待/派发分流**：`split_by_wait` 按保留参数 `wait`（或工具 `default_wait`）
//!   拆分——等待类走 `execute_batch` 同步收敛；派发类走 `dispatch_batch` 后台执行，
//!   完成结果由调用方注入（入队，不阻塞主智能体）
//! - **结果返回**：`execute_batch` / `dispatch_batch` 返回 `ExecutedTool`，调用方异步回写

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use referee_core::Kernel;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Semaphore;
use tracing::{debug, instrument, warn};

use crate::provider::ToolCall;
use crate::store::Store;
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

/// 工具执行结果分类 — 数据出口，供程序化消费（observer 回调 / 上层策略）
///
/// 穷尽 executor 全部收敛分支：
/// - 工具正常返回（含主动 `Err(ToolError)`，错误文本保留在 `result`）→ `Ok`
/// - 参数解析失败不短路：降级为 Null 参数继续执行，结局由实际执行结果承载
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcome {
    /// 正常完成（含工具主动报错的可观测完成，非崩溃）
    Ok,
    /// 单工具执行超时
    Timeout,
    /// 工具或执行段 panic（含 pre-execute panic / 后台任务 join 失败）
    Panic,
    /// 工具未注册
    NotFound,
    /// 并发许可获取失败
    PermitUnavailable,
    /// 等待类批次总 deadline 收敛（区别于单工具超时）
    BatchDeadline,
}

/// 工具执行结果（回写给会话的消息载荷）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutedTool {
    pub tool_call_id: String,
    /// 工具名（异步派发完成时用于生成注入文本）
    pub tool_name: String,
    pub result: String,
    /// 执行结果分类（结构化数据出口，替代字符串折叠）
    pub outcome: ToolOutcome,
    /// 执行耗时（毫秒，含许可等待）
    pub duration_ms: u64,
}

/// 工具执行器 — 无状态，可跨 Session 共享
///
/// 持有配置和 Semaphore，不持有任何可变状态。
/// 每次 `execute_batch` 创建独立的 permit 上下文。
///
/// 执行器无状态，可跨 Session 共享；每次 `execute_batch` 创建独立 permit 上下文。
/// 可选注入 `kernel` / `store` 以启用对等 RPC 与通用落库能力
/// （构造 `ToolContext` 时透传给所有工具，本地工具无感知）。
#[derive(Clone)]
pub struct ToolExecutor {
    config: ExecutorConfig,
    semaphore: Arc<Semaphore>,
    /// 对等 RPC 能力（上层工具经 `kernel.invoke` 调用目标能力）
    kernel: Option<Kernel>,
    /// 通用 KV 存储能力（成果/大结果落库）
    store: Option<Arc<dyn Store>>,
}

impl std::fmt::Debug for ToolExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolExecutor")
            .field("max_per_turn", &self.config.max_per_turn)
            .field("tool_timeout", &self.config.tool_timeout)
            .field("max_concurrency", &self.config.max_concurrency)
            .field("kernel", &self.kernel.is_some())
            .field("store", &self.store.is_some())
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
            store: None,
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

    /// 注入通用 KV 存储（启用成果/大结果落库能力）
    pub fn with_store(mut self, store: Arc<dyn Store>) -> Self {
        self.store = Some(store);
        self
    }

    /// 配置引用
    pub fn config(&self) -> &ExecutorConfig {
        &self.config
    }

    /// 按 `max_per_turn` 截断：返回 (head 执行, tail 截断)。截断项由调用方
    /// 生成引导错误消息（职责与执行分离，分流前统一截断一次）。
    pub fn truncate(&self, tool_calls: Vec<ToolCall>) -> (Vec<ToolCall>, Vec<ToolCall>) {
        if tool_calls.len() <= self.config.max_per_turn {
            return (tool_calls, Vec::new());
        }
        let (head, tail) = tool_calls.split_at(self.config.max_per_turn);
        (head.to_vec(), tail.to_vec())
    }

    /// 按等待决策拆分：返回 (等待类, 派发类)
    ///
    /// 每个调用解析保留参数 `wait`（LLM 显式选择）；未传则取工具 `default_wait()`；
    /// 工具不存在时按等待处理（同步返回 not found，避免异步悬空）。
    pub fn split_by_wait(
        &self,
        tool_calls: Vec<ToolCall>,
        registry: &crate::tool::ToolRegistry,
    ) -> (Vec<ToolCall>, Vec<ToolCall>) {
        let (mut waiting, mut dispatched) = (Vec::new(), Vec::new());
        for tc in tool_calls {
            if wants_wait(&tc, registry) {
                waiting.push(tc);
            } else {
                dispatched.push(tc);
            }
        }
        (waiting, dispatched)
    }

    /// 批量执行工具调用，返回所有结果（仅等待类；调用方应先 `truncate`）
    ///
    /// - 全部并行执行，逐项收敛（每个独立 `catch_unwind` + 单工具 `timeout`）
    /// - `batch_deadline`：批次总 deadline（引擎侧传 `awaiting_calls_timeout`）。
    ///   到达时已完成项保留真实结果，未完成项以超时收敛消息合成结果
    ///   （在飞 future 就地取消），每项输入恰对应一项输出，调用方无部分失败感知
    /// - `max_concurrency`：可选并发上限；`Some(1)` 强制串行（厂商不支持并行工具时降级），
    ///   `None` 沿用执行器默认 `max_concurrency`
    pub async fn execute_batch(
        &self,
        tool_calls: Vec<ToolCall>,
        registry: &crate::tool::ToolRegistry,
        session_id: uuid::Uuid,
        turn_id: u64,
        peer_depth: u32,
        max_concurrency: Option<usize>,
        batch_deadline: Duration,
    ) -> Vec<ExecutedTool> {
        if tool_calls.is_empty() {
            return Vec::new();
        }

        let sem = max_concurrency
            .map(|n| Arc::new(Semaphore::new(n)))
            .unwrap_or_else(|| self.semaphore.clone());

        let kernel = self.kernel.clone();
        let store = self.store.clone();
        let deadline = tokio::time::Instant::now() + batch_deadline;
        let batch_started = Instant::now();

        // 未完成项登记（id → 工具名；deadline 到达时合成超时收敛结果）
        let mut outstanding: HashMap<String, String> = tool_calls
            .iter()
            .map(|tc| (tc.id.clone(), tc.function.name.clone()))
            .collect();

        let mut futures: FuturesUnordered<_> = tool_calls
            .into_iter()
            .map(|tc| {
                let sem = sem.clone();
                let registry = registry.clone();
                let timeout = self.config.tool_timeout;
                let kernel = kernel.clone();
                let store = store.clone();
                async move {
                    guarded_execute(
                        &registry, tc, session_id, turn_id, peer_depth, timeout, sem, kernel, store,
                    )
                    .await
                }
            })
            .collect();

        let mut results = Vec::with_capacity(outstanding.len());
        loop {
            if futures.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                r = futures.next() => {
                    // 循环顶已拦截空集，此处必为 Some
                    let r = r.expect("non-empty FuturesUnordered yields Some");
                    outstanding.remove(&r.tool_call_id);
                    results.push(r);
                }
                _ = tokio::time::sleep_until(deadline) => {
                    // 总 deadline：未完成项合成超时收敛消息，在飞 future 就地取消
                    let unfinished = outstanding.len();
                    warn!(
                        remaining = unfinished,
                        deadline_ms = batch_deadline.as_millis() as u64,
                        "waiting-tool batch deadline exceeded"
                    );
                    for (tool_call_id, tool_name) in outstanding.drain() {
                        results.push(ExecutedTool {
                            tool_call_id,
                            tool_name,
                            result: BATCH_DEADLINE_MESSAGE.to_string(),
                            outcome: ToolOutcome::BatchDeadline,
                            duration_ms: batch_started.elapsed().as_millis() as u64,
                        });
                    }
                    break;
                }
            }
        }
        results
    }

    /// 派发一批工具调用（仅派发类）— 后台执行，立即返回句柄，不阻塞调用方
    ///
    /// 每个调用 spawn 独立 task（复用 `execute_single` 的限流/隔离/超时）。
    /// 调用方 await 句柄取得结果后自行注入（入队，等待下一次模型调用合并）。
    pub fn dispatch_batch(
        &self,
        tool_calls: Vec<ToolCall>,
        registry: &crate::tool::ToolRegistry,
        session_id: uuid::Uuid,
        turn_id: u64,
        peer_depth: u32,
    ) -> Vec<tokio::task::JoinHandle<ExecutedTool>> {
        let kernel = self.kernel.clone();
        let store = self.store.clone();
        tool_calls
            .into_iter()
            .map(|tc| {
                let sem = self.semaphore.clone();
                let registry = registry.clone();
                let timeout = self.config.tool_timeout;
                let kernel = kernel.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    guarded_execute(
                        &registry, tc, session_id, turn_id, peer_depth, timeout, sem, kernel, store,
                    )
                    .await
                })
            })
            .collect()
    }
}

/// 等待类批次总 deadline 到达时，未完成项的收敛消息（回写会话，下一轮对模型可见）
const BATCH_DEADLINE_MESSAGE: &str =
    "Timed out: waiting-tool batch deadline exceeded. Re-issue this tool call in the next turn.";

/// 单个工具调用的保护执行：外层 `catch_unwind` 兜底 pre-execute 段
/// （参数解析 / 注册查找 / 并发槽位获取）的 panic。
///
/// `execute_single` 内部已对 `tool.execute` 单独 catch_unwind；此处再包一层
/// 保证同步与派发路径一致：任何 panic 都收敛为带 `tool_call_id` / `tool_name`
/// 的错误结果，不静默丢失调用身份。
#[allow(clippy::too_many_arguments)]
async fn guarded_execute(
    registry: &crate::tool::ToolRegistry,
    tc: ToolCall,
    session_id: uuid::Uuid,
    turn_id: u64,
    peer_depth: u32,
    timeout: Duration,
    sem: Arc<Semaphore>,
    kernel: Option<Kernel>,
    store: Option<Arc<dyn Store>>,
) -> ExecutedTool {
    let tool_call_id = tc.id.clone();
    let tool_name = tc.function.name.clone();
    let started = Instant::now();
    AssertUnwindSafe(async {
        execute_single(
            registry, tc, session_id, turn_id, peer_depth, timeout, sem, kernel, store,
        )
        .await
    })
    .catch_unwind()
    .await
    .unwrap_or_else(|_| ExecutedTool {
        tool_call_id,
        tool_name,
        result: format!("{}", ToolError::Panic("<pre-execute panic>".into())),
        outcome: ToolOutcome::Panic,
        duration_ms: started.elapsed().as_millis() as u64,
    })
}

/// 解析单个工具调用的等待决策：保留参数 `wait` > 工具 `default_wait` > 默认不等待。
/// 工具不存在时按等待处理（同步返回 not found，避免异步悬空）。
fn wants_wait(tc: &ToolCall, registry: &crate::tool::ToolRegistry) -> bool {
    if let Ok(args) = serde_json::from_str::<Value>(&tc.function.arguments) {
        if let Some(w) = args.get("wait").and_then(|v| v.as_bool()) {
            return w;
        }
    }
    registry
        .get(&tc.function.name)
        .map(|t| t.default_wait())
        .unwrap_or(true)
}

/// 从参数中剥离引擎保留参数 `wait`（不传给工具实现）
fn strip_wait(args: &mut Value) {
    if let Some(obj) = args.as_object_mut() {
        obj.remove("wait");
    }
}

/// 执行单个工具调用（分类限流 + panic 隔离 + 超时）
///
/// 限流策略：`Remote` 工具获取 Semaphore permit（外部 IO 并发上限）；
/// `Local` 工具（如 AgentTool）不占槽位，直接执行——避免对等调用
/// 相互等待外部 IO 槽位的资源池死锁。
#[instrument(skip(registry, tc, sem, kernel, store), fields(tool_call_id = %tc.id, tool_name = %tc.function.name))]
#[allow(clippy::too_many_arguments)]
async fn execute_single(
    registry: &crate::tool::ToolRegistry,
    tc: ToolCall,
    session_id: uuid::Uuid,
    turn_id: u64,
    peer_depth: u32,
    timeout: Duration,
    sem: Arc<Semaphore>,
    kernel: Option<Kernel>,
    store: Option<Arc<dyn Store>>,
) -> ExecutedTool {
    let tool_call_id = tc.id.clone();
    let tool_name = tc.function.name.clone();
    let started = Instant::now();

    // 解析参数 + 剥离保留参数 wait（解析失败降级 Null 继续，结局由执行结果承载）
    let mut args: Value = serde_json::from_str(&tc.function.arguments).unwrap_or_else(|e| {
        warn!(error = %e, tool = %tool_name, "failed to parse tool arguments");
        Value::Null
    });
    strip_wait(&mut args);

    // 查找工具
    let tool: Arc<dyn Tool> = match registry.get(&tool_name) {
        Some(t) => t,
        None => {
            return ExecutedTool {
                tool_call_id,
                tool_name: tool_name.clone(),
                result: format!("Tool '{}' not found", tool_name),
                outcome: ToolOutcome::NotFound,
                duration_ms: started.elapsed().as_millis() as u64,
            };
        }
    };

    // 等待决策（保留参数 wait > 工具默认 > 默认不等待）
    let wait = wants_wait(&tc, registry);

    // 按分类获取并发 permit：Remote 受限流，Local 不占槽位
    let _permit = if tool.category() == ToolCategory::Remote {
        match sem.acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => {
                return ExecutedTool {
                    tool_call_id,
                    tool_name,
                    result: "Failed to acquire concurrency permit".to_string(),
                    outcome: ToolOutcome::PermitUnavailable,
                    duration_ms: started.elapsed().as_millis() as u64,
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
        store,
        wait,
        peer_depth,
    };

    // 执行（panic 隔离 + 超时）
    let result =
        AssertUnwindSafe(async { tokio::time::timeout(timeout, tool.execute(ctx, args)).await })
            .catch_unwind()
            .await;

    // 结果收敛：错误文本保留在 result，分类写入 outcome（穷尽、无通配）
    let (content, outcome) = match result {
        Ok(Ok(Ok(output))) => (output.content, ToolOutcome::Ok),
        Ok(Ok(Err(e))) => {
            warn!(error = %e, tool = %tool_name, "tool execution failed");
            // 工具主动报错：正常可观测完成，归 Ok（区别于 Timeout/Panic 崩溃类）
            (format!("{e}"), ToolOutcome::Ok)
        }
        Ok(Err(_)) => {
            warn!(tool = %tool_name, "tool execution timed out");
            (format!("{}", ToolError::Timeout), ToolOutcome::Timeout)
        }
        Err(panic_payload) => {
            let msg = panic_message(panic_payload);
            warn!(tool = %tool_name, panic = %msg, "tool execution panicked");
            (format!("{}", ToolError::Panic(msg)), ToolOutcome::Panic)
        }
    };

    debug!(tool = %tool_name, content_len = content.len(), "tool executed");

    ExecutedTool {
        tool_call_id,
        tool_name,
        result: content,
        outcome,
        duration_ms: started.elapsed().as_millis() as u64,
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
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0, 0, None, Duration::from_secs(30))
            .await;

        assert_eq!(results.len(), 2);
        let ids: Vec<_> = results.iter().map(|r| r.tool_call_id.as_str()).collect();
        assert!(ids.contains(&"tc_a"));
        assert!(ids.contains(&"tc_b"));
    }

    #[tokio::test]
    async fn truncation() {
        let exec = ToolExecutor::new(ExecutorConfig {
            max_per_turn: 2,
            tool_timeout: Duration::from_secs(5),
            max_concurrency: 5,
        });
        let calls: Vec<ToolCall> = (0..5)
            .map(|i| make_call("a", &format!("{{\"i\":{}}}", i)))
            .collect();
        let (head, tail) = exec.truncate(calls);

        assert_eq!(head.len(), 2);
        assert_eq!(tail.len(), 3); // 超出部分交由调用方生成引导错误
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
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0, 0, None, Duration::from_secs(30))
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
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0, 0, None, Duration::from_secs(30))
            .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].result.contains("timed out"));
    }

    #[tokio::test]
    async fn batch_deadline_converges_partial_results() {
        // 快工具（20ms）+ 慢工具（10s，远超批次 deadline 且不触发单工具 timeout）：
        // batch_deadline=100ms 到达时快工具保留真实结果、慢工具超时收敛
        let reg = make_registry(vec![
            Arc::new(SlowTool {
                name: "fast".into(),
                delay_ms: 20,
                result: "fast_result".into(),
            }),
            Arc::new(SlowTool {
                name: "very_slow".into(),
                delay_ms: 10_000,
                result: "never".into(),
            }),
        ]);
        let exec = ToolExecutor::new(ExecutorConfig {
            max_per_turn: 10,
            tool_timeout: Duration::from_secs(30),
            max_concurrency: 5,
        });
        let calls = vec![make_call("fast", "{}"), make_call("very_slow", "{}")];
        let start = std::time::Instant::now();
        let results = exec
            .execute_batch(
                calls,
                &reg,
                uuid::Uuid::new_v4(),
                0,
                0,
                None,
                Duration::from_millis(100),
            )
            .await;
        let elapsed = start.elapsed();

        assert_eq!(results.len(), 2, "each input yields exactly one result");
        assert!(
            elapsed < Duration::from_secs(1),
            "batch must converge at deadline, took {elapsed:?}"
        );
        let fast = results.iter().find(|r| r.tool_call_id == "tc_fast").unwrap();
        assert_eq!(fast.result, "fast_result");
        let slow = results
            .iter()
            .find(|r| r.tool_call_id == "tc_very_slow")
            .unwrap();
        assert!(
            slow.result.contains("batch deadline"),
            "unfinished item must carry deadline message: {}",
            slow.result
        );
    }

    #[tokio::test]
    async fn tool_not_found() {
        let reg = ToolRegistry::with_defaults();
        let exec = ToolExecutor::with_defaults();
        let calls = vec![make_call("nonexistent", "{}")];
        let results = exec
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0, 0, None, Duration::from_secs(30))
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
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0, 0, None, Duration::from_secs(30))
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
            .execute_batch(calls, &reg, uuid::Uuid::new_v4(), 0, 0, None, Duration::from_secs(30))
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

    /// 显式声明默认等待的工具
    struct WaitByDefaultTool;
    #[async_trait]
    impl Tool for WaitByDefaultTool {
        fn name(&self) -> &str {
            "wait_default"
        }
        fn description(&self) -> &str {
            "waits by default"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        fn default_wait(&self) -> bool {
            true
        }
        async fn execute(&self, _ctx: ToolContext, _args: Value) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("ok"))
        }
    }

    /// 记录实际收到的参数（验证保留参数 wait 已被剥离）
    struct RecordArgsTool {
        seen: Arc<parking_lot::Mutex<Option<Value>>>,
    }
    #[async_trait]
    impl Tool for RecordArgsTool {
        fn name(&self) -> &str {
            "record_args"
        }
        fn description(&self) -> &str {
            "records args"
        }
        fn input_schema(&self) -> Value {
            json!({"type": "object"})
        }
        async fn execute(&self, _ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
            *self.seen.lock() = Some(args);
            Ok(ToolOutput::text("recorded"))
        }
    }

    #[tokio::test]
    async fn split_by_wait_override_default_and_missing() {
        let reg = make_registry(vec![Arc::new(WaitByDefaultTool)]);
        let exec = ToolExecutor::with_defaults();
        let calls = vec![
            // 显式 wait:true → 等待类
            make_call("wait_default", r#"{"wait":true}"#),
            // 显式 wait:false → 派发类（覆盖工具默认）
            make_call("wait_default", r#"{"wait":false}"#),
            // 未传 + 工具 default_wait=true → 等待类
            make_call("wait_default", "{}"),
            // 未传 + 工具不存在 → 按等待处理（同步返回 not found）
            make_call("missing", "{}"),
        ];
        let (waiting, dispatched) = exec.split_by_wait(calls, &reg);
        assert_eq!(waiting.len(), 3);
        assert_eq!(dispatched.len(), 1);
        // 派发类仅显式 wait:false 的调用；wait:true / 工具默认 / 缺失工具均按等待
        assert_eq!(dispatched[0].id, "tc_wait_default");
        assert!(waiting.iter().any(|c| c.id == "tc_missing"));
    }

    #[tokio::test]
    async fn wait_override_wins_for_dispatch() {
        // 默认不等待的工具，显式 wait:false → 派发类
        let reg = make_registry(vec![Arc::new(SlowTool {
            name: "a".into(),
            delay_ms: 5,
            result: "ok".into(),
        })]);
        let exec = ToolExecutor::with_defaults();
        let calls = vec![make_call("a", r#"{"wait":false}"#)];
        let (waiting, dispatched) = exec.split_by_wait(calls, &reg);
        assert!(waiting.is_empty());
        assert_eq!(dispatched.len(), 1);
    }

    #[tokio::test]
    async fn reserved_wait_key_is_stripped_before_execute() {
        let seen = Arc::new(parking_lot::Mutex::new(None));
        let reg = make_registry(vec![Arc::new(RecordArgsTool { seen: seen.clone() })]);
        let exec = ToolExecutor::with_defaults();
        // 等待模式执行：保留参数 wait 不应传给工具实现
        let results = exec
            .execute_batch(
                vec![make_call("record_args", r#"{"x":1,"wait":true}"#)],
                &reg,
                uuid::Uuid::new_v4(),
                0,
                0,
                None,
                Duration::from_secs(30),
            )
            .await;
        assert_eq!(results.len(), 1);
        let received = seen.lock().clone().expect("tool must have run");
        assert_eq!(received.get("x"), Some(&json!(1)));
        assert!(
            received.get("wait").is_none(),
            "reserved wait key must be stripped"
        );
    }
}

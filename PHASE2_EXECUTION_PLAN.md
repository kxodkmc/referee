# Phase 2 执行方案 — 工具调用（FunctionCall 统一抽象）

> 状态：待评审 · 范围：`referee-agent` Phase 2 · 前置：Phase 0 + Phase 1 已完成
> 核心原则：**零改动 `referee-core`** · **Phase 1 行为向后兼容** · **有界、隔离、可观测**
>
> **状态快照（2026-08-12）**：本文为**历史执行方案**。P2 已按此落地（工具抽象 / 有界
> 注册表 / 并行执行 / 结果回写 / Resume 循环），并随三层重构迁至 `referee-ai-base/tool/`；
> 同步/异步派发与 `peer_depth` 嵌套限制为落地后的增强（见 PHASE_STATUS.md）。
> 文中「65 条 Phase 1 测试」为当时口径；「Phase 7 实现 MCP/Skills」已被重构决策移除。

---

## 1. 目标与边界

### 1.1 做什么

| 项 | 说明 |
|----|------|
| Tool trait | 统一的工具抽象（name / description / input_schema / execute） |
| ToolRegistry | 有界注册表（DashMap + 上限），提供工具声明导出（`ToolDeclaration`） |
| ToolExecutor | 并行执行器（Semaphore 并发上限 + 每轮 `max_per_turn` 截断 + 超时 + panic 隔离） |
| 结果回写 | 通过 `kernel.emit(self_id, ToolResult)` 消息驱动收敛入 `AwaitingCalls.pending` |
| Resume 循环 | pending 全部清空后自动 `emit(Resume)`，触发下一轮 Thinking，turn_id 递增 |
| 多轮回复 | forwarder 模式：`handle_chat` 的 ctx 存入轻量 forwarder task，最终回复经 oneshot 通道传递 |
| 向后兼容 | 未注册 ToolRegistry 时，AwaitingCalls 强制回 Idle + 立即回传（Phase 1 行为） |

### 1.2 不做什么

- **不碰 `referee-core`**：内核零改动，所有改动在 `referee-agent` 内
- **不引入新依赖**：使用已有白名单库（dashmap / tokio / futures / serde_json / tracing / metrics）
- **不做 MCP / Skills 桥接**：`tool/bridge.rs` 预留位置，Phase 7 实现
- **不做记忆 / 提示词组装 / Token 计量**：后续 Phase 职责
- **不做工具声明格式转换**：当前 `ToolDeclaration` 已兼容 OpenAI 格式；Anthropic / Responses 格式转换在 P5/P6

### 1.3 验收标准（对应 AGENT_RUNTIME_PLAN §5.3）

1. **并行**：一次 5 个调用并发执行，总耗时 ≈ max(单个) 而非 sum；全部一一回写
2. **上限**：响应 100 个 tool_calls → 截断到 `max_per_turn`，多余回写引导消息
3. **隔离**：某工具 panic → 该调用回错误结果，其余调用与注册表不受影响
4. **背压**：工具结果洪泛 → 内核有界通道满 → `ResourceExhausted` + warn 日志，无 OOM
5. **Resume 循环**：pending 全部完成后自动进入下一轮 Thinking，turn_id 递增
6. **向后兼容**：无 ToolRegistry 时 65 条 Phase 1 测试全绿、不修改
7. **中断**：AwaitingCalls 期间收 Interrupt → 直接回 Idle + Cancelled 回信
8. **AwaitingCalls 超时**：pending 项超时后自动回 Idle + Error 回信 + DLQ 记录

---

## 2. 文件结构

```
referee-agent/src/
├── lib.rs                  ← 修改：AgentRuntime 集成、handle_* 分发、spawn_turn_task 签名变更
├── provider/
│   └── mod.rs              ← 不修改（ToolCall / ToolDeclaration / FinishReason 已就绪）
├── session/
│   ├── mod.rs             ← 修改：SessionState 新增 AwaitingCalls、pending_reply、apply_tool_result
│   ├── message.rs         ← 修改：SessionReply 新增变体
│   ├── task.rs            ← 不修改（run_turn / TurnOutcome 已就绪）
│   └── timeout.rs         ← 不修改（awaiting_calls_timeout 已定义）
└── tool/                   ← 新增目录
    ├── mod.rs              ← Tool trait + ToolOutput + ToolError + ToolContext
    ├── registry.rs         ← ToolRegistry（有界注册表）
    └── executor.rs         ← ToolExecutor（并行执行 + 截断 + 隔离 + 结果回写）
```

测试：

```
referee-agent/tests/
├── session_test.rs         ← 修改：补充 AwaitingCalls 相关用例
├── tool_test.rs            ← 新增：Tool trait / Registry / Executor 单元 + 集成测试
└── common/mod.rs           ← 不修改
```

---

## 3. 核心类型定义（`tool/mod.rs`）

```rust
//! 工具调用抽象层 — Phase 2 核心交付
//!
//! 设计约束：
//! - Tool trait 是纯行为接口，不持状态
//! - 工具声明（ToolDeclaration）由 Registry 自动导出，供 ChatRequest 使用
//! - 执行隔离：每个工具调用包 catch_unwind + timeout，panic 不外泄

use std::time::Duration;
use async_trait::async_trait;
use serde_json::Value;

/// 工具执行输出
#[derive(Debug, Clone)]
pub struct ToolOutput {
    /// 返回给 LLM 的文本内容
    pub content: String,
}

impl ToolOutput {
    pub fn text(content: impl Into<String>) -> Self {
        Self { content: content.into() }
    }
}

/// 工具执行错误 — 全部映射为字符串结果回写 LLM
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("invalid arguments: {0}")]
    InvalidArgs(String),
    #[error("execution failed: {0}")]
    Execution(String),
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    #[error("tool panicked: {0}")]
    Panic(String),
}

/// 工具统一接口 — 厂商无关、协议无关
#[async_trait]
pub trait Tool: Send + Sync {
    /// 工具名称（唯一键，用于注册与 LLM 调用匹配）
    fn name(&self) -> &str;

    /// 工具描述（注入 LLM 的 tool description）
    fn description(&self) -> &str;

    /// 输入参数 JSON Schema（转换为厂商工具声明格式）
    fn input_schema(&self) -> Value;

    /// 执行工具 — 允许内部自管耗时 I/O
    ///
    /// 实现要求：
    /// - `args` 为 LLM 返回的 JSON 字符串解析后的 Value
    /// - 返回 `ToolOutput` 或 `ToolError`
    /// - 实现可以 `panic`，由 ToolExecutor 的 catch_unwind 兜底
    async fn execute(&self, args: Value) -> Result<ToolOutput, ToolError>;
}
```

---

## 4. ToolRegistry（`tool/registry.rs`）

```rust
//! 有界工具注册表 — DashMap + 上限 + 声明导出

use std::sync::Arc;
use dashmap::DashMap;
use referee_core::provider::ToolDeclaration;

use super::Tool;

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("registry full: {0}/{0}")]
    Full(usize),
    #[error("tool name conflict: {0}")]
    Conflict(String),
}

/// 工具注册表配置
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// 注册表容量上限
    pub max_tools: usize,
    /// 每轮最大工具调用数（超出截断）
    pub max_per_turn: usize,
    /// 并行执行上限
    pub max_concurrent: usize,
    /// 单个工具默认超时
    pub default_timeout: Duration,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            max_tools: 64,
            max_per_turn: 10,
            max_concurrent: 5,
            default_timeout: Duration::from_secs(30),
        }
    }
}

/// 有界工具注册表
pub struct ToolRegistry {
    tools: DashMap<String, Arc<dyn Tool>>,
    config: RegistryConfig,
}

impl ToolRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self { tools: DashMap::new(), config }
    }

    /// 注册工具（名称冲突或超限时返回错误）
    pub fn register(&self, tool: Arc<dyn Tool>) -> Result<(), RegistryError> {
        if self.tools.len() >= self.config.max_tools {
            return Err(RegistryError::Full(self.config.max_tools));
        }
        let name = tool.name().to_string();
        if self.tools.contains_key(&name) {
            return Err(RegistryError::Conflict(name));
        }
        self.tools.insert(name, tool);
        Ok(())
    }

    /// 按名称获取工具
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).map(|r| r.clone())
    }

    /// 导出全部工具声明（供 ChatRequest.tools 使用）
    pub fn declarations(&self) -> Vec<ToolDeclaration> {
        self.tools.iter().map(|r| ToolDeclaration {
            name: r.name().to_string(),
            description: r.description().to_string(),
            parameters: r.input_schema(),
        }).collect()
    }

    pub fn config(&self) -> &RegistryConfig { &self.config }
    pub fn len(&self) -> usize { self.tools.len() }
}
```

---

## 5. ToolExecutor（`tool/executor.rs`）

```rust
//! 并行工具执行器 — 截断 + 隔离 + 超时 + 结果回写
//!
//! 设计约束（对应 AGENT_RUNTIME_PLAN §2）：
//! - **终态自管**：每个工具包 catch_unwind + timeout
//! - **有界**：每轮 max_per_turn 截断，并发 max_concurrent 限制
//! - **隔离**：工具 panic 只影响该调用，不波及其他工具或 Session
//! - **消息驱动**：结果经 kernel.emit 回写，不直接操作 Session 状态

use std::sync::Arc;
use std::time::Duration;
use std::panic::AssertUnwindSafe;
use futures::FutureExt;
use tokio::sync::Semaphore;
use tracing::warn;

use referee_core::Kernel;
use referee_core::provider::ToolCall;

use crate::session::{SessionId, SessionMessage};
use crate::tool::{Tool, ToolError, ToolOutput};
use crate::tool::registry::ToolRegistry;

use uuid::Uuid;

pub struct ToolExecutor {
    registry: Arc<ToolRegistry>,
    semaphore: Arc<Semaphore>,
    default_timeout: Duration,
    self_id: referee_core::CapabilityId,
}

impl ToolExecutor {
    pub fn new(
        registry: Arc<ToolRegistry>,
        self_id: referee_core::CapabilityId,
    ) -> Self {
        let config = registry.config();
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            default_timeout: config.default_timeout,
            registry,
            self_id,
        }
    }

    /// 批量执行工具调用 — 非阻塞，立即返回
    ///
    /// 流程：
    /// 1. 截断：tool_calls.len() > max_per_turn → 前 N 个执行，多余发引导消息
    /// 2. 并行：每个工具 spawn 独立 task，Semaphore 控制并发上限
    /// 3. 隔离：每个 task 包 catch_unwind + timeout
    /// 4. 回写：结果经 kernel.emit(self_id, ToolResult) 回写
    pub fn execute_batch(
        &self,
        tool_calls: &[ToolCall],
        session_id: SessionId,
        turn_id: u64,
        kernel: Kernel,
    ) {
        let max_per_turn = self.registry.config().max_per_turn;

        // ── 1. 截断 ──
        let (kept, truncated) = if tool_calls.len() > max_per_turn {
            tool_calls.split_at(max_per_turn)
        } else {
            (tool_calls, &[][..])
        };

        // 为截断的调用发送引导消息
        for tc in truncated {
            let result = format!(
                "Error: tool call rejected — exceeds max_tools_per_turn \
                 limit ({max_per_turn}). The first {max_per_turn} calls in \
                 this turn are being executed. Please re-issue this call \
                 in the next turn."
            );
            self.emit_tool_result(kernel.clone(), session_id, turn_id, &tc.id, result);
        }

        // ── 2. 并行执行 ──
        for tc in kept {
            let tool = match self.registry.get(&tc.function.name) {
                Some(t) => t,
                None => {
                    self.emit_tool_result(
                        kernel.clone(), session_id, turn_id, &tc.id,
                        format!("Error: tool '{}' not found", tc.function.name),
                    );
                    continue;
                }
            };

            let args = tc.function.arguments.clone();
            let tool_call_id = tc.id.clone();
            let timeout = self.default_timeout;
            let semaphore = self.semaphore.clone();
            let kernel = kernel.clone();
            let self_id = self.self_id;

            tokio::spawn(async move {
                // 获取并发许可
                let _permit = match semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => {
                        warn!(tool_call_id = %tool_call_id, "semaphore closed");
                        return;
                    }
                };

                // 执行（含隔离 + 超时）
                let result = execute_single(tool, args, timeout).await;

                // 回写结果
                let env = SessionMessage::ToolResult {
                    session_id,
                    turn_id,
                    tool_call_id,
                    result,
                }.to_envelope();

                if let Err(e) = kernel.emit(self_id, env) {
                    warn!(
                        error = ?e,
                        session_id = %session_id,
                        "emit tool result failed (kernel channel full?)"
                    );
                }
            });
        }
    }

    fn emit_tool_result(
        &self,
        kernel: Kernel,
        session_id: SessionId,
        turn_id: u64,
        tool_call_id: &str,
        result: String,
    ) {
        let env = SessionMessage::ToolResult {
            session_id,
            turn_id,
            tool_call_id: tool_call_id.to_string(),
            result,
        }.to_envelope();
        if let Err(e) = kernel.emit(self.self_id, env) {
            warn!(error = ?e, "emit (truncated/notfound) tool result failed");
        }
    }
}

/// 单个工具执行 — catch_unwind + timeout 四路径全覆盖
async fn execute_single(
    tool: Arc<dyn Tool>,
    args_json: String,
    timeout: Duration,
) -> String {
    // 解析参数
    let args: serde_json::Value = match serde_json::from_str(&args_json) {
        Ok(v) => v,
        Err(e) => return format!("Error: invalid arguments — {e}"),
    };

    // catch_unwind + timeout
    let fut = AssertUnwindSafe(tool.execute(args));
    match tokio::time::timeout(timeout, fut.catch_unwind()).await {
        // 正常返回
        Ok(Ok(Ok(output))) => output.content,
        // 执行错误
        Ok(Ok(Err(e))) => format!("Error: {e}"),
        // Panic 被捕获
        Ok(Err(payload)) => {
            let msg = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("unknown panic");
            format!("Error: tool panicked — {msg}")
        }
        // 超时
        Err(_) => format!("Error: tool execution timed out after {timeout:?}"),
    }
}
```

---

## 6. Session 状态机扩展（`session/mod.rs`）

### 6.1 状态枚举变更

```rust
/// Session 状态 — Phase 2 新增 AwaitingCalls
#[derive(Debug)]
pub enum SessionState {
    Idle,
    Thinking {
        turn_id: u64,
        cancel: tokio::sync::oneshot::Sender<()>,
    },
    /// P2 新增：等待工具/子 Agent 完成
    AwaitingCalls {
        turn_id: u64,
        /// 未完成的 tool_call_id 集合（空 = 全部完成，可 resume）
        pending: std::collections::HashSet<String>,
    },
}
```

### 6.2 Session 结构变更

```rust
pub struct Session {
    // ── 既有字段（不修改） ──
    pub(crate) state: SessionState,
    pub(crate) turn_id: u64,
    history: std::collections::VecDeque<Message>,
    config: SessionConfig,

    // ── P2 新增 ──
    /// 多轮回复转发器 — Chat 入口创建，最终 turn 完成时发送
    pending_reply: Option<tokio::sync::oneshot::Sender<Envelope>>,
    /// 上一轮 ChatOptions（resume 时重建 ChatRequest）
    last_options: ChatOptions,
}
```

### 6.3 `finish_thinking` 变更

```rust
/// finish_thinking 返回的动作指令
pub enum FinishAction {
    /// 终态 Idle（成功 / 错误 / 取消 / 超时 / panic）
    Idle { response: Option<ChatResponse> },
    /// 进入 AwaitingCalls（P2 新增）
    AwaitingCalls {
        response: ChatResponse,
        pending: std::collections::HashSet<String>,
    },
}

impl Session {
    pub fn finish_thinking(
        &mut self,
        turn_id: u64,
        outcome: TurnOutcome,
    ) -> FinishAction {
        // 校验 turn_id + 状态匹配（既有逻辑）
        match &self.state {
            SessionState::Thinking { turn_id: t, .. } if *t == turn_id => {}
            _ => return FinishAction::Idle { response: None }, // stale
        }

        match outcome {
            TurnOutcome::Success(resp) => {
                // 推入 history（assistant 消息，含 tool_calls）
                self.push_history(resp.message.clone());

                if resp.message.tool_calls.is_empty() {
                    // 无工具调用 → 终态 Idle
                    self.state = SessionState::Idle;
                    FinishAction::Idle { response: Some(resp) }
                } else {
                    // 有工具调用 → 进入 AwaitingCalls
                    let pending: HashSet<_> = resp
                        .message
                        .tool_calls
                        .iter()
                        .map(|tc| tc.id.clone())
                        .collect();
                    self.state = SessionState::AwaitingCalls {
                        turn_id,
                        pending: pending.clone(),
                    };
                    FinishAction::AwaitingCalls { response: resp, pending }
                }
            }
            // 错误 / 取消 / 超时 / panic → 终态 Idle
            _ => {
                self.state = SessionState::Idle;
                FinishAction::Idle { response: None }
            }
        }
    }
}
```

### 6.4 `apply_tool_result` — 新增方法

```rust
/// 工具结果回写 — 更新 pending，返回是否全部完成
///
/// 调用方（handle_tool_result）据此决定是否 emit Resume。
/// 同时将 tool result Message 推入 history（供下一轮 LLM 调用）。
pub fn apply_tool_result(
    &mut self,
    turn_id: u64,
    tool_call_id: &str,
    result: String,
) -> ToolResultAction {
    let (is_complete, turn_id_matches) = match &mut self.state {
        SessionState::AwaitingCalls { turn_id: t, pending } if *t == turn_id => {
            pending.remove(tool_call_id);
            (pending.is_empty(), true)
        }
        _ => return ToolResultAction::Ignored,
    };

    if !turn_id_matches {
        return ToolResultAction::Ignored;
    }

    // 推入 history（role=Tool, tool_call_id, content=result）
    self.push_history(Message {
        role: Role::Tool,
        content: MessageContent::Text(result),
        reasoning_content: None,
        tool_calls: Vec::new(),
        tool_call_id: Some(tool_call_id.to_string()),
    });

    if is_complete {
        ToolResultAction::AllComplete
    } else {
        ToolResultAction::Pending
    }
}

pub enum ToolResultAction {
    Pending,
    AllComplete,
    Ignored, // stale turn_id 或非 AwaitingCalls 状态
}
```

### 6.5 `cancel_awaiting` — 新增方法

```rust
/// Interrupt 在 AwaitingCalls 时的处理：直接回 Idle
pub fn cancel_awaiting(&mut self) -> bool {
    let was_awaiting = matches!(self.state, SessionState::AwaitingCalls { .. });
    if was_awaiting {
        self.state = SessionState::Idle;
    }
    was_awaiting
}
```

### 6.6 `take_pending_reply` — 新增方法

```rust
/// 取出 pending_reply sender（最终回复时消费）
pub fn take_pending_reply(&mut self) -> Option<tokio::sync::oneshot::Sender<Envelope>> {
    self.pending_reply.take()
}
```

---

## 7. SessionReply 扩展（`session/message.rs`）

```rust
/// 新增 SessionReply 变体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionReply {
    // ── 既有 ──
    Success { response: ChatResponse },
    Error { message: String },
    Busy { turn_id: u64 },
    Cancelled,
    Unhandled { reason: String },

    // ── P2 新增 ──
    /// 进入工具执行阶段（调用方可据此知道本轮需要等待工具完成）
    AwaitingTools { turn_id: u64 },
}
```

---

## 8. AgentRuntime 集成（`lib.rs`）

### 8.1 结构变更

```rust
pub struct AgentRuntime {
    id: CapabilityId,
    kernel: Kernel,
    provider: Arc<dyn LLMProvider>,
    sessions: Arc<DashMap<SessionId, Session>>,
    config: AgentConfig,

    // P2 新增
    tools: Option<Arc<ToolExecutor>>,
}

/// Builder — 显式启用工具能力
impl AgentRuntime {
    /// Phase 1 兼容：无工具
    pub fn new(kernel: Kernel, provider: Arc<dyn LLMProvider>, config: AgentConfig) -> Self {
        Self {
            id: CapabilityId::new(),
            kernel, provider,
            sessions: Arc::new(DashMap::new()),
            config,
            tools: None,
        }
    }

    /// Phase 2：启用工具
    pub fn with_tools(
        kernel: Kernel,
        provider: Arc<dyn LLMProvider>,
        config: AgentConfig,
        registry: Arc<ToolRegistry>,
    ) -> Self {
        let id = CapabilityId::new();
        let executor = Arc::new(ToolExecutor::new(registry, id));
        Self {
            id,
            kernel, provider,
            sessions: Arc::new(DashMap::new()),
            config,
            tools: Some(executor),
        }
    }
}
```

### 8.2 `handle` 分发变更

```rust
async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
    let msg = SessionMessage::from_envelope(&env)?;
    let span = info_span!("agent_msg", kind = message_kind_label(&msg));

    match msg {
        SessionMessage::Chat { session_id, payload } => {
            self.handle_chat(ctx, session_id, payload).instrument(span);
        }
        SessionMessage::Interrupt { session_id } => {
            self.handle_interrupt(ctx, session_id);
        }
        // P2 新增
        SessionMessage::ToolResult { session_id, turn_id, tool_call_id, result } => {
            self.handle_tool_result(ctx, session_id, turn_id, tool_call_id, result);
        }
        SessionMessage::Resume { session_id, turn_id } => {
            self.handle_resume(ctx, session_id, turn_id);
        }
        // P3 预留
        SessionMessage::SubagentDone { .. } => {
            self.handle_unhandled(ctx, "subagent_done");
        }
    }
    Ok(())
}
```

### 8.3 `handle_chat` — forwarder 模式

```rust
fn handle_chat(
    &self,
    ctx: KernelContext,
    session_id: SessionId,
    payload: ChatPayload,
) {
    let session_entry = match self.get_or_create_session(session_id) {
        Some(e) => e,
        None => {
            let _ = ctx.reply(SessionReply::Error {
                message: "max sessions reached".into()
            }.to_envelope());
            return;
        }
    };

    // ── 创建 forwarder：多轮工具调用的最终回复通道 ──
    let (reply_tx, reply_rx) = oneshot::channel::<Envelope>();
    let forwarder_timeout = Duration::from_secs(300); // 5 min 全局上限

    tokio::spawn(async move {
        let reply_env = match tokio::time::timeout(forwarder_timeout, reply_rx).await {
            Ok(Ok(env)) => env,                          // 正常收到最终回复
            Ok(Err(_)) => SessionReply::Error {         // sender 被 drop（session 移除）
                message: "session terminated".into()
            }.to_envelope(),
            Err(_) => SessionReply::Error {              // 全局超时
                message: "session timed out (5min)".into()
            }.to_envelope(),
        };
        let _ = ctx.reply(reply_env);
    });

    // ── 状态转移 + 构建请求 ──
    let (turn_id, cancel_rx, req, timeout) = {
        let mut session = session_entry;

        if session.is_busy() {
            drop(session);
            // pending_reply 还没设置，直接回 Busy
            // 注意：forwarder 已 spawn，需要通过 reply_tx 回复
            let _ = reply_tx.send(SessionReply::Busy { turn_id: 0 }.to_envelope());
            return;
        }

        // 存储 pending_reply + last_options
        session.pending_reply = Some(reply_tx);
        session.last_options = payload.options.clone();

        // 推入 user 消息
        session.push_history(payload.message.clone());

        // Idle → Thinking
        let (turn_id, cancel_rx) = match session.start_thinking() {
            Some(pair) => pair,
            None => {
                drop(session);
                return; // 防御性兜底
            }
        };

        let req = session.build_chat_request(&payload.options);
        let timeout = session.config().timeout.thinking_timeout;
        (turn_id, cancel_rx, req, timeout)
    };
    // guard 已 drop，无跨 await 持锁

    // ── spawn 派生 turn 任务 ──
    let sessions = self.sessions.clone();
    let provider = self.provider.clone();
    let tools = self.tools.clone();
    let kernel = self.kernel.clone();
    let self_id = self.id;

    spawn_turn_task(
        sessions, provider, req, cancel_rx,
        session_id, turn_id, timeout,
        tools, kernel, self_id,
    );
}
```

### 8.4 `spawn_turn_task` — 签名变更

```rust
#[allow(clippy::too_many_arguments)]
fn spawn_turn_task(
    sessions: Arc<DashMap<SessionId, Session>>,
    provider: Arc<dyn LLMProvider>,
    req: ChatRequest,
    cancel_rx: oneshot::Receiver<()>,
    session_id: SessionId,
    turn_id: u64,
    timeout: Duration,
    // P2 新增
    tools: Option<Arc<ToolExecutor>>,
    kernel: Kernel,
    self_id: CapabilityId,
) {
    tokio::spawn(async move {
        let span = info_span!("agent_turn", session_id = %session_id, turn_id);
        let outcome = session::run_turn(provider.chat(req), cancel_rx, timeout)
            .instrument(span)
            .await;

        // 终态收敛（catch_unwind 兜底）
        let result = AssertUnwindSafe(async {
            converge_and_reply(
                &sessions, session_id, turn_id, outcome,
                &tools, &kernel, self_id,
            ).await;
        })
        .catch_unwind()
        .await;

        if result.is_err() {
            // 收敛逻辑 panic — 强制恢复 Idle + 回错
            warn!(session_id = %session_id, "convergence panicked, forcing Idle");
            if let Some(mut session) = sessions.get_mut(&session_id) {
                if matches!(session.state, SessionState::Thinking { .. }) {
                    session.state = SessionState::Idle;
                }
                if let Some(tx) = session.take_pending_reply() {
                    let _ = tx.send(SessionReply::Error {
                        message: "turn task panicked".into()
                    }.to_envelope());
                }
            }
        }
    });
}
```

### 8.5 `converge_and_reply` — AwaitingCalls 分支

```rust
async fn converge_and_reply(
    sessions: &Arc<DashMap<SessionId, Session>>,
    session_id: SessionId,
    turn_id: u64,
    outcome: TurnOutcome,
    tools: &Option<Arc<ToolExecutor>>,
    kernel: &Kernel,
    self_id: CapabilityId,
) {
    // 1. 终态收敛（短暂持锁，无 await）
    let action = if let Some(mut session) = sessions.get_mut(&session_id) {
        session.finish_thinking(turn_id, outcome)
    } else {
        return; // Session 已移除
    };
    // guard 已 drop

    match action {
        // ── 终态 Idle：通过 pending_reply 回复 ──
        FinishAction::Idle { response: Some(resp) } => {
            if let Some(mut session) = sessions.get_mut(&session_id) {
                let tx = session.take_pending_reply();
                drop(session);
                if let Some(tx) = tx {
                    let _ = tx.send(SessionReply::from_response(resp).to_envelope());
                }
            }
        }
        FinishAction::Idle { response: None } => {
            if let Some(mut session) = sessions.get_mut(&session_id) {
                let tx = session.take_pending_reply();
                drop(session);
                if let Some(tx) = tx {
                    let _ = tx.send(SessionReply::Error {
                        message: "turn ended without success".into()
                    }.to_envelope());
                }
            }
        }

        // ── AwaitingCalls：派发工具执行 ──
        FinishAction::AwaitingCalls { response, pending: _ } => {
            if let Some(executor) = tools {
                // 有工具执行器 → 派发执行
                let tool_calls = response.message.tool_calls.clone();
                executor.execute_batch(
                    &tool_calls, session_id, turn_id,
                    kernel.clone(),
                );
                // pending_reply 不动 — 等 resume 循环最终回 Idle 时消费
            } else {
                // 无工具执行器（向后兼容）→ 强制回 Idle + 回传响应
                if let Some(mut session) = sessions.get_mut(&session_id) {
                    session.state = SessionState::Idle;
                    let tx = session.take_pending_reply();
                    drop(session);
                    if let Some(tx) = tx {
                        let _ = tx.send(
                            SessionReply::from_response(response).to_envelope()
                        );
                    }
                }
            }
        }
    }
}
```

### 8.6 `handle_tool_result` — 新增

```rust
fn handle_tool_result(
    &self,
    _ctx: KernelContext,   // emit 路径，ctx.reply() 是 no-op
    session_id: SessionId,
    turn_id: u64,
    tool_call_id: String,
    result: String,
) {
    let action = if let Some(mut session) = self.sessions.get_mut(&session_id) {
        session.apply_tool_result(turn_id, &tool_call_id, result)
    } else {
        return;
    };
    // guard 已 drop

    // 全部完成 → emit Resume 触发下一轮
    if matches!(action, ToolResultAction::AllComplete) {
        let env = SessionMessage::Resume { session_id, turn_id }.to_envelope();
        if let Err(e) = self.kernel.emit(self.id, env) {
            warn!(error = ?e, "emit Resume failed");
            // 兜底：awaiting_calls_timeout 会超时回 Idle
        }
    }
}
```

### 8.7 `handle_resume` — 新增

```rust
fn handle_resume(
    &self,
    _ctx: KernelContext,   // emit 路径，ctx.reply() 是 no-op
    session_id: SessionId,
    turn_id: u64,
) {
    let (new_turn_id, cancel_rx, req, timeout) = {
        let mut session = match self.sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return,
        };

        // 校验：必须处于 AwaitingCalls + turn_id 匹配 + pending 为空
        let options = match &session.state {
            SessionState::AwaitingCalls { turn_id: t, pending, .. }
                if *t == turn_id && pending.is_empty() =>
            {
                session.last_options.clone()
            }
            _ => return, // 状态不匹配或 pending 未清空
        };

        // AwaitingCalls → Thinking（复用 start_thinking，已扩展支持 AwaitingCalls 来源）
        let (new_turn_id, cancel_rx) = match session.start_thinking() {
            Some(pair) => pair,
            None => return,
        };

        let req = session.build_chat_request(&options);
        let timeout = session.config().timeout.thinking_timeout;
        (new_turn_id, cancel_rx, req, timeout)
    };
    // guard 已 drop

    // spawn 新一轮 turn 任务
    let sessions = self.sessions.clone();
    let provider = self.provider.clone();
    let tools = self.tools.clone();
    let kernel = self.kernel.clone();
    let self_id = self.id;

    spawn_turn_task(
        sessions, provider, req, cancel_rx,
        session_id, new_turn_id, timeout,
        tools, kernel, self_id,
    );
}
```

### 8.8 `handle_interrupt` — 扩展 AwaitingCalls 分支

```rust
fn handle_interrupt(&self, ctx: KernelContext, session_id: SessionId) {
    let reply = if let Some(mut session) = self.sessions.get_mut(&session_id) {
        match &session.state {
            SessionState::Thinking { .. } => {
                // 既有逻辑：发送取消信号
                // 取出 pending_reply — turn task 收敛时发现 None，跳过回信
                let tx = session.take_pending_reply();
                // 触发 cancel
                session.cancel_thinking();
                // 通过 forwarder 回 Cancelled
                if let Some(tx) = tx {
                    let _ = tx.send(SessionReply::Cancelled.to_envelope());
                }
                SessionReply::Cancelled
            }
            SessionState::AwaitingCalls { .. } => {
                // P2 新增：直接回 Idle + 取消
                let tx = session.take_pending_reply();
                session.cancel_awaiting();
                if let Some(tx) = tx {
                    let _ = tx.send(SessionReply::Cancelled.to_envelope());
                }
                // 正在执行的工具任务会自然完成，结果回写时
                // handle_tool_result 发现状态非 AwaitingCalls → ToolResultAction::Ignored
                SessionReply::Cancelled
            }
            SessionState::Idle => {
                SessionReply::Unhandled {
                    reason: "session not thinking or not found".into()
                }
            }
        }
    } else {
        SessionReply::Unhandled {
            reason: "session not thinking or not found".into()
        }
    };

    let _ = ctx.reply(reply.to_envelope());
}
```

---

## 9. 完整消息流程

### 9.1 正常多轮工具调用

```
用户                     Kernel              AgentRuntime           ToolExecutor          LLM Provider
  │                        │                      │                      │                     │
  │── invoke(Chat) ──────▶│                      │                      │                     │
  │                        │── Envelope ─────────▶│                      │                     │
  │                        │                      │── spawn forwarder ──│                     │
  │                        │                      │── spawn turn task ───│                     │
  │                        │                      │                      │── chat(req) ───────▶│
  │                        │                      │                      │◀──── ChatResponse ─│
  │                        │                      │   (tool_calls=[3个])│                     │
  │                        │                      │                      │                     │
  │                        │                      │── finish_thinking ──│                     │
  │                        │                      │   → AwaitingCalls   │                     │
  │                        │                      │── execute_batch ────│                     │
  │                        │                      │                      │── spawn 3 tasks ──│
  │                        │                      │                      │   (并行 + Semaphore)│
  │                        │                      │                      │                     │
  │                        │◀── emit(ToolResult#1)│──────────────────────│                     │
  │                        │── Envelope ─────────▶│                      │                     │
  │                        │                      │── apply_tool_result │                     │
  │                        │                      │   pending={2,3}    │                     │
  │                        │                      │                      │                     │
  │                        │◀── emit(ToolResult#2)│──────────────────────│                     │
  │                        │── Envelope ─────────▶│                      │                     │
  │                        │                      │── apply_tool_result │                     │
  │                        │                      │   pending={3}       │                     │
  │                        │                      │                      │                     │
  │                        │◀── emit(ToolResult#3)│──────────────────────│                     │
  │                        │── Envelope ─────────▶│                      │                     │
  │                        │                      │── apply_tool_result │                     │
  │                        │                      │   pending={} (empty)│                     │
  │                        │                      │                      │                     │
  │                        │◀── emit(Resume) ─────│                      │                     │
  │                        │── Envelope ─────────▶│                      │                     │
  │                        │                      │── start_thinking ───│                     │
  │                        │                      │   (new turn_id)     │                     │
  │                        │                      │── spawn turn task ───│                     │
  │                        │                      │                      │── chat(req) ───────▶│
  │                        │                      │                      │◀──── ChatResponse ─│
  │                        │                      │                      │   (no tool_calls)   │
  │                        │                      │── finish_thinking ──│                     │
  │                        │                      │   → Idle             │                     │
  │                        │                      │── pending_reply ────│                     │
  │                        │                      │   (send via oneshot)│                     │
  │◀──── reply ────────────│◀── ctx.reply() ──────│ (forwarder task)    │                     │
```

### 9.2 截断流程

```
LLM 返回 15 个 tool_calls, max_per_turn=10

ToolExecutor:
  ├── 前 10 个 → spawn 10 个并行任务（Semaphore 限流）
  └── 后 5 个 → 各发一条 ToolResult：
        "Error: tool call rejected — exceeds max_tools_per_turn
         limit (10). The first 10 calls in this turn are being
         executed. Please re-issue this call in the next turn."

→ 15 条 ToolResult 全部回写，pending 清空，触发 Resume
→ LLM 下一轮看到 10 个成功 + 5 个引导错误，可决定是否重发剩余 5 个
```

### 9.3 中断流程

```
状态: AwaitingCalls (3 个工具执行中)

用户 ── invoke(Interrupt) ──▶ Kernel ──▶ AgentRuntime

handle_interrupt:
  1. session.cancel_awaiting() → state = Idle
  2. take_pending_reply() → send Cancelled via forwarder
  3. ctx.reply(Cancelled) → Interrupt 调用方收到 Cancelled
  4. forwarder 收到 Cancelled → ctx.reply(Cancelled) → Chat 调用方收到 Cancelled

正在执行的 3 个工具任务:
  → 自然完成 → emit ToolResult
  → handle_tool_result 发现 state=Idle → ToolResultAction::Ignored
  → 结果丢弃，不影响系统
```

---

## 10. `start_thinking` 扩展

```rust
impl Session {
    /// Idle 或 AwaitingCalls → Thinking（turn_id 递增）
    pub fn start_thinking(&mut self) -> Option<(u64, oneshot::Receiver<()>)> {
        match self.state {
            SessionState::Idle | SessionState::AwaitingCalls { .. } => {
                self.turn_id += 1;
                let (tx, rx) = oneshot::channel();
                self.state = SessionState::Thinking {
                    turn_id: self.turn_id,
                    cancel: tx,
                };
                Some((self.turn_id, rx))
            }
            SessionState::Thinking { .. } => None,
        }
    }
}
```

---

## 11. AwaitingCalls 超时兜底

AwaitingCalls 超时由 `timeout.rs` 中已定义的 `awaiting_calls_timeout`（默认 60s）治理。

实现方式：在 `AgentRuntime::handle` 中为 AwaitingCalls 状态启动一个兜底定时器：

```rust
/// 在进入 AwaitingCalls 时启动超时兜底
fn spawn_awaiting_timeout(
    sessions: Arc<DashMap<SessionId, Session>>,
    session_id: SessionId,
    turn_id: u64,
    timeout: Duration,
    kernel: Kernel,
    self_id: CapabilityId,
) {
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;

        if let Some(mut session) = sessions.get_mut(&session_id) {
            let is_awaiting = matches!(
                &session.state,
                SessionState::AwaitingCalls { turn_id: t, .. } if *t == turn_id
            );
            if is_awaiting {
                warn!(session_id = %session_id, turn_id, "awaiting_calls timeout, forcing Idle");
                session.state = SessionState::Idle;
                let tx = session.take_pending_reply();
                drop(session);

                if let Some(tx) = tx {
                    let _ = tx.send(SessionReply::Error {
                        message: format!(
                            "awaiting calls timeout after {timeout:?}"
                        ),
                    }.to_envelope());
                }

                // 记录 metrics
                counter!("agent.awaiting_timeout").increment(1);
            }
        }
    });
}
```

调用时机：`converge_and_reply` 中进入 AwaitingCalls 分支时 spawn。

---

## 12. 测试计划

### 12.1 单元测试（`tool_test.rs`）

| 用例 | 验证点 |
|------|--------|
| `tool_register_and_get` | 注册 → get → declarations |
| `tool_register_conflict` | 名称冲突返回 Conflict |
| `tool_registry_full` | 超限返回 Full |
| `executor_single_tool` | 1 个工具执行 → 结果回写 |
| `executor_parallel_5` | 5 个并行，总耗时 ≈ max(单个) |
| `executor_truncation` | 15 个调用 → 10 执行 + 5 引导消息 |
| `executor_panic_isolated` | 1 个 panic → 返回错误结果，其余正常 |
| `executor_timeout` | 慢工具超时 → 返回 timeout 错误 |
| `executor_tool_not_found` | 未注册工具 → 返回 not found 错误 |

### 12.2 集成测试（`tool_test.rs`）

| 用例 | 验证点 |
|------|--------|
| `full_cycle_single_tool` | Chat → LLM 返回 1 tool_call → 执行 → resume → 最终回复 |
| `full_cycle_multi_turn` | 2 轮工具调用 → 最终回复 |
| `interrupt_during_awaiting` | AwaitingCalls 时 Interrupt → Idle + Cancelled |
| `no_tools_backward_compat` | 无 ToolRegistry → tool_calls 响应直接回传（Phase 1 行为） |
| `phase1_tests_still_pass` | 65 条 Phase 1 测试全绿 |

### 12.3 测试 Mock 策略

复用 `tests/common/mod.rs` 的 HTTP mock 服务器。Mock LLM 按预设序列返回不同响应：

```rust
enum MockBehavior {
    /// 第一轮返回 tool_calls，第二轮返回纯文本
    ToolThenText {
        tool_calls: Vec<ToolCall>,
        final_text: String,
    },
    /// 始终返回纯文本（Phase 1 行为）
    Ok(ChatResponse),
    /// 永不响应（超时测试）
    Hang,
}
```

Mock Tool 实现：

```rust
struct MockTool {
    name: String,
    delay: Duration,
    result: String,
    should_panic: bool,
}

#[async_trait]
impl Tool for MockTool {
    fn name(&self) -> &str { &self.name }
    fn description(&self) -> &str { "mock tool" }
    fn input_schema(&self) -> Value { json!({"type": "object"}) }

    async fn execute(&self, _args: Value) -> Result<ToolOutput, ToolError> {
        if self.should_panic {
            panic!("mock panic");
        }
        tokio::time::sleep(self.delay).await;
        Ok(ToolOutput::text(&self.result))
    }
}
```

---

## 13. 依赖与不变量

### 13.1 依赖

无新增 crate 依赖。使用已有白名单库：
- `dashmap`（ToolRegistry）
- `tokio::sync::Semaphore`（并发限流，tokio "full" feature 已包含）
- `tokio::time::timeout`（工具超时）
- `futures::FutureExt::catch_unwind`（panic 隔离）
- `tracing` / `metrics`（可观测）

### 13.2 不变量

Phase 2 完成后以下必须成立：

1. **`referee-core` 零改动** — `git diff referee-core` 为空
2. **Phase 1 测试全绿** — 原 65 条测试不修改、不跳过
3. **无新依赖** — `Cargo.toml` 不新增 crate
4. **handle 内零 await** — 所有异步操作在 spawn 的派生任务中
5. **pending_reply 单消费** — 每个 Chat 生命周期内 oneshot::Sender 最多被 send 一次
6. **turn_id 单调递增** — resume 循环中 turn_id 严格递增
7. **AwaitingCalls 永不泄漏** — 正常 resume / 超时回 Idle / Interrupt 回 Idle，三路径收敛
8. **工具 panic 不外泄** — catch_unwind 在 execute 边界
9. **有界保证** — Registry 有上限、每轮工具数有上限、并发数有上限、内核通道有界

---

## 14. 已知限制（Phase 2 范围内可接受）

| 限制 | 说明 | 缓解 |
|------|------|------|
| Session 移除后工具任务继续执行 | 工具结果回写时找不到 session → Ignored 丢弃 | 工具数有上限(10) + 独立超时(30s)，资源消耗可控 |
| emit 失败时工具结果丢失 | 内核通道满 → warn 日志，pending 项不会被移除 | awaiting_calls_timeout 兜底，最终回 Idle + Error |
| 全局 5min forwarder 超时 | 极端情况下多轮工具调用可能超过 5min | 可配置化，后续 Phase 按需调整 |
```

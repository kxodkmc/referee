# Referee Agent — 智能体运行时模块

> 基于 `referee-core` 的**可选 SDK 式扩展模块**：提供 LLM 厂商抽象与多会话状态机，让业务方以「一个内核扩展」的方式获得完整智能体能力。
> 当前进度：**Phase 0 ~ 3 + 预算治理 + Phase 5 已完成（146 条测试全绿）**：厂商抽象、会话状态机、工具调用、对等智能体协作与工件存储、Token 双层级预算、提示词组装与语义缓存。详见 [路线图](#9-路线图)。

---

## 1. 定位与边界

| 项 | 说明 |
|----|------|
| 独立 crate | `referee-agent` 与内核物理隔离，**不触碰 `referee-core` 任何代码**；不使用它的项目不编译它 |
| 按需启用 | 使用方在 `Cargo.toml` 按需引入，并用 features 裁剪厂商适配器 |
| 面向用户 | 需要「智能体能力」的项目（对话、工具、子 Agent、记忆），与面向「通信与治理」的 `referee-core` 是两个正交消费面 |
| 组合者 | Agent 层只**组合**内核原语（emit / 有界通道 / catch_unwind / WAL），不重造轮子 |

### 启用方式

```toml
[dependencies]
referee-agent = { path = "referee-agent", features = ["xiaomi", "deepseek"] }
```

**feature 清单**（默认 `["xiaomi", "deepseek"]`，即第一批深度适配的厂商）：

| feature | 含义 | 状态 |
|---------|------|------|
| `xiaomi` | Xiaomi MiMo 适配器 | ✅ 已实现 |
| `deepseek` | DeepSeek 适配器 | ✅ 已实现 |
| `openai` / `anthropic` / `responses` | 其他厂商适配器 | 预留扩展点（保证接口稳定） |
| `mcp-stdio` / `memory-persist` | MCP 桥 / 记忆落盘 | 预留扩展点（Phase 7 / Phase 4） |

未启用的适配器不参与编译（`#[cfg(feature = "...")]` 隔离）；trait 与核心状态机始终存在。

---

## 2. 架构总览

```
┌────────────────────────────────────────────────────────────────┐
│                        referee-agent                          │
│                                                                │
│  ┌──────────────────────┐     Envelope      ┌───────────────┐  │
│  │      Kernel          │ ─────────────────▶│ AgentRuntime  │  │
│  │   (referee-core)     │◀───────────────── │ (Extension)   │  │
│  └──────────────────────┘  ctx.reply()      └───────┬───────┘  │
│                                                    │ spawn     │
│                                                    ▼           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Turn Task（派生任务）                  │   │
│  │   run_turn：LLM 调用 │ 取消 │ 超时 → finally 收敛 + reply  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌──────────────────────┐   ┌──────────────────────────────┐  │
│  │  provider/           │   │  session/                    │  │
│  │  唯一 I/O 边界        │   │  并发正确性核心（状态机）      │  │
│  │  LLMProvider trait   │   │  SessionState / run_turn     │  │
│  │  + 厂商适配器         │   │  / 消息协议 / 超时治理 / 计量   │  │
│  └──────────────────────┘   └──────────────────────────────┘  │
│                                                                │
│  ┌──────────────────────┐   ┌──────────────────────────────┐  │
│  │  tool/               │   │  artifact/  ·  budget/        │  │
│  │  工具调用 + 对等协作   │   │  工件 ACL  ·  Token 双层级预算  │  │
│  └──────────────────────┘   └──────────────────────────────┘  │
│                                                               │
│  ┌──────────────────────┐   ┌──────────────────────────────┐  │
│  │  prompt/             │   │  cache/                      │  │
│  │  提示词组装+预算截断   │   │  内存 LRU/TTL 缓存 + 合成流    │  │
│  └──────────────────────┘   └──────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

**分层依赖规则**（可拓展性的根基）：

- `provider` 不依赖任何上层模块；`session` 只依赖 `provider`（`session` → `prompt` 单向依赖，`build_chat_request` 统一走预算截断）。
- `tool` / `artifact` / `budget` / `prompt` / `cache` 通过 trait 与数据载体互引用（`tool` 依赖 `artifact` 的 `ArtifactStore`、`referee-core` 的 `Kernel` 句柄），不绑定具体实现。
- 新增厂商 = 新增一个适配器文件；新增能力 = 新增一个模块，均不触碰既有代码。

---

## 3. 模块详解

### 3.1 `provider/` — 厂商抽象层（Phase 0 交付）

**定位**：Agent 运行时唯一的外部 I/O 边界。上层只面对统一接口，不写厂商分支。

| 组件 | 说明 |
|------|------|
| [`LLMProvider`](src/provider/mod.rs) trait | `id()` / `capabilities()` / `chat()` / `chat_stream()`，要求 `Send + Sync`，可在多 task 间共享 |
| [`ProviderCapabilities`](src/provider/mod.rs) | 能力声明（`parallel_tool_calls` / `system_role` / `streaming` / `usage_reported` / `max_output_tokens`），上层据此**自动降级** |
| [`LlmError`](src/provider/mod.rs) | 统一错误枚举：`Network / Timeout / RateLimited / BadRequest / Server / Auth / Cancelled / Protocol`，厂商差异全部归一 |
| [`RetryPolicy`](src/provider/mod.rs) | 重试仅对 `Network / Server / RateLimited` 三类生效，指数退避（`initial × 2ⁿ`，封顶 `max_backoff`），受 `max_retries` 上限 |
| [`ChatRequest` / `ChatResponse`](src/provider/mod.rs) | 厂商无关的请求 / 响应模型（消息、工具声明、thinking 配置、usage 等） |
| [`StreamChunk`](src/provider/mod.rs) | 流式增量（`Delta`）+ 终止（`Finish`），收敛后必须与 `chat()` 语义等价 |
| [`openai_compat`](src/provider/openai_compat.rs) | **`pub(crate)` 共享底座**：HTTP 发送、错误归一、重试、SSE 解析，MiMo / DeepSeek 复用 |

**已实现适配器**：

| 适配器 | 端点 / 模型 | 特性 |
|--------|------------|------|
| [`xiaomi.rs`](src/provider/xiaomi.rs) | `https://api.xiaomimimo.com/v1` · `mimo-v2.5-pro` / `mimo-v2.5` | 思考开关（默认开启）、`reasoning_content`、`reasoning_tokens` usage 扩展 |
| [`deepseek.rs`](src/provider/deepseek.rs) | `https://api.deepseek.com` · `deepseek-v4-flash` / `deepseek-v4-pro` | 思考开关、`reasoning_effort`（low/high/max）、硬盘缓存 usage 扩展 |

> 多模态（音频/视频/图片）通过扩展 `MessageContent` 启用，已有适配器无需改动。

### 3.2 `session/` — 会话状态机（Phase 1 交付）

**定位**：并发正确性核心 ——「永不幽灵、永不阻塞、可中断」的会话。

**状态机**（统一工具与子 Agent 的等待态，P2/P3 复用）：

```
Idle ──Chat──▶ Thinking ──outcome──▶ Idle
  ▲              │
  │         Interrupt
  │         (协作取消信号)
  │              ▼
  │         Thinking ──cancelled──▶ Idle
  │
  │  Idle ──Chat(with tools)──▶ Thinking ──tool_calls──▶ AwaitingCalls
  │                                                         │
  │                                                  all done (P2/P3 resume)
  └─────────────────────────────────────────────────────────┘
```

| 组件 | 说明 |
|------|------|
| [`SessionState`](src/session/mod.rs) | `Idle` / `Thinking { turn_id, cancel }` / `AwaitingCalls { turn_id, pending }` |
| [`Session`](src/session/mod.rs) | 纯状态持有者（不含 I/O 句柄）：`start_thinking` / `cancel_thinking` / `finish_thinking`（finally 式唯一终态写入）、有界 history（FIFO 淘汰）、`turn_id` 单调递增、`consumed_tokens`（预算计量） |
| [`run_turn`](src/session/task.rs) | 终态自管 wrapper：三路 `select!`（LLM / 取消 / 超时）+ 外层 `catch_unwind`，五路径（Success / Error / Cancelled / Timeout / Panic）全部收敛为 `TurnOutcome` |
| [`TimeoutConfig`](src/session/timeout.rs) | 双 deadline：Thinking 超时（默认 30s）+ AwaitingCalls 超时（默认 60s，P2/P3 用） |
| [`SessionMessage`](src/session/message.rs) | 驱动状态机流转的唯一入参（见 [消息协议](#33-消息协议)） |

**防幽灵设计**（turn_id 双重校验）：

- `finish_thinking(expected_turn_id, outcome)` 只接受与当前 `Thinking.turn_id` 匹配的收敛 —— 过期的 cancel / timeout / 旧任务结果会被丢弃，不污染新一轮状态。
- 派生任务外层 `catch_unwind` 兜底：即使收敛逻辑 panic，也强制恢复 `Idle`。

### 3.3 消息协议

内核的 `Envelope.metadata: HashMap<String, String>` 是专为扩展留的数据出口，本模块用 JSON 编解码类型化消息：

| 键 | 内容 |
|----|------|
| `_msg` | 序列化的 `SessionMessage`（请求） |
| `_reply` | 序列化的 `SessionReply`（回信） |

**消息类型**（`#[serde(tag = "kind")]`，新增类型只需加变体 + 编解码分支）：

| 消息 | 含义 | 状态 |
|------|------|------|
| `Chat { session_id, payload }` | 用户发起对话（触发 Idle → Thinking） | ✅ Phase 1 |
| `Interrupt { session_id }` | 中断当前思考（协作取消） | ✅ Phase 1 |
| `ToolResult { ... }` | 工具结果回写 | ✅ Phase 2 实现 |
| `Resume { ... }` | 等待项全部完成，进入下一轮思考 | ✅ Phase 2 实现 |
| `SubagentDone { ... }` | 子 Agent 完成通知 | 编解码就绪，预留（P3 走 Agent as Tool 路线未采用） |

**优先级约定**（对应内核三分桶：0..=49 High / 50..=149 Normal / >=150 Low）：

- `Interrupt` → `priority = 0`（High 桶，保证及时打断 Thinking）
- 其余 → `priority = 100`（Normal 桶）

**回信类型**（`SessionReply`）：`Success` / `Busy { turn_id }` / `Error { message }` / `Cancelled` / `Unhandled { reason }`。

### 3.4 `AgentRuntime` — 扩展入口（Phase 1 交付）

实现 `referee-core` 的 `Extension` trait，注册到 Kernel 后即为一个部署单元，管理 N 个会话（一个 Session = 一个 Agent 实例，会话级隔离，`DashMap` 存储，互不阻塞）。

关键行为：

| 行为 | 实现 |
|------|------|
| `handle` 零阻塞 | 只做「状态转移 + spawn 派生任务」，LLM 调用全部在派生任务中执行，`handle` 内无任何 await I/O |
| busy 拒绝显式可见 | 并发 Chat → 回 `SessionReply::Busy { turn_id }` + metrics 计数，不静默丢弃 |
| 会话容量有界 | `AgentConfig.max_sessions`（默认 100），超限回 `Error` 拒绝新会话 |
| 预算守门员 | `AgentConfig.budget`（session_limit / global_limit，0 = 无限制）：`handle_chat` 在 `start_thinking` 前置检查，超限回 `Error`，避免无效计费 |
| 语义缓存 | `AgentConfig.cache`（enabled / capacity / ttl）：相同请求命中缓存直接回 `Success`（`TurnOutcome::Cached`，**不计量 Token**）；仅无工具调用的响应落缓存；可整体禁用 |
| 终态收敛 + 回信 | 派生任务 finally 中 `finish_thinking` + `ctx.reply`（无锁、消费式） |
| 对等能力注入 | `with_tools` 自动注入 `Kernel`（对等 RPC）与 `ArtifactStore`（大结果落库）到 `ToolExecutor`；`register_peer_tool` 将目标 Session 注册为 Local 工具 |

**可观测**：tracing span（`agent_handle` / `agent_turn`，含 session_id / turn_id）；metrics（`referee_agent_turns_total{outcome}`、`referee_agent_busy_rejections_total`）。预算观测：`total_consumed_tokens()` / `session_consumed_tokens(session_id)`。

### 3.5 `tool/` — 工具调用与对等协作（Phase 2 + Phase 3 交付）

**定位**：工具统一抽象 + 有界注册表 + 并行执行器；对等智能体（另一 Runtime 上的 Session）作为 Local 工具接入。

| 组件 | 说明 |
|------|------|
| [`Tool`](src/tool/definition.rs) trait | `name` / `description` / `input_schema` / `execute` / `category()` |
| [`ToolCategory`](src/tool/definition.rs) | `Local`（内部调用，如对等 Agent RPC，不占 IO 槽位）/ `Remote`（外部 IO，受 Semaphore 限流，默认） |
| [`ToolContext`](src/tool/definition.rs) | 注入 `kernel`（对等 RPC）与 `artifact_store`（大结果落库），均为 `Option`；信任边界：仅授予可信注册工具 |
| [`ToolRegistry`](src/tool/registry.rs) | 有界注册表（DashMap + `max_tools`），声明自动导出为厂商格式 |
| [`ToolExecutor`](src/tool/executor.rs) | 并行执行：`Remote` 工具持 Semaphore permit，`Local` 工具直接并发；每轮 `max_per_turn` 截断 + 超时 + `catch_unwind` panic 隔离 |
| [`AgentTool`](src/tool/agent_tool.rs) | **对等智能体工具**：`category() = Local`，`execute` 经 `kernel.invoke` 同步 RPC 调用目标 Agent（带超时）；返回文本 > 4096 字节时写入 `ArtifactStore` 并显式授权调用者读取，仅回传 Artifact ID |

**死锁修复**：`ToolExecutor` 的 Semaphore 是「permit 持有至工具完成」模型；若 AgentTool 占用槽位等待目标 Agent、而目标 Agent 又需要槽位执行自身工具，则并发上限耗尽即死锁。`Local` 分类使对等调用不占 IO 槽位，验收测试（`resource_pool_deadlock_fixed`）验证 `max_concurrency=2` 下 A→B、C→D 全链路成功。

**循环调用拒绝（DAG 约束）**：A 调 B 时 A 处于 `AwaitingCalls`（Busy）；B 回调 A → `SessionReply::Busy` → 工具转错误回传，系统不挂死（`cyclic_call_rejected` 验证）。

### 3.6 `artifact/` — 工件存储（Phase 3 交付）

**定位**：对等协作的数据底座——有界、带 ACL 的成果载体。

| 组件 | 说明 |
|------|------|
| [`Artifact`](src/artifact/mod.rs) | 纯数据载体：`id / owner / allowed_readers / content_type / bytes / created_at` |
| [`ArtifactStore`](src/artifact/mod.rs) trait | `store` / `get(id, requester)` / `grant_access(id, owner, reader)`；**读取路径全鉴权**：仅 owner 或显式授权读者可读，杜绝「猜中 ID 即越权读取」 |
| [`InMemoryArtifactStore`](src/artifact/mod.rs) | 有界实现：数量 + 总字节双上限，超限回 `StoreError::CapacityExceeded`（背压硬约束） |

> 信任边界：写入路径（`store` / `grant_access`）调用方须为可信注册工具（当前唯一写入者为 `AgentTool`）；引入不可信工具前须为写入路径增加主体验证。

### 3.7 `budget/` — Token 预算治理

**定位**：Session 级 + 全局级双层级 Token 限额，前置阻断避免无效计费。

| 组件 | 说明 |
|------|------|
| [`BudgetConfig`](src/budget/mod.rs) | `session_limit` / `global_limit`（0 = 无限制），挂载于 `AgentConfig.budget` |
| [`BudgetError`](src/budget/mod.rs) | `SessionExceeded` / `GlobalExceeded`（含 used / limit） |
| [`TokenEstimator`](src/budget/mod.rs) | 厂商未返回 usage 时的保守估算（字符 × 2/3 + 1，向上取整，高估防超支） |
| [`tokens_from_response`](src/budget/mod.rs) | 统一计量口径：优先 `usage.total_tokens`，缺失则估算响应文本——Session 级与全局共用，保证一致 |
| 全局计数器 | `Arc<AtomicU64>`，`with_global_budget` 注入共享实例：**主 Agent + 子 Agent 注入同一计数器即系统级总预算**（子任务消耗计入总盘子） |

**语义（软限制）**：前置检查为 check-then-act——允许最后一次超额，其后拒绝（单轮消耗无法预知）；并发下最多超额一轮并发量。

### 3.8 `prompt/` — 提示词组装与预算截断（Phase 5 交付）

**定位**：将系统提示词 / 工具声明 / 对话历史 / 记忆 / 工件碎片统一组装为 `ChatRequest`，按 Token 预算做优先级截断，杜绝「Prompt 爆炸」。

| 组件 | 说明 |
|------|------|
| [`PromptFragment`](src/prompt/mod.rs) | 片段枚举（System / Tools / History / Memory / Artifacts），各带估算与截断策略 |
| [`build_prompt`](src/prompt/mod.rs) | 纯函数组装 + 预算分配：**优先级 System > Tools > History > Memory > Artifacts**；`Session.build_chat_request` 内部调用（预算取 `SessionConfig.prompt_budget_tokens`，默认 8000，0 = 不截断） |

**截断策略**：

- **System**（最高优先级）：预算不足时**按字符截断文本**（估算系数反推字符数并扣除截断后缀成本，保证截断后估算恒 ≤ 预算；CJK 安全），绝不整段丢弃。
- **Tools**：仅当整段超预算才整体丢弃；估算 = name + description + parameters JSON 求和（与总量上限断言同源）。
- **History**：滑动窗口保留最近 N 条，并修正首条角色配对——`tool_calls` 配对的 assistant 开头合法保留、裸 assistant / 悬空 tool 开头移除（OpenAI 协议 400 防护）。
- **Memory / Artifacts**（低优先级）：预算不足整体丢弃（P4 落地后接入注入）。

### 3.9 `cache/` — 内存 LRU/TTL 语义缓存（Phase 5 交付）

**定位**：相同请求直接返回缓存响应，避免重复 LLM 调用（降成本 + 降延迟）；命中时合成流保持流式语义一致。

| 组件 | 说明 |
|------|------|
| [`CacheKey`](src/cache/mod.rs) | `provider/model + content_hash + params_hash`——`params_hash` **覆盖全部影响输出的参数**（temperature / max_tokens / thinking enabled+effort / tool_choice / extra），杜绝不同参数错误共享缓存 |
| [`InMemoryCache`](src/cache/mod.rs) | 内存 LRU（DashMap + VecDeque 顺序队列）+ TTL；容量有界软上限；过期键在 evict 时惰性清理（无死键堆积）；`capacity = 0` 视为禁用 |
| [`synthetic_stream`](src/cache/mod.rs) | 将缓存响应按 10 字符切分为 `Delta` 块 + 末尾 `Finish`（携带 finish_reason + usage），拼接结果与原文完全等价 |
| [`CacheConfig`](src/cache/mod.rs) | `enabled / capacity / ttl`，挂载于 `AgentConfig.cache`；`CacheConfig::disabled()` 整体禁用 |

**集成语义**（与预算/状态机正交）：

- 缓存键基于**最终发送的 `ChatRequest`**（截断后），相同输入跨会话可命中。
- 命中走 `TurnOutcome::Cached`：回信 / 入 history 与 `Success` 完全等价，但**不计量 Token**（未发生真实 LLM 调用）；metrics 记 `outcome="cached"`。
- **仅无 `tool_calls` 的响应落缓存**（tool_call_id 是一次性 ID，重放会破坏工具流程）。
- 缓存读写均包 `catch_unwind`：缓存路径异常降级为正常 LLM 调用，绝不破坏终态收敛（无幽灵会话）。

---

## 4. 快速上手

```toml
[dependencies]
referee-agent = { path = "referee-agent" }   # 默认启用 xiaomi + deepseek
referee-core  = { path = "referee-core" }
tokio         = { version = "1", features = ["full"] }
```

```rust
use std::sync::Arc;
use referee_agent::provider::deepseek::{DeepSeekConfig, DeepSeekModel, DeepSeekProvider};
use referee_agent::session::{SessionMessage, SessionReply};
use referee_agent::{AgentConfig, AgentRuntime};
use referee_core::{Kernel, SupervisionPolicy};
use uuid::Uuid;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 内核 + 厂商适配器 + 运行时
    let kernel = Kernel::new();
    let provider = Arc::new(DeepSeekProvider::new(
        DeepSeekModel::V4Pro,
        DeepSeekConfig::new(std::env::var("DEEPSEEK_API_KEY")?),
    )?);
    let runtime = AgentRuntime::new(kernel.clone(), provider, AgentConfig::default());
    let runtime_id = runtime.id();
    kernel
        .register(Box::new(runtime), 64, SupervisionPolicy::Transient)
        .await?;

    // 2. 发起对话（invoke：请求-响应）
    let session_id = Uuid::new_v4();
    let msg = SessionMessage::Chat {
        session_id,
        payload: referee_agent::session::ChatPayload {
            message: referee_agent::provider::Message::user("你好"),
            options: Default::default(),
        },
    };
    let resp_env = kernel.invoke(runtime_id, msg.to_envelope(), 30_000).await?;
    let reply = SessionReply::from_envelope(&resp_env)?;
    println!("{reply:?}");

    // 3. 中断思考（可选；Interrupt 走 High 优先级桶）
    let interrupt = SessionMessage::Interrupt { session_id };
    let resp_env = kernel.invoke(runtime_id, interrupt.to_envelope(), 1_000).await?;
    let reply = SessionReply::from_envelope(&resp_env)?;
    println!("{reply:?}");

    Ok(())
}
```

---

## 5. 设计约束（AGENT_RUNTIME_PLAN §2 落地情况）

| # | 约束 | 落地位置 |
|---|------|---------|
| 1 | 终态自管：四路径 + panic 必须收敛 Session 状态 | `session/task.rs`（`run_turn`）+ `converge_and_reply` finally 收敛 |
| 2 | 协作式取消唯一：`oneshot` 通道，不用 `abort()` | `SessionState::Thinking.cancel`（`Option<oneshot::Sender>`，`take()` 发送） |
| 3 | 禁止跨 await 持 guard：先取快照、释放、再 await | `handle_chat`：状态转移与 `build_chat_request` 在 guard 内同步完成，guard drop 后才 `tokio::spawn` |
| 4 | busy 拒绝显式可见 | `SessionReply::Busy { turn_id }` + `referee_agent_busy_rejections_total` |
| 5 | emit 失败必须可见（P2 起适用） | 预留：派生任务回信失败 `warn!` 记录 |
| 6 | 所有通道有界 | `max_sessions` 上限、`max_history` FIFO 淘汰、`AgentConfig` 可配 |
| 7 | 不引入白名单外依赖 | 依赖仅 `referee-core` + 白名单库 + 已批准 `reqwest` / `serde_json` |
| 8 | handle 内零阻塞 | `handle` 只做状态转移 + spawn，无 await I/O |
| 9 | 内存上界可证明 | history / 会话数均有界；测试含洪泛断言（见 [测试覆盖](#7-测试覆盖)） |

---

## 6. 运行模型

- **一个 `AgentRuntime` 扩展 = 一个部署单元**，管理 N 个 `Session`（会话级隔离），所有会话共存于 `DashMap`，互不阻塞。
- **对等智能体协作（Phase 3，Agent as Tool 路线）**：另一 Runtime 上的 Session 经 `AgentTool`（Local 工具）注册为本 Runtime 的能力；`execute` 内 `kernel.invoke` 同步 RPC 调用目标 Agent（在派生任务中执行，不违反 handle 零阻塞）。循环调用（A→B→A）被 `Busy` 拒绝，系统不挂死；大结果经 `ArtifactStore` 落库并按 ACL 授权读取。
- **长耗时 I/O（LLM、工具、对等 RPC 等待）全部在「终态自管」的派生任务中执行**，`handle` 永远快速返回、永不跨 await 持锁 —— `Interrupt` 消息总能被及时处理（High 桶投递）。
- **消息驱动循环**：`Chat` / `Interrupt` / `ToolResult` / `Resume` 驱动状态机流转，不使用直接递归（防栈溢出），不等待任何响应（emit-only）。

---

## 7. 测试覆盖

共 **146 条测试全绿**（referee-agent 侧；workspace 合计 171 条）：

| 测试文件 | 条数 | 覆盖点 |
|----------|------|--------|
| `src/`（单元测试） | 75 | 状态机转移（Idle↔Thinking、busy 拒绝、取消、过期 turn_id 忽略）、history 有界、`run_turn` 五路径、消息编解码往返、工具（Local 绕过 Semaphore / Remote 限流 / panic 隔离 / 截断）、Artifact ACL（owner / 授权读者 / 拒绝未授权 / 容量上限）、预算估算与统一口径、Session 计量累加、**prompt 预算截断**（优先级顺序 / System 字符截断 / CJK 无 panic / 角色配对）、**缓存**（LRU 顺序 / TTL / 死键清理 / 参数进键 / 合成流拼接） |
| `tests/deepseek_test.rs` | 13 | DeepSeek 适配器：请求体组装、流式、错误归一（400/401/402/422/429/500）、重试行为、thinking 开关 |
| `tests/xiaomi_test.rs` | 13 | MiMo 适配器：同上 + `reasoning_content` / usage 解析、多轮保真 |
| `tests/equivalence_test.rs` | 5 | 跨厂商语义等价：同一请求 → 两个适配器结果等价；流式收敛 == 一次性响应 |
| `tests/session_test.rs` | 14 | Phase 1/2 验收：busy 拒绝、中断（含二次中断）、四路径 + panic 无幽灵、工具多轮循环、会话容量上限 |
| `tests/tool_test.rs` | 9 | Phase 2 验收：并行执行、截断、panic 隔离、背压、向后兼容、多轮循环 |
| `tests/peer_test.rs` | 4 | Phase 3 验收：资源池死锁修复、循环调用拒绝、Artifact ACL 端到端、对等注册并行调用 |
| `tests/budget_test.rs` | 6 | 预算验收：会话级阻断、全局级阻断、计量准确性、并发安全、子 Agent 共享全局预算、估算兜底 |
| `tests/cache_test.rs` | 7 | **Phase 5 验收**：命中（相同输入二次调用 LLM 计数 = 1）、LRU 容量淘汰 / get 刷新顺序、TTL 过期、含 tool_calls 响应不缓存、缓存命中不计量 Token、缓存可禁用 |

验收口径（AGENT_RUNTIME_PLAN §5.1 / §5.2 / §5.3 / §5.4 + 预算治理 + §5.6）：

- ✅ 中断：协作取消信号及时打断 Thinking，Interrupt 走 High 桶
- ✅ 幽灵治理：success / error / cancel / timeout / panic 全部收敛 Idle，无幽灵会话
- ✅ 挂死心跳：mock LLM 挂起不阻塞其他会话与内核
- ✅ busy 拒绝可见：并发 Chat → 明确 `Busy` 回信，扩展不熔断
- ✅ 错误归一与重试：仅 `Network / Server / RateLimited` 重试，指数退避受上限
- ✅ 工具闭环：并行执行 + 截断 + panic 隔离 + 多轮 resume；对等协作无死锁、循环调用被拒
- ✅ 工件 ACL：未授权者读取被拒；存储有界（数量 + 字节双上限）
- ✅ 预算治理：Session / 全局双层级阻断；并发原子累加无丢失；子 Agent 消耗并入共享全局；usage 缺失时保守估算不为 0
- ✅ Phase 5 缓存：相同输入二次调用 LLM 计数 = 1；LRU 超限淘汰 / get 刷新顺序 / TTL 过期失效；含工具调用响应不落缓存；缓存命中不计量 Token；预算截断总量恒 ≤ 上限（System/Tools 保留、History/Artifacts 丢弃）；合成流拼接 == 原文（Delta 分块 + Finish）

## 8. 验证命令

```bash
cargo test -p referee-agent                 # 146 条测试
cargo test --workspace                      # 全量回归（core 25 + agent 146 = 171 条）
cargo clippy -p referee-agent --all-targets -- -D warnings
cargo fmt --check
```

## 9. 路线图

| 阶段 | 主题 | 状态 |
|------|------|------|
| Phase 0 | 厂商抽象层（LLMProvider + 适配器 + 流式 + 错误归一） | ✅ 完成 |
| Phase 1 | 会话状态机（并发正确性 + 中断 + 幽灵治理 + 消息驱动） | ✅ 完成 |
| Phase 2 | 工具调用（Tool trait + 并行执行 + 结果回写） | ✅ 完成 |
| Phase 3 | 对等智能体协作 + 工件存储（Agent as Tool + ArtifactStore ACL） | ✅ 完成 |
| 预算治理 | Token 双层级限额（Session 级 + 全局共享计数器） | ✅ 完成 |
| Phase 4 | 记忆模块（三层记忆 + 注入策略 + 容量） | ⏳ 待开发 |
| Phase 5 | 提示词与缓存（PromptBuilder 预算截断 + 内存 LRU/TTL 缓存 + 合成流） | ✅ 完成 |
| Phase 6 | 计量与可观测（Token 用量 + 全链路 tracing + metrics） | ⏳ 待开发 |
| Phase 7 | MCP 与 Skills（stdio 桥 + Skills 注册） | ⏳ 待开发 |

> 阶段顺序与验收标准详见 [`../AGENT_RUNTIME_PLAN.md`](../AGENT_RUNTIME_PLAN.md)（规划文档）。
> Phase 3 实现路线与原规划的差异（Agent as Tool 同步 invoke vs emit 异步派发）见该文档 §5.4 偏差记录；
> Phase 5 偏差（缓存键含全部参数、不缓存工具响应、缓存命中不计量、流式验收由合成流单测覆盖等）见 §5.6 偏差表。

---

## 10. 相关文档

| 文档 | 说明 |
|------|------|
| [`../README.md`](../README.md) | 仓库总览（目录 / 大纲） |
| [`../AGENT_RUNTIME_PLAN.md`](../AGENT_RUNTIME_PLAN.md) | Agent 运行时落地计划（阶段验收标准） |
| [`../referee-core/README.md`](../referee-core/README.md) | 内核模块（通信与治理）描述 |
| [`../PHASE_STATUS.md`](../PHASE_STATUS.md) | Phase 状态跟踪（referee-core 侧） |
| [`../AGENTS.md`](../AGENTS.md) | 工程约束（设计思想 / 依赖清单 / 工作纪律） |

# Referee Agent — 智能体运行时模块

> 基于 `referee-core` 的**可选 SDK 式扩展模块**：提供 LLM 厂商抽象与多会话状态机，让业务方以「一个内核扩展」的方式获得完整智能体能力。
> 当前进度：**Phase 0（厂商抽象层）+ Phase 1（会话状态机）已完成，65 条测试全绿**。详见 [路线图](#路线图)。

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
│  │  + 厂商适配器         │   │  / 消息协议 / 超时治理         │  │
│  └──────────────────────┘   └──────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

**分层依赖规则**（可拓展性的根基）：

- `provider` 不依赖任何上层模块；`session` 只依赖 `provider`。
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
| [`Session`](src/session/mod.rs) | 纯状态持有者（不含 I/O 句柄）：`start_thinking` / `cancel_thinking` / `finish_thinking`（finally 式唯一终态写入）、有界 history（FIFO 淘汰）、`turn_id` 单调递增 |
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
| `ToolResult { ... }` | 工具结果回写 | 编解码就绪，P2 实现 |
| `Resume { ... }` | 等待项全部完成，进入下一轮思考 | 编解码就绪，P2 实现 |
| `SubagentDone { ... }` | 子 Agent 完成通知 | 编解码就绪，P3 实现 |

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
| 会话容量有界 | `AgentConfig.max_sessions`（默认 1024），超限回 `Error` 拒绝新会话 |
| 终态收敛 + 回信 | 派生任务 finally 中 `finish_thinking` + `ctx.reply`（无锁、消费式） |
| Phase 1 边界 | 模型返回 `tool_calls` 时强制回 Idle 并回传完整响应（调用方自行处理）；P2/P3 消息回 `Unhandled` |

**可观测**：tracing span（`agent_handle` / `agent_turn`，含 session_id / turn_id）；metrics（`referee_agent_turns_total{outcome}`、`referee_agent_busy_rejections_total`）。

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
- **子 Agent 不是进程/线程**（P3）：另一个 Session + 消息路由，主 Agent 经 `emit` 派发子任务，完成时写 Artifact 并回完成通知。
- **长耗时 I/O（LLM、工具、子 Agent 等待）全部在「终态自管」的派生任务中执行**，`handle` 永远快速返回、永不跨 await 持锁 —— `Interrupt` 消息总能被及时处理（High 桶投递）。
- **消息驱动循环**：五种消息类型驱动状态机流转，不使用直接递归（防栈溢出），不等待任何响应（emit-only）。

---

## 7. 测试覆盖

共 **65 条测试全绿**（含 16 条单元测试）：

| 测试文件 | 条数 | 覆盖点 |
|----------|------|--------|
| `src/`（单元测试） | 16 | 状态机转移（Idle↔Thinking、busy 拒绝、取消、过期 turn_id 忽略）、history 有界、`run_turn` 五路径（success / cancel / timeout / error / panic）、消息编解码往返、Interrupt 高优先级 |
| `tests/deepseek_test.rs` | 14 | DeepSeek 适配器：请求体组装、流式、错误归一（400/401/402/422/429/500）、重试行为、thinking 开关 |
| `tests/xiaomi_test.rs` | 13 | MiMo 适配器：同上 + `reasoning_content` / usage 解析、多轮保真 |
| `tests/equivalence_test.rs` | 9 | 跨厂商语义等价：同一请求 → 两个适配器结果等价；流式收敛 == 一次性响应 |
| `tests/session_test.rs` | 13 | Phase 1 验收：busy 拒绝、中断（含二次中断）、**四路径 + panic 无幽灵**、tool_calls 回 Idle、P2/P3 消息 Unhandled、会话容量上限 |

验收口径（AGENT_RUNTIME_PLAN §5.1 / §5.2）：

- ✅ 中断：协作取消信号及时打断 Thinking，Interrupt 走 High 桶
- ✅ 幽灵治理：success / error / cancel / timeout / panic 全部收敛 Idle，无幽灵会话
- ✅ 挂死心跳：mock LLM 挂起不阻塞其他会话与内核
- ✅ busy 拒绝可见：并发 Chat → 明确 `Busy` 回信，扩展不熔断
- ✅ 错误归一与重试：仅 `Network / Server / RateLimited` 重试，指数退避受上限

## 8. 验证命令

```bash
cargo test -p referee-agent                 # 65 条测试
cargo test --workspace                      # 全量回归（core 25 + agent 65 = 90 条）
cargo clippy -p referee-agent --all-targets -- -D warnings
cargo fmt --check
```

## 9. 路线图

| 阶段 | 主题 | 状态 |
|------|------|------|
| Phase 0 | 厂商抽象层（LLMProvider + 适配器 + 流式 + 错误归一） | ✅ 完成 |
| Phase 1 | 会话状态机（并发正确性 + 中断 + 幽灵治理 + 消息驱动） | ✅ 完成 |
| Phase 2 | 工具调用（Tool trait + 并行执行 + 结果回写） | ⏳ 待开发 |
| Phase 3 | 子 Agent 与成果（ArtifactStore + 派发/聚合 + 可见性注入） | ⏳ 待开发 |
| Phase 4 | 记忆模块（三层记忆 + 注入策略 + 容量） | ⏳ 待开发 |
| Phase 5 | 提示词与缓存（PromptBuilder + 预算 + 缓存命中） | ⏳ 待开发 |
| Phase 6 | 计量与可观测（Token 用量 + 全链路 tracing + metrics） | ⏳ 待开发 |
| Phase 7 | MCP 与 Skills（stdio 桥 + Skills 注册） | ⏳ 待开发 |

> 阶段顺序与验收标准详见 [`../AGENT_RUNTIME_PLAN.md`](../AGENT_RUNTIME_PLAN.md)（规划文档）。

---

## 10. 相关文档

| 文档 | 说明 |
|------|------|
| [`../README.md`](../README.md) | 仓库总览（目录 / 大纲） |
| [`../AGENT_RUNTIME_PLAN.md`](../AGENT_RUNTIME_PLAN.md) | Agent 运行时落地计划（阶段验收标准） |
| [`../referee-core/README.md`](../referee-core/README.md) | 内核模块（通信与治理）描述 |
| [`../PHASE_STATUS.md`](../PHASE_STATUS.md) | Phase 状态跟踪（referee-core 侧） |
| [`../AGENTS.md`](../AGENTS.md) | 工程约束（设计思想 / 依赖清单 / 工作纪律） |

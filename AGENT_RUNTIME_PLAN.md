# Referee Agent Runtime 落地计划 v1.0

> 状态：已批准（D1/D2 通过） · 性质：规划文档，非实现
> 定位：`referee-agent` 作为**可选 SDK 式扩展模块**，独立于内核，不触碰 `referee-core` 任何代码；用户按需求启用，我们提供但不强制。

---

## 1. 定位与边界

### 1.1 为什么是独立 crate（而不是放进 referee-core）

- **AGENTS.md 硬约束**：「内核是最小引擎，不承载业务逻辑、不预置扩展；模块按需组合」。Agent 运行时是完整的业务级组件（LLM 调用、工具、记忆、缓存），放进内核会同时破坏"轻量"与"零冗余"两条原则。
- **隔离收益**：独立 crate 意味着它不编译进不使用它的项目；依赖（reqwest、serde_json）只在启用时进入依赖树。内核与 Agent 层各自的故障、测试、演进互不传染。
- **SDK 语义**：`referee-agent` 面向需要"智能体能力"的用户，与面向"通信与治理"的 `referee-core` 是两个正交消费面。用户只装自己需要的。

### 1.2 为什么可以完全不动内核

上一版设计的多个问题（锁竞态、幽灵会话、中断、跨 await 持锁）**全部是扩展侧实现问题**，不是内核能力缺失。内核已提供全部所需原语：

| Agent 层需求 | 内核已提供 |
|---|---|
| 消息路由、即发即弃 | `KernelContext::emit`（`extension/context.rs:46`） |
| 中断 / 优先级 | `Envelope.priority` 三分桶（High 桶，`kernel/priority.rs`） |
| 背压 | 有界通道满即 `ResourceExhausted` + 自动 DLQ（`kernel/mod.rs:277`） |
| 崩溃隔离 | `catch_unwind` 熔断（`kernel/supervisor.rs:177`） |
| 进程级兜底 | WAL 重放（`kernel/mod.rs:177`） |
| 受限上下文 | `KernelContext` 编译期禁止 invoke（防嵌套死锁） |

Agent 层只需要**组合**这些原语。这带来一个明确验收口径：**实现 `referee-agent` 期间 `referee-core` 零改动**（除 Cargo 依赖关系外）。

### 1.3 启用方式（提供不强制）

- 使用方在 `Cargo.toml` 中按需引入 `referee-agent`，并用 features 裁剪能力：

```toml
referee-agent = { path = "referee-agent", features = ["openai", "mcp-stdio"] }
```

- feature 清单（默认 `["openai"]` 最小集，保证开箱可用）：

```toml
[features]
default = ["openai"]
openai        = []   # OpenAI Chat Completions 适配器
anthropic     = []   # Anthropic Messages 适配器
responses     = []   # OpenAI Responses API 适配器
mcp-stdio     = []   # MCP stdio 传输桥（零新依赖）
memory-persist = []  # 记忆/成果落盘（接 referee-core WalSink 抽象）
```

- 未启用的适配器**不参与编译**，通过 `#[cfg(feature = "...")]` 隔离；trait 与核心状态机始终存在，保证接口稳定。

---

## 2. 设计原则（横切约束，附理由）

以下 9 条是所有阶段的**共同验收前提**。每条都对应上一版评审中暴露的真实缺陷。

1. **终态自管**：所有派生任务包 `catch_unwind` + finally 式清理，成功/失败/取消/panic 四路径必须收敛 Session 状态。
   *为什么*：上一版"占位 Thinking + 外部 alter 补写"制造了双窗口竞态，任务先结束、alter 后覆盖 → 永久幽灵会话。终态由任务自己收敛后，不存在窗口。
2. **协作式取消唯一**：只用 `oneshot` 取消通道，不用 `JoinHandle::abort()` 做业务取消。
   *为什么*：abort 强杀任务会跳过清理与错误回复，且 LLM 侧连接是否释放取决于厂商实现。协作取消让任务走统一的 finally 收敛。
3. **禁止跨 await 持 guard**：任何 `DashMap` guard / 锁，先取快照、释放、再 await。
   *为什么*：上一版 `get_mut` 跨 `store().await` 持锁，阻塞整个 shard 且 future 非 Send。这是"无锁 I/O"原则的落地形式。
4. **busy 拒绝显式可见**：并发提交被拒 → 回拒信或进 DLQ + 日志，绝不静默 `Err` 蒸发。
   *为什么*：supervisor 对 handle 返回 `Err` 只是吞掉 + ACK WAL（`supervisor.rs:189`），调用方永远不知道"Agent 忙"。静默丢弃违反背压"安全降级"语义。
5. **emit 失败必须可见**：工具/子 Agent 派发失败 → `tracing::warn!` + DLQ，绝不 `let _ =`。
   *为什么*：上一版 `let _ = ctx_clone.emit(...)` 让派发失败静默丢失 → pending 永久等待直到超时。背压拒绝必须可观测、可审计。
6. **所有通道有界**：工具数量、并发 LLM、成果容量、记忆条目、缓存容量全部有上限，超限行为明确（截断/淘汰/拒绝）。
   *为什么*：AGENTS.md「背压是硬约束：绝不允许无限制内存分配」；上一版 history 无界增长直接违反。
7. **不引入白名单外依赖**（D1/D2 已批准除外）；token 估算用字符/字节近似 + 厂商 usage 校准，不引 tokenizer。
   *为什么*：AGENTS.md 依赖清单约束；tiktoken 需下载模型文件，破坏"轻量"且是供应链风险。
8. **handle 内零阻塞**：所有 await 只出现在派生任务中，`handle` 本身只做状态转移与消息派发。
   *为什么*：AGENTS.md「阻塞即违规」；handle 阻塞会拖死同扩展后续消息（含 interrupt）。
9. **内存上界可证明**：每个 Phase 的测试都含"洪泛后队列深度/内存不增长"断言。
   *为什么*：背压是项目第一验证目标（Phase 1 路线图），Agent 层必须继承同一验收口径。

---

## 3. 架构与模块划分

### 3.1 目录结构

```
referee-agent/
├── Cargo.toml                      # 依赖：referee-core + reqwest(D1) + serde_json(D2) + 白名单库
├── src/
│   ├── lib.rs                      # AgentRuntime 扩展入口（实现 referee-core Extension trait）
│   ├── provider/                   # 【厂商适配层】唯一 I/O 边界
│   │   ├── mod.rs                  #   LLMProvider trait、ProviderId、ProviderCapabilities
│   │   ├── openai.rs               #   OpenAI Chat Completions 适配器   (#[cfg(feature="openai")])
│   │   ├── anthropic.rs            #   Anthropic Messages 适配器       (#[cfg(feature="anthropic")])
│   │   └── responses.rs            #   OpenAI Responses API 适配器     (#[cfg(feature="responses")])
│   ├── session/                    # 【会话状态机】并发正确性核心
│   │   ├── mod.rs                  #   SessionState 状态机 + 消息驱动
│   │   ├── task.rs                 #   后台任务 wrapper：catch_unwind + finally 终态收敛
│   │   └── timeout.rs              #   超时治理（Thinking / AwaitingCalls 双 deadline）
│   ├── tool/                       # 【工具层】FunctionCall 统一抽象
│   │   ├── mod.rs                  #   Tool trait、Registry（有界注册表）
│   │   ├── executor.rs             #   并行执行器（Semaphore + 每轮上限）
│   │   └── bridge.rs               #   MCP / Skills → Tool 桥接
│   ├── agent/                      # 【子 Agent 与成果】
│   │   ├── mod.rs                  #   子任务派发、完成聚合（复用 AwaitingCalls 通道）
│   │   └── artifact.rs             #   ArtifactStore（有界、哈希、可见性白名单）
│   ├── memory/                     # 【记忆】全局 / 项目 / 会话三层
│   ├── prompt/                     # 【提示词】PromptBuilder + 预算分配 + 缓存
│   ├── usage/                      # 【计量】Token 估算 + 厂商 usage 校准
│   └── observe/                    # 【可观测】tracing 全链路 + metrics
└── tests/                          # 阶段验收测试（沿用 referee-core 测试风格）
```

### 3.2 分层依赖规则（可拓展性的根基）

- `provider` 不依赖任何上层模块；`session` 只依赖 `provider`。
- `tool` / `agent` / `memory` / `prompt` / `usage` 之间**只通过 trait 引用**，不引用具体实现——任一模块可整体替换或 mock 单测。
- 所有存储（Artifact、Memory、Cache）都是 trait + 有界默认实现（如 `ArtifactStore` / `MemoryStore` / `CacheStore`），持久化行为通过 trait 替换。

*为什么*：分层依赖规则保证"模块化、易维护、易拓展"落到结构上而非口号：新增厂商 = 新增适配器文件；新增存储后端 = 新增 trait 实现；新增能力 = 新增模块，均不触碰既有代码。

---

## 4. 运行模型

- **一个 `AgentRuntime` 扩展 = 一个部署单元**，管理 N 个 `Session`（一个 Session = 一个 Agent 实例，会话级隔离）。所有 Session 共存于扩展内的 `DashMap`，互不阻塞。
- **子 Agent 不是进程/线程**，而是另一个 Session + 消息路由：主 Agent 经 `emit` 派发子任务消息，子 Agent 完成时写 Artifact 并回完成通知。
- **长耗时 I/O（LLM、工具、子 Agent 等待）全部在"终态自管"的派生任务中执行**，`handle` 永远快速返回、永不跨 await 持锁。interrupt 消息因此总能被及时处理（优先级 High 桶投递）。
- **消息驱动循环**：`chat` / `tool_result` / `resume` / `interrupt` / `subagent_done` 五种消息类型驱动状态机流转，不使用直接递归（防栈溢出），不等待任何响应（emit-only）。

---

## 5. 阶段计划

### 5.0 阶段总览

| 阶段 | 名称 | 核心交付 | 前置 |
|---|---|---|---|
| P0 | 厂商抽象层 | LLMProvider trait + 3 适配器 + 流式 + 错误归一 | D1/D2 |
| P1 | 会话状态机 | 并发正确性 + 中断 + 幽灵治理 + 消息驱动循环 | P0 |
| P2 | 工具调用 | Tool trait + 并行执行 + 结果回写 | P1 |
| P3 | 子 Agent 与成果 | ArtifactStore + 派发/聚合 + 可见性注入 | P2 |
| P4 | 记忆模块 | 三层记忆 + 注入策略 + 容量 | P1 |
| P5 | 提示词与缓存 | PromptBuilder + 预算 + 缓存命中 | P0/P4 |
| P6 | 计量与可观测 | Token 用量 + 全链路 tracing + metrics | P0–P5 |
| P7 | MCP 与 Skills | MCP stdio 桥 + Skills 注册 | P2 |

**为什么是这个顺序**：
- P0 是唯一 I/O 边界，先立边界，后续所有模块都只面对统一接口，不写厂商分支。
- P1 是并发正确性核心——上一版失败的根源全在状态机；不先修它，上层全部建立在流沙上。
- P2 建立"工具最小可用路径"，P3 的子 Agent **复用 P2 的 AwaitingCalls 统一等待通道**（工具与子 Agent 同一 pending 机制），避免两套状态机。
- P4/P5 依赖 P0/P1 的会话与预算能力，放后不阻塞主路径。
- P6 贯穿始终但收口计量与指标（需要全部上游产生数据）。
- P7 外部集成最重（子进程管理、协议桥接），放最后且默认零新依赖。

### 5.1 Phase 0 — 厂商抽象层

**目标**：确立唯一 I/O 边界；OpenAI Chat Completions / Anthropic Messages / OpenAI Responses 三个厂商在统一接口下等价可用；厂商特殊能力通过能力声明驱动上层**自动降级**，上层不写厂商分支。

**关键接口**：

```rust
/// 厂商能力声明 — 上层据此降级，不写死厂商分支
pub struct ProviderCapabilities {
    pub parallel_tool_calls: bool,   // 不支持 → 调度层自动串行
    pub system_role: bool,           // 不支持 → system 前缀到首条 user
    pub streaming: bool,
    pub usage_reported: bool,        // 不回 usage → 走估算
    pub max_output_tokens: usize,
}

#[async_trait]
pub trait LLMProvider: Send + Sync {
    fn id(&self) -> ProviderId;
    fn capabilities(&self) -> &ProviderCapabilities;
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, LlmError>;
    /// 流式：chunk 收敛后必须与 chat() 语义等价
    async fn chat_stream(&self, req: ChatRequest)
        -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError>;
}

pub enum LlmError {
    Network, Timeout,
    RateLimited { retry_after: Option<Duration> },
    BadRequest(String), Server(String), Auth, Cancelled,
}
```

**验收标准**：
1. 同一 `ChatRequest` 在三个适配器（mock 服务器）下语义等价；流式收敛 == 一次性响应。
2. 错误归一：超时/限流/4xx/5xx 全部映射 `LlmError`；重试仅对 `RateLimited/Server/Network`，指数退避且受次数上限。
3. 能力降级有测试：声明 `parallel_tool_calls=false` 的厂商 → 调度层自动串行化。
4. `cargo check` + 测试全绿；依赖仅 D1/D2 + 白名单。

### 5.2 Phase 1 — 会话状态机与消息驱动

**目标**：修复上一版全部并发缺陷，建立"永不幽灵、永不阻塞、可中断"的会话核心。**本项目最关键的一阶段。**

**状态机**（统一工具与子 Agent 的等待态）：

```rust
enum SessionState {
    Idle,
    Thinking { turn_id: u64, cancel: oneshot::Sender<()>, deadline: Instant },
    AwaitingCalls { deadline: Instant, pending: HashMap<String, PendingKind> }, // 工具 / 子Agent 统一
}
```

**终态自管 wrapper**（`session/task.rs`）：

```rust
/// 运行一轮 LLM 任务；无论 Ok/Err/Cancelled/Panic，都经 catch_unwind 收敛后
/// 调用 on_exit 做唯一一次终态写入 — 外部禁止再 alter 状态。
pub async fn run_turn<F, Fut>(mut f: F, on_exit: impl FnOnce(TurnOutcome) + Send + 'static)
where F: FnMut() -> Fut, Fut: Future<Output = TurnOutcome> + Send;
```

**验收标准**：
1. **中断**：Thinking 期间收 interrupt → 协作取消 → 终态 Idle，无幽灵、无 abort 残留。
2. **幽灵治理**：pending 永不返回 → 超时后会话自动恢复 Idle + DLQ 记录；洪泛 10k 条后队列深度/内存不增长。
3. **挂死心跳**：mock LLM 永不返回 → 不阻塞其他会话与内核（isolation 风格）。
4. **busy 拒绝可见**：并发 chat → 明确拒绝消息 + DLQ 落一条，扩展不熔断。
5. **resume 循环**：等待项全部完成后自动进入下一轮思考，断言轮次递增（修复上一版断掉的流程）。

### 5.3 Phase 2 — 工具调用（FunctionCall 统一抽象）

**目标**：工具定义、发现、并行执行、结果回写闭环；背压与隔离达标。

**关键接口**：

```rust
#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn input_schema(&self) -> serde_json::Value;   // JSON Schema → 各厂商工具声明格式
    async fn execute(&self, ctx: ToolContext, args: serde_json::Value)
        -> Result<ToolOutput, ToolError>;            // 允许内部自管耗时/spawn
}
```

- 注册表有界（DashMap + 上限）；工具声明自动转 OpenAI / Anthropic / Responses 三种格式。
- 并行执行器：`Semaphore` 并发上限 + 每轮 `max_tools_per_turn` 上限；结果经 `tool_result` 消息回写收敛入 `AwaitingCalls.pending`。

**验收标准**：
1. **并行**：一次 5 个调用并发执行，总耗时 ≈ max(单个) 而非 sum；全部一一回写。
2. **上限**：响应 100 个 tool_calls → 截断到上限，多余进 DLQ + 日志。
3. **隔离**：某工具 panic → 该调用回错误，其余调用与注册表不受影响（catch_unwind 在 execute 边界）。
4. **背压**：工具结果洪泛 → 有界队列满 → 明确错误，无 OOM。

### 5.4 Phase 3 — 子 Agent 与成果管理

**目标**：主 Agent 并行派生 N 个子 Agent；成果落库；调用时按白名单注入可见成果。

**机制**：
- `Artifact`：`id / hash / bytes / meta / source_agent / created_at / ttl`；`ArtifactStore` 有界（容量 + 最旧淘汰）。
- 子 Agent = 另一 Session：主 Agent 经 `emit` 派发子任务消息（含 `parent_session_id`、任务、成果规格）；子 Agent 完成时写 Artifact + emit 完成通知（含 artifact_id 列表）。
- 主 Agent 在 `AwaitingCalls.pending` 中记 N 个子任务；全部收敛后进入 resume。
- **可见性注入（安全关键）**：主 Agent 的调用请求带 `artifact_ids` 白名单 → 注入 prompt 前**仅取白名单内且仍属该项目命名空间**的 Artifact（双重校验，防越权注入）。

**验收标准**：
1. 1 主 + 3 子并发完成，主收到全部成果；总耗时 ≈ max(子)。
2. **可见性**：仅白名单内 artifact 出现在注入后的 prompt 中（断言 prompt 文本）；白名单外的即使存在也不注入。
3. **容量**：成果超限 → 最旧淘汰或拒绝，行为明确，无 OOM。
4. **隔离**：子 Agent 崩溃 → 主收到失败通知并继续运行，内核不受影响。

### 5.5 Phase 4 — 记忆模块

**目标**：三层记忆 + 注入策略 + 容量治理。

| 层 | 作用域 | 生命周期 | 存储 |
|---|---|---|---|
| 全局 | 所有 Agent | 永久 | `MemoryStore` trait（默认有界内存；`memory-persist` feature 下接 referee-core `WalSink`/文件） |
| 项目 | 单 project 命名空间 | 项目生命周期 | 同上 |
| 会话 | 单 Session | 会话内 | history 窗口化（最近 N 轮 + 摘要压缩） |

- 注入策略：按相关性排序 + token 预算截断（与 P5 共用同一分配器）。
- 写入走 emit 消息驱动（异步落库，不在 handle 内 await）。

**验收标准**：
1. **三层隔离**：全局/项目/会话互不可见，命名空间断言。
2. **容量**：每层超限行为明确（截断/淘汰），洪泛后无 OOM。
3. **注入预算**：记忆 + 历史 + 成果 + 工具声明 总和 ≤ 配置 token 上限（P5 断言）。
4. **可替换**：mock `MemoryStore` 断言写穿与读回；不依赖具体存储实现。

### 5.6 Phase 5 — 提示词组装与缓存

**目标**：PromptBuilder 统一组装 + 预算分配 + 缓存命中，杜绝"Prompt 爆炸"。

- 段落优先级（超限按序截断）：`system > 工具声明 > 历史 > 项目/全局记忆 > 会话记忆 > 成果注入`。
- 缓存：`CacheKey = provider + model + prompt_hash + params`；命中返回缓存响应（合成流）；不命中调用后按策略落缓存；**内存 LRU 自实现（dashmap，容量有界）+ TTL**，不引 lru crate。

**验收标准**：
1. **命中**：相同输入二次调用 → 命中，LLM 调用计数 = 1（计数 mock 断言）。
2. **容量/TTL**：缓存超限淘汰、过期失效，均有测试。
3. **预算**：超长输入按优先级截断，总量恒 ≤ 上限；断言截断后的实际 token 估算值。
4. 流式缓存：完整响应收敛后落缓存，再次命中返回等价合成流。

### 5.7 Phase 6 — Token 计量与可观测

**目标**：用量可算、全链路可追、指标可断言。

- `TokenUsage { prompt, completion, total }`：厂商 `usage` 字段优先（`usage_reported=true`），缺失走 `estimate_tokens`（字符/字节近似，零依赖）。
- tracing：沿用 Referee 的 `trace_id` / `correlation_id`（内核已注入），agent 内部 span 树（turn → llm → tool → subagent）全部挂接。
- metrics：`referee_agent_*`（调用数、token 用量累计、缓存命中率、工具/子 Agent 成功率、队列深度）。

**验收标准**：
1. 计量正确：mock 厂商回 `usage` → 断言累计；不回 → 断言回退估算。
2. **全链路**：一条消息从 emit → 子 Agent → 成果，trace_id 贯穿整棵 span 树（断言 span 层级与 ID）。
3. metrics 输出可断言（复用 `observability_test.rs` 收集方式）。

### 5.8 Phase 7 — MCP 与 Skills 适配

**目标**：外部能力接入；默认零新依赖。

- **Skills**：注册式提示片段/能力单元（`name/description/usage`），复用 Tool 注册表或纯注入型；Agent 按需选择注入。
- **MCP**：默认仅 **stdio 传输**（`tokio::process` 自带，白名单内）：子进程管理（spawn、有界 stdout 读取、超时、崩溃重启/熔断、退出清理）；MCP 工具发现后映射为 `Tool` 代理。HTTP/SSE 传输留 trait 扩展点（需新依赖，默认不启用）。

**验收标准**：
1. Skills：声明后出现在工具/提示清单，可被调用、可被注入。
2. MCP stdio：发现 N 个工具 → 调用 → 结果回写；MCP 进程崩溃 → 工具失败显式可见，Agent 与内核存活。
3. MCP 输出洪泛 → 有界处理，无 OOM；进程退出后子进程/句柄无泄漏。
4. 依赖：stdio 路径零新依赖；HTTP 路径仅在批准 D4 后启用。

---

## 6. 决策记录

| 编号 | 决策 | 结论 | 状态 |
|---|---|---|---|
| D1 | HTTP 客户端 | 引入 `reqwest` | ✅ 已批准 |
| D2 | JSON 序列化 | 引入 `serde_json` | ✅ 已批准 |
| D3 | 代码位置 | `referee-agent/` 独立 crate，不碰内核 | ✅ 已确认 |
| D4 | MCP 传输范围 | 仅 stdio，HTTP 留扩展点 | 默认采纳，可复议 |
| D5 | 记忆/成果持久化 | 默认内存；`memory-persist` feature 接 WalSink/文件 | 默认采纳，可复议 |
| D6 | Token 估算 | 字符/字节近似 + 厂商 usage 校准，零依赖 | 默认采纳，可复议 |

**已批准依赖（新增白名单条目，仅 `referee-agent` 使用，`referee-core` 不动）**：`reqwest`、`serde_json`。

---

## 7. 风险与开放问题

1. **厂商 API 漂移**：三厂商 API 均在演进。缓解：适配器集中、能力声明驱动降级；每个适配器配契约测试（mock 服务器）。
2. **MCP 生态复杂**：stdio 进程的生命周期管理是 P7 主要风险。缓解：独立进程治理（超时/熔断/清理）+ 专用测试；HTTP 后置。
3. **记忆的长期一致性**：全局记忆跨会话写并发。缓解：写入统一走 emit 消息队列串行化，存储 trait 层保证原子性。
4. **缓存正确性**：缓存键必须包含全部影响输出的参数（含温度、工具声明集合）。缓解：P5 验收 1 的计数断言 + 键结构单测。
5. **Token 估算误差**：近似估算与厂商实际 usage 存在偏差。缓解：usage 优先、估算兜底；计量口径文档化。

---

## 8. 总验收口径

全部阶段完成后，以下必须同时成立：
1. `referee-core` 零改动（`git diff referee-core` 为空）。
2. 未启用 `referee-agent` 的项目不受任何影响（可选性验证：默认 feature 最小集可编译、可跑测试）。
3. 九个横切约束（§2）在测试中逐条可断言。
4. 核心场景演示通过：主 Agent 并行调用 N 个子 Agent + 工具 + MCP 工具，成果按白名单注入，全链路 trace 与 token 计量正确，任意环节故障（厂商挂死、工具 panic、子 Agent 崩溃、背压洪泛）都不影响内核与其余会话。

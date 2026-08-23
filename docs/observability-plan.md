# referee 可观测性优化执行计划

> 状态：P0 / P1 / P2 已全部实施（验收测试全绿）。
> 源头：Fluen 侧六项优化提案，经子仓库侧评估后收敛为本计划。
> 原则：按架构成本与设计哲学排序，先修复内部一致性，再建通用观测面；
> 冲击内核契约的项缓做或不做。

## 1. 背景

Motis 经 `delegate_agent` 委派子智能体（`referee-agent` 的 `AgentRuntime`），子智能体在
引擎内部以非流式 `engine.chat()` 跑完整「LLM ↔ 工具」循环。Fluen 侧可观测委派起止与
工具调用，**观测不到子智能体 LLM 输出增量**；同时评估发现引擎存在两处内部不一致
（错误信息降级、超时配置虚设）。本计划解决这三类问题。

## 2. 范围总览

| 编号 | 项 | 决策 | 一句话理由 |
|---|---|---|---|
| P0 | SessionReply 错误类型化 | **执行** | 修复既定设计方向被中断；性价比最高 |
| P1 | awaiting_calls_timeout 落地 | **执行** | 配置面承诺未兑现；补「无单轮等待类批量总 deadline」漏洞 |
| P2 | EngineObserver 事件钩子 | **执行** | 通用观测出口；顺带吸收 P2-4 结构化结果 |
| P2-4 | Executor 观测 | **并入 P2** | `ExecutedTool.outcome` 结构化 + 事件并入 observer |
| D1 | AgentRuntime 流式转发桥 | **缓做** | observer 落地后紧迫性大降；改 invoke 单回信契约代价过重 |
| D2 | 内核 invoke 心跳 | **不做** | 被 observer 天然覆盖；跨边界需求出现再议 |

## 3. 执行阶段

### P0 — SessionReply 错误类型化（✅ 已实施）

**模块**：`referee-ai/src/session/message.rs`、`referee-ai/src/engine/mod.rs`
**改动点**：

1. 新增 `ErrorKind` 枚举（`referee-ai`，随 `SessionReply` 导出）：

   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
   pub enum ErrorKind { Timeout, RateLimited, Llm, Budget, Internal }
   ```

   `Default = Internal`（供 `#[serde(default)]` 旧 JSON 兜底）。

2. `SessionReply::Error` 增加 `kind` 字段 + `retry_after_ms: Option<u64>`（默认 `None`，
   serde-friendly，透传 `LlmError::RateLimited.retry_after` 的类型化载体）。注意：**仅加
   `kind` 无法结构上携带 retry_after**，须显式加时长字段或另作编码，此处取显式字段。

3. **分类发生在两条独立转换路径**（`Budget` / decode 错误**不在** `From<EngineReply>` 内）：
   - `referee-ai` 内 `From<EngineReply> for SessionReply` 只覆盖回合级 `EngineReply`
     （无法看到同步段返回的 `EngineStartError`，二者是不同类型）：
     - `EngineReply::Timeout` → `Timeout`
     - `EngineError::Llm(LlmError::RateLimited { retry_after })` → `RateLimited`，并写
       `retry_after_ms`
     - `EngineError::Llm(_)` → `Llm`
     - 其余 `EngineError`（StateConflict / TurnIncomplete / ChannelClosed）→ `Internal`
   - **agent 层**（`referee-agent/src/lib.rs` 的 `handle_chat` `Err(e)` 分支）新增
     `From<EngineStartError> for ErrorKind`：
     - `EngineStartError::Budget` → `Budget`
     - `MaxSessions` / `Busy` → `Internal`（agent 层显式设 kind，不再裸 `e.to_string()`）
     - `handle()` 的 decode 错误分支 → `Internal`（同址赋值，不经 `From`）

4. 同步修改所有关联点：
   - **构造点**（需补 `kind`/`retry_after_ms`）：`referee-agent/src/lib.rs`（**2 处**：
     `handle_chat` 失败分支、decode 失败分支）、`referee-channel/tests/router_test.rs`。
   - **消费点**（模式匹配因新增字段被破坏，需补 `..`）：`referee-channel/src/policy.rs`
     的 `SessionReply::Error { message }` 解构；该处**不是构造点**。

**设计要点**：
- serde 兼容：`kind` 用 `#[serde(default)]`，`ErrorKind` 默认 `Internal`，
  保证旧 JSON 可解码（延续 `SessionMessage` 既有兜底约定）。
- `message: String` 保留（人读），`kind` 供程序化分支——上游 policy 据此
  区分「超时提示重试 / 限流退避 / 直接报错」。

**验收**：新旧 JSON 编解码单测（旧 payload 缺 `kind` 解码为 `Internal`、
`retry_after_ms` 缺省 `None`）；`From<EngineReply>` 覆盖 Timeout/Llm，agent 层
`From<EngineStartError>` 覆盖 Budget；`cargo check` 全仓；既有测试全绿。

### P1 — awaiting_calls_timeout 落地（决策：接入，不移除；✅ 已实施）

**模块**：`referee-ai/src/engine/mod.rs`（`run_tool_calls`）
**决策依据**：字段注释承诺「超时 pending 进 DLQ、会话恢复 Idle」从未兑现，
配置面在撒谎；且 `tool_timeout` 只对单个工具生效（每工具均被 `tokio::time::timeout`
兜底，最坏单轮 ≈ `max_per_turn × tool_timeout`）——**当前没有「单轮等待类工具批次总
deadline」**，且 semaphore 许可等待（`acquire_owned`）无超时；Fluen 长工具场景下回合
可被一轮慢的等待类批次长时间占用。接入恰好在「工具轮」处加上总上限，
兑现「任何负载下安全降级」硬约束。

**改动点**：

1. `run_tool_calls` 等待类分支（`execute_batch`）外层加
   `timeout(awaiting_calls_timeout)` 总 deadline。
2. 超时语义：**不能**简单丢弃已完成结果（朴素 `join_all` 外包 timeout 的问题）；
   需 `select` 收敛——deadline 到达时，未完成项生成超时收敛消息回写
   `finish_tool_call`，已完成项正常收敛，会话恢复一致状态（Resume/Settled）。
3. 修正 `timeout.rs` 字段注释：删除「P2/P3 使用」措辞，写明实际生效位置
   （单轮工具阶段总 deadline，区别于单工具 `tool_timeout`）。
   **注意这是语义重定义而非单纯落地**：原注释描述的「AwaitingCalls 跨消息回环 → DLQ」
   在当前「同任务内顺序收敛」架构下并不存在，接入把该字段重定义为「等待类工具批次总
   deadline」，需在注释中明示，避免后人按旧语义误用。

**设计要点**：
- 默认值 60s 保留；Fluen 类长工具场景由使用方自行调大（与 tool_timeout 同理）。
- 派发类（`dispatch_batch`）不适用本 deadline（后台任务不阻塞回合）。

**验收**：新增测试——慢工具 + 短 `awaiting_calls_timeout`：部分完成 + 部分超时
收敛，会话不悬空（次轮 chat 可正常进入）；`cargo check` + 既有测试全绿。

### P2 — EngineObserver 事件钩子（含 Executor 结构化结果；✅ 已实施）

**模块**：`referee-ai/src/engine/`（新增 observer 定义）、`referee-ai/src/tool/executor.rs`
**改动点**：

1. 新增 trait（`referee-ai` 导出）：

   ```rust
   pub trait EngineObserver: Send + Sync {
       fn on_turn_started(&self, session_id: SessionId, turn_id: u64) {}
       fn on_thinking_delta(&self, session_id: SessionId, delta: &str) {}
       fn on_text_delta(&self, session_id: SessionId, delta: &str) {}
       fn on_tool_started(&self, session_id: SessionId, tool_call_id: &str, name: &str) {}
       fn on_tool_finished(&self, session_id: SessionId, tool_call_id: &str,
                           outcome: ToolOutcome, duration_ms: u64) {}
       fn on_turn_finished(&self, session_id: SessionId, turn_id: u64,
                           usage: Option<TokenUsage>) {}
   }
   ```

   注入方式：`Engine::with_observer(Arc<dyn EngineObserver>)`（builder，
   与 `with_tools` 对称），非 `EngineConfig` 字段（行为不进配置数据）。

2. **非流式路径内部改流式收敛**（关键实现成本，提案未提及）：
   非流式 `provider.chat()` 一次性返回、无增量可回调；须改为内部消费
   `chat_stream` 累积收敛（复用 `StreamAccumulator`），对外仍返回完整
   `ChatResponse`——「内部流式、外部非流式」，两条路径在此统一。
   **必须条件化启用**：仅当注入了 observer（且 `capabilities().streaming == true`）才
   走内部流式收敛；否则保持原 `provider.chat()`。避免给最常见、无需可观测性的场景
   静默引入「非流式改走 SSE 流式端点」的行为回归与额外开销，并消除
   `streaming=false` 厂商下 `chat_stream` 语义未定义的隐患（当前内置厂商均 `streaming:true`）。

3. `ExecutedTool` 增加 `outcome: ToolOutcome` 字段（serde 友好）：修复 executor
   超时/panic/not-found 折叠被字符串化、装饰器观测不到的
   问题。这是**数据出口**而非行为出口，任何调用方可程序化消费；
   `on_tool_finished` 复用同一枚举。
   **枚举必须穷尽现有折叠分支**——`execute_single` 目前收敛≥5 种：
   参数解析失败 / not-found / semaphore 许可失败 / timeout / 工具 `Err` / panic。
   `ToolOutcome` 定义建议：`Ok` / `Timeout` / `Panic` / `NotFound` / `PermitUnavailable`，
   其中**工具正常返回 `Err(ToolError::X)` 归 `Ok` 并保留错误文本**（属正常可观测完成、
   非崩溃），避免与 `Timeout/Panic` 混淆。落地时按实际分支逐一对齐，不得留 `_ =>`。

**设计要点**：
- observer 回调**必须在热循环内非阻塞**（与扩展 `handle` 同理）。仅「文档化 + 
  `catch_unwind`」不足以防同步阻塞（阻塞不是 panic）——回调若做重活会直接卡住
  stream_loop。故强制**实现侧只做 `mpsc::Sender::try_send`** 交给外部消费者，
  慢消费者由实现方自负；引擎侧逐回调 `catch_unwind` 兜底异常。
- `on_tool_finished` 需覆盖**两条路径**：等待类（`execute_batch` 同步收敛）与
  **派发类（`dispatch_batch` 后台任务完成）**——后者在 `run_tool_calls` 的派生
  task 完成后用捕获的 `engine` 触发，两者复用同一 `ToolOutcome`。
- observer 是行为句柄，只存在于 Engine，**绝不进 Envelope / SessionReply**
  （数据与行为严格分离）。
- delta 回调在流式 `stream_loop` 与内部流式收敛路径统一触发，
  双路径共享一份推送代码。

**验收**：mock observer 收集事件断言（turn 起止成对、delta 顺序、usage 携带）；
工具超时路径 `on_tool_finished(Timeout)` 可观测；**未注入 observer 时非流式路径仍走
`provider.chat()`**（无内部流式回归）；派发类工具完成后亦触发 `on_tool_finished`
（复用 `ToolOutcome`）；既有测试全绿。

### 使用方接入示意（Fluen 侧，非本仓职责）

`build_agent_runtime` 构建 Engine 时注入 observer，由 `AgentReporter` 转为
`motis:agent-thought` / `motis:agent-text` 事件；`SessionReply::Error.kind`
用于委派失败分类上报。`ObservedTool` 装饰器在 P2 交付后可退役。

## 4. 缓做与不做决策记录

### D1 — AgentRuntime 流式转发桥（缓做）

- **提案形态**：`SessionReply::Streaming { stream_id }` + 内核分片消息
  （`StreamChunk`/`StreamEnd`）。
- **缓做理由**：内核 invoke 是消费式 oneshot 单回信（类型安全回信的结构保证），
  分片流本质是把 RPC 改为「RPC + 订阅」混合模型，需 correlation 路由、分片通道
  生命周期治理、分片背压策略——一整套新内核语义，与「内核是最小引擎」冲突；
  且 Fluen 同进程场景下 observer 已覆盖实时增量，仅跨内核/跨进程不可替代。
- **重启条件**：出现真实跨内核流式回信需求时，优先评估
  「`emit` 推观测分片 + 调用方订阅」形态，而非改 invoke 契约。

### D2 — 内核 invoke 心跳（不做）

- observer 的 turn/tool 事件天然是「扩展存活」信号（同进程直接消费）；
  内核层心跳仅跨内核边界有意义，现为超前设计。跨边界需求出现时随 D1 一并评估。

## 5. 全局约束

- **零新增依赖**：全部改动使用规范清单内库。
- **背压**：不引入无界分配；observer 实现侧自行保证有界。
- **隔离**：不改 panic 熔断边界；observer 回调异常不得影响回合循环
  （回调包 `catch_unwind` 或文档化约束）。
- **数据/行为分离**：observer 只存于 Engine，不进任何 Envelope 载荷。
- **每阶段交付**：`cargo check` 全仓 + 对应 crate 测试 + 新增验收测试全绿
  后再进入下一阶段（core：`tests/backpressure_test.rs`、`tests/isolation_test.rs`；
  ai/agent/channel：对应模块测试）。
- 提交信息遵循 `git-commit-message.md` 规范（中文、简洁、全面描述变更）。

## 6. 依赖与顺序

P0 → P1 → P2 顺序执行，无并行依赖；P2 依赖 P0 的 `ErrorKind`
（`ToolOutcome` 与 `ErrorKind` 分类语义保持一致但不合并——前者工具执行结果，
后者回合级错误）。P2 完成后 Fluen 侧装饰器可退役（其自行安排）。

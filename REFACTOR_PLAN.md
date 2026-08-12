# Referee-AI-Base 重构执行规划

> 目标：将现有 `referee-agent`（业务级 Agent 运行时）重构拆分为两层——
> **地基层 `referee-ai-base`**（基础 AI 设施，积木式、可拓展、易维护）与
> **业务层 `referee-agent`**（完整 Agent 业务封装，开箱即用）。
> 业务扩展模块（记忆 / MCP & Skills）明确移除；已实现的非地基能力迁移至业务层。

## 1. 定位与边界（用户核心判断标准）

> 「如果一个模块去掉，框架仍能跑通『接 LLM → 组装 prompt → 调工具 → 管预算』
> 这个最小闭环，那它就是业务扩展而非基础设施。」

按此标准逐项判定：

| 模块 | 原规划阶段 | 性质结论 | 去留 |
|---|---|---|---|
| 厂商抽象层 | P0 | 唯一 I/O 边界，地基 | ✅ 保留（base） |
| 会话状态机 | P1 | 并发/中断/背压骨架 | ✅ 保留（base） |
| 工具调用 | P2 | Tool trait + 执行机制 | ✅ 保留（base，仅抽象/执行器） |
| 子 Agent + Artifact | P3 | Artifact 泛化为通用 KV 存储；子 Agent 概念去掉 | ♻️ Artifact→通用 Store；AgentTool/对等→agent |
| 预算治理 | P4 前置 | Token 双层级限额，基础设施级 | ✅ 保留（base） |
| Prompt 组装 + 缓存 | P5 | 预算分配 + prompt 必要能力 | ✅ 保留（base） |
| 记忆模块 | P4 | 业务决策（使用者自建/二次封装） | ❌ 移除 |
| 计量与可观测 | P6 | 基础设施级 | ✅ 保留但精简（base） |
| MCP 与 Skills | P7 | 协议桥接集成层，基于 Tool 自接 | ❌ 移除 |

### 最终工程结构
```
referee-core      内核（不动，零改动）
referee-ai-base   地基：provider / session / tool抽象 / store(通用KV) /
                           budget / prompt / cache / observe / engine(最小闭环)
referee-agent     业务封装：AgentRuntime(Extension) + 对等协作(AgentTool) + 编排
```

## 2. 模块划分细则

### 2.1 referee-ai-base（地基，新增 crate）
- **provider/**：原样迁移 P0。`LLMProvider` trait、纯数据（Message/ChatRequest/
  ChatResponse/StreamChunk/ToolCall/TokenUsage/ThinkingConfig/FinishReason）、
  `ProviderCapabilities`、`LlmError`、`RetryPolicy`、`OpenAiCompat` 底座 + 厂商适配器
  （xiaomi/deepseek，feature 隔离）。**补强**：清晰的可观测 span。
- **session/**：状态机（Idle/Thinking/AwaitingCalls）+ TimeoutConfig + 消息编解码。
  **去掉** `SubagentDone` 消息变体与 `PendingKind::Subagent`（业务概念）。
  保留 `PendingKind::Tool`。`run_turn` 终态自管 wrapper 保留（核心并发机制）。
- **tool/**：`Tool` trait + `ToolCategory` + `ToolContext` + `ToolRegistry` + `ToolExecutor`。
  **去掉** `agent_tool.rs`（对等业务）。保留抽象与并行/截断/panic隔离/超时执行机制。
- **store/**：将 `artifact` 泛化为**通用 KV Store**：`Store` trait（store/get/删除/
  容量）+ 有界内存实现。**去除** owner/allowed_readers 这类对等 Agent 特定语义，
  保留有界容量（数量+字节双上限）。作为通用成果/状态存储地基。
- **budget/**：原样迁移。
- **prompt/**：原样迁移（系统/工具/历史/记忆/工件多段优先级截断）。
- **cache/**：原样迁移（LRU+TTL，语义缓存，合成流）。
- **observe/**：**新增补强**。tracing span 门面 + metrics 计数器（调用数、token、
  工具成功率、缓存命中率、队列深度），作为可观测地基。
- **engine/**：**新增补强 / 核心**。将现有 `turn.rs` 编排提炼为不依赖
  `Extension` 的**会话引擎** `Engine`：接收一条用户消息，驱动
  「LLM → 工具→ 收敛 → 回复」最小闭环，处理并发/中断/超时/预算/缓存。
  不直接耦合 referee-core 的 `Extension`，仅通过 kernel 完成转发与回复。
  对外暴露清晰、同步可测的驱动接口（便于 referee-agent 与第三方集成）。

### 2.2 referee-agent（业务封装，改造现有 crate）
- 保留 `AgentRuntime`（`referee-core::Extension` 实现）、消息路由
  （chat/interrupt/tool_result/resume）、`register_peer_tool`、turn 编排。
- 依赖 `referee-ai-base` 作为地基，组装 base 的积木。
- 对等协作 `AgentTool` 迁移于此（业务：Agent as Tool）。
- 依赖：`referee-core` + `referee-ai-base` + 业务所需的 crate。

### 2.3 移除项
- 记忆模块（P4）：无实现，不迁移。
- MCP/Skills（P7）：无实现，不迁移。
- 相应 feature 标志（mcp-stdio / memory-persist）移除或降为业务层的预留。

## 3. 地基补强点（用户强调：补齐、完善）
1. **可观测**：全链路 tracing（engine→llm→tool），metrics 指标，可断言。
2. **调试友好**：结构化日志、关键决策点（缓存命中/截断/重试/超时）有日志。
3. **流式输入输出的消费**：`chat_stream` 语义与 `chat` 等价（契约测试）；
   流式增量正确累积、Finish 收敛。
4. **并发**：Session 短暂持锁、无跨 await guard、队列有界、洪泛后内存/队列深度
   不增长断言。
5. **错误处理——绝不吞异常**：emit 失败、budget 超限、工具失败、解码失败、
   并发 busy 拒绝，全部显式可见（日志/DLQ/明确回信），杜绝 `let _ =` 静默丢弃。
6. **超时防护**：Thinking / AwaitingCalls 双 deadline，测试不依赖真实挂死等待。

## 4. 代码质量约束
- 单文件 ≤ 600 行（特殊情况例外）。
- 清晰、简洁、零冗余；分层单向依赖（provider 无上层依赖；模块间只经 trait）。
- 不新增白名单外依赖（reqwest/serde_json 已批准；不引入 tiktoken 等）。
- `referee-core` 零改动（git diff referee-core 为空）。

## 5. 测试矩阵
- base：`engine_test`（含超时防护的会话闭环）、`tool_test`、`session_test`、
  `budget_test`、`cache_test`、`prompt_test`、`provider_equivalence_test`、
  `store_test`、`observe_test`。
- agent：`runtime_test`（Extension 集成）、`peer_test`（对等协作）。
- 所有异步测试设超时（`#timeout` 或 tokio 超时包装），不锁死。

## 6. 执行步骤
1. 建立 `referee-ai-base` crate 骨架 + workspace 更新。
2. 迁移地基模块（provider/session/tool/store/budget/prompt/cache/observe/engine）。
3. 改造 `referee-agent` 为业务层，依赖 base，迁移 AgentRuntime/AgentTool。
4. 全量测试 + 超时防护 + `cargo fmt` + `cargo clippy`。
5. 代码 review（分层、安全、错误可见性、背压、≤600行）。
6. 更新文档（README/AGENT_PLAN/模块注释），推送 GitHub。

## 7. 验收口径
1. `git diff referee-core` 为空。
2. base 可独立编译、可编译测试、最小闭环跑通（mock LLM + mock 工具 + 预算 + 缓存）。
3. 9 条横切约束在测试中逐条可断言。
4. 任意故障（厂商挂死 / 工具 panic / 背压洪泛 / 缓存竞争）不影响内核与其他会话。

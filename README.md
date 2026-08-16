# Referee — 轻量微内核 + 智能体三层架构

> 工业级防护能力的轻量引擎仓库。内核只做**通信与治理**；AI 能力分为**地基层**与
> **业务封装层**：`referee-ai` 提供业务无关的核心支撑积木，`referee-agent`
> 提供开箱即用的完整 Agent 封装。

```
┌─────────────────────────────────────────────────────────────┐
│ referee-agent（业务封装，开箱即用）                            │
│   AgentRuntime (Extension) · Agent 定义/配置（agent）         │
│   对等协作 AgentTool · ACL 工件存储 · 成果板读取工具           │
│   MCP stdio（mcp-stdio）· Skills（skills）                   │
├─────────────────────────────────────────────────────────────┤
│ referee-ai（核心支撑，地基积木）                          │
│   provider · session · tool · store · budget · prompt ·     │
│   cache · observe · engine（最小闭环 + 流式 + 会话生命周期）    │
│   （prompt 分段编排 · 消息用量元数据 · 缓存命中计量）            │
├─────────────────────────────────────────────────────────────┤
│ referee-core（微内核：路由 / 治理）                            │
│   背压 · 熔断 · 监督 · 停机 · DLQ · WAL                        │
└─────────────────────────────────────────────────────────────┘
```

## 模块导航

| 模块 | 定位 | 测试 |
|------|------|------|
| [`referee-core`](referee-core/) | **微内核**：路由 / 原语 / 治理（背压、熔断、挂起治理、监督、停机、DLQ、WAL） | 34 条 |
| [`referee-ai`](referee-ai/) | **核心支撑（地基）**：厂商抽象、会话引擎（最小闭环 + 流式 + 会话生命周期）、工具执行（同步/异步派发 + 白名单过滤）、通用 KV、预算、**提示词分段编排**、缓存、可观测、**用量/缓存命中计量** | 121 条 |
| [`referee-agent`](referee-agent/) | **业务封装（开箱即用）**：Extension 集成、Agent 定义/配置（`agent`）、对等协作、ACL 工件存储、成果板读取工具；MCP stdio（`mcp-stdio`）与 Skills（`skills`）按需 feature | 31 条（25 库 + 6 对等集成，默认） |

合计 **186 条测试全绿**（`cargo test --workspace`，默认 feature；启用 `skills` / `mcp-stdio` 后更多）。

## 组合使用

```toml
[dependencies]
referee-core    = { path = "referee-core" }                # 通信与治理
referee-ai = { path = "referee-ai" }             # 核心支撑积木
referee-agent   = { path = "referee-agent" }               # 开箱即用 Agent（可选，按需引入）
```

1. 用 `referee-core` 的 `Kernel` 注册扩展，获得有界通道、Panic 熔断、优雅停机等治理能力。
2. 用 `referee-ai` 的 `Engine` 直接驱动「接 LLM → 组装 prompt → 调工具 → 管预算 →
   回复」的最小闭环；也可自由组合其积木搭建定制能力。`prompt::assemble` 提供**分段编排**
   （稳定段在前、空能力段省略），`Message::usage` 携带单条消息的用量与缓存命中数据。
3. 需要完整/协作者 Agent 时，用 `referee-agent` 的 `AgentRuntime`（实现 `Extension` trait）
   注册为内核扩展，即获得多会话 + 对等协作能力；用 `agent` 模块以声明式（`AgentDefinition`）
   或 builder 方式定义 Agent，并通过**能力白名单**精细控制每个 Agent 可用的工具 / 技能 / MCP。
   需要边生成边消费时用 base 引擎的 `chat_stream` 流式接口，会话生命周期（快照 / 枚举 /
   删除 / 空闲回收）由引擎直接提供。需要接入远程工具或注入技能时，启用 `referee-agent`
   的 `mcp-stdio` / `skills` feature（默认关闭，核心保持轻量）。

## 文档地图

| 文档 | 说明 |
|------|------|
| [`referee-core/README.md`](referee-core/README.md) | 内核模块：设计原则、核心能力、错误模型、安全契约 |
| [`referee-ai/README.md`](referee-ai/README.md) | 地基层：核心支撑积木（provider/session/tool/store/budget/prompt/cache/observe/engine） |
| [`referee-agent/README.md`](referee-agent/README.md) | 业务封装层：Extension 集成、Agent 定义/配置、对等协作、开箱即用 Agent |
| [`AGENT_RUNTIME_PLAN.md`](AGENT_RUNTIME_PLAN.md) | Agent 运行时落地计划（历史规划，P0 ~ P7 阶段验收标准） |
| [`REFACTOR_PLAN.md`](REFACTOR_PLAN.md) | 重构执行规划（两层拆分边界与验收口径） |
| [`PHASE_STATUS.md`](PHASE_STATUS.md) | Phase 状态跟踪（core / ai-base / agent 已完成阶段、关键决策与偏差记录） |
| [`AGENTS.md`](AGENTS.md) | 工程约束（设计思想 / 依赖清单 / 工作纪律） |

## 验证

```bash
cargo test --workspace                      # 全量回归（core 34 + base 121 + agent 31 = 186 条）
cargo test --workspace --features "skills mcp-stdio"   # 含业务扩展（MCP 协议 / Skills 注入）
cargo clippy --workspace --all-targets -- -D warnings  # 零警告
cargo fmt --check                           # 格式整洁
```

## 能力速览

### referee-core（微内核）— 全部完成

Phase 1 骨架与背压 → Phase 2 invoke 原语 → Phase 3 容错与隔离 → Phase 4 治理与生命周期 → Phase 5 可观测 → Phase 6 并发安全与 WAL → 监督治理加固（挂起治理 / 积压转储 / 停机消息守恒）。

### referee-ai（核心支撑地基）— 全部完成

- **厂商抽象**：`LLMProvider` trait、纯数据模型、错误归一与重试、能力声明、OpenAI 兼容底座 + 适配器（DeepSeek / MiMo）。`TokenUsage` 记录输入/输出/推理/缓存命中（命中 → 未命中，含归一化 `cache_read/write_tokens`）。
- **会话引擎**：`Engine` 最小闭环，流式输出（`chat_stream`）与会话生命周期（快照 / 枚举 / 删除 / 空闲回收）。
- **工具执行**：`Tool` trait + 有界注册表 + 并行/截断/panic 隔离/超时执行器 + 同步/异步派发；`declarations_visible` 支持按白名单过滤工具声明。
- **提示词分段编排**：`SystemSection`（稳定/易变 + 空则省略）+ `assemble`（条件省略 → 稳定排前 → 预算截断），`build_prompt` 兼容保留。
- **消息用量元数据**：`Message::usage` 携带单条消息的用量与缓存命中，供 observe 与审计。
- 另有 `PromptParts` 参数封装、`LlmError::is_retryable` 重试门控与 `llm_retry` 指标。

### referee-agent（业务封装）— 完成核心；扩展能力可在此基础上建设

- **Agent 定义/配置**：`agent` 模块提供 `AgentDefinition`（纯数据，可来自 JSON/TOML/builder）、`AgentBuilder`、`AgentRegistry`（以可调用 `AgentId` 为 key，唯一 + kebab-case 校验）、`bind` → `BoundAgent`（解析白名单 + 渲染模板）。
- **能力白名单（封闭默认）**：每个 Agent 声明可用工具 / 技能 / MCP；`["*"]`=全部、`["a"]`=白名单、`[]`=无该能力（空则不进提示词）。
- **对等协作**：`AgentTool`（Agent as Tool）、ACL 工件存储与成果板读取工具（`list_my_board` / `read_artifact`）。
- **按需拓展**：MCP 2.0 stdio 客户端桥（feature `mcp-stdio`）、Agent Skills（SKILL.md）注入（feature `skills`），默认不加载、零新增依赖；记忆等业务策略不预置（由使用方二次封装）。
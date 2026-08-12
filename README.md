# Referee — 轻量微内核 + 智能体三层架构

> 工业级防护能力的轻量引擎仓库。内核只做**通信与治理**；AI 能力分为**地基层**与
> **业务封装层**：`referee-ai-base` 提供业务无关的核心支撑积木，`referee-agent`
> 提供开箱即用的完整 Agent 封装。

```
┌─────────────────────────────────────────────────────────────┐
│ referee-agent（业务封装，开箱即用）                            │
│   AgentRuntime (Extension) · 对等协作 AgentTool · ACL 工件存储  │
├─────────────────────────────────────────────────────────────┤
│ referee-ai-base（核心支撑，地基积木）                          │
│   provider · session · tool · store · budget · prompt ·      │
│   cache · observe · engine（最小闭环）                         │
├─────────────────────────────────────────────────────────────┤
│ referee-core（微内核：路由 / 治理）                            │
│   背压 · 熔断 · 监督 · 停机 · DLQ · WAL                        │
└─────────────────────────────────────────────────────────────┘
```

## 模块导航

| 模块 | 定位 | 测试 |
|------|------|------|
| [`referee-core`](referee-core/) | **微内核**：路由 / 原语 / 治理（背压、熔断、监督、停机、DLQ、WAL） | 25 条 |
| [`referee-ai-base`](referee-ai-base/) | **核心支撑（地基）**：厂商抽象、会话引擎（最小闭环）、工具执行、通用 KV、预算、提示词、缓存、可观测 | 89 条 |
| [`referee-agent`](referee-agent/) | **业务封装（开箱即用）**：Extension 集成、对等协作、ACL 工件存储 | 12 条 |

合计 **126 条测试全绿**（`cargo test --workspace`）。

## 组合使用

```toml
[dependencies]
referee-core    = { path = "referee-core" }                # 通信与治理
referee-ai-base = { path = "referee-ai-base" }             # 核心支撑积木
referee-agent   = { path = "referee-agent" }               # 开箱即用 Agent（可选，按需引入）
```

1. 用 `referee-core` 的 `Kernel` 注册扩展，获得有界通道、Panic 熔断、优雅停机等治理能力。
2. 用 `referee-ai-base` 的 `Engine` 直接驱动「接 LLM → 组装 prompt → 调工具 → 管预算 →
   回复」的最小闭环；也可以自由组合其积木搭建定制能力。
3. 需要完整/协作者 Agent 时，用 `referee-agent` 的 `AgentRuntime`（实现 `Extension` trait）
   注册为内核扩展，即获得多会话 + 对等协作能力。

## 文档地图

| 文档 | 说明 |
|------|------|
| [`referee-core/README.md`](referee-core/README.md) | 内核模块：设计原则、核心能力、错误模型、安全契约 |
| [`referee-ai-base/README.md`](referee-ai-base/README.md) | 地基层：核心支撑积木（provider/session/tool/store/budget/prompt/cache/observe/engine） |
| [`referee-agent/README.md`](referee-agent/README.md) | 业务封装层：Extension 集成、对等协作、开箱即用 Agent |
| [`AGENT_RUNTIME_PLAN.md`](AGENT_RUNTIME_PLAN.md) | Agent 运行时落地计划（历史规划，P0 ~ P7 阶段验收标准） |
| [`REFACTOR_PLAN.md`](REFACTOR_PLAN.md) | 重构执行规划（两层拆分边界与验收口径） |
| [`PHASE_STATUS.md`](PHASE_STATUS.md) | Phase 状态跟踪（referee-core 已完成阶段） |
| [`AGENTS.md`](AGENTS.md) | 工程约束（设计思想 / 依赖清单 / 工作纪律） |

## 验证

```bash
cargo test --workspace                      # 全量回归（core 25 + base 89 + agent 12 = 126 条）
cargo clippy --workspace --all-targets -- -D warnings    # 零警告
cargo fmt --check                           # 格式整洁
```

## 路线图

### referee-core（微内核）— 全部完成

Phase 1 骨架与背压 → Phase 2 invoke 原语 → Phase 3 容错与隔离 → Phase 4 治理与生命周期 → Phase 5 可观测 → Phase 6 并发安全与 WAL。

### referee-ai-base（核心支撑地基）— 全部完成

厂商抽象、会话引擎（最小闭环）、工具执行、通用 KV 存储、预算治理、提示词组装与缓存、可观测。

### referee-agent（业务封装）— 完成核心；扩展能力可在此基础上建设

Extension 集成、对等协作（Agent as Tool）、ACL 工件存储。记忆 / MCP / Skills 等业务策略
由使用者或二次封装搭建（不预置在地基层）。

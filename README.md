# Referee — 轻量微内核 + 智能体运行时

> 工业级防护能力的轻量引擎仓库。内核只做**通信与治理**；智能体能力作为**可选 SDK 模块**按需组合。

---

## 模块导航

| 模块 | 定位 | 进度 | 测试 | 文档 |
|------|------|------|------|------|
| [`referee-core`](referee-core/) | **微内核**：路由 / 原语 / 治理（背压、熔断、监督、停机、DLQ、WAL） | Phase 1 ~ 6 ✅ 完成 | 25 条 | [README](referee-core/README.md) |
| [`referee-agent`](referee-agent/) | **智能体运行时**（基于内核的可选 SDK）：厂商抽象 + 会话状态机 + 工具调用 + 对等协作 + 预算治理 + 提示词组装与缓存 | Phase 0 ~ 3 + 预算治理 + P5 ✅ 完成 | 146 条 | [README](referee-agent/README.md) |

合计 **171 条测试全绿**（`cargo test --workspace`）。

---

## 组合使用

```toml
[dependencies]
referee-core  = { path = "referee-core" }              # 通信与治理
referee-agent = { path = "referee-agent" }             # 智能体能力（可选，按需引入）
```

1. 用 `referee-core` 的 `Kernel` 注册扩展，获得有界通道、Panic 熔断、优雅停机等治理能力。
2. 用 `referee-agent` 的 `AgentRuntime`（实现 `Extension` trait）注册为内核扩展，即获得多会话智能体能力（LLM 调用、中断、超时治理）。
3. 两模块可独立使用：只用内核的扩展通信；或只用 agent 层的会话能力（配合内核的 invoke / emit）。

示例见各模块 README 的「快速上手」。

---

## 文档地图

| 文档 | 说明 |
|------|------|
| [`referee-core/README.md`](referee-core/README.md) | 内核模块：设计原则、核心能力、错误模型、架构、安全契约 |
| [`referee-agent/README.md`](referee-agent/README.md) | 智能体模块：厂商抽象层、会话状态机、消息协议、设计约束、路线图 |
| [`AGENT_RUNTIME_PLAN.md`](AGENT_RUNTIME_PLAN.md) | Agent 运行时落地计划（P0 ~ P7 阶段验收标准，规划性质） |
| [`PHASE_STATUS.md`](PHASE_STATUS.md) | Phase 状态跟踪（referee-core 已完成阶段、关键设计决策与偏差记录） |
| [`AGENTS.md`](AGENTS.md) | 工程约束（设计思想 / 依赖清单 / 工作纪律） |

---

## 验证

```bash
cargo test --workspace                      # 全量回归（core 25 + agent 146 = 171 条）
cargo clippy --workspace --all-targets -- -D warnings    # 零警告
cargo fmt --check                           # 格式整洁
```

---

## 路线图

### referee-core（微内核）— 全部完成

Phase 1 骨架与背压 → Phase 2 invoke 原语 → Phase 3 容错与隔离 → Phase 4 治理与生命周期 → Phase 5 可观测 → Phase 6 并发安全与 WAL。

### referee-agent（智能体运行时）— 进行中

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

> 各阶段验收标准见 [`AGENT_RUNTIME_PLAN.md`](AGENT_RUNTIME_PLAN.md)。

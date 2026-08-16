# AGENTS.md — Referee 微内核 v0.1

## 项目定位

构建具备工业级防护能力的**轻量引擎**，核心验证目标：
1. 路由机制（能力寻址 + 有界通道）
2. 异步原语（emit 即发即弃 / invoke 请求响应）
3. 资源隔离（Panic 熔断、状态机治理）
4. 背压控制（任何负载下安全降级，拒绝 OOM）

工程定位：**轻量化、模块化、易维护、易拓展、易集成**。内核是最小引擎，不承载业务逻辑；代码简洁、规范、零冗余。

## 设计思想（必须恪守）

- **轻量为本**：内核只做通信与治理，不承载业务逻辑、不预置扩展；模块按需组合，易维护、易拓展、易集成。
- **数据与行为严格分离**：Envelope 是纯数据载体，绝不含逻辑句柄；行为只存在于 Extension 与 Kernel。
- **背压是硬约束**：所有通道必须有界；缓冲满即返回 `ResourceExhausted`，绝不允许无限制内存分配。
- **隔离即防御**：扩展崩溃（Panic）只熔断自身，绝不影响内核与其余扩展；`catch_unwind` 是安全边界。
- **类型安全的回信**：回复依赖消费式 `oneshot`，`reply` 消费 `self`，从结构上杜绝重复回复。
- **内核永远存活**：失败路径返回错误码，不 panic；治理状态（Running/Crashed/Stopped）决定路由行为。
- **阻塞即违规**：扩展 `handle` 必须非阻塞，重计算须移交 `ctx.spawn_blocking`；`handle` 内不得等待其他扩展响应（`invoke` 未注入，编译期即被禁止）。

## 开发路线图

- **Phase 1 ~ 6（referee-core）**：✅ 全部完成——骨架与背压 → invoke 原语 → 容错与隔离 →
  治理闭环 → 可观测层 → 并发安全与 WAL。
- **referee-ai（地基积木）**：✅ 已完成——厂商抽象、会话状态机、工具执行（同步/异步
  派发）、通用 KV、预算、提示词、缓存、可观测、会话引擎（最小闭环 + 流式 + 会话生命周期）。
- **referee-agent（业务封装）**：✅ 已完成核心——Extension 集成、对等协作（Agent as Tool）、
  ACL 工件存储、成果板读取工具；MCP 2.0 stdio 客户端桥 / Agent Skills（SKILL.md）注入以
  按需 feature `mcp-stdio` / `skills` 提供（默认不加载、零新增依赖）；记忆等业务策略不预置
  （由使用方二次封装）。

## 工作约束

- 遵循三层目录结构（`referee-core` 内核 / `referee-ai` 地基 / `referee-agent` 业务封装），职责不越界。
- 改动后运行对应测试（core：`tests/backpressure_test.rs`、`tests/isolation_test.rs`；base/agent：对应模块测试）与 `cargo check`。
- 依赖仅使用规范清单内的库，不擅自引入新依赖：
  - `referee-core`：tokio、dashmap、parking_lot、serde、bytes、thiserror、async-trait、uuid、futures、tracing、metrics、tracing-subscriber[dev]
  - `referee-ai` / `referee-agent`：上列 + `serde_json`、`reqwest`（referee-ai 专用）
- 提交前自查：是否引入无界分配？是否破坏 Panic 隔离？是否违反数据/行为分离？

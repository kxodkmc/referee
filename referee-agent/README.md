# Referee Agent — 开箱即用的完整 Agent 业务封装

> 建立在 `referee-ai-base`（地基）之上的**业务层**：把 base 的积木（厂商抽象、
> 会话引擎、工具执行、预算、缓存）组装为可直接使用的 Agent 运行时，并提供业务能力：
> Extension 集成、对等协作（Agent as Tool）、带 ACL 的工件存储。
>
> **分层**：`referee-core`（内核，通信与治理）→ `referee-ai-base`（核心支撑积木）
> → `referee-agent`（本模块，业务封装，开箱即用）。

## 1. 定位

| 项 | 说明 |
|----|------|
| 业务层 | 基于 `referee-ai-base` 组装；base 提供最小闭环积木，本模块提供「如何把它们变成完整、可用、协作的 Agent」 |
| Extension 集成 | `AgentRuntime` 实现 `referee-core::Extension`，把 base 引擎接入内核消息路由（`Chat` / `Interrupt`） |
| 业务能力 | 对等/子 Agent 协作（`AgentTool`，Agent as Tool）、ACL 工件存储（`artifact`） |
| 不预置 | 记忆、MCP、Skills 等业务策略由使用者/二次封装搭建，本模块与 base 均不绑定 |

### 启用方式

```toml
[dependencies]
referee-agent   = { path = "referee-agent", features = ["xiaomi", "deepseek"] }
referee-ai-base = { path = "referee-ai-base" }
referee-core    = { path = "referee-core" }
```

features（默认 `["xiaomi", "deepseek"]`）通过 `referee-agent` 转发到 `referee-ai-base`
裁剪厂商适配器。

## 2. 架构

```
┌──────────────────────────────────────────────────────────────┐
│  referee-core：Kernel（路由 / 治理）                           │
└───────────────────────────────┬──────────────────────────────┘
                                │ Envelope (Chat / Interrupt)
┌───────────────────────────────▼──────────────────────────────┐
│  referee-agent：AgentRuntime (implements Extension)            │
│    · handle_chat / handle_interrupt → 转译 base 引擎调用        │
│    · register_peer_tool / with_artifact_store（业务能力）       │
├──────────────────────────────────────────────────────────────┤
│  referee-ai-base：Engine（会话引擎，最小闭环）                   │
│    provider │ session │ tool │ store │ budget │ prompt │ cache│
└──────────────────────────────────────────────────────────────┘
```

## 3. 模块

| 模块 | 职责 |
|------|------|
| [`AgentRuntime`](src/lib.rs) | `Extension` 实现：接收 `Chat` / `Interrupt` 消息，委托 base `Engine` 驱动回合；观测（会话数 / token / 缓存）；`register_peer_tool` |
| [`tool::AgentTool`](src/tool/agent_tool.rs) | 对等/子 Agent 工具（Agent as Tool）：`Local` 分类不占 IO 槽位，同步 RPC 调用目标会话，大结果 ACL 落库 |
| [`artifact`](src/artifact/mod.rs) | 带 ACL 的工件存储：owner / 授权读者读取校验，有界（数量 + 字节双上限） |

## 4. 快速上手

```rust
use std::sync::Arc;
use referee_ai_base::engine::{Engine, EngineConfig};
use referee_ai_base::provider::deepseek::{DeepSeekConfig, DeepSeekModel, DeepSeekProvider};
use referee_ai_base::tool::{ExecutorConfig, ToolExecutor};
use referee_agent::AgentRuntime;
use referee_core::{Kernel, SupervisionPolicy};
use uuid::Uuid;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let kernel = Kernel::new();

    // 1. 构造地基引擎（provider + 工具 + 预算 + 缓存）
    let provider = Arc::new(DeepSeekProvider::new(
        DeepSeekModel::V4Pro,
        DeepSeekConfig::new(std::env::var("DEEPSEEK_API_KEY")?),
    )?);
    let executor = ToolExecutor::with_defaults().with_kernel(kernel.clone());
    let engine = Engine::new(provider, EngineConfig::default())
        .with_tools(
            referee_ai_base::tool::ToolRegistry::with_defaults(),
            executor,
        );

    // 2. 业务封装为内核扩展
    let runtime = AgentRuntime::new(engine);
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime), 64, SupervisionPolicy::Transient)
        .await?;

    // 3. 发起对话（invoke：请求-响应）
    use referee_ai_base::session::{ChatPayload, Message, SessionMessage};
    let msg = SessionMessage::Chat {
        session_id: Uuid::new_v4(),
        payload: ChatPayload {
            message: Message::user("你好"),
            options: Default::default(),
        },
    };
    let resp_env = kernel.invoke(rid, msg.to_envelope(), 30_000).await?;
    let reply = referee_ai_base::session::SessionReply::from_envelope(&resp_env)?;
    println!("{reply:?}");
    Ok(())
}
```

## 5. 设计约束（继承 base + 业务）

- base 保证最小闭环的并发正确性（回合内顺序异步、协作取消、无跨 await 持锁、错误显式可见）。
- `AgentRuntime.handle` 零阻塞：只做转译 + spawn，回复在派生任务中异步完成。
- 对等能力信任边界：`kernel` / artifact 句柄仅授予可信注册工具（`register_peer_tool`）。
- `referee-core` 零改动。

## 6. 测试

```bash
cargo test -p referee-agent     # 库单测（artifact ACL）+ 集成（peer 对等协作 4 项验收）
cargo clippy -p referee-agent --all-targets -- -D warnings
```

## 7. 相关文档

| 文档 | 说明 |
|------|------|
| [`../README.md`](../README.md) | 仓库总览 |
| [`../referee-ai-base/README.md`](../referee-ai-base/README.md) | 地基模块（核心支撑能力） |
| [`../referee-core/README.md`](../referee-core/README.md) | 内核模块 |
| [`../REFACTOR_PLAN.md`](../REFACTOR_PLAN.md) | 重构执行规划（分层边界与验收口径） |

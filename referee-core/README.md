# Referee Core — 微内核模块

> 工业级防护能力的轻量微内核引擎。只做**通信与治理**，不承载业务逻辑；扩展按需组合。
> 当前进度：**Phase 1 ~ 6 全部完成，25 条测试全绿**。详见 [路线图](#路线图)。

---

## 1. 定位与边界

| 项 | 说明 |
|----|------|
| 最小引擎 | 内核只提供路由、原语、治理三类能力，不预置扩展、不承载业务逻辑 |
| 模块化 | 上层能力（如 [`referee-agent`](../referee-agent/README.md)）作为独立 crate 按需组合 |
| 数据与行为分离 | `Envelope` 是纯数据载体，绝不含逻辑句柄；行为只存在于 `Extension` 与 `Kernel` |

### 设计原则

| 原则 | 含义 |
|------|------|
| 轻量为本 | 内核只做通信与治理，扩展按需组合；易维护、易拓展、易集成 |
| 背压是硬约束 | 所有通道必须有界；缓冲满即返回 `ResourceExhausted`，绝不允许无界分配 |
| 隔离即防御 | 扩展 Panic 只熔断自身，绝不影响内核与其余扩展；`catch_unwind` 是安全边界 |
| 内核永远存活 | 失败路径返回错误码，不 panic；治理状态决定路由行为 |
| 阻塞即违规 | 扩展 `handle` 必须非阻塞，重计算须移交 `spawn_blocking` |
| 类型安全的回信 | 回复依赖消费式 `oneshot`，`reply` 消费 `self`，从结构上杜绝重复回复 |

---

## 2. 核心能力

### 2.1 路由与背压

每个扩展注册时创建一条**优先级三分桶有界通道**（High / Normal / Low，每桶容量 `queue_size`）。发送一律走 `try_send`：

- 队列满 → `ResourceExhausted`（即时拒绝，不阻塞调用方）
- 目标不存在 / 已注销 / 通道关闭 → `TargetUnreachable`

### 2.2 emit — 即发即弃

```rust
kernel.emit(ext_id, Envelope::new()).await?;
```

不等待响应。扩展崩溃 / 停机等异常路径均返回对应错误码。

### 2.3 invoke — 请求响应

```rust
let resp = kernel.invoke(ext_id, req, timeout_ms).await?;
```

`oneshot` 回信通道强关联响应与请求；`timeout_ms` 超时自动切断返回 `Timeout`。响应信封须回传请求的 `correlation_id`。

### 2.4 严格优先级路由 + 老化防饥饿

`Envelope.priority` 决定分桶：`0..=49` High、`50..=149` Normal、`>=150` Low。

- 发送端：独立有界桶 —— **High 永不被满的 Low 桶阻塞**（背压即时返回 `ResourceExhausted`）
- 接收端：自定义有界队列（`Mutex<VecDeque>` + `Notify`）—— **消费严格按 High → Normal → Low，杜绝优先级反转**
- 老化防饥饿：Low 队列头部等待超过 1s 即**优先于 High 消费** —— 持续 High 负载下 Low 最多延迟 1s，绝不永久饿死

### 2.5 受限通信（KernelContext）

`handle` 注入受限 `KernelContext` 而非完整 `Kernel`：

- `ctx.emit(target, env)` — 唯一允许的通信原语（即发即弃，非阻塞）
- `ctx.reply(resp)` — 消费式回信（仅 `invoke` 请求携带回信通道）
- `ctx.spawn_blocking(f)` — 唯一允许的阻塞出口（移交独立线程池）

`invoke` **未注入** —— 扩展无法在 `handle` 内等待其他扩展的响应，从编译期切断嵌套请求响应链（A→invoke B→invoke A 会耗尽线程池死锁）。

### 2.6 监督与自愈

扩展运行循环为两层结构：外层 Supervisor 持有接收端，内层执行 `handle`（`catch_unwind` 隔离）。

| 策略 | 行为 |
|------|------|
| `Transient` | 崩溃即熔断，不重启 |
| `OneForOne { max_restarts, window_secs }` | 窗口内指数退避重启（100ms × 2ⁿ）；超限转 `Stopped` 熔断 |

重启保留通道 —— **崩溃前积压的消息不丢失**，恢复后继续消费。

### 2.7 优雅停机

```rust
kernel.shutdown_graceful(timeout_ms).await?;
```

1. 广播停机信号 → 所有扩展进入 **drain 模式**，排空已入队消息后退出
2. 全局状态置 `Stopping` → 期间所有新 `emit` / `invoke` 返回 `SystemShuttingDown`（并写入死信）
3. 等待全部任务结束；`timeout_ms` 超时则强制中止剩余任务（尽力而为，不无限等待）

### 2.8 死信队列（DLQ）

所有被拦截的 Envelope 连同原因写入死信，供审计 / 重放：

- 崩溃拦截 → `ExtensionCrashed`
- 背压拦截 → `ResourceExhausted`
- 停机拦截 → `SystemShuttingDown`
- 目标不可达 → `TargetUnreachable`

`DlqSink` 为 trait，可注入任意持久化实现；默认 `InMemoryDlq`（环形缓冲，容量受限防 OOM）。

```rust
let dlq = Arc::new(InMemoryDlq::new(1024));
let kernel = Kernel::with_dlq(dlq); // 注入自定义死信队列
```

### 2.9 容错与隔离

- 扩展 `handle` 的 Future 被 `AssertUnwindSafe + catch_unwind` 包裹：Panic → 标记 `Crashed` → 熔断（或按策略重启）
- 崩溃后所有 `emit` / `invoke` 被 dispatch 拦截，返回 `ExtensionCrashed`
- 注销 → 移除路由条目 → Sender drop → 通道关闭 → 监督循环自然退出

### 2.10 WAL 崩溃兜底（可选）

进程被强杀（OOM Kill / 断电）时，内存中积压消息会丢失。启用 WAL 后：

1. `dispatch` 在入队前先 `append` 落盘（先持久化再投递）
2. 扩展处理成功后监督器自动 `ack` 确认
3. 下次启动时 `start_with_recovery()` 将未确认消息**绕过 WAL 追加**直接注入路由表（至少一次投递）

```rust
let wal = Arc::new(InMemoryWal::new());
let kernel = Kernel::with_wal(wal); // 注入 WAL（应用可替换为文件 / 数据库实现）
// ... 注册扩展 ...
kernel.start_with_recovery().await?; // 必须在注册完成后调用
```

---

## 3. 快速上手

```toml
[dependencies]
referee-core = "0.1"
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
```

```rust
use async_trait::async_trait;
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelResult, SupervisionPolicy,
};

// 1. 实现扩展契约
struct EchoExtension { id: CapabilityId }

#[async_trait]
impl Extension for EchoExtension {
    fn id(&self) -> CapabilityId { self.id }

    // 注意：必须非阻塞。重计算请移交 ctx.spawn_blocking。
    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let mut resp = Envelope::new();
        resp.correlation_id = env.correlation_id;
        ctx.reply(resp) // 消费式回信，结构上杜绝重复回复
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kernel = Kernel::new();

    // 2. 注册：优先级有界通道 + 监督策略（崩溃后最多重启 3 次）
    let ext = EchoExtension { id: CapabilityId::new() };
    let ext_id = ext.id();
    let policy = SupervisionPolicy::OneForOne { max_restarts: 3, window_secs: 30 };
    kernel.register(Box::new(ext), 16, policy).await?;

    // 3. invoke：请求响应
    let req = Envelope::new();
    let expected_cid = req.correlation_id;
    let resp = kernel.invoke(ext_id, req, 1000).await?;
    assert_eq!(resp.correlation_id, expected_cid);

    // 4. emit：即发即弃（priority 决定路由桶，0..=49 为 High）
    let mut env = Envelope::new();
    env.priority = 0;
    kernel.emit(ext_id, env).await?;

    // 5. 优雅停机：排空积压后退出
    kernel.shutdown_graceful(1000).await?;
    Ok(())
}
```

---

## 4. 错误模型

| 错误 | 含义 |
|------|------|
| `TargetUnreachable` | 目标未注册 / 已注销 / 通道已关闭 |
| `ExtensionCrashed` | 目标扩展已崩溃（Panic 被熔断） |
| `ResourceExhausted` | 通道缓冲满，背压触发 |
| `Timeout` | `invoke` 在限定时间内未收到响应 |
| `InvalidResponse` | 响应数据非法（预留） |
| `SystemShuttingDown` | 内核处于优雅停机拦截态 |
| `Storage` | WAL 落盘失败 |

## 5. 扩展生命周期

```
Running ── 正常注册 / 正常消费消息
   │
   ├── handle Panic ──→ Crashed ──→（Transient）熔断退出
   │                              └──（OneForOne）退避重启 ──→ Running
   │                                   └── 超限 ──→ Stopped（永久拒绝路由）
   │
   ├── 停机信号 ──→ drain 积压 ──→ 退出（Stopped）
   │
   └── unregister / Sender drop ──→ Stopped（通道关闭，循环自然退出）
```

---

## 6. 架构

```
referee-core/
├── src/
│   ├── lib.rs                  # 顶层重导出（Kernel / Envelope / CapabilityId / KernelContext / SupervisionPolicy / DlqSink / InMemoryDlq / WalSink / InMemoryWal）
│   ├── common/                 # 纯数据载体
│   │   ├── error.rs            # KernelError：... / SystemShuttingDown / Storage
│   │   └── envelope.rs         # Envelope：target/queued_at + 四 ID + priority（路由分桶依据）+ metadata
│   ├── extension/              # SDK 侧契约
│   │   ├── mod.rs              # CapabilityId（Copy + Display）+ Extension trait（handle(ctx, env)）
│   │   ├── context.rs          # KernelContext：emit / reply / spawn_blocking（无 invoke）
│   │   └── dlq.rs              # DlqSink trait + InMemoryDlq（环形缓冲，容量受限）
│   └── kernel/                 # 内核聚合
│       ├── monitor.rs          # GlobalState 全局治理状态（Stopping 拦截）
│       ├── priority.rs         # 自定义三分桶有界队列：老化防饥饿 + Notify 唤醒 + 深度 gauge
│       ├── router.rs           # DashMap<CapabilityId, RouteEntry{sender,state}>，原子状态+路由分发
│       ├── wal.rs              # WalSink trait + InMemoryWal（崩溃兜底 / 恢复通道）
│       ├── shutdown.rs         # ShutdownTx/Rx：watch 广播停机信号，subscribe 派生多接收端
│       ├── supervisor.rs       # ExtensionRuntime：KernelContext 组装 + WAL ACK + 重启决策 / drain
│       └── mod.rs              # Kernel 入口：register / emit / invoke / shutdown_graceful / start_with_recovery / dispatch
└── tests/
    ├── backpressure_test.rs    # Phase 1：背压 + 路由基础（3 条）
    ├── invoke_test.rs          # Phase 2：invoke 原语（3 条）
    ├── isolation_test.rs       # Phase 3：Panic 熔断 + 死循环心跳（2 条）
    ├── governance_test.rs      # Phase 4：优先级 / 自愈 / 熔断 / 停机 / DLQ（5 条）
    ├── observability_test.rs   # Phase 5：tracing 关联 + metrics 指标（4 条）
    └── robustness_test.rs      # Phase 6：老化防饥饿 / WAL 恢复 / 受限通信（8 条）
```

### 分层依赖规则

- `common/` 为纯数据层，不依赖任何其他层（数据与行为分离）。
- `extension/`（SDK 契约）→ `kernel/`（实现），单向依赖，无反向引用。
- `Envelope.target` 用 `Uuid` 而非 `CapabilityId`，避免 common → extension 分层反向依赖。

---

## 7. 安全契约

- **`handle` 必须非阻塞**：纯 CPU 死循环 / 密集计算会永久占用单个 Tokio Worker，耗尽线程池。重计算必须移交 `ctx.spawn_blocking`。
- **无嵌套等待**：`handle` 内仅 `emit` / `reply` / `spawn_blocking`；`invoke` 未注入，编译期杜绝嵌套请求响应链死锁。
- **背压**：所有通道有界（每优先级桶独立容量），缓冲满即拒绝，绝无无界分配（拒绝 OOM）。
- **回复唯一性**：`reply` 消费 `self`，重复回复在结构上不可能。
- **停机不丢已确认消息**：drain 模式保证已入队消息处理完才退出；超时强制中止是尽力而为的兜底。
- **死信有界**：`InMemoryDlq` 为环形缓冲，满则丢弃最旧，不随负载增长内存。

---

## 8. 验证

```bash
cargo test -p referee-core -- --nocapture     # 25 条测试（Phase 1 ~ 6）
cargo test --workspace                        # 全量回归（core 25 + base 121 + agent 31 = 177 条）
cargo clippy --all-targets -- -D warnings     # 零警告
cargo fmt --check                             # 格式整洁
```

## 9. 路线图

| 阶段 | 主题 | 状态 |
|------|------|------|
| Phase 1 | 骨架与背压验证 | ✅ 完成 |
| Phase 2 | 原语与上下文（invoke） | ✅ 完成 |
| Phase 3 | 容错与隔离（catch_unwind） | ✅ 完成 |
| Phase 4 | 治理与生命周期闭环（优先级 / 自愈 / 停机 / DLQ） | ✅ 完成 |
| Phase 5 | 可观测层（tracing 关联 + metrics 指标） | ✅ 完成 |
| Phase 6 | 健壮性深化与并发安全（KernelContext / 老化防饥饿 / 状态路由原子合并 / WAL） | ✅ 完成 |

依赖范围（规范清单，不擅自引入新依赖）：`tokio`、`dashmap`、`parking_lot`、`serde`、`bytes`、`thiserror`、`async-trait`、`uuid`、`futures`、`tracing`、`metrics`、`tracing-subscriber`（dev）。

---

## 10. 相关文档

| 文档 | 说明 |
|------|------|
| [`../README.md`](../README.md) | 仓库总览（目录 / 大纲） |
| [`../referee-ai-base/README.md`](../referee-ai-base/README.md) | 核心支撑层模块描述（会话引擎 / 流式 / 会话生命周期 / 提示词分段编排 / 用量计量） |
| [`../referee-agent/README.md`](../referee-agent/README.md) | 业务封装层模块描述 |
| [`../PHASE_STATUS.md`](../PHASE_STATUS.md) | Phase 状态跟踪（含关键设计决策与偏差记录） |
| [`../AGENTS.md`](../AGENTS.md) | 工程约束（设计思想 / 依赖清单 / 工作纪律） |

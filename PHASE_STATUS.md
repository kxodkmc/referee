# Phase 状态跟踪

> 用途：记录已完成阶段、验收结果与遗留事项，供后续阶段开工前核对进度。
> 更新时间：Phase 4 完成时（cargo build / test / clippy / fmt 全部通过）。

## 总览

| 阶段 | 主题 | 状态 | 完成时间 |
|------|------|------|----------|
| Phase 1 | 骨架与背压验证 | ✅ 已完成 | — |
| Phase 2 | 原语与上下文（invoke） | ✅ 已完成 | — |
| Phase 3 | 容错与隔离（catch_unwind） | ✅ 已完成 | — |
| Phase 4 | 治理与生命周期闭环（优先级 / 自愈 / 停机 / DLQ） | ✅ 已完成 | — |

## Phase 1 — 骨架与背压验证（已完成）

### 交付范围

| 项 | 状态 | 位置 |
|----|------|------|
| Kernel / Router / Monitor 骨架 | ✅ | `referee-core/src/kernel/` |
| register — 有界通道 + 任务派生 | ✅ | `referee-core/src/kernel/mod.rs` |
| unregister — 路由移除 + 状态标记 | ✅ | `referee-core/src/kernel/mod.rs` |
| emit — try_send 背压 | ✅ | `referee-core/src/kernel/mod.rs` |
| catch_unwind 隔离 | ✅ | 后续阶段完成（Phase 3） |

### 验收清单（Phase 1 结果）

| # | 检查项 | 预期 | 结果 |
|---|--------|------|------|
| 1 | `cargo build` | 零错误 | ✅ |
| 2 | `cargo clippy --all-targets -- -D warnings` | 零警告 | ✅ |
| 3 | 测试：通道满载 | `ResourceExhausted` 在 `QUEUE_SIZE ± 1` 条内触发 | ✅ |
| 4 | 测试：未注册目标 | 立即返回 `TargetUnreachable` | ✅ |
| 5 | 测试：unregister 后 | `emit` 返回 `TargetUnreachable` | ✅ |
| 6 | 内存稳定 | `sent` 不超过 `QUEUE_SIZE * 2`（证明无无限分配） | ✅ |
| 7 | 无 OOM | 进程存活、正常退出 | ✅ |

## Phase 2 — 原语与上下文（已完成）

### 交付范围

| 项 | 状态 | 位置 |
|----|------|------|
| Router 提取 `dispatch`（统一分发入口） | ✅ | `referee-core/src/kernel/router.rs` |
| `MessageContext::with_reply` 接通 | ✅ | `referee-core/src/extension/context.rs` |
| `Kernel::invoke`（oneshot + timeout） | ✅ | `referee-core/src/kernel/mod.rs` |
| `Kernel::check_state` 抽取（emit/invoke 共用） | ✅ | Phase 4 已并入 `dispatch` 拦截 |
| catch_unwind 熔断隔离 | ✅ | 后续阶段完成（Phase 3） |

### invoke 语义

```
dispatch(Running) → 创建 oneshot → MessageContext::with_reply → dispatch
  → timeout 等待 rx：
      Ok(Ok(resp))      → 返回响应
      Ok(Err(_))        → 扩展崩溃/注销（Sender drop）→ TargetUnreachable
      Err(_)            → 超时切断 → Timeout
```

### 验收清单（Phase 2 结果）

| # | 检查项 | 预期 | 结果 |
|---|--------|------|------|
| 1 | `cargo build` | 零错误 | ✅ |
| 2 | `cargo clippy --all-targets -- -D warnings` | 零警告，`dead_code` 标记全部消除 | ✅ |
| 3 | 测试：正常回复 | 成功返回 Envelope 且 `correlation_id` 匹配 | ✅ |
| 4 | 测试：超时切断 | 50ms 后返回 `Timeout`，未阻塞线程 | ✅ |
| 5 | 测试：目标消失 | 立即返回 `TargetUnreachable` | ✅ |
| 6 | 数据一致性 | `reply()` 消费 self 后通道正常释放，无悬挂 Sender | ✅ |

### 对执行文档的偏差（有意为之，均已验证）

| 偏差 | 原因 |
|------|------|
| 测试 1 中 `ctx.envelope.clone()` 而非移动 | 直接 `let req = ctx.envelope;` 会部分移动字段，随后 `ctx.reply()` 消费 self 无法编译 |
| 测试 2/3 用 `result.unwrap_err()` 断言错误 | `Envelope` 未实现 `PartialEq`，不能对整个 `Result<Envelope, _>` 用 `assert_eq!`；比对错误值更精确 |

## Phase 3 — 容错与隔离（已完成）

### 交付范围

| 项 | 状态 | 位置 |
|----|------|------|
| `run_extension_loop` 插入 `catch_unwind` 熔断边界 | ✅ | Phase 4 重构为 `supervisor.rs` |
| Panic 后标记 `Crashed` 并 break 熔断循环 | ✅ | `referee-core/src/kernel/supervisor.rs` |
| 通道关闭标记 `Stopped`（不覆盖 Crashed） | ✅ | `referee-core/src/kernel/supervisor.rs` |
| 测试：Panic 存活 + 状态拦截 | ✅ | `referee-core/tests/isolation_test.rs` |
| 测试：死循环不阻塞 Runtime 心跳 | ✅ | `referee-core/tests/isolation_test.rs` |
| 新增依赖 `futures`（`FutureExt::catch_unwind`） | ✅ | `referee-core/Cargo.toml` |

### 验收清单（Phase 3 结果）

| # | 检查项 | 预期 | 结果 |
|---|--------|------|------|
| 1 | `cargo build` | 零错误 | ✅ |
| 2 | `cargo clippy --all-targets -- -D warnings` | 零警告 | ✅ |
| 3 | 测试 1：Panic 存活 | 触发 Panic 后，后续 emit 返回 `ExtensionCrashed` | ✅ |
| 4 | 测试 1：内核存活 | Panic 不影响内核对象，unregister 正常响应 | ✅ |
| 5 | 测试 2：死循环心跳 | 多线程环境下，心跳计数器 > 0，Runtime 未被卡死 | ✅ |

### 对执行文档的偏差（有意为之，均已验证）

| 偏差 | 原因 |
|------|------|
| 测试 2 不用 `#[tokio::test(flavor = "multi_thread")]`，改为手动 `Builder` + `rt.shutdown_background()` | `Runtime::drop` 会**永久等待**被死循环卡死的 Worker；`#[tokio::test]` 隐式 drop runtime 必然挂死；必须 `shutdown_background()` 跳过等待 |
| 移除 `run_extension_loop` 导入清单中的 `KernelResult` | 改造后该函数不再引用它，保留会触发 clippy `unused_imports` 告警 |
| 死循环体 `loop {}` 改为 `loop { std::hint::spin_loop(); }` | 空 `loop {}` 触发 clippy warn-by-default `empty_loop` lint，`-D warnings` 下无法通过 |
| 新增 `futures` 依赖并同步 AGENTS.md 清单 | 项目约束「依赖变更须先同步 AGENTS.md」；顺带补录 Phase 2 遗漏的 `uuid` |

## Phase 4 — 治理与生命周期闭环（已完成）

### 交付范围

| 项 | 状态 | 位置 |
|----|------|------|
| `common/error.rs` 新增 `SystemShuttingDown` | ✅ | `referee-core/src/common/error.rs` |
| 严格优先级通道（三分桶 + biased 消费） | ✅ | `referee-core/src/kernel/priority.rs` |
| 优雅停机信号（广播 + 保留最新状态） | ✅ | `referee-core/src/kernel/shutdown.rs` |
| Monitor 全局治理状态（Stopping 拦截） | ✅ | `referee-core/src/kernel/monitor.rs` |
| 死信队列 Trait + 默认实现 | ✅ | `referee-core/src/extension/dlq.rs` |
| 监督器：Transient / OneForOne 重启 + 窗口熔断 | ✅ | `referee-core/src/kernel/supervisor.rs` |
| Router 升级为 PrioritySender 路由 | ✅ | `referee-core/src/kernel/router.rs` |
| Kernel 聚合：register(policy) / shutdown_graceful / dispatch 三级拦截 | ✅ | `referee-core/src/kernel/mod.rs` |
| `Kernel` 可 `Clone`（共享同一内核，支持并发停机/路由） | ✅ | `referee-core/src/kernel/mod.rs` |
| 新增测试：严格优先级 / OneForOne 自愈 / 窗口熔断 / 优雅停机 / DLQ 降级 | ✅ | `referee-core/tests/governance_test.rs` |

### 验收清单（Phase 4 结果）

| # | 检查项 | 预期 | 结果 |
|---|--------|------|------|
| 1 | `cargo build` | 零错误 | ✅ |
| 2 | `cargo clippy --all-targets -- -D warnings` | 零警告 | ✅ |
| 3 | `cargo fmt --check` | 格式整洁 | ✅ |
| 4 | 严格优先级 | 塞满 Low 触发 `ResourceExhausted`，High 发送成功且先被消费 | ✅ |
| 5 | OneForOne 自愈 | Panic 后退避重启，3 条积压消息全部处理（无丢失），路由恢复 | ✅ |
| 6 | 窗口熔断 | 连续 Panic 超限（max_restarts=2）后返回 `TargetUnreachable` | ✅ |
| 7 | 优雅停机 | 10 条积压全部排空后退出；停机期间新 emit 返回 `SystemShuttingDown` | ✅ |
| 8 | DLQ 降级 | Crashed 扩展消息返回 `ExtensionCrashed`，`InMemoryDlq` 捕获对应 Envelope | ✅ |
| 9 | 全量回归 | Phase 1 ~ 4 共 13 条测试全绿 | ✅ |

### 对执行文档的偏差（有意为之，均已验证）

| 偏差 | 原因 |
|------|------|
| `priority.rs` 放 `kernel/` 而非方案中的 `common/` | `common/` 是纯数据层（数据与行为分离）；优先级通道是行为组件且依赖 `MessageContext`（extension 层），放 `kernel/` 保持分层单向依赖 |
| 停机信号用 `watch` 而非方案中的 `Notify` | `Notify::notify_waiters` **不存储 permit**：先 trigger 后注册的 waiter 会永久挂起（多接收端 + 任意时序下有竞态）；`watch` 语义等价（广播 + 保留最新值）且无竞态，代码更短 |
| 内层循环用 `res = rx.recv()` 而非方案中的 `Some(ctx) = rx.recv()` | 方案写法在「所有 Sender drop」时该分支仅被 disabled，而 `select!` 的 `else` 要求**所有**分支 disabled（停机分支仍 active），导致通道关闭时循环挂死；改为显式匹配 `None` 返回 `NormalExit` |
| drain 模式用 `try_recv` 排空（方案未给实现细节） | 停机后新消息已被 dispatch 拦截，队列只会清空不会增长；`try_recv` 循环即可确定性排空，避免无限等待 |
| 全局状态用独立 `GlobalState`（Running/Stopping）而非复用 `ExtensionState` | 扩展三态与全局治理状态语义分离；`SystemShuttingDown` 是错误码而非扩展状态，不混入状态机 |
| `Kernel` 增加 `with_dlq` 注入点 + `derive(Clone)` | DLQ 测试需共享实例断言捕获内容；停机并发触发需 Clone（JoinSet 由 `Arc<Mutex>` 共享） |
| `Router::dispatch` 失败返回 `(KernelError, Envelope)` | 需将被拒 Envelope 回传上层写入 DLQ（方案中 dispatch 与 DLQ 混在 Kernel 内，此拆分保持 Router 纯路由职责） |
| `supervisor.rs` 用 `self.ext.as_ref()`（`&dyn Extension`） | 满足 clippy `borrowed-box` lint；`&Box<T>` 应改为 `&T` |

### 验证命令

```bash
cargo build                                          # 零错误
cargo test --test governance_test -- --nocapture     # 5 条 Phase 4 测试
cargo test -- --nocapture                            # 全量回归（Phase 1 ~ 4，共 13 条）
cargo clippy --all-targets -- -D warnings            # 零警告
cargo fmt --check                                    # 格式整洁
```

## 当前源码结构

```
referee-core/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # 顶层重导出（Kernel / Envelope / CapabilityId / SupervisionPolicy / DlqSink / InMemoryDlq）
│   ├── common/                 # 纯数据载体，不含逻辑句柄
│   │   ├── error.rs            # KernelError（... / Timeout / InvalidResponse / SystemShuttingDown）
│   │   └── envelope.rs         # Envelope：四 ID + priority + metadata
│   ├── extension/              # SDK 侧契约
│   │   ├── mod.rs              # CapabilityId（Copy）+ Extension trait（id / handle）
│   │   ├── context.rs          # MessageContext：reply(self) 消费式回信，with_reply 注入回信通道
│   │   └── dlq.rs              # DlqSink trait + InMemoryDlq（环形缓冲，容量受限防 OOM）
│   └── kernel/                 # 内核聚合
│       ├── monitor.rs          # ExtensionState 状态机 + GlobalState 全局治理状态（parking_lot::RwLock，读优先）
│       ├── priority.rs         # PrioritySender/Receiver：三分桶有界通道，biased 严格优先级消费
│       ├── router.rs           # DashMap<CapabilityId, PrioritySender>，dispatch 统一分发 + 被拒 Envelope 回传
│       ├── shutdown.rs         # ShutdownTx/ShutdownRx：watch 广播停机信号，subscribe 派生多接收端
│       ├── supervisor.rs       # ExtensionRuntime + SupervisionPolicy：重启决策 / 指数退避 / drain 模式
│       └── mod.rs              # Kernel 入口（register/unregister/emit/invoke/shutdown_graceful/dispatch）
└── tests/
    ├── backpressure_test.rs    # 3 条 Phase 1 测试
    ├── invoke_test.rs          # 3 条 Phase 2 测试
    ├── isolation_test.rs       # 2 条 Phase 3 测试
    └── governance_test.rs      # 5 条 Phase 4 测试
```

## 关键设计决策（P1 ~ P4 已落地）

| 决策 | 理由 |
|------|------|
| `try_send` 而非 `send().await` | 背压即时拒绝，不阻塞调用方 |
| `DashMap` + `PrioritySender` | 并发读路由零锁争用；PrioritySender 内部 Sender Clone 廉价 |
| Monitor 用 `parking_lot::RwLock` | 状态读远多于写，读优先 |
| Router / Monitor 内部 `Arc` 包装 | 廉价 Clone，可在 spawned task 间共享 |
| 运行循环在 register 中派生 | 通道创建后立即激活消费端，避免消息黑洞 |
| 循环退出时标记 `Stopped` | unregister 移除路由 → Sender drop → 通道关闭 → 循环自然退出 |
| `MessageContext.reply_to` 私有 | 仅 `reply()` 可访问，防误用 |
| `Router::dispatch` 统一入口 | emit / invoke 共用同一底层发送路径，消除重复逻辑 |
| `invoke` 用 `oneshot` + `timeout` | 响应与请求强关联；超时自动切断，无悬挂 Sender |
| 优先级三分桶独立有界 | High 永不被满的 Low 桶阻塞；biased 消费杜绝优先级反转 |
| 监督两层循环 | 外层保通道重启不丢消息；内层 catch_unwind 隔离 |
| 停机用 `JoinSet` 统一跟踪 | register 收集全部运行时 task，shutdown 时统一 join / 超时 abort_all |
| dispatch 三级拦截（停机 → 状态 → 路由） | 单一入口强制全局状态治理，所有拦截点统一写 DLQ |
| `DlqSink` trait 注入 | 死信实现可替换（持久化 / 观测 / 测试），内核不绑定具体实现 |

## 历史偏差记录

| 偏差 | 原因 |
|------|------|
| 仓库根改为 workspace，`referee-core` 为唯一成员 | 原根目录是 `referee` 二进制桩，无 workspace 时无法从根目录构建 |
| `CapabilityId` 派生 `Copy` | 文档源码多处按值复用 ID；内部仅包 `Uuid`（本身 Copy），加 Copy 更自洽 |
| 引入 `uuid` 依赖 | 执行文档明确要求（AGENTS.md 依赖清单已同步补录） |
| 引入 `futures` 依赖 | Phase 3 需要 `FutureExt::catch_unwind`（AGENTS.md 依赖清单已同步补录） |
| `priority.rs` 置于 `kernel/` 而非 `common/` | 保持 common 纯数据层，避免 common → extension 反向依赖（详见 Phase 4 偏差表） |
| 停机信号用 `watch` 而非 `Notify` | `notify_waiters` 不存储 permit，存在通知丢失竞态（详见 Phase 4 偏差表） |

## 后续阶段待办

> Phase 1 ~ 4 已全部完成，当前无待办。

## 开工前核对清单

1. `cargo build` / `cargo clippy --all-targets -- -D warnings` / `cargo fmt --check` 零告警
2. 有界分配：无新引入无界 buffer / HashMap（优先级桶、DLQ 环形缓冲均容量受限）
3. Panic 隔离：不破坏 `catch_unwind` 边界
4. 数据/行为分离：Envelope 仍为纯数据，不引入逻辑句柄
5. 依赖变更须先同步 AGENTS.md 清单

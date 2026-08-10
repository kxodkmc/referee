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
| Phase 5 | 可观测层（tracing 全链路关联 + metrics 核心指标） | ✅ 已完成 | — |
| Phase 6 | 健壮性深化与并发安全（KernelContext / 老化防饥饿 / 状态路由原子合并 / WAL） | ✅ 已完成 | — |

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

## Phase 5 — 可观测层（已完成）

### 交付范围

| 项 | 状态 | 位置 |
|----|------|------|
| 新增依赖：`tracing` / `metrics`（门面），`tracing-subscriber`（dev） | ✅ | `referee-core/Cargo.toml` |
| `CapabilityId` 实现 `Display` | ✅ | `referee-core/src/extension/mod.rs` |
| `dispatch` 入口 `kernel_dispatch` Span（trace_id 穿透）+ `referee_dispatch_total` 计数 | ✅ | `referee-core/src/kernel/mod.rs` |
| `handle_observed`：`extension_handle` Span + 延迟直方图 + Panic 计数器（先 catch_unwind 再 instrument） | ✅ | `referee-core/src/kernel/supervisor.rs` |
| 优先级通道共享 `Arc<AtomicUsize>` 深度计数 + `referee_queue_depth` gauge | ✅ | `referee-core/src/kernel/priority.rs` |
| 新增测试：Span 关联 / 队列深度 / 延迟与 Panic / 路由失败 | ✅ | `referee-core/tests/observability_test.rs` |

### 验收清单（Phase 5 结果）

| # | 检查项 | 预期 | 结果 |
|---|--------|------|------|
| 1 | `cargo build` | 零错误 | ✅ |
| 2 | `cargo clippy --all-targets -- -D warnings` | 零警告 | ✅ |
| 3 | `cargo fmt --check` | 格式整洁 | ✅ |
| 4 | Span 穿透 | `kernel_dispatch` 与 `extension_handle` 的日志行共享同一 `trace_id` | ✅ |
| 5 | 队列深度 | emit 5 条深度=5；消费后回落 0 | ✅ |
| 6 | 处理延迟 | sleep 100ms 扩展耗时记录 ~0.1s，`outcome=ok` | ✅ |
| 7 | Panic 指标 | `outcome=panic` 耗时记录 + `referee_extension_panics_total` 递增 | ✅ |
| 8 | 路由失败 | 满队列时 `referee_dispatch_total{result=full}` 递增 | ✅ |
| 9 | 全量回归 | Phase 1 ~ 5 共 17 条测试全绿 | ✅ |

### 对执行文档的偏差（有意为之，均已验证）

| 偏差 | 原因 |
|------|------|
| `metrics` 版本保持 0.22，测试自写内存 Recorder（约 80 行） | `metrics` 0.22 **没有** `testing` feature（0.23+ 才有）；自写 Recorder 实现 `metrics::Recorder` trait（register 去重 + 句柄共享），不引入新依赖 |
| `dispatch` 用 `.instrument(span).await` 而非方案中的 `span.enter()` | async fn 中 `enter()` guard 在 await 挂起期间保持线程 current span，会污染同线程其他任务的日志归属；`instrument` 仅在 poll 时进入，语义正确 |
| `priority.rs` 的 `update_depth` 用 `fetch_update + checked_sub` | 方案写法 `fetch_sub(1) - 1` 在 depth=0 时会回绕成天文数字；`checked_sub` 原子防下溢（正常协议下 recv 先于 send 成功，此为防御性兜底） |
| 测试 1 用已知 `trace_id` 过滤断言行 | 并行测试输出混入同一 buffer，按本请求的 trace_id 精确匹配，杜绝 flaky |
| 测试 2 用 `PrioritySender` 直连（不 recv）验证深度 | 「向扩展 emit 5 条且不消费」通过不 recv 的直连通道确定性复现，避免 supervisor 异步消费竞态 |
| 测试初始化重定向 fmt 输出到共享 buffer | `TestWriter` 走 `print!` 依赖 libtest 捕获，无法程序化断言；自定义 `SharedWriter`（`MakeWriter` 写回 `Arc<Mutex<Vec<u8>>>`）+ `with_ansi(false)` |

## Phase 6 — 健壮性深化与并发安全（已完成）

### 交付范围

| 项 | 状态 | 位置 |
|----|------|------|
| `KernelContext` 受限上下文注入 `handle`（emit / reply / spawn_blocking，无 invoke） | ✅ | `referee-core/src/extension/context.rs` |
| `Extension` trait 签名变更：`handle(&self, ctx: KernelContext, env: Envelope)` | ✅ | `referee-core/src/extension/mod.rs` |
| 自定义优先级队列：`Mutex<VecDeque>` + `Notify`，Low 老化优先防饥饿 + 关闭语义 | ✅ | `referee-core/src/kernel/priority.rs` |
| 状态与路由原子合并：`RouteEntry{sender, state}`，dispatch 同锁判定 | ✅ | `referee-core/src/kernel/router.rs` |
| Monitor 瘦身为全局治理状态（扩展状态并入 Router） | ✅ | `referee-core/src/kernel/monitor.rs` |
| WAL：`WalSink` trait + `InMemoryWal`，dispatch 落盘 / 监督器 ACK / 恢复通道 | ✅ | `referee-core/src/kernel/wal.rs` |
| `Envelope` 增加 `target`（恢复路由）与 `queued_at`（老化判定）；`KernelError::Storage` | ✅ | `referee-core/src/common/` |
| Kernel：`with_wal` / `with_dlq_wal` 构造、dispatch 预检 + WAL、`start_with_recovery` | ✅ | `referee-core/src/kernel/mod.rs` |
| 新增测试：老化防饥饿 / WAL 记录与 ACK / WAL 恢复 / emit 穿透 / spawn_blocking / 恢复失败 DLQ / 同 id 重注册 / Panic ACK | ✅ | `referee-core/tests/robustness_test.rs` |

### 验收清单（Phase 6 结果）

| # | 检查项 | 预期 | 结果 |
|---|--------|------|------|
| 1 | `cargo build` | 零错误 | ✅ |
| 2 | `cargo clippy --all-targets -- -D warnings` | 零警告 | ✅ |
| 3 | `cargo fmt --check` | 格式整洁 | ✅ |
| 4 | 老化防饥饿 | 持续 High 负载下 Low 在 1s 阈值内被消费（`robustness_test` 用例 1） | ✅ |
| 5 | WAL 记录 + ACK | dispatch 落盘 → handle 成功 → `pending_len()==0` | ✅ |
| 6 | WAL 恢复 | 未确认消息重放至扩展，处理成功 ACK；二次恢复不重复投递 | ✅ |
| 7 | 恢复失败兜底 | 恢复投递失败 → ACK（防无限重放）+ 进 DLQ | ✅ |
| 8 | 受限通信 | `ctx.emit` 可送达目标扩展；`ctx.spawn_blocking` 可用 | ✅ |
| 9 | 全量回归 | Phase 1 ~ 6 共 25 条测试全绿 | ✅ |

### 对执行文档的偏差（有意为之，均已验证）

| 偏差 | 原因 |
|------|------|
| `KernelContext` 保留 `reply` 方法 | 规划仅列 emit / spawn_blocking，但 invoke 原语（Phase 2 验收）依赖回信通道；`reply` 是非阻塞发送，不构成嵌套等待死锁，故保留 |
| 老化实现修正：Low 头部超阈值**优先于** High 消费 | 规划代码只在 high/norm 空时检查 low 老化，无法解决持续 High 负载下的饥饿；改为老化优先 + 无竞争时 Low 直接消费（最多饿 1s） |
| 通道关闭语义修正：最后一个 Sender drop 置 `closed` 标志 + `notify_one` | 规划代码「队列空即返回 None」错误（空 ≠ 关闭，会让监督循环立即退出）；`PrioritySender::Drop` 用 `Arc::strong_count==2` 判定最后一个 Sender |
| `Monitor` 未完全废弃 | 扩展状态并入 Router（`RouteEntry`），但全局治理状态（Stopping 拦截）与 `is_stopping` 保留在 Monitor，语义独立 |
| WAL `recover` 返回 `Vec<(Uuid, Envelope)>` | 规划返回 `Vec<Envelope>` 丢失日志 ID，恢复后无法 ACK 对应记录（会造成无限重放）；恢复消息携带原 WAL ID，处理成功自动 ACK |
| WAL ACK 由监督器在 `handle` 成功后自动触发 | 规划称「通过 KernelContext 触发」但未给出方法；监督器统一 ACK 使扩展对持久化无感，panic 消息不 ACK（崩溃重放兜底） |
| `Envelope` 增加 `target: Uuid` 与 `queued_at: Instant` | 规划代码引用 `env.target` / `ctx.envelope.queued_at` 但原结构无此字段；target 用 `Uuid` 而非 `CapabilityId`，避免 common → extension 分层反向依赖 |
| dispatch 采用「状态预检 + 原子投递」两层 | WAL 落盘决策需先于投递判定状态（不为注定失败投递落盘）；最终投递仍以 `router.dispatch` 同锁原子判定为准（状态可能预检后变化） |
| 队列 `recv` / `try_recv` 改为 `&self` | 自定义实现内部锁 + Notify，无需 `&mut self`；supervisor 调用处兼容 |
| 路由条目引入注册代际（`gen`） | 同 id 注销后快速重注册时，旧监督任务退出若直接 `get_state==Running → Stopped` 会误置新条目；代际匹配才收敛（review 修复） |
| Panic 消费尝试后同样 ACK | 进程内 `catch_unwind` 捕获的 panic 若留在 WAL 会无界滞留且存活期恢复重复投递；进程级崩溃时该行不执行，重放语义不变（review 修复） |
| 扩展运行时注入 `KernelView`（不含 task 集合）而非 `Kernel` | 打破「task → Kernel → task 集合 → task」循环引用：死循环扩展在 Kernel 释放后仍可被 JoinSet drop 强制中止（review 修复） |

## 当前源码结构

```
referee-core/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # 顶层重导出（Kernel / Envelope / CapabilityId / KernelContext / SupervisionPolicy / DlqSink / InMemoryDlq / WalSink / InMemoryWal）
│   ├── common/                 # 纯数据载体，不含逻辑句柄
│   │   ├── error.rs            # KernelError（... / SystemShuttingDown / Storage）
│   │   └── envelope.rs         # Envelope：target/queued_at + 四 ID + priority + metadata
│   ├── extension/              # SDK 侧契约
│   │   ├── mod.rs              # CapabilityId（Copy + Display）+ Extension trait（handle(ctx, env)）
│   │   ├── context.rs          # KernelContext（emit/reply/spawn_blocking）+ MessageContext（队列元素）
│   │   └── dlq.rs              # DlqSink trait + InMemoryDlq（环形缓冲，容量受限防 OOM）
│   └── kernel/                 # 内核聚合
│       ├── monitor.rs          # GlobalState 全局治理状态（Stopping 拦截）
│       ├── priority.rs         # 自定义三分桶有界队列：老化防饥饿 + Notify 唤醒 + 深度 gauge
│       ├── router.rs           # DashMap<CapabilityId, RouteEntry{sender,state}>，原子状态+路由分发
│       ├── wal.rs              # WalSink trait + InMemoryWal（崩溃兜底 / 恢复通道）
│       ├── shutdown.rs         # ShutdownTx/Rx：watch 广播停机信号，subscribe 派生多接收端
│       ├── supervisor.rs       # ExtensionRuntime：KernelContext 组装 + WAL ACK + 重启决策 / drain
│       └── mod.rs              # Kernel 入口（register/emit/invoke/shutdown_graceful/start_with_recovery/dispatch）
└── tests/
    ├── backpressure_test.rs    # 3 条 Phase 1 测试
    ├── invoke_test.rs          # 3 条 Phase 2 测试
    ├── isolation_test.rs       # 2 条 Phase 3 测试
    ├── governance_test.rs      # 5 条 Phase 4 测试
    ├── observability_test.rs   # 4 条 Phase 5 测试
    └── robustness_test.rs      # 8 条 Phase 6 测试
```

## 关键设计决策（P1 ~ P6 已落地）

| 决策 | 理由 |
|------|------|
| `try_send` 而非 `send().await` | 背压即时拒绝，不阻塞调用方 |
| 自定义优先级队列（`Mutex<VecDeque>` + `Notify`） | 需要头部 peek 做老化探测（`tokio::mpsc` 无此能力）；`notify_one` 存储 permit，无通知丢失竞态 |
| Low 老化优先（1s 阈值） | 严格优先级在持续 High 负载下会永久饿死 Low；老化优先保证 Low 最多延迟 1s |
| 状态与路由合并（`RouteEntry{sender,state}`） | register/unregister 与并发 dispatch 无盲区窗口，杜绝消息静默丢失 |
| `KernelContext` 注入取代完整 Kernel | handle 内仅 emit / reply / spawn_blocking，编译期切断嵌套 invoke 死锁链条 |
| WAL 先落盘再入队 | 进程级崩溃（OOM Kill / 断电）后未确认消息经恢复通道重放（at-least-once） |
| 恢复通道绕过 WAL 追加 | 防止恢复消息被重复落盘形成死循环；恢复消息带原 WAL ID，成功即 ACK |
| Router / Monitor 内部 `Arc` 包装 | 廉价 Clone，可在 spawned task 间共享 |
| 运行循环在 register 中派生 | 通道创建后立即激活消费端，避免消息黑洞 |
| 循环退出时标记 `Stopped` | unregister 移除路由 → Sender drop → 通道关闭 → 循环自然退出 |
| `invoke` 用 `oneshot` + `timeout` | 响应与请求强关联；超时自动切断，无悬挂 Sender |
| 优先级三分桶独立有界 | High 永不被满的 Low 桶阻塞；biased 消费杜绝优先级反转 |
| 监督两层循环 | 外层保通道重启不丢消息；内层 catch_unwind 隔离 |
| 停机用 `JoinSet` 统一跟踪 | register 收集全部运行时 task，shutdown 时统一 join / 超时 abort_all |
| dispatch 拦截链（停机 → 状态预检 → WAL → 原子投递） | 单一入口强制全局状态治理，所有拦截点统一写 DLQ |
| `DlqSink` / `WalSink` trait 注入 | 死信与持久化实现可替换，内核不绑定具体后端 |

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

> Phase 1 ~ 6 已全部完成，当前无待办。

## 开工前核对清单

1. `cargo build` / `cargo clippy --all-targets -- -D warnings` / `cargo fmt --check` 零告警
2. 有界分配：无新引入无界 buffer / HashMap（优先级桶、DLQ 环形缓冲、WAL 内存实现均容量受限或语义等价）
3. Panic 隔离：不破坏 `catch_unwind` 边界
4. 数据/行为分离：Envelope 仍为纯数据，不引入逻辑句柄
5. 依赖变更须先同步 AGENTS.md 清单
6. 死锁防线：扩展 `handle` 内仅 `emit` / `reply` / `spawn_blocking`，无 `invoke` 注入

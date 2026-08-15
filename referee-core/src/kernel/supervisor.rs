//! 监督与生命周期闭环 — 扩展运行循环 + 崩溃 / 超时治理
//!
//! 两层结构：外层 Supervisor 持有接收端，负责崩溃后的重启决策并保留
//! 通道（积压消息不丢失）；内层为实际 `handle` 执行，受 `catch_unwind`
//! （Panic 熔断）与可选 `handle_timeout`（挂起治理）双重隔离。
//!
//! 三条治理保证：
//! 1. Panic 只熔断自身 — `catch_unwind` 是安全边界（`panic=abort` 下不生效）；
//! 2. 挂起等同崩溃 — 超时切断视为一次 Crashed，走监督策略退避重启 / 熔断，
//!    杜绝「无声停摆：队列堆满 → 全量进 DLQ 而内核无感」；
//! 3. 积压绝不静默丢失 — 任何终态退出（熔断 / 重启超限 / 停机）前，
//!    未消费积压逐条转储 DLQ 供审计 / 重放。
//!
//! 停机封路：drain 完成后先将状态收敛为 `Stopped`（与 `router.dispatch`
//! 的状态检查在同一路由条目锁内互斥），此后迟到投递必然被拒进 DLQ；
//! 随后做终末 drain 消费收敛窗口内最后入队的消息 —— 两者共同杜绝
//! 「入队成功却无人消费」的停机滞留竞态。

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::FutureExt;
use metrics::{counter, histogram};
use tracing::{info_span, Instrument};
use uuid::Uuid;

use crate::common::{KernelError, KernelResult};
use crate::extension::{CapabilityId, Extension, KernelContext, MessageContext};
use crate::kernel::priority::PriorityReceiver;
use crate::kernel::router::{ExtensionState, Router};
use crate::kernel::shutdown::ShutdownRx;
use crate::kernel::KernelView;

/// 监督策略
#[derive(Debug, Clone)]
pub enum SupervisionPolicy {
    /// 崩溃即熔断，不重启（Phase 3 行为）
    Transient,
    /// 单点重启：窗口内最多重启 `max_restarts` 次，指数退避
    OneForOne { max_restarts: u32, window_secs: u64 },
}

/// 扩展运行时 — 由 Kernel 在注册时派生为独立 task
///
/// 持有 `KernelView`（不含 task 集合）与注册代际：避免 task→Kernel
/// 循环引用；退出收敛仅在代际匹配时生效，防旧任务误改同 id 新条目。
pub struct ExtensionRuntime {
    ext: Box<dyn Extension>,
    policy: SupervisionPolicy,
    view: KernelView,
    gen: u64,
    /// 单条消息处理时限（挂起治理）；`None` 表示不限时，仅 Panic 熔断
    handle_timeout: Option<Duration>,
    /// 重启计数 — 与路由条目共享（本运行时唯一写入方，快照只读）
    restarts: Arc<AtomicU32>,
}

/// 内层循环结果
enum InnerOutcome {
    /// 所有通道关闭（unregister / Sender drop），队列已排空
    NormalExit,
    /// 收到停机信号且积压已排空（含封路后的终末 drain）
    Shutdown,
    /// `handle` Panic 或超时被熔断
    Crashed,
}

/// 单条消息的治理结果
enum HandleOutcome {
    /// handle 正常返回（业务错误由扩展语义自负，内核视角即完成）
    Completed,
    /// Panic 被 `catch_unwind` 熔断
    Panicked,
    /// 超过 `handle_timeout` 被切断（挂起治理）
    TimedOut,
}

impl HandleOutcome {
    fn is_failure(&self) -> bool {
        !matches!(self, HandleOutcome::Completed)
    }
}

/// 内层循环依赖集 — 收敛传参，新增治理参数无需扩散签名
struct LoopDeps<'a> {
    ext: &'a dyn Extension,
    view: &'a KernelView,
    router: &'a Router,
    /// 注册代际（终态收敛的状态写入保护）
    gen: u64,
    handle_timeout: Option<Duration>,
}

impl ExtensionRuntime {
    /// 由 Kernel::register_with 调用（内部构造，KernelView 为 crate 私有）
    pub(crate) fn new(
        ext: Box<dyn Extension>,
        policy: SupervisionPolicy,
        view: KernelView,
        gen: u64,
        handle_timeout: Option<Duration>,
        restarts: Arc<AtomicU32>,
    ) -> Self {
        Self {
            ext,
            policy,
            view,
            gen,
            handle_timeout,
            restarts,
        }
    }

    /// 监督循环：崩溃（Panic / 超时）后按策略重启；停机 / 通道关闭 /
    /// 熔断超限时转储积压并终态退出。
    pub async fn run_supervised(
        self,
        mut rx: PriorityReceiver,
        router: Router,
        shutdown_rx: ShutdownRx,
    ) {
        let id = self.ext.id();
        let mut window_start = Instant::now();

        loop {
            let deps = LoopDeps {
                ext: self.ext.as_ref(),
                view: &self.view,
                router: &router,
                gen: self.gen,
                handle_timeout: self.handle_timeout,
            };
            match Self::run_inner_loop(deps, &mut rx, &shutdown_rx).await {
                // 自然退出（通道关闭 / 停机排空）：状态收敛已在内层完成
                InnerOutcome::NormalExit | InnerOutcome::Shutdown => return,
                InnerOutcome::Crashed => {
                    router.set_state(id, ExtensionState::Crashed);
                    // 停机已触发：自愈无意义（重启后只会再次 drain 退出），
                    // 直接转储积压终态退出，缩短停机等待
                    if shutdown_rx.is_triggered() {
                        Self::drain_backlog_to_dlq(&self.view, &mut rx, id).await;
                        return;
                    }
                    let terminal = match &self.policy {
                        SupervisionPolicy::Transient => true,
                        SupervisionPolicy::OneForOne {
                            max_restarts,
                            window_secs,
                        } => {
                            if window_start.elapsed().as_secs() >= *window_secs {
                                self.restarts.store(0, Ordering::Relaxed);
                                window_start = Instant::now();
                            }
                            let restarts =
                                self.restarts.fetch_add(1, Ordering::Relaxed) + 1;
                            if restarts > *max_restarts {
                                router.set_state(id, ExtensionState::Stopped);
                                true
                            } else {
                                // 指数退避：100ms × 2^restarts（上限 102.4s）
                                let base = 2u32.pow(restarts.min(10)) as u64;
                                tokio::time::sleep(Duration::from_millis(base * 100)).await;
                                router.set_state(id, ExtensionState::Running);
                                false
                            }
                        }
                    };
                    if terminal {
                        // 终态退出：未消费积压全部转储 DLQ，绝不静默丢弃
                        Self::drain_backlog_to_dlq(&self.view, &mut rx, id).await;
                        return;
                    }
                }
            }
        }
    }

    /// 内层消息循环：正常消费 / 停机 drain（含封路终末轮）/ 通道关闭退出
    async fn run_inner_loop(
        deps: LoopDeps<'_>,
        rx: &mut PriorityReceiver,
        shutdown_rx: &ShutdownRx,
    ) -> InnerOutcome {
        loop {
            tokio::select! {
                _ = shutdown_rx.wait() => {
                    // drain 模式：排空已入队积压
                    if !Self::drain_handled(&deps, rx).await {
                        return InnerOutcome::Crashed;
                    }
                    // 封路：状态收敛 Stopped 与 router.dispatch 的状态检查
                    // 在同一路由条目锁内互斥 —— 收敛完成后，迟到投递必然
                    // 走 TargetUnreachable → DLQ，不可能再入队
                    Self::settle_stopped(&deps);
                    // 终末 drain：消费「封路前最后入队」的尾部消息
                    if !Self::drain_handled(&deps, rx).await {
                        return InnerOutcome::Crashed;
                    }
                    return InnerOutcome::Shutdown;
                }
                res = rx.recv() => match res {
                    Some(mctx) => {
                        if Self::handle_observed(&deps, mctx).await.is_failure() {
                            return InnerOutcome::Crashed;
                        }
                    }
                    // 所有 Sender drop → 通道关闭 → 正常退出
                    None => {
                        Self::settle_stopped(&deps);
                        return InnerOutcome::NormalExit;
                    }
                },
            }
        }
    }

    /// 排空当前积压并正常处理；false 表示处理中崩溃（Panic / 超时）
    async fn drain_handled(deps: &LoopDeps<'_>, rx: &mut PriorityReceiver) -> bool {
        while let Ok(mctx) = rx.try_recv() {
            if Self::handle_observed(deps, mctx).await.is_failure() {
                return false;
            }
        }
        true
    }

    /// 自然退出的状态收敛：仅当代际匹配且仍为 Running 时置 Stopped
    ///（防旧任务误改同 id 新注册条目的状态）
    fn settle_stopped(deps: &LoopDeps<'_>) {
        let id = deps.ext.id();
        if deps.router.matches_generation(&id, deps.gen)
            && deps.router.get_state(&id) == Some(ExtensionState::Running)
        {
            deps.router.set_state(id, ExtensionState::Stopped);
        }
    }

    /// 终态退出的积压转储：未消费消息逐条 ACK 后写入 DLQ
    ///
    /// 消息已无处理机会：先 ACK（防 WAL 无界滞留 / 下次启动重复投递），
    /// 再连同 `ExtensionStopped` 原因落死信供审计 / 重放；转储时 drop
    /// 消息内嵌的回信通道，等待中的 invoke 立即收到 `TargetUnreachable`。
    async fn drain_backlog_to_dlq(
        view: &KernelView,
        rx: &mut PriorityReceiver,
        id: CapabilityId,
    ) {
        let mut drained = 0u64;
        while let Ok(mctx) = rx.try_recv() {
            if let (Some(wal), Some(wal_id)) = (&view.wal, mctx.wal_id) {
                wal.ack(wal_id).await;
            }
            view.dlq
                .sink(mctx.envelope, KernelError::ExtensionStopped)
                .await;
            drained += 1;
        }
        if drained > 0 {
            counter!("referee_backlog_drained_total", "ext_id" => id.to_string()).increment(drained);
        }
    }

    /// Panic 捕获边界 + 挂起切断 + 观测埋点 + WAL ACK
    ///
    /// 顺序保证：先 `catch_unwind` 再 `instrument` —— Panic 被捕获后 Future
    /// 正常返回，`instrument` 保证 Span 妥善关闭；回写处理延迟直方图
    /// （`outcome=ok` / `outcome=panic` / `outcome=timeout`）。
    ///
    /// 超时切断直接丢弃进行中的 handle Future：被切断的消息不重试
    ///（与 Panic 消费后同等取舍），已产生的部分副作用由监督重启策略
    /// 覆盖；回信通道随 Future drop，invoke 端立即收到 `TargetUnreachable`。
    async fn handle_observed(deps: &LoopDeps<'_>, mut mctx: MessageContext) -> HandleOutcome {
        // 拆包：提取 WAL ID 与回信通道，组装受限上下文
        let wal_id = mctx.wal_id;
        let reply = mctx.take_reply();
        let env = mctx.envelope;
        let ctx = KernelContext::new(deps.ext.id(), deps.view.clone(), reply);

        let span = info_span!(
            "extension_handle",
            trace_id = %env.trace_id,
            correlation_id = %env.correlation_id,
            ext_id = %deps.ext.id(),
        );
        let start = Instant::now();
        let executed = AssertUnwindSafe(deps.ext.handle(ctx, env))
            .catch_unwind()
            .instrument(span);
        let outcome = match deps.handle_timeout {
            Some(limit) => match tokio::time::timeout(limit, executed).await {
                Ok(result) => Self::classify(result),
                Err(_elapsed) => HandleOutcome::TimedOut,
            },
            None => Self::classify(executed.await),
        };
        let elapsed = start.elapsed().as_secs_f64();
        let ext_id = deps.ext.id().to_string();
        let status = match outcome {
            HandleOutcome::Completed => "ok",
            HandleOutcome::Panicked => "panic",
            HandleOutcome::TimedOut => "timeout",
        };
        histogram!(
            "referee_handle_duration_seconds",
            "ext_id" => ext_id.clone(),
            "outcome" => status
        )
        .record(elapsed);
        match outcome {
            HandleOutcome::Completed => {
                // 处理成功 → WAL ACK：确认后进程崩溃重放不会重复投递
                Self::ack_consumed(deps.view, wal_id).await;
                outcome
            }
            HandleOutcome::Panicked => {
                Self::ack_consumed(deps.view, wal_id).await;
                counter!("referee_extension_panics_total", "ext_id" => ext_id).increment(1);
                outcome
            }
            HandleOutcome::TimedOut => {
                Self::ack_consumed(deps.view, wal_id).await;
                counter!("referee_handle_timeouts_total", "ext_id" => ext_id).increment(1);
                outcome
            }
        }
    }

    /// 消费尝试已终止（Panic / 超时切断）后的 WAL ACK：
    /// 防 WAL 无界滞留 / 存活期手动恢复重复投递（进程级崩溃时不会
    /// 执行到此处，WAL 记录保留供下次启动重放）
    async fn ack_consumed(view: &KernelView, wal_id: Option<Uuid>) {
        if let (Some(wal), Some(id)) = (&view.wal, wal_id) {
            wal.ack(id).await;
        }
    }

    /// catch_unwind 结果归类：正常返回（含业务 Err）即 Completed
    fn classify(result: Result<KernelResult<()>, Box<dyn std::any::Any + Send>>) -> HandleOutcome {
        match result {
            Ok(_) => HandleOutcome::Completed,
            Err(_) => HandleOutcome::Panicked,
        }
    }
}

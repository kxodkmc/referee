//! 监督与生命周期闭环 — 扩展运行循环 + 崩溃重启策略
//!
//! 两层结构：外层 Supervisor 持有接收端，负责 Panic 后的重启决策并
//! 保留通道（积压消息不丢失）；内层为实际 `handle` 执行，被
//! `catch_unwind` 隔离。停机信号到达后进入 drain 模式，处理完积压退出。
//!
//! 注入 `Kernel` 与 `WalSink`：每次 `handle` 组装受限 `KernelContext`
//! （仅 emit / reply / spawn_blocking，无 invoke），处理成功后 ACK WAL，
//! 为进程级崩溃提供消息重放兜底。

use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures::FutureExt;
use metrics::{counter, histogram};
use tracing::{info_span, Instrument};

use crate::extension::{Extension, KernelContext, MessageContext};
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
}

enum InnerOutcome {
    /// 所有通道关闭（unregister / Sender drop）
    NormalExit,
    /// 收到停机信号且积压已排空
    Shutdown,
    /// `handle` Panic 被熔断
    Crashed,
}

impl ExtensionRuntime {
    /// 由 Kernel::register 调用（内部构造，KernelView 为 crate 私有）
    pub(crate) fn new(
        ext: Box<dyn Extension>,
        policy: SupervisionPolicy,
        view: KernelView,
        gen: u64,
    ) -> Self {
        Self {
            ext,
            policy,
            view,
            gen,
        }
    }

    /// 监督循环：崩溃后按策略重启；停机 / 通道关闭 / 熔断超限时退出
    pub async fn run_supervised(
        self,
        mut rx: PriorityReceiver,
        router: Router,
        shutdown_rx: ShutdownRx,
    ) {
        let id = self.ext.id();
        let mut restarts = 0u32;
        let mut window_start = Instant::now();

        loop {
            match Self::run_inner_loop(self.ext.as_ref(), &self.view, &mut rx, &shutdown_rx).await {
                InnerOutcome::NormalExit | InnerOutcome::Shutdown => {
                    // 自然退出时收敛状态；仅当代际匹配（同一次注册）且仍为 Running
                    // 才收敛，避免旧任务误改同 id 新注册条目的状态。
                    if router.matches_generation(&id, self.gen)
                        && router.get_state(&id) == Some(ExtensionState::Running)
                    {
                        router.set_state(id, ExtensionState::Stopped);
                    }
                    return;
                }
                InnerOutcome::Crashed => {
                    router.set_state(id, ExtensionState::Crashed);
                    let delay = match &self.policy {
                        SupervisionPolicy::Transient => return,
                        SupervisionPolicy::OneForOne {
                            max_restarts,
                            window_secs,
                        } => {
                            if window_start.elapsed().as_secs() >= *window_secs {
                                restarts = 0;
                                window_start = Instant::now();
                            }
                            restarts += 1;
                            if restarts > *max_restarts {
                                router.set_state(id, ExtensionState::Stopped);
                                return;
                            }
                            // 指数退避：100ms × 2^restarts（上限 102.4s）
                            let base = 2u32.pow(restarts.min(10)) as u64;
                            Duration::from_millis(base * 100)
                        }
                    };
                    tokio::time::sleep(delay).await;
                    router.set_state(id, ExtensionState::Running);
                }
            }
        }
    }

    /// 内层消息循环：正常消费 / 停机 drain / 通道关闭退出
    async fn run_inner_loop(
        ext: &dyn Extension,
        view: &KernelView,
        rx: &mut PriorityReceiver,
        shutdown_rx: &ShutdownRx,
    ) -> InnerOutcome {
        loop {
            tokio::select! {
                _ = shutdown_rx.wait() => {
                    // drain 模式：排空已入队积压后退出
                    while let Ok(mctx) = rx.try_recv() {
                        if !Self::handle_observed(ext, view, mctx).await {
                            return InnerOutcome::Crashed;
                        }
                    }
                    return InnerOutcome::Shutdown;
                }
                res = rx.recv() => match res {
                    Some(mctx) => {
                        if !Self::handle_observed(ext, view, mctx).await {
                            return InnerOutcome::Crashed;
                        }
                    }
                    // 所有 Sender drop → 通道关闭 → 正常退出
                    None => return InnerOutcome::NormalExit,
                },
            }
        }
    }

    /// Panic 捕获边界 + 观测埋点 + WAL ACK：handle 异常只熔断自身，绝不外泄
    ///
    /// 顺序保证：先 `catch_unwind` 再 `instrument` —— Panic 被捕获后 Future
    /// 正常返回，`instrument` 保证 Span 妥善关闭；随后回写处理延迟直方图
    /// （`outcome=ok` / `outcome=panic`）与 Panic 计数器。
    /// 注意：`catch_unwind` 只覆盖 handle Future 的 poll 阶段，且
    /// `panic=abort` 编译配置下不生效（本项目默认 unwind）。
    async fn handle_observed(
        ext: &dyn Extension,
        view: &KernelView,
        mut mctx: MessageContext,
    ) -> bool {
        // 拆包：提取 WAL ID 与回信通道，组装受限上下文
        let wal_id = mctx.wal_id;
        let reply = mctx.take_reply();
        let env = mctx.envelope;
        let ctx = KernelContext::new(ext.id(), view.clone(), reply);

        let span = info_span!(
            "extension_handle",
            trace_id = %env.trace_id,
            correlation_id = %env.correlation_id,
            ext_id = %ext.id(),
        );
        let start = Instant::now();
        let outcome = AssertUnwindSafe(ext.handle(ctx, env))
            .catch_unwind()
            .instrument(span)
            .await;
        let elapsed = start.elapsed().as_secs_f64();
        let status = if outcome.is_ok() { "ok" } else { "panic" };
        histogram!(
            "referee_handle_duration_seconds",
            "ext_id" => ext.id().to_string(),
            "outcome" => status
        )
        .record(elapsed);
        if outcome.is_ok() {
            // 处理成功 → WAL ACK：确认后进程崩溃重放不会重复投递
            if let (Some(wal), Some(id)) = (&view.wal, wal_id) {
                wal.ack(id).await;
            }
            true
        } else {
            // Panic 已被消费尝试：ACK 防 WAL 无界滞留 / 存活期手动恢复重复投递
            // （进程级崩溃时本行不执行，WAL 记录保留供下次启动重放）
            if let (Some(wal), Some(id)) = (&view.wal, wal_id) {
                wal.ack(id).await;
            }
            counter!(
                "referee_extension_panics_total",
                "ext_id" => ext.id().to_string()
            )
            .increment(1);
            false
        }
    }
}

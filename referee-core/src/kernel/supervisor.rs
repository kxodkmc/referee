//! 监督与生命周期闭环 — 扩展运行循环 + 崩溃重启策略
//!
//! 两层结构：外层 Supervisor 持有接收端，负责 Panic 后的重启决策并
//! 保留通道（积压消息不丢失）；内层为实际 `handle` 执行，被
//! `catch_unwind` 隔离。停机信号到达后进入 drain 模式，处理完积压退出。

use std::panic::AssertUnwindSafe;
use std::time::{Duration, Instant};

use futures::FutureExt;

use crate::extension::{Extension, MessageContext};
use crate::kernel::monitor::{ExtensionState, Monitor};
use crate::kernel::priority::PriorityReceiver;
use crate::kernel::shutdown::ShutdownRx;

/// 监督策略
#[derive(Debug, Clone)]
pub enum SupervisionPolicy {
    /// 崩溃即熔断，不重启（Phase 3 行为）
    Transient,
    /// 单点重启：窗口内最多重启 `max_restarts` 次，指数退避
    OneForOne { max_restarts: u32, window_secs: u64 },
}

/// 扩展运行时 — 由 Kernel 在注册时派生为独立 task
pub struct ExtensionRuntime {
    ext: Box<dyn Extension>,
    policy: SupervisionPolicy,
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
    pub fn new(ext: Box<dyn Extension>, policy: SupervisionPolicy) -> Self {
        Self { ext, policy }
    }

    /// 监督循环：崩溃后按策略重启；停机 / 通道关闭 / 熔断超限时退出
    pub async fn run_supervised(
        self,
        mut rx: PriorityReceiver,
        monitor: Monitor,
        shutdown_rx: ShutdownRx,
    ) {
        let id = self.ext.id();
        let mut restarts = 0u32;
        let mut window_start = Instant::now();

        loop {
            match Self::run_inner_loop(self.ext.as_ref(), &mut rx, &shutdown_rx).await {
                InnerOutcome::NormalExit | InnerOutcome::Shutdown => {
                    // 自然退出时收敛状态；Panic 已在上方标记 Crashed，此处不覆盖
                    if monitor.get_state(&id) == Some(ExtensionState::Running) {
                        monitor.set_state(id, ExtensionState::Stopped);
                    }
                    return;
                }
                InnerOutcome::Crashed => {
                    monitor.set_state(id, ExtensionState::Crashed);
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
                                monitor.set_state(id, ExtensionState::Stopped);
                                return;
                            }
                            // 指数退避：100ms × 2^restarts（上限 102.4s）
                            let base = 2u32.pow(restarts.min(10)) as u64;
                            Duration::from_millis(base * 100)
                        }
                    };
                    tokio::time::sleep(delay).await;
                    monitor.set_state(id, ExtensionState::Running);
                }
            }
        }
    }

    /// 内层消息循环：正常消费 / 停机 drain / 通道关闭退出
    async fn run_inner_loop(
        ext: &dyn Extension,
        rx: &mut PriorityReceiver,
        shutdown_rx: &ShutdownRx,
    ) -> InnerOutcome {
        loop {
            tokio::select! {
                _ = shutdown_rx.wait() => {
                    // drain 模式：排空已入队积压后退出
                    while let Ok(ctx) = rx.try_recv() {
                        if !Self::handle_guarded(ext, ctx).await {
                            return InnerOutcome::Crashed;
                        }
                    }
                    return InnerOutcome::Shutdown;
                }
                res = rx.recv() => match res {
                    Some(ctx) => {
                        if !Self::handle_guarded(ext, ctx).await {
                            return InnerOutcome::Crashed;
                        }
                    }
                    // 所有 Sender drop → 通道关闭 → 正常退出
                    None => return InnerOutcome::NormalExit,
                },
            }
        }
    }

    /// Panic 捕获边界：handle 异常只熔断自身，绝不外泄
    async fn handle_guarded(ext: &dyn Extension, ctx: MessageContext) -> bool {
        AssertUnwindSafe(ext.handle(ctx))
            .catch_unwind()
            .await
            .is_ok()
    }
}

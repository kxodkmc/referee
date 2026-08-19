//! 内核聚合逻辑 — Router + Monitor + DLQ + WAL + 监督运行时 + 停机协调

pub mod monitor;
pub mod priority;
pub mod router;
pub mod shutdown;
pub mod supervisor;
pub mod wal;

pub use monitor::{GlobalState, Monitor};
pub use router::{ExtensionInfo, ExtensionState, Router};
pub use shutdown::{ShutdownRx, ShutdownTx};
pub use supervisor::SupervisionPolicy;
pub use wal::{InMemoryWal, WalSink};

use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;

use metrics::counter;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tracing::{info_span, Instrument};

use crate::common::{Envelope, KernelError, KernelResult};
use crate::extension::dlq::{DlqSink, InMemoryDlq};
use crate::extension::{CapabilityId, Extension, MessageContext};
use crate::kernel::priority::PrioritySender;
use crate::kernel::shutdown::shutdown_channel;
use crate::kernel::supervisor::ExtensionRuntime;

/// 内核路由视图 — 仅含路由 / 治理 / 死信 / WAL，**不含 task 集合**
///
/// 注入扩展运行时与 `KernelContext`：打破「task → Kernel → task 集合 → task」
/// 的循环引用，保证死循环扩展在 Kernel 释放后也能被 JoinSet drop 强制中止。
#[derive(Clone)]
pub(crate) struct KernelView {
    router: Router,
    monitor: Monitor,
    dlq: Arc<dyn DlqSink>,
    wal: Option<Arc<dyn WalSink>>,
}

/// 微内核入口 — 应用侧唯一交互对象
#[derive(Clone)]
pub struct Kernel {
    view: KernelView,
    /// 全部扩展运行时 task 集合，用于优雅停机时统一等待 / 中止
    tasks: Arc<Mutex<JoinSet<()>>>,
    shutdown_tx: ShutdownTx,
}

/// 扩展注册配置 — 集中收敛注册参数，按需拓展不破坏既有签名
#[derive(Debug, Clone)]
pub struct RegisterOptions {
    /// 每个优先级桶的缓冲上限（背压硬约束）
    pub queue_size: usize,
    /// 崩溃 / 超时后的监督策略（重启决策）
    pub policy: SupervisionPolicy,
    /// 单条消息处理时限（挂起治理）：超时切断视为一次崩溃走监督策略，
    /// 被切断的消息不重试。`None` 表示不限时（仅 Panic 熔断）。
    pub handle_timeout: Option<Duration>,
}

impl RegisterOptions {
    pub fn new(queue_size: usize, policy: SupervisionPolicy) -> Self {
        Self {
            queue_size,
            policy,
            handle_timeout: None,
        }
    }

    /// 启用挂起治理：设定单条消息处理时限
    pub fn with_handle_timeout(mut self, timeout: Duration) -> Self {
        self.handle_timeout = Some(timeout);
        self
    }
}

/// 扩展注册句柄 — 持有者可移除并停机该扩展
#[derive(Clone)]
pub struct ExtensionHandle {
    id: CapabilityId,
    kernel: Kernel,
}

impl ExtensionHandle {
    /// 句柄对应的扩展 id
    pub fn id(&self) -> CapabilityId {
        self.id
    }

    /// 移除扩展：注销路由条目 → Sender drop → 通道关闭 → 监督循环退出并调用
    /// `Extension::shutdown()` 释放资源。移除后对其的 `emit` / `invoke` 返回
    /// `TargetUnreachable`。
    pub async fn remove(&self) -> KernelResult<()> {
        self.kernel.unregister(self.id).await
    }
}

impl Kernel {
    /// 默认实现：内存环形死信队列（容量 1024），无 WAL
    pub fn new() -> Self {
        Self::with_dlq(Arc::new(InMemoryDlq::default()))
    }

    /// 注入自定义死信队列（持久化 / 观测 / 测试共享），无 WAL
    pub fn with_dlq(dlq: Arc<dyn DlqSink>) -> Self {
        Self::with_dlq_wal_opt(dlq, None)
    }

    /// 注入 WAL（进程级崩溃兜底），死信队列用默认内存实现
    pub fn with_wal(wal: Arc<dyn WalSink>) -> Self {
        Self::with_dlq_wal_opt(Arc::new(InMemoryDlq::default()), Some(wal))
    }

    /// 同时注入自定义死信队列与 WAL
    pub fn with_dlq_wal(dlq: Arc<dyn DlqSink>, wal: Arc<dyn WalSink>) -> Self {
        Self::with_dlq_wal_opt(dlq, Some(wal))
    }

    fn with_dlq_wal_opt(dlq: Arc<dyn DlqSink>, wal: Option<Arc<dyn WalSink>>) -> Self {
        let (shutdown_tx, _) = shutdown_channel();
        Self {
            view: KernelView {
                router: Router::new(),
                monitor: Monitor::new(),
                dlq,
                wal,
            },
            tasks: Arc::new(Mutex::new(JoinSet::new())),
            shutdown_tx,
        }
    }

    /// 注册扩展（便捷入口）：默认不启用 handle 超时（仅 Panic 熔断）
    ///
    /// `queue_size` 为每个优先级桶的缓冲上限；`policy` 决定崩溃后的重启策略。
    pub async fn register(
        &self,
        ext: Box<dyn Extension>,
        queue_size: usize,
        policy: SupervisionPolicy,
    ) -> KernelResult<()> {
        self.register_with(ext, RegisterOptions::new(queue_size, policy))
            .await
    }

    /// 注册扩展（完整配置）：支持挂起治理（`handle_timeout`）等参数
    ///
    /// 优先级有界通道 → 写入路由表 → 派生监督运行时。运行时注入路由视图
    /// （受限上下文）与 WAL（处理成功 ACK），并携带注册代际（防旧任务
    /// 退出时误改同 id 新条目）。
    pub async fn register_with(
        &self,
        ext: Box<dyn Extension>,
        opts: RegisterOptions,
    ) -> KernelResult<()> {
        let id = ext.id();
        let (tx, rx) = PrioritySender::new(opts.queue_size, id);
        let gen = self.view.router.next_generation();
        // 重启计数：路由条目（快照可读）与监督运行时（崩溃后写入）共享
        let restarts = Arc::new(AtomicU32::new(0));
        // 状态与路由同条目原子写入（register 完成即对 dispatch 可见）
        self.view.router.insert(
            id,
            tx,
            ExtensionState::Running,
            gen,
            restarts.clone(),
        );
        let runtime = ExtensionRuntime::new(
            ext,
            opts.policy,
            self.view.clone(),
            gen,
            opts.handle_timeout,
            restarts,
        );
        self.tasks.lock().spawn(runtime.run_supervised(
            rx,
            self.view.router.clone(),
            self.shutdown_tx.subscribe(),
        ));
        Ok(())
    }

    /// 注册扩展并返回可回收句柄（`register` 的句柄版本）
    pub async fn register_handle(
        &self,
        ext: Box<dyn Extension>,
        queue_size: usize,
        policy: SupervisionPolicy,
    ) -> KernelResult<ExtensionHandle> {
        self.register_with_handle(ext, RegisterOptions::new(queue_size, policy))
            .await
    }

    /// 注册扩展（完整配置）并返回可回收句柄（`register_with` 的句柄版本）
    pub async fn register_with_handle(
        &self,
        ext: Box<dyn Extension>,
        opts: RegisterOptions,
    ) -> KernelResult<ExtensionHandle> {
        let id = ext.id();
        self.register_with(ext, opts).await?;
        Ok(ExtensionHandle { id, kernel: self.clone() })
    }

    /// 扩展自省快照 — 路由表全量条目（id / 状态 / 队列深度 / 重启计数）
    ///
    /// 只读视图，供运维展示与监控；跨条目不保证同一瞬时（见 `Router::snapshot`）
    pub fn extensions(&self) -> Vec<ExtensionInfo> {
        self.view.router.snapshot()
    }

    /// 注销扩展：移除路由（含状态）→ Sender drop → 通道关闭 → 监督循环自然退出
    pub async fn unregister(&self, id: CapabilityId) -> KernelResult<()> {
        if !self.view.router.contains(&id) {
            return Err(KernelError::TargetUnreachable);
        }
        self.view.router.remove(&id);
        Ok(())
    }

    /// 优雅停机：广播停机信号 → 拒绝新消息 → 等待扩展排空积压
    ///
    /// 所有扩展运行循环处理完已入队消息后自然退出；`timeout_ms` 超时
    /// 则强制中止剩余任务（尽力而为，不无限等待）
    pub async fn shutdown_graceful(&self, timeout_ms: u64) -> KernelResult<()> {
        self.shutdown_tx.trigger();
        self.view.monitor.set_global_state(GlobalState::Stopping);
        let mut tasks = std::mem::take(&mut *self.tasks.lock());
        tokio::select! {
            _ = async { while tasks.join_next().await.is_some() {} } => {}
            _ = tokio::time::sleep(Duration::from_millis(timeout_ms)) => {
                tasks.abort_all();
            }
        }
        Ok(())
    }

    /// 即发即弃 — 不等待响应
    pub async fn emit(&self, target: CapabilityId, envelope: Envelope) -> KernelResult<()> {
        self.dispatch(target, MessageContext::new(envelope)).await
    }

    /// Request-Response 原语 — 同步阻塞等待响应
    ///
    /// 1. 组装带 oneshot 回信通道的 Context 并分发（拦截 / 背压立即返回）
    /// 2. `timeout` 限定等待窗口，超时则切断（返回 `Timeout`）
    pub async fn invoke(
        &self,
        target: CapabilityId,
        envelope: Envelope,
        timeout_ms: u64,
    ) -> KernelResult<Envelope> {
        let (tx, rx) = oneshot::channel();
        let ctx = MessageContext::with_reply(envelope, tx);
        self.dispatch(target, ctx).await?;
        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            // 扩展崩溃或被注销，Sender drop
            Ok(Err(_)) => Err(KernelError::TargetUnreachable),
            Err(_) => Err(KernelError::Timeout),
        }
    }

    /// 统一分发入口（委托路由视图）
    async fn dispatch(&self, target: CapabilityId, ctx: MessageContext) -> KernelResult<()> {
        self.view.dispatch(target, ctx).await
    }

    /// 恢复通道：从 WAL 重放未确认消息，直接注入路由表
    ///
    /// 必须在扩展**注册完成后**调用（恢复消息需要路由目标已存在）。
    /// 关键：直接调用 `router.dispatch`，**绕过 WAL 追加**，防止恢复消息
    /// 被重复落盘形成死循环；恢复消息携带原 WAL ID，处理成功后正常 ACK。
    pub async fn start_with_recovery(&self) -> KernelResult<()> {
        let view = &self.view;
        if let Some(wal) = &view.wal {
            let pending = wal.recover().await;
            for (id, env) in pending {
                let target = CapabilityId::from_uuid(env.target);
                let mut ctx = MessageContext::from_recovered(env.clone());
                ctx.wal_id = Some(id);
                if let Err((err, _)) = view.router.dispatch(&target, ctx) {
                    // 恢复期投递失败：确认记录（避免下次启动重复重放）并进死信
                    wal.ack(id).await;
                    view.dlq.sink(env, err).await;
                }
            }
        }
        Ok(())
    }
}

impl KernelView {
    /// 统一分发路径：停机拦截 → 状态预检 → WAL 落盘 → 原子投递
    ///
    /// 所有拦截点都将被拒的 Envelope 写入死信队列，供审计 / 重放。
    /// 入口建立 `kernel_dispatch` Span（携带 trace_id），路由阶段日志与
    /// 错误自动归属该上下文；各出口按结果回写 `referee_dispatch_total` 计数。
    pub(crate) async fn dispatch(
        &self,
        target: CapabilityId,
        ctx: MessageContext,
    ) -> KernelResult<()> {
        let span = info_span!(
            "kernel_dispatch",
            trace_id = %ctx.envelope.trace_id,
            correlation_id = %ctx.envelope.correlation_id,
            target = %target,
        );
        async move {
            // 1. 全局停机拦截
            if self.monitor.is_stopping() {
                self.dlq
                    .sink(ctx.envelope, KernelError::SystemShuttingDown)
                    .await;
                counter!(
                    "referee_dispatch_total",
                    "ext_id" => target.to_string(),
                    "result" => "shutting_down"
                )
                .increment(1);
                return Err(KernelError::SystemShuttingDown);
            }
            // 2. 状态预检（WAL 决策依据）：非 Running 直接拒绝，不为注定失败投递落盘
            match self.router.get_state(&target) {
                Some(ExtensionState::Crashed) => {
                    self.dlq
                        .sink(ctx.envelope, KernelError::ExtensionCrashed)
                        .await;
                    counter!(
                        "referee_dispatch_total",
                        "ext_id" => target.to_string(),
                        "result" => "crashed"
                    )
                    .increment(1);
                    return Err(KernelError::ExtensionCrashed);
                }
                Some(ExtensionState::Stopped) | None => {
                    self.dlq
                        .sink(ctx.envelope, KernelError::TargetUnreachable)
                        .await;
                    counter!(
                        "referee_dispatch_total",
                        "ext_id" => target.to_string(),
                        "result" => "unreachable"
                    )
                    .increment(1);
                    return Err(KernelError::TargetUnreachable);
                }
                _ => {}
            }
            // 3. 填充路由目标（WAL 恢复路由依据）→ WAL 落盘（先持久化再入队）
            let mut ctx = ctx;
            ctx.envelope.target = target.as_uuid();
            let wal_id = if let Some(wal) = &self.wal {
                match wal.append(&ctx.envelope).await {
                    Ok(id) => Some(id),
                    Err(e) => {
                        self.dlq.sink(ctx.envelope, KernelError::Storage).await;
                        counter!(
                            "referee_dispatch_total",
                            "ext_id" => target.to_string(),
                            "result" => "storage"
                        )
                        .increment(1);
                        return Err(e);
                    }
                }
            } else {
                None
            };
            ctx.wal_id = wal_id;
            // 4. 原子投递（状态 + 背压同一锁内；状态变化以 router 判定为准）
            match self.router.dispatch(&target, ctx) {
                Ok(()) => {
                    counter!(
                        "referee_dispatch_total",
                        "ext_id" => target.to_string(),
                        "result" => "ok"
                    )
                    .increment(1);
                    Ok(())
                }
                Err((err, env)) => {
                    // 投递失败：消息未入队，无需持久化，撤销 WAL 记录
                    if let (Some(wal), Some(id)) = (&self.wal, wal_id) {
                        wal.ack(id).await;
                    }
                    self.dlq.sink(env, err).await;
                    let result = match err {
                        KernelError::ResourceExhausted => "full",
                        KernelError::TargetUnreachable => "closed",
                        _ => "other",
                    };
                    counter!(
                        "referee_dispatch_total",
                        "ext_id" => target.to_string(),
                        "result" => result
                    )
                    .increment(1);
                    Err(err)
                }
            }
        }
        .instrument(span)
        .await
    }
}

impl Default for Kernel {
    fn default() -> Self {
        Self::new()
    }
}

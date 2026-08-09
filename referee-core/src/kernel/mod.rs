//! 内核聚合逻辑 — Router + Monitor + DLQ + 监督运行时 + 停机协调

pub mod monitor;
pub mod priority;
pub mod router;
pub mod shutdown;
pub mod supervisor;

pub use monitor::{ExtensionState, GlobalState, Monitor};
pub use router::Router;
pub use shutdown::{ShutdownRx, ShutdownTx};
pub use supervisor::SupervisionPolicy;

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::common::{Envelope, KernelError, KernelResult};
use crate::extension::dlq::{DlqSink, InMemoryDlq};
use crate::extension::{CapabilityId, Extension, MessageContext};
use crate::kernel::priority::PrioritySender;
use crate::kernel::shutdown::shutdown_channel;
use crate::kernel::supervisor::ExtensionRuntime;

/// 微内核入口 — 应用侧唯一交互对象
#[derive(Clone)]
pub struct Kernel {
    router: Router,
    monitor: Monitor,
    dlq: Arc<dyn DlqSink>,
    /// 全部扩展运行时 task 集合，用于优雅停机时统一等待 / 中止
    tasks: Arc<Mutex<JoinSet<()>>>,
    shutdown_tx: ShutdownTx,
}

impl Kernel {
    /// 默认实现：内存环形死信队列（容量 1024）
    pub fn new() -> Self {
        Self::with_dlq(Arc::new(InMemoryDlq::default()))
    }

    /// 注入自定义死信队列（持久化 / 观测 / 测试共享）
    pub fn with_dlq(dlq: Arc<dyn DlqSink>) -> Self {
        let (shutdown_tx, _) = shutdown_channel();
        Self {
            router: Router::new(),
            monitor: Monitor::new(),
            dlq,
            tasks: Arc::new(Mutex::new(JoinSet::new())),
            shutdown_tx,
        }
    }

    /// 注册扩展：优先级有界通道 → 写入路由表 → 派生监督运行时
    ///
    /// `queue_size` 为每个优先级桶的缓冲上限；`policy` 决定崩溃后的重启策略
    pub async fn register(
        &self,
        ext: Box<dyn Extension>,
        queue_size: usize,
        policy: SupervisionPolicy,
    ) -> KernelResult<()> {
        let id = ext.id();
        let (tx, rx) = PrioritySender::new(queue_size);
        // 先标记状态，再写入路由 — 缩短不可见窗口
        self.monitor.set_state(id, ExtensionState::Running);
        self.router.insert(id, tx);
        let runtime = ExtensionRuntime::new(ext, policy);
        self.tasks.lock().spawn(runtime.run_supervised(
            rx,
            self.monitor.clone(),
            self.shutdown_tx.subscribe(),
        ));
        Ok(())
    }

    /// 注销扩展：移除路由 → 标记 Stopped
    /// 路由移除后 Sender 计数归零 → 通道关闭 → 监督循环自然退出
    pub async fn unregister(&self, id: CapabilityId) -> KernelResult<()> {
        if !self.router.contains(&id) {
            return Err(KernelError::TargetUnreachable);
        }
        self.router.remove(&id);
        self.monitor.set_state(id, ExtensionState::Stopped);
        Ok(())
    }

    /// 优雅停机：广播停机信号 → 拒绝新消息 → 等待扩展排空积压
    ///
    /// 所有扩展运行循环处理完已入队消息后自然退出；`timeout_ms` 超时
    /// 则强制中止剩余任务（尽力而为，不无限等待）
    pub async fn shutdown_graceful(&self, timeout_ms: u64) -> KernelResult<()> {
        self.shutdown_tx.trigger();
        self.monitor.set_global_state(GlobalState::Stopping);
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

    /// 统一分发路径：全局停机拦截 → 扩展状态拦截 → 路由与背压
    ///
    /// 所有拦截点都将被拒的 Envelope 写入死信队列，供审计 / 重放
    async fn dispatch(&self, target: CapabilityId, ctx: MessageContext) -> KernelResult<()> {
        // 1. 全局停机拦截
        if self.monitor.is_stopping() {
            self.dlq
                .sink(ctx.envelope, KernelError::SystemShuttingDown)
                .await;
            return Err(KernelError::SystemShuttingDown);
        }
        // 2. 扩展状态拦截
        match self.monitor.get_state(&target) {
            Some(ExtensionState::Crashed) => {
                self.dlq
                    .sink(ctx.envelope, KernelError::ExtensionCrashed)
                    .await;
                return Err(KernelError::ExtensionCrashed);
            }
            Some(ExtensionState::Stopped) | None => {
                self.dlq
                    .sink(ctx.envelope, KernelError::TargetUnreachable)
                    .await;
                return Err(KernelError::TargetUnreachable);
            }
            _ => {}
        }
        // 3. 路由与背压拦截
        match self.router.dispatch(&target, ctx) {
            Ok(()) => Ok(()),
            Err((err, env)) => {
                self.dlq.sink(env, err).await;
                Err(err)
            }
        }
    }
}

impl Default for Kernel {
    fn default() -> Self {
        Self::new()
    }
}

//! Phase 4 测试：治理与生命周期闭环
//! 严格优先级路由 / OneForOne 自愈 / 窗口熔断 / 优雅停机 / 死信降级

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use referee_core::{
    CapabilityId, Envelope, Extension, ExtensionState, InMemoryDlq, Kernel, KernelContext,
    KernelError, KernelResult, SupervisionPolicy,
};

// ───────────────────────────────────────────────
// 测试夹具
// ───────────────────────────────────────────────

/// 记录消费顺序的扩展（模拟慢消费 1ms/条）
struct RecorderExtension {
    id: CapabilityId,
    order: Arc<Mutex<Vec<u8>>>,
}

#[async_trait]
impl Extension for RecorderExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        self.order.lock().push(env.priority);
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }
}

/// 首次调用 Panic，之后正常的扩展（用于自愈验证）
struct FlakyExtension {
    id: CapabilityId,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Extension for FlakyExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            panic!("simulated one-time crash");
        }
        Ok(())
    }
}

/// 持续 Panic 的扩展（用于熔断 / DLQ 验证）
struct PanicExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for PanicExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        panic!("simulated persistent crash");
    }
}

/// 慢消费扩展（10ms/条，用于停机 drain 验证）
struct SlowConsumerExtension {
    id: CapabilityId,
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl Extension for SlowConsumerExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }
}

// ───────────────────────────────────────────────
// 测试 1：严格优先级 — Low 满不阻塞 High，且 High 首先被消费
// ───────────────────────────────────────────────
#[tokio::test]
async fn priority_routing_high_first() {
    let kernel = Kernel::new();
    let order = Arc::new(Mutex::new(Vec::new()));
    let ext = RecorderExtension {
        id: CapabilityId::new(),
        order: order.clone(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 4, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // 塞满 Low 桶（priority = 200）触发背压
    let mut exhausted = false;
    for _ in 0..64 {
        let mut env = Envelope::new();
        env.priority = 200;
        match kernel.emit(ext_id, env).await {
            Ok(()) => {}
            Err(KernelError::ResourceExhausted) => {
                exhausted = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert!(exhausted, "Low bucket must eventually exhaust");

    // High 走独立桶，不应被满的 Low 桶阻塞
    let mut high = Envelope::new();
    high.priority = 0;
    kernel
        .emit(ext_id, high)
        .await
        .expect("High must bypass full Low bucket");

    // 等待消费完成，验证 High 插队（先于部分 Low 被消费）
    tokio::time::sleep(Duration::from_millis(200)).await;
    let order = order.lock();
    let first_high = order
        .iter()
        .position(|&p| p == 0)
        .expect("High must be consumed");
    let last_low = order
        .iter()
        .rposition(|&p| p == 200)
        .expect("Low must be consumed");
    assert!(
        first_high < last_low,
        "priority inversion: High consumed after all Low"
    );
}

// ───────────────────────────────────────────────
// 测试 2：OneForOne 自愈 — Panic 后退避重启，积压消息不丢失
// ───────────────────────────────────────────────
#[tokio::test]
async fn one_for_one_self_healing() {
    let kernel = Kernel::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let ext = FlakyExtension {
        id: CapabilityId::new(),
        calls: calls.clone(),
    };
    let ext_id = ext.id();
    let policy = SupervisionPolicy::OneForOne {
        max_restarts: 3,
        window_secs: 30,
    };
    kernel.register(Box::new(ext), 8, policy).await.unwrap();

    // 3 条消息全部入队：第 1 条触发 Panic，重启后继续消费剩余
    for _ in 0..3 {
        kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
    }
    // 退避（200ms）+ 消费时间
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "queued messages must survive restart"
    );

    // 自愈后路由恢复
    kernel
        .emit(ext_id, Envelope::new())
        .await
        .expect("emit after healing ok");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

// ───────────────────────────────────────────────
// 测试 3：窗口熔断 — 连续 Panic 超限后 Stopped，永久拒绝路由
// ───────────────────────────────────────────────
#[tokio::test]
async fn circuit_breaker_after_max_restarts() {
    let kernel = Kernel::new();
    let ext = PanicExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    let policy = SupervisionPolicy::OneForOne {
        max_restarts: 2,
        window_secs: 30,
    };
    kernel.register(Box::new(ext), 4, policy).await.unwrap();

    // 3 次触发 Panic：前 2 次退避重启，第 3 次超出上限 → 熔断
    // （每次等待覆盖对应退避：200ms / 400ms）
    for _ in 0..3 {
        kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    // 熔断后状态 Stopped → 路由拒绝
    let result = kernel.emit(ext_id, Envelope::new()).await;
    assert_eq!(result, Err(KernelError::TargetUnreachable));
}

// ───────────────────────────────────────────────
// 测试 4：优雅停机 — 积压排空后退出，停机期间新 emit 被拒
// ───────────────────────────────────────────────
#[tokio::test]
async fn graceful_shutdown_drains_queued() {
    let kernel = Kernel::new();
    let count = Arc::new(AtomicUsize::new(0));
    let ext = SlowConsumerExtension {
        id: CapabilityId::new(),
        count: count.clone(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 16, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // 积压 10 条
    for _ in 0..10 {
        kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
    }
    // 并发触发优雅停机
    let shutdown_kernel = kernel.clone();
    let shutdown = tokio::spawn(async move { shutdown_kernel.shutdown_graceful(1000).await });
    // 停机信号生效后，新 emit 返回 SystemShuttingDown
    tokio::time::sleep(Duration::from_millis(20)).await;
    let rejected = kernel.emit(ext_id, Envelope::new()).await;
    assert_eq!(rejected, Err(KernelError::SystemShuttingDown));
    shutdown
        .await
        .expect("shutdown task join")
        .expect("graceful shutdown ok");
    // 10 条积压消息全部处理完毕
    assert_eq!(count.load(Ordering::SeqCst), 10);
}

// ───────────────────────────────────────────────
// 测试 5：DLQ 降级 — Crashed 扩展的消息被拒并写入死信
// ───────────────────────────────────────────────
#[tokio::test]
async fn dlq_captures_rejected_envelope() {
    let dlq = Arc::new(InMemoryDlq::new(16));
    let kernel = Kernel::with_dlq(dlq.clone());
    let ext = PanicExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // 触发 Panic → Crashed
    let _ = kernel.emit(ext_id, Envelope::new()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // 向 Crashed 扩展发消息 → 拒绝并捕获对应 Envelope
    let env = Envelope::new();
    let context_id = env.context_id;
    let result = kernel.emit(ext_id, env).await;
    assert_eq!(result, Err(KernelError::ExtensionCrashed));
    let drained = dlq.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].0.context_id, context_id);
    assert_eq!(drained[0].1, KernelError::ExtensionCrashed);
}

// ───────────────────────────────────────────────
// 测试 6：自省快照 — 状态 / 队列深度 / 重启计数对外可见
// ───────────────────────────────────────────────
#[tokio::test]
async fn introspection_snapshot_reports_state_depth_and_restarts() {
    let kernel = Kernel::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let flaky = FlakyExtension {
        id: CapabilityId::new(),
        calls: calls.clone(),
    };
    let flaky_id = flaky.id();
    let panicky = PanicExtension {
        id: CapabilityId::new(),
    };
    let panicky_id = panicky.id();
    let slow = SlowConsumerExtension {
        id: CapabilityId::new(),
        count: Arc::new(AtomicUsize::new(0)),
    };
    let slow_id = slow.id();
    kernel
        .register(
            Box::new(flaky),
            8,
            SupervisionPolicy::OneForOne {
                max_restarts: 3,
                window_secs: 30,
            },
        )
        .await
        .unwrap();
    kernel
        .register(Box::new(panicky), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    kernel
        .register(Box::new(slow), 16, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // 初始快照：三者 Running、零重启
    let snap = kernel.extensions();
    assert_eq!(snap.len(), 3);
    for info in &snap {
        assert_eq!(info.state, ExtensionState::Running);
        assert_eq!(info.restarts, 0);
    }

    // 慢消费者积压可见：入队后立即快照，depth 必然大于 0
    for _ in 0..4 {
        kernel.emit(slow_id, Envelope::new()).await.expect("emit ok");
    }
    let info = kernel
        .extensions()
        .into_iter()
        .find(|i| i.id == slow_id)
        .expect("slow ext in snapshot");
    assert!(info.queue_depth > 0, "queued messages must be visible in depth");

    // 触发 flaky 一次 Panic（退避 200ms 后自愈）与 panicky 熔断
    kernel.emit(flaky_id, Envelope::new()).await.expect("emit ok");
    kernel.emit(panicky_id, Envelope::new()).await.expect("emit ok");
    tokio::time::sleep(Duration::from_millis(600)).await;

    let snap = kernel.extensions();
    let flaky_info = snap.iter().find(|i| i.id == flaky_id).expect("flaky info");
    let panicky_info = snap
        .iter()
        .find(|i| i.id == panicky_id)
        .expect("panicky info");
    // 自愈后恢复 Running 且重启计数为 1；熔断者标记 Crashed 且从不重启
    assert_eq!(flaky_info.state, ExtensionState::Running);
    assert_eq!(flaky_info.restarts, 1);
    assert_eq!(panicky_info.state, ExtensionState::Crashed);
    assert_eq!(panicky_info.restarts, 0);
}

/// 停机记录扩展 — `shutdown` 钩子被调用即置位
struct ShutdownTracker {
    id: CapabilityId,
    shutdown_called: Arc<AtomicUsize>,
}

#[async_trait]
impl Extension for ShutdownTracker {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        Ok(())
    }

    async fn shutdown(&self) {
        self.shutdown_called.fetch_add(1, Ordering::SeqCst);
    }
}

/// `ExtensionHandle`：注册返回句柄 → `remove()` 注销路由并触发 `shutdown` 钩子
#[tokio::test]
async fn extension_handle_removes_and_shuts_down() {
    let kernel = Kernel::new();
    let shutdown_called = Arc::new(AtomicUsize::new(0));
    let ext_id = CapabilityId::new();

    let handle = kernel
        .register_handle(
            Box::new(ShutdownTracker {
                id: ext_id,
                shutdown_called: shutdown_called.clone(),
            }),
            8,
            SupervisionPolicy::Transient,
        )
        .await
        .expect("register_handle ok");

    assert_eq!(handle.id(), ext_id);

    // 注册后可正常 emit
    kernel.emit(ext_id, Envelope::new()).await.expect("emit before remove ok");

    // 移除 → Sender drop → 通道关闭 → 监督循环 NormalExit → 调用 ext.shutdown()
    handle.remove().await.expect("remove ok");
    // 给后台监督任务一点时间执行 shutdown
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        shutdown_called.load(Ordering::SeqCst),
        1,
        "extension shutdown hook must be invoked after remove"
    );

    // 移除后对目标 emit 返回 TargetUnreachable
    let err = kernel
        .emit(ext_id, Envelope::new())
        .await
        .expect_err("emit after remove must fail");
    assert!(matches!(err, KernelError::TargetUnreachable));

    // 扩展已从路由表注销
    assert_eq!(
        kernel
            .extensions()
            .iter()
            .filter(|i| i.id == ext_id)
            .count(),
        0
    );
}

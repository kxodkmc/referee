//! Phase 6 测试：健壮性深化与并发安全
//! 老化防饥饿 / WAL 落盘与恢复 / KernelContext 受限通信（emit / spawn_blocking 可用）

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use referee_core::kernel::priority::PrioritySender;
use referee_core::{
    CapabilityId, Envelope, Extension, InMemoryWal, Kernel, KernelContext, KernelError,
    KernelResult, MessageContext, SupervisionPolicy, WalSink,
};

// ───────────────────────────────────────────────
// 测试夹具
// ───────────────────────────────────────────────

/// 计数器扩展（验证消息送达 / WAL ACK）
struct CountingExtension {
    id: CapabilityId,
    count: Arc<AtomicUsize>,
}

#[async_trait]
impl Extension for CountingExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// 在 handle 内向另一扩展 emit 的扩展（验证 KernelContext 受限通信）
struct EmitterExtension {
    id: CapabilityId,
    target: CapabilityId,
}

#[async_trait]
impl Extension for EmitterExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        ctx.emit(self.target, Envelope::new()).await
    }
}

/// 使用 spawn_blocking 的扩展（验证阻塞出口注入）
struct BlockingExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for BlockingExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        let handle = ctx.spawn_blocking(|| 40 + 2);
        let result = handle.await.map_err(|_| KernelError::TargetUnreachable)?;
        assert_eq!(result, 42);
        Ok(())
    }
}

// ───────────────────────────────────────────────
// 用例 1：老化防饥饿 — 持续 High 负载下 Low 在阈值内被消费
// ───────────────────────────────────────────────
#[tokio::test]
async fn low_priority_ages_out_under_sustained_high_load() {
    let ext_id = CapabilityId::new();
    let (tx, rx) = PrioritySender::new(64, ext_id);

    // 先入队 1 条 Low（priority = 200）
    let mut low_env = Envelope::new();
    low_env.priority = 200;
    tx.try_send(MessageContext::new(low_env))
        .expect("low send ok");

    // 后台持续涌入 High（priority = 0），消费多少补多少
    let producer = {
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let mut env = Envelope::new();
                env.priority = 0;
                if tx.try_send(MessageContext::new(env)).is_err() {
                    // 队列满：等消费腾出空间
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        })
    };

    // 主循环消费：Low 应在老化阈值（1s）内被抢在 High 之前消费
    let mut saw_low = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && !saw_low {
        if let Some(ctx) = rx.recv().await {
            saw_low = ctx.envelope.priority == 200;
        } else {
            break;
        }
    }
    producer.abort();

    assert!(
        saw_low,
        "Low message must be consumed within aging threshold despite sustained High load"
    );
}

// ───────────────────────────────────────────────
// 用例 2：WAL 记录 — dispatch 落盘，handle 成功后 ACK
// ───────────────────────────────────────────────
#[tokio::test]
async fn wal_records_then_acks_on_success() {
    let wal = Arc::new(InMemoryWal::new());
    let kernel = Kernel::with_wal(wal.clone());
    let count = Arc::new(AtomicUsize::new(0));
    let ext = CountingExtension {
        id: CapabilityId::new(),
        count: count.clone(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
    // 处理成功前 WAL 应有未确认记录（dispatch 同步落盘，此处必然已 append）
    // 等待处理完成（ACK）
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(count.load(Ordering::SeqCst), 1, "message must be handled");
    assert_eq!(
        wal.pending_len(),
        0,
        "WAL record must be acked after successful handling"
    );
}

// ───────────────────────────────────────────────
// 用例 3：WAL 恢复 — 未确认消息在下次启动经恢复通道重放
// ───────────────────────────────────────────────
#[tokio::test]
async fn wal_recovery_replays_unacked_messages() {
    let wal = Arc::new(InMemoryWal::new());
    let kernel = Kernel::with_wal(wal.clone());
    let count = Arc::new(AtomicUsize::new(0));
    let ext = CountingExtension {
        id: CapabilityId::new(),
        count: count.clone(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // 模拟上次运行崩溃残留：WAL 中有一条未确认消息（target 指向已注册扩展）
    let mut env = Envelope::new();
    env.target = ext_id.as_uuid();
    let _ = wal.append(&env).await.expect("append ok");
    assert_eq!(wal.pending_len(), 1);

    // 恢复通道：绕过 WAL 追加直接注入路由表
    kernel.start_with_recovery().await.expect("recovery ok");
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "recovered message must be delivered exactly once"
    );
    assert_eq!(
        wal.pending_len(),
        0,
        "recovered message must be acked after handling"
    );

    // 二次恢复：无残留，不重复投递
    kernel.start_with_recovery().await.expect("recovery ok");
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ───────────────────────────────────────────────
// 用例 4：KernelContext 受限通信 — emit 可送达目标扩展
// ───────────────────────────────────────────────
#[tokio::test]
async fn kernel_context_emit_reaches_target() {
    let kernel = Kernel::new();
    let count = Arc::new(AtomicUsize::new(0));
    let counter = CountingExtension {
        id: CapabilityId::new(),
        count: count.clone(),
    };
    let counter_id = counter.id();
    kernel
        .register(Box::new(counter), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    let emitter = EmitterExtension {
        id: CapabilityId::new(),
        target: counter_id,
    };
    let emitter_id = emitter.id();
    kernel
        .register(Box::new(emitter), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    kernel
        .emit(emitter_id, Envelope::new())
        .await
        .expect("emit ok");
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "emit from handle must deliver to target extension"
    );
}

// ───────────────────────────────────────────────
// 用例 5：spawn_blocking 注入可用（重计算移交独立线程池）
// ───────────────────────────────────────────────
#[tokio::test]
async fn kernel_context_spawn_blocking_works() {
    let kernel = Kernel::new();
    let ext = BlockingExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // handle 内 spawn_blocking 计算结果并断言（失败会 panic 熔断，此处验证成功路径）
    kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 若 spawn_blocking 不可用 / 结果错误，handle 断言失败 → 扩展熔断 → 状态 Crashed
    let result = kernel.emit(ext_id, Envelope::new()).await;
    assert!(
        result.is_ok(),
        "extension must stay healthy after spawn_blocking handle"
    );
}

// ───────────────────────────────────────────────
// 用例 6：恢复投递失败进死信且 ACK（避免下次启动重复重放）
// ───────────────────────────────────────────────
#[tokio::test]
async fn wal_recovery_failure_goes_to_dlq() {
    use referee_core::InMemoryDlq;

    let wal = Arc::new(InMemoryWal::new());
    let dlq = Arc::new(InMemoryDlq::new(16));
    let kernel = Kernel::with_dlq_wal(dlq.clone(), wal.clone());

    // WAL 中残留一条指向「未注册扩展」的消息（target 无路由）
    let mut env = Envelope::new();
    env.target = CapabilityId::new().as_uuid();
    let _ = wal.append(&env).await.expect("append ok");

    kernel.start_with_recovery().await.expect("recovery ok");

    assert_eq!(
        wal.pending_len(),
        0,
        "failed recovery must ack to avoid infinite replay"
    );
    assert_eq!(dlq.drain().len(), 1, "failed recovery must go to DLQ");
}

// ───────────────────────────────────────────────
// 用例 7：同 id 重注册 — 旧任务退出不得覆盖新条目状态（代际保护）
// ───────────────────────────────────────────────
#[tokio::test]
async fn re_register_same_id_not_overridden_by_old_task() {
    let kernel = Kernel::new();
    let id = CapabilityId::new();
    let count = Arc::new(AtomicUsize::new(0));

    // 第一次注册 → 注销（旧任务即将自然退出）
    let ext1 = CountingExtension {
        id,
        count: count.clone(),
    };
    kernel
        .register(Box::new(ext1), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    kernel.unregister(id).await.unwrap();

    // 立即同 id 重注册（新代际）
    let ext2 = CountingExtension {
        id,
        count: count.clone(),
    };
    kernel
        .register(Box::new(ext2), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // 等待旧任务退出（竞态窗口：若无代际保护，旧任务会把新条目置 Stopped）
    tokio::time::sleep(Duration::from_millis(100)).await;

    kernel
        .emit(id, Envelope::new())
        .await
        .expect("re-registered extension must accept messages");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(count.load(Ordering::SeqCst), 1, "new extension must handle");
}

// ───────────────────────────────────────────────
// 用例 8：Panic 被消费尝试后 WAL ACK — 防无界滞留与重复重放
// ───────────────────────────────────────────────
#[tokio::test]
async fn wal_acked_after_panic_consumed() {
    let wal = Arc::new(InMemoryWal::new());
    let kernel = Kernel::with_wal(wal.clone());
    let ext = PanicExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    let _ = kernel.emit(ext_id, Envelope::new()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        wal.pending_len(),
        0,
        "panic-consumed message must be acked (no unbounded WAL growth)"
    );
}

// ───────────────────────────────────────────────
// 测试夹具：主动 Panic 的扩展
// ───────────────────────────────────────────────
struct PanicExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for PanicExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        panic!("simulated panic for wal ack test");
    }
}

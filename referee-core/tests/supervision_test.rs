//! 监督治理加固测试：挂起超时治理 / 终态积压转储 / 停机封路与消息守恒

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use referee_core::{
    CapabilityId, Envelope, Extension, InMemoryDlq, Kernel, KernelContext, KernelError,
    KernelResult, RegisterOptions, SupervisionPolicy,
};

// ───────────────────────────────────────────────
// 测试夹具
// ───────────────────────────────────────────────

/// 持续 Panic 的扩展（终态转储验证）
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

/// 每次 handle 都长时间挂起的扩展（模拟外呼无超时 / 内部死锁）
struct HangingExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for HangingExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        tokio::time::sleep(Duration::from_secs(30)).await;
        Ok(())
    }
}

/// 首次 handle 挂起，其后正常完成的扩展（自愈验证）
struct FlakyHangingExtension {
    id: CapabilityId,
    calls: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

#[async_trait]
impl Extension for FlakyHangingExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            // 首次调用：挂起远超 handle_timeout（模拟外呼无超时 / 内部死锁）
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// 快速计数扩展（停机守恒验证）
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
        tokio::time::sleep(Duration::from_millis(1)).await;
        self.count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// ───────────────────────────────────────────────
// 加固 1：挂起治理 — 超时切断视为崩溃，走监督策略自愈
// ───────────────────────────────────────────────
#[tokio::test]
async fn handle_timeout_cuts_hang_and_self_heals() {
    let kernel = Kernel::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let ext = FlakyHangingExtension {
        id: CapabilityId::new(),
        calls: calls.clone(),
        completed: completed.clone(),
    };
    let ext_id = ext.id();
    kernel
        .register_with(
            Box::new(ext),
            RegisterOptions::new(
                8,
                SupervisionPolicy::OneForOne {
                    max_restarts: 3,
                    window_secs: 30,
                },
            )
            .with_handle_timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap();

    // 2 条消息：第 1 条触发挂起被切断，退避重启后第 2 条正常完成
    kernel.emit(ext_id, Envelope::new()).await.unwrap();
    kernel.emit(ext_id, Envelope::new()).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    while completed.load(Ordering::SeqCst) < 1 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // 若无超时治理，第 1 条将挂起 30s，远超 deadline
    assert!(
        Instant::now() < deadline,
        "hang must be cut within handle_timeout and self-heal"
    );
    // 被切断的第 1 条不重试（消费尝试已终止），仅第 2 条正常完成
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    // 自愈后恢复可用
    kernel
        .emit(ext_id, Envelope::new())
        .await
        .expect("emit after healing ok");
    let deadline = Instant::now() + Duration::from_secs(2);
    while completed.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(completed.load(Ordering::SeqCst), 2);
}

// ───────────────────────────────────────────────
// 加固 1 + 2：Transient 策略下超时熔断，积压转储 DLQ
// ───────────────────────────────────────────────
#[tokio::test]
async fn handle_timeout_transient_circuit_breaks_with_backlog_to_dlq() {
    let dlq = Arc::new(InMemoryDlq::new(16));
    let kernel = Kernel::with_dlq(dlq.clone());
    let ext = HangingExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register_with(
            Box::new(ext),
            RegisterOptions::new(8, SupervisionPolicy::Transient)
                .with_handle_timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap();

    // 2 条消息：第 1 条挂起被切断熔断，第 2 条积压转储 DLQ 而非静默丢弃
    kernel.emit(ext_id, Envelope::new()).await.unwrap();
    kernel.emit(ext_id, Envelope::new()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let drained = dlq.drain();
    assert_eq!(drained.len(), 1, "backlog must be drained to DLQ");
    assert_eq!(drained[0].1, KernelError::ExtensionStopped);
    // 熔断后路由拒绝
    let result = kernel.emit(ext_id, Envelope::new()).await;
    assert_eq!(result, Err(KernelError::ExtensionCrashed));
}

// ───────────────────────────────────────────────
// 加固 2：Panic 熔断（Transient）— 剩余积压转储 DLQ
// ───────────────────────────────────────────────
#[tokio::test]
async fn transient_crash_drains_backlog_to_dlq() {
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

    // 3 条入队：第 1 条触发 Panic 熔断，剩余 2 条积压必须转储 DLQ
    for _ in 0..3 {
        kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let drained = dlq.drain();
    assert_eq!(drained.len(), 2, "backlog must be drained to DLQ, not dropped");
    assert!(
        drained
            .iter()
            .all(|(_, reason)| *reason == KernelError::ExtensionStopped),
        "drain reason must be ExtensionStopped"
    );
    // 熔断后路由拒绝
    let result = kernel.emit(ext_id, Envelope::new()).await;
    assert_eq!(result, Err(KernelError::ExtensionCrashed));
}

// ───────────────────────────────────────────────
// 加固 2：OneForOne 重启超限 — Stopped 后积压转储 DLQ
// ───────────────────────────────────────────────
#[tokio::test]
async fn restart_limit_drains_backlog_to_dlq() {
    let dlq = Arc::new(InMemoryDlq::new(16));
    let kernel = Kernel::with_dlq(dlq.clone());
    let ext = PanicExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(
            Box::new(ext),
            8,
            SupervisionPolicy::OneForOne {
                max_restarts: 1,
                window_secs: 30,
            },
        )
        .await
        .unwrap();

    // 3 条消息：第 1 条 Panic → 退避重启（200ms）→ 第 2 条 Panic →
    // 超出 max_restarts=1 → Stopped → 第 3 条转储 DLQ
    for _ in 0..3 {
        kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
    }
    tokio::time::sleep(Duration::from_millis(600)).await;

    let drained = dlq.drain();
    assert_eq!(drained.len(), 1, "remaining backlog must be drained to DLQ");
    assert_eq!(drained[0].1, KernelError::ExtensionStopped);
    // Stopped 终态：路由拒绝
    let result = kernel.emit(ext_id, Envelope::new()).await;
    assert_eq!(result, Err(KernelError::TargetUnreachable));
}

// ───────────────────────────────────────────────
// 加固 3：停机封路 — 投递与停机并发时消息守恒，零滞留丢失
// ───────────────────────────────────────────────
#[tokio::test]
async fn shutdown_race_no_message_loss() {
    let dlq = Arc::new(InMemoryDlq::new(4096));
    let kernel = Kernel::with_dlq(dlq.clone());
    let processed = Arc::new(AtomicUsize::new(0));
    let ext = CountingExtension {
        id: CapabilityId::new(),
        count: processed.clone(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 64, SupervisionPolicy::Transient)
        .await
        .unwrap();

    // 并发生产者：全速投递；背压满则让步重试（该条被拒并落 DLQ），
    // 停机类错误终止
    let producer_kernel = kernel.clone();
    let sent_ok = Arc::new(AtomicUsize::new(0));
    let sent_err = Arc::new(AtomicUsize::new(0));
    let sent_full = Arc::new(AtomicUsize::new(0));
    let p_ok = sent_ok.clone();
    let p_err = sent_err.clone();
    let p_full = sent_full.clone();
    let producer = tokio::spawn(async move {
        loop {
            match producer_kernel.emit(ext_id, Envelope::new()).await {
                Ok(()) => {
                    p_ok.fetch_add(1, Ordering::SeqCst);
                }
                Err(KernelError::ResourceExhausted) => {
                    p_full.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(_) => {
                    // 停机拦截：消息被拒并落 DLQ，生产终止
                    p_err.fetch_add(1, Ordering::SeqCst);
                    break;
                }
            }
        }
    });

    // 生产与消费并发运行后触发停机（drain + 封路 + 终末 drain）
    tokio::time::sleep(Duration::from_millis(50)).await;
    kernel.shutdown_graceful(3000).await.expect("shutdown ok");
    producer.abort();

    // 守恒断言：成功入队的消息必须全部被处理（封路 + 终末 drain 保证），
    // 被拒的消息（停机拦截 + 背压满）必须全部落 DLQ —— 任何一侧不等
    // 即存在静默丢失
    let ok = sent_ok.load(Ordering::SeqCst);
    let rejected = sent_err.load(Ordering::SeqCst);
    let exhausted = sent_full.load(Ordering::SeqCst);
    let handled = processed.load(Ordering::SeqCst);
    let dead = dlq.drain().len();
    assert!(ok + rejected + exhausted > 0, "race must actually produce traffic");
    assert_eq!(handled, ok, "every accepted message must be handled");
    assert_eq!(
        dead,
        rejected + exhausted,
        "every rejected message must land in DLQ"
    );
}

// ───────────────────────────────────────────────
// 加固 3：停机期崩溃 — 跳过自愈直接转储，积压进 DLQ 而非被反复消费
// ───────────────────────────────────────────────
#[tokio::test]
async fn shutdown_during_crash_skips_restart_and_drains() {
    let dlq = Arc::new(InMemoryDlq::new(64));
    let kernel = Kernel::with_dlq(dlq.clone());
    let ext = HangingExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register_with(
            Box::new(ext),
            RegisterOptions::new(
                8,
                SupervisionPolicy::OneForOne {
                    max_restarts: 100,
                    window_secs: 3600,
                },
            )
            .with_handle_timeout(Duration::from_millis(80)),
        )
        .await
        .unwrap();

    // 2 条消息：第 1 条进入挂起处理中
    for _ in 0..2 {
        kernel.emit(ext_id, Envelope::new()).await.expect("emit ok");
    }
    tokio::time::sleep(Duration::from_millis(30)).await;

    let start = Instant::now();
    kernel
        .shutdown_graceful(5000)
        .await
        .expect("shutdown ok");
    let elapsed = start.elapsed();

    // 挂起被超时切断后检测到停机已触发：跳过退避重启直接转储退出，
    // 停机远早于 5s 强杀上限；若无此治理，将经历退避 → 重启 → 再挂起
    // 的长循环，且积压被反复消费而非转储
    assert!(
        elapsed < Duration::from_secs(2),
        "shutdown must not wait for restart backoff: {elapsed:?}"
    );
    // 第 2 条积压转储 DLQ（第 1 条已被超时切断消费）
    assert_eq!(
        dlq.drain().len(),
        1,
        "remaining backlog must be drained to DLQ"
    );
}

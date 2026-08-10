//! Phase 1 测试：背压机制 + 路由基础

use std::time::{Duration, Instant};

use async_trait::async_trait;
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelError, KernelResult,
    SupervisionPolicy,
};

// ───────────────────────────────────────────────
// 测试夹具：永不消费消息的扩展（handle 永久 pending）
// ───────────────────────────────────────────────
struct NeverConsumingExtension {
    id: CapabilityId,
}

impl NeverConsumingExtension {
    fn new() -> Self {
        Self {
            id: CapabilityId::new(),
        }
    }
}

#[async_trait]
impl Extension for NeverConsumingExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        // 永不返回 — 模拟扩展卡死，循环阻塞在首条消息
        std::future::pending::<()>().await;
        Ok(())
    }
}

// ───────────────────────────────────────────────
// 测试 1：通道满载后 emit 返回 ResourceExhausted
// ───────────────────────────────────────────────
#[tokio::test]
async fn backpressure_triggers_resource_exhausted() {
    let kernel = Kernel::new();
    let ext = NeverConsumingExtension::new();
    let ext_id = ext.id();
    const QUEUE_SIZE: usize = 8;
    kernel
        .register(Box::new(ext), QUEUE_SIZE, SupervisionPolicy::Transient)
        .await
        .expect("register should succeed");
    // 让运行循环有机会启动并拉取首条消息
    tokio::time::sleep(Duration::from_millis(50)).await;
    // 先发一条让循环吞掉（循环随后卡在 handle）
    let _ = kernel.emit(ext_id, Envelope::new()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // 持续 emit 直到背压触发
    let mut sent = 0;
    let start = Instant::now();
    loop {
        match kernel.emit(ext_id, Envelope::new()).await {
            Ok(_) => sent += 1,
            Err(KernelError::ResourceExhausted) => break,
            Err(other) => panic!("unexpected error: {:?}", other),
        }
        if sent > QUEUE_SIZE * 2 {
            panic!(
                "backpressure failed: sent {} messages without exhaustion",
                sent
            );
        }
        if start.elapsed() > Duration::from_secs(2) {
            panic!("timeout waiting for ResourceExhausted");
        }
    }
    // 验证：发送量在合理范围内（≤ QUEUE_SIZE + 1）
    assert!(
        sent <= QUEUE_SIZE + 1,
        "sent {} should be <= {}",
        sent,
        QUEUE_SIZE + 1
    );
}

// ───────────────────────────────────────────────
// 测试 2：未注册目标返回 TargetUnreachable
// ───────────────────────────────────────────────
#[tokio::test]
async fn emit_to_unregistered_returns_target_unreachable() {
    let kernel = Kernel::new();
    let fake_id = CapabilityId::new();
    let result = kernel.emit(fake_id, Envelope::new()).await;
    assert_eq!(result, Err(KernelError::TargetUnreachable));
}

// ───────────────────────────────────────────────
// 测试 3：unregister 后 emit 返回 TargetUnreachable
// ───────────────────────────────────────────────
#[tokio::test]
async fn unregister_blocks_subsequent_emits() {
    let kernel = Kernel::new();
    let ext = NeverConsumingExtension::new();
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    // 注销
    kernel.unregister(ext_id).await.expect("unregister ok");
    // 后续 emit 应被拦截
    let result = kernel.emit(ext_id, Envelope::new()).await;
    assert_eq!(result, Err(KernelError::TargetUnreachable));
}

//! Phase 3 测试：容错与隔离（Panic 熔断 + 死循环不阻塞 Runtime）

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelError, KernelResult,
    SupervisionPolicy,
};

// ───────────────────────────────────────────────
// 测试夹具 1：主动 Panic 的扩展
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
        panic!("simulated crash!");
    }
}

// ───────────────────────────────────────────────
// 测试夹具 2：死循环（无 await）的扩展
// 注：循环体含 spin_loop，仅为规避 clippy `empty_loop` lint，语义仍是纯 CPU 占用。
// ───────────────────────────────────────────────
struct DeadLoopExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for DeadLoopExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        // 纯 CPU 死循环，不主动 yield — 永久占用单个 Tokio Worker
        loop {
            std::hint::spin_loop();
        }
    }
}

// ───────────────────────────────────────────────
// 测试 1：Panic 被隔离，内核存活且拦截后续消息
// ───────────────────────────────────────────────
#[tokio::test]
async fn panic_isolation_and_state_marking() {
    let kernel = Kernel::new();
    let ext = PanicExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    // 发一条消息触发 Panic
    let _ = kernel.emit(ext_id, Envelope::new()).await;
    // 等待 catch_unwind 捕获并更新状态
    tokio::time::sleep(Duration::from_millis(50)).await;
    // 验证后续消息被拦截
    let result = kernel.emit(ext_id, Envelope::new()).await;
    assert_eq!(result, Err(KernelError::ExtensionCrashed));
    // 验证内核依然存活，可正常操作其他对象
    let kernel_alive_check = kernel.unregister(ext_id).await;
    assert!(
        kernel_alive_check.is_ok(),
        "Kernel API should still respond"
    );
}

// ───────────────────────────────────────────────
// 测试 2：死循环不阻塞 Runtime 心跳调度
// 注 1：必须使用多线程 runtime，单线程会被死循环彻底卡死。
// 注 2：不能依赖 `#[tokio::test]` 隐式 drop runtime —— 被死循环卡死的
//       Worker 永不返回，Runtime::drop 会永久阻塞。必须手动
//       `shutdown_background()` 跳过对卡死 Worker 的等待。
// ───────────────────────────────────────────────
#[test]
fn dead_loop_does_not_block_runtime() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .expect("build multi_thread runtime");
    let count = rt.block_on(async {
        let kernel = Kernel::new();
        let ext = DeadLoopExtension {
            id: CapabilityId::new(),
        };
        let ext_id = ext.id();
        kernel
            .register(Box::new(ext), 8, SupervisionPolicy::Transient)
            .await
            .expect("register ok");
        // 启动一个独立的心跳 Task
        let counter = Arc::new(AtomicUsize::new(0));
        let hb_counter = counter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                interval.tick().await;
                hb_counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        // 投递死循环任务
        let _ = kernel.emit(ext_id, Envelope::new()).await;
        // 等待 100ms，验证心跳 Task 是否正常递增
        tokio::time::sleep(Duration::from_millis(100)).await;
        counter.load(Ordering::SeqCst)
    });
    // 后台关闭：不等被死循环卡死的 Worker（否则会永久阻塞），其线程在进程退出时回收
    rt.shutdown_background();
    assert!(
        count > 0,
        "Runtime heartbeat blocked! count = {}. Dead loop broke isolation.",
        count
    );
    // 健康警告：若此测试失败，说明扩展在 handle 中直接执行了死循环，
    // 占用了 Tokio Worker 线程。必须强制要求扩展使用 spawn_blocking 执行重计算。
}

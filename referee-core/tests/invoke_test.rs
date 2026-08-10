//! Phase 2 测试：invoke 原语（oneshot + timeout + 目标边界）

use std::time::Duration;

use async_trait::async_trait;
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelError, KernelResult,
    SupervisionPolicy,
};
use tokio::time::sleep;

// ───────────────────────────────────────────────
// 测试夹具：Echo 扩展（正常回复，回传 correlation_id）
// ───────────────────────────────────────────────
struct EchoExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for EchoExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let mut resp = Envelope::new();
        // 遵循契约：响应信封必须携带请求的 correlation_id
        resp.correlation_id = env.correlation_id;
        ctx.reply(resp)
    }
}

// ───────────────────────────────────────────────
// 测试夹具：延迟扩展（用于触发超时）
// ───────────────────────────────────────────────
struct DelayExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for DelayExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, _env: Envelope) -> KernelResult<()> {
        sleep(Duration::from_millis(200)).await;
        let resp = Envelope::new();
        ctx.reply(resp)
    }
}

// ───────────────────────────────────────────────
// 测试 1：invoke 正常匹配响应
// ───────────────────────────────────────────────
#[tokio::test]
async fn invoke_returns_matched_response() {
    let kernel = Kernel::new();
    let ext = EchoExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    let mut req = Envelope::new();
    req.correlation_id = Envelope::new().correlation_id; // 随机 ID
    let expected_cid = req.correlation_id;
    let resp = kernel
        .invoke(ext_id, req, 1000)
        .await
        .expect("invoke should succeed");
    assert_eq!(resp.correlation_id, expected_cid);
}

// ───────────────────────────────────────────────
// 测试 2：invoke 超时切断
// ───────────────────────────────────────────────
#[tokio::test]
async fn invoke_times_out() {
    let kernel = Kernel::new();
    let ext = DelayExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    let result = kernel.invoke(ext_id, Envelope::new(), 50).await;
    assert_eq!(result.unwrap_err(), KernelError::Timeout);
}

// ───────────────────────────────────────────────
// 测试 3：目标注销后 invoke 返回不可达
// ───────────────────────────────────────────────
#[tokio::test]
async fn invoke_to_unregistered_fails() {
    let kernel = Kernel::new();
    let ext = EchoExtension {
        id: CapabilityId::new(),
    };
    let ext_id = ext.id();
    kernel
        .register(Box::new(ext), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    kernel.unregister(ext_id).await.expect("unregister ok");
    let result = kernel.invoke(ext_id, Envelope::new(), 100).await;
    assert_eq!(result.unwrap_err(), KernelError::TargetUnreachable);
}

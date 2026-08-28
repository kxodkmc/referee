//! A2 验收：ChannelHost（MockAdapter）——受理 / 显式拒绝 / adapter 监督 /
//! 入站反压 / DLQ / 停机 flush 幂等 / im.sent 观测

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use referee_channel::message::{kind, meta};
use referee_channel::{
    AdapterError, AdapterState, ChannelAdapter, ChannelCapabilities, ChannelContent, ChannelHost,
    ChannelIo, InboundMessage, OutboundCommand, SendReceipt, SentNotice,
};
use referee_core::{
    CapabilityId, Envelope, Extension, ExtensionState, InMemoryDlq, Kernel, KernelContext,
    KernelError, KernelResult, SupervisionPolicy,
};

// ───────────────────────────────────────────────
// Mock 适配器：行为可注入
// ───────────────────────────────────────────────

#[derive(Default)]
struct MockScript {
    /// run 启动时注入的入站消息（send 成功后才计游标——背压契约）
    inbound: Mutex<Vec<InboundMessage>>,
    /// 入站注入间隔
    inject_gap_ms: AtomicU64,
    /// 出站消费延迟
    consume_delay_ms: AtomicU64,
    /// 暂停出站消费（不 select outbound_rx，用于构造队列满）
    pause_outbound: AtomicBool,
    /// 前 N 次 run 直接 panic（fetch 递减）
    panics: AtomicU32,
    outbound_log: Mutex<Vec<OutboundCommand>>,
    cursor_advanced: AtomicU32,
    run_exits: AtomicU32,
}

#[derive(Default)]
struct MockState {
    writes: AtomicU32,
    dirty: AtomicBool,
}

#[async_trait]
impl AdapterState for MockState {
    async fn flush(&self) -> Result<(), AdapterError> {
        if self.dirty.swap(false, Ordering::SeqCst) {
            self.writes.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

struct MockAdapter {
    script: Arc<MockScript>,
    state: Arc<MockState>,
}

impl MockAdapter {
    fn new(script: Arc<MockScript>) -> (Self, Arc<MockState>) {
        let state = Arc::new(MockState {
            writes: AtomicU32::new(0),
            dirty: AtomicBool::new(true),
        });
        (Self { script, state: state.clone() }, state)
    }
}

#[async_trait]
impl ChannelAdapter for MockAdapter {
    fn kind(&self) -> &'static str {
        "mock"
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            max_text_len: 4000,
            batch_idle_window_ms: 8000,
            max_batch_messages: 10,
            max_batch_window_ms: 30000,
        }
    }

    fn state(&self) -> Arc<dyn AdapterState> {
        self.state.clone()
    }

    async fn run(&self, mut io: ChannelIo) -> Result<(), AdapterError> {
        if self.script.panics.load(Ordering::SeqCst) > 0 {
            self.script.panics.fetch_sub(1, Ordering::SeqCst);
            panic!("mock adapter crash");
        }
        let gap = Duration::from_millis(self.script.inject_gap_ms.load(Ordering::SeqCst));
        // 独立语句结束锁守卫生命周期：drain 的借用不得跨循环体内的 await
        let messages = self.script.inbound.lock().drain(..).collect::<Vec<_>>();
        for msg in messages {
            io.inbound_tx
                .send(msg)
                .await
                .map_err(|e| format!("inbound send failed: {e}"))?;
            self.script.cursor_advanced.fetch_add(1, Ordering::SeqCst);
            if !gap.is_zero() {
                tokio::time::sleep(gap).await;
            }
        }
        let delay = Duration::from_millis(self.script.consume_delay_ms.load(Ordering::SeqCst));
        loop {
            if self.script.pause_outbound.load(Ordering::SeqCst) {
                // 只等停机信号，出站永远不消费
                io.shutdown
                    .changed()
                    .await
                    .map_err(|_| "shutdown sender dropped".to_string())?;
                break;
            }
            tokio::select! {
                changed = io.shutdown.changed() => {
                    if changed.is_err() || *io.shutdown.borrow() {
                        break;
                    }
                }
                cmd = io.outbound_rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                    self.script.outbound_log.lock().push(cmd);
                }
            }
        }
        self.script.run_exits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// ───────────────────────────────────────────────
// 辅助扩展与工具
// ───────────────────────────────────────────────

#[derive(Default)]
struct RouterLog {
    sent: Mutex<Vec<SentNotice>>,
    inbound: Mutex<Vec<InboundMessage>>,
}

#[derive(Clone)]
struct RecordingRouter {
    id: CapabilityId,
    log: Arc<RouterLog>,
}

impl RecordingRouter {
    fn new() -> Self {
        Self {
            id: CapabilityId::new(),
            log: Arc::new(RouterLog::default()),
        }
    }
}

#[async_trait]
impl Extension for RecordingRouter {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        match env.metadata.get(meta::KIND).map(String::as_str) {
            Some(kind::SENT) => {
                if let Ok(notice) = SentNotice::from_envelope(&env) {
                    self.log.sent.lock().push(notice);
                }
            }
            Some(kind::INBOUND) => {
                if let Ok(msg) = InboundMessage::from_envelope(&env) {
                    self.log.inbound.lock().push(msg);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

struct EchoExtension {
    id: CapabilityId,
}

#[async_trait]
impl Extension for EchoExtension {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let _ = ctx.reply(env);
        Ok(())
    }
}

fn sample_command(text: &str) -> OutboundCommand {
    OutboundCommand {
        endpoint: "mock/1".into(),
        peer: "user-甲".into(),
        content: ChannelContent::Text(text.into()),
    }
}

fn sample_inbound(i: usize) -> InboundMessage {
    InboundMessage {
        endpoint: "mock/1".into(),
        peer: "user-甲".into(),
        message_id: format!("m-{i}"),
        content: ChannelContent::Text(format!("消息 {i}")),
        session_ctx: "ctx".into(),
        occurred_at: 1_756_000_000_000 + i as i64,
        raw: None,
    }
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not met within 2s: {what}");
}

// ───────────────────────────────────────────────
// A2-1 / A2-7：受理 → 命令落线 + im.sent 归因通知
// ───────────────────────────────────────────────

#[tokio::test]
async fn send_accepted_delivers_command_and_notifies_router() {
    let kernel = Kernel::new();
    let router = RecordingRouter::new();
    let router_id = router.id;
    kernel
        .register(Box::new(router.clone()), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    let script = Arc::new(MockScript::default());
    let (adapter, _state) = MockAdapter::new(script.clone());
    let host = ChannelHost::new(adapter, 8, 8);
    let host_id = host.id();
    kernel
        .register(Box::new(host.clone()), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    host.start(kernel.clone(), router_id);

    let cmd = sample_command("你好");
    let session_id = Uuid::new_v4();
    let started = Instant::now();
    let resp = kernel
        .invoke(host_id, cmd.to_send_envelope(session_id, Some(42)), 2_000)
        .await
        .expect("invoke ok");
    let receipt = SendReceipt::from_envelope(&resp).expect("receipt decodes");
    assert!(receipt.accepted);
    assert!(started.elapsed() < Duration::from_secs(1), "受理必须即时");

    eventually("outbound consumed", || script.outbound_log.lock().len() == 1).await;
    assert_eq!(script.outbound_log.lock()[0], cmd);

    eventually("im.sent noticed", || router.log.sent.lock().len() == 1).await;
    let notice = router.log.sent.lock()[0].clone();
    assert_eq!((notice.session_id, notice.turn_id), (session_id, 42));
    assert_eq!(notice.endpoint, "mock/1");
    assert_eq!(notice.peer, "user-甲");
}

// ───────────────────────────────────────────────
// A2-2：出站队列满 → 显式拒绝，不挂起
// ───────────────────────────────────────────────

#[tokio::test]
async fn outbound_full_rejects_without_hanging() {
    let kernel = Kernel::new();
    let router = RecordingRouter::new();
    kernel
        .register(Box::new(router), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();

    let script = Arc::new(MockScript::default());
    script.pause_outbound.store(true, Ordering::SeqCst); // 出站永不消费
    let (adapter, _) = MockAdapter::new(script);
    let host = ChannelHost::new(adapter, 8, 1); // 出站容量 1
    let host_id = host.id();
    kernel
        .register(Box::new(host.clone()), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    host.start(kernel.clone(), CapabilityId::new());

    let first = kernel
        .invoke(host_id, sample_command("一").to_send_envelope(Uuid::new_v4(), Some(1)), 1_000)
        .await
        .unwrap();
    assert!(SendReceipt::from_envelope(&first).unwrap().accepted);

    let started = Instant::now();
    let second = kernel
        .invoke(host_id, sample_command("二").to_send_envelope(Uuid::new_v4(), Some(2)), 1_000)
        .await
        .unwrap();
    let receipt = SendReceipt::from_envelope(&second).unwrap();
    assert!(!receipt.accepted, "队列满必须显式拒绝");
    assert!(started.elapsed() < Duration::from_millis(500), "拒绝不得挂起");
    assert_eq!(
        second.metadata.get(meta::ERROR).map(String::as_str),
        Some("channel queue full (rejected)")
    );
}

// ───────────────────────────────────────────────
// A2-3：panic 退避重启 → 超限降级；内核与其余扩展不受影响
// ───────────────────────────────────────────────

#[tokio::test]
async fn adapter_panics_restart_then_degrade_kernel_unaffected() {
    let kernel = Kernel::new();
    let echo_id = CapabilityId::new();
    kernel
        .register(
            Box::new(EchoExtension { id: echo_id }),
            8,
            SupervisionPolicy::Transient,
        )
        .await
        .unwrap();

    let script = Arc::new(MockScript::default());
    script.panics.store(10, Ordering::SeqCst); // 每次尝试都 panic
    let (adapter, _) = MockAdapter::new(script);
    let host = ChannelHost::new(adapter, 8, 8);
    let host_id = host.id();
    kernel
        .register(Box::new(host.clone()), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    host.start(kernel.clone(), CapabilityId::new());

    eventually("host degraded", || host.is_degraded()).await;
    assert_eq!(host.run_attempts(), 4, "首次运行 + 3 次退避重启");

    // 内核与两个扩展的治理状态均不受影响
    let snapshot = kernel.extensions();
    assert_eq!(snapshot.len(), 2);
    for info in snapshot {
        assert_eq!(info.state, ExtensionState::Running);
    }
    kernel
        .invoke(echo_id, Envelope::new(), 1_000)
        .await
        .expect("其余扩展正常 invoke");

    // 降级后 im.send 显式拒绝
    let resp = kernel
        .invoke(host_id, sample_command("三").to_system_envelope(), 1_000)
        .await
        .unwrap();
    let receipt = SendReceipt::from_envelope(&resp).unwrap();
    assert!(!receipt.accepted);
    assert_eq!(
        resp.metadata.get(meta::ERROR).map(String::as_str),
        Some("channel adapter: adapter degraded")
    );
}

// ───────────────────────────────────────────────
// A2-4：入站反压——通道满时游标停滞，腾空后恢复
//     （契约级验证：MockAdapter 履行「send 成功后才推进游标」的义务）
// ───────────────────────────────────────────────

#[tokio::test]
async fn inbound_backpressure_stalls_cursor_advance() {
    let script = Arc::new(MockScript::default());
    *script.inbound.lock() = vec![sample_inbound(1), sample_inbound(2), sample_inbound(3)];
    let (adapter, _) = MockAdapter::new(script.clone());

    let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
    let (_outbound_tx, outbound_rx) = mpsc::channel(1);
    let (_watch_tx, shutdown_rx) = watch::channel(false);
    let io = ChannelIo {
        inbound_tx,
        outbound_rx,
        shutdown: shutdown_rx,
    };
    tokio::spawn(async move {
        let _ = adapter.run(io).await;
    });

    let advanced = || script.cursor_advanced.load(Ordering::SeqCst);
    eventually("第一条入队", || advanced() == 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(advanced(), 1, "通道满（容量 1）时游标必须停滞");

    inbound_rx.recv().await.unwrap();
    eventually("第二条入队", || advanced() == 2).await;
    inbound_rx.recv().await.unwrap();
    inbound_rx.recv().await.unwrap();
    eventually("第三条入队", || advanced() == 3).await;
}

// ───────────────────────────────────────────────
// A2-5：emit 目标不存在 → 消息落 DLQ，搬运循环继续
// ───────────────────────────────────────────────

#[tokio::test]
async fn inbound_to_missing_router_lands_in_dlq_and_loop_continues() {
    let dlq = Arc::new(InMemoryDlq::new(16));
    let kernel = Kernel::with_dlq(dlq.clone());

    let script = Arc::new(MockScript::default());
    script.inject_gap_ms.store(20, Ordering::SeqCst);
    *script.inbound.lock() = vec![sample_inbound(1), sample_inbound(2), sample_inbound(3)];
    let (adapter, _) = MockAdapter::new(script);
    let host = ChannelHost::new(adapter, 8, 8);
    kernel
        .register(Box::new(host.clone()), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    // router 未注册：三条 im.inbound 全部被拒
    host.start(kernel.clone(), CapabilityId::new());

    eventually("三条全部入 DLQ", || dlq.len() == 3).await;
    let drained = dlq.drain();
    assert!(drained.iter().all(|(env, err)| {
        env.metadata.get(meta::KIND).map(String::as_str) == Some(kind::INBOUND)
            && matches!(err, KernelError::TargetUnreachable)
    }), "DLQ 内容应为 TargetUnreachable 拒绝的 im.inbound");
}

// ───────────────────────────────────────────────
// A2-6：shutdown → run 2s 内退出，flush 幂等
// ───────────────────────────────────────────────

#[tokio::test]
async fn shutdown_stops_run_and_flush_is_idempotent() {
    let kernel = Kernel::new();
    let script = Arc::new(MockScript::default());
    let (adapter, state) = MockAdapter::new(script.clone());
    let host = ChannelHost::new(adapter, 8, 8);
    kernel
        .register(Box::new(host.clone()), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    host.start(kernel.clone(), CapabilityId::new());

    let started = Instant::now();
    host.shutdown().await;
    assert!(started.elapsed() < Duration::from_secs(2), "停机宽限 2s");
    assert!(script.run_exits.load(Ordering::SeqCst) >= 1);
    assert_eq!(state.writes.load(Ordering::SeqCst), 1, "flush 落盘一次");

    host.shutdown().await; // 二次停机
    assert_eq!(state.writes.load(Ordering::SeqCst), 1, "flush 幂等：不重复落盘");
}

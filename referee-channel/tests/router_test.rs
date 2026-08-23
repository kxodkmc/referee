//! A4 验收：ImRouter——批次三条件闭合 / 队列满拒绝 / 会话道与信号量 /
//! 交付契约 / 中断 / 回合超时。时间边界全部以 `start_paused` 虚拟时钟操纵。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::watch;

use referee_channel::batch::BatchConfig;
use referee_channel::message::{kind, ChannelContent, InboundMessage, OutboundCommand, SendReceipt};
use referee_channel::{ImRouter, ImRouterConfig};
use referee_ai::provider::{FinishReason, Message};
use referee_ai::{ErrorKind, SessionId, SessionMessage, SessionReply};
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelResult, SupervisionPolicy,
};
use uuid::Uuid;

// ───────────────────────────────────────────────
// Mock Agent：脚本化回信 + 闸门 + 中断
// ───────────────────────────────────────────────

enum Scripted {
    /// 挂起至闸门放行（1=Success(text)，2=Cancelled），模拟长回合
    Gated(&'static str),
    Success(&'static str),
    Empty,
    Busy,
    Fail,
    Cancel,
    /// 不回信（测 invoke 超时）
    Hang,
}

struct AgentInner {
    chats: Mutex<Vec<(SessionId, String)>>,
    interrupts: Mutex<Vec<SessionId>>,
    script: Mutex<VecDeque<Scripted>>,
    /// 闸门：0=等待 1=放行 2=取消
    gate_tx: watch::Sender<u8>,
    /// Interrupt 到达时不回信（卡住 driver，测队列满）
    hang_interrupts: AtomicBool,
}

#[derive(Clone)]
struct MockAgent {
    id: CapabilityId,
    inner: Arc<AgentInner>,
}

impl MockAgent {
    fn new(script: Vec<Scripted>) -> Self {
        let (gate_tx, _) = watch::channel(0);
        Self {
            id: CapabilityId::new(),
            inner: Arc::new(AgentInner {
                chats: Mutex::new(Vec::new()),
                interrupts: Mutex::new(Vec::new()),
                script: Mutex::new(script.into()),
                gate_tx,
                hang_interrupts: AtomicBool::new(false),
            }),
        }
    }

    fn set_gate(&self, state: u8) {
        self.inner.gate_tx.send(state).unwrap();
    }

    fn set_hang_interrupts(&self, hang: bool) {
        self.inner.hang_interrupts.store(hang, Ordering::SeqCst);
    }

    fn chats(&self) -> Vec<(SessionId, String)> {
        self.inner.chats.lock().clone()
    }

    fn chat_texts(&self) -> Vec<String> {
        self.chats().into_iter().map(|(_, text)| text).collect()
    }

    fn interrupts(&self) -> usize {
        self.inner.interrupts.lock().len()
    }
}

fn success_reply(session: SessionId, text: &str) -> SessionReply {
    SessionReply::Success {
        id: session.to_string(),
        model: "mock".into(),
        message: Box::new(Message::assistant(text)),
        finish_reason: FinishReason::Stop,
        usage: None,
    }
}

#[async_trait]
impl Extension for MockAgent {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let Ok(msg) = SessionMessage::from_envelope(&env) else {
            return Ok(());
        };
        match msg {
            SessionMessage::Chat {
                session_id,
                payload,
            } => {
                let text = payload.message.content.as_text().unwrap_or("").to_owned();
                self.inner.chats.lock().push((session_id, text));
                let scripted = self.inner.script.lock().pop_front();
                match scripted {
                    Some(Scripted::Success(text)) => {
                        let _ = ctx.reply(success_reply(session_id, text).to_envelope());
                    }
                    Some(Scripted::Gated(text)) => {
                        let inner = self.inner.clone();
                        tokio::spawn(async move {
                            let mut gate = inner.gate_tx.subscribe();
                            let reply = loop {
                                match *gate.borrow() {
                                    1 => break success_reply(session_id, text),
                                    2 => break SessionReply::Cancelled,
                                    _ => {}
                                }
                                if gate.changed().await.is_err() {
                                    break SessionReply::Cancelled;
                                }
                            };
                            let _ = ctx.reply(reply.to_envelope());
                        });
                    }
                    Some(Scripted::Empty) | None => {
                        let _ = ctx.reply(success_reply(session_id, "").to_envelope());
                    }
                    Some(Scripted::Busy) => {
                        let _ = ctx.reply(SessionReply::Busy { turn_id: 0 }.to_envelope());
                    }
                    Some(Scripted::Fail) => {
                        let _ = ctx.reply(
                            SessionReply::Error {
                                message: "mock 失败".into(),
                                kind: ErrorKind::Internal,
                                retry_after_ms: None,
                            }
                            .to_envelope(),
                        );
                    }
                    Some(Scripted::Cancel) => {
                        let _ = ctx.reply(SessionReply::Cancelled.to_envelope());
                    }
                    // Hang：handle 立即返回（不阻塞邮箱，同真实 AgentRuntime 的
                    // spawn 模式），回信通道由挂起任务持有——ctx 存活则 invoke
                    // 保持等待、走真实超时路径（ctx 丢弃会立即 TargetUnreachable）
                    Some(Scripted::Hang) => {
                        tokio::spawn(async move {
                            let _hold = ctx;
                            std::future::pending::<()>().await;
                        });
                    }
                }
            }
            SessionMessage::Interrupt { session_id } => {
                self.inner.interrupts.lock().push(session_id);
                if self.inner.hang_interrupts.load(Ordering::SeqCst) {
                    // 同 Hang：挂起任务持有回信通道，driver 的 invoke 保持等待
                    tokio::spawn(async move {
                        let _hold = ctx;
                        std::future::pending::<()>().await;
                    });
                } else {
                    // 模拟真实引擎：中断取消运行中的回合（闸门置 2 → Gated 回 Cancelled）
                    let _ = self.inner.gate_tx.send(2);
                    let _ = ctx.reply(SessionReply::Cancelled.to_envelope());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

// ───────────────────────────────────────────────
// Mock Host：记录出站命令 + 受理回执
// ───────────────────────────────────────────────

#[derive(Clone)]
struct RecordingHost {
    id: CapabilityId,
    sends: Arc<Mutex<Vec<String>>>,
    systems: Arc<Mutex<Vec<String>>>,
}

impl RecordingHost {
    fn new() -> Self {
        Self {
            id: CapabilityId::new(),
            sends: Arc::new(Mutex::new(Vec::new())),
            systems: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn text_of(cmd: &OutboundCommand) -> String {
        match &cmd.content {
            ChannelContent::Text(text) => text.clone(),
            _ => "<media>".into(),
        }
    }
}

#[async_trait]
impl Extension for RecordingHost {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let kind = env.metadata.get("kind").map(String::as_str);
        if let Ok(cmd) = OutboundCommand::from_envelope(&env) {
            match kind {
                Some(kind::SEND) => self.sends.lock().push(Self::text_of(&cmd)),
                Some(kind::SYSTEM) => self.systems.lock().push(Self::text_of(&cmd)),
                _ => {}
            }
        }
        let _ = ctx.reply(
            SendReceipt {
                accepted: true,
                queue_depth: 0,
            }
            .to_envelope(),
        );
        Ok(())
    }
}

// ───────────────────────────────────────────────
// 夹具
// ───────────────────────────────────────────────

struct Fixture {
    kernel: Kernel,
    router_id: CapabilityId,
    host_id: CapabilityId,
    agent: MockAgent,
    host: RecordingHost,
}

async fn setup(
    agent: MockAgent,
    batch: BatchConfig,
    task_queue: usize,
    concurrency: usize,
    keywords: &[&str],
    chat_timeout_ms: u64,
    send_timeout_ms: u64,
) -> Fixture {
    let kernel = Kernel::new();
    let host = RecordingHost::new();
    kernel
        .register(Box::new(host.clone()), 16, SupervisionPolicy::Transient)
        .await
        .unwrap();
    kernel
        .register(Box::new(agent.clone()), 16, SupervisionPolicy::Transient)
        .await
        .unwrap();
    let router = ImRouter::new(
        kernel.clone(),
        ImRouterConfig {
            hosts: vec![host.id()],
            agent: agent.id(),
            batch,
            concurrency,
            task_queue,
            chat_timeout_ms,
            send_timeout_ms,
            interrupt_keywords: keywords.iter().map(|k| k.to_string()).collect(),
        },
    );
    let router_id = router.id();
    kernel
        .register(Box::new(router), 16, SupervisionPolicy::Transient)
        .await
        .unwrap();
    Fixture {
        kernel,
        router_id,
        host_id: host.id(),
        agent,
        host,
    }
}

fn batch_cfg(idle_s: u64, max_messages: usize, window_s: u64) -> BatchConfig {
    BatchConfig {
        idle_window: Duration::from_secs(idle_s),
        max_messages,
        max_window: Duration::from_secs(window_s),
    }
}

async fn feed(fx: &Fixture, peer: &str, text: &str) {
    let msg = InboundMessage {
        endpoint: "wechat/bid-1".into(),
        peer: peer.into(),
        message_id: Uuid::new_v4().to_string(),
        content: ChannelContent::Text(text.into()),
        session_ctx: "T".into(),
        occurred_at: 0,
        raw: None,
    };
    let env = msg.to_envelope();
    // 连发可能打满 router 邮箱（内核背压生效）——让出调度等消化后重试
    loop {
        match fx.kernel.emit(fx.router_id, env.clone()).await {
            Ok(()) => return,
            Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
        }
    }
}

/// 连发 count 条（触发条数上限即时闭合的常用量）
async fn burst(fx: &Fixture, peer: &str, text: &str, count: usize) {
    for _ in 0..count {
        feed(fx, peer, text).await;
    }
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..300 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not met within 3s (virtual): {what}");
}

// ───────────────────────────────────────────────
// A4-1：批次——静默闭合、计时随每条重置
// ───────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn batch_idle_close_merges_and_timer_resets() {
    let fx = setup(
        MockAgent::new(vec![]),
        batch_cfg(8, 10, 30),
        8,
        3,
        &[],
        10_000,
        2_000,
    )
    .await;
    feed(&fx, "u1", "你好").await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    feed(&fx, "u1", "帮我查股票").await;

    // 第二条后仅 4.5s（总 t=8.5s）：静默未满，不闭合
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    assert!(fx.agent.chats().is_empty(), "静默窗口未满不得闭合");

    // 第二条后 8.5s（总 t=12.5s）：计时随每条重置后到期闭合
    tokio::time::sleep(Duration::from_secs(4)).await;
    eventually("批次闭合并派发", || fx.agent.chats().len() == 1).await;
    assert_eq!(fx.agent.chat_texts()[0], "你好\n帮我查股票");
}

// ───────────────────────────────────────────────
// A4-1：批次——条数上限即时闭合（不等静默）
// ───────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn batch_count_cap_closes_immediately() {
    let fx = setup(
        MockAgent::new(vec![]),
        batch_cfg(8, 10, 30),
        8,
        3,
        &[],
        10_000,
        2_000,
    )
    .await;
    burst(&fx, "u1", "m", 10).await;

    // 无任何静默等待，10 条即时闭合成一个任务
    eventually("条数上限即时闭合", || fx.agent.chats().len() == 1).await;
    assert_eq!(fx.agent.chat_texts()[0].split('\n').count(), 10);

    // 第 11 条进入新批次，等静默后才闭合
    feed(&fx, "u1", "m11").await;
    assert_eq!(fx.agent.chats().len(), 1);
    tokio::time::sleep(Duration::from_millis(8_500)).await;
    eventually("残余批次静默闭合", || fx.agent.chats().len() == 2).await;
    assert_eq!(fx.agent.chat_texts()[1], "m11");
}

// ───────────────────────────────────────────────
// A4-1：批次——总窗上限到期闭合（覆盖静默重置，防饥饿）
// ───────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn batch_window_cap_overrides_idle_reset() {
    let fx = setup(
        MockAgent::new(vec![]),
        batch_cfg(8, 100, 3),
        8,
        3,
        &[],
        10_000,
        2_000,
    )
    .await;
    for i in 1..=3 {
        feed(&fx, "u1", &format!("w{i}")).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // 每条间隔 1s（静默计时不断重置），但总窗 3s 到期强制闭合
    eventually("总窗到期闭合", || fx.agent.chats().len() == 1).await;
    assert_eq!(fx.agent.chat_texts()[0].split('\n').count(), 3);
}

// ───────────────────────────────────────────────
// A4-2：任务队列满 → im.system 拒绝提示，原任务不入队
// ───────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn queue_full_sends_reject_notice() {
    let agent = MockAgent::new(vec![Scripted::Gated(""), Scripted::Empty]);
    // send_timeout 放大：driver 卡在 Interrupt 的整个测试窗口内不解卡，
    // 队列（容量 1）才能保持占用供第三批触发 Full
    let fx = setup(agent, batch_cfg(8, 10, 30), 1, 3, &["停"], 10_000, 60_000).await;

    burst(&fx, "u1", "任务", 10).await; // 即时闭合 → 回合运行中
    eventually("u1 回合运行", || fx.agent.chats().len() == 1).await;
    fx.agent.set_hang_interrupts(true);

    burst(&fx, "u1", "停下", 10).await; // 关键字批 → driver 转发 Interrupt 并卡住
    eventually("Interrupt 已转发", || fx.agent.interrupts() == 1).await;

    burst(&fx, "u2", "A", 10).await; // 占满队列（容量 1）
    burst(&fx, "u3", "B", 10).await; // 队列满 → 拒绝提示
    eventually("拒绝提示送达", || !fx.host.systems.lock().is_empty()).await;
    let systems = fx.host.systems.lock().clone();
    assert!(systems.iter().any(|text| text.contains("任务较多")));
}

// ───────────────────────────────────────────────
// A4-3：会话道串行 + 跨会话并行 + 全局信号量
// ───────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn lanes_serialize_and_semaphore_limits() {
    let agent = MockAgent::new(vec![
        Scripted::Gated(""), // u1 第 1 批
        Scripted::Gated(""), // u2
        Scripted::Gated(""), // u3
        Scripted::Empty,     // u4（信号量释放后获得）
        // u1 第 2 批脚本耗尽 → 默认空 Success
    ]);
    let fx = setup(agent, batch_cfg(8, 10, 30), 8, 3, &[], 10_000, 2_000).await;

    burst(&fx, "u1", "一", 10).await; // u1 任务 1 运行（Gated）
    eventually("u1 任务 1 运行", || fx.agent.chats().len() == 1).await;
    feed(&fx, "u1", "二").await; // u1 任务 2（静默闭合前先挂起）
    tokio::time::sleep(Duration::from_millis(8_500)).await; // 静默闭合 → 同道排队
    tokio::time::sleep(Duration::from_millis(500)).await;

    burst(&fx, "u2", "甲", 10).await; // 不同会话：并行派发
    burst(&fx, "u3", "乙", 10).await;
    eventually("三会话并行", || fx.agent.chats().len() == 3).await;

    burst(&fx, "u4", "丙", 10).await; // 信号量已满（3）：等待
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(fx.agent.chats().len(), 3, "并发上限 3，第 4 个必须等待");
    // 同会话道串行：u1 第二个任务在第一个回信之前绝不被 invoke
    assert!(!fx.agent.chat_texts().iter().any(|t| t.contains("二")));

    fx.agent.set_gate(1); // 放行：u1/u2/u3 完成，u4 与 u1 任务 2 依次获得许可
    eventually("全部 5 个回合完成", || fx.agent.chats().len() == 5).await;
    let texts = fx.agent.chat_texts();
    assert!(texts.iter().any(|t| t == "二"), "同道任务在先行回合结束后才执行");
    assert_eq!(texts.iter().filter(|t| t.contains("甲")).count(), 1);
}

// ───────────────────────────────────────────────
// A4-4 交付契约
// ───────────────────────────────────────────────

/// Success 非空 → 兜底恰好一次；回合内工具回执与最终输出各恰好一次
#[tokio::test(start_paused = true)]
async fn deliver_fallback_exactly_once_alongside_tool_send() {
    let agent = MockAgent::new(vec![Scripted::Gated("最终结果")]);
    let fx = setup(agent, batch_cfg(8, 10, 30), 8, 3, &[], 10_000, 2_000).await;
    burst(&fx, "u1", "查股票", 10).await;
    eventually("回合运行", || fx.agent.chats().len() == 1).await;

    // 模拟回合内工具回执（阶段 5 的 im_send_text 同此路径）
    let tool_cmd = OutboundCommand {
        endpoint: "wechat/bid-1".into(),
        peer: "u1".into(),
        content: ChannelContent::Text("正在查询…".into()),
    };
    let resp = fx
        .kernel
        .invoke(
            fx.host_id,
            tool_cmd.to_send_envelope(Uuid::new_v4(), Some(1)),
            2_000,
        )
        .await
        .unwrap();
    assert!(SendReceipt::from_envelope(&resp).unwrap().accepted);

    fx.agent.set_gate(1); // 回合完成 → 兜底交付最终输出
    eventually("兜底送达", || fx.host.sends.lock().len() == 2).await;
    let sends = fx.host.sends.lock().clone();
    assert!(sends.contains(&"正在查询…".to_owned()));
    assert!(sends.contains(&"最终结果".to_owned()));
}

#[tokio::test(start_paused = true)]
async fn empty_success_and_cancelled_send_nothing() {
    let fx = setup(
        MockAgent::new(vec![Scripted::Empty]),
        batch_cfg(8, 10, 30),
        8,
        3,
        &[],
        10_000,
        2_000,
    )
    .await;
    burst(&fx, "u1", "hi", 10).await;
    eventually("回合完成", || fx.agent.chats().len() == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fx.host.sends.lock().is_empty(), "空文本无兜底");
    assert!(fx.host.systems.lock().is_empty());
}

#[tokio::test(start_paused = true)]
async fn error_reply_notifies_user() {
    let fx = setup(
        MockAgent::new(vec![Scripted::Fail]),
        batch_cfg(8, 10, 30),
        8,
        3,
        &[],
        10_000,
        2_000,
    )
    .await;
    burst(&fx, "u1", "炸一下", 10).await;
    eventually("失败通知", || fx.host.systems.lock().len() == 1).await;
    assert!(fx.host.sends.lock().is_empty());
    assert!(fx.host.systems.lock()[0].contains("任务失败"));
}

#[tokio::test(start_paused = true)]
async fn busy_requeues_then_delivers() {
    let fx = setup(
        MockAgent::new(vec![Scripted::Busy, Scripted::Success("好了")]),
        batch_cfg(8, 10, 30),
        8,
        3,
        &[],
        10_000,
        2_000,
    )
    .await;
    burst(&fx, "u1", "稍等重试", 10).await;
    eventually("重试后交付", || fx.host.sends.lock().len() == 1).await;
    assert_eq!(fx.host.sends.lock()[0], "好了");
    assert_eq!(fx.agent.chats().len(), 2, "Busy 一次后二次派发成功");
}

#[tokio::test(start_paused = true)]
async fn cancelled_reply_sends_nothing() {
    let fx = setup(
        MockAgent::new(vec![Scripted::Cancel]),
        batch_cfg(8, 10, 30),
        8,
        3,
        &[],
        10_000,
        2_000,
    )
    .await;
    burst(&fx, "u1", "取消我", 10).await;
    eventually("回合完成", || fx.agent.chats().len() == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fx.host.sends.lock().is_empty());
    assert!(fx.host.systems.lock().is_empty());
}

/// 回合 invoke 超时：按 Error 处理 + 会话道释放（后续任务可运行）
#[tokio::test(start_paused = true)]
async fn chat_timeout_releases_lane() {
    let agent = MockAgent::new(vec![Scripted::Hang, Scripted::Empty]);
    let fx = setup(agent, batch_cfg(8, 10, 30), 8, 3, &[], 300, 2_000).await; // 超时 300ms
    burst(&fx, "u1", "挂起", 10).await;
    eventually("超时按 Error 通知", || fx.host.systems.lock().len() == 1).await;

    // 会话道已释放：同 peer 后续任务正常派发（worker 不泄漏）
    feed(&fx, "u1", "再来").await;
    tokio::time::sleep(Duration::from_millis(8_500)).await;
    eventually("后续任务运行", || fx.agent.chats().len() == 2).await;
}

// ───────────────────────────────────────────────
// A4-5：中断关键字
// ───────────────────────────────────────────────

/// 会话在跑：关键字批 → Interrupt 转发 → Cancelled → 会话道释放
#[tokio::test(start_paused = true)]
async fn interrupt_keyword_cancels_running_turn() {
    let agent = MockAgent::new(vec![Scripted::Gated("做完了")]);
    let fx = setup(agent, batch_cfg(8, 10, 30), 8, 3, &["停"], 10_000, 2_000).await;
    burst(&fx, "u1", "长任务", 10).await;
    eventually("回合运行", || fx.agent.chats().len() == 1).await;

    burst(&fx, "u1", "停下", 10).await;
    eventually("Interrupt 转发", || fx.agent.interrupts() == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(fx.host.sends.lock().is_empty(), "取消后无兜底");

    // 会话道已释放：后续任务正常派发
    feed(&fx, "u1", "新任务").await;
    tokio::time::sleep(Duration::from_millis(8_500)).await;
    eventually("中断后新任务运行", || fx.agent.chats().len() == 2).await;
}

/// 会话未在跑：关键字批直接丢弃，不建会话不派发
#[tokio::test(start_paused = true)]
async fn interrupt_keyword_drops_when_idle() {
    let fx = setup(
        MockAgent::new(vec![]),
        batch_cfg(8, 10, 30),
        8,
        3,
        &["停"],
        10_000,
        2_000,
    )
    .await;
    burst(&fx, "u1", "别做了停下", 10).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(fx.agent.chats().is_empty(), "未运行的会话，关键字批直接丢弃");
    assert_eq!(fx.agent.interrupts(), 0);
    assert!(fx.host.sends.lock().is_empty());
}

//! ImRouter——Extension：串联批次累积 → 调度 → 交付契约（设计文档 §3.2/§4.6）。
//!
//! `handle` 只做 DashMap push 与 try_send（非阻塞）；等待型工作全部在
//! 后台 driver（sweeper + 队列消费）与调度 worker 里，经完整 Kernel 句柄 invoke。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::{mpsc, watch};

use referee_ai::{SessionId, SessionMessage, SessionReply};
use referee_core::{CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelResult};
use uuid::Uuid;

use crate::batch::{BatchAccumulator, BatchConfig, ClosedBatch};
use crate::dispatch::{Dispatcher, Task};
use crate::message::{
    kind, meta, ChannelContent, InboundMessage, OutboundCommand, PeerKey, SentNotice,
};
use crate::policy::hit_keyword;

/// 批次 sweeper 周期
const SWEEP_INTERVAL: Duration = Duration::from_millis(500);

pub struct ImRouterConfig {
    /// 出站 host——拒绝提示与兜底交付都发往此（多账号路由见 Phase 2）
    pub host: CapabilityId,
    pub agent: CapabilityId,
    pub batch: BatchConfig,
    /// 全局并发上限（同时进行的回合数）
    pub concurrency: usize,
    /// 任务队列容量（闭合批次受理上限，满则拒绝）
    pub task_queue: usize,
    pub chat_timeout_ms: u64,
    pub send_timeout_ms: u64,
    /// 中断关键字：闭合批次的合并文本命中即中断/丢弃该批
    pub interrupt_keywords: Vec<String>,
}

/// peer ↔ 会话 映射——router 惰性创建；阶段 5 的工具经 `peer_of` 反查收件人
#[derive(Clone, Default)]
pub struct SessionMap {
    by_peer: Arc<DashMap<PeerKey, SessionId>>,
    by_session: Arc<DashMap<SessionId, PeerKey>>,
}

impl SessionMap {
    /// 该 peer 的会话；不存在则创建（并发创建时单胜者，双射保持一致）
    pub fn session_of(&self, peer: &PeerKey) -> SessionId {
        if let Some(existing) = self.by_peer.get(peer) {
            return *existing;
        }
        let created = Uuid::new_v4();
        let entry = self.by_peer.entry(peer.clone()).or_insert(created);
        if *entry == created {
            self.by_session.insert(created, peer.clone());
        }
        *entry
    }

    /// 已有会话（不创建）——中断场景
    pub fn existing_session(&self, peer: &PeerKey) -> Option<SessionId> {
        self.by_peer.get(peer).map(|session| *session)
    }

    /// 工具侧反查：会话 → 对端
    pub fn peer_of(&self, session: &SessionId) -> Option<PeerKey> {
        self.by_session.get(session).map(|peer| peer.value().clone())
    }
}

/// driver 与 Extension 共享的核心状态
struct RouterCore {
    kernel: Kernel,
    agent: CapabilityId,
    host: CapabilityId,
    keywords: Vec<String>,
    send_timeout: Duration,
    sessions: SessionMap,
    batch: Arc<BatchAccumulator>,
    dispatcher: Dispatcher,
}

impl RouterCore {
    /// 闭合批次的处置：关键字 → 中断/丢弃；否则建会话并入道
    async fn submit_batch(&self, batch: ClosedBatch) {
        if hit_keyword(&batch.merged_text, &self.keywords) {
            self.interrupt(batch).await;
            return;
        }
        let session = self.sessions.session_of(&batch.peer);
        self.dispatcher
            .submit(Task::new(batch.peer, session, batch.merged_text));
    }

    /// 中断关键字命中：会话在跑则转发 Interrupt（会话道由该回合的 Chat 回信
    /// 自然释放）；未在跑或尚无会话则丢弃该批
    async fn interrupt(&self, batch: ClosedBatch) {
        let Some(session) = self.sessions.existing_session(&batch.peer) else {
            tracing::info!(peer = %batch.peer.peer, "中断关键字命中，无会话，丢弃该批");
            return;
        };
        if !self.dispatcher.lane_running(&batch.peer) {
            tracing::info!(peer = %batch.peer.peer, "中断关键字命中，会话未在运行，丢弃该批");
            return;
        }
        let request = SessionMessage::Interrupt { session_id: session }.to_envelope();
        match self
            .kernel
            .invoke(self.agent, request, self.send_timeout.as_millis() as u64)
            .await
        {
            Ok(env) => match SessionReply::from_envelope(&env) {
                Ok(SessionReply::Cancelled) => {
                    tracing::info!(peer = %batch.peer.peer, "已中断运行中任务")
                }
                Ok(other) => tracing::warn!(reply = ?other, "中断回信异常"),
                Err(e) => tracing::warn!(error = %e, "中断回信解码失败"),
            },
            Err(e) => tracing::warn!(error = ?e, peer = %batch.peer.peer, "Interrupt 转发失败"),
        }
    }

    /// 任务队列满时的拒绝提示（后台执行——handle 内不得 await invoke）
    async fn reject(&self, batch: &ClosedBatch) {
        let cmd = OutboundCommand {
            endpoint: batch.peer.endpoint.clone(),
            peer: batch.peer.peer.clone(),
            content: ChannelContent::Text("当前任务较多，请稍后再发送".into()),
        };
        if let Err(e) = self
            .kernel
            .invoke(self.host, cmd.to_system_envelope(), self.send_timeout.as_millis() as u64)
            .await
        {
            tracing::warn!(error = ?e, peer = %batch.peer.peer, "拒绝提示发送失败");
        }
    }
}

pub struct ImRouter {
    id: CapabilityId,
    core: Arc<RouterCore>,
    events_tx: mpsc::Sender<ClosedBatch>,
    shutdown_tx: watch::Sender<bool>,
}

impl ImRouter {
    /// 构造即启动后台 driver（sweeper + 任务队列消费），与注册顺序无关。
    /// 组装约束（§4.5）：router 需先于 host 注册，host 的 im.inbound 才不落 DLQ。
    pub fn new(kernel: Kernel, config: ImRouterConfig) -> Self {
        let host = config.host;
        let (events_tx, events_rx) = mpsc::channel(config.task_queue.max(1));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let core = Arc::new(RouterCore {
            batch: Arc::new(BatchAccumulator::new(config.batch.clone())),
            dispatcher: Dispatcher::new(
                kernel.clone(),
                config.agent,
                host,
                config.concurrency,
                Duration::from_millis(config.chat_timeout_ms),
                Duration::from_millis(config.send_timeout_ms),
            ),
            keywords: config.interrupt_keywords,
            sessions: SessionMap::default(),
            send_timeout: Duration::from_millis(config.send_timeout_ms),
            kernel,
            agent: config.agent,
            host,
        });
        let router = Self {
            id: CapabilityId::new(),
            core: core.clone(),
            events_tx: events_tx.clone(),
            shutdown_tx,
        };
        tokio::spawn(async move {
            driver(core, events_tx, events_rx, shutdown_rx).await;
        });
        router
    }

    /// 与 im_send_text 工具共享的会话映射（阶段 5）
    pub fn session_map(&self) -> SessionMap {
        self.core.sessions.clone()
    }

    /// 闭合批次受理（handle 即时闭合路径；非阻塞，满则 spawn 拒绝提示）
    fn submit_closed(&self, batch: ClosedBatch) {
        if let Err(mpsc::error::TrySendError::Full(batch)) = self.events_tx.try_send(batch) {
            tracing::warn!(peer = %batch.peer.peer, "任务队列已满，拒绝该批");
            let core = self.core.clone();
            tokio::spawn(async move {
                core.reject(&batch).await;
            });
        }
    }
}

/// 后台 driver：消费闭合批次 + 周期扫描到期批次。
/// 两条路径的批次统一经有界队列受理（满即拒绝）——有界保证对全部闭合来源生效。
async fn driver(
    core: Arc<RouterCore>,
    events_tx: mpsc::Sender<ClosedBatch>,
    mut events_rx: mpsc::Receiver<ClosedBatch>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
    loop {
        tokio::select! {
            batch = events_rx.recv() => match batch {
                Some(batch) => core.submit_batch(batch).await,
                None => break,
            },
            _ = sweep.tick() => {
                for batch in core.batch.close_due() {
                    if let Err(mpsc::error::TrySendError::Full(batch)) = events_tx.try_send(batch) {
                        tracing::warn!(peer = %batch.peer.peer, "任务队列已满，拒绝该批");
                        core.reject(&batch).await;
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    // 停机：未闭合批次丢弃并记录（Phase 2 任务日志接管恢复）
    for batch in core.batch.close_all() {
        tracing::info!(peer = %batch.peer.peer, count = batch.message_count, "停机丢弃未闭合批次");
    }
}

#[async_trait]
impl Extension for ImRouter {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        match env.metadata.get(meta::KIND).map(String::as_str) {
            Some(kind::INBOUND) => match InboundMessage::from_envelope(&env) {
                Ok(msg) => {
                    if let ChannelContent::Text(text) = &msg.content {
                        if let Some(closed) = self.core.batch.push(&msg.peer_key(), text) {
                            self.submit_closed(closed);
                        }
                    } else {
                        tracing::debug!(peer = %msg.peer, "非文本入站暂不支持（Phase 2 媒体）");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "im.inbound 解码失败，丢弃"),
            },
            Some(kind::SENT) => match SentNotice::from_envelope(&env) {
                // 仅观测归因，不参与控制流（§4.6a）
                Ok(notice) => tracing::debug!(
                    session = %notice.session_id,
                    turn = notice.turn_id,
                    peer = %notice.peer,
                    "im.sent"
                ),
                Err(e) => tracing::warn!(error = %e, "im.sent 解码失败"),
            },
            other => tracing::warn!(kind = ?other, "ImRouter 收到未知 kind，忽略"),
        }
        Ok(())
    }

    async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

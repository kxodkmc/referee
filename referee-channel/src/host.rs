//! ChannelHost — 每通道账号一个的 Extension — 设计文档 §4.6b/§4.6d
//!
//! 职责：受理出站（有界队列 try_send，满即显式拒绝）、搬运入站
//! （emit 给 router，被拒消息由内核落 DLQ）、监督 adapter（退避重启 →
//! 超限降级）。`handle` 只做 try_send / emit / reply，天然非阻塞。

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use referee_core::{CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelResult};

use crate::adapter::{ChannelAdapter, ChannelIo};
use crate::error::ChannelError;
use crate::message::{kind, meta, InboundMessage, OutboundCommand, SendReceipt, SentNotice};

/// adapter 终止后的退避重启上限（不含首次运行）；超过即降级
const MAX_RESTARTS: u32 = 3;
const BACKOFF_BASE: Duration = Duration::from_millis(100);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

pub struct ChannelHost<A: ChannelAdapter> {
    id: CapabilityId,
    adapter: Arc<A>,
    outbound_capacity: usize,
    /// 每次 adapter 运行尝试换新通道（panic 会连带消费掉旧 Receiver）；
    /// start() 之前指向一个已关闭通道，try_send 返回 Closed = 未启动拒绝
    outbound_tx: Arc<Mutex<mpsc::Sender<OutboundCommand>>>,
    inbound_tx: mpsc::Sender<InboundMessage>,
    inbound_rx: Arc<Mutex<Option<mpsc::Receiver<InboundMessage>>>>,
    shutdown_tx: watch::Sender<bool>,
    router_id: Arc<OnceLock<CapabilityId>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    degraded: Arc<AtomicBool>,
    run_attempts: Arc<AtomicU32>,
}

/// 克隆共享同一底层——注册副本与控制副本同源（§4.5 组装：register 后仍可 start）
impl<A: ChannelAdapter> Clone for ChannelHost<A> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            adapter: self.adapter.clone(),
            outbound_capacity: self.outbound_capacity,
            outbound_tx: self.outbound_tx.clone(),
            inbound_tx: self.inbound_tx.clone(),
            inbound_rx: self.inbound_rx.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            router_id: self.router_id.clone(),
            tasks: self.tasks.clone(),
            degraded: self.degraded.clone(),
            run_attempts: self.run_attempts.clone(),
        }
    }
}

impl<A: ChannelAdapter + 'static> ChannelHost<A> {
    pub fn new(adapter: A, inbound_capacity: usize, outbound_capacity: usize) -> Self {
        let (inbound_tx, inbound_rx) = mpsc::channel(inbound_capacity);
        let (outbound_tx, outbound_rx) = mpsc::channel(outbound_capacity);
        drop(outbound_rx);
        // 各任务的 shutdown 接收端在 start() 时经 subscribe() 派生
        let (shutdown_tx, _initial_rx) = watch::channel(false);
        Self {
            id: CapabilityId::new(),
            adapter: Arc::new(adapter),
            outbound_capacity,
            outbound_tx: Arc::new(Mutex::new(outbound_tx)),
            inbound_tx,
            inbound_rx: Arc::new(Mutex::new(Some(inbound_rx))),
            shutdown_tx,
            router_id: Arc::new(OnceLock::new()),
            tasks: Arc::new(Mutex::new(Vec::new())),
            degraded: Arc::new(AtomicBool::new(false)),
            run_attempts: Arc::new(AtomicU32::new(0)),
        }
    }

    /// adapter.run 启动总次数（含首次）——监督行为的观测点
    pub fn run_attempts(&self) -> u32 {
        self.run_attempts.load(Ordering::Relaxed)
    }

    pub fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }

    /// 注册完成后启动收发循环。幂等：二次调用直接返回。
    /// 首个出站通道在此同步建立——start 返回后受理即刻生效，无竞态窗口。
    pub fn start(&self, kernel: Kernel, router_id: CapabilityId) {
        let _ = self.router_id.set(router_id);
        let Some(inbound_rx) = self.inbound_rx.lock().take() else {
            return;
        };
        let (outbound_tx, first_rx) = mpsc::channel(self.outbound_capacity);
        *self.outbound_tx.lock() = outbound_tx;
        let mut tasks = self.tasks.lock();
        tasks.push(tokio::spawn(supervise_adapter(
            self.adapter.clone(),
            self.inbound_tx.clone(),
            self.outbound_tx.clone(),
            self.outbound_capacity,
            Some(first_rx),
            self.shutdown_tx.subscribe(),
            self.degraded.clone(),
            self.run_attempts.clone(),
        )));
        tasks.push(tokio::spawn(pump_inbound(
            kernel,
            router_id,
            inbound_rx,
            self.shutdown_tx.subscribe(),
        )));
    }

    /// 受理出站命令；返回 (回执, 错误说明)。非阻塞：队列满/降级均立即拒绝。
    fn accept_outbound(&self, cmd: OutboundCommand) -> (SendReceipt, Option<ChannelError>) {
        if self.degraded.load(Ordering::Acquire) {
            return reject(ChannelError::Adapter("adapter degraded".into()));
        }
        let sender = self.outbound_tx.lock().clone();
        match sender.try_send(cmd) {
            Ok(()) => {
                let queue_depth = self.outbound_capacity - sender.capacity();
                (
                    SendReceipt {
                        accepted: true,
                        queue_depth,
                    },
                    None,
                )
            }
            Err(TrySendError::Full(_)) => reject(ChannelError::Rejected),
            Err(TrySendError::Closed(_)) => {
                reject(ChannelError::Adapter("outbound channel closed".into()))
            }
        }
    }
}

fn reject(error: ChannelError) -> (SendReceipt, Option<ChannelError>) {
    (
        SendReceipt {
            accepted: false,
            queue_depth: 0,
        },
        Some(error),
    )
}

/// im.send 信封附带的回合归因（工具写入）
fn attribution(env: &Envelope) -> Option<(Uuid, u64)> {
    let session_id = env.metadata.get(meta::SESSION_ID)?;
    let turn_id = env.metadata.get(meta::TURN_ID)?;
    Some((session_id.parse().ok()?, turn_id.parse().ok()?))
}

#[async_trait]
impl<A: ChannelAdapter + 'static> Extension for ChannelHost<A> {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let kind = env.metadata.get(meta::KIND).map(String::as_str);
        let mut notice = None;
        let (receipt, error) = match kind {
            Some(k) if k == kind::SEND || k == kind::SYSTEM => {
                match OutboundCommand::from_envelope(&env) {
                    Ok(cmd) => {
                        if k == kind::SEND {
                            notice = attribution(&env).map(|(session_id, turn_id)| SentNotice {
                                endpoint: cmd.endpoint.clone(),
                                peer: cmd.peer.clone(),
                                session_id,
                                turn_id,
                            });
                        }
                        self.accept_outbound(cmd)
                    }
                    Err(e) => reject(e),
                }
            }
            other => reject(ChannelError::Decode(format!("unexpected kind {other:?}"))),
        };

        // 受理成功的 im.send → 先发观测通知再回信（emit 先于 reply，§4.6a）
        if receipt.accepted {
            if let (Some(notice), Some(router)) = (notice, self.router_id.get()) {
                if let Err(e) = ctx.emit(*router, notice.to_envelope()).await {
                    tracing::warn!(error = ?e, peer = %notice.peer, "im.sent emit failed");
                }
            }
        }

        let mut reply = receipt.to_envelope();
        if let Some(error) = error {
            reply.metadata.insert(meta::ERROR.to_owned(), error.to_string());
        }
        let _ = ctx.reply(reply);
        Ok(())
    }

    async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
        let mut tasks = self.tasks.lock().drain(..).collect::<Vec<_>>();
        for task in tasks.iter_mut() {
            let remain = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remain.is_zero() {
                break;
            }
            let _ = tokio::time::timeout(remain, task).await;
        }
        if let Err(e) = self.adapter.state().flush().await {
            tracing::error!(error = %e, "channel adapter state flush failed");
        }
    }
}

/// adapter 监督：每次尝试独立 spawn（panic 表现为 JoinError，不波及本任务），
/// 退避重启至多 MAX_RESTARTS 次，超限置 degraded。停机信号触发的退出视为正常。
/// 首次运行复用 start() 同步建立的出站通道；重启时换新（旧 Receiver 已随 panic 消亡）。
#[allow(clippy::too_many_arguments)]
async fn supervise_adapter<A: ChannelAdapter + 'static>(
    adapter: Arc<A>,
    inbound_tx: mpsc::Sender<InboundMessage>,
    outbound_tx: Arc<Mutex<mpsc::Sender<OutboundCommand>>>,
    outbound_capacity: usize,
    mut first_rx: Option<mpsc::Receiver<OutboundCommand>>,
    mut shutdown: watch::Receiver<bool>,
    degraded: Arc<AtomicBool>,
    run_attempts: Arc<AtomicU32>,
) {
    let mut restarts = 0u32;
    let mut backoff = BACKOFF_BASE;
    loop {
        run_attempts.fetch_add(1, Ordering::Relaxed);
        let outbound_rx = match first_rx.take() {
            Some(rx) => rx,
            None => {
                let (tx, rx) = mpsc::channel(outbound_capacity);
                *outbound_tx.lock() = tx;
                rx
            }
        };
        let io = ChannelIo {
            inbound_tx: inbound_tx.clone(),
            outbound_rx,
            shutdown: shutdown.clone(),
        };
        let runner = {
            let adapter = adapter.clone();
            tokio::spawn(async move { adapter.run(io).await })
        };
        let outcome = runner.await;
        if *shutdown.borrow() {
            break;
        }
        match outcome {
            Err(join) => tracing::warn!(panic = join.is_panic(), "channel adapter task died"),
            Ok(Err(e)) => tracing::warn!(error = %e, "channel adapter exited with error"),
            Ok(Ok(())) => tracing::warn!("channel adapter exited before shutdown"),
        }
        if restarts >= MAX_RESTARTS {
            degraded.store(true, Ordering::Release);
            tracing::error!(restarts, "channel adapter exhausted restarts, host degraded");
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => break,
        }
        restarts += 1;
        backoff *= 2;
    }
}

/// 入站搬运：有界入站通道 → emit(im.inbound) 给 router。
/// emit 被拒的消息由内核落 DLQ，这里只记 warn 并继续。
async fn pump_inbound(
    kernel: Kernel,
    router_id: CapabilityId,
    mut inbound_rx: mpsc::Receiver<InboundMessage>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            msg = inbound_rx.recv() => {
                let Some(msg) = msg else { break };
                if let Err(e) = kernel.emit(router_id, msg.to_envelope()).await {
                    tracing::warn!(error = ?e, endpoint = %msg.endpoint, peer = %msg.peer,
                        "im.inbound rejected (kernel DLQ'd)");
                }
            }
        }
    }
}

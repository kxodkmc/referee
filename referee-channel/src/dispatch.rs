//! 任务调度——会话道 FIFO + 全局并发上限 + 回信处置（交付契约执行处）。
//!
//! 会话道：同 peer 严格串行（running 标志 + pending 队列），不同 peer 并行；
//! 全局 `Semaphore(concurrency)` 限制同时进行的回合数。取下一个任务的
//! 判空与置 running=false 在同一临界区内——与 submit 的检查/入队无交错窗口，
//! 杜绝「已入队却无人消费」的丢唤醒。

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use referee_ai::{ChatOptions, ChatPayload, Message, SessionId, SessionMessage};
use referee_core::{CapabilityId, Envelope, Kernel, KernelError};

use crate::message::{ChannelContent, OutboundCommand, PeerKey, SendReceipt};
use crate::policy::{disposition, TurnDisposition};

/// 一个已受理的任务（= 一个闭合批次）
#[derive(Debug, Clone)]
pub struct Task {
    pub peer: PeerKey,
    pub session_id: SessionId,
    pub text: String,
    busy_retries: u8,
}

impl Task {
    pub fn new(peer: PeerKey, session_id: SessionId, text: String) -> Self {
        Self {
            peer,
            session_id,
            text,
            busy_retries: 0,
        }
    }
}

const MAX_BUSY_RETRIES: u8 = 2;
const BUSY_RETRY_DELAY: Duration = Duration::from_millis(500);

#[derive(Default)]
struct Lane {
    pending: VecDeque<Task>,
    running: bool,
}

/// 调度器——字段全部 Arc/Clone 共享，worker 持副本跨 await
#[derive(Clone)]
pub struct Dispatcher {
    kernel: Kernel,
    agent: CapabilityId,
    host: CapabilityId,
    lanes: Arc<DashMap<PeerKey, Arc<Mutex<Lane>>>>,
    semaphore: Arc<Semaphore>,
    chat_timeout: Duration,
    send_timeout: Duration,
}

/// host invoke 失败原因——`Kernel`(上游不可达)与 `Decode`(回信解码失败)严格区分，
/// 避免解码 BUG 被误诊断成"队列满/降级"而排查方向错误。
#[derive(Debug)]
enum InvokeError {
    Kernel(KernelError),
    Decode(String),
}

impl Dispatcher {
    pub fn new(
        kernel: Kernel,
        agent: CapabilityId,
        host: CapabilityId,
        concurrency: usize,
        chat_timeout: Duration,
        send_timeout: Duration,
    ) -> Self {
        Self {
            kernel,
            agent,
            host,
            lanes: Arc::new(DashMap::new()),
            semaphore: Arc::new(Semaphore::new(concurrency.max(1))),
            chat_timeout,
            send_timeout,
        }
    }

    /// 该 peer 是否有回合在跑（中断场景判定）
    pub fn lane_running(&self, peer: &PeerKey) -> bool {
        self.lanes
            .get(peer)
            .map(|lane| lane.lock().running)
            .unwrap_or(false)
    }

    /// 任务入道：道在跑则排队，否则接管 running 并 spawn worker
    pub fn submit(&self, task: Task) {
        let lane = self.lanes.entry(task.peer.clone()).or_default().clone();
        let mut guard = lane.lock();
        if guard.running {
            guard.pending.push_back(task);
        } else {
            guard.running = true;
            drop(guard);
            let dispatcher = self.clone();
            tokio::spawn(async move {
                run_lane(dispatcher, lane, task).await;
            });
        }
    }

    /// 兜底交付：im.send 发送最终输出（turn 未知 → 无 im.sent 归因）。
    /// 出口唯一化（§4.6a）：无论模型是否用工具发过消息，最终输出都走这里。
    async fn deliver(&self, task: &Task, text: &str) {
        let cmd = outbound(task, ChannelContent::Text(text.to_owned()));
        let env = cmd.to_send_envelope(task.session_id, None);
        match self.invoke_host(env).await {
            Ok(receipt) if receipt.accepted => {}
            Ok(receipt) => tracing::warn!(
                peer = %task.peer.peer,
                queue_depth = receipt.queue_depth,
                "兜底交付未被受理（出站队列满/降级）；Phase 2 补投接管"
            ),
            Err(InvokeError::Decode(e)) => tracing::error!(
                peer = %task.peer.peer,
                error = %e,
                "host 回信解码失败，受理状态未知——非队列满"
            ),
            Err(InvokeError::Kernel(e)) => tracing::warn!(
                peer = %task.peer.peer,
                error = ?e,
                "host invoke 失败"
            ),
        }
    }

    /// 系统提示（拒绝/失败通知），同样走出站队列
    async fn notify(&self, task: &Task, text: &str) {
        let cmd = outbound(task, ChannelContent::Text(text.to_owned()));
        match self.invoke_host(cmd.to_system_envelope()).await {
            Ok(_) => {}
            Err(InvokeError::Decode(e)) => tracing::error!(
                peer = %task.peer.peer,
                error = %e,
                "im.system 回信解码失败"
            ),
            Err(InvokeError::Kernel(e)) => tracing::warn!(
                peer = %task.peer.peer,
                error = ?e,
                "im.system 通知失败"
            ),
        }
    }

    /// 回信状态：解码失败时不确定是否已受理，单独成支，不折叠成"未受理"
    async fn invoke_host(&self, env: Envelope) -> Result<SendReceipt, InvokeError> {
        let resp = self
            .kernel
            .invoke(self.host, env, self.send_timeout.as_millis() as u64)
            .await
            .map_err(InvokeError::Kernel)?;
        SendReceipt::from_envelope(&resp).map_err(|e| InvokeError::Decode(e.to_string()))
    }
}

/// 会话道 worker：串行处理本道任务，直至排空
async fn run_lane(dispatcher: Dispatcher, lane: Arc<Mutex<Lane>>, mut task: Task) {
    loop {
        let _permit = dispatcher
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore 未关闭");
        let chat = SessionMessage::Chat {
            session_id: task.session_id,
            payload: ChatPayload {
                message: Message::user(task.text.clone()),
                options: ChatOptions::default(),
                peer_depth: 0,
            },
        }
        .to_envelope();
        let reply = dispatcher
            .kernel
            .invoke(dispatcher.agent, chat, dispatcher.chat_timeout.as_millis() as u64)
            .await;
        drop(_permit);

        match disposition(&reply) {
            TurnDisposition::Deliver(text) => dispatcher.deliver(&task, &text).await,
            TurnDisposition::Notify(text) => dispatcher.notify(&task, &text).await,
            TurnDisposition::Skip => {
                tracing::info!(peer = %task.peer.peer, "回合无兜底输出（空文本/已取消）")
            }
            TurnDisposition::Busy => {
                if task.busy_retries < MAX_BUSY_RETRIES {
                    task.busy_retries += 1;
                    tracing::warn!(peer = %task.peer.peer, retry = task.busy_retries, "会话忙，回队重试");
                    tokio::time::sleep(BUSY_RETRY_DELAY).await;
                    let mut lane = lane.lock();
                    lane.pending.push_back(task.clone());
                } else {
                    dispatcher
                        .notify(&task, "当前有任务正在进行，请稍后再发送")
                        .await;
                }
            }
        }

        // 判空与置 running 同临界区，防与 submit 交错丢唤醒
        let next = {
            let mut lane = lane.lock();
            match lane.pending.pop_front() {
                Some(next) => Some(next),
                None => {
                    lane.running = false;
                    None
                }
            }
        };
        match next {
            Some(next) => task = next,
            None => break,
        }
    }
}

fn outbound(task: &Task, content: ChannelContent) -> OutboundCommand {
    OutboundCommand {
        endpoint: task.peer.endpoint.clone(),
        peer: task.peer.peer.clone(),
        content,
    }
}

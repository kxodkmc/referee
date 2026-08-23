//! 批次累积器——三条件闭合（设计文档 §5 阶段 4）：
//! ① 静默窗口（每条消息重置计时）；② 条数上限（触发即时闭合）；
//! ③ 总窗上限（自批内首条起算，防连续输入永不闭合的饥饿）。

use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;

use crate::message::{ChannelCapabilities, PeerKey};

#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// 静默闭合窗口（每条重置）
    pub idle_window: Duration,
    /// 单批条数上限
    pub max_messages: usize,
    /// 总窗上限（自首条起算）
    pub max_window: Duration,
}

impl BatchConfig {
    pub fn from_capabilities(caps: &ChannelCapabilities) -> Self {
        Self {
            idle_window: Duration::from_millis(caps.batch_idle_window_ms),
            max_messages: caps.max_batch_messages,
            max_window: Duration::from_millis(caps.max_batch_window_ms),
        }
    }
}

/// 闭合的批次——合并文本即一个任务的输入
#[derive(Debug, Clone)]
pub struct ClosedBatch {
    pub peer: PeerKey,
    /// 多条消息以换行拼接
    pub merged_text: String,
    pub message_count: usize,
}

#[derive(Debug)]
struct PendingBatch {
    texts: Vec<String>,
    first_at: Instant,
    last_at: Instant,
}

pub struct BatchAccumulator {
    config: BatchConfig,
    pending: DashMap<PeerKey, PendingBatch>,
}

impl BatchAccumulator {
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            pending: DashMap::new(),
        }
    }

    /// 压入一条消息。条数达到上限时返回立即闭合的批次（不等 sweeper）。
    pub fn push(&self, peer: &PeerKey, text: &str) -> Option<ClosedBatch> {
        let now = Instant::now();
        let mut entry = self
            .pending
            .entry(peer.clone())
            .or_insert_with(|| PendingBatch {
                texts: Vec::new(),
                first_at: now,
                last_at: now,
            });
        entry.texts.push(text.to_owned());
        entry.last_at = now;
        let full = entry.texts.len() >= self.config.max_messages;
        drop(entry);
        full.then(|| self.take(peer)).flatten()
    }

    /// 关闭全部到期批次（sweeper 周期调用）。
    /// `remove_if` 在锁内复检条件——与并发 push 竞争时晚到者存活，不丢消息。
    pub fn close_due(&self) -> Vec<ClosedBatch> {
        let now = Instant::now();
        let due: Vec<PeerKey> = self
            .pending
            .iter()
            .filter(|entry| {
                let batch = entry.value();
                now - batch.last_at >= self.config.idle_window
                    || now - batch.first_at >= self.config.max_window
            })
            .map(|entry| entry.key().clone())
            .collect();
        due.into_iter()
            .filter_map(|peer| {
                self.pending
                    .remove_if(&peer, |_, batch| {
                        now - batch.last_at >= self.config.idle_window
                            || now - batch.first_at >= self.config.max_window
                    })
                    .map(|(_, batch)| Self::closed(&peer, batch))
            })
            .collect()
    }

    /// 取走该 peer 的未闭合批次（条数上限触发的即时闭合用）
    fn take(&self, peer: &PeerKey) -> Option<ClosedBatch> {
        self.pending
            .remove(peer)
            .map(|(_, batch)| Self::closed(peer, batch))
    }

    /// 取走全部未闭合批次（停机清理）
    pub fn close_all(&self) -> Vec<ClosedBatch> {
        let peers: Vec<PeerKey> = self.pending.iter().map(|entry| entry.key().clone()).collect();
        peers
            .into_iter()
            .filter_map(|peer| {
                self.pending
                    .remove(&peer)
                    .map(|(_, batch)| Self::closed(&peer, batch))
            })
            .collect()
    }

    fn closed(peer: &PeerKey, batch: PendingBatch) -> ClosedBatch {
        ClosedBatch {
            peer: peer.clone(),
            message_count: batch.texts.len(),
            merged_text: batch.texts.join("\n"),
        }
    }
}

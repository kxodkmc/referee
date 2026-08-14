//! 会话事实日志 — 会话消息历史的唯一事实源
//!
//! 与"给模型看的窗口"分离：模型可见窗口是 [`SessionLog::tail`] 派生的有界视图，
//! 事实本身只增不减（有界上限 `max_events`）。超限返回 [`LogError::CapacityExceeded`]，
//! 绝不静默丢弃（背压硬约束）。数据与行为分离：本类型只存纯数据 `Message`，不含逻辑句柄。

use crate::provider::Message;

/// 日志容量超限（背压硬约束）
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LogError {
    #[error("session event log full (max {max})")]
    CapacityExceeded { max: usize },
}

/// 内存 append-only 会话事实日志
#[derive(Debug, Clone)]
pub struct SessionLog {
    facts: Vec<Message>,
    max_events: usize,
}

impl SessionLog {
    /// 新建空日志（`max_events` 为事实上限，超限拒绝写入）
    pub fn new(max_events: usize) -> Self {
        Self {
            facts: Vec::with_capacity(max_events.min(256)),
            max_events,
        }
    }

    /// 追加一条事实。满则返回 `CapacityExceeded`，拒绝写入，绝不静默丢弃。
    pub fn append(&mut self, msg: Message) -> Result<usize, LogError> {
        if self.facts.len() >= self.max_events {
            return Err(LogError::CapacityExceeded { max: self.max_events });
        }
        self.facts.push(msg);
        Ok(self.facts.len() - 1)
    }

    /// 模型可见窗口：最近 `max` 条（有界派生视图，超窗丢弃只影响"给模型看的窗口"，事实源无损）
    pub fn tail(&self, max: usize) -> &[Message] {
        let start = self.facts.len().saturating_sub(max);
        &self.facts[start..]
    }

    /// 全量事实视图（审计 / 回放）
    pub fn snapshot(&self) -> &[Message] {
        &self.facts
    }

    /// 已记录事实总数
    pub fn len(&self) -> usize {
        self.facts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.facts.is_empty()
    }
}
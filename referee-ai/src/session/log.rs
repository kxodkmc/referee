//! 会话事实日志 — 会话消息历史的唯一事实源
//!
//! 与"给模型看的窗口"分离：模型可见窗口是 [`SessionLog::tail`] 派生的有界视图，
//! 事实本身只增不减（有界上限 `max_events`）。超限返回 [`LogError::CapacityExceeded`]，
//! 绝不静默丢弃（背压硬约束）；容量压力的**正常降级路径**是 [`SessionLog::compact`]：
//! 显式丢弃头部旧事实腾出空间（模型可见窗口为尾部派生，丢头部无感）。
//! 数据与行为分离：本类型只存纯数据 `Message`，不含逻辑句柄。

use crate::provider::Message;

#[cfg(feature = "persist")]
use crate::session::SessionId;
#[cfg(feature = "persist")]
use std::sync::Arc;

/// 日志错误
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LogError {
    /// 内存会话事实日志容量超限（背压硬约束，绝不静默丢弃）
    #[error("session event log full (max {max})")]
    CapacityExceeded { max: usize },
    /// 落盘 sink 失败（仅 `persist` feature；显式报错，不静默丢弃）
    #[error("session persist io error: {0}")]
    Io(String),
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

    /// 压缩日志：保留最近 `keep_last` 条事实，丢弃头部更旧的（容量压力的正常降级路径）。
    ///
    /// 内存仍有界（`≤ keep_last < max_events`），丢弃仅影响头部旧事实——模型可见窗口
    /// 由 [`SessionLog::tail`] 从尾部派生，丢头部对模型无感。返回压缩后的事实数；
    /// `keep_last >= 当前长度` 时不做任何事。
    pub fn compact(&mut self, keep_last: usize) -> usize {
        let len = self.facts.len();
        if len > keep_last {
            // drain 保留容量（无再分配），O(n) 平移可接受（仅在满时触发）
            self.facts.drain(..len - keep_last);
        }
        self.facts.len()
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

/// 会话事实落盘 sink — 可插拔，对齐 `WalSink` append 语义（同步追加）。
///
/// 实现需 `Send + Sync`，可在多任务间共享。失败**显式返回错误**，绝不静默丢弃。
/// 同步签名的原因：会话状态机（`Session::push_history`）在无 `await` 的同步路径
/// 写入，落盘为小字节 append（OS 缓冲、非每行 fsync），阻塞可忽略。
#[cfg(feature = "persist")]
pub trait SessionLogSink: Send + Sync {
    /// 追加一条事实到后端（含会话标识，供按会话分文件落盘）
    fn append(&self, session_id: &SessionId, msg: &Message) -> Result<(), LogError>;
}

/// 带落盘 sink 的会话事实日志 — 内存日志 + 可插拔落盘。
///
/// 追加路径：先写内存（容量超限拒绝，背压硬约束），再尽力落盘；落盘失败
/// 显式 `tracing::error!` 记录（不吞异常、不静默丢弃），不阻塞内存会话。
#[cfg(feature = "persist")]
pub struct PersistedSessionLog {
    inner: SessionLog,
    sink: Arc<dyn SessionLogSink>,
    session_id: SessionId,
}

#[cfg(feature = "persist")]
impl PersistedSessionLog {
    pub fn new(max_events: usize, session_id: SessionId, sink: Arc<dyn SessionLogSink>) -> Self {
        Self {
            inner: SessionLog::new(max_events),
            sink,
            session_id,
        }
    }

    /// 追加并落盘；内存超限返回 `CapacityExceeded`；落盘失败记录但成功返回
    pub fn append(&mut self, msg: Message) -> Result<usize, LogError> {
        let idx = self.inner.append(msg.clone())?;
        if let Err(e) = self.sink.append(&self.session_id, &msg) {
            tracing::error!(
                error = ?e,
                session_id = %self.session_id,
                "persist: session fact append to sink failed"
            );
        }
        Ok(idx)
    }

    /// 全量事实视图
    pub fn snapshot(&self) -> &[Message] {
        self.inner.snapshot()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

/// SessionLog 默认（非 persist）单元测试
#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::Message;

    #[test]
    fn compact_drops_head_keeps_tail() {
        let mut log = SessionLog::new(8);
        for i in 0..5 {
            log.append(Message::user(format!("m{i}"))).unwrap();
        }
        // 压缩保留最近 2 条：丢弃头部 3 条
        assert_eq!(log.compact(2), 2);
        let facts: Vec<&str> = log
            .snapshot()
            .iter()
            .filter_map(|m| m.content.as_text())
            .collect();
        assert_eq!(facts, vec!["m3", "m4"]);
        // keep_last >= 当前长度时无操作
        assert_eq!(log.compact(10), 2);
    }

    #[test]
    fn compact_frees_room_for_append() {
        let mut log = SessionLog::new(2);
        log.append(Message::user("a")).unwrap();
        log.append(Message::user("b")).unwrap();
        // 满：压缩留出空间后仍可继续写入（降级路径前提）
        log.compact(1);
        log.append(Message::user("c")).unwrap();
        assert_eq!(log.len(), 2);
        let facts: Vec<&str> = log
            .snapshot()
            .iter()
            .filter_map(|m| m.content.as_text())
            .collect();
        assert_eq!(facts, vec!["b", "c"]);
    }
}

#[cfg(all(test, feature = "persist"))]
mod persist_tests {
    use super::*;
    use crate::provider::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountingSink {
        count: std::sync::Arc<AtomicUsize>,
    }

    impl SessionLogSink for CountingSink {
        fn append(&self, _session_id: &SessionId, _msg: &Message) -> Result<(), LogError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn persisted_log_append_forwards_to_sink() {
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let mut log = PersistedSessionLog::new(
            16,
            SessionId::new_v4(),
            std::sync::Arc::new(CountingSink { count: count.clone() }),
        );
        log.append(Message::user("hi")).unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn persisted_log_capacity_blocks_without_sink_call() {
        let count = std::sync::Arc::new(AtomicUsize::new(0));
        let mut log = PersistedSessionLog::new(
            1,
            SessionId::new_v4(),
            std::sync::Arc::new(CountingSink { count: count.clone() }),
        );
        log.append(Message::user("a")).unwrap();
        // 容量满：拒绝第二行，落盘也应被阻断（不产生半落盘）
        let err = log.append(Message::user("b")).unwrap_err();
        assert_eq!(
            err,
            LogError::CapacityExceeded { max: 1 }
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
//! 信封结构 — 纯数据载体，不含任何逻辑句柄

use std::collections::HashMap;
use std::time::Instant;

use bytes::Bytes;
use uuid::Uuid;

/// 所有跨扩展消息的统一数据格式
#[derive(Debug, Clone)]
pub struct Envelope {
    pub context_id: Uuid,
    pub correlation_id: Uuid,
    pub artifact_id: Uuid,
    pub trace_id: Uuid,
    /// 路由目标能力 ID（uuid 形式）— dispatch 入队前由内核填充，WAL 恢复时的路由依据
    pub target: Uuid,
    /// 入队时间戳 — 优先级队列老化判定的依据（入队时由内核刷新）
    pub queued_at: Instant,
    pub priority: u8,
    pub metadata: HashMap<String, String>,
    /// 内联消息载荷 — 引用计数共享（含 WAL / DLQ 转储路径的克隆均零拷贝）。
    ///
    /// `None` 表示纯控制消息或载荷走外部存储；大工件应存外部传引用
    /// （metadata 放引用 ID），防信封膨胀破坏有界内存约束。
    pub payload: Option<Bytes>,
}

impl Envelope {
    /// 构造一个全新随机 ID 的空载荷信封
    pub fn new() -> Self {
        Self {
            context_id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            artifact_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            target: Uuid::new_v4(),
            queued_at: Instant::now(),
            priority: 0,
            metadata: HashMap::new(),
            payload: None,
        }
    }

    /// 构造携带载荷的信封（`Vec<u8>` / `String` / `&[u8]` / `Bytes` 均可直接转入）
    pub fn with_payload(payload: impl Into<Bytes>) -> Self {
        let mut env = Self::new();
        env.payload = Some(payload.into());
        env
    }
}

impl Default for Envelope {
    fn default() -> Self {
        Self::new()
    }
}

//! 信封结构 — 纯数据载体，不含任何逻辑句柄

use std::collections::HashMap;

use uuid::Uuid;

/// 所有跨扩展消息的统一数据格式
#[derive(Debug, Clone)]
pub struct Envelope {
    pub context_id: Uuid,
    pub correlation_id: Uuid,
    pub artifact_id: Uuid,
    pub trace_id: Uuid,
    pub priority: u8,
    pub metadata: HashMap<String, String>,
}

impl Envelope {
    /// 构造一个全新随机 ID 的信封
    pub fn new() -> Self {
        Self {
            context_id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            artifact_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            priority: 0,
            metadata: HashMap::new(),
        }
    }
}

impl Default for Envelope {
    fn default() -> Self {
        Self::new()
    }
}

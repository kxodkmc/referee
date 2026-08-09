//! 死信队列 — 容量受限的降级出口
//!
//! 路由被拦截（崩溃 / 背压 / 停机）的 Envelope 连同原因写入死信，
//! 供事后审计或重放。`InMemoryDlq` 为环形缓冲默认实现，容量受限防 OOM。

use std::collections::VecDeque;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::common::{Envelope, KernelError};

/// 死信落盘接口 — 可替换为任意持久化实现
#[async_trait]
pub trait DlqSink: Send + Sync {
    /// 记录一条死信
    async fn sink(&self, env: Envelope, reason: KernelError);
}

/// 内存环形缓冲死信队列：满则丢弃最旧，容量恒定
pub struct InMemoryDlq {
    buf: Mutex<VecDeque<(Envelope, KernelError)>>,
    capacity: usize,
}

impl InMemoryDlq {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// 当前死信数量
    pub fn len(&self) -> usize {
        self.buf.lock().len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.buf.lock().is_empty()
    }

    /// 取出全部死信（消费式，用于审计 / 重放 / 测试断言）
    pub fn drain(&self) -> Vec<(Envelope, KernelError)> {
        self.buf.lock().drain(..).collect()
    }
}

impl Default for InMemoryDlq {
    fn default() -> Self {
        Self::new(1024)
    }
}

#[async_trait]
impl DlqSink for InMemoryDlq {
    async fn sink(&self, env: Envelope, reason: KernelError) {
        let mut buf = self.buf.lock();
        if buf.len() >= self.capacity {
            buf.pop_front();
        }
        buf.push_back((env, reason));
    }
}

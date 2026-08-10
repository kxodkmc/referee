//! 写前日志（WAL）— 进程级崩溃的持久化兜底
//!
//! 正常 `dispatch` 在入队前先 `append` 落盘；扩展处理成功后由监督器
//! `ack` 确认。进程被强杀（OOM Kill / 断电）时，未确认消息通过
//! `recover` 在下次启动经独立恢复通道重新注入路由表（至少一次投递）。
//!
//! 恢复通道直接调用 `router.dispatch`，**绕过 WAL 追加**，杜绝恢复消息
//! 被重复落盘的死循环。

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::common::{Envelope, KernelResult};

/// WAL 落盘接口 — 可替换为任意持久化实现（文件 / 数据库）
#[async_trait]
pub trait WalSink: Send + Sync {
    /// 追加一条待投递消息，返回可 ACK 的日志 ID
    async fn append(&self, env: &Envelope) -> KernelResult<Uuid>;

    /// 处理成功后确认，崩溃重放不再包含该条
    async fn ack(&self, id: Uuid);

    /// 读取全部未确认消息及其 ID（恢复通道，绕过追加）
    async fn recover(&self) -> Vec<(Uuid, Envelope)>;
}

/// 内存版 WAL — 测试 / 演示默认兜底
///
/// 进程崩溃即丢失（等价于无持久化），但完整实现 WalSink 语义，
/// 可用于验证恢复路径与 ACK 时序。
pub struct InMemoryWal {
    pending: Mutex<Vec<(Uuid, Envelope)>>,
    seq: AtomicU64,
}

impl InMemoryWal {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            seq: AtomicU64::new(0),
        }
    }

    /// 当前未确认消息数
    pub fn pending_len(&self) -> usize {
        self.pending.lock().len()
    }

    fn next_id(&self) -> Uuid {
        // 单调递增 + 进程内唯一即可（无需真随机，便于测试排序）
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        Uuid::from_u128(n as u128)
    }
}

impl Default for InMemoryWal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WalSink for InMemoryWal {
    async fn append(&self, env: &Envelope) -> KernelResult<Uuid> {
        let id = self.next_id();
        self.pending.lock().push((id, env.clone()));
        Ok(id)
    }

    async fn ack(&self, id: Uuid) {
        self.pending.lock().retain(|(i, _)| *i != id);
    }

    async fn recover(&self) -> Vec<(Uuid, Envelope)> {
        self.pending.lock().clone()
    }
}

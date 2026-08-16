//! 通用 KV 存储抽象 — 有界、可替换持久化（地基）
//!
//! 将原「对等 Agent 工件存储」泛化为**通用键值存储**，作为成果/状态/大结果的
//! 通用载体。去除了对等 Agent 特定语义（owner / allowed_readers / 可见性注入），
//! 权限与业务语义交由上层（referee-agent）二次封装，本模块只提供地基能力。
//!
//! ## 设计约束
//! - **数据/行为分离**：[`StoredValue`] 是纯数据（content_type / bytes / 时间），
//!   无逻辑句柄；[`Store`] trait 规定读写契约。
//! - **有界存储**：`max_keys` + `max_total_bytes` 双上限，超限返回
//!   [`StoreError::CapacityExceeded`]，绝不无界增长（背压硬约束）。
//! - **可替换**：存储经 [`Store`] trait 抽象，默认有界内存实现；持久化后端
//!   （文件 / WAL）由调用方提供相同 trait 实现即可替换。
//!
//! ## 用法
//! ```text
//! store.store("key", StoredValue::from_bytes(b"...", "text/plain")) → Ok(())
//! store.get("key")     → Some(StoredValue)
//! store.delete("key")  → bool
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use parking_lot::Mutex;
use thiserror::Error;

/// 存储值 — 纯数据载体
#[derive(Debug, Clone)]
pub struct StoredValue {
    /// 内容 MIME 类型
    pub content_type: String,
    /// 内容字节
    pub bytes: Vec<u8>,
    /// 写入时间
    pub created_at: SystemTime,
}

impl StoredValue {
    /// 从字节构造
    pub fn from_bytes(bytes: Vec<u8>, content_type: impl Into<String>) -> Self {
        Self {
            content_type: content_type.into(),
            bytes,
            created_at: SystemTime::now(),
        }
    }

    /// 从 UTF-8 文本构造（默认 `text/plain`）
    pub fn from_text(text: impl Into<String>) -> Self {
        Self::from_bytes(text.into().into_bytes(), "text/plain")
    }
}

/// 存储错误
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// 容量耗尽（数量或总字节超限）
    #[error("capacity exceeded")]
    CapacityExceeded,
    /// 键不存在
    #[error("key not found: {0}")]
    NotFound(String),
}

/// 通用 KV 存储抽象 — 后端可替换
///
/// 实现需 `Send + Sync`，可在多 task 间共享。
#[async_trait]
pub trait Store: Send + Sync {
    /// 写入键值；同键覆盖。容量超限返回 `CapacityExceeded`。
    async fn store(&self, key: String, value: StoredValue) -> Result<(), StoreError>;

    /// 读取；键不存在返回 `None`。
    async fn get(&self, key: &str) -> Option<StoredValue>;

    /// 删除；键存在返回 `true`。
    async fn delete(&self, key: &str) -> bool;

    /// 当前键数量
    fn len(&self) -> usize;

    /// 当前总字节数
    fn total_bytes(&self) -> usize;

    /// 是否为空
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// 存储容量配置（有界硬约束）
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// 最大键数
    pub max_keys: usize,
    /// 最大总字节数（所有值 bytes 之和）
    pub max_total_bytes: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_keys: 1024,
            max_total_bytes: 64 * 1024 * 1024,
        }
    }
}

/// 存储内部状态
struct StoreInner {
    values: HashMap<String, StoredValue>,
    total_bytes: usize,
}

/// 内存 KV 存储 — 有界、线程安全
#[derive(Clone)]
pub struct InMemoryStore {
    inner: Arc<Mutex<StoreInner>>,
    config: StoreConfig,
}

impl InMemoryStore {
    pub fn new(config: StoreConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                values: HashMap::new(),
                total_bytes: 0,
            })),
            config,
        }
    }

    /// 默认容量配置
    pub fn with_defaults() -> Self {
        Self::new(StoreConfig::default())
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn store(&self, key: String, value: StoredValue) -> Result<(), StoreError> {
        let mut inner = self.inner.lock();
        let size = value.bytes.len();

        // 有界检查：数量 + 总字节双上限（同键覆盖时剔除旧体积后重新判定）
        let old_size = inner.values.get(&key).map(|v| v.bytes.len()).unwrap_or(0);
        let would_exceed_count =
            inner.values.len() >= self.config.max_keys && !inner.values.contains_key(&key);
        let would_exceed_bytes = inner
            .total_bytes
            .saturating_sub(old_size)
            .saturating_add(size)
            > self.config.max_total_bytes;
        if would_exceed_count || would_exceed_bytes {
            return Err(StoreError::CapacityExceeded);
        }

        inner.total_bytes = inner.total_bytes - old_size + size;
        inner.values.insert(key, value);
        Ok(())
    }

    async fn get(&self, key: &str) -> Option<StoredValue> {
        self.inner.lock().values.get(key).cloned()
    }

    async fn delete(&self, key: &str) -> bool {
        let mut inner = self.inner.lock();
        match inner.values.remove(key) {
            Some(v) => {
                inner.total_bytes = inner.total_bytes.saturating_sub(v.bytes.len());
                true
            }
            None => false,
        }
    }

    fn len(&self) -> usize {
        self.inner.lock().values.len()
    }

    fn total_bytes(&self) -> usize {
        self.inner.lock().total_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn store_and_get() {
        let store = InMemoryStore::with_defaults();
        store
            .store("k1".into(), StoredValue::from_text("hello"))
            .await
            .unwrap();
        let v = store.get("k1").await.unwrap();
        assert_eq!(v.content_type, "text/plain");
        assert_eq!(v.bytes, b"hello");
        assert!(store.get("nope").await.is_none());
    }

    #[tokio::test]
    async fn overwrite_and_delete() {
        let store = InMemoryStore::with_defaults();
        store
            .store("k1".into(), StoredValue::from_text("a"))
            .await
            .unwrap();
        store
            .store("k1".into(), StoredValue::from_text("bb"))
            .await
            .unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.total_bytes(), 2);
        assert!(store.delete("k1").await);
        assert!(!store.delete("k1").await);
        assert!(store.is_empty());
    }

    #[tokio::test]
    async fn capacity_by_count() {
        let store = InMemoryStore::new(StoreConfig {
            max_keys: 1,
            max_total_bytes: 1024,
        });
        store
            .store("a".into(), StoredValue::from_text("x"))
            .await
            .unwrap();
        let err = store
            .store("b".into(), StoredValue::from_text("y"))
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::CapacityExceeded));
        // 同键覆盖不受数量限制
        store
            .store("a".into(), StoredValue::from_text("zz"))
            .await
            .unwrap();
        assert_eq!(store.len(), 1);
    }

    #[tokio::test]
    async fn capacity_by_bytes() {
        let store = InMemoryStore::new(StoreConfig {
            max_keys: 8,
            max_total_bytes: 10,
        });
        store
            .store("a".into(), StoredValue::from_text("123456"))
            .await
            .unwrap();
        store
            .store("b".into(), StoredValue::from_text("7890"))
            .await
            .unwrap();
        let err = store
            .store("c".into(), StoredValue::from_text("1"))
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::CapacityExceeded));
    }
}

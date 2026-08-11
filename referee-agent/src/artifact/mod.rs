//! 工件存储 — 有界、带 ACL 的成果载体（对等智能体协作的数据底座）
//!
//! ## 设计约束
//! - **数据/行为分离**：`Artifact` 是纯数据载体（id / owner / readers / bytes /
//!   元数据），不包含任何逻辑句柄；权限判定完全由 `ArtifactStore` 执行。
//! - **访问控制**：读取者必须为 `owner` 或显式授权的 `allowed_readers` 成员；
//!   授权操作（`grant_access`）仅 owner 可执行。杜绝「猜中 ID 即越权读取」。
//! - **有界存储**：数量 + 总字节双上限，超限返回 `CapacityExceeded`，
//!   绝不无界增长（背压硬约束）。
//!
//! ## 用法
//! ```text
//! store.store(artifact)        → Ok(artifact_id)
//! store.get(id, requester)     → Ok(Some(..)) / Ok(None) / Err(PermissionDenied)
//! store.grant_access(id, owner, reader) → Ok(()) / Err(..)
//! ```

//! ## 信任边界（安全声明）
//! 写入路径（`store` / `grant_access`）的调用方必须是**可信注册的工具**
//! （当前唯一写入者为 `AgentTool`，生成新鲜 UUID、owner 固定为产出 Agent）。
//! store 层不校验 acting principal——一旦引入不可信调用方，须为写入路径
//! 增加主体验证（拒绝同 id 覆盖 + 校验 owner 声明），本模块的「猜中 ID
//! 即越权读取」防线仅覆盖读取路径。

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 工件 — 纯数据载体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    /// 工件 ID（创建方生成，通常为 UUID 字符串）
    pub id: String,
    /// 创建者（拥有全部权限，含授权他人读取）
    pub owner: uuid::Uuid,
    /// 显式授权的读者（owner 之外）
    pub allowed_readers: HashSet<uuid::Uuid>,
    /// 内容 MIME 类型
    pub content_type: String,
    /// 内容字节
    pub bytes: Vec<u8>,
    /// 创建时间
    pub created_at: SystemTime,
}

/// 存储错误
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// 请求者无读取 / 授权权限
    #[error("permission denied for artifact {0}")]
    PermissionDenied(String),
    /// 工件不存在
    #[error("artifact not found: {0}")]
    NotFound(String),
    /// 存储容量耗尽（数量或总字节超限）
    #[error("capacity exceeded")]
    CapacityExceeded,
}

/// 工件存储抽象 — 所有读取路径强制鉴权
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// 存储工件，返回其 ID（与 `artifact.id` 一致）
    async fn store(&self, artifact: Artifact) -> Result<String, StoreError>;

    /// 鉴权读取：仅 `owner` 或 `allowed_readers` 成员可读。
    /// 返回 `None` 表示工件不存在；权限不足返回 `PermissionDenied`。
    async fn get(&self, id: &str, requester: uuid::Uuid) -> Result<Option<Artifact>, StoreError>;

    /// owner 授权他人读取
    async fn grant_access(
        &self,
        id: &str,
        owner: uuid::Uuid,
        reader: uuid::Uuid,
    ) -> Result<(), StoreError>;
}

/// 存储容量配置（有界硬约束）
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// 最大工件数
    pub max_artifacts: usize,
    /// 最大总字节数（所有工件 bytes 之和）
    pub max_total_bytes: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            max_artifacts: 1024,
            max_total_bytes: 64 * 1024 * 1024,
        }
    }
}

/// 存储内部状态
struct StoreInner {
    artifacts: HashMap<String, Artifact>,
    total_bytes: usize,
}

/// 内存工件存储 — 有界、线程安全
#[derive(Clone)]
pub struct InMemoryArtifactStore {
    inner: Arc<Mutex<StoreInner>>,
    config: StoreConfig,
}

impl InMemoryArtifactStore {
    pub fn new(config: StoreConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StoreInner {
                artifacts: HashMap::new(),
                total_bytes: 0,
            })),
            config,
        }
    }

    /// 默认容量配置
    pub fn with_defaults() -> Self {
        Self::new(StoreConfig::default())
    }

    /// 当前工件数
    pub fn len(&self) -> usize {
        self.inner.lock().artifacts.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 当前总字节数
    pub fn total_bytes(&self) -> usize {
        self.inner.lock().total_bytes
    }

    /// 容量配置引用
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }
}

#[async_trait]
impl ArtifactStore for InMemoryArtifactStore {
    async fn store(&self, artifact: Artifact) -> Result<String, StoreError> {
        let mut inner = self.inner.lock();
        let size = artifact.bytes.len();

        // 有界检查：数量 + 总字节双上限（同 id 覆盖时剔除旧体积后重新判定）
        let old_size = inner
            .artifacts
            .get(&artifact.id)
            .map(|a| a.bytes.len())
            .unwrap_or(0);
        let would_exceed_count = inner.artifacts.len() >= self.config.max_artifacts
            && !inner.artifacts.contains_key(&artifact.id);
        let would_exceed_bytes = inner
            .total_bytes
            .saturating_sub(old_size)
            .saturating_add(size)
            > self.config.max_total_bytes;
        if would_exceed_count || would_exceed_bytes {
            return Err(StoreError::CapacityExceeded);
        }

        inner.total_bytes = inner.total_bytes - old_size + size;
        let id = artifact.id.clone();
        inner.artifacts.insert(id.clone(), artifact);
        Ok(id)
    }

    async fn get(&self, id: &str, requester: uuid::Uuid) -> Result<Option<Artifact>, StoreError> {
        let inner = self.inner.lock();
        let Some(artifact) = inner.artifacts.get(id) else {
            return Ok(None);
        };
        if artifact.owner == requester || artifact.allowed_readers.contains(&requester) {
            Ok(Some(artifact.clone()))
        } else {
            Err(StoreError::PermissionDenied(id.to_string()))
        }
    }

    async fn grant_access(
        &self,
        id: &str,
        owner: uuid::Uuid,
        reader: uuid::Uuid,
    ) -> Result<(), StoreError> {
        let mut inner = self.inner.lock();
        let Some(artifact) = inner.artifacts.get_mut(id) else {
            return Err(StoreError::NotFound(id.to_string()));
        };
        if artifact.owner != owner {
            return Err(StoreError::PermissionDenied(id.to_string()));
        }
        artifact.allowed_readers.insert(reader);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_artifact(id: &str, owner: uuid::Uuid, bytes: usize) -> Artifact {
        Artifact {
            id: id.to_string(),
            owner,
            allowed_readers: HashSet::new(),
            content_type: "text/plain".into(),
            bytes: vec![0u8; bytes],
            created_at: SystemTime::now(),
        }
    }

    #[tokio::test]
    async fn owner_can_read() {
        let store = InMemoryArtifactStore::with_defaults();
        let owner = uuid::Uuid::new_v4();
        store.store(make_artifact("a1", owner, 4)).await.unwrap();
        let got = store.get("a1", owner).await.unwrap().unwrap();
        assert_eq!(got.owner, owner);
    }

    #[tokio::test]
    async fn authorized_reader_can_read() {
        let store = InMemoryArtifactStore::with_defaults();
        let owner = uuid::Uuid::new_v4();
        let reader = uuid::Uuid::new_v4();
        let mut artifact = make_artifact("a1", owner, 4);
        artifact.allowed_readers.insert(reader);
        store.store(artifact).await.unwrap();
        assert!(store.get("a1", reader).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn unauthorized_reader_denied() {
        let store = InMemoryArtifactStore::with_defaults();
        let owner = uuid::Uuid::new_v4();
        let stranger = uuid::Uuid::new_v4();
        store.store(make_artifact("s", owner, 4)).await.unwrap();
        let err = store.get("s", stranger).await.unwrap_err();
        assert!(matches!(err, StoreError::PermissionDenied(_)));
    }

    #[tokio::test]
    async fn grant_access_allows_reader() {
        let store = InMemoryArtifactStore::with_defaults();
        let owner = uuid::Uuid::new_v4();
        let reader = uuid::Uuid::new_v4();
        store.store(make_artifact("s", owner, 4)).await.unwrap();
        store.grant_access("s", owner, reader).await.unwrap();
        assert!(store.get("s", reader).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn non_owner_cannot_grant() {
        let store = InMemoryArtifactStore::with_defaults();
        let owner = uuid::Uuid::new_v4();
        let attacker = uuid::Uuid::new_v4();
        let reader = uuid::Uuid::new_v4();
        store.store(make_artifact("s", owner, 4)).await.unwrap();
        let err = store.grant_access("s", attacker, reader).await.unwrap_err();
        assert!(matches!(err, StoreError::PermissionDenied(_)));
    }

    #[tokio::test]
    async fn missing_artifact_is_none() {
        let store = InMemoryArtifactStore::with_defaults();
        let who = uuid::Uuid::new_v4();
        assert!(store.get("nope", who).await.unwrap().is_none());
        let err = store.grant_access("nope", who, who).await.unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn capacity_exceeded_by_count() {
        let store = InMemoryArtifactStore::new(StoreConfig {
            max_artifacts: 1,
            max_total_bytes: 1024 * 1024,
        });
        let owner = uuid::Uuid::new_v4();
        store.store(make_artifact("a", owner, 4)).await.unwrap();
        let err = store.store(make_artifact("b", owner, 4)).await.unwrap_err();
        assert!(matches!(err, StoreError::CapacityExceeded));
        // 同 id 覆盖不受数量限制
        store.store(make_artifact("a", owner, 8)).await.unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.total_bytes(), 8);
    }

    #[tokio::test]
    async fn capacity_exceeded_by_bytes() {
        let store = InMemoryArtifactStore::new(StoreConfig {
            max_artifacts: 8,
            max_total_bytes: 10,
        });
        let owner = uuid::Uuid::new_v4();
        store.store(make_artifact("a", owner, 6)).await.unwrap();
        store.store(make_artifact("b", owner, 4)).await.unwrap();
        let err = store.store(make_artifact("c", owner, 1)).await.unwrap_err();
        assert!(matches!(err, StoreError::CapacityExceeded));
    }
}

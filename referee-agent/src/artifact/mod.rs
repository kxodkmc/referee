//! 成果板存储 — 有界、以 ID 为访问凭证的子智能体成果载体
//!
//! ## 模型
//! - **板归属**：每层调用者（父会话）各自建板，作用域隔离、互不影响。
//! - **写入**：子智能体只写自己被调入板内的条目（`owner_session` 标识归属）。
//! - **读取**：板创建者可 `list_by_creator` 列自己板内条目；任何持结果 ID 者
//!   可 `get` 正文（ID = capability，上抛 ID 即完成授权）。
//! - **排序**：每个条目含板内单调 `seq` 与 `created_at` / `updated_at`，
//!   `list_by_creator` 按 `seq` 排序供调用者判断产出顺序。
//!
//! ## 信任边界
//! 写入者当前唯一为可信注册的 `AgentTool`（生成不可预测 UUID、`owner_session`
//! 固定为其调入的子会话），故 store 层不校验写入主体；引入不可信写入工具前
//! 需为写入路径增加主体验证。读取路径由 ID 凭证语义天然隔离（不知 ID 即不可读）。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// 成果板标识（UUID，由 store 生成，不可预测）
pub type BoardId = uuid::Uuid;

/// 工件 — 成果板内的一个条目（纯数据载体）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    /// 结果 ID（访问凭证：持有即可读正文）
    pub id: String,
    /// 所属成果板（由调用者创建）
    pub board: BoardId,
    /// 写入该条目的子会话实例（强身份归属）
    pub owner_session: uuid::Uuid,
    /// 产出智能体类型名（弱标签，展示用）
    pub producer_label: String,
    /// 条目标题（任务摘要）
    pub title: String,
    /// 内容 MIME 类型
    pub content_type: String,
    /// 内容字节
    pub bytes: Vec<u8>,
    /// 板内单调序号（顺序线索）
    pub seq: u64,
    /// 首次写入时间
    pub created_at: SystemTime,
    /// 最近更新时间（结果可更新）
    pub updated_at: SystemTime,
}

impl Artifact {
    /// 构造新条目（ID 由框架生成不可预测 UUID；`seq` / 时间戳由 store 归一）
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        board: BoardId,
        owner_session: uuid::Uuid,
        producer_label: impl Into<String>,
        title: impl Into<String>,
        content_type: impl Into<String>,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            board,
            owner_session,
            producer_label: producer_label.into(),
            title: title.into(),
            content_type: content_type.into(),
            bytes,
            seq: 0,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
        }
    }
}

/// 存储错误
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// 工件或成果板不存在
    #[error("not found: {0}")]
    NotFound(String),
    /// 存储容量耗尽（数量或总字节超限）
    #[error("capacity exceeded")]
    CapacityExceeded,
}

/// 成果板存储抽象 — 读取路径以 ID 为凭证
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// 写入条目，返回其 ID（与 `artifact.id` 一致）；`seq` / `updated_at` 由实现归一
    async fn store(&self, artifact: Artifact) -> Result<String, StoreError>;

    /// 按 ID 读取正文（凭证语义，无需 requester）；不存在返回 `Ok(None)`
    async fn get(&self, id: &str) -> Result<Option<Artifact>, StoreError>;

    /// 列出创建者自己板内的全部条目，按 `seq` 升序
    async fn list_by_creator(&self, creator: uuid::Uuid) -> Result<Vec<Artifact>, StoreError>;

    /// 获取或创建调用者的成果板（幂等：同一调用者始终同一板）
    async fn ensure_board(&self, creator: uuid::Uuid) -> Result<BoardId, StoreError>;
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
    boards: HashMap<BoardId, uuid::Uuid>,
    board_by_creator: HashMap<uuid::Uuid, BoardId>,
    total_bytes: usize,
}

/// 内存成果板存储 — 有界、线程安全
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
                boards: HashMap::new(),
                board_by_creator: HashMap::new(),
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
        if !inner.boards.contains_key(&artifact.board) {
            return Err(StoreError::NotFound(format!("board {}", artifact.board)));
        }

        let old = inner.artifacts.get(&artifact.id).cloned();
        let old_size = old.as_ref().map(|a| a.bytes.len()).unwrap_or(0);
        let size = artifact.bytes.len();
        let would_exceed_count =
            inner.artifacts.len() >= self.config.max_artifacts && old.is_none();
        let would_exceed_bytes = inner
            .total_bytes
            .saturating_sub(old_size)
            .saturating_add(size)
            > self.config.max_total_bytes;
        if would_exceed_count || would_exceed_bytes {
            return Err(StoreError::CapacityExceeded);
        }

        let seq = old
            .as_ref()
            .map(|a| a.seq)
            .unwrap_or_else(|| next_seq(&inner.artifacts, artifact.board));
        let created_at = old
            .as_ref()
            .map(|a| a.created_at)
            .unwrap_or(artifact.created_at);

        let mut artifact = artifact;
        artifact.seq = seq;
        artifact.created_at = created_at;
        artifact.updated_at = SystemTime::now();

        inner.total_bytes = inner.total_bytes - old_size + size;
        let id = artifact.id.clone();
        inner.artifacts.insert(id.clone(), artifact);
        Ok(id)
    }

    async fn get(&self, id: &str) -> Result<Option<Artifact>, StoreError> {
        Ok(self.inner.lock().artifacts.get(id).cloned())
    }

    async fn list_by_creator(&self, creator: uuid::Uuid) -> Result<Vec<Artifact>, StoreError> {
        let inner = self.inner.lock();
        let Some(board) = inner.board_by_creator.get(&creator).copied() else {
            return Ok(Vec::new());
        };
        let mut items: Vec<Artifact> = inner
            .artifacts
            .values()
            .filter(|a| a.board == board)
            .cloned()
            .collect();
        items.sort_by_key(|a| a.seq);
        Ok(items)
    }

    async fn ensure_board(&self, creator: uuid::Uuid) -> Result<BoardId, StoreError> {
        let mut inner = self.inner.lock();
        if let Some(board) = inner.board_by_creator.get(&creator) {
            return Ok(*board);
        }
        let board = uuid::Uuid::new_v4();
        inner.boards.insert(board, creator);
        inner.board_by_creator.insert(creator, board);
        Ok(board)
    }
}

/// 板内下一个 seq（现有最大 seq + 1）
fn next_seq(artifacts: &HashMap<String, Artifact>, board: BoardId) -> u64 {
    artifacts
        .values()
        .filter(|a| a.board == board)
        .map(|a| a.seq)
        .max()
        .unwrap_or(0)
        + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_artifact(board: BoardId, owner: uuid::Uuid, bytes: usize) -> Artifact {
        Artifact::new(
            board,
            owner,
            "producer",
            "title",
            "text/plain",
            vec![0u8; bytes],
        )
    }

    #[tokio::test]
    async fn store_and_get_by_id_credential() {
        let store = InMemoryArtifactStore::with_defaults();
        let owner = uuid::Uuid::new_v4();
        let board = store.ensure_board(owner).await.unwrap();
        let id = store.store(make_artifact(board, owner, 4)).await.unwrap();
        let got = store.get(&id).await.unwrap().expect("must exist");
        assert_eq!(got.owner_session, owner);
        assert_eq!(got.bytes, vec![0u8; 4]);
    }

    #[tokio::test]
    async fn list_by_creator_returns_only_own_board_sorted_by_seq() {
        let store = InMemoryArtifactStore::with_defaults();
        let parent_a = uuid::Uuid::new_v4();
        let parent_b = uuid::Uuid::new_v4();
        let child = uuid::Uuid::new_v4();

        let board_a = store.ensure_board(parent_a).await.unwrap();
        let board_b = store.ensure_board(parent_b).await.unwrap();

        store.store(make_artifact(board_a, child, 2)).await.unwrap();
        store.store(make_artifact(board_a, child, 3)).await.unwrap();
        store.store(make_artifact(board_b, child, 1)).await.unwrap();

        let items_a = store.list_by_creator(parent_a).await.unwrap();
        assert_eq!(items_a.len(), 2);
        assert_eq!(items_a[0].seq, 1);
        assert_eq!(items_a[1].seq, 2);

        let items_b = store.list_by_creator(parent_b).await.unwrap();
        assert_eq!(items_b.len(), 1);
        assert_eq!(items_b[0].board, board_b);
    }

    #[tokio::test]
    async fn ensure_board_is_idempotent() {
        let store = InMemoryArtifactStore::with_defaults();
        let creator = uuid::Uuid::new_v4();
        let b1 = store.ensure_board(creator).await.unwrap();
        let b2 = store.ensure_board(creator).await.unwrap();
        assert_eq!(b1, b2);
    }

    #[tokio::test]
    async fn store_requires_existing_board() {
        let store = InMemoryArtifactStore::with_defaults();
        let err = store
            .store(make_artifact(uuid::Uuid::new_v4(), uuid::Uuid::new_v4(), 1))
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn update_preserves_seq_and_created_at() {
        let store = InMemoryArtifactStore::with_defaults();
        let owner = uuid::Uuid::new_v4();
        let board = store.ensure_board(owner).await.unwrap();
        let first = make_artifact(board, owner, 4);
        let first_created = first.created_at;
        let id = store.store(first.clone()).await.unwrap();

        let mut updated = first;
        updated.id = id.clone();
        updated.bytes = vec![1u8; 8];
        store.store(updated).await.unwrap();

        let got = store.get(&id).await.unwrap().unwrap();
        assert_eq!(got.seq, 1, "update must keep seq");
        assert_eq!(got.created_at, first_created, "update must keep created_at");
        assert_eq!(got.bytes.len(), 8, "update must replace bytes");
    }

    #[tokio::test]
    async fn missing_artifact_is_none() {
        let store = InMemoryArtifactStore::with_defaults();
        assert!(store.get("nope").await.unwrap().is_none());
        assert!(store
            .list_by_creator(uuid::Uuid::new_v4())
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn capacity_exceeded_by_count() {
        let store = InMemoryArtifactStore::new(StoreConfig {
            max_artifacts: 1,
            max_total_bytes: 1024 * 1024,
        });
        let owner = uuid::Uuid::new_v4();
        let board = store.ensure_board(owner).await.unwrap();
        store.store(make_artifact(board, owner, 4)).await.unwrap();
        let err = store
            .store(make_artifact(board, owner, 4))
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::CapacityExceeded));
    }

    #[tokio::test]
    async fn capacity_exceeded_by_bytes() {
        let store = InMemoryArtifactStore::new(StoreConfig {
            max_artifacts: 8,
            max_total_bytes: 10,
        });
        let owner = uuid::Uuid::new_v4();
        let board = store.ensure_board(owner).await.unwrap();
        store.store(make_artifact(board, owner, 6)).await.unwrap();
        store.store(make_artifact(board, owner, 4)).await.unwrap();
        let err = store
            .store(make_artifact(board, owner, 1))
            .await
            .unwrap_err();
        assert!(matches!(err, StoreError::CapacityExceeded));
    }
}

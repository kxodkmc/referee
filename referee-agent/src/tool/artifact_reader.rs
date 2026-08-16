//! 成果板读取工具 — 主 Agent 读取自己板内成果、按 ID 凭证读取深成果

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use referee_ai::tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
use serde_json::{json, Value};

use crate::artifact::ArtifactStore;

/// 列出调用者自己创建的成果板内条目（其直接子成果），按产出顺序
pub struct ListMyBoard {
    store: Arc<dyn ArtifactStore>,
}

impl ListMyBoard {
    pub fn new(store: Arc<dyn ArtifactStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Tool for ListMyBoard {
    fn name(&self) -> &str {
        "list_my_board"
    }

    fn description(&self) -> &str {
        "列出本会话作为调用者创建的成果板内的条目（其直接子智能体成果），按产出顺序返回"
    }

    fn input_schema(&self) -> Value {
        json!({"type": "object"})
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Local
    }

    fn default_wait(&self) -> bool {
        true
    }

    async fn execute(&self, ctx: ToolContext, _args: Value) -> Result<ToolOutput, ToolError> {
        let items = self
            .store
            .list_by_creator(ctx.session_id)
            .await
            .map_err(|e| ToolError::Execution(format!("{e}")))?;
        let rows: Vec<Value> = items
            .into_iter()
            .map(|a| {
                json!({
                    "artifact_id": a.id,
                    "title": a.title,
                    "producer_label": a.producer_label,
                    "updated_at": unix_secs(a.updated_at),
                })
            })
            .collect();
        Ok(ToolOutput::from_json(&Value::Array(rows)))
    }
}

/// 按 ID（凭证）读取成果正文
pub struct ArtifactReader {
    store: Arc<dyn ArtifactStore>,
}

impl ArtifactReader {
    pub fn new(store: Arc<dyn ArtifactStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Tool for ArtifactReader {
    fn name(&self) -> &str {
        "read_artifact"
    }

    fn description(&self) -> &str {
        "按 artifact_id（访问凭证）读取子智能体产出的成果正文"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "artifact_id": { "type": "string" } },
            "required": ["artifact_id"]
        })
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Local
    }

    fn default_wait(&self) -> bool {
        true
    }

    async fn execute(&self, _ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        let id = args
            .get("artifact_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArguments("missing artifact_id".into()))?;
        match self.store.get(id).await {
            Ok(Some(a)) => Ok(ToolOutput::text(
                String::from_utf8_lossy(&a.bytes).to_string(),
            )),
            Ok(None) => Err(ToolError::Execution("artifact not found".into())),
            Err(e) => Err(ToolError::Execution(format!("{e}"))),
        }
    }
}

/// SystemTime → Unix 秒（展示用时间戳，不可排序场景回退 0）
fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Artifact, InMemoryArtifactStore};

    fn ctx(session_id: uuid::Uuid) -> ToolContext {
        ToolContext {
            tool_call_id: "tc".into(),
            session_id,
            turn_id: 0,
            kernel: None,
            store: None,
            wait: true,
            peer_depth: 0,
        }
    }

    #[tokio::test]
    async fn list_my_board_returns_own_entries_sorted() {
        let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());
        let parent = uuid::Uuid::new_v4();
        let child = uuid::Uuid::new_v4();
        let board = store.ensure_board(parent).await.unwrap();
        store
            .store(Artifact::new(
                board,
                child,
                "sub_agent",
                "first",
                "text/plain",
                b"a".to_vec(),
            ))
            .await
            .unwrap();
        store
            .store(Artifact::new(
                board,
                child,
                "sub_agent",
                "second",
                "text/plain",
                b"b".to_vec(),
            ))
            .await
            .unwrap();

        let tool = ListMyBoard::new(store);
        let out = tool.execute(ctx(parent), json!({})).await.unwrap();
        let rows: Value = serde_json::from_str(&out.content).unwrap();
        let arr = rows.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["title"], "first");
        assert_eq!(arr[1]["title"], "second");
    }

    #[tokio::test]
    async fn read_artifact_returns_body() {
        let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());
        let parent = uuid::Uuid::new_v4();
        let child = uuid::Uuid::new_v4();
        let board = store.ensure_board(parent).await.unwrap();
        let id = store
            .store(Artifact::new(
                board,
                child,
                "sub",
                "t",
                "text/plain",
                b"hello body".to_vec(),
            ))
            .await
            .unwrap();

        let tool = ArtifactReader::new(store);
        let out = tool
            .execute(ctx(parent), json!({"artifact_id": id}))
            .await
            .unwrap();
        assert_eq!(out.content, "hello body");
    }

    #[tokio::test]
    async fn read_artifact_not_found() {
        let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());
        let tool = ArtifactReader::new(store);
        let err = tool
            .execute(ctx(uuid::Uuid::new_v4()), json!({"artifact_id": "nope"}))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }
}

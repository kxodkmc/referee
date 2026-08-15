//! `write` 工具 — 创建 / 完全替换文件（原子写，防半写）

use async_trait::async_trait;
use referee_ai_base::tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
use serde_json::{json, Value};

use super::fs_common::{atomic_write, resolve_path, FsConfig};

/// `write` 工具
pub struct WriteTool {
    config: FsConfig,
}

impl WriteTool {
    pub fn new(config: FsConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Tool for WriteTool {
    fn name(&self) -> &str {
        "write"
    }

    fn description(&self) -> &str {
        "创建文件或完全替换文件内容；写入是原子的（临时文件 + rename），不会留下半写状态"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": { "type": "string", "description": "目标文件路径（相对或绝对）" },
                "content": { "type": "string", "description": "完整文件内容" }
            },
            "required": ["file_path", "content"]
        })
    }

    fn category(&self) -> ToolCategory {
        ToolCategory::Remote
    }

    fn default_wait(&self) -> bool {
        true
    }

    async fn execute(&self, _ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        let file_path = args
            .get("file_path")
            .and_then(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| ToolError::InvalidArguments("missing file_path".into()))?;
        let content = args
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArguments("missing content".into()))?;

        // 内容上限（防 OOM）
        if content.len() as u64 > self.config.max_file_bytes {
            return Err(ToolError::Execution("content exceeds max_file_bytes".into()));
        }

        let path = resolve_path(file_path, &self.config.root)?;
        let existed = tokio::fs::metadata(&path).await.is_ok();
        atomic_write(&path, content.as_bytes()).await?;

        Ok(ToolOutput::from_json(&json!({
            "path": path.display().to_string(),
            "operation": if existed { "update" } else { "create" },
            "bytes_written": content.len()
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn ctx() -> ToolContext {
        ToolContext {
            tool_call_id: "tc".into(),
            session_id: Uuid::new_v4(),
            turn_id: 0,
            kernel: None,
            store: None,
            wait: true,
            peer_depth: 0,
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join(format!("referee_write_{name}_"))
            .join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn write_creates_new_file() {
        let dir = temp_dir("create");
        let file = dir.join("new.txt");
        let tool = WriteTool::new(FsConfig::default());
        let out = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "content": "hello" }))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&out.content).unwrap();
        assert_eq!(v["operation"], "create");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello");
    }

    #[tokio::test]
    async fn write_overwrites_existing() {
        let dir = temp_dir("update");
        let file = dir.join("a.txt");
        std::fs::write(&file, "old").unwrap();
        let tool = WriteTool::new(FsConfig::default());
        let out = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "content": "new" }))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&out.content).unwrap();
        assert_eq!(v["operation"], "update");
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "new");
    }

    #[tokio::test]
    async fn write_missing_args_is_invalid_arguments() {
        let tool = WriteTool::new(FsConfig::default());
        assert!(matches!(
            tool.execute(ctx(), json!({})).await.unwrap_err(),
            ToolError::InvalidArguments(_)
        ));
        assert!(matches!(
            tool.execute(ctx(), json!({ "file_path": "x" })).await.unwrap_err(),
            ToolError::InvalidArguments(_)
        ));
    }

    #[tokio::test]
    async fn write_content_over_limit_is_execution_error() {
        let dir = temp_dir("over");
        let file = dir.join("big.txt");
        let tool = WriteTool::new(FsConfig { max_file_bytes: 4, ..FsConfig::default() });
        let err = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "content": "12345" }))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }
}
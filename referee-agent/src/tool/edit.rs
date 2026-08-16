//! `edit` 工具 — 精确替换 old_string → new_string（支持 replace_all）
//!
//! 安全点：
//! - 原子写防半写；
//! - `replace_all=false` 时 old_string 须恰好出现 1 次，防止模型误替换；
//! - 拒绝空 old_string；仅处理合法 UTF-8 文本（`from_utf8` 严格校验，绝不 lossy 改写）
//!   并对二进制嗅探，杜绝损坏文件。

use async_trait::async_trait;
use referee_ai::tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
use serde_json::{json, Value};

use super::fs_common::{atomic_write, looks_binary, read_bounded, resolve_path, FsConfig};

/// `edit` 工具
pub struct EditTool {
    config: FsConfig,
}

impl EditTool {
    pub fn new(config: FsConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Tool for EditTool {
    fn name(&self) -> &str {
        "edit"
    }

    fn description(&self) -> &str {
        "精确替换文件中的字面文本；默认 old_string 必须恰好出现一次，多处需 replace_all=true；写入为原子操作"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": { "type": "string", "description": "目标文件路径" },
                "old_string": { "type": "string", "description": "要替换的字面文本（非空）" },
                "new_string": { "type": "string", "description": "替换后的文本（空串=删除）" },
                "replace_all": { "type": "boolean", "description": "是否替换所有匹配，默认 false" }
            },
            "required": ["file_path", "old_string", "new_string"]
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
        let old_string = args
            .get("old_string")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArguments("missing old_string".into()))?;
        let new_string = args
            .get("new_string")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let replace_all = args
            .get("replace_all")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // 防御：拒绝空 old_string（防歧义 / 无限替换）
        if old_string.is_empty() {
            return Err(ToolError::InvalidArguments("old_string must not be empty".into()));
        }

        let path = resolve_path(file_path, &self.config.root)?;
        let bytes = read_bounded(&path, self.config.max_file_bytes).await?;

        // 二进制 / 非 UTF-8 拒绝：绝不 lossy 改写（会损坏文件）
        if looks_binary(&bytes) {
            return Err(ToolError::Execution(
                "file appears to be binary; edit supports UTF-8 text only".into(),
            ));
        }
        let text = String::from_utf8(bytes)
            .map_err(|_| ToolError::Execution("file is not valid UTF-8 text; edit aborted".into()))?;

        let count = count_occurrences(&text, old_string);
        if count == 0 {
            return Err(ToolError::Execution(format!(
                "old_string not found in {}",
                path.display()
            )));
        }
        if !replace_all && count > 1 {
            return Err(ToolError::Execution(format!(
                "old_string appears {count} times; set replace_all=true or provide a more specific old_string"
            )));
        }
        let new_text = if replace_all {
            text.replace(old_string, new_string)
        } else {
            text.replacen(old_string, new_string, 1)
        };

        atomic_write(&path, new_text.as_bytes()).await?;

        // 只回传计数与路径，不回传全文（避免大文件 token 爆炸；模型已知 old/new）
        Ok(ToolOutput::from_json(&json!({
            "path": path.display().to_string(),
            "replacements": count
        })))
    }
}

/// 统计 old_string 在 text 中的非重叠出现次数
///
/// 用 `match_indices`：按字节边界正确匹配，不会把多字节字符中间切开；
/// 与 `replace`/`replacen` 的非重叠匹配语义一致。
fn count_occurrences(text: &str, needle: &str) -> usize {
    text.match_indices(needle).count()
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
            .join(format!("referee_edit_{name}_"))
            .join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn edit_single_replacement() {
        let dir = temp_dir("single");
        let file = dir.join("a.txt");
        std::fs::write(&file, "hello world").unwrap();
        let tool = EditTool::new(FsConfig::default());
        let out = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "old_string": "world", "new_string": "there" }))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&out.content).unwrap();
        assert_eq!(v["replacements"], 1);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello there");
    }

    #[tokio::test]
    async fn edit_multiple_without_replace_all_is_error() {
        let dir = temp_dir("multi");
        let file = dir.join("a.txt");
        std::fs::write(&file, "a-b a-b a-b").unwrap();
        let tool = EditTool::new(FsConfig::default());
        let err = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "old_string": "a-b", "new_string": "x" }))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }

    #[tokio::test]
    async fn edit_replace_all_replaces_everywhere() {
        let dir = temp_dir("all");
        let file = dir.join("a.txt");
        std::fs::write(&file, "a-b a-b a-b").unwrap();
        let tool = EditTool::new(FsConfig::default());
        let out = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "old_string": "a-b", "new_string": "x", "replace_all": true }))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&out.content).unwrap();
        assert_eq!(v["replacements"], 3);
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "x x x");
    }

    #[tokio::test]
    async fn edit_not_found_is_error() {
        let dir = temp_dir("notfound");
        let file = dir.join("a.txt");
        std::fs::write(&file, "hello").unwrap();
        let tool = EditTool::new(FsConfig::default());
        let err = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "old_string": "zzz", "new_string": "x" }))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }

    #[tokio::test]
    async fn edit_empty_old_string_is_invalid_arguments() {
        let dir = temp_dir("empty");
        let file = dir.join("a.txt");
        std::fs::write(&file, "hello").unwrap();
        let tool = EditTool::new(FsConfig::default());
        let err = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "old_string": "", "new_string": "x" }))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn edit_chinese_boundary() {
        let dir = temp_dir("zh");
        let file = dir.join("a.txt");
        std::fs::write(&file, "你好世界，你好世界").unwrap();
        let tool = EditTool::new(FsConfig::default());
        tool.execute(
            ctx(),
            json!({ "file_path": file.display().to_string(), "old_string": "世界", "new_string": "图门", "replace_all": true }),
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "你好图门，你好图门");
    }

    #[tokio::test]
    async fn edit_rejects_binary() {
        let dir = temp_dir("bin");
        let file = dir.join("a.bin");
        std::fs::write(&file, b"\x00\x01\x02 payload").unwrap();
        let tool = EditTool::new(FsConfig::default());
        let err = tool
            .execute(ctx(), json!({ "file_path": file.display().to_string(), "old_string": "x", "new_string": "y" }))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }
}
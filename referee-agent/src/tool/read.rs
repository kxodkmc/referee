//! 本地文本读取工具 — 字符级窗口 + 行/句边界回溯的语义自然截断
//!
//! 设计约束（对齐 Referee 工具机制）：
//! - `Remote` 分类：文件 IO 受执行器 Semaphore 限流；
//! - `default_wait() = true`：同步查询类工具，模型需结果才能继续；
//! - 安全：二进制嗅探 + `from_utf8_lossy` 兜底（防 panic）；`max_file_bytes` 防 OOM；
//! - 确定性：窗口边界回溯是纯函数，相同输入相同输出，可单测。
//!
//! 明确不做（本期）：`.docx` 等二进制文档解析、NLP 语义连贯性匹配、自动分页。

use std::path::PathBuf;

use async_trait::async_trait;
use referee_ai_base::tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
use serde::Serialize;
use serde_json::{json, Value};

use super::fs_common::{looks_binary, read_bounded, resolve_path, FsConfig, SENTENCE_BOUNDARIES};

/// 默认窗口字符数
const DEFAULT_LIMIT_CHARS: usize = 3000;
/// 窗口字符数上限（背压硬约束）
const MAX_LIMIT_CHARS: usize = 20000;

/// 工具配置（可配置，无硬编码 tunable）
#[derive(Debug, Clone)]
pub struct ReadToolConfig {
    /// 默认窗口字符数
    pub default_limit_chars: usize,
    /// 窗口字符数上限
    pub max_limit_chars: usize,
    /// 单文件读取字节上限
    pub max_file_bytes: u64,
    /// 可选的根目录约束（None = 不限制，安全交由沙箱/上层）
    pub root: Option<PathBuf>,
}

impl Default for ReadToolConfig {
    fn default() -> Self {
        Self {
            default_limit_chars: DEFAULT_LIMIT_CHARS,
            max_limit_chars: MAX_LIMIT_CHARS,
            max_file_bytes: super::fs_common::DEFAULT_MAX_FILE_BYTES,
            root: None,
        }
    }
}

/// `read` 参数
#[derive(Debug, Clone, PartialEq)]
pub struct ReadArgs {
    pub file_path: String,
    pub offset: usize,
    pub limit: usize,
}

/// `read` 返回的结构化结果（经 `ToolOutput::from_json` 序列化）
///
/// 关键：`content` 之外必须带 `offset`/`end`/`total_chars`，
/// 让模型知道实际读取的窗口区间（边界回溯可能缩短 `end`）。
#[derive(Debug, Serialize)]
pub struct ReadMeta {
    pub file_path: String,
    /// 实际起始字符索引（= 请求的 offset）
    pub offset: usize,
    /// 实际结束字符索引（exclusive；边界回溯后 ≤ offset+limit）
    pub end: usize,
    /// 文件总字符数
    pub total_chars: usize,
    /// 窗口后是否仍有余量（提示模型用 offset=end 继续读）
    pub truncated: bool,
    /// 窗口内容（行/句边界对齐，行内容完整）
    pub content: String,
}

/// `read` 工具
pub struct ReadTool {
    config: ReadToolConfig,
}

impl ReadTool {
    pub fn new(config: ReadToolConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Tool for ReadTool {
    fn name(&self) -> &str {
        "read"
    }

    fn description(&self) -> &str {
        "读取本地 UTF-8 文本文件，返回行/句边界对齐的字符窗口与实际区间；用 read 而非 shell cat"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "file_path": { "type": "string", "description": "目标文件路径（相对或绝对）" },
                "offset": { "type": "integer", "minimum": 0, "description": "起始字符索引，默认 0" },
                "limit": { "type": "integer", "minimum": 1, "description": "窗口字符数上限，默认 3000" }
            },
            "required": ["file_path"]
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

        let offset = args
            .get("offset")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        // limit 封底到 1（避免空窗口），再封顶到 max_limit_chars
        let limit = args
            .get("limit")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(self.config.default_limit_chars)
            .max(1);

        let fs = FsConfig {
            max_file_bytes: self.config.max_file_bytes,
            root: self.config.root.clone(),
        };
        let path = resolve_path(file_path, &fs.root)?;
        let bytes = read_bounded(&path, fs.max_file_bytes).await?;

        // 二进制嗅探：前 N 字节含 NUL 即视为二进制（防乱码 / 非法 UTF-8）
        if looks_binary(&bytes) {
            return Err(ToolError::Execution(
                "file appears to be binary; read supports UTF-8 text only".into(),
            ));
        }

        let text = String::from_utf8_lossy(&bytes);
        let chars: Vec<char> = text.chars().collect();
        let (start, end) = window_chars(&chars, offset, limit, self.config.max_limit_chars);
        let content: String = chars[start..end].iter().collect();
        let truncated = end < chars.len();

        let meta = ReadMeta {
            file_path: path.display().to_string(),
            offset: start,
            end,
            total_chars: chars.len(),
            truncated,
            content,
        };
        Ok(ToolOutput::from_json(&json!(meta)))
    }
}

/// 按字符索引取窗口；`start` 收敛到文件末尾，`limit` 封顶 `max_limit_chars`
fn window_chars(
    chars: &[char],
    offset: usize,
    limit: usize,
    max_limit_chars: usize,
) -> (usize, usize) {
    let start = offset.min(chars.len());
    let bounded = limit.min(max_limit_chars);
    let raw_end = start.saturating_add(bounded).min(chars.len());
    let end = back_to_boundary(chars, start, raw_end);
    (start, end)
}

/// 从 `raw_end` 向前回溯到行/句边界（仅影响结束位置，确定性）
///
/// 1. 找最近换行 `\n` → 取其后一位（该行完整结束）；
/// 2. 无换行：找最近句子结束符 → 取其后一位；
/// 3. 都无：保持 `raw_end`。
fn back_to_boundary(chars: &[char], start: usize, raw_end: usize) -> usize {
    if raw_end > start {
        // 1. 行边界（优先级最高）
        let mut i = raw_end;
        while i > start {
            i -= 1;
            if chars[i] == '\n' {
                return i + 1;
            }
        }
        // 2. 句子结束符
        let mut j = raw_end;
        while j > start {
            j -= 1;
            if SENTENCE_BOUNDARIES.contains(&chars[j]) {
                return j + 1;
            }
        }
    }
    // 3. 无边界，保持原始窗口
    raw_end
}

#[cfg(test)]
mod tests {
    use super::*;
    use referee_ai_base::tool::ToolError;
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

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("referee_read_{}-{}", name, Uuid::new_v4()))
    }

    // ── 纯函数 ─────────────────────────────

    #[test]
    fn looks_binary_detects_nul() {
        assert!(looks_binary(b"abc\0def"));
        assert!(!looks_binary(b"plain ascii"));
        assert!(!looks_binary(b""));
    }

    #[test]
    fn window_chars_basic() {
        let chars: Vec<char> = "hello world".chars().collect();
        assert_eq!(window_chars(&chars, 6, 5, 100), (6, 11));
    }

    #[test]
    fn window_chars_offset_clamped_to_eof() {
        let chars: Vec<char> = "hello".chars().collect();
        assert_eq!(window_chars(&chars, 100, 5, 100), (5, 5));
    }

    #[test]
    fn window_chars_limit_capped() {
        let chars: Vec<char> = "aaaaaaaaaa".chars().collect();
        assert_eq!(window_chars(&chars, 0, usize::MAX, 4), (0, 4));
    }

    #[test]
    fn back_to_boundary_line() {
        let chars: Vec<char> = "aaa\nbbb".chars().collect();
        // raw_end=7（越过换行），回溯到换行后一位 => 4
        assert_eq!(back_to_boundary(&chars, 0, 7), 4);
    }

    #[test]
    fn back_to_boundary_sentence() {
        let chars: Vec<char> = "你好。世界".chars().collect();
        // raw_end=5，无换行，回溯到 '。'（索引 2）后一位 => 3
        assert_eq!(back_to_boundary(&chars, 0, 5), 3);
    }

    #[test]
    fn back_to_boundary_no_boundary_keeps_raw() {
        let chars: Vec<char> = "abcdef".chars().collect();
        assert_eq!(back_to_boundary(&chars, 0, 6), 6);
    }

    #[test]
    fn back_to_boundary_chinese_multibyte_no_panic() {
        // 中文按 char 索引，不按字节，回溯不会 panic
        let chars: Vec<char> = "这是一句话".chars().collect();
        assert_eq!(chars.len(), 5);
        // raw_end 已由 window_chars 封顶到 len，此处传 5
        assert_eq!(back_to_boundary(&chars, 0, 5), 5);
    }

    // ── 工具级集成 ─────────────────────────

    #[tokio::test]
    async fn execute_reads_window_with_meta() {
        let path = temp_path("win");
        std::fs::write(&path, "line1\nline2\nline3\n").unwrap();
        let tool = ReadTool::new(ReadToolConfig::default());
        let out = tool
            .execute(ctx(), json!({ "file_path": path.display().to_string(), "limit": 20 }))
            .await
            .unwrap();
        let v: Value = serde_json::from_str(&out.content).unwrap();
        assert_eq!(v["offset"], 0);
        // 尾部回溯到换行后一位，内容为前三行完整
        assert_eq!(v["content"], "line1\nline2\nline3\n");
        assert_eq!(v["total_chars"], 18);
        assert_eq!(v["truncated"], false);
    }

    #[tokio::test]
    async fn execute_rejects_binary() {
        let path = temp_path("bin");
        std::fs::write(&path, b"\x00\x01\x02\x03").unwrap();
        let tool = ReadTool::new(ReadToolConfig::default());
        let err = tool
            .execute(ctx(), json!({ "file_path": path.display().to_string() }))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }

    #[tokio::test]
    async fn execute_missing_file_path_is_invalid_arguments() {
        let tool = ReadTool::new(ReadToolConfig::default());
        let err = tool.execute(ctx(), json!({})).await.unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    async fn execute_root_restricts_escape() {
        let dir = temp_path("dir");
        std::fs::create_dir_all(&dir).unwrap();
        let outside = dir.with_extension("outside");
        std::fs::write(&outside, "secret").unwrap();
        let tool = ReadTool::new(ReadToolConfig {
            root: Some(dir.clone()),
            ..Default::default()
        });
        let err = tool
            .execute(ctx(), json!({ "file_path": outside.display().to_string() }))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }
}
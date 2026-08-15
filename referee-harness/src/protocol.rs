//! 协议类型 — 纯数据 serde 载体（与传输解耦）
//!
//! 本模块只定义「客户端与 daemon 之间传输什么」，不承载任何业务逻辑；
//! 复用的业务类型（`AgentDefinition` / `EngineConfig`）来自下层 crate。
//! 所有类型均 `Serialize + Deserialize`，可跨传输层（TCP / 未来 HTTP）复用。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use referee_ai_base::engine::EngineConfig;
use referee_agent::AgentDefinition;
use referee_agent::AgentId;

// ── 实例身份 ──────────────────────────────────

/// kebab-case 实例标识（与 `AgentId` 同规则校验；管理器强制唯一）
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstanceId(String);

/// 实例标识错误
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InstanceIdError {
    #[error("instance id '{0}' invalid: use kebab-case (lowercase/digits/hyphen), <=64 chars, no leading/trailing/consecutive hyphen")]
    Invalid(String),
}

impl InstanceId {
    /// 校验并构造（kebab-case，规则与 `AgentId` 一致）
    pub fn new(s: impl Into<String>) -> Result<Self, InstanceIdError> {
        let s = s.into();
        // 复用 AgentId 的 kebab-case 强校验（保证全项目命名统一）
        AgentId::new(&s).map_err(|_| InstanceIdError::Invalid(s.clone()))?;
        Ok(Self(s))
    }

    /// 自动生成 kebab-case 标识（`general-<uuid8>`）
    pub fn generate() -> Self {
        let uuid8 = &uuid::Uuid::new_v4().to_string()[..8];
        Self(format!("general-{uuid8}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// ── 实例规格（声明式 JSON，create 时提交）──────

/// 实例规格 — 全声明式 JSON，零代码创建实例
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceSpec {
    /// 实例身份（kebab-case；空则自动生成）
    pub id: Option<String>,
    /// Agent 定义（复用 `AgentDefinition`：model / template / tools / skills / params）
    pub agent: AgentDefinition,
    /// 引擎配置
    #[serde(default)]
    pub engine: EngineConfig,
    /// 模板变量（`bind_with` 插值，如 `{"cwd": "/workspace"}`）
    #[serde(default)]
    pub template_vars: HashMap<String, String>,
    /// 工具选配
    #[serde(default)]
    pub tools: InstanceTools,
    /// 厂商配置
    #[serde(default)]
    pub provider: ProviderConfig,
}

/// 实例的工具选配
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceTools {
    /// 启用文件读写编辑工具（read / write / edit）
    pub fs: Option<FsToolConfig>,
    /// 启用成果板工具（list_my_board / read_artifact）
    pub artifact: bool,
}

/// 文件工具配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsToolConfig {
    /// 根目录约束（实例工作区根，实例间文件视图互不可见）
    pub root: Option<String>,
    /// 单文件读取上限
    pub max_file_bytes: u64,
    /// 默认窗口字符数
    pub default_limit_chars: usize,
}

impl Default for FsToolConfig {
    fn default() -> Self {
        Self {
            root: None,
            max_file_bytes: 1_048_576,
            default_limit_chars: 3000,
        }
    }
}

/// 厂商配置（运行时决定，不硬编码 feature）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProviderConfig {
    #[serde(rename = "deepseek")]
    DeepSeek {
        api_key: String,
        base_url: Option<String>,
        /// model 覆盖 `AgentDefinition.model`
        #[serde(default)]
        model: Option<String>,
    },
    #[serde(rename = "xiaomi")]
    XiaoMi {
        api_key: String,
        base_url: Option<String>,
    },
    #[serde(rename = "openai")]
    OpenAI {
        api_key: String,
        base_url: Option<String>,
        model: String,
    },
}

/// 实例规格缺省厂商（`#[serde(default)]` 反序列化兜底；空 key 需实例创建时显式配置）
impl Default for ProviderConfig {
    fn default() -> Self {
        Self::DeepSeek {
            api_key: String::new(),
            base_url: None,
            model: None,
        }
    }
}

// ── 实例信息（list/get 响应）──────────────────

/// 实例信息 — 观测视图（状态 / 会话 / 指标）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub id: InstanceId,
    pub model: String,
    pub state: InstanceState,
    pub sessions: usize,
    pub max_sessions: usize,
    pub consumed_tokens: u64,
    pub cache_entries: usize,
    /// ISO 8601
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceState {
    Running,
    Stopped,
}

/// 会话信息（instance.sessions 响应）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub id: String,
    pub messages: usize,
    pub phase: String,
    pub consumed_tokens: u64,
}

// ── 对话协议 ──────────────────────────────────

/// 单轮对话请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    /// 会话标识（空则自动生成）
    pub session_id: Option<String>,
    pub message: String,
    #[serde(default)]
    pub stream: bool,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<usize>,
}

/// 单轮对话响应（非流式）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatReply {
    pub session_id: String,
    pub content: String,
    pub finish_reason: String,
    pub usage: Option<TokenUsageData>,
}

/// 流式帧（对齐 base `StreamChunk` + serde）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamFrame {
    #[serde(rename = "delta")]
    Delta {
        content: Option<String>,
        reasoning_content: Option<String>,
    },
    #[serde(rename = "finish")]
    Finish {
        finish_reason: String,
        usage: Option<TokenUsageData>,
    },
    #[serde(rename = "error")]
    Error { message: String },
}

/// Token 用量（跨进程载荷）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsageData {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

impl From<&referee_ai_base::provider::TokenUsage> for TokenUsageData {
    fn from(u: &referee_ai_base::provider::TokenUsage) -> Self {
        Self {
            prompt_tokens: u.prompt_tokens as u64,
            completion_tokens: u.completion_tokens as u64,
            total_tokens: u.total_tokens as u64,
        }
    }
}

// ── 管理错误 ──────────────────────────────────

/// 服务端错误响应载荷
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerError {
    pub code: i32,
    pub message: String,
}

impl ServerError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

/// 实例不存在
pub const ERR_INSTANCE_NOT_FOUND: i32 = -32000;
/// 实例容量已满
pub const ERR_INSTANCE_FULL: i32 = -32001;
/// 会话忙碌
pub const ERR_SESSION_BUSY: i32 = -32002;
/// 内部错误
pub const ERR_INTERNAL: i32 = -32003;
/// 实例规格非法
pub const ERR_INVALID_SPEC: i32 = -32004;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_id_valid_kebab_case() {
        for good in ["my-agent", "general-abc123", "a1", "x-y-z"] {
            assert!(InstanceId::new(good).is_ok(), "'{good}' must pass");
        }
    }

    #[test]
    fn instance_id_rejects_invalid() {
        for bad in ["", "MyAgent", "my agent", "-my", "my-", "a--b", &"x".repeat(65)] {
            assert!(InstanceId::new(bad).is_err(), "'{bad}' must fail");
        }
    }

    #[test]
    fn instance_id_generate_is_kebab_case() {
        let id = InstanceId::generate();
        assert!(id.as_str().starts_with("general-"));
        assert!(InstanceId::new(id.as_str()).is_ok());
    }

    #[test]
    fn provider_config_serde_roundtrip() {
        let p = ProviderConfig::DeepSeek {
            api_key: "k".into(),
            base_url: None,
            model: None,
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"type\":\"deepseek\""));
        let back: ProviderConfig = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, ProviderConfig::DeepSeek { .. }));
    }

    #[test]
    fn stream_frame_serde_roundtrip() {
        let f = StreamFrame::Delta {
            content: Some("hi".into()),
            reasoning_content: None,
        };
        let json = serde_json::to_string(&f).unwrap();
        assert!(json.contains("\"type\":\"delta\""));
        let back: StreamFrame = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, StreamFrame::Delta { .. }));
    }

    #[test]
    fn token_usage_data_from_base() {
        let u = referee_ai_base::provider::TokenUsage {
            prompt_tokens: 1,
            completion_tokens: 2,
            total_tokens: 3,
            ..Default::default()
        };
        let d = TokenUsageData::from(&u);
        assert_eq!((d.prompt_tokens, d.completion_tokens, d.total_tokens), (1, 2, 3));
    }
}
//! Agent 定义 — 纯数据（声明式），可来自 TOML / builder
//!
//! 数据/行为分离：本模块只定义"一个 Agent 是什么"，不负责装配。
//! 装配（解析白名单、解析模板、生成系统片段）在 [`crate::agent::builder`]。
//!
//! 白名单语义（封闭默认）：
//! - `["*"]` = 继承全部能力
//! - `["a", "b"]` = 仅白名单内能力
//! - `[]` = 明确无该能力（缺省即空，不注入）

use serde::{Deserialize, Serialize};

use super::id::AgentId;

/// 系统提示词模板引用 — 决定静态段内容
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum TemplateRef {
    /// 通用模板（默认）
    #[default]
    #[serde(rename = "generic")]
    Generic,
    /// 针对 DeepSeek 优化的模板
    #[serde(rename = "deepseek")]
    DeepSeek,
    /// 针对 Claude 优化的模板
    #[serde(rename = "claude")]
    Claude,
    /// 内联文本模板
    #[serde(rename = "inline")]
    Inline(String),
}

/// 生成参数（调用侧偏好）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChatParams {
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<usize>,
}

/// Agent 定义 — 纯数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDefinition {
    /// 可调用标识（唯一、规范）
    pub id: AgentId,
    /// 人类可读描述
    pub description: String,
    /// 厂商 + 模型标识（如 `"deepseek/deepseek-v3"`）
    pub model: String,
    pub template: TemplateRef,
    /// 允许的工具名；`["*"]`=全部，`[]`=无工具（封闭默认）
    #[serde(default)]
    pub tools: Vec<String>,
    /// 允许的技能名；`["*"]`=全部，`[]`=无技能
    #[serde(default)]
    pub skills: Vec<String>,
    /// 允许的 MCP 服务名；`["*"]`=全部，`[]`=无 MCP
    #[serde(default)]
    pub mcp_servers: Vec<String>,
    #[serde(default)]
    pub params: ChatParams,
}

impl AgentDefinition {
    /// 组装入口（行为）— 见 [`crate::agent::builder`]
    pub fn builder() -> super::builder::AgentBuilder {
        super::builder::AgentBuilder::default()
    }
}

/// 通配符：表示继承全部能力
pub const WILDCARD_ALL: &str = "*";

/// 将白名单解析为集合语义：`["*"]` → None（全部），其余 → Some(集合)
pub(crate) fn whitelist_to_set(list: &[String]) -> Option<std::collections::HashSet<String>> {
    if list.iter().any(|s| s == WILDCARD_ALL) {
        None // 通配 = 全部（不过滤）
    } else {
        Some(list.iter().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitelist_semantics() {
        // [*] → None（全部）
        assert!(whitelist_to_set(&["*".into()]).is_none());
        // 空 → Some(空集)（封闭）
        assert_eq!(whitelist_to_set(&[]), Some(Default::default()));
        // 具体 → Some(集合)
        let set = whitelist_to_set(&["a".into(), "b".into()]).unwrap();
        assert!(set.contains("a") && set.contains("b"));
    }

    #[test]
    fn serde_json_roundtrip() {
        // AgentDefinition 是 serde 数据载体；具体格式（TOML/JSON）由消费方决定，
        // 本项目零新增依赖，仅验证 JSON 往返（serde_json 已在白名单内）。
        let def = AgentDefinition {
            id: AgentId::new("coder").unwrap(),
            description: "Code agent".into(),
            model: "deepseek/deepseek-v3".into(),
            template: TemplateRef::DeepSeek,
            tools: vec!["apply_patch".into(), "grep".into()],
            skills: vec![],
            mcp_servers: vec![],
            params: ChatParams {
                temperature: Some(0.2),
                max_tokens: None,
            },
        };
        let json = serde_json::to_string(&def).unwrap();
        let back: AgentDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, def.id);
        assert_eq!(back.model, "deepseek/deepseek-v3");
        assert!(matches!(back.template, TemplateRef::DeepSeek));
        assert_eq!(back.tools, def.tools);
        assert_eq!(back.params.temperature, Some(0.2));
    }
}
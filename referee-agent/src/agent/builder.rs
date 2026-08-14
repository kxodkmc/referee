//! Agent 装配（行为层）— 把纯数据定义解析为可用的 [`BoundAgent`]
//!
//! 数据/行为分离：定义在 [`super::definition`]，本模块负责
//! - [`AgentBuilder`]：链式构造 `AgentDefinition`
//! - [`AgentDefinition::bind`]：解析白名单 + 渲染模板 → [`BoundAgent`]
//!
//! Skill / MCP 的注册表注入由上层（AgentRuntime 集成）按 feature 门控接入，
//! 本模块只解析白名单为集合语义，不依赖 skill/mcp 模块（任一 feature 未启用
//! 均可独立编译）。

use std::collections::HashSet;
use std::sync::Arc;

use referee_ai_base::provider::ToolDeclaration;
use referee_ai_base::prompt::SystemSection;
use referee_ai_base::tool::ToolRegistry;

use super::definition::{whitelist_to_set, AgentDefinition, ChatParams, TemplateRef};
use super::id::AgentId;
use super::template::{interpolate, TemplateError, TemplateRegistry};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuilderError {
    #[error("agent id is required")]
    MissingId,
}

/// 链式构造 `AgentDefinition`
#[derive(Default)]
pub struct AgentBuilder {
    id: Option<AgentId>,
    description: String,
    model: String,
    template: TemplateRef,
    tools: Vec<String>,
    skills: Vec<String>,
    mcp_servers: Vec<String>,
    params: ChatParams,
}

impl AgentBuilder {
    pub fn id(mut self, id: AgentId) -> Self {
        self.id = Some(id);
        self
    }

    pub fn description(mut self, d: impl Into<String>) -> Self {
        self.description = d.into();
        self
    }

    pub fn model(mut self, m: impl Into<String>) -> Self {
        self.model = m.into();
        self
    }

    pub fn template(mut self, t: TemplateRef) -> Self {
        self.template = t;
        self
    }

    pub fn tools(mut self, tools: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tools = tools.into_iter().map(Into::into).collect();
        self
    }

    pub fn skills(mut self, skills: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.skills = skills.into_iter().map(Into::into).collect();
        self
    }

    pub fn mcp_servers(mut self, servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.mcp_servers = servers.into_iter().map(Into::into).collect();
        self
    }

    pub fn params(mut self, p: ChatParams) -> Self {
        self.params = p;
        self
    }

    pub fn build(self) -> Result<AgentDefinition, BuilderError> {
        Ok(AgentDefinition {
            id: self.id.ok_or(BuilderError::MissingId)?,
            description: self.description,
            model: self.model,
            template: self.template,
            tools: self.tools,
            skills: self.skills,
            mcp_servers: self.mcp_servers,
            params: self.params,
        })
    }
}

/// 装配结果 — 定义 + 解析后的白名单 + 渲染后的系统片段
#[derive(Debug, Clone)]
pub struct BoundAgent {
    pub def: Arc<AgentDefinition>,
    /// 系统片段（模板静态段；技能段由上层按 feature 追加）
    pub system_sections: Vec<SystemSection>,
    /// 工具白名单：None=全部，Some(集合)=仅命中
    pub tools: Option<HashSet<String>>,
    pub skills: Option<HashSet<String>>,
    pub mcp_servers: Option<HashSet<String>>,
}

impl BoundAgent {
    /// 按白名单 + 子 Agent 深度过滤出工具声明（空则无工具）
    pub fn tool_declarations(
        &self,
        registry: &ToolRegistry,
        current_depth: u32,
        max_depth: u32,
    ) -> Vec<ToolDeclaration> {
        registry.declarations_visible(self.tools.as_ref(), current_depth, max_depth)
    }
}

impl AgentDefinition {
    /// 无注册表绑定：仅内建模板（Generic/DeepSeek/Claude/Inline）。
    ///
    /// `Named` 命名槽位必须经 [`Self::bind_with`] 提供注册表；缺失视为编程错误（panic）。
    pub fn bind(self: &Arc<Self>) -> BoundAgent {
        self.bind_with(None, &[])
            .unwrap_or_else(|e| panic!("bind requires a TemplateRegistry for named templates: {e}"))
    }

    /// 注册表感知绑定 — 解析白名单 + 渲染模板 → [`BoundAgent`]
    ///
    /// 传递设计（参考 DSH `AssembleContext`）：`Named` 模板经 `templates` 解析
    /// （可替换语义），模板中的 `{{variable}}` 用 `vars` 严格插值，产出稳定
    /// `SystemSection` 供 base `assemble` 使用。
    pub fn bind_with(
        self: &Arc<Self>,
        templates: Option<&TemplateRegistry>,
        vars: &[(&str, &str)],
    ) -> Result<BoundAgent, TemplateError> {
        Ok(BoundAgent {
            def: self.clone(),
            system_sections: vec![render_template(&self.template, templates, vars)?],
            tools: whitelist_to_set(&self.tools),
            skills: whitelist_to_set(&self.skills),
            mcp_servers: whitelist_to_set(&self.mcp_servers),
        })
    }
}

/// 内建模板静态段（最小占位，供上层按需定制）
const GENERIC_TEXT: &str = "You are a helpful AI assistant.";
const DEEPSEEK_TEXT: &str = "You are a capable AI coding assistant. Work step by step, verify \
     your results, and report honestly including any verification failures.";
const CLAUDE_TEXT: &str = "You are a capable AI coding assistant. Follow tool-use guidance, make \
     precise edits, and verify changes before reporting.";

/// 渲染模板为稳定系统片段；`Named` 经注册表解析（可替换），空模板标记可省略
fn render_template(
    t: &TemplateRef,
    templates: Option<&TemplateRegistry>,
    vars: &[(&str, &str)],
) -> Result<SystemSection, TemplateError> {
    let (text, omit_if_empty) = match t {
        TemplateRef::Generic => (GENERIC_TEXT.to_string(), false),
        TemplateRef::DeepSeek => (DEEPSEEK_TEXT.to_string(), false),
        TemplateRef::Claude => (CLAUDE_TEXT.to_string(), false),
        TemplateRef::Inline(s) => (s.clone(), false),
        TemplateRef::Named(name) => {
            let reg = templates.ok_or_else(|| TemplateError::Unknown(name.clone()))?;
            let text = reg
                .get(name)
                .ok_or_else(|| TemplateError::Unknown(name.clone()))?;
            // 空则整体省略（Named 槽位可被替换为空以禁用该能力）
            (text, true)
        }
    };
    Ok(SystemSection {
        stable: true,
        text: interpolate(&text, vars)?,
        omit_if_empty,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AgentDefinition {
        AgentDefinition::builder()
            .id(AgentId::new("coder").unwrap())
            .description("Code agent")
            .model("deepseek/deepseek-v3")
            .template(TemplateRef::DeepSeek)
            .tools(["apply_patch", "grep"])
            .build()
            .unwrap()
    }

    #[test]
    fn builder_requires_id() {
        let err = AgentDefinition::builder()
            .description("d")
            .build()
            .unwrap_err();
        assert!(matches!(err, BuilderError::MissingId));
    }

    #[test]
    fn builder_builds_definition() {
        let def = sample();
        assert_eq!(def.id.as_str(), "coder");
        assert_eq!(def.tools, vec!["apply_patch", "grep"]);
    }

    #[test]
    fn bind_resolves_whitelists_and_renders_template() {
        let def = Arc::new(sample());
        let bound = def.bind();
        // 白名单解析
        assert!(bound.tools.is_some());
        assert!(bound.tools.as_ref().unwrap().contains("grep"));
        assert!(bound.skills.is_some()); // 空列表 → Some(空集)，封闭
        assert!(bound.skills.as_ref().unwrap().is_empty());
        // 模板渲染为稳定片段
        assert_eq!(bound.system_sections.len(), 1);
        assert!(bound.system_sections[0].stable);
        assert!(bound.system_sections[0].text.contains("coding assistant"));
    }

    #[test]
    fn wildcard_tools_means_all() {
        let def = Arc::new(
            AgentDefinition::builder()
                .id(AgentId::new("all").unwrap())
                .description("d")
                .model("m")
                .tools(["*"])
                .build()
                .unwrap(),
        );
        let bound = def.bind();
        assert!(bound.tools.is_none(), "['*'] 解析为 None（全部）");
    }

    fn named_def(name: &str) -> Arc<AgentDefinition> {
        Arc::new(
            AgentDefinition::builder()
                .id(AgentId::new(name).unwrap())
                .description("d")
                .model("m")
                .template(TemplateRef::Named(name.to_string()))
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn bind_with_resolves_named_template_and_interpolates() {
        let templates = TemplateRegistry::with_defaults();
        templates.register("coder-prompt", "You are {{role}}.").unwrap();
        let bound = named_def("coder-prompt")
            .bind_with(Some(&templates), &[("role", "coder")])
            .unwrap();
        assert_eq!(bound.system_sections[0].text, "You are coder.");
        assert!(bound.system_sections[0].stable);
    }

    #[test]
    fn bind_with_unknown_named_template_errors() {
        let err = named_def("missing")
            .bind_with(Some(&TemplateRegistry::with_defaults()), &[])
            .unwrap_err();
        assert!(matches!(err, TemplateError::Unknown(_)));
    }

    #[test]
    fn bind_with_missing_variable_errors() {
        let templates = TemplateRegistry::with_defaults();
        templates.register("with-var", "cwd={{cwd}}").unwrap();
        let err = named_def("with-var")
            .bind_with(Some(&templates), &[])
            .unwrap_err();
        assert!(matches!(err, TemplateError::UnknownVariable(_)));
    }

    #[test]
    fn bind_panics_on_named_without_registry() {
        let result = std::panic::catch_unwind(|| named_def("general").bind());
        assert!(result.is_err(), "Named 模板无注册表绑定必须 panic");
    }

    #[test]
    fn empty_named_template_is_omittable() {
        let templates = TemplateRegistry::with_defaults();
        templates.register("empty-slot", "").unwrap();
        let bound = named_def("empty-slot")
            .bind_with(Some(&templates), &[])
            .unwrap();
        assert!(bound.system_sections[0].omit_if_empty);
    }
}
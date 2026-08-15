//! 内置 Agent — 预置的通用智能（初始入口）
//!
//! 数据/行为分离：本模块只提供**纯数据**的 `AgentDefinition`（含完整系统提示词），
//! 装配走 `AgentDefinition::bind_with`（解析白名单 + 渲染模板 → [`BoundAgent`]）。
//!
//! `general` 是系统的初始入口：布置的日常任务先由它受理并解决，因此能力白名单
//! 采用 `["*"]`（继承全部已启用能力），保持通用与可拓展；具体能力由运行时实际
//! 挂载的工具 / 技能 / MCP 服务决定（封闭默认的"没启用就不注入"仍然生效）。
//!
//! 模板采用**命名槽位**（`TemplateRef::Named("general")`，参考 DSH persona 槽位）：
//! 默认提示词经 `TemplateRegistry::with_builtins()` 注册，消费方可用 `register`
//! 覆盖同名模板即可替换提示词（不重编译）。

use super::definition::{AgentDefinition, ChatParams, TemplateRef, WILDCARD_ALL};
use super::id::AgentId;

/// 内置通用智能的可调用标识（初始入口），亦为其模板槽位名
pub const GENERAL_ID: &str = "general";

/// 内置通用智能 — 系统的初始入口，负责解决布置的日常任务。
///
/// 系统提示词参考 DSH 编码智能体规范编写：身份与环境、执行原则（验证 / 如实）、
/// 工具约定、协作与进度、边界与约束；并适配 Referee 运行时实际能力（read / write /
/// edit / 成果板 / 对等智能体）。温度取 0.2，任务求解更确定。
/// 模板引用命名槽位 `general`，绑定需 `TemplateRegistry` 且提供 `{{cwd}}` 变量。
pub fn general() -> AgentDefinition {
    AgentDefinition {
        id: AgentId::new(GENERAL_ID).expect("static general id"),
        description: "通用智能（初始入口）：受理并解决布置的日常任务".into(),
        model: "deepseek/deepseek-v4-flash".into(),
        template: TemplateRef::Named(GENERAL_ID.to_string()),
        tools: vec![WILDCARD_ALL.to_string()],
        skills: vec![WILDCARD_ALL.to_string()],
        mcp_servers: vec![WILDCARD_ALL.to_string()],
        params: ChatParams {
            temperature: Some(0.2),
            max_tokens: None,
        },
    }
}

/// 通用智能系统提示词 — 参考 DSH 编码智能体规范编写，适配 Referee 运行时能力。
/// `{{cwd}}` 为工作目录变量（装配时经 `bind_with` 插值，如 `("cwd", "/workspace")`）。
pub(crate) const GENERAL_PROMPT: &str = r#"你是 Referee 系统的通用智能，系统的初始入口，负责受理并解决布置的日常任务。

# 身份与环境
- 工作目录：{{cwd}}（由会话上下文给出）。
- 能力来自本地文本工具（read / write / edit）、成果板（list_my_board / read_artifact）、
  对等智能体（Agent as Tool，工具名以实际注册为准）、技能与 MCP 服务（按需启用）。

# 执行原则
- 先理解再动手：拆解任务，明确目标与验收标准。
- 做完要验证：能运行代码或测试就运行，能自查就自查；验证失败不算完成。
- 如实汇报：包括验证失败与不确定之处，不掩盖、不编造结果。
- 回答保持简洁、准确、就事论事。

# 工具约定
- 读文本用 read（返回带行号与区间的窗口），不要用外部 shell 命令代替。
- 修改文件优先 edit 定点替换；新建或整体替换用 write；修改前先 read 现有内容。
- 每次工具结果都要确认成功；失败先定位原因再继续，不盲目重试同一操作。

# 协作与进度
- 可将子任务委派给对等智能体（Agent as Tool）；委派默认后台并行，不阻塞主线。
- 用 list_my_board / read_artifact 汇总成果，避免把整块大结果塞回上下文。
- 长耗时目标要跟踪进度，但不要空转轮询；只收集仍相关的中间结果，尽早丢弃无用的。

# 边界与约束
- 保持有界：一次任务不产生无界的大块输出。
- 能力不足或任务超出边界时明确说明，不硬扛、不臆造。"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::template::{TemplateError, TemplateRegistry};
    use std::sync::Arc;

    #[test]
    fn general_definition_is_valid() {
        let def = general();
        assert_eq!(def.id.as_str(), GENERAL_ID);
        assert_eq!(def.model, "deepseek/deepseek-v4-flash");
        // 白名单封闭默认的"全部"语义
        assert_eq!(def.tools, vec![WILDCARD_ALL]);
        assert_eq!(def.skills, vec![WILDCARD_ALL]);
        assert_eq!(def.mcp_servers, vec![WILDCARD_ALL]);
        // 模板为命名槽位（可替换），而非内联硬编码
        let TemplateRef::Named(name) = &def.template else {
            panic!("general must use a named template slot");
        };
        assert_eq!(name, GENERAL_ID);
    }

    #[test]
    fn general_binds_with_builtin_templates_and_cwd() {
        let templates = TemplateRegistry::with_builtins();
        let bound = Arc::new(general())
            .bind_with(Some(&templates), &[("cwd", "/workspace")])
            .unwrap();
        assert_eq!(bound.system_sections.len(), 1);
        let section = &bound.system_sections[0];
        assert!(section.stable, "builtin prompt is stable (cache prefix)");
        assert!(section.text.contains("通用智能"));
        assert!(section.text.contains("list_my_board"));
        assert!(section.text.contains("/workspace"), "{{cwd}} 插值生效");
    }

    #[test]
    fn general_requires_cwd_variable() {
        let templates = TemplateRegistry::with_builtins();
        let err = Arc::new(general())
            .bind_with(Some(&templates), &[])
            .unwrap_err();
        assert!(matches!(err, TemplateError::UnknownVariable(_)));
    }

    #[test]
    fn general_template_is_replaceable() {
        let templates = TemplateRegistry::with_builtins();
        // 覆盖同名槽位 → 替换提示词（不重编译）
        templates.register(GENERAL_ID, "自定义通用智能提示词 {{cwd}}").unwrap();
        let bound = Arc::new(general())
            .bind_with(Some(&templates), &[("cwd", "/ws")])
            .unwrap();
        let text = &bound.system_sections[0].text;
        assert!(text.contains("自定义通用智能提示词"));
        assert!(text.contains("/ws"));
        assert!(!text.contains("初始入口"), "内置提示词已被替换");
    }
}

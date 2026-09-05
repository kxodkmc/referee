//! 工具注册表 — 有界 DashMap + 声明导出
//!
//! ## 设计约束
//! - **有界**：注册上限 `max_tools`，超限拒绝（返回错误）
//! - **线程安全**：`DashMap` + `Arc`，多 Session 共享同一注册表
//! - **只读快照**：`declarations()` 导出 `Vec<ToolDeclaration>` 供构建
//!   `ChatRequest.tools`，避免持锁跨 await

use std::sync::Arc;

use dashmap::DashMap;

use crate::provider::ToolDeclaration;
use crate::tool::Tool;

/// 注册表配置
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// 最大工具数（有界，防 OOM）
    pub max_tools: usize,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self { max_tools: 64 }
    }
}

/// 工具注册表 — 有界、线程安全
///
/// 一个 `AgentRuntime` 持有一个 `ToolRegistry`，所有 Session 共享。
/// 工具声明自动导出为 `ToolDeclaration`，供构建 `ChatRequest` 时使用。
#[derive(Clone)]
pub struct ToolRegistry {
    tools: Arc<DashMap<String, Arc<dyn Tool>>>,
    config: RegistryConfig,
}

impl std::fmt::Debug for ToolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolRegistry")
            .field("count", &self.tools.len())
            .field("max", &self.config.max_tools)
            .finish()
    }
}

/// 注册错误
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("tool name '{0}' already registered")]
    Duplicate(String),
    #[error("registry full: max {0} tools")]
    Full(usize),
    #[error("tools are not enabled on this runtime (call with_tools first)")]
    NotEnabled,
    #[error("terminal tool '{0}' must be waiting-type (default_wait = true), dispatch-type conflicts with terminal convergence")]
    TerminalRequiresWait(String),
}

impl ToolRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            tools: Arc::new(DashMap::new()),
            config,
        }
    }

    /// 创建空注册表（默认配置）
    pub fn with_defaults() -> Self {
        Self::new(RegistryConfig::default())
    }

    /// 注册工具
    ///
    /// 名称冲突或超限时返回错误，不覆盖已有注册。
    pub fn register(&self, tool: Arc<dyn Tool>) -> Result<(), RegistryError> {
        let name = tool.name().to_string();

        if tool.terminal() && !tool.default_wait() {
            return Err(RegistryError::TerminalRequiresWait(name));
        }

        // 容量检查（软上限，多线程下有微小竞窗，可接受）
        if self.tools.len() >= self.config.max_tools {
            return Err(RegistryError::Full(self.config.max_tools));
        }

        // 用 or_insert_with（内部经 dashmap 的 insert 路径），避免裸 `entry()`
        // match 触发的 shrink 死锁；闭包仅在 Vacant 分支执行，据此判断名称冲突。
        let mut created = false;
        self.tools.entry(name.clone()).or_insert_with(|| {
            created = true;
            tool
        });
        if created {
            Ok(())
        } else {
            Err(RegistryError::Duplicate(name))
        }
    }

    /// 全部已注册工具的只读快照（供上层枚举，如停机时清理外部资源）
    ///
    /// 快照不持锁跨 await；调用方据此 iterate，介意一致性的场景应直接读实时表。
    pub fn all(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.iter().map(|e| e.value().clone()).collect()
    }

    /// 按名称获取工具
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).map(|r| r.clone())
    }

    /// 导出所有工具声明（快照，无持锁跨 await）
    ///
    /// 供 `AgentRuntime` 构建 `ChatRequest.tools` 时调用。
    pub fn declarations(&self) -> Vec<ToolDeclaration> {
        self.tools
            .iter()
            .map(|r| r.value().to_declaration())
            .collect()
    }

    /// 按子智能体嵌套深度过滤后导出工具声明
    ///
    /// 当会话当前深度 `current_depth >= max_depth` 时，剔除 `depth_limited` 工具
    /// （子 Agent 工具）——LLM 看不到即无法发起更深嵌套调用（声明层防线）。
    /// 未达上限时行为与 [`declarations`](Self::declarations) 一致。
    pub fn declarations_for_depth(
        &self,
        current_depth: u32,
        max_depth: u32,
    ) -> Vec<ToolDeclaration> {
        self.tools
            .iter()
            .filter(|r| !(r.value().depth_limited() && current_depth >= max_depth))
            .map(|r| r.value().to_declaration())
            .collect()
    }

    /// 复合过滤导出工具声明 — 白名单 + 子 Agent 深度
    ///
    /// `allowed` 为 `None` 时不过滤（继承全部）；为 `Some(set)` 时仅导出
    /// 名字命中白名单的工具。用于 per-Agent 能力白名单（"没启用的工具不进提示词"）。
    pub fn declarations_visible(
        &self,
        allowed: Option<&std::collections::HashSet<String>>,
        current_depth: u32,
        max_depth: u32,
    ) -> Vec<ToolDeclaration> {
        self.tools
            .iter()
            .filter(|r| allowed.is_none_or(|a| a.contains(r.value().name())))
            .filter(|r| !(r.value().depth_limited() && current_depth >= max_depth))
            .map(|r| r.value().to_declaration())
            .collect()
    }

    /// 当前工具数
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// 配置引用
    pub fn config(&self) -> &RegistryConfig {
        &self.config
    }

    /// 移除工具（测试 / 动态卸载用）
    pub fn unregister(&self, name: &str) -> bool {
        self.tools.remove(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    struct DummyTool {
        name: String,
    }

    #[async_trait]
    impl Tool for DummyTool {
        fn name(&self) -> &str {
            &self.name
        }
        fn description(&self) -> &str {
            "dummy"
        }
        fn input_schema(&self) -> serde_json::Value {
            json!({"type": "object"})
        }
        async fn execute(
            &self,
            _ctx: crate::tool::ToolContext,
            _args: serde_json::Value,
        ) -> Result<crate::tool::ToolOutput, crate::tool::ToolError> {
            Ok(crate::tool::ToolOutput::text("ok"))
        }
    }

    #[test]
    fn register_and_get() {
        let reg = ToolRegistry::with_defaults();
        let tool: Arc<dyn Tool> = Arc::new(DummyTool { name: "foo".into() });
        reg.register(tool).unwrap();
        assert!(reg.get("foo").is_some());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn duplicate_rejected() {
        let reg = ToolRegistry::with_defaults();
        let tool: Arc<dyn Tool> = Arc::new(DummyTool { name: "foo".into() });
        reg.register(tool.clone()).unwrap();
        let err = reg.register(tool).unwrap_err();
        assert!(matches!(err, RegistryError::Duplicate(_)));
    }

    #[test]
    fn full_rejected() {
        let reg = ToolRegistry::new(RegistryConfig { max_tools: 2 });
        reg.register(Arc::new(DummyTool { name: "a".into() }))
            .unwrap();
        reg.register(Arc::new(DummyTool { name: "b".into() }))
            .unwrap();
        let err = reg
            .register(Arc::new(DummyTool { name: "c".into() }))
            .unwrap_err();
        assert!(matches!(err, RegistryError::Full(2)));
    }

    #[test]
    fn declarations_snapshot() {
        let reg = ToolRegistry::with_defaults();
        reg.register(Arc::new(DummyTool { name: "a".into() }))
            .unwrap();
        reg.register(Arc::new(DummyTool { name: "b".into() }))
            .unwrap();
        let decls = reg.declarations();
        assert_eq!(decls.len(), 2);
    }

    #[test]
    fn unregister() {
        let reg = ToolRegistry::with_defaults();
        reg.register(Arc::new(DummyTool { name: "foo".into() }))
            .unwrap();
        assert!(reg.unregister("foo"));
        assert!(!reg.unregister("foo"));
        assert_eq!(reg.len(), 0);
    }

    /// 受子智能体嵌套深度限制的工具（模拟 AgentTool）
    struct DepthLimitedTool {
        name: String,
    }

    #[async_trait]
    impl Tool for DepthLimitedTool {
        fn name(&self) -> &str {
            &self.name
        }
        fn description(&self) -> &str {
            "subagent tool"
        }
        fn input_schema(&self) -> serde_json::Value {
            json!({"type": "object"})
        }
        fn depth_limited(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            _ctx: crate::tool::ToolContext,
            _args: serde_json::Value,
        ) -> Result<crate::tool::ToolOutput, crate::tool::ToolError> {
            Ok(crate::tool::ToolOutput::text("ok"))
        }
    }

    #[test]
    fn declarations_for_depth_filters_subagent_tools_at_limit() {
        let reg = ToolRegistry::with_defaults();
        reg.register(Arc::new(DummyTool {
            name: "plain".into(),
        }))
        .unwrap();
        reg.register(Arc::new(DepthLimitedTool { name: "sub".into() }))
            .unwrap();

        // 深度未达上限：子 Agent 工具可见（如 B 层可调 C）
        let decls = reg.declarations_for_depth(1, 2);
        let names: Vec<_> = decls.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"sub"));

        // 深度达上限：子 Agent 工具被剔除（如 C 层不可调 D），普通工具保留
        let decls = reg.declarations_for_depth(2, 2);
        let names: Vec<_> = decls.iter().map(|d| d.name.as_str()).collect();
        assert!(!names.contains(&"sub"));
        assert!(names.contains(&"plain"));
    }

    #[test]
    fn declarations_visible_filters_by_whitelist() {
        let reg = ToolRegistry::with_defaults();
        reg.register(Arc::new(DummyTool { name: "a".into() }))
            .unwrap();
        reg.register(Arc::new(DummyTool { name: "b".into() }))
            .unwrap();

        // None = 继承全部
        let all_decls = reg.declarations_visible(None, 0, 2);
        let all: Vec<_> = all_decls.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(all.len(), 2);

        // Some(白名单) = 仅命中者
        let allow: std::collections::HashSet<String> =
            ["a".into()].into_iter().collect();
        let visible = reg.declarations_visible(Some(&allow), 0, 2);
        let vis: Vec<_> = visible.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(vis, vec!["a"]);

        // Some(空) = 无工具
        let none: std::collections::HashSet<String> = std::collections::HashSet::new();
        assert!(reg.declarations_visible(Some(&none), 0, 2).is_empty());
    }
}

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

        // 容量检查（软上限，多线程下有微小竞窗，可接受）
        if self.tools.len() >= self.config.max_tools {
            return Err(RegistryError::Full(self.config.max_tools));
        }

        match self.tools.entry(name.clone()) {
            dashmap::Entry::Occupied(_) => Err(RegistryError::Duplicate(name)),
            dashmap::Entry::Vacant(entry) => {
                entry.insert(tool);
                Ok(())
            }
        }
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
}

//! Agent 注册表 — 以可调用 ID 为 key，强制唯一，有界

use std::sync::Arc;

use dashmap::DashMap;

use super::definition::AgentDefinition;
use super::id::AgentId;

/// 注册表配置
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// 最大 Agent 数（有界，防 OOM）
    pub max_agents: usize,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self { max_agents: 128 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    #[error("agent id '{0}' already registered")]
    Duplicate(AgentId),
    #[error("agent id '{0}' not found")]
    NotFound(AgentId),
    #[error("registry full: max {0} agents")]
    Full(usize),
}

/// Agent 注册表 — 有界、线程安全；`AgentId` 为唯一 key
#[derive(Clone)]
pub struct AgentRegistry {
    map: Arc<DashMap<AgentId, Arc<AgentDefinition>>>,
    config: RegistryConfig,
}

impl std::fmt::Debug for AgentRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentRegistry")
            .field("count", &self.map.len())
            .field("max", &self.config.max_agents)
            .finish()
    }
}

impl AgentRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            config,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(RegistryConfig::default())
    }

    /// 注册 Agent；ID 重复或超限时返回错误，不覆盖已有项
    pub fn register(&self, def: AgentDefinition) -> Result<(), RegistryError> {
        if self.map.len() >= self.config.max_agents {
            return Err(RegistryError::Full(self.config.max_agents));
        }
        // or_insert_with 避免裸 entry() 触发的 dashmap shrink 死锁
        let id = def.id.clone();
        let mut created = false;
        self.map.entry(id.clone()).or_insert_with(|| {
            created = true;
            Arc::new(def)
        });
        if created {
            Ok(())
        } else {
            Err(RegistryError::Duplicate(id))
        }
    }

    pub fn get(&self, id: &AgentId) -> Option<Arc<AgentDefinition>> {
        self.map.get(id).map(|r| r.clone())
    }

    /// 全部定义快照（无持锁跨 await）
    pub fn all(&self) -> Vec<Arc<AgentDefinition>> {
        self.map.iter().map(|r| r.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 移除 Agent（测试 / 动态卸载用）
    pub fn unregister(&self, id: &AgentId) -> bool {
        self.map.remove(id).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(id: &str) -> AgentDefinition {
        AgentDefinition {
            id: AgentId::new(id).unwrap(),
            description: "d".into(),
            model: "m".into(),
            template: Default::default(),
            tools: vec![],
            skills: vec![],
            mcp_servers: vec![],
            params: Default::default(),
        }
    }

    #[test]
    fn register_and_get() {
        let reg = AgentRegistry::with_defaults();
        reg.register(def("coder")).unwrap();
        let got = reg.get(&AgentId::new("coder").unwrap()).unwrap();
        assert_eq!(got.id.as_str(), "coder");
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn duplicate_rejected() {
        let reg = AgentRegistry::with_defaults();
        reg.register(def("coder")).unwrap();
        let err = reg.register(def("coder")).unwrap_err();
        assert!(matches!(err, RegistryError::Duplicate(_)));
    }

    #[test]
    fn not_found() {
        let reg = AgentRegistry::with_defaults();
        assert!(reg.get(&AgentId::new("nope").unwrap()).is_none());
    }

    #[test]
    fn full_rejected() {
        let reg = AgentRegistry::new(RegistryConfig { max_agents: 1 });
        reg.register(def("a")).unwrap();
        let err = reg.register(def("b")).unwrap_err();
        assert!(matches!(err, RegistryError::Full(1)));
    }

    #[test]
    fn unregister() {
        let reg = AgentRegistry::with_defaults();
        let id = AgentId::new("coder").unwrap();
        reg.register(def("coder")).unwrap();
        assert!(reg.unregister(&id));
        assert!(!reg.unregister(&id));
        assert!(reg.is_empty());
    }
}
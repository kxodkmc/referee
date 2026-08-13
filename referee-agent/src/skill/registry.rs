//! Skill 注册表 — 有界 DashMap + L1 声明导出
//!
//! 镜像 base `ToolRegistry` 的套路：有界、线程安全、只读快照。
//! 一个 `AgentRuntime` 持有一个 `SkillRegistry`，所有 Session 共享。
//!
//! ## 设计约束
//! - **有界**：注册上限 `max_skills`，超限拒绝（返回错误）
//! - **线程安全**：`DashMap` + `Arc`，多 Session 共享同一注册表
//! - **只读快照**：`declarations()` 导出 `Vec<SkillDeclaration>`（L1，供注入
//!   system prompt），避免持锁跨 await

use std::sync::Arc;

use dashmap::DashMap;

use crate::skill::{Skill, SkillDeclaration};

/// 注册表配置
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// 最大技能数（有界，防 OOM）
    pub max_skills: usize,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self { max_skills: 128 }
    }
}

/// 注册错误
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("skill name '{0}' already registered")]
    Duplicate(String),
    #[error("skill registry full: max {0} skills")]
    Full(usize),
}

/// 有界、线程安全的技能注册表
#[derive(Clone)]
pub struct SkillRegistry {
    skills: Arc<DashMap<String, Arc<Skill>>>,
    config: RegistryConfig,
}

impl std::fmt::Debug for SkillRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkillRegistry")
            .field("count", &self.skills.len())
            .field("max", &self.config.max_skills)
            .finish()
    }
}

impl SkillRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            skills: Arc::new(DashMap::new()),
            config,
        }
    }

    /// 创建空注册表（默认配置）
    pub fn with_defaults() -> Self {
        Self::new(RegistryConfig::default())
    }

    /// 注册技能（名称冲突或超限时返回错误，不覆盖已有注册）
    pub fn register(&self, skill: Arc<Skill>) -> Result<(), RegistryError> {
        let name = skill.name().to_string();

        if self.skills.len() >= self.config.max_skills {
            return Err(RegistryError::Full(self.config.max_skills));
        }

        // 用 or_insert_with（内部经 dashmap 的 insert 路径），避免裸 `entry()`
        // match 触发的 shrink 死锁；闭包仅在 Vacant 分支执行，据此判断名称冲突。
        let mut created = false;
        self.skills.entry(name.clone()).or_insert_with(|| {
            created = true;
            skill
        });
        if created {
            Ok(())
        } else {
            Err(RegistryError::Duplicate(name))
        }
    }

    /// 按名称获取技能
    pub fn get(&self, name: &str) -> Option<Arc<Skill>> {
        self.skills.get(name).map(|r| r.clone())
    }

    /// 导出全部 L1 声明（快照，无持锁跨 await）
    pub fn declarations(&self) -> Vec<SkillDeclaration> {
        self.skills
            .iter()
            .map(|r| r.value().to_declaration())
            .collect()
    }

    /// 全部技能引用（快照，供 router 选择）
    pub fn all(&self) -> Vec<Arc<Skill>> {
        self.skills.iter().map(|r| r.value().clone()).collect()
    }

    /// 当前技能数
    pub fn len(&self) -> usize {
        self.skills.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.skills.is_empty()
    }

    /// 移除技能（测试 / 动态卸载用）
    pub fn remove(&self, name: &str) -> bool {
        self.skills.remove(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill(name: &str) -> Arc<Skill> {
        Arc::new(Skill::from_parts(name, format!("does {name}"), "body"))
    }

    #[test]
    fn register_and_get() {
        let reg = SkillRegistry::with_defaults();
        reg.register(skill("foo")).unwrap();
        assert!(reg.get("foo").is_some());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn duplicate_rejected() {
        let reg = SkillRegistry::with_defaults();
        reg.register(skill("foo")).unwrap();
        let err = reg.register(skill("foo")).unwrap_err();
        assert!(matches!(err, RegistryError::Duplicate(_)));
    }

    #[test]
    fn full_rejected() {
        let reg = SkillRegistry::new(RegistryConfig { max_skills: 2 });
        reg.register(skill("a")).unwrap();
        reg.register(skill("b")).unwrap();
        let err = reg.register(skill("c")).unwrap_err();
        assert!(matches!(err, RegistryError::Full(2)));
    }

    #[test]
    fn declarations_snapshot() {
        let reg = SkillRegistry::with_defaults();
        reg.register(skill("a")).unwrap();
        reg.register(skill("b")).unwrap();
        let decls = reg.declarations();
        assert_eq!(decls.len(), 2);
        let names: Vec<_> = decls.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }

    #[test]
    fn all_and_remove() {
        let reg = SkillRegistry::with_defaults();
        reg.register(skill("foo")).unwrap();
        assert_eq!(reg.all().len(), 1);
        assert!(reg.remove("foo"));
        assert!(!reg.remove("foo"));
        assert_eq!(reg.len(), 0);
    }
}
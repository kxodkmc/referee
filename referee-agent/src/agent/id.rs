//! 可调用 Agent 标识 — 构造即校验、有界、可作路由与注册 key
//!
//! 规则与 Skill 的 `name` 完全一致（复用小写字母/数字/连字符规范），
//! 保证全项目命名统一。格式校验在构造时强制，唯一性在注册时强制。

use serde::{Deserialize, Serialize};

/// 可调用 Agent 标识 — newtype 包装，非法格式在构造时被拒
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AgentIdError {
    #[error("agent id '{0}' invalid: use kebab-case (lowercase/digits/hyphen), <=64 chars, no leading/trailing/consecutive hyphen")]
    Invalid(String),
}

/// Kebab-case 校验：小写字母/数字/连字符，≤64 字符，
/// 不以连字符开头/结尾，无连续连字符（与 Skill `name` 相同）
fn validate(s: &str) -> Result<(), AgentIdError> {
    if s.chars().count() > 64 {
        return Err(AgentIdError::Invalid(s.to_string()));
    }
    let mut prev_hyphen = false;
    for (idx, c) in s.char_indices() {
        let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
        if !ok {
            return Err(AgentIdError::Invalid(s.to_string()));
        }
        if c == '-' {
            if prev_hyphen || idx == 0 {
                return Err(AgentIdError::Invalid(s.to_string()));
            }
            prev_hyphen = true;
        } else {
            prev_hyphen = false;
        }
    }
    if s.ends_with('-') || s.is_empty() {
        return Err(AgentIdError::Invalid(s.to_string()));
    }
    Ok(())
}

impl AgentId {
    /// 校验并构造
    pub fn new(s: impl Into<String>) -> Result<Self, AgentIdError> {
        let s = s.into();
        validate(&s)?;
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ids_accepted() {
        for good in ["coder", "code-reviewer", "a1", "x-y-z"] {
            assert!(AgentId::new(good).is_ok(), "'{good}' must pass");
        }
    }

    #[test]
    fn invalid_ids_rejected() {
        for bad in ["", "Coder", "coder!", "coder name", "-coder", "coder-", "a--b", &"x".repeat(65)] {
            assert_eq!(AgentId::new(bad), Err(AgentIdError::Invalid(bad.to_string())));
        }
    }

    #[test]
    fn roundtrip() {
        let id = AgentId::new("coder").unwrap();
        assert_eq!(id.as_str(), "coder");
        assert_eq!(format!("{id}"), "coder");
    }
}
//! 模板注册表 — 命名模板槽位的可替换存储与变量插值
//!
//! 参考 DeepSeek Harness `system-prompt` 的 persona 槽位设计：
//! - **命名槽位**：persona 是命名片段（`deployment:persona`，order 0），任何作用域可用
//!   同名片段**遮蔽**全局实现，实现"不重编译即可替换"。本模块落地为纯数据的
//!   `TemplateRegistry`：`name → text`，`register` 为**覆盖（替换）**语义。
//! - **传递设计**：模板文本经 `TemplateRegistry`（数据）持有，`bind_with`（行为）在装配
//!   时按「注册表 + 变量」解析并插值，产出 `SystemSection` 进入 base `assemble`。
//!
//! `interpolate` 对齐 DSH `renderPrompt` 的严格语义：`{{variable}}` 引用须匹配
//! `[a-z][a-z0-9_]*`，未知/畸形引用 **fail-loud**（显式报错），孤立的 `{{`（无闭合）
//! 视为字面文本。零新增依赖。

use std::sync::Arc;

use dashmap::DashMap;

/// 模板注册表配置
#[derive(Debug, Clone)]
pub struct TemplateConfig {
    /// 最大模板数（有界，防 OOM）
    pub max_templates: usize,
}

impl Default for TemplateConfig {
    fn default() -> Self {
        Self { max_templates: 128 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    #[error("template '{0}' not found: supply a TemplateRegistry with that name registered (bind_with)")]
    Unknown(String),
    #[error("template registry full: max {0} templates")]
    Full(usize),
    #[error("template name '{0}' invalid: use kebab-case (lowercase/digits/hyphen), <=64 chars")]
    InvalidName(String),
    #[error("malformed prompt variable reference \"{{{0}}}\": names match [a-z][a-z0-9_]*")]
    MalformedVariable(String),
    #[error("prompt variable \"{{{0}}}\" has no value for this binding")]
    UnknownVariable(String),
}

/// 模板注册表 — 有界、线程安全；`register` 为覆盖语义（同名模板可被替换）
#[derive(Clone)]
pub struct TemplateRegistry {
    map: Arc<DashMap<String, String>>,
    config: TemplateConfig,
}

impl std::fmt::Debug for TemplateRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemplateRegistry")
            .field("count", &self.map.len())
            .field("max", &self.config.max_templates)
            .finish()
    }
}

/// kebab-case 校验（与 `AgentId` 规则一致）：小写字母/数字/连字符，≤64 字符
fn valid_name(name: &str) -> bool {
    if name.is_empty() || name.chars().count() > 64 {
        return false;
    }
    let mut prev_hyphen = false;
    for (idx, c) in name.char_indices() {
        let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
        if !ok {
            return false;
        }
        if c == '-' {
            if prev_hyphen || idx == 0 {
                return false;
            }
            prev_hyphen = true;
        } else {
            prev_hyphen = false;
        }
    }
    !name.ends_with('-')
}

impl TemplateRegistry {
    pub fn new(config: TemplateConfig) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            config,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(TemplateConfig::default())
    }

    /// 预置内置模板（内置通用智能的默认提示词作为可替换的命名槽位）
    pub fn with_builtins() -> Self {
        let reg = Self::with_defaults();
        reg.register(
            super::builtin::GENERAL_ID,
            super::builtin::GENERAL_PROMPT,
        )
        .expect("builtin general template name valid & unique in a fresh registry");
        reg
    }

    /// 注册（或覆盖）命名模板。已有同名模板被**替换**（可替换语义）；
    /// 仅新增 key 消耗容量，超出上限返回 [`TemplateError::Full`]。
    pub fn register(
        &self,
        name: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<(), TemplateError> {
        let name = name.into();
        if !valid_name(&name) {
            return Err(TemplateError::InvalidName(name));
        }
        if !self.map.contains_key(&name) && self.map.len() >= self.config.max_templates {
            return Err(TemplateError::Full(self.config.max_templates));
        }
        self.map.insert(name, text.into());
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<String> {
        self.map.get(name).map(|r| r.clone())
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// `{{name}}` 引用：名称须匹配 `[a-z][a-z0-9_]*`
fn var_name_ok(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// 严格插值 `{{variable}}`；未知/畸形引用报错，孤立的 `{{`（无闭合）保留为字面文本。
///
/// 对齐 DSH `renderPrompt`：完整 `{{name}}` 组必须解析到变量值，否则 fail-loud；
/// 无闭合的 `{{` 不构成引用，原样保留。
pub fn interpolate(text: &str, vars: &[(&str, &str)]) -> Result<String, TemplateError> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let tail = &rest[open + 2..];
        match tail.find("}}") {
            Some(close) => {
                let name = &tail[..close];
                if !var_name_ok(name) {
                    return Err(TemplateError::MalformedVariable(name.to_string()));
                }
                let value = vars
                    .iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| *v)
                    .ok_or_else(|| TemplateError::UnknownVariable(name.to_string()))?;
                out.push_str(value);
                rest = &tail[close + 2..];
            }
            None => {
                // 孤立 `{{`（无闭合）：按字面保留，继续扫描其后内容
                out.push_str("{{");
                rest = tail;
            }
        }
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get() {
        let reg = TemplateRegistry::with_defaults();
        reg.register("general", "prompt").unwrap();
        assert_eq!(reg.get("general").as_deref(), Some("prompt"));
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn register_overrides_existing() {
        // 可替换语义：同名注册覆盖旧文本，不新增条目
        let reg = TemplateRegistry::with_defaults();
        reg.register("general", "old").unwrap();
        reg.register("general", "new").unwrap();
        assert_eq!(reg.get("general").as_deref(), Some("new"));
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn register_bounded_by_new_keys() {
        let reg = TemplateRegistry::new(TemplateConfig { max_templates: 1 });
        reg.register("a", "x").unwrap();
        // 覆盖已有 key 不消耗容量
        reg.register("a", "y").unwrap();
        // 新 key 超出上限 → Full
        let err = reg.register("b", "z").unwrap_err();
        assert!(matches!(err, TemplateError::Full(1)));
    }

    #[test]
    fn register_rejects_invalid_name() {
        let reg = TemplateRegistry::with_defaults();
        for bad in ["", "My", "a b", "-a", "a--b", "a b"] {
            assert!(matches!(
                reg.register(bad, "x"),
                Err(TemplateError::InvalidName(_))
            ));
        }
    }

    #[test]
    fn with_builtins_seeds_general() {
        let reg = TemplateRegistry::with_builtins();
        assert_eq!(reg.len(), 1);
        assert!(reg.get(crate::agent::GENERAL_ID).is_some());
    }

    #[test]
    fn interpolate_substitutes() {
        let text = "cwd={{cwd}} role={{role}}";
        let out = interpolate(text, &[("cwd", "/ws"), ("role", "coder")]).unwrap();
        assert_eq!(out, "cwd=/ws role=coder");
    }

    #[test]
    fn interpolate_unknown_variable_errors() {
        let err = interpolate("a {{nope}} b", &[("cwd", "/ws")]).unwrap_err();
        assert!(matches!(err, TemplateError::UnknownVariable(_)));
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn interpolate_malformed_errors() {
        // 空名 / 大写开头 / 含非法字符 → MalformedVariable
        for text in ["{{}}", "{{A}}", "{{a b}}", "{{a-b}}"] {
            assert!(
                matches!(
                    interpolate(text, &[]),
                    Err(TemplateError::MalformedVariable(_))
                ),
                "'{text}' must be malformed"
            );
        }
    }

    #[test]
    fn interpolate_lone_open_braces_is_literal() {
        // 无闭合的 `{{` 不是引用，原样保留
        assert_eq!(interpolate("a {{ b", &[]).unwrap(), "a {{ b");
        assert_eq!(interpolate("{{", &[]).unwrap(), "{{");
    }

    #[test]
    fn interpolate_without_variables_unchanged() {
        assert_eq!(interpolate("plain text", &[]).unwrap(), "plain text");
        assert_eq!(interpolate("", &[]).unwrap(), "");
    }
}

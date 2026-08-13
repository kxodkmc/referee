//! SKILL.md frontmatter 极简解析 — 零新增依赖
//!
//! Agent Skills 开放标准（[agentskills.io](https://agentskills.io/specification)）
//! 要求 `SKILL.md` 以 `---` 分隔的 YAML frontmatter 开头，必填字段仅
//! `name` / `description` 两个**标量**。据此用「行级 `key: value`」手工解析即可，
//! 无需引入 YAML 依赖（受 AGENTS.md 依赖白名单约束）。
//!
//! ## 覆盖范围
//! - 必填：`name`（kebab-case，≤64 字符）、`description`（≤1024 字符，非空）
//! - 可选标量：`license`、`compatibility`、`allowed-tools`（空格分隔列表）
//! - 可选 `metadata`：仅支持块式缩进 `key: value`；流式 `{}`/数组/嵌套对象
//!   优雅跳过（不 panic，metadata 为空）
//! - 未知顶层标量键：忽略（保持确定性）
//!
//! ## 设计约束
//! - **数据/行为分离**：产出 [`Frontmatter`] 为纯数据，不含任何行为
//! - **有界**：`metadata` 条目数受 `max_metadata` 上限约束，防 OOM
//! - **零新增依赖**：仅用 `std`

use thiserror::Error;

/// 解析后的 frontmatter 元数据（纯数据载体）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frontmatter {
    /// 技能名（kebab-case，必填，须与目录同名）
    pub name: String,
    /// 技能描述（必填，≤1024 字符）
    pub description: String,
    /// 许可证（可选）
    pub license: Option<String>,
    /// 环境要求（可选）
    pub compatibility: Option<String>,
    /// 预授权工具白名单（可选，空格分隔）
    pub allowed_tools: Vec<String>,
    /// 任意附加元数据（可选，块式 `key: value`）
    pub metadata: Vec<(String, String)>,
}

/// frontmatter 解析错误
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FrontmatterError {
    /// 缺少 `---` 分隔符（开头或结尾）
    #[error("skill frontmatter: missing '---' delimiter")]
    MissingDelimiter,
    /// 无 frontmatter 内容
    #[error("skill frontmatter: empty")]
    Empty,
    /// 行格式非法（非 `key: value`）
    #[error("skill frontmatter: malformed line: {0}")]
    Malformed(String),
    /// 缺少必填 `name`
    #[error("skill frontmatter: missing required 'name'")]
    MissingName,
    /// `name` 不符合 kebab-case 规范
    #[error("skill frontmatter: invalid name '{0}'")]
    InvalidName(String),
    /// 缺少必填 `description`
    #[error("skill frontmatter: missing required 'description'")]
    MissingDescription,
    /// `description` 超长（>1024 字符）
    #[error("skill frontmatter: description too long ({0} chars)")]
    DescriptionTooLong(usize),
    /// `metadata` 条目数超限
    #[error("skill frontmatter: metadata too large (max {0} entries)")]
    MetadataTooLarge(usize),
}

/// 从完整 `SKILL.md` 文本解析 frontmatter，返回 `(元数据, 正文)`
///
/// `max_metadata` 为 metadata 条目数上限（有界约束）。正文为
/// 关闭 `---` 之后的全部 Markdown 内容（原样保留）。
pub fn parse(full: &str, max_metadata: usize) -> Result<(Frontmatter, String), FrontmatterError> {
    let all: Vec<&str> = full.lines().collect();

    // 1. 跳过开头空行，定位 `---`
    let mut i = 0;
    while i < all.len() && all[i].trim().is_empty() {
        i += 1;
    }
    if i >= all.len() || all[i].trim() != "---" {
        return Err(FrontmatterError::MissingDelimiter);
    }
    i += 1;

    // 2. 收集 frontmatter 行，直到关闭 `---`
    let mut fm: Vec<&str> = Vec::new();
    while i < all.len() && all[i].trim() != "---" {
        fm.push(all[i]);
        i += 1;
    }
    if i >= all.len() {
        return Err(FrontmatterError::MissingDelimiter);
    }
    i += 1;

    // 3. 正文为关闭 `---` 之后内容
    let body = all[i..].join("\n");
    let frontmatter = parse_lines(&fm, max_metadata)?;
    Ok((frontmatter, body))
}

/// 逐行解析 frontmatter 内容
fn parse_lines(fm: &[&str], max_metadata: usize) -> Result<Frontmatter, FrontmatterError> {
    let mut name: Option<String> = None;
    let mut description: Option<String> = None;
    let mut license: Option<String> = None;
    let mut compatibility: Option<String> = None;
    let mut allowed_tools: Vec<String> = Vec::new();
    let mut metadata: Vec<(String, String)> = Vec::new();
    let mut in_metadata = false;

    for &line in fm {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            in_metadata = false;
            continue;
        }

        // metadata 块内：仅接受缩进行的 `key: value`
        if in_metadata {
            if line.starts_with(' ') || line.starts_with('\t') {
                if let Some((k, v)) = split_kv(trimmed) {
                    if metadata.len() >= max_metadata {
                        return Err(FrontmatterError::MetadataTooLarge(max_metadata));
                    }
                    metadata.push((k.to_string(), v.to_string()));
                }
                continue;
            }
            in_metadata = false;
        }

        let (key, value) = match split_kv(trimmed) {
            Some(kv) => kv,
            None => return Err(FrontmatterError::Malformed(line.to_string())),
        };
        match key {
            "name" => name = Some(value.to_string()),
            "description" => description = Some(value.to_string()),
            "license" => license = Some(value.to_string()),
            "compatibility" => compatibility = Some(value.to_string()),
            "allowed-tools" => {
                allowed_tools = value.split_whitespace().map(str::to_string).collect()
            }
            "metadata" => in_metadata = true,
            // 未知顶层标量键：忽略（保持确定性）
            _ => {}
        }
    }

    let name = name.ok_or(FrontmatterError::MissingName)?;
    validate_name(&name)?;
    let description = description.ok_or(FrontmatterError::MissingDescription)?;
    let desc_len = description.chars().count();
    if desc_len > 1024 {
        return Err(FrontmatterError::DescriptionTooLong(desc_len));
    }

    Ok(Frontmatter {
        name,
        description,
        license,
        compatibility,
        allowed_tools,
        metadata,
    })
}

/// 拆分 `key: value`，返回 `(key, value)`；无冒号返回 None
fn split_kv(line: &str) -> Option<(&str, &str)> {
    let colon = line.find(':')?;
    let key = line[..colon].trim();
    if key.is_empty() {
        return None;
    }
    let value = unquote(line[colon + 1..].trim());
    Some((key, value))
}

/// 去除值两侧的成对引号（单/双引号）
fn unquote(s: &str) -> &str {
    let b = s.as_bytes();
    if b.len() >= 2 && ((b[0] == b'"' && b[b.len() - 1] == b'"') || (b[0] == b'\'' && b[b.len() - 1] == b'\'')) {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// 校验 kebab-case 技能名：小写字母/数字/连字符，≤64 字符，
/// 不以连字符开头/结尾，无连续连字符
fn validate_name(name: &str) -> Result<(), FrontmatterError> {
    if name.chars().count() > 64 {
        return Err(FrontmatterError::InvalidName(name.to_string()));
    }
    let mut prev_hyphen = false;
    for (idx, c) in name.char_indices() {
        let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
        if !ok {
            return Err(FrontmatterError::InvalidName(name.to_string()));
        }
        if c == '-' {
            if prev_hyphen || idx == 0 {
                return Err(FrontmatterError::InvalidName(name.to_string()));
            }
            prev_hyphen = true;
        } else {
            prev_hyphen = false;
        }
    }
    if name.ends_with('-') || name.is_empty() {
        return Err(FrontmatterError::InvalidName(name.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_body() -> String {
        "# Expense Report\n\nFile and validate expense reports.\n".to_string()
    }

    #[test]
    fn parse_valid_minimal() {
        let src = format!("---\nname: expense-report\ndescription: File and validate expense reports.\n---\n{}", sample_body());
        let (fm, body) = parse(&src, 16).unwrap();
        assert_eq!(fm.name, "expense-report");
        assert_eq!(fm.description, "File and validate expense reports.");
        assert!(fm.license.is_none());
        assert!(fm.allowed_tools.is_empty());
        assert!(fm.metadata.is_empty());
        assert!(body.contains("File and validate"));
    }

    #[test]
    fn parse_optional_fields_and_metadata() {
        let src = "---\nname: pdf-processing\ndescription: Extract text from PDFs.\nlicense: Apache-2.0\ncompatibility: Requires python3\nallowed-tools: read_file run_script\nmetadata:\n  author: contoso-finance\n  version: \"2.1\"\n---\nbody";
        let (fm, body) = parse(src, 16).unwrap();
        assert_eq!(fm.license.as_deref(), Some("Apache-2.0"));
        assert_eq!(fm.compatibility.as_deref(), Some("Requires python3"));
        assert_eq!(fm.allowed_tools, vec!["read_file", "run_script"]);
        assert_eq!(
            fm.metadata,
            vec![
                ("author".to_string(), "contoso-finance".to_string()),
                ("version".to_string(), "2.1".to_string()),
            ]
        );
        assert_eq!(body, "body");
    }

    #[test]
    fn parse_missing_delimiter() {
        assert_eq!(parse("no frontmatter", 16).unwrap_err(), FrontmatterError::MissingDelimiter);
        // 只有开头 `---` 无结尾
        assert_eq!(parse("---\nname: x\ndescription: y", 16).unwrap_err(), FrontmatterError::MissingDelimiter);
    }

    #[test]
    fn parse_missing_name() {
        let src = "---\ndescription: only desc\n---\nbody";
        assert_eq!(parse(src, 16).unwrap_err(), FrontmatterError::MissingName);
    }

    #[test]
    fn parse_missing_description() {
        let src = "---\nname: foo\n---\nbody";
        assert_eq!(parse(src, 16).unwrap_err(), FrontmatterError::MissingDescription);
    }

    #[test]
    fn parse_invalid_name_rejected() {
        for bad in ["PDF-Processing", "-foo", "foo-", "foo--bar", "my skill", "my_skill"] {
            let src = format!("---\nname: {bad}\ndescription: d\n---\nbody");
            assert!(
                parse(&src, 16).is_err(),
                "name '{bad}' must be rejected"
            );
        }
    }

    #[test]
    fn parse_valid_names_accepted() {
        for good in ["foo", "pdf-processing", "my-skill-v2", "a"] {
            let src = format!("---\nname: {good}\ndescription: d\n---\nbody");
            assert!(parse(&src, 16).is_ok(), "name '{good}' must pass");
        }
    }

    #[test]
    fn parse_description_too_long() {
        let desc = "x".repeat(1025);
        let src = format!("---\nname: foo\ndescription: {desc}\n---\nbody");
        assert!(matches!(
            parse(&src, 16).unwrap_err(),
            FrontmatterError::DescriptionTooLong(_)
        ));
    }

    #[test]
    fn parse_metadata_bounded() {
        let src = "---\nname: foo\ndescription: d\nmetadata:\n  a: 1\n  b: 2\n  c: 3\n---\nbody";
        // 上限 2：第 3 条触发超限
        assert!(matches!(
            parse(src, 2).unwrap_err(),
            FrontmatterError::MetadataTooLarge(2)
        ));
    }

    #[test]
    fn parse_unknown_key_ignored() {
        let src = "---\nname: foo\ndescription: d\nsome-unknown: value\n---\nbody";
        let (fm, _) = parse(src, 16).unwrap();
        assert_eq!(fm.name, "foo");
        assert!(fm.metadata.is_empty());
    }

    #[test]
    fn parse_malformed_line() {
        let src = "---\nname: foo\nbare-line-no-colon\ndescription: d\n---\nbody";
        assert!(matches!(
            parse(src, 16).unwrap_err(),
            FrontmatterError::Malformed(_)
        ));
    }

    #[test]
    fn unquote_handles_quotes() {
        assert_eq!(unquote("\"hello\""), "hello");
        assert_eq!(unquote("'hello'"), "hello");
        assert_eq!(unquote("hello"), "hello");
    }
}
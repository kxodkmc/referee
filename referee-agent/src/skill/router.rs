//! Skill 相关性选择与注入渲染 — 行为所在（数据/行为分离）
//!
//! ## 职责
//! - [`SkillRouter`]：给定用户消息 + 候选技能，按相关性选出需激活的技能
//!   （渐进式披露 L1→L2：先用 L1 元数据判断，选中后取 L2 正文）
//! - [`KeywordRouter`]：默认实现，基于关键词/子串匹配的确定性启发式
//! - [`render_skill_context`]：把选中的技能渲染为可追加进 system prompt 的文本
//!
//! ## 设计约束
//! - **零模型依赖**：默认路由不引入嵌入模型，保持轻量；可替换为自定义
//!   [`SkillRouter`] 实现
//! - **有界**：最大选中数、注入总字符数均有上限，防上下文膨胀
//! - **确定性**：关键词匹配纯函数，可单测

use std::sync::Arc;

use crate::skill::Skill;

/// 相关性选择策略 — 可替换（按需拓展）
pub trait SkillRouter: Send + Sync {
    /// 从候选技能中选出应激活的技能（按得分降序，受实现约束裁剪）
    fn select(&self, query: &str, skills: &[Arc<Skill>]) -> Vec<Arc<Skill>>;
}

/// 关键词路由配置
#[derive(Debug, Clone)]
pub struct KeywordConfig {
    /// 命中阈值：`score > threshold` 才入选
    /// （0.0 = 至少一处命中即入选；score 0 表示完全无关，恒被排除）
    pub threshold: f32,
    /// 最大选中数（防止上下文膨胀）
    pub max_skills: usize,
    /// 注入总字符数上限（防止上下文膨胀）
    pub max_total_chars: usize,
}

impl Default for KeywordConfig {
    fn default() -> Self {
        Self {
            threshold: 0.0,
            max_skills: 3,
            max_total_chars: 8000,
        }
    }
}

/// 默认关键词路由 — 基于 L1 `description` 与查询的 token 重叠/子串匹配
///
/// 得分 = 查询 token 中「在技能描述里出现（相等或子串）」的比例。
/// 中文（CJK）按连续语义串切分，由子串匹配覆盖，无需分词库。
pub struct KeywordRouter {
    config: KeywordConfig,
}

impl KeywordRouter {
    pub fn new(config: KeywordConfig) -> Self {
        Self { config }
    }
}

impl Default for KeywordRouter {
    fn default() -> Self {
        Self::new(KeywordConfig::default())
    }
}

impl SkillRouter for KeywordRouter {
    fn select(&self, query: &str, skills: &[Arc<Skill>]) -> Vec<Arc<Skill>> {
        let mut ranked: Vec<(f32, Arc<Skill>)> = skills
            .iter()
            .filter_map(|s| {
                let score = score(query, s.description());
                if score > self.config.threshold {
                    Some((score, s.clone()))
                } else {
                    None
                }
            })
            .collect();
        // 得分降序，稳定排序（同分保持注册顺序）
        ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        let mut out = Vec::new();
        let mut total = 0usize;
        for (_, s) in ranked {
            if out.len() >= self.config.max_skills {
                break;
            }
            let cost = s.description().len() + s.body().len();
            if total + cost > self.config.max_total_chars {
                break;
            }
            total += cost;
            out.push(s);
        }
        out
    }
}

/// 计算查询与技能描述的匹配得分（0.0 ~ 1.0）
fn score(query: &str, description: &str) -> f32 {
    let qt = tokens(query);
    if qt.is_empty() {
        return 0.0;
    }
    let dt = tokens(description);
    let mut hits = 0usize;
    for q in &qt {
        if dt.iter().any(|d| shares_signal(q, d)) {
            hits += 1;
        }
    }
    hits as f32 / qt.len() as f32
}

/// 判断查询 token 与描述 token 是否共享相关性信号
///
/// 含三种情况：相等 / 子串包含 / **共享至少 2 字符的子串**（覆盖中文嵌入词，
/// 如查询「帮我报销」与描述「报销与费用审核流程」共享「报销」）。
/// 仅对含 CJK 的查询 token 启用 2 字符窗口——纯 ASCII 词用 2 字符窗口会产生
/// 大量无意义命中（如 "validate" 与 "text" 共享 "te"）。
fn shares_signal(q: &str, d: &str) -> bool {
    if q == d || d.contains(q) || q.contains(d) {
        return true;
    }
    if !has_cjk(q) {
        return false;
    }
    let chars: Vec<char> = q.chars().collect();
    if chars.len() < 2 {
        return false;
    }
    chars.windows(2).any(|w| {
        let sub: String = w.iter().collect();
        d.contains(&sub)
    })
}

/// 是否含 CJK 字符（汉字基本区 U+4E00 ~ U+9FFF）
fn has_cjk(s: &str) -> bool {
    s.chars().any(|c| ('\u{4e00}'..='\u{9fff}').contains(&c))
}

/// 切分语义 token：连续字母/数字/CJK 字符（`char::is_alphanumeric` 覆盖中文）
fn tokens(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() {
            cur.push(c.to_ascii_lowercase());
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// 渲染选中技能为可追加进 system prompt 的文本（L2 正文）
///
/// 输出含统一标题 + L1 描述 + L2 正文，便于模型识别「何时用 / 怎么做」。
pub fn render_skill_context(skills: &[Arc<Skill>]) -> String {
    let mut out = String::new();
    for s in skills {
        out.push_str(&format!("## Skill: {}\n\n", s.name()));
        out.push_str(&format!("> {}\n\n", s.description()));
        out.push_str(s.body());
        out.push_str("\n\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill(name: &str, desc: &str) -> Arc<Skill> {
        Arc::new(Skill::from_parts(name, desc, format!("body of {name}")))
    }

    #[test]
    fn keyword_selects_relevant() {
        let skills = vec![
            skill("expense", "file and validate employee expense reports"),
            skill("pdf", "extract text from pdf documents"),
        ];
        let router = KeywordRouter::default();
        let picked = router.select("please validate my expense report", &skills);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].name(), "expense");
    }

    #[test]
    fn keyword_threshold_filters() {
        let skills = vec![skill("pdf", "extract text from pdf documents")];
        // 高阈值：无关查询被过滤
        let router = KeywordRouter::new(KeywordConfig {
            threshold: 0.5,
            ..KeywordConfig::default()
        });
        let picked = router.select("what is the weather", &skills);
        assert!(picked.is_empty());
    }

    #[test]
    fn keyword_cjk_substring_match() {
        let skills = vec![skill("expense", "报销与费用审核流程")];
        let router = KeywordRouter::default();
        let picked = router.select("帮我报销", &skills);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].name(), "expense");
    }

    #[test]
    fn max_skills_bounded() {
        let skills = vec![
            skill("a", "shared keyword alpha"),
            skill("b", "shared keyword beta"),
            skill("c", "shared keyword gamma"),
        ];
        let router = KeywordRouter::new(KeywordConfig {
            max_skills: 2,
            ..KeywordConfig::default()
        });
        let picked = router.select("shared", &skills);
        assert_eq!(picked.len(), 2);
    }

    #[test]
    fn max_total_chars_bounded() {
        let skills = vec![
            skill("a", "keyword one"),
            skill("b", "keyword two"),
        ];
        let router = KeywordRouter::new(KeywordConfig {
            max_total_chars: 22, // 只够 1 个（name+desc+body 起步即超）
            ..KeywordConfig::default()
        });
        let picked = router.select("keyword", &skills);
        assert!(picked.len() <= 1);
    }

    #[test]
    fn empty_query_selects_nothing() {
        let skills = vec![skill("a", "anything")];
        let router = KeywordRouter::default();
        assert!(router.select("", &skills).is_empty());
    }

    #[test]
    fn render_contains_l1_and_l2() {
        let skills = vec![skill("expense", "file expense reports")];
        let text = render_skill_context(&skills);
        assert!(text.contains("## Skill: expense"));
        assert!(text.contains("file expense reports"));
        assert!(text.contains("body of expense"));
    }

    #[test]
    fn tokens_split_cjk_and_ascii() {
        assert_eq!(tokens("Hello, world!"), vec!["hello", "world"]);
        assert_eq!(tokens("报销审核"), vec!["报销审核"]);
        assert_eq!(tokens(""), Vec::<String>::new());
    }
}
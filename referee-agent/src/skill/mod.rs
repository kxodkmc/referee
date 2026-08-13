//! Agent Skills 开放标准（SKILL.md）集成 — 业务层按需拓展
//!
//! 镜像 MCP 范式：在 `referee-agent` 内实现，启用 `skills` feature 后加载，
//! **零新增依赖**（仅用白名单内的 std / thiserror / dashmap）。
//!
//! ## 定位（对照代理 Skills 开放标准，agentskills.io）
//! Skill 是**目录**：必填 `SKILL.md`（frontmatter 元数据 + Markdown 正文）+
//! 可选 `scripts/` `references/` `assets/` 等资源。与工具不同，Skill 不是
//! 「执行并返回结果」，而是**注入程序性知识 / 改写执行上下文**，遵循
//! [渐进式披露](https://agentskills.io/specification)：
//! - **L1 元数据**（启动即加载）：`name` + `description` → 进 system prompt
//! - **L2 正文**：`SKILL.md` body，判定相关时才加载
//! - **L3 资源**：关联文件，按需读取
//!
//! ## 模块结构
//! - [`frontmatter`]：SKILL.md frontmatter 极简解析（零依赖 YAML）
//! - [`loader`]：目录扫描 → 构造 [`Skill`]（含资源有界加载）
//! - [`registry`]：有界 [`SkillRegistry`]
//! - [`router`]：相关性选择（[`SkillRouter`]）+ 注入渲染
//!
//! ## 设计约束（对齐 AGENTS.md）
//! - **数据/行为分离**：`Skill` 是纯数据载体；选择/注入行为在 `router`
//! - **有界**：资源总字节、条数、metadata 条目均有上限，防 OOM
//! - **隔离**：Skill 正文为不可信文本，仅作注入，不执行；`scripts/` 不自动执行
//! - **零新增依赖**：frontmatter 用行级解析，不回写 base（base 保持零改动）

pub mod frontmatter;
pub mod loader;
pub mod registry;
pub mod router;

use std::collections::BTreeMap;

pub use frontmatter::{Frontmatter, FrontmatterError};
pub use loader::{load_root, SkillConfig};
pub use registry::{RegistryConfig, RegistryError, SkillRegistry};
pub use router::{render_skill_context, KeywordConfig, KeywordRouter, SkillRouter};

/// 技能加载/构造错误
#[derive(Debug, thiserror::Error)]
pub enum SkillError {
    /// 读取文件失败
    #[error("skill io error: {0}")]
    Io(String),
    /// frontmatter 解析失败
    #[error("skill frontmatter error: {0}")]
    Frontmatter(#[from] FrontmatterError),
    /// `name` 与技能目录名不一致（规范要求一致）
    #[error("skill name '{0}' does not match directory '{1}'")]
    NameMismatch(String, String),
    /// 资源文件超长（单文件字节上限）
    #[error("skill resource '{0}' too large ({1} bytes, max {2})")]
    ResourceTooLarge(String, usize, usize),
    /// 资源总字节超限
    #[error("skill resources too large ({0} bytes, max {1})")]
    ResourcesTooLarge(usize, usize),
    /// 资源文件数超限
    #[error("skill too many resources ({0}, max {1})")]
    TooManyResources(usize, usize),
}

/// 单个 Agent Skill — 纯数据载体（渐进式披露三层）
///
/// 通过 [`Skill::load`]（磁盘目录）或 [`Skill::from_parts`]（内存构造）获得。
/// 所有字段私有，经只读访问器暴露；`Clone` 支持 `Arc` 共享。
#[derive(Debug, Clone)]
pub struct Skill {
    name: String,
    description: String,
    license: Option<String>,
    compatibility: Option<String>,
    metadata: Vec<(String, String)>,
    /// SKILL.md Markdown 正文（L2）
    body: String,
    /// 关联资源（L3）：相对路径 → 内容
    resources: BTreeMap<String, String>,
}

impl Skill {
    /// 从磁盘目录加载（读取 `SKILL.md` + 有界加载资源）
    ///
    /// 见 [`loader`] 模块与 [`SkillConfig`]。
    pub fn load(dir: &std::path::Path, config: &SkillConfig) -> Result<Self, SkillError> {
        loader::load_skill(dir, config)
    }

    /// 从已知碎片构造（测试 / 内存集成用），跳过目录校验
    pub fn from_parts(
        name: impl Into<String>,
        description: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            license: None,
            compatibility: None,
            metadata: Vec::new(),
            body: body.into(),
            resources: BTreeMap::new(),
        }
    }

    /// 技能名（L1 元数据，进 system prompt）
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 技能描述（L1 元数据，供相关性判断）
    pub fn description(&self) -> &str {
        &self.description
    }

    /// 许可证（可选）
    pub fn license(&self) -> Option<&str> {
        self.license.as_deref()
    }

    /// 环境要求（可选）
    pub fn compatibility(&self) -> Option<&str> {
        self.compatibility.as_deref()
    }

    /// 附加元数据（可选）
    pub fn metadata(&self) -> &[(String, String)] {
        &self.metadata
    }

    /// SKILL.md 正文（L2，判定相关后加载）
    pub fn body(&self) -> &str {
        &self.body
    }

    /// 读取关联资源（L3，按需），无则 `None`
    pub fn resource(&self, rel_path: &str) -> Option<&str> {
        self.resources.get(rel_path).map(String::as_str)
    }

    /// 全部关联资源相对路径（用于选择/只读枚举）
    pub fn resource_names(&self) -> impl Iterator<Item = &str> {
        self.resources.keys().map(String::as_str)
    }

    /// 导出 L1 声明（`name` + `description`，供注册表快照 / 注入）
    pub fn to_declaration(&self) -> SkillDeclaration {
        SkillDeclaration {
            name: self.name.clone(),
            description: self.description.clone(),
        }
    }
}

/// Skill 的 L1 声明（纯数据，供启动注入 system prompt）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillDeclaration {
    /// 技能名
    pub name: String,
    /// 技能描述（LLM 判断何时使用的依据）
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use referee_ai_base::prompt::{build_prompt, PromptParts};
    use referee_ai_base::provider::{Message, ThinkingConfig};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

    /// 测试用临时目录：Drop 时自动清理
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let id = TEMP_SEQ.fetch_add(1, Ordering::SeqCst);
            let root = std::env::temp_dir()
                .join(format!("referee_skill_e2e_{id}_{}", std::process::id()));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn from_parts_and_accessors() {
        let s = Skill::from_parts("foo", "does foo things", "# Foo\nsteps");
        assert_eq!(s.name(), "foo");
        assert_eq!(s.description(), "does foo things");
        assert_eq!(s.body(), "# Foo\nsteps");
        assert!(s.license().is_none());
        assert!(s.compatibility().is_none());
        assert!(s.metadata().is_empty());
        assert_eq!(s.resource("x"), None);
        assert_eq!(s.resource_names().count(), 0);
    }

    #[test]
    fn declaration_shape() {
        let s = Skill::from_parts("foo", "do foo", "body");
        let d = s.to_declaration();
        assert_eq!(d.name, "foo");
        assert_eq!(d.description, "do foo");
    }

    /// 端到端：skills 目录 → 加载注册 → 路由选择 → 渲染注入 → build_prompt
    ///
    /// 覆盖完整链路，验证 Skill 的 L1（描述）与 L2（正文）最终进入 system prompt，
    /// 且既有对话历史不受影响。
    #[test]
    fn end_to_end_skill_dir_to_prompt_injection() {
        // 1. 临时 skills 根目录，写入一个真实 SKILL.md
        let tmp = TempDir::new();
        let skill_dir = tmp.path().join("expense-report");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("SKILL.md"),
            concat!(
                "---\n",
                "name: expense-report\n",
                "description: File and validate employee expense reports according to company policy.\n",
                "---\n",
                "# Expense Report\n\n",
                "Follow these steps to validate an expense report.\n",
            ),
        )
        .unwrap();

        // 2. 加载 + 注册
        let skills = load_root(tmp.path(), &SkillConfig::default()).unwrap();
        assert_eq!(skills.len(), 1, "one skill loaded from root");
        let registry = SkillRegistry::with_defaults();
        for s in skills {
            registry.register(Arc::new(s)).unwrap();
        }
        assert_eq!(registry.len(), 1);

        // 3. 路由选择（查询命中英文描述）
        let router = KeywordRouter::default();
        let activated = router.select("please validate my expense report", &registry.all());
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0].name(), "expense-report");

        // 4. 渲染注入文本
        let ctx = render_skill_context(&activated);
        assert!(ctx.contains("## Skill: expense-report"));
        assert!(ctx.contains("# Expense Report"));

        // 5. 拼进 system 消息 → base build_prompt（预算 0 = 不截断，完整保留）
        let system = format!("You are a helpful assistant.\n\n{ctx}");
        let req = build_prompt(PromptParts {
            system: Some(Message::system(system)),
            tools: vec![],
            history: vec![Message::user("帮我报销")],
            memory: vec![],
            artifacts: vec![],
            temperature: None,
            max_tokens: None,
            thinking: ThinkingConfig::default(),
            prompt_budget: 0,
        });

        // 6. 断言 system 消息含 L1 描述与 L2 正文，且对话历史保留
        let sys_text = req
            .messages
            .iter()
            .find(|m| m.role == referee_ai_base::Role::System)
            .expect("system message present")
            .content
            .as_text()
            .unwrap()
            .to_string();
        assert!(sys_text.contains("## Skill: expense-report"));
        assert!(sys_text.contains("File and validate employee expense reports"));
        assert!(sys_text.contains("Follow these steps to validate an expense report"));
        assert_eq!(req.messages.len(), 2, "system + user history both kept");
    }
}
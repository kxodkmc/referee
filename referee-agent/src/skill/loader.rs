//! Skill 目录加载 — 扫描技能根目录，读取 `SKILL.md` 构造 [`Skill`]
//!
//! ## 使命
//! 将磁盘上的 Agent Skills 目录（`SKILL.md` + 资源）装载为内存 [`Skill`]。
//! 这是**注册期一次性**操作，使用同步 `std::fs`（非热路径，无需 async）。
//!
//! ## 有界约束（防 OOM）
//! [`SkillConfig`] 控制：单资源文件字节上限、资源总字节上限、资源条数上限。
//! 违反任一项均返回显式 [`SkillError`]，不静默截断。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::skill::frontmatter;
use crate::skill::{Skill, SkillError};

/// 技能加载配置（有界硬约束）
#[derive(Debug, Clone)]
pub struct SkillConfig {
    /// 单个资源文件字节上限（默认 512 KiB）
    pub max_resource_file_bytes: usize,
    /// 全部资源总字节上限（默认 4 MiB）
    pub max_total_resource_bytes: usize,
    /// 资源条数上限（默认 64）
    pub max_resources: usize,
    /// metadata 条目数上限（默认 32）
    pub max_metadata: usize,
}

impl Default for SkillConfig {
    fn default() -> Self {
        Self {
            max_resource_file_bytes: 512 * 1024,
            max_total_resource_bytes: 4 * 1024 * 1024,
            max_resources: 64,
            max_metadata: 32,
        }
    }
}

/// 从技能根目录加载全部技能目录
///
/// 遍历 `root` 的每个子目录：含 `SKILL.md` 则加载；不含则跳过（允许根目录
/// 混放 README 等）。`SKILL.md` 存在但解析失败时**向上传播错误**（不静默丢弃）。
pub fn load_root(root: &Path, config: &SkillConfig) -> Result<Vec<Skill>, SkillError> {
    let entries = std::fs::read_dir(root).map_err(|e| SkillError::Io(e.to_string()))?;
    let mut skills = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| SkillError::Io(e.to_string()))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if !path.join("SKILL.md").exists() {
            continue;
        }
        skills.push(load_skill(&path, config)?);
    }
    Ok(skills)
}

/// 从单个技能目录加载（`SKILL.md` + 有界资源）
///
/// 校验：目录名等于 `name`（规范要求）；`SKILL.md` frontmatter 合法。
pub(crate) fn load_skill(dir: &Path, config: &SkillConfig) -> Result<Skill, SkillError> {
    let skill_md = dir.join("SKILL.md");
    let raw = std::fs::read_to_string(&skill_md).map_err(|e| SkillError::Io(e.to_string()))?;
    let (fm, body) = frontmatter::parse(&raw, config.max_metadata)?;

    // 规范要求：name 与目录同名
    let dir_name = dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    if fm.name != dir_name {
        return Err(SkillError::NameMismatch(fm.name, dir_name));
    }

    let resources = load_resources(dir, config)?;

    Ok(Skill {
        name: fm.name,
        description: fm.description,
        license: fm.license,
        compatibility: fm.compatibility,
        metadata: fm.metadata,
        body,
        resources,
    })
}

/// 递归加载技能目录下除 `SKILL.md` 外的资源（相对路径 → 内容），有界
fn load_resources(
    dir: &Path,
    config: &SkillConfig,
) -> Result<BTreeMap<String, String>, SkillError> {
    let mut resources = std::collections::BTreeMap::new();
    let mut total = 0usize;
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];

    while let Some(current) = stack.pop() {
        let entries =
            std::fs::read_dir(&current).map_err(|e| SkillError::Io(e.to_string()))?;
        let mut subdirs: Vec<PathBuf> = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| SkillError::Io(e.to_string()))?;
            let path = entry.path();
            if path.is_dir() {
                subdirs.push(path);
                continue;
            }
            // 跳过 SKILL.md 自身
            if path.file_name().and_then(|n| n.to_str()) == Some("SKILL.md") {
                continue;
            }
            let rel = path
                .strip_prefix(dir)
                .map_err(|e| SkillError::Io(e.to_string()))?;
            let rel_str = rel.to_string_lossy().replace('\\', "/");

            if resources.len() >= config.max_resources {
                return Err(SkillError::TooManyResources(
                    resources.len() + 1,
                    config.max_resources,
                ));
            }
            let bytes =
                std::fs::read(&path).map_err(|e| SkillError::Io(e.to_string()))?;
            if bytes.len() > config.max_resource_file_bytes {
                return Err(SkillError::ResourceTooLarge(
                    rel_str,
                    bytes.len(),
                    config.max_resource_file_bytes,
                ));
            }
            total += bytes.len();
            if total > config.max_total_resource_bytes {
                return Err(SkillError::ResourcesTooLarge(
                    total,
                    config.max_total_resource_bytes,
                ));
            }
            // 资源为文本注入用，非法 UTF-8 以 lossy 保留（不 panic）
            resources.insert(rel_str, String::from_utf8_lossy(&bytes).into_owned());
        }
        stack.extend(subdirs);
    }
    Ok(resources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

    /// 测试用临时目录：Drop 时自动清理
    struct TempDir(PathBuf);

    impl TempDir {
        /// 构造根目录并在其下创建 `name` 技能目录（含 SKILL.md）
        fn new_with_skill(name: &str, skill_md: &str) -> Self {
            let id = TEMP_SEQ.fetch_add(1, Ordering::SeqCst);
            let root = std::env::temp_dir()
                .join(format!("referee_skill_test_{id}_{}", std::process::id()));
            let dir = root.join(name);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("SKILL.md"), skill_md).unwrap();
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

    fn setup_skill_root(name: &str, skill_md: &str) -> TempDir {
        TempDir::new_with_skill(name, skill_md)
    }

    fn skill_md(name: &str, desc: &str, body: &str) -> String {
        format!("---\nname: {name}\ndescription: {desc}\n---\n{body}")
    }

    #[test]
    fn load_single_skill() {
        let tmp = setup_skill_root("expense-report", &skill_md("expense-report", "File expense reports", "# body"));
        let skills = load_root(tmp.path(), &SkillConfig::default()).unwrap();
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name(), "expense-report");
        assert_eq!(skills[0].body(), "# body");
    }

    #[test]
    fn load_root_skips_non_skill_dirs() {
        let tmp = setup_skill_root("expense-report", &skill_md("expense-report", "d", "b"));
        // 根目录再放一个非技能目录 + 一个普通文件
        fs::create_dir_all(tmp.path().join("notes")).unwrap();
        fs::write(tmp.path().join("README.md"), "readme").unwrap();
        let skills = load_root(tmp.path(), &SkillConfig::default()).unwrap();
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name(), "expense-report");
    }

    #[test]
    fn load_name_mismatch_rejected() {
        let tmp = setup_skill_root("skill-dir-a", &skill_md("other-name", "d", "b"));
        let err = load_root(tmp.path(), &SkillConfig::default()).unwrap_err();
        assert!(matches!(err, SkillError::NameMismatch(_, _)));
    }

    #[test]
    fn load_malformed_skill_propagates_error() {
        let tmp = setup_skill_root("bad", "no frontmatter here");
        assert!(load_root(tmp.path(), &SkillConfig::default()).is_err());
    }

    #[test]
    fn load_resources_bounded_and_exposed() {
        let tmp = setup_skill_root("foo", &skill_md("foo", "d", "b"));
        let dir = tmp.path().join("foo");
        fs::create_dir_all(dir.join("references")).unwrap();
        fs::write(dir.join("references/policy.md"), "# Policy").unwrap();
        fs::write(dir.join("script.sh"), "#!/bin/sh").unwrap();

        let skills = load_root(tmp.path(), &SkillConfig::default()).unwrap();
        let s = &skills[0];
        assert_eq!(s.resource("references/policy.md"), Some("# Policy"));
        assert_eq!(s.resource("script.sh"), Some("#!/bin/sh"));
        assert_eq!(s.resource("SKILL.md"), None, "SKILL.md 不应作为资源");
        let names: Vec<_> = s.resource_names().collect();
        assert!(names.contains(&"script.sh"));
    }

    #[test]
    fn resource_file_too_large_rejected() {
        let tmp = setup_skill_root("foo", &skill_md("foo", "d", "b"));
        fs::write(tmp.path().join("foo/big.bin"), vec![0u8; 10]).unwrap();
        let config = SkillConfig {
            max_resource_file_bytes: 5,
            ..SkillConfig::default()
        };
        let err = load_root(tmp.path(), &config).unwrap_err();
        assert!(matches!(err, SkillError::ResourceTooLarge(_, _, _)));
    }

    #[test]
    fn too_many_resources_rejected() {
        let tmp = setup_skill_root("foo", &skill_md("foo", "d", "b"));
        for i in 0..3 {
            fs::write(tmp.path().join("foo").join(format!("r{i}")), "x").unwrap();
        }
        let config = SkillConfig {
            max_resources: 2,
            ..SkillConfig::default()
        };
        let err = load_root(tmp.path(), &config).unwrap_err();
        assert!(matches!(err, SkillError::TooManyResources(_, _)));
    }
}
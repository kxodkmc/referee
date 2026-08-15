//! 文件工具共享基础设施 — 路径解析、字节上限、有界读取、原子写、二进制嗅探
//!
//! 供 `read` / `write` / `edit`（及未来 `glob`/`grep`）复用，保持各工具职责单一、
//! 无重复实现。所有上限可配置（对齐「无硬编码 tunable」原则）。

use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt;

use referee_ai_base::tool::ToolError;

/// 单文件读写字节默认上限（16 MiB）
pub const DEFAULT_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// 二进制嗅探字节数
const SNIFF_BYTES: usize = 8192;

/// 句子结束符集合（read 边界回溯用，集中维护）
pub const SENTENCE_BOUNDARIES: [char; 9] = ['。', '！', '？', '!', '?', '.', '；', ';', '：'];

/// 文件工具共享配置
#[derive(Debug, Clone)]
pub struct FsConfig {
    /// 单文件读写字节上限（防 OOM / 超时）
    pub max_file_bytes: u64,
    /// 可选的根目录约束（None = 不限制，安全交由沙箱/上层）
    pub root: Option<PathBuf>,
}

impl Default for FsConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            root: None,
        }
    }
}

/// 路径解析 + 可选 root 约束
///
/// 目标已存在时 canonicalize 全路径（解析符号链接，杜绝经链接逃逸 root）；
/// 目标不存在时（`write` 创建新文件）改为 canonicalize 父目录再拼接文件名，
/// 避免新文件因无法解析而误报。
pub fn resolve_path(raw: &str, root: &Option<PathBuf>) -> Result<PathBuf, ToolError> {
    let path = Path::new(raw);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| ToolError::Execution(format!("cannot resolve cwd: {e}")))?
            .join(path)
    };
    let Some(root) = root else {
        return Ok(abs);
    };
    let root_abs = root
        .canonicalize()
        .map_err(|e| ToolError::Execution(format!("bad root: {e}")))?;
    let resolved = match abs.canonicalize() {
        Ok(canon) => canon,
        Err(_) => {
            let parent = abs
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let parent_abs = parent
                .canonicalize()
                .map_err(|e| ToolError::Execution(format!("cannot resolve parent: {e}")))?;
            let file_name = abs
                .file_name()
                .ok_or_else(|| ToolError::Execution("path has no file name".into()))?;
            parent_abs.join(file_name)
        }
    };
    if !resolved.starts_with(&root_abs) {
        return Err(ToolError::Execution("path escapes configured root".into()));
    }
    Ok(resolved)
}

/// 有界读取：超过 max_file_bytes 拒绝
pub async fn read_bounded(path: &Path, max_file_bytes: u64) -> Result<Vec<u8>, ToolError> {
    let meta = tokio::fs::metadata(path)
        .await
        .map_err(|e| ToolError::Execution(format!("cannot stat {}: {e}", path.display())))?;
    if meta.len() > max_file_bytes {
        return Err(ToolError::Execution(format!(
            "file too large ({} bytes > max {}) for a single operation",
            meta.len(),
            max_file_bytes
        )));
    }
    tokio::fs::read(path)
        .await
        .map_err(|e| ToolError::Execution(format!("cannot read {}: {e}", path.display())))
}

/// 二进制嗅探：前 `SNIFF_BYTES` 字节含 NUL 即视为二进制
pub fn looks_binary(head: &[u8]) -> bool {
    head.iter().take(SNIFF_BYTES).any(|&b| b == 0)
}

/// 原子写：同目录临时文件 + 落盘 + rename 发布，防半写
///
/// - 写入临时文件并 `sync_all` 后再 rename，确保数据先落盘、后原子可见；
/// - 覆盖已有文件时保留其权限位（避免覆盖可执行文件后丢失 `+x`）；
/// - rename 失败时尽力清理临时文件。
pub async fn atomic_write(path: &Path, content: &[u8]) -> Result<(), ToolError> {
    let tmp = tmp_path(path);
    let existing_perm = tokio::fs::metadata(path)
        .await
        .map(|m| m.permissions())
        .ok();

    let write_result = async {
        let mut f = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| ToolError::Execution(format!("cannot create temp: {e}")))?;
        f.write_all(content)
            .await
            .map_err(|e| ToolError::Execution(format!("cannot write temp: {e}")))?;
        f.sync_all()
            .await
            .map_err(|e| ToolError::Execution(format!("cannot sync temp: {e}")))?;
        if let Some(perm) = existing_perm {
            tokio::fs::set_permissions(&tmp, perm)
                .await
                .map_err(|e| ToolError::Execution(format!("cannot set temp permissions: {e}")))?;
        }
        Ok::<(), ToolError>(())
    }
    .await;
    if let Err(e) = write_result {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }

    tokio::fs::rename(&tmp, path)
        .await
        .map_err(|e| {
            let _ = tokio::fs::remove_file(&tmp);
            ToolError::Execution(format!("cannot publish {}: {e}", path.display()))
        })?;
    Ok(())
}

/// 生成同目录隐藏临时文件路径（与目标同目录，保证 rename 原子 / 同文件系统）
fn tmp_path(path: &Path) -> PathBuf {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("tmp");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    parent.join(format!(".{file_name}.{nanos}.tmp"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("referee_fs_{}_", name)).join(
            Uuid::new_v4().to_string(),
        );
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn resolve_path_relative_joins_cwd() {
        let p = resolve_path("foo.txt", &None).unwrap();
        assert!(p.is_absolute());
        assert!(p.ends_with("foo.txt"));
    }

    #[test]
    fn resolve_path_root_allows_inside() {
        let dir = temp_dir("inside");
        let file = dir.join("a.txt");
        std::fs::write(&file, "x").unwrap();
        let root = Some(dir.clone());
        let p = resolve_path(file.to_str().unwrap(), &root).unwrap();
        assert!(p.starts_with(dir.canonicalize().unwrap()));
    }

    #[test]
    fn resolve_path_root_rejects_outside() {
        let dir = temp_dir("outside_root");
        let root_dir = temp_dir("outside_root_root");
        let file = dir.join("a.txt");
        std::fs::write(&file, "x").unwrap();
        let err = resolve_path(file.to_str().unwrap(), &Some(root_dir)).unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }

    #[test]
    fn resolve_path_root_allows_new_file() {
        // write 创建的新文件尚不存在：须能解析（FIX-1）
        let dir = temp_dir("newfile");
        let target = dir.join("new.txt");
        let root = Some(dir.clone());
        let p = resolve_path(target.to_str().unwrap(), &root).unwrap();
        assert_eq!(p, dir.canonicalize().unwrap().join("new.txt"));
    }

    #[tokio::test]
    async fn read_bounded_rejects_oversize() {
        let dir = temp_dir("oversize");
        let file = dir.join("big.bin");
        std::fs::write(&file, vec![0u8; 32]).unwrap();
        let err = read_bounded(&file, 16).await.unwrap_err();
        assert!(matches!(err, ToolError::Execution(_)));
    }

    #[tokio::test]
    async fn atomic_write_publishes_no_temp_leftover() {
        let dir = temp_dir("atomic");
        let file = dir.join("out.txt");
        atomic_write(&file, b"hello").await.unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "hello");
        // 无临时文件残留
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "temp files leaked: {leftovers:?}");
    }
}
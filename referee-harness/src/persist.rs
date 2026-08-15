//! 持久化 — 实例规格与会话事实的 JSONL 落盘 + 崩溃恢复扫描
//!
//! **职责边界**：只做文件 IO 与序列化存取，不做会话语义（会话语义由
//! `instance` / base 引擎负责）。损坏处理（broken 清单）在此显式完成：
//! 恢复时不可解析的实例 / 会话文件进入 broken 清单，不阻塞启动。
//!
//! 目录布局：
//! ```text
//!   <state_dir>/
//!     instances/<id>.json                     # 实例规格（InstanceSpec）
//!     sessions/<instance_id>/<session>.jsonl  # 会话事实，一行一条 Message
//! ```
//!
//! 写入用追加式（OS 缓冲，非每行 fsync）；落盘失败**显式返回错误**，不吞异常。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use referee_ai_base::provider::Message;
use referee_ai_base::session::{LogError, SessionId, SessionLogSink};

use crate::protocol::{InstanceId, InstanceSpec};

/// 持久化错误
#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("serialize/deserialize error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("bad instance id: {0}")]
    InstanceId(String),
}

impl PersistError {
    fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

/// 崩溃恢复条目 — 无法恢复的实例 / 会话
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokenEntry {
    pub path: String,
    pub reason: String,
}

/// 崩溃恢复结果
#[derive(Debug, Clone, Default)]
pub struct RecoveryResult {
    pub recovered_instances: usize,
    pub recovered_sessions: usize,
    pub broken: Vec<BrokenEntry>,
}

/// `load_instances` 返回：可恢复实例列表 + 损坏清单
pub type LoadedInstances = (Vec<(InstanceId, InstanceSpec)>, Vec<BrokenEntry>);
/// `load_instance_sessions` 返回：可恢复会话列表 + 损坏清单
pub type LoadedSessions = (Vec<(SessionId, Vec<Message>)>, Vec<BrokenEntry>);

/// 持久化后端
#[derive(Debug, Clone)]
pub struct PersistStore {
    state_dir: PathBuf,
}

impl PersistStore {
    /// 新建后端；自动创建 `instances/` 与 `sessions/` 目录
    pub fn new(state_dir: PathBuf) -> Result<Self, PersistError> {
        let store = Self { state_dir };
        store
            .instances_dir()
            .map_err(|e| PersistError::io(store.state_dir.clone(), e))?;
        store
            .sessions_root()
            .map_err(|e| PersistError::io(store.state_dir.clone(), e))?;
        Ok(store)
    }

    fn instances_dir(&self) -> std::io::Result<PathBuf> {
        let p = self.state_dir.join("instances");
        std::fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn sessions_root(&self) -> std::io::Result<PathBuf> {
        let p = self.state_dir.join("sessions");
        std::fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn instance_dir(&self, id: &InstanceId) -> std::io::Result<PathBuf> {
        let p = self.sessions_root()?.join(id.as_str());
        std::fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn instance_path(&self, id: &InstanceId) -> PathBuf {
        self.state_dir
            .join("instances")
            .join(format!("{}.json", id.as_str()))
    }

    fn session_path(&self, id: &InstanceId, session_id: &SessionId) -> PathBuf {
        self.state_dir
            .join("sessions")
            .join(id.as_str())
            .join(format!("{session_id}.jsonl"))
    }

    // ── 实例规格 ──────────────────────────────

    /// 保存实例规格（覆盖写）
    pub fn save_instance(&self, id: &InstanceId, spec: &InstanceSpec) -> Result<(), PersistError> {
        let path = self.instance_path(id);
        let json = serde_json::to_string_pretty(spec)?;
        std::fs::write(&path, json).map_err(|e| PersistError::io(&path, e))?;
        Ok(())
    }

    /// 移除实例规格文件
    pub fn remove_instance(&self, id: &InstanceId) -> Result<(), PersistError> {
        let path = self.instance_path(id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PersistError::io(&path, e)),
        }
    }

    /// 扫描并反序列化全部实例规格；损坏文件进入 broken 清单（不阻塞）
    pub fn load_instances(&self) -> Result<LoadedInstances, PersistError> {
        let dir = self.instances_dir().map_err(|e| PersistError::io(self.state_dir.clone(), e))?;
        let mut specs = Vec::new();
        let mut broken = Vec::new();
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| PersistError::io(&dir, e))?
            .flatten()
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            match Self::read_instance(&path, &name) {
                Ok(Some(spec)) => specs.push(spec),
                Ok(None) => {} // 非法 id：作为 broken 记录
                Err(e) => broken.push(BrokenEntry {
                    path: path.display().to_string(),
                    reason: e.to_string(),
                }),
            }
        }
        Ok((specs, broken))
    }

    fn read_instance(
        path: &Path,
        name: &str,
    ) -> Result<Option<(InstanceId, InstanceSpec)>, PersistError> {
        let id = match InstanceId::new(name) {
            Ok(id) => id,
            Err(_) => {
                return Ok(None);
            }
        };
        let raw = std::fs::read_to_string(path).map_err(|e| PersistError::io(path, e))?;
        let spec: InstanceSpec = serde_json::from_str(&raw)?;
        Ok(Some((id, spec)))
    }

    // ── 会话事实 ──────────────────────────────

    /// 追加一条会话事实（JSONL append）
    pub fn append_session_event(
        &self,
        instance_id: &InstanceId,
        session_id: &SessionId,
        msg: &Message,
    ) -> Result<(), PersistError> {
        let dir = self.instance_dir(instance_id).map_err(|e| PersistError::io(self.state_dir.clone(), e))?;
        let path = dir.join(format!("{session_id}.jsonl"));
        let mut line = serde_json::to_vec(msg)?;
        line.push(b'\n');
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| PersistError::io(&path, e))?;
        f.write_all(&line).map_err(|e| PersistError::io(&path, e))?;
        Ok(())
    }

    /// 读取某实例的全部会话事实（按文件名解析会话 id）
    pub fn load_instance_sessions(
        &self,
        instance_id: &InstanceId,
    ) -> Result<LoadedSessions, PersistError> {
        let dir = self
            .instance_dir(instance_id)
            .map_err(|e| PersistError::io(self.state_dir.clone(), e))?;
        let mut sessions = Vec::new();
        let mut broken = Vec::new();
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| PersistError::io(&dir, e))?
            .flatten()
        {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
                continue;
            }
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            let session_id = match SessionId::parse_str(name) {
                Ok(id) => id,
                Err(_) => {
                    broken.push(BrokenEntry {
                        path: path.display().to_string(),
                        reason: format!("invalid session id '{name}'"),
                    });
                    continue;
                }
            };
            match Self::read_session_events(&path) {
                Ok(msgs) => sessions.push((session_id, msgs)),
                Err(e) => broken.push(BrokenEntry {
                    path: path.display().to_string(),
                    reason: e.to_string(),
                }),
            }
        }
        Ok((sessions, broken))
    }

    /// 读取单个会话事件（一行一条 Message）
    pub fn load_session_events(
        &self,
        instance_id: &InstanceId,
        session_id: &SessionId,
    ) -> Result<Vec<Message>, PersistError> {
        Self::read_session_events(&self.session_path(instance_id, session_id))
    }

    fn read_session_events(path: &Path) -> Result<Vec<Message>, PersistError> {
        let raw = match std::fs::read_to_string(path) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(PersistError::io(path, e)),
        };
        let mut out = Vec::new();
        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<Message>(line) {
                Ok(msg) => out.push(msg),
                Err(e) => return Err(PersistError::Serde(e)),
            }
        }
        Ok(out)
    }

    /// 移除单个会话文件
    pub fn remove_session(
        &self,
        instance_id: &InstanceId,
        session_id: &SessionId,
    ) -> Result<(), PersistError> {
        let path = self.session_path(instance_id, session_id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PersistError::io(&path, e)),
        }
    }

    /// 构造按实例划分的会话落盘 sink（实现 base `SessionLogSink`）
    pub fn instance_sink(&self, instance_id: InstanceId) -> InstanceLogSink {
        InstanceLogSink {
            store: self.clone(),
            instance_id,
        }
    }
}

/// 按实例划分的会话落盘 sink — 把 base `Session::push_history` 事实追加到
/// `sessions/<instance>/<session>.jsonl`。
#[derive(Debug, Clone)]
pub struct InstanceLogSink {
    store: PersistStore,
    instance_id: InstanceId,
}

impl SessionLogSink for InstanceLogSink {
    fn append(&self, session_id: &SessionId, msg: &Message) -> Result<(), LogError> {
        self.store
            .append_session_event(&self.instance_id, session_id, msg)
            .map_err(|e| LogError::Io(e.to_string()))
    }
}

/// 便捷：把 sink 装箱为 trait object
pub fn into_dyn(sink: InstanceLogSink) -> Arc<dyn SessionLogSink> {
    Arc::new(sink)
}
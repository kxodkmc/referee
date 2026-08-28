//! 持久化状态——凭据（登录后一次写入）、游标与会话令牌（随收发即时落盘）
//!
//! 游标语义（协议文档 §3.2/§8）：崩溃时宁可重放不可丢消息——落盘永远发生在
//! 入站消息成功投递之后。`flush` 承担的仅是补写异常退出遗留的脏数据。

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use referee_channel::adapter::{AdapterError, AdapterState};

/// 登录凭据——`<state_dir>/credentials.json`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub bot_token: String,
    pub ilink_bot_id: String,
    pub ilink_user_id: String,
}

impl Credentials {
    /// 文件缺失或损坏返回 None——调用方走扫码登录
    pub fn load(dir: &Path) -> Option<Self> {
        let text = fs::read_to_string(dir.join("credentials.json")).ok()?;
        serde_json::from_str(&text).ok()
    }

    pub fn save(&self, dir: &Path) -> Result<(), AdapterError> {
        fs::create_dir_all(dir).map_err(|e| format!("create state dir: {e}"))?;
        let json = serde_json::to_vec_pretty(self).map_err(|e| format!("encode credentials: {e}"))?;
        atomic_write(&dir.join("credentials.json"), &json)
            .map_err(|e| -> AdapterError { format!("write credentials: {e}").into() })
    }
}

/// 原子写：先写临时文件再同目录 rename 替换（POSIX/NTFS 均原子），
/// 避免崩溃窗口产生半写文件；写入失败时旧文件保持原样。
fn atomic_write(path: &Path, json: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json)?;
    fs::rename(&tmp, path)
}

/// 状态文件损坏时留存副本（`<name>.corrupt-<ts>`）便于审计，并返回错误而非静默清零
fn corrupt_backup(path: &Path) -> AdapterError {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let backup = path.with_extension(format!("corrupt-{ts}"));
    let copy = fs::copy(path, &backup).map_err(|e| format!("backup corrupt state: {e}"));
    tracing::error!(path = %path.display(), backup = %backup.display(), ?copy, "状态文件损坏，已备份并拒绝静默清零");
    format!("corrupt state file: {}", path.display()).into()
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct BotData {
    /// getUpdates 游标：重启续传，避免消息重放
    cursor: String,
    /// peer → 最近 context_token（可复用，约 1 小时有效）
    context_tokens: std::collections::HashMap<String, String>,
}

/// 共享状态句柄：poll 循环写入（游标 + 令牌，写后即落盘），send 循环读取令牌
pub struct WechatState {
    path: PathBuf,
    data: Mutex<BotData>,
    dirty: AtomicBool,
}

impl WechatState {
    /// 目录不存在则创建；状态文件缺失视为全新状态
    pub fn load(dir: &Path) -> Result<Arc<Self>, AdapterError> {
        fs::create_dir_all(dir).map_err(|e| format!("create state dir: {e}"))?;
        let path = dir.join("bot-state.json");
        let data = match fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text).map_err(|e| -> AdapterError {
                tracing::error!(path = %path.display(), error = %e, "状态文件解析失败");
                corrupt_backup(&path)
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BotData::default(),
            Err(e) => return Err(format!("read state {}: {e}", path.display()).into()),
        };
        Ok(Arc::new(Self {
            path,
            data: Mutex::new(data),
            dirty: AtomicBool::new(false),
        }))
    }

    pub fn cursor(&self) -> String {
        self.data.lock().cursor.clone()
    }

    pub fn context_token(&self, peer: &str) -> Option<String> {
        self.data.lock().context_tokens.get(peer).cloned()
    }

    /// 推进游标并记录本批会话令牌，随后立即落盘；内容未变则跳过写盘
    pub fn advance(&self, cursor: &str, tokens: &[(String, String)]) -> Result<(), AdapterError> {
        let changed = {
            let mut data = self.data.lock();
            let mut changed = data.cursor != cursor;
            data.cursor = cursor.to_owned();
            for (peer, token) in tokens {
                changed |= data.context_tokens.insert(peer.clone(), token.clone()).as_deref()
                    != Some(token.as_str());
            }
            changed
        };
        if changed {
            self.write()?;
        }
        Ok(())
    }

    fn write(&self) -> Result<(), AdapterError> {
        self.dirty.store(true, Ordering::SeqCst);
        let json =
            serde_json::to_vec_pretty(&*self.data.lock()).map_err(|e| format!("encode state: {e}"))?;
        atomic_write(&self.path, &json)
            .map_err(|e| -> AdapterError { format!("write state: {e}").into() })?;
        self.dirty.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl AdapterState for WechatState {
    /// 仅补写遗留脏数据（正常路径已即时落盘），幂等
    async fn flush(&self) -> Result<(), AdapterError> {
        if self.dirty.swap(false, Ordering::SeqCst) {
            self.write()
        } else {
            Ok(())
        }
    }
}

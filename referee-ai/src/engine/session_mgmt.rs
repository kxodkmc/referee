//! 会话生命周期管理 — 观测、枚举、删除与空闲回收
//!
//! 会话表为 `DashMap<SessionId, Session>`，本模块提供纯操作与后台回收任务：
//! 不参与回合执行、不依赖 provider。快照/枚举/删除均无跨 await 持锁。

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;

use crate::session::{Session, SessionId, SessionState};

/// 会话状态快照（观测用，不含内部句柄）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    Idle,
    Thinking,
    AwaitingCalls,
}

impl From<&SessionState> for SessionPhase {
    fn from(state: &SessionState) -> Self {
        match state {
            SessionState::Idle => SessionPhase::Idle,
            SessionState::Thinking { .. } => SessionPhase::Thinking,
            SessionState::AwaitingCalls { .. } => SessionPhase::AwaitingCalls,
        }
    }
}

/// 会话观测快照 — 供集成方查询单个会话的运行状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSnapshot {
    pub state: SessionPhase,
    pub history_len: usize,
    pub consumed_tokens: u64,
    pub peer_depth: u32,
}

/// 从会话表移除指定会话，返回是否确有会话被移除
pub(super) fn remove_session(sessions: &DashMap<SessionId, Session>, id: SessionId) -> bool {
    sessions.remove(&id).is_some()
}

/// 枚举全部会话 ID
pub(super) fn list_sessions(sessions: &DashMap<SessionId, Session>) -> Vec<SessionId> {
    sessions.iter().map(|entry| *entry.key()).collect()
}

/// 生成单个会话的快照
pub(super) fn snapshot(session: &Session) -> SessionSnapshot {
    SessionSnapshot {
        state: SessionPhase::from(&session.state),
        history_len: session.history_len(),
        consumed_tokens: session.consumed_tokens(),
        peer_depth: session.peer_depth(),
    }
}

/// 空闲回收句柄 — `stop` 后后台任务优雅退出
pub struct ReaperHandle {
    shutdown: oneshot::Sender<()>,
}

impl std::fmt::Debug for ReaperHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaperHandle").finish_non_exhaustive()
    }
}

impl ReaperHandle {
    /// 停止后台回收任务（幂等；任务在下个 tick 内退出）
    pub fn stop(self) {
        let _ = self.shutdown.send(());
    }
}

/// 启动空闲回收后台任务：周期扫描并移除「Idle 且空闲超时」的会话
///
/// 仅清理 Idle 会话（Thinking / AwaitingCalls 不动），扫描间隔 = `idle_timeout / 2`。
pub(super) fn start_idle_reaper(
    sessions: Arc<DashMap<SessionId, Session>>,
    idle_timeout: Duration,
) -> ReaperHandle {
    let (shutdown, mut stop_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(idle_timeout / 2);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => sweep_idle(&sessions, idle_timeout),
                _ = &mut stop_rx => break,
            }
        }
    });
    ReaperHandle { shutdown }
}

/// 单次扫描：移除所有空闲超时的 Idle 会话
fn sweep_idle(sessions: &DashMap<SessionId, Session>, idle_timeout: Duration) {
    let now = Instant::now();
    let stale: Vec<SessionId> = sessions
        .iter()
        .filter(|entry| {
            let session = entry.value();
            session.is_idle() && now.duration_since(session.last_active()) >= idle_timeout
        })
        .map(|entry| *entry.key())
        .collect();
    for id in stale {
        if sessions.remove(&id).is_some() {
            tracing::info!(session_id = %id, "reaped idle session");
        }
    }
}

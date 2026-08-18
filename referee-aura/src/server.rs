//! 后端装配与监听 — 供各 bin 复用（feature 正交）
//!
//! **职责边界**：把「构造 `InstanceManager` + 持久化 + 崩溃恢复 + 启动监听器」
//! 收敛为可复用单元，避免 daemon / TUI 前端各自复制装配逻辑。本模块不承载
//! 业务判定，也不决定进程生命周期（由调用方装配 shutdown 与回收句柄）。

use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::instance::{InstanceManager, InstanceManagerConfig};
use crate::persist::PersistStore;
use crate::protocol::ServerError;

/// 后端装配参数
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// 持久化目录
    pub state_dir: PathBuf,
    /// 最大实例数
    pub max_instances: usize,
    /// 每实例最大会话数
    pub max_sessions: usize,
    /// 系统级总预算（0 = 无限制）
    pub global_budget_limit: u64,
}

/// 装配完成的后端：管理器 + 可选持久化
pub struct Server {
    pub manager: InstanceManager,
    pub persist: Option<PersistStore>,
}

impl Server {
    /// 构造后端：初始化持久化（失败则降级为无持久化）、装配管理器、崩溃恢复。
    ///
    /// 不可恢复的实例/会话进入 broken 清单，记录日志但不阻塞启动。
    pub async fn build(cfg: ServerConfig) -> Self {
        let persist = match PersistStore::new(cfg.state_dir) {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!(error = %e, "persist init failed, running without persistence");
                None
            }
        };

        let manager = InstanceManager::new(InstanceManagerConfig {
            max_instances: cfg.max_instances,
            max_sessions_per_instance: cfg.max_sessions,
            global_budget_limit: cfg.global_budget_limit,
        });
        let manager = match &persist {
            Some(p) => manager.with_persist(p.clone()),
            None => manager,
        };

        if let Some(p) = &persist {
            let result = manager.recover(p).await;
            tracing::info!(
                recovered_instances = result.recovered_instances,
                recovered_sessions = result.recovered_sessions,
                broken = result.broken.len(),
                "crash recovery complete"
            );
            for b in &result.broken {
                tracing::warn!(path = %b.path, reason = %b.reason, "broken entry skipped");
            }
        }

        Self { manager, persist }
    }

    /// 启动 TCP 监听（feature `tcp`）。返回句柄，由调用方等待退出。
    #[cfg(feature = "tcp")]
    pub fn spawn_tcp(
        &self,
        bind: SocketAddr,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<Result<(), ServerError>> {
        let instances = self.manager.clone();
        let persist = self.persist.clone();
        tokio::spawn(async move { crate::transport::serve_tcp(bind, instances, persist, shutdown).await })
    }

    /// 启动 HTTP 监听（feature `http`）。返回句柄，由调用方等待退出。
    #[cfg(feature = "http")]
    pub fn spawn_http(
        &self,
        bind: SocketAddr,
        shutdown: watch::Receiver<bool>,
    ) -> JoinHandle<Result<(), ServerError>> {
        let instances = self.manager.clone();
        tokio::spawn(async move { crate::http::serve_http(bind, instances, shutdown).await })
    }
}
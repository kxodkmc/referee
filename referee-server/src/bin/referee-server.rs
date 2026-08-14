//! referee-server 常驻 daemon 入口
//!
//! 职责：参数解析 + 装配（manager / persist / transport）+ 生命周期（启动/优雅关闭）。
//! 不含业务逻辑。参数：
//! - `--state-dir <dir>` 持久化目录（默认 `~/.referee/state`）
//! - `--bind <addr>` TCP 监听地址（默认 `127.0.0.1:7100`）
//! - `--http-bind [<addr>]` HTTP 监听地址（feature `http`；缺省 `127.0.0.1:7101`，
//!   不传则禁用 HTTP）
//! - `--max-instances <N>` 最大实例数（默认 64）
//! - `--max-sessions <N>` 每实例最大会话数（默认 100）

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use referee_server::instance::{InstanceManager, InstanceManagerConfig};
use referee_server::persist::PersistStore;
use referee_server::protocol::{ServerError, ERR_INTERNAL};
use referee_server::transport::serve_tcp;
#[cfg(feature = "http")]
use referee_server::http::serve_http;

/// HTTP 监听缺省地址
const DEFAULT_HTTP_BIND: &str = "127.0.0.1:7101";

fn main() {
    // 参数解析（零依赖，手工扫描）
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut state_dir: Option<PathBuf> = None;
    let mut bind: Option<SocketAddr> = None;
    let mut http_bind: Option<SocketAddr> = None;
    let mut max_instances: usize = 64;
    let mut max_sessions: usize = 100;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--state-dir" => {
                i += 1;
                state_dir = args.get(i).map(PathBuf::from);
            }
            "--bind" => {
                i += 1;
                bind = args.get(i).and_then(|s| s.parse().ok());
            }
            "--http-bind" => {
                // 可选值：紧跟一个可解析地址则消费之，否则用缺省地址
                let next = args.get(i + 1).and_then(|s| s.parse::<SocketAddr>().ok());
                if next.is_some() {
                    i += 1;
                }
                http_bind = Some(next.unwrap_or_else(|| DEFAULT_HTTP_BIND.parse().expect("static addr")));
            }
            "--max-instances" => {
                i += 1;
                if let Some(n) = args.get(i).and_then(|s| s.parse().ok()) {
                    max_instances = n;
                }
            }
            "--max-sessions" => {
                i += 1;
                if let Some(n) = args.get(i).and_then(|s| s.parse().ok()) {
                    max_sessions = n;
                }
            }
            "--help" | "-h" => {
                print_usage(&args[0]);
                return;
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_usage(&args[0]);
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let state_dir = state_dir.unwrap_or_else(|| {
        dirs_home().join(".referee").join("state")
    });

    let bind = bind.unwrap_or_else(|| "127.0.0.1:7100".parse().expect("static addr"));

    // 初始化 tracing（本地 daemon 日志）
    tracing_subscriber_env();

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async_main(bind, http_bind, state_dir, max_instances, max_sessions));
}

#[cfg_attr(not(feature = "http"), allow(unused_variables))]
async fn async_main(
    bind: SocketAddr,
    http_bind: Option<SocketAddr>,
    state_dir: PathBuf,
    max_instances: usize,
    max_sessions: usize,
) {
    // 1. 持久化 + 恢复
    let persist = match PersistStore::new(state_dir.clone()) {
        Ok(p) => Some(p),
        Err(e) => {
            tracing::warn!(state_dir = %state_dir.display(), error = %e, "persist init failed, running without persistence");
            None
        }
    };

    let config = InstanceManagerConfig {
        max_instances,
        max_sessions_per_instance: max_sessions,
        global_budget_limit: 0,
    };
    let manager = InstanceManager::new(config);
    let manager = match &persist {
        Some(p) => manager.with_persist(p.clone()),
        None => manager,
    };

    // 2. 崩溃恢复（打印 recovered / broken，不阻塞启动）
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

    // 3. 优雅关闭信号
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_tx = Arc::new(shutdown_tx);
    spawn_shutdown_handler(shutdown_tx.clone());

    // 4. 并行启动监听器（同一 manager 实例与同一 shutdown 通道）
    let tcp_rx = shutdown_rx.clone();
    let tcp_instances = manager.clone();
    let tcp_task = tokio::spawn(async move {
        serve_tcp(bind, tcp_instances, persist, tcp_rx).await
    });

    // HTTP（P2，feature 门控）：--http-bind 指定则启用，否则不监听
    #[cfg(feature = "http")]
    let http_task = match http_bind {
        Some(addr) => {
            let http_rx = shutdown_rx.clone();
            let http_instances = manager.clone();
            Some(tokio::spawn(async move { serve_http(addr, http_instances, http_rx).await }))
        }
        None => None,
    };

    // 5. 等待监听器退出（shutdown 触发优雅关闭）
    let tcp_result = match tcp_task.await {
        Ok(r) => r,
        Err(e) => Err(ServerError::new(
            ERR_INTERNAL,
            format!("tcp task join: {e}"),
        )),
    };

    #[cfg(feature = "http")]
    if let Some(task) = http_task {
        let _ = task.await;
    }

    if let Err(e) = tcp_result {
        tracing::error!(error = %e, "server exited with error");
        std::process::exit(1);
    }
    tracing::info!("server shut down gracefully");
}

/// 信号处理：SIGINT / SIGTERM → 触发优雅关闭
fn spawn_shutdown_handler(shutdown_tx: Arc<tokio::sync::watch::Sender<bool>>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install sigterm handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        let _ = shutdown_tx.send(true);
    });
}

/// 用户主目录（零依赖）
fn dirs_home() -> PathBuf {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

/// 初始化 tracing 订阅（尽力而为，失败不影响启动）
fn tracing_subscriber_env() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();
}

fn print_usage(prog: &str) {
    eprintln!(
        "USAGE: {prog} [--state-dir <dir>] [--bind <addr>] [--http-bind [<addr>]] [--max-instances <N>] [--max-sessions <N>]"
    );
}
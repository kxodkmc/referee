//! referee-aura 常驻 daemon 入口
//!
//! 职责：参数解析 + 装配（`server::Server`）+ 生命周期（启动/优雅关闭）。
//! 只启动后端监听器，不承载任何前端；前端（TUI / Web）经 TCP/HTTP 连接。
//! 参数：
//! - `--state-dir <dir>` 持久化目录（默认 `~/.referee/state`）
//! - `--bind <addr>` TCP 监听地址（默认 `127.0.0.1:7100`）
//! - `--http-bind [<addr>]` HTTP 监听地址（feature `http`；缺省 `127.0.0.1:7101`，
//!   不传则禁用 HTTP）
//! - `--max-instances <N>` 最大实例数（默认 64）
//! - `--max-sessions <N>` 每实例最大会话数（默认 100）

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use referee_aura::server::{Server, ServerConfig};

/// HTTP 监听缺省地址
const DEFAULT_HTTP_BIND: &str = "127.0.0.1:7101";

/// 已解析的 daemon 参数
struct Args {
    state_dir: PathBuf,
    bind: SocketAddr,
    http_bind: Option<SocketAddr>,
    max_instances: usize,
    max_sessions: usize,
}

fn main() {
    let args = parse_args();
    tracing_subscriber_env();

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async_main(args));
}

async fn async_main(args: Args) {
    // 1. 装配后端（manager + persist + 崩溃恢复）
    let server = Server::build(ServerConfig {
        state_dir: args.state_dir,
        max_instances: args.max_instances,
        max_sessions: args.max_sessions,
        global_budget_limit: 0,
    })
    .await;

    // 2. 优雅关闭信号
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let shutdown_tx = Arc::new(shutdown_tx);
    spawn_shutdown_handler(shutdown_tx.clone());

    // 3. 并行启动监听器（同一 server 与同一 shutdown 通道）
    let tcp_task = server.spawn_tcp(args.bind, shutdown_rx.clone());

    // HTTP（P2，feature 门控）：--http-bind 指定则启用
    #[cfg(feature = "http")]
    let http_task = args.http_bind.map(|addr| server.spawn_http(addr, shutdown_rx));

    // 4. 等待监听器退出（shutdown 触发优雅关闭）
    let tcp_result = match tcp_task.await {
        Ok(r) => r,
        Err(e) => Err(referee_aura::protocol::ServerError::new(
            referee_aura::protocol::ERR_INTERNAL,
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

/// 参数解析（零依赖，手工扫描）
fn parse_args() -> Args {
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
                // 可选值：紧跟可解析地址则消费之，否则用缺省地址
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
                print_usage();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_usage();
                std::process::exit(2);
            }
        }
        i += 1;
    }

    Args {
        state_dir: state_dir.unwrap_or_else(|| dirs_home().join(".referee").join("state")),
        bind: bind.unwrap_or_else(|| "127.0.0.1:7100".parse().expect("static addr")),
        http_bind,
        max_instances,
        max_sessions,
    }
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

fn print_usage() {
    eprintln!(
        "USAGE: referee-aura [--state-dir <dir>] [--bind <addr>] [--http-bind [<addr>]] [--max-instances <N>] [--max-sessions <N>]"
    );
}
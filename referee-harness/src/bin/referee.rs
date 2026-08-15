//! referee — 一条命令启动后端 + TUI（feature `tui`）
//!
//! 职责：在当前目录启动后端监听器，并打开 TUI 前端连接该后端；自动把
//! **当前目录**设为 TUI 创建实例的「工作区根」。
//!
//! 前后端分离：TUI 经 TCP 连接本进程内启动的后端（`server::Server`），
//! 与 `referee-harness` 共用同一 JSON-RPC 协议；TUI 退出即优雅关闭后端。
//! 纯常驻后端（供 Web 等其它 UI 连接）仍由 `referee-harness` 提供。
//!
//! 参数（与 referee-harness 一致，另加 `--no-root`）：
//! - `--state-dir <dir>` / `--bind <addr>` / `--http-bind [<addr>]`
//! - `--max-instances <N>` / `--max-sessions <N>`
//! - `--no-root` 不自动注入当前目录为工作区根

use std::net::SocketAddr;
use std::path::PathBuf;

use referee_harness::server::{Server, ServerConfig};

/// HTTP 监听缺省地址
const DEFAULT_HTTP_BIND: &str = "127.0.0.1:7101";

/// 已解析参数
struct Args {
    state_dir: PathBuf,
    bind: SocketAddr,
    http_bind: Option<SocketAddr>,
    max_instances: usize,
    max_sessions: usize,
    /// 是否注入当前目录为默认工作区根
    use_root: bool,
}

fn main() -> std::io::Result<()> {
    let args = parse_args();
    tracing_subscriber_env();

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async_main(args))
}

async fn async_main(args: Args) -> std::io::Result<()> {
    // 1. 装配后端（manager + persist + 崩溃恢复）
    let server = Server::build(ServerConfig {
        state_dir: args.state_dir,
        max_instances: args.max_instances,
        max_sessions: args.max_sessions,
        global_budget_limit: 0,
    })
    .await;

    // 2. 后端生命周期：TUI 退出 → shutdown → 后端优雅关闭
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // 3. 启动监听器（同一 server 与同一 shutdown 通道）
    let tcp_task = server.spawn_tcp(args.bind, shutdown_rx.clone());

    #[cfg(feature = "http")]
    let http_task = args.http_bind.map(|addr| server.spawn_http(addr, shutdown_rx));

    // 4. 前台打开 TUI（连接本进程内后端），默认根 = 当前目录
    let default_root = if args.use_root {
        std::env::current_dir()
            .ok()
            .map(|p| p.to_string_lossy().into_owned())
    } else {
        None
    };
    let tui_result = referee_harness::tui::run(args.bind, default_root).await;

    // 5. TUI 退出 → 触发后端优雅关闭
    let _ = shutdown_tx.send(true);
    let _ = tcp_task.await;
    #[cfg(feature = "http")]
    if let Some(task) = http_task {
        let _ = task.await;
    }

    tui_result
}

/// 参数解析（零依赖，手工扫描）
fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut state_dir: Option<PathBuf> = None;
    let mut bind: Option<SocketAddr> = None;
    let mut http_bind: Option<SocketAddr> = None;
    let mut max_instances: usize = 64;
    let mut max_sessions: usize = 100;
    let mut use_root = true;

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
            "--no-root" => use_root = false,
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
        use_root,
    }
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
        "USAGE: referee [--state-dir <dir>] [--bind <addr>] [--http-bind [<addr>]] [--max-instances <N>] [--max-sessions <N>] [--no-root]"
    );
}
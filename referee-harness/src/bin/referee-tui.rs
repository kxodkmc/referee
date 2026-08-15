//! referee-tui — 官方 TUI 客户端（feature `tui`）
//!
//! 连接常驻 daemon（TCP JSON-RPC 2.0），提供实例管理与流式对话界面。
//!
//! 用法：
//! ```text
//! referee-tui [--daemon <addr>]   # 默认 127.0.0.1:7100
//! ```

use std::net::SocketAddr;

fn main() -> std::io::Result<()> {
    let daemon = parse_daemon_addr();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(std::io::Error::other)?;
    rt.block_on(referee_harness::tui::run(daemon))
}

/// 解析 `--daemon <addr>`，缺省 `127.0.0.1:7100`。
fn parse_daemon_addr() -> SocketAddr {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--daemon" => {
                i += 1;
                if let Some(s) = args.get(i) {
                    if let Ok(a) = s.parse() {
                        return a;
                    }
                }
            }
            "--help" | "-h" => {
                eprintln!("USAGE: referee-tui [--daemon <addr>]  (默认 127.0.0.1:7100)");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                eprintln!("USAGE: referee-tui [--daemon <addr>]  (默认 127.0.0.1:7100)");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    "127.0.0.1:7100".parse().expect("static addr")
}

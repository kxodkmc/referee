//! stdio 子进程传输层 — MCP 服务器进程的有界读写通道
//!
//! ## 职责
//! - 启动 / 管理 MCP 服务器子进程（`tokio::process`，白名单内零新增依赖）
//! - 启动期存活检测（延迟探测，识别「一启动即退出」的坏进程）
//! - 请求/响应并发分发：按 `id` 关联 oneshot 通道，支持多 in-flight 请求
//! - 有界读取：单行长度上限 + 待处理请求数上限，防 OOM（背压硬约束）
//! - 取消：写 `notifications/cancelled` 通知
//! - 停机：关闭 stdin（优雅停机信号）+ 超时强杀 + 后台 reader 回收
//!
//! ## 信任边界
//! 服务器输出视为不可信输入：单行超限或解析失败即熔断该连接，不落入业务层。

use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};

use crate::tool::mcp::McpError;

/// 待处理请求通道（oneshot 消费式，杜绝重复送达）
type PendingSender = oneshot::Sender<Result<Value, McpError>>;

/// 传输层配置（有界硬约束）
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// 最大 in-flight 请求数（防无限挂起）
    pub max_pending: usize,
    /// 单行 stdout 最大长度（字节，防无界内存）
    pub max_line_len: usize,
    /// 单个请求超时
    pub request_timeout: Duration,
    /// 启动期存活探测延迟（毫秒）
    pub startup_probe_ms: u64,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_pending: 64,
            max_line_len: 128 * 1024,
            request_timeout: Duration::from_secs(30),
            startup_probe_ms: 500,
        }
    }
}

/// stdio 传输通道 — 一个 MCP 服务器进程
///
/// `Clone` 共享同一进程与待处理表；内部 reader / stderr 任务各起一个。
pub struct StdioTransport {
    config: TransportConfig,
    child: Mutex<Option<Child>>,
    stdin: Mutex<tokio::process::ChildStdin>,
    pending: Arc<DashMap<u64, PendingSender>>,
    next_id: AtomicU64,
    /// 连接是否已关闭（reader 任务在 EOF/错误时置位）
    closed: Arc<AtomicBool>,
    reader: JoinHandle<()>,
    stderr_reader: JoinHandle<()>,
}

impl std::fmt::Debug for StdioTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdioTransport")
            .field("max_pending", &self.config.max_pending)
            .field("max_line_len", &self.config.max_line_len)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .field("in_flight", &self.pending.len())
            .finish()
    }
}

impl StdioTransport {
    /// 启动 MCP 服务器子进程
    pub async fn spawn(
        command: &str,
        args: &[String],
        envs: &[(String, String)],
        config: TransportConfig,
    ) -> Result<Self, McpError> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .envs(envs.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| McpError::Io(e.to_string()))?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::Protocol("server has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::Protocol("server has no stdout".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| McpError::Protocol("server has no stderr".into()))?;

        let pending: Arc<DashMap<u64, PendingSender>> = Arc::new(DashMap::new());
        let closed = Arc::new(AtomicBool::new(false));

        // 后台 reader：解析 stdout 行并分发到对应 pending
        let pending_rd = pending.clone();
        let closed_rd = closed.clone();
        let max_line = config.max_line_len;
        let reader = tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut lines = BufReader::new(stdout);
            loop {
                buffer.clear();
                match lines.read_until(b'\n', &mut buffer).await {
                    Ok(0) => break, // EOF：服务器优雅退出
                    Ok(_) => {
                        if buffer.len() > max_line {
                            fail_all(&pending_rd, McpError::LineTooLong(buffer.len()));
                            break;
                        }
                        match parse_response(&buffer) {
                            Some((id, result)) => deliver(&pending_rd, id, result),
                            None => warn!(line = %String::from_utf8_lossy(&buffer).trim(), "mcp server returned malformed line"),
                        }
                    }
                    Err(e) => {
                        fail_all(&pending_rd, McpError::Io(e.to_string()));
                        break;
                    }
                }
            }
            fail_all(&pending_rd, McpError::Closed("server stdout closed".into()));
            closed_rd.store(true, Ordering::Relaxed);
        });

        // 后台 stderr reader：日志转发（UTF-8 自由格式），并防 stdout 阻塞
        let stderr_reader = tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut lines = BufReader::new(stderr);
            loop {
                buffer.clear();
                match lines.read_until(b'\n', &mut buffer).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let line = String::from_utf8_lossy(&buffer);
                        warn!(line = %line.trim(), "mcp server stderr");
                    }
                    Err(_) => break,
                }
            }
        });

        let transport = Self {
            config,
            child: Mutex::new(Some(child)),
            stdin: Mutex::new(stdin),
            pending,
            next_id: AtomicU64::new(1),
            closed,
            reader,
            stderr_reader,
        };

        // 启动期存活探测：识别「一启动即退出」的坏进程
        transport.probe_startup().await?;
        Ok(transport)
    }

    /// 是否仍存活（reader 尚未感知 EOF）
    fn is_alive(&self) -> bool {
        !self.closed.load(Ordering::Relaxed)
    }

    /// 当前 in-flight 请求数
    pub fn in_flight(&self) -> usize {
        self.pending.len()
    }

    /// 发送请求并等待响应（按 id 关联，支持并发）
    #[instrument(skip(self, params), fields(method = method))]
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, McpError> {
        if !self.is_alive() {
            return Err(McpError::Closed("server not alive".into()));
        }
        if self.pending.len() >= self.config.max_pending {
            return Err(McpError::PendingLimit(self.config.max_pending));
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);

        let mut line = serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params
        }))
        .map_err(|e| McpError::Protocol(e.to_string()))?;
        line.push(b'\n');

        let write = {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(&line).await
        };
        if let Err(e) = write {
            self.pending.remove(&id);
            return Err(McpError::Io(e.to_string()));
        }
        debug!(id, method, "mcp request sent");

        match tokio::time::timeout(self.config.request_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending.remove(&id);
                Err(McpError::Closed("response channel closed".into()))
            }
            Err(_) => {
                self.pending.remove(&id);
                let _ = self.notify_cancelled(id, "request timeout").await;
                Err(McpError::Timeout)
            }
        }
    }

    /// 发送通知（无 id、无响应）
    pub async fn notify(&self, method: &str, params: Value) -> Result<(), McpError> {
        let mut line = serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "method": method, "params": params
        }))
        .map_err(|e| McpError::Protocol(e.to_string()))?;
        line.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(&line).await.map_err(|e| McpError::Io(e.to_string()))
    }

    /// 通知取消指定请求（`notifications/cancelled`）
    pub async fn notify_cancelled(&self, request_id: u64, reason: &str) -> Result<(), McpError> {
        self.notify(
            "notifications/cancelled",
            json!({"requestId": request_id, "reason": reason}),
        )
        .await
    }

    /// 优雅停机：关闭 stdin（停机信号）+ 超时等待 + 强杀 + 回收后台任务
    pub async fn shutdown(&self) {
        {
            let mut stdin = self.stdin.lock().await;
            let _ = stdin.shutdown().await;
        }
        let child = self.child.lock().await.take();
        if let Some(mut child) = child {
            let waited = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
            if waited.is_err() {
                let _ = child.kill().await;
                let _ = child.wait().await;
            }
        }
        self.reader.abort();
        self.stderr_reader.abort();
    }

    /// 启动期存活探测：进程若在探测窗口内即退出，视为坏进程并报错
    async fn probe_startup(&self) -> Result<(), McpError> {
        tokio::time::sleep(Duration::from_millis(self.config.startup_probe_ms)).await;
        let mut child = self.child.lock().await;
        if let Some(child) = child.as_mut() {
            if let Ok(Some(status)) = child.try_wait() {
                return Err(McpError::Closed(format!(
                    "server exited during startup: {status}"
                )));
            }
        }
        Ok(())
    }
}

/// 解析一行 stdout 为 (id, 结果)。失败返回 `None`（不可信输入→熔断由调用方决定，
/// 此处仅拒收该行）。
fn parse_response(line: &[u8]) -> Option<(u64, Result<Value, McpError>)> {
    let value: Value = serde_json::from_slice(line).ok()?;
    let id = value.get("id")?.as_u64()?;
    if let Some(err) = value.get("error") {
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
        let message = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error")
            .to_string();
        let data = err.get("data").cloned();
        Some((id, Err(McpError::Rpc { code, message, data })))
    } else {
        Some((id, Ok(value.get("result").cloned().unwrap_or(Value::Null))))
    }
}

/// 按 id 分发结果到待处理通道
fn deliver(pending: &DashMap<u64, PendingSender>, id: u64, result: Result<Value, McpError>) {
    if let Some((_, tx)) = pending.remove(&id) {
        let _ = tx.send(result);
    }
}

/// 连接级失败：将所有待处理请求以同一错误收尾（一旦连接不可用，全部请求失败）
fn fail_all(pending: &DashMap<u64, PendingSender>, err: McpError) {
    let ids: Vec<u64> = pending.iter().map(|e| *e.key()).collect();
    for id in ids {
        if let Some((_, tx)) = pending.remove(&id) {
            let _ = tx.send(Err(err.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_response_ok() {
        let line = b"{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{\"ok\":true}}\n";
        let (id, res) = parse_response(line).unwrap();
        assert_eq!(id, 3);
        assert_eq!(res.unwrap()["ok"], true);
    }

    #[test]
    fn parse_response_error_carries_data() {
        let line = br#"{"jsonrpc":"2.0","id":7,"error":{"code":-32002,"message":"unsupported","data":{"supported":["2026-07-28"]}}}"#;
        let (id, res) = parse_response(line).unwrap();
        assert_eq!(id, 7);
        match res {
            Err(McpError::Rpc { code, data, .. }) => {
                assert_eq!(code, -32002);
                assert_eq!(data.unwrap()["supported"][0], "2026-07-28");
            }
            _ => panic!("expected rpc error"),
        }
    }

    #[test]
    fn parse_response_malformed_is_none() {
        assert!(parse_response(b"not json\n").is_none());
        assert!(parse_response(b"{\"jsonrpc\":\"2.0\"}\n").is_none()); // 缺 id
    }

    #[test]
    fn fail_all_clears_pending() {
        let pending: Arc<DashMap<u64, PendingSender>> = Arc::new(DashMap::new());
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        pending.insert(1, tx1);
        pending.insert(2, tx2);
        fail_all(&pending, McpError::PendingLimit(10));
        assert!(pending.is_empty());
    }
}
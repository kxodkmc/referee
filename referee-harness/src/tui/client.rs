//! JSON-RPC 2.0 over NDJSON 客户端（TUI 专用）
//!
//! 职责边界：只做网络 IO 与帧编解码，业务判定全部在 daemon。请求用
//! `serde_json::Value` 构造、响应按需反序列化为 [`crate::protocol`] 类型，
//! 与 daemon 侧 [`crate::transport`] 服务器严格对齐（NDJSON 逐行、同 id 流式多帧）。

use std::net::SocketAddr;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

use crate::protocol::StreamFrame;

/// RPC 客户端错误
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(String),
    #[error("server error [{code}]: {message}")]
    Server { code: i32, message: String },
    #[error("connection closed")]
    Closed,
}

/// 管理类 RPC 客户端（list / create / get / remove / sessions / interrupt）
///
/// 持有一条常驻连接；服务端逐行串行处理，一次 `call` 对应单帧响应。
pub struct RpcClient {
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    next_id: u64,
}

impl RpcClient {
    /// 连接 daemon。
    pub async fn connect(addr: SocketAddr) -> Result<Self, RpcError> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            writer: BufWriter::new(writer),
            reader: BufReader::new(reader),
            next_id: 1,
        })
    }

    /// 发起请求并等待单帧响应，返回 `result` 值。
    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id;
        self.next_id += 1;
        let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        let line = serde_json::to_string(&req).map_err(|e| RpcError::Parse(e.to_string()))?;
        self.writer.write_all(line.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;

        loop {
            let mut buf = String::new();
            if self.reader.read_line(&mut buf).await? == 0 {
                return Err(RpcError::Closed);
            }
            let v: Value =
                serde_json::from_str(buf.trim_end()).map_err(|e| RpcError::Parse(e.to_string()))?;
            // 防御：忽略其它请求的帧（正常不会出现）
            if v.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(err) = v.get("error") {
                return Err(RpcError::Server {
                    code: err.get("code").and_then(Value::as_i64).unwrap_or(-1) as i32,
                    message: err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                });
            }
            return v
                .get("result")
                .cloned()
                .ok_or_else(|| RpcError::Parse("missing result".into()));
        }
    }
}

/// 流式聊天事件（后台任务经 mpsc 推给 UI）
#[derive(Debug)]
pub enum ChatEvent {
    /// 增量文本；`reasoning` 为思考内容（reasoning_content，可空）
    Delta {
        content: String,
        reasoning: String,
    },
    Finish { reason: String },
    Error { message: String },
}

/// 发起流式 chat：独立连接 + 后台读取任务，事件经 mpsc 推给 UI。
///
/// `params` 需含 `id` / `session_id` / `message` / `stream: true`。
/// 流结束（finish/error）或对端断开时关闭通道。
pub async fn open_chat_stream(
    addr: SocketAddr,
    params: Value,
) -> Result<tokio::sync::mpsc::Receiver<ChatEvent>, RpcError> {
    let stream = TcpStream::connect(addr).await?;
    let (reader, mut writer) = stream.into_split();
    let req = json!({ "jsonrpc": "2.0", "id": 1, "method": "instance.chat", "params": params });
    let line = serde_json::to_string(&req).map_err(|e| RpcError::Parse(e.to_string()))?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    drop(writer); // 只读响应流

    let (tx, rx) = tokio::sync::mpsc::channel(64);
    tokio::spawn(async move {
        let mut reader = BufReader::new(reader);
        let mut buf = String::new();
        loop {
            buf.clear();
            match reader.read_line(&mut buf).await {
                Ok(0) | Err(_) => break, // EOF / 断开
                Ok(_) => {}
            }
            let v: Value = match serde_json::from_str(buf.trim_end()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Some(err) = v.get("error") {
                let _ = tx
                    .send(ChatEvent::Error {
                        message: err
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("server error")
                            .to_string(),
                    })
                    .await;
                break;
            }
            let result = match v.get("result") {
                Some(r) => r,
                None => continue,
            };
            let frame: StreamFrame = match serde_json::from_value(result.clone()) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let done = match frame {
                StreamFrame::Delta {
                    content,
                    reasoning_content,
                } => {
                    let _ = tx
                        .send(ChatEvent::Delta {
                            content: content.unwrap_or_default(),
                            reasoning: reasoning_content.unwrap_or_default(),
                        })
                        .await;
                    false
                }
                StreamFrame::Finish { finish_reason, .. } => {
                    let _ = tx.send(ChatEvent::Finish { reason: finish_reason }).await;
                    true
                }
                StreamFrame::Error { message } => {
                    let _ = tx.send(ChatEvent::Error { message }).await;
                    true
                }
            };
            if done {
                break;
            }
        }
    });
    Ok(rx)
}

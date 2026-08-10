//! 测试共享：极简 HTTP + SSE mock 服务器
//!
//! 仅用 `tokio::net::TcpListener` + 手写 HTTP 解析，零新依赖。
//! 支持：
//! - JSON 响应（成功 / 错误）
//! - SSE 流式响应（多 chunk + `[DONE]`）
//! - Raw 响应（自定义 headers，如 `Retry-After`）
//! - 请求体 JSON 记录（用于断言调用方发送的 body）
//!
//! 本模块为跨测试二进制共享的测试工具集，不同测试文件使用其 API 的不同子集，
//! 且后续 Phase（P1 超时测试等）会用到当前未引用的工具项（如 `Hang`）。
//! 故整体允许 dead_code，避免每个测试二进制各自的子集差异触发告警。

#![allow(dead_code)]

use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// 已接收的 HTTP 请求
pub struct MockRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl MockRequest {
    /// 解析 body 为 JSON（非 JSON 时返回 None）
    pub fn body_json(&self) -> Option<Value> {
        serde_json::from_slice(&self.body).ok()
    }

    /// 是否为流式请求（body 中 `stream: true`）
    pub fn is_stream(&self) -> bool {
        self.body_json()
            .and_then(|j| j.get("stream").and_then(|s| s.as_bool()))
            .unwrap_or(false)
    }
}

/// mock 响应类型
pub enum MockResponse {
    /// JSON 一次性响应
    Json { status: u16, body: Value },
    /// SSE 流式响应：逐个写入 chunk，可选 `[DONE]`
    Sse {
        status: u16,
        chunks: Vec<Value>,
        with_done: bool,
    },
    /// 自定义 headers / body（用于 Retry-After 等错误响应）
    Raw {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    },
    /// 永不响应（超时测试）
    Hang,
}

pub struct MockServer {
    pub base_url: String,
    requests: Arc<Mutex<Vec<Value>>>,
}

impl MockServer {
    /// 启动 mock 服务器；handler 决定每个请求的响应
    pub async fn start<F>(handler: F) -> Self
    where
        F: Fn(MockRequest) -> MockResponse + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{}", addr);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handler = Arc::new(handler);
        let requests_clone = requests.clone();

        tokio::spawn(async move {
            loop {
                let (mut socket, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let handler = handler.clone();
                let requests = requests_clone.clone();
                tokio::spawn(async move {
                    handle_connection(&mut socket, handler, requests).await;
                });
            }
        });

        Self { base_url, requests }
    }

    /// 已接收的请求数（按 JSON body 记录）
    pub fn request_count(&self) -> usize {
        self.requests.lock().len()
    }

    /// 取出全部已记录请求 JSON（用于断言 body 字段）
    pub fn requests(&self) -> Vec<Value> {
        self.requests.lock().clone()
    }
}

async fn handle_connection<F>(
    socket: &mut tokio::net::TcpStream,
    handler: Arc<F>,
    requests: Arc<Mutex<Vec<Value>>>,
) where
    F: Fn(MockRequest) -> MockResponse + Send + Sync + 'static,
{
    // 读取请求行 + headers（直到 \r\n\r\n）
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    loop {
        match socket.read(&mut tmp).await {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => return,
        }
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 1 << 20 {
            return; // 防御性上限：1MB headers
        }
    }

    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let header_str = match std::str::from_utf8(&buf[..header_end]) {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut lines = header_str.lines();
    let request_line = match lines.next() {
        Some(l) => l,
        None => return,
    };
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(": ") {
            headers.push((k.to_string().to_lowercase(), v.to_string()));
        }
    }

    // 读取 body（可能已部分在 buf 中）
    let content_length: usize = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let body_start = header_end + 4;
    let mut body = if buf.len() > body_start {
        buf[body_start..].to_vec()
    } else {
        Vec::new()
    };
    while body.len() < content_length {
        match socket.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&tmp[..n]),
            Err(_) => return,
        }
    }
    body.truncate(content_length);

    let req = MockRequest {
        method,
        path,
        headers,
        body: body.clone(),
    };

    // 记录 JSON body（用于断言）
    if let Ok(json) = serde_json::from_slice::<Value>(&body) {
        requests.lock().push(json);
    }

    let response = handler(req);

    match response {
        MockResponse::Json { status, body } => {
            let body_str = serde_json::to_string(&body).unwrap();
            let resp = format!(
                "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                reason_phrase(status),
                body_str.len(),
                body_str,
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        }
        MockResponse::Sse {
            status,
            chunks,
            with_done,
        } => {
            let header = format!(
                "HTTP/1.1 {} {}\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
                status,
                reason_phrase(status),
            );
            let _ = socket.write_all(header.as_bytes()).await;
            for chunk in chunks {
                let chunk_str = serde_json::to_string(&chunk).unwrap();
                let _ = socket
                    .write_all(format!("data: {}\n\n", chunk_str).as_bytes())
                    .await;
                let _ = socket.flush().await;
            }
            if with_done {
                let _ = socket.write_all(b"data: [DONE]\n\n").await;
            }
        }
        MockResponse::Raw {
            status,
            headers,
            body,
        } => {
            let mut resp = format!("HTTP/1.1 {} {}\r\n", status, reason_phrase(status));
            for (k, v) in headers {
                resp.push_str(&format!("{}: {}\r\n", k, v));
            }
            resp.push_str(&format!(
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            ));
            let _ = socket.write_all(resp.as_bytes()).await;
            let _ = socket.write_all(&body).await;
        }
        MockResponse::Hang => {
            // 永不响应，等待调用方超时
            std::future::pending::<()>().await;
        }
    }
    let _ = socket.flush().await;
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        408 => "Request Timeout",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

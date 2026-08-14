//! TCP JSON-RPC 2.0 传输（feature `tcp`）
//!
//! **职责边界**：只做网络 IO 与 JSON-RPC 帧映射（NDJSON 逐行编解码 + 连接管理 +
//! 请求分发），业务判定全部委托 [`InstanceManager`]。可被未来 HTTP 传输复用
//! `dispatch`（transport-agnostic 核心）。
//!
//! 连接级背压治理：
//! - `max_concurrent_requests = 16`：每连接信号量，串行化并发请求
//! - `max_request_len = 1 MiB`：单行超限关闭连接（防无界内存）
//! - 流式 chat 写端持续输出 Delta/Finish 帧（同 id 多 result），直到 finish/断开

use std::net::SocketAddr;
use std::sync::Arc;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};

use referee_ai_base::engine::EngineReply;
use referee_ai_base::session::SessionId;

use crate::chat;
use crate::instance::{err as ERR, InstanceManager};
use crate::persist::PersistStore;
use crate::protocol::{InstanceId, InstanceSpec, ServerError};

/// 每连接最大并发请求（背压硬约束）
const MAX_CONCURRENT_REQUESTS: usize = 16;
/// 单行最大字节数（超限关闭连接）
const MAX_REQUEST_LEN: usize = 1_048_576;

/// JSON-RPC 2.0 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// JSON-RPC 2.0 响应
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

fn ok(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: Some(result),
        error: None,
    }
}

fn error(id: Value, code: i32, message: String) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(JsonRpcError { code, message }),
    }
}

/// TCP JSON-RPC 2.0 服务器（常驻 daemon）
///
/// `shutdown` 收到变更即优雅退出监听循环（已建立的连接由各自任务自然结束）。
pub async fn serve_tcp(
    bind_addr: SocketAddr,
    instances: InstanceManager,
    _persist: Option<PersistStore>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|e| ServerError::new(ERR::ERR_INTERNAL, format!("bind {bind_addr}: {e}")))?;
    tracing::info!(addr = %bind_addr, "referee-server listening on tcp");
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                tracing::info!("shutdown signal received, stopping listener");
                break;
            }
            result = listener.accept() => {
                let (stream, addr) = result.map_err(|e| ServerError::new(ERR::ERR_INTERNAL, format!("accept: {e}")))?;
                let inst = instances.clone();
                tokio::spawn(async move {
                    handle_connection(stream, addr, inst).await;
                });
            }
        }
    }
    Ok(())
}

/// 单连接：逐行读 NDJSON → 信号量限流 → dispatch → 逐行写响应（流式多帧同 id）
async fn handle_connection(stream: TcpStream, addr: SocketAddr, instances: InstanceManager) {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader).lines();
    let mut writer = BufWriter::new(writer);
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));

    while let Ok(Some(line)) = reader.next_line().await {
        if line.len() > MAX_REQUEST_LEN {
            tracing::warn!(addr = %addr, bytes = line.len(), "request too large, closing connection");
            break;
        }
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => break,
        };
        let req: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = error(Value::Null, -32700, format!("parse error: {e}"));
                if write_frame(&mut writer, &resp).await.is_err() {
                    break;
                }
                continue;
            }
        };
        let responses = dispatch(&instances, &req).await;
        drop(permit);
        for resp in responses {
            if write_frame(&mut writer, &resp).await.is_err() {
                // 对端断开：停止整条连接
                return;
            }
        }
        if writer.flush().await.is_err() {
            return;
        }
    }
}

async fn write_frame(writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>, resp: &JsonRpcResponse) -> std::io::Result<()> {
    let data = serde_json::to_vec(resp).map_err(std::io::Error::other)?;
    writer.write_all(&data).await?;
    writer.write_all(b"\n").await
}

/// 请求分发 — transport-agnostic 核心（可被 HTTP 传输复用）
///
/// 返回 `Vec<JsonRpcResponse>`：常规方法单响应；流式 chat 返回 Delta/Finish 多帧（同 id）。
pub async fn dispatch(
    instances: &InstanceManager,
    req: &JsonRpcRequest,
) -> Vec<JsonRpcResponse> {
    let id = req.id.clone();
    match req.method.as_str() {
        "instance.create" => {
            let spec: InstanceSpec = match serde_json::from_value(req.params.clone()) {
                Ok(s) => s,
                Err(e) => return vec![error(id, ERR::ERR_INVALID_SPEC, format!("invalid spec: {e}"))],
            };
            match instances.create(spec) {
                Ok(iid) => vec![ok(id, json!({ "id": iid.as_str() }))],
                Err(e) => vec![error(id, e.code, e.message)],
            }
        }
        "instance.list" => {
            let infos = instances.list().await;
            vec![ok(id, json!(infos))]
        }
        "instance.get" => {
            let iid = match param_id(&req.params) {
                Ok(i) => i,
                Err(resp) => return vec![resp],
            };
            match instances.get(&iid) {
                Ok(inst) => vec![ok(id, json!(inst.snapshot().await))],
                Err(e) => vec![error(id, e.code, e.message)],
            }
        }
        "instance.remove" => {
            let iid = match param_id(&req.params) {
                Ok(i) => i,
                Err(resp) => return vec![resp],
            };
            match instances.remove(&iid).await {
                Ok(()) => vec![ok(id, json!({}))],
                Err(e) => vec![error(id, e.code, e.message)],
            }
        }
        "instance.chat" => dispatch_chat(instances, req).await,
        "instance.interrupt" => {
            let iid = match param_id(&req.params) {
                Ok(i) => i,
                Err(resp) => return vec![resp],
            };
            let sid = match param_session(&req.params) {
                Ok(s) => s,
                Err(resp) => return vec![resp],
            };
            match instances.get(&iid) {
                Ok(inst) => {
                    let cancelled = inst.interrupt(sid);
                    vec![ok(id, json!({ "cancelled": cancelled }))]
                }
                Err(e) => vec![error(id, e.code, e.message)],
            }
        }
        "instance.sessions" => {
            let iid = match param_id(&req.params) {
                Ok(i) => i,
                Err(resp) => return vec![resp],
            };
            match instances.get(&iid) {
                Ok(inst) => vec![ok(id, json!(inst.session_infos()))],
                Err(e) => vec![error(id, e.code, e.message)],
            }
        }
        other => vec![error(id, -32601, format!("method not found: {other}"))],
    }
}

/// 流式/非流式 chat 分发
async fn dispatch_chat(instances: &InstanceManager, req: &JsonRpcRequest) -> Vec<JsonRpcResponse> {
    let id = req.id.clone();
    let iid = match param_id(&req.params) {
        Ok(i) => i,
        Err(resp) => return vec![resp],
    };
    let session_id = match param_session(&req.params) {
        Ok(s) => s,
        Err(resp) => return vec![resp],
    };
    let instance = match instances.get(&iid) {
        Ok(i) => i,
        Err(e) => return vec![error(id, e.code, e.message)],
    };

    let message = req
        .params
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let stream = req.params.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let temperature = req
        .params
        .get("temperature")
        .and_then(|v| v.as_f64())
        .map(|f| f as f32);
    let max_tokens = req
        .params
        .get("max_tokens")
        .and_then(|v| v.as_u64())
        .map(|u| u as usize);

    let handle = match instance.chat(session_id, chat::build_payload(&message, temperature, max_tokens)) {
        Ok(h) => h,
        Err(e) => return vec![error(id, ERR::ERR_SESSION_BUSY, e.to_string())],
    };

    let reply = match handle.wait().await {
        Some(r) => r,
        None => return vec![error(id, ERR::ERR_INTERNAL, "chat channel closed".into())],
    };

    let stream_rx = match reply {
        EngineReply::Streaming(s) => s,
        EngineReply::Success(resp) => {
            // 极少数非流式回退：直接组 ChatReply
            return vec![ok(id, json!(chat::reply_from_success(session_id, &resp)))];
        }
        EngineReply::Busy { .. } => return vec![error(id, ERR::ERR_SESSION_BUSY, "session busy".into())],
        EngineReply::Error(m) => return vec![error(id, ERR::ERR_INTERNAL, m)],
        EngineReply::Cancelled => return vec![error(id, ERR::ERR_INTERNAL, "chat cancelled".into())],
        EngineReply::Timeout => return vec![error(id, ERR::ERR_INTERNAL, "chat timeout".into())],
    };

    if stream {
        // 流式：逐 chunk 映射为 StreamFrame 响应（Delta/Finish/Error）
        let mut responses = Vec::new();
        let mut stream_rx = stream_rx;
        while let Some(chunk) = stream_rx.next().await {
            responses.push(ok(id.clone(), json!(chat::chunk_to_frame(chunk))));
        }
        responses
    } else {
        // 非流式：收敛流为单条 ChatReply
        match chat::converge_stream(stream_rx, session_id).await {
            Ok(reply) => vec![ok(id, json!(reply))],
            Err(e) => vec![error(id, e.code, e.message)],
        }
    }
}

/// 从 params 解析实例 id
fn param_id(params: &Value) -> Result<InstanceId, JsonRpcResponse> {
    match params.get("id").and_then(|v| v.as_str()) {
        Some(s) => InstanceId::new(s)
            .map_err(|e| error(Value::Null, ERR::ERR_INVALID_SPEC, e.to_string())),
        None => Err(error(Value::Null, ERR::ERR_INVALID_SPEC, "missing 'id'".into())),
    }
}

/// 从 params 解析会话 id（缺省自动生成）
fn param_session(params: &Value) -> Result<SessionId, JsonRpcResponse> {
    match params.get("session_id").and_then(|v| v.as_str()) {
        Some(s) => SessionId::parse_str(s)
            .map_err(|_| error(Value::Null, ERR::ERR_INVALID_SPEC, format!("invalid session_id '{s}'"))),
        None => Ok(SessionId::new_v4()),
    }
}
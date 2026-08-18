//! HTTP 路由处理器 — 只做「路径解析 + 载荷编解码 + 委托 `InstanceManager`」，无业务判定

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use referee_ai::engine::EngineReply;
use referee_ai::session::SessionId;

use crate::chat;
use crate::instance::{err, InstanceManager};
use crate::protocol::{
    ChatReply, ChatRequest, InstanceId, InstanceInfo, InstanceSpec, ServerError, SessionInfo,
};

use super::error::HttpError;
use super::sse::sse_stream;

/// POST /v1/instances — 创建实例（有界；201 + InstanceInfo）
pub async fn create(
    State(m): State<InstanceManager>,
    Json(spec): Json<InstanceSpec>,
) -> Result<(StatusCode, Json<InstanceInfo>), HttpError> {
    let id = m.create(spec).map_err(HttpError::from)?;
    let inst = m.get(&id).map_err(HttpError::from)?;
    Ok((StatusCode::CREATED, Json(inst.snapshot().await)))
}

/// GET /v1/instances — 列出全部实例
pub async fn list(
    State(m): State<InstanceManager>,
) -> Result<Json<Vec<InstanceInfo>>, HttpError> {
    Ok(Json(m.list().await))
}

/// GET /v1/instances/{id} — 单个实例详情
pub async fn get(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
) -> Result<Json<InstanceInfo>, HttpError> {
    let iid = parse_id(&id)?;
    let inst = m.get(&iid).map_err(HttpError::from)?;
    Ok(Json(inst.snapshot().await))
}

/// DELETE /v1/instances/{id} — 停止并移除实例（204 No Content）
pub async fn remove(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
) -> Result<StatusCode, HttpError> {
    let iid = parse_id(&id)?;
    m.remove(&iid).await.map_err(HttpError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /v1/instances/{id}/chat — 单轮对话（收敛流为 ChatReply）
pub async fn chat(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
    Json(req): Json<ChatRequest>,
) -> Result<Json<ChatReply>, HttpError> {
    let iid = parse_id(&id)?;
    let inst = m.get(&iid).map_err(HttpError::from)?;
    let sid = parse_or_new_session(req.session_id.as_deref())?;
    let payload = chat::build_payload(&req.message, req.temperature, req.max_tokens);
    let handle = inst
        .chat(sid, payload)
        .map_err(|e| HttpError(ServerError::new(err::ERR_SESSION_BUSY, e.to_string())))?;
    let reply = handle
        .wait()
        .await
        .ok_or_else(|| HttpError(ServerError::new(err::ERR_INTERNAL, "chat channel closed")))?;
    match reply {
        EngineReply::Streaming(s) => Ok(Json(chat::converge_stream(s, sid).await?)),
        EngineReply::Success(resp) => Ok(Json(chat::reply_from_success(sid, &resp))),
        other => Err(HttpError(ServerError::new(
            err::ERR_INTERNAL,
            format!("unexpected reply: {other:?}"),
        ))),
    }
}

/// POST /v1/instances/{id}/chat/stream — 流式对话（SSE）
pub async fn chat_stream(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
    Json(req): Json<ChatRequest>,
) -> Result<Response, HttpError> {
    let iid = parse_id(&id)?;
    let inst = m.get(&iid).map_err(HttpError::from)?;
    let sid = parse_or_new_session(req.session_id.as_deref())?;
    let payload = chat::build_payload(&req.message, req.temperature, req.max_tokens);
    let handle = inst
        .chat(sid, payload)
        .map_err(|e| HttpError(ServerError::new(err::ERR_SESSION_BUSY, e.to_string())))?;
    let stream = match handle.wait().await {
        Some(EngineReply::Streaming(s)) => s,
        Some(other) => {
            return Err(HttpError(ServerError::new(
                err::ERR_INTERNAL,
                format!("unexpected reply: {other:?}"),
            )))
        }
        None => {
            return Err(HttpError(ServerError::new(
                err::ERR_INTERNAL,
                "chat channel closed",
            )))
        }
    };
    Ok(sse_stream(stream).into_response())
}

/// POST /v1/instances/{id}/interrupt — 中断会话当前回合
pub async fn interrupt(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
    Json(body): Json<InterruptBody>,
) -> Result<Json<Value>, HttpError> {
    let iid = parse_id(&id)?;
    let inst = m.get(&iid).map_err(HttpError::from)?;
    let sid = parse_or_new_session(body.session_id.as_deref())?;
    Ok(Json(json!({ "cancelled": inst.interrupt(sid) })))
}

/// GET /v1/instances/{id}/sessions — 列出实例会话
pub async fn sessions(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
) -> Result<Json<Vec<SessionInfo>>, HttpError> {
    let iid = parse_id(&id)?;
    let inst = m.get(&iid).map_err(HttpError::from)?;
    Ok(Json(inst.session_infos()))
}

/// 中断请求体（HTTP 专属 REST 形状）
#[derive(Debug, Deserialize)]
pub struct InterruptBody {
    #[serde(default)]
    session_id: Option<String>,
}

/// 解析实例 id（非法 → 400）
fn parse_id(s: &str) -> Result<InstanceId, HttpError> {
    InstanceId::new(s)
        .map_err(|e| HttpError(ServerError::new(err::ERR_INVALID_SPEC, e.to_string())))
}

/// 解析会话 id（缺省自动生成；非法 → 400）
fn parse_or_new_session(s: Option<&str>) -> Result<SessionId, HttpError> {
    match s {
        Some(s) => SessionId::parse_str(s).map_err(|_| {
            HttpError(ServerError::new(
                err::ERR_INVALID_SPEC,
                format!("invalid session_id '{s}'"),
            ))
        }),
        None => Ok(SessionId::new_v4()),
    }
}
//! HTTP 层错误 — 业务级 `ServerError` → axum 响应（唯一错误映射点）
//!
//! `InstanceManager` / `protocol` 不感知 HTTP 状态码（保持 transport-agnostic）；
//! HTTP 状态映射集中在此处，全局唯一。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::instance::err;
use crate::protocol::ServerError;

/// HTTP 层错误包装 — 携带业务级 `ServerError`，`IntoResponse` 时映射状态码 + JSON 错误体
#[derive(Debug)]
pub struct HttpError(pub ServerError);

impl From<ServerError> for HttpError {
    fn from(e: ServerError) -> Self {
        Self(e)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = match self.0.code {
            err::ERR_INVALID_SPEC => StatusCode::BAD_REQUEST,
            err::ERR_INSTANCE_NOT_FOUND => StatusCode::NOT_FOUND,
            err::ERR_INSTANCE_FULL => StatusCode::CONFLICT,
            err::ERR_SESSION_BUSY => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "code": self.0.code, "message": self.0.message }))).into_response()
    }
}
//! HTTP + SSE 传输（feature `http`）
//!
//! **职责边界**：只做 HTTP/SSE 编解码 + 路由 + 错误码映射，业务判定全部
//! 委托 [`InstanceManager`](crate::instance::InstanceManager)（与 TCP 传输同源）。
//!
//! REST 语义（方法/路径/状态码），不复用 JSON-RPC 的 `{method,params,id}` 帧：
//! - `POST   /v1/instances`                创建实例（201）
//! - `GET    /v1/instances`                列出全部实例
//! - `GET    /v1/instances/{id}`           单个实例详情
//! - `DELETE /v1/instances/{id}`           停止并移除实例（204）
//! - `POST   /v1/instances/{id}/chat`      单轮对话
//! - `POST   /v1/instances/{id}/chat/stream` 流式对话（SSE）
//! - `POST   /v1/instances/{id}/interrupt` 中断会话回合
//! - `GET    /v1/instances/{id}/sessions`  列出实例会话

pub mod error;
pub mod handlers;
pub mod sse;

use std::net::SocketAddr;

use axum::Router;
use tokio::sync::watch;

use crate::instance::{err, InstanceManager};
use crate::protocol::ServerError;

/// 装配 HTTP 路由（注册路由 + 注入 `InstanceManager` 状态）
pub fn router(instances: InstanceManager) -> Router {
    Router::new()
        .route("/v1/instances", axum::routing::post(handlers::create).get(handlers::list))
        .route(
            "/v1/instances/{id}",
            axum::routing::get(handlers::get).delete(handlers::remove),
        )
        .route("/v1/instances/{id}/chat", axum::routing::post(handlers::chat))
        .route(
            "/v1/instances/{id}/chat/stream",
            axum::routing::post(handlers::chat_stream),
        )
        .route("/v1/instances/{id}/interrupt", axum::routing::post(handlers::interrupt))
        .route("/v1/instances/{id}/sessions", axum::routing::get(handlers::sessions))
        .with_state(instances)
}

/// HTTP 服务器入口（常驻，`shutdown` 触发优雅退出）
pub async fn serve_http(
    bind_addr: SocketAddr,
    instances: InstanceManager,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|e| ServerError::new(err::ERR_INTERNAL, format!("bind {bind_addr}: {e}")))?;
    tracing::info!(addr = %bind_addr, "referee-harness listening on http");
    axum::serve(listener, router(instances))
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await
        .map_err(|e| ServerError::new(err::ERR_INTERNAL, format!("http server: {e}")))
}
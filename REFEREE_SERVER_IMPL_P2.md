# Referee Server — Phase 2 执行实现规划（HTTP + SSE）

> 本文档是 [REFEREE_SERVER_PLAN.md](REFEREE_SERVER_PLAN.md) 的 **Phase 2 落地细化**，
> 只回答"每个文件怎么落地"，不重复架构与决策（见 PLAN §2 决策记录、§5 Phase 2）。
> 与 [REFEREE_SERVER_IMPL.md](REFEREE_SERVER_IMPL.md)（Phase 1）同风格，供开发执行。

## 1. 范围与职责

| 项 | 归属 | 说明 |
|---|---|---|
| 落地差距 | **G9**（HTTP 依赖决案）+ Web 接入 | P2 唯一目标 |
| 传输形态 | REST（管理/单轮）+ **SSE**（流式对话） | 接入同一 `protocol` 层与 `InstanceManager` |
| 依赖 | `http` feature 门控引入 axum | **默认关、核心零依赖**（对齐 `mcp-stdio` / `skills` 模式） |
| 硬约束 | 零既有行为改变；背压有界；不吞异常 | 继承 referee 内核哲学 |

**职责边界**：HTTP 层只做「HTTP/SSE 编解码 + 路由 + 错误码映射」，业务判定全部
委托 `InstanceManager`（与 TCP 传输同源）。**不引入任何新业务逻辑到 manager/protocol。**

## 2. 决策记录（对齐 G1 / G9）

- **HTTP 框架**：`http` feature 门控引入 **axum 0.8**（默认关）。核心（无 `http` feature）
  保持零 HTTP 依赖，对齐 `mcp-stdio` / `skills` 的按需拓展模式。
- **传输共享**：TCP（Phase 1）与 HTTP（Phase 2）**共用** `protocol` 载荷类型与
  `InstanceManager` 方法；HTTP 暴露 **REST 语义**（方法/路径/状态码），不复用
  JSON-RPC 的 `{method,params,id}` 帧。
- **流式**：SSE，`POST /v1/instances/{id}/chat/stream`，事件 `data` 为
  [`StreamFrame`](REFEREE_SERVER_IMPL.md#4-protocolrs--serde-协议类型)（Delta/Finish/Error）。
- **错误映射**：`ServerError.code` → HTTP 状态码（见 §6），响应体为统一 JSON 错误。
- **daemon 装配**：`http` feature 开启时，daemon **同时**起 TCP + HTTP 两个监听器
  （同一 `InstanceManager` 实例），`--http-bind` 默认 `127.0.0.1:7101`。
- **保持 P1 不回归**：`tcp` feature 与 `http` feature 正交、可独立编译；两者都关仍可编译。

## 3. 文件结构

```
referee-server/
├── Cargo.toml                     # 新增 feature "http"（axum）
├── src/
│   ├── lib.rs                     # #[cfg(feature="http")] pub mod http;
│   ├── protocol.rs                # 复用（无改动）
│   ├── instance.rs                # 复用（无改动）
│   ├── persist.rs                 # 复用（无改动）
│   ├── http.rs                    # §5 HTTP 路由 + 处理器（feature "http"）
│   ├── http/
│   │   ├── mod.rs                 # #![cfg(feature="http")]；Router 装配
│   │   ├── error.rs               # http::Error（ServerError → axum 响应）
│   │   ├── sse.rs                 # SSE 流式输出（chat/stream）
│   │   └── handlers.rs           # 各路由处理器（只做编解码 + 委托）
│   └── bin/
│       └── referee-server.rs     # 装配：http feature 时追加 HTTP 监听
└── tests/
    ├── server_test.rs             # 既有（不回归）
    └── http_test.rs               # §9 HTTP/SSE 集成测试
```

模块职责边界：
- `http/mod.rs`：`Router` 装配（路由注册 + `State` 注入）。
- `http/error.rs`：`ServerError` → HTTP 状态码 + JSON 错误体（唯一错误映射点）。
- `http/sse.rs`：`ImpulseReader` 消费 `ChatHandle` 流 → `Sse<Event>`（SSE 帧封装）。
- `http/handlers.rs`：路径解析 + 载荷编解码 + 委托 `InstanceManager`（无业务判定）。

## 4. Cargo.toml 与 feature 设计

```toml
[features]
default = ["tcp", "deepseek"]
tcp        = []
# Phase 2：HTTP + SSE（默认关、核心零依赖，对齐 mcp-stdio 模式）
http       = ["dep:axum", "dep:tokio/rt", "dep:tokio/net"]
deepseek   = ["referee-agent/deepseek"]
xiaomi     = ["referee-agent/xiaomi"]
openai     = ["referee-agent/openai"]

[dependencies]
# 仅 feature "http" 时启用；核心（无 http feature）不含 axum 及其传递依赖
axum = { version = "0.8", optional = true }
```

要点：
- `axum` 用 `optional = true`，`http` feature 通过 `dep:axum` 显式启用，杜绝隐式联动。
- axum 自带 tokio/tower 等，但均为其传递依赖，不影响核心依赖清单。
- `tcp` 与 `http` feature 互不依赖，可独立编译、独立测试。

## 5. http.rs — 路由表与装配

### 路由表（REST 语义）

| 方法 | 路径 | 请求体 | 成功响应 | 说明 |
|---|---|---|---|---|
| POST | `/v1/instances` | `InstanceSpec` | `201 InstanceInfo` | 创建实例（有界） |
| GET | `/v1/instances` | — | `200 [InstanceInfo]` | 列出全部实例 |
| GET | `/v1/instances/{id}` | — | `200 InstanceInfo` | 单个实例详情 |
| DELETE | `/v1/instances/{id}` | — | `204` | 停止并移除实例 |
| POST | `/v1/instances/{id}/chat` | `ChatRequest` | `200 ChatReply` | 单轮对话 |
| POST | `/v1/instances/{id}/chat/stream` | `ChatRequest` | `200 text/event-stream` | 流式对话 |
| POST | `/v1/instances/{id}/interrupt` | `{session_id}` | `200 {cancelled}` | 中断会话回合 |
| GET | `/v1/instances/{id}/sessions` | — | `200 [SessionInfo]` | 列出实例会话 |

### 装配骨架

```rust
// http/mod.rs（feature "http"）
pub fn router(instances: InstanceManager) -> axum::Router {
    axum::Router::new()
        .route("/v1/instances", axum::routing::post(handlers::create)
            .get(handlers::list))
        .route("/v1/instances/{id}", axum::routing::get(handlers::get)
            .delete(handlers::remove))
        .route("/v1/instances/{id}/chat", axum::routing::post(handlers::chat))
        .route("/v1/instances/{id}/chat/stream", axum::routing::post(handlers::chat_stream))
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
    let listener = tokio::net::TcpListener::bind(bind_addr).await
        .map_err(|e| ServerError::new(ERR::ERR_INTERNAL, format!("bind {bind_addr}: {e}")))?;
    axum::serve(listener, router(instances))
        .with_graceful_shutdown(async move { let _ = shutdown.changed().await; })
        .await
        .map_err(|e| ServerError::new(ERR::ERR_INTERNAL, format!("http server: {e}")))
}
```

要点：
- 路由路径用 axum 0.8 的 `{id}` 语法（非 `:id`）。
- `with_state(instances)` 注入 `InstanceManager`（`Clone`），处理器经 `State<InstanceManager>` 取用。
- `with_graceful_shutdown` 复用与 TCP 同一 `watch::Receiver<bool>`，优雅关闭一致。

## 6. http/error.rs — 错误码 → HTTP 状态映射

`ServerError { code, message }` 是业务级错误（P1 定义）。HTTP 层是**唯一**将其映射为
HTTP 状态码的地方：

| `ServerError.code` | HTTP 状态 | 语义 |
|---|---|---|
| `ERR_INVALID_SPEC`（-32004） | `400 Bad Request` | 规格非法 / 参数缺失 |
| `ERR_SESSION_BUSY`（-32002） | `409 Conflict` | 会话忙碌 |
| `ERR_INSTANCE_NOT_FOUND`（-32000） | `404 Not Found` | 实例不存在 |
| `ERR_INSTANCE_FULL`（-32001） | `409 Conflict` | 实例容量已满 |
| `ERR_INTERNAL`（-32003） | `500 Internal Server Error` | 内部错误 |
| 其余/未知 | `500` | 兜底 |

```rust
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = match self.0.code {
            ERR::ERR_INVALID_SPEC       => StatusCode::BAD_REQUEST,
            ERR::ERR_SESSION_BUSY       => StatusCode::CONFLICT,
            ERR::ERR_INSTANCE_NOT_FOUND => StatusCode::NOT_FOUND,
            ERR::ERR_INSTANCE_FULL      => StatusCode::CONFLICT,
            _                           => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "code": self.0.code, "message": self.0.message }))).into_response()
    }
}
```

要点：错误映射集中于此，`InstanceManager` / `protocol` 不感知 HTTP 状态码（保持
transport-agnostic）。

## 7. http/sse.rs — SSE 流式输出

`POST /v1/instances/{id}/chat/stream` 返回 `text/event-stream`。事件 `data` 为
`StreamFrame`（`Delta`/`Finish`/`Error`），与 TCP 流式帧**同构**，客户端可共用解析。

```rust
// http/sse.rs
/// 消费 Instance::chat 的 BoxStream，产出 SSE 事件流
pub fn sse_stream(
    stream: BoxStream<'static, Result<StreamChunk, LlmError>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let events = stream.map(|chunk| {
        let frame = match chunk {
            Ok(StreamChunk::Delta { content, reasoning_content, .. }) => {
                StreamFrame::Delta { content, reasoning_content }
            }
            Ok(StreamChunk::Finish { finish_reason, usage }) => {
                StreamFrame::Finish {
                    finish_reason: fr_str(&finish_reason),
                    usage: usage.as_ref().map(TokenUsageData::from),
                }
            }
            Err(e) => StreamFrame::Error { message: e.to_string() },
        };
        Ok::<_, Infallible>(Event::default().data(serde_json::to_string(&frame).unwrap()))
    });
    Sse::new(events)
}
```

要点：
- 流式 handler 不缓冲整段，逐 chunk 即时下发（**背压硬约束**：客户端可流式消费）。
- `fr_str` / `TokenUsageData::from` 复用 P1 transport 的既有工具（可提取为
  `protocol` 公共助手，避免重复）。

## 8. handlers.rs — 处理器（只做编解码 + 委托）

以核心两例说明：

```rust
/// POST /v1/instances/{id}/chat（单轮）
async fn chat(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
    Json(req): Json<ChatRequest>,
) -> Result<Json<ChatReply>, HttpError> {
    let iid = InstanceId::new(id).map_err(|e| HttpError(ServerError::new(ERR::ERR_INVALID_SPEC, e.to_string())))?;
    let inst = m.get(&iid)?;                       // Err → HttpError（自动映射状态码）
    let sid = parse_or_new_session(req.session_id.as_deref())?;
    let (content, finish_reason, usage) = run_chat_once(&inst, sid, &req).await?;  // 收敛流
    Ok(Json(ChatReply { session_id: sid.to_string(), content, finish_reason, usage }))
}

/// POST /v1/instances/{id}/chat/stream（SSE）
async fn chat_stream(
    State(m): State<InstanceManager>,
    Path(id): Path<String>,
    Json(req): Json<ChatRequest>,
) -> Result<Response, HttpError> {
    let iid = InstanceId::new(id).map_err(HttpError::spec)?;
    let inst = m.get(&iid)?;
    let sid = parse_or_new_session(req.session_id.as_deref())?;
    let handle = inst.chat(sid, chat_payload(&req))?;   // Err(busy) → 409
    let stream = match handle.wait().await {
        Some(EngineReply::Streaming(s)) => s,
        other => return Err(HttpError::internal(format!("unexpected reply: {other:?}"))),
    };
    Ok(sse_stream(stream).into_response())
}
```

要点：
- `run_chat_once` 收敛流为 `ChatReply` 的逻辑与 P1 transport 的**非流式分支**同构，
  建议提取为 `protocol`/`instance` 的公共助手，TCP 与 HTTP 复用（避免双份实现）。
- 中断 / 会话列表 / 管理类处理器直接委托 `InstanceManager`，无额外业务。

## 9. daemon 装配（bin/referee-server.rs）

`http` feature 开启时，在 TCP 监听之外追加 HTTP 监听（同一 `InstanceManager` 实例与
同一 `shutdown` 通道）：

```rust
// 参数新增：--http-bind <addr>（默认 127.0.0.1:7101）
let http_bind = parse_http_bind();   // None 表示不启用 HTTP

let (shutdown_tx, shutdown_rx) = watch::channel(false);
spawn_shutdown_handler(shutdown_tx);

// TCP（P1）
let tcp_rx = shutdown_rx.clone();
tokio::spawn(serve_tcp(tcp_bind, manager.clone(), persist, tcp_rx));

// HTTP（P2，feature 门控）
#[cfg(feature = "http")]
if let Some(addr) = http_bind {
    let http_rx = shutdown_rx.clone();
    tokio::spawn(serve_http(addr, manager.clone(), http_rx));
}
```

要点：两监听器共享 `InstanceManager`（`DashMap` + 内部锁，线程安全），可同时服务
TCP 客户端与 Web/TUI。

## 10. 集成测试（tests/http_test.rs）

用 `reqwest`（加入 dev-dependencies）发起真实 HTTP 请求，覆盖：

| # | 用例 | 断言 |
|---|---|---|
| 1 | `http_create_get_list` | POST create 201 → GET get 信息正确 → list 含该实例 |
| 2 | `http_duplicate_rejected` | 同 id 二次 create → 400 |
| 3 | `http_remove` | DELETE → 204 → GET 404 |
| 4 | `http_not_found` | GET 不存在 id → 404 |
| 5 | `http_chat_roundtrip`（MockProvider） | POST chat → `ChatReply.content` 正确 |
| 6 | `http_chat_stream_sse`（MockProvider） | POST chat/stream → 累计 Delta + Finish |
| 7 | `http_interrupt`（延迟 MockProvider） | interrupt → `{cancelled:true}` |
| 8 | `http_sessions` | 对话后 GET sessions → 非空 |
| 9 | `both_transports_share_manager` | HTTP 建实例 → TCP instance.list 可见（同 daemon） |

测试骨架（feature 门控）：

```rust
// tests/http_test.rs —— 仅当 feature "http" 开启时编译
#![cfg(feature = "http")]
```

要点：SSE 测试用 `reqwest::Response::bytes_stream()` 读流，累计 `data:` 行解析
`StreamFrame`，断言 Delta ≥1 + Finish 存在。

## 11. 实现顺序

| 步骤 | 依赖 | 输出 |
|---|---|---|
| Step 1 | — | Cargo.toml 增 `http` feature + axum optional；lib.rs 门控 `http` 模块 |
| Step 2 | Step 1 | `http/error.rs`：错误码 → 状态码映射 |
| Step 3 | Step 2 | `http/mod.rs` + `handlers.rs`：路由表 + 管理/单轮处理器 |
| Step 4 | Step 3 | `http/sse.rs`：SSE 流式输出 |
| Step 5 | Step 3,4 | 提取公共助手（收敛流 / fr_str / payload 构造），TCP 与 HTTP 复用 |
| Step 6 | Step 5 | bin 装配：`--http-bind` + 双监听器 + 优雅关闭 |
| Step 7 | Step 2-5 | `tests/http_test.rs` 集成测试 |
| Step 8 | Step 7 | 全量回归（P1 不回归 + 新用例） |

## 12. 验收标准

1. `cargo build -p referee-server`（默认 feature）通过，且**不含 axum**（核心零依赖）。
2. `cargo build -p referee-server --features http` 通过。
3. `cargo test -p referee-server --features http`：Phase 1 既有用例 + Phase 2 新用例全绿。
4. `cargo test -p referee-server`（无 http feature）：P1 用例不回归、可独立编译。
5. `cargo clippy -p referee-server --all-targets --features http` 零告警。
6. 手动：`referee-server --http-bind 127.0.0.1:7101` 后，`curl` 建实例、`curl -N` 流式
   对话、POST interrupt、GET sessions 均得预期响应。
7. 手动：同一 daemon 同时开 TCP + HTTP，跨传输可见同一实例（SSE 流式 + JSON-RPC 单轮）。
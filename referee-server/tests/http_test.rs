//! referee-server HTTP + SSE 集成测试（feature `http`）
//!
//! 覆盖 REFEREE_SERVER_IMPL_P2.md §10 的 9 个用例。对话类用例用 MockProvider
//! 直连（不触网）；管理类用例用真实 DeepSeek 构造（build_provider 不发起网络请求）。
#![cfg(feature = "http")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use referee_ai_base::engine::EngineConfig;
use referee_ai_base::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities,
    ProviderId, StreamChunk, TokenUsage,
};
use referee_server::http::serve_http;
use referee_server::instance::{InstanceManager, InstanceManagerConfig};
use referee_server::protocol::{InstanceSpec, InstanceTools, ProviderConfig};
use referee_server::transport::serve_tcp;
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

// ───────────────────────────────────────────────
// MockProvider — 固定回复 / 流式 / 延迟
// ───────────────────────────────────────────────

struct MockProvider {
    reply: String,
    delay: Option<Duration>,
}

impl MockProvider {
    fn plain(reply: &str) -> Arc<Self> {
        Arc::new(Self {
            reply: reply.to_string(),
            delay: None,
        })
    }
    fn delayed(reply: &str, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            reply: reply.to_string(),
            delay: Some(delay),
        })
    }
}

fn caps() -> &'static ProviderCapabilities {
    static C: ProviderCapabilities = ProviderCapabilities {
        parallel_tool_calls: true,
        system_role: true,
        streaming: true,
        usage_reported: true,
        max_output_tokens: 4096,
        multimodal: referee_ai_base::provider::MultimodalCapabilities::NONE,
    };
    &C
}

#[async_trait]
impl LLMProvider for MockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("mock")
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        caps()
    }
    async fn chat(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
        if let Some(d) = self.delay {
            tokio::time::sleep(d).await;
        }
        Ok(ChatResponse {
            id: "mock".into(),
            model: "mock".into(),
            message: Message::assistant(self.reply.clone()),
            finish_reason: FinishReason::Stop,
            usage: Some(TokenUsage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
                ..Default::default()
            }),
        })
    }
    async fn chat_stream(
        &self,
        _req: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        let reply = self.reply.clone();
        let delay = self.delay;
        let stream = stream::once(async move {
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            Ok(StreamChunk::Delta {
                content: Some(reply),
                reasoning_content: None,
                tool_calls: vec![],
                role: Some(referee_ai_base::provider::Role::Assistant),
            })
        })
        .chain(stream::once(async move {
            Ok(StreamChunk::Finish {
                finish_reason: FinishReason::Stop,
                usage: Some(TokenUsage {
                    total_tokens: 2,
                    ..Default::default()
                }),
            })
        }));
        Ok(Box::pin(stream))
    }
}

// ───────────────────────────────────────────────
// 装配辅助
// ───────────────────────────────────────────────

fn spec(id: Option<&str>) -> InstanceSpec {
    InstanceSpec {
        id: id.map(Into::into),
        agent: referee_agent::general(),
        engine: EngineConfig::default(),
        template_vars: HashMap::from([(
            "cwd".to_string(),
            std::env::temp_dir().to_string_lossy().to_string(),
        )]),
        tools: InstanceTools::default(),
        provider: ProviderConfig::DeepSeek {
            api_key: "test".into(),
            base_url: None,
            model: None,
        },
    }
}

fn manager(max_instances: usize) -> InstanceManager {
    InstanceManager::new(InstanceManagerConfig {
        max_instances,
        max_sessions_per_instance: 100,
        global_budget_limit: 0,
    })
}

async fn free_port() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

/// 启动 HTTP 监听（返回地址 + shutdown sender）
async fn start_http(manager: InstanceManager) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
    let addr = free_port().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(serve_http(addr, manager, rx));
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, tx)
}

/// TCP JSON-RPC 请求 → 响应帧列表（对齐 server_test 的 send_rpc）
async fn send_rpc(addr: SocketAddr, method: &str, params: Value) -> Vec<Value> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r).lines();
    let req = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    w.write_all(serde_json::to_vec(&req).unwrap().as_slice())
        .await
        .unwrap();
    w.write_all(b"\n").await.unwrap();
    w.flush().await.unwrap();

    let mut frames = Vec::new();
    while let Ok(Some(line)) = reader.next_line().await {
        let v: Value = serde_json::from_str(&line).unwrap();
        let is_error = v.get("error").is_some();
        let is_finish = matches!(
            v.pointer("/result/type").and_then(|t| t.as_str()),
            Some("finish") | Some("error")
        );
        let is_non_stream_done = v.get("result").is_some() && v.pointer("/result/type").is_none();
        frames.push(v);
        if is_finish || is_non_stream_done || is_error {
            break;
        }
    }
    frames
}

// ───────────────────────────────────────────────
// 1-4 管理用例
// ───────────────────────────────────────────────

#[tokio::test]
async fn http_create_get_list() {
    let m = manager(8);
    let (addr, _tx) = start_http(m.clone()).await;
    let c = reqwest::Client::new();
    let base = format!("http://{addr}");

    let resp = c
        .post(format!("{base}/v1/instances"))
        .json(&spec(Some("http-a")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let info: Value = resp.json().await.unwrap();
    assert_eq!(info["id"], "http-a");

    let resp = c
        .get(format!("{base}/v1/instances/http-a"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let info: Value = resp.json().await.unwrap();
    assert_eq!(info["id"], "http-a");

    let resp = c.get(format!("{base}/v1/instances")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Value = resp.json().await.unwrap();
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["id"], "http-a");
}

#[tokio::test]
async fn http_duplicate_rejected() {
    let m = manager(8);
    m.create_with_provider(spec(Some("dup")), Some(MockProvider::plain("x")))
        .unwrap();
    let (addr, _tx) = start_http(m).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/instances"))
        .json(&spec(Some("dup")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_remove() {
    let m = manager(8);
    m.create_with_provider(spec(Some("gone")), Some(MockProvider::plain("x")))
        .unwrap();
    let (addr, _tx) = start_http(m).await;
    let c = reqwest::Client::new();
    let base = format!("http://{addr}");

    let resp = c
        .delete(format!("{base}/v1/instances/gone"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = c
        .get(format!("{base}/v1/instances/gone"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_not_found() {
    let m = manager(8);
    let (addr, _tx) = start_http(m).await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/v1/instances/nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ───────────────────────────────────────────────
// 5-8 对话用例
// ───────────────────────────────────────────────

#[tokio::test]
async fn http_chat_roundtrip() {
    let m = manager(8);
    m.create_with_provider(spec(Some("chat-a")), Some(MockProvider::plain("hello world")))
        .unwrap();
    let (addr, _tx) = start_http(m).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/instances/chat-a/chat"))
        .json(&json!({ "message": "ping" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let reply: Value = resp.json().await.unwrap();
    assert_eq!(reply["content"], "hello world");
    assert_eq!(reply["finish_reason"], "stop");
}

#[tokio::test]
async fn http_chat_stream_sse() {
    let m = manager(8);
    m.create_with_provider(spec(Some("chat-b")), Some(MockProvider::plain("streamed")))
        .unwrap();
    let (addr, _tx) = start_http(m).await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/instances/chat-b/chat/stream"))
        .json(&json!({ "message": "go" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));

    // 读流，累计 data: 行解析 StreamFrame
    let mut deltas = 0usize;
    let mut finished = false;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let bytes = chunk.unwrap();
        let text = String::from_utf8_lossy(&bytes);
        for line in text.lines() {
            if let Some(data) = line.strip_prefix("data:") {
                let frame: Value = serde_json::from_str(data.trim()).unwrap();
                match frame["type"].as_str() {
                    Some("delta") => deltas += 1,
                    Some("finish") => finished = true,
                    _ => {}
                }
            }
        }
    }
    assert!(deltas >= 1, "expected at least one delta");
    assert!(finished, "expected a finish frame");
}

#[tokio::test]
async fn http_interrupt() {
    let m = manager(8);
    m.create_with_provider(
        spec(Some("slow")),
        Some(MockProvider::delayed("slow", Duration::from_secs(30))),
    )
    .unwrap();
    let (addr, _tx) = start_http(m).await;
    let c = reqwest::Client::new();
    let base = format!("http://{addr}");
    let sid = "11111111-1111-1111-1111-111111111111";

    // 后台发起流式回合（消费 SSE，避免连接挂起）
    let stream_task = tokio::spawn({
        let c = c.clone();
        let url = format!("{base}/v1/instances/slow/chat/stream");
        async move {
            let resp = c
                .post(&url)
                .json(&json!({ "message": "go", "session_id": sid }))
                .send()
                .await
                .unwrap();
            let mut _stream = resp.bytes_stream();
            while _stream.next().await.is_some() {}
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = c
        .post(format!("{base}/v1/instances/slow/interrupt"))
        .json(&json!({ "session_id": sid }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["cancelled"], true);

    stream_task.await.unwrap();
}

#[tokio::test]
async fn http_sessions() {
    let m = manager(8);
    m.create_with_provider(spec(Some("sess")), Some(MockProvider::plain("hi")))
        .unwrap();
    let (addr, _tx) = start_http(m).await;
    let c = reqwest::Client::new();
    let base = format!("http://{addr}");

    let resp = c
        .post(format!("{base}/v1/instances/sess/chat"))
        .json(&json!({ "message": "hello", "session_id": "22222222-2222-2222-2222-222222222222" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = c
        .get(format!("{base}/v1/instances/sess/sessions"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let sessions: Value = resp.json().await.unwrap();
    assert!(
        !sessions.as_array().unwrap().is_empty(),
        "sessions must be non-empty after a chat"
    );
}

// ───────────────────────────────────────────────
// 9 跨传输共享
// ───────────────────────────────────────────────

#[tokio::test]
async fn both_transports_share_manager() {
    let m = manager(8);
    let tcp_addr = free_port().await;
    let http_addr = free_port().await;
    let (tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let tcp_daemon = tokio::spawn(serve_tcp(tcp_addr, m.clone(), None, shutdown_rx.clone()));
    let http_daemon = tokio::spawn(serve_http(http_addr, m.clone(), shutdown_rx));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // HTTP 建实例
    let resp = reqwest::Client::new()
        .post(format!("http://{http_addr}/v1/instances"))
        .json(&spec(Some("shared")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // TCP instance.list 可见（同 manager）
    let frames = send_rpc(tcp_addr, "instance.list", json!({})).await;
    let list = frames[0]["result"].as_array().unwrap();
    assert!(list.iter().any(|v| v["id"] == "shared"), "tcp must see the shared instance");

    let _ = tx.send(true);
    tcp_daemon.await.unwrap().unwrap();
    http_daemon.await.unwrap().unwrap();
}
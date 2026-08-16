//! referee-harness 集成测试 — 多实例管理 / 对话往返 / 崩溃恢复 / TCP 传输
//!
//! 覆盖 REFEREE_HARNESS_IMPL.md §10 的 13 个用例。全部使用 MockProvider 直连，
//! 不触网；TCP 用例用随机端口 + 进程内 daemon。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::StreamExt;
use referee_ai::engine::EngineConfig;
use referee_ai::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities,
    ProviderId, StreamChunk, TokenUsage,
};
use referee_ai::session::{ChatOptions, ChatPayload, SessionId};
use referee_harness::instance::{InstanceManager, InstanceManagerConfig};
use referee_harness::persist::PersistStore;
use referee_harness::protocol::{
    InstanceId, InstanceSpec, InstanceTools, ProviderConfig, ERR_INSTANCE_FULL,
    ERR_INSTANCE_NOT_FOUND, ERR_INVALID_SPEC,
};
use referee_harness::transport::serve_tcp;
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
        multimodal: referee_ai::provider::MultimodalCapabilities::NONE,
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
                role: Some(referee_ai::provider::Role::Assistant),
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
// Spec 辅助
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

fn chat_payload(msg: &str) -> ChatPayload {
    ChatPayload {
        message: Message::user(msg),
        options: ChatOptions::default(),
        peer_depth: 0,
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("referee_harness_test_{tag}_{}", uuid::Uuid::new_v4()))
}

// ───────────────────────────────────────────────
// 1-4 管理用例
// ───────────────────────────────────────────────

#[tokio::test]
async fn manager_create_list_get() {
    let m = manager(8);
    let id = m
        .create_with_provider(spec(Some("my-agent")), Some(MockProvider::plain("hi")))
        .unwrap();
    assert_eq!(id.as_str(), "my-agent");

    let list = m.list().await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, id);

    let info = m.get(&id).unwrap().snapshot().await;
    assert_eq!(info.model, "deepseek/deepseek-v4-flash");
    assert_eq!(info.sessions, 0);
    assert!(!info.created_at.is_empty());
}

#[tokio::test]
async fn manager_create_duplicate_rejected() {
    let m = manager(8);
    m.create_with_provider(spec(Some("dup")), Some(MockProvider::plain("x")))
        .unwrap();
    let err = m
        .create_with_provider(spec(Some("dup")), Some(MockProvider::plain("x")))
        .unwrap_err();
    assert_eq!(err.code, ERR_INVALID_SPEC);
}

#[tokio::test]
async fn manager_max_instances_rejected() {
    let m = manager(1);
    m.create_with_provider(spec(Some("a")), Some(MockProvider::plain("x")))
        .unwrap();
    let err = m
        .create_with_provider(spec(Some("b")), Some(MockProvider::plain("x")))
        .unwrap_err();
    assert_eq!(err.code, ERR_INSTANCE_FULL);
}

#[tokio::test]
async fn manager_remove() {
    let m = manager(8);
    let id = m
        .create_with_provider(spec(Some("gone")), Some(MockProvider::plain("x")))
        .unwrap();
    m.remove(&id).await.unwrap();
    assert!(m.list().await.is_empty());
    assert_eq!(m.get(&id).unwrap_err().code, ERR_INSTANCE_NOT_FOUND);
}

// ───────────────────────────────────────────────
// 5-8 对话用例
// ───────────────────────────────────────────────

async fn chat_reply_content(m: &InstanceManager, id: &InstanceId, msg: &str) -> String {
    let inst = m.get(id).unwrap();
    let sid = SessionId::new_v4();
    let handle = inst
        .chat(sid, chat_payload(msg))
        .expect("chat start");
    let mut content = String::new();
    match handle.wait().await.expect("chat wait") {
        referee_ai::engine::EngineReply::Streaming(mut s) => {
            while let Some(chunk) = s.next().await {
                if let Ok(StreamChunk::Delta { content: Some(c), .. }) = chunk {
                    content.push_str(&c);
                }
            }
        }
        other => panic!("expected streaming, got {other:?}"),
    }
    content
}

#[tokio::test]
async fn instance_chat_roundtrip() {
    let m = manager(8);
    let id = m
        .create_with_provider(spec(None), Some(MockProvider::plain("hello world")))
        .unwrap();
    let content = chat_reply_content(&m, &id, "ping").await;
    assert_eq!(content, "hello world");
}

#[tokio::test]
async fn instance_chat_stream() {
    let m = manager(8);
    let id = m
        .create_with_provider(spec(None), Some(MockProvider::plain("streamed")))
        .unwrap();
    let inst = m.get(&id).unwrap();
    let sid = SessionId::new_v4();
    let handle = inst.chat(sid, chat_payload("go")).unwrap();
    let mut deltas = 0usize;
    let mut finished = false;
    match handle.wait().await.unwrap() {
        referee_ai::engine::EngineReply::Streaming(mut s) => {
            while let Some(chunk) = s.next().await {
                match chunk.unwrap() {
                    StreamChunk::Delta { .. } => deltas += 1,
                    StreamChunk::Finish { .. } => finished = true,
                }
            }
        }
        other => panic!("expected streaming, got {other:?}"),
    }
    assert_eq!(deltas, 1);
    assert!(finished);
}

#[tokio::test]
async fn instance_interrupt() {
    let m = manager(8);
    let id = m
        .create_with_provider(
            spec(None),
            Some(MockProvider::delayed("slow", Duration::from_secs(30))),
        )
        .unwrap();
    let inst = m.get(&id).unwrap();
    let sid = SessionId::new_v4();
    let handle = inst.chat(sid, chat_payload("go")).unwrap();
    // 立即中断，等待一小会
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(inst.interrupt(sid));
    let reply = handle.wait().await.unwrap();
    // 流式路径下中断表现为流立即结束（无任何 chunk）
    match reply {
        referee_ai::engine::EngineReply::Streaming(mut s) => {
            assert!(
                s.next().await.is_none(),
                "interrupted stream must end immediately with no chunks"
            );
        }
        other => panic!("expected streaming reply, got {other:?}"),
    }
}

#[tokio::test]
async fn parallel_instances_independent() {
    let m = manager(8);
    let id_a = m
        .create_with_provider(spec(Some("a")), Some(MockProvider::plain("AAAA")))
        .unwrap();
    let id_b = m
        .create_with_provider(spec(Some("b")), Some(MockProvider::plain("BBBB")))
        .unwrap();
    let (ca, cb) = tokio::join!(
        async { chat_reply_content(&m, &id_a, "go").await },
        async { chat_reply_content(&m, &id_b, "go").await }
    );
    assert_eq!(ca, "AAAA");
    assert_eq!(cb, "BBBB");
}

// ───────────────────────────────────────────────
// 9-10 崩溃恢复用例
// ───────────────────────────────────────────────

#[tokio::test]
async fn crash_recovery_roundtrip() {
    let dir = temp_dir("recovery");
    let persist = PersistStore::new(dir.clone()).unwrap();

    // 第一次进程：建实例 + 发起对话（会话事实落盘）
    let m1 = manager(8).with_persist(persist.clone());
    let id = m1
        .create_with_provider(spec(Some("recovered")), Some(MockProvider::plain("persisted")))
        .unwrap();
    let content = chat_reply_content(&m1, &id, "hello").await;
    assert_eq!(content, "persisted");
    drop(m1); // 模拟进程结束

    // 第二次进程：新 manager 恢复
    let m2 = InstanceManager::new(InstanceManagerConfig {
        max_instances: 8,
        max_sessions_per_instance: 100,
        global_budget_limit: 0,
    })
    .with_persist(persist.clone());
    let result = m2.recover(&persist).await;
    assert_eq!(result.recovered_instances, 1);
    assert!(result.broken.is_empty());

    // 实例与会话一致
    let inst = m2.get(&id).unwrap();
    let sessions = inst.session_infos();
    assert!(!sessions.is_empty(), "session must be restored");
    assert!(result.recovered_sessions > 0);
    // 会话历史含首条 user 消息 + 助手回复（≥2）
    assert!(sessions[0].messages >= 2, "history must be replayed");

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn persist_broken_file_does_not_block_start() {
    let dir = temp_dir("broken");
    let persist = PersistStore::new(dir.clone()).unwrap();

    // 一个健康实例
    let m = manager(8).with_persist(persist.clone());
    m.create_with_provider(spec(Some("healthy")), Some(MockProvider::plain("x")))
        .unwrap();

    // 写入损坏文件：坏实例 JSON + 坏会话 JSONL
    std::fs::write(
        dir.join("instances").join("bad-instance.json"),
        "{ not valid json",
    )
    .unwrap();
    let bad_sess_dir = dir.join("sessions").join("healthy");
    std::fs::create_dir_all(&bad_sess_dir).unwrap();
    std::fs::write(bad_sess_dir.join("00000000-0000-0000-0000-000000000000.jsonl"), "garbage\n")
        .unwrap();

    // 新 manager 恢复：健康实例恢复，损坏进入 broken，不阻塞
    let m2 = InstanceManager::new(InstanceManagerConfig::default()).with_persist(persist.clone());
    let result = m2.recover(&persist).await;
    assert_eq!(result.recovered_instances, 1, "healthy instance must recover");
    assert!(!result.broken.is_empty(), "broken entries must be listed");
    // 健康实例可用
    assert!(m2.get(&InstanceId::new("healthy").unwrap()).is_ok());

    let _ = std::fs::remove_dir_all(dir);
}

// ───────────────────────────────────────────────
// 11-13 传输用例
// ───────────────────────────────────────────────

async fn free_port() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

async fn send_rpc(addr: SocketAddr, method: &str, params: Value) -> Vec<Value> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r).lines();
    let req = json!({
        "jsonrpc": "2.0", "id": 1, "method": method, "params": params
    });
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

#[tokio::test]
async fn tcp_transport_roundtrip() {
    let m = manager(8);
    let addr = free_port().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let daemon = tokio::spawn(serve_tcp(addr, m.clone(), None, rx));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let frames = send_rpc(addr, "instance.create", json!(spec(Some("tcp-a")))).await;
    let id = frames[0]["result"]["id"].as_str().unwrap().to_string();
    assert_eq!(id, "tcp-a");

    let frames = send_rpc(addr, "instance.list", json!({})).await;
    assert_eq!(frames[0]["result"].as_array().unwrap().len(), 1);

    let frames = send_rpc(addr, "instance.remove", json!({ "id": "tcp-a" })).await;
    assert!(frames[0]["result"].is_object());

    let _ = tx.send(true);
    daemon.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_transport_chat_and_stream() {
    let m = manager(8);
    let addr = free_port().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let daemon = tokio::spawn(serve_tcp(addr, m.clone(), None, rx));
    tokio::time::sleep(Duration::from_millis(50)).await;

    m.create_with_provider(spec(Some("tcp-b")), Some(MockProvider::plain("streamed")))
        .unwrap();

    // 非流式
    let frames = send_rpc(
        addr,
        "instance.chat",
        json!({ "id": "tcp-b", "message": "hi", "session_id": "11111111-1111-1111-1111-111111111111" }),
    )
    .await;
    assert_eq!(frames[0]["result"]["content"].as_str().unwrap(), "streamed");

    // 流式 ≥1 Delta + 1 Finish
    let frames = send_rpc(
        addr,
        "instance.chat",
        json!({ "id": "tcp-b", "message": "go", "session_id": "22222222-2222-2222-2222-222222222222", "stream": true }),
    )
    .await;
    let types: Vec<&str> = frames
        .iter()
        .map(|f| f["result"]["type"].as_str().unwrap())
        .collect();
    assert!(types.contains(&"delta"));
    assert!(types.contains(&"finish"));

    let _ = tx.send(true);
    daemon.await.unwrap().unwrap();
}

#[tokio::test]
async fn transport_method_not_found() {
    let m = manager(8);
    let addr = free_port().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let daemon = tokio::spawn(serve_tcp(addr, m.clone(), None, rx));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let frames = send_rpc(addr, "nope.method", json!({})).await;
    assert_eq!(frames[0]["error"]["code"].as_i64().unwrap(), -32601);

    let _ = tx.send(true);
    daemon.await.unwrap().unwrap();
}

#[tokio::test]
async fn graceful_shutdown() {
    // serve_tcp 收到 shutdown 信号即返回 Ok（优雅退出路径）
    let m = manager(8);
    let addr = free_port().await;
    let (tx, rx) = tokio::sync::watch::channel(false);
    let daemon = tokio::spawn(serve_tcp(addr, m.clone(), None, rx));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = tx.send(true);
    assert!(daemon.await.unwrap().is_ok());
}
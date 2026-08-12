//! 业务层验收测试 — 对等智能体协作与安全工件存储
//!
//! 验收项（AGENT_RUNTIME_PLAN §5.3 对等协作路线 + 执行方案）：
//! 1. **资源池死锁修复**：AgentTool 为 Local 不占 IO 槽位，目标 Agent 的 Remote
//!    工具总能拿到槽位（验收 1）
//! 2. **循环调用拒绝**：A→B→A 被 Busy 拒绝，系统不挂死（验收 2，DAG 约束）
//! 3. **工件访问控制**：owner/授权读者可读，未授权者 PermissionDenied（验收 3）
//! 4. **对等性注册**：一个 Agent 持有多个对等工具并并行调用、结果汇聚（验收 4）
//!
//! 所有测试经 kernel.invoke 设置显式超时，绝不挂死。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use referee_agent::artifact::{ArtifactStore, InMemoryArtifactStore, StoreError};
use referee_agent::tool::AgentTool;
use referee_agent::AgentRuntime;
use referee_ai_base::engine::{Engine, EngineConfig};
use referee_ai_base::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities,
    ProviderId, StreamChunk, TokenUsage, ToolCall, ToolCallFunction,
};
use referee_ai_base::session::{ChatOptions, ChatPayload, SessionId, SessionMessage, SessionReply};
use referee_ai_base::tool::{ExecutorConfig, Tool, ToolContext, ToolRegistry};
use referee_core::{CapabilityId, Kernel, SupervisionPolicy};
use serde_json::{json, Value};
use uuid::Uuid;

// ───────────────────────────────────────────────
// Peer Mock Provider — 行为队列 + 工具结果回显 + 调用计数
// ───────────────────────────────────────────────

enum Behavior {
    /// 预置响应
    Ok(ChatResponse),
    /// 延迟后返回（并行性验证用）
    OkDelayed(ChatResponse, Duration),
    /// 回显本轮 history 中最后一条 role=Tool 消息的 content
    EchoLastToolResult(&'static str),
}

struct MockControl {
    behaviors: parking_lot::Mutex<std::collections::VecDeque<Behavior>>,
}

impl MockControl {
    fn new(behaviors: Vec<Behavior>) -> Self {
        Self {
            behaviors: parking_lot::Mutex::new(behaviors.into()),
        }
    }

    fn next(&self) -> Behavior {
        self.behaviors
            .lock()
            .pop_front()
            .unwrap_or_else(|| panic!("peer mock provider out of behaviors"))
    }
}

struct PeerMockProvider {
    control: Arc<MockControl>,
    calls: Arc<AtomicUsize>,
}

impl PeerMockProvider {
    fn new(behaviors: Vec<Behavior>) -> Self {
        Self {
            control: Arc::new(MockControl::new(behaviors)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn caps() -> &'static ProviderCapabilities {
    static CAPS: ProviderCapabilities = ProviderCapabilities {
        parallel_tool_calls: true,
        system_role: true,
        streaming: false,
        usage_reported: true,
        max_output_tokens: 4096,
    };
    &CAPS
}

#[async_trait]
impl LLMProvider for PeerMockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("peer-mock")
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        caps()
    }

    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.control.next() {
            Behavior::Ok(resp) => Ok(resp),
            Behavior::OkDelayed(resp, d) => {
                tokio::time::sleep(d).await;
                Ok(resp)
            }
            Behavior::EchoLastToolResult(fallback) => {
                let last_tool = req
                    .messages
                    .iter()
                    .rev()
                    .find(|m| m.role == referee_ai_base::Role::Tool);
                let content = last_tool
                    .and_then(|m| m.content.as_text())
                    .unwrap_or(fallback);
                Ok(mock_response(content))
            }
        }
    }

    async fn chat_stream(
        &self,
        _req: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Err(LlmError::BadRequest("streaming not supported".into()))
    }
}

// ───────────────────────────────────────────────
// 工具
// ───────────────────────────────────────────────

/// 外部 IO 工具（Remote 分类）— 模拟耗时 HTTP 调用
struct HttpSlowTool;

#[async_trait]
impl Tool for HttpSlowTool {
    fn name(&self) -> &str {
        "http_slow"
    }
    fn description(&self) -> &str {
        "A slow external HTTP tool (Remote)"
    }
    fn input_schema(&self) -> Value {
        json!({"type": "object", "properties": {}})
    }
    async fn execute(
        &self,
        _ctx: ToolContext,
        _args: Value,
    ) -> Result<referee_ai_base::tool::ToolOutput, referee_ai_base::tool::ToolError> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(referee_ai_base::tool::ToolOutput::text("http_ok"))
    }
}

// ───────────────────────────────────────────────
// 辅助函数
// ───────────────────────────────────────────────

fn mock_response(content: &str) -> ChatResponse {
    ChatResponse {
        id: "test".into(),
        model: "mock".into(),
        message: Message::assistant(content),
        finish_reason: FinishReason::Stop,
        usage: Some(TokenUsage::default()),
    }
}

fn mock_tool_response(content: &str, calls: Vec<ToolCall>) -> ChatResponse {
    let mut msg = Message::assistant(content);
    msg.tool_calls = calls;
    ChatResponse {
        id: "test".into(),
        model: "mock".into(),
        message: msg,
        finish_reason: FinishReason::ToolCalls,
        usage: Some(TokenUsage::default()),
    }
}

fn make_tool_call(id: &str, name: &str, args: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        function: ToolCallFunction {
            name: name.into(),
            arguments: args.into(),
        },
    }
}

fn chat_msg(session_id: SessionId, content: &str) -> SessionMessage {
    SessionMessage::Chat {
        session_id,
        payload: ChatPayload {
            message: Message::user(content),
            options: ChatOptions::default(),
        },
    }
}

fn decode_reply(env: &referee_core::Envelope) -> SessionReply {
    SessionReply::from_envelope(env).expect("decode reply")
}

/// 注册一个 Agent（引擎包含工具注册表 + 带内核的执行器 + 可选工件存储）
async fn setup_runtime(
    kernel: &Kernel,
    provider: Arc<PeerMockProvider>,
    tools: Vec<Arc<dyn Tool>>,
    executor: referee_ai_base::tool::ToolExecutor,
    store: Option<Arc<dyn ArtifactStore>>,
) -> CapabilityId {
    let registry = ToolRegistry::with_defaults();
    for tool in tools {
        registry.register(tool).unwrap();
    }
    let engine = Engine::new(provider, EngineConfig::default())
        .with_tools(registry, executor.with_kernel(kernel.clone()));

    let mut runtime = AgentRuntime::new(engine);
    if let Some(s) = store {
        runtime = runtime.with_artifact_store(s);
    }
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    rid
}

// ═══════════════════════════════════════════════
// 验收 1 — 资源池死锁修复
// ═══════════════════════════════════════════════
#[tokio::test]
async fn resource_pool_deadlock_fixed() {
    let kernel = Kernel::new();
    let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());
    let executor = referee_ai_base::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 2,
    });

    let rid_b = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling http",
                vec![make_tool_call("tc_http", "http_slow", "{}")],
            )),
            Behavior::EchoLastToolResult("b_fallback"),
        ])),
        vec![Arc::new(HttpSlowTool)],
        executor.clone(),
        Some(store.clone()),
    )
    .await;

    let rid_d = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling http",
                vec![make_tool_call("tc_http", "http_slow", "{}")],
            )),
            Behavior::EchoLastToolResult("d_fallback"),
        ])),
        vec![Arc::new(HttpSlowTool)],
        executor.clone(),
        Some(store.clone()),
    )
    .await;

    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();
    let sid_c = Uuid::new_v4();
    let sid_d = Uuid::new_v4();

    let rid_a = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling agent_b",
                vec![make_tool_call("tc_peer", "agent_b", r#"{"task":"work"}"#)],
            )),
            Behavior::EchoLastToolResult("a_fallback"),
        ])),
        vec![Arc::new(AgentTool::new("agent_b", "peer", rid_b, sid_b))],
        executor.clone(),
        Some(store.clone()),
    )
    .await;

    let rid_c = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling agent_d",
                vec![make_tool_call("tc_peer", "agent_d", r#"{"task":"work"}"#)],
            )),
            Behavior::EchoLastToolResult("c_fallback"),
        ])),
        vec![Arc::new(AgentTool::new("agent_d", "peer", rid_d, sid_d))],
        executor.clone(),
        Some(store.clone()),
    )
    .await;

    let start = Instant::now();
    let (resp_a, resp_c) = tokio::join!(
        kernel.invoke(rid_a, chat_msg(sid_a, "task").to_envelope(), 10_000),
        kernel.invoke(rid_c, chat_msg(sid_c, "task").to_envelope(), 10_000),
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "deadlock: peers took too long: {elapsed:?}"
    );

    let reply_a = decode_reply(&resp_a.expect("invoke A ok"));
    let reply_c = decode_reply(&resp_c.expect("invoke C ok"));
    match (reply_a, reply_c) {
        (SessionReply::Success { message, .. }, SessionReply::Success { .. }) => {
            let content = message.content.as_text().unwrap_or("");
            assert!(
                content.contains("http_ok"),
                "B/D http_slow must succeed (got: {content})"
            );
        }
        other => panic!("expected both Success, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════
// 验收 2 — 循环调用拒绝
// ═══════════════════════════════════════════════
#[tokio::test]
async fn cyclic_call_rejected() {
    let kernel = Kernel::new();
    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();

    // 先构造运行时对象（各自含空工具引擎），交换 id 后互相注册对等工具
    let engine_a = Engine::new(
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling agent_b",
                vec![make_tool_call("tc_ab", "agent_b", r#"{"task":"ping"}"#)],
            )),
            Behavior::EchoLastToolResult("a_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        referee_ai_base::tool::ToolExecutor::with_defaults().with_kernel(kernel.clone()),
    );
    let engine_b = Engine::new(
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling agent_a",
                vec![make_tool_call("tc_ba", "agent_a", r#"{"task":"confirm"}"#)],
            )),
            Behavior::EchoLastToolResult("b_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        referee_ai_base::tool::ToolExecutor::with_defaults().with_kernel(kernel.clone()),
    );

    let runtime_a = AgentRuntime::new(engine_a);
    let rid_a = runtime_a.id();
    let runtime_b = AgentRuntime::new(engine_b);
    let rid_b = runtime_b.id();

    runtime_a
        .register_peer_tool("agent_b", "peer B", rid_b, sid_b)
        .unwrap();
    runtime_b
        .register_peer_tool("agent_a", "peer A", rid_a, sid_a)
        .unwrap();

    kernel
        .register(Box::new(runtime_a), 8, SupervisionPolicy::Transient)
        .await
        .expect("register A ok");
    kernel
        .register(Box::new(runtime_b), 8, SupervisionPolicy::Transient)
        .await
        .expect("register B ok");

    let start = Instant::now();
    let resp = kernel
        .invoke(rid_a, chat_msg(sid_a, "ping").to_envelope(), 10_000)
        .await
        .expect("invoke ok");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "cyclic call must not hang the system"
    );

    match decode_reply(&resp) {
        SessionReply::Success { message, .. } => {
            let content = message.content.as_text().unwrap_or("");
            assert!(
                content.to_lowercase().contains("busy"),
                "expected Busy rejection to propagate back, got: {content}"
            );
        }
        other => panic!("expected Success, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════
// 验收 3 — 工件访问控制（端到端）
// ═══════════════════════════════════════════════
#[tokio::test]
async fn artifact_acl_end_to_end() {
    let kernel = Kernel::new();
    let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());

    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();
    let sid_c = Uuid::new_v4();

    let long_text = format!("secret-{}", "x".repeat(5000));
    let rid_b = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![Behavior::Ok(mock_response(
            &long_text,
        ))])),
        vec![],
        referee_ai_base::tool::ToolExecutor::with_defaults(),
        Some(store.clone()),
    )
    .await;

    // 以 A 的身份执行 AgentTool（注入 ACL 工件存储；大结果落库 + 授权读者）
    let tool = AgentTool::new("agent_b", "peer", rid_b, sid_b).with_artifact_store(store.clone());
    let ctx = ToolContext {
        tool_call_id: "tc_peer".into(),
        session_id: sid_a,
        turn_id: 0,
        kernel: Some(kernel.clone()),
        store: None,
    };
    let out = tool
        .execute(ctx, json!({"task": "give me the secret"}))
        .await
        .expect("peer execute ok");

    let artifact_id = out
        .content
        .strip_prefix("Artifact created: ")
        .expect("large result must be stored as artifact")
        .to_string();

    // A（被授权读者）可读
    let got = store
        .get(&artifact_id, sid_a)
        .await
        .expect("read ok")
        .expect("artifact exists");
    assert_eq!(got.owner, sid_b, "owner must be the producing agent");
    // C（未授权）被拒
    let err = store
        .get(&artifact_id, sid_c)
        .await
        .expect_err("C must be denied");
    assert!(matches!(err, StoreError::PermissionDenied(_)));
}

// ═══════════════════════════════════════════════
// 验收 4 — 对等性注册（并行调用）
// ═══════════════════════════════════════════════
#[tokio::test]
async fn peer_registration_parallel() {
    let kernel = Kernel::new();
    let executor = referee_ai_base::tool::ToolExecutor::with_defaults();

    let sid_x = Uuid::new_v4();
    let sid_y = Uuid::new_v4();

    let provider_x = Arc::new(PeerMockProvider::new(vec![Behavior::OkDelayed(
        mock_response("x_done"),
        Duration::from_millis(200),
    )]));
    let provider_y = Arc::new(PeerMockProvider::new(vec![Behavior::OkDelayed(
        mock_response("y_done"),
        Duration::from_millis(200),
    )]));
    let rid_x = setup_runtime(&kernel, provider_x.clone(), vec![], executor.clone(), None).await;
    let rid_y = setup_runtime(&kernel, provider_y.clone(), vec![], executor.clone(), None).await;

    let sid_z = Uuid::new_v4();
    let engine_z = Engine::new(
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling both",
                vec![
                    make_tool_call("tc_x", "agent_x", r#"{"task":"job_x"}"#),
                    make_tool_call("tc_y", "agent_y", r#"{"task":"job_y"}"#),
                ],
            )),
            Behavior::EchoLastToolResult("z_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        executor.with_kernel(kernel.clone()),
    );
    let runtime = AgentRuntime::new(engine_z);
    runtime
        .register_peer_tool("agent_x", "peer X", rid_x, sid_x)
        .expect("register agent_x");
    runtime
        .register_peer_tool("agent_y", "peer Y", rid_y, sid_y)
        .expect("register agent_y");
    let rid_z = runtime.id();
    kernel
        .register(Box::new(runtime), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    let start = Instant::now();
    let resp = kernel
        .invoke(rid_z, chat_msg(sid_z, "both").to_envelope(), 10_000)
        .await
        .expect("invoke ok");
    let elapsed = start.elapsed();

    assert_eq!(provider_x.call_count(), 1, "X must be called once");
    assert_eq!(provider_y.call_count(), 1, "Y must be called once");
    assert!(
        matches!(decode_reply(&resp), SessionReply::Success { .. }),
        "Z must complete successfully"
    );
    assert!(
        elapsed < Duration::from_millis(380),
        "peers should be invoked in parallel, took {elapsed:?}"
    );
}

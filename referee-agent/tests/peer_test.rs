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
use referee_agent::artifact::{ArtifactStore, InMemoryArtifactStore};
use referee_agent::tool::AgentTool;
use referee_agent::AgentRuntime;
use referee_ai::engine::{Engine, EngineConfig};
use referee_ai::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities,
    ProviderId, StreamChunk, TokenUsage, ToolCall, ToolCallFunction,
};
use referee_ai::session::{ChatOptions, ChatPayload, SessionId, SessionMessage, SessionReply};
use referee_ai::tool::{ExecutorConfig, Tool, ToolContext, ToolRegistry};
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
    /// 检查请求上下文中是否已注入异步工具结果（user 消息含 Artifact ID）
    /// — 有则回 "injected"，否则回 fallback
    EchoAsyncInjection(&'static str),
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
        context_window_tokens: 4096,
        multimodal: referee_ai::provider::MultimodalCapabilities::NONE,
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
                    .find(|m| m.role == referee_ai::Role::Tool);
                let content = last_tool
                    .and_then(|m| m.content.as_text())
                    .unwrap_or(fallback);
                Ok(mock_response(content))
            }
            Behavior::EchoAsyncInjection(fallback) => {
                let injected = req.messages.iter().rev().any(|m| {
                    m.role == referee_ai::Role::User
                        && m.content
                            .as_text()
                            .map(|t| t.contains("artifact_id"))
                            .unwrap_or(false)
                });
                Ok(mock_response(if injected { "injected" } else { fallback }))
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
    ) -> Result<referee_ai::tool::ToolOutput, referee_ai::tool::ToolError> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(referee_ai::tool::ToolOutput::text("http_ok"))
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
            peer_depth: 0,
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
    executor: referee_ai::tool::ToolExecutor,
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
    let executor = referee_ai::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 2,
    });

    let rid_b = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling http",
                vec![make_tool_call("tc_http", "http_slow", r#"{"wait":true}"#)],
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
                vec![make_tool_call("tc_http", "http_slow", r#"{"wait":true}"#)],
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
                vec![make_tool_call(
                    "tc_peer",
                    "agent_b",
                    r#"{"task":"work","wait":true}"#,
                )],
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
                vec![make_tool_call(
                    "tc_peer",
                    "agent_d",
                    r#"{"task":"work","wait":true}"#,
                )],
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
                vec![make_tool_call(
                    "tc_ab",
                    "agent_b",
                    r#"{"task":"ping","wait":true}"#,
                )],
            )),
            Behavior::EchoLastToolResult("a_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        referee_ai::tool::ToolExecutor::with_defaults().with_kernel(kernel.clone()),
    );
    let engine_b = Engine::new(
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling agent_a",
                vec![make_tool_call(
                    "tc_ba",
                    "agent_a",
                    r#"{"task":"confirm","wait":true}"#,
                )],
            )),
            Behavior::EchoLastToolResult("b_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        referee_ai::tool::ToolExecutor::with_defaults().with_kernel(kernel.clone()),
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
// 验收 3 — 成果板端到端（父侧落库 + ID 凭证读取 + 父列自己板）
// ═══════════════════════════════════════════════
#[tokio::test]
async fn artifact_board_end_to_end() {
    let kernel = Kernel::new();
    let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());

    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();

    let long_text = format!("secret-{}", "x".repeat(5000));
    let rid_b = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![Behavior::Ok(mock_response(
            &long_text,
        ))])),
        vec![],
        referee_ai::tool::ToolExecutor::with_defaults(),
        Some(store.clone()),
    )
    .await;

    // 以 A 的身份执行 AgentTool：写入 A 的成果板，owner = B（子），返回结果 ID
    let tool = AgentTool::new("agent_b", "peer", rid_b, sid_b).with_artifact_store(store.clone());
    let ctx = ToolContext {
        tool_call_id: "tc_peer".into(),
        session_id: sid_a,
        turn_id: 0,
        kernel: Some(kernel.clone()),
        store: None,
        wait: false,
        peer_depth: 0,
    };
    let out = tool
        .execute(ctx, json!({"task": "give me the secret"}))
        .await
        .expect("peer execute ok");

    let parsed: Value = serde_json::from_str(&out.content).expect("artifact id json");
    let artifact_id = parsed["artifact_id"]
        .as_str()
        .expect("artifact_id")
        .to_string();

    // 凭证读取：持 ID 即可读正文
    let got = store
        .get(&artifact_id)
        .await
        .expect("read ok")
        .expect("artifact exists");
    assert_eq!(
        got.owner_session, sid_b,
        "owner must be the producing (child) agent"
    );
    assert_eq!(got.title, "give me the secret");

    // 父 A 列自己的板，看到该条目（按 seq 排序）
    let items = store.list_by_creator(sid_a).await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, artifact_id);
}

// ═══════════════════════════════════════════════
// 验收 4 — 对等性注册（并行调用）
// ═══════════════════════════════════════════════
#[tokio::test]
async fn peer_registration_parallel() {
    let kernel = Kernel::new();
    let executor = referee_ai::tool::ToolExecutor::with_defaults();

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
                    make_tool_call("tc_x", "agent_x", r#"{"task":"job_x","wait":true}"#),
                    make_tool_call("tc_y", "agent_y", r#"{"task":"job_y","wait":true}"#),
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

// ═══════════════════════════════════════════════
// 验收 5 — 异步派发：不等待的子智能体默认并行执行，主智能体不阻塞；
//          成果落库成果板，完成结果在**下一回合**自动注入上下文
// ═══════════════════════════════════════════════
#[tokio::test]
async fn async_dispatch_peer_result_injected_next_turn() {
    let kernel = Kernel::new();
    let store = Arc::new(InMemoryArtifactStore::with_defaults());
    let executor = referee_ai::tool::ToolExecutor::with_defaults();

    // 目标 Agent B：收到任务后延迟回复（模拟耗时子任务）
    let sid_b = Uuid::new_v4();
    let provider_b = Arc::new(PeerMockProvider::new(vec![Behavior::OkDelayed(
        mock_response("b_result"),
        Duration::from_millis(150),
    )]));
    let rid_b = setup_runtime(&kernel, provider_b, vec![], executor.clone(), None).await;

    // 主 Agent A：第一轮派发（未传 wait → 默认不等待），第二轮检查注入
    let sid_a = Uuid::new_v4();
    let provider_a = Arc::new(PeerMockProvider::new(vec![
        Behavior::Ok(mock_tool_response(
            "dispatching",
            vec![make_tool_call("tc_peer", "agent_b", r#"{"task":"do"}"#)],
        )),
        Behavior::EchoAsyncInjection("no_injection"),
    ]));
    let engine_a = Engine::new(provider_a.clone(), EngineConfig::default()).with_tools(
        ToolRegistry::with_defaults(),
        executor.with_kernel(kernel.clone()),
    );
    let runtime_a = AgentRuntime::new(engine_a).with_artifact_store(store.clone());
    runtime_a
        .register_peer_tool("agent_b", "peer B", rid_b, sid_b)
        .expect("register agent_b");
    let rid_a = runtime_a.id();
    kernel
        .register(Box::new(runtime_a), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    // 第一回合：不等待 → 立即返回模型原文，绝不阻塞等待 B 完成
    let start = Instant::now();
    let resp1 = kernel
        .invoke(rid_a, chat_msg(sid_a, "do it").to_envelope(), 10_000)
        .await
        .expect("invoke ok");
    let elapsed = start.elapsed();
    match decode_reply(&resp1) {
        SessionReply::Success { message, .. } => {
            assert_eq!(message.content.as_text().unwrap(), "dispatching")
        }
        other => panic!("expected immediate Success, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_millis(150),
        "main agent must not block on dispatched peer, took {elapsed:?}"
    );

    // 等待 B 完成后成果落库（后台派发收敛，轮询有界）
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && store.is_empty() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!store.is_empty(), "peer result must be stored as artifact");

    // 第二回合：注入结果应在本次请求上下文中（user 消息携带 Artifact ID）
    let resp2 = kernel
        .invoke(rid_a, chat_msg(sid_a, "continue").to_envelope(), 10_000)
        .await
        .expect("invoke ok");
    match decode_reply(&resp2) {
        SessionReply::Success { message, .. } => {
            assert_eq!(message.content.as_text().unwrap(), "injected")
        }
        other => panic!("expected injected Success, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════
// 验收 6 — 子智能体嵌套深度限制：主A → 子B → 附属C 允许，
//          C（深度达上限）无法再调子 Agent D，拒绝消息沿链回传
// ═══════════════════════════════════════════════
#[tokio::test]
async fn subagent_nesting_depth_limit_chain() {
    let kernel = Kernel::new();
    let executor = referee_ai::tool::ToolExecutor::with_defaults();

    // D：叶子 runtime（C 深度 2 无法触达，仅作为 agent_d 目标占位）
    let sid_d = Uuid::new_v4();
    let rid_d = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![Behavior::Ok(mock_response(
            "d_result",
        ))])),
        vec![],
        executor.clone(),
        None,
    )
    .await;

    // C：注册 agent_d；被 B 调用两次（第一次模型原文，第二次回显拒绝消息）
    let sid_c = Uuid::new_v4();
    let engine_c = Engine::new(
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "call d",
                vec![make_tool_call("tc_d", "agent_d", r#"{"task":"deep"}"#)],
            )),
            Behavior::EchoLastToolResult("c_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        executor.clone().with_kernel(kernel.clone()),
    );
    let runtime_c = AgentRuntime::new(engine_c);
    runtime_c
        .register_peer_tool("agent_d", "peer D", rid_d, sid_d)
        .expect("register agent_d");
    let rid_c = runtime_c.id();
    kernel
        .register(Box::new(runtime_c), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    // B：注册 agent_c；调用 C 两次（深度 1 允许调 C）
    let sid_b = Uuid::new_v4();
    let engine_b = Engine::new(
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "call c",
                vec![make_tool_call(
                    "tc_c",
                    "agent_c",
                    r#"{"task":"go","wait":true}"#,
                )],
            )),
            Behavior::Ok(mock_tool_response(
                "call c again",
                vec![make_tool_call(
                    "tc_c2",
                    "agent_c",
                    r#"{"task":"go2","wait":true}"#,
                )],
            )),
            Behavior::EchoLastToolResult("b_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        executor.clone().with_kernel(kernel.clone()),
    );
    let runtime_b = AgentRuntime::new(engine_b);
    runtime_b
        .register_peer_tool("agent_c", "peer C", rid_c, sid_c)
        .expect("register agent_c");
    let rid_b = runtime_b.id();
    kernel
        .register(Box::new(runtime_b), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    // A：注册 agent_b（深度 0 允许调 B）
    let sid_a = Uuid::new_v4();
    let engine_a = Engine::new(
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "call b",
                vec![make_tool_call(
                    "tc_b",
                    "agent_b",
                    r#"{"task":"root","wait":true}"#,
                )],
            )),
            Behavior::EchoLastToolResult("a_fallback"),
        ])),
        EngineConfig::default(),
    )
    .with_tools(
        ToolRegistry::with_defaults(),
        executor.clone().with_kernel(kernel.clone()),
    );
    let runtime_a = AgentRuntime::new(engine_a);
    runtime_a
        .register_peer_tool("agent_b", "peer B", rid_b, sid_b)
        .expect("register agent_b");
    let rid_a = runtime_a.id();
    kernel
        .register(Box::new(runtime_a), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    // 主 A 发起：A→B→C 链允许；C（深度 2）调 D 被拒，拒绝消息沿链回传至 A
    let resp = kernel
        .invoke(rid_a, chat_msg(sid_a, "root").to_envelope(), 10_000)
        .await
        .expect("invoke ok");
    match decode_reply(&resp) {
        SessionReply::Success { message, .. } => {
            let text = message.content.as_text().unwrap_or("");
            assert!(
                text.contains("subagent nesting depth limit"),
                "C must be blocked from calling D, got: {text}"
            );
        }
        other => panic!("expected Success, got {other:?}"),
    }
}

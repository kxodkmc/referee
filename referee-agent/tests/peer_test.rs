//! Phase 3 验收测试 — 对等智能体协作与安全工件存储
//!
//! 验收项（AGENT_RUNTIME_PLAN §5.3 对等协作路线 + 执行方案）：
//! 1. **资源池死锁修复**：AgentTool 为 Local 不占 IO 槽位，目标 Agent 的
//!    Remote 工具总能拿到槽位（验收 1）
//! 2. **循环调用拒绝**：A→B→A 被 Busy 拒绝，系统不挂死（验收 2，DAG 约束）
//! 3. **工件访问控制**：owner/授权读者可读，未授权者 PermissionDenied（验收 3）
//! 4. **对等性注册**：一个 Agent 持有多个对等工具并并行调用、结果汇聚（验收 4）

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::BoxStream;
use referee_agent::artifact::{ArtifactStore, InMemoryArtifactStore, StoreError};
use referee_agent::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities,
    ProviderId, Role, StreamChunk, TokenUsage, ToolCall, ToolCallFunction,
};
use referee_agent::session::{ChatOptions, ChatPayload, SessionId, SessionMessage, SessionReply};
use referee_agent::tool::{
    AgentTool, ExecutorConfig, Tool, ToolContext, ToolError, ToolExecutor, ToolRegistry,
};
use referee_agent::{AgentConfig, AgentRuntime};
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
    /// （用于观察目标 Agent 实际收到的工具执行结果）
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
        let mut guard = self.behaviors.lock();
        guard
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

#[async_trait]
impl LLMProvider for PeerMockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("peer-mock")
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        static CAPS: ProviderCapabilities = ProviderCapabilities {
            parallel_tool_calls: true,
            system_role: true,
            streaming: false,
            usage_reported: true,
            max_output_tokens: 4096,
        };
        &CAPS
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
                let last_tool = req.messages.iter().rev().find(|m| m.role == Role::Tool);
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
    ) -> Result<referee_agent::tool::ToolOutput, ToolError> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(referee_agent::tool::ToolOutput::text("http_ok"))
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

/// 注册一个 AgentRuntime（含工具注册表 + 执行器 + 可选工件存储）
async fn setup_runtime(
    kernel: &Kernel,
    provider: Arc<PeerMockProvider>,
    tools: Vec<Arc<dyn Tool>>,
    executor: ToolExecutor,
    store: Option<Arc<dyn ArtifactStore>>,
) -> CapabilityId {
    let registry = ToolRegistry::with_defaults();
    for tool in tools {
        registry.register(tool).unwrap();
    }

    let mut runtime = AgentRuntime::new(kernel.clone(), provider, AgentConfig::default());
    if let Some(s) = store {
        runtime = runtime.with_artifact_store(s);
    }
    let runtime = runtime.with_tools(registry, executor);

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
//
// 场景：ToolExecutor 最大并发 2。A、C 同时发起调用：
//   A —AgentTool(Local)→ B —http_slow(Remote)→ 300ms
//   C —AgentTool(Local)→ D —http_slow(Remote)→ 300ms
// 预期：A/C 的 AgentTool 不占槽位，B/D 的 http_slow 能拿到槽位，全部成功。
// 若 AgentTool 占用槽位（旧行为）：B/D 的 http_slow 拿不到 permit → 2s 超时，
// 断言最终回复为 "http_ok" 将失败。

#[tokio::test]
async fn resource_pool_deadlock_fixed() {
    let kernel = Kernel::new();
    let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());

    let executor = ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 2,
    });

    // B：先调 Remote HTTP 工具（300ms），再回显工具结果
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

    // D：同 B
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

    // A：调 agent_b；C：调 agent_d
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

    // A、C 同时发起调用
    let start = Instant::now();
    let (resp_a, resp_c) = tokio::join!(
        kernel.invoke(rid_a, chat_msg(sid_a, "task").to_envelope(), 10_000),
        kernel.invoke(rid_c, chat_msg(sid_c, "task").to_envelope(), 10_000),
    );

    let elapsed = start.elapsed();
    // 防挂死兜底：即使全部超时也不应无限等待
    assert!(
        elapsed < Duration::from_secs(5),
        "deadlock: peers took too long: {elapsed:?}"
    );

    let reply_a = decode_reply(&resp_a.expect("invoke A ok"));
    let reply_c = decode_reply(&resp_c.expect("invoke C ok"));

    // 核心断言：B/D 的 http_slow 成功执行（结果经 AgentTool 链路回显为最终回复）
    // 旧行为下该结果为 "tool execution timed out"
    match (reply_a, reply_c) {
        (SessionReply::Success { message, .. }, SessionReply::Success { message: msg_c, .. }) => {
            assert_eq!(
                message.content.as_text().unwrap(),
                "http_ok",
                "B's http tool must have executed successfully"
            );
            assert_eq!(
                msg_c.content.as_text().unwrap(),
                "http_ok",
                "D's http tool must have executed successfully"
            );
        }
        (ra, rc) => panic!("expected both Success, got {ra:?} / {rc:?}"),
    }
}

// ═══════════════════════════════════════════════
// 验收 2 — 逻辑死锁边界（循环调用被拒绝）
// ═══════════════════════════════════════════════
//
// 场景：A —AgentTool→ B，B —AgentTool→ A。
// A 调 B 时 A 处于 AwaitingCalls（Busy）；B 对 A 的 invoke 收到 Busy，
// 转为工具错误 → B 完成并回复 → A 完成。系统不挂死，循环调用被拒绝。

#[tokio::test]
async fn cyclic_call_rejected() {
    let kernel = Kernel::new();
    let executor = ToolExecutor::with_defaults();

    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();

    // A、B 互相需要对方的 runtime id：先构造 runtime 对象（未注册）交换 id，
    // 再配置工具并注册
    let runtime_a = AgentRuntime::new(
        kernel.clone(),
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling agent_b",
                vec![make_tool_call("tc_ab", "agent_b", r#"{"task":"ping"}"#)],
            )),
            Behavior::EchoLastToolResult("a_fallback"),
        ])),
        AgentConfig::default(),
    );
    let rid_a = runtime_a.id();
    let runtime_b = AgentRuntime::new(
        kernel.clone(),
        Arc::new(PeerMockProvider::new(vec![
            Behavior::Ok(mock_tool_response(
                "calling agent_a",
                vec![make_tool_call("tc_ba", "agent_a", r#"{"task":"confirm"}"#)],
            )),
            Behavior::EchoLastToolResult("b_fallback"),
        ])),
        AgentConfig::default(),
    );
    let rid_b = runtime_b.id();

    // A 持有 agent_b 工具（指向 B），B 持有 agent_a 工具（指向 A）
    let registry_a = ToolRegistry::with_defaults();
    registry_a
        .register(Arc::new(AgentTool::new("agent_b", "peer B", rid_b, sid_b)))
        .unwrap();
    let registry_b = ToolRegistry::with_defaults();
    registry_b
        .register(Arc::new(AgentTool::new("agent_a", "peer A", rid_a, sid_a)))
        .unwrap();

    kernel
        .register(
            Box::new(runtime_a.with_tools(registry_a, executor.clone())),
            8,
            SupervisionPolicy::Transient,
        )
        .await
        .expect("register A ok");
    kernel
        .register(
            Box::new(runtime_b.with_tools(registry_b, executor.clone())),
            8,
            SupervisionPolicy::Transient,
        )
        .await
        .expect("register B ok");

    // A 调 B → B 调 A → A 处于 AwaitingCalls 返回 Busy → 错误经 B 回传 A
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
//
// 场景：目标 Agent 返回超长结果 → AgentTool 落库为 Artifact（owner=目标
// Session，allowed_readers=调用者）。调用者 A 可读；恶意 C 读取被拒。

#[tokio::test]
async fn artifact_acl_end_to_end() {
    let kernel = Kernel::new();
    let store: Arc<dyn ArtifactStore> = Arc::new(InMemoryArtifactStore::with_defaults());

    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();
    let sid_c = Uuid::new_v4();

    // B 的 provider 返回超长文本（触发 >4096 落库）
    let long_text = format!("secret-{}", "x".repeat(5000));
    let rid_b = setup_runtime(
        &kernel,
        Arc::new(PeerMockProvider::new(vec![Behavior::Ok(mock_response(
            &long_text,
        ))])),
        vec![],
        ToolExecutor::with_defaults(),
        Some(store.clone()),
    )
    .await;

    // 以 A 的身份执行 AgentTool（手动构造 ToolContext，等价于 executor 注入）
    let tool = AgentTool::new("agent_b", "peer", rid_b, sid_b);
    let ctx = ToolContext {
        tool_call_id: "tc_peer".into(),
        session_id: sid_a,
        turn_id: 0,
        kernel: Some(kernel.clone()),
        artifact_store: Some(store.clone()),
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
// 验收 4 — 对等性注册
// ═══════════════════════════════════════════════
//
// 场景：Z 同时持有 tool_x / tool_y（X、Y 各自独立 Agent），并行调用，
// 结果正确汇聚。X、Y 各延迟 200ms 验证并行性。

#[tokio::test]
async fn peer_registration_parallel() {
    let kernel = Kernel::new();
    let executor = ToolExecutor::with_defaults();

    let sid_x = Uuid::new_v4();
    let sid_y = Uuid::new_v4();

    // X、Y：直接回复，各延迟 200ms
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

    // Z：注册 agent_x、agent_y，第一轮并行调用两者，第二轮回显
    let sid_z = Uuid::new_v4();
    let registry = ToolRegistry::with_defaults();
    let runtime = AgentRuntime::new(
        kernel.clone(),
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
        AgentConfig::default(),
    )
    .with_tools(registry, executor.clone());
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

    // X、Y 都被调用且成功（结果汇聚到 Z 的最终回复）
    assert_eq!(provider_x.call_count(), 1, "X must be called once");
    assert_eq!(provider_y.call_count(), 1, "Y must be called once");
    assert!(
        matches!(decode_reply(&resp), SessionReply::Success { .. }),
        "Z must complete successfully"
    );
    // 并行验证：200ms 延迟的双调用若串行则 ≥400ms，并行应明显更短
    assert!(
        elapsed < Duration::from_millis(380),
        "peers should be invoked in parallel, took {elapsed:?}"
    );
}

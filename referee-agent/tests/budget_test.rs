//! 预算治理验收测试
//!
//! 验收项（执行方案 §4，语义按软限制修正：允许最后一次超额，其后拒绝）：
//! 1. **会话级阻断**：累计达到 session_limit 后，该会话新请求被拒
//! 2. **全局级阻断**：累计达到 global_limit 后，任何会话新请求被拒
//! 3. **计量准确性**：usage.total_tokens 精确计入 Session 与全局
//! 4. **并发安全**：并发会话原子累加，无丢失
//! 5. **子 Agent 场景**（强化）：主 + 子 Agent 共享全局计数器，子任务消耗
//!    计入系统总预算，超限后主 Agent 新请求被拒
//! 6. **估算兜底**：厂商未返回 usage 时按响应文本保守估算（绝不计 0）

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use referee_agent::budget::BudgetConfig;
use referee_agent::cache::CacheConfig;
use referee_agent::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities,
    ProviderId, StreamChunk, TokenUsage, ToolCall, ToolCallFunction,
};
use referee_agent::session::{ChatOptions, ChatPayload, SessionId, SessionMessage, SessionReply};
use referee_agent::tool::{AgentTool, ToolExecutor, ToolRegistry};
use referee_agent::{AgentConfig, AgentRuntime};
use referee_core::{CapabilityId, Kernel, SupervisionPolicy};
use uuid::Uuid;

// ───────────────────────────────────────────────
// Mock Provider — 行为队列（预置响应）
// ───────────────────────────────────────────────

struct BudgetMockProvider {
    control: Mutex<VecDeque<ChatResponse>>,
}

impl BudgetMockProvider {
    fn new(responses: Vec<ChatResponse>) -> Self {
        Self {
            control: Mutex::new(responses.into()),
        }
    }
}

#[async_trait]
impl LLMProvider for BudgetMockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("budget-mock")
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

    async fn chat(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.control
            .lock()
            .pop_front()
            .ok_or_else(|| LlmError::BadRequest("out of behaviors".into()))
    }

    async fn chat_stream(
        &self,
        _req: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Err(LlmError::BadRequest("streaming not supported".into()))
    }
}

// ───────────────────────────────────────────────
// 辅助函数
// ───────────────────────────────────────────────

fn resp_with_usage(content: &str, total: usize) -> ChatResponse {
    ChatResponse {
        id: "t".into(),
        model: "mock".into(),
        message: Message::assistant(content),
        finish_reason: FinishReason::Stop,
        usage: Some(TokenUsage {
            prompt_tokens: total / 2,
            completion_tokens: total - total / 2,
            total_tokens: total,
            ..Default::default()
        }),
    }
}

fn resp_no_usage(text: &str) -> ChatResponse {
    ChatResponse {
        id: "t".into(),
        model: "mock".into(),
        message: Message::assistant(text),
        finish_reason: FinishReason::Stop,
        usage: None,
    }
}

fn tool_call_resp(content: &str, total: usize, name: &str) -> ChatResponse {
    let mut msg = Message::assistant(content);
    msg.tool_calls = vec![ToolCall {
        id: "tc_peer".into(),
        function: ToolCallFunction {
            name: name.into(),
            arguments: r#"{"task":"work"}"#.into(),
        },
    }];
    ChatResponse {
        id: "t".into(),
        model: "mock".into(),
        message: msg,
        finish_reason: FinishReason::ToolCalls,
        usage: Some(TokenUsage {
            prompt_tokens: total / 2,
            completion_tokens: total - total / 2,
            total_tokens: total,
            ..Default::default()
        }),
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

fn make_runtime(
    kernel: &Kernel,
    provider: Arc<BudgetMockProvider>,
    session_limit: u64,
    global_limit: u64,
    shared_counter: Option<Arc<AtomicU64>>,
) -> AgentRuntime {
    let mut runtime = AgentRuntime::new(
        kernel.clone(),
        provider,
        AgentConfig {
            session: Default::default(),
            max_sessions: 100,
            budget: BudgetConfig {
                session_limit,
                global_limit,
            },
            cache: CacheConfig::default(),
        },
    );
    if let Some(counter) = shared_counter {
        runtime = runtime.with_global_budget(counter);
    }
    runtime
}

async fn register_runtime(kernel: &Kernel, runtime: AgentRuntime) -> CapabilityId {
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    rid
}

// ═══════════════════════════════════════════════
// 验收 1 — 会话级阻断
// ═══════════════════════════════════════════════
//
// session_limit=100，每轮消耗 60（软限制语义：允许最后一次超额，其后拒绝）：
// 第 1 轮放行（60<100）→ 第 2 轮放行（累计 120，最后一次超额）→
// 第 3 轮起被拒。

#[tokio::test]
async fn session_level_block() {
    let kernel = Kernel::new();
    let provider = Arc::new(BudgetMockProvider::new(vec![
        resp_with_usage("round1", 60),
        resp_with_usage("round2", 60),
    ]));
    let runtime = make_runtime(&kernel, provider, 100, 0, None);
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime.clone()), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    let sid = Uuid::new_v4();

    // 第 1 轮：60 < 100 放行
    let r1 = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, "hello").to_envelope(), 5000)
            .await
            .expect("round1 invoke ok"),
    );
    assert!(matches!(r1, SessionReply::Success { .. }), "round1: {r1:?}");

    // 第 2 轮：检查时累计 60 < 100 → 放行（允许最后一次超额，本轮后累计 120）
    let r2 = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, "again").to_envelope(), 5000)
            .await
            .expect("round2 invoke ok"),
    );
    assert!(
        matches!(r2, SessionReply::Success { .. }),
        "round2 (last allowed overshoot) expected Success, got {r2:?}"
    );

    // 第 3 轮：累计 120 >= 100 → 拒绝（不消耗、不入 Thinking）
    let r3 = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, "again2").to_envelope(), 5000)
            .await
            .expect("round3 invoke ok"),
    );
    match r3 {
        SessionReply::Error { message } => {
            assert!(
                message.contains("Session budget exceeded"),
                "round3: {message}"
            );
        }
        other => panic!("round3 expected Error, got {other:?}"),
    }

    // 第 4 轮：仍被拒（累计未变，会话保持 Idle 未入 Thinking）
    let r4 = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, "again3").to_envelope(), 5000)
            .await
            .expect("round4 invoke ok"),
    );
    assert!(
        matches!(r4, SessionReply::Error { .. }),
        "round4 expected Error, got {r4:?}"
    );

    // 观测：session 累计 120（前两轮计入，被拒轮不计）
    tokio::task::yield_now().await;
    assert_eq!(runtime.session_consumed_tokens(sid), Some(120));
    assert_eq!(runtime.total_consumed_tokens(), 120);
}

// ═══════════════════════════════════════════════
// 验收 2 — 全局级阻断
// ═══════════════════════════════════════════════
//
// global_limit=500，预置消耗 500：任何会话（含新 Session）新请求被拒。

#[tokio::test]
async fn global_level_block() {
    let kernel = Kernel::new();
    let shared = Arc::new(AtomicU64::new(0));
    shared.store(500, Ordering::Relaxed); // 预置已消耗达到上限

    let provider = Arc::new(BudgetMockProvider::new(vec![]));
    let runtime = make_runtime(&kernel, provider, 0, 500, Some(shared));
    let rid = register_runtime(&kernel, runtime).await;

    let sid = Uuid::new_v4();
    let r = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, "hello").to_envelope(), 5000)
            .await
            .expect("invoke ok"),
    );
    match r {
        SessionReply::Error { message } => {
            assert!(message.contains("Global budget exceeded"), "{message}");
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════
// 验收 3 — 计量准确性
// ═══════════════════════════════════════════════
//
// usage.total_tokens=50 → Session 与全局各 +50（保留句柄验证观测方法）。

#[tokio::test]
async fn metering_accuracy() {
    let kernel = Kernel::new();
    let provider = Arc::new(BudgetMockProvider::new(vec![resp_with_usage("hi", 50)]));
    let runtime = make_runtime(&kernel, provider, 0, 0, None);
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime.clone()), 64, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    let sid = Uuid::new_v4();
    let r = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, "hello").to_envelope(), 5000)
            .await
            .expect("invoke ok"),
    );
    assert!(matches!(r, SessionReply::Success { .. }));

    // 让出调度，确保派生任务 converge 已更新计数器
    tokio::task::yield_now().await;
    assert_eq!(runtime.session_consumed_tokens(sid), Some(50));
    assert_eq!(runtime.total_consumed_tokens(), 50);
}
// ═══════════════════════════════════════════════
// 验收 4 — 并发安全
// ═══════════════════════════════════════════════
//
// 10 个并发 Session 各消耗 10 → 全局最终 = 100，无丢失。

#[tokio::test]
async fn concurrent_metering() {
    let kernel = Kernel::new();
    let responses: Vec<ChatResponse> = (0..10).map(|_| resp_with_usage("t", 10)).collect();
    let provider = Arc::new(BudgetMockProvider::new(responses));
    let runtime = make_runtime(&kernel, provider, 0, 0, None);
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime.clone()), 64, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    let mut handles = Vec::new();
    for i in 0..10 {
        let kernel = kernel.clone();
        handles.push(tokio::spawn(async move {
            let sid = Uuid::new_v4();
            // 并发计量测的是原子累加，各会话用不同内容避开语义缓存去重
            kernel
                .invoke(rid, chat_msg(sid, &format!("msg_{i}")).to_envelope(), 5000)
                .await
                .expect("invoke ok");
        }));
    }
    for h in handles {
        h.await.expect("task ok");
    }
    tokio::task::yield_now().await;

    assert_eq!(runtime.total_consumed_tokens(), 100);
}

// ═══════════════════════════════════════════════
// 验收 5 — 子 Agent 共享全局预算（针对子 Agent 场景）
// ═══════════════════════════════════════════════
//
// 主 Agent A 与子 Agent B 注入同一全局计数器（global_limit=100）。
// A 消耗 40 → 调 B（B 消耗 60）→ A resume 消耗 40 → 全局 140。
// 之后 A 的新请求被拒（Global budget exceeded）。

#[tokio::test]
async fn sub_agent_shared_global_budget() {
    let kernel = Kernel::new();
    let shared = Arc::new(AtomicU64::new(0));

    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();

    // 子 Agent B：直接回复，消耗 60
    let provider_b = Arc::new(BudgetMockProvider::new(vec![resp_with_usage("b_done", 60)]));
    let runtime_b = make_runtime(&kernel, provider_b, 0, 100, Some(shared.clone()));
    let rid_b = runtime_b.id();
    kernel
        .register(Box::new(runtime_b), 8, SupervisionPolicy::Transient)
        .await
        .expect("register B ok");

    // 主 Agent A：第一轮调 agent_b（消耗 40），resume 后回复（消耗 40）
    let provider_a = Arc::new(BudgetMockProvider::new(vec![
        tool_call_resp("calling", 40, "agent_b"),
        resp_with_usage("a_done", 40),
    ]));
    let registry = ToolRegistry::with_defaults();
    registry
        .register(Arc::new(AgentTool::new("agent_b", "peer", rid_b, sid_b)))
        .unwrap();
    let runtime_a = make_runtime(&kernel, provider_a, 0, 100, Some(shared.clone()))
        .with_tools(registry, ToolExecutor::with_defaults());
    let rid_a = runtime_a.id();
    kernel
        .register(Box::new(runtime_a.clone()), 8, SupervisionPolicy::Transient)
        .await
        .expect("register A ok");

    // A 发起任务：整条链（A→B→A resume）应全部完成
    let r = decode_reply(
        &kernel
            .invoke(rid_a, chat_msg(sid_a, "go").to_envelope(), 10_000)
            .await
            .expect("invoke A ok"),
    );
    assert!(matches!(r, SessionReply::Success { .. }), "{r:?}");

    tokio::task::yield_now().await;

    // 核心断言：子 Agent B 的消耗计入共享全局（40 + 60 + 40 = 140）
    assert_eq!(
        shared.load(Ordering::Relaxed),
        140,
        "sub-agent consumption must merge into the shared global budget"
    );
    assert_eq!(runtime_a.session_consumed_tokens(sid_a), Some(80));
    assert_eq!(runtime_a.total_consumed_tokens(), 140);

    // 超限后：A 的新请求被拒（140 >= 100）
    let r2 = decode_reply(
        &kernel
            .invoke(rid_a, chat_msg(sid_a, "more").to_envelope(), 5000)
            .await
            .expect("invoke A2 ok"),
    );
    match r2 {
        SessionReply::Error { message } => {
            assert!(message.contains("Global budget exceeded"), "{message}");
        }
        other => panic!("expected GlobalExceeded, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════
// 验收 6 — 估算兜底（厂商未返回 usage）
// ═══════════════════════════════════════════════
//
// 5000 字符响应无 usage → 保守估算约 3334，绝不计 0。

#[tokio::test]
async fn estimate_fallback_when_usage_missing() {
    let kernel = Kernel::new();
    let long = "x".repeat(5000);
    let provider = Arc::new(BudgetMockProvider::new(vec![resp_no_usage(&long)]));
    let runtime = make_runtime(&kernel, provider, 0, 0, None);
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime.clone()), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    let sid = Uuid::new_v4();
    let r = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, "hi").to_envelope(), 5000)
            .await
            .expect("invoke ok"),
    );
    assert!(matches!(r, SessionReply::Success { .. }));

    tokio::task::yield_now().await;
    let consumed = runtime.session_consumed_tokens(sid).unwrap();
    assert!(
        consumed > 0,
        "missing usage must fall back to a conservative estimate, got 0"
    );
    assert_eq!(consumed, 3334); // 5000*2/3 + 1
    assert_eq!(runtime.total_consumed_tokens(), 3334);
}

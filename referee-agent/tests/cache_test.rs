//! Phase 5 验收测试 — 提示词组装与缓存
//!
//! 验收项（执行方案 §4 + AGENT_RUNTIME_PLAN §5.6）：
//! 1. **命中**：相同输入二次调用 → 命中，LLM 调用计数 = 1（计数 mock 断言）
//! 2. **容量/TTL**：LRU 超限淘汰、get 刷新顺序、过期失效
//! 3. **预算截断**（prompt 模块单测，见 `src/prompt/mod.rs`）：按优先级截断，
//!    总量恒 ≤ 上限，中文截断无 panic
//! 4. **流式缓存**（cache 模块单测，见 `src/cache/mod.rs`）：合成流拼接 == 原文，
//!    Delta 块 + Finish 块（协议层无流式接口，验收 4 由合成流函数级断言）
//!
//! 额外断言：
//! - 含 tool_calls 的响应**不落缓存**（tool_call_id 是一次性 ID，重放会破坏工具流程）
//! - 缓存命中**不计量 Token**（未发生真实 LLM 调用，不占预算）
//! - 缓存可整体禁用（`CacheConfig::disabled`）

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
use referee_agent::{AgentConfig, AgentRuntime};
use referee_core::{Kernel, SupervisionPolicy};
use uuid::Uuid;

// ───────────────────────────────────────────────
// Counting Mock Provider — 记录调用次数 + 预置响应
// ───────────────────────────────────────────────

struct CountingProvider {
    calls: AtomicUsize,
    control: Mutex<VecDeque<ChatResponse>>,
    default_content: String,
}

impl CountingProvider {
    fn new(responses: Vec<ChatResponse>, default_content: &str) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            control: Mutex::new(responses.into()),
            default_content: default_content.into(),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl LLMProvider for CountingProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("cache-mock/model-v1")
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
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .control
            .lock()
            .pop_front()
            .unwrap_or_else(|| resp(&self.default_content, 50)))
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

fn resp(content: &str, total: usize) -> ChatResponse {
    ChatResponse {
        id: "t".into(),
        model: "mock".into(),
        message: Message::assistant(content),
        finish_reason: FinishReason::Stop,
        usage: Some(TokenUsage {
            prompt_tokens: 10,
            completion_tokens: total.saturating_sub(10),
            total_tokens: total,
            ..Default::default()
        }),
    }
}

fn tool_call_resp(content: &str) -> ChatResponse {
    let mut msg = Message::assistant(content);
    msg.tool_calls = vec![ToolCall {
        id: "tc_cached".into(),
        function: ToolCallFunction {
            name: "some_tool".into(),
            arguments: r#"{}"#.into(),
        },
    }];
    ChatResponse {
        id: "t".into(),
        model: "mock".into(),
        message: msg,
        finish_reason: FinishReason::ToolCalls,
        usage: Some(TokenUsage::default()),
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

/// 构造启用了缓存（capacity/ttl 可配）的 Runtime 并注册
async fn make_runtime(
    kernel: &Kernel,
    provider: Arc<CountingProvider>,
    capacity: usize,
    ttl: Duration,
) -> (AgentRuntime, referee_core::CapabilityId) {
    let runtime = AgentRuntime::new(
        kernel.clone(),
        provider,
        AgentConfig {
            session: Default::default(),
            max_sessions: 100,
            budget: BudgetConfig::unlimited(),
            cache: CacheConfig {
                enabled: true,
                capacity,
                ttl,
            },
        },
    );
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime.clone()), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    (runtime, rid)
}

/// 发送 chat 并返回解码后的回复
async fn send_chat(
    kernel: &Kernel,
    rid: referee_core::CapabilityId,
    content: &str,
) -> SessionReply {
    let sid = Uuid::new_v4();
    decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid, content).to_envelope(), 5000)
            .await
            .expect("invoke ok"),
    )
}

fn reply_content(reply: &SessionReply) -> String {
    match reply {
        SessionReply::Success { message, .. } => {
            message.content.as_text().unwrap_or("").to_string()
        }
        other => panic!("expected Success, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════
// 验收 1 — 命中：相同输入二次调用 → LLM 调用计数 = 1
// ═══════════════════════════════════════════════
//
// 两个独立 Session 发送完全相同的消息（history 均为单条 user 消息，
// 缓存键基于最终请求内容，跨会话可命中）。

#[tokio::test]
async fn cache_hit_skips_second_llm_call() {
    let kernel = Kernel::new();
    let provider = Arc::new(CountingProvider::new(vec![], "hello response"));
    let (_runtime, rid) =
        make_runtime(&kernel, provider.clone(), 16, Duration::from_secs(60)).await;

    let r1 = send_chat(&kernel, rid, "hello").await;
    assert_eq!(provider.call_count(), 1);
    assert_eq!(reply_content(&r1), "hello response");

    // 相同请求（新 Session，相同 history）→ 缓存命中
    let r2 = send_chat(&kernel, rid, "hello").await;
    assert_eq!(
        provider.call_count(),
        1,
        "second identical request must hit cache"
    );
    assert_eq!(reply_content(&r2), "hello response");

    // 不同请求 → 未命中
    let r3 = send_chat(&kernel, rid, "different").await;
    assert_eq!(provider.call_count(), 2);
    assert_eq!(reply_content(&r3), "hello response");
}

// ═══════════════════════════════════════════════
// 验收 2a — 容量淘汰（LRU）：capacity=2，A、B、C → A 被淘汰
// ═══════════════════════════════════════════════

#[tokio::test]
async fn lru_evicts_oldest_entry() {
    let kernel = Kernel::new();
    let provider = Arc::new(CountingProvider::new(vec![], "r"));
    let (_runtime, rid) = make_runtime(&kernel, provider.clone(), 2, Duration::from_secs(60)).await;

    assert!(matches!(
        send_chat(&kernel, rid, "a").await,
        SessionReply::Success { .. }
    ));
    assert!(matches!(
        send_chat(&kernel, rid, "b").await,
        SessionReply::Success { .. }
    ));
    assert!(matches!(
        send_chat(&kernel, rid, "c").await,
        SessionReply::Success { .. }
    ));
    assert_eq!(
        provider.call_count(),
        3,
        "three distinct requests must all call LLM"
    );

    // 插入 C 后最久未使用的 A 已被淘汰 → B 仍在缓存（命中）
    assert!(matches!(
        send_chat(&kernel, rid, "b").await,
        SessionReply::Success { .. }
    ));
    assert_eq!(provider.call_count(), 3, "B must still hit cache");

    // 重新请求 A → 未命中（A 已被淘汰）；其响应重写缓存（容量满 → 挤出 B）
    assert!(matches!(
        send_chat(&kernel, rid, "a").await,
        SessionReply::Success { .. }
    ));
    assert_eq!(provider.call_count(), 4, "evicted A must miss cache");
}

// ═══════════════════════════════════════════════
// 验收 2b — get 刷新 LRU 顺序：访问 A 后，淘汰的是 B 而非 A
// ═══════════════════════════════════════════════

#[tokio::test]
async fn get_refreshes_lru_order() {
    let kernel = Kernel::new();
    let provider = Arc::new(CountingProvider::new(vec![], "r"));
    let (_runtime, rid) = make_runtime(&kernel, provider.clone(), 2, Duration::from_secs(60)).await;

    send_chat(&kernel, rid, "a").await; // 1: 入缓存
    send_chat(&kernel, rid, "b").await; // 2: 入缓存
    send_chat(&kernel, rid, "a").await; // 命中 A（刷新顺序）
    assert_eq!(provider.call_count(), 2);

    send_chat(&kernel, rid, "c").await; // 3: 插入 C → 淘汰最久未用的 B
    send_chat(&kernel, rid, "b").await; // B 已淘汰 → 调用 LLM
    assert_eq!(
        provider.call_count(),
        4,
        "B must be evicted (A was refreshed)"
    );
}

// ═══════════════════════════════════════════════
// 验收 2c — TTL：过期失效
// ═══════════════════════════════════════════════

#[tokio::test]
async fn ttl_expiry_invalidates_entry() {
    let kernel = Kernel::new();
    let provider = Arc::new(CountingProvider::new(vec![], "r"));
    let (_runtime, rid) =
        make_runtime(&kernel, provider.clone(), 16, Duration::from_millis(30)).await;

    send_chat(&kernel, rid, "a").await;
    assert_eq!(provider.call_count(), 1);

    tokio::time::sleep(Duration::from_millis(80)).await;

    send_chat(&kernel, rid, "a").await;
    assert_eq!(provider.call_count(), 2, "expired entry must miss cache");
}

// ═══════════════════════════════════════════════
// 附加 — 含 tool_calls 的响应不落缓存
// ═══════════════════════════════════════════════

#[tokio::test]
async fn tool_call_responses_are_not_cached() {
    let kernel = Kernel::new();
    // 预置两条 tool_calls 响应：两次相同请求都应真实调用 LLM
    let provider = Arc::new(CountingProvider::new(
        vec![tool_call_resp("use tool"), tool_call_resp("use tool")],
        "fallback",
    ));
    let (_runtime, rid) =
        make_runtime(&kernel, provider.clone(), 16, Duration::from_secs(60)).await;

    assert!(matches!(
        send_chat(&kernel, rid, "a").await,
        SessionReply::Success { .. }
    ));
    assert!(matches!(
        send_chat(&kernel, rid, "a").await,
        SessionReply::Success { .. }
    ));
    assert_eq!(
        provider.call_count(),
        2,
        "tool_calls response must not be cached (tool_call_id is one-shot)"
    );
}

// ═══════════════════════════════════════════════
// 附加 — 缓存命中不计量 Token（不占 Session/全局预算）
// ═══════════════════════════════════════════════

#[tokio::test]
async fn cached_hit_does_not_charge_budget() {
    let kernel = Kernel::new();
    let provider = Arc::new(CountingProvider::new(vec![], "hello response"));
    let (runtime, rid) = make_runtime(&kernel, provider.clone(), 16, Duration::from_secs(60)).await;

    // 首次真实调用：计量 50 tokens
    let sid_a = Uuid::new_v4();
    let r1 = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid_a, "hello").to_envelope(), 5000)
            .await
            .expect("invoke ok"),
    );
    assert!(matches!(r1, SessionReply::Success { .. }));
    tokio::task::yield_now().await;
    assert_eq!(runtime.total_consumed_tokens(), 50);
    assert_eq!(runtime.session_consumed_tokens(sid_a), Some(50));

    // 缓存命中：不新增消耗
    let sid_b = Uuid::new_v4();
    let r2 = decode_reply(
        &kernel
            .invoke(rid, chat_msg(sid_b, "hello").to_envelope(), 5000)
            .await
            .expect("invoke ok"),
    );
    assert!(matches!(r2, SessionReply::Success { .. }));
    tokio::task::yield_now().await;
    assert_eq!(
        runtime.total_consumed_tokens(),
        50,
        "cache hit must not charge budget"
    );
    assert_eq!(
        runtime.session_consumed_tokens(sid_b),
        Some(0),
        "cached session must not be charged"
    );
}

// ═══════════════════════════════════════════════
// 附加 — 缓存可整体禁用
// ═══════════════════════════════════════════════

#[tokio::test]
async fn cache_can_be_disabled() {
    let kernel = Kernel::new();
    let provider = Arc::new(CountingProvider::new(vec![], "r"));
    let runtime = AgentRuntime::new(
        kernel.clone(),
        provider.clone(),
        AgentConfig {
            session: Default::default(),
            max_sessions: 100,
            budget: BudgetConfig::unlimited(),
            cache: CacheConfig::disabled(),
        },
    );
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime.clone()), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");

    assert_eq!(runtime.cache_len(), 0);
    send_chat(&kernel, rid, "a").await;
    send_chat(&kernel, rid, "a").await;
    assert_eq!(provider.call_count(), 2, "disabled cache must never hit");
}

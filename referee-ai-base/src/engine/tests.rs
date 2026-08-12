//! 引擎测试 — 最小闭环、并发正确性、超时防护
//!
//! 所有可能挂起的等待均用 `tokio::time::timeout` 包裹（测试不锁死）。
//! 覆盖：最小闭环 / 多轮工具 / busy 拒绝 / 中断 / 超时 / 预算 / 缓存 / panic 隔离。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::provider::{
    ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities, ProviderId,
    StreamChunk, TokenUsage,
};
use crate::session::{ChatOptions, ChatPayload, SessionConfig, TimeoutConfig};
use crate::tool::{
    ExecutorConfig, RegistryConfig, Tool, ToolContext, ToolError, ToolOutput, ToolRegistry,
};
use crate::{
    budget::BudgetConfig, ChatHandle, Engine, EngineConfig, EngineReply, EngineStartError,
};
use futures::stream::{self, BoxStream};

// ───────────────────────────────────────────────
// Mock 提供器
// ───────────────────────────────────────────────

struct MockProvider {
    id: &'static str,
    responses: Arc<parking_lot::Mutex<VecDeque<Result<ChatResponse, LlmError>>>>,
    call_count: Arc<AtomicUsize>,
}

fn caps() -> &'static ProviderCapabilities {
    static C: ProviderCapabilities = ProviderCapabilities {
        parallel_tool_calls: true,
        system_role: true,
        streaming: false,
        usage_reported: true,
        max_output_tokens: 1024,
    };
    &C
}

fn resp(text: &str, tool_calls: Vec<crate::provider::ToolCall>) -> ChatResponse {
    let has_tools = !tool_calls.is_empty();
    ChatResponse {
        id: "mock".into(),
        model: "mock".into(),
        message: Message {
            role: crate::provider::Role::Assistant,
            content: crate::provider::MessageContent::text(text),
            reasoning_content: None,
            tool_calls,
            tool_call_id: None,
        },
        finish_reason: if has_tools {
            FinishReason::ToolCalls
        } else {
            FinishReason::Stop
        },
        usage: Some(TokenUsage {
            prompt_tokens: 5,
            completion_tokens: 3,
            total_tokens: 8,
            ..Default::default()
        }),
    }
}

fn tool_call(name: &str) -> crate::provider::ToolCall {
    crate::provider::ToolCall {
        id: format!("tc_{name}"),
        function: crate::provider::ToolCallFunction {
            name: name.to_string(),
            arguments: "{}".to_string(),
        },
    }
}

fn mock(responses: Vec<Result<ChatResponse, LlmError>>) -> Arc<MockProvider> {
    Arc::new(MockProvider {
        id: "mock",
        responses: Arc::new(parking_lot::Mutex::new(responses.into())),
        call_count: Arc::new(AtomicUsize::new(0)),
    })
}

#[async_trait::async_trait]
impl LLMProvider for MockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new(self.id)
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        caps()
    }
    async fn chat(&self, _req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.responses
            .lock()
            .pop_front()
            .unwrap_or_else(|| Err(LlmError::Protocol("no more mock responses".into())))
    }
    async fn chat_stream(
        &self,
        _req: crate::provider::ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Ok(Box::pin(stream::empty()))
    }
}

// 带超时的 wait
async fn wait_with_timeout(handle: ChatHandle) -> EngineReply {
    tokio::time::timeout(Duration::from_secs(5), handle.wait())
        .await
        .expect("chat must not hang")
        .expect("reply channel closed unexpectedly")
}

fn chat_payload(text: &str) -> ChatPayload {
    ChatPayload {
        message: Message::user(text),
        options: ChatOptions::default(),
    }
}

fn config() -> EngineConfig {
    EngineConfig {
        session: SessionConfig {
            timeout: TimeoutConfig {
                thinking_timeout: Duration::from_secs(5),
                awaiting_calls_timeout: Duration::from_secs(5),
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

fn session_id() -> crate::SessionId {
    uuid::Uuid::new_v4()
}

// ───────────────────────────────────────────────
// 测试
// ───────────────────────────────────────────────

#[tokio::test]
async fn minimal_loop_success() {
    // 「接 LLM → 组装 prompt → 回复」最小闭环
    let engine = Engine::new(mock(vec![Ok(resp("hello there", vec![]))]), config());
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("hi")).unwrap()).await;
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "hello there"),
        other => panic!("expected Success, got {other:?}"),
    }
}

#[tokio::test]
async fn multi_turn_tool_loop() {
    // 模型发起工具调用 → 执行工具 → resume → 最终回复
    #[async_trait::async_trait]
    impl Tool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }
        fn description(&self) -> &str {
            "echo"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object", "properties": {"x": {"type":"string"}}})
        }
        async fn execute(
            &self,
            _ctx: ToolContext,
            args: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text(
                args.get("x")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            ))
        }
    }
    struct EchoTool;

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(EchoTool)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 5,
    });

    // 第一轮：tool_calls；第二轮：最终回复
    let provider = mock(vec![
        Ok(resp("", vec![tool_call("echo")])),
        Ok(resp("done", vec![])),
    ]);
    let engine = Engine::new(provider, config()).with_tools(registry, executor);

    let reply = wait_with_timeout(
        engine
            .chat(session_id(), chat_payload("call echo"))
            .unwrap(),
    )
    .await;
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "done"),
        other => panic!("expected Success, got {other:?}"),
    }
}

#[tokio::test]
async fn busy_rejected() {
    // LLM 永不返回（pending）→ 第二次 chat 应显式拒绝
    let engine = Engine::new(Arc::new(PendingProvider), config());
    let sid = session_id();
    let handle = engine.chat(sid, chat_payload("a")).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    match engine.chat(sid, chat_payload("b")) {
        Err(EngineStartError::Busy) => {}
        other => panic!("expected Busy, got {other:?}"),
    }
    handle.cancel();
}

#[tokio::test]
async fn interrupt_cancels_pending() {
    let engine = Engine::new(Arc::new(PendingProvider), config());
    let sid = session_id();
    let handle = engine.chat(sid, chat_payload("a")).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.cancel();
    let reply = wait_with_timeout(handle).await;
    assert!(matches!(reply, EngineReply::Cancelled));
}

#[tokio::test]
async fn timeout_returns() {
    // 超时防护：LLM 挂死 → 返回 Timeout，不阻塞
    let mut cfg = config();
    cfg.session.timeout.thinking_timeout = Duration::from_millis(50);
    let engine = Engine::new(Arc::new(PendingProvider), cfg);
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("a")).unwrap()).await;
    assert!(matches!(reply, EngineReply::Timeout) || matches!(reply, EngineReply::Error(_)));
}

#[tokio::test]
async fn budget_session_rejected() {
    // 预算：session_limit 超限 → 拒绝
    let mut cfg = config();
    cfg.budget = BudgetConfig {
        session_limit: 5, // 第一轮 usage 8 已超（软限制），第二轮拒绝
        global_limit: 1_000_000,
    };
    let engine = Engine::new(mock(vec![Ok(resp("x", vec![]))]), cfg.clone());
    let sid = session_id();
    wait_with_timeout(engine.chat(sid, chat_payload("a")).unwrap()).await;
    // 第二轮 → 预算拒绝
    match engine.chat(sid, chat_payload("b")) {
        Err(EngineStartError::Budget(_)) => {}
        other => panic!("expected Budget error, got {other:?}"),
    }
}

#[tokio::test]
async fn cache_hit_skips_llm() {
    // 缓存：两个不同会话发送完全相同的请求 → 第二次命中，LLM 仅调用 1 次
    let provider = mock(vec![Ok(resp("same", vec![]))]);
    let engine = Engine::new(provider.clone(), config());

    let s1 = session_id();
    let s2 = session_id();
    wait_with_timeout(engine.chat(s1, chat_payload("duplicate")).unwrap()).await;
    let reply = wait_with_timeout(engine.chat(s2, chat_payload("duplicate")).unwrap()).await;
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "same"),
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(
        provider.call_count.load(Ordering::SeqCst),
        1,
        "cache must skip second LLM call"
    );
}

#[tokio::test]
async fn tool_panic_isolated() {
    #[async_trait::async_trait]
    impl Tool for PanicTool {
        fn name(&self) -> &str {
            "boom"
        }
        fn description(&self) -> &str {
            "panics"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            panic!("boom");
        }
    }
    struct PanicTool;

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(PanicTool)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 5,
    });
    // 第一轮 tool_calls(boom)，第二轮最终回复
    let provider = mock(vec![
        Ok(resp("", vec![tool_call("boom")])),
        Ok(resp("after panic", vec![])),
    ]);
    let engine = Engine::new(provider, config()).with_tools(registry, executor);
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("go")).unwrap()).await;
    // 工具 panic 被隔离，回合仍能继续并成功
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "after panic"),
        other => panic!("expected Success after isolated panic, got {other:?}"),
    }
}

#[tokio::test]
async fn max_sessions_rejected() {
    let mut cfg = config();
    cfg.max_sessions = 1;
    let engine = Engine::new(Arc::new(PendingProvider), cfg);
    let s1 = session_id();
    let s2 = session_id();
    let _h1 = engine.chat(s1, chat_payload("a")).unwrap();
    // 第二会话超限
    assert!(matches!(
        engine.chat(s2, chat_payload("b")),
        Err(EngineStartError::MaxSessions)
    ));
    // 清除 pending 会话
    engine.interrupt(s1);
}

// Pending 提供器：永不返回（模拟 LLM 挂死）
struct PendingProvider;
#[async_trait::async_trait]
impl LLMProvider for PendingProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("pending")
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        caps()
    }
    async fn chat(&self, _req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        std::future::pending::<Result<ChatResponse, LlmError>>().await
    }
    async fn chat_stream(
        &self,
        _req: crate::provider::ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Ok(Box::pin(stream::empty()))
    }
}

#[tokio::test]
async fn multi_turn_accumulates_all_round_tokens() {
    // 预算全局口径：两轮（工具轮 + 最终轮）每轮 usage.total=8，全局应累计 16
    #[async_trait::async_trait]
    impl Tool for AccumEcho {
        fn name(&self) -> &str {
            "accum_echo"
        }
        fn description(&self) -> &str {
            "echo"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("ok"))
        }
    }
    struct AccumEcho;

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(AccumEcho)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 5,
    });
    let provider = mock(vec![
        Ok(resp("", vec![tool_call("accum_echo")])),
        Ok(resp("done", vec![])),
    ]);
    let engine = Engine::new(provider, config()).with_tools(registry, executor);
    wait_with_timeout(engine.chat(session_id(), chat_payload("go")).unwrap()).await;
    // 每轮真实成功总 usage 8 → 两轮累计 16
    assert_eq!(
        engine.total_consumed_tokens(),
        16,
        "every round must count toward global budget"
    );
}

#[tokio::test]
async fn concurrent_chat_same_session_only_one_ok() {
    // H1 回归：同一 session 连续两个 Chat，仅第一个成功（StartRound 原子置
    // Thinking），第二个显式 Busy；且被拒的 Chat **不会**污染 history。
    let engine = Engine::new(Arc::new(PendingProvider), config());
    let sid = session_id();

    let (r1, r2) = (
        engine.chat(sid, chat_payload("a")),
        engine.chat(sid, chat_payload("b")),
    );
    let (handle, busy) = match (r1, r2) {
        (Ok(h), Err(EngineStartError::Busy)) => (h, true),
        (Err(EngineStartError::Busy), Ok(h)) => (h, true),
        o => panic!("expected exactly one Ok + one Busy, got {o:?}"),
    };
    assert!(busy, "second concurrent chat must be rejected as Busy");

    // 被拒的 Chat 不应把消息写进 history（不重复污染）
    assert_eq!(
        engine.history_len(sid),
        Some(1),
        "only the first (accepted) chat should appear in history"
    );

    handle.cancel();
}

#[tokio::test]
async fn many_new_sessions_start_safely() {
    // 多个新 session 连续启动不挂起（ensure_session 经 DashMap or_insert 路径，
    // 不触发裸 entry 的 shrink 死锁）。
    let engine = Engine::new(Arc::new(PendingProvider), config());
    let sids: Vec<_> = (0..8).map(|_| session_id()).collect();
    let handles: Vec<_> = sids
        .iter()
        .map(|sid| engine.chat(*sid, chat_payload("hi")))
        .map(|r| r.expect("must start for fresh session"))
        .collect();
    for h in &handles {
        h.cancel();
    }
    assert_eq!(engine.session_count(), 8);
}

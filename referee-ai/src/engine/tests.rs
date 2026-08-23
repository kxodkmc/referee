//! 引擎测试 — 最小闭环、并发正确性、超时防护
//!
//! 所有可能挂起的等待均用 `tokio::time::timeout` 包裹（测试不锁死）。
//! 覆盖：最小闭环 / 多轮工具 / busy 拒绝 / 中断 / 超时 / 预算 / 缓存 / panic 隔离。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::provider::{
    ChatResponse, FinishReason, LLMProvider, LlmError, Message, ModelSpec, ProviderCapabilities,
    ProviderId, StreamChunk, TokenUsage,
};
use crate::session::{ChatOptions, ChatPayload, ErrorKind, SessionConfig, SessionReply, TimeoutConfig};
use crate::tool::{
    ExecutorConfig, RegistryConfig, Tool, ToolContext, ToolError, ToolOutcome, ToolOutput,
    ToolRegistry,
};
use crate::{
    budget::{BudgetConfig, BudgetError}, ChatHandle, Engine, EngineConfig, EngineError,
    EngineObserver, EngineReply, EngineStartError, SessionPhase,
};
use futures::stream::{self, BoxStream};
use futures::StreamExt;

// ───────────────────────────────────────────────
// Mock 提供器
// ───────────────────────────────────────────────

struct MockProvider {
    id: &'static str,
    responses: Arc<parking_lot::Mutex<VecDeque<Result<ChatResponse, LlmError>>>>,
    call_count: Arc<AtomicUsize>,
    /// 每次请求的 messages 文本（验证异步注入等请求内容）
    requests: Arc<parking_lot::Mutex<Vec<String>>>,
    /// 每次请求的工具声明名（逗号分隔；验证嵌套深度过滤）
    requests_tools: Arc<parking_lot::Mutex<Vec<String>>>,
}

fn caps() -> &'static ProviderCapabilities {
    static C: ProviderCapabilities = ProviderCapabilities {
        parallel_tool_calls: true,
        system_role: true,
        streaming: false,
        usage_reported: true,
        multimodal: crate::provider::MultimodalCapabilities::NONE,
    };
    &C
}

/// mock 模型规格（上下文窗口 / 最大输出一致）
fn model_spec() -> ModelSpec {
    ModelSpec {
        context_window_tokens: 1024,
        max_output_tokens: 1024,
    }
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
            usage: None,
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
        requests: Arc::new(parking_lot::Mutex::new(Vec::new())),
        requests_tools: Arc::new(parking_lot::Mutex::new(Vec::new())),
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

    fn model_spec(&self) -> ModelSpec {
        model_spec()
    }
    async fn chat(&self, req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let text = req
            .messages
            .iter()
            .map(|m| m.content.as_text().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        self.requests.lock().push(text);
        let tools = req
            .tools
            .iter()
            .map(|t| t.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        self.requests_tools.lock().push(tools);
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
        peer_depth: 0,
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
        // 工具配置默认等待：本测试验证同步多轮收敛
        fn default_wait(&self) -> bool {
            true
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
        // 工具配置默认等待：本测试验证同步多轮收敛
        fn default_wait(&self) -> bool {
            true
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

    fn model_spec(&self) -> ModelSpec {
        model_spec()
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
        // 工具配置默认等待：本测试验证同步多轮收敛
        fn default_wait(&self) -> bool {
            true
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

#[tokio::test]
async fn interrupt_semantics_idle_vs_active() {
    // M1 语义：空闲会话不可取消（返回 false）；活动回合可取消（返回 true）；
    // 已取消但回合未终结时仍可再次取消（语义清晰，不分「首次/重复」）。
    let engine = Engine::new(Arc::new(PendingProvider), config());
    let sid = session_id();

    // 从未 chat → 空闲，interrupt 返回 false（不误报「已取消」）
    assert!(
        !engine.interrupt(sid),
        "idle session must not report interruptible"
    );

    let handle = engine.chat(sid, chat_payload("a")).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await; // 进入 Thinking

    assert!(engine.interrupt(sid), "active round must be interruptible");
    // 仍在 Thinking（等待收敛）→ 再中断仍返回 true
    assert!(
        engine.interrupt(sid),
        "still thinking → interrupt still true"
    );

    let reply = wait_with_timeout(handle).await;
    assert!(matches!(reply, EngineReply::Cancelled), "got {reply:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_burst_same_session_strictly_one() {
    // H1 强并发边界：8 线程 × 16 路 barrier 同步同时 chat **同一 session**。
    // start_round 在单一 guard 内原子置 Thinking → 必须严格恰一个成功、其余 Busy，
    // 且被拒的 Chat 不得污染 history。
    let engine = Arc::new(Engine::new(Arc::new(PendingProvider), config()));
    let sid = session_id();
    let barrier = Arc::new(tokio::sync::Barrier::new(16));

    let tasks: Vec<_> = (0..16)
        .map(|i| {
            let e = engine.clone();
            let b = barrier.clone();
            tokio::spawn(async move {
                b.wait().await;
                e.chat(sid, chat_payload(&format!("m{i}")))
            })
        })
        .collect();

    let mut ok = 0usize;
    let mut busy = 0usize;
    let mut handles = Vec::new();
    let mut others = Vec::new();
    for t in tasks {
        match t.await.expect("task panicked") {
            Ok(h) => {
                ok += 1;
                handles.push(h);
            }
            Err(EngineStartError::Busy) => busy += 1,
            Err(o) => others.push(format!("{o:?}")),
        }
    }

    assert_eq!(ok, 1, "exactly one chat must start, got {ok}");
    assert_eq!(busy, 15, "all the rest must be Busy, got {busy}");
    assert!(others.is_empty(), "unexpected errors: {others:?}");
    assert_eq!(
        engine.history_len(sid),
        Some(1),
        "rejected concurrent chats must not pollute history"
    );

    for h in handles {
        h.cancel();
    }
}

#[tokio::test]
async fn session_reusable_after_cancel() {
    // 中断收敛后会话回到 Idle，不永久卡 busy：可再次发起 Chat 并再次取消。
    let engine = Engine::new(Arc::new(PendingProvider), config());
    let sid = session_id();

    let h1 = engine.chat(sid, chat_payload("first")).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    h1.cancel();
    assert!(matches!(
        wait_with_timeout(h1).await,
        EngineReply::Cancelled
    ));

    // 会话应已复位 → 第二轮可正常启动
    assert!(!engine.interrupt(sid), "idle after cancel");
    let h2 = engine.chat(sid, chat_payload("second")).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(engine.interrupt(sid), "second round active");
    let h2b = h2;
    // wait 由 interrupt 驱动收敛
    h2b.cancel();
    assert!(matches!(
        wait_with_timeout(h2b).await,
        EngineReply::Cancelled
    ));

    // history 记录了两轮用户消息（被拒的除外），不因中断而丢失状态一致
    assert_eq!(engine.history_len(sid), Some(2));
}

// ─────────────────────────────────────────────
// 异步派发（不等待工具）— 端到端：纯派发轮立即结束，结果下一回合注入
// ─────────────────────────────────────────────

#[tokio::test]
async fn async_dispatch_injected_on_next_round() {
    // 默认不等待（未覆写 default_wait）的慢工具
    #[async_trait::async_trait]
    impl Tool for SlowAsyncTool {
        fn name(&self) -> &str {
            "slow_async"
        }
        fn description(&self) -> &str {
            "slow async tool"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_millis(150)).await;
            Ok(ToolOutput::text("async_result"))
        }
    }
    struct SlowAsyncTool;

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(SlowAsyncTool)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 5,
    });
    let provider = mock(vec![
        Ok(resp("dispatching", vec![tool_call("slow_async")])),
        Ok(resp("done", vec![])),
    ]);
    let engine = Engine::new(provider.clone(), config()).with_tools(registry, executor);
    let sid = session_id();

    // 第一回合：不等待 → 立即返回模型原文，绝不阻塞 150ms 慢工具
    let start = std::time::Instant::now();
    let r1 = wait_with_timeout(engine.chat(sid, chat_payload("go")).unwrap()).await;
    let elapsed = start.elapsed();
    match r1 {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "dispatching"),
        other => panic!("expected immediate Success, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_millis(150),
        "main agent must not block on async tool, took {elapsed:?}"
    );

    // 等待后台完成并注入（工具 150ms + 余量，纯内存注入即时生效）
    tokio::time::sleep(Duration::from_millis(350)).await;

    // 第二回合：注入结果应出现在本次请求上下文中（不为此主动触发 LLM）
    let r2 = wait_with_timeout(engine.chat(sid, chat_payload("again")).unwrap()).await;
    assert!(matches!(
        r2,
        EngineReply::Success(r) if r.message.content.as_text().unwrap() == "done"
    ));
    let reqs = provider.requests.lock();
    let last = reqs.last().expect("second request recorded");
    assert!(
        last.contains("async_result"),
        "async result must be injected into next-round context: {last}"
    );
    assert!(
        last.contains("[async tool 'slow_async' completed]"),
        "injection must carry tool name: {last}"
    );
}

// ─────────────────────────────────────────────
// 子智能体嵌套深度限制 — 声明过滤 + 执行兜底
// ─────────────────────────────────────────────

#[tokio::test]
async fn subagent_depth_limit_enforced() {
    // 深度受限工具（模拟子 Agent 工具）
    #[async_trait::async_trait]
    impl Tool for SubAgentTool {
        fn name(&self) -> &str {
            "sub"
        }
        fn description(&self) -> &str {
            "subagent tool"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        fn depth_limited(&self) -> bool {
            true
        }
        // 短查询类工具默认同步等待（聚焦深度限制语义）
        fn default_wait(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            Ok(ToolOutput::text("sub_ok"))
        }
    }
    struct SubAgentTool;

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(SubAgentTool)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 5,
    });
    let config = EngineConfig {
        max_subagent_depth: 2,
        ..config()
    };
    let provider = mock(vec![
        // B 层（深度 1）：调用 sub 应执行 → resume 第二轮
        Ok(resp("call sub", vec![tool_call("sub")])),
        Ok(resp("b done", vec![])),
        // C 层（深度 2）：发起 sub 调用应被拒绝 → 回合直接结束（仅一轮）
        Ok(resp("call sub again", vec![tool_call("sub")])),
    ]);
    let engine = Engine::new(provider.clone(), config).with_tools(registry, executor);

    // B 层（peer_depth=1）：声明含 sub，调用成功（两轮 LLM）
    let sid_b = session_id();
    let payload = ChatPayload {
        message: Message::user("go"),
        options: ChatOptions::default(),
        peer_depth: 1,
    };
    let r1 = wait_with_timeout(engine.chat(sid_b, payload).unwrap()).await;
    assert!(matches!(
        r1,
        EngineReply::Success(r) if r.message.content.as_text().unwrap() == "b done"
    ));
    assert!(
        provider.requests_tools.lock()[0].contains("sub"),
        "depth 1 must keep subagent tool declaration"
    );

    // C 层（peer_depth=2）：声明剔除 sub；即便发起调用也被拒绝（仅一轮，无 resume）
    let sid_c = session_id();
    let payload = ChatPayload {
        message: Message::user("go"),
        options: ChatOptions::default(),
        peer_depth: 2,
    };
    let r2 = wait_with_timeout(engine.chat(sid_c, payload).unwrap()).await;
    assert!(matches!(
        r2,
        EngineReply::Success(r) if r.message.content.as_text().unwrap() == "call sub again"
    ));
    assert_eq!(
        provider.call_count.load(Ordering::SeqCst),
        3,
        "depth-limited call must be rejected without a resume round"
    );
    assert!(
        !provider
            .requests_tools
            .lock()
            .last()
            .unwrap()
            .contains("sub"),
        "depth 2 must drop subagent tool declaration"
    );
}

// ─────────────────────────────────────────────
// 会话生命周期 + 空闲回收（3.2 / 4.2）
// ─────────────────────────────────────────────

#[tokio::test]
async fn session_lifecycle_remove_list_info() {
    let engine = Engine::new(Arc::new(PendingProvider), config());
    let s1 = session_id();
    let s2 = session_id();
    engine.chat(s1, chat_payload("a")).unwrap();
    engine.chat(s2, chat_payload("b")).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let listed = engine.list_sessions();
    assert_eq!(listed.len(), 2);

    let info = engine.session_info(s1).expect("session must exist");
    assert_eq!(info.state, SessionPhase::Thinking);
    assert_eq!(info.history_len, 1);

    // 移除正在 Thinking 的会话不 panic（DashMap remove 安全）
    assert!(engine.remove_session(s1));
    assert!(!engine.remove_session(s1));
    assert_eq!(engine.session_count(), 1);
    assert!(engine.session_info(s1).is_none());
    assert!(engine.session_info(s2).is_some());

    engine.interrupt(s2);
}

#[tokio::test]
async fn idle_reaper_removes_stale_sessions() {
    let mut cfg = config();
    cfg.session.idle_timeout = Some(Duration::from_millis(100));
    let engine = Engine::new(
        mock(vec![Ok(resp("ok", vec![])), Ok(resp("ok", vec![]))]),
        cfg,
    );
    let s1 = session_id();
    let s2 = session_id();
    wait_with_timeout(engine.chat(s1, chat_payload("a")).unwrap()).await;
    wait_with_timeout(engine.chat(s2, chat_payload("b")).unwrap()).await;

    let reaper = engine.start_idle_reaper().expect("reaper must start");
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(engine.session_count(), 0, "idle sessions must be reaped");
    reaper.stop();
}

#[tokio::test]
async fn idle_reaper_skips_active_sessions() {
    let mut cfg = config();
    cfg.session.idle_timeout = Some(Duration::from_millis(100));
    let engine = Engine::new(Arc::new(PendingProvider), cfg);
    let s1 = session_id();
    let handle = engine.chat(s1, chat_payload("a")).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let reaper = engine.start_idle_reaper().expect("reaper must start");
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(engine.session_count(), 1, "Thinking session must survive");
    reaper.stop();
    handle.cancel();
}

#[tokio::test]
async fn no_idle_timeout_means_no_reaper() {
    let engine = Engine::new(mock(vec![Ok(resp("ok", vec![]))]), config());
    assert!(engine.start_idle_reaper().is_none());
}

// ─────────────────────────────────────────────
// 流式输出（4.1）
// ─────────────────────────────────────────────

/// 流式 mock 的共享流类型（Arc<Mutex<Option<...>>> 便于逐次取流）
type SharedStream =
    Arc<parking_lot::Mutex<Option<BoxStream<'static, Result<StreamChunk, LlmError>>>>>;

struct StreamMockProvider {
    stream: SharedStream,
}

impl StreamMockProvider {
    fn new(stream: BoxStream<'static, Result<StreamChunk, LlmError>>) -> Self {
        Self {
            stream: Arc::new(parking_lot::Mutex::new(Some(stream))),
        }
    }
}

fn caps_streaming() -> &'static ProviderCapabilities {
    static C: ProviderCapabilities = ProviderCapabilities {
        parallel_tool_calls: true,
        system_role: true,
        streaming: true,
        usage_reported: true,
        multimodal: crate::provider::MultimodalCapabilities::NONE,
    };
    &C
}

#[async_trait::async_trait]
impl LLMProvider for StreamMockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("stream-mock")
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        caps_streaming()
    }

    fn model_spec(&self) -> ModelSpec {
        model_spec()
    }

    async fn chat(&self, _req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        Err(LlmError::BadRequest("use chat_stream".into()))
    }
    async fn chat_stream(
        &self,
        _req: crate::provider::ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        self.stream
            .lock()
            .take()
            .ok_or_else(|| LlmError::Protocol("stream consumed".into()))
    }
}

fn delta(content: &str) -> StreamChunk {
    StreamChunk::Delta {
        content: Some(content.into()),
        reasoning_content: None,
        tool_calls: vec![],
        role: Some(crate::provider::Role::Assistant),
    }
}

#[tokio::test]
async fn streaming_returns_chunks_and_converges() {
    let chunks = vec![
        Ok(delta("Hello")),
        Ok(delta(" world")),
        Ok(StreamChunk::Finish {
            finish_reason: FinishReason::Stop,
            usage: Some(TokenUsage {
                total_tokens: 8,
                ..Default::default()
            }),
        }),
    ];
    let engine = Engine::new(
        Arc::new(StreamMockProvider::new(Box::pin(stream::iter(chunks)))),
        config(),
    );
    let sid = session_id();
    let handle = engine.chat_stream(sid, chat_payload("hi")).unwrap();
    let reply = wait_with_timeout(handle).await;

    let mut stream = match reply {
        EngineReply::Streaming(s) => s,
        other => panic!("expected Streaming, got {other:?}"),
    };

    let mut contents = Vec::new();
    let mut finished = false;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(StreamChunk::Delta { content, .. }) => {
                if let Some(c) = content {
                    contents.push(c);
                }
            }
            Ok(StreamChunk::Finish { .. }) => finished = true,
            Err(_) => panic!("unexpected stream error"),
        }
    }
    assert_eq!(contents, vec!["Hello".to_string(), " world".to_string()]);
    assert!(finished, "stream must end with Finish chunk");

    // 会话收敛与非流式一致：history 含 user + assistant("Hello world")，状态 Idle
    assert_eq!(engine.history_len(sid), Some(2));
    assert_eq!(engine.session_consumed_tokens(sid), Some(8));
    assert_eq!(
        engine.session_info(sid).map(|s| s.state),
        Some(SessionPhase::Idle)
    );
}

#[tokio::test]
async fn streaming_interruptible() {
    // 无限 Delta 流：interrupt 应打断并结束转发流
    let inf = stream::repeat(Ok(delta("x")));
    let engine = Engine::new(Arc::new(StreamMockProvider::new(Box::pin(inf))), config());
    let sid = session_id();
    let handle = engine.chat_stream(sid, chat_payload("hi")).unwrap();
    let reply = wait_with_timeout(handle.clone()).await;

    let mut stream = match reply {
        EngineReply::Streaming(s) => s,
        other => panic!("expected Streaming, got {other:?}"),
    };

    // 消费若干 chunk 后中断
    stream.next().await;
    stream.next().await;
    assert!(handle.cancel(), "cancel must be accepted (session busy)");

    // 流应在取消后结束（有界消费，最多读 100 个 chunk 后必结束）
    let mut count = 0;
    while stream.next().await.is_some() {
        count += 1;
        assert!(count < 100, "stream must terminate after interrupt");
    }
}

// ─────────────────────────────────────────────
// 错误重试（4.3）
// ─────────────────────────────────────────────

#[tokio::test]
async fn retry_recovers_from_rate_limited() {
    let provider = mock(vec![
        Err(LlmError::RateLimited { retry_after: None }),
        Err(LlmError::RateLimited { retry_after: None }),
        Ok(resp("recovered", vec![])),
    ]);
    let mut cfg = config();
    cfg.max_retries = 2;
    let engine = Engine::new(provider.clone(), cfg);
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("hi")).unwrap()).await;
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "recovered"),
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(
        provider.call_count.load(Ordering::SeqCst),
        3,
        "two retries then success"
    );
}

#[tokio::test]
async fn max_retries_zero_keeps_old_behavior() {
    let provider = mock(vec![Err(LlmError::RateLimited { retry_after: None })]);
    let mut cfg = config();
    cfg.max_retries = 0;
    let engine = Engine::new(provider.clone(), cfg);
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("hi")).unwrap()).await;
    assert!(matches!(reply, EngineReply::Error(_)));
    assert_eq!(provider.call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn non_retryable_error_not_retried() {
    let provider = mock(vec![Err(LlmError::BadRequest("bad".into()))]);
    let mut cfg = config();
    cfg.max_retries = 2;
    let engine = Engine::new(provider.clone(), cfg);
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("hi")).unwrap()).await;
    assert!(matches!(reply, EngineReply::Error(_)));
    assert_eq!(
        provider.call_count.load(Ordering::SeqCst),
        1,
        "BadRequest must not retry"
    );
}

// ─────────────────────────────────────────────
// 能力声明驱动降级（5.2）
// ─────────────────────────────────────────────

struct SerialProvider {
    responses: Arc<parking_lot::Mutex<VecDeque<Result<ChatResponse, LlmError>>>>,
}

fn caps_serial() -> &'static ProviderCapabilities {
    static C: ProviderCapabilities = ProviderCapabilities {
        parallel_tool_calls: false,
        system_role: true,
        streaming: false,
        usage_reported: true,
        multimodal: crate::provider::MultimodalCapabilities::NONE,
    };
    &C
}

#[async_trait::async_trait]
impl LLMProvider for SerialProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("serial-mock")
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        caps_serial()
    }

    fn model_spec(&self) -> ModelSpec {
        model_spec()
    }

    async fn chat(&self, _req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        self.responses
            .lock()
            .pop_front()
            .unwrap_or_else(|| Err(LlmError::Protocol("no more responses".into())))
    }
    async fn chat_stream(
        &self,
        _req: crate::provider::ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Ok(Box::pin(stream::empty()))
    }
}

#[tokio::test]
async fn serial_tool_execution_when_parallel_unsupported() {
    // Remote 慢工具（等待类）：不支持并行工具的 Provider 下应串行执行
    #[async_trait::async_trait]
    impl Tool for SlowRemote {
        fn name(&self) -> &str {
            "slow_remote"
        }
        fn description(&self) -> &str {
            "slow remote"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        fn default_wait(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(ToolOutput::text("ok"))
        }
    }
    struct SlowRemote;

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(SlowRemote)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 5,
    });

    let provider = Arc::new(SerialProvider {
        responses: Arc::new(parking_lot::Mutex::new(
            vec![
                Ok(resp(
                    "",
                    vec![tool_call("slow_remote"), tool_call("slow_remote")],
                )),
                Ok(resp("done", vec![])),
            ]
            .into(),
        )),
    });
    let engine = Engine::new(provider, config()).with_tools(registry, executor);

    let start = Instant::now();
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("go")).unwrap()).await;
    let elapsed = start.elapsed();
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "done"),
        other => panic!("expected Success, got {other:?}"),
    }
    assert!(
        elapsed >= Duration::from_millis(90),
        "parallel-unsupported provider must serialize tools, took {elapsed:?}"
    );
}

// ─────────────────────────────────────────────
// SessionReply 错误类型化（P0）— 两条转换路径的分类
// ─────────────────────────────────────────────

#[test]
fn engine_reply_error_kind_classification() {
    // Timeout → kind=Timeout
    match SessionReply::from(EngineReply::Timeout) {
        SessionReply::Error {
            kind,
            retry_after_ms,
            ..
        } => {
            assert_eq!(kind, ErrorKind::Timeout);
            assert_eq!(retry_after_ms, None);
        }
        other => panic!("expected Error, got {other:?}"),
    }
    // Llm RateLimited → kind=RateLimited + retry_after 毫秒透传
    let reply = EngineReply::Error(EngineError::Llm(LlmError::RateLimited {
        retry_after: Some(Duration::from_millis(500)),
    }));
    match SessionReply::from(reply) {
        SessionReply::Error {
            kind,
            retry_after_ms,
            ..
        } => {
            assert_eq!(kind, ErrorKind::RateLimited);
            assert_eq!(retry_after_ms, Some(500));
        }
        other => panic!("expected Error, got {other:?}"),
    }
    // 其余 Llm 错误 → kind=Llm
    let reply = EngineReply::Error(EngineError::Llm(LlmError::BadRequest("bad".into())));
    match SessionReply::from(reply) {
        SessionReply::Error { kind, .. } => assert_eq!(kind, ErrorKind::Llm),
        other => panic!("expected Error, got {other:?}"),
    }
    // 非 Llm 错误（StateConflict / TurnIncomplete / ChannelClosed）→ kind=Internal
    for reply in [
        EngineReply::Error(EngineError::StateConflict("resume failed")),
        EngineReply::Error(EngineError::TurnIncomplete),
        EngineReply::Error(EngineError::ChannelClosed),
    ] {
        match SessionReply::from(reply) {
            SessionReply::Error { kind, .. } => assert_eq!(kind, ErrorKind::Internal),
            other => panic!("expected Error, got {other:?}"),
        }
    }
}

#[test]
fn engine_start_error_kind_classification() {
    // Budget → Budget；MaxSessions / Busy → Internal（agent 层构造 kind 的依据）
    assert_eq!(
        ErrorKind::from(EngineStartError::Budget(BudgetError::SessionExceeded {
            used: 10,
            limit: 5
        })),
        ErrorKind::Budget
    );
    assert_eq!(
        ErrorKind::from(EngineStartError::Budget(BudgetError::GlobalExceeded {
            used: 10,
            limit: 5
        })),
        ErrorKind::Budget
    );
    assert_eq!(ErrorKind::from(EngineStartError::MaxSessions), ErrorKind::Internal);
    assert_eq!(ErrorKind::from(EngineStartError::Busy), ErrorKind::Internal);
}

// ─────────────────────────────────────────────
// awaiting_calls_timeout 落地（P1）— 单轮等待类批次总 deadline
// ─────────────────────────────────────────────

#[tokio::test]
async fn awaiting_calls_deadline_converges_and_session_not_stuck() {
    // 快工具（20ms）+ 慢工具（10s，远超批次 deadline 且不触发单工具 timeout）；
    // awaiting_calls_timeout=150ms：快工具真实收敛、慢工具超时消息收敛，
    // 回合正常走完（resume → 最终回复），会话恢复 Idle、次轮 chat 可正常进入。
    struct FastTool;
    #[async_trait::async_trait]
    impl Tool for FastTool {
        fn name(&self) -> &str {
            "fast"
        }
        fn description(&self) -> &str {
            "fast tool"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        fn default_wait(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(ToolOutput::text("fast_result"))
        }
    }

    struct VerySlowTool;
    #[async_trait::async_trait]
    impl Tool for VerySlowTool {
        fn name(&self) -> &str {
            "very_slow"
        }
        fn description(&self) -> &str {
            "very slow tool"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        fn default_wait(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(ToolOutput::text("never"))
        }
    }

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(FastTool)).unwrap();
    registry.register(Arc::new(VerySlowTool)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(30), // 不触发单工具 timeout
        max_concurrency: 5,
    });

    let mut cfg = config();
    cfg.session.timeout.awaiting_calls_timeout = Duration::from_millis(150);
    // 第三条响应供次轮 chat 使用（验证会话不悬空）
    let provider = mock(vec![
        Ok(resp("", vec![tool_call("fast"), tool_call("very_slow")])),
        Ok(resp("done", vec![])),
        Ok(resp("next round ok", vec![])),
    ]);
    let engine = Engine::new(provider.clone(), cfg).with_tools(registry, executor);
    let sid = session_id();

    // 回合应在批次 deadline 附近收敛（远小于慢工具 10s / 单工具 30s）
    let start = Instant::now();
    let reply = wait_with_timeout(engine.chat(sid, chat_payload("go")).unwrap()).await;
    let elapsed = start.elapsed();
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "done"),
        other => panic!("expected Success, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(2),
        "turn must converge at batch deadline, took {elapsed:?}"
    );

    // 部分完成 + 部分超时收敛：第二轮请求同时含快工具真实结果与超时收敛消息
    let reqs = provider.requests.lock();
    let second = &reqs[1];
    assert!(second.contains("fast_result"), "fast result kept: {second}");
    assert!(
        second.contains("batch deadline"),
        "slow tool converged with deadline message: {second}"
    );
    drop(reqs);

    // 会话不悬空：状态 Idle，次轮 chat 正常进入并完成
    assert_eq!(
        engine.session_info(sid).map(|s| s.state),
        Some(SessionPhase::Idle)
    );
    let reply2 = wait_with_timeout(engine.chat(sid, chat_payload("again")).unwrap()).await;
    match reply2 {
        EngineReply::Success(r) => {
            assert_eq!(r.message.content.as_text().unwrap(), "next round ok")
        }
        other => panic!("second chat must start normally, got {other:?}"),
    }
}

// ─────────────────────────────────────────────
// EngineObserver 事件钩子（P2）— mock observer 五场景
// ─────────────────────────────────────────────

/// 观测事件记录（usage 简化为 total_tokens）
#[derive(Debug, Clone, PartialEq)]
enum Observed {
    TurnStarted(u64),
    ThinkingDelta(String),
    TextDelta(String),
    ToolStarted(String),
    ToolFinished(String, ToolOutcome, u64),
    TurnFinished(u64, Option<usize>),
}

/// mock observer — Mutex push（测试内轻量操作，满足非阻塞契约）
#[derive(Clone, Default)]
struct Recorder {
    events: std::sync::Arc<parking_lot::Mutex<Vec<Observed>>>,
}

impl Recorder {
    fn events(&self) -> Vec<Observed> {
        self.events.lock().clone()
    }
}

impl EngineObserver for Recorder {
    fn on_turn_started(&self, _sid: crate::SessionId, turn_id: u64) {
        self.events.lock().push(Observed::TurnStarted(turn_id));
    }
    fn on_thinking_delta(&self, _sid: crate::SessionId, delta: &str) {
        self.events
            .lock()
            .push(Observed::ThinkingDelta(delta.to_string()));
    }
    fn on_text_delta(&self, _sid: crate::SessionId, delta: &str) {
        self.events.lock().push(Observed::TextDelta(delta.to_string()));
    }
    fn on_tool_started(&self, _sid: crate::SessionId, tool_call_id: &str, _name: &str) {
        self.events
            .lock()
            .push(Observed::ToolStarted(tool_call_id.to_string()));
    }
    fn on_tool_finished(
        &self,
        _sid: crate::SessionId,
        tool_call_id: &str,
        outcome: ToolOutcome,
        duration_ms: u64,
    ) {
        self.events
            .lock()
            .push(Observed::ToolFinished(tool_call_id.to_string(), outcome, duration_ms));
    }
    fn on_turn_finished(&self, _sid: crate::SessionId, turn_id: u64, usage: Option<TokenUsage>) {
        self.events
            .lock()
            .push(Observed::TurnFinished(turn_id, usage.map(|u| u.total_tokens)));
    }
}

/// 双模流式提供器：chat 报错（内部流式收敛启用时不得调用），chat_stream 返回脚本
struct DualStreamProvider {
    chunks: Vec<Result<StreamChunk, LlmError>>,
}

#[async_trait::async_trait]
impl LLMProvider for DualStreamProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("dual")
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        caps_streaming()
    }
    fn model_spec(&self) -> ModelSpec {
        model_spec()
    }
    async fn chat(&self, _req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        Err(LlmError::BadRequest(
            "chat() must not be called when observer enables internal streaming".into(),
        ))
    }
    async fn chat_stream(
        &self,
        _req: crate::provider::ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Ok(Box::pin(stream::iter(self.chunks.clone())))
    }
}

/// chat 优先提供器：chat 正常返回，chat_stream 返回空流（内部收敛若被误用必失败）
struct ChatFirstStreamProvider {
    call_count: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl LLMProvider for ChatFirstStreamProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("chat-first")
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        caps_streaming()
    }
    fn model_spec(&self) -> ModelSpec {
        model_spec()
    }
    async fn chat(&self, _req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(resp("from chat", vec![]))
    }
    async fn chat_stream(
        &self,
        _req: crate::provider::ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Ok(Box::pin(stream::empty()))
    }
}

fn finish_chunk(total: usize) -> Result<StreamChunk, LlmError> {
    Ok(StreamChunk::Finish {
        finish_reason: FinishReason::Stop,
        usage: Some(TokenUsage {
            total_tokens: total,
            ..Default::default()
        }),
    })
}

#[tokio::test]
async fn observer_internal_stream_convergence_on_non_stream_path() {
    // 注入 observer + 厂商流式：非流式 chat() 内部改走 chat_stream 收敛，
    // delta 逐条回调，对外仍返回完整 ChatResponse
    let provider = Arc::new(DualStreamProvider {
        chunks: vec![
            Ok(delta("Hello")),
            Ok(delta(" world")),
            finish_chunk(8),
        ],
    });
    let recorder = Recorder::default();
    let engine = Engine::new(provider, config()).with_observer(Arc::new(recorder.clone()));
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("hi")).unwrap()).await;
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "Hello world"),
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(
        recorder.events(),
        vec![
            Observed::TurnStarted(1),
            Observed::TextDelta("Hello".into()),
            Observed::TextDelta(" world".into()),
            Observed::TurnFinished(1, Some(8)),
        ],
        "turn start/finish must pair; deltas in order; usage carried"
    );
}

#[tokio::test]
async fn observer_non_streaming_provider_keeps_plain_chat() {
    // observer + 厂商 streaming=false → 仍走 provider.chat()，无 delta 事件
    let provider = mock(vec![Ok(resp("plain ok", vec![]))]);
    let recorder = Recorder::default();
    let engine = Engine::new(provider.clone(), config()).with_observer(Arc::new(recorder.clone()));
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("hi")).unwrap()).await;
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "plain ok"),
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(provider.call_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        recorder.events(),
        vec![
            Observed::TurnStarted(1),
            Observed::TurnFinished(1, Some(8)), // resp() 默认 usage total=8
        ],
        "non-streaming provider must not emit deltas"
    );
}

#[tokio::test]
async fn no_observer_keeps_plain_chat_even_if_streaming_capable() {
    // 未注入 observer：即使厂商支持流式也保持 provider.chat()（无内部流式回归）
    let provider = Arc::new(ChatFirstStreamProvider {
        call_count: Arc::new(AtomicUsize::new(0)),
    });
    let engine = Engine::new(provider.clone(), config()); // 无 observer
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("hi")).unwrap()).await;
    match reply {
        EngineReply::Success(r) => assert_eq!(r.message.content.as_text().unwrap(), "from chat"),
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(
        provider.call_count.load(Ordering::SeqCst),
        1,
        "plain chat must be used exactly once"
    );
}

#[tokio::test]
async fn observer_tool_timeout_observable() {
    // 等待类工具超时：on_tool_finished 携带 ToolOutcome::Timeout；回合事件成对
    struct SleepyTool;
    #[async_trait::async_trait]
    impl Tool for SleepyTool {
        fn name(&self) -> &str {
            "sleepy"
        }
        fn description(&self) -> &str {
            "sleeps long"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        fn default_wait(&self) -> bool {
            true
        }
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(ToolOutput::text("never"))
        }
    }

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(SleepyTool)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_millis(50), // 单工具超时先于批次 deadline
        max_concurrency: 5,
    });
    let provider = mock(vec![
        Ok(resp("", vec![tool_call("sleepy")])),
        Ok(resp("done", vec![])),
    ]);
    let recorder = Recorder::default();
    let engine = Engine::new(provider, config())
        .with_tools(registry, executor)
        .with_observer(Arc::new(recorder.clone()));
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("go")).unwrap()).await;
    assert!(matches!(reply, EngineReply::Success(_)));

    let events = recorder.events();
    // 工具事件：started + finished(Timeout)
    assert!(events.contains(&Observed::ToolStarted("tc_sleepy".into())));
    assert!(events.iter().any(|e| matches!(
        e,
        Observed::ToolFinished(id, ToolOutcome::Timeout, _)
            if id == "tc_sleepy"
    )));
    // 回合事件成对（两轮思考：工具轮 + 最终轮）
    let started = events
        .iter()
        .filter(|e| matches!(e, Observed::TurnStarted(_)))
        .count();
    let finished = events
        .iter()
        .filter(|e| matches!(e, Observed::TurnFinished(_, _)))
        .count();
    assert_eq!((started, finished), (2, 2), "turn events must pair: {events:?}");
}

#[tokio::test]
async fn observer_dispatched_tool_finished_fires() {
    // 派发类（wait=false）后台完成后同样触发 on_tool_finished（复用 ToolOutcome）
    struct LazyTool;
    #[async_trait::async_trait]
    impl Tool for LazyTool {
        fn name(&self) -> &str {
            "lazy"
        }
        fn description(&self) -> &str {
            "async lazy tool"
        }
        fn input_schema(&self) -> serde_json::Value {
            serde_json::json!({"type": "object"})
        }
        // 不覆写 default_wait（默认 false）→ 派发类
        async fn execute(
            &self,
            _c: ToolContext,
            _a: serde_json::Value,
        ) -> Result<ToolOutput, ToolError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(ToolOutput::text("lazy_result"))
        }
    }

    let registry = ToolRegistry::new(RegistryConfig::default());
    registry.register(Arc::new(LazyTool)).unwrap();
    let executor = crate::tool::ToolExecutor::new(ExecutorConfig {
        max_per_turn: 10,
        tool_timeout: Duration::from_secs(2),
        max_concurrency: 5,
    });
    let provider = mock(vec![Ok(resp("dispatching", vec![tool_call("lazy")]))]);
    let recorder = Recorder::default();
    let engine = Engine::new(provider, config())
        .with_tools(registry, executor)
        .with_observer(Arc::new(recorder.clone()));
    let reply = wait_with_timeout(engine.chat(session_id(), chat_payload("go")).unwrap()).await;
    assert!(matches!(reply, EngineReply::Success(_)));

    // 等待后台派发任务完成（工具 100ms + 余量）
    tokio::time::sleep(Duration::from_millis(400)).await;
    let events = recorder.events();
    assert!(events.contains(&Observed::ToolStarted("tc_lazy".into())));
    assert!(events.iter().any(|e| matches!(
        e,
        Observed::ToolFinished(id, ToolOutcome::Ok, _) if id == "tc_lazy"
    )));
}

#[tokio::test]
async fn observer_streaming_path_deltas() {
    // chat_stream 路径：delta 回调与内部收敛路径共享同一推送点
    let chunks = vec![Ok(delta("Hello")), Ok(delta(" world")), finish_chunk(8)];
    let recorder = Recorder::default();
    let engine = Engine::new(
        Arc::new(StreamMockProvider::new(Box::pin(stream::iter(chunks)))),
        config(),
    )
    .with_observer(Arc::new(recorder.clone()));
    let sid = session_id();
    let handle = engine.chat_stream(sid, chat_payload("hi")).unwrap();
    let reply = wait_with_timeout(handle).await;
    let mut stream = match reply {
        EngineReply::Streaming(s) => s,
        other => panic!("expected Streaming, got {other:?}"),
    };
    while stream.next().await.is_some() {} // 消费完转发流

    assert_eq!(
        recorder.events(),
        vec![
            Observed::TurnStarted(1),
            Observed::TextDelta("Hello".into()),
            Observed::TextDelta(" world".into()),
            Observed::TurnFinished(1, Some(8)),
        ]
    );
}

//! Phase 1 验收测试 — 会话状态机与消息驱动执行
//!
//! 验证项（AGENT_RUNTIME_PLAN §5.2）：
//! - **中断**：协作取消信号及时打断 Thinking，Interrupt 走 High 优先级桶
//! - **幽灵治理**：四路径（success / error / cancel / timeout）+ panic 全部收敛 Idle
//! - **busy 拒绝**：并发 Chat 显式返回 `Busy`，不静默丢弃
//! - **Phase 1 边界**：tool_calls 强制回 Idle + Success；P2/P3 消息返回 `Unhandled`

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use parking_lot::Mutex;
use referee_agent::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, LlmError, Message, ProviderCapabilities,
    ProviderId, StreamChunk, TokenUsage, ToolCall, ToolCallFunction,
};
use referee_agent::session::{
    ChatOptions, ChatPayload, SessionConfig, SessionId, SessionMessage, SessionReply, TimeoutConfig,
};
use referee_agent::{AgentConfig, AgentRuntime};
use referee_core::{CapabilityId, Kernel, SupervisionPolicy};
use tokio::sync::Notify;
use uuid::Uuid;

// ───────────────────────────────────────────────
// Mock Provider — 行为可由测试侧运行时切换
// ───────────────────────────────────────────────

/// Mock 行为枚举（Clone，可运行时切换）
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
enum MockBehavior {
    /// 立即返回成功响应
    Ok(ChatResponse),
    /// 立即返回错误
    Err(LlmError),
    /// 挂起直到被 `release()` 唤醒（用于测试 interrupt / timeout）
    Hang,
    /// 内部 panic（测试 catch_unwind 收敛）
    Panic(String),
}

/// Mock 控制器 — 测试侧通过此对象切换 provider 行为
struct MockControl {
    behavior: Mutex<MockBehavior>,
    release: Notify,
}

impl MockControl {
    fn new(behavior: MockBehavior) -> Self {
        Self {
            behavior: Mutex::new(behavior),
            release: Notify::new(),
        }
    }

    /// 切换后续 chat() 行为
    fn set(&self, behavior: MockBehavior) {
        *self.behavior.lock() = behavior;
    }
}

/// Mock LLM Provider — 行为由 `MockControl` 控制
struct MockProvider {
    control: Arc<MockControl>,
    caps: ProviderCapabilities,
}

impl MockProvider {
    fn new(control: Arc<MockControl>) -> Self {
        Self {
            control,
            caps: ProviderCapabilities {
                parallel_tool_calls: true,
                system_role: true,
                streaming: false,
                usage_reported: true,
                max_output_tokens: 4096,
            },
        }
    }
}

#[async_trait]
impl LLMProvider for MockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("mock")
    }

    fn capabilities(&self) -> &ProviderCapabilities {
        &self.caps
    }

    async fn chat(&self, _req: ChatRequest) -> Result<ChatResponse, LlmError> {
        loop {
            // 先 clone 出 behavior，确保 MutexGuard 在 await 前 drop（guard 非 Send）
            let behavior = self.control.behavior.lock().clone();
            match behavior {
                MockBehavior::Ok(resp) => return Ok(resp),
                MockBehavior::Err(e) => return Err(e),
                MockBehavior::Panic(msg) => panic!("{msg}"),
                MockBehavior::Hang => {
                    // 等待释放信号；被 cancel / timeout drop 时自动退出
                    self.control.release.notified().await;
                    // 唤醒后重新检查 behavior
                }
            }
        }
    }

    async fn chat_stream(
        &self,
        _req: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Err(LlmError::BadRequest(
            "streaming not supported in mock".into(),
        ))
    }
}

// ───────────────────────────────────────────────
// 测试辅助函数
// ───────────────────────────────────────────────

/// 构造普通 mock 响应
fn mock_response(content: &str) -> ChatResponse {
    ChatResponse {
        id: "test-id".into(),
        model: "mock-model".into(),
        message: Message::assistant(content),
        finish_reason: FinishReason::Stop,
        usage: Some(TokenUsage::default()),
    }
}

/// 构造带工具调用的 mock 响应（Phase 1 应强制回 Idle + Success）
fn mock_response_with_tool_calls() -> ChatResponse {
    let mut resp = mock_response("calling tool");
    resp.message.tool_calls = vec![ToolCall {
        id: "call_1".into(),
        function: ToolCallFunction {
            name: "get_weather".into(),
            arguments: "{}".into(),
        },
    }];
    resp.finish_reason = FinishReason::ToolCalls;
    resp
}

/// 构造 Chat 消息
fn chat_msg(session_id: SessionId, content: &str) -> SessionMessage {
    SessionMessage::Chat {
        session_id,
        payload: ChatPayload {
            message: Message::user(content),
            options: ChatOptions::default(),
        },
    }
}

/// 构造 Interrupt 消息
fn interrupt_msg(session_id: SessionId) -> SessionMessage {
    SessionMessage::Interrupt { session_id }
}

/// 极短超时配置（thinking_timeout = 50ms，快速触发超时路径）
fn fast_config() -> AgentConfig {
    AgentConfig {
        session: SessionConfig {
            timeout: TimeoutConfig {
                thinking_timeout: Duration::from_millis(50),
                awaiting_calls_timeout: Duration::from_millis(50),
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

/// 注册 AgentRuntime，返回 (kernel, runtime_id, mock_control)
async fn setup(
    config: AgentConfig,
    behavior: MockBehavior,
) -> (Kernel, CapabilityId, Arc<MockControl>) {
    let kernel = Kernel::new();
    let control = Arc::new(MockControl::new(behavior));
    let provider = MockProvider::new(control.clone());
    let runtime = AgentRuntime::new(kernel.clone(), Arc::new(provider), config);
    let runtime_id = runtime.id();
    kernel
        .register(Box::new(runtime), 8, SupervisionPolicy::Transient)
        .await
        .expect("register ok");
    (kernel, runtime_id, control)
}

/// 等待一小段时间，确保 spawn 的 turn task 已启动
async fn yield_once() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// 解码 invoke 响应为 SessionReply
fn decode_reply(env: &referee_core::Envelope) -> SessionReply {
    SessionReply::from_envelope(env).expect("decode reply")
}

// ═══════════════════════════════════════════════
// Happy Path
// ═══════════════════════════════════════════════

#[tokio::test]
async fn chat_success_returns_response() {
    let (kernel, rid, _ctrl) = setup(
        AgentConfig::default(),
        MockBehavior::Ok(mock_response("hello")),
    )
    .await;

    let sid = Uuid::new_v4();
    let resp = kernel
        .invoke(rid, chat_msg(sid, "hi").to_envelope(), 5000)
        .await
        .expect("invoke ok");

    match decode_reply(&resp) {
        SessionReply::Success { message, .. } => {
            assert_eq!(message.content.as_text(), Some("hello"));
        }
        other => panic!("expected Success, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════
// Busy 拒绝
// ═══════════════════════════════════════════════

#[tokio::test]
async fn busy_rejection_when_thinking() {
    let (kernel, rid, _ctrl) = setup(AgentConfig::default(), MockBehavior::Hang).await;

    let sid = Uuid::new_v4();

    // emit Chat（provider 挂起，session 进入 Thinking）
    kernel
        .emit(rid, chat_msg(sid, "first").to_envelope())
        .await
        .expect("emit ok");
    yield_once().await;

    // invoke Chat → 应返回 Busy（不静默丢弃）
    let resp = kernel
        .invoke(rid, chat_msg(sid, "second").to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Busy { .. }));
}

// ═══════════════════════════════════════════════
// 中断 / 协作取消
// ═══════════════════════════════════════════════

#[tokio::test]
async fn interrupt_cancels_thinking_and_no_ghost() {
    let (kernel, rid, ctrl) = setup(AgentConfig::default(), MockBehavior::Hang).await;

    let sid = Uuid::new_v4();

    // 1. emit Chat（provider 挂起）
    kernel
        .emit(rid, chat_msg(sid, "hello").to_envelope())
        .await
        .expect("emit ok");
    yield_once().await;

    // 2. invoke Interrupt → Cancelled（handle_interrupt 同步回复）
    let resp = kernel
        .invoke(rid, interrupt_msg(sid).to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Cancelled));

    // 3. 等待 turn task 收敛（cancel 信号 → run_turn 返回 Cancelled → converge）
    yield_once().await;

    // 4. 切换 provider 为立即返回，验证 session 已回到 Idle（非幽灵）
    ctrl.set(MockBehavior::Ok(mock_response("after cancel")));
    let resp = kernel
        .invoke(rid, chat_msg(sid, "again").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));
}

#[tokio::test]
async fn interrupt_nonexistent_session_returns_unhandled() {
    let (kernel, rid, _ctrl) = setup(
        AgentConfig::default(),
        MockBehavior::Ok(mock_response("ok")),
    )
    .await;

    let sid = Uuid::new_v4(); // 未创建过的 session
    let resp = kernel
        .invoke(rid, interrupt_msg(sid).to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(
        decode_reply(&resp),
        SessionReply::Unhandled { .. }
    ));
}

#[tokio::test]
async fn interrupt_idle_session_returns_unhandled() {
    let (kernel, rid, _ctrl) = setup(
        AgentConfig::default(),
        MockBehavior::Ok(mock_response("ok")),
    )
    .await;

    let sid = Uuid::new_v4();

    // 先完成一次 Chat（session 回到 Idle）
    let _ = kernel
        .invoke(rid, chat_msg(sid, "hi").to_envelope(), 5000)
        .await
        .expect("invoke ok");

    // Interrupt 一个 Idle session → Unhandled
    let resp = kernel
        .invoke(rid, interrupt_msg(sid).to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(
        decode_reply(&resp),
        SessionReply::Unhandled { .. }
    ));
}

#[tokio::test]
async fn double_interrupt_second_returns_unhandled() {
    let (kernel, rid, _ctrl) = setup(AgentConfig::default(), MockBehavior::Hang).await;

    let sid = Uuid::new_v4();

    kernel
        .emit(rid, chat_msg(sid, "hello").to_envelope())
        .await
        .expect("emit ok");
    yield_once().await;

    // 第一次 Interrupt → Cancelled
    let resp = kernel
        .invoke(rid, interrupt_msg(sid).to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Cancelled));

    // 第二次 Interrupt → Unhandled（cancel sender 已被 take）
    let resp = kernel
        .invoke(rid, interrupt_msg(sid).to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(
        decode_reply(&resp),
        SessionReply::Unhandled { .. }
    ));
}

// ═══════════════════════════════════════════════
// 幽灵治理 — 四路径 + panic 全部收敛 Idle
// ═══════════════════════════════════════════════

#[tokio::test]
async fn no_ghost_after_success() {
    let (kernel, rid, _ctrl) = setup(
        AgentConfig::default(),
        MockBehavior::Ok(mock_response("first")),
    )
    .await;

    let sid = Uuid::new_v4();

    // 第一次 Chat → Success
    let resp = kernel
        .invoke(rid, chat_msg(sid, "round 1").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));

    // 第二次 Chat → 也应 Success（session 已回到 Idle，非幽灵）
    let resp = kernel
        .invoke(rid, chat_msg(sid, "round 2").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));
}

#[tokio::test]
async fn no_ghost_after_error() {
    let (kernel, rid, ctrl) =
        setup(AgentConfig::default(), MockBehavior::Err(LlmError::Timeout)).await;

    let sid = Uuid::new_v4();

    // Chat → Error（provider 返回错误）
    let resp = kernel
        .invoke(rid, chat_msg(sid, "fail").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Error { .. }));

    // 切换为成功，再次 Chat → Success（session 已回到 Idle）
    ctrl.set(MockBehavior::Ok(mock_response("recovered")));
    let resp = kernel
        .invoke(rid, chat_msg(sid, "retry").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));
}

#[tokio::test]
async fn no_ghost_after_timeout() {
    let (kernel, rid, ctrl) = setup(fast_config(), MockBehavior::Hang).await;

    let sid = Uuid::new_v4();

    // Chat → Error（provider 挂起，thinking_timeout=50ms 触发）
    let resp = kernel
        .invoke(rid, chat_msg(sid, "hang").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Error { .. }));

    // 切换为成功，再次 Chat → Success（session 已回到 Idle）
    ctrl.set(MockBehavior::Ok(mock_response("after timeout")));
    let resp = kernel
        .invoke(rid, chat_msg(sid, "retry").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));
}

#[tokio::test]
async fn no_ghost_after_panic() {
    let (kernel, rid, ctrl) =
        setup(AgentConfig::default(), MockBehavior::Panic("boom".into())).await;

    let sid = Uuid::new_v4();

    // Chat → Error（provider panic 被 catch_unwind 捕获，收敛为 Error）
    let resp = kernel
        .invoke(rid, chat_msg(sid, "panic").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Error { .. }));

    // 切换为成功，再次 Chat → Success（session 已回到 Idle）
    ctrl.set(MockBehavior::Ok(mock_response("after panic")));
    let resp = kernel
        .invoke(rid, chat_msg(sid, "retry").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));
}

// ═══════════════════════════════════════════════
// Phase 1 边界
// ═══════════════════════════════════════════════

#[tokio::test]
async fn tool_calls_response_returns_success_in_phase1() {
    let (kernel, rid, ctrl) = setup(
        AgentConfig::default(),
        MockBehavior::Ok(mock_response_with_tool_calls()),
    )
    .await;

    let sid = Uuid::new_v4();

    // 第一次 Chat：provider 返回 tool_calls → Phase 1 强制回 Idle + Success
    let resp = kernel
        .invoke(rid, chat_msg(sid, "use tool").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));

    // 切换为普通响应，再次 Chat → Success（证明 session 已回到 Idle，非 AwaitingCalls 幽灵）
    ctrl.set(MockBehavior::Ok(mock_response("plain")));
    let resp = kernel
        .invoke(rid, chat_msg(sid, "again").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));
}

#[tokio::test]
async fn unhandled_messages_return_unhandled() {
    let (kernel, rid, _ctrl) = setup(
        AgentConfig::default(),
        MockBehavior::Ok(mock_response("ok")),
    )
    .await;

    let sid = Uuid::new_v4();

    // ToolResult → Unhandled（P2 消息在 P1 阶段收到）
    let msg = SessionMessage::ToolResult {
        session_id: sid,
        turn_id: 1,
        tool_call_id: "call_1".into(),
        result: "{}".into(),
    };
    let resp = kernel
        .invoke(rid, msg.to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(
        decode_reply(&resp),
        SessionReply::Unhandled { .. }
    ));

    // Resume → Unhandled
    let msg = SessionMessage::Resume {
        session_id: sid,
        turn_id: 1,
    };
    let resp = kernel
        .invoke(rid, msg.to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(
        decode_reply(&resp),
        SessionReply::Unhandled { .. }
    ));

    // SubagentDone → Unhandled
    let msg = SessionMessage::SubagentDone {
        session_id: sid,
        turn_id: 1,
        subagent_id: Uuid::new_v4(),
        artifact_ids: vec![],
    };
    let resp = kernel
        .invoke(rid, msg.to_envelope(), 1000)
        .await
        .expect("invoke ok");
    assert!(matches!(
        decode_reply(&resp),
        SessionReply::Unhandled { .. }
    ));
}

// ═══════════════════════════════════════════════
// 资源限制
// ═══════════════════════════════════════════════

#[tokio::test]
async fn max_sessions_rejection() {
    let config = AgentConfig {
        max_sessions: 1,
        ..Default::default()
    };
    let (kernel, rid, _ctrl) = setup(config, MockBehavior::Ok(mock_response("ok"))).await;

    let sid_a = Uuid::new_v4();
    let sid_b = Uuid::new_v4();

    // 第一个 session 的 Chat → Success
    let resp = kernel
        .invoke(rid, chat_msg(sid_a, "first").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    assert!(matches!(decode_reply(&resp), SessionReply::Success { .. }));

    // 第二个 session 的 Chat → Error（max sessions reached）
    let resp = kernel
        .invoke(rid, chat_msg(sid_b, "second").to_envelope(), 5000)
        .await
        .expect("invoke ok");
    match decode_reply(&resp) {
        SessionReply::Error { message } => {
            assert!(message.contains("max sessions"), "got: {message}");
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

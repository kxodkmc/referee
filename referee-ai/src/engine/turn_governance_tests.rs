//! 回合治理测试 — terminal 终止式工具收敛 + 单回合轮数上限
//!
//! 覆盖：terminal 收敛省略收尾轮 / usage 逐轮聚合 / terminal 失败回退 / 轮数上限。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use serde_json::json;

use crate::provider::{
    ChatResponse, FinishReason, LLMProvider, LlmError, Message, MessageContent, ModelSpec,
    ProviderCapabilities, ProviderId, Role, StreamChunk, TokenUsage, ToolCall, ToolCallFunction,
};
use crate::session::{ChatOptions, ChatPayload, SessionConfig, TimeoutConfig};
use crate::tool::{
    RegistryError, Tool, ToolContext, ToolError, ToolOutput, ToolRegistry, ToolExecutor,
};
use crate::{Engine, EngineConfig, EngineError, EngineReply, SessionPhase};

// ── Mock 提供器 ──────────────────────────────────

struct MockProvider {
    responses: parking_lot::Mutex<VecDeque<ChatResponse>>,
    call_count: Arc<AtomicUsize>,
}

const CAPS: ProviderCapabilities = ProviderCapabilities {
    parallel_tool_calls: true,
    system_role: true,
    streaming: false,
    usage_reported: true,
    multimodal: crate::provider::MultimodalCapabilities::NONE,
};

#[async_trait]
impl LLMProvider for MockProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new("mock")
    }
    fn capabilities(&self) -> &ProviderCapabilities {
        &CAPS
    }
    fn model_spec(&self) -> ModelSpec {
        ModelSpec {
            context_window_tokens: 8192,
            max_output_tokens: 1024,
        }
    }
    async fn chat(&self, _req: crate::provider::ChatRequest) -> Result<ChatResponse, LlmError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        self.responses
            .lock()
            .pop_front()
            .map(Ok)
            .unwrap_or_else(|| Err(LlmError::Protocol("no more mock responses".into())))
    }
    async fn chat_stream(
        &self,
        _req: crate::provider::ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        Ok(Box::pin(stream::empty()))
    }
}

fn resp(text: &str, tool_calls: Vec<ToolCall>, total_tokens: usize) -> ChatResponse {
    let has_tools = !tool_calls.is_empty();
    ChatResponse {
        id: "mock".into(),
        model: "mock".into(),
        message: Message {
            role: Role::Assistant,
            content: MessageContent::text(text),
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
            prompt_tokens: total_tokens - 3,
            completion_tokens: 3,
            total_tokens,
            ..Default::default()
        }),
    }
}

fn tool_call(name: &str) -> ToolCall {
    ToolCall {
        id: format!("tc_{name}"),
        function: ToolCallFunction {
            name: name.to_string(),
            arguments: "{}".into(),
        },
    }
}

// ── 测试工具 ─────────────────────────────────────

struct EchoTool;

#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "echo"
    }
    fn input_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }
    fn default_wait(&self) -> bool {
        true
    }
    async fn execute(&self, _ctx: ToolContext, _args: serde_json::Value) -> Result<ToolOutput, ToolError> {
        Ok(ToolOutput::text("ok"))
    }
}

struct SubmitTool {
    fail: bool,
}

#[async_trait]
impl Tool for SubmitTool {
    fn name(&self) -> &str {
        "submit_plan"
    }
    fn description(&self) -> &str {
        "submit"
    }
    fn input_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }
    fn default_wait(&self) -> bool {
        true
    }
    fn terminal(&self) -> bool {
        true
    }
    async fn execute(&self, _ctx: ToolContext, _args: serde_json::Value) -> Result<ToolOutput, ToolError> {
        if self.fail {
            Err(ToolError::Execution("boom".into()))
        } else {
            Ok(ToolOutput::from_json(&json!({"accepted": true})))
        }
    }
}

struct TerminalDispatchTool;

#[async_trait]
impl Tool for TerminalDispatchTool {
    fn name(&self) -> &str {
        "terminal_dispatch"
    }
    fn description(&self) -> &str {
        "conflicting tool"
    }
    fn input_schema(&self) -> serde_json::Value {
        json!({"type": "object"})
    }
    fn terminal(&self) -> bool {
        true
    }
    async fn execute(&self, _ctx: ToolContext, _args: serde_json::Value) -> Result<ToolOutput, ToolError> {
        Ok(ToolOutput::text("ok"))
    }
}

// ── 测试设施 ─────────────────────────────────────

fn engine_with(
    responses: Vec<ChatResponse>,
    tools: Vec<Arc<dyn Tool>>,
    max_rounds: Option<u32>,
) -> (Engine, Arc<AtomicUsize>) {
    let call_count = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(MockProvider {
        responses: parking_lot::Mutex::new(responses.into()),
        call_count: call_count.clone(),
    });
    let config = EngineConfig {
        session: SessionConfig {
            timeout: TimeoutConfig {
                thinking_timeout: Duration::from_secs(5),
                awaiting_calls_timeout: Duration::from_secs(5),
            },
            max_rounds_per_chat: max_rounds,
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = Engine::new(provider, config)
        .with_tools(ToolRegistry::with_defaults(), ToolExecutor::with_defaults());
    for t in tools {
        engine.register_tool(t).unwrap();
    }
    (engine, call_count)
}

fn payload(text: &str) -> ChatPayload {
    ChatPayload {
        message: Message::user(text),
        options: ChatOptions::default(),
        peer_depth: 0,
    }
}

async fn reply(engine: &Engine, text: &str) -> EngineReply {
    let handle = engine.chat(uuid::Uuid::new_v4(), payload(text)).unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle.wait())
        .await
        .expect("chat must not hang")
        .expect("reply channel closed")
}

// ── 注册期校验 ───────────────────────────────────

#[test]
fn terminal_dispatch_tool_rejected_at_registration() {
    let registry = ToolRegistry::with_defaults();
    let err = registry
        .register(Arc::new(TerminalDispatchTool))
        .unwrap_err();
    assert!(matches!(err, RegistryError::TerminalRequiresWait(_)));
}

// ── terminal 收敛 ────────────────────────────────

#[tokio::test]
async fn terminal_tool_converges_without_closing_round() {
    let (engine, calls) = engine_with(
        vec![resp("submitting", vec![tool_call("submit_plan")], 8)],
        vec![Arc::new(EchoTool), Arc::new(SubmitTool { fail: false })],
        None,
    );
    let reply = reply(&engine, "go").await;
    match reply {
        EngineReply::Success(r) => {
            assert_eq!(r.message.tool_calls.len(), 1);
            assert_eq!(r.message.tool_calls[0].function.name, "submit_plan");
            assert_eq!(r.usage.as_ref().unwrap().total_tokens, 8);
        }
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        engine.session_info(engine.list_sessions()[0]).unwrap().state,
        SessionPhase::Idle
    );
}

#[tokio::test]
async fn terminal_convergence_aggregates_usage_across_rounds() {
    let (engine, calls) = engine_with(
        vec![
            resp("working", vec![tool_call("echo")], 8),
            resp("submitting", vec![tool_call("submit_plan")], 13),
        ],
        vec![Arc::new(EchoTool), Arc::new(SubmitTool { fail: false })],
        None,
    );
    let reply = reply(&engine, "go").await;
    match reply {
        EngineReply::Success(r) => {
            assert_eq!(r.usage.as_ref().unwrap().total_tokens, 21);
        }
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn terminal_tool_failure_resumes_for_correction() {
    let (engine, calls) = engine_with(
        vec![
            resp("submitting", vec![tool_call("submit_plan")], 8),
            resp("submission failed, I see", vec![], 10),
        ],
        vec![Arc::new(SubmitTool { fail: true })],
        None,
    );
    let reply = reply(&engine, "go").await;
    match reply {
        EngineReply::Success(r) => {
            assert_eq!(r.message.content.as_text().unwrap(), "submission failed, I see");
            assert_eq!(r.usage.as_ref().unwrap().total_tokens, 10);
        }
        other => panic!("expected Success, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

// ── 单回合轮数上限 ───────────────────────────────

#[tokio::test]
async fn max_rounds_per_chat_stops_runaway_loop() {
    let (engine, calls) = engine_with(
        vec![resp("calling", vec![tool_call("echo")], 8)],
        vec![Arc::new(EchoTool)],
        Some(1),
    );
    let sid = uuid::Uuid::new_v4();
    let handle = engine.chat(sid, payload("go")).unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), handle.wait())
        .await
        .expect("chat must not hang")
        .expect("reply channel closed");

    match reply {
        EngineReply::Error(EngineError::MaxRoundsExceeded { rounds }) => assert_eq!(rounds, 1),
        other => panic!("expected MaxRoundsExceeded, got {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let snap = engine.session_info(sid).unwrap();
    assert_eq!(snap.state, SessionPhase::Idle);
    // 历史不回滚：user + assistant(tool_calls) + tool result
    assert_eq!(engine.history_len(sid), Some(3));
}

#[tokio::test]
async fn max_rounds_none_keeps_current_behavior() {
    let (engine, calls) = engine_with(
        vec![
            resp("calling", vec![tool_call("echo")], 8),
            resp("done", vec![], 10),
        ],
        vec![Arc::new(EchoTool)],
        None,
    );
    let reply = reply(&engine, "go").await;
    assert!(matches!(reply, EngineReply::Success(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

//! Xiaomi MiMo 适配器测试：契约 + 流式 + 错误归一 + 重试 + 能力声明 + thinking 开关

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use referee_agent::provider::xiaomi::{
    ids, XiaomiConfig, XiaomiModel, XiaomiProvider, MAX_OUTPUT_TOKENS,
};
use referee_agent::provider::{
    ChatRequest, FinishReason, LLMProvider, LlmError, Message, RetryPolicy, StreamChunk,
    ToolDeclaration,
};
use serde_json::json;

use common::{MockResponse, MockServer};

fn make_provider(server: &MockServer) -> XiaomiProvider {
    XiaomiProvider::new(
        XiaomiModel::MimoV25Pro,
        XiaomiConfig::new("test-key")
            .with_base_url(&server.base_url)
            .with_retry(RetryPolicy::no_retry()),
    )
    .expect("provider creation should succeed")
}

// ───────────────────────────────────────────────
// 测试 1：非流式 chat() 契约 — 解析 content / reasoning_content / usage
// ───────────────────────────────────────────────
#[tokio::test]
async fn chat_parses_content_reasoning_and_usage() {
    let server = MockServer::start(|req| {
        assert!(
            !req.is_stream(),
            "non-streaming request should have stream=false"
        );
        MockResponse::Json {
            status: 200,
            body: json!({
                "id": "chatcmpl-4e57d676",
                "object": "chat.completion",
                "created": 1781234029,
                "model": "mimo-v2.5-pro",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "# Tips for Improving Work",
                        "reasoning_content": "The user is asking for tips"
                    },
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 61,
                    "completion_tokens": 339,
                    "total_tokens": 400,
                    "completion_tokens_details": {
                        "reasoning_tokens": 41
                    },
                    "prompt_tokens_details": {}
                }
            }),
        }
    })
    .await;

    let provider = make_provider(&server);
    let resp = provider
        .chat(ChatRequest::simple("Give me some tips"))
        .await
        .expect("chat should succeed");

    assert_eq!(resp.id, "chatcmpl-4e57d676");
    assert_eq!(resp.model, "mimo-v2.5-pro");
    assert_eq!(resp.message.role, referee_agent::provider::Role::Assistant);
    assert_eq!(
        resp.message.content.as_text().unwrap(),
        "# Tips for Improving Work"
    );
    assert_eq!(
        resp.message.reasoning_content.as_deref(),
        Some("The user is asking for tips")
    );
    assert_eq!(resp.finish_reason, FinishReason::Stop);
    let usage = resp.usage.expect("usage should be present");
    assert_eq!(usage.prompt_tokens, 61);
    assert_eq!(usage.completion_tokens, 339);
    assert_eq!(usage.total_tokens, 400);
    assert_eq!(usage.reasoning_tokens, Some(41));
}

// ───────────────────────────────────────────────
// 测试 2：流式 chat_stream() — Delta 增量 + Finish 收敛
// ───────────────────────────────────────────────
#[tokio::test]
async fn chat_stream_emits_deltas_and_finish() {
    let server = MockServer::start(|req| {
        assert!(req.is_stream(), "streaming request should have stream=true");
        MockResponse::Sse {
            status: 200,
            with_done: true,
            chunks: vec![
                json!({
                    "id": "chatcmpl-1",
                    "model": "mimo-v2.5-pro",
                    "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": null}]
                }),
                json!({
                    "id": "chatcmpl-1",
                    "model": "mimo-v2.5-pro",
                    "choices": [{"index": 0, "delta": {"reasoning_content": "Thinking..."}, "finish_reason": null}]
                }),
                json!({
                    "id": "chatcmpl-1",
                    "model": "mimo-v2.5-pro",
                    "choices": [{"index": 0, "delta": {"content": "Hello"}, "finish_reason": null}]
                }),
                json!({
                    "id": "chatcmpl-1",
                    "model": "mimo-v2.5-pro",
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
                }),
                json!({
                    "id": "chatcmpl-1",
                    "model": "mimo-v2.5-pro",
                    "choices": [],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
                }),
            ],
        }
    })
    .await;

    let provider = make_provider(&server);
    let mut stream = provider
        .chat_stream(ChatRequest::simple("Hi"))
        .await
        .expect("stream should start");

    let mut deltas = Vec::new();
    let mut finish = None;
    while let Some(chunk) = stream.next().await {
        match chunk.expect("chunk should be ok") {
            StreamChunk::Delta {
                content,
                reasoning_content,
                role,
                ..
            } => {
                deltas.push((content, reasoning_content, role));
            }
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                finish = Some((finish_reason, usage));
            }
        }
    }

    // 4 个 Delta（role / reasoning / content / 空 delta 被过滤）
    assert_eq!(deltas.len(), 3, "empty delta should be filtered");
    assert_eq!(deltas[0].2, Some(referee_agent::provider::Role::Assistant));
    assert_eq!(deltas[1].1.as_deref(), Some("Thinking..."));
    assert_eq!(deltas[2].0.as_deref(), Some("Hello"));

    let (fr, usage) = finish.expect("Finish chunk should be emitted");
    assert_eq!(fr, FinishReason::Stop);
    let usage = usage.expect("usage should be present");
    assert_eq!(usage.prompt_tokens, 10);
    assert_eq!(usage.completion_tokens, 5);
    assert_eq!(usage.total_tokens, 15);
}

// ───────────────────────────────────────────────
// 测试 3：thinking 开关 — 请求 body 包含 thinking.type
// ───────────────────────────────────────────────
#[tokio::test]
async fn request_body_includes_thinking_toggle() {
    let server = MockServer::start(|_req| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "mimo-v2.5-pro",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = make_provider(&server);
    let mut req = ChatRequest::simple("test");
    req.thinking.enabled = false;
    let _ = provider.chat(req).await.expect("chat should succeed");

    let recorded = server.requests();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0]
            .get("thinking")
            .and_then(|t| t.get("type"))
            .and_then(|t| t.as_str()),
        Some("disabled"),
        "thinking.type should be 'disabled' when thinking is off"
    );
}

// ───────────────────────────────────────────────
// 测试 4：thinking 默认开启
// ───────────────────────────────────────────────
#[tokio::test]
async fn thinking_enabled_by_default() {
    let server = MockServer::start(|_req| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "mimo-v2.5-pro",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = make_provider(&server);
    let _ = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect("chat should succeed");

    let recorded = server.requests();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0]
            .get("thinking")
            .and_then(|t| t.get("type"))
            .and_then(|t| t.as_str()),
        Some("enabled"),
        "thinking.type should be 'enabled' by default"
    );
}

// ───────────────────────────────────────────────
// 测试 5：错误归一 — 400 BadRequest / 401 Auth / 429 RateLimited / 500 Server
// ───────────────────────────────────────────────
#[tokio::test]
async fn error_normalization_400_bad_request() {
    let server = MockServer::start(|_| MockResponse::Raw {
        status: 400,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: br#"{"error":{"message":"invalid model"}}"#.to_vec(),
    })
    .await;

    let provider = make_provider(&server);
    let err = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect_err("should return error");
    assert!(
        matches!(err, LlmError::BadRequest(ref s) if s.contains("400")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn error_normalization_401_auth() {
    let server = MockServer::start(|_| MockResponse::Raw {
        status: 401,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: br#"{"error":"invalid api key"}"#.to_vec(),
    })
    .await;

    let provider = make_provider(&server);
    let err = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect_err("should return error");
    assert_eq!(err, LlmError::Auth);
}

#[tokio::test]
async fn error_normalization_429_rate_limited_with_retry_after() {
    let server = MockServer::start(|_| MockResponse::Raw {
        status: 429,
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("Retry-After".into(), "5".into()),
        ],
        body: br#"{"error":"rate limited"}"#.to_vec(),
    })
    .await;

    let provider = make_provider(&server);
    let err = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect_err("should return error");
    assert_eq!(
        err,
        LlmError::RateLimited {
            retry_after: Some(Duration::from_secs(5))
        }
    );
}

#[tokio::test]
async fn error_normalization_500_server() {
    let server = MockServer::start(|_| MockResponse::Raw {
        status: 500,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: br#"{"error":"internal"}"#.to_vec(),
    })
    .await;

    let provider = make_provider(&server);
    let err = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect_err("should return error");
    assert!(
        matches!(err, LlmError::Server { status: 500, .. }),
        "got {err:?}"
    );
}

// ───────────────────────────────────────────────
// 测试 6：重试 — 500 后成功（指数退避）
// ───────────────────────────────────────────────
#[tokio::test]
async fn retry_on_500_then_succeed() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let server = MockServer::start(move |_| {
        let n = counter_clone.fetch_add(1, Ordering::SeqCst);
        if n < 2 {
            MockResponse::Raw {
                status: 500,
                headers: vec![],
                body: b"server error".to_vec(),
            }
        } else {
            MockResponse::Json {
                status: 200,
                body: json!({
                    "id": "test",
                    "model": "mimo-v2.5-pro",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
                }),
            }
        }
    })
    .await;

    let provider = XiaomiProvider::new(
        XiaomiModel::MimoV25Pro,
        XiaomiConfig::new("test-key")
            .with_base_url(&server.base_url)
            .with_retry(RetryPolicy {
                max_retries: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
            }),
    )
    .expect("provider creation");

    let resp = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect("should succeed after retries");
    assert_eq!(resp.message.content.as_text().unwrap(), "ok");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        3,
        "should have 3 attempts (2 failures + 1 success)"
    );
}

// ───────────────────────────────────────────────
// 测试 7：不重试 — 400 直接返回错误
// ───────────────────────────────────────────────
#[tokio::test]
async fn no_retry_on_400_bad_request() {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();

    let server = MockServer::start(move |_| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
        MockResponse::Raw {
            status: 400,
            headers: vec![],
            body: b"bad request".to_vec(),
        }
    })
    .await;

    let provider = XiaomiProvider::new(
        XiaomiModel::MimoV25Pro,
        XiaomiConfig::new("test-key")
            .with_base_url(&server.base_url)
            .with_retry(RetryPolicy {
                max_retries: 3,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(10),
            }),
    )
    .expect("provider creation");

    let _err = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect_err("should return error");
    assert_eq!(counter.load(Ordering::SeqCst), 1, "should not retry on 400");
}

// ───────────────────────────────────────────────
// 测试 8：能力声明
// ───────────────────────────────────────────────
#[tokio::test]
async fn capabilities_are_correct() {
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({"id": "x", "model": "x", "choices": [{"index": 0, "message": {"role": "assistant", "content": ""}, "finish_reason": "stop"}]}),
    })
    .await;

    let provider = make_provider(&server);
    let caps = provider.capabilities();
    assert!(
        caps.parallel_tool_calls,
        "MiMo supports parallel tool calls"
    );
    assert!(caps.system_role, "MiMo supports system role");
    assert!(caps.streaming, "MiMo supports streaming");
    assert!(caps.usage_reported, "MiMo reports usage");
    assert_eq!(caps.max_output_tokens, MAX_OUTPUT_TOKENS);
    assert_eq!(provider.id(), ids::MIMO_V25_PRO);
}

// ───────────────────────────────────────────────
// 测试 9：多轮工具调用 — assistant 消息回传 reasoning_content
// ───────────────────────────────────────────────
#[tokio::test]
async fn multi_turn_preserves_reasoning_content_in_body() {
    let server = MockServer::start(|_req| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "mimo-v2.5-pro",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = make_provider(&server);
    let mut req = ChatRequest::simple("turn 2");
    // 模拟多轮：历史中包含带 reasoning_content 的 assistant 消息
    req.messages.insert(
        0,
        Message::assistant("turn 1 response").into_with_reasoning("previous reasoning"),
    );

    let _ = provider.chat(req).await.expect("chat should succeed");

    let recorded = server.requests();
    assert_eq!(recorded.len(), 1);
    let messages = recorded[0]
        .get("messages")
        .and_then(|m| m.as_array())
        .unwrap();
    // 第二条消息（assistant）应包含 reasoning_content 字段
    let assistant_msg = &messages[0];
    assert_eq!(
        assistant_msg.get("role").and_then(|r| r.as_str()),
        Some("assistant")
    );
    assert_eq!(
        assistant_msg
            .get("reasoning_content")
            .and_then(|r| r.as_str()),
        Some("previous reasoning"),
        "reasoning_content must be preserved in multi-turn body"
    );
}

// ───────────────────────────────────────────────
// 测试 10：工具声明 — 请求 body 包含 tools 数组
// ───────────────────────────────────────────────
#[tokio::test]
async fn tools_are_included_in_body() {
    let server = MockServer::start(|_req| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "mimo-v2.5-pro",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok", "tool_calls": [{"id": "call_1", "function": {"name": "get_weather", "arguments": "{\"location\":\"Beijing\"}"}}]}, "finish_reason": "tool_calls"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = make_provider(&server);
    let mut req = ChatRequest::simple("weather?");
    req.tools = vec![ToolDeclaration {
        name: "get_weather".into(),
        description: "Get weather".into(),
        parameters: json!({"type": "object", "properties": {"location": {"type": "string"}}}),
    }];

    let resp = provider.chat(req).await.expect("chat should succeed");
    assert_eq!(resp.finish_reason, FinishReason::ToolCalls);
    assert_eq!(resp.message.tool_calls.len(), 1);
    assert_eq!(resp.message.tool_calls[0].id, "call_1");
    assert_eq!(resp.message.tool_calls[0].function.name, "get_weather");
    assert_eq!(
        resp.message.tool_calls[0].function.arguments,
        r#"{"location":"Beijing"}"#
    );

    // 验证 body 包含 tools
    let recorded = server.requests();
    let tools = recorded[0].get("tools").and_then(|t| t.as_array()).unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(
        tools[0].get("type").and_then(|t| t.as_str()),
        Some("function")
    );
    assert_eq!(
        tools[0]
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(|n| n.as_str()),
        Some("get_weather")
    );
}

// 辅助 trait：为 Message 添加 reasoning_content 的链式构造
trait MessageExt {
    fn into_with_reasoning(self, reasoning: &str) -> Self;
}

impl MessageExt for Message {
    fn into_with_reasoning(mut self, reasoning: &str) -> Self {
        self.reasoning_content = Some(reasoning.to_string());
        self
    }
}

//! DeepSeek 适配器测试：契约 + 流式 + 错误归一 + 重试 + 能力声明
//!
//! 重点覆盖 DeepSeek 独有特性：
//! - `reasoning_effort` 参数（low/high/max，MiMo 不支持）
//! - `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens` 缓存命中指标
//! - 402 余额不足 → `InsufficientBalance` 归一（与 MiMo 共享语义）
//! - `thinking` 开关（与 MiMo 协议一致，但独立验证以隔离 regression）

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use referee_agent::provider::deepseek::{
    ids, DeepSeekConfig, DeepSeekModel, DeepSeekProvider, MAX_OUTPUT_TOKENS,
};
use referee_agent::provider::{
    ChatRequest, FinishReason, LLMProvider, LlmError, Message, ReasoningEffort, RetryPolicy,
    StreamChunk, ToolDeclaration,
};
use serde_json::json;

use common::{MockResponse, MockServer};

fn make_provider(server: &MockServer) -> DeepSeekProvider {
    DeepSeekProvider::new(
        DeepSeekModel::V4Pro,
        DeepSeekConfig::new("test-key")
            .with_base_url(&server.base_url)
            .with_retry(RetryPolicy::no_retry()),
    )
    .expect("provider creation should succeed")
}

// ───────────────────────────────────────────────
// 测试 1：非流式 chat() 契约 — content / reasoning_content / 缓存 usage
// ───────────────────────────────────────────────
#[tokio::test]
async fn chat_parses_content_reasoning_and_cache_usage() {
    let server = MockServer::start(|req| {
        assert!(
            !req.is_stream(),
            "non-streaming request should have stream=false"
        );
        MockResponse::Json {
            status: 200,
            body: json!({
                "id": "chatcmpl-ds-1",
                "object": "chat.completion",
                "created": 1781234029,
                "model": "deepseek-v4-pro",
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "Beijing is the capital of China.",
                        "reasoning_content": "User asks about China's capital."
                    },
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                    "total_tokens": 150,
                    "prompt_cache_hit_tokens": 80,
                    "prompt_cache_miss_tokens": 20,
                    "completion_tokens_details": {
                        "reasoning_tokens": 15
                    }
                }
            }),
        }
    })
    .await;

    let provider = make_provider(&server);
    let resp = provider
        .chat(ChatRequest::simple("What is the capital of China?"))
        .await
        .expect("chat should succeed");

    assert_eq!(resp.id, "chatcmpl-ds-1");
    assert_eq!(resp.model, "deepseek-v4-pro");
    assert_eq!(
        resp.message.content.as_text().unwrap(),
        "Beijing is the capital of China."
    );
    assert_eq!(
        resp.message.reasoning_content.as_deref(),
        Some("User asks about China's capital.")
    );
    assert_eq!(resp.finish_reason, FinishReason::Stop);

    let usage = resp.usage.expect("usage should be present");
    assert_eq!(usage.prompt_tokens, 100);
    assert_eq!(usage.completion_tokens, 50);
    assert_eq!(usage.total_tokens, 150);
    assert_eq!(usage.reasoning_tokens, Some(15));
    // DeepSeek 独有缓存指标
    assert_eq!(usage.prompt_cache_hit_tokens, Some(80));
    assert_eq!(usage.prompt_cache_miss_tokens, Some(20));
}

// ───────────────────────────────────────────────
// 测试 2：流式 chat_stream() — Delta 增量 + 缓存 usage 收敛
// ───────────────────────────────────────────────
#[tokio::test]
async fn chat_stream_emits_deltas_with_reasoning_and_cache_usage() {
    let server = MockServer::start(|req| {
        assert!(req.is_stream(), "streaming request should have stream=true");
        MockResponse::Sse {
            status: 200,
            with_done: true,
            chunks: vec![
                json!({
                    "id": "ds-1",
                    "model": "deepseek-v4-pro",
                    "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": null}]
                }),
                json!({
                    "id": "ds-1",
                    "model": "deepseek-v4-pro",
                    "choices": [{"index": 0, "delta": {"reasoning_content": "Reasoning step 1"}, "finish_reason": null}]
                }),
                json!({
                    "id": "ds-1",
                    "model": "deepseek-v4-pro",
                    "choices": [{"index": 0, "delta": {"content": "Final"}, "finish_reason": null}]
                }),
                json!({
                    "id": "ds-1",
                    "model": "deepseek-v4-pro",
                    "choices": [{"index": 0, "delta": {"content": " answer"}, "finish_reason": "stop"}]
                }),
                json!({
                    "id": "ds-1",
                    "model": "deepseek-v4-pro",
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 5,
                        "total_tokens": 15,
                        "prompt_cache_hit_tokens": 8,
                        "prompt_cache_miss_tokens": 2
                    }
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

    let mut content_acc = String::new();
    let mut reasoning_acc = String::new();
    let mut finish = None;
    while let Some(chunk) = stream.next().await {
        match chunk.expect("chunk should be ok") {
            StreamChunk::Delta {
                content,
                reasoning_content,
                ..
            } => {
                if let Some(c) = content {
                    content_acc.push_str(&c);
                }
                if let Some(r) = reasoning_content {
                    reasoning_acc.push_str(&r);
                }
            }
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                finish = Some((finish_reason, usage));
            }
        }
    }

    assert_eq!(content_acc, "Final answer");
    assert_eq!(reasoning_acc, "Reasoning step 1");

    let (fr, usage) = finish.expect("Finish chunk should be emitted");
    assert_eq!(fr, FinishReason::Stop);
    let usage = usage.expect("usage should be present");
    assert_eq!(usage.prompt_cache_hit_tokens, Some(8));
    assert_eq!(usage.prompt_cache_miss_tokens, Some(2));
}

// ───────────────────────────────────────────────
// 测试 3：reasoning_effort 在设置时写入 body（DeepSeek 独有）
// ───────────────────────────────────────────────
#[tokio::test]
async fn reasoning_effort_included_when_set() {
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "deepseek-v4-pro",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = make_provider(&server);
    let mut req = ChatRequest::simple("test");
    req.thinking.effort = Some(ReasoningEffort::Max);

    let _ = provider.chat(req).await.expect("chat should succeed");

    let recorded = server.requests();
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].get("reasoning_effort").and_then(|v| v.as_str()),
        Some("max"),
        "reasoning_effort should be 'max' when set"
    );
    // thinking 字段应同时存在
    assert_eq!(
        recorded[0]
            .get("thinking")
            .and_then(|t| t.get("type"))
            .and_then(|t| t.as_str()),
        Some("enabled"),
        "thinking.type should be 'enabled' (default)"
    );
}

// ───────────────────────────────────────────────
// 测试 4：reasoning_effort 默认不写入 body（让服务端用默认 high）
// ───────────────────────────────────────────────
#[tokio::test]
async fn reasoning_effort_omitted_by_default() {
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "deepseek-v4-pro",
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
    assert!(
        recorded[0].get("reasoning_effort").is_none(),
        "reasoning_effort should NOT be in body when not explicitly set"
    );
}

// ───────────────────────────────────────────────
// 测试 5：reasoning_effort 各取值序列化正确
// ───────────────────────────────────────────────
#[tokio::test]
async fn reasoning_effort_serializes_all_variants() {
    let cases = [
        (ReasoningEffort::Low, "low"),
        (ReasoningEffort::High, "high"),
        (ReasoningEffort::Max, "max"),
    ];

    for (effort, expected) in cases {
        let server = MockServer::start(|_| MockResponse::Json {
            status: 200,
            body: json!({
                "id": "test",
                "model": "deepseek-v4-pro",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
            }),
        })
        .await;

        let provider = make_provider(&server);
        let mut req = ChatRequest::simple("test");
        req.thinking.effort = Some(effort);
        let _ = provider.chat(req).await.expect("chat should succeed");

        let recorded = server.requests();
        assert_eq!(
            recorded[0].get("reasoning_effort").and_then(|v| v.as_str()),
            Some(expected),
            "reasoning_effort should serialize as {expected} for {effort:?}"
        );
    }
}

// ───────────────────────────────────────────────
// 测试 6：thinking 开关 — disabled 时 body 包含 thinking.type=disabled
// ───────────────────────────────────────────────
#[tokio::test]
async fn thinking_toggle_disabled_writes_disabled_type() {
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "deepseek-v4-pro",
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
// 测试 7：错误归一 — 402 余额不足 → InsufficientBalance（两家共有语义）
// ───────────────────────────────────────────────
#[tokio::test]
async fn error_normalization_402_balance_maps_to_insufficient_balance() {
    let server = MockServer::start(|_| MockResponse::Raw {
        status: 402,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: br#"{"error":{"message":"insufficient balance"}}"#.to_vec(),
    })
    .await;

    let provider = make_provider(&server);
    let err = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect_err("should return error");
    assert!(
        matches!(err, LlmError::InsufficientBalance(ref s) if s.contains("402")),
        "402 should map to InsufficientBalance with status, got {err:?}"
    );
}

// ───────────────────────────────────────────────
// 测试 8：错误归一 — 422 参数错误 → BadRequest
// ───────────────────────────────────────────────
#[tokio::test]
async fn error_normalization_422_param_error_maps_to_bad_request() {
    let server = MockServer::start(|_| MockResponse::Raw {
        status: 422,
        headers: vec![("Content-Type".into(), "application/json".into())],
        body: br#"{"error":{"message":"param error"}}"#.to_vec(),
    })
    .await;

    let provider = make_provider(&server);
    let err = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect_err("should return error");
    assert!(
        matches!(err, LlmError::BadRequest(ref s) if s.contains("422")),
        "422 should map to BadRequest, got {err:?}"
    );
}

// ───────────────────────────────────────────────
// 测试 9：错误归一 — 429 限流带 Retry-After
// ───────────────────────────────────────────────
#[tokio::test]
async fn error_normalization_429_rate_limited_with_retry_after() {
    let server = MockServer::start(|_| MockResponse::Raw {
        status: 429,
        headers: vec![
            ("Content-Type".into(), "application/json".into()),
            ("Retry-After".into(), "3".into()),
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
            retry_after: Some(Duration::from_secs(3))
        }
    );
}

// ───────────────────────────────────────────────
// 测试 10：重试 — 500 后成功（指数退避，RateLimited 用 Retry-After）
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
                    "model": "deepseek-v4-pro",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
                }),
            }
        }
    })
    .await;

    let provider = DeepSeekProvider::new(
        DeepSeekModel::V4Pro,
        DeepSeekConfig::new("test-key")
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
// 测试 11：能力声明
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
        "DeepSeek supports parallel tool calls"
    );
    assert!(caps.system_role, "DeepSeek supports system role");
    assert!(caps.streaming, "DeepSeek supports streaming");
    assert!(caps.usage_reported, "DeepSeek reports usage");
    assert_eq!(caps.max_output_tokens, MAX_OUTPUT_TOKENS);
    assert_eq!(provider.id(), ids::DEEPSEEK_V4_PRO);
}

// ───────────────────────────────────────────────
// 测试 12：多轮工具调用 — assistant 消息回传 reasoning_content
// ───────────────────────────────────────────────
#[tokio::test]
async fn multi_turn_preserves_reasoning_content_in_body() {
    let server = MockServer::start(|_req| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "deepseek-v4-pro",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = make_provider(&server);
    let mut req = ChatRequest::simple("turn 2");
    req.messages.insert(
        0,
        MessageExt::into_with_reasoning(
            Message::assistant("turn 1 response"),
            "previous reasoning from turn 1",
        ),
    );

    let _ = provider.chat(req).await.expect("chat should succeed");

    let recorded = server.requests();
    assert_eq!(recorded.len(), 1);
    let messages = recorded[0]
        .get("messages")
        .and_then(|m| m.as_array())
        .unwrap();
    let assistant_msg = &messages[0];
    assert_eq!(
        assistant_msg.get("role").and_then(|r| r.as_str()),
        Some("assistant")
    );
    assert_eq!(
        assistant_msg
            .get("reasoning_content")
            .and_then(|r| r.as_str()),
        Some("previous reasoning from turn 1"),
        "reasoning_content must be preserved in multi-turn body"
    );
}

// ───────────────────────────────────────────────
// 测试 13：工具声明 + 工具调用响应解析
// ───────────────────────────────────────────────
#[tokio::test]
async fn tools_are_included_in_body_and_parsed() {
    let server = MockServer::start(|_req| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "deepseek-v4-pro",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": null,
                    "reasoning_content": "Need to call get_weather",
                    "tool_calls": [{
                        "id": "call_ds_1",
                        "function": {"name": "get_weather", "arguments": "{\"city\":\"Hangzhou\"}"}
                    }]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = make_provider(&server);
    let mut req = ChatRequest::simple("weather in Hangzhou?");
    req.tools = vec![ToolDeclaration {
        name: "get_weather".into(),
        description: "Get weather for a city".into(),
        parameters: json!({
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"]
        }),
    }];

    let resp = provider.chat(req).await.expect("chat should succeed");
    assert_eq!(resp.finish_reason, FinishReason::ToolCalls);
    assert_eq!(resp.message.tool_calls.len(), 1);
    assert_eq!(resp.message.tool_calls[0].id, "call_ds_1");
    assert_eq!(resp.message.tool_calls[0].function.name, "get_weather");
    assert_eq!(
        resp.message.tool_calls[0].function.arguments,
        r#"{"city":"Hangzhou"}"#
    );
    assert_eq!(
        resp.message.reasoning_content.as_deref(),
        Some("Need to call get_weather")
    );

    // body 中 tools 应被转译为 OpenAI function 格式
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
    // parameters schema 应保留 required 字段
    assert_eq!(
        tools[0]
            .get("function")
            .and_then(|f| f.get("parameters"))
            .and_then(|p| p.get("required"))
            .and_then(|r| r.as_array())
            .and_then(|a| a.first())
            .and_then(|s| s.as_str()),
        Some("city"),
        "required field in JSON Schema must be preserved"
    );
}

// ───────────────────────────────────────────────
// 测试 14：V4Flash 模型 id 与 model 字段
// ───────────────────────────────────────────────
#[tokio::test]
async fn v4_flash_model_id_and_model_field() {
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "test",
            "model": "deepseek-v4-flash",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        }),
    })
    .await;

    let provider = DeepSeekProvider::new(
        DeepSeekModel::V4Flash,
        DeepSeekConfig::new("test-key")
            .with_base_url(&server.base_url)
            .with_retry(RetryPolicy::no_retry()),
    )
    .expect("provider creation");

    assert_eq!(provider.id(), ids::DEEPSEEK_V4_FLASH);

    let _ = provider
        .chat(ChatRequest::simple("test"))
        .await
        .expect("chat should succeed");

    let recorded = server.requests();
    assert_eq!(
        recorded[0].get("model").and_then(|m| m.as_str()),
        Some("deepseek-v4-flash"),
        "model field should match V4Flash"
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

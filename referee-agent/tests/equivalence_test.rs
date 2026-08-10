//! 跨厂商语义等价测试
//!
//! Phase 0 验收标准 1：同一 `ChatRequest` 在不同适配器下语义等价；
//! 流式收敛 == 一次性响应。
//!
//! 设计要点：
//! - 厂商特定字段（id / model）天然不同，等价性只校验语义字段：
//!   content / reasoning_content / finish_reason / usage（含缓存指标）
//! - 流式收敛：mock 返回的 SSE chunks 累积后必须等于非流式 chat() 解析结果
//! - 公共 body 字段（messages / tools / tool_choice / stream）必须一致；
//!   厂商特有字段（MiMo 无 reasoning_effort，DeepSeek 有）允许差异

mod common;

use futures::StreamExt;
use referee_agent::provider::deepseek::{DeepSeekConfig, DeepSeekModel, DeepSeekProvider};
use referee_agent::provider::xiaomi::{XiaomiConfig, XiaomiModel, XiaomiProvider};
use referee_agent::provider::{
    ChatRequest, ChatResponse, FinishReason, LLMProvider, RetryPolicy, StreamChunk, ToolDeclaration,
};
use serde_json::json;

use common::{MockResponse, MockServer};

/// 构造 MiMo 适配器（指向同一 mock server）
fn make_mimo(server: &MockServer) -> XiaomiProvider {
    XiaomiProvider::new(
        XiaomiModel::MimoV25Pro,
        XiaomiConfig::new("test-key")
            .with_base_url(&server.base_url)
            .with_retry(RetryPolicy::no_retry()),
    )
    .expect("mimo provider")
}

/// 构造 DeepSeek 适配器（指向同一 mock server）
fn make_deepseek(server: &MockServer) -> DeepSeekProvider {
    DeepSeekProvider::new(
        DeepSeekModel::V4Pro,
        DeepSeekConfig::new("test-key")
            .with_base_url(&server.base_url)
            .with_retry(RetryPolicy::no_retry()),
    )
    .expect("deepseek provider")
}

/// 等价性核心断言：忽略 id / model（厂商特定），其余语义字段必须相等
fn assert_semantically_equivalent(a: &ChatResponse, b: &ChatResponse, ctx: &str) {
    assert_eq!(
        a.message.content, b.message.content,
        "{ctx}: content mismatch"
    );
    assert_eq!(
        a.message.reasoning_content, b.message.reasoning_content,
        "{ctx}: reasoning_content mismatch"
    );
    assert_eq!(
        a.message.tool_calls, b.message.tool_calls,
        "{ctx}: tool_calls mismatch"
    );
    assert_eq!(
        a.finish_reason, b.finish_reason,
        "{ctx}: finish_reason mismatch"
    );
    assert_eq!(a.usage, b.usage, "{ctx}: usage mismatch");
}

// ───────────────────────────────────────────────
// 测试 1：同一 ChatRequest 在 MiMo / DeepSeek 下语义等价
// ───────────────────────────────────────────────
#[tokio::test]
async fn same_request_yields_semantically_equivalent_response() {
    // mock 返回中性的响应（不携带任何厂商特有字段，仅 OpenAI 标准字段）
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "neutral-id",
            "object": "chat.completion",
            "created": 1781234029,
            "model": "neutral-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Paris is the capital of France.",
                    "reasoning_content": "User asks about France's capital."
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 8,
                "total_tokens": 20,
                "completion_tokens_details": {"reasoning_tokens": 3}
            }
        }),
    })
    .await;

    let req = ChatRequest::simple("Capital of France?");

    let mimo_resp = make_mimo(&server)
        .chat(req.clone())
        .await
        .expect("mimo chat");
    let ds_resp = make_deepseek(&server)
        .chat(req)
        .await
        .expect("deepseek chat");

    // 厂商特定字段：id / model 来自 mock body，二者一致（同一 mock）
    // 真实场景下会不同，本测试只断言语义字段
    assert_semantically_equivalent(&mimo_resp, &ds_resp, "cross-vendor chat()");
}

// ───────────────────────────────────────────────
// 测试 2：带工具调用的响应在两家厂商下语义等价
// ───────────────────────────────────────────────
#[tokio::test]
async fn tool_call_response_is_semantically_equivalent() {
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "tc-1",
            "model": "any",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": null,
                    "reasoning_content": "Need to call get_weather",
                    "tool_calls": [
                        {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}},
                        {"id": "call_2", "type": "function", "function": {"name": "get_date", "arguments": "{}"}}
                    ]
                },
                "finish_reason": "tool_calls"
            }],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
        }),
    })
    .await;

    let mut req = ChatRequest::simple("Weather in Paris?");
    req.tools = vec![
        ToolDeclaration {
            name: "get_weather".into(),
            description: "Get weather".into(),
            parameters: json!({"type": "object", "properties": {"city": {"type": "string"}}}),
        },
        ToolDeclaration {
            name: "get_date".into(),
            description: "Get current date".into(),
            parameters: json!({"type": "object", "properties": {}}),
        },
    ];

    let mimo_resp = make_mimo(&server)
        .chat(req.clone())
        .await
        .expect("mimo chat");
    let ds_resp = make_deepseek(&server)
        .chat(req)
        .await
        .expect("deepseek chat");

    assert_semantically_equivalent(&mimo_resp, &ds_resp, "cross-vendor tool_calls");
    // 额外校验：finish_reason 必须是 ToolCalls（工具调用语义）
    assert_eq!(mimo_resp.finish_reason, FinishReason::ToolCalls);
    assert_eq!(ds_resp.finish_reason, FinishReason::ToolCalls);
    assert_eq!(mimo_resp.message.tool_calls.len(), 2);
}

// ───────────────────────────────────────────────
// 测试 3：流式收敛 == 非流式响应（MiMo）
// ───────────────────────────────────────────────
#[tokio::test]
async fn streaming_converges_to_chat_mimo() {
    streaming_converges_to_chat("mimo").await;
}

// ───────────────────────────────────────────────
// 测试 4：流式收敛 == 非流式响应（DeepSeek）
// ───────────────────────────────────────────────
#[tokio::test]
async fn streaming_converges_to_chat_deepseek() {
    streaming_converges_to_chat("deepseek").await;
}

/// 流式收敛通用实现：同一 mock 响应内容分别以 JSON 和 SSE 形式返回，
/// 断言累积流式结果与非流式 chat() 等价
async fn streaming_converges_to_chat(vendor: &str) {
    // 非流式响应内容
    let final_content = "The answer is 42.";
    let final_reasoning = "Reasoning about life, universe and everything.";
    let usage_json = json!({
        "prompt_tokens": 11,
        "completion_tokens": 7,
        "total_tokens": 18,
        "completion_tokens_details": {"reasoning_tokens": 4}
    });
    // 为两个独立闭包各克隆一份（闭包需 move 捕获以满足 'static）
    let usage_json_for_sse = usage_json.clone();

    // 两个 server：一个返回非流式 JSON，一个返回流式 SSE（内容拼起来等价）
    let json_server = MockServer::start(move |_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "conv-1",
            "model": "any",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": final_content,
                    "reasoning_content": final_reasoning,
                },
                "finish_reason": "stop"
            }],
            "usage": usage_json.clone()
        }),
    })
    .await;

    let sse_server = MockServer::start(move |_| MockResponse::Sse {
        status: 200,
        with_done: true,
        chunks: vec![
            json!({
                "id": "conv-1",
                "model": "any",
                "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": null}]
            }),
            // 拆分 reasoning 为 2 个 delta，验证累积
            json!({
                "id": "conv-1",
                "model": "any",
                "choices": [{"index": 0, "delta": {"reasoning_content": "Reasoning about "}, "finish_reason": null}]
            }),
            json!({
                "id": "conv-1",
                "model": "any",
                "choices": [{"index": 0, "delta": {"reasoning_content": "life, universe and everything."}, "finish_reason": null}]
            }),
            // 拆分 content 为 2 个 delta
            json!({
                "id": "conv-1",
                "model": "any",
                "choices": [{"index": 0, "delta": {"content": "The answer is "}, "finish_reason": null}]
            }),
            json!({
                "id": "conv-1",
                "model": "any",
                "choices": [{"index": 0, "delta": {"content": "42."}, "finish_reason": "stop"}]
            }),
            // 独立 usage chunk（MiMo / DeepSeek 都可能这样发）
            json!({
                "id": "conv-1",
                "model": "any",
                "choices": [],
                "usage": usage_json_for_sse.clone()
            }),
        ],
    })
    .await;

    let req = ChatRequest::simple("What is the answer?");

    // 非流式
    let chat_resp = match vendor {
        "mimo" => make_mimo(&json_server)
            .chat(req.clone())
            .await
            .expect("mimo chat"),
        "deepseek" => make_deepseek(&json_server)
            .chat(req.clone())
            .await
            .expect("deepseek chat"),
        _ => unreachable!(),
    };

    // 流式：累积
    let mut stream = match vendor {
        "mimo" => make_mimo(&sse_server)
            .chat_stream(req)
            .await
            .expect("mimo stream"),
        "deepseek" => make_deepseek(&sse_server)
            .chat_stream(req)
            .await
            .expect("deepseek stream"),
        _ => unreachable!(),
    };

    let mut content_acc = String::new();
    let mut reasoning_acc = String::new();
    let mut finish: Option<(FinishReason, Option<_>)> = None;
    while let Some(chunk) = stream.next().await {
        match chunk.expect("chunk ok") {
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

    // 流式累积 == 非流式
    assert_eq!(
        content_acc,
        chat_resp.message.content.as_text().unwrap_or(""),
        "{vendor}: streamed content must equal chat() content"
    );
    assert_eq!(
        reasoning_acc,
        chat_resp.message.reasoning_content.as_deref().unwrap_or(""),
        "{vendor}: streamed reasoning must equal chat() reasoning"
    );

    let (stream_fr, stream_usage) = finish.expect("Finish chunk must be emitted");
    assert_eq!(
        stream_fr, chat_resp.finish_reason,
        "{vendor}: streamed finish_reason must equal chat()"
    );
    assert_eq!(
        stream_usage, chat_resp.usage,
        "{vendor}: streamed usage must equal chat() usage"
    );
}

// ───────────────────────────────────────────────
// 测试 5：跨厂商公共 body 字段一致（messages / tools / tool_choice / temperature）
// ───────────────────────────────────────────────
#[tokio::test]
async fn common_body_fields_are_identical_across_vendors() {
    // 用两个独立 server（每个厂商一个）以分别记录 body
    let mimo_server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({"id": "x", "model": "x", "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}]}),
    })
    .await;
    let ds_server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({"id": "x", "model": "x", "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}]}),
    })
    .await;

    let mut req = ChatRequest::simple("common body test");
    req.messages.insert(
        0,
        referee_agent::provider::Message::system("You are helpful."),
    );
    req.temperature = Some(0.7);
    req.tools = vec![ToolDeclaration {
        name: "get_date".into(),
        description: "Get date".into(),
        parameters: json!({"type": "object", "properties": {}}),
    }];

    let _ = make_mimo(&mimo_server)
        .chat(req.clone())
        .await
        .expect("mimo chat");
    let _ = make_deepseek(&ds_server)
        .chat(req)
        .await
        .expect("deepseek chat");

    let mimo_body = &mimo_server.requests()[0];
    let ds_body = &ds_server.requests()[0];

    // 公共字段必须一致
    assert_eq!(
        mimo_body.get("messages"),
        ds_body.get("messages"),
        "messages must be identical across vendors"
    );
    assert_eq!(
        mimo_body.get("tools"),
        ds_body.get("tools"),
        "tools must be identical across vendors"
    );
    assert_eq!(
        mimo_body.get("tool_choice"),
        ds_body.get("tool_choice"),
        "tool_choice must be identical across vendors"
    );
    assert_eq!(
        mimo_body.get("temperature"),
        ds_body.get("temperature"),
        "temperature must be identical across vendors"
    );
    assert_eq!(
        mimo_body.get("stream"),
        ds_body.get("stream"),
        "stream flag must be identical"
    );

    // thinking 字段：两家协议一致（type=enabled/disabled）
    assert_eq!(
        mimo_body.get("thinking"),
        ds_body.get("thinking"),
        "thinking field must be identical (both vendors share protocol)"
    );

    // 厂商特有字段：DeepSeek 默认不写 reasoning_effort（除非显式设置），
    // MiMo 永远不写 reasoning_effort。两者 body 都不应包含。
    assert!(
        mimo_body.get("reasoning_effort").is_none(),
        "MiMo body must never include reasoning_effort"
    );
    assert!(
        ds_body.get("reasoning_effort").is_none(),
        "DeepSeek body must NOT include reasoning_effort by default"
    );

    // model 字段：厂商特定，允许不同
    assert_ne!(
        mimo_body.get("model"),
        ds_body.get("model"),
        "model field should differ across vendors"
    );
}

// ───────────────────────────────────────────────
// 测试 6：能力声明驱动降级 — MiMo 忽略 reasoning_effort（不写入 body）
// ───────────────────────────────────────────────
#[tokio::test]
async fn mimo_silently_ignores_reasoning_effort() {
    // 验证设计原则：能力声明驱动的厂商降级。
    // 调用方设置了 thinking.effort（DeepSeek 概念），MiMo 适配器应忽略此字段，
    // 不写入 body 也不报错。这是「不写厂商分支」原则的体现。
    let server = MockServer::start(|_| MockResponse::Json {
        status: 200,
        body: json!({"id": "x", "model": "x", "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}]}),
    })
    .await;

    let provider = make_mimo(&server);
    let mut req = ChatRequest::simple("test");
    req.thinking.effort = Some(referee_agent::provider::ReasoningEffort::Max);

    let resp = provider.chat(req).await.expect("chat should succeed");
    assert_eq!(resp.message.content.as_text().unwrap(), "ok");

    let body = &server.requests()[0];
    assert!(
        body.get("reasoning_effort").is_none(),
        "MiMo must silently drop reasoning_effort (capability-driven downgrade)"
    );
    // thinking 字段仍应存在（两家协议共享）
    assert_eq!(
        body.get("thinking")
            .and_then(|t| t.get("type"))
            .and_then(|t| t.as_str()),
        Some("enabled"),
        "thinking field must still be present"
    );
}

// ───────────────────────────────────────────────
// 测试 7：两家厂商对同一中立错误响应归一为相同的 LlmError
// ───────────────────────────────────────────────
#[tokio::test]
async fn error_normalization_is_equivalent_across_vendors() {
    let cases: Vec<(u16, &'static str)> = vec![
        (401, "auth"),
        (429, "rate_limited"),
        (500, "server"),
        (400, "bad_request"),
    ];

    for (status, label) in cases {
        let mimo_server = MockServer::start(move |_| MockResponse::Raw {
            status,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: br#"{"error":"x"}"#.to_vec(),
        })
        .await;
        let ds_server = MockServer::start(move |_| MockResponse::Raw {
            status,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: br#"{"error":"x"}"#.to_vec(),
        })
        .await;

        let mimo_err = make_mimo(&mimo_server)
            .chat(ChatRequest::simple("test"))
            .await
            .expect_err("mimo should err");
        let ds_err = make_deepseek(&ds_server)
            .chat(ChatRequest::simple("test"))
            .await
            .expect_err("deepseek should err");

        assert_eq!(
            mimo_err, ds_err,
            "status {status} ({label}): error normalization must be equivalent across vendors"
        );
    }
}

// ───────────────────────────────────────────────
// 测试 8：无 [DONE] 终止符时流仍能收敛（部分厂商不发 [DONE]）
// ───────────────────────────────────────────────
#[tokio::test]
async fn stream_converges_without_done_marker() {
    let server = MockServer::start(|_| MockResponse::Sse {
        status: 200,
        with_done: false, // 不发 [DONE]
        chunks: vec![
            json!({
                "id": "no-done",
                "model": "any",
                "choices": [{"index": 0, "delta": {"content": "Hi"}, "finish_reason": null}]
            }),
            json!({
                "id": "no-done",
                "model": "any",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]
            }),
            json!({
                "id": "no-done",
                "model": "any",
                "choices": [],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
            }),
        ],
    })
    .await;

    let req = ChatRequest::simple("Hi");

    // MiMo
    let mut stream = make_mimo(&server)
        .chat_stream(req.clone())
        .await
        .expect("mimo stream");
    let mut mimo_content = String::new();
    let mut mimo_finish = None;
    while let Some(chunk) = stream.next().await {
        match chunk.expect("chunk ok") {
            StreamChunk::Delta { content, .. } => {
                if let Some(c) = content {
                    mimo_content.push_str(&c);
                }
            }
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                mimo_finish = Some((finish_reason, usage));
            }
        }
    }
    assert_eq!(mimo_content, "Hi");
    let (fr, usage) = mimo_finish.expect("mimo Finish");
    assert_eq!(fr, FinishReason::Stop);
    assert!(usage.is_some(), "usage must be picked up without [DONE]");

    // DeepSeek
    let mut stream = make_deepseek(&server)
        .chat_stream(req)
        .await
        .expect("deepseek stream");
    let mut ds_content = String::new();
    let mut ds_finish = None;
    while let Some(chunk) = stream.next().await {
        match chunk.expect("chunk ok") {
            StreamChunk::Delta { content, .. } => {
                if let Some(c) = content {
                    ds_content.push_str(&c);
                }
            }
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                ds_finish = Some((finish_reason, usage));
            }
        }
    }
    assert_eq!(ds_content, "Hi");
    assert_eq!(ds_finish.map(|(fr, _)| fr), Some(FinishReason::Stop));

    // 跨厂商流式收敛结果等价
    assert_eq!(
        mimo_content, ds_content,
        "cross-vendor stream content equal"
    );
}

// ───────────────────────────────────────────────
// 测试 9：两家厂商 chat() 与 chat_stream() 在相同中立响应下也互相等价
// （MiMo chat vs DeepSeek stream，反向亦然）
// ───────────────────────────────────────────────
#[tokio::test]
async fn cross_vendor_chat_equals_cross_vendor_stream() {
    let final_content = "Equivalence check.";
    let usage_json = json!({
        "prompt_tokens": 4,
        "completion_tokens": 3,
        "total_tokens": 7
    });
    let usage_json_for_sse = usage_json.clone();

    // MiMo 用 JSON（chat），DeepSeek 用 SSE（stream）
    let mimo_json_server = MockServer::start(move |_| MockResponse::Json {
        status: 200,
        body: json!({
            "id": "xv-1",
            "model": "any",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": final_content},
                "finish_reason": "stop"
            }],
            "usage": usage_json.clone()
        }),
    })
    .await;

    let ds_sse_server = MockServer::start(move |_| MockResponse::Sse {
        status: 200,
        with_done: true,
        chunks: vec![
            json!({
                "id": "xv-1",
                "model": "any",
                "choices": [{"index": 0, "delta": {"content": final_content}, "finish_reason": "stop"}]
            }),
            json!({
                "id": "xv-1",
                "model": "any",
                "choices": [],
                "usage": usage_json_for_sse.clone()
            }),
        ],
    })
    .await;

    let req = ChatRequest::simple("test");

    let mimo_chat_resp = make_mimo(&mimo_json_server)
        .chat(req.clone())
        .await
        .expect("mimo chat");

    let mut ds_stream = make_deepseek(&ds_sse_server)
        .chat_stream(req)
        .await
        .expect("deepseek stream");
    let mut ds_content = String::new();
    let mut ds_finish = None;
    while let Some(chunk) = ds_stream.next().await {
        match chunk.expect("chunk ok") {
            StreamChunk::Delta { content, .. } => {
                if let Some(c) = content {
                    ds_content.push_str(&c);
                }
            }
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                ds_finish = Some((finish_reason, usage));
            }
        }
    }

    assert_eq!(
        ds_content,
        mimo_chat_resp.message.content.as_text().unwrap(),
        "deepseek streamed content == mimo chat content"
    );
    let (ds_fr, ds_usage) = ds_finish.expect("deepseek Finish");
    assert_eq!(ds_fr, mimo_chat_resp.finish_reason);
    assert_eq!(ds_usage, mimo_chat_resp.usage);
}

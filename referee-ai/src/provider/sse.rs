//! 共用 SSE 流解析 — OpenAI 兼容（Chat Completions）与 Anthropic Messages
//! 协议共享的字节流 → JSON 事件解析器
//!
//! 两者均以单行 `data: {json}` + `\n\n` 事件分隔符推送增量。本模块负责：
//! 1. 缓冲字节直到出现完整事件（`\n\n` / `\r\n\r\n` 分隔）
//! 2. 提取 `data:` 字段（多行 data 以 `\n` 拼接）并解析为 JSON `Value`
//! 3. `[DONE]`（OpenAI）或厂商流终止信号处理
//!
//! 本模块只产出「JSON 事件流」，协议特有的事件→[`StreamChunk`] 语义
//! 归各协议映射（`openai_compat` / `anthropic_compat`），以恪守职责分明。

use std::collections::BTreeMap;

use futures::stream::{self, BoxStream, StreamExt};
use futures::Stream;
use serde_json::Value;

use crate::provider::{
    ChatResponse, FinishReason, LlmError, Message, MessageContent, Role, StreamChunk, TokenUsage,
    ToolCall, ToolCallFunction,
};

/// 将字节流解析为 SSE 事件的 JSON `Value` 流
///
/// `BoxStream` 自身 `Unpin`，可在 async 闭包中直接 `.next().await`。
pub(crate) fn parse_sse_stream(
    byte_stream: impl Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send + 'static,
) -> BoxStream<'static, Result<Value, LlmError>> {
    let state = SseState {
        buffer: Vec::new(),
        inner: Box::pin(byte_stream),
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        loop {
            // 1. 尝试从缓冲提取完整事件
            if let Some((event_bytes, rest)) = take_sse_event(&state.buffer) {
                state.buffer = rest;
                if let Some(data) = parse_data_field(&event_bytes) {
                    if data.trim() == "[DONE]" {
                        return None;
                    }
                    match serde_json::from_str::<Value>(&data) {
                        Ok(v) => return Some((Ok(v), state)),
                        Err(e) => {
                            return Some((
                                Err(LlmError::Protocol(format!("SSE JSON parse: {e}"))),
                                state,
                            ))
                        }
                    }
                }
                // 非 data 事件（event:/id:/comment）— 跳过
                continue;
            }
            // 2. 拉取更多字节
            match state.inner.next().await {
                Some(Ok(bytes)) => state.buffer.extend_from_slice(&bytes),
                Some(Err(e)) => return Some((Err(map_reqwest_err(e)), state)),
                None => {
                    // 流结束：刷新残余缓冲（部分厂商不发终止信号）
                    if !state.buffer.is_empty() {
                        if let Some(data) = parse_data_field(&state.buffer) {
                            state.buffer.clear();
                            if data.trim() == "[DONE]" {
                                return None;
                            }
                            match serde_json::from_str::<Value>(&data) {
                                Ok(v) => return Some((Ok(v), state)),
                                Err(e) => {
                                    return Some((
                                        Err(LlmError::Protocol(format!("SSE JSON parse: {e}"))),
                                        state,
                                    ))
                                }
                            }
                        }
                    }
                    return None;
                }
            }
        }
    }))
}

struct SseState {
    buffer: Vec<u8>,
    inner: BoxStream<'static, Result<bytes::Bytes, reqwest::Error>>,
}

/// 从缓冲头部提取一个完整 SSE 事件，返回 (事件字节, 剩余缓冲)
///
/// 兼容规范四种事件分隔符：`\n\n` / `\r\r` / `\r\n\r\n`，取最早出现者切分。
/// 对 `\r\n\r\n` 流，事件字节天然不含尾部 `\r`（`i` 落在首个 `\r` 处）。
fn take_sse_event(buf: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let len = buf.len();
    for i in 0..len.saturating_sub(1) {
        let (a, b) = (buf[i], buf[i + 1]);
        // `\n\n` 或 `\r\r`
        if (a == b'\n' && b == b'\n') || (a == b'\r' && b == b'\r') {
            return Some((buf[..i].to_vec(), buf[i + 2..].to_vec()));
        }
        // `\r\n\r\n`
        if a == b'\r' && b == b'\n' && i + 3 < len && buf[i + 2] == b'\r' && buf[i + 3] == b'\n' {
            return Some((buf[..i].to_vec(), buf[i + 4..].to_vec()));
        }
    }
    None
}

/// 从 SSE 事件字节中提取 `data:` 字段（多行 data 用 `\n` 拼接）
fn parse_data_field(event: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(event).ok()?;
    let mut parts: Vec<String> = Vec::new();
    for line in s.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(rest) = line.strip_prefix("data:") {
            parts.push(rest.trim_start().to_string());
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn map_reqwest_err(e: reqwest::Error) -> LlmError {
    if e.is_timeout() {
        LlmError::Timeout
    } else {
        LlmError::Network(e.to_string())
    }
}

// ───────────────────────────────────────────────
// 非流式响应解码 — 端点若无视 `stream=false` 强制 SSE，在此兜底
// ───────────────────────────────────────────────

/// 解码后的响应主体：真正的 JSON 文档，或一组 SSE 事件
#[derive(Debug)]
pub(crate) enum WireBody {
    Json(Value),
    Sse(Vec<Value>),
}

/// 将一次性读取的完整响应体解码为 JSON 或 SSE 事件列表
///
/// 首次尝试 JSON（合规端点）；当响应 `Content-Type` 为 `text/event-stream` 或
/// body 以 `data:` 开头（端点无视 `stream=false` 强制流式，推理网关常见）时，
/// 直接走 SSE 事件切分。两者都不成立时返回带诊断信息的 [`LlmError::Protocol`]。
pub(crate) fn parse_wire_body(bytes: &[u8], content_type: Option<&str>) -> Result<WireBody, LlmError> {
    let ct_is_sse = content_type
        .map(|ct| ct.to_ascii_lowercase().contains("text/event-stream"))
        .unwrap_or(false);
    let looks_sse = {
        let start = bytes
            .iter()
            .position(|b| !b.is_ascii_whitespace())
            .unwrap_or(bytes.len());
        bytes[start..].starts_with(b"data:")
    };
    if ct_is_sse || looks_sse {
        let events = parse_sse_events(bytes)?;
        // 误判或空 SSE：回退尝试 JSON（如 Content-Type 标注错误但 body 实为 JSON）
        if events.is_empty() {
            if let Ok(v) = serde_json::from_slice::<Value>(bytes) {
                return Ok(WireBody::Json(v));
            }
        }
        return Ok(WireBody::Sse(events));
    }
    match serde_json::from_slice::<Value>(bytes) {
        Ok(v) => Ok(WireBody::Json(v)),
        Err(json_err) => {
            // 非 JSON 也非明确 SSE：可能是未带正确 Content-Type 的强制流式端点
            let events = parse_sse_events(bytes)?;
            if events.is_empty() {
                Err(protocol_decode_error(&json_err, content_type, bytes))
            } else {
                Ok(WireBody::Sse(events))
            }
        }
    }
}

/// SSE 事件切分终止标记（含恶意/异常时的安全返回语义）
fn emit_data(events: &mut Vec<Value>, data: &str) -> Result<bool, LlmError> {
    if data.trim() == "[DONE]" {
        return Ok(true); // true = 终止
    }
    events.push(
        serde_json::from_str::<Value>(data)
            .map_err(|e| LlmError::Protocol(format!("SSE JSON parse: {e}")))?,
    );
    Ok(false)
}

/// 一次性缓冲 → 全部 SSE 事件的 JSON 列表（复用字节流切分逻辑，遇 `[DONE]` 终止）
fn parse_sse_events(bytes: &[u8]) -> Result<Vec<Value>, LlmError> {
    let mut events = Vec::new();
    let mut buffer = bytes.to_vec();
    loop {
        if let Some((event_bytes, rest)) = take_sse_event(&buffer) {
            buffer = rest;
            if let Some(data) = parse_data_field(&event_bytes) {
                if emit_data(&mut events, &data)? {
                    break;
                }
            }
            continue;
        }
        // 无完整事件：刷新剩余缓冲（厂商可能不发尾部 `\n\n`）
        if !buffer.is_empty() {
            if let Some(data) = parse_data_field(&buffer) {
                if emit_data(&mut events, &data)? {
                    break;
                }
            }
        }
        break;
    }
    Ok(events)
}

/// 两路皆败时的诊断错误：指明尝试过的解码路径 / Content-Type / body 头部
fn protocol_decode_error(
    json_err: &serde_json::Error,
    content_type: Option<&str>,
    bytes: &[u8],
) -> LlmError {
    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(180)]);
    LlmError::Protocol(format!(
        "response decode failed: JSON ({json_err}); not parseable as SSE (content-type={ct:?}, head={head:?})",
        ct = content_type.unwrap_or("none")
    ))
}

type ChunkMapper = dyn Fn(
    &Value,
) -> Result<(Option<StreamChunk>, Option<FinishReason>, Option<TokenUsage>), LlmError>;

/// 将厂商事件序列收敛为完整的一次性 [`ChatResponse`]（SSE 非流式回退用）
///
/// 厂商差异经闭包注入：`parse_chunk` 把单个事件映射为 (Delta / finish / usage)，
/// `capture_meta` 从首个携带 `id`/`model` 的事件收集中继标识。本函数自包含累积
/// 逻辑，**不**反向依赖 `engine::StreamAccumulator`（恪守 provider → engine 单向分层）。
pub(crate) fn sse_fold_into_response(
    events: Vec<Value>,
    parse_chunk: &ChunkMapper,
    capture_meta: &dyn Fn(&Value) -> Option<(String, String)>,
    id_fallback: &str,
    model_fallback: &str,
) -> Result<ChatResponse, LlmError> {
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut role = None;
    let mut tool_calls: BTreeMap<u32, ToolCallAccum> = BTreeMap::new();
    let mut finish = None;
    let mut usage = None;
    let mut id = String::new();
    let mut model = String::new();
    for ev in &events {
        // 取首个携带 id/model 的事件
        if id.is_empty() || model.is_empty() {
            if let Some((i, m)) = capture_meta(ev) {
                if id.is_empty() {
                    id = i;
                }
                if model.is_empty() {
                    model = m;
                }
            }
        }
        let (delta, f, u) = parse_chunk(ev)?;
        if let Some(f) = f {
            finish = Some(f);
        }
        if let Some(u) = u {
            usage = Some(u);
        }
        if let Some(StreamChunk::Delta {
            content: c,
            reasoning_content: r,
            tool_calls: tc,
            role: rl,
        }) = delta
        {
            if let Some(c) = c {
                content.push_str(&c);
            }
            if let Some(r) = r {
                reasoning.push_str(&r);
            }
            if let Some(r) = rl {
                role = Some(r);
            }
            for dc in tc {
                let acc = tool_calls.entry(dc.index).or_default();
                if let Some(idd) = dc.id {
                    acc.id = idd;
                }
                if let Some(f) = dc.function {
                    if let Some(n) = f.name {
                        acc.name = n;
                    }
                    if let Some(a) = f.arguments {
                        acc.arguments.push_str(&a);
                    }
                }
            }
        }
    }
    if id.is_empty() {
        id = id_fallback.to_string();
    }
    if model.is_empty() {
        model = model_fallback.to_string();
    }
    let tool_calls = tool_calls
        .into_values()
        .map(|a| ToolCall {
            id: a.id,
            function: ToolCallFunction {
                name: a.name,
                arguments: a.arguments,
            },
        })
        .collect();
    let message = Message {
        role: role.unwrap_or(Role::Assistant),
        content: MessageContent::Text(content),
        reasoning_content: if reasoning.is_empty() {
            None
        } else {
            Some(reasoning)
        },
        tool_calls,
        tool_call_id: None,
        usage: usage.clone(),
    };
    Ok(ChatResponse {
        id,
        model,
        message,
        finish_reason: finish.unwrap_or(FinishReason::Stop),
        usage,
    })
}

/// 工具调用增量累积（index → id / name / 参数片段）
#[derive(Default)]
struct ToolCallAccum {
    id: String,
    name: String,
    arguments: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use futures::StreamExt;

    /// 构造 SSE 字节流（每个元素 = 一次网络 read 的字节块）
    fn sse_stream(payloads: &[&str]) -> BoxStream<'static, Result<Bytes, reqwest::Error>> {
        let bytes = payloads
            .iter()
            .map(|p| format!("data: {p}\n\n"))
            .collect::<Vec<_>>()
            .concat();
        // 拆成若干小块，模拟分片到达
        let chunks: Vec<Result<Bytes, reqwest::Error>> = bytes
            .as_bytes()
            .chunks(7)
            .map(|c| Ok(Bytes::copy_from_slice(c)))
            .collect();
        Box::pin(stream::iter(chunks))
    }

    #[test]
    fn parses_events_across_fragment_boundaries() {
        let json_stream = parse_sse_stream(sse_stream(&[
            r#"{"type":"a"}"#,
            r#"{"type":"b"}"#,
        ]));
        let mut out = Vec::new();
        let mut stream = Box::pin(json_stream);
        while let Some(item) = futures::executor::block_on(stream.next()) {
            out.push(item.expect("ok"));
        }
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["type"], "a");
        assert_eq!(out[1]["type"], "b");
    }

    #[test]
    fn parses_crlf_delimited_events() {
        // `\r\n\r\n` 分隔（部分代理/网关）：流式路径必须能逐个切出事件，
        // 而非堆积到流结束再合并解析（AI-2 修复锁定）。
        let bytes =
            format!("data: {}\r\n\r\ndata: {}\r\n\r\n", r#"{"type":"a"}"#, r#"{"type":"b"}"#);
        let stream = futures::stream::iter(vec![Ok::<_, reqwest::Error>(bytes::Bytes::from(bytes))]);
        let mut out = Vec::new();
        let mut stream = Box::pin(parse_sse_stream(stream));
        while let Some(item) = futures::executor::block_on(stream.next()) {
            out.push(item.expect("ok"));
        }
        assert_eq!(out.len(), 2, "both CRLF events must be cut");
        assert_eq!(out[0]["type"], "a");
        assert_eq!(out[1]["type"], "b");
    }

    #[test]
    fn parses_cr_cr_delimited_events() {
        // `\r\r` 分隔同样属于规范四分隔符之一
        let bytes =
            format!("data: {}\r\rdata: {}\r\r", r#"{"type":"a"}"#, r#"{"type":"b"}"#);
        let stream = futures::stream::iter(vec![Ok::<_, reqwest::Error>(bytes::Bytes::from(bytes))]);
        let mut out = Vec::new();
        let mut stream = Box::pin(parse_sse_stream(stream));
        while let Some(item) = futures::executor::block_on(stream.next()) {
            out.push(item.expect("ok"));
        }
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["type"], "a");
        assert_eq!(out[1]["type"], "b");
    }

    #[test]
    fn done_terminates_stream() {
        let mut stream = Box::pin(parse_sse_stream(sse_stream(&[
            r#"{"type":"a"}"#,
            "[DONE]",
            r#"{"type":"b"}"#, // 不应被消费
        ])));
        let mut count = 0;
        while let Some(item) = futures::executor::block_on(stream.next()) {
            count += 1;
            item.expect("ok");
        }
        assert_eq!(count, 1);
    }

    #[test]
    fn parse_wire_body_rejects_real_json() {
        let body = br#"{"id":"x","model":"m","choices":[]}"#;
        match parse_wire_body(body, None).expect("ok") {
            WireBody::Json(v) => assert_eq!(v["id"], "x"),
            WireBody::Sse(_) => panic!("expected JSON"),
        }
    }

    #[test]
    fn parse_wire_body_detects_sse_by_prefix() {
        let body = b"data: {\"type\":\"a\"}\n\ndata: [DONE]\n\n";
        match parse_wire_body(body, None).expect("ok") {
            WireBody::Sse(events) => {
                assert_eq!(events.len(), 1);
                assert_eq!(events[0]["type"], "a");
            }
            WireBody::Json(_) => panic!("expected SSE"),
        }
    }

    #[test]
    fn parse_wire_body_detects_sse_by_content_type() {
        let body = b"data: {\"type\":\"a\"}\n\n";
        match parse_wire_body(body, Some("text/event-stream")).expect("ok") {
            WireBody::Sse(events) => assert_eq!(events.len(), 1),
            WireBody::Json(_) => panic!("expected SSE"),
        }
    }

    #[test]
    fn parse_wire_body_is_json_even_when_content_type_mislabels_sse() {
        // Content-Type 标注 SSE，但 body 实为 JSON → 空 SSE 事件回退为 JSON
        let body = br#"{"id":"x","choices":[]}"#;
        match parse_wire_body(body, Some("text/event-stream")).expect("ok") {
            WireBody::Json(v) => assert_eq!(v["id"], "x"),
            WireBody::Sse(_) => panic!("expected JSON fallback"),
        }
    }

    #[test]
    fn parse_wire_body_errors_with_diagnostic_for_garbage() {
        let err = parse_wire_body(b"not json nor sse", None).expect_err("err");
        match err {
            LlmError::Protocol(msg) => {
                assert!(msg.contains("content-type"));
                assert!(msg.contains("head"));
            }
            _ => panic!("expected Protocol"),
        }
    }

    /// 供应商无关的 fold：用 stub 映射验证内容 / finish / usage / tool_calls 收敛
    #[test]
    fn sse_fold_accumulates_content_finish_usage_and_tool_calls() {
        use serde_json::json;

        let events = vec![
            json!({"id":"r1","model":"m"}),
            json!({"choices":[{"delta":{"role":"assistant"}}]}),
            json!({"choices":[{"delta":{"content":"你好"}}]}),
            json!({"choices":[{"delta":{"content":"世界"}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"f","arguments":"{\"x\":"}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}}]}),
            json!({"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}),
        ];
        let mapper = |v: &Value| Ok(parse_stub_chunk(v));
        let capture = |v: &Value| -> Option<(String, String)> {
            Some((
                v.get("id")?.as_str()?.to_string(),
                v.get("model")?.as_str()?.to_string(),
            ))
        };

        let resp = sse_fold_into_response(events, &mapper, &capture, "fb", "fb").expect("fold");
        assert_eq!(resp.id, "r1");
        assert_eq!(resp.model, "m");
        assert_eq!(resp.message.content.as_text().unwrap(), "你好世界");
        assert_eq!(resp.message.role, Role::Assistant);
        assert_eq!(resp.finish_reason, FinishReason::Stop);
        let u = resp.usage.expect("usage");
        assert_eq!(u.prompt_tokens, 4);
        assert_eq!(u.completion_tokens, 2);
        assert_eq!(resp.message.tool_calls.len(), 1);
        assert_eq!(resp.message.tool_calls[0].function.name, "f");
        assert_eq!(resp.message.tool_calls[0].function.arguments, "{\"x\":1}");
        assert_eq!(resp.message.tool_calls[0].id, "c1");
    }

    /// stub：角色字符串 → [`Role`]
    fn stub_role(s: &str) -> Option<Role> {
        match s {
            "system" => Some(Role::System),
            "user" => Some(Role::User),
            "assistant" => Some(Role::Assistant),
            "tool" => Some(Role::Tool),
            _ => None,
        }
    }

    /// stub：OpenAI 兼容 chunk 形态 → (delta / finish / usage)
    fn parse_stub_chunk(
        json: &Value,
    ) -> (Option<StreamChunk>, Option<FinishReason>, Option<TokenUsage>) {
        use crate::provider::{StreamChunk, TokenUsage, ToolCallDelta, ToolCallFunctionDelta};
        let mut delta = None;
        let mut finish = None;
        if let Some(choices) = json.get("choices").and_then(|c| c.as_array()) {
            if let Some(first) = choices.first() {
                if let Some(fr) = first.get("finish_reason").and_then(|f| f.as_str()) {
                    finish = Some(FinishReason::from_vendor_str(fr));
                }
                if let Some(d) = first.get("delta") {
                    let content = d
                        .get("content")
                        .and_then(|c| c.as_str())
                        .filter(|s| !s.is_empty())
                        .map(String::from);
                    let role = d.get("role").and_then(|r| r.as_str()).and_then(stub_role);
                    let tool_calls: Vec<ToolCallDelta> = d
                        .get("tool_calls")
                        .and_then(|t| t.as_array())
                        .map(|arr| {
                            arr.iter()
                                .map(|tc| ToolCallDelta {
                                    index: tc.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32,
                                    id: tc.get("id").and_then(|i| i.as_str()).map(String::from),
                                    function: tc.get("function").map(|f| ToolCallFunctionDelta {
                                        name: f.get("name").and_then(|n| n.as_str()).map(String::from),
                                        arguments: f.get("arguments").and_then(|a| a.as_str()).map(String::from),
                                    }),
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    if content.is_some() || role.is_some() || !tool_calls.is_empty() {
                        delta = Some(StreamChunk::Delta {
                            content,
                            reasoning_content: None,
                            tool_calls,
                            role,
                        });
                    }
                }
            }
        }
        let usage = json
            .get("usage")
            .filter(|u| !u.is_null())
            .map(|u| TokenUsage {
                prompt_tokens: u.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                completion_tokens: u.get("completion_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                total_tokens: u.get("total_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                reasoning_tokens: None,
                prompt_cache_hit_tokens: None,
                prompt_cache_miss_tokens: None,
                cache_read_tokens: None,
                cache_write_tokens: None,
            });
        (delta, finish, usage)
    }
}
//! OpenAI 兼容协议共享客户端 — MiMo / DeepSeek 等厂商的通用底座
//!
//! 本模块为 `pub(crate)`：仅 provider 内部各厂商适配器使用，不对外暴露。
//! 厂商差异（base_url / model / 厂商特殊参数）由各适配器在调用前组装进
//! `serde_json::Value` 形式的请求 body；本模块只负责：
//!
//! 1. HTTP 发送（`reqwest`，rustls-tls，跨平台零系统依赖）
//! 2. 错误归一（HTTP 状态码 / 网络错误 / 超时 → [`LlmError`]）
//! 3. 重试（仅 `Network / Server / RateLimited`，指数退避，受 [`RetryPolicy`] 上限）
//! 4. 响应解析（非流式 JSON → [`ChatResponse`]；流式 SSE → [`StreamChunk`] 流）
//!
//! ## 流式语义
//! SSE 增量解析为 [`StreamChunk::Delta`]，流终止时（`[DONE]` 或连接关闭）
//! 发出单一 [`StreamChunk::Finish`]。`finish_reason` 与 `usage` 可能分布在不同
//! chunk 中（MiMo 模式：倒数第二 chunk 携带 `finish_reason`，最后 chunk 携带
//! `usage`），状态机在两者齐备或流结束时统一收敛。

use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use futures::Stream;
use serde_json::{json, Value};
use tracing::debug;

use crate::provider::{
    ChatResponse, FinishReason, LlmError, Message, MessageContent, RetryPolicy, Role, StreamChunk,
    TokenUsage, ToolCall, ToolCallDelta, ToolCallFunction, ToolCallFunctionDelta,
};

// ───────────────────────────────────────────────
// 客户端配置
// ───────────────────────────────────────────────

pub(crate) struct OpenAiCompatConfig {
    pub base_url: String,
    pub api_key: String,
    pub timeout: Duration,
    pub retry: RetryPolicy,
}

/// OpenAI 兼容协议客户端（共享底座）
///
/// 各厂商适配器持有本客户端，组装 vendor-specific body 后调用 `chat` / `chat_stream`。
/// 内部 `reqwest::Client` 复用连接池，线程安全（`Send + Sync + Clone`）。
pub(crate) struct OpenAiCompatClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    retry: RetryPolicy,
}

impl OpenAiCompatClient {
    pub fn new(cfg: OpenAiCompatConfig) -> Result<Self, LlmError> {
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            .map_err(|e| LlmError::Network(format!("http client build: {e}")))?;
        Ok(Self {
            http,
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            api_key: cfg.api_key,
            retry: cfg.retry,
        })
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    /// 非流式调用 — 重试仅对可恢复错误生效
    pub(crate) async fn chat(&self, body: Value) -> Result<ChatResponse, LlmError> {
        let mut body = body;
        body["stream"] = json!(false);

        let mut last_err: Option<LlmError> = None;
        for attempt in 0..=self.retry.max_retries {
            match self.chat_once(&body).await {
                Ok(resp) => return Ok(resp),
                Err(e) if RetryPolicy::is_retryable(&e) && attempt < self.retry.max_retries => {
                    debug!(attempt, error = %e, "retryable error, backing off");
                    let backoff = compute_backoff(&self.retry, &e, attempt);
                    tokio::time::sleep(backoff).await;
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        // max_retries == 0 时直接走 Err(e) 分支；此处兜底
        Err(last_err.unwrap_or_else(|| LlmError::Network("retry loop exhausted".into())))
    }

    async fn chat_once(&self, body: &Value) -> Result<ChatResponse, LlmError> {
        let resp = self
            .http
            .post(self.endpoint())
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .map_err(map_reqwest_err)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_http_error(resp).await);
        }
        let json: Value = resp
            .json()
            .await
            .map_err(|e| LlmError::Protocol(format!("response decode: {e}")))?;
        parse_chat_response(&json)
    }

    /// 流式调用 — 仅对初始连接失败重试；已开始流出后不重试
    pub(crate) async fn chat_stream(
        &self,
        body: Value,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        let mut body = body;
        body["stream"] = json!(true);

        let mut last_err: Option<LlmError> = None;
        for attempt in 0..=self.retry.max_retries {
            match self.chat_stream_once(&body).await {
                Ok(stream) => return Ok(stream),
                Err(e) if RetryPolicy::is_retryable(&e) && attempt < self.retry.max_retries => {
                    debug!(attempt, error = %e, "stream init retryable, backing off");
                    let backoff = compute_backoff(&self.retry, &e, attempt);
                    tokio::time::sleep(backoff).await;
                    last_err = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap_or_else(|| LlmError::Network("retry loop exhausted".into())))
    }

    async fn chat_stream_once(
        &self,
        body: &Value,
    ) -> Result<BoxStream<'static, Result<StreamChunk, LlmError>>, LlmError> {
        let resp = self
            .http
            .post(self.endpoint())
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .map_err(map_reqwest_err)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_http_error(resp).await);
        }
        let byte_stream = resp.bytes_stream();
        let json_stream = parse_sse_stream(byte_stream);
        let chunk_stream = parse_chunk_stream(json_stream);
        Ok(Box::pin(chunk_stream))
    }
}

/// 退避时长：RateLimited 时优先尊重 Retry-After，否则指数退避
fn compute_backoff(policy: &RetryPolicy, err: &LlmError, attempt: u32) -> Duration {
    if let LlmError::RateLimited {
        retry_after: Some(ra),
    } = err
    {
        if !ra.is_zero() {
            return *ra;
        }
    }
    policy.backoff_for(attempt)
}

// ───────────────────────────────────────────────
// 请求 body 公共构造（vendor 复用）
// ───────────────────────────────────────────────

/// 构造 OpenAI 兼容协议的公共 body 字段
///
/// 厂商适配器调用本函数后，再追加厂商特殊字段（如 `thinking` / `reasoning_effort`）
/// 与 `req.extra` 透传字段。`stream` 字段由客户端在发送前填入。
pub(crate) fn build_common_body(
    messages: &[Message],
    tools: &[crate::provider::ToolDeclaration],
    tool_choice: crate::provider::ToolChoice,
    temperature: Option<f32>,
    max_tokens: Option<usize>,
    model: &str,
) -> Value {
    let mut body = json!({
        "model": model,
        "messages": messages,
    });
    if !tools.is_empty() {
        let tools_json: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect();
        body["tools"] = json!(tools_json);
        body["tool_choice"] = json!(match tool_choice {
            crate::provider::ToolChoice::Auto => "auto",
            crate::provider::ToolChoice::None => "none",
            crate::provider::ToolChoice::Required => "required",
        });
    }
    if let Some(t) = temperature {
        body["temperature"] = json!(t);
    }
    if let Some(m) = max_tokens {
        body["max_completion_tokens"] = json!(m);
    }
    body
}

// ───────────────────────────────────────────────
// 错误归一
// ───────────────────────────────────────────────

fn map_reqwest_err(e: reqwest::Error) -> LlmError {
    if e.is_timeout() {
        LlmError::Timeout
    } else {
        LlmError::Network(e.to_string())
    }
}

async fn map_http_error(resp: reqwest::Response) -> LlmError {
    let status_code = resp.status().as_u16();
    // Retry-After 必须在消费 body 前提取
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs);
    let body = resp.text().await.unwrap_or_default();
    match status_code {
        401 | 403 => LlmError::Auth,
        408 => LlmError::Timeout,
        429 => LlmError::RateLimited { retry_after },
        // 402 余额不足：两家（MiMo / DeepSeek）语义一致，均为确定性用户可行动错误
        402 => LlmError::InsufficientBalance(format!("HTTP {status_code}: {body}")),
        // 404 资源/能力不存在（MiMo：模型不支持图像输入）— 确定性错误，不重试
        404 => LlmError::BadRequest(format!("HTTP {status_code}: {body}")),
        // 421 内容审核拦截（MiMo 特有语义）— 确定性错误，不重试
        421 => LlmError::ContentBlocked(format!("HTTP {status_code}: {body}")),
        400 | 422 => LlmError::BadRequest(format!("HTTP {status_code}: {body}")),
        500..=599 => LlmError::Server {
            status: status_code,
            body,
        },
        _ => LlmError::Server {
            status: status_code,
            body,
        },
    }
}

// ───────────────────────────────────────────────
// 非流式响应解析
// ───────────────────────────────────────────────

fn parse_chat_response(json: &Value) -> Result<ChatResponse, LlmError> {
    let id = json
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LlmError::Protocol("missing 'id' in response".into()))?
        .to_string();
    let model = json
        .get("model")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LlmError::Protocol("missing 'model' in response".into()))?
        .to_string();
    let choices = json
        .get("choices")
        .and_then(|c| c.as_array())
        .ok_or_else(|| LlmError::Protocol("missing 'choices' array".into()))?;
    let first = choices
        .first()
        .ok_or_else(|| LlmError::Protocol("empty 'choices' array".into()))?;
    let finish_reason = first
        .get("finish_reason")
        .and_then(|f| f.as_str())
        .map(FinishReason::from_vendor_str)
        .unwrap_or(FinishReason::Stop);
    let message_json = first
        .get("message")
        .ok_or_else(|| LlmError::Protocol("missing 'message' in choice".into()))?;
    let message = parse_message(message_json)?;
    let usage = json.get("usage").filter(|u| !u.is_null()).map(parse_usage);
    Ok(ChatResponse {
        id,
        model,
        message,
        finish_reason,
        usage,
    })
}

fn parse_message(m: &Value) -> Result<Message, LlmError> {
    let role_str = m
        .get("role")
        .and_then(|r| r.as_str())
        .ok_or_else(|| LlmError::Protocol("missing 'role' in message".into()))?;
    let role = parse_role(role_str)
        .ok_or_else(|| LlmError::Protocol(format!("unknown role: {role_str}")))?;
    // content 可能为 null（当存在 tool_calls 时）或字符串
    let content = match m.get("content") {
        Some(Value::String(s)) => MessageContent::Text(s.clone()),
        Some(Value::Null) | None => MessageContent::Text(String::new()),
        Some(Value::Array(parts)) => {
            // 兼容部分厂商偶发的数组形式文本：拼接所有 text part
            let text = parts
                .iter()
                .filter_map(|p| {
                    if p.get("type").and_then(|t| t.as_str()) == Some("text") {
                        p.get("text").and_then(|t| t.as_str()).map(String::from)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("");
            MessageContent::Text(text)
        }
        Some(other) => {
            return Err(LlmError::Protocol(format!(
                "unsupported content form: {other}"
            )))
        }
    };
    let reasoning_content = m
        .get("reasoning_content")
        .and_then(|r| r.as_str())
        .map(String::from);
    let tool_calls = m
        .get("tool_calls")
        .and_then(|t| t.as_array())
        .map(|arr| arr.iter().filter_map(parse_tool_call).collect())
        .unwrap_or_default();
    let tool_call_id = m
        .get("tool_call_id")
        .and_then(|t| t.as_str())
        .map(String::from);
    Ok(Message {
        role,
        content,
        reasoning_content,
        tool_calls,
        tool_call_id,
    })
}

fn parse_role(s: &str) -> Option<Role> {
    match s {
        "system" => Some(Role::System),
        "user" => Some(Role::User),
        "assistant" => Some(Role::Assistant),
        "tool" => Some(Role::Tool),
        _ => None,
    }
}

fn parse_tool_call(t: &Value) -> Option<ToolCall> {
    let id = t.get("id")?.as_str()?.to_string();
    let func = t.get("function")?;
    let name = func.get("name")?.as_str()?.to_string();
    let arguments = func.get("arguments")?.as_str()?.to_string();
    Some(ToolCall {
        id,
        function: ToolCallFunction { name, arguments },
    })
}

fn parse_usage(u: &Value) -> TokenUsage {
    let get = |k: &str| u.get(k).and_then(|v| v.as_u64()).map(|n| n as usize);
    let prompt_tokens = get("prompt_tokens").unwrap_or(0);
    let completion_tokens = get("completion_tokens").unwrap_or(0);
    let total_tokens = get("total_tokens").unwrap_or(prompt_tokens + completion_tokens);
    let reasoning_tokens = u
        .get("completion_tokens_details")
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(|v| v.as_u64())
        .map(|n| n as usize);
    let prompt_cache_hit = get("prompt_cache_hit_tokens");
    let prompt_cache_miss = get("prompt_cache_miss_tokens");
    TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
        reasoning_tokens,
        prompt_cache_hit_tokens: prompt_cache_hit,
        prompt_cache_miss_tokens: prompt_cache_miss,
    }
}

// ───────────────────────────────────────────────
// SSE 流解析
// ───────────────────────────────────────────────

/// 从字节流解析 SSE 事件，输出每个 `data:` 行的 JSON Value
///
/// 实现：`unfold` 状态机，缓冲字节直到出现 `\n\n` 事件分隔符；
/// 提取 `data:` 字段；`[DONE]` 终止流。`BoxStream` 自身 `Unpin`，
/// 可在 async 闭包中直接 `.next().await`。
fn parse_sse_stream(
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
                    // 流结束：刷新残余缓冲（部分厂商不发 [DONE]）
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

/// 从缓冲头部提取一个完整 SSE 事件（以 `\n\n` 分隔），返回 (事件字节, 剩余缓冲)
fn take_sse_event(buf: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    // 查找 `\n\n` 分隔符
    for i in 0..buf.len().saturating_sub(1) {
        if buf[i] == b'\n' && buf[i + 1] == b'\n' {
            let mut event_end = i;
            // 兼容 `\r\n\r\n`：剔除尾部 `\r`
            if event_end > 0 && buf[event_end - 1] == b'\r' {
                event_end -= 1;
            }
            let event = buf[..event_end].to_vec();
            let rest = buf[i + 2..].to_vec();
            return Some((event, rest));
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

// ───────────────────────────────────────────────
// JSON chunk → StreamChunk 状态机
// ───────────────────────────────────────────────

/// 将 JSON chunk 流转换为 [`StreamChunk`] 流
///
/// 状态机：累积 `finish_reason` 与 `usage`（可能分布在不同 chunk），
/// 两者齐备或流结束时发出 [`StreamChunk::Finish`]。
fn parse_chunk_stream(
    json_stream: BoxStream<'static, Result<Value, LlmError>>,
) -> BoxStream<'static, Result<StreamChunk, LlmError>> {
    let state = ChunkState {
        inner: json_stream,
        pending_finish: None,
        pending_usage: None,
        finish_emitted: false,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        loop {
            // 1. finish_reason + usage 齐备 → 发出 Finish
            if !state.finish_emitted
                && state.pending_finish.is_some()
                && state.pending_usage.is_some()
            {
                state.finish_emitted = true;
                let fr = state.pending_finish.take().unwrap();
                let usage = state.pending_usage.take();
                return Some((
                    Ok(StreamChunk::Finish {
                        finish_reason: fr,
                        usage,
                    }),
                    state,
                ));
            }
            // 2. finish 已发出 → 结束
            if state.finish_emitted {
                return None;
            }
            // 3. 拉取下一个 JSON chunk
            match state.inner.next().await {
                Some(Ok(json)) => match parse_chunk_json(&json) {
                    Ok((delta_opt, finish_opt, usage_opt)) => {
                        if let Some(fr) = finish_opt {
                            state.pending_finish = Some(fr);
                        }
                        if let Some(u) = usage_opt {
                            state.pending_usage = Some(u);
                        }
                        if let Some(delta) = delta_opt {
                            return Some((Ok(delta), state));
                        }
                        // 无 delta：循环回去（可能因 pending 齐备而发出 Finish）
                        continue;
                    }
                    Err(e) => return Some((Err(e), state)),
                },
                Some(Err(e)) => return Some((Err(e), state)),
                None => {
                    // 流结束：若有 pending_finish 则补发 Finish
                    if !state.finish_emitted {
                        if let Some(fr) = state.pending_finish.take() {
                            state.finish_emitted = true;
                            return Some((
                                Ok(StreamChunk::Finish {
                                    finish_reason: fr,
                                    usage: state.pending_usage.take(),
                                }),
                                state,
                            ));
                        }
                    }
                    return None;
                }
            }
        }
    }))
}

struct ChunkState {
    inner: BoxStream<'static, Result<Value, LlmError>>,
    pending_finish: Option<FinishReason>,
    pending_usage: Option<TokenUsage>,
    finish_emitted: bool,
}

/// 单个 JSON chunk 的解析产物：可选 Delta / 可选 finish_reason / 可选 usage
type ChunkParts = (
    Option<StreamChunk>,
    Option<FinishReason>,
    Option<TokenUsage>,
);

/// 解析单个 JSON chunk：返回 (可选 Delta, 可选 finish_reason, 可选 usage)
fn parse_chunk_json(json: &Value) -> Result<ChunkParts, LlmError> {
    let mut delta_chunk = None;
    let mut finish_reason = None;

    if let Some(choices) = json.get("choices").and_then(|c| c.as_array()) {
        if let Some(first) = choices.first() {
            if let Some(fr) = first.get("finish_reason").and_then(|f| f.as_str()) {
                finish_reason = Some(FinishReason::from_vendor_str(fr));
            }
            if let Some(delta) = first.get("delta") {
                let content = delta
                    .get("content")
                    .and_then(|c| c.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from);
                let reasoning = delta
                    .get("reasoning_content")
                    .and_then(|c| c.as_str())
                    .filter(|s| !s.is_empty())
                    .map(String::from);
                let role = delta
                    .get("role")
                    .and_then(|r| r.as_str())
                    .and_then(parse_role);
                let tool_calls: Vec<ToolCallDelta> = delta
                    .get("tool_calls")
                    .and_then(|t| t.as_array())
                    .map(|arr| arr.iter().filter_map(parse_tool_call_delta).collect())
                    .unwrap_or_default();
                if content.is_some()
                    || reasoning.is_some()
                    || !tool_calls.is_empty()
                    || role.is_some()
                {
                    delta_chunk = Some(StreamChunk::Delta {
                        content,
                        reasoning_content: reasoning,
                        tool_calls,
                        role,
                    });
                }
            }
        }
    }

    let usage = json.get("usage").filter(|u| !u.is_null()).map(parse_usage);

    Ok((delta_chunk, finish_reason, usage))
}

fn parse_tool_call_delta(t: &Value) -> Option<ToolCallDelta> {
    let index = t.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
    let id = t.get("id").and_then(|i| i.as_str()).map(String::from);
    let function = t.get("function").map(|f| {
        let name = f.get("name").and_then(|n| n.as_str()).map(String::from);
        let arguments = f
            .get("arguments")
            .and_then(|a| a.as_str())
            .map(String::from);
        ToolCallFunctionDelta { name, arguments }
    });
    Some(ToolCallDelta {
        index,
        id,
        function,
    })
}

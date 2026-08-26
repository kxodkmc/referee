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
use serde_json::{json, Value};
use tracing::debug;

use crate::provider::sse::{parse_sse_stream, parse_wire_body, sse_fold_into_response, WireBody};
use crate::provider::{
    ChatResponse, ContentPart, FinishReason, ImageDetail, LlmError, MediaResolution, MediaSource,
    Message, MessageContent, RetryPolicy, Role, StreamChunk, TokenUsage, ToolCall, ToolCallDelta,
    ToolCallFunction, ToolCallFunctionDelta,
};

// ───────────────────────────────────────────────
// 客户端配置
// ───────────────────────────────────────────────

pub(crate) struct OpenAiCompatConfig {
    pub base_url: String,
    pub api_key: String,
    pub timeout: Duration,
    pub retry: RetryPolicy,
    /// 附加请求头（如 OpenRouter 的 `HTTP-Referer` / `X-OpenRouter-Title`），默认空
    pub extra_headers: Vec<(String, String)>,
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
    extra_headers: Vec<(String, String)>,
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
            extra_headers: cfg.extra_headers,
        })
    }

    fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url)
    }

    /// 构造附加请求头（非法键/值静默忽略，不阻断请求）
    pub(crate) fn header_map(&self) -> reqwest::header::HeaderMap {
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
        let mut map = HeaderMap::with_capacity(self.extra_headers.len());
        for (k, v) in &self.extra_headers {
            match (HeaderName::try_from(k), HeaderValue::try_from(v)) {
                (Ok(name), Ok(value)) => {
                    map.insert(name, value);
                }
                _ => {
                    tracing::warn!(key = %k, "skip invalid extra header");
                }
            }
        }
        map
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
            .headers(self.header_map())
            .json(body)
            .send()
            .await
            .map_err(map_reqwest_err)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(map_http_error(resp).await);
        }
        // 单次读取完整响应体，随后按格式分流（JSON 直解 / SSE 事件收敛）
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| LlmError::Protocol(format!("response read: {e}")))?;
        match parse_wire_body(&bytes, content_type.as_deref())? {
            WireBody::Json(json) => parse_chat_response(&json),
            WireBody::Sse(events) => sse_fold_into_response(
                events,
                &parse_chunk_json,
                &openai_capture_meta,
                "",
                "",
            ),
        }
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
            .headers(self.header_map())
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
    let messages_json: Vec<Value> = messages.iter().map(message_to_body_json).collect();
    let mut body = json!({
        "model": model,
        "messages": messages_json,
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
// 消息 → OpenAI 请求 body JSON（多模态序列化）
// ───────────────────────────────────────────────

/// 将 [`Message`] 序列化为 OpenAI 请求体中的消息对象
///
/// `content` 按 [`MessageContent`] 分派：`Text` → 字符串简写；`Multimodal` →
/// 多模态数组。其余字段（reasoning_content / tool_calls / tool_call_id）按
/// OpenAI 协议透传。
fn message_to_body_json(m: &Message) -> Value {
    let mut obj = json!({ "role": m.role });
    match &m.content {
        MessageContent::Text(s) => {
            obj["content"] = json!(s);
        }
        MessageContent::Multimodal(parts) => {
            let parts_json: Vec<Value> = parts.iter().map(content_part_to_json).collect();
            obj["content"] = json!(parts_json);
        }
    }
    if let Some(rc) = &m.reasoning_content {
        obj["reasoning_content"] = json!(rc);
    }
    if !m.tool_calls.is_empty() {
        let calls: Vec<Value> = m.tool_calls.iter().map(tool_call_to_json).collect();
        obj["tool_calls"] = json!(calls);
    }
    if let Some(tool_call_id) = &m.tool_call_id {
        obj["tool_call_id"] = json!(tool_call_id);
    }
    obj
}

/// 将 [`ContentPart`] 序列化为 OpenAI 多模态数组元素
///
/// 厂商差异（图片/音频/视频的 `type` 与字段名）在此归一，上层不写厂商分支。
fn content_part_to_json(part: &ContentPart) -> Value {
    match part {
        ContentPart::Text { text } => json!({ "type": "text", "text": text }),
        ContentPart::Image { source, detail } => {
            let mut v = json!({
                "type": "image_url",
                "image_url": { "url": media_source_to_wire(source) }
            });
            if let Some(d) = detail {
                v["image_url"]["detail"] = json!(match d {
                    ImageDetail::Low => "low",
                    ImageDetail::High => "high",
                    ImageDetail::Original => "original",
                    ImageDetail::Auto => "auto",
                });
            }
            v
        }
        ContentPart::Audio { source } => json!({
            "type": "input_audio",
            "input_audio": { "data": media_source_to_wire(source) }
        }),
        ContentPart::Video { source, params } => {
            let mut v = json!({
                "type": "video_url",
                "video_url": { "url": media_source_to_wire(source) }
            });
            if let Some(fps) = params.fps {
                v["fps"] = json!(fps);
            }
            if let Some(res) = params.media_resolution {
                v["media_resolution"] = json!(match res {
                    MediaResolution::Default => "default",
                    MediaResolution::Max => "max",
                });
            }
            v
        }
    }
}

/// 将 [`MediaSource`] 映射为请求中的 `url`/`data` 值
///
/// - `Url` → 原样 URL
/// - `Base64` → `data:{mime};base64,{data}`（OpenAI 多模态标准）
/// - `FileId` → `ms://<id>`（Kimi 文件引用协议）
fn media_source_to_wire(src: &MediaSource) -> Value {
    match src {
        MediaSource::Url { url } => json!(url),
        MediaSource::Base64 { mime, data } => json!(format!("data:{mime};base64,{data}")),
        MediaSource::FileId { file_id } => json!(format!("ms://{file_id}")),
    }
}

/// 将 [`ToolCall`] 序列化为 OpenAI 工具调用对象
fn tool_call_to_json(tc: &ToolCall) -> Value {
    json!({
        "id": tc.id,
        "type": "function",
        "function": {
            "name": tc.function.name,
            "arguments": tc.function.arguments,
        }
    })
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
    let usage = json.get("usage").filter(|u| !u.is_null()).map(parse_usage);
    // 把本轮用量挂进消息元数据（供 observe / 审计）
    let mut message = parse_message(message_json)?;
    message.usage = usage.clone();
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
        usage: None,
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
        // 归一化视角：hit→read，miss→write（厂商无关）
        cache_read_tokens: prompt_cache_hit,
        cache_write_tokens: prompt_cache_miss,
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

/// 从首个携带 `id`/`model` 的 SSE 事件收集中继标识（OpenAI chunk 常内嵌）
fn openai_capture_meta(json: &Value) -> Option<(String, String)> {
    let id = json.get("id")?.as_str()?.to_string();
    let model = json.get("model")?.as_str()?.to_string();
    Some((id, model))
}

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

    fn parse_chunks(payloads: &[&str]) -> Result<Vec<StreamChunk>, LlmError> {
        let json_stream = parse_sse_stream(sse_stream(payloads));
        let chunk_stream = parse_chunk_stream(json_stream);
        let mut items = Vec::new();
        let mut stream = Box::pin(chunk_stream);
        while let Some(item) = futures::executor::block_on(stream.next()) {
            items.push(item?);
        }
        Ok(items)
    }

    #[test]
    fn sse_delta_and_finish_are_accumulated() {
        let chunks = parse_chunks(&[
            r#"{"id":"1","choices":[{"index":0,"delta":{"content":"你好"}}]}"#,
            r#"{"id":"1","choices":[{"index":0,"delta":{"content":"世界"}}]}"#,
            r#"{"id":"1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}"#,
            "[DONE]",
        ])
        .expect("parse ok");

        let mut joined = String::new();
        let mut finish: Option<StreamChunk> = None;
        for c in chunks {
            match c {
                StreamChunk::Delta { content, .. } => {
                    joined.push_str(content.as_deref().unwrap_or(""))
                }
                f @ StreamChunk::Finish { .. } => finish = Some(f),
            }
        }
        assert_eq!(joined, "你好世界");
        match finish.expect("must have Finish") {
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                assert_eq!(finish_reason, FinishReason::Stop);
                let u = usage.expect("usage present");
                assert_eq!(u.total_tokens, 6);
            }
            StreamChunk::Delta { .. } => panic!("expected Finish"),
        }
    }

    #[test]
    fn sse_done_terminates_without_extra() {
        let chunks = parse_chunks(&[
            r#"{"id":"1","choices":[{"index":0,"delta":{"content":"hi"}}]}"#,
            "[DONE]",
        ])
        .expect("parse ok");
        // 无 finish_reason/usage → 流结束不发 Finish （语义：增量流；收敛由上层完成）
        let deltas = chunks
            .iter()
            .filter(|c| matches!(c, StreamChunk::Delta { .. }))
            .count();
        assert_eq!(deltas, 1);
    }

    #[test]
    fn build_common_body_serializes_multimodal_content() {
        use crate::provider::{ContentPart, MediaSource, Message, VideoParams};
        let text_msg = Message::user("plain text");
        let mm_msg = crate::provider::Message::user(MessageContent::multimodal(vec![
            ContentPart::text("describe this image"),
            ContentPart::image(MediaSource::Url {
                url: "https://example.png".into(),
            }),
            ContentPart::image(MediaSource::Base64 {
                mime: "image/png".into(),
                data: "aGVsbG8=".into(),
            }),
            ContentPart::audio(MediaSource::Url {
                url: "https://example.wav".into(),
            }),
            ContentPart::video(
                MediaSource::Url {
                    url: "https://example.mp4".into(),
                },
                VideoParams {
                    fps: Some(2.0),
                    media_resolution: Some(MediaResolution::Max),
                },
            ),
        ]));
        let body = build_common_body(
            &[text_msg, mm_msg],
            &[],
            crate::provider::ToolChoice::Auto,
            None,
            None,
            "m",
        );
        let msgs = body["messages"].as_array().unwrap();
        // 纯文本消息 → 字符串简写
        assert_eq!(msgs[0]["content"], json!("plain text"));
        // 多模态消息 → 数组
        let parts = msgs[1]["content"].as_array().unwrap();
        assert_eq!(parts[0], json!({"type":"text","text":"describe this image"}));
        assert_eq!(
            parts[1],
            json!({"type":"image_url","image_url":{"url":"https://example.png"}})
        );
        assert_eq!(
            parts[2],
            json!({"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}})
        );
        assert_eq!(
            parts[3],
            json!({"type":"input_audio","input_audio":{"data":"https://example.wav"}})
        );
        assert_eq!(
            parts[4],
            json!({"type":"video_url","video_url":{"url":"https://example.mp4"},
                   "fps":2.0,"media_resolution":"max"})
        );
    }

    #[test]
    fn image_detail_serialized_into_image_url() {
        use crate::provider::ImageDetail;
        let mm_msg = crate::provider::Message::user(MessageContent::multimodal(vec![
            ContentPart::image_with_detail(
                MediaSource::Url {
                    url: "https://example.png".into(),
                },
                ImageDetail::Low,
            ),
            ContentPart::image(MediaSource::Url {
                url: "https://example2.png".into(),
            }),
        ]));
        let body = build_common_body(
            &[mm_msg],
            &[],
            crate::provider::ToolChoice::Auto,
            None,
            None,
            "m",
        );
        let parts = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(
            parts[0],
            json!({"type":"image_url","image_url":{"url":"https://example.png","detail":"low"}})
        );
        // 未指定 detail → 不输出该字段
        assert_eq!(
            parts[1],
            json!({"type":"image_url","image_url":{"url":"https://example2.png"}})
        );
    }

    #[test]
    fn non_stream_response_parses_tool_calls_and_reasoning() {
        let json = serde_json::json!({
            "id": "r1", "model": "m",
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "role": "assistant",
                    "content": null,
                    "reasoning_content": "thinking...",
                    "tool_calls": [{"id":"call_1","type":"function",
                        "function":{"name":"f","arguments":"{\"x\":1}"}}]
                }
            }],
            "usage": {"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}
        });
        let resp = parse_chat_response(&json).expect("parse");
        assert_eq!(resp.finish_reason, FinishReason::ToolCalls);
        assert_eq!(
            resp.message.reasoning_content.as_deref(),
            Some("thinking...")
        );
        assert_eq!(resp.message.tool_calls.len(), 1);
        assert_eq!(resp.usage.map(|u| u.total_tokens), Some(3));
    }

    /// 非流式 SSE 回退：端点无视 stream=false 强制流式时，经真实解析器收敛出 ChatResponse
    #[test]
    fn sse_fallback_folds_into_response() {
        use serde_json::json;
        let events = vec![
            json!({"id":"chat-1","model":"reasoner",
                   "choices":[{"index":0,"delta":{"role":"assistant"}}]}),
            json!({"id":"chat-1","model":"reasoner",
                   "choices":[{"index":0,"delta":{"reasoning_content":"推理文本"}}]}),
            json!({"id":"chat-1","model":"reasoner",
                   "choices":[{"index":0,"delta":{"content":"回答正文"}}]}),
            json!({"id":"chat-1","model":"reasoner",
                   "choices":[{"index":0,"delta":{"tool_calls":[
                       {"index":0,"id":"c","function":{"name":"f","arguments":"{\"x\":"}}]}}]}),
            json!({"id":"chat-1","model":"reasoner",
                   "choices":[{"index":0,"delta":{"tool_calls":[
                       {"index":0,"function":{"arguments":"1}"}}]}}]}),
            json!({"id":"chat-1","model":"reasoner",
                   "choices":[{"index":0,"delta":{},"finish_reason":"stop"}],
                   "usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}),
        ];
        let resp = sse_fold_into_response(events, &parse_chunk_json, &openai_capture_meta, "", "")
            .expect("fold");
        assert_eq!(resp.id, "chat-1");
        assert_eq!(resp.model, "reasoner");
        assert_eq!(resp.message.content.as_text().unwrap(), "回答正文");
        assert_eq!(resp.message.reasoning_content.as_deref(), Some("推理文本"));
        assert_eq!(resp.finish_reason, FinishReason::Stop);
        assert_eq!(resp.usage.map(|u| u.total_tokens), Some(3));
        assert_eq!(resp.message.tool_calls.len(), 1);
        assert_eq!(
            resp.message.tool_calls[0].function.arguments,
            "{\"x\":1}"
        );
    }
}

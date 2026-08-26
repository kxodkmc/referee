//! Anthropic Messages 协议共享客户端 — MiniMax-Anthropic 兼容等厂商的通用底座
//!
//! 本模块为 `pub(crate)`：仅 provider 内部各厂商适配器使用，不对外暴露。
//! 与 [`super::openai_compat`] 相对：那是 OpenAI `chat/completions` 协议底座，
//! 本模块承载 Anthropic `POST /v1/messages` 协议。厂商差异（base_url / model /
//! 厂商特殊参数）由各适配器组装进 `serde_json::Value` 请求 body；本模块负责：
//!
//! 1. HTTP 发送（`reqwest`，`Authorization: Bearer` 鉴权）
//! 2. 错误归一（HTTP 状态码 / 网络错误 / 超时 → [`crate::provider::LlmError`]）
//! 3. 重试（仅 `Network / Server / RateLimited`，指数退避，受 [`RetryPolicy`] 上限）
//! 4. 响应解析（非流式 JSON → [`ChatResponse`]；流式 SSE 事件 → [`StreamChunk`]）
//!
//! ## 请求映射（Anthropic Messages 标准）
//! - `system` 为顶层字段：消息中的 System 角色聚合至顶层 `system`
//! - `content` 一律为块数组：`text` / `image` / `video` / `tool_use` / `thinking`
//! - 工具结果（`Role::Tool`）映射为 `user` 角色的 `tool_result` 块
//!
//! ## 流式语义
//! Anthropic SSE 事件流（`message_start` / `content_block_start` /
//! `content_block_delta` / `content_block_stop` / `message_delta` / `message_stop`），
//! 不含 `[DONE]`。`thinking_delta`→`reasoning_content`、`text_delta`→`content`、
//! `input_json_delta`→工具参数增量；`message_delta` 携带 `stop_reason` + `usage`，
//! 状态机在两者齐备或流结束时统一收敛为 [`StreamChunk::Finish`]。

use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use serde_json::{json, Value};
use tracing::debug;

use crate::provider::sse::parse_sse_stream;
use crate::provider::{
    ChatResponse, ContentPart, FinishReason, LlmError, MediaResolution, MediaSource, Message,
    MessageContent, RetryPolicy, Role, StreamChunk, TokenUsage, ToolCall, ToolCallDelta,
    ToolCallFunction, ToolCallFunctionDelta, ToolChoice, ToolDeclaration,
};

// ───────────────────────────────────────────────
// 客户端配置
// ───────────────────────────────────────────────

pub(crate) struct AnthropicConfig {
    /// 协议根地址（不含 `/v1/messages`），如 MiniMax `https://api.minimaxi.com/anthropic`
    pub base_url: String,
    pub api_key: String,
    pub timeout: Duration,
    pub retry: RetryPolicy,
    /// 附加请求头，默认空
    pub extra_headers: Vec<(String, String)>,
}

/// Anthropic Messages 协议客户端（共享底座）
///
/// 各厂商适配器持有本客户端，组装 vendor-specific body 后调用 `chat` / `chat_stream`。
/// 内部 `reqwest::Client` 复用连接池，线程安全（`Send + Sync + Clone`）。
pub(crate) struct AnthropicClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    retry: RetryPolicy,
    extra_headers: Vec<(String, String)>,
}

impl AnthropicClient {
    pub fn new(cfg: AnthropicConfig) -> Result<Self, LlmError> {
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
        format!("{}/v1/messages", self.base_url)
    }

    /// 构造附加请求头（非法键/值静默忽略，不阻断请求）
    fn header_map(&self) -> reqwest::header::HeaderMap {
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
        let chunk_stream = parse_anth_stream(json_stream);
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

/// 构造 Anthropic Messages 协议的公共 body 字段
///
/// - `max_tokens` 为协议必填，由厂商适配器解析（`req.max_tokens` 缺失时回退模型上限）
/// - System 消息聚合至顶层 `system` 字段，不进 `messages`
/// - `stream` 字段由客户端在发送前填入
pub(crate) fn build_common_body(
    messages: &[Message],
    tools: &[ToolDeclaration],
    tool_choice: ToolChoice,
    temperature: Option<f32>,
    max_tokens: usize,
    model: &str,
) -> Value {
    let mut system: Vec<String> = Vec::new();
    let mut messages_json: Vec<Value> = Vec::new();
    for m in messages {
        match m.role {
            Role::System => {
                if let Some(t) = m.content.as_text() {
                    system.push(t.to_string());
                }
            }
            _ => messages_json.push(message_to_body_json(m)),
        }
    }

    let mut body = json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages_json,
    });
    if !system.is_empty() {
        body["system"] = json!(system.join("\n\n"));
    }
    if !tools.is_empty() {
        let tools_json: Vec<Value> = tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.parameters,
                })
            })
            .collect();
        body["tools"] = json!(tools_json);
        body["tool_choice"] = json!(match tool_choice {
            ToolChoice::Auto => json!({ "type": "auto" }),
            ToolChoice::None => json!({ "type": "none" }),
            ToolChoice::Required => json!({ "type": "any" }),
        });
    }
    if let Some(t) = temperature {
        body["temperature"] = json!(t);
    }
    body
}

// ───────────────────────────────────────────────
// 消息 → Anthropic 请求 body JSON（多模态序列化）
// ───────────────────────────────────────────────

/// 将 [`Message`] 序列化为 Anthropic 请求体中的消息对象
///
/// `content` 一律映射为内容块数组；`Role::System` 已由 [`build_common_body`]
/// 提取至顶层，不在此处理。
fn message_to_body_json(m: &Message) -> Value {
    let mut obj = json!({ "role": role_to_str(&m.role) });
    match m.role {
        // 工具结果：Anthropic 以 `user` 角色的 `tool_result` 块表达
        Role::Tool => {
            let is_text = matches!(m.content, MessageContent::Text(_));
            // tool_result 块：单文本用字符串简写，多模态 / 复杂内容用块数组
            let content = if is_text {
                json!(m.content.as_text().unwrap_or_default())
            } else {
                json!(content_blocks(&m.content))
            };
            obj["content"] = json!([{
                "type": "tool_result",
                "tool_use_id": m.tool_call_id.clone().unwrap_or_default(),
                "content": content,
            }]);
        }
        Role::Assistant => {
            // assistant 回传：文本块 + 思考块 + tool_use 块（多轮工具调用协议要求）
            obj["content"] = json!(assistant_blocks(m));
        }
        _ => {
            obj["content"] = json!(content_blocks(&m.content));
        }
    }
    obj
}

fn role_to_str(r: &Role) -> &'static str {
    match r {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "user",
    }
}

/// 普通消息文本/多模态 → 内容块数组（user / assistant 无工具时的通用形态）
fn content_blocks(content: &MessageContent) -> Vec<Value> {
    match content {
        MessageContent::Text(t) => vec![json!({ "type": "text", "text": t })],
        MessageContent::Multimodal(parts) => parts.iter().map(content_part_to_block).collect(),
    }
}

/// assistant 消息：文本块 + 思考块 + 工具调用块（多轮回传协议要求）
fn assistant_blocks(m: &Message) -> Vec<Value> {
    let mut blocks = content_blocks(&m.content);
    // 深度思考回传：Anthropic 协议要求携带 thinking 块（signature 为协议扩展点，
    // 本层未保留时缺省；个别厂商多轮强制要求时由调用方经 `extra` 补充）
    if let Some(rc) = &m.reasoning_content {
        if !rc.is_empty() {
            blocks.push(json!({ "type": "thinking", "thinking": rc }));
        }
    }
    for tc in &m.tool_calls {
        blocks.push(tool_call_to_tool_use(tc));
    }
    blocks
}

/// 将 [`ContentPart`] 序列化为 Anthropic 内容块
fn content_part_to_block(part: &ContentPart) -> Value {
    match part {
        ContentPart::Text { text } => json!({ "type": "text", "text": text }),
        ContentPart::Image { source, .. } => image_block(source),
        ContentPart::Video { source, params } => {
            let mut v = json!({
                "type": "video",
                "source": media_source_to_source(source),
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
        ContentPart::Audio { .. } => {
            // Anthropic Messages 协议（MiniMax M3）未定义音频块；能力声明 audio=false 上游不会发送
            json!({ "type": "text", "text": "[audio media omitted]" })
        }
    }
}

fn image_block(source: &MediaSource) -> Value {
    match source {
        MediaSource::Url { url } => json!({ "type": "image", "source": { "type": "url", "url": url } }),
        MediaSource::Base64 { mime, data } => json!({
            "type": "image",
            "source": { "type": "base64", "media_type": mime, "data": data },
        }),
        MediaSource::FileId { file_id } => json!({
            "type": "image",
            "source": { "type": "url", "url": format!("ms://{file_id}") },
        }),
    }
}

/// 将 [`MediaSource`] 映射为 Anthropic `source` 对象
fn media_source_to_source(src: &MediaSource) -> Value {
    match src {
        MediaSource::Url { url } => json!({ "type": "url", "url": url }),
        MediaSource::Base64 { mime, data } => {
            json!({ "type": "base64", "media_type": mime, "data": data })
        }
        MediaSource::FileId { file_id } => json!({ "type": "url", "url": format!("ms://{file_id}") }),
    }
}

/// 将 [`crate::provider::ToolCall`] 序列化为 Anthropic `tool_use` 内容块
fn tool_call_to_tool_use(tc: &ToolCall) -> Value {
    let input = serde_json::from_str(&tc.function.arguments).unwrap_or(Value::Null);
    json!({
        "type": "tool_use",
        "id": tc.id,
        "name": tc.function.name,
        "input": input,
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
        402 => LlmError::InsufficientBalance(format!("HTTP {status_code}: {body}")),
        404 => LlmError::BadRequest(format!("HTTP {status_code}: {body}")),
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
    let id = json_str(json, "id")?.to_string();
    let model = json_str(json, "model")?.to_string();
    let stop_reason = json
        .get("stop_reason")
        .and_then(|v| v.as_str())
        .map(map_stop_reason)
        .unwrap_or(FinishReason::Stop);

    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<ToolCall> = Vec::new();
    if let Some(blocks) = json.get("content").and_then(|c| c.as_array()) {
        for block in blocks {
            match block.get("type").and_then(|t| t.as_str()) {
                Some("text") => {
                    if let Some(s) = block.get("text").and_then(|t| t.as_str()) {
                        text.push_str(s);
                    }
                }
                Some("thinking") => {
                    if let Some(s) = block.get("thinking").and_then(|t| t.as_str()) {
                        reasoning.push_str(s);
                    }
                }
                Some("tool_use") => {
                    if let Some(tc) = parse_tool_use(block) {
                        tool_calls.push(tc);
                    }
                }
                _ => {} // signature_delta / 其它块不产出模型输入
            }
        }
    }

    let usage = json.get("usage").filter(|u| !u.is_null()).map(parse_usage);
    let message = Message {
        role: Role::Assistant,
        content: MessageContent::Text(text),
        reasoning_content: if reasoning.is_empty() { None } else { Some(reasoning) },
        tool_calls,
        tool_call_id: None,
        usage: usage.clone(),
    };
    Ok(ChatResponse {
        id,
        model,
        message,
        finish_reason: stop_reason,
        usage,
    })
}

fn json_str<'a>(v: &'a Value, key: &str) -> Result<&'a str, LlmError> {
    v.get(key)
        .and_then(|x| x.as_str())
        .ok_or_else(|| LlmError::Protocol(format!("missing '{key}' in response")))
}

/// 将 Anthropic `tool_use` 块解析为 [`ToolCall`]（参数对象序列化为 JSON 字符串）
fn parse_tool_use(block: &Value) -> Option<ToolCall> {
    let id = block.get("id")?.as_str()?.to_string();
    let name = block.get("name")?.as_str()?.to_string();
    let input = block.get("input").cloned().unwrap_or(Value::Null);
    let arguments = match input {
        Value::Null => String::new(),
        other => other.to_string(),
    };
    Some(ToolCall {
        id,
        function: ToolCallFunction { name, arguments },
    })
}

fn map_stop_reason(s: &str) -> FinishReason {
    match s {
        "end_turn" | "stop_sequence" => FinishReason::Stop,
        "max_tokens" => FinishReason::Length,
        "tool_use" => FinishReason::ToolCalls,
        other => FinishReason::Other(other.to_string()),
    }
}

fn parse_usage(u: &Value) -> TokenUsage {
    let get = |k: &str| u.get(k).and_then(|v| v.as_u64()).map(|n| n as usize);
    let prompt_tokens = get("input_tokens").unwrap_or(0);
    let completion_tokens = get("output_tokens").unwrap_or(0);
    // 归一化视角：Anthropic 缓存命中的读=read，创建（写入）=write
    let cache_read = get("cache_read_input_tokens");
    let cache_write = get("cache_creation_input_tokens");
    TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens + completion_tokens,
        // Anthropic 不单独报告推理 token（thinking 计入 output）
        reasoning_tokens: None,
        prompt_cache_hit_tokens: None,
        prompt_cache_miss_tokens: None,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
    }
}

// ───────────────────────────────────────────────
// Anthropic SSE 事件 → StreamChunk 流
// ───────────────────────────────────────────────

/// 将 Anthropic SSE 事件流转换为 [`StreamChunk`] 流
///
/// 状态机：累积 `finish_reason` 与 `usage`（来自 `message_delta`），
/// 两者齐备或流结束时发出 [`StreamChunk::Finish`]（引擎依赖它收敛终态）。
fn parse_anth_stream(
    json_stream: BoxStream<'static, Result<Value, LlmError>>,
) -> BoxStream<'static, Result<StreamChunk, LlmError>> {
    let state = AnthState {
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
            // 3. 拉取下一个事件
            match state.inner.next().await {
                Some(Ok(json)) => match parse_anth_chunk(&json) {
                    (delta_opt, finish_opt, usage_opt) => {
                        if let Some(fr) = finish_opt {
                            state.pending_finish = Some(fr);
                        }
                        if let Some(u) = usage_opt {
                            state.pending_usage = Some(u);
                        }
                        if let Some(delta) = delta_opt {
                            return Some((Ok(delta), state));
                        }
                        continue;
                    }
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

struct AnthState {
    inner: BoxStream<'static, Result<Value, LlmError>>,
    pending_finish: Option<FinishReason>,
    pending_usage: Option<TokenUsage>,
    finish_emitted: bool,
}

/// 单个 Anthropic 事件的解析产物：(可选 Delta, 可选 finish_reason, 可选 usage)
type AnthChunkParts = (
    Option<StreamChunk>,
    Option<FinishReason>,
    Option<TokenUsage>,
);

/// 解析单个 Anthropic SSE 事件
///
/// - `content_block_start`（tool_use）→ 建立工具调用（index/id/name）
/// - `content_block_delta`：`thinking_delta`→reasoning / `text_delta`→content /
///   `input_json_delta`→工具参数增量片段
/// - `message_delta` → 携带 `stop_reason` + `usage`
fn parse_anth_chunk(json: &Value) -> AnthChunkParts {
    let mut delta = None;
    let mut finish = None;
    let mut usage = None;

    match json.get("type").and_then(|t| t.as_str()) {
        // content_block_start：thinking/text 起始为空文本不产出；tool_use 建立工具调用身份
        Some("content_block_start") => {
            let index = json.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
            let is_tool_use = json
                .get("content_block")
                .and_then(|c| c.get("type"))
                .and_then(|t| t.as_str())
                == Some("tool_use");
            if is_tool_use {
                let cb = json.get("content_block");
                let id = cb.and_then(|c| c.get("id")).and_then(|x| x.as_str()).map(String::from);
                let name = cb
                    .and_then(|c| c.get("name"))
                    .and_then(|x| x.as_str())
                    .map(String::from);
                delta = Some(StreamChunk::Delta {
                    content: None,
                    reasoning_content: None,
                    tool_calls: vec![ToolCallDelta {
                        index,
                        id,
                        function: Some(ToolCallFunctionDelta { name, arguments: None }),
                    }],
                    role: None,
                });
            }
        }
        Some("content_block_delta") => {
            let index = json.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
            let d = json.get("delta");
            match d.and_then(|x| x.get("type")).and_then(|t| t.as_str()) {
                Some("thinking_delta") => {
                    let thinking = d
                        .and_then(|x| x.get("thinking"))
                        .and_then(|x| x.as_str())
                        .map(String::from)
                        .filter(|s| !s.is_empty());
                    if thinking.is_some() {
                        delta = Some(StreamChunk::Delta {
                            content: None,
                            reasoning_content: thinking,
                            tool_calls: vec![],
                            role: None,
                        });
                    }
                }
                Some("text_delta") => {
                    let text = d
                        .and_then(|x| x.get("text"))
                        .and_then(|x| x.as_str())
                        .map(String::from)
                        .filter(|s| !s.is_empty());
                    if text.is_some() {
                        delta = Some(StreamChunk::Delta {
                            content: text,
                            reasoning_content: None,
                            tool_calls: vec![],
                            role: None,
                        });
                    }
                }
                Some("input_json_delta") => {
                    let args = d
                        .and_then(|x| x.get("partial_json"))
                        .and_then(|x| x.as_str())
                        .map(String::from)
                        .filter(|s| !s.is_empty());
                    if args.is_some() {
                        delta = Some(StreamChunk::Delta {
                            content: None,
                            reasoning_content: None,
                            tool_calls: vec![ToolCallDelta {
                                index,
                                id: None,
                                function: Some(ToolCallFunctionDelta { name: None, arguments: args }),
                            }],
                            role: None,
                        });
                    }
                }
                // signature_delta 等事件不产出模型输入
                _ => {}
            }
        }
        Some("message_delta") => {
            if let Some(fr) = json
                .get("delta")
                .and_then(|x| x.get("stop_reason"))
                .and_then(|x| x.as_str())
            {
                finish = Some(map_stop_reason(fr));
            }
            if let Some(u) = json.get("usage").filter(|u| !u.is_null()) {
                usage = Some(parse_usage(u));
            }
        }
        // message_start / content_block_stop / message_stop / ping — 无增量
        _ => {}
    }

    (delta, finish, usage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// 验证请求 body 映射：system 聚合顶层 / 多模态块 / thinking / max_tokens
    #[test]
    fn build_common_body_maps_anthropic_fields() {
        let body = build_common_body(
            &[
                Message::system("你是智能助手"),
                Message::user(MessageContent::multimodal(vec![
                    ContentPart::image(MediaSource::Url {
                        url: "https://example.com/img.jpeg".into(),
                    }),
                    ContentPart::text("图中是什么？"),
                ])),
            ],
            &[],
            ToolChoice::Auto,
            None,
            500,
            "minimax-m3",
        );
        assert_eq!(body["model"], json!("minimax-m3"));
        assert_eq!(body["max_tokens"], json!(500));
        assert_eq!(body["system"], json!("你是智能助手"));
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        let blocks = msgs[0]["content"].as_array().unwrap();
        assert_eq!(
            blocks[0],
            json!({"type":"image","source":{"type":"url","url":"https://example.com/img.jpeg"}})
        );
        assert_eq!(blocks[1], json!({"type":"text","text":"图中是什么？"}));
    }

    /// 非流式响应：thinking → reasoning_content、text → content、usage 归一
    #[test]
    fn parse_chat_response_maps_thinking_and_usage() {
        let json = json!({
            "id": "066a381bdc3c0ded310e27c9a46d16e7",
            "type": "message",
            "role": "assistant",
            "model": "MiniMax-M3",
            "content": [
                {"type": "thinking", "thinking": "思考一下", "signature": "sig-1"},
                {"type": "text", "text": "回答正文"}
            ],
            "usage": {
                "input_tokens": 1209,
                "output_tokens": 211,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 156
            },
            "stop_reason": "end_turn"
        });
        let resp = parse_chat_response(&json).expect("parse");
        assert_eq!(resp.id, "066a381bdc3c0ded310e27c9a46d16e7");
        assert_eq!(resp.message.content.as_text().unwrap(), "回答正文");
        assert_eq!(resp.message.reasoning_content.as_deref(), Some("思考一下"));
        assert_eq!(resp.finish_reason, FinishReason::Stop);
        let u = resp.usage.expect("usage");
        assert_eq!(u.prompt_tokens, 1209);
        assert_eq!(u.completion_tokens, 211);
        assert_eq!(u.total_tokens, 1420);
        assert_eq!(u.cache_read_tokens, Some(156));
        assert_eq!(u.cache_write_tokens, Some(0));
    }

    /// 流式事件收敛：thinking/text 增量 + tool_use + message_delta → Finish
    #[test]
    fn anth_stream_accumulates_deltas_and_finish() {
        use futures::stream;
        use futures::StreamExt;
        let json_stream = Box::pin(stream::iter(vec![
            Ok::<Value, LlmError>(json!({"type": "message_start", "message": {"id": "x"}})),
            Ok::<Value, LlmError>(json!({"type": "content_block_start", "index": 0,
                "content_block": {"type": "thinking", "thinking": ""}})),
            Ok::<Value, LlmError>(json!({"type": "content_block_delta", "index": 0,
                "delta": {"type": "thinking_delta", "thinking": "推理"}})),
            Ok::<Value, LlmError>(json!({"type": "content_block_delta", "index": 1,
                "delta": {"type": "text_delta", "text": "正文"}})),
            Ok::<Value, LlmError>(json!({"type": "content_block_delta", "index": 2,
                "delta": {"type": "input_json_delta", "partial_json": "{\"x\":"}})),
            Ok::<Value, LlmError>(json!({"type": "message_delta",
                "delta": {"stop_reason": "end_turn"},
                "usage": {"input_tokens": 10, "output_tokens": 5}})),
            Ok::<Value, LlmError>(json!({"type": "message_stop"})),
        ]));
        let chunk_stream = parse_anth_stream(json_stream);
        let mut reasoning = String::new();
        let mut content = String::new();
        let mut finish: Option<StreamChunk> = None;
        let mut stream = Box::pin(chunk_stream);
        while let Some(item) = futures::executor::block_on(stream.next()) {
            match item.expect("ok") {
                StreamChunk::Delta { content: c, reasoning_content: r, .. } => {
                    if let Some(c) = c {
                        content.push_str(&c);
                    }
                    if let Some(r) = r {
                        reasoning.push_str(&r);
                    }
                }
                f @ StreamChunk::Finish { .. } => finish = Some(f),
            }
        }
        assert_eq!(reasoning, "推理");
        assert_eq!(content, "正文");
        match finish.expect("finish") {
            StreamChunk::Finish { finish_reason, usage } => {
                assert_eq!(finish_reason, FinishReason::Stop);
                assert_eq!(usage.expect("usage").prompt_tokens, 10);
            }
            StreamChunk::Delta { .. } => panic!("expected finish"),
        }
    }

    /// 工具声明序列化为 Anthropic input_schema
    #[test]
    fn tools_serialize_as_input_schema() {
        use crate::provider::ToolDeclaration;
        let body = build_common_body(
            &[Message::user("hi")],
            &[ToolDeclaration {
                name: "get_weather".into(),
                description: "查询天气".into(),
                parameters: json!({"type": "object", "properties": {"city": {"type": "string"}}}),
            }],
            ToolChoice::Required,
            None,
            500,
            "m",
        );
        assert_eq!(body["tool_choice"], json!({"type": "any"}));
        assert_eq!(body["tools"][0]["name"], json!("get_weather"));
        assert_eq!(body["tools"][0]["input_schema"]["properties"]["city"]["type"], json!("string"));
    }

    /// max_tokens 为必填字段，透传调用方取值
    #[test]
    fn max_tokens_always_present() {
        let body = build_common_body(&[Message::user("hi")], &[], ToolChoice::Auto, None, 1024, "m");
        assert_eq!(body["max_tokens"], json!(1024));
    }
}
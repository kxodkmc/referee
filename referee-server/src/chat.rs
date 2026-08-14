//! 对话公共助手 — transport-agnostic，供 TCP 与 HTTP 传输复用
//!
//! 把「用户消息 → `ChatPayload`」、「chunk 流 → `StreamFrame` / `ChatReply`」等
//! 与传输无关的转换集中于此，避免 TCP / HTTP 双份实现。**不承载业务判定**，
//! 只做纯数据组装与收敛。

use futures::stream::BoxStream;
use futures::StreamExt;

use referee_ai_base::provider::{ChatResponse, FinishReason, LlmError, Message, StreamChunk, TokenUsage};
use referee_ai_base::session::{ChatOptions, ChatPayload, SessionId};

use crate::protocol::{ChatReply, ServerError, StreamFrame, TokenUsageData, ERR_INTERNAL};

/// 由 user 消息 + 可选采样参数构造对话载荷
pub fn build_payload(message: &str, temperature: Option<f32>, max_tokens: Option<usize>) -> ChatPayload {
    ChatPayload {
        message: Message::user(message),
        options: ChatOptions {
            temperature,
            max_tokens,
            ..Default::default()
        },
        peer_depth: 0,
    }
}

/// `FinishReason` → 稳定字符串（跨传输统一口径）
pub fn finish_reason_str(fr: &FinishReason) -> String {
    match fr {
        FinishReason::Stop => "stop",
        FinishReason::Length => "length",
        FinishReason::ToolCalls => "tool_calls",
        FinishReason::ContentFilter => "content_filter",
        FinishReason::Other(s) => return s.clone(),
    }
    .to_string()
}

/// 由流式 reply 收敛为 `ChatReply`（非流式路径；TCP 与 HTTP 复用）
///
/// 遍历 chunk 流：累积 Delta content，取最后 Finish 的 finish_reason / usage；
/// 任一 chunk 报错则返回 `ServerError`（不吞异常）。
pub async fn converge_stream(
    stream: BoxStream<'static, Result<StreamChunk, LlmError>>,
    session_id: SessionId,
) -> Result<ChatReply, ServerError> {
    let mut content = String::new();
    let mut finish_reason = "stop".to_string();
    let mut usage: Option<TokenUsage> = None;
    let mut stream = stream;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(StreamChunk::Delta { content: c, .. }) => {
                if let Some(c) = c {
                    content.push_str(&c);
                }
            }
            Ok(StreamChunk::Finish { finish_reason: fr, usage: u }) => {
                finish_reason = finish_reason_str(&fr);
                usage = u;
            }
            Err(e) => return Err(ServerError::new(ERR_INTERNAL, e.to_string())),
        }
    }
    Ok(ChatReply {
        session_id: session_id.to_string(),
        content,
        finish_reason,
        usage: usage.as_ref().map(TokenUsageData::from),
    })
}

/// 由非流式 `ChatResponse` 组装 `ChatReply`（极少数非流式回退路径）
pub fn reply_from_success(session_id: SessionId, resp: &ChatResponse) -> ChatReply {
    ChatReply {
        session_id: session_id.to_string(),
        content: resp.message.content.as_text().unwrap_or("").to_string(),
        finish_reason: finish_reason_str(&resp.finish_reason),
        usage: resp.usage.as_ref().map(TokenUsageData::from),
    }
}

/// 单 chunk → `StreamFrame`（流式帧；TCP 与 HTTP 同构，客户端可共用解析）
pub fn chunk_to_frame(chunk: Result<StreamChunk, LlmError>) -> StreamFrame {
    match chunk {
        Ok(StreamChunk::Delta {
            content,
            reasoning_content,
            ..
        }) => StreamFrame::Delta {
            content,
            reasoning_content,
        },
        Ok(StreamChunk::Finish {
            finish_reason,
            usage,
        }) => StreamFrame::Finish {
            finish_reason: finish_reason_str(&finish_reason),
            usage: usage.as_ref().map(TokenUsageData::from),
        },
        Err(e) => StreamFrame::Error {
            message: e.to_string(),
        },
    }
}
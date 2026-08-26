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

use futures::stream::{self, BoxStream, StreamExt};
use futures::Stream;
use serde_json::Value;

use crate::provider::LlmError;

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

fn map_reqwest_err(e: reqwest::Error) -> LlmError {
    if e.is_timeout() {
        LlmError::Timeout
    } else {
        LlmError::Network(e.to_string())
    }
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
}
//! SSE 流式输出 — 消费 `Instance::chat` 的 chunk 流，产出 `text/event-stream`
//!
//! 事件 `data` 为 [`StreamFrame`](crate::protocol::StreamFrame)（Delta/Finish/Error），
//! 与 TCP 流式帧**同构**，客户端可共用解析。逐 chunk 即时下发，不缓冲整段
//! （背压硬约束：客户端可流式消费）。

use std::convert::Infallible;

use axum::response::sse::{Event, Sse};
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;

use referee_ai::provider::{LlmError, StreamChunk};

use crate::chat;
use crate::protocol::StreamFrame;

/// 把 chat chunk 流映射为 SSE 事件流
pub fn sse_stream(
    stream: BoxStream<'static, Result<StreamChunk, LlmError>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let events = stream.map(|chunk| {
        let frame: StreamFrame = chat::chunk_to_frame(chunk);
        let data =
            serde_json::to_string(&frame).expect("StreamFrame serialization cannot fail");
        Ok::<_, Infallible>(Event::default().data(data))
    });
    Sse::new(events)
}
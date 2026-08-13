//! 流式输出 — 引擎流式执行路径
//!
//! 与 [`super::run_chat_inner`] 对称：复用同步段（`prepare_round`）与
//! [`Session::finish_thinking`] 终态收敛，仅 LLM 调用段改用 `chat_stream`。
//!
//! ## 结构
//! - `run_stream`：入口，创建转发通道并返回 `EngineReply::Streaming`，
//!   循环在派生任务中推进。
//! - `stream_loop`：流式执行循环（含多轮工具调用），每轮把 chunk 转发给
//!   调用方并交给 [`StreamAccumulator`] 累积。
//! - `StreamAccumulator`：把 Delta 序列收敛为完整 `ChatResponse`，供
//!   `finish_thinking` 复用，保证流式与非流式在 Session 上的终态一致。

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::channel::mpsc;
use futures::stream::BoxStream;
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::sync::oneshot;

use crate::budget::{add_tokens, tokens_from_response};
use crate::provider::{
    ChatResponse, FinishReason, LlmError, Message, MessageContent, Role, StreamChunk, TokenUsage,
    ToolCall, ToolCallFunction,
};
use crate::session::{FinishAction, RoundStart, SessionId, TurnOutcome};

use super::{panic_message, Engine, EngineReply, RoundSource, ToolRound};

/// 流式派生任务入口：转发通道建立后立即返回，循环在后台推进
impl Engine {
    pub(crate) async fn run_stream(
        &self,
        session_id: &SessionId,
        first: RoundStart,
    ) -> EngineReply {
        let (tx, rx) = mpsc::channel::<Result<StreamChunk, LlmError>>(64);
        let engine = self.clone();
        let sid = *session_id;
        tokio::spawn(async move {
            stream_loop(&engine, &sid, first, tx).await;
        });
        EngineReply::Streaming(Box::pin(rx))
    }
}

/// 流式执行循环 — 每轮：流式调用 → 转发 + 累积 → 计量 → 收敛 → 工具轮
async fn stream_loop(
    engine: &Engine,
    session_id: &SessionId,
    first: RoundStart,
    mut tx: mpsc::Sender<Result<StreamChunk, LlmError>>,
) {
    let timeout = engine.config.session.timeout.thinking_timeout;
    let mut src = RoundSource::First(first);

    loop {
        let cur = std::mem::replace(&mut src, RoundSource::Resume);
        let (turn_id, cancel_rx, request) = match cur {
            RoundSource::First(f) => (f.turn_id, f.cancel_rx, f.request),
            RoundSource::Resume => {
                match engine
                    .sessions
                    .get_mut(session_id)
                    .and_then(|mut s| s.resume_thinking())
                {
                    Some(x) => x,
                    None => break,
                }
            }
        };

        if engine.is_interrupted(session_id) {
            break;
        }

        let stream = match engine.provider.chat_stream(request).await {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                break;
            }
        };

        let provider_id = engine.provider.id().to_string();
        let outcome = consume_stream(stream, cancel_rx, timeout, &mut tx, provider_id).await;

        if let TurnOutcome::Success(resp) = &outcome {
            add_tokens(&engine.total_consumed_tokens, tokens_from_response(resp));
        }

        let action = engine
            .sessions
            .get_mut(session_id)
            .map(|mut s| s.finish_thinking(turn_id, outcome))
            .unwrap_or(FinishAction::Idle { response: None });

        match action {
            FinishAction::Idle { .. } => break,
            FinishAction::AwaitingCalls { tool_calls, .. } => {
                if engine.tools.is_none() || engine.tool_executor.is_none() {
                    if let Some(mut s) = engine.sessions.get_mut(session_id) {
                        s.force_idle();
                    }
                    break;
                }
                match engine.run_tool_calls(session_id, turn_id, tool_calls).await {
                    ToolRound::Resume => {}
                    ToolRound::Settled => break,
                }
            }
        }
    }
}

/// 消费流：逐 chunk 转发给调用方并累积；取消逐 chunk 检查、超时覆盖整体、panic 兜底
async fn consume_stream(
    mut stream: BoxStream<'static, Result<StreamChunk, LlmError>>,
    mut cancel_rx: oneshot::Receiver<()>,
    timeout: Duration,
    tx: &mut mpsc::Sender<Result<StreamChunk, LlmError>>,
    provider_id: String,
) -> TurnOutcome {
    let mut acc = StreamAccumulator::new();
    let result = AssertUnwindSafe(async {
        let inner = async {
            loop {
                tokio::select! {
                    chunk = stream.next() => match chunk {
                        Some(Ok(c)) => {
                            let _ = tx.send(Ok(c.clone())).await;
                            acc.push(c);
                        }
                        Some(Err(e)) => {
                            let _ = tx.send(Err(e.clone())).await;
                            return TurnOutcome::Error(e);
                        }
                        None => break,
                    },
                    _ = &mut cancel_rx => return TurnOutcome::Cancelled,
                }
            }
            match acc.finish() {
                Some((message, finish_reason, usage)) => {
                    TurnOutcome::Success(Box::new(ChatResponse {
                        id: provider_id.clone(),
                        model: provider_id,
                        message,
                        finish_reason,
                        usage,
                    }))
                }
                None => TurnOutcome::Error(LlmError::Protocol(
                    "stream ended without finish chunk".into(),
                )),
            }
        };
        match tokio::time::timeout(timeout, inner).await {
            Ok(outcome) => outcome,
            Err(_) => TurnOutcome::Timeout,
        }
    })
    .catch_unwind()
    .await;

    match result {
        Ok(outcome) => outcome,
        Err(payload) => TurnOutcome::Panic(panic_message(payload)),
    }
}

/// 流内累积器 — 把 Delta 序列收敛为完整 `ChatResponse`
pub(crate) struct StreamAccumulator {
    content: String,
    reasoning_content: String,
    role: Option<Role>,
    tool_calls: BTreeMap<u32, ToolCallAccum>,
    finish_reason: Option<FinishReason>,
    usage: Option<TokenUsage>,
    finished: bool,
}

#[derive(Default)]
struct ToolCallAccum {
    id: String,
    name: String,
    arguments: String,
}

impl StreamAccumulator {
    pub(crate) fn new() -> Self {
        Self {
            content: String::new(),
            reasoning_content: String::new(),
            role: None,
            tool_calls: BTreeMap::new(),
            finish_reason: None,
            usage: None,
            finished: false,
        }
    }

    /// 累积一个 chunk（Delta 追加，Finish 终结）
    pub(crate) fn push(&mut self, chunk: StreamChunk) {
        match chunk {
            StreamChunk::Delta {
                content,
                reasoning_content,
                tool_calls,
                role,
            } => {
                if let Some(c) = content {
                    self.content.push_str(&c);
                }
                if let Some(r) = reasoning_content {
                    self.reasoning_content.push_str(&r);
                }
                if let Some(r) = role {
                    self.role = Some(r);
                }
                for delta in tool_calls {
                    let acc = self.tool_calls.entry(delta.index).or_default();
                    if let Some(id) = delta.id {
                        acc.id = id;
                    }
                    if let Some(f) = delta.function {
                        if let Some(n) = f.name {
                            acc.name = n;
                        }
                        if let Some(a) = f.arguments {
                            acc.arguments.push_str(&a);
                        }
                    }
                }
            }
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                self.finish_reason = Some(finish_reason);
                self.usage = usage;
                self.finished = true;
            }
        }
    }

    /// 收敛为完整消息；未收到 Finish chunk 返回 None（流不完整）
    pub(crate) fn finish(self) -> Option<(Message, FinishReason, Option<TokenUsage>)> {
        if !self.finished {
            return None;
        }
        let tool_calls: Vec<ToolCall> = self
            .tool_calls
            .into_values()
            .map(|acc| ToolCall {
                id: acc.id,
                function: ToolCallFunction {
                    name: acc.name,
                    arguments: acc.arguments,
                },
            })
            .collect();
        let reasoning_content = if self.reasoning_content.is_empty() {
            None
        } else {
            Some(self.reasoning_content)
        };
        let message = Message {
            role: self.role.unwrap_or(Role::Assistant),
            content: MessageContent::text(self.content),
            reasoning_content,
            tool_calls,
            tool_call_id: None,
        };
        Some((
            message,
            self.finish_reason.unwrap_or(FinishReason::Stop),
            self.usage,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::ToolCallDelta;

    #[test]
    fn accumulator_merges_delta_and_tool_calls() {
        let mut acc = StreamAccumulator::new();
        acc.push(StreamChunk::Delta {
            content: Some("Hello".into()),
            reasoning_content: None,
            tool_calls: vec![],
            role: Some(Role::Assistant),
        });
        acc.push(StreamChunk::Delta {
            content: Some(" world".into()),
            reasoning_content: Some("think".into()),
            tool_calls: vec![
                ToolCallDelta {
                    index: 0,
                    id: Some("tc_1".into()),
                    function: Some(crate::provider::ToolCallFunctionDelta {
                        name: Some("echo".into()),
                        arguments: Some("{\"x\":".into()),
                    }),
                },
                ToolCallDelta {
                    index: 0,
                    id: None,
                    function: Some(crate::provider::ToolCallFunctionDelta {
                        name: None,
                        arguments: Some("\"1\"}".into()),
                    }),
                },
            ],
            role: None,
        });
        acc.push(StreamChunk::Finish {
            finish_reason: FinishReason::ToolCalls,
            usage: Some(TokenUsage {
                total_tokens: 5,
                ..Default::default()
            }),
        });

        let (message, finish_reason, usage) = acc.finish().expect("finish");
        assert_eq!(message.content.as_text().unwrap(), "Hello world");
        assert_eq!(message.reasoning_content.as_deref(), Some("think"));
        assert_eq!(message.tool_calls.len(), 1);
        assert_eq!(message.tool_calls[0].id, "tc_1");
        assert_eq!(message.tool_calls[0].function.name, "echo");
        assert_eq!(message.tool_calls[0].function.arguments, r#"{"x":"1"}"#);
        assert_eq!(finish_reason, FinishReason::ToolCalls);
        assert_eq!(usage.unwrap().total_tokens, 5);
    }

    #[test]
    fn accumulator_without_finish_returns_none() {
        let mut acc = StreamAccumulator::new();
        acc.push(StreamChunk::Delta {
            content: Some("no finish".into()),
            reasoning_content: None,
            tool_calls: vec![],
            role: Some(Role::Assistant),
        });
        assert!(acc.finish().is_none());
    }
}

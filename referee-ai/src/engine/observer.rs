//! 引擎观测器 — 回合起止 / 输出增量 / 工具起止事件钩子
//!
//! ## 实现契约（实现方必须遵守）
//! - **非阻塞**：回调在引擎热循环内同步执行，实现侧只做轻量操作
//!   （推荐 `mpsc::Sender::try_send` 转交外部消费者；慢消费者由实现方自负，
//!   引擎不为回调缓冲任何数据——背压硬约束）
//! - **不得依赖 panic 兜底**：引擎侧逐回调 `catch_unwind`，异常被吞并记日志，
//!   观测故障绝不影响回合循环；实现方仍应自行保证不 panic
//!
//! ## 边界（数据/行为分离）
//! observer 是行为句柄，只存在于 [`Engine`]（`with_observer` 注入，不进
//! `EngineConfig`），绝不进 Envelope / SessionReply 等数据载体。
//!
//! 结构化工具结果（[`ToolOutcome`]）是数据出口，随 `ExecutedTool` 传递，
//! 任何调用方可程序化消费，与 observer 解耦。

use std::panic::AssertUnwindSafe;

use crate::provider::{StreamChunk, TokenUsage};
use crate::session::SessionId;
use crate::tool::ToolOutcome;

use super::Engine;

/// 引擎观测器 — 默认空实现，按需覆写子集
///
/// 事件语义：
/// - 回合（turn）= 一次思考轮（每次 LLM 调用一轮，含工具 resume 后的后续轮），
///   `on_turn_started` / `on_turn_finished` 严格成对
/// - 工具事件仅覆盖实际执行项（等待类 + 派发类）；截断 / 深度拦截项不执行、不触发
#[allow(unused_variables)]
pub trait EngineObserver: Send + Sync {
    /// 一个思考轮开始
    fn on_turn_started(&self, session_id: SessionId, turn_id: u64) {}

    /// 思考增量（reasoning delta）
    fn on_thinking_delta(&self, session_id: SessionId, delta: &str) {}

    /// 文本增量（content delta）
    fn on_text_delta(&self, session_id: SessionId, delta: &str) {}

    /// 工具调用开始
    fn on_tool_started(&self, session_id: SessionId, tool_call_id: &str, name: &str) {}

    /// 工具调用结束（等待类与派发类复用同一 [`ToolOutcome`]）
    fn on_tool_finished(
        &self,
        session_id: SessionId,
        tool_call_id: &str,
        outcome: ToolOutcome,
        duration_ms: u64,
    ) {
    }

    /// 一个思考轮结束（usage 仅成功轮携带，错误/取消/超时轮为 None）
    fn on_turn_finished(&self, session_id: SessionId, turn_id: u64, usage: Option<TokenUsage>) {}
}

impl Engine {
    /// 触发一次 observer 回调（未注入时零开销；panic 兜底不影响回合循环）
    pub(crate) fn observe_event<F: FnOnce(&dyn EngineObserver)>(&self, f: F) {
        let Some(o) = &self.observer else {
            return;
        };
        if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| f(o.as_ref()))) {
            tracing::warn!(
                panic = %super::panic_message(payload),
                "engine observer callback panicked"
            );
        }
    }

    /// 逐 chunk 触发 delta 回调 — 流式转发（`stream_loop`）与内部流式收敛
    /// （`internal_stream_call`）双路径共享的唯一推送点
    pub(crate) fn observe_chunk_deltas(&self, session_id: &SessionId, chunk: &StreamChunk) {
        if let StreamChunk::Delta {
            content,
            reasoning_content,
            ..
        } = chunk
        {
            if let Some(r) = reasoning_content {
                self.observe_event(|o| o.on_thinking_delta(*session_id, r));
            }
            if let Some(t) = content {
                self.observe_event(|o| o.on_text_delta(*session_id, t));
            }
        }
    }
}

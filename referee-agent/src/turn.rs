//! Turn 任务 — LLM 调用 + 终态收敛 + 工具派发
//!
//! 从 `lib.rs` 提取，避免主文件超过 500 行。
//!
//! ## 流程
//! 1. `spawn_turn_task`：spawn 派生任务执行 LLM 调用
//! 2. `run_turn`：catch_unwind + cancel + timeout → `TurnOutcome`
//! 3. `converge`：终态收敛（`finish_thinking`），根据 `FinishAction`：
//!    - `Idle` → 经 `pending_reply` 回信
//!    - `AwaitingCalls` → spawn 工具执行 + emit 结果

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::FutureExt;
use metrics::counter;
use referee_core::{CapabilityId, Kernel};
use tracing::{info_span, warn, Instrument};

use crate::provider::{ChatRequest, LLMProvider};
use crate::session::{
    FinishAction, Session, SessionId, SessionMessage, SessionReply, SessionState, TurnOutcome,
};
use crate::tool::{ToolExecutor, ToolRegistry};

/// Turn 共享上下文 — 派生任务通过 `Arc` 引用
pub struct TurnContext {
    pub sessions: Arc<DashMap<SessionId, Session>>,
    pub provider: Arc<dyn LLMProvider>,
    pub kernel: Kernel,
    pub self_id: CapabilityId,
    pub tools: Option<ToolRegistry>,
    pub tool_executor: Option<ToolExecutor>,
}

/// 派生 turn 任务 — 终态自管
///
/// 执行 LLM 调用（`run_turn`），finally 收敛 Session 状态 + reply。
/// 外层 `catch_unwind` 兜底：即使收敛逻辑 panic 也强制恢复 Idle。
pub fn spawn_turn_task(
    tctx: Arc<TurnContext>,
    req: ChatRequest,
    cancel_rx: tokio::sync::oneshot::Receiver<()>,
    session_id: SessionId,
    turn_id: u64,
    timeout: Duration,
) {
    let sessions = tctx.sessions.clone();

    tokio::spawn(async move {
        let span = info_span!("agent_turn", session_id = %session_id, turn_id);
        let outcome = crate::session::run_turn(tctx.provider.chat(req), cancel_rx, timeout)
            .instrument(span)
            .await;

        let label = outcome_label(&outcome);

        // 终态收敛（外层 catch_unwind 兜底）
        let result = AssertUnwindSafe(async {
            converge(&tctx, session_id, turn_id, outcome).await;
        })
        .catch_unwind()
        .await;

        if result.is_err() {
            warn!(session_id = %session_id, "turn convergence panicked, forcing Idle");
            if let Some(mut session) = sessions.get_mut(&session_id) {
                if matches!(session.state, SessionState::Thinking { .. }) {
                    session.state = SessionState::Idle;
                }
            }
        }

        counter!("referee_agent_turns_total", "outcome" => label).increment(1);
    });
}

/// 终态收敛 — 根据 `FinishAction` 决定后续动作
async fn converge(tctx: &TurnContext, session_id: SessionId, turn_id: u64, outcome: TurnOutcome) {
    // 1. 终态收敛（短暂持锁，无 await）
    let action = if let Some(mut session) = tctx.sessions.get_mut(&session_id) {
        session.finish_thinking(turn_id, outcome)
    } else {
        FinishAction::Idle { response: None }
    };
    // guard dropped

    match action {
        FinishAction::Idle {
            response: Some(resp),
        } => {
            // 成功完成 → 经 pending_reply 回信
            reply_to_caller(tctx, session_id, SessionReply::from_response(resp));
        }
        FinishAction::Idle { response: None } => {
            // 错误/取消/超时/panic → 经 pending_reply 回信错误
            reply_to_caller(
                tctx,
                session_id,
                SessionReply::Error {
                    message: "turn ended without success (error/timeout/cancelled/panic)".into(),
                },
            );
        }
        FinishAction::AwaitingCalls {
            response,
            tool_calls,
        } => {
            if let (Some(registry), Some(executor)) = (&tctx.tools, &tctx.tool_executor) {
                // Phase 2: spawn 工具执行
                spawn_tool_execution(
                    tctx,
                    tool_calls,
                    registry.clone(),
                    executor.clone(),
                    session_id,
                    turn_id,
                );
            } else {
                // 无工具注册表 — Phase 1 兼容：强制 Idle + 回传完整响应
                if let Some(mut session) = tctx.sessions.get_mut(&session_id) {
                    session.force_idle();
                }
                reply_to_caller(tctx, session_id, SessionReply::from_response(response));
            }
        }
    }
}

/// 经 `pending_reply` 回信（oneshot channel → forwarder task → ctx.reply）
fn reply_to_caller(tctx: &TurnContext, session_id: SessionId, reply: SessionReply) {
    if let Some(mut session) = tctx.sessions.get_mut(&session_id) {
        if let Some(tx) = session.take_pending_reply() {
            let _ = tx.send(reply);
        }
    }
}

/// Spawn 工具执行任务 — 并行执行工具 + emit 结果
#[allow(clippy::too_many_arguments)]
fn spawn_tool_execution(
    tctx: &TurnContext,
    tool_calls: Vec<crate::provider::ToolCall>,
    registry: ToolRegistry,
    executor: ToolExecutor,
    session_id: SessionId,
    turn_id: u64,
) {
    let kernel = tctx.kernel.clone();
    let self_id = tctx.self_id;
    let sessions = tctx.sessions.clone();

    tokio::spawn(async move {
        let results = executor
            .execute_batch(tool_calls, &registry, session_id, turn_id)
            .await;

        for result in results {
            let tool_call_id = result.tool_call_id.clone();
            let result_content = result.result.clone();
            let msg = SessionMessage::ToolResult {
                session_id,
                turn_id,
                tool_call_id,
                result: result_content,
            };
            if let Err(e) = kernel.emit(self_id, msg.to_envelope()).await {
                // emit 失败（通道满）：直接更新 session pending，
                // 避免 pending 永远不清零导致幽灵会话
                warn!(error = ?e, session_id = %session_id, "emit ToolResult failed, updating session directly");
                if let Some(mut session) = sessions.get_mut(&session_id) {
                    session.finish_tool_call(&result.tool_call_id, result.result);
                }
            }
        }
    });
}

/// 获取 TurnOutcome 的标签字符串（用于 metrics）
pub fn outcome_label(outcome: &TurnOutcome) -> &'static str {
    match outcome {
        TurnOutcome::Success(_) => "success",
        TurnOutcome::Error(_) => "error",
        TurnOutcome::Cancelled => "cancelled",
        TurnOutcome::Timeout => "timeout",
        TurnOutcome::Panic(_) => "panic",
    }
}

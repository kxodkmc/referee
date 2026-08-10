//! Referee Agent Runtime — 基于 referee-core 的可选 SDK 式扩展模块
//!
//! 独立于内核（不触碰 `referee-core`），按需启用。
//!
//! ## 当前范围
//! - **Phase 0**：[`provider`] 厂商抽象层（LLMProvider trait + MiMo/DeepSeek 适配器）
//! - **Phase 1**：[`session`] 会话状态机 + [`AgentRuntime`] 扩展（实现 `Extension` trait）
//!
//! ## 架构
//! ```text
//!   ┌──────────────┐     Envelope      ┌──────────────────────┐
//!   │   Kernel     │ ─────────────────▶│   AgentRuntime       │
//!   │ (referee-core)│◀─────────────────│  (Extension trait)   │
//!   └──────────────┘   ctx.reply()     └─────────┬────────────┘
//!                                                │ spawn
//!                                                ▼
//!                                     ┌──────────────────────┐
//!                                     │  Turn Task (派生)     │
//!                                     │  run_turn + finally   │
//!                                     │  converge + reply     │
//!                                     └──────────────────────┘
//! ```
//!
//! ## 设计约束（AGENT_RUNTIME_PLAN §2）
//! - handle 内零阻塞：只做状态转移 + spawn
//! - 终态自管：派生任务 finally 唯一终态写入
//! - 禁止跨 await 持 guard：`get_mut` 短暂持锁，释放后再 reply
//! - busy 拒绝显式可见：返回 `Busy` 回信

pub mod provider;
pub mod session;

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::FutureExt;
use metrics::counter;
use referee_core::{CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelResult};
use tracing::Instrument;
use tracing::{info_span, warn};

use crate::provider::LLMProvider;
use crate::session::{
    message::{SessionMessage, SessionReply},
    FinishAction, Session, SessionConfig, SessionId, SessionState, TurnOutcome,
};

/// Agent Runtime 全局配置
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// 会话级配置模板（每个新 Session 继承此配置）
    pub session: SessionConfig,
    /// 最大并发会话数（有界，超限拒绝新会话）
    pub max_sessions: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            session: SessionConfig::default(),
            max_sessions: 1024,
        }
    }
}

/// Agent Runtime 扩展 — 管理 N 个 Session 的智能体运行时
///
/// 实现 `referee-core` 的 [`Extension`] trait，注册到 Kernel 后：
/// - 接收 `Chat` / `Interrupt` 等消息（经 `Envelope.metadata` 编解码）
/// - 每个 `SessionId` 对应一个独立会话（`DashMap` 隔离，互不阻塞）
/// - LLM 调用在派生任务中执行（`handle` 零阻塞）
///
/// ## 创建与注册
/// ```ignore
/// let kernel = Kernel::new();
/// let provider = Arc::new(XiaomiProvider::new(...)?);
/// let runtime = AgentRuntime::new(kernel.clone(), provider, AgentConfig::default());
/// let runtime_id = runtime.id();
/// kernel.register(Box::new(runtime), 64, SupervisionPolicy::Transient).await?;
/// // 通过 kernel.emit(runtime_id, msg.to_envelope()) 或 kernel.invoke(...) 发消息
/// ```
pub struct AgentRuntime {
    id: CapabilityId,
    /// 内核句柄（Clone 的，供派生任务未来 emit 子任务消息用；Phase 1 不直接使用）
    #[allow(dead_code)]
    kernel: Kernel,
    provider: Arc<dyn LLMProvider>,
    sessions: Arc<DashMap<SessionId, Session>>,
    config: AgentConfig,
}

impl std::fmt::Debug for AgentRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentRuntime")
            .field("id", &self.id)
            .field("sessions", &self.sessions.len())
            .field("max_sessions", &self.config.max_sessions)
            .finish()
    }
}

impl AgentRuntime {
    /// 创建 Agent Runtime
    ///
    /// `kernel` 参数为 `Kernel` 的 clone（内部全 `Arc`，clone 廉价），
    /// 供派生任务在未来 Phase（P2/P3）中 `emit` 子任务消息。
    pub fn new(kernel: Kernel, provider: Arc<dyn LLMProvider>, config: AgentConfig) -> Self {
        Self {
            id: CapabilityId::new(),
            kernel,
            provider,
            sessions: Arc::new(DashMap::new()),
            config,
        }
    }

    /// 扩展的 CapabilityId（注册前获取，用于 emit/invoke 目标寻址）
    pub fn id(&self) -> CapabilityId {
        self.id
    }

    /// 当前会话数
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    // ─────────────────────────────────────────────
    // 消息处理（handle 内调用，全部同步无 await）
    // ─────────────────────────────────────────────

    /// 处理 Chat 消息
    ///
    /// 流程：获取/创建 Session → busy 检查 → push history → start thinking →
    /// build request → spawn turn task（ctx move 进去）→ 返回
    fn handle_chat(
        &self,
        ctx: KernelContext,
        session_id: SessionId,
        payload: session::ChatPayload,
    ) {
        // 获取或创建 Session（短暂持锁，无 await）
        let session_entry = match self.get_or_create_session(session_id) {
            Some(entry) => entry,
            None => {
                // 会话数超限
                let _ = ctx.reply(
                    SessionReply::Error {
                        message: "max sessions reached".into(),
                    }
                    .to_envelope(),
                );
                return;
            }
        };

        // 状态转移 + history + build request（全部同步，持有 guard）
        let (turn_id, cancel_rx, req, timeout) = {
            let mut session = session_entry;
            if session.is_busy() {
                // busy 拒绝：显式回信，不静默 Err
                let turn_id = session.turn_id();
                drop(session);
                let _ = ctx.reply(SessionReply::Busy { turn_id }.to_envelope());
                counter!("referee_agent_busy_rejections_total").increment(1);
                return;
            }

            // 追加 user 消息到 history
            session.push_history(payload.message.clone());

            // Idle → Thinking
            let (turn_id, cancel_rx) = match session.start_thinking() {
                Some(pair) => pair,
                None => {
                    // 理论上不可达（已检查 is_busy），防御性兜底
                    drop(session);
                    let _ = ctx.reply(
                        SessionReply::Error {
                            message: "failed to start thinking".into(),
                        }
                        .to_envelope(),
                    );
                    return;
                }
            };

            // 构建 ChatRequest（history 已含最新 user 消息）
            let req = session.build_chat_request(&payload.options);
            let timeout = session.config().timeout.thinking_timeout;
            (turn_id, cancel_rx, req, timeout)
        };
        // guard 已 drop，无跨 await 持锁

        // spawn 派生任务（ctx move 进去）
        let sessions = self.sessions.clone();
        let provider = self.provider.clone();
        spawn_turn_task(
            sessions, provider, ctx, req, cancel_rx, session_id, turn_id, timeout,
        );
    }

    /// 处理 Interrupt 消息
    ///
    /// 发送取消信号（协作取消），不直接转 Idle（由 turn task finally 收敛）。
    fn handle_interrupt(&self, ctx: KernelContext, session_id: SessionId) {
        let cancelled = if let Some(mut session) = self.sessions.get_mut(&session_id) {
            session.cancel_thinking()
        } else {
            false
        };

        let reply = if cancelled {
            SessionReply::Cancelled
        } else {
            SessionReply::Unhandled {
                reason: "session not thinking or not found".into(),
            }
        };
        let _ = ctx.reply(reply.to_envelope());
    }

    /// 处理未支持的消息类型（P2/P3 预留）
    fn handle_unhandled(&self, ctx: KernelContext, kind: &str) {
        let _ = ctx.reply(
            SessionReply::Unhandled {
                reason: format!("message kind '{kind}' not supported in Phase 1"),
            }
            .to_envelope(),
        );
    }

    /// 获取或创建 Session
    ///
    /// 返回 `None` 表示会话数超限。
    fn get_or_create_session(
        &self,
        session_id: SessionId,
    ) -> Option<dashmap::mapref::one::RefMut<'_, SessionId, Session>> {
        // 先尝试获取已有 session
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return Some(session);
        }
        // 新建 session — 检查容量（软限制，多 shard 下有微小竞态）
        if self.sessions.len() >= self.config.max_sessions {
            return None;
        }
        // 插入新 session（如果已被其他线程插入，or_insert 不会覆盖）
        let _ = self
            .sessions
            .entry(session_id)
            .or_insert_with(|| Session::new(self.config.session.clone()));
        self.sessions.get_mut(&session_id)
    }
}

#[async_trait]
impl Extension for AgentRuntime {
    fn id(&self) -> CapabilityId {
        self.id
    }

    /// 消息处理入口 — 零阻塞（只做状态转移 + spawn）
    ///
    /// 解码 `Envelope.metadata` → 路由到对应 handler → 立即返回。
    /// 所有 I/O（LLM 调用）在派生任务中执行。
    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let msg = match SessionMessage::from_envelope(&env) {
            Ok(msg) => msg,
            Err(e) => {
                warn!(error = %e, "failed to decode session message");
                let _ = ctx.reply(
                    SessionReply::Error {
                        message: format!("decode error: {e}"),
                    }
                    .to_envelope(),
                );
                return Ok(());
            }
        };

        let span = info_span!(
            "agent_handle",
            session_id = %msg.session_id(),
            kind = %message_kind_label(&msg),
        );
        let _enter = span.enter();

        match msg {
            SessionMessage::Chat {
                session_id,
                payload,
            } => {
                self.handle_chat(ctx, session_id, payload);
            }
            SessionMessage::Interrupt { session_id } => {
                self.handle_interrupt(ctx, session_id);
            }
            SessionMessage::ToolResult { .. } => {
                self.handle_unhandled(ctx, "tool_result");
            }
            SessionMessage::Resume { .. } => {
                self.handle_unhandled(ctx, "resume");
            }
            SessionMessage::SubagentDone { .. } => {
                self.handle_unhandled(ctx, "subagent_done");
            }
        }

        Ok(())
    }
}

/// 派生 turn 任务 — 终态自管
///
/// 执行 LLM 调用（`run_turn`），finally 收敛 Session 状态 + reply。
/// 外层 `catch_unwind` 兜底：即使收敛逻辑 panic 也强制恢复 Idle。
#[allow(clippy::too_many_arguments)]
fn spawn_turn_task(
    sessions: Arc<DashMap<SessionId, Session>>,
    provider: Arc<dyn LLMProvider>,
    ctx: KernelContext,
    req: crate::provider::ChatRequest,
    cancel_rx: tokio::sync::oneshot::Receiver<()>,
    session_id: SessionId,
    turn_id: u64,
    timeout: std::time::Duration,
) {
    tokio::spawn(async move {
        let span = info_span!("agent_turn", session_id = %session_id, turn_id);
        let outcome = session::run_turn(provider.chat(req), cancel_rx, timeout)
            .instrument(span)
            .await;

        // 记录 outcome label（在 outcome 被 move 前取）
        let outcome_label = outcome_label(&outcome);

        // 终态收敛 + reply（外层 catch_unwind 兜底）
        let result = AssertUnwindSafe(async {
            converge_and_reply(&sessions, session_id, turn_id, outcome, ctx).await;
        })
        .catch_unwind()
        .await;

        if result.is_err() {
            // 收敛逻辑 panic — 强制恢复 Idle（防幽灵会话）
            warn!(session_id = %session_id, "turn task convergence panicked, forcing Idle");
            if let Some(mut session) = sessions.get_mut(&session_id) {
                if matches!(session.state, SessionState::Thinking { .. }) {
                    session.state = SessionState::Idle;
                }
            }
        }

        // Metrics
        counter!("referee_agent_turns_total", "outcome" => outcome_label).increment(1);
    });
}

/// 终态收敛 + reply（turn task 的 finally 逻辑）
///
/// 1. `get_mut` Session → `finish_thinking`（短暂持锁，无 await）
/// 2. 释放 guard
/// 3. `ctx.reply`（无锁）
async fn converge_and_reply(
    sessions: &DashMap<SessionId, Session>,
    session_id: SessionId,
    turn_id: u64,
    outcome: TurnOutcome,
    ctx: KernelContext,
) {
    // 1. 终态收敛（短暂持锁，无 await）
    let action = if let Some(mut session) = sessions.get_mut(&session_id) {
        session.finish_thinking(turn_id, outcome)
    } else {
        // Session 已被移除 — 无需状态变更
        FinishAction::Idle { response: None }
    };
    // guard 已 drop

    // 2. Reply（无锁，消费 ctx）
    let reply = match action {
        FinishAction::Idle {
            response: Some(resp),
        } => SessionReply::from_response(resp),
        FinishAction::Idle { response: None } => SessionReply::Error {
            message: "turn ended without success (error/timeout/cancelled/panic)".into(),
        },
        FinishAction::AwaitingCalls { response, .. } => {
            // Phase 1：无工具执行器 — 强制回 Idle，回传完整响应
            // 调用方可从 response.message.tool_calls 自行处理
            if let Some(mut session) = sessions.get_mut(&session_id) {
                session.state = SessionState::Idle;
            }
            SessionReply::from_response(response)
        }
    };

    if let Err(e) = ctx.reply(reply.to_envelope()) {
        warn!(error = ?e, "turn task reply failed");
    }
}

/// 获取 TurnOutcome 的标签字符串（用于 metrics）
fn outcome_label(outcome: &TurnOutcome) -> &'static str {
    match outcome {
        TurnOutcome::Success(_) => "success",
        TurnOutcome::Error(_) => "error",
        TurnOutcome::Cancelled => "cancelled",
        TurnOutcome::Timeout => "timeout",
        TurnOutcome::Panic(_) => "panic",
    }
}

/// 获取 SessionMessage 的类型标签（用于 tracing span）
fn message_kind_label(msg: &SessionMessage) -> &'static str {
    match msg {
        SessionMessage::Chat { .. } => "chat",
        SessionMessage::Interrupt { .. } => "interrupt",
        SessionMessage::ToolResult { .. } => "tool_result",
        SessionMessage::Resume { .. } => "resume",
        SessionMessage::SubagentDone { .. } => "subagent_done",
    }
}

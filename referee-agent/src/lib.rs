//! Referee Agent Runtime — 基于 referee-core 的可选 SDK 式扩展模块
//!
//! 独立于内核（不触碰 `referee-core`），按需启用。
//!
//! ## 当前范围
//! - **Phase 0**：[`provider`] 厂商抽象层（LLMProvider trait + MiMo/DeepSeek 适配器）
//! - **Phase 1**：[`session`] 会话状态机 + [`AgentRuntime`] 扩展（实现 `Extension` trait）
//! - **Phase 2**：[`tool`] 工具调用（Tool trait + Registry + Executor + 多轮循环）
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
//!                                     │  run_turn + converge  │
//!                                     └──────────┬─────────────┘
//!                                                │ emit(ToolResult)
//!                                                ▼
//!                                     ┌──────────────────────┐
//!                                     │  handle_tool_result   │
//!                                     │  → handle_resume      │
//!                                     │  → spawn_turn_task    │
//!                                     └──────────────────────┘
//! ```

pub mod provider;
pub mod session;
pub mod tool;
pub mod turn;

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use referee_core::extension::{CapabilityId, Extension, KernelContext};
use referee_core::Kernel;
use tokio::sync::oneshot;
use tracing::{info_span, warn, Instrument};

use provider::LLMProvider;
use session::{
    ChatPayload, SessionConfig, SessionId, SessionMessage, SessionReply, ToolCallAction,
};
use tool::{ToolExecutor, ToolRegistry};

// ─────────────────────────────────────────────
// AgentRuntime — Extension 实现
// ─────────────────────────────────────────────

/// Agent 运行时配置
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
            max_sessions: 100,
        }
    }
}

/// Agent Runtime — 一个 Extension 实例，管理 N 个 Session
///
/// 所有 Session 共存于扩展内的 `DashMap`，互不阻塞。
/// 长耗时 I/O 在派生任务中执行，`handle` 永远快速返回。
pub struct AgentRuntime {
    id: CapabilityId,
    kernel: Kernel,
    provider: Arc<dyn LLMProvider>,
    sessions: Arc<DashMap<SessionId, session::Session>>,
    config: AgentConfig,
    tools: Option<ToolRegistry>,
    tool_executor: Option<ToolExecutor>,
}

impl std::fmt::Debug for AgentRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentRuntime")
            .field("id", &self.id)
            .field("sessions", &self.sessions.len())
            .field("max_sessions", &self.config.max_sessions)
            .field("has_tools", &self.tools.is_some())
            .finish()
    }
}

impl AgentRuntime {
    /// 创建 Agent Runtime
    pub fn new(kernel: Kernel, provider: Arc<dyn LLMProvider>, config: AgentConfig) -> Self {
        Self {
            id: CapabilityId::new(),
            kernel,
            provider,
            sessions: Arc::new(DashMap::new()),
            config,
            tools: None,
            tool_executor: None,
        }
    }

    /// 启用工具调用（builder 模式）
    pub fn with_tools(mut self, registry: ToolRegistry, executor: ToolExecutor) -> Self {
        self.tools = Some(registry);
        self.tool_executor = Some(executor);
        self
    }

    /// 扩展的 CapabilityId
    pub fn id(&self) -> CapabilityId {
        self.id
    }

    /// 当前会话数
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// 获取或创建 Session（短暂持锁，无 await）
    fn get_or_create_session(
        &self,
        session_id: SessionId,
    ) -> Option<dashmap::mapref::one::RefMut<'_, SessionId, session::Session>> {
        if let Some(session) = self.sessions.get_mut(&session_id) {
            return Some(session);
        }
        if self.sessions.len() >= self.config.max_sessions {
            return None;
        }
        self.sessions
            .entry(session_id)
            .or_insert_with(|| session::Session::new(self.config.session.clone()));
        self.sessions.get_mut(&session_id)
    }

    // ── 消息处理 ──────────────────────────────

    /// 处理 Chat 消息 — 创建 forwarder + spawn turn task
    fn handle_chat(&self, ctx: KernelContext, session_id: SessionId, payload: ChatPayload) {
        let session_entry = match self.get_or_create_session(session_id) {
            Some(entry) => entry,
            None => {
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
                let turn_id = session.turn_id();
                drop(session);
                let _ = ctx.reply(SessionReply::Busy { turn_id }.to_envelope());
                return;
            }

            // 如果有 ToolRegistry，注入工具声明
            let mut options = payload.options;
            if let Some(registry) = &self.tools {
                if options.tools.is_empty() {
                    options.tools = registry.declarations();
                }
            }

            session.push_history(payload.message.clone());
            session.set_chat_options(options.clone());

            let (turn_id, cancel_rx) = match session.start_thinking() {
                Some(pair) => pair,
                None => {
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

            let req = session.build_chat_request(&options);
            let timeout = session.config().timeout.thinking_timeout;
            (turn_id, cancel_rx, req, timeout)
        };
        // guard dropped

        // 创建 forwarder：pending_reply (oneshot) → ctx.reply
        let (reply_tx, reply_rx) = oneshot::channel::<SessionReply>();
        if let Some(mut session) = self.sessions.get_mut(&session_id) {
            session.set_pending_reply(reply_tx);
        }

        tokio::spawn(async move {
            match reply_rx.await {
                Ok(reply) => {
                    let _ = ctx.reply(reply.to_envelope());
                }
                Err(_) => {
                    // pending_reply sender dropped without sending
                    // (session removed or forwarder timeout)
                }
            }
        });

        // spawn turn task
        let tctx = Arc::new(turn::TurnContext {
            sessions: self.sessions.clone(),
            provider: self.provider.clone(),
            kernel: self.kernel.clone(),
            self_id: self.id,
            tools: self.tools.clone(),
            tool_executor: self.tool_executor.clone(),
        });

        turn::spawn_turn_task(tctx, req, cancel_rx, session_id, turn_id, timeout);
    }

    /// 处理 Interrupt 消息
    fn handle_interrupt(&self, ctx: KernelContext, session_id: SessionId) {
        let reply = if let Some(mut session) = self.sessions.get_mut(&session_id) {
            // 先尝试取消 Thinking
            if session.cancel_thinking() {
                SessionReply::Cancelled
            } else if session.is_busy() {
                // AwaitingCalls — 强制 Idle + 回复错误给 pending_reply
                session.force_idle();
                if let Some(tx) = session.take_pending_reply() {
                    let _ = tx.send(SessionReply::Error {
                        message: "interrupted while awaiting tool results".into(),
                    });
                }
                SessionReply::Cancelled
            } else {
                SessionReply::Unhandled {
                    reason: "session not thinking or not found".into(),
                }
            }
        } else {
            SessionReply::Unhandled {
                reason: "session not thinking or not found".into(),
            }
        };

        let _ = ctx.reply(reply.to_envelope());
    }

    /// 处理 ToolResult 消息 — 更新 pending，清空时 emit Resume
    fn handle_tool_result(
        &self,
        ctx: KernelContext,
        session_id: SessionId,
        turn_id: u64,
        tool_call_id: String,
        result: String,
    ) {
        let action = if let Some(mut session) = self.sessions.get_mut(&session_id) {
            session.finish_tool_call(&tool_call_id, result)
        } else {
            let _ = ctx.reply(
                SessionReply::Unhandled {
                    reason: "session not found".into(),
                }
                .to_envelope(),
            );
            return;
        };

        match action {
            ToolCallAction::Pending => { /* 等待更多工具结果 */ }
            ToolCallAction::AllDone => {
                let kernel = self.kernel.clone();
                let self_id = self.id;
                tokio::spawn(async move {
                    let msg = SessionMessage::Resume {
                        session_id,
                        turn_id,
                    };
                    if let Err(e) = kernel.emit(self_id, msg.to_envelope()).await {
                        warn!(error = ?e, "failed to emit Resume");
                    }
                });
            }
            ToolCallAction::Ignored => { /* stale result, 丢弃 */ }
        }
        let _ = ctx;
    }

    /// 处理 Resume 消息 — 进入下一轮 Thinking
    fn handle_resume(&self, ctx: KernelContext, session_id: SessionId, _turn_id: u64) {
        let (turn_id, cancel_rx, req) = {
            let Some(mut session) = self.sessions.get_mut(&session_id) else {
                let _ = ctx.reply(
                    SessionReply::Unhandled {
                        reason: "session not found".into(),
                    }
                    .to_envelope(),
                );
                return;
            };
            match session.resume_thinking() {
                Some(triple) => triple,
                None => {
                    let _ = ctx.reply(
                        SessionReply::Unhandled {
                            reason: "not in AwaitingCalls state".into(),
                        }
                        .to_envelope(),
                    );
                    return;
                }
            }
        };

        let timeout = {
            if let Some(session) = self.sessions.get(&session_id) {
                session.config().timeout.thinking_timeout
            } else {
                Duration::from_secs(30)
            }
        };

        let tctx = Arc::new(turn::TurnContext {
            sessions: self.sessions.clone(),
            provider: self.provider.clone(),
            kernel: self.kernel.clone(),
            self_id: self.id,
            tools: self.tools.clone(),
            tool_executor: self.tool_executor.clone(),
        });

        turn::spawn_turn_task(tctx, req, cancel_rx, session_id, turn_id, timeout);
    }

    /// 处理未支持的消息类型
    fn handle_unhandled(&self, ctx: KernelContext, kind: &str) {
        let _ = ctx.reply(
            SessionReply::Unhandled {
                reason: format!("message kind '{kind}' not supported"),
            }
            .to_envelope(),
        );
    }
}

// ─────────────────────────────────────────────
// Extension trait 实现
// ─────────────────────────────────────────────

#[async_trait::async_trait]
impl Extension for AgentRuntime {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(
        &self,
        ctx: KernelContext,
        env: referee_core::Envelope,
    ) -> referee_core::KernelResult<()> {
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
            kind = message_kind_label(&msg),
            session_id = %msg.session_id()
        );

        async {
            match msg {
                SessionMessage::Chat {
                    session_id,
                    payload,
                } => self.handle_chat(ctx, session_id, payload),
                SessionMessage::Interrupt { session_id } => self.handle_interrupt(ctx, session_id),
                SessionMessage::ToolResult {
                    session_id,
                    turn_id,
                    tool_call_id,
                    result,
                } => self.handle_tool_result(ctx, session_id, turn_id, tool_call_id, result),
                SessionMessage::Resume {
                    session_id,
                    turn_id,
                } => self.handle_resume(ctx, session_id, turn_id),
                SessionMessage::SubagentDone { .. } => self.handle_unhandled(ctx, "subagent_done"),
            }
        }
        .instrument(span)
        .await;

        Ok(())
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

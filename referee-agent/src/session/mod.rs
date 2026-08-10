//! 会话状态机 — 并发正确性核心
//!
//! 本模块是 Phase 1 最关键的交付：修复上一版全部并发缺陷，建立
//! "永不幽灵、永不阻塞、可中断"的会话核心。
//!
//! ## 状态机
//! ```text
//!   ┌──────────────────────────────────────────────┐
//!   │                                              │
//!   ▼                                              │
//!  Idle ──Chat──▶ Thinking ──outcome──▶ Idle       │
//!   │                │                             │
//!   │           Interrupt                          │
//!   │           (cancel signal)                    │
//!   │                ▼                             │
//!   │           Thinking ──cancelled──▶ Idle       │
//!   │                                              │
//!   │  Idle ──Chat(with tools)──▶ Thinking         │
//!   │                                  │           │
//!   │                           tool_calls         │
//!   │                                  ▼           │
//!   │                           AwaitingCalls      │
//!   │                                  │           │
//!   │                           all done            │
//!   │                           (P2/P3 resume)     │
//!   └──────────────────────────────────┘           │
//! ```
//!
//! ## 设计约束（AGENT_RUNTIME_PLAN §2）
//! - **终态自管**（第 1 条）：派生任务 finally 唯一终态写入
//! - **协作式取消**（第 2 条）：`oneshot` 通道，不用 `abort()`
//! - **禁止跨 await 持 guard**（第 3 条）：`get_mut` 短暂持锁，释放后再 await
//! - **busy 拒绝显式可见**（第 4 条）：返回 `Busy` 回信，不静默 `Err`
//! - **handle 内零阻塞**（第 8 条）：handle 只做状态转移 + spawn

pub mod message;
pub mod task;
pub mod timeout;

// 便捷重导出
pub use message::{ChatOptions, ChatPayload, SessionId, SessionMessage, SessionReply};
pub use task::{run_turn, TurnOutcome};
pub use timeout::TimeoutConfig;

use std::collections::{HashMap, VecDeque};

use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::provider::{ChatRequest, ChatResponse, Message, ToolCall, ToolChoice};

/// 等待项类型（P2/P3 预留，Phase 1 不使用 AwaitingCalls）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingKind {
    /// 工具调用（P2）
    Tool,
    /// 子 Agent（P3）
    Subagent,
}

/// 会话状态机 — 统一工具与子 Agent 的等待态
///
/// `Thinking.cancel` 用 `Option` 包装：`cancel_thinking` 通过 `take()` 取出
/// 发送端发送取消信号，避免 `oneshot::Sender::send(self)` 需要所有权的冲突。
#[derive(Debug)]
pub enum SessionState {
    /// 空闲，可接受新 Chat
    Idle,
    /// 思考中（LLM 调用进行中）
    Thinking {
        turn_id: u64,
        /// 取消信号发送端（`take()` 后为 None，表示已发送取消）
        cancel: Option<oneshot::Sender<()>>,
    },
    /// 等待工具/子 Agent 完成（P2/P3）
    AwaitingCalls {
        turn_id: u64,
        pending: HashMap<String, PendingKind>,
    },
}

/// 终态收敛后的动作（由 AgentRuntime 执行）
#[derive(Debug)]
pub enum FinishAction {
    /// 回到 Idle，可选回复（Success 时含 ChatResponse）
    Idle { response: Option<ChatResponse> },
    /// 进入 AwaitingCalls（模型发起了工具调用，P2/P3 处理）
    AwaitingCalls {
        /// 完整响应（含 tool_calls，供 Phase 1 回传或 P2 执行后回传）
        response: ChatResponse,
        tool_calls: Vec<ToolCall>,
    },
}

/// 会话配置 — 每会话独立（从 AgentConfig 模板派生）
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// history 最大消息数（有界，FIFO 淘汰最旧）
    pub max_history: usize,
    /// 超时配置
    pub timeout: TimeoutConfig,
    /// 默认采样温度
    pub default_temperature: Option<f32>,
    /// 默认最大输出 token
    pub default_max_tokens: Option<usize>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_history: 50,
            timeout: TimeoutConfig::default(),
            default_temperature: None,
            default_max_tokens: None,
        }
    }
}

/// 会话 — 一个 Agent 实例，会话级隔离
///
/// 纯状态持有者，不含 I/O 句柄。状态转移由 [`Session`] 方法驱动，
/// I/O（LLM 调用、reply）由 `AgentRuntime` 在派生任务中执行。
pub struct Session {
    pub state: SessionState,
    history: VecDeque<Message>,
    turn_id: u64,
    config: SessionConfig,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("state", &self.state)
            .field("history_len", &self.history.len())
            .field("turn_id", &self.turn_id)
            .finish()
    }
}

impl Session {
    /// 创建新会话
    pub fn new(config: SessionConfig) -> Self {
        let cap = config.max_history.min(64); // 预分配上限 64，避免大预分配
        Self {
            state: SessionState::Idle,
            history: VecDeque::with_capacity(cap),
            turn_id: 0,
            config,
        }
    }

    /// 当前轮次 ID（单调递增）
    pub fn turn_id(&self) -> u64 {
        self.turn_id
    }

    /// 是否忙碌（Thinking 或 AwaitingCalls）
    pub fn is_busy(&self) -> bool {
        !matches!(self.state, SessionState::Idle)
    }

    /// 当前 history 长度
    pub fn history_len(&self) -> usize {
        self.history.len()
    }

    // ─────────────────────────────────────────────
    // 状态转移方法（全部 &mut self，无 async，无 await）
    // ─────────────────────────────────────────────

    /// Idle → Thinking：分配 turn_id + 创建取消通道
    ///
    /// 返回 `(turn_id, cancel_rx)`；若非 Idle 则返回 None（busy 拒绝）。
    pub fn start_thinking(&mut self) -> Option<(u64, oneshot::Receiver<()>)> {
        if self.is_busy() {
            return None;
        }
        self.turn_id += 1;
        let turn_id = self.turn_id;
        let (cancel_tx, cancel_rx) = oneshot::channel();
        self.state = SessionState::Thinking {
            turn_id,
            cancel: Some(cancel_tx),
        };
        Some((turn_id, cancel_rx))
    }

    /// 发送取消信号（不直接转 Idle，由 `finish_thinking` 收敛终态）
    ///
    /// 返回 true 表示成功发送（当前正在 Thinking 且未发送过取消）。
    pub fn cancel_thinking(&mut self) -> bool {
        if let SessionState::Thinking { cancel, .. } = &mut self.state {
            if let Some(tx) = cancel.take() {
                let _ = tx.send(());
                return true;
            }
        }
        false
    }

    /// Thinking → Idle/AwaitingCalls：终态收敛（finally 式，唯一终态写入）
    ///
    /// 成功时将 assistant 消息追加到 history。
    /// 返回 [`FinishAction`] 指示后续动作（reply / AwaitingCalls）。
    ///
    /// # 状态一致性
    /// 若当前不是 Thinking 状态（已被 Interrupt 强制取消或 turn_id 不匹配），
    /// 不做任何状态变更，返回 `Idle { response: None }`。
    pub fn finish_thinking(&mut self, expected_turn_id: u64, outcome: TurnOutcome) -> FinishAction {
        // 校验 turn_id：防止过期的 cancel/timeout 在 finish 之后重复收敛
        let current_turn = match &self.state {
            SessionState::Thinking { turn_id, .. } if *turn_id == expected_turn_id => *turn_id,
            _ => {
                // 状态已不匹配（可能已 Idle 或 turn_id 过期）— 不做任何变更
                warn!(
                    expected_turn_id,
                    current_state = ?self.state,
                    "finish_thinking: state mismatch, skipping convergence"
                );
                return FinishAction::Idle { response: None };
            }
        };

        let action = match outcome {
            TurnOutcome::Success(resp) => {
                let resp = *resp; // unbox ChatResponse
                let has_tool_calls = !resp.message.tool_calls.is_empty();
                self.push_history(resp.message.clone());
                if has_tool_calls {
                    let tool_calls = resp.message.tool_calls.clone();
                    FinishAction::AwaitingCalls {
                        response: resp,
                        tool_calls,
                    }
                } else {
                    FinishAction::Idle {
                        response: Some(resp),
                    }
                }
            }
            TurnOutcome::Error(e) => {
                warn!(turn_id = current_turn, error = ?e, "turn ended with error");
                FinishAction::Idle { response: None }
            }
            TurnOutcome::Cancelled => {
                info!(turn_id = current_turn, "turn cancelled by interrupt");
                FinishAction::Idle { response: None }
            }
            TurnOutcome::Timeout => {
                warn!(turn_id = current_turn, "turn timed out");
                FinishAction::Idle { response: None }
            }
            TurnOutcome::Panic(msg) => {
                warn!(turn_id = current_turn, panic_msg = %msg, "turn panicked");
                FinishAction::Idle { response: None }
            }
        };

        // 状态收敛
        self.state = match &action {
            FinishAction::AwaitingCalls { tool_calls, .. } => {
                let pending = tool_calls
                    .iter()
                    .map(|tc| (tc.id.clone(), PendingKind::Tool))
                    .collect();
                SessionState::AwaitingCalls {
                    turn_id: current_turn,
                    pending,
                }
            }
            FinishAction::Idle { .. } => SessionState::Idle,
        };

        action
    }

    // ─────────────────────────────────────────────
    // History 管理（有界）
    // ─────────────────────────────────────────────

    /// 追加消息到 history（有界，FIFO 淘汰最旧）
    pub fn push_history(&mut self, msg: Message) {
        if self.history.len() >= self.config.max_history {
            self.history.pop_front();
        }
        self.history.push_back(msg);
    }

    /// 构建 ChatRequest（从 history + 可选参数）
    ///
    /// 调用时机：`start_thinking` 之后（history 已含最新 user 消息）。
    pub fn build_chat_request(&self, options: &ChatOptions) -> ChatRequest {
        let messages: Vec<Message> = self.history.iter().cloned().collect();
        let tool_choice = if options.tools.is_empty() {
            ToolChoice::None
        } else {
            ToolChoice::Auto
        };
        ChatRequest {
            messages,
            tools: options.tools.clone(),
            tool_choice,
            temperature: options.temperature.or(self.config.default_temperature),
            max_tokens: options.max_tokens.or(self.config.default_max_tokens),
            thinking: options.thinking,
            extra: Default::default(),
        }
    }

    /// 会话配置引用
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{FinishReason, TokenUsage};

    fn mock_response(content: &str) -> ChatResponse {
        ChatResponse {
            id: "test".into(),
            model: "test".into(),
            message: Message::assistant(content),
            finish_reason: FinishReason::Stop,
            usage: Some(TokenUsage::default()),
        }
    }

    #[test]
    fn start_thinking_from_idle() {
        let mut session = Session::new(SessionConfig::default());
        assert!(!session.is_busy());
        let (turn_id, _rx) = session.start_thinking().expect("should start from Idle");
        assert_eq!(turn_id, 1);
        assert!(session.is_busy());
    }

    #[test]
    fn start_thinking_rejected_when_busy() {
        let mut session = Session::new(SessionConfig::default());
        let _ = session.start_thinking().expect("first start ok");
        assert!(
            session.start_thinking().is_none(),
            "should reject when busy"
        );
    }

    #[test]
    fn cancel_sends_signal() {
        let mut session = Session::new(SessionConfig::default());
        let (_, mut rx) = session.start_thinking().expect("start ok");
        assert!(session.cancel_thinking());
        // Receiver should get the signal
        match rx.try_recv() {
            Ok(()) => {}
            other => panic!("expected Ok(()), got {other:?}"),
        }
        // Second cancel should fail (already taken)
        assert!(!session.cancel_thinking());
    }

    #[test]
    fn finish_thinking_success_to_idle() {
        let mut session = Session::new(SessionConfig::default());
        let (turn_id, _rx) = session.start_thinking().expect("start ok");
        let action = session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(mock_response("hi"))));
        match action {
            FinishAction::Idle { response } => {
                assert!(response.is_some());
                assert_eq!(response.unwrap().message.content.as_text(), Some("hi"));
            }
            _ => panic!("expected Idle"),
        }
        assert!(!session.is_busy());
    }

    #[test]
    fn finish_thinking_with_tool_calls_to_awaiting() {
        let mut session = Session::new(SessionConfig::default());
        let (turn_id, _rx) = session.start_thinking().expect("start ok");

        let mut resp = mock_response("calling tool");
        resp.message.tool_calls = vec![ToolCall {
            id: "call_1".into(),
            function: crate::provider::ToolCallFunction {
                name: "get_weather".into(),
                arguments: "{}".into(),
            },
        }];
        resp.finish_reason = FinishReason::ToolCalls;

        let action = session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));
        match action {
            FinishAction::AwaitingCalls {
                response,
                tool_calls,
            } => {
                assert_eq!(tool_calls.len(), 1);
                assert_eq!(response.message.content.as_text(), Some("calling tool"));
            }
            _ => panic!("expected AwaitingCalls"),
        }
        assert!(session.is_busy());
    }

    #[test]
    fn finish_thinking_cancelled_to_idle() {
        let mut session = Session::new(SessionConfig::default());
        let (turn_id, _rx) = session.start_thinking().expect("start ok");
        let action = session.finish_thinking(turn_id, TurnOutcome::Cancelled);
        assert!(matches!(action, FinishAction::Idle { response: None }));
        assert!(!session.is_busy());
    }

    #[test]
    fn finish_thinking_stale_turn_id_ignored() {
        let mut session = Session::new(SessionConfig::default());
        let (_, _rx) = session.start_thinking().expect("start ok (turn 1)");
        // Simulate: turn 1 finishes, goes back to Idle
        let _ = session.finish_thinking(1, TurnOutcome::Cancelled);
        // Start turn 2
        let (turn2, _rx2) = session.start_thinking().expect("start ok (turn 2)");
        // Stale finish for turn 1 should be ignored
        let action = session.finish_thinking(1, TurnOutcome::Success(Box::new(mock_response("stale"))));
        assert!(matches!(action, FinishAction::Idle { response: None }));
        // Session should still be in turn 2's Thinking
        assert!(session.is_busy());
        assert_eq!(session.turn_id(), turn2);
    }

    #[test]
    fn history_is_bounded() {
        let mut session = Session::new(SessionConfig {
            max_history: 3,
            ..Default::default()
        });
        for i in 0..5 {
            session.push_history(Message::user(format!("msg {i}")));
        }
        assert_eq!(session.history_len(), 3);
        // Oldest should be evicted (msg 0, msg 1), keeping msg 2, 3, 4
        let req = session.build_chat_request(&ChatOptions::default());
        let msgs: Vec<&Message> = req.messages.iter().collect();
        assert_eq!(msgs[0].content.as_text(), Some("msg 2"));
        assert_eq!(msgs[2].content.as_text(), Some("msg 4"));
    }

    #[test]
    fn build_chat_request_includes_history() {
        let mut session = Session::new(SessionConfig::default());
        session.push_history(Message::user("hello"));
        session.push_history(Message::assistant("hi there"));

        let req = session.build_chat_request(&ChatOptions::default());
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].content.as_text(), Some("hello"));
        assert_eq!(req.messages[1].content.as_text(), Some("hi there"));
    }
}

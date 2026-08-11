//! 会话状态机 — 并发正确性核心
//!
//! 本模块是 Phase 1 最关键的交付：修复上一版全部并发缺陷，建立
//! "永不幽灵、永不阻塞、可中断"的会话核心。
//!
//! Phase 2 扩展：AwaitingCalls 状态新增 `tool_results` 收集，
//! `Session` 新增 `pending_reply` 与 `last_chat_options` 支持
//! 多轮工具调用循环。
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
//!   │                          finish_reason=      │
//!   │                          tool_calls          │
//!   │                                  ▼           │
//!   │                           AwaitingCalls      │
//!   │                              │     │         │
//!   │                    tool_result│     │timeout  │
//!   │                    (all done) │     │         │
//!   │                              ▼     ▼         │
//!   │                    Resume→Thinking  Idle      │
//!   └──────────────────────────────────────────────┘
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

use crate::budget::tokens_from_response;
use crate::provider::{ChatRequest, ChatResponse, Message, ToolCall};

/// 等待项类型（P2/P3 预留）
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
        /// 已收集的工具结果（tool_call_id → JSON content）
        tool_results: HashMap<String, String>,
    },
}

/// 终态收敛后的动作（由 AgentRuntime 执行）
#[derive(Debug)]
pub enum FinishAction {
    /// 回到 Idle，可选回复（Success 时含 ChatResponse）
    Idle { response: Option<ChatResponse> },
    /// 进入 AwaitingCalls（模型发起了工具调用）
    AwaitingCalls {
        /// 完整响应（供 Phase 1 回传或 P2 执行后回传）
        response: ChatResponse,
        tool_calls: Vec<ToolCall>,
    },
}

/// 工具结果回写后的动作（由 AgentRuntime 执行）
#[derive(Debug)]
pub enum ToolCallAction {
    /// 结果已记录，仍有 pending 项
    Pending,
    /// 全部完成，可以 resume
    AllDone,
    /// tool_call_id 不在 pending 中（stale 或 session 不在 AwaitingCalls）
    Ignored,
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
    /// 【P5 提示词】上下文 Token 预算上限（0 = 不截断，超限按优先级截断）
    pub prompt_budget_tokens: usize,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_history: 50,
            timeout: TimeoutConfig::default(),
            default_temperature: None,
            default_max_tokens: None,
            prompt_budget_tokens: 8000,
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
    /// Chat 调用方的回信通道（forwarder 模式）
    ///
    /// `handle_chat` 创建 oneshot channel，sender 存入此字段，
    /// receiver 存入 forwarder task 等待最终响应。
    /// 每个会话生命周期内最多 send 一次。
    pending_reply: Option<oneshot::Sender<SessionReply>>,
    /// 上一轮 Chat 选项（resume 时复用）
    last_chat_options: ChatOptions,
    /// 【预算治理】本会话已消耗 Token 数（finish_thinking 成功分支累加）
    consumed_tokens: u64,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("state", &self.state)
            .field("history_len", &self.history.len())
            .field("turn_id", &self.turn_id)
            .field("has_pending_reply", &self.pending_reply.is_some())
            .field("consumed_tokens", &self.consumed_tokens)
            .finish()
    }
}

impl Session {
    /// 创建新会话
    pub fn new(config: SessionConfig) -> Self {
        let cap = config.max_history.min(64);
        Self {
            state: SessionState::Idle,
            history: VecDeque::with_capacity(cap),
            turn_id: 0,
            config,
            pending_reply: None,
            last_chat_options: ChatOptions::default(),
            consumed_tokens: 0,
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

    /// 本会话已消耗 Token 数（预算治理计量）
    pub fn consumed_tokens(&self) -> u64 {
        self.consumed_tokens
    }

    // ─────────────────────────────────────────────
    // Reply 管理（forwarder 模式）
    // ─────────────────────────────────────────────

    /// 存入 pending_reply（handle_chat 入口调用）
    pub fn set_pending_reply(&mut self, tx: oneshot::Sender<SessionReply>) {
        if self.pending_reply.is_some() {
            warn!("overwriting existing pending_reply — previous caller will get dropped");
        }
        self.pending_reply = Some(tx);
    }

    /// 取出 pending_reply（终态收敛时调用，消费式 oneshot）
    pub fn take_pending_reply(&mut self) -> Option<oneshot::Sender<SessionReply>> {
        self.pending_reply.take()
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
    /// 若 AwaitingCalls 状态则强制转 Idle + 取出 pending_reply 回 Cancelled。
    pub fn cancel_thinking(&mut self) -> bool {
        if let SessionState::Thinking { cancel, .. } = &mut self.state {
            if let Some(tx) = cancel.take() {
                let _ = tx.send(());
                return true;
            }
        }
        false
    }

    /// 强制转 Idle（Interrupt 在 AwaitingCalls 时调用）
    ///
    /// 取出 pending_reply 供调用方回 Cancelled。
    pub fn force_idle(&mut self) {
        self.state = SessionState::Idle;
    }

    /// Thinking → Idle/AwaitingCalls：终态收敛（finally 式，唯一终态写入）
    ///
    /// 成功时将 assistant 消息追加到 history。
    /// 返回 [`FinishAction`] 指示后续动作（reply / AwaitingCalls）。
    pub fn finish_thinking(&mut self, expected_turn_id: u64, outcome: TurnOutcome) -> FinishAction {
        let current_turn = match &self.state {
            SessionState::Thinking { turn_id, .. } if *turn_id == expected_turn_id => *turn_id,
            _ => {
                warn!(
                    expected_turn_id,
                    current_state = ?self.state,
                    "finish_thinking: state mismatch, skipping convergence"
                );
                return FinishAction::Idle { response: None };
            }
        };

        // 缓存命中不计量（未发生真实 LLM 调用，不占预算）
        let from_cache = matches!(&outcome, TurnOutcome::Cached(_));

        let action = match outcome {
            TurnOutcome::Success(resp) | TurnOutcome::Cached(resp) => {
                let resp = *resp;
                // 【预算治理】累计本会话 Token 消耗（含 AwaitingCalls 分支）；
                // 缓存命中未发生真实 LLM 调用，不计量。
                if !from_cache {
                    self.consumed_tokens += tokens_from_response(&resp);
                }
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
                    tool_results: HashMap::new(),
                }
            }
            FinishAction::Idle { .. } => SessionState::Idle,
        };

        action
    }

    // ─────────────────────────────────────────────
    // 工具结果管理（Phase 2）
    // ─────────────────────────────────────────────

    /// 记录工具执行结果，更新 pending
    ///
    /// 若所有 pending 项已清空，返回 [`ToolCallAction::AllDone`]。
    pub fn finish_tool_call(&mut self, tool_call_id: &str, result: String) -> ToolCallAction {
        match &mut self.state {
            SessionState::AwaitingCalls {
                pending,
                tool_results,
                ..
            } => {
                if pending.remove(tool_call_id).is_some() {
                    tool_results.insert(tool_call_id.to_string(), result);
                    if pending.is_empty() {
                        ToolCallAction::AllDone
                    } else {
                        ToolCallAction::Pending
                    }
                } else {
                    ToolCallAction::Ignored
                }
            }
            _ => ToolCallAction::Ignored,
        }
    }

    /// AwaitingCalls → Idle → Thinking：resume 循环
    ///
    /// 1. 收集全部工具结果
    /// 2. 按 tool_call_id 排序，追加 Tool 角色消息到 history
    /// 3. 转 Idle → start_thinking → build request
    ///
    /// 返回 `(turn_id, cancel_rx, ChatRequest)` 或 None（不在 AwaitingCalls）。
    pub fn resume_thinking(&mut self) -> Option<(u64, oneshot::Receiver<()>, ChatRequest)> {
        // 收集 + 排序工具结果
        let results = if let SessionState::AwaitingCalls { tool_results, .. } = &mut self.state {
            let mut items: Vec<(String, String)> = tool_results.drain().collect();
            items.sort_by(|a, b| a.0.cmp(&b.0));
            items
        } else {
            return None;
        };

        // 追加 Tool 消息到 history
        for (tool_call_id, content) in &results {
            self.push_history(Message {
                role: crate::provider::Role::Tool,
                content: crate::provider::MessageContent::text(content.clone()),
                reasoning_content: None,
                tool_calls: Vec::new(),
                tool_call_id: Some(tool_call_id.clone()),
            });
        }

        // AwaitingCalls → Idle
        self.state = SessionState::Idle;

        // Idle → Thinking
        let (turn_id, cancel_rx) = self.start_thinking()?;

        // 构建 ChatRequest（复用上一轮 options）
        let req = self.build_chat_request(&self.last_chat_options.clone());

        Some((turn_id, cancel_rx, req))
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

    /// 构建 ChatRequest（从 history + 可选参数 + 预算截断）
    ///
    /// 调用时机：`start_thinking` 之后（history 已含最新 user 消息）。
    /// 经 [`crate::prompt::build_prompt`] 统一组装并按 `prompt_budget_tokens`
    /// 预算截断（P5：杜绝 Prompt 爆炸）。
    pub fn build_chat_request(&self, options: &ChatOptions) -> ChatRequest {
        let history: Vec<Message> = self.history.iter().cloned().collect();
        let temperature = options.temperature.or(self.config.default_temperature);
        let max_tokens = options.max_tokens.or(self.config.default_max_tokens);
        crate::prompt::build_prompt(
            None, // system：P5 预留（AgentConfig/SessionConfig 注入点）
            options.tools.clone(),
            history,
            Vec::new(), // memory：P4 注入
            Vec::new(), // artifacts：P4 白名单可见性注入
            temperature,
            max_tokens,
            options.thinking,
            self.config.prompt_budget_tokens,
        )
    }

    /// 保存 ChatOptions（供 resume 使用）
    pub fn set_chat_options(&mut self, options: ChatOptions) {
        self.last_chat_options = options;
    }

    /// 会话配置引用
    pub fn config(&self) -> &SessionConfig {
        &self.config
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

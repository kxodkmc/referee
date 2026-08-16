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

pub mod log;
pub mod message;
pub mod task;
pub mod timeout;

// 便捷重导出
pub use log::{LogError, SessionLog};
#[cfg(feature = "persist")]
pub use log::{PersistedSessionLog, SessionLogSink};
pub use message::{ChatOptions, ChatPayload, SessionId, SessionMessage, SessionReply};
pub use task::{run_turn, TurnOutcome};
pub use timeout::TimeoutConfig;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tracing::{error, info, warn};

use serde::{Deserialize, Serialize};

use crate::budget::tokens_from_response;
use crate::provider::{ChatRequest, ChatResponse, Message, ToolCall};

/// 等待项类型 — 当前仅工具调用；调用方二次封装时可自行扩展
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingKind {
    /// 工具调用
    Tool,
}

/// 会话状态机 — 统一等待态（当前为工具调用）
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
    /// 等待工具调用完成
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
#[derive(Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    /// history 最大消息数（有界，FIFO 淘汰最旧）
    pub max_history: usize,
    /// 会话事实日志上限（同时受 `max_history` 窗口约束，超限返回 `CapacityExceeded`）
    pub max_events: usize,
    /// 超时配置
    pub timeout: TimeoutConfig,
    /// 默认采样温度
    pub default_temperature: Option<f32>,
    /// 默认最大输出 token
    pub default_max_tokens: Option<usize>,
    /// 【P5 提示词】上下文 Token 预算上限（0 = 不截断，超限按优先级截断）
    pub prompt_budget_tokens: usize,
    /// 会话级默认系统提示词（Agent 角色设定 / 工具使用指导；ChatOptions 未指定时生效）
    pub default_system_prompt: Option<String>,
    /// 会话空闲超时（None = 永不超时，默认）。超时后由空闲回收任务移除 Idle 会话。
    pub idle_timeout: Option<Duration>,
    /// 【persist】可插拔会话事实落盘 sink（默认 None；运行时注入，不参与序列化）
    #[cfg(feature = "persist")]
    #[serde(skip)]
    pub log_sink: Option<Arc<dyn SessionLogSink>>,
}

impl std::fmt::Debug for SessionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SessionConfig");
        d.field("max_history", &self.max_history)
            .field("max_events", &self.max_events)
            .field("timeout", &self.timeout)
            .field("default_temperature", &self.default_temperature)
            .field("default_max_tokens", &self.default_max_tokens)
            .field("prompt_budget_tokens", &self.prompt_budget_tokens)
            .field("default_system_prompt", &self.default_system_prompt)
            .field("idle_timeout", &self.idle_timeout);
        #[cfg(feature = "persist")]
        d.field("has_log_sink", &self.log_sink.is_some());
        d.finish()
    }
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_history: 50,
            max_events: 4096,
            timeout: TimeoutConfig::default(),
            default_temperature: None,
            default_max_tokens: None,
            prompt_budget_tokens: 8000,
            default_system_prompt: None,
            idle_timeout: None,
            #[cfg(feature = "persist")]
            log_sink: None,
        }
    }
}

/// 一轮 `Chat` 的原子启动产物 — 由 [`Session::start_round`] 在单一 guard 内产出
#[derive(Debug)]
pub struct RoundStart {
    /// 本轮 turn_id（`finish_thinking` 校验用）
    pub turn_id: u64,
    /// 本轮取消通道（LLM 等待中 interrupt 触发）
    pub cancel_rx: oneshot::Receiver<()>,
    /// 已组装（含最新 user 消息 + 预算截断）的请求
    pub request: ChatRequest,
}

/// 会话 — 一个 Agent 实例，会话级隔离
///
/// 纯状态持有者，不含 I/O 句柄。状态转移由 [`Session`] 方法驱动，
/// I/O（LLM 调用、reply）由 `AgentRuntime` 在派生任务中执行。
pub struct Session {
    pub state: SessionState,
    /// 会话事实源（append-only，有界，超限返回 CapacityExceeded，绝不静默丢弃）
    log: SessionLog,
    /// 本会话标识（persist 落盘按会话分文件用；默认 nil，经 `with_session_id` 注入）
    session_id: SessionId,
    turn_id: u64,
    config: SessionConfig,
    /// 回合级取消标志（回合内持续有效，`start_round` 时重置）。
    /// 供「轮隙间」（工具执行中 / 思考间隙）的 interrupt 检查，
    /// 与 `Thinking.cancel` 通道互补：前者覆盖面内任意时点，后者即时打断 LLM 等待。
    interrupt: Arc<AtomicBool>,
    /// Chat 调用方的回信通道（forwarder 模式）
    ///
    /// `handle_chat` 创建 oneshot channel，sender 存入此字段，
    /// receiver 存入 forwarder task 等待最终响应。
    /// 每个会话生命周期内最多 send 一次。
    pending_reply: Option<oneshot::Sender<SessionReply>>,
    /// 本会话的子智能体嵌套深度（start_round 时从 ChatOptions 设置，回合内保持）
    peer_depth: u32,
    /// 上一轮 Chat 选项（resume 时复用）
    last_chat_options: ChatOptions,
    /// 【预算治理】本会话已消耗 Token 数（finish_thinking 成功分支累加）
    consumed_tokens: u64,
    /// 【异步工具】已完成但尚未注入的不等待工具结果队列
    ///
    /// 派发类（wait=false）工具后台完成后入队；在**下一次**模型调用/回合构建
    /// 请求时作为 user 消息合并注入 history。绝不为此主动触发 LLM 调用。
    /// 有界：超限丢弃最旧（背压硬约束，防结果堆积无界增长）。
    pending_injections: VecDeque<String>,
    /// 最近一次活动时间（`start_round` 与 `inject_tool_result` 时刷新），
    /// 供空闲回收任务判定 Idle 会话是否超时（4.2）。
    last_active: Instant,
}

/// 异步工具结果注入队列容量上限（超限丢最旧 + 告警）
const MAX_PENDING_INJECTIONS: usize = 64;

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("state", &self.state)
            .field("history_len", &self.history_len())
            .field("turn_id", &self.turn_id)
            .field("has_pending_reply", &self.pending_reply.is_some())
            .field("consumed_tokens", &self.consumed_tokens)
            .finish()
    }
}

impl Session {
    /// 创建新会话
    pub fn new(config: SessionConfig) -> Self {
        Self {
            state: SessionState::Idle,
            log: SessionLog::new(config.max_events),
            session_id: SessionId::nil(),
            turn_id: 0,
            config,
            interrupt: Arc::new(AtomicBool::new(false)),
            pending_reply: None,
            last_chat_options: ChatOptions::default(),
            consumed_tokens: 0,
            pending_injections: VecDeque::new(),
            peer_depth: 0,
            last_active: Instant::now(),
        }
    }

    /// 注入会话标识（persist 落盘按会话分文件用；引擎在创建时调用）
    pub fn with_session_id(mut self, id: SessionId) -> Self {
        self.session_id = id;
        self
    }

    /// 当前轮次 ID（单调递增）
    pub fn turn_id(&self) -> u64 {
        self.turn_id
    }

    /// 是否忙碌（Thinking 或 AwaitingCalls）
    pub fn is_busy(&self) -> bool {
        !matches!(self.state, SessionState::Idle)
    }

    /// 当前模型可见历史长度（`min(事实数, max_history)`，与窗口语义一致）
    pub fn history_len(&self) -> usize {
        self.log.tail(self.config.max_history).len()
    }

    /// 本会话的子智能体嵌套深度（0 = 主调用；经子 Agent 工具调用时递增）
    pub fn peer_depth(&self) -> u32 {
        self.peer_depth
    }

    /// 本会话已消耗 Token 数（预算治理计量）
    pub fn consumed_tokens(&self) -> u64 {
        self.consumed_tokens
    }

    /// 最近一次活动时间（空闲回收判定用）
    pub fn last_active(&self) -> Instant {
        self.last_active
    }

    /// 是否处于 Idle（无进行中回合）
    pub fn is_idle(&self) -> bool {
        !self.is_busy()
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
    /// 供工具多轮的 `resume_thinking` 复用；**不重置中断标志**。
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

    /// **原子启动一轮 Chat**：busy 检查 + turn_id + 取消通道 + history + options + request
    ///
    /// 在调用方持有的单一 `&mut self` guard 内完成，消除 `busy 检查` 与
    /// `start_thinking` 之间的 TOCTOU 窗口：并发第二个 `chat` 会因 `is_busy()`
    /// 立即返回 `None`，**不会污染 history、不会错乱取消标志**。
    ///
    /// 同时重置回合级中断标志（标志随新回合清零）。
    pub fn start_round(
        &mut self,
        message: Message,
        options: &ChatOptions,
        peer_depth: u32,
    ) -> Option<RoundStart> {
        if self.is_busy() {
            return None;
        }
        // 事实源写入放在状态变更之前：容量耗尽时拒绝本轮，不污染状态机
        if let Err(e) = self.push_history(message) {
            warn!(error = ?e, "start_round: session fact log full, round rejected");
            return None;
        }
        // 新回合：清零上一回合遗留的中断标志
        self.interrupt.store(false, Ordering::Relaxed);
        self.last_active = Instant::now();
        self.turn_id += 1;
        let turn_id = self.turn_id;
        let (cancel_tx, cancel_rx) = oneshot::channel();
        self.state = SessionState::Thinking {
            turn_id,
            cancel: Some(cancel_tx),
        };
        self.last_chat_options = options.clone();
        // 记录本轮子智能体嵌套深度（框架内部字段，回合内 resume 保持不变）
        self.peer_depth = peer_depth;
        // 合并注入：新回合构建请求前，把已完成的不等待工具结果追加为 user 消息
        self.flush_injections();
        let request = self.build_chat_request(options);
        Some(RoundStart {
            turn_id,
            cancel_rx,
            request,
        })
    }

    // ── 回合级中断（与取消通道互补，覆盖面内任意时点）──────────────

    /// 是否收到回合级中断（工具执行中 / 轮隙间检查）
    pub fn is_interrupted(&self) -> bool {
        self.interrupt.load(Ordering::Relaxed)
    }

    /// 置位回合级中断标志（由 [`crate::engine::Engine::interrupt`] 调用）
    pub fn raise_interrupt(&self) {
        self.interrupt.store(true, Ordering::Relaxed);
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
                if let Err(e) = self.push_history(resp.message.clone()) {
                    // 容量耗尽：助手消息仍经 FinishAction 回传调用方，仅记录错误而非静默覆盖
                    error!(error = ?e, turn_id = current_turn, "finish_thinking: session fact log full, assistant message not persisted");
                }
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

    /// 登记异步（不等待）工具完成结果 — 入队，等待下一次模型调用/回合时注入。
    /// 队列有界：超限丢弃最旧（背压硬约束），绝不无界增长。
    pub fn inject_tool_result(&mut self, content: String) {
        if content.is_empty() {
            return;
        }
        self.last_active = Instant::now();
        if self.pending_injections.len() >= MAX_PENDING_INJECTIONS {
            warn!(
                "async tool result queue full, dropping oldest ({} pending)",
                self.pending_injections.len()
            );
            self.pending_injections.pop_front();
        }
        self.pending_injections.push_back(content);
    }

    /// 把已完成的异步工具结果作为 user 消息追加到事实源（仅非空时）
    fn flush_injections(&mut self) {
        let pending: Vec<String> = self.pending_injections.drain(..).collect();
        for content in pending {
            if let Err(e) = self.push_history(Message::user(content)) {
                warn!(error = ?e, "flush_injections: session fact log full, injection rejected");
            }
        }
    }

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
    /// 1. 收集全部工具结果（含派发类占位）并追加 Tool 消息
    /// 2. 合并注入：已完成的不等待工具结果作为 user 消息追加
    /// 3. 转 Idle → start_thinking → build request
    ///
    /// 返回 `(turn_id, cancel_rx, ChatRequest)` 或 None（不在 AwaitingCalls）。
    pub fn resume_thinking(&mut self) -> Option<(u64, oneshot::Receiver<()>, ChatRequest)> {
        // 仅 AwaitingCalls 可 resume（保持原语义：非等待态返回 None）
        if !matches!(self.state, SessionState::AwaitingCalls { .. }) {
            return None;
        }
        // 1. 收集 + 追加工具结果（含派发占位）
        self.append_tool_results();

        // 2. 合并注入（已完成的不等待工具结果）
        self.flush_injections();

        // AwaitingCalls → Idle
        self.state = SessionState::Idle;

        // Idle → Thinking
        let (turn_id, cancel_rx) = self.start_thinking()?;

        // 构建 ChatRequest（复用上一轮 options）
        let req = self.build_chat_request(&self.last_chat_options.clone());

        Some((turn_id, cancel_rx, req))
    }

    /// 收集 AwaitingCalls 的工具结果（排序）+ 追加 Tool 消息到 history
    fn append_tool_results(&mut self) {
        let results = if let SessionState::AwaitingCalls { tool_results, .. } = &mut self.state {
            let mut items: Vec<(String, String)> = tool_results.drain().collect();
            items.sort_by(|a, b| a.0.cmp(&b.0));
            items
        } else {
            return;
        };

        for (tool_call_id, content) in &results {
            if let Err(e) = self.push_history(Message {
                role: crate::provider::Role::Tool,
                content: crate::provider::MessageContent::text(content.clone()),
                reasoning_content: None,
                tool_calls: Vec::new(),
                tool_call_id: Some(tool_call_id.clone()),
                usage: None,
            }) {
                warn!(error = ?e, tool_call_id, "append_tool_results: session fact log full, tool result rejected");
            }
        }
    }

    /// 纯派发轮收尾：把 AwaitingCalls 的占位工具结果追加为 Tool 消息并回到 Idle。
    ///
    /// 本轮全部为不等待工具（无等待项可 resume），回合就此结束；占位 Tool 消息
    /// 保证 assistant tool_calls 与 tool 结果配对（厂商协议硬约束）。
    /// 返回是否成功收敛（非 AwaitingCalls 返回 false）。
    pub fn settle_dispatched(&mut self) -> bool {
        if !matches!(self.state, SessionState::AwaitingCalls { .. }) {
            return false;
        }
        self.append_tool_results();
        self.state = SessionState::Idle;
        true
    }

    // ─────────────────────────────────────────────
    // 事实管理（append-only，有界）
    // ─────────────────────────────────────────────

    /// 追加一条事实到会话日志（事实只增不减；满则返回 `CapacityExceeded`，不静默丢弃）
    ///
    /// 配置了落盘 sink 时，内存写入成功后尽力落盘；落盘失败显式 `error!` 记录
    /// （不吞异常、不阻塞内存会话），内存事实源仍为权威。
    pub fn push_history(&mut self, msg: Message) -> Result<(), LogError> {
        self.log.append(msg.clone()).map(|_| ())?;
        #[cfg(feature = "persist")]
        if let Some(sink) = &self.config.log_sink {
            if let Err(e) = sink.append(&self.session_id, &msg) {
                error!(
                    error = ?e,
                    session_id = %self.session_id,
                    "push_history: persist sink append failed"
                );
            }
        }
        Ok(())
    }

    /// 构建 ChatRequest（从模型可见窗口 + 可选参数 + 预算截断）
    ///
    /// 调用时机：`start_thinking` 之后（窗口已含最新 user 消息）。
    /// 经 [`crate::prompt::build_prompt`] 统一组装并按 `prompt_budget_tokens`
    /// 预算截断（P5：杜绝 Prompt 爆炸）。
    pub fn build_chat_request(&self, options: &ChatOptions) -> ChatRequest {
        let history: Vec<Message> = self.log.tail(self.config.max_history).to_vec();
        let temperature = options.temperature.or(self.config.default_temperature);
        let max_tokens = options.max_tokens.or(self.config.default_max_tokens);
        let system = options
            .system_prompt
            .clone()
            .or_else(|| self.config.default_system_prompt.clone())
            .map(Message::system);
        crate::prompt::build_prompt(crate::prompt::PromptParts {
            system,
            tools: options.tools.clone(),
            history,
            memory: Vec::new(),
            artifacts: Vec::new(),
            temperature,
            max_tokens,
            thinking: options.thinking,
            prompt_budget: self.config.prompt_budget_tokens,
        })
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

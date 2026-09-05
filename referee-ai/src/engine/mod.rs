//! 会话引擎 — 最小闭环核心（地基）
//!
//! 将一次 `Chat` 抽象为**单个派生任务内的顺序异步闭环**：
//! `接 LLM → 组装 prompt → 调工具 → 管预算 → 回复`，多轮工具调用在同一任务内
//! 顺序收敛。相较跨消息回环（工具结果经内核消息再路由回 handle），本设计把
//! 一个会话回合的所有状态变化收敛到单一任务，**从结构上消除跨消息状态竞态**，
//! 是「永不幽灵、永不阻塞、可中断」的直接落地。
//!
//! ## 并发与中断
//! - 回合级取消：`AtomicBool` 标志 + `session.cancel_thinking()` 协作取消。
//!   中断在任意时点生效：LLM 等待中被即时打断，轮隙间被下次开头检查拦截。
//! - `select!` 管理每轮 `LLM / 取消 / 超时` 三路，配合 `catch_unwind` 兜底，
//!   任何路径都收敛为 [`EngineReply`]，绝不 panic 外泄、绝不挂死。
//! - 会话短暂持锁（无跨 await 持 guard）；并发 busy 拒绝显式可见。
//!
//! ## 独立性
//! 本引擎不依赖 `referee-core` 的 `Extension` 消息协议，仅通过 `Session` 状态机、
//! `LLMProvider`、`ToolExecutor`、`budget`、`cache` 组合而成；可被 referee-agent
//! 或任何调用方直接驱动。

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::budget::{add_tokens, tokens_from_response, BudgetConfig, BudgetError};
use crate::cache::{CacheConfig, InMemoryCache};
use crate::observe;
use crate::provider::{ChatResponse, LLMProvider, LlmError, StreamChunk, TokenUsage};
use crate::session::{
    ChatPayload, ErrorKind, FinishAction, RoundStart, Session, SessionConfig, SessionId,
    SessionReply, TurnOutcome,
};
use crate::tool::{ToolExecutor, ToolRegistry};

pub mod observer;
pub mod session_mgmt;
pub mod stream;
pub mod tool_round;

pub use observer::EngineObserver;
pub use session_mgmt::{ReaperHandle, SessionPhase, SessionSnapshot};
pub(crate) use tool_round::ToolRound;
use stream::StreamAccumulator;

/// 引擎配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// 会话级配置模板（每个新 Session 继承）
    pub session: SessionConfig,
    /// Token 预算（session_limit / global_limit，0 = 无限制）
    pub budget: BudgetConfig,
    /// 响应缓存（enabled=false 时完全禁用）
    pub cache: CacheConfig,
    /// 最大并发会话数（超限拒绝新会话）
    pub max_sessions: usize,
    /// 子智能体最大嵌套深度（0 = 主调用；达上限的会话无法再调用子 Agent）
    ///
    /// 例：`max_subagent_depth = 2` 允许 主A → 子B → 附属C（C 深度 2 达上限，
    /// 不能再调更深的子 Agent），B（深度 1）仍可调用。
    pub max_subagent_depth: u32,
    /// 引擎层重试次数（可恢复错误 Network/Server/RateLimited 触发，默认 1）
    pub max_retries: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            session: SessionConfig::default(),
            budget: BudgetConfig::default(),
            cache: CacheConfig::default(),
            max_sessions: 100,
            max_subagent_depth: 2,
            max_retries: 1,
        }
    }
}

/// 引擎错误 — 结构化错误类型，取代裸 `String`
///
/// 将引擎执行中可能遇到的错误归一为枚举变体，上层可按类型决策
///（如 `Llm` 中的 `RateLimited` 可回传 `retry_after`，`StateConflict`
/// 映射 HTTP 409 等），不再需要解析错误字符串。
#[derive(Debug, Clone, thiserror::Error)]
pub enum EngineError {
    /// LLM 调用错误（网络/限流/认证/协议等）— 透传最终 `LlmError`
    #[error("llm error: {0}")]
    Llm(#[from] LlmError),
    /// 会话状态冲突（如 resume 时非 AwaitingCalls 状态）
    #[error("state conflict: {0}")]
    StateConflict(&'static str),
    /// 回合异常结束（未产出响应、未取消、未超时，但无成功结果）
    #[error("turn ended without success")]
    TurnIncomplete,
    /// 内部通道关闭（派生任务意外终止）
    #[error("chat channel closed")]
    ChannelClosed,
    /// 单回合 LLM 轮数达上限（`max_rounds_per_chat`）；已发生的工具结果保留于 history
    #[error("max rounds per chat exceeded ({rounds})")]
    MaxRoundsExceeded { rounds: u32 },
}

/// 引擎回信 — `chat` 的执行产物
pub enum EngineReply {
    /// 正常完成（含缓存命中；缓存命中不计量 Token）
    Success(Box<ChatResponse>),
    /// 流式输出：调用方消费 chunk 流（含累积 Delta 与最终 Finish）
    Streaming(BoxStream<'static, Result<StreamChunk, LlmError>>),
    /// 会话忙碌：已有回合进行中，拒绝并发 Chat
    Busy { turn_id: u64 },
    /// 会话不存在 / 预算超限 / 回合异常
    Error(EngineError),
    /// 已取消（Interrupt 生效）
    Cancelled,
    /// 回合超时未完成
    Timeout,
}

impl std::fmt::Debug for EngineReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineReply::Success(r) => f.debug_tuple("Success").field(r).finish(),
            EngineReply::Streaming(_) => f.write_str("Streaming(..)"),
            EngineReply::Busy { turn_id } => {
                f.debug_struct("Busy").field("turn_id", turn_id).finish()
            }
            EngineReply::Error(e) => f.debug_tuple("Error").field(e).finish(),
            EngineReply::Cancelled => f.write_str("Cancelled"),
            EngineReply::Timeout => f.write_str("Timeout"),
        }
    }
}

/// 发起 Chat 的启动阶段错误（同步、可立即返回的错误）
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EngineStartError {
    #[error("max sessions reached")]
    MaxSessions,
    #[error("session busy")]
    Busy,
    #[error("budget limit reached: {0}")]
    Budget(#[from] BudgetError),
    #[error("request input exceeds model context window (estimated {estimated}, window {window})")]
    PromptTooLarge { estimated: u64, window: usize },
}

/// Chat 句柄 — 快速返回后获取结果 / 发起取消
#[derive(Clone)]
pub struct ChatHandle {
    engine: Engine,
    session_id: SessionId,
    rx: Arc<tokio::sync::Mutex<Option<oneshot::Receiver<EngineReply>>>>,
}

impl std::fmt::Debug for ChatHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatHandle")
            .field("session_id", &self.session_id)
            .finish()
    }
}

impl ChatHandle {
    /// 等待回合结果（一次）。无结果（receiver 已消费或断开）返回 `None`。
    pub async fn wait(&self) -> Option<EngineReply> {
        let mut guard = self.rx.lock().await;
        let rx = guard.take()?;
        rx.await.ok()
    }

    /// 请求取消本回合（幂等）
    pub fn cancel(&self) -> bool {
        self.engine.interrupt(self.session_id)
    }
}

/// 会话引擎 — 管理 N 个 `Session`，驱动最小闭环
///
/// 所有字段为 `Arc`，`Clone` 后可在多 task 间共享、注册应使用同一实例。
#[derive(Clone)]
pub struct Engine {
    pub(crate) sessions: Arc<DashMap<SessionId, Session>>,
    pub(crate) provider: Arc<dyn LLMProvider>,
    pub(crate) config: EngineConfig,
    tools: Option<ToolRegistry>,
    tool_executor: Option<ToolExecutor>,
    /// 引擎观测器（行为句柄，builder 注入，不进 EngineConfig——行为不进配置数据）
    pub(crate) observer: Option<Arc<dyn EngineObserver>>,
    pub(crate) total_consumed_tokens: Arc<AtomicU64>,
    cache: Option<Arc<InMemoryCache>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("sessions", &self.sessions.len())
            .field("max_sessions", &self.config.max_sessions)
            .field("has_tools", &self.tools.is_some())
            .field("has_observer", &self.observer.is_some())
            .field("total_consumed_tokens", &self.total_consumed_tokens())
            .field("cache_entries", &self.cache_len())
            .finish()
    }
}

impl Engine {
    /// 创建引擎
    pub fn new(provider: Arc<dyn LLMProvider>, config: EngineConfig) -> Self {
        let cache = if config.cache.enabled {
            Some(Arc::new(InMemoryCache::new(
                config.cache.capacity,
                config.cache.ttl,
            )))
        } else {
            None
        };
        Self {
            sessions: Arc::new(DashMap::new()),
            provider,
            config,
            tools: None,
            tool_executor: None,
            observer: None,
            total_consumed_tokens: Arc::new(AtomicU64::new(0)),
            cache,
        }
    }

    /// 启用工具能力（builder）
    pub fn with_tools(mut self, registry: ToolRegistry, executor: ToolExecutor) -> Self {
        self.tools = Some(registry);
        self.tool_executor = Some(executor);
        self
    }

    /// 注入引擎观测器（builder，与 [`with_tools`](Self::with_tools) 对称）
    ///
    /// 注入后：非流式路径在厂商支持流式时改走「内部流式收敛」以产生 delta 事件；
    /// 未注入或厂商 `streaming=false` 时保持 `provider.chat()` 直调（零行为回归）。
    pub fn with_observer(mut self, observer: Arc<dyn EngineObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// 向已启用的工具注册表添加工具（供上层注册对等/自定义工具）
    pub fn register_tool(
        &self,
        tool: std::sync::Arc<dyn crate::tool::Tool>,
    ) -> Result<(), crate::tool::RegistryError> {
        self.tools
            .as_ref()
            .ok_or(crate::tool::RegistryError::NotEnabled)?
            .register(tool)
    }

    /// 已启用的工具注册表（供上层枚举/清理，如停机时回收外部工具资源）
    pub fn tools(&self) -> Option<&ToolRegistry> {
        self.tools.as_ref()
    }

    /// 注入共享全局 Token 计数器（多引擎合并系统级总预算）
    pub fn with_global_budget(mut self, counter: Arc<AtomicU64>) -> Self {
        self.total_consumed_tokens = counter;
        self
    }

    // ── 观测方法 ──────────────────────────────

    /// 当前会话数
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// 全局已消耗 Token 数
    pub fn total_consumed_tokens(&self) -> u64 {
        self.total_consumed_tokens.load(Ordering::Relaxed)
    }

    /// 指定会话已消耗 Token 数
    pub fn session_consumed_tokens(&self, session_id: SessionId) -> Option<u64> {
        self.sessions.get(&session_id).map(|s| s.consumed_tokens())
    }

    /// 指定会话的 history 消息数（观测 / 测试用）
    pub fn history_len(&self, session_id: SessionId) -> Option<usize> {
        self.sessions.get(&session_id).map(|s| s.history_len())
    }

    /// 当前缓存条目数（未启用时为 0）
    pub fn cache_len(&self) -> usize {
        self.cache.as_ref().map(|c| c.len()).unwrap_or(0)
    }

    // ── 会话生命周期（转发至 session_mgmt）────────────────

    /// 移除指定会话，返回是否确有会话被移除
    pub fn remove_session(&self, session_id: SessionId) -> bool {
        session_mgmt::remove_session(&self.sessions, session_id)
    }

    /// 枚举全部会话 ID
    pub fn list_sessions(&self) -> Vec<SessionId> {
        session_mgmt::list_sessions(&self.sessions)
    }

    /// 恢复会话历史 — 崩溃恢复入口
    ///
    /// 接受 `(session_id, Vec<Message>)`，忠实重建上下文（仅追加，不调 LLM）。
    /// 自动创建会话（受 `max_sessions` 有界约束）；逐条 `push_history`，
    /// 返回成功追加条数。容量满时停止，保留已恢复前缀。
    pub fn restore_session_history(
        &self,
        session_id: SessionId,
        messages: Vec<crate::provider::Message>,
    ) -> Result<usize, EngineError> {
        if !self.ensure_session(session_id) {
            return Err(EngineError::StateConflict("max sessions reached"));
        }
        let mut n = 0usize;
        if let Some(mut s) = self.sessions.get_mut(&session_id) {
            for m in messages {
                match s.push_history(m) {
                    Ok(()) => n += 1,
                    Err(_) => break, // 容量满：停止，保留已恢复前缀
                }
            }
        }
        Ok(n)
    }

    /// 回放会话历史（向后兼容别名，错误映射为 `String`）
    #[deprecated(note = "use `restore_session_history` instead")]
    pub fn replay_history(
        &self,
        session_id: SessionId,
        messages: Vec<crate::provider::Message>,
    ) -> Result<usize, String> {
        self.restore_session_history(session_id, messages)
            .map_err(|e| e.to_string())
    }

    /// 查询单个会话的运行快照（不存在返回 None）
    pub fn session_info(&self, session_id: SessionId) -> Option<SessionSnapshot> {
        self.sessions
            .get(&session_id)
            .map(|s| session_mgmt::snapshot(&s))
    }

    /// 启动空闲回收任务；`idle_timeout` 未配置（None）时返回 None
    pub fn start_idle_reaper(&self) -> Option<ReaperHandle> {
        self.config
            .session
            .idle_timeout
            .map(|t| session_mgmt::start_idle_reaper(self.sessions.clone(), t))
    }

    // ── 主入口 ────────────────────────────────

    /// 发起一轮 Chat，返回句柄（快速返回，实际执行在派生任务中）
    ///
    /// 同步段原子完成：会话创建 / busy 拒绝（含 `start_thinking`）/ 预算守门 /
    /// history 写入 / request 组装，不进行任何 await。**并发第二个 `chat`（同 session）
    /// 会在 `start_round` 的单一 guard 内被 `Busy` 拒绝，不会再污染 history 或
    /// 错乱取消标志（TOCTOU 修复）。**
    pub fn chat(
        &self,
        session_id: SessionId,
        payload: ChatPayload,
    ) -> Result<ChatHandle, EngineStartError> {
        let round = self.prepare_round(session_id, payload)?;

        let engine = self.clone();
        let (reply_tx, reply_rx) = oneshot::channel();
        tokio::spawn(async move {
            let reply = engine.run_chat(&session_id, round).await;
            let _ = reply_tx.send(reply);
        });

        Ok(ChatHandle {
            engine: self.clone(),
            session_id,
            rx: Arc::new(tokio::sync::Mutex::new(Some(reply_rx))),
        })
    }

    /// 流式发起一轮 Chat：返回句柄，`wait()` 得到 [`EngineReply::Streaming`]
    ///
    /// 同步段与 [`chat`](Self::chat) 完全一致；执行段改用流式，chunk 逐条透传，
    /// 引擎内部累积收敛为 `ChatResponse` 后走与非流式相同的 `finish_thinking` 路径。
    pub fn chat_stream(
        &self,
        session_id: SessionId,
        payload: ChatPayload,
    ) -> Result<ChatHandle, EngineStartError> {
        let round = self.prepare_round(session_id, payload)?;

        let engine = self.clone();
        let (reply_tx, reply_rx) = oneshot::channel();
        tokio::spawn(async move {
            let reply = engine.run_stream(&session_id, round).await;
            let _ = reply_tx.send(reply);
        });

        Ok(ChatHandle {
            engine: self.clone(),
            session_id,
            rx: Arc::new(tokio::sync::Mutex::new(Some(reply_rx))),
        })
    }

    /// 同步段：会话创建 / 预算守门 / 工具声明注入 / 原子启动回合
    ///
    /// 不进行任何 await；并发第二个 `chat`（同 session）在 `start_round` 的单一
    /// guard 内被 `Busy` 拒绝，不污染 history 或错乱取消标志。
    fn prepare_round(
        &self,
        session_id: SessionId,
        payload: ChatPayload,
    ) -> Result<RoundStart, EngineStartError> {
        if !self.ensure_session(session_id) {
            return Err(EngineStartError::MaxSessions);
        }
        if let Err(e) = self.check_budget(&session_id) {
            return Err(EngineStartError::Budget(e));
        }
        let mut options = payload.options;
        let peer_depth = payload.peer_depth;
        if let Some(registry) = &self.tools {
            if options.tools.is_empty() {
                options.tools =
                    registry.declarations_for_depth(peer_depth, self.config.max_subagent_depth);
            }
        }
        // context 硬护栏：核心载荷（system + 当前轮输入）须放得进模型窗口；
        // 放不进就立即 fail-loud —— 绝不带着注定超窗的载荷去调用 provider。
        // 注意核对口径：此处估算的是「恒定交付的核心载荷」，与组装层中被裁减的
        // 可裁上下文无关（后者由 prompt_budget 收紧 + WARN/metrics 观察）。
        let window = self.provider.model_spec().context_window_tokens;
        {
            let session = self.sessions.get(&session_id);
            let system = options
                .system_prompt
                .clone()
                .or_else(|| session.and_then(|s| s.config().default_system_prompt.clone()));
            let estimated = system
                .as_deref()
                .map(crate::budget::TokenEstimator::estimate)
                .unwrap_or(0)
                + crate::budget::TokenEstimator::estimate(
                    payload.message.content.as_text().unwrap_or(""),
                );
            if estimated >= window as u64 {
                return Err(EngineStartError::PromptTooLarge {
                    estimated,
                    window,
                });
            }
        }
        self.sessions
            .get_mut(&session_id)
            .and_then(|mut s| s.start_round(payload.message, &options, peer_depth))
            .ok_or(EngineStartError::Busy)
    }

    /// 中断一个会话的当前回合（幂等）
    ///
    /// 任一时点生效：LLM 等待中即时打断；轮隙间（工具执行 / 思考间隙）由
    /// `Session` 内的回合级中断标志逐轮检查拦截。
    ///
    /// 返回 `true` 表示**确实对活动回合发出取消**；会话为空或空闲（Idle，无活动回合）
    /// 返回 `false`。不再存在「已在取消中 vs 首次触发」被混淆的情况（M1 修复）。
    pub fn interrupt(&self, session_id: SessionId) -> bool {
        let mut s = match self.sessions.get_mut(&session_id) {
            Some(s) => s,
            None => return false,
        };
        // 仅活动回合可取消；Idle 无回合可取消
        if !s.is_busy() {
            return false;
        }
        // 置位回合级标志（轮隙间检查）+ 触发 LLM 等待中的取消通道
        s.raise_interrupt();
        s.cancel_thinking();
        true
    }

    // ── 内部 ──────────────────────────────────

    /// 创建或获取会话；超限返回 false
    ///
    /// 用 `or_insert_with`（内部走 dashmap 的 insert 路径），避免裸 `entry()`
    /// match 触发的 shrink 死锁（dashmap entry/shrink 竞态）。
    fn ensure_session(&self, session_id: SessionId) -> bool {
        if self.sessions.contains_key(&session_id) {
            return true;
        }
        // 软边界：并发创建多个新会话时可能最多多建一个（可接受的近似）
        if self.sessions.len() >= self.config.max_sessions {
            return false;
        }
        self.sessions
            .entry(session_id)
            .or_insert_with(|| Session::new(self.config.session.clone()).with_session_id(session_id));
        true
    }

    /// 预算检查（返回 Err 表示超限）
    fn check_budget(&self, session_id: &SessionId) -> Result<(), BudgetError> {
        let cfg = self.config.budget;
        if cfg.session_limit > 0 {
            let used = self
                .sessions
                .get(session_id)
                .map(|s| s.consumed_tokens())
                .unwrap_or(0);
            if used >= cfg.session_limit {
                return Err(BudgetError::SessionExceeded {
                    used,
                    limit: cfg.session_limit,
                });
            }
        }
        if cfg.global_limit > 0 {
            let used = self.total_consumed_tokens.load(Ordering::Relaxed);
            if used >= cfg.global_limit {
                return Err(BudgetError::GlobalExceeded {
                    used,
                    limit: cfg.global_limit,
                });
            }
        }
        Ok(())
    }

    /// 执行完整回合（顺序异步闭环，含多轮工具循环）
    async fn run_chat(&self, session_id: &SessionId, first: RoundStart) -> EngineReply {
        let span = observe::turn_span(session_id, first.turn_id);
        let timer = observe::Timer::start();
        let outcome = self
            .run_chat_inner(session_id, first)
            .instrument(span)
            .await;
        observe::record_turn_duration(outcome_label(&outcome), timer.finish());
        observe::turn_completed(outcome_label(&outcome));
        outcome
    }

    async fn run_chat_inner(&self, session_id: &SessionId, first: RoundStart) -> EngineReply {
        let timeout = self.config.session.timeout.thinking_timeout;
        // RoundSource::First 仅在首迭代由 chat() 原子启动；此后经 resume 恢复。
        // 每迭代用 mem::replace 统一取本轮输入（fresh owned），规避跨迭代 move 累积。
        let mut src = RoundSource::First(first);
        // 本回合已发起的 LLM 轮数（含首轮与全部工具中间轮；重试不另计）
        let mut rounds_used: u32 = 0;
        // 本回合各轮 usage 之和（terminal 收敛时作为返回响应的 usage）
        let mut turn_usage_sum: Option<TokenUsage> = None;

        loop {
            let cur = std::mem::replace(&mut src, RoundSource::Resume);
            let (turn_id, mut cancel_rx, request) = match cur {
                RoundSource::First(f) => (f.turn_id, f.cancel_rx, f.request),
                RoundSource::Resume => {
                    if self.round_limit_reached(rounds_used) {
                        if let Some(mut s) = self.sessions.get_mut(session_id) {
                            s.settle_tool_results();
                        }
                        return EngineReply::Error(EngineError::MaxRoundsExceeded {
                            rounds: rounds_used,
                        });
                    }
                    match self
                        .sessions
                        .get_mut(session_id)
                        .and_then(|mut s| s.resume_thinking())
                    {
                        Some(x) => x,
                        None => return EngineReply::Error(EngineError::StateConflict("resume failed (not awaiting)")),
                    }
                }
            };
            rounds_used += 1;

            // 回合级中断（轮隙间：工具执行后 / 思考间隙）
            if self.is_interrupted(session_id) {
                return EngineReply::Cancelled;
            }
            self.observe_event(|o| o.on_turn_started(*session_id, turn_id));

            // 缓存命中检查（不调 LLM；catch_unwind 兜底降级为真实调用）
            let cache_key = self
                .cache
                .as_ref()
                .map(|c| c.key_for_request(&request, self.provider.id().as_str()));
            let cached = self
                .cache
                .as_ref()
                .zip(cache_key.as_ref())
                .and_then(|(c, key)| {
                    std::panic::catch_unwind(AssertUnwindSafe(|| c.get(key)))
                        .ok()
                        .flatten()
                });
            observe::cache_access(cached.is_some());

            // 执行 LLM（命中缓存则跳过）；引擎层重试 + 三路 select + catch_unwind
            let outcome = if let Some(resp) = cached {
                TurnOutcome::Cached(Box::new(resp))
            } else {
                self.llm_call_with_retry(session_id, request, &mut cancel_rx, timeout)
                    .await
            };

            // 缓存写入：真实调用成功且无工具调用才可缓存
            if let (Some(cache), Some(key)) = (&self.cache, &cache_key) {
                if let TurnOutcome::Success(resp) = &outcome {
                    if resp.message.tool_calls.is_empty() {
                        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                            cache.set(key.clone(), (**resp).clone());
                        }));
                    }
                }
            }

            // 全局 Token 累计：**每轮真实成功**都计入（含 AwaitingCalls 中间轮），
            // 与 session 级口径一致；缓存命中不计量。
            if let TurnOutcome::Success(resp) = &outcome {
                add_tokens(&self.total_consumed_tokens, tokens_from_response(resp));
            }

            // 记录是否超时（outcome 即将被收敛消费）
            let timed_out = matches!(&outcome, TurnOutcome::Timeout);
            // 捕获 LLM 错误（finish_thinking 会消费 outcome 但不回传错误）
            let llm_error = match &outcome {
                TurnOutcome::Error(e) => Some(e.clone()),
                _ => None,
            };

            // 终态收敛（session 内累加 consumed_tokens；guard 短暂持有后立即释放）
            let turn_usage = match &outcome {
                TurnOutcome::Success(resp) | TurnOutcome::Cached(resp) => resp.usage.clone(),
                _ => None,
            };
            match (&mut turn_usage_sum, &turn_usage) {
                (Some(sum), Some(u)) => sum.merge(u),
                (None, Some(u)) => turn_usage_sum = Some(u.clone()),
                _ => {}
            }
            let action = self
                .sessions
                .get_mut(session_id)
                .map(|mut s| s.finish_thinking(turn_id, outcome))
                .unwrap_or(FinishAction::Idle { response: None });
            self.observe_event(|o| {
                o.on_turn_finished(*session_id, turn_id, turn_usage.clone());
            });

            match action {
                FinishAction::Idle {
                    response: Some(resp),
                } => {
                    return EngineReply::Success(Box::new(resp));
                }
                FinishAction::Idle { response: None } => {
                    // 取消 / 超时 / 错误 / panic
                    if self.is_interrupted(session_id) {
                        return EngineReply::Cancelled;
                    }
                    // M2：超时独立归类，不与普通错误混淆
                    if timed_out {
                        return EngineReply::Timeout;
                    }
                    // LLM 错误透传（结构化，不再丢失）
                    if let Some(e) = llm_error {
                        return EngineReply::Error(EngineError::Llm(e));
                    }
                    return EngineReply::Error(EngineError::TurnIncomplete);
                }
                FinishAction::AwaitingCalls {
                    response,
                    tool_calls,
                } => {
                    if self.tool_executor.is_none() || self.tools.is_none() {
                        // M3：无工具能力但模型发起了工具调用 → 返回模型原文，不吞正文
                        if let Some(mut s) = self.sessions.get_mut(session_id) {
                            s.force_idle();
                        }
                        return EngineReply::Success(Box::new(response));
                    }
                    // 工具轮：截断 → 按 wait 分流 → 等待类同步 / 派发类后台注入
                    match self.run_tool_calls(session_id, turn_id, tool_calls).await {
                        // 有待等待的工具 → 循环继续恢复（src 已在迭代顶部置为 Resume）
                        ToolRound::Resume => {}
                        // terminal 收敛（aggregate_usage=true，usage 取各轮之和）
                        // 或纯派发轮（全部不等待）→ 回合就此结束，返回模型原文
                        ToolRound::Settled { aggregate_usage } => {
                            let mut response = response;
                            if aggregate_usage {
                                response.usage = turn_usage_sum;
                            }
                            return EngineReply::Success(Box::new(response));
                        }
                    }
                }
            }
        }
    }

    /// 引擎层 LLM 调用 + 可恢复错误重试
    ///
    /// `Cancelled` / `Timeout` / `Panic` 与不可恢复错误不重试；每次重试前检查
    /// 中断，每次重试发 `tracing::warn!` + metrics 计数。
    async fn llm_call_with_retry(
        &self,
        session_id: &SessionId,
        request: crate::provider::ChatRequest,
        cancel_rx: &mut oneshot::Receiver<()>,
        timeout: Duration,
    ) -> TurnOutcome {
        let max_retries = self.config.max_retries;
        let mut attempt = 0u32;
        loop {
            if self.is_interrupted(session_id) {
                return TurnOutcome::Cancelled;
            }
            let outcome = self
                .single_llm_call(session_id, request.clone(), cancel_rx, timeout)
                .await;
            match outcome {
                TurnOutcome::Error(e) if e.is_retryable() && attempt < max_retries => {
                    attempt += 1;
                    tracing::warn!(error = %e, attempt, "llm call failed, retrying");
                    observe::llm_retry();
                    // 重试前必须退避：provider 层已做整条指数退避链，引擎补的这一轮
                    // 若零间隔立即再发，限流场景会雪崩放大（AI-5 修复）。
                    // 优先尊重 provider 透传的 Retry-After，否则指数退避，均封顶防挂起。
                    let wait = match &e {
                        LlmError::RateLimited {
                            retry_after: Some(ra),
                        } if !ra.is_zero() => (*ra).min(ENGINE_RETRY_MAX_WAIT),
                        _ => ENGINE_RETRY_BASE_WAIT
                            .saturating_mul(1u32 << attempt.min(5))
                            .min(ENGINE_RETRY_MAX_WAIT),
                    };
                    tokio::time::sleep(wait).await;
                }
                other => return other,
            }
        }
    }

    /// 单次 LLM 调用：三路 select（LLM / 取消 / 超时）+ catch_unwind
    ///
    /// 注入 observer 且厂商支持流式时走「内部流式收敛」（delta 逐条回调，
    /// 对外仍返回完整 `ChatResponse`，两条路径在此统一）；否则保持
    /// `provider.chat()` 直调——不给无需可观测性的场景引入流式端点回归。
    async fn single_llm_call(
        &self,
        session_id: &SessionId,
        request: crate::provider::ChatRequest,
        cancel_rx: &mut oneshot::Receiver<()>,
        timeout: Duration,
    ) -> TurnOutcome {
        let internal_stream = self.observer.is_some() && self.provider.capabilities().streaming;
        let result = AssertUnwindSafe(async {
            if internal_stream {
                self.internal_stream_call(session_id, request, cancel_rx, timeout)
                    .await
            } else {
                tokio::select! {
                    res = self.provider.chat(request) => match res {
                        Ok(r) => TurnOutcome::Success(Box::new(r)),
                        Err(e) => TurnOutcome::Error(e),
                    },
                    _ = &mut *cancel_rx => TurnOutcome::Cancelled,
                    _ = tokio::time::sleep(timeout) => TurnOutcome::Timeout,
                }
            }
        })
        .catch_unwind()
        .await;
        match result {
            Ok(outcome) => outcome,
            Err(payload) => TurnOutcome::Panic(panic_message(payload)),
        }
    }

    /// 内部流式收敛：消费 `chat_stream` 累积为完整 `ChatResponse`（对外仍非流式），
    /// delta 经 [`Engine::observe_chunk_deltas`] 逐条回调；取消/超时语义与非流式一致
    async fn internal_stream_call(
        &self,
        session_id: &SessionId,
        request: crate::provider::ChatRequest,
        cancel_rx: &mut oneshot::Receiver<()>,
        timeout: Duration,
    ) -> TurnOutcome {
        let sid = *session_id;
        tokio::select! {
            outcome = async {
                let mut stream = match self.provider.chat_stream(request).await {
                    Ok(s) => s,
                    Err(e) => return TurnOutcome::Error(e),
                };
                let mut acc = StreamAccumulator::new();
                while let Some(chunk) = stream.next().await {
                    match chunk {
                        Ok(c) => {
                            self.observe_chunk_deltas(&sid, &c);
                            acc.push(c);
                        }
                        Err(e) => return TurnOutcome::Error(e),
                    }
                }
                match acc.finish() {
                    Some((message, finish_reason, usage)) => {
                        let id = self.provider.id().to_string();
                        TurnOutcome::Success(Box::new(ChatResponse {
                            id: id.clone(),
                            model: id,
                            message,
                            finish_reason,
                            usage,
                        }))
                    }
                    None => TurnOutcome::Error(LlmError::Protocol(
                        "stream ended without finish chunk".into(),
                    )),
                }
            } => outcome,
            _ = &mut *cancel_rx => TurnOutcome::Cancelled,
            _ = tokio::time::sleep(timeout) => TurnOutcome::Timeout,
        }
    }

    /// 会话是否收到回合级中断
    pub(crate) fn is_interrupted(&self, session_id: &SessionId) -> bool {
        self.sessions
            .get(session_id)
            .map(|s| s.is_interrupted())
            .unwrap_or(false)
    }

    /// 回合轮数是否已达上限（None = 不限）
    fn round_limit_reached(&self, rounds_used: u32) -> bool {
        self.config
            .session
            .max_rounds_per_chat
            .is_some_and(|max| rounds_used >= max)
    }
}

/// 引擎层重试退避上限（避免重试等待无限期挂起回合）
const ENGINE_RETRY_MAX_WAIT: Duration = Duration::from_secs(10);
/// 引擎层重试基础退避（指数 ×2）
const ENGINE_RETRY_BASE_WAIT: Duration = Duration::from_millis(250);

/// 每轮输入来源
pub(crate) enum RoundSource {
    /// 首轮：由 chat() 原子启动的 RoundStart
    First(RoundStart),
    /// 后续工具轮：从 AwaitingCalls 的 pending 结果恢复
    Resume,
}

/// 提取 panic 消息
pub(crate) fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}

/// EngineReply 标签（metrics）
fn outcome_label(reply: &EngineReply) -> &'static str {
    match reply {
        EngineReply::Success(_) => "success",
        EngineReply::Streaming(_) => "streaming",
        EngineReply::Busy { .. } => "busy",
        EngineReply::Error(_) => "error",
        EngineReply::Cancelled => "cancelled",
        EngineReply::Timeout => "timeout",
    }
}

// 供上层（agent）将 SessionReply 编码回 Envelope 的便捷转换
impl From<EngineReply> for SessionReply {
    fn from(r: EngineReply) -> Self {
        match r {
            EngineReply::Success(resp) => SessionReply::from_response(*resp),
            EngineReply::Streaming(_) => SessionReply::Error {
                message: "streaming reply must be consumed directly, not over envelope".into(),
                kind: ErrorKind::Internal,
                retry_after_ms: None,
            },
            EngineReply::Busy { turn_id } => SessionReply::Busy { turn_id },
            EngineReply::Error(e) => {
                // 错误分类（回合级）：限流独立成类并透传 retry_after，
                // 其余 LLM 错误归 Llm，非 LLM 错误（状态冲突/回合异常/通道关闭）归 Internal
                let (kind, retry_after_ms) = match &e {
                    EngineError::Llm(LlmError::RateLimited { retry_after }) => (
                        ErrorKind::RateLimited,
                        retry_after.map(|d| d.as_millis() as u64),
                    ),
                    EngineError::Llm(_) => (ErrorKind::Llm, None),
                    _ => (ErrorKind::Internal, None),
                };
                SessionReply::Error {
                    message: e.to_string(),
                    kind,
                    retry_after_ms,
                }
            }
            EngineReply::Cancelled => SessionReply::Cancelled,
            EngineReply::Timeout => SessionReply::Error {
                message: "turn timeout".into(),
                kind: ErrorKind::Timeout,
                retry_after_ms: None,
            },
        }
    }
}

/// 启动阶段错误的回合级分类（供 agent 层构造 `SessionReply::Error.kind`）
///
/// `Budget` 独立成类（上游可提示预算耗尽而非笼统失败）；
/// `MaxSessions` / `Busy` 属引擎并发治理，归 `Internal`。
impl From<EngineStartError> for ErrorKind {
    fn from(e: EngineStartError) -> Self {
        match e {
            EngineStartError::Budget(_) => ErrorKind::Budget,
            // MaxSessions / Busy / PromptTooLarge 均属输入侧或并发治理的启动拒绝
            EngineStartError::MaxSessions
            | EngineStartError::Busy
            | EngineStartError::PromptTooLarge { .. } => ErrorKind::Internal,
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "turn_governance_tests.rs"]
mod turn_governance_tests;

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
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::budget::{add_tokens, tokens_from_response, BudgetConfig, BudgetError};
use crate::cache::{CacheConfig, InMemoryCache};
use crate::observe;
use crate::provider::{ChatResponse, LLMProvider, LlmError, StreamChunk};
use crate::session::{
    ChatPayload, FinishAction, RoundStart, Session, SessionConfig, SessionId, SessionReply,
    TurnOutcome,
};
use crate::tool::{ExecutedTool, ToolExecutor, ToolRegistry};

pub mod session_mgmt;
pub mod stream;

pub use session_mgmt::{ReaperHandle, SessionPhase, SessionSnapshot};

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

/// 引擎回信 — `chat` 的执行产物
pub enum EngineReply {
    /// 正常完成（含缓存命中；缓存命中不计量 Token）
    Success(Box<ChatResponse>),
    /// 流式输出：调用方消费 chunk 流（含累积 Delta 与最终 Finish）
    Streaming(BoxStream<'static, Result<StreamChunk, LlmError>>),
    /// 会话忙碌：已有回合进行中，拒绝并发 Chat
    Busy { turn_id: u64 },
    /// 会话不存在 / 预算超限 / 回合异常
    Error(String),
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
            EngineReply::Error(msg) => f.debug_tuple("Error").field(msg).finish(),
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
    pub(crate) total_consumed_tokens: Arc<AtomicU64>,
    cache: Option<Arc<InMemoryCache>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("sessions", &self.sessions.len())
            .field("max_sessions", &self.config.max_sessions)
            .field("has_tools", &self.tools.is_some())
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

    /// 回放已确认的会话事实到指定会话历史（崩溃恢复用）
    ///
    /// 自动创建会话（受 `max_sessions` 有界约束）；逐条 `push_history`，
    /// 返回成功追加条数。用于恢复到已确认前缀（不触发 LLM，忠实重建上下文）。
    pub fn replay_history(
        &self,
        session_id: SessionId,
        messages: Vec<crate::provider::Message>,
    ) -> Result<usize, String> {
        if !self.ensure_session(session_id) {
            return Err("max sessions reached, cannot restore session".into());
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

        loop {
            let cur = std::mem::replace(&mut src, RoundSource::Resume);
            let (turn_id, mut cancel_rx, request) = match cur {
                RoundSource::First(f) => (f.turn_id, f.cancel_rx, f.request),
                RoundSource::Resume => {
                    match self
                        .sessions
                        .get_mut(session_id)
                        .and_then(|mut s| s.resume_thinking())
                    {
                        Some(x) => x,
                        None => return EngineReply::Error("resume failed (not awaiting)".into()),
                    }
                }
            };

            // 回合级中断（轮隙间：工具执行后 / 思考间隙）
            if self.is_interrupted(session_id) {
                return EngineReply::Cancelled;
            }

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

            // 终态收敛（session 内累加 consumed_tokens；guard 短暂持有后立即释放）
            let action = self
                .sessions
                .get_mut(session_id)
                .map(|mut s| s.finish_thinking(turn_id, outcome))
                .unwrap_or(FinishAction::Idle { response: None });

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
                    return EngineReply::Error("turn ended without success".into());
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
                        // 纯派发轮（全部不等待）→ 回合就此结束，返回模型原文
                        ToolRound::Settled => return EngineReply::Success(Box::new(response)),
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
                .single_llm_call(request.clone(), cancel_rx, timeout)
                .await;
            match outcome {
                TurnOutcome::Error(e) if e.is_retryable() && attempt < max_retries => {
                    attempt += 1;
                    tracing::warn!(error = %e, attempt, "llm call failed, retrying");
                    observe::llm_retry();
                }
                other => return other,
            }
        }
    }

    /// 单次 LLM 调用：三路 select（LLM / 取消 / 超时）+ catch_unwind
    async fn single_llm_call(
        &self,
        request: crate::provider::ChatRequest,
        cancel_rx: &mut oneshot::Receiver<()>,
        timeout: Duration,
    ) -> TurnOutcome {
        let fut = self.provider.chat(request);
        let result = AssertUnwindSafe(async {
            tokio::select! {
                res = fut => match res {
                    Ok(r) => TurnOutcome::Success(Box::new(r)),
                    Err(e) => TurnOutcome::Error(e),
                },
                _ = &mut *cancel_rx => TurnOutcome::Cancelled,
                _ = tokio::time::sleep(timeout) => TurnOutcome::Timeout,
            }
        })
        .catch_unwind()
        .await;
        match result {
            Ok(outcome) => outcome,
            Err(payload) => TurnOutcome::Panic(panic_message(payload)),
        }
    }

    /// 会话是否收到回合级中断
    pub(crate) fn is_interrupted(&self, session_id: &SessionId) -> bool {
        self.sessions
            .get(session_id)
            .map(|s| s.is_interrupted())
            .unwrap_or(false)
    }
    /// 一轮工具调用的完整处理：截断 → 按 wait 分流 → 等待类同步 / 派发类后台注入
    ///
    /// - 截断项：生成引导错误消息（下一轮重发），立即收敛
    /// - 派发类（不等待）：占位结果立即收敛（保证 assistant tool_calls 与 tool 结果
    ///   配对），后台任务执行完成后结果入队，等待下一次模型调用/回合合并注入
    /// - 等待类：同步执行（并行 + 隔离 + 超时），完成后收敛结果
    pub(crate) async fn run_tool_calls(
        &self,
        session_id: &SessionId,
        turn_id: u64,
        mut tool_calls: Vec<crate::provider::ToolCall>,
    ) -> ToolRound {
        let (registry, executor) = match (&self.tools, &self.tool_executor) {
            (Some(r), Some(e)) => (r.clone(), e.clone()),
            _ => return ToolRound::Settled,
        };

        // 0. 深度兜底（声明层过滤被绕过时的防线）：嵌套深度达上限的会话
        //    拒绝调用子 Agent 工具（depth_limited），生成明确错误并立即收敛
        let depth = self
            .sessions
            .get(session_id)
            .map(|s| s.peer_depth())
            .unwrap_or(0);
        if depth >= self.config.max_subagent_depth {
            let (blocked, rest): (Vec<_>, Vec<_>) = tool_calls.into_iter().partition(|tc| {
                registry
                    .get(&tc.function.name)
                    .map(|t| t.depth_limited())
                    .unwrap_or(false)
            });
            for tc in blocked {
                if let Some(mut s) = self.sessions.get_mut(session_id) {
                    s.finish_tool_call(&tc.id, DEPTH_LIMIT_MESSAGE.to_string());
                }
            }
            tool_calls = rest;
        }

        // 1. 截断：超出 max_per_turn 的生成引导错误（由调用方统一截断一次）
        let (head, tail) = executor.truncate(tool_calls);
        for tc in tail {
            if let Some(mut s) = self.sessions.get_mut(session_id) {
                s.finish_tool_call(
                    &tc.id,
                    format!(
                        "Exceeds max_tools_per_turn limit ({}). \
                         Please re-issue this tool call in the next turn.",
                        executor.config().max_per_turn
                    ),
                );
            }
        }

        // 2. 按等待决策分流
        let (waiting, dispatched) = executor.split_by_wait(head, &registry);

        // 3. 派发类：占位收敛 + 后台执行完成后入队注入
        if !dispatched.is_empty() {
            for tc in &dispatched {
                if let Some(mut s) = self.sessions.get_mut(session_id) {
                    s.finish_tool_call(&tc.id, DISPATCHED_PLACEHOLDER.to_string());
                }
            }
            let handles =
                executor.dispatch_batch(dispatched, &registry, *session_id, turn_id, depth);
            let engine = self.clone();
            let sid = *session_id;
            tokio::spawn(async move {
                for h in handles {
                    let r = h.await.unwrap_or_else(|_| ExecutedTool {
                        tool_call_id: String::new(),
                        tool_name: "<unknown>".into(),
                        result: "async tool task panicked".into(),
                    });
                    observe::tool_completed(!r.result.is_empty());
                    let text = format!("[async tool '{}' completed]\n{}", r.tool_name, r.result);
                    if let Some(mut s) = engine.sessions.get_mut(&sid) {
                        s.inject_tool_result(text);
                    }
                }
            });
        }

        // 4. 等待类：同步执行并收敛结果
        if !waiting.is_empty() {
            // 能力降级：厂商不支持并行工具时强制串行（Some(1)）
            let max_concurrent = if self.provider.capabilities().parallel_tool_calls {
                None
            } else {
                Some(1)
            };
            let results = executor
                .execute_batch(
                    waiting,
                    &registry,
                    *session_id,
                    turn_id,
                    depth,
                    max_concurrent,
                )
                .await;
            for r in results {
                observe::tool_completed(!r.result.is_empty());
                if let Some(mut s) = self.sessions.get_mut(session_id) {
                    s.finish_tool_call(&r.tool_call_id, r.result);
                }
            }
            return ToolRound::Resume;
        }

        // 5. 纯派发轮：占位 Tool 消息落 history → Idle（回合结束）
        if let Some(mut s) = self.sessions.get_mut(session_id) {
            s.settle_dispatched();
        }
        ToolRound::Settled
    }
}

/// 派发类（不等待）工具的占位结果 — 立即收敛进 history，满足厂商协议
/// assistant tool_calls 与 tool 结果配对；真实结果完成后入队，在**下一次**
/// 模型调用/回合时合并注入（绝不为此主动触发 LLM）。
const DISPATCHED_PLACEHOLDER: &str =
    "Task dispatched (async execution); real result will be injected into a later turn.";

/// 子智能体嵌套深度超限的拒绝消息（执行层兜底）
const DEPTH_LIMIT_MESSAGE: &str =
    "Rejected: subagent nesting depth limit reached. This agent cannot call sub-agents.";

/// 工具轮处理结果
pub(crate) enum ToolRound {
    /// 有待等待的工具 → 继续 resume 循环
    Resume,
    /// 纯派发轮（全部不等待）→ 回合就此结束
    Settled,
}

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
            },
            EngineReply::Busy { turn_id } => SessionReply::Busy { turn_id },
            EngineReply::Error(msg) => SessionReply::Error { message: msg },
            EngineReply::Cancelled => SessionReply::Cancelled,
            EngineReply::Timeout => SessionReply::Error {
                message: "turn timeout".into(),
            },
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

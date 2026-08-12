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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use futures::FutureExt;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::budget::{add_tokens, tokens_from_response, BudgetConfig, BudgetError};
use crate::cache::{CacheConfig, InMemoryCache};
use crate::observe;
use crate::provider::{ChatResponse, LLMProvider};
use crate::session::{
    ChatOptions, ChatPayload, FinishAction, Session, SessionConfig, SessionId, SessionReply,
};
use crate::tool::{ToolExecutor, ToolRegistry};

/// 引擎配置
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// 会话级配置模板（每个新 Session 继承）
    pub session: SessionConfig,
    /// Token 预算（session_limit / global_limit，0 = 无限制）
    pub budget: BudgetConfig,
    /// 响应缓存（enabled=false 时完全禁用）
    pub cache: CacheConfig,
    /// 最大并发会话数（超限拒绝新会话）
    pub max_sessions: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            session: SessionConfig::default(),
            budget: BudgetConfig::default(),
            cache: CacheConfig::default(),
            max_sessions: 100,
        }
    }
}

/// 引擎回信 — `chat` 的执行产物
#[derive(Debug, Clone)]
pub enum EngineReply {
    /// 正常完成（含缓存命中；缓存命中不计量 Token）
    Success(Box<ChatResponse>),
    /// 会话忙碌：已有回合进行中，拒绝并发 Chat
    Busy { turn_id: u64 },
    /// 会话不存在 / 预算超限 / 回合异常
    Error(String),
    /// 已取消（Interrupt 生效）
    Cancelled,
    /// 回合超时未完成
    Timeout,
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
    sessions: Arc<DashMap<SessionId, Session>>,
    provider: Arc<dyn LLMProvider>,
    config: EngineConfig,
    tools: Option<ToolRegistry>,
    tool_executor: Option<ToolExecutor>,
    total_consumed_tokens: Arc<AtomicU64>,
    cache: Option<Arc<InMemoryCache>>,
    /// 回合级取消标志：session_id → 标志（Interrupt 置位，run_chat 每轮检查）
    cancels: Arc<DashMap<SessionId, Arc<AtomicBool>>>,
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
            cancels: Arc::new(DashMap::new()),
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

    /// 当前缓存条目数（未启用时为 0）
    pub fn cache_len(&self) -> usize {
        self.cache.as_ref().map(|c| c.len()).unwrap_or(0)
    }

    // ── 主入口 ────────────────────────────────

    /// 发起一轮 Chat，返回句柄（快速返回，实际执行在派生任务中）
    ///
    /// 同步段完成：会话创建 / busy 拒绝 / 预算守门 / 取消标志注册，
    /// 不进行任何 await。实际回合由内部 spawn 的 `run_chat` 执行。
    pub fn chat(
        &self,
        session_id: SessionId,
        payload: ChatPayload,
    ) -> Result<ChatHandle, EngineStartError> {
        // 0. 创建/获取会话（短暂持锁，无 await）
        if !self.get_or_create_session(session_id) {
            return Err(EngineStartError::MaxSessions);
        }

        // 1. busy 拒绝（显式可见）
        if self
            .sessions
            .get(&session_id)
            .map(|s| s.is_busy())
            .unwrap_or(false)
        {
            return Err(EngineStartError::Busy);
        }

        // 2. 预算守门：会话级 + 全局级（软限制，check-then-act）
        if let Err(e) = self.check_budget(&session_id) {
            return Err(EngineStartError::Budget(e));
        }

        // 3. 注入工具声明（若本轮未显式指定）
        let mut options = payload.options;
        if let Some(registry) = &self.tools {
            if options.tools.is_empty() {
                options.tools = registry.declarations();
            }
        }

        // 4. 写入 history + options（锁内）
        if let Some(mut s) = self.sessions.get_mut(&session_id) {
            s.push_history(payload.message.clone());
            s.set_chat_options(options.clone());
        }

        // 5. 回合取消标志
        let flag = Arc::new(AtomicBool::new(false));
        self.cancels.insert(session_id, flag);

        // 6. spawn 回合执行
        let engine = self.clone();
        let (reply_tx, reply_rx) = oneshot::channel();
        tokio::spawn(async move {
            let reply = engine.run_chat(&session_id, options).await;
            let _ = reply_tx.send(reply);
            engine.cancels.remove(&session_id);
        });

        Ok(ChatHandle {
            engine: self.clone(),
            session_id,
            rx: Arc::new(tokio::sync::Mutex::new(Some(reply_rx))),
        })
    }

    /// 中断一个会话的当前回合（幂等）
    ///
    /// 任一时点生效：LLM 等待中即时打断；轮隙间由下次检查拦截。
    pub fn interrupt(&self, session_id: SessionId) -> bool {
        let flag = match self.cancels.get(&session_id) {
            Some(f) => f.clone(),
            None => return false,
        };
        if flag.swap(true, Ordering::SeqCst) {
            return true; // 已在取消中
        }
        // 触发正在等待的 LLM 调用
        if let Some(mut s) = self.sessions.get_mut(&session_id) {
            s.cancel_thinking();
        }
        true
    }

    // ── 内部 ──────────────────────────────────

    /// 创建或获取会话；超限返回 false
    fn get_or_create_session(&self, session_id: SessionId) -> bool {
        if self.sessions.contains_key(&session_id) {
            return true;
        }
        if self.sessions.len() >= self.config.max_sessions {
            return false;
        }
        self.sessions
            .entry(session_id)
            .or_insert_with(|| Session::new(self.config.session.clone()));
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
    async fn run_chat(&self, session_id: &SessionId, options: ChatOptions) -> EngineReply {
        let span = observe::turn_span(
            session_id,
            self.sessions
                .get(session_id)
                .map(|s| s.turn_id())
                .unwrap_or(0),
        );
        let flag = self.cancels.get(session_id).map(|f| f.clone());
        let cancelled = flag
            .as_ref()
            .map(|f| f.clone())
            .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));

        let timer = observe::Timer::start();
        let outcome = self
            .run_chat_inner(session_id, options, cancelled.clone())
            .instrument(span)
            .await;
        observe::record_turn_duration(outcome_label(&outcome), timer.finish());
        observe::turn_completed(outcome_label(&outcome));
        outcome
    }

    async fn run_chat_inner(
        &self,
        session_id: &SessionId,
        options: ChatOptions,
        cancelled: Arc<AtomicBool>,
    ) -> EngineReply {
        let timeout = self.config.session.timeout.thinking_timeout;
        let mut phase = Phase::First(options);

        loop {
            if cancelled.load(Ordering::SeqCst) {
                return EngineReply::Cancelled;
            }

            // 1. 启动本轮 thinking（拿到 turn_id + 本轮取消通道）
            let (turn_id, cancel_rx, request) = match &phase {
                Phase::First(opts) => {
                    let pair = self
                        .sessions
                        .get_mut(session_id)
                        .and_then(|mut s| s.start_thinking());
                    match pair {
                        Some((turn_id, cancel_rx)) => {
                            let req = self
                                .sessions
                                .get(session_id)
                                .map(|s| s.build_chat_request(opts))
                                .unwrap_or_default();
                            (turn_id, cancel_rx, req)
                        }
                        None => {
                            return EngineReply::Error("failed to start thinking".into());
                        }
                    }
                }
                Phase::Resume => {
                    match self
                        .sessions
                        .get_mut(session_id)
                        .and_then(|mut s| s.resume_thinking())
                    {
                        Some((turn_id, cancel_rx, req)) => (turn_id, cancel_rx, req),
                        None => {
                            return EngineReply::Error("resume failed (not awaiting)".into());
                        }
                    }
                }
            };

            // 2. 缓存命中检查（不调 LLM；catch_unwind 兜底降级为真实调用）
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

            // 3. 执行 LLM（命中缓存则跳过）
            let outcome = if let Some(resp) = cached {
                crate::session::TurnOutcome::Cached(Box::new(resp))
            } else {
                // 三路 select（LLM / 取消 / 超时）+ catch_unwind
                let fut = self.provider.chat(request);
                let result = AssertUnwindSafe(async {
                    tokio::select! {
                        res = fut => match res {
                            Ok(r) => crate::session::TurnOutcome::Success(Box::new(r)),
                            Err(e) => crate::session::TurnOutcome::Error(e),
                        },
                        _ = cancel_rx => crate::session::TurnOutcome::Cancelled,
                        _ = tokio::time::sleep(timeout) => crate::session::TurnOutcome::Timeout,
                    }
                })
                .catch_unwind()
                .await;
                match result {
                    Ok(o) => o,
                    Err(payload) => crate::session::TurnOutcome::Panic(panic_message(payload)),
                }
            };

            // 缓存写入：真实调用成功且无工具调用才可缓存
            if let (Some(cache), Some(key)) = (&self.cache, &cache_key) {
                if let crate::session::TurnOutcome::Success(resp) = &outcome {
                    if resp.message.tool_calls.is_empty() {
                        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                            cache.set(key.clone(), (**resp).clone());
                        }));
                    }
                }
            }

            // 4. 终态收敛（session 内累加 consumed_tokens；guard 短暂持有后立即释放）
            // 4b. 全局 Token 累计：**每轮真实成功**都计入（含 AwaitingCalls 中间轮），
            //     与 session 级口径一致；缓存命中不计量。
            if let crate::session::TurnOutcome::Success(resp) = &outcome {
                add_tokens(&self.total_consumed_tokens, tokens_from_response(resp));
            }

            // 4. 终态收敛（session 内累加 consumed_tokens；guard 短暂持有后立即释放）
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
                    // 错误 / 取消 / 超时 / panic
                    if cancelled.load(Ordering::SeqCst) {
                        return EngineReply::Cancelled;
                    }
                    return EngineReply::Error("turn ended without success".into());
                }
                FinishAction::AwaitingCalls { tool_calls, .. } => {
                    if self.tool_executor.is_none() || self.tools.is_none() {
                        // 无工具能力但模型发起了工具调用：强制 Idle + 返回响应
                        if let Some(mut s) = self.sessions.get_mut(session_id) {
                            s.force_idle();
                        }
                        return EngineReply::Error(
                            "model requested tools but tools are not enabled".into(),
                        );
                    }
                    // 6. 执行工具（并行 + 截断 + 隔离 + 超时）
                    self.execute_tools(session_id, tool_calls).await;
                    phase = Phase::Resume;
                }
            }
        }
    }

    /// 执行一批工具调用，将结果回写进 session 的 AwaitingCalls pending
    async fn execute_tools(
        &self,
        session_id: &SessionId,
        tool_calls: Vec<crate::provider::ToolCall>,
    ) {
        let Some(registry) = self.tools.clone() else {
            return;
        };
        let Some(executor) = self.tool_executor.clone() else {
            return;
        };

        let results = executor
            .execute_batch(tool_calls, &registry, *session_id, 0)
            .await;

        for r in results {
            observe::tool_completed(!r.result.is_empty());
            if let Some(mut s) = self.sessions.get_mut(session_id) {
                s.finish_tool_call(&r.tool_call_id, r.result);
            }
        }
    }
}

/// 回合阶段
enum Phase {
    /// 首轮：使用传入 options
    First(ChatOptions),
    /// 后续轮：由 resume_thinking 从已收集的工具结果恢复
    Resume,
}

/// 提取 panic 消息
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
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

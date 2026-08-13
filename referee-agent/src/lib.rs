//! # Referee Agent — 开箱即用的完整 Agent 业务封装
//!
//! 建立在 `referee-ai-base`（地基）之上的**业务层**：
//! 将 base 的积木（厂商抽象、会话引擎、工具执行、预算、缓存）组装为可直接使用的
//! Agent 运行时，并提供业务能力：
//!
//! - [`AgentRuntime`]：实现 `referee-core::Extension`，把 base 引擎接入内核消息
//!   路由（`Chat` / `Interrupt`）。
//! - [`tool::AgentTool`]：对等/子 Agent 协作（Agent as Tool）。
//! - [`tool::mcp`]：MCP 2.0 stdio 客户端桥（远程 MCP 工具接入 `Tool` 抽象；
//!   按需拓展，启用 `mcp-stdio` feature 后加载）。
//! - [`artifact`]：带 ACL 的工件存储（业务层对等成果共享的安全能力）。
//!
//! ## 定位
//! base 提供「接 LLM → 组装 prompt → 调工具 → 管预算 → 回复」的地基；
//! 本模块提供「如何把地基变成一个完整、可用、协作的 Agent」。

pub mod artifact;
pub mod tool;
// Agent Skills 开放标准（SKILL.md）注入，按需拓展，启用 `skills` feature 后加载
#[cfg(feature = "skills")]
pub mod skill;

pub use artifact::{
    Artifact, ArtifactStore, BoardId, InMemoryArtifactStore, StoreConfig, StoreError,
};
pub use tool::{AgentTool, ArtifactReader, ListMyBoard};
#[cfg(feature = "skills")]
pub use skill::{
    render_skill_context, KeywordRouter, RegistryConfig as SkillRegistryConfig, RegistryError as SkillRegistryError,
    Skill, SkillConfig, SkillDeclaration, SkillError, SkillRegistry, SkillRouter,
};

use std::sync::Arc;

use referee_ai_base::engine::{ChatHandle, Engine, EngineReply, EngineStartError, SessionSnapshot};
use referee_ai_base::session::{ChatPayload, SessionId, SessionMessage, SessionReply};
use referee_core::extension::{CapabilityId, Extension, KernelContext};
use referee_core::Envelope;
use tracing::info_span;
use tracing::Instrument;

/// Agent 运行时 — `referee-core` 扩展，管理 N 个会话
///
/// 内部委托 [`Engine`] 驱动最小闭环；本类型负责把内核 `Envelope` 消息
/// 转译为引擎调用，并承载业务能力（对等工具、ACL 工件存储）。
///
/// `Clone` 用于共享观测句柄（会话表 / 计数器均为 `Arc`），注册时应使用同一实例。
#[derive(Clone)]
pub struct AgentRuntime {
    id: CapabilityId,
    engine: Engine,
    artifact_store: Option<Arc<dyn ArtifactStore>>,
}

impl std::fmt::Debug for AgentRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentRuntime")
            .field("id", &self.id)
            .field("sessions", &self.session_count())
            .field("has_artifact_store", &self.artifact_store.is_some())
            .field("total_consumed_tokens", &self.total_consumed_tokens())
            .field("cache_entries", &self.cache_len())
            .finish()
    }
}

impl AgentRuntime {
    /// 创建 Agent 运行时
    ///
    /// `engine` 应已配置（含 provider、工具、预算、缓存）。需对等协作时，
    /// 调用方在构造 `engine` 时对 `ToolExecutor` 调用 `.with_kernel(kernel)`。
    pub fn new(engine: Engine) -> Self {
        Self {
            id: CapabilityId::new(),
            engine,
            artifact_store: None,
        }
    }

    /// 注入 ACL 工件存储（启用对等工具大结果落库 + 授权读取）
    pub fn with_artifact_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
        self
    }

    /// 扩展的 CapabilityId
    pub fn id(&self) -> CapabilityId {
        self.id
    }

    /// 当前会话数
    pub fn session_count(&self) -> usize {
        self.engine.session_count()
    }

    /// 移除指定会话（转发引擎）
    pub fn remove_session(&self, session_id: SessionId) -> bool {
        self.engine.remove_session(session_id)
    }

    /// 枚举全部会话 ID（转发引擎）
    pub fn list_sessions(&self) -> Vec<SessionId> {
        self.engine.list_sessions()
    }

    /// 查询单个会话的运行快照（转发引擎）
    pub fn session_info(&self, session_id: SessionId) -> Option<SessionSnapshot> {
        self.engine.session_info(session_id)
    }

    /// 流式发起一轮 Chat（库 API，不经 Envelope 协议）
    ///
    /// 返回句柄，`wait()` 得到 `EngineReply::Streaming`；调用方消费 chunk 流，
    /// 引擎内部累积收敛与非流式一致。适合需要边生成边消费的集成方直接调用。
    pub fn chat_stream(
        &self,
        session_id: SessionId,
        payload: ChatPayload,
    ) -> Result<ChatHandle, EngineStartError> {
        self.engine.chat_stream(session_id, payload)
    }

    /// 全局已消耗 Token 数（观测）
    pub fn total_consumed_tokens(&self) -> u64 {
        self.engine.total_consumed_tokens()
    }

    /// 指定会话已消耗 Token 数（观测）
    pub fn session_consumed_tokens(&self, session_id: SessionId) -> Option<u64> {
        self.engine.session_consumed_tokens(session_id)
    }

    /// 当前缓存条目数（观测）
    pub fn cache_len(&self) -> usize {
        self.engine.cache_len()
    }

    /// 注册对等 Agent（另一 Runtime 上的 Session）为 Local 工具
    ///
    /// 需引擎已 `with_tools`。注册后本 Runtime 的任意会话即可通过该工具名
    /// 同步调用目标 Agent。
    pub fn register_peer_tool(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        target_runtime_id: CapabilityId,
        target_session_id: SessionId,
    ) -> Result<(), referee_ai_base::tool::RegistryError> {
        let mut tool = AgentTool::new(name, description, target_runtime_id, target_session_id);
        if let Some(store) = &self.artifact_store {
            tool = tool.with_artifact_store(store.clone());
        }
        self.engine.register_tool(Arc::new(tool))
    }

    /// 注册成果板读取工具（`list_my_board` / `read_artifact`）
    ///
    /// 需已 `with_artifact_store` 且引擎已 `with_tools`。
    pub fn register_artifact_tools(&self) -> Result<(), referee_ai_base::tool::RegistryError> {
        let store = self
            .artifact_store
            .clone()
            .ok_or(referee_ai_base::tool::RegistryError::NotEnabled)?;
        self.engine
            .register_tool(Arc::new(ListMyBoard::new(store.clone())))?;
        self.engine
            .register_tool(Arc::new(ArtifactReader::new(store)))?;
        Ok(())
    }

    // ── 消息处理 ──────────────────────────────

    /// 处理 Chat 消息 — 委托引擎驱动回合，结果异步回复调用方
    fn handle_chat(&self, ctx: KernelContext, session_id: SessionId, payload: ChatPayload) {
        match self.engine.chat(session_id, payload) {
            Ok(handle) => {
                // spawn 等待回合结果后 reply（handle 内零阻塞）
                tokio::spawn(async move {
                    let reply = handle
                        .wait()
                        .await
                        .unwrap_or(EngineReply::Error("chat channel closed".into()));
                    let _ = ctx.reply(SessionReply::from(reply).to_envelope());
                });
            }
            Err(e) => {
                // 启动阶段错误（busy / 预算 / 超会话）显式回信，不静默丢弃
                let _ = ctx.reply(
                    SessionReply::Error {
                        message: e.to_string(),
                    }
                    .to_envelope(),
                );
            }
        }
    }

    /// 处理 Interrupt 消息 — 取消目标会话当前回合
    fn handle_interrupt(&self, ctx: KernelContext, session_id: SessionId) {
        let reply = if self.engine.interrupt(session_id) {
            SessionReply::Cancelled
        } else {
            SessionReply::Unhandled {
                reason: "no active turn for session".into(),
            }
        };
        let _ = ctx.reply(reply.to_envelope());
    }
}

#[async_trait::async_trait]
impl Extension for AgentRuntime {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> referee_core::KernelResult<()> {
        let msg = match SessionMessage::from_envelope(&env) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!(error = %e, "failed to decode session message");
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
                // base 引擎采用内部回合循环，不再产生/消费 ToolResult / Resume
                // （旧协议消息出于兼容性显式拒绝，避免静默丢弃）。
                SessionMessage::ToolResult { .. } | SessionMessage::Resume { .. } => {
                    let _ = ctx.reply(
                        SessionReply::Unhandled {
                            reason: "legacy message: engine uses internal loop".into(),
                        }
                        .to_envelope(),
                    );
                }
            }
        }
        .instrument(span)
        .await;

        Ok(())
    }
}

/// 获取 SessionMessage 类型标签（tracing span）
fn message_kind_label(msg: &SessionMessage) -> &'static str {
    match msg {
        SessionMessage::Chat { .. } => "chat",
        SessionMessage::Interrupt { .. } => "interrupt",
        SessionMessage::ToolResult { .. } => "tool_result",
        SessionMessage::Resume { .. } => "resume",
    }
}

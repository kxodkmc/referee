//! # Referee AI — Agent 核心支撑层（地基）
//!
//! 在 `referee-core`（微内核）之上，提供一套**基础、底层、易维护、易拓展**的
//! AI 能力积木。定位为业务无关的基础设施：只提供「接 LLM → 组装 prompt →
//! 调工具 → 管预算 → 回复」的最小闭环，不预置记忆、MCP、Skills 等业务策略。
//!
//! 业务化、开箱即用的完整 Agent 封装（Extension 集成、对等协作等）在
//! `referee-agent` crate，本 crate 供其与第三方搭建。
//!
//! ## 模块
//! - [`provider`]：厂商唯一 I/O 边界。`LLMProvider` trait、纯数据模型、
//!   错误归一与重试、能力声明、OpenAI 兼容底座 + 厂商适配器。
//! - [`session`]：会话状态机 `Idle / Thinking / AwaitingCalls`，并发正确性、
//!   中断、超时、终态自管（`run_turn`）。
//! - [`tool`]：工具抽象与执行机制。`Tool` trait + 有界注册表 + 并行/截断/
//!   panic 隔离/超时执行器。
//! - [`store`]：通用有界 KV 存储抽象（成果/大结果落库），后端可替换。
//! - [`budget`]：Token 预算治理（Session 级 + 全局共享计数器）。
//! - [`prompt`]：提示词组装与优先级截断（杜绝 Prompt 爆炸）。
//! - [`cache`]：LRU + TTL 语义缓存，一致性流合成。
//! - [`observe`]：可观测门面（tracing span + metrics 指标 + 计时）。
//! - [`engine`]：会话引擎，把最小闭环收敛到单任务顺序异步流程，可直接驱动。
//!
//! ## 分层
//! ```text
//!   referee-core（内核，零改动）
//!        │
//!   referee-ai（本 crate：地基积木 + 会话引擎）
//!        │
//!   referee-agent（业务封装：Extension 集成、对等协作，开箱即用）
//! ```

pub mod budget;
pub mod cache;
pub mod engine;
pub mod observe;
pub mod prompt;
pub mod provider;
pub mod session;
pub mod store;
pub mod tool;

// 便捷重导出
pub use engine::{
    ChatHandle, Engine, EngineConfig, EngineError, EngineObserver, EngineReply, EngineStartError,
    ReaperHandle, SessionPhase, SessionSnapshot,
};
pub use provider::{
    LLMProvider, Message, ProviderRegistry, ProviderRegistryError, ProviderStatus, Role,
};
pub use session::{ChatOptions, ChatPayload, ErrorKind, SessionId, SessionMessage, SessionReply};
pub use store::{InMemoryStore, Store, StoreConfig, StoreError, StoredValue};
pub use tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutcome, ToolOutput, ToolRegistry};

//! 超时治理 — Thinking / 等待类工具批次双 deadline
//!
//! 两类超时：
//! - **Thinking 超时**：LLM 调用超时（`run_turn` 内 `tokio::time::timeout` 切断）
//! - **AwaitingCalls 超时**：单轮等待类工具批次总 deadline（引擎工具轮施加；
//!   到达时未完成项以超时消息收敛、会话恢复一致状态，回合不被慢批次无限占用）
//!
//! ## 可配置
//! 所有超时时长通过 [`TimeoutConfig`] 配置，默认值见 [`TimeoutConfig::default`]。
//! 应用侧可在创建 `AgentRuntime` 时覆盖。

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// 超时配置 — 会话级双 deadline
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeoutConfig {
    /// Thinking 状态超时（LLM 调用上限）
    ///
    /// 超时后 `run_turn` 返回 `TurnOutcome::Timeout`，派生任务终态收敛为 Idle。
    pub thinking_timeout: Duration,

    /// 单轮等待类工具批次总 deadline（区别于单工具 `tool_timeout`）
    ///
    /// 引擎在工具轮对等待类（wait=true）批次施加的总上限：deadline 到达时，
    /// 已完成项正常收敛，未完成项以超时消息回写（下一轮对模型可见），
    /// 会话恢复一致状态。派发类（wait=false）后台任务不受此约束。
    /// 历史注：旧注释「AwaitingCalls 跨消息回环 → DLQ」语义在同任务内顺序
    /// 收敛的现行架构下并不存在，本字段已重定义如上，勿按旧语义使用。
    pub awaiting_calls_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            // Thinking：30s（LLM 调用通常 5-15s，30s 足够覆盖深度思考厂商）
            thinking_timeout: Duration::from_secs(30),
            // AwaitingCalls：60s（工具完成通常 10-30s，60s 留余量）
            awaiting_calls_timeout: Duration::from_secs(60),
        }
    }
}

impl TimeoutConfig {
    /// 测试用：极短超时（快速触发超时路径）
    #[cfg(test)]
    pub const fn test_fast() -> Self {
        Self {
            thinking_timeout: Duration::from_millis(50),
            awaiting_calls_timeout: Duration::from_millis(50),
        }
    }
}

//! 超时治理 — Thinking / AwaitingCalls 双 deadline
//!
//! 两类超时：
//! - **Thinking 超时**：LLM 调用超时（`run_turn` 内 `tokio::time::timeout` 切断）
//! - **AwaitingCalls 超时**：等待工具调用完成的超时（
//!   超时后会话自动恢复 Idle + DLQ 记录，杜绝幽灵会话）
//!
//! ## 可配置
//! 所有超时时长通过 [`TimeoutConfig`] 配置，默认值见 [`TimeoutConfig::default`]。
//! 应用侧可在创建 `AgentRuntime` 时覆盖。

use std::time::Duration;

/// 超时配置 — 会话级双 deadline
#[derive(Debug, Clone, Copy)]
pub struct TimeoutConfig {
    /// Thinking 状态超时（LLM 调用上限）
    ///
    /// 超时后 `run_turn` 返回 `TurnOutcome::Timeout`，派生任务终态收敛为 Idle。
    pub thinking_timeout: Duration,

    /// AwaitingCalls 状态超时（等待工具调用完成上限）
    ///
    /// P2/P3 使用。超时后未完成的 pending 项进 DLQ，会话恢复 Idle。
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

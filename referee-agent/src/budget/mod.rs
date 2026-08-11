//! 预算治理 — Token 消耗的双层级限额（Session 级 + 全局级）
//!
//! ## 设计要点
//! - **前置阻断**：`AgentRuntime::handle_chat` 在 `start_thinking` 之前检查
//!   已消耗量，超限直接拒绝（软限制：允许最后一次超额，其后拒绝）。
//! - **精准计量**：优先使用厂商返回的 `usage.total_tokens`；厂商未返回时
//!   用 [`TokenEstimator`] 保守估算响应文本，绝不计 0。
//! - **统一口径**：Session 级与全局级共用 [`tokens_from_response`]，保证
//!   两侧计数一致（含 AwaitingCalls 分支的每轮消耗）。
//! - **可共享全局**：全局计数器为 `Arc<AtomicU64>`，多个 AgentRuntime
//!   （主 Agent + 子 Agent）可注入同一计数器实现系统级总预算。

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::provider::ChatResponse;

/// 预算配置
///
/// 任一上限为 0 表示无限制（如仅启用 Session 级，可设 `global_limit: 0`）。
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BudgetConfig {
    /// 单会话最大 Token 消耗量（0 表示无限制）
    pub session_limit: u64,
    /// 全局最大 Token 消耗量（0 表示无限制）
    pub global_limit: u64,
}

impl Default for BudgetConfig {
    fn default() -> Self {
        Self {
            session_limit: 100_000,  // 默认 10 万
            global_limit: 1_000_000, // 默认 100 万
        }
    }
}

impl BudgetConfig {
    /// 无限制（全部关闭）
    pub const fn unlimited() -> Self {
        Self {
            session_limit: 0,
            global_limit: 0,
        }
    }
}

/// 预算错误
#[derive(Debug, Clone, thiserror::Error)]
pub enum BudgetError {
    #[error("Session budget exceeded (used: {used}, limit: {limit})")]
    SessionExceeded { used: u64, limit: u64 },
    #[error("Global budget exceeded (used: {used}, limit: {limit})")]
    GlobalExceeded { used: u64, limit: u64 },
}

/// Token 估算器 — 厂商未返回 usage 时的保守兜底
pub struct TokenEstimator;

impl TokenEstimator {
    /// 粗略估算：中文/混合文本约 1.5 字符 = 1 Token，英文约 4 字符 = 1 Token。
    /// 采用保守系数（约 1.5 字符/token，即 token ≈ 字符数 × 2/3，向上取整），
    /// 高估 Token 数以防止超支。
    pub fn estimate(text: &str) -> u64 {
        let char_count = text.chars().count() as u64;
        (char_count * 2 / 3) + 1
    }
}

/// 从响应提取本轮 Token 消耗（统一口径）
///
/// - 厂商返回 `usage`：直接使用 `total_tokens`；
/// - 厂商未返回：保守估算响应文本（输入部分无法可靠获取，估算仅覆盖输出，
///   作为保底计量；接入方可在此追加输入侧估算）。
pub fn tokens_from_response(resp: &ChatResponse) -> u64 {
    match &resp.usage {
        Some(u) => u.total_tokens as u64,
        None => {
            let text = resp.message.content.as_text().unwrap_or("");
            TokenEstimator::estimate(text)
        }
    }
}

/// 共享全局计数器（可注入多个 AgentRuntime 实现系统级总预算）
pub type SharedTokenCounter = Arc<AtomicU64>;

/// 原子累加（Relaxed 足够：计数单调、无顺序依赖）
pub fn add_tokens(counter: &AtomicU64, tokens: u64) {
    if tokens > 0 {
        counter.fetch_add(tokens, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{FinishReason, Message, TokenUsage};

    fn mock_response_with_usage(total: usize) -> ChatResponse {
        ChatResponse {
            id: "t".into(),
            model: "mock".into(),
            message: Message::assistant("hello world"),
            finish_reason: FinishReason::Stop,
            usage: Some(TokenUsage {
                prompt_tokens: 10,
                completion_tokens: total,
                total_tokens: total,
                ..Default::default()
            }),
        }
    }

    fn mock_response_no_usage(text: &str) -> ChatResponse {
        ChatResponse {
            id: "t".into(),
            model: "mock".into(),
            message: Message::assistant(text),
            finish_reason: FinishReason::Stop,
            usage: None,
        }
    }

    #[test]
    fn estimator_is_conservative() {
        // 空文本也至少计 1（防止计 0）
        assert_eq!(TokenEstimator::estimate(""), 1);
        // 6 字符 → 6*2/3+1 = 5
        assert_eq!(TokenEstimator::estimate("abcdef"), 5);
        // 长文本单调不减
        let a = TokenEstimator::estimate("short");
        let b = TokenEstimator::estimate("a much longer piece of text here");
        assert!(b > a);
    }

    #[test]
    fn usage_preferred_over_estimate() {
        let resp = mock_response_with_usage(50);
        assert_eq!(tokens_from_response(&resp), 50);
    }

    #[test]
    fn missing_usage_falls_back_to_estimate() {
        // 5000 字符 ≈ 5000*2/3+1 = 3334，绝不为 0
        let resp = mock_response_no_usage(&"x".repeat(5000));
        let tokens = tokens_from_response(&resp);
        assert!(tokens > 0, "missing usage must not be counted as 0");
        assert_eq!(tokens, 3334); // 5000*2/3 + 1
    }

    #[test]
    fn add_tokens_skips_zero() {
        let counter = AtomicU64::new(0);
        add_tokens(&counter, 0);
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        add_tokens(&counter, 7);
        assert_eq!(counter.load(Ordering::SeqCst), 7);
    }
}

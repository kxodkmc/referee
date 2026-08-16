//! 可观测门面 — tracing 全链路 + metrics 指标（地基）
//!
//! 为会话闭环提供统一的观测入口：spans 串联（回合→LLM→工具）、
//! 结构化日志记录关键决策点（缓存命中、截断、重试、超时、拒绝）、
//! metrics 计数器输出可断言指标。后端（tracing-subscriber / metrics exporter）
//! 由应用层在 main 中组装，本模块仅依赖门面，不绑定具体后端。

use std::time::{Duration, Instant};

use tracing::{info_span, Span};

/// 会话回合 span — 贯穿一轮 chat（含多轮工具循环）全链路
///
/// 返回 span 后由调用方 `.instrument(future).await` 包裹回合执行。
pub fn turn_span(session_id: impl std::fmt::Display, turn_id: u64) -> Span {
    info_span!("base_turn", session_id = %session_id, turn_id)
}

/// 记录回合耗时（成功后更新 metrics 与 tracing）
pub fn record_turn_duration(outcome: &'static str, elapsed: Duration) {
    metrics::histogram!("referee_base_turn_duration_seconds", "outcome" => outcome)
        .record(elapsed.as_secs_f64());
}

/// 统计一次回合结束（outcome: success/error/cancelled/timeout/panic）
pub fn turn_completed(outcome: &'static str) {
    metrics::counter!("referee_base_turns_total", "outcome" => outcome).increment(1);
}

/// 统计一次缓存命中 / 未命中
pub fn cache_access(hit: bool) {
    metrics::counter!("referee_base_cache_total", "result" => if hit { "hit" } else { "miss" })
        .increment(1);
}

/// 统计一次工具执行成功 / 失败
pub fn tool_completed(ok: bool) {
    metrics::counter!("referee_base_tool_total", "result" => if ok { "ok" } else { "fail" })
        .increment(1);
}

/// 统计一次引擎层 LLM 重试（可恢复错误触发）
pub fn llm_retry() {
    metrics::counter!("referee_base_llm_retry_total").increment(1);
}

/// 统计一次预算拒绝（kind: session/global）
pub fn budget_rejected(kind: &'static str) {
    metrics::counter!("referee_base_budget_rejected_total", "scope" => kind).increment(1);
}

/// 计时器 — 测量一段执行耗时
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// 启动计时
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// 结束并返回耗时（同时记录 metrics + 可选 span 字段）
    pub fn finish(self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::start()
    }
}

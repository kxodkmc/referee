//! 出站限速退避 — 协议事实来源：`docs/wechat-clawbot-integration.md` §9
//! （社区安全阈值 ≤ 5 条/分钟，微信侧默认基准 12s + 抖动 4s）

use std::time::Duration;

use rand::Rng;

/// 基准间隔 + 随机抖动：模拟真人节奏，规避风控。
/// 用 tokio 时钟而非 std，便于测试以 `start_paused` 虚拟时间推进。
pub struct RateLimiter {
    base: Duration,
    jitter: Duration,
    last: Option<tokio::time::Instant>,
}

impl RateLimiter {
    pub fn new(base: Duration, jitter: Duration) -> Self {
        Self {
            base,
            jitter,
            last: None,
        }
    }

    /// 挂起直到距上次 `wait` 至少 `base + [0, jitter]`；首次调用立即通过
    pub async fn wait(&mut self) {
        let jitter = Duration::from_millis(
            rand::thread_rng().gen_range(0..=self.jitter.as_millis() as u64),
        );
        let min_gap = self.base + jitter;
        if let Some(last) = self.last {
            let elapsed = last.elapsed();
            if elapsed < min_gap {
                tokio::time::sleep(min_gap - elapsed).await;
            }
        }
        self.last = Some(tokio::time::Instant::now());
    }
}

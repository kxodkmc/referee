//! 协议层纯函数单测：版本编码 / 4000 分段 / 限速间隔（A3 单元部分）

use std::time::Duration;

use referee_channel_wechat::client::split_for_wechat;
use referee_channel_wechat::ratelimit::RateLimiter;
use referee_channel_wechat::types::channel_version_u32;

#[test]
fn channel_version_follows_official_u32_layout() {
    assert_eq!(channel_version_u32("2.4.6"), 338_16582);
    assert_eq!(channel_version_u32("2.4.0"), (2 << 24) | (4 << 16));
    assert_eq!(channel_version_u32("1.2"), (1 << 24) | (2 << 16));
    assert_eq!(channel_version_u32(""), 0);
    assert_eq!(channel_version_u32("a.b.c"), 0);
}

#[test]
fn split_respects_4000_char_boundary() {
    let chunks = split_for_wechat(&"你".repeat(4001));
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].chars().count(), 4000);
    assert_eq!(chunks[1], "你");

    assert_eq!(split_for_wechat(&"好".repeat(4000)).len(), 1);
    assert_eq!(split_for_wechat("hello").len(), 1);
}

#[test]
fn split_counts_chars_not_bytes() {
    // emoji 占 4 字节但计 1 字符
    let chunks = split_for_wechat(&"😀".repeat(4001));
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].chars().count(), 4000);
}

#[test]
fn split_empty_yields_single_empty_chunk() {
    assert_eq!(split_for_wechat(""), vec![String::new()]);
}

#[tokio::test(start_paused = true)]
async fn rate_limiter_enforces_minimum_gap() {
    let mut limiter = RateLimiter::new(Duration::from_secs(12), Duration::from_secs(4));
    let mut gaps = Vec::new();
    let mut prev = tokio::time::Instant::now();
    for _ in 0..5 {
        limiter.wait().await;
        let now = tokio::time::Instant::now();
        gaps.push(now - prev);
        prev = now;
    }
    assert!(gaps[0].is_zero(), "首次调用应立即通过");
    for gap in &gaps[1..] {
        assert!(gap >= &Duration::from_secs(12), "间隔 {gap:?} 低于基准 12s");
    }
}

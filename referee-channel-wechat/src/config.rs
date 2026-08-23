//! 微信通道配置——默认值即协议安全预设（限速 ≤5 条/分钟等），全部可覆写

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// 扫码二维码的呈现方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QrRender {
    /// 打印 `qrcode_img_content` 链接，由用户自行生成二维码扫码
    Url,
    /// 终端渲染 ASCII 二维码（需启用 `qr` feature）
    Terminal,
}

impl Default for QrRender {
    #[cfg(feature = "qr")]
    fn default() -> Self {
        Self::Terminal
    }

    #[cfg(not(feature = "qr"))]
    fn default() -> Self {
        Self::Url
    }
}

/// 微信通道配置（serde 可序列化：文件 / 环境注入均可）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WechatConfig {
    /// 凭据与游标的持久化目录——登录一次后重启免扫码
    pub state_dir: PathBuf,
    /// iLink 服务地址。生产恒为官方端点；测试可指向本地 mock
    pub base_url: String,
    /// 出站限速基准间隔（毫秒）。协议安全阈值 ≤5 条/分钟
    pub rate_base_ms: u64,
    /// 出站限速抖动上限（毫秒）
    pub rate_jitter_ms: u64,
    /// 出站线级重试上限（仅瞬时错误；TokenExpired / 服务端拒绝不重试）
    pub send_retries: u32,
    /// 空轮询后的额外等待（毫秒）。真实服务端为 35s 长轮询，默认 0；
    /// 对接即时返回的 mock 端点时设小值避免空转
    pub poll_idle_ms: u64,
    /// 扫码二维码呈现方式
    pub qr_render: QrRender,
}

impl Default for WechatConfig {
    fn default() -> Self {
        Self {
            state_dir: PathBuf::from("wechat-data"),
            base_url: crate::client::BASE_URL.to_owned(),
            rate_base_ms: 12_000,
            rate_jitter_ms: 4_000,
            send_retries: 3,
            poll_idle_ms: 0,
            qr_render: QrRender::default(),
        }
    }
}

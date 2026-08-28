//! 扫码登录——协议文档 §6：
//! get_bot_qrcode → 渲染二维码 → 手机扫码确认 → 轮询 get_qrcode_status → bot_token

use std::time::Duration;

use serde_json::Value;

use crate::config::QrRender;
use crate::state::Credentials;

#[derive(Debug, thiserror::Error)]
pub enum LoginError {
    #[error("login http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("login json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("login response missing field: {0}")]
    MissingField(&'static str),
    #[error("login timeout")]
    Timeout,
}

/// 呈现给用户的二维码信息
pub struct QrView {
    /// get_qrcode_status 的查询参数（32 位十六进制串）
    pub code: String,
    /// 真正要编码进二维码图片的 URL——手机扫的是它，不是 code 本身
    pub img_url: String,
}

/// 扫码登录全流程。二维码由 `present` 回调呈现（可编程获取/转发），
/// 最长等待 `timeout`；超时返回 `LoginError::Timeout`，避免二维码过期后永久轮询。
pub async fn login_via_qr(
    base_url: &str,
    timeout: Duration,
    present: impl Fn(&QrView),
) -> Result<Credentials, LoginError> {
    let http = reqwest::Client::new();
    let resp: Value = http
        .get(format!("{base_url}/ilink/bot/get_bot_qrcode"))
        .query(&[("bot_type", "3")])
        .send()
        .await?
        .json()
        .await?;
    let code = resp["qrcode"]
        .as_str()
        .ok_or(LoginError::MissingField("qrcode"))?
        .to_owned();
    let img_url = resp["qrcode_img_content"]
        .as_str()
        .unwrap_or(&code)
        .to_owned();
    present(&QrView {
        code: code.clone(),
        img_url,
    });

    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(LoginError::Timeout);
        }
        // get_qrcode_status 是阻塞长轮询（等待扫码期间不返回），用 remaining 封顶以强制超时
        let req = async {
            let resp = http
                .get(format!("{base_url}/ilink/bot/get_qrcode_status"))
                .query(&[("qrcode", &code)])
                .send()
                .await
                .map_err(LoginError::from)?;
            resp.json().await.map_err(LoginError::from)
        };
        let status: Value = match tokio::time::timeout(remaining, req).await {
            Ok(res) => res?,
            Err(_) => return Err(LoginError::Timeout),
        };
        // 线上实测字段在根级；官方早期版本包了一层 data——两处都取
        let field = |key: &str| {
            status[key]
                .as_str()
                .or_else(|| status["data"][key].as_str())
        };
        if let Some(token) = field("bot_token") {
            return Ok(Credentials {
                bot_token: token.to_owned(),
                ilink_bot_id: field("ilink_bot_id").unwrap_or_default().to_owned(),
                ilink_user_id: field("ilink_user_id").unwrap_or_default().to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// 默认二维码呈现：feature `qr` 下终端渲染 ASCII，失败/未启用则输出链接
pub fn render_qr(view: &QrView, _render: QrRender) {
    // 终端渲染失败（如内容超二维码容量）时回退为链接输出
    #[cfg(feature = "qr")]
    if matches!(_render, QrRender::Terminal) {
        match qrcode::QrCode::new(view.img_url.as_bytes()) {
            Ok(code) => {
                println!("{}", code.render::<char>().quiet_zone(true).build());
                println!("请使用手机微信「扫一扫」上方二维码并确认授权……");
                return;
            }
            Err(e) => tracing::warn!(error = %e, "二维码编码失败，回退为链接输出"),
        }
    }
    println!(
        "请将下方链接生成为二维码，用手机微信「扫一扫」扫码并确认授权：\n  {}",
        view.img_url
    );
}

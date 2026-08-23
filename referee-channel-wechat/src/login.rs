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
}

/// 呈现给用户的二维码信息
pub struct QrView {
    /// get_qrcode_status 的查询参数（32 位十六进制串）
    pub code: String,
    /// 真正要编码进二维码图片的 URL——手机扫的是它，不是 code 本身
    pub img_url: String,
}

/// 扫码登录全流程。阻塞至用户扫码确认（长轮询），由调用方决定超时策略。
pub async fn login_via_qr(base_url: &str, render: QrRender) -> Result<Credentials, LoginError> {
    // 无超时客户端：get_qrcode_status 等待扫码期间不返回（协议文档 §6 实测）
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
    present_qr(&QrView {
        code: code.clone(),
        img_url,
    }, render);

    loop {
        let status: Value = http
            .get(format!("{base_url}/ilink/bot/get_qrcode_status"))
            .query(&[("qrcode", &code)])
            .send()
            .await?
            .json()
            .await?;
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

fn present_qr(view: &QrView, _render: QrRender) {
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

//! iLink HTTP 客户端（文本收发）— 协议事实来源：`docs/wechat-clawbot-integration.md` §5

use std::time::Duration;

use base64::Engine as _;
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderValue, InvalidHeaderValue, CONTENT_TYPE};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::types::{
    channel_version_u32, BaseInfo, GetUpdatesRequest, GetUpdatesResponse, OutboundItem,
    OutboundMsg, SendMessageRequest, CHANNEL_VERSION, ILINK_APP_ID,
};

pub const BASE_URL: &str = "https://ilinkai.weixin.qq.com";

#[derive(Debug, thiserror::Error)]
pub enum WechatError {
    #[error("ilink http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("ilink json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ilink header value: {0}")]
    InvalidHeader(#[from] InvalidHeaderValue),
    /// context_token 失效：服务端以 errcode = -14 表达（不返回 401）
    #[error("ilink context token expired")]
    TokenExpired,
    #[error("ilink sendmessage rejected: ret={ret} errcode={errcode} body={body}")]
    SendRejected { ret: i64, errcode: i64, body: String },
}

#[derive(Clone)]
pub struct IlinkClient {
    http: reqwest::Client,
    base_url: String,
    bot_token: String,
    bot_agent: String,
}

impl IlinkClient {
    pub fn new(bot_token: impl Into<String>) -> Result<Self, WechatError> {
        Self::with_base_url(bot_token, BASE_URL)
    }

    /// base_url 可注入——测试对接本地 mock 服务端；生产恒为官方端点
    pub fn with_base_url(
        bot_token: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Result<Self, WechatError> {
        // 服务端长轮询 35s，客户端放宽到 45s
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(45))
            .build()?;
        Ok(Self {
            http,
            base_url: base_url.into(),
            bot_token: bot_token.into(),
            bot_agent: "referee-channel-wechat".to_owned(),
        })
    }

    fn base_info(&self) -> BaseInfo<'_> {
        BaseInfo {
            channel_version: CHANNEL_VERSION,
            bot_agent: &self.bot_agent,
        }
    }

    /// 官方编码方式：random u32 → 十进制字符串 → base64
    fn random_uin() -> String {
        let n: u32 = rand::thread_rng().gen();
        base64::engine::general_purpose::STANDARD.encode(n.to_string())
    }

    /// 每个请求携带全部 6 个签名头（对齐官方客户端行为）
    fn signed_headers(&self) -> Result<HeaderMap, WechatError> {
        let mut headers = HeaderMap::with_capacity(6);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "AuthorizationType",
            HeaderValue::from_static("ilink_bot_token"),
        );
        headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", self.bot_token))?,
        );
        headers.insert("X-WECHAT-UIN", HeaderValue::from_str(&Self::random_uin())?);
        headers.insert("iLink-App-Id", HeaderValue::from_static(ILINK_APP_ID));
        let version = channel_version_u32(CHANNEL_VERSION).to_string();
        headers.insert(
            "iLink-App-ClientVersion",
            HeaderValue::from_str(&version)?,
        );
        Ok(headers)
    }

    /// 统一 POST：序列化一次、以字节发送（对齐官方「JSON.stringify 一次」）
    async fn post(&self, path: &str, body: &impl Serialize) -> Result<String, WechatError> {
        let raw = serde_json::to_vec(body)?;
        let resp = self
            .http
            .post(format!("{}{path}", self.base_url))
            .headers(self.signed_headers()?)
            .body(raw)
            .send()
            .await?;
        Ok(resp.text().await?)
    }

    /// 长轮询。无新消息时服务端返回空体或 `{}`，此时游标原样保留。
    pub async fn get_updates(&self, cursor: &str) -> Result<GetUpdatesResponse, WechatError> {
        let body = GetUpdatesRequest {
            get_updates_buf: cursor,
            base_info: self.base_info(),
        };
        let text = self.post("/ilink/bot/getupdates", &body).await?;
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed == "{}" {
            return Ok(GetUpdatesResponse {
                get_updates_buf: cursor.to_owned(),
                ret: 0,
                errcode: 0,
                msgs: Vec::new(),
                extra: Value::Null,
            });
        }
        Ok(serde_json::from_str(trimmed)?)
    }

    /// 发送文本（自动 4000 字符分段）。受理成功 ≠ 投递成功（协议文档 §12-2）。
    pub async fn send_text(
        &self,
        to_user_id: &str,
        context_token: &str,
        text: &str,
    ) -> Result<(), WechatError> {
        for chunk in split_for_wechat(text) {
            let req = SendMessageRequest {
                msg: OutboundMsg {
                    to_user_id,
                    context_token,
                    item_list: vec![OutboundItem::text(chunk)],
                    from_user_id: "",
                    client_id: format!("bot-{}", Uuid::new_v4().simple()),
                    message_type: 2,
                    message_state: 2,
                },
                base_info: self.base_info(),
            };
            self.post_sendmessage(&req).await?;
        }
        Ok(())
    }

    async fn post_sendmessage(&self, req: &SendMessageRequest<'_>) -> Result<(), WechatError> {
        let text = self.post("/ilink/bot/sendmessage", req).await?;
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed == "{}" {
            return Ok(());
        }
        let ack: Value = serde_json::from_str(trimmed)?;
        let ret = ack.get("ret").and_then(Value::as_i64).unwrap_or(0);
        let errcode = ack.get("errcode").and_then(Value::as_i64).unwrap_or(0);
        match (ret, errcode) {
            (0, 0) => Ok(()),
            // 线上实测错误报文为 errcode / errmsg（无效 token 也返回 -14）；官方源码另查 ret——两者都判
            (_, -14) => Err(WechatError::TokenExpired),
            (ret, errcode) => Err(WechatError::SendRejected {
                ret,
                errcode,
                body: trimmed.to_owned(),
            }),
        }
    }
}

/// 社区经验：单条消息 4000 字符上限，按字符（非字节）安全分段；空文本也产出一条空消息
pub fn split_for_wechat(text: &str) -> Vec<String> {
    const MAX_CHARS: usize = 4000;
    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut len = 0;
    for ch in text.chars() {
        if len == MAX_CHARS {
            chunks.push(std::mem::take(&mut current));
            len = 0;
        }
        current.push(ch);
        len += 1;
    }
    chunks.push(current);
    chunks
}

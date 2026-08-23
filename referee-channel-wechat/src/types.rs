//! iLink 协议数据结构（文本链路）— 协议事实来源：`docs/wechat-clawbot-integration.md` §4

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 通道版本线（对应 OpenClaw ≥ 2026.5.12 的 2.x 插件线）
pub const CHANNEL_VERSION: &str = "2.4.6";

/// `iLink-App-Id` 请求头的值（官方插件 package.json 的 ilink_appid）
pub const ILINK_APP_ID: &str = "bot";

/// 官方 versionToUint32 布局 `0x00MM_NNPP`（"2.4.6" → 33816582）
pub fn channel_version_u32(version: &str) -> u32 {
    let mut parts = version.split('.').map(|p| p.parse::<u32>().unwrap_or(0));
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    let patch = parts.next().unwrap_or(0);
    (major << 24) | (minor << 16) | patch
}

// ── 出站：sendMessage ────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct SendMessageRequest<'a> {
    pub msg: OutboundMsg<'a>,
    pub base_info: BaseInfo<'a>,
}

#[derive(Debug, Serialize)]
pub struct BaseInfo<'a> {
    pub channel_version: &'a str,
    /// 自声明客户端标识（类比 User-Agent），仅观测用
    pub bot_agent: &'a str,
}

#[derive(Debug, Serialize)]
pub struct OutboundMsg<'a> {
    pub to_user_id: &'a str,
    /// 会话令牌：来自入站消息，可复用，约 1 小时有效
    pub context_token: &'a str,
    pub item_list: Vec<OutboundItem>,
    /// 以下四字段为协议分析所得的隐藏要求（官方最小示例不含），
    /// 缺失时消息可能被服务端静默丢弃（协议文档 §12-2）
    pub from_user_id: &'a str,
    /// 每条唯一；网络重试时复用同一 id 可借服务端去重实现幂等
    pub client_id: String,
    pub message_type: i32,
    /// 固定 2 = FINISH（流式更新规则未验证，先走最终态，见协议文档 §15）
    pub message_state: i32,
}

#[derive(Debug, Serialize)]
pub struct OutboundItem {
    #[serde(rename = "type")]
    pub item_type: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_item: Option<TextItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_item: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextItem {
    pub text: String,
}

impl OutboundItem {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            item_type: 1,
            text_item: Some(TextItem { text: text.into() }),
            media_item: None,
        }
    }
}

// ── 入站：getUpdates ─────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct GetUpdatesRequest<'a> {
    /// 同步游标：上次响应原样回传；首次空串。必须持久化（重启防重放）
    pub get_updates_buf: &'a str,
    pub base_info: BaseInfo<'a>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetUpdatesResponse {
    #[serde(default)]
    pub get_updates_buf: String,
    #[serde(default)]
    pub ret: i64,
    /// -14 = context_token 失效（会话超时），继续轮询即可拿到新 token，无需重登录
    #[serde(default)]
    pub errcode: i64,
    #[serde(default)]
    pub msgs: Vec<InboundMsg>,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InboundMsg {
    #[serde(default)]
    pub from_user_id: String,
    #[serde(default)]
    pub to_user_id: String,
    #[serde(default)]
    pub client_id: String,
    /// 1 = 用户消息；2 = 自己发出的回环——主循环必须过滤，否则死循环（协议文档 §12-5）
    #[serde(default)]
    pub message_type: i32,
    #[serde(default)]
    pub context_token: String,
    #[serde(default)]
    pub item_list: Vec<InboundItem>,
    // 官方结构另有 seq / message_id / create_time_ms 等，需要排序去重时增补
}

#[derive(Debug, Clone, Deserialize)]
pub struct InboundItem {
    #[serde(rename = "type", default)]
    pub item_type: i32,
    #[serde(default)]
    pub text_item: Option<TextItem>,
    #[serde(default)]
    pub media_item: Option<Value>,
}

impl InboundMsg {
    /// 纯文本内容（多条 text_item 以换行拼接）
    pub fn text(&self) -> String {
        self.item_list
            .iter()
            .filter_map(|item| item.text_item.as_ref())
            .map(|t| t.text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

# 微信 ClawBot Rust 接入文档（iLink AI Bot 协议）

> **文档定位**：面向 Rust 智能体框架，直接对接微信官方 ClawBot 底层的 iLink AI Bot 协议
> （`https://ilinkai.weixin.qq.com`），不依赖 Node.js 运行时。
>
> **可信度标注**：本文已逐条对照官方开源仓库 [Tencent/openclaw-weixin](https://github.com/Tencent/openclaw-weixin)
> 的 `src/api/api.ts`、`src/api/types.ts`、`package.json` 等源码核实（对照时点 2026-08-22，
> main 分支 tree `789146c`，插件版本 2.4.6）。文中标注含义：
>
> - ✅ —— 已对照官方源码证实，可直接信赖
> - ⚠️ —— 来自协议分析或社区经验，官方源码未见，接入时需抓包验证
>
> **重要前提**：iLink 是腾讯内部协议，可能随版本变更且不另行通知。建议接入前通读官方仓库源码
> （`src/api/api.ts` / `src/api/types.ts` / `src/messaging/`），那是最权威的参照物。

---

## 目录

1. [架构选型](#1-架构选型)
2. [环境与依赖](#2-环境与依赖)
3. [协议参考](#3-协议参考)
4. [协议数据结构 types.rs](#4-协议数据结构typesrs)
5. [iLink 客户端 client.rs](#5-ilink-客户端clientrs)
6. [登录授权 login.rs](#6-登录授权loginrs)
7. [媒体上传 media.rs](#7-媒体上传mediars)
8. [智能体桥接与主循环 main.rs](#8-智能体桥接与主循环mainrs)
9. [限速退避 ratelimit.rs](#9-限速退避ratelimitrs)
10. [状态持久化 state.rs](#10-状态持久化staters)
11. [错误处理与重试策略](#11-错误处理与重试策略)
12. [关键陷阱清单](#12-关键陷阱清单)
13. [首次接入验证清单](#13-首次接入验证清单)
14. [合规红线](#14-合规红线)
15. [后续扩展指引（工具化 / AI 经微信工作）](#15-后续扩展指引)

---

## 1. 架构选型

| | 方案 A：Rust 直连 iLink（本文主线） | 方案 B：官方插件旁挂 |
|---|---|---|
| 结构 | Rust 框架内实现 iLink 客户端 | Node 侧跑 OpenClaw + 官方插件收发消息，Rust 框架暴露 OpenAI 兼容 API 作为模型后端 |
| 优点 | 无 Node 依赖、全链路控制、低延迟 | 协议演进由腾讯维护，风险最低 |
| 缺点 | 需自行跟踪协议变更 | 引入 Node 运行时、多一跳转发 |
| 适用 | 追求纯净 Rust 技术栈、深度定制 | 快速上线、生产环境求稳 |

两案可混用：**登录用官方 CLI 完成（最易变的环节交给官方工具），运行期消息收发由 Rust 接管**（见第 6 节）。

## 2. 环境与依赖

前置条件：微信 iOS ≥ 8.0.70 / 安卓 ≥ 8.0.69；Rust ≥ 1.75；⚠️ ClawBot 仅支持单聊、每账号绑一个智能体（社区结论，官方未文档化）。

```toml
# Cargo.toml
[package]
name = "clawbot-rs"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
reqwest = { version = "0.12", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4"] }
rand = "0.8"        # X-WECHAT-UIN 随机数
base64 = "0.22"     # X-WECHAT-UIN / aes_key 编码
anyhow = "1"
async-trait = "0.1" # 智能体桥接 trait

# 仅媒体功能需要（纯文本链路可不引入）：
hex = "0.4"
md-5 = "0.10"       # RustCrypto MD5，上传校验
aes = "0.8"         # RustCrypto AES
ecb = "0.1"         # AES-128-ECB（微信 CDN 媒体加密）
qrcode = "0.14"     # 终端渲染登录二维码
```

> **与 referee 工作区集成的依赖提示**：referee 规范清单内仅有 tokio / reqwest / serde /
> serde_json / uuid 等。文本链路还需 `rand`、`base64` 两个小依赖；媒体与扫码登录另需
> `md-5 / hex / aes / ecb / qrcode`。按工作约束，引入前需确认审批；或将本客户端保持为
> 独立 crate，referee 侧仅依赖其上层接口。

建议工程结构：

```
src/
├── main.rs        # 运行主循环
├── types.rs       # 协议数据结构
├── client.rs      # iLink HTTP 客户端
├── login.rs       # 扫码登录
├── state.rs       # 游标/令牌持久化
├── ratelimit.rs   # 限速退避
└── media.rs       # 媒体上传（AES-128-ECB）
```

## 3. 协议参考

### 3.1 接口列表 ✅（基地址 `https://ilinkai.weixin.qq.com`）

| 接口 | 方法 & 路径 | 用途 |
|---|---|---|
| getUpdates | `POST /ilink/bot/getupdates` | 长轮询接收消息（服务端 35s 超时） |
| sendMessage | `POST /ilink/bot/sendmessage` | 发送消息 |
| getUploadUrl | `POST /ilink/bot/getuploadurl` | 获取 CDN 预签名上传地址 |
| getConfig | `POST /ilink/bot/getconfig` | 获取账号配置（含 typing ticket） |
| sendTyping | `POST /ilink/bot/sendtyping` | 发送/取消"正在输入" |
| get_bot_qrcode | `GET /ilink/bot/get_bot_qrcode?bot_type=3` | 获取登录二维码 |
| get_qrcode_status | `GET /ilink/bot/get_qrcode_status?qrcode=xxx` | 轮询扫码状态 |
| notifyStart | `POST /ilink/bot/msg/notifystart` | （辅助）上线通知，官方客户端启停时调用 |
| notifyStop | `POST /ilink/bot/msg/notifystop` | （辅助）下线通知 |

### 3.2 鉴权头 ✅（每个请求都带，共 6 个）

```
Content-Type:           application/json
AuthorizationType:      ilink_bot_token
Authorization:          Bearer <bot_token>
X-WECHAT-UIN:           <base64(random u32 的十进制字符串)>   ← 每次请求重新生成
iLink-App-Id:           bot          ← 官方插件 package.json 的 ilink_appid 字段值
iLink-App-ClientVersion: <uint32>    ← (major<<24)|(minor<<16)|patch
```

> 注意两个容易遗漏的头：
>
> - `iLink-App-Id`：官方插件固定取其 package.json 顶层 `ilink_appid` 字段，当前值为 `bot`。
> - `iLink-App-ClientVersion`：版本号编码为 uint32。以 2.4.6 为例：
>   `(2<<24)|(4<<16)|6 = 33816582`。
>
> 另有可选头 `SKRouteTag`（官方从配置读取，非必需）。
>
> ✅ 线上实测（2026-08-22，无效 token 探测）：缺失 `iLink-App-Id` /
> `iLink-App-ClientVersion` 时鉴权层响应不变（仍返回 errcode=-14），服务端不强制校验
> 这两个头；但官方客户端总是携带，建议保持一致。

### 3.3 关键枚举 ✅（对照官方 types.ts）

| 枚举 | 取值 |
|---|---|
| `message_type` | 1 = USER（用户消息），2 = BOT（机器人消息） |
| `message_state` | 0 = NEW（新建），1 = GENERATING（生成中），2 = FINISH（完成） |
| `item.type` | 1 = 文本，2 = 图片，3 = 语音（SILK），4 = 文件，5 = 视频；另有 11 = TOOL_CALL_START，12 = TOOL_CALL_RESULT |
| `media_type`（上传用） | 1 = 图片，2 = 视频，3 = 文件，4 = 语音 |

### 3.4 会话令牌 context_token ✅

来自入站消息，**可复用**（非一次性），有效期约 1 小时，复用则服务端会话延续。必须随消息持久化；丢失后只能等用户再发消息。这是实现"会话内主动续话"的关键（见第 15 节）。

## 4. 协议数据结构（types.rs）

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 通道版本号：跟随所用插件版本线。官方实现直接取插件自身版本（当前 2.x 线，如 2.4.6）。
/// ✅ 2.x 线对应 OpenClaw >= 2026.5.12（package.json peerDependencies）。
pub const CHANNEL_VERSION: &str = "2.4.6";

/// ✅ iLink-App-Id 请求头的值，来自官方插件 package.json 的 ilink_appid 字段。
pub const ILINK_APP_ID: &str = "bot";

/// ✅ 官方 versionToUint32 逻辑：0x00MM_NNPP
pub fn channel_version_u32(version: &str) -> u32 {
    let mut it = version.split('.').map(|p| p.parse::<u32>().unwrap_or(0));
    let (major, minor, patch) = (
        it.next().unwrap_or(0),
        it.next().unwrap_or(0),
        it.next().unwrap_or(0),
    );
    (major << 24) | (minor << 16) | patch
}

// ==================== 出站：sendMessage ====================

#[derive(Debug, Serialize)]
pub struct SendMessageRequest<'a> {
    pub msg: OutboundMsg<'a>,
    pub base_info: BaseInfo<'a>,
}

/// ✅ 官方 base_info 同时携带 channel_version 与 bot_agent 两个字段
#[derive(Debug, Serialize)]
pub struct BaseInfo<'a> {
    pub channel_version: &'a str,
    /// 自声明客户端标识（类比 User-Agent），仅用于观测。官方默认 "OpenClaw"
    pub bot_agent: &'a str,
}

#[derive(Debug, Serialize)]
pub struct OutboundMsg<'a> {
    pub to_user_id: &'a str,
    /// 会话令牌，来自入站消息，可复用（约 1 小时有效期）
    pub context_token: &'a str,
    pub item_list: Vec<OutboundItem>,
    // ---- ⚠️ 以下字段来自协议分析；官方 README 的最小出站示例仅含
    //      to_user_id / context_token / item_list 三项。首次接入请抓包比对，
    //      缺失或错误时消息可能被服务端静默丢弃 ----
    pub from_user_id: &'a str, // 空串
    pub client_id: String,     // 每条消息唯一；网络重试时复用同一 id 可借助服务端去重实现幂等
    pub message_type: i32,     // 固定 2（BOT）
    pub message_state: i32,    // 简单场景固定 2（FINISH）；流式见第 15 节
}

#[derive(Debug, Serialize)]
pub struct OutboundItem {
    #[serde(rename = "type")]
    pub item_type: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_item: Option<TextItem>,
    /// ⚠️ 官方类型定义中媒体项按 image_item / voice_item / file_item / video_item
    /// 分字段（另有 ref_msg 引用结构）；此处以通用 Value 承载，结构以抓包为准
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_item: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

// ==================== 入站：getUpdates ====================

#[derive(Debug, Serialize)]
pub struct GetUpdatesRequest<'a> {
    /// 同步游标：上次响应原样回传；首次传空串。务必持久化
    pub get_updates_buf: &'a str,
    /// ✅ 官方实现的 getupdates 请求同样携带 base_info
    pub base_info: BaseInfo<'a>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetUpdatesResponse {
    #[serde(default)]
    pub get_updates_buf: String,
    #[serde(default)]
    pub ret: i64,
    #[serde(default)]
    pub errcode: i64,
    /// ✅ 官方字段名为 msgs（注意：不是 msg_list）
    #[serde(default)]
    pub msgs: Vec<InboundMsg>,
    /// 捕获未知字段，便于调试对照
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
    /// 1 = 用户消息；2 = 自己发出的回环，主循环必须过滤，否则死循环
    #[serde(default)]
    pub message_type: i32,
    #[serde(default)]
    pub context_token: String,
    #[serde(default)]
    pub item_list: Vec<InboundItem>,
    // 官方结构还含 seq / message_id / create_time_ms / session_id / group_id 等，
    // 需要排序或去重时按需增补（官方按 seq 升序处理）
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
    /// 提取纯文本（多条 text_item 以换行拼接）
    pub fn text(&self) -> String {
        self.item_list
            .iter()
            .filter_map(|i| i.text_item.as_ref())
            .map(|t| t.text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// ==================== typing（✅ 字段名已对照官方 types.ts） ====================

#[derive(Debug, Clone, Default, Deserialize)]
pub struct GetConfigResponse {
    /// ⚠️ ticket 在响应中的确切路径以实际响应为准，首次运行打印原始 JSON 核对
    #[serde(default)]
    pub typing_ticket: String,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Serialize)]
pub struct SendTypingRequest<'a> {
    /// 注意：官方字段是 ilink_user_id（不是 to_user_id）
    pub ilink_user_id: &'a str,
    /// 必须先通过 getConfig 获取
    pub typing_ticket: &'a str,
    /// 1 = 正在输入；2 = 取消
    pub status: i32,
}

pub mod typing_status {
    pub const TYPING: i32 = 1;
    pub const CANCEL: i32 = 2;
}

// ==================== 媒体上传 getUploadUrl（✅ 字段名已对照官方 types.ts） ====================

#[derive(Debug, Serialize)]
pub struct GetUploadUrlRequest<'a> {
    pub filekey: &'a str,
    /// 1 = 图片，2 = 视频，3 = 文件
    pub media_type: i32,
    pub to_user_id: &'a str,
    /// 明文大小 / 明文 MD5
    pub rawsize: u64,
    pub rawfilemd5: &'a str,
    /// 密文大小（AES-128-ECB + PKCS7 后）。注意：官方无密文 MD5 字段
    pub filesize: u64,
    /// 缩略图三参数（图片/视频需要；无缩略图时配合 no_need_thumb = true）
    pub thumb_rawsize: u64,
    pub thumb_rawfilemd5: &'a str,
    pub thumb_filesize: u64,
    pub no_need_thumb: bool,
    /// Base64 编码的 16 字节 AES 密钥
    pub aeskey: &'a str,
    pub base_info: BaseInfo<'a>,
}
```

## 5. iLink 客户端（client.rs）

```rust
use std::time::Duration;

use base64::Engine as _;
use rand::Rng;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::types::*;

/// ✅ 官方 DEFAULT_BASE_URL
pub const BASE_URL: &str = "https://ilinkai.weixin.qq.com";
/// ✅ 官方 CDN_BASE_URL（媒体中转）
pub const CDN_BASE_URL: &str = "https://novac2c.cdn.weixin.qq.com/c2c";

pub struct IlinkClient {
    http: reqwest::Client,
    pub bot_token: String,
    /// 自声明标识（base_info.bot_agent），类比 User-Agent
    pub bot_agent: String,
}

impl IlinkClient {
    pub fn new(bot_token: impl Into<String>) -> Self {
        Self {
            // 长轮询服务端 35s 超时（官方 longpolling_timeout_ms = 35000），客户端放宽到 45s
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(45))
                .build()
                .expect("构建 HTTP 客户端失败"),
            bot_token: bot_token.into(),
            bot_agent: "clawbot-rs".to_string(),
        }
    }

    fn base_info(&self) -> BaseInfo<'_> {
        BaseInfo {
            channel_version: CHANNEL_VERSION,
            bot_agent: &self.bot_agent,
        }
    }

    /// ✅ 官方编码方式：random u32 → 十进制字符串 → base64
    fn random_uin() -> String {
        let n: u32 = rand::thread_rng().gen();
        base64::engine::general_purpose::STANDARD.encode(n.to_string())
    }

    /// ✅ 官方客户端每个请求都携带全部 6 个头
    fn signed_headers(&self) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        h.insert("AuthorizationType", HeaderValue::from_static("ilink_bot_token"));
        h.insert(
            "Authorization",
            format!("Bearer {}", self.bot_token).parse().unwrap(),
        );
        h.insert("X-WECHAT-UIN", Self::random_uin().parse().unwrap());
        h.insert("iLink-App-Id", ILINK_APP_ID.parse().unwrap());
        h.insert(
            "iLink-App-ClientVersion",
            channel_version_u32(CHANNEL_VERSION)
                .to_string()
                .parse()
                .unwrap(),
        );
        h
    }

    /// 统一 POST：序列化一次、以字节发送（与官方实现一致：JSON.stringify 一次后作为 body）
    async fn post(&self, path: &str, body: &impl Serialize) -> anyhow::Result<String> {
        let raw = serde_json::to_vec(body)?;
        let resp = self
            .http
            .post(format!("{BASE_URL}{path}"))
            .headers(self.signed_headers())
            .body(raw)
            .send()
            .await?;
        Ok(resp.text().await?)
    }

    /// 长轮询。cursor 首次传空串；超时无新消息时服务端返回空体或 "{}"
    pub async fn get_updates(&self, cursor: &str) -> anyhow::Result<GetUpdatesResponse> {
        let body = GetUpdatesRequest {
            get_updates_buf: cursor,
            base_info: self.base_info(),
        };
        let text = self.post("/ilink/bot/getupdates", &body).await?;
        if text.trim().is_empty() || text.trim() == "{}" {
            return Ok(GetUpdatesResponse {
                get_updates_buf: cursor.to_string(),
                ret: 0,
                errcode: 0,
                msgs: Vec::new(),
                extra: Value::Null,
            });
        }
        // 首次接入建议打开此日志，确认字段名与 types.rs 一致
        // eprintln!("[getupdates] {text}");
        Ok(serde_json::from_str(&text)?)
    }

    /// 发送文本。⚠️ 受理成功 ≠ 投递成功，隐藏字段缺失时可能被静默丢弃（见第 12 节）
    pub async fn send_text(
        &self,
        to_user_id: &str,
        context_token: &str,
        text: &str,
    ) -> anyhow::Result<()> {
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
            let resp_text = self.post("/ilink/bot/sendmessage", &req).await?;
            if !resp_text.trim().is_empty() && resp_text.trim() != "{}" {
                let v: Value = serde_json::from_str(&resp_text)?;
                // ✅ 线上实测错误报文字段为 errcode / errmsg（无效 token 也返回
                //    errcode=-14）；官方源码另检查 ret 字段——两者都查最稳
                let ret = v.get("ret").and_then(|e| e.as_i64()).unwrap_or(0);
                let errcode = v.get("errcode").and_then(|e| e.as_i64()).unwrap_or(0);
                if ret != 0 || errcode != 0 {
                    anyhow::bail!("sendmessage ret={ret} errcode={errcode}: {resp_text}");
                }
            }
        }
        Ok(())
    }

    /// 获取账号配置（含 typing ticket）
    pub async fn get_config(&self) -> anyhow::Result<GetConfigResponse> {
        let text = self
            .post("/ilink/bot/getconfig", &serde_json::json!({}))
            .await?;
        Ok(serde_json::from_str(&text)?)
    }

    /// "正在输入"状态。typing = false 取消。
    /// ✅ 官方要求先 getConfig 取 typing_ticket；status：1 = 输入中，2 = 取消
    pub async fn send_typing(
        &self,
        ilink_user_id: &str,
        typing_ticket: &str,
        typing: bool,
    ) -> anyhow::Result<()> {
        let body = SendTypingRequest {
            ilink_user_id,
            typing_ticket,
            status: if typing { typing_status::TYPING } else { typing_status::CANCEL },
        };
        self.post("/ilink/bot/sendtyping", &body).await?;
        Ok(())
    }
}

/// ⚠️ 社区经验：单条消息 4000 字符上限，按字符（非字节）安全分段
pub fn split_for_wechat(text: &str) -> Vec<String> {
    const MAX: usize = 4000;
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in text.chars() {
        if cur.chars().count() >= MAX {
            out.push(std::mem::take(&mut cur));
        }
        cur.push(ch);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}
```

**关于 Content-Length**：序列化一次、以 `Vec<u8>` 发送 body 即可（官方实现同样是序列化一次后
作为 body 传递）。reqwest 会基于实际发送的字节自动填入精确的 Content-Length；使用 `.json(&body)`
也不会造成长度错位，但统一"序列化一次"便于日志与调试对照。

## 6. 登录授权（login.rs）

```rust
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::client::BASE_URL;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    pub bot_token: String,     // API 认证令牌
    pub ilink_bot_id: String,  // 机器人账号 ID
    pub ilink_user_id: String, // 绑定用户的微信 ID（形如 xxx@im.wechat）
}

/// 扫码登录全流程：
/// get_bot_qrcode → 终端渲染二维码 → 用户手机微信扫码确认 →
/// get_qrcode_status 轮询 → 提取 bot_token
/// ✅ 端点与参数（bot_type=3 / qrcode=xxx）已对照官方 login-qr.ts
pub async fn login_via_qr(http: &reqwest::Client) -> anyhow::Result<Credentials> {
    // 1. 申请二维码
    let resp: Value = http
        .get(format!("{BASE_URL}/ilink/bot/get_bot_qrcode"))
        .query(&[("bot_type", "3")])
        .send()
        .await?
        .json()
        .await?;
    eprintln!("[login] get_bot_qrcode 响应:\n{resp:#}"); // 首次运行必看
    // ✅ 线上实测（2026-08-22）响应为 { qrcode, qrcode_img_content, ret }：
    //    qrcode —— 32 位十六进制串，用作 get_qrcode_status 的查询参数；
    //    qrcode_img_content —— 真正要渲染进二维码图片的 URL
    let qr = resp["qrcode"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("无法解析 qrcode 字段，请对照上方日志修正取值路径"))?;
    let qr_img = resp["qrcode_img_content"].as_str().unwrap_or(qr).to_string();

    // 2. 终端渲染二维码（渲染 qrcode_img_content，而不是 qrcode 本身——手机扫不出 hex 串）
    let code = qrcode::QrCode::new(qr_img.as_bytes())?;
    println!("{}", code.render::<char>().quiet_zone(true).build());
    println!("请使用手机微信「扫一扫」上方二维码并确认授权……");

    // 3. 轮询扫码状态。
    //    ✅ 线上实测该端点为长轮询（等待扫码事件期间不返回），HTTP 客户端
    //    不宜设短超时——本例用无超时的 reqwest::Client::new()（见 main.rs）
    loop {
        let status: Value = http
            .get(format!("{BASE_URL}/ilink/bot/get_qrcode_status"))
            .query(&[("qrcode", &qr)])
            .send()
            .await?
            .json()
            .await?;
        if let Some(token) = status["bot_token"]
            .as_str()
            .or_else(|| status["data"]["bot_token"].as_str())
        {
            return Ok(Credentials {
                bot_token: token.to_string(),
                ilink_bot_id: status["ilink_bot_id"]
                    .as_str()
                    .or_else(|| status["data"]["ilink_bot_id"].as_str())
                    .unwrap_or_default()
                    .to_string(),
                ilink_user_id: status["ilink_user_id"]
                    .as_str()
                    .or_else(|| status["data"]["ilink_user_id"].as_str())
                    .unwrap_or_default()
                    .to_string(),
            });
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
```

**生产环境推荐**：用官方 CLI 完成扫码登录，从插件本地状态中取出 `bot_token` 写入 Rust 侧凭据文件
（登录是最易变的环节，交给官方工具最省心），运行期消息收发仍由 Rust 接管：

```bash
npx -y @tencent-weixin/openclaw-weixin-cli install
openclaw channels login --channel openclaw-weixin
```

凭据存储位置参考官方仓库 README（`accounts.ts` 负责读写）。

## 7. 媒体上传（media.rs）

图片/语音/文件/视频经微信 CDN 中转，全程 **AES-128-ECB + PKCS7 加密**（✅ 官方证实）。
加密部分实现是确定的，请求字段名已对照官方 `types.ts` 修正：

```rust
use aes::Aes128;
use ecb::cipher::{block_padding::Pkcs7, KeyInit};
use ecb::Encryptor;
use md5::{Digest, Md5};

pub fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// 微信 CDN 媒体加密：AES-128-ECB + PKCS7
pub fn aes_ecb_encrypt(key: &[u8], plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
    let enc = Encryptor::<Aes128>::new_from_slice(key)
        .map_err(|e| anyhow::anyhow!("AES 密钥须为 16 字节: {e}"))?;
    Ok(enc.encrypt_padded_vec_mut::<Pkcs7>(plaintext))
}

/// 上传四步（以发送图片为例，media_type = 1）：
/// ① 生成 16 字节随机 AES 密钥；计算明文大小/MD5；加密得到密文及密文大小
/// ② POST /ilink/bot/getuploadurl，携带 ✅ 官方字段：
///    filekey / media_type / to_user_id / rawsize / rawfilemd5 / filesize /
///    thumb_rawsize / thumb_rawfilemd5 / thumb_filesize / no_need_thumb / aeskey
///    （注意：无密文 MD5 字段；官方字段名无下划线，如 rawsize 而非 raw_file_size）
/// ③ 从响应的 upload_param（及 thumb_upload_param）取出预签名 URL，
///    PUT 密文到该 URL，Content-Length = 密文长度
/// ④ 构造 item_list 中的媒体项：CDN 引用（encrypt_query_param）
///    + aes_key（Base64 的 16 字节密钥），type = 2/3/4/5
///    ⚠️ 官方类型中媒体项按 image_item / voice_item / file_item / video_item
///    分字段，确切结构以抓包为准
pub async fn upload_media(
    _http: &reqwest::Client,
    _bot_token: &str,
    _media_type: i32, // 1 = 图片，2 = 视频，3 = 文件
    _plaintext: &[u8],
    _aes_key: &[u8; 16],
) -> anyhow::Result<String> {
    unimplemented!("按上方四步注释补全，字段名对照实际响应")
}
```

语音消息为 SILK 编码，需额外引入 SILK 编解码库（官方开发依赖为 silk-wasm）；纯文本智能体可暂不实现类型 3/4/5。

## 8. 智能体桥接与主循环（main.rs）

```rust
mod client;
mod login;
mod media;
mod ratelimit;
mod state;
mod types;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::client::IlinkClient;
use crate::login::Credentials;
use crate::types::InboundMsg;

const STATE_FILE: &str = "clawbot-state.json";
const TOKEN_FILE: &str = "clawbot-token.json";

// ---------- 智能体接入点：把此 trait 实现挂到你的框架上 ----------
#[async_trait]
pub trait ClawBotAgent: Send + Sync {
    /// 返回 Some(text) 则回复；返回 None 表示静默处理
    async fn on_message(&self, msg: &InboundMsg) -> Option<String>;
}

/// 示例实现：替换为你框架的调度逻辑
struct EchoAgent;

#[async_trait]
impl ClawBotAgent for EchoAgent {
    async fn on_message(&self, msg: &InboundMsg) -> Option<String> {
        let text = msg.text();
        if text.is_empty() {
            return None;
        }
        Some(format!("（智能体回复）收到：{text}"))
    }
}

// ---------- 主循环 ----------
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 载入或新建登录凭据
    let creds: Credentials = match std::fs::read_to_string(TOKEN_FILE) {
        Ok(s) => serde_json::from_str(&s)?,
        Err(_) => {
            let http = reqwest::Client::new();
            let c = login::login_via_qr(&http).await?;
            std::fs::write(TOKEN_FILE, serde_json::to_string_pretty(&c)?)?;
            c
        }
    };
    let client = IlinkClient::new(&creds.bot_token);
    let agent: Arc<dyn ClawBotAgent> = Arc::new(EchoAgent);
    let mut bot_state = state::BotState::load(STATE_FILE)?;

    // ⚠️ 社区安全阈值：≤ 5 条/分钟。基准 12s + 抖动 4s → 实际间隔 12~16s
    let mut limiter = ratelimit::RateLimiter::new(
        Duration::from_secs(12),
        Duration::from_secs(4),
    );

    // typing ticket：启动时取一次，失效时重取（⚠️ 有效期以实测为准）
    let mut typing_ticket = client.get_config().await?.typing_ticket;

    println!("ClawBot 已就绪，开始长轮询……");
    loop {
        match client.get_updates(&bot_state.cursor).await {
            Ok(resp) => {
                // 2. 推进并持久化游标（重启不重放消息）
                if !resp.get_updates_buf.is_empty() {
                    bot_state.cursor = resp.get_updates_buf.clone();
                    bot_state.save(STATE_FILE)?;
                }
                for msg in resp.msgs {
                    // 3. 过滤回环：只处理用户消息（message_type = 1）
                    if msg.message_type != 1 {
                        continue;
                    }
                    println!("[收到] from={}: {}", msg.from_user_id, msg.text());

                    // 4. 派发给智能体
                    let Some(reply) = agent.on_message(&msg).await else { continue };

                    // 限速后回复；回复前可先 send_typing 提升体验
                    if client
                        .send_typing(&msg.from_user_id, &typing_ticket, true)
                        .await
                        .is_err()
                    {
                        // ticket 可能过期，重取一次
                        if let Ok(cfg) = client.get_config().await {
                            typing_ticket = cfg.typing_ticket;
                        }
                    }
                    limiter.wait().await;
                    client.send_text(&msg.from_user_id, &msg.context_token, &reply).await?;
                    client
                        .send_typing(&msg.from_user_id, &typing_ticket, false)
                        .await
                        .ok();

                    // 5. 记录会话令牌（context_token 可复用，支持会话内续话）
                    bot_state
                        .context_tokens
                        .insert(msg.from_user_id.clone(), msg.context_token.clone());
                    bot_state.save(STATE_FILE)?;
                }
            }
            Err(e) => {
                eprintln!("[轮询异常] {e:#}，3 秒后重试");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}
```

## 9. 限速退避（ratelimit.rs）

```rust
use std::time::{Duration, Instant};

use rand::Rng;

/// 基准间隔 + 随机抖动，模拟真人节奏，规避风控
pub struct RateLimiter {
    base: Duration,
    jitter: Duration,
    last: Option<Instant>,
}

impl RateLimiter {
    pub fn new(base: Duration, jitter: Duration) -> Self {
        Self { base, jitter, last: None }
    }

    pub async fn wait(&mut self) {
        let jitter_ms = rand::thread_rng().gen_range(0..=self.jitter.as_millis() as u64);
        let target = self.base + Duration::from_millis(jitter_ms);
        if let Some(last) = self.last {
            let elapsed = last.elapsed();
            if elapsed < target {
                tokio::time::sleep(target - elapsed).await;
            }
        }
        self.last = Some(Instant::now());
    }
}
```

## 10. 状态持久化（state.rs）

```rust
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BotState {
    /// getUpdates 游标：重启续传，避免消息重放
    pub cursor: String,
    /// peer → 最近一次 context_token（令牌可复用，有效期约 1 小时）
    pub context_tokens: HashMap<String, String>,
}

impl BotState {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => Ok(serde_json::from_str(&s)?),
            Err(_) => Ok(Self::default()),
        }
    }

    pub fn save(&self, path: &str) -> anyhow::Result<()> {
        Ok(std::fs::write(path, serde_json::to_string_pretty(self)?)?)
    }
}
```

## 11. 错误处理与重试策略

| 现象 | 含义 | 处理 |
|---|---|---|
| sendMessage 返回 `{}` 或空体 | 受理成功 | ≠ 投递成功——缺隐藏字段时可能静默丢弃；自检 `from_user_id` 为空串、`client_id` / `message_type` / `message_state` / `base_info.channel_version` / `context_token` 齐全（⚠️ 隐藏字段要求来自协议分析） |
| sendMessage 响应 `ret` / `errcode` != 0 | 出错 | ✅ 线上实测错误报文为 `errcode` / `errmsg`（官方源码检查 `ret`），两者都查，按 errmsg 排查 |
| getUpdates 响应 `errcode = -14` | 会话超时 | ✅ 无需重新登录，继续 getUpdates 即可拿到新 context_token |
| 持续 `errcode = -14`（长时间无消息） | 登录态丢失 | ✅ 实测无效 token 同样返回 -14（**不返回 401**）；持续 -14 时重新走扫码流程刷新 bot_token |
| 网络超时（getUpdates） | 服务端 35s 无消息 | 正常现象，携带旧游标重发 |
| 发送重试 | — | 复用同一 `client_id`，借助服务端去重实现幂等（⚠️） |

## 12. 关键陷阱清单

1. **消息列表字段名**：✅ 响应字段是 `msgs`，不是 `msg_list`（本档代码已修正）。
2. **请求头不全**：✅ 除四个常见头外，官方每个请求还带 `iLink-App-Id: bot` 与
   `iLink-App-ClientVersion`（版本编码 uint32）。实测缺失时鉴权层不拒绝，但建议与官方一致。
3. **sendTyping 报文**：✅ 官方为 `{ ilink_user_id, typing_ticket, status }`（1 = 输入中，
   2 = 取消），ticket 须先经 getConfig 获取；`to_user_id / context_token / typing` 结构不成立。
4. **getUploadUrl 字段名**：✅ 官方为 `filekey / rawsize / rawfilemd5 / filesize / thumb_* /
   no_need_thumb / aeskey`（无下划线风格，且没有密文 MD5 字段）。
5. **回环消息**：不按 `message_type == 1` 过滤入站消息会自己回复自己，死循环 + 触发风控。
6. **游标丢失**：游标不持久化，重启后会重放旧消息。
7. **context_token 误判一次性**：它可复用（约 1 小时），但必须随消息持久化；丢失后只能等用户再发消息。
8. **HTTP 200 ≠ 成功**：所有"看起来成功"的响应都要按第 11 节复核。
9. **版本线匹配**：`base_info.channel_version` 应与实际绑定的插件版本线一致（当前 2.x 线 ↔
   OpenClaw >= 2026.5.12；官方取插件自身版本号）。
10. **多账号**：每个微信账号独立 token 与游标；如需多账号，为每个账号实例化独立的
    `IlinkClient` + `BotState`。

## 13. 首次接入验证清单

- [ ] 打开 getUpdates 原始响应日志，确认 `msgs` 等字段名与 types.rs 一致，不一致则修正结构体
- [ ] 登录流程打印 get_bot_qrcode / get_qrcode_status 原始 JSON，校正字段取值路径
- [ ] 用手机发一条消息，确认长轮询能收到、`message_type == 1` 过滤生效
- [ ] 回复一条测试消息，确认隐藏字段齐全（对方真实收到）；若未收到，逐项排查第 12 节清单
- [ ] 抓包一次官方插件的 sendTyping / getUploadUrl 请求，复核字段名
- [ ] 压测限速：连续 10 条消息观察是否被丢弃，据此微调 RateLimiter 参数
- [ ] 模拟进程重启，验证游标续传与 token 复用

## 14. 合规红线（务必写入框架默认行为）

以下均为 ⚠️ 社区/协议分析结论，官方未文档化，但违反风险高：

- **仅单聊**：协议能力元数据未声明群聊，任何变通发群消息都可能触发风控。
- **频率**：默认 ≤ 5 条/分钟、间隔 ≥ 15 秒（本文限速器默认 12s + 4s 抖动）。
- **单条消息 ≤ 4000 字符**（已在 `split_for_wechat` 处理）。
- **不支持主动推送**：只能在用户发起会话后回复（context_token 有效窗口内的续话除外）。
- **一账号一智能体**：与其他平台机器人同时绑定会互相顶替会话。
- **协议风险**：iLink 为腾讯内部协议，可能随时变更且不另行通知；建议在客户端层预留协议版本
  抽象，并订阅官方仓库 release。

## 15. 后续扩展指引

本节面向规划中的"微信接入服务"：把发消息做成工具、让 AI 通过微信工作。第 8 节的
`ClawBotAgent` trait 与 `IlinkClient` 的公开方法即为扩展锚点。

### 15.1 出站工具化（AI 主动发消息）

将 `IlinkClient` 的能力包装为工具（tool）暴露给模型：

| 工具 | 对应客户端方法 | 参数设计要点 |
|---|---|---|
| `wechat_send_text` | `send_text` | 模型侧只给 `peer`（联系人标识）与文本；`context_token` 由服务端从 `BotState.context_tokens` 解析，**不暴露给模型** |
| `wechat_send_typing` | `send_typing` | 同上；ticket 由服务端管理 |
| `wechat_upload_media` | `upload_media` | 接受本地文件路径或字节，返回 CDN 引用供消息组装 |

要点：`context_token` 有效期约 1 小时，工具执行前应检查令牌新鲜度（建议在 `BotState` 中随令牌
记录获取时间）；令牌过期时工具应返回明确错误，提示"等待用户再次发消息"。

### 15.2 入站驱动（微信消息 → AI 工作 → 微信回复）

第 8 节主循环即事件源。接入智能体框架时，把 `on_message` 实现替换为框架的调度逻辑：

```rust
#[async_trait]
impl ClawBotAgent for MyFrameworkAgent {
    async fn on_message(&self, msg: &InboundMsg) -> Option<String> {
        // 1. msg.text() / item_list 解析用户输入（文本 / 媒体引用）
        // 2. 交给框架的会话引擎处理（可调用工具、可长耗时）
        // 3. 汇总结果作为回复文本返回
        // 注意：长耗时任务应在发送 typing 后异步执行，避免阻塞轮询循环
        //（生产实现建议把消息投入队列，由独立 worker 消费，见下）
        todo!()
    }
}
```

生产建议：主循环只做「收消息 → 入队 → 应答 ACK」，worker 从队列消费并调用智能体，避免单条
长耗时消息阻塞后续轮询（也符合有界队列的背压约束）。

### 15.3 流式回复（可选进阶）

协议的 `message_state` 支持三态（0 = NEW，1 = GENERATING，2 = FINISH，✅ 官方枚举）。长回复可
先发 `message_state = 1` 的增量分片，最后以 `message_state = 2` 收尾，实现"逐字输出"体验。
⚠️ 分片规则（同一 `client_id` 还是每片独立）未见官方文档，接入前抓包比对官方插件的流式行为。

### 15.4 与 referee 工作区集成

- **内核不承载业务**（AGENTS.md 约束）：本客户端应作为独立 crate，或挂在 referee-agent
  层作为通道扩展/工具提供方，不进 referee-core。
- **依赖边界**：文本链路在规范清单外仅需 `rand`、`base64`；媒体与扫码登录另需
  `md-5 / hex / aes / ecb / qrcode`。引入前按工作约束确认。
- **背压**：入站消息队列必须有界；缓冲满时按内核语义返回 `ResourceExhausted`，配合第 9 节
  限速器天然削峰。
- **多账号**：每账号一个 `IlinkClient` + `BotState` 实例，映射为独立扩展实例即可获得
  Panic 隔离与独立治理。

---

## 参考资源

- 官方插件源码（字段名最权威参照）：<https://github.com/Tencent/openclaw-weixin>
  - 协议核心：`src/api/api.ts`（请求头 / 端点 / 错误检查）、`src/api/types.ts`（全部报文结构）
  - 登录：`src/auth/login-qr.ts`、凭据：`src/auth/accounts.ts`
  - 收发：`src/messaging/inbound.ts`、`src/messaging/send.ts`
- npm 包：`@tencent-weixin/openclaw-weixin` / `@tencent-weixin/openclaw-weixin-cli`
- OpenClaw 文档渠道页：<https://docs.openclaw.ai/channels/wechat>

## 修订记录

| 日期 | 说明 |
|---|---|
| 2026-08-22 | 依据官方仓库 main 分支（tree `789146c`，插件 2.4.6）全面核对并修正：`msgs` 字段名、sendTyping / getUploadUrl 报文、6 个鉴权头（补 `iLink-App-Id` / `iLink-App-ClientVersion`）、getUpdates 携带 base_info、sendMessage 错误字段；新增扩展指引一节 |
| 2026-08-22 | 线上实测（无登录态探针，5 项探测）：`get_bot_qrcode` 返回 `qrcode` / `qrcode_img_content` / `ret`，二维码须渲染 `qrcode_img_content`（URL）而非 `qrcode`（hex 串）；`get_qrcode_status` 为长轮询；`getupdates` / `sendmessage` 错误报文字段实测均为 `errcode` / `errmsg`，无效 token 返回 `errcode = -14` 而非 401；文档 `GetUpdatesResponse` 结构经真实报文反序列化验证兼容 |

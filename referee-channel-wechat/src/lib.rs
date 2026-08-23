//! 微信 iLink 通道适配器（referee-channel-wechat）。
//!
//! ## 开箱即用
//! ```no_run
//! # async fn demo() -> Result<(), referee_channel_wechat::ConnectError> {
//! use referee_channel::ChannelHost;
//! use referee_channel_wechat::{WechatAdapter, WechatConfig};
//!
//! let adapter = WechatAdapter::connect(WechatConfig::default()).await?; // 凭据在则复用，无则扫码
//! let host = ChannelHost::new(adapter, 64, 64); // 入站/出站容量
//! # Ok(())
//! # }
//! ```
//! 此后按 `referee-channel` 的通用契约组装（`Kernel::register` + `host.start`），
//! 参见 `examples/echo.rs`——任何实现了 `Extension` 的「大脑」（Agent、规则逻辑、
//! MCP/Skill 桥）都能以同样方式接入，通道层零改动。
//!
//! ## 集成面
//! - 预设：`WechatConfig::default()` 内置协议安全参数（限速 ≤5 条/分钟、
//!   线级重试 3 次、4000 分段），serde 可序列化，按需覆写。
//! - 持久化：凭据一次落盘重启免扫码；游标/会话令牌随收发即时落盘，崩溃不丢已确认消息。
//! - 协议事实来源：`docs/wechat-clawbot-integration.md`。

pub mod client;
pub mod config;
pub mod login;
pub mod ratelimit;
pub mod state;
pub mod types;

pub use client::{split_for_wechat, IlinkClient, WechatError};
pub use config::{QrRender, WechatConfig};
pub use login::{login_via_qr, LoginError, QrView};
pub use state::{Credentials, WechatState};

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

use referee_channel::adapter::{AdapterError, AdapterState, ChannelAdapter, ChannelIo};
use referee_channel::message::{ChannelCapabilities, ChannelContent, InboundMessage, OutboundCommand};

use crate::ratelimit::RateLimiter;

/// 连接阶段错误（登录 / 客户端构建 / 状态目录）
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("wechat login: {0}")]
    Login(#[from] LoginError),
    #[error("wechat client: {0}")]
    Client(#[from] WechatError),
    #[error("wechat state: {0}")]
    State(#[from] AdapterError),
}

pub struct WechatAdapter {
    config: WechatConfig,
    client: IlinkClient,
    state: Arc<WechatState>,
    endpoint: String,
}

impl WechatAdapter {
    /// 开箱即用入口：凭据存在则复用（免扫码），否则走扫码登录并落盘。
    pub async fn connect(config: WechatConfig) -> Result<Self, ConnectError> {
        let creds = match Credentials::load(&config.state_dir) {
            Some(creds) => creds,
            None => {
                let creds = login_via_qr(&config.base_url, config.qr_render).await?;
                creds.save(&config.state_dir)?;
                creds
            }
        };
        Self::with_credentials(config, creds).await
    }

    /// 已持凭据的入口——例如官方 CLI 登录后导出（协议文档 §6「生产环境推荐」）
    pub async fn with_credentials(config: WechatConfig, creds: Credentials) -> Result<Self, ConnectError> {
        let client = IlinkClient::with_base_url(&creds.bot_token, config.base_url.clone())?;
        let state = WechatState::load(&config.state_dir)?;
        Ok(Self {
            endpoint: format!("wechat/{}", creds.ilink_bot_id),
            config,
            client,
            state,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[async_trait]
impl ChannelAdapter for WechatAdapter {
    fn kind(&self) -> &'static str {
        "wechat"
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            max_text_len: 4000,
            batch_idle_window_ms: 8000,
            max_batch_messages: 10,
            max_batch_window_ms: 30000,
            supports_typing: false,
            stream_update: false,
        }
    }

    fn state(&self) -> Arc<dyn AdapterState> {
        self.state.clone()
    }

    async fn run(&self, io: ChannelIo) -> Result<(), AdapterError> {
        // 收/发拆为并行子任务：长轮询期间出站不受阻塞，反之亦然。
        // 任一子任务 panic/失败 → run 返回 Err → host 监督接管（退避重启/降级）。
        let poll = {
            let (client, state, endpoint) =
                (self.client.clone(), self.state.clone(), self.endpoint.clone());
            let (inbound_tx, shutdown, idle) =
                (io.inbound_tx.clone(), io.shutdown.clone(), self.config.poll_idle_ms);
            tokio::spawn(async move {
                poll_loop(client, state, endpoint, inbound_tx, shutdown, idle).await
            })
        };
        let send = {
            let (client, state) = (self.client.clone(), self.state.clone());
            let limiter = RateLimiter::new(
                Duration::from_millis(self.config.rate_base_ms),
                Duration::from_millis(self.config.rate_jitter_ms),
            );
            let (outbound_rx, shutdown, retries) =
                (io.outbound_rx, io.shutdown.clone(), self.config.send_retries);
            tokio::spawn(async move {
                send_loop(client, state, limiter, outbound_rx, shutdown, retries).await
            })
        };
        let (poll, send) = tokio::join!(poll, send);
        for result in [poll, send] {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(join) => return Err(join.into()),
            }
        }
        Ok(())
    }
}

/// 长轮询循环：回环过滤 → 投递有界入站通道（背压点）→ 游标/令牌即时落盘。
/// 网络/协议错误不熔断通道（warn + 1s 后重试）——只有 panic 会被 host 监督捕获。
async fn poll_loop(
    client: IlinkClient,
    state: Arc<WechatState>,
    endpoint: String,
    inbound_tx: mpsc::Sender<InboundMessage>,
    mut shutdown: watch::Receiver<bool>,
    poll_idle_ms: u64,
) -> Result<(), AdapterError> {
    let mut expired_streak = 0u32;
    loop {
        let resp = match client.get_updates(&state.cursor()).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(error = %e, "get_updates failed, retry in 1s");
                if sleep_or_shutdown(Duration::from_secs(1), &mut shutdown).await {
                    return Ok(());
                }
                continue;
            }
        };
        if resp.errcode == -14 {
            // 会话令牌失效：继续轮询即可随下一条用户消息自愈（协议文档 §11）
            expired_streak += 1;
            if expired_streak % 10 == 1 {
                tracing::error!(
                    streak = expired_streak,
                    "get_updates sustained errcode=-14：若长时间无消息，请重新扫码登录"
                );
            }
            if sleep_or_shutdown(Duration::from_secs(1), &mut shutdown).await {
                return Ok(());
            }
            continue;
        }
        expired_streak = 0;

        let mut tokens = Vec::with_capacity(resp.msgs.len());
        for (index, msg) in resp.msgs.iter().enumerate() {
            if msg.message_type != 1 {
                continue; // 回环过滤：BOT 消息（含自己发出的），不过滤会死循环
            }
            // 任何用户消息都刷新会话令牌（含纯媒体消息，令牌才不会过期）
            tokens.push((msg.from_user_id.clone(), msg.context_token.clone()));
            let text = msg.text();
            if text.is_empty() {
                tracing::debug!(peer = %msg.from_user_id, "非文本消息暂不支持（Phase 2 媒体）");
                continue;
            }
            // 背压点：send 成功（host 已接手）之后才允许推进游标
            if inbound_tx
                .send(InboundMessage {
                    endpoint: endpoint.clone(),
                    peer: msg.from_user_id.clone(),
                    message_id: if msg.client_id.is_empty() {
                        format!("{}-{index}", state.cursor())
                    } else {
                        msg.client_id.clone()
                    },
                    content: ChannelContent::Text(text),
                    session_ctx: msg.context_token.clone(),
                    occurred_at: now_ms(),
                    raw: None,
                })
                .await
                .is_err()
            {
                return Ok(()); // 接收端已关闭 = host 停机
            }
        }
        state.advance(&resp.get_updates_buf, &tokens)?;
        if resp.msgs.is_empty() && poll_idle_ms > 0 {
            if sleep_or_shutdown(Duration::from_millis(poll_idle_ms), &mut shutdown).await {
                return Ok(());
            }
        }
    }
}

/// 出站循环：取件 → 限速 → 落线（瞬时错误重试 ≤ retries；令牌过期/服务端拒绝放弃并记录）
async fn send_loop(
    client: IlinkClient,
    state: Arc<WechatState>,
    mut limiter: RateLimiter,
    mut outbound_rx: mpsc::Receiver<OutboundCommand>,
    mut shutdown: watch::Receiver<bool>,
    retries: u32,
) -> Result<(), AdapterError> {
    loop {
        let cmd = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            cmd = outbound_rx.recv() => match cmd {
                Some(cmd) => cmd,
                None => return Ok(()),
            },
        };
        let Some(token) = state.context_token(&cmd.peer) else {
            tracing::warn!(peer = %cmd.peer, "出站丢弃：该 peer 尚未发过消息，无会话令牌");
            continue;
        };
        let ChannelContent::Text(text) = cmd.content else {
            tracing::warn!(peer = %cmd.peer, "出站丢弃：媒体发送为 Phase 2 能力");
            continue;
        };
        tokio::select! {
            _ = limiter.wait() => {}
            _ = shutdown.changed() => return Ok(()),
        }
        send_with_retry(&client, &cmd.peer, &token, &text, retries).await;
    }
}

async fn send_with_retry(
    client: &IlinkClient,
    peer: &str,
    token: &str,
    text: &str,
    retries: u32,
) {
    let mut attempt = 0u32;
    loop {
        match client.send_text(peer, token, text).await {
            Ok(()) => return,
            Err(WechatError::TokenExpired) => {
                tracing::error!(peer, "出站失败：会话令牌过期（用户下次发消息自愈；补投见 Phase 2）");
                return;
            }
            Err(WechatError::SendRejected { ret, errcode, body }) => {
                tracing::error!(peer, ret, errcode, body, "出站被服务端拒绝");
                return;
            }
            Err(e) => {
                attempt += 1;
                if attempt > retries {
                    tracing::error!(peer, attempt, error = %e, "出站重试耗尽，放弃本条");
                    return;
                }
                tracing::warn!(peer, attempt, error = %e, "出站瞬时错误，退避重试");
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }
        }
    }
}

/// 到点返回 false；期间停机返回 true
async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => *shutdown.borrow(),
        _ = shutdown.changed() => *shutdown.borrow(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

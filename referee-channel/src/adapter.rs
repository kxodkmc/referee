//! 适配器契约 — 设计文档 `docs/channel-execution.md` §4.2
//!
//! 每通道一个实现（如 `referee-channel-wechat`），传输方式无关
//! （长轮询 / webhook / websocket 均可）。通道由 host 创建并注入 `run`。

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

use crate::message::{ChannelCapabilities, InboundMessage, OutboundCommand};

/// 适配器错误：实现方自有错误装箱传递；host 只区分「运行终止」与「panic」
pub type AdapterError = Box<dyn std::error::Error + Send + Sync>;

/// 游标/凭据持久化句柄——生命周期独立于 `run` 循环：run panic 后 host 仍可 flush。
#[async_trait]
pub trait AdapterState: Send + Sync {
    /// 幂等落盘：连调多次不产生重复写入
    async fn flush(&self) -> Result<(), AdapterError>;
}

/// 通道适配器
#[async_trait]
pub trait ChannelAdapter: Send + Sync {
    fn kind(&self) -> &'static str; // "wechat" / "feishu" / "qq"
    fn capabilities(&self) -> ChannelCapabilities;
    /// 持久化句柄（host 在 shutdown 时 flush）
    fn state(&self) -> Arc<dyn AdapterState>;

    /// 长期运行：登录/重连 + 收发循环，直到 shutdown 置位。义务：
    /// - 入站消息 send 进 `inbound_tx` 成功后，才允许推进并持久化游标（背压点）
    /// - 出站命令从 `outbound_rx` 逐条取件、限速后落线（线级失败自行重试）
    /// - 自维护 peer→会话句柄映射（随入站消息刷新）
    /// - panic 只允许发生在本方法内（host 经 JoinHandle 监督，§4.6b）
    async fn run(&self, io: ChannelIo) -> Result<(), AdapterError>;
}

/// host ↔ adapter 通信端点。注意：adapter 自己的持久化经 `self.state()` 访问，
/// 不经 io——io 只承载通道两端。
pub struct ChannelIo {
    pub inbound_tx: mpsc::Sender<InboundMessage>,
    /// 每次运行尝试一个新 Receiver（panic 会连带消费掉旧的）；host 侧同步换新 Sender
    pub outbound_rx: mpsc::Receiver<OutboundCommand>,
    pub shutdown: watch::Receiver<bool>,
}

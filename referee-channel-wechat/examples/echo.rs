//! 端到端集成示例——微信 echo 机器人，兼作 A3 真机验证乎架。
//!
//! 演示「任意大脑」接入通道的标准范式：EchoBrain 是一个普通 Extension，
//! 消费 `im.inbound`、经 `kernel.invoke(host, im.send)` 回话。把 EchoBrain
//! 换成 Agent / 规则引擎 / MCP 桥，通道层零改动。
//!
//! 运行：
//! ```text
//! cargo run -p referee-channel-wechat --example echo                 # 二维码以链接输出
//! cargo run -p referee-channel-wechat --example echo --features qr   # 终端渲染二维码
//! WECHAT_STATE_DIR=./data cargo run ...                              # 自定义持久化目录
//! ```
//! 真机验证（对照 docs/channel-execution.md A3）：扫码登录落盘 → 重启免扫码 →
//! 手机发消息收到 echo 回复 → 自己网页端发的内容不回环 → 重启后旧消息不重放。

use async_trait::async_trait;
use referee_channel::message::{ChannelContent, InboundMessage, OutboundCommand};
use referee_channel::ChannelHost;
use referee_channel_wechat::{WechatAdapter, WechatConfig};
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelResult, SupervisionPolicy,
};

/// 最小「大脑」：收到什么回什么。真正的 Agent 以同样契约接入。
struct EchoBrain {
    id: CapabilityId,
    host_id: CapabilityId,
    kernel: Kernel,
}

#[async_trait]
impl Extension for EchoBrain {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, _ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let Ok(msg) = InboundMessage::from_envelope(&env) else {
            return Ok(());
        };
        let ChannelContent::Text(text) = msg.content else {
            return Ok(());
        };
        let reply = OutboundCommand {
            endpoint: msg.endpoint,
            peer: msg.peer,
            content: ChannelContent::Text(format!("echo: {text}")),
        };
        // handle 必须非阻塞：im.send 受理是等待型原语，移交后台任务
        let (kernel, host_id) = (self.kernel.clone(), self.host_id);
        tokio::spawn(async move {
            if let Err(e) = kernel.invoke(host_id, reply.to_system_envelope(), 5_000).await {
                tracing::warn!(error = ?e, "回话受理失败");
            }
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let state_dir =
        std::env::var("WECHAT_STATE_DIR").unwrap_or_else(|_| "wechat-data".to_owned());
    let config = WechatConfig {
        state_dir: state_dir.into(),
        ..Default::default()
    };
    let adapter = WechatAdapter::connect(config).await?;
    println!("微信通道已连接（{}），启动 echo……", adapter.endpoint());

    let kernel = Kernel::new();
    let host = ChannelHost::new(adapter, 64, 64);
    let host_id = host.id();
    let brain = EchoBrain {
        id: CapabilityId::new(),
        host_id,
        kernel: kernel.clone(),
    };
    let brain_id = brain.id;
    kernel
        .register(Box::new(brain), 16, SupervisionPolicy::Transient)
        .await?;
    kernel
        .register(Box::new(host.clone()), 16, SupervisionPolicy::Transient)
        .await?;
    host.start(kernel.clone(), brain_id);

    println!("已上线：给机器人发消息将收到 echo 回复，Ctrl+C 退出。");
    tokio::signal::ctrl_c().await?;
    host.shutdown().await;
    Ok(())
}

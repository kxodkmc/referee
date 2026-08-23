//! A5 端到端乎架——微信 × 智能体的完整闭环（真机验收用）。
//!
//! 组装顺序即 §4.5：agent → router → host → host.start（router 先于 host 注册，
//! host 的 im.inbound 才不会落 DLQ）。批次累积 8s 静默闭合；模型可在回合内用
//! `im_send_text` 发中间回执；最终输出由 router 兜底管道确定交付。
//!
//! 运行：
//! ```text
//! DEEPSEEK_API_KEY=sk-... cargo run -p referee-channel-wechat --example agent
//! # 可选：DEEPSEEK_MODEL=pro（默认 flash）、WECHAT_STATE_DIR=./wechat-data、--features qr
//! ```
//! A5 真机清单：连发两条 ≤8s → 合并一个回复；中间回执 ≤10s；结果恰好一条；
//! 连续 5 个任务依次完成、出站间隔 ≥12s；kill 重启后旧消息不重放、凭据免扫码。

use std::sync::Arc;
use std::time::Duration;

use referee_agent::AgentRuntime;
use referee_channel::batch::BatchConfig;
use referee_channel::{ChannelHost, ImRouter, ImRouterConfig, ImSendText};
use referee_channel_wechat::{WechatAdapter, WechatConfig};
use referee_core::{Extension, Kernel, SupervisionPolicy};
use referee_ai::engine::{Engine, EngineConfig};
use referee_ai::provider::deepseek::{DeepSeekConfig, DeepSeekModel, DeepSeekProvider};
use referee_ai::tool::{ToolExecutor, ToolRegistry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let api_key = std::env::var("DEEPSEEK_API_KEY")
        .map_err(|_| "请设置 DEEPSEEK_API_KEY（厂商按需替换，见 referee-ai provider 模块）")?;
    let model = match std::env::var("DEEPSEEK_MODEL").as_deref() {
        Ok("pro") => DeepSeekModel::V4Pro,
        _ => DeepSeekModel::V4Flash,
    };

    let state_dir =
        std::env::var("WECHAT_STATE_DIR").unwrap_or_else(|_| "wechat-data".to_owned());
    let adapter = WechatAdapter::connect(WechatConfig {
        state_dir: state_dir.into(),
        ..Default::default()
    })
    .await?;

    let kernel = Kernel::new();

    // 智能体：引擎 + 工具执行器注入 kernel（im_send_text 经它 invoke host）
    let provider = DeepSeekProvider::new(model, DeepSeekConfig::new(api_key))?;
    let engine = Engine::new(Arc::new(provider), EngineConfig::default()).with_tools(
        ToolRegistry::with_defaults(),
        ToolExecutor::with_defaults().with_kernel(kernel.clone()),
    );
    let runtime = AgentRuntime::new(engine);
    let agent_id = runtime.id();

    // host 先构造（id 供 router 配置与工具使用，注册放在 router 之后）
    let host = ChannelHost::new(adapter, 64, 64);
    let host_id = host.id();

    // 路由：批次 → 调度 → 交付契约
    let router = ImRouter::new(
        kernel.clone(),
        ImRouterConfig {
            hosts: vec![host_id],
            agent: agent_id,
            batch: BatchConfig {
                idle_window: Duration::from_secs(8),
                max_messages: 10,
                max_window: Duration::from_secs(30),
            },
            concurrency: 3,
            task_queue: 64,
            chat_timeout_ms: 600_000,
            send_timeout_ms: 5_000,
            interrupt_keywords: vec![],
        },
    );
    let router_id = router.id();
    let session_map = router.session_map();

    // 工具依赖 router 的会话映射：在 runtime 注册前注入（流量尚未开始，顺序安全）
    runtime.register_tool(Arc::new(ImSendText::new(
        kernel.clone(),
        host_id,
        session_map,
    )))?;

    // 注册顺序：agent → router → host（router 先于 host，im.inbound 不落 DLQ）
    kernel
        .register(Box::new(runtime), 8, SupervisionPolicy::Transient)
        .await?;
    kernel
        .register(Box::new(router), 16, SupervisionPolicy::Transient)
        .await?;
    kernel
        .register(Box::new(host.clone()), 16, SupervisionPolicy::Transient)
        .await?;
    host.start(kernel.clone(), router_id);

    println!("微信智能体已上线（Ctrl+C 退出）");
    tokio::signal::ctrl_c().await?;
    host.shutdown().await;
    Ok(())
}

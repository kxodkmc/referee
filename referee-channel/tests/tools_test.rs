//! A5 前置（自动化）：im_send_text 工具——收件人反查 / 回合归因 / 错误路径。
//! A5 其余项为真机验收，乎架见 referee-channel-wechat/examples/agent.rs。

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::json;
use uuid::Uuid;

use referee_channel::message::{meta, ChannelContent, OutboundCommand, PeerKey, SendReceipt};
use referee_channel::{ImRouter, ImRouterConfig, ImSendText, SessionMap};
use referee_ai::{Tool, ToolContext, ToolError};
use referee_core::{
    CapabilityId, Envelope, Extension, Kernel, KernelContext, KernelResult, SupervisionPolicy,
};

/// 记录出站请求（含完整信封 metadata），可配置回执
struct MockHost {
    id: CapabilityId,
    requests: Mutex<Vec<(OutboundCommand, Option<(Uuid, Option<u64>)>)>>,
    reject_next: Mutex<bool>,
}

#[async_trait]
impl Extension for MockHost {
    fn id(&self) -> CapabilityId {
        self.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        let cmd = OutboundCommand::from_envelope(&env).ok();
        let attribution = env.metadata.get(meta::SESSION_ID).and_then(|s| {
            let turn = env
                .metadata
                .get(meta::TURN_ID)
                .and_then(|t| t.parse::<u64>().ok());
            Uuid::parse_str(s).ok().map(|session| (session, turn))
        });
        if let Some(cmd) = cmd {
            self.requests.lock().push((cmd, attribution));
        }
        let accepted = !*self.reject_next.lock();
        let _ = ctx.reply(
            SendReceipt {
                accepted,
                queue_depth: 0,
            }
            .to_envelope(),
        );
        Ok(())
    }
}

fn tool_context(session_id: Uuid) -> ToolContext {
    ToolContext {
        tool_call_id: "call-1".into(),
        session_id,
        turn_id: 7,
        kernel: None,
        store: None,
        wait: false,
        peer_depth: 0,
    }
}

/// 建一套 kernel + router（提供真实 SessionMap）+ mock host + 工具
async fn setup() -> (Kernel, Arc<MockHost>, ImSendText, SessionMap) {
    let kernel = Kernel::new();
    let host = Arc::new(MockHost {
        id: CapabilityId::new(),
        requests: Mutex::new(Vec::new()),
        reject_next: Mutex::new(false),
    });
    kernel
        .register(Box::new(HostExt(host.clone())), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    let router = ImRouter::new(
        kernel.clone(),
        ImRouterConfig {
            hosts: vec![host.id()],
            agent: CapabilityId::new(), // 本测试不派发任务，无需真实 agent
            batch: referee_channel::BatchConfig {
                idle_window: std::time::Duration::from_secs(8),
                max_messages: 10,
                max_window: std::time::Duration::from_secs(30),
            },
            concurrency: 2,
            task_queue: 8,
            chat_timeout_ms: 1_000,
            send_timeout_ms: 2_000,
            interrupt_keywords: vec![],
        },
    );
    let sessions = router.session_map();
    kernel
        .register(Box::new(router), 8, SupervisionPolicy::Transient)
        .await
        .unwrap();
    let tool = ImSendText::new(kernel.clone(), host.id(), sessions.clone());
    (kernel, host, tool, sessions)
}

/// Arc<MockHost> 的 Extension 转发壳（Arc 不可直接 Box 注册）
struct HostExt(Arc<MockHost>);

#[async_trait]
impl Extension for HostExt {
    fn id(&self) -> CapabilityId {
        self.0.id
    }

    async fn handle(&self, ctx: KernelContext, env: Envelope) -> KernelResult<()> {
        self.0.handle(ctx, env).await
    }
}

#[tokio::test]
async fn sends_to_mapped_peer_with_attribution() {
    let (_kernel, host, tool, sessions) = setup().await;
    let session = sessions.session_of(&PeerKey {
        endpoint: "wechat/bid-1".into(),
        peer: "user-甲".into(),
    });

    let output = tool
        .execute(tool_context(session), json!({"text": "正在查询…"}))
        .await
        .unwrap();
    assert_eq!(output.content, "已送达通道");

    let requests = host.requests.lock();
    assert_eq!(requests.len(), 1);
    let (cmd, attribution) = &requests[0];
    assert_eq!(cmd.peer, "user-甲");
    assert_eq!(cmd.endpoint, "wechat/bid-1");
    assert_eq!(cmd.content, ChannelContent::Text("正在查询…".into()));
    assert_eq!(*attribution, Some((session, Some(7))), "归因 session+turn 供 im.sent 观测");
}

#[tokio::test]
async fn unknown_session_and_blank_text_are_rejected() {
    let (_kernel, _host, tool, _sessions) = setup().await;

    // 会话无 IM 对端（模型无从伪造收件人）
    let err = tool
        .execute(tool_context(Uuid::new_v4()), json!({"text": "hi"}))
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::Execution(_)));

    // 空文本 / 缺字段
    assert!(matches!(
        tool.execute(tool_context(Uuid::new_v4()), json!({"text": "  "})).await,
        Err(ToolError::InvalidArguments(_))
    ));
    assert!(matches!(
        tool.execute(tool_context(Uuid::new_v4()), json!({})).await,
        Err(ToolError::InvalidArguments(_))
    ));
}

#[tokio::test]
async fn rejected_receipt_surfaces_as_error() {
    let (_kernel, host, tool, sessions) = setup().await;
    let session = sessions.session_of(&PeerKey {
        endpoint: "wechat/bid-1".into(),
        peer: "user-乙".into(),
    });
    *host.reject_next.lock() = true;

    let err = tool
        .execute(tool_context(session), json!({"text": "队列满测试"}))
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::Execution(_)));
}

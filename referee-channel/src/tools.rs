//! im_send_text —— 回合内的中间回执 / 主动汇报工具（设计文档 §4.6c）。
//!
//! 安全边界：参数只有 `text`，收件人由 `ctx.session_id` 经共享会话映射反查，
//! 模型无法选择发送对象。交付契约：最终答案不经过本工具——由 router 兜底
//! 管道确定交付，工具 description 对模型显式约束。

use async_trait::async_trait;
use serde_json::{json, Value};

use referee_ai::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
use referee_core::{CapabilityId, Kernel};

use crate::message::{ChannelContent, OutboundCommand, SendReceipt};
use crate::router::SessionMap;

const DEFAULT_TIMEOUT_MS: u64 = 5_000;

pub struct ImSendText {
    kernel: Kernel,
    host: CapabilityId,
    sessions: SessionMap,
    timeout_ms: u64,
}

impl ImSendText {
    /// `sessions` 必须与 ImRouter 共享同一映射（`router.session_map()`）
    pub fn new(kernel: Kernel, host: CapabilityId, sessions: SessionMap) -> Self {
        Self {
            kernel,
            host,
            sessions,
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

#[async_trait]
impl Tool for ImSendText {
    fn name(&self) -> &str {
        "im_send_text"
    }

    fn description(&self) -> &str {
        "向当前对话的用户发送一条即时消息。仅用于中间进展、回执或主动汇报；\
         最终答案不要用本工具——直接把最终答案作为你的回复输出返回，系统会自动送达用户。"
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "text": { "type": "string", "description": "要发送的消息文本" }
            },
            "required": ["text"]
        })
    }

    /// 出站为外部通道副作用，走 Remote 分类（异步派发不阻塞回合）
    fn category(&self) -> ToolCategory {
        ToolCategory::Remote
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        let text = args
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| ToolError::InvalidArguments("缺少非空 text 字段".into()))?;
        let peer = self
            .sessions
            .peer_of(&ctx.session_id)
            .ok_or_else(|| ToolError::Execution("当前会话没有关联的 IM 对端".into()))?;
        let cmd = OutboundCommand {
            endpoint: peer.endpoint,
            peer: peer.peer,
            content: ChannelContent::Text(text.to_owned()),
        };
        let env = cmd.to_send_envelope(ctx.session_id, Some(ctx.turn_id));
        let resp = self
            .kernel
            .invoke(self.host, env, self.timeout_ms)
            .await
            .map_err(|e| ToolError::Execution(format!("通道不可达：{e}")))?;
        match SendReceipt::from_envelope(&resp) {
            Ok(receipt) if receipt.accepted => Ok(ToolOutput::text("已送达通道")),
            Ok(_) => Err(ToolError::Execution(
                "通道当前繁忙，消息未送达，稍后由最终回复补发".into(),
            )),
            Err(e) => Err(ToolError::Execution(format!("通道未受理：{e}"))),
        }
    }
}

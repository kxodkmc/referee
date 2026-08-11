//! 对等智能体工具 — 将一个 Session 暴露为 Local 工具（Agent as Tool）
//!
//! ## 设计要点
//! - **Local 分类**：`category() = ToolCategory::Local`，不占用 `ToolExecutor`
//!   的外部 IO 槽位——对等调用等待目标 Agent 完成时，不会阻塞目标 Agent
//!   自身执行 HTTP 等 Remote 工具的槽位（资源池死锁修复）。
//! - **同步 RPC**：`execute` 经 `ToolContext.kernel.invoke` 发起阻塞式
//!   请求-响应调用（带超时）。invoke 在派生任务（turn task）中执行，
//!   不违反「扩展 `handle` 内零阻塞」约束。
//! - **循环调用拒绝**：目标 Agent 忙碌（Thinking / AwaitingCalls）时返回
//!   `SessionReply::Busy`，本工具转为错误结果回传，系统不会挂死（DAG 约束）。
//! - **大结果落库**：返回文本超过阈值时写入 `ArtifactStore`，并将调用者
//!   显式加入 `allowed_readers`（ACL 授权），仅返回 Artifact ID 回传 LLM。

use std::collections::HashSet;
use std::time::SystemTime;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::artifact::{Artifact, StoreError};
use crate::provider::Message;
use crate::session::{ChatPayload, SessionMessage, SessionReply};
use crate::tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};

/// 大结果落库阈值（字节）— 超过则存 Artifact，仅回传 ID
const LARGE_RESULT_THRESHOLD: usize = 4096;
/// 默认 RPC 超时（毫秒）
const DEFAULT_RPC_TIMEOUT_MS: u64 = 30_000;

/// 对等智能体工具 — 将一个目标 Runtime 上的 Session 暴露为工具
pub struct AgentTool {
    name: String,
    description: String,
    /// 目标 Runtime（注册了 `target_session_id` 的扩展）
    runtime_id: referee_core::CapabilityId,
    /// 目标 Session
    target_session_id: uuid::Uuid,
    /// RPC 超时（毫秒）
    timeout_ms: u64,
}

impl AgentTool {
    /// 构造对等工具
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        runtime_id: referee_core::CapabilityId,
        target_session_id: uuid::Uuid,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            runtime_id,
            target_session_id,
            timeout_ms: DEFAULT_RPC_TIMEOUT_MS,
        }
    }

    /// 覆盖 RPC 超时（毫秒）
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

#[async_trait]
impl Tool for AgentTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    /// 声明为 Local：不阻塞 ToolExecutor 的 IO 槽位
    fn category(&self) -> ToolCategory {
        ToolCategory::Local
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "task": { "type": "string", "description": "任务描述" }
            },
            "required": ["task"]
        })
    }

    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        // 1. 提取任务
        let task = args
            .get("task")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ToolError::InvalidArguments("missing task".into()))?;

        // 2. 对等能力校验（ToolExecutor 未注入 kernel 时不可用）
        let kernel = ctx.kernel.as_ref().ok_or_else(|| {
            ToolError::Execution("peer RPC not enabled: kernel not injected".into())
        })?;

        // 3. 发起同步 RPC 调用（不占用 ToolExecutor 槽位）
        let msg = SessionMessage::Chat {
            session_id: self.target_session_id,
            payload: ChatPayload {
                message: Message::user(task),
                options: Default::default(),
            },
        };
        let resp_env = kernel
            .invoke(self.runtime_id, msg.to_envelope(), self.timeout_ms)
            .await
            .map_err(|e| ToolError::Execution(format!("peer RPC failed: {e}")))?;

        // 4. 解析回信
        let reply = SessionReply::from_envelope(&resp_env)
            .map_err(|e| ToolError::Execution(format!("decode peer reply failed: {e}")))?;

        match reply {
            SessionReply::Success { message, .. } => {
                let content = message.content.as_text().unwrap_or("").to_string();

                // 5. 大结果落库：写入 ArtifactStore + 显式授权调用者读取
                if content.len() > LARGE_RESULT_THRESHOLD {
                    if let Some(store) = &ctx.artifact_store {
                        let artifact = Artifact {
                            id: uuid::Uuid::new_v4().to_string(),
                            // 内容由目标 Agent 产出，归属目标 Session
                            owner: self.target_session_id,
                            allowed_readers: HashSet::from([ctx.session_id]),
                            content_type: "text/plain".into(),
                            bytes: content.into_bytes(),
                            created_at: SystemTime::now(),
                        };
                        return match store.store(artifact).await {
                            Ok(artifact_id) => {
                                Ok(ToolOutput::text(format!("Artifact created: {artifact_id}")))
                            }
                            Err(StoreError::CapacityExceeded) => {
                                Err(ToolError::Execution("artifact store full".into()))
                            }
                            Err(e) => {
                                Err(ToolError::Execution(format!("artifact store error: {e}")))
                            }
                        };
                    }
                }
                Ok(ToolOutput::text(content))
            }
            reply => Err(ToolError::Execution(format!(
                "peer agent did not succeed: {reply:?}"
            ))),
        }
    }
}

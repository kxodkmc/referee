//! 对等智能体工具 — 将一个 Session 暴露为 Local 工具（Agent as Tool）
//!
//! 业务层能力：把一个 Agent 会话封装为另一个 Agent 可调用的工具，实现
//! 子 Agent / 多 Agent 协作。基于 `referee-ai-base` 的统一 `Tool` 抽象接入。
//!
//! ## 设计要点
//! - **Local 分类**：`category() = Local`，不占用 `ToolExecutor` 外部 IO 槽位，
//!   避免对等调用相互等待外部 IO 槽位的资源池死锁。
//! - **同步 RPC**：`execute` 经 `ToolContext.kernel.invoke` 发起请求-响应调用
//!   （带超时）。invoke 在派生任务（引擎回合）中执行，不违反「handle 内零阻塞」。
//! - **循环调用拒绝**：目标 Agent 忙碌时返回 `Busy` → 转为错误结果，系统不挂死。
//! - **大结果落库**：返回文本超阈值时写入带 ACL 的 [`ArtifactStore`]，并将调用者
//!   显式加入 `allowed_readers`，仅回传 Artifact ID（业务层安全能力）。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use referee_ai_base::provider::Message;
use referee_ai_base::session::{ChatPayload, SessionMessage, SessionReply};
use referee_ai_base::tool::{Tool, ToolCategory, ToolContext, ToolError, ToolOutput};
use serde_json::{json, Value};

use crate::artifact::{Artifact, ArtifactStore, StoreError};

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
    /// 带 ACL 的工件存储（大结果落库；未注入则仅返回原文）
    artifact_store: Option<Arc<dyn ArtifactStore>>,
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
            artifact_store: None,
        }
    }

    /// 覆盖 RPC 超时（毫秒）
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// 注入 ACL 工件存储（业务层：大结果落库 + 授权读取）
    pub fn with_artifact_store(mut self, store: Arc<dyn ArtifactStore>) -> Self {
        self.artifact_store = Some(store);
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

        // 2. 对等能力校验（引擎未注入 kernel 时不可用）
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

                // 5. 大结果落库：写入 ArtifactStore + 显式授权调用者读取（业务层 ACL）
                if content.len() > LARGE_RESULT_THRESHOLD {
                    if let Some(store) = &self.artifact_store {
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

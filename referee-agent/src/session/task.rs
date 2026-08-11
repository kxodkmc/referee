//! 终态自管 wrapper — 派生任务的安全边界
//!
//! 所有 LLM 调用等异步长耗时操作必须经 [`run_turn`] 包裹，保证四路径
//! （Ok / Err / Cancelled / Panic）都收敛为 [`TurnOutcome`]，由调用方
//! 在 finally 中做唯一一次终态写入。
//!
//! ## 设计约束（对应 AGENT_RUNTIME_PLAN §2）
//! - **终态自管**（第 1 条）：`catch_unwind` 捕获 panic，绝外泄
//! - **协作式取消唯一**（第 2 条）：仅 `oneshot` 通道，不用 `abort()`
//! - **超时治理**：`tokio::time::timeout` 切断挂死调用
//!
//! ## 为什么不用 `JoinHandle::abort()`
//! abort 强杀任务会跳过清理与错误回复，且 LLM 侧连接释放取决于厂商实现。
//! 协作取消让任务走统一的 finally 收敛路径，保证状态一致。

use std::panic::AssertUnwindSafe;
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::oneshot;

use crate::provider::{ChatResponse, LlmError};

/// 一轮 LLM 调用的终态结果
///
/// 四路径全覆盖：成功 / 错误 / 取消 / 超时 / Panic。
/// 调用方据此做唯一一次 Session 状态写入（finally 式收敛）。
#[derive(Debug)]
pub enum TurnOutcome {
    /// LLM 正常返回
    Success(Box<ChatResponse>),
    /// 缓存命中 — 与 Success 语义等价（回信/入 history 相同），
    /// 但未发生真实 LLM 调用，**不计量 Token**（缓存不产生成本）
    Cached(Box<ChatResponse>),
    /// LLM 返回错误（已归一为 `LlmError`，含重试后的最终错误）
    Error(LlmError),
    /// 收到中断信号（协作取消）
    Cancelled,
    /// 超时未返回
    Timeout,
    /// 派生任务 panic（`catch_unwind` 捕获，含 panic 信息）
    Panic(String),
}

impl TurnOutcome {
    /// 是否为成功结果（含缓存命中）
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success(_) | Self::Cached(_))
    }

    /// 是否为可恢复错误（调用方可据此决定是否重试或降级）
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

/// 运行一轮 LLM 调用 — 终态自管的唯一入口
///
/// 三路 `tokio::select!` + 外层 `catch_unwind`，保证任何路径都返回
/// `TurnOutcome`，绝不 panic 外泄、绝不挂死。
///
/// # 参数
/// - `llm_future`: LLM 调用 Future（由调用方构造，如 `provider.chat(req)`）
/// - `cancel_rx`: 中断信号接收端（`Interrupt` 消息触发 `cancel_tx.send(())`）
/// - `timeout_duration`: 超时上限（超时后返回 `TurnOutcome::Timeout`）
///
/// # 终态保证
/// | 路径 | 返回值 | 副作用 |
/// |---|---|---|
/// | LLM 正常返回 | `Success(resp)` | 无 |
/// | LLM 返回错误 | `Error(err)` | 无（重试已在 provider 内部完成） |
/// | 收到中断信号 | `Cancelled` | `llm_future` 被 drop（reqwest 会取消底层 HTTP 请求） |
/// | 超时 | `Timeout` | `llm_future` 被 drop |
/// | LLM future panic | `Panic(msg)` | panic 被捕获，不外泄 |
pub async fn run_turn<F>(
    llm_future: F,
    cancel_rx: oneshot::Receiver<()>,
    timeout_duration: Duration,
) -> TurnOutcome
where
    F: std::future::Future<Output = Result<ChatResponse, LlmError>> + Send,
{
    // 外层 catch_unwind：捕获 llm_future 内部 panic
    // AssertUnwindSafe：LLM future 内部不需要 UnwindSafe 语义（我们只关心不外泄）
    let result = AssertUnwindSafe(run_turn_inner(llm_future, cancel_rx, timeout_duration))
        .catch_unwind()
        .await;

    match result {
        Ok(outcome) => outcome,
        Err(panic_payload) => {
            let msg = panic_payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            TurnOutcome::Panic(msg)
        }
    }
}

/// 内层：三路 select（LLM / 取消 / 超时）
async fn run_turn_inner<F>(
    llm_future: F,
    cancel_rx: oneshot::Receiver<()>,
    timeout_duration: Duration,
) -> TurnOutcome
where
    F: std::future::Future<Output = Result<ChatResponse, LlmError>> + Send,
{
    tokio::select! {
        // LLM 正常完成或错误
        result = llm_future => match result {
            Ok(resp) => TurnOutcome::Success(Box::new(resp)),
            Err(e) => TurnOutcome::Error(e),
        },
        // 收到中断信号 → 协作取消
        Ok(()) = cancel_rx => TurnOutcome::Cancelled,
        // 超时切断
        _ = tokio::time::sleep(timeout_duration) => TurnOutcome::Timeout,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{FinishReason, Message, TokenUsage};

    fn mock_response() -> ChatResponse {
        ChatResponse {
            id: "test".into(),
            model: "test".into(),
            message: Message::assistant("hello"),
            finish_reason: FinishReason::Stop,
            usage: Some(TokenUsage::default()),
        }
    }

    #[tokio::test]
    async fn success_path() {
        let (_tx, rx) = oneshot::channel();
        let outcome = run_turn(async { Ok(mock_response()) }, rx, Duration::from_secs(10)).await;
        assert!(outcome.is_success());
    }

    #[tokio::test]
    async fn cancelled_path() {
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = tx.send(());
        });
        let outcome = run_turn(
            std::future::pending::<Result<ChatResponse, LlmError>>(),
            rx,
            Duration::from_secs(10),
        )
        .await;
        assert!(matches!(outcome, TurnOutcome::Cancelled));
    }

    #[tokio::test]
    async fn timeout_path() {
        let (_tx, rx) = oneshot::channel();
        let outcome = run_turn(
            std::future::pending::<Result<ChatResponse, LlmError>>(),
            rx,
            Duration::from_millis(50),
        )
        .await;
        assert!(matches!(outcome, TurnOutcome::Timeout));
    }

    #[tokio::test]
    async fn error_path() {
        let (_tx, rx) = oneshot::channel();
        let outcome = run_turn(
            async { Err(LlmError::Timeout) },
            rx,
            Duration::from_secs(10),
        )
        .await;
        assert!(outcome.is_error());
    }

    #[tokio::test]
    async fn panic_is_caught() {
        let (_tx, rx) = oneshot::channel();
        let outcome = run_turn(
            async {
                panic!("boom");
                #[allow(unreachable_code)]
                Ok(mock_response())
            },
            rx,
            Duration::from_secs(10),
        )
        .await;
        match outcome {
            TurnOutcome::Panic(msg) => assert!(msg.contains("boom")),
            other => panic!("expected Panic, got {other:?}"),
        }
    }
}

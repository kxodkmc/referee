//! 交付契约——回合回信的穷尽分类（设计文档 §4.6a）与中断关键字匹配。
//! 出口唯一化：最终结果只从兜底管道交付，不受模型是否调用过工具影响。

use referee_ai::SessionReply;
use referee_core::{Envelope, KernelError};

/// 回合终态的处置动作
#[derive(Debug, Clone, PartialEq)]
pub enum TurnDisposition {
    /// `Success` 且文本非空——兜底发送最终输出
    Deliver(String),
    /// `Success` 空文本 / `Cancelled`——静默（info 日志由调用方记）
    Skip,
    /// `Busy`——回队尾重试
    Busy,
    /// `Error` / invoke 超时 / `Unhandled` / 回信解码失败——im.system 通知用户
    Notify(String),
}

/// 分类 invoke(AgentRuntime, Chat) 的回信。穷尽且互斥，实现处不留 `_ =>` 通配。
pub fn disposition(reply: &Result<Envelope, KernelError>) -> TurnDisposition {
    match reply {
        Err(e) => TurnDisposition::Notify(format!("任务失败：{e}")),
        Ok(env) => match SessionReply::from_envelope(env) {
            Ok(SessionReply::Success { message, .. }) => match message.content.as_text() {
                Some(text) if !text.trim().is_empty() => TurnDisposition::Deliver(text.to_owned()),
                _ => TurnDisposition::Skip,
            },
            Ok(SessionReply::Busy { .. }) => TurnDisposition::Busy,
            Ok(SessionReply::Error { message, .. }) => {
                TurnDisposition::Notify(format!("任务失败：{message}"))
            }
            Ok(SessionReply::Cancelled) => TurnDisposition::Skip,
            Ok(SessionReply::Unhandled { reason }) => {
                TurnDisposition::Notify(format!("任务未被处理：{reason}"))
            }
            Err(e) => TurnDisposition::Notify(format!("回信解码失败：{e}")),
        },
    }
}

/// 中断关键字命中判定：合并文本包含任一非空关键字即命中
pub fn hit_keyword(text: &str, keywords: &[String]) -> bool {
    keywords
        .iter()
        .any(|keyword| !keyword.is_empty() && text.contains(keyword.as_str()))
}

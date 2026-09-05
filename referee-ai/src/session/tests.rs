//! Session 单元测试 — Phase 1 基础 + Phase 2 工具结果

use super::*;
use crate::provider::{FinishReason, Role, TokenUsage};

fn mock_response(content: &str) -> ChatResponse {
    ChatResponse {
        id: "test".into(),
        model: "test".into(),
        message: Message::assistant(content),
        finish_reason: FinishReason::Stop,
        usage: Some(TokenUsage::default()),
    }
}

fn mock_tool_response(content: &str, tool_calls: Vec<ToolCall>) -> ChatResponse {
    let mut msg = Message::assistant(content);
    msg.tool_calls = tool_calls;
    ChatResponse {
        id: "test".into(),
        model: "test".into(),
        message: msg,
        finish_reason: FinishReason::ToolCalls,
        usage: Some(TokenUsage::default()),
    }
}

fn make_tool_call(id: &str, name: &str) -> ToolCall {
    use crate::provider::{ToolCall, ToolCallFunction};
    ToolCall {
        id: id.into(),
        function: ToolCallFunction {
            name: name.into(),
            arguments: "{}".into(),
        },
    }
}

// ─────────────────────────────────────────────
// Phase 1 基础测试（从原文件提取，保持不变）
// ─────────────────────────────────────────────

#[test]
fn start_thinking_from_idle() {
    let mut session = Session::new(SessionConfig::default());
    assert!(!session.is_busy());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");
    assert_eq!(turn_id, 1);
    assert!(session.is_busy());
}

#[test]
fn start_thinking_rejected_when_busy() {
    let mut session = Session::new(SessionConfig::default());
    let _ = session.start_thinking().expect("first start ok");
    assert!(
        session.start_thinking().is_none(),
        "should reject when busy"
    );
}

#[test]
fn cancel_sends_signal() {
    let mut session = Session::new(SessionConfig::default());
    let (_, mut rx) = session.start_thinking().expect("start ok");
    assert!(session.cancel_thinking());
    match rx.try_recv() {
        Ok(()) => {}
        other => panic!("expected Ok(()), got {other:?}"),
    }
    assert!(!session.cancel_thinking());
}

#[test]
fn finish_thinking_success_to_idle() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");
    let action =
        session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(mock_response("hi"))));
    match action {
        FinishAction::Idle { response } => {
            assert!(response.is_some());
            assert_eq!(response.unwrap().message.content.as_text(), Some("hi"));
        }
        _ => panic!("expected Idle"),
    }
    assert!(!session.is_busy());
}

#[test]
fn finish_thinking_with_tool_calls_to_awaiting() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");

    let resp = mock_tool_response("calling tool", vec![make_tool_call("tc_1", "echo")]);
    let action = session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    match action {
        FinishAction::AwaitingCalls { tool_calls, .. } => {
            assert_eq!(tool_calls.len(), 1);
            assert_eq!(tool_calls[0].id, "tc_1");
        }
        _ => panic!("expected AwaitingCalls"),
    }
    assert!(session.is_busy());
}

#[test]
fn finish_thinking_cancelled_to_idle() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");
    let action = session.finish_thinking(turn_id, TurnOutcome::Cancelled);
    assert!(matches!(action, FinishAction::Idle { response: None }));
    assert!(!session.is_busy());
}

#[test]
fn finish_thinking_counts_consumed_tokens() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");

    let mut resp = mock_response("hi");
    resp.usage = Some(TokenUsage {
        prompt_tokens: 10,
        completion_tokens: 40,
        total_tokens: 50,
        ..Default::default()
    });
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    assert_eq!(session.consumed_tokens(), 50);
}

#[test]
fn finish_thinking_with_tool_calls_also_counts() {
    // AwaitingCalls 分支（LLM 返回工具调用）同样消耗 token，必须计入
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");

    let mut resp = mock_tool_response("calling tool", vec![make_tool_call("tc_1", "echo")]);
    resp.usage = Some(TokenUsage {
        prompt_tokens: 10,
        completion_tokens: 40,
        total_tokens: 50,
        ..Default::default()
    });
    let action = session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    assert!(matches!(action, FinishAction::AwaitingCalls { .. }));
    assert_eq!(session.consumed_tokens(), 50);
}

#[test]
fn finish_thinking_stale_turn_id_ignored() {
    let mut session = Session::new(SessionConfig::default());
    let (_turn_id, _rx) = session.start_thinking().expect("start ok");
    let action =
        session.finish_thinking(999, TurnOutcome::Success(Box::new(mock_response("stale"))));
    assert!(matches!(action, FinishAction::Idle { response: None }));
}

#[test]
fn cancel_thinking_when_idle_returns_false() {
    let mut session = Session::new(SessionConfig::default());
    assert!(!session.cancel_thinking());
}

#[test]
fn history_eviction() {
    let mut session = Session::new(SessionConfig {
        max_history: 3,
        ..Default::default()
    });
    for i in 0..5 {
        session.push_history(Message::user(format!("msg {i}"))).unwrap();
    }
    assert_eq!(session.history_len(), 3);
    let req = session.build_chat_request(&ChatOptions::default());
    let msgs: Vec<&Message> = req.messages.iter().collect();
    assert_eq!(msgs[0].content.as_text(), Some("msg 2"));
    assert_eq!(msgs[2].content.as_text(), Some("msg 4"));
}

#[test]
fn facts_preserved_beyond_window() {
    let mut session = Session::new(SessionConfig {
        max_history: 3,
        ..Default::default()
    });
    for i in 0..5 {
        session.push_history(Message::user(format!("msg {i}"))).unwrap();
    }
    // 模型可见窗口有界：只派生最近 3 条
    assert_eq!(session.history_len(), 3);
    let req = session.build_chat_request(&ChatOptions::default());
    let texts: Vec<&str> = req
        .messages
        .iter()
        .filter_map(|m| m.content.as_text())
        .collect();
    assert_eq!(texts, vec!["msg 2", "msg 3", "msg 4"]);
    // 事实源无损：全部 5 条事实仍在，可随时回放/审计
    assert_eq!(session.log.len(), 5);
    let facts: Vec<&str> = session
        .log
        .snapshot()
        .iter()
        .filter_map(|m| m.content.as_text())
        .collect();
    assert_eq!(facts, vec!["msg 0", "msg 1", "msg 2", "msg 3", "msg 4"]);
}

#[test]
fn fact_log_capacity_degrades_by_compacting() {
    let mut session = Session::new(SessionConfig {
        max_events: 2,
        ..Default::default()
    });
    session.push_history(Message::user("a")).unwrap();
    session.push_history(Message::user("b")).unwrap();
    // 满时降级：压缩头部旧事实腾出空间，新事实照常入队（拒绝会致会话永久死局）
    session.push_history(Message::user("c")).unwrap();
    assert_eq!(session.log.len(), 2);
    let facts: Vec<&str> = session
        .log
        .snapshot()
        .iter()
        .filter_map(|m| m.content.as_text())
        .collect();
    assert_eq!(facts, vec!["b", "c"], "oldest fact dropped to admit new one");
}

#[test]
fn build_chat_request_includes_history() {
    let mut session = Session::new(SessionConfig::default());
    session.push_history(Message::user("hello")).unwrap();
    session.push_history(Message::assistant("hi there")).unwrap();
    let req = session.build_chat_request(&ChatOptions::default());
    assert_eq!(req.messages.len(), 2);
    assert_eq!(req.messages[0].content.as_text(), Some("hello"));
    assert_eq!(req.messages[1].content.as_text(), Some("hi there"));
}

// ─────────────────────────────────────────────
// Phase 2 新增测试
// ─────────────────────────────────────────────

#[test]
fn finish_tool_call_records_result() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");

    let resp = mock_tool_response(
        "calling",
        vec![
            make_tool_call("tc_a", "echo"),
            make_tool_call("tc_b", "echo"),
        ],
    );
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    // 第一个工具完成
    let action = session.finish_tool_call("tc_a", r#"{"output":"a"}"#.into());
    assert!(matches!(action, ToolCallAction::Pending));

    // 第二个工具完成 → AllDone
    let action = session.finish_tool_call("tc_b", r#"{"output":"b"}"#.into());
    assert!(matches!(action, ToolCallAction::AllDone));
}

#[test]
fn finish_tool_call_ignores_unknown_id() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");

    let resp = mock_tool_response("calling", vec![make_tool_call("tc_1", "echo")]);
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    let action = session.finish_tool_call("unknown", "{}".into());
    assert!(matches!(action, ToolCallAction::Ignored));
}

#[test]
fn resume_thinking_pushes_tool_results_to_history() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");

    let resp = mock_tool_response("calling", vec![make_tool_call("tc_1", "echo")]);
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    session.finish_tool_call("tc_1", "result_data".into());

    let result = session.resume_thinking();
    assert!(result.is_some());

    // history 应包含: [assistant(tool_calls), tool(result)]
    assert_eq!(session.history_len(), 2);

    let req = session.build_chat_request(&ChatOptions::default());
    assert_eq!(req.messages.len(), 2);
    assert_eq!(req.messages[1].role, Role::Tool);
    assert_eq!(req.messages[1].tool_call_id.as_deref(), Some("tc_1"));
}

#[test]
fn resume_thinking_returns_none_when_not_awaiting() {
    let mut session = Session::new(SessionConfig::default());
    assert!(session.resume_thinking().is_none());
}

#[test]
fn pending_reply_set_and_take() {
    let mut session = Session::new(SessionConfig::default());
    let (tx, _rx) = oneshot::channel();
    session.set_pending_reply(tx);
    assert!(session.pending_reply.is_some());
    assert!(session.take_pending_reply().is_some());
    assert!(session.take_pending_reply().is_none());
}

#[test]
fn force_idle_clears_awaiting() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");

    let resp = mock_tool_response("calling", vec![make_tool_call("tc_1", "echo")]);
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    assert!(session.is_busy());
    session.force_idle();
    assert!(!session.is_busy());
}

#[test]
fn set_chat_options_persists_for_resume() {
    let mut session = Session::new(SessionConfig::default());
    let opts = ChatOptions {
        temperature: Some(0.7),
        ..Default::default()
    };
    session.set_chat_options(opts);

    let (turn_id, _rx) = session.start_thinking().expect("start ok");
    let resp = mock_tool_response("calling", vec![make_tool_call("tc_1", "echo")]);
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));
    session.finish_tool_call("tc_1", "ok".into());

    let (_, _, req) = session.resume_thinking().expect("resume ok");
    assert_eq!(req.temperature, Some(0.7));
}

// ─────────────────────────────────────────────
// 异步派发（不等待工具）— 注入队列与纯派发轮收敛
// ─────────────────────────────────────────────

#[test]
fn inject_tool_result_flushes_on_next_start_round() {
    let mut session = Session::new(SessionConfig::default());
    // 后台派发完成 → 入队（非空忽略，保持简单语义）
    session.inject_tool_result("async result data".into());

    // 下一次模型调用（新回合）构建请求前合并注入
    let round = session
        .start_round(Message::user("continue"), &ChatOptions::default(), 0)
        .expect("start ok");
    let req = round.request;
    let last = req.messages.last().expect("has messages");
    assert_eq!(last.role, Role::User);
    assert_eq!(last.content.as_text().unwrap(), "async result data");
}

#[test]
fn empty_injection_is_ignored() {
    let mut session = Session::new(SessionConfig::default());
    session.inject_tool_result(String::new());
    let round = session
        .start_round(Message::user("hi"), &ChatOptions::default(), 0)
        .expect("start ok");
    // 仅用户消息，无注入占位
    assert_eq!(round.request.messages.len(), 1);
}

#[test]
fn resume_thinking_flushes_async_injections_after_tool_results() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");
    let resp = mock_tool_response("calling", vec![make_tool_call("tc_1", "echo")]);
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    // 占位结果收敛 + 后台真实结果入队
    session.finish_tool_call("tc_1", "dispatched".into());
    session.inject_tool_result("real async result".into());

    assert!(session.resume_thinking().is_some());
    let req = session.build_chat_request(&ChatOptions::default());
    // [assistant(tool_calls), tool(占位), user(异步结果)]
    assert_eq!(req.messages.len(), 3);
    assert_eq!(req.messages[1].role, Role::Tool);
    assert_eq!(req.messages[2].role, Role::User);
    assert_eq!(
        req.messages[2].content.as_text().unwrap(),
        "real async result"
    );
}

#[test]
fn settle_tool_results_converges_pure_dispatch_round() {
    let mut session = Session::new(SessionConfig::default());
    let (turn_id, _rx) = session.start_thinking().expect("start ok");
    let resp = mock_tool_response("dispatching", vec![make_tool_call("tc_1", "echo")]);
    session.finish_thinking(turn_id, TurnOutcome::Success(Box::new(resp)));

    // 派发类占位收敛（清 pending）
    let action = session.finish_tool_call("tc_1", "dispatched".into());
    assert!(matches!(action, ToolCallAction::AllDone));

    // 纯派发轮收尾：占位 Tool 消息落 history → Idle
    assert!(session.settle_tool_results());
    assert!(!session.is_busy());
    let req = session.build_chat_request(&ChatOptions::default());
    assert_eq!(req.messages[1].role, Role::Tool);
    assert_eq!(req.messages[1].tool_call_id.as_deref(), Some("tc_1"));

    // 非 AwaitingCalls 状态重复收敛返回 false
    assert!(!session.settle_tool_results());
}

// ─────────────────────────────────────────────
// System Prompt 注入（3.1）
// ─────────────────────────────────────────────

#[test]
fn system_prompt_from_options_becomes_first_message() {
    let mut session = Session::new(SessionConfig::default());
    session.push_history(Message::user("hello")).unwrap();
    let opts = ChatOptions {
        system_prompt: Some("You are a helpful assistant".into()),
        ..Default::default()
    };
    let req = session.build_chat_request(&opts);
    assert_eq!(req.messages[0].role, Role::System);
    assert_eq!(
        req.messages[0].content.as_text(),
        Some("You are a helpful assistant")
    );
}

#[test]
fn default_system_prompt_falls_back() {
    let mut session = Session::new(SessionConfig {
        default_system_prompt: Some("Default persona".into()),
        ..Default::default()
    });
    session.push_history(Message::user("hello")).unwrap();
    let req = session.build_chat_request(&ChatOptions::default());
    assert_eq!(req.messages[0].role, Role::System);
    assert_eq!(req.messages[0].content.as_text(), Some("Default persona"));
}

#[test]
fn no_system_prompt_keeps_user_first() {
    let mut session = Session::new(SessionConfig::default());
    session.push_history(Message::user("hello")).unwrap();
    let req = session.build_chat_request(&ChatOptions::default());
    assert_eq!(req.messages[0].role, Role::User);
}

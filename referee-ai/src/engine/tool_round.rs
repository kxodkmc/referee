//! 工具轮编排 — 回合内一次工具调用的完整处理
//!
//! 截断 → 按 wait 分流 → 等待类同步执行 / 派发类后台注入 → 收敛决策：
//! - 等待类全部成功且任一 terminal → 回合立即收敛（不发起收尾轮）
//! - 其余有等待项的轮次 → resume 循环
//! - 纯派发轮 → 回合就此结束

use crate::observe;
use crate::provider::ToolCall;
use crate::tool::{ExecutedTool, ToolOutcome, ToolRegistry};

use super::Engine;

/// 工具轮处理结果
pub(crate) enum ToolRound {
    /// 有待等待的工具 → 继续 resume 循环
    Resume,
    /// 回合就此收敛；`aggregate_usage` 为 true 时（terminal 收敛）以本回合
    /// 各轮 usage 之和作为返回响应的 usage
    Settled { aggregate_usage: bool },
}

impl Engine {
    /// 一轮工具调用的完整处理：截断 → 按 wait 分流 → 等待类同步 / 派发类后台注入
    ///
    /// - 截断项：生成引导错误消息（下一轮重发），立即收敛
    /// - 派发类（不等待）：占位结果立即收敛（保证 assistant tool_calls 与 tool 结果
    ///   配对），后台任务执行完成后结果入队，等待下一次模型调用/回合合并注入
    /// - 等待类：同步执行（并行 + 隔离 + 单工具超时），批次受
    ///   `awaiting_calls_timeout` 总 deadline 约束——未完成项以超时消息收敛，
    ///   会话恢复一致状态，回合不被慢批次无限占用
    /// - terminal 收敛：等待类批次无失败且任一为 terminal 工具时，回合立即收敛
    pub(crate) async fn run_tool_calls(
        &self,
        session_id: &crate::SessionId,
        turn_id: u64,
        mut tool_calls: Vec<ToolCall>,
    ) -> ToolRound {
        let (registry, executor) = match (&self.tools, &self.tool_executor) {
            (Some(r), Some(e)) => (r.clone(), e.clone()),
            _ => return ToolRound::Settled { aggregate_usage: false },
        };

        // 0. 深度兜底（声明层过滤被绕过时的防线）：嵌套深度达上限的会话
        //    拒绝调用子 Agent 工具（depth_limited），生成明确错误并立即收敛
        let depth = self
            .sessions
            .get(session_id)
            .map(|s| s.peer_depth())
            .unwrap_or(0);
        if depth >= self.config.max_subagent_depth {
            let (blocked, rest): (Vec<_>, Vec<_>) = tool_calls.into_iter().partition(|tc| {
                registry
                    .get(&tc.function.name)
                    .map(|t| t.depth_limited())
                    .unwrap_or(false)
            });
            for tc in blocked {
                if let Some(mut s) = self.sessions.get_mut(session_id) {
                    s.finish_tool_call(&tc.id, DEPTH_LIMIT_MESSAGE.to_string());
                }
            }
            tool_calls = rest;
        }

        // 1. 截断：超出 max_per_turn 的生成引导错误（由调用方统一截断一次）
        let (head, tail) = executor.truncate(tool_calls);
        for tc in tail {
            if let Some(mut s) = self.sessions.get_mut(session_id) {
                s.finish_tool_call(
                    &tc.id,
                    format!(
                        "Exceeds max_tools_per_turn limit ({}). \
                         Please re-issue this tool call in the next turn.",
                        executor.config().max_per_turn
                    ),
                );
            }
        }

        // 2. 按等待决策分流
        let (waiting, dispatched) = executor.split_by_wait(head, &registry);
        let has_dispatched = !dispatched.is_empty();

        // 观测：实际执行项（等待 + 派发）触发 started（截断/深度拦截项不执行，不触发）
        for tc in waiting.iter().chain(dispatched.iter()) {
            self.observe_event(|o| o.on_tool_started(*session_id, &tc.id, &tc.function.name));
        }

        // 3. 派发类：占位收敛 + 后台执行完成后入队注入
        if !dispatched.is_empty() {
            for tc in &dispatched {
                if let Some(mut s) = self.sessions.get_mut(session_id) {
                    s.finish_tool_call(&tc.id, DISPATCHED_PLACEHOLDER.to_string());
                }
            }
            let handles =
                executor.dispatch_batch(dispatched, &registry, *session_id, turn_id, depth);
            let engine = self.clone();
            let sid = *session_id;
            tokio::spawn(async move {
                for h in handles {
                    let r = h.await.unwrap_or_else(|_| ExecutedTool {
                        tool_call_id: String::new(),
                        tool_name: "<unknown>".into(),
                        result: "async tool task panicked".into(),
                        outcome: ToolOutcome::Panic,
                        duration_ms: 0,
                    });
                    observe::tool_completed(!r.result.is_empty());
                    // 派发类完成事件（与等待类复用同一 ToolOutcome）
                    engine.observe_event(|o| {
                        o.on_tool_finished(sid, &r.tool_call_id, r.outcome, r.duration_ms)
                    });
                    let text = format!("[async tool '{}' completed]\n{}", r.tool_name, r.result);
                    if let Some(mut s) = engine.sessions.get_mut(&sid) {
                        s.inject_tool_result(text);
                    } else {
                        // 会话已移除（空闲回收 / 上层 remove）：结果无处注入。
                        // best-effort 交付缺口必须可观测，绝不静默（AI-6 修复）。
                        tracing::warn!(
                            session_id = %sid,
                            tool = %r.tool_name,
                            "dispatch tool result dropped: session no longer exists"
                        );
                        observe::tool_result_dropped();
                    }
                }
            });
        }

        // 4. 等待类：同步执行并收敛结果（批次总 deadline = awaiting_calls_timeout）
        if !waiting.is_empty() {
            // 能力降级：厂商不支持并行工具时强制串行（Some(1)）
            let max_concurrent = if self.provider.capabilities().parallel_tool_calls {
                None
            } else {
                Some(1)
            };
            let results = executor
                .execute_batch(
                    waiting,
                    &registry,
                    *session_id,
                    turn_id,
                    depth,
                    max_concurrent,
                    self.config.session.timeout.awaiting_calls_timeout,
                )
                .await;
            // terminal 收敛判定须在写入前完成（results 随后被消费）；
            // 混合轮（含派发项）不收敛——派发结果尚未返回
            let terminal_converged =
                !has_dispatched && is_terminal_convergence(&results, &registry);
            for r in results {
                observe::tool_completed(!r.result.is_empty());
                self.observe_event(|o| {
                    o.on_tool_finished(*session_id, &r.tool_call_id, r.outcome, r.duration_ms)
                });
                if let Some(mut s) = self.sessions.get_mut(session_id) {
                    s.finish_tool_call(&r.tool_call_id, r.result);
                }
            }
            if terminal_converged {
                if let Some(mut s) = self.sessions.get_mut(session_id) {
                    s.settle_tool_results();
                }
                return ToolRound::Settled { aggregate_usage: true };
            }
            return ToolRound::Resume;
        }

        // 5. 纯派发轮：占位 Tool 消息落 history → Idle（回合结束）
        if let Some(mut s) = self.sessions.get_mut(session_id) {
            s.settle_tool_results();
        }
        ToolRound::Settled { aggregate_usage: false }
    }
}

/// terminal 收敛判定：本轮等待类工具全部执行成功（截断 / 深度拦截 / 派发占位
/// 均视为失败，不收敛）且至少一个为 terminal 工具
fn is_terminal_convergence(results: &[ExecutedTool], registry: &ToolRegistry) -> bool {
    results.iter().all(|r| r.outcome == ToolOutcome::Ok)
        && results
            .iter()
            .any(|r| registry.get(&r.tool_name).is_some_and(|t| t.terminal()))
}

/// 派发类（不等待）工具的占位结果 — 立即收敛进 history，满足厂商协议
/// assistant tool_calls 与 tool 结果配对；真实结果完成后入队，在**下一次**
/// 模型调用/回合时合并注入（绝不为此主动触发 LLM）。
const DISPATCHED_PLACEHOLDER: &str =
    "Task dispatched (async execution); real result will be injected into a later turn.";

/// 子智能体嵌套深度超限的拒绝消息（执行层兜底）
const DEPTH_LIMIT_MESSAGE: &str =
    "Rejected: subagent nesting depth limit reached. This agent cannot call sub-agents.";

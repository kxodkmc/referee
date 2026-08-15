//! 提示词组装与预算截断 — Phase 5
//!
//! 将系统提示词 / 工具声明 / 对话历史 / 记忆 / 工件等碎片统一组装为
//! [`ChatRequest`]，并按 Token 预算（`prompt_budget`）做优先级截断，
//! 杜绝"Prompt 爆炸"（AGENT_RUNTIME_PLAN §5.6）。
//!
//! ## 优先级（超限按序丢弃，System 最后保留）
//! `system > 工具声明 > 对话历史 > 记忆 > 工件`
//!
//! ## 截断策略
//! - **System**：最高优先级，预算不足时**按字符截断文本**（绝不整段丢弃）
//! - **Tools**：高优先级，仅当整段超预算才整体丢弃
//! - **History**：中优先级，滑动窗口保留最近 N 条；截断后修正首条角色
//!   （OpenAI 协议要求首条为 system/user，残留的 assistant/tool 开头会 400）
//! - **Memory / Artifacts**：低优先级，预算不足整体丢弃
//!
//! ## 设计约束
//! - 纯函数，无 I/O 句柄；只依赖 `provider` 与 `budget`（分层单向依赖）
//! - 截断后估算总量恒 ≤ 预算（估算系数与截断系数同源，见 [`CHARS_PER_TOKEN`]）

use crate::budget::TokenEstimator;
use crate::provider::{
    ChatRequest, Message, MessageContent, Role, ThinkingConfig, ToolDeclaration,
};
use std::collections::VecDeque;

/// 每 Token 可容纳的字符数（由 `TokenEstimator::estimate = chars*2/3+1` 反推：
/// 要 estimate(text) ≤ budget，须 chars ≤ (budget-1)*3/2，即约 1.5 字符/token）
const CHARS_PER_TOKEN: u64 = 3;

/// 提示词片段及其优先级（从高到低）
#[derive(Debug, Clone)]
pub enum PromptFragment {
    /// 系统提示词（最高优先级，最后保留）
    System(Message),
    /// 工具声明（高优先级，尽量保留）
    Tools(Vec<ToolDeclaration>),
    /// 对话历史（中优先级，滑动窗口保留最近 N 条）
    History(Vec<Message>),
    /// 记忆（低优先级，优先丢弃）
    Memory(Vec<Message>),
    /// 工件（最低优先级，优先丢弃）
    Artifacts(Vec<Message>),
}

impl PromptFragment {
    /// 估算该片段的 Token 数（与 `TokenEstimator` 同源，保守口径）
    fn estimate_tokens(&self) -> u64 {
        match self {
            PromptFragment::System(msg) => {
                TokenEstimator::estimate(msg.content.as_text().unwrap_or(""))
            }
            // 工具声明：name + description + parameters JSON 的估算求和
            // （估算同源保证"截断后总量恒 ≤ 预算"声明成立）
            PromptFragment::Tools(tools) => tools
                .iter()
                .map(|t| {
                    TokenEstimator::estimate(&t.name)
                        + TokenEstimator::estimate(&t.description)
                        + TokenEstimator::estimate(
                            &serde_json::to_string(&t.parameters).unwrap_or_default(),
                        )
                })
                .sum(),
            PromptFragment::History(msgs)
            | PromptFragment::Memory(msgs)
            | PromptFragment::Artifacts(msgs) => msgs
                .iter()
                .map(|m| TokenEstimator::estimate(m.content.as_text().unwrap_or("")))
                .sum(),
        }
    }

    /// 截断以适应剩余预算，返回 `(截断后的片段, 实际估算 Token 数)`；预算不足时返回 None
    fn truncate(&self, budget: u64) -> Option<(PromptFragment, u64)> {
        match self {
            PromptFragment::System(msg) => {
                let cost = self.estimate_tokens();
                if cost <= budget {
                    return Some((self.clone(), cost));
                }
                // 预算不足：按字符截断文本（System 最后保留，绝不整段丢弃）。
                // 截断长度按估算系数反推，且必须**扣除截断后缀的成本**，
                // 保证「截断内容 + 后缀」的总估算 ≤ 预算。
                let text = msg.content.as_text().unwrap_or("");
                const SUFFIX: &str = "...[Truncated]";
                let suffix_cost = TokenEstimator::estimate(SUFFIX);
                let content_budget = budget.saturating_sub(suffix_cost);
                let max_chars = (content_budget.saturating_sub(1)) * CHARS_PER_TOKEN / 2;
                let truncated: String = text.chars().take(max_chars as usize).collect();
                let mut new_msg = msg.clone();
                new_msg.content = MessageContent::text(format!("{truncated}{SUFFIX}"));
                let new_cost = TokenEstimator::estimate(&format!("{truncated}{SUFFIX}"));
                Some((PromptFragment::System(new_msg), new_cost))
            }
            PromptFragment::Tools(_) => {
                let cost = self.estimate_tokens();
                if cost <= budget {
                    Some((self.clone(), cost))
                } else {
                    None // 极度超限才丢弃工具能力
                }
            }
            PromptFragment::History(msgs) => {
                // 滑动窗口：从最新往前保留，直到预算耗尽
                let mut current_tokens = 0u64;
                let mut keep = VecDeque::new();
                for msg in msgs.iter().rev() {
                    let cost = TokenEstimator::estimate(msg.content.as_text().unwrap_or(""));
                    if current_tokens + cost > budget {
                        break;
                    }
                    current_tokens += cost;
                    keep.push_front(msg.clone());
                }
                let mut kept: Vec<Message> = keep.into_iter().collect();
                // 角色配对修正：OpenAI 协议要求首条为 system/user。
                // 窗口切在中间时可能残留非法开头，循环丢弃至合法：
                // - Tool 开头：tool 必须有其前缀 assistant（悬空 tool 非法）
                // - 裸 Assistant 开头（无 tool_calls）：无前缀的裸 assistant 非法
                // - Assistant 带 tool_calls 开头：工具调用轮的完整片段，合法保留
                while let Some(first) = kept.first() {
                    let keep = match first.role {
                        Role::User | Role::System => true,
                        Role::Assistant => !first.tool_calls.is_empty(),
                        Role::Tool => false,
                    };
                    if keep {
                        break;
                    }
                    kept.remove(0);
                }
                if kept.is_empty() {
                    None
                } else {
                    Some((PromptFragment::History(kept), current_tokens))
                }
            }
            PromptFragment::Memory(_) | PromptFragment::Artifacts(_) => {
                let cost = self.estimate_tokens();
                if cost <= budget {
                    Some((self.clone(), cost))
                } else {
                    None
                }
            }
        }
    }
}

/// 系统提示词片段 — 一个可独立省略、并参与缓存排序的 system 片段
///
/// 分段组装的核心：**稳定内容排前、易变内容靠后**，是 DeepSeek 前缀单元与
/// Anthropic 前缀缓存共同命中的根本前提。`omit_if_empty = true` 时，空片段
/// 整体省略（实现"没启用的能力就不注入"）。
#[derive(Debug, Clone)]
pub struct SystemSection {
    /// 稳定片段（进缓存前缀，排前）；false = 每轮易变（排后，破缓存）
    pub stable: bool,
    pub text: String,
    /// 空则整体省略
    pub omit_if_empty: bool,
}

impl SystemSection {
    pub fn new(stable: bool, text: impl Into<String>) -> Self {
        Self {
            stable,
            text: text.into(),
            omit_if_empty: false,
        }
    }

    /// 空则省略的稳定片段
    pub fn stable(text: impl Into<String>) -> Self {
        Self::new(true, text)
    }
}

/// 提示词组装参数（legacy）— 碎片参数统一封装，新增参数不破坏既有调用方
pub struct PromptParts {
    /// 系统提示词（最高优先级，最后保留）
    pub system: Option<Message>,
    pub tools: Vec<ToolDeclaration>,
    pub history: Vec<Message>,
    pub memory: Vec<Message>,
    pub artifacts: Vec<Message>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<usize>,
    pub thinking: ThinkingConfig,
    /// 上下文 Token 预算上限（0 = 不截断）
    pub prompt_budget: usize,
}

/// 分段组装参数 — 编排器入口
pub struct AssembleParts {
    /// 系统片段（已由上层按能力准备；编排器按稳定性精排 + 空则省略）
    pub sections: Vec<SystemSection>,
    pub tools: Vec<ToolDeclaration>,
    pub history: Vec<Message>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<usize>,
    pub thinking: ThinkingConfig,
    /// 上下文 Token 预算上限（0 = 不截断）
    pub prompt_budget: usize,
}

/// 分段编排：条件省略 → 稳定性排序（稳定在前）→ 预算截断 → `ChatRequest`
///
/// 返回的 `ChatRequest` 保证：片段总估算 ≤ `prompt_budget`（System 按字符
/// 截断兜底，其余按优先级丢弃）。
pub fn assemble(parts: AssembleParts) -> ChatRequest {
    let AssembleParts {
        sections,
        tools,
        history,
        temperature,
        max_tokens,
        thinking,
        prompt_budget,
    } = parts;

    // 1. 条件省略：空片段丢弃
    let kept: Vec<SystemSection> = sections
        .into_iter()
        .filter(|s| !(s.omit_if_empty && s.text.trim().is_empty()))
        .collect();

    // 2. 稳定性排序：稳定片段在前（保持各自相对顺序），易变片段在后
    let mut stable: Vec<String> = Vec::new();
    let mut volatile: Vec<String> = Vec::new();
    for s in kept {
        if s.stable {
            stable.push(s.text);
        } else {
            volatile.push(s.text);
        }
    }
    let mut system_text = String::new();
    for part in stable.into_iter().chain(volatile) {
        if !system_text.is_empty() {
            system_text.push('\n');
        }
        system_text.push_str(&part);
    }
    let system = if system_text.is_empty() {
        None
    } else {
        Some(Message::system(system_text))
    };

    finalize(
        system,
        tools,
        history,
        Vec::new(),
        Vec::new(),
        temperature,
        max_tokens,
        thinking,
        prompt_budget,
    )
}

/// 组装并截断 Prompt，生成最终 `ChatRequest`（legacy 入口）
///
/// 委托 [`finalize`]，与 [`assemble`] 共享同一套截断逻辑。
pub fn build_prompt(parts: PromptParts) -> ChatRequest {
    let PromptParts {
        system,
        tools,
        history,
        memory,
        artifacts,
        temperature,
        max_tokens,
        thinking,
        prompt_budget,
    } = parts;

    finalize(
        system,
        tools,
        history,
        memory,
        artifacts,
        temperature,
        max_tokens,
        thinking,
        prompt_budget,
    )
}

/// 统一截断与组装 — 按优先级截断，生成 `ChatRequest`
///
/// 返回的 `ChatRequest` 保证：片段总估算 ≤ `prompt_budget`（System 按字符
/// 截断兜底，其余按优先级丢弃）。
fn finalize(
    system: Option<Message>,
    tools: Vec<ToolDeclaration>,
    history: Vec<Message>,
    memory: Vec<Message>,
    artifacts: Vec<Message>,
    temperature: Option<f32>,
    max_tokens: Option<usize>,
    thinking: ThinkingConfig,
    prompt_budget: usize,
) -> ChatRequest {
    // 预算 0 = 不截断
    if prompt_budget == 0 {
        let mut messages = Vec::with_capacity(history.len() + memory.len() + artifacts.len());
        if let Some(sys) = system {
            messages.push(sys);
        }
        messages.extend(history);
        messages.extend(memory);
        messages.extend(artifacts);
        return ChatRequest {
            messages,
            tools,
            temperature,
            max_tokens,
            thinking,
            ..Default::default()
        };
    }

    // 1. 按优先级组装片段列表：System > Tools > History > Memory > Artifacts
    let mut fragments: Vec<PromptFragment> = Vec::new();
    if let Some(sys) = system {
        fragments.push(PromptFragment::System(sys));
    }
    if !tools.is_empty() {
        fragments.push(PromptFragment::Tools(tools));
    }
    if !history.is_empty() {
        fragments.push(PromptFragment::History(history));
    }
    if !memory.is_empty() {
        fragments.push(PromptFragment::Memory(memory));
    }
    if !artifacts.is_empty() {
        fragments.push(PromptFragment::Artifacts(artifacts));
    }

    // 2. 预算分配与截断（System 优先获得预算，其余按序缩减）
    let mut remaining = prompt_budget as u64;
    let mut messages: Vec<Message> = Vec::new();
    let mut final_tools: Vec<ToolDeclaration> = Vec::new();

    for fragment in fragments {
        if remaining == 0 {
            break;
        }
        match fragment.truncate(remaining) {
            Some((kept, cost)) => {
                match kept {
                    PromptFragment::System(msg) => messages.insert(0, msg),
                    PromptFragment::Tools(t) => final_tools = t,
                    PromptFragment::History(msgs)
                    | PromptFragment::Memory(msgs)
                    | PromptFragment::Artifacts(msgs) => messages.extend(msgs),
                }
                remaining = remaining.saturating_sub(cost);
            }
            None => {
                // 预算不足，丢弃该片段。System 不经过此路径（内部按字符截断兜底）。
            }
        }
    }

    ChatRequest {
        messages,
        tools: final_tools,
        temperature,
        max_tokens,
        thinking,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试便捷封装：位置参数 → PromptParts（保持既有断言可读）
    #[allow(clippy::too_many_arguments)]
    fn build(
        system: Option<Message>,
        tools: Vec<ToolDeclaration>,
        history: Vec<Message>,
        memory: Vec<Message>,
        artifacts: Vec<Message>,
        temperature: Option<f32>,
        max_tokens: Option<usize>,
        thinking: ThinkingConfig,
        prompt_budget: usize,
    ) -> ChatRequest {
        build_prompt(PromptParts {
            system,
            tools,
            history,
            memory,
            artifacts,
            temperature,
            max_tokens,
            thinking,
            prompt_budget,
        })
    }

    fn msg(role: Role, text: &str) -> Message {
        Message {
            role,
            content: MessageContent::text(text),
            reasoning_content: None,
            tool_calls: Vec::new(),
            tool_call_id: None,
            usage: None,
        }
    }

    /// 生成约 `tokens` token 的文本（估算 = chars*2/3+1 → chars ≈ tokens*1.5）
    fn text_of_tokens(tokens: u64) -> String {
        let chars = ((tokens - 1) * 3 / 2) as usize;
        "x".repeat(chars)
    }

    #[test]
    fn truncation_respects_priority_order() {
        // 验收 3 语义：按优先级截断，System/Tools 保留，History/Artifacts 丢弃。
        // 预算 150：System(≈50) + Tools(≈6) 保留后剩余 < 99 → History(≈199)/Artifacts(≈99) 均丢弃
        let req = build(
            Some(msg(Role::System, &text_of_tokens(50))),
            vec![ToolDeclaration {
                name: "tool".into(),
                description: "d".into(),
                parameters: serde_json::json!({}),
            }],
            vec![msg(Role::User, &text_of_tokens(200))],
            vec![msg(Role::User, &text_of_tokens(100))],
            vec![msg(Role::User, &text_of_tokens(100))],
            None,
            None,
            ThinkingConfig::default(),
            150,
        );

        // System 完整保留（首条）
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, Role::System);
        // Tools 完整保留
        assert_eq!(req.tools.len(), 1);
        // History / Artifacts 被丢弃
        assert!(
            req.messages.iter().all(|m| m.role != Role::User),
            "history/artifacts must be dropped, got {:?}",
            req.messages
        );

        // 总量恒 ≤ 预算（System + Tools 估算，与实现同源）
        let total: u64 = req
            .messages
            .iter()
            .map(|m| TokenEstimator::estimate(m.content.as_text().unwrap_or("")))
            .sum::<u64>()
            + req
                .tools
                .iter()
                .map(|t| {
                    TokenEstimator::estimate(&t.name)
                        + TokenEstimator::estimate(&t.description)
                        + TokenEstimator::estimate(
                            &serde_json::to_string(&t.parameters).unwrap_or_default(),
                        )
                })
                .sum::<u64>();
        assert!(total <= 150, "total {total} exceeds budget 150");
    }

    #[test]
    fn system_truncates_text_not_dropped() {
        // System 超预算：按字符截断，绝不整段丢弃，且截断后估算 ≤ 预算
        let req = build(
            Some(msg(Role::System, &text_of_tokens(500))),
            vec![],
            vec![],
            vec![],
            vec![],
            None,
            None,
            ThinkingConfig::default(),
            100,
        );
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].role, Role::System);
        let text = req.messages[0].content.as_text().unwrap();
        assert!(text.ends_with("[Truncated]"));
        let cost = TokenEstimator::estimate(text);
        assert!(cost <= 100, "truncated system cost {cost} exceeds 100");
    }

    #[test]
    fn system_truncate_cjk_no_panic() {
        // 中文文本截断：字节切片陷阱回归（字符数做字节索引会 panic）
        let system = "系统提示词：" .to_string() + &"这是一个很长的中文系统提示词，用来验证预算不足时的文本截断逻辑不会因为多字节字符而崩溃。".repeat(50);
        let req = build(
            Some(msg(Role::System, &system)),
            vec![],
            vec![],
            vec![],
            vec![],
            None,
            None,
            ThinkingConfig::default(),
            50,
        );
        let text = req.messages[0].content.as_text().unwrap();
        assert!(text.ends_with("[Truncated]"));
        assert!(TokenEstimator::estimate(text) <= 50);
    }

    #[test]
    fn history_sliding_window_keeps_recent() {
        // 每条消息 7 字符 ≈ 5 token；预算 11 只够最近 2 条 → 保留最新、丢最旧
        let req = build(
            None,
            vec![],
            vec![
                msg(Role::User, "old msg"),
                msg(Role::User, "mid msg"),
                msg(Role::User, "new msg"),
            ],
            vec![],
            vec![],
            None,
            None,
            ThinkingConfig::default(),
            11,
        );
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].content.as_text(), Some("mid msg"));
        assert_eq!(req.messages[1].content.as_text(), Some("new msg"));
    }

    #[test]
    fn history_window_fixes_leading_role() {
        // 窗口切在中间残留裸 assistant 开头 → 丢弃，修正为首条为 user。
        // 每条 2 字符 ≈ 2 token；预算 6 保留 [a1, q2, a2]（各 2）→ 首条裸 assistant 被丢弃 → [q2, a2]
        let req = build(
            None,
            vec![],
            vec![
                msg(Role::User, "q1"),
                msg(Role::Assistant, "a1"),
                msg(Role::User, "q2"),
                msg(Role::Assistant, "a2"),
            ],
            vec![],
            vec![],
            None,
            None,
            ThinkingConfig::default(),
            6,
        );
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].role, Role::User);
        assert_eq!(req.messages[1].role, Role::Assistant);
    }

    #[test]
    fn history_keeps_tool_call_round() {
        // 工具调用轮的 assistant(tool_calls) 开头：合法片段，不得被角色修正误删
        let mut a = msg(Role::Assistant, "");
        a.tool_calls = vec![crate::provider::ToolCall {
            id: "tc_1".into(),
            function: crate::provider::ToolCallFunction {
                name: "t".into(),
                arguments: "{}".into(),
            },
        }];
        let tool = msg(Role::Tool, "result");
        let req = build(
            None,
            vec![],
            vec![a.clone(), tool],
            vec![],
            vec![],
            None,
            None,
            ThinkingConfig::default(),
            100,
        );
        assert_eq!(req.messages.len(), 2, "tool-call round must be kept intact");
        assert_eq!(req.messages[0].role, Role::Assistant);
        assert!(req.messages[0].tool_calls.len() == 1);
        assert_eq!(req.messages[1].role, Role::Tool);
    }

    #[test]
    fn budget_zero_means_no_truncation() {
        let req = build(
            None,
            vec![],
            vec![msg(Role::User, "a"), msg(Role::User, "b")],
            vec![],
            vec![],
            None,
            None,
            ThinkingConfig::default(),
            0,
        );
        assert_eq!(req.messages.len(), 2);
    }

    #[test]
    fn params_preserved_through_truncation() {
        let req = build(
            None,
            vec![],
            vec![msg(Role::User, "hi")],
            vec![],
            vec![],
            Some(0.7),
            Some(256),
            ThinkingConfig::default(),
            100,
        );
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.max_tokens, Some(256));
    }

    // ── assemble：分段编排器 ─────────────────────────

    fn assemble_one(sections: Vec<SystemSection>, budget: usize) -> ChatRequest {
        assemble(AssembleParts {
            sections,
            tools: vec![],
            history: vec![msg(Role::User, "hi")],
            temperature: None,
            max_tokens: None,
            thinking: ThinkingConfig::default(),
            prompt_budget: budget,
        })
    }

    #[test]
    fn assemble_omits_empty_sections() {
        // omit_if_empty + 空文本 → 整段省略；非空段保留
        let req = assemble_one(
            vec![
                SystemSection::stable("identity"),
                SystemSection {
                    stable: true,
                    text: "   ".into(),
                    omit_if_empty: true,
                },
                SystemSection {
                    stable: false,
                    text: "volatile".into(),
                    omit_if_empty: false,
                },
            ],
            0,
        );
        let sys = &req.messages[0];
        let text = sys.content.as_text().unwrap();
        assert!(text.contains("identity"));
        assert!(text.contains("volatile"));
        assert!(!text.contains("   \n"));
    }

    #[test]
    fn assemble_orders_stable_first() {
        // 易变片段即便先传入，也排在稳定片段之后（缓存前缀铁律）
        let req = assemble_one(
            vec![
                SystemSection {
                    stable: false,
                    text: "volatile".into(),
                    omit_if_empty: false,
                },
                SystemSection::stable("stable-a"),
                SystemSection::stable("stable-b"),
            ],
            0,
        );
        let text = req.messages[0].content.as_text().unwrap();
        let stable_a = text.find("stable-a").unwrap();
        let stable_b = text.find("stable-b").unwrap();
        let volatile = text.find("volatile").unwrap();
        assert!(stable_a < volatile, "stable-a must precede volatile");
        assert!(stable_b < volatile, "stable-b must precede volatile");
        assert!(stable_a < stable_b, "stable-a before stable-b (stable order kept)");
    }

    #[test]
    fn assemble_empty_system_means_no_system_message() {
        // 全空且 omit → system 为 None，仅 history
        let req = assemble_one(
            vec![SystemSection {
                stable: true,
                text: String::new(),
                omit_if_empty: true,
            }],
            0,
        );
        assert!(req.messages.iter().all(|m| m.role != Role::System));
        assert_eq!(req.messages.len(), 1, "only history remains");
    }

    #[test]
    fn assemble_respects_budget() {
        // 稳定段超预算 → 按字符截断兜底（System 绝不整段丢弃）
        let req = assemble_one(vec![SystemSection::stable(&text_of_tokens(500))], 100);
        let sys = req.messages[0].content.as_text().unwrap();
        assert!(sys.ends_with("[Truncated]"));
        assert!(TokenEstimator::estimate(sys) <= 100);
    }
}

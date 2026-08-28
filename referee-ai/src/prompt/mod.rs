//! 提示词组装与预算截断 — Phase 5
//!
//! 将系统提示词 / 工具声明 / 对话历史 / 记忆 / 工件等碎片统一组装为
//! [`ChatRequest`]，并按 Token 预算（`prompt_budget`）做优先级截断，
//! 杜绝"Prompt 爆炸"（AGENT_RUNTIME_PLAN §5.6）。
//!
//! ## 优先级（超限按序丢弃，System 最后保留）
//! `system > 工具声明 > 对话历史 > 记忆 > 工件`
//!
//! ## 截断策略（职责分离：核心载荷恒保留，可裁上下文按预算裁剪）
//! - **System**：恒保留，预算不足时按字符截断文本（绝不整段丢弃）
//! - **核心载荷**（history 末条的 user = 当前轮请求输入）：**恒完整保留**，
//!   绝不因预算裁剪或 reorder；超预算仅告警
//!   ——硬上限由 engine 依据 `ModelSpec.context_window_tokens` 兜底（fail-loud）。
//! - **Tools**：高优先级，仅当整段超剩余预算才整体丢弃
//! - **可裁 History / Memory / Artifacts**：按剩余预算裁剪；滑动窗口保留最近 N 条，
//!   截断后修正首条角色（残留的 assistant/tool 开头会 400）；每次丢弃显式告警
//!
//! ## 设计约束
//! - 纯函数，无 I/O 句柄；只依赖 `provider` 与 `budget`（分层单向依赖）
//! - 可裁上下文估算 ≤ `prompt_budget`；核心载荷恒保留、可超预算受 engine 护栏约束
//! - 所有截断 / 丢弃均为**可观测降级**（`tracing::warn!` + metrics），杜绝静默丢失
//!   （估算系数与截断系数同源，见 [`CHARS_PER_TOKEN`]）

use crate::budget::TokenEstimator;
use crate::observe::prompt_truncated;
use crate::provider::{
    ChatRequest, Message, MessageContent, Role, ThinkingConfig, ToolDeclaration,
};
use std::collections::VecDeque;
use tracing::warn;

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

/// 分离「当前轮输入」：chat 请求的末条 user 消息恒为可交付核心，**不可裁剪**。
///
/// 这是职责分离的根基——请求载荷（函数的输入）与可裁剪的历史上下文（函数
/// 的记忆）语义不同：前者必须完整交付或明确报错，后者才允许按预算裁剪。
/// 返回 `(核心载荷, 可裁剪历史)`；末条非 user（如工具轮收尾）则无核心载荷，
/// 全部历史视为可裁剪。
fn split_round_input(mut history: Vec<Message>) -> (Option<Message>, Vec<Message>) {
    if history.last().is_some_and(|m| m.role == Role::User) {
        let core = history.pop().expect("last message exists");
        (Some(core), history)
    } else {
        (None, history)
    }
}

/// 片段类型名（日志 / metrics 标签用，不含内容）
fn fragment_kind(f: &PromptFragment) -> &'static str {
    match f {
        PromptFragment::System(_) => "system",
        PromptFragment::Tools(_) => "tools",
        PromptFragment::History(_) => "history",
        PromptFragment::Memory(_) => "memory",
        PromptFragment::Artifacts(_) => "artifacts",
    }
}

/// 组装【可裁】片段列表（Tools > History > Memory > Artifacts），空片段省略
fn build_cuttable(
    tools: Vec<ToolDeclaration>,
    history: Vec<Message>,
    memory: Vec<Message>,
    artifacts: Vec<Message>,
) -> Vec<PromptFragment> {
    let mut frags = Vec::new();
    if !tools.is_empty() {
        frags.push(PromptFragment::Tools(tools));
    }
    if !history.is_empty() {
        frags.push(PromptFragment::History(history));
    }
    if !memory.is_empty() {
        frags.push(PromptFragment::Memory(memory));
    }
    if !artifacts.is_empty() {
        frags.push(PromptFragment::Artifacts(artifacts));
    }
    frags
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
                .map(TokenEstimator::estimate_message)
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
                warn!(
                    budget,
                    before_tokens = cost,
                    after_tokens = new_cost,
                    "system prompt truncation: text character-cropped to fit budget"
                );
                prompt_truncated("system");
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
                let total_tokens: u64 = msgs
                    .iter()
                    .map(TokenEstimator::estimate_message)
                    .sum();
                let mut current_tokens = 0u64;
                let mut keep = VecDeque::new();
                for msg in msgs.iter().rev() {
                    let cost = TokenEstimator::estimate_message(msg);
                    if current_tokens + cost > budget {
                        // 窗口截断不可避免（budget 是收紧上限），必须显式告警。
                        // 若连最早一条都放不下（keep 为空），则整段可裁历史尽数丢弃——
                        // 当前轮核心载荷仍由 finalize 恒定保留，绝不静默消失。
                        if keep.is_empty() {
                            warn!(
                                budget,
                                fragment_tokens = total_tokens,
                                "history fragment fully dropped: budget too small to fit earliest message"
                            );
                        } else {
                            warn!(
                                budget,
                                kept_tokens = current_tokens,
                                dropped_msgs = msgs.len() - keep.len(),
                                dropped_tokens = total_tokens - current_tokens,
                                "history truncation: earliest messages dropped to fit budget"
                            );
                        }
                        prompt_truncated("history");
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

    finalize(PromptParts {
        system,
        tools,
        history,
        memory: Vec::new(),
        artifacts: Vec::new(),
        temperature,
        max_tokens,
        thinking,
        prompt_budget,
    })
}

/// 组装并截断 Prompt，生成最终 `ChatRequest`（legacy 入口）
///
/// 委托 [`finalize`]，与 [`assemble`] 共享同一套截断逻辑。
pub fn build_prompt(parts: PromptParts) -> ChatRequest {
    finalize(parts)
}

/// 统一截断与组装 — 按职责分离：核心载荷恒保留，可裁上下文按剩余预算裁剪
///
/// 三层职责（详见模块文档）：
/// - **核心载荷**（system + 当前轮 user 消息）：恒保留；system 超预算按字符截断
///   兜底，当前轮输入恒完整交付、绝不裁剪。二者超出 `prompt_budget` **仅告警**——
///   `prompt_budget` 是"可裁上下文"的收紧上限，不是核心载荷的交付门槛。
/// - **可裁上下文**（tools + 可裁历史 + memory + artifacts）：按剩余预算裁剪，
///   每次丢弃 / 截断显式 `WARN` + metrics（不再静默）。
/// - **context 硬护栏**由 engine 依据 `ModelSpec.context_window_tokens` 兜底：
///   核心载荷放不进模型窗口时显式报错（`EngineStartError::PromptTooLarge`）。
fn finalize(parts: PromptParts) -> ChatRequest {
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

    // 1. 剥离当前轮核心载荷（末条 user，不可裁剪）；余下为可裁历史
    let (core, history) = split_round_input(history);

    // 2. System 恒保留，最高优先：超预算按字符截断兜底（绝不整段丢弃）
    let mut sys_msg: Option<Message> = None;
    let mut sys_cost = 0u64;
    if let Some(sys) = system {
        if let Some((PromptFragment::System(m), cost)) =
            PromptFragment::System(sys).truncate(prompt_budget as u64)
        {
            sys_msg = Some(m);
            sys_cost = cost;
        }
    }

    // 3. 核心载荷恒保留：超预算仅告警（预算 = 可裁上下文收紧上限，非交付门槛；
    //    模型窗口这一硬上限由 engine 的 context 护栏兜底）
    let core_cost = core
        .as_ref()
        .map(TokenEstimator::estimate_message)
        .unwrap_or(0);
    if core_cost > prompt_budget as u64 {
        warn!(
            budget = prompt_budget,
            core_tokens = core_cost,
            "current-round request input kept in full despite exceeding prompt budget \
             (hard limit enforced by engine context-window guard)"
        );
    }

    // 4. 可裁上下文预算 = 总预算 − system。核心载荷是恒交付载荷，`remaining`
    //    不减 core——否则超大输入会把 tools / 上下文一并连坐丢弃。其窗口硬上限
    //    由 engine 依据 `context_window_tokens` 兜底（fail-loud）。
    let mut remaining = (prompt_budget as u64).saturating_sub(sys_cost);

    // 5. system 恒保留，置首；当前轮核心载荷在可裁上下文落定后按原始相对位置
    //    （对话末尾）追加——只豁免裁剪，不改变消息顺序
    let mut messages: Vec<Message> = Vec::new();
    if let Some(m) = sys_msg {
        messages.push(m);
    }

    // 6. 可裁上下文分段截断（Tools > History > Memory > Artifacts）
    let mut final_tools: Vec<ToolDeclaration> = Vec::new();
    for fragment in build_cuttable(tools, history, memory, artifacts) {
        match fragment.truncate(remaining) {
            Some((kept, cost)) => {
                match kept {
                    PromptFragment::Tools(t) => final_tools = t,
                    PromptFragment::History(msgs)
                    | PromptFragment::Memory(msgs)
                    | PromptFragment::Artifacts(msgs) => messages.extend(msgs),
                    PromptFragment::System(_) => {
                        unreachable!("system handled above, outside budget loop")
                    }
                }
                remaining = remaining.saturating_sub(cost);
            }
            None => {
                warn!(
                    kind = fragment_kind(&fragment),
                    remaining,
                    "cuttable fragment dropped: remaining budget exhausted"
                );
                prompt_truncated(fragment_kind(&fragment));
            }
        }
    }
    // 核心载荷恒在对话末尾交付，绝不因预算裁剪或 reorder
    if let Some(c) = core {
        messages.push(c);
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
            vec![msg(Role::User, &text_of_tokens(200))], // history 末条 = 核心载荷
            vec![msg(Role::User, &text_of_tokens(200))], // memory：可裁，超剩余预算丢弃
            vec![msg(Role::User, &text_of_tokens(200))], // artifacts：可裁，丢弃
            None,
            None,
            ThinkingConfig::default(),
            150,
        );

        // 预算 150：System(≈50)+Tools(≈6) 保留；可裁上下文 History/Artifacts
        // 预算不足被丢弃。但末条 user（history 末条，200 token）是**核心载荷**，
        // 恒完整保留在末尾，绝不因预算丢弃。

        // 核心载荷 + system 恒保留：System 在前，核心载荷在对话末尾
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].role, Role::System);
        assert_eq!(
            req.messages[1].content.as_text(),
            Some(text_of_tokens(200).as_str()),
            "current-round request input must be kept in full"
        );
        // Tools 完整保留
        assert_eq!(req.tools.len(), 1);
        // memory / artifacts（可裁上下文）被丢弃：仅核心载荷这一条 user 残留
        let user_count = req
            .messages
            .iter()
            .filter(|m| m.role == Role::User)
            .count();
        assert_eq!(user_count, 1, "only the core request input may remain, got {req:?}");
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
    fn core_input_exceeding_budget_kept_in_full() {
        // 反馈复现场景：单条 user 消息本身超预算，必须**完整保留**、绝不静默丢弃
        // （旧实现会把该条整体截掉，模型收不到输入）。预算 5，核心载荷 ≈ 100 token，
        // 恒保留于末尾；无 system / 工具，可裁上下文为空不受连坐。
        let long = text_of_tokens(100);
        let req = build(
            None,
            vec![],
            vec![msg(Role::User, &long)],
            vec![],
            vec![],
            None,
            None,
            ThinkingConfig::default(),
            5,
        );
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.messages[0].content.as_text(), Some(long.as_str()));
    }

    #[test]
    fn history_sliding_window_keeps_recent() {
        let req = build(
            None,
            vec![],
            // 每条 7 字符 ≈ 5 token；预算 11 只够 2 条 → 可裁历史保留 [old, mid]
            //（丢 oldest）；末条 user「new msg」是核心载荷，独立恒保留于末尾
            vec![
                msg(Role::User, "oldest msg"),
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
        assert_eq!(req.messages.len(), 3);
        assert_eq!(req.messages[0].content.as_text(), Some("old msg"));
        assert_eq!(req.messages[1].content.as_text(), Some("mid msg"));
        assert_eq!(req.messages[2].content.as_text(), Some("new msg"));
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
        let req = assemble_one(vec![SystemSection::stable(text_of_tokens(500))], 100);
        let sys = req.messages[0].content.as_text().unwrap();
        assert!(sys.ends_with("[Truncated]"));
        assert!(TokenEstimator::estimate(sys) <= 100);
    }
}

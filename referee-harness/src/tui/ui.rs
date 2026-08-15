//! TUI 渲染层 — 纯绘制，不修改状态。
//!
//! 版式对齐 OpenCode/ClaudeCode（参考设计稿 opencode.html）：
//! ```text
//! ◆ referee · 工作目录           model: xxx  ●generating  session 8f3a
//! ────────────────────────────────────────────────────────────
//! 消息区：» 用户 / ● thinking 折叠块 / ● 助手 / · 系统
//! ────────────────────────────────────────────────────────────
//!  / 命令  ↑↓ 历史  PgUp/PgDn 滚动  Ctrl+U 清空  Ctrl+T 思考  Esc 中断  Ctrl+C 退出
//! ❯ 输入消息…（输入 / 唤起命令）
//! ◆ general-abc · deepseek-v3 · 会话 8f3a      1234 tok · 就绪
//! ```

use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use unicode_width::UnicodeWidthChar;

use crate::tui::app::{short_id, App, ChatLine, ChatRole, COMMANDS, CreateForm, Modal};

// OpenCode 风格配色（对齐设计稿 CSS 变量）
const ELEV: Color = Color::Rgb(17, 20, 26);
const FG: Color = Color::Rgb(196, 204, 214);
const FG_BRIGHT: Color = Color::Rgb(240, 244, 248);
const FG_MUTED: Color = Color::Rgb(110, 118, 129);
const FG_DIM: Color = Color::Rgb(72, 79, 88);
const ACCENT: Color = Color::Rgb(121, 192, 255);
const ACCENT_PURPLE: Color = Color::Rgb(210, 168, 255);
const USER: Color = Color::Rgb(126, 231, 135);
const THINKING: Color = Color::Rgb(163, 113, 247);
const WARN: Color = Color::Rgb(240, 136, 62);
const ERROR: Color = Color::Rgb(255, 123, 114);

/// 主渲染入口
pub fn draw(frame: &mut Frame, app: &App) {
    // Home 空态（无对话）：居中 Logo + 居中输入框 + 底部 footer（OpenCode Home 风格）
    let home = app.chat_lines.is_empty() && !app.chat_active;
    let input_top = if home {
        draw_home(frame, app)
    } else {
        let [header, messages, hint, input, status] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .areas(frame.area());

        draw_header(frame, header, app);
        draw_messages(frame, messages, app);
        draw_hint(frame, hint, app);
        draw_input(frame, input, app);
        draw_status(frame, status, app);
        input.y
    };

    // 模态弹层（覆盖其余内容）
    if let Some(modal) = app.modal {
        match modal {
            Modal::Switch => draw_switch_modal(frame, app),
            Modal::Sessions => draw_sessions_modal(frame, app),
            Modal::New => draw_new_modal(frame, app),
            Modal::Help => draw_help_modal(frame),
        }
    } else if app.chat_input.starts_with('/') && !app.chat_input.starts_with("//") {
        draw_command_palette(frame, app, input_top);
    }
}

/// Home 空态：居中 Logo + 居中输入框 + 底部 footer。返回输入框顶部 y（供命令面板锚定）。
fn draw_home(frame: &mut Frame, app: &App) -> u16 {
    let area = frame.area();
    let areas = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(3),
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .split(area);
    let logo = areas[1];
    let input = areas[3];
    let footer = areas[5];

    // 居中 Logo
    let logo_text = Text::from(vec![
        Line::from(Span::styled(
            "◆ referee",
            Style::new().fg(FG_BRIGHT).add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "轻量引擎 · 对话式编码代理",
            Style::new().fg(FG_MUTED),
        )),
    ])
    .alignment(Alignment::Center);
    frame.render_widget(Clear, logo);
    frame.render_widget(Paragraph::new(logo_text).alignment(Alignment::Center), logo);

    // 居中输入框（水平居中，maxWidth 限制）
    let input_width = (75u16).min(area.width.saturating_sub(4));
    let input_area = centered_h(input_width, input);
    draw_input(frame, input_area, app);

    // 底部 footer（快捷键提示）
    draw_hint(frame, footer, app);

    input.y
}

/// 水平居中一个指定宽度的区域（y/height 不变）
fn centered_h(width: u16, area: Rect) -> Rect {
    let [_, mid, _] = Layout::horizontal([
        Constraint::Min(0),
        Constraint::Length(width),
        Constraint::Min(0),
    ])
    .areas(area);
    Rect::new(mid.x, area.y, mid.width, area.height)
}

// ── 顶部 Header ──────────────────────────────

fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    frame.render_widget(Clear, area);

    let path = if app.cwd.is_empty() {
        app.instance_id.clone()
    } else {
        app.cwd.clone()
    };
    let left = vec![
        Span::styled("◆ ", Style::new().fg(USER)),
        Span::styled(
            "referee",
            Style::new().fg(FG_BRIGHT).add_modifier(Modifier::BOLD),
        ),
        Span::styled(" · ", Style::new().fg(FG_DIM)),
        Span::styled(path, Style::new().fg(FG_MUTED)),
    ];

    let model = if app.instance_model.is_empty() {
        "—".to_string()
    } else {
        app.instance_model.clone()
    };
    let (status_ch, status_color, status_label) = if app.chat_active {
        let spins: Vec<char> = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏".chars().collect();
        (
            spins[(app.tick as usize / 2) % spins.len()],
            WARN,
            "generating",
        )
    } else {
        ('●', USER, "idle")
    };
    let session = if app.session_id.is_empty() {
        "—".to_string()
    } else {
        short_id(&app.session_id)
    };
    let right = vec![
        Span::styled("model ", Style::new().fg(FG_DIM)),
        Span::styled(model, Style::new().fg(ACCENT)),
        Span::raw("  "),
        Span::styled(status_ch.to_string(), Style::new().fg(status_color)),
        Span::styled(format!(" {status_label}"), Style::new().fg(FG_MUTED)),
        Span::raw("  "),
        Span::styled("session ", Style::new().fg(FG_DIM)),
        Span::styled(session, Style::new().fg(FG)),
    ];

    frame.render_widget(Paragraph::new(row_span(left, right, area.width)), area);
}

/// 左右拼接为一行（右侧贴边；超宽时右侧丢弃）
fn row_span(left: Vec<Span<'static>>, right: Vec<Span<'static>>, width: u16) -> Line<'static> {
    let lw: usize = left.iter().map(|s| s.width()).sum();
    let rw: usize = right.iter().map(|s| s.width()).sum();
    let width = width as usize;
    if lw + rw + 2 <= width {
        let mut spans = left;
        spans.push(Span::raw(" ".repeat(width - lw - rw)));
        spans.extend(right);
        Line::from(spans)
    } else {
        Line::from(left)
    }
}

// ── 消息区 ──────────────────────────────────

fn draw_messages(frame: &mut Frame, area: Rect, app: &App) {
    if area.height == 0 {
        return;
    }
    // 先清空：ratatui 逐帧 diff，未覆盖区域旧内容会残留
    frame.render_widget(Clear, area);
    // 无消息：居中欢迎提示
    if app.chat_lines.is_empty() {
        let hint = Paragraph::new(Text::from(vec![
            Line::from(Span::styled(
                "◆ referee — 输入消息或按 / 唤起命令",
                Style::new().fg(FG_DIM),
            )),
            Line::from(Span::styled(
                "示例：/new 创建实例 · /list 切换实例 · /help 查看帮助",
                Style::new().fg(FG_DIM),
            )),
        ]))
        .alignment(Alignment::Center);
        frame.render_widget(hint, area);
        return;
    }

    // 每条消息：预换行文本 + 显示高度（含消息间隔空行）
    let width = area.width;
    let mut blocks: Vec<(Text<'static>, usize)> = Vec::with_capacity(app.chat_lines.len());
    for (i, m) in app.chat_lines.iter().enumerate() {
        let streaming = app.chat_active && i == app.chat_lines.len() - 1 && m.role == ChatRole::Assistant;
        let (text, h) = message_text(m, streaming, app.tick, width);
        blocks.push((text, h + 1)); // +1 消息间隔空行
    }
    let total: usize = blocks.iter().map(|(_, h)| *h).sum();
    let view_h = area.height as usize;

    // 滚动窗口：scroll = 距底部行数（0 = 跟随最新）
    let scroll = app.chat_scroll.min(total.saturating_sub(view_h));
    let end_row = total.saturating_sub(scroll);
    let start_row = end_row.saturating_sub(view_h);

    // 定位首个可见消息与顶部跳过行数
    let mut row = 0usize;
    let mut start_idx = 0usize;
    let mut top_skip = 0usize;
    for (i, (_, h)) in blocks.iter().enumerate() {
        if row + *h > start_row {
            start_idx = i;
            top_skip = start_row - row;
            break;
        }
        row += *h;
    }

    // 构建可见消息的垂直布局（首条部分可见，末条按剩余高度裁剪）
    let mut constraints = Vec::new();
    let mut remaining = view_h;
    let mut cursor = start_idx;
    let mut first_skip = top_skip;
    while remaining > 0 && cursor < blocks.len() {
        let h = blocks[cursor].1;
        let visible = h.saturating_sub(first_skip);
        if visible == 0 {
            break;
        }
        let len = visible.min(remaining);
        constraints.push(Constraint::Length(len as u16));
        remaining -= len;
        first_skip = 0;
        cursor += 1;
    }
    if constraints.is_empty() {
        return;
    }

    let areas = Layout::vertical(&constraints).split(area);
    for (k, (text, _)) in blocks.iter().enumerate().skip(start_idx).take(constraints.len()) {
        let sub = areas[k - start_idx];
        let mut para = Paragraph::new(text.clone());
        if k == start_idx && top_skip > 0 {
            para = para.scroll((top_skip as u16, 0));
        }
        frame.render_widget(para, sub);
    }
}

/// 单条消息 → 预换行文本（角色着色；助手含 thinking 块 / 加载动画 / 流式光标）
fn message_text(m: &ChatLine, streaming: bool, tick: u64, width: u16) -> (Text<'static>, usize) {
    let width = width.max(1) as usize;
    let mut lines: Vec<Line<'static>> = Vec::new();
    match m.role {
        // 用户消息：左侧竖条边框（OpenCode 风格，`border:["left"]`），
        // 内容自列 2 起，与助手消息内容对齐形成消息层级。
        ChatRole::User => {
            let segs = wrap_to_lines(&m.text, width.saturating_sub(2));
            for seg in segs {
                lines.push(Line::from(vec![
                    Span::styled("│ ", Style::new().fg(USER).add_modifier(Modifier::BOLD)),
                    Span::styled(seg, Style::new().fg(FG_BRIGHT)),
                ]));
            }
        }
        // 助手消息：统一从列 2 起缩进（`indent`），与用户消息内容（左竖线占 2 列）对齐，
        // 形成「用户左竖线 → 助手缩进」的层级；thinking 内容再缩进一级。
        ChatRole::Assistant => {
            let indent = "  ";
            let has_reasoning = !m.reasoning.is_empty();
            // thinking 折叠块
            if has_reasoning {
                let arrow = if m.show_thinking { "▼" } else { "▶" };
                let mut spans = vec![
                    Span::raw(indent),
                    Span::styled("● ", Style::new().fg(ACCENT_PURPLE)),
                    Span::styled(
                        format!("thinking {arrow} · {} 字符", m.reasoning.chars().count()),
                        Style::new().fg(THINKING),
                    ),
                ];
                // 思考中（文本未开始）：尾部加转圈
                if streaming && m.text.is_empty() {
                    let spins: Vec<char> = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏".chars().collect();
                    spans.push(Span::styled(
                        format!(" {}", spins[(tick as usize / 2) % spins.len()]),
                        Style::new().fg(FG_MUTED),
                    ));
                }
                lines.push(Line::from(spans));
                if m.show_thinking {
                    for seg in wrap_to_lines(&m.reasoning, width.saturating_sub(4)) {
                        lines.push(Line::from(vec![
                            Span::raw("    "),
                            Span::styled(seg, Style::new().fg(FG_MUTED).italic()),
                        ]));
                    }
                }
            }
            // 内容文本（无内容时：思考中则不画，否则画加载动画）
            if m.text.is_empty() {
                if !has_reasoning && streaming {
                    lines.push(loading_dots(tick));
                }
            } else {
                let segs = wrap_to_lines(&m.text, width.saturating_sub(2));
                for (i, seg) in segs.iter().enumerate() {
                    let mut spans = Vec::new();
                    if i == 0 && !has_reasoning {
                        spans.push(Span::raw(indent));
                        spans.push(Span::styled("● ", Style::new().fg(ACCENT_PURPLE)));
                    } else {
                        spans.push(Span::raw(indent));
                    }
                    spans.push(Span::styled(seg.clone(), Style::new().fg(FG)));
                    if streaming && i == segs.len() - 1 {
                        spans.push(Span::styled(
                            "▍",
                            Style::new().fg(ACCENT_PURPLE).add_modifier(Modifier::SLOW_BLINK),
                        ));
                    }
                    lines.push(Line::from(spans));
                }
            }
        }
        ChatRole::System => {
            let segs = wrap_to_lines(&m.text, width.saturating_sub(2));
            for (i, seg) in segs.iter().enumerate() {
                let mut spans = Vec::new();
                if i == 0 {
                    spans.push(Span::styled("· ", Style::new().fg(FG_DIM)));
                }
                spans.push(Span::raw(seg.clone()));
                lines.push(Line::from(spans));
            }
        }
    }
    if lines.is_empty() {
        lines.push(Line::default());
    }
    let height = lines.len();
    (Text::from(lines), height)
}

/// 加载动画：三点轮播（与助手消息同缩进）
fn loading_dots(tick: u64) -> Line<'static> {
    let base = (tick as usize / 3) % 3;
    let mut spans = vec![
        Span::raw("  "),
        Span::styled("● ", Style::new().fg(ACCENT_PURPLE)),
    ];
    for i in 0..3 {
        let on = (base + i).is_multiple_of(3);
        spans.push(Span::styled(
            if on { "●" } else { "○" },
            Style::new().fg(if on { ACCENT_PURPLE } else { FG_DIM }),
        ));
    }
    Line::from(spans)
}

/// 按显示宽度手动换行（CJK 全角按 2 列计）
fn wrap_to_lines(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut cur_w = 0usize;
    for ch in text.chars() {
        let w = UnicodeWidthChar::width(ch).unwrap_or(0);
        if ch == '\n' || cur_w + w > width {
            out.push(std::mem::take(&mut cur));
            cur_w = 0;
            if ch == '\n' {
                continue;
            }
        }
        cur.push(ch);
        cur_w += w;
    }
    if !cur.is_empty() || out.is_empty() {
        out.push(cur);
    }
    out
}

// ── 输入框 ──────────────────────────────────

fn draw_input(frame: &mut Frame, area: Rect, app: &App) {
    frame.render_widget(Clear, area);
    let width = area.width.saturating_sub(1) as usize;
    let prompt = "❯ ";
    let prompt_len = 2;

    let input = &app.chat_input;
    let len = input.chars().count();
    let cursor = app.input_cursor.min(len);
    let max = width.saturating_sub(prompt_len);
    // 光标跟随：超宽时滚动输入窗口
    let offset = len.saturating_sub(max).min(cursor);
    let chars: Vec<char> = input.chars().skip(offset).take(max).collect();
    let visible: String = chars.iter().collect();
    let vis_cursor = (cursor - offset).min(max);

    let line = if visible.is_empty() {
        Line::from(vec![
            Span::styled(prompt, Style::new().fg(ACCENT_PURPLE).add_modifier(Modifier::BOLD)),
            Span::styled("输入消息…（输入 / 唤起命令）", Style::new().fg(FG_DIM).italic()),
        ])
    } else {
        Line::from(vec![
            Span::styled(prompt, Style::new().fg(ACCENT_PURPLE).add_modifier(Modifier::BOLD)),
            Span::raw(visible),
        ])
    };
    frame.render_widget(Paragraph::new(line).style(Style::new().fg(FG_BRIGHT)), area);
    let cx = area
        .x
        .saturating_add(prompt_len as u16)
        .saturating_add(vis_cursor as u16)
        .min(area.right().saturating_sub(1));
    frame.set_cursor_position(Position::new(cx, area.y));
}

// ── 底部提示行（kbd 风格）───────────────────

fn key_span(s: &str) -> Span<'static> {
    Span::styled(s.to_string(), Style::new().fg(FG_MUTED))
}

fn draw_hint(frame: &mut Frame, area: Rect, app: &App) {
    frame.render_widget(Clear, area);
    let line = Line::from(vec![
        Span::raw(" "),
        key_span("/ 命令"),
        Span::raw("  "),
        key_span("↑↓ 历史"),
        Span::raw("  "),
        key_span("PgUp/PgDn 滚动"),
        Span::raw("  "),
        key_span("Ctrl+U 清空"),
        Span::raw("  "),
        key_span("Ctrl+T 思考"),
        Span::raw("  "),
        if app.chat_active {
            key_span("Esc 中断")
        } else {
            key_span("Esc 清空")
        },
        Span::raw("  "),
        key_span("Ctrl+C 退出"),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::new().fg(FG_DIM)),
        area,
    );
}

// ── 底部状态栏（实例 · 模型 · 会话 | tokens · 状态）──

fn draw_status(frame: &mut Frame, area: Rect, app: &App) {
    frame.render_widget(Clear, area);

    let inst = if app.instance_id.is_empty() {
        "未连接实例".to_string()
    } else {
        app.instance_id.clone()
    };
    let model = if app.instance_model.is_empty() {
        "—".to_string()
    } else {
        app.instance_model.clone()
    };
    let session = if app.session_id.is_empty() {
        "新会话".to_string()
    } else {
        format!("会话 {}", short_id(&app.session_id))
    };
    let left = vec![
        Span::styled("◆ ", Style::new().fg(USER)),
        Span::styled(inst, Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::raw(" · "),
        Span::styled(model, Style::new().fg(ACCENT)),
        Span::raw(" · "),
        Span::styled(session, Style::new().fg(FG_MUTED)),
    ];

    let status = if app.status.is_empty() {
        "就绪".to_string()
    } else {
        app.status.clone()
    };
    let status_fg = if app.status_error {
        ERROR
    } else {
        FG_MUTED
    };
    let right = vec![
        Span::raw(format!("{} tok", app.consumed_tokens)),
        Span::raw(" · "),
        Span::styled(status, Style::new().fg(status_fg)),
    ];

    frame.render_widget(Paragraph::new(row_span(left, right, area.width)), area);
}

// ── 命令面板（输入 / 前缀时弹出）─────────────

fn draw_command_palette(frame: &mut Frame, app: &App, input_top: u16) {
    let input = app.chat_input.as_str();
    let matches: Vec<(&str, &str)> = COMMANDS
        .iter()
        .filter(|(name, _)| name.starts_with(input))
        .copied()
        .collect();
    if matches.is_empty() {
        return;
    }
    let width = area_width_for(&matches).min(frame.area().width.saturating_sub(4));
    let height = (matches.len() as u16).saturating_add(2).min(input_top.saturating_sub(1));
    if height < 2 {
        return;
    }
    let x = frame
        .area()
        .x
        .saturating_add(frame.area().width.saturating_sub(width).saturating_div(2));
    let y = input_top.saturating_sub(height);
    let popup = Rect::new(x, y, width, height);

    frame.render_widget(Clear, popup);
    let items: Vec<ListItem> = matches
        .iter()
        .map(|(name, desc)| {
            ListItem::new(Line::from(vec![
                Span::styled(*name, Style::new().fg(ACCENT).add_modifier(Modifier::BOLD)),
                Span::raw("  "),
                Span::styled(*desc, Style::new().fg(FG_MUTED)),
            ]))
        })
        .collect();
    // 首项（回车将执行的命令）高亮，作为选中暗示
    let list = List::new(items)
        .block(Block::bordered().title(" 命令 ").border_type(BorderType::Rounded))
        .highlight_style(Style::new().bg(ELEV).add_modifier(Modifier::BOLD))
        .highlight_symbol("❯ ");
    let mut state = ListState::default();
    state.select(Some(0));
    frame.render_stateful_widget(list, popup, &mut state);
}

fn area_width_for(matches: &[(&str, &str)]) -> u16 {
    let w = matches
        .iter()
        .map(|(n, d)| n.chars().count() + d.chars().count() + 2)
        .max()
        .unwrap_or(20)
        .max(20) as u16;
    w.saturating_add(4)
}

// ── 模态弹层 ─────────────────────────────────

/// 居中弹层区域
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .split(area)[1];
    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(vertical)[1]
}

fn draw_switch_modal(frame: &mut Frame, app: &App) {
    let popup = centered_rect(62, 70, frame.area());
    frame.render_widget(Clear, popup);

    let items: Vec<ListItem> = app
        .instances
        .iter()
        .map(|i| {
            ListItem::new(Line::from(vec![
                Span::styled(i.id.as_str(), Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::raw("  "),
                Span::styled(&i.model, Style::new().fg(ACCENT)),
                Span::raw(format!("  {} 会话  {} tok", i.sessions, i.consumed_tokens)),
            ]))
        })
        .collect();
    let list = if items.is_empty() {
        List::new(vec![ListItem::new(
            Span::styled("（无实例 — 输入 /new 创建）", Style::new().fg(FG_MUTED)),
        )])
    } else {
        List::new(items)
    }
    .block(
        Block::bordered()
            .title(" 切换实例 — /new 新建, Enter 选择, Esc 取消 ")
            .border_type(BorderType::Rounded),
    )
    .highlight_style(Style::new().bg(ELEV).add_modifier(Modifier::BOLD))
    .highlight_symbol("❯ ");

    let mut state = ListState::default();
    state.select(Some(app.instance_selected));
    frame.render_stateful_widget(list, popup, &mut state);
}

fn draw_sessions_modal(frame: &mut Frame, app: &App) {
    let popup = centered_rect(62, 70, frame.area());
    frame.render_widget(Clear, popup);

    let items: Vec<ListItem> = app
        .sessions
        .iter()
        .map(|s| {
            ListItem::new(Line::from(vec![
                Span::styled(short_id(&s.id), Style::new().fg(Color::Yellow)),
                Span::raw(format!("  {} 条消息  {} tok  {}", s.messages, s.consumed_tokens, s.phase)),
            ]))
        })
        .collect();
    let list = if items.is_empty() {
        List::new(vec![ListItem::new(
            Span::styled("（该实例暂无会话）", Style::new().fg(FG_MUTED)),
        )])
    } else {
        List::new(items)
    }
    .block(
        Block::bordered()
            .title(" 切换会话 — Enter 选择, Esc 取消 ")
            .border_type(BorderType::Rounded),
    )
    .highlight_style(Style::new().bg(ELEV).add_modifier(Modifier::BOLD))
    .highlight_symbol("❯ ");

    let mut state = ListState::default();
    state.select(Some(app.session_selected));
    frame.render_stateful_widget(list, popup, &mut state);
}

fn draw_new_modal(frame: &mut Frame, app: &App) {
    let popup = centered_rect(72, 80, frame.area());
    frame.render_widget(Clear, popup);

    let labels = CreateForm::labels();
    let mut lines = Vec::new();
    for (i, label) in labels.iter().enumerate() {
        let val = app.form.field(i);
        let focused = i == app.form_field;
        let prefix = if focused { "❯ " } else { "  " };
        let label_span = if focused {
            Span::styled(format!("{prefix}{label}: "), Style::new().fg(Color::Yellow).bold())
        } else {
            Span::styled(format!("{prefix}{label}: "), Style::new().fg(ACCENT))
        };
        let value = if val.is_empty() {
            Span::styled("(空)", Style::new().fg(FG_MUTED).italic())
        } else {
            Span::raw(val.to_string())
        };
        lines.push(Line::from(vec![label_span, value]));
        lines.push(Line::default());
    }
    lines.push(Line::from(vec![Span::styled(
        " Tab 切换字段 · Enter 提交 · Esc 取消 ",
        Style::new().fg(FG_MUTED).italic(),
    )]));

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::bordered().title(" 创建实例 ").border_type(BorderType::Rounded))
            .wrap(Wrap { trim: false }),
        popup,
    );

    // 光标定位到当前字段输入处
    let focus_row = (app.form_field * 2 + 1) as u16;
    let col = (format!("{}: ", labels[app.form_field]).chars().count() as u16) + 2;
    let x = popup
        .x
        .saturating_add(col)
        .min(popup.right().saturating_sub(2));
    let y = popup
        .y
        .saturating_add(focus_row)
        .min(popup.bottom().saturating_sub(2));
    frame.set_cursor_position(Position::new(x, y));
}

fn draw_help_modal(frame: &mut Frame) {
    let popup = centered_rect(66, 70, frame.area());
    frame.render_widget(Clear, popup);

    let mut lines = Vec::new();
    lines.push(Line::from(Span::styled(" 命令 ", Style::new().fg(Color::Yellow).bold())));
    for (name, desc) in COMMANDS {
        lines.push(Line::from(vec![
            Span::styled(*name, Style::new().fg(ACCENT).bold()),
            Span::raw("  —  "),
            Span::styled(*desc, Style::new().fg(FG)),
        ]));
    }
    lines.push(Line::default());
    lines.push(Line::from(Span::styled(" 快捷键 ", Style::new().fg(Color::Yellow).bold())));
    let keys = [
        ("Enter", "发送消息 / 执行命令"),
        ("↑↓", "浏览输入历史"),
        ("PgUp/PgDn", "滚动对话（跟随最新时自动回到底部）"),
        ("Ctrl+U", "清空输入"),
        ("Ctrl+T", "展开/折叠当前助手消息的 thinking 块"),
        ("Esc", "生成中中断；空闲清空输入"),
        ("Ctrl+C / Ctrl+D", "退出 TUI"),
    ];
    for (k, v) in keys {
        lines.push(Line::from(vec![
            Span::styled(k, Style::new().fg(ACCENT).bold()),
            Span::raw("  —  "),
            Span::styled(v, Style::new().fg(FG)),
        ]));
    }
    lines.push(Line::default());
    lines.push(Line::from(Span::styled(
        " 按 Esc 或 Enter 关闭帮助 ",
        Style::new().fg(FG_MUTED).italic(),
    )));

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(Block::bordered().title(" 帮助 ").border_type(BorderType::Rounded)),
        popup,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{InstanceId, InstanceState, InstanceInfo, SessionInfo};
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn render_at(app: &App, w: u16, h: u16) {
        let mut terminal = Terminal::new(TestBackend::new(w, h)).unwrap();
        terminal.draw(|f| draw(f, app)).unwrap();
    }

    fn render(app: &App) {
        render_at(app, 90, 26);
    }

    fn fake_instance() -> InstanceInfo {
        InstanceInfo {
            id: InstanceId::new("general-abc").unwrap(),
            model: "deepseek/deepseek-v3".into(),
            state: InstanceState::Running,
            sessions: 2,
            max_sessions: 100,
            consumed_tokens: 1234,
            cache_entries: 0,
            created_at: "2026-08-15T00:00:00Z".into(),
        }
    }

    #[test]
    fn renders_empty_chat() {
        render(&App::new(None));
    }

    #[test]
    fn renders_chat_lines_with_wrap() {
        let mut app = App::new(None);
        app.chat_lines.push(ChatLine::user(
            "这是一段很长的中文消息，用于验证 CJK 全角字符的手动换行是否正常。".repeat(6),
        ));
        app.chat_lines.push(ChatLine::assistant(
            "assistant reply with long content that must wrap across many lines plus 中文混排。",
        ));
        app.chat_lines.push(ChatLine::system("已连接实例 general-abc"));
        render(&app);
    }

    #[test]
    fn renders_thinking_block_collapsed_and_expanded() {
        let mut app = App::new(None);
        app.chat_lines.push(ChatLine::user("问题"));
        let mut asst = ChatLine::assistant("结论文本");
        asst.reasoning = "这是思考过程内容，用于验证 thinking 块的折叠与展开渲染。".repeat(4);
        app.chat_lines.push(asst);
        render(&app); // 折叠
        app.chat_lines[1].show_thinking = true;
        render(&app); // 展开
    }

    #[test]
    fn renders_thinking_without_text_while_streaming() {
        let mut app = App::new(None);
        app.chat_lines.push(ChatLine::user("问题"));
        let mut asst = ChatLine::assistant(String::new());
        asst.reasoning = "还在思考中…".into();
        app.chat_lines.push(asst);
        app.chat_active = true;
        app.tick = 5;
        render(&app);
    }

    #[test]
    fn renders_loading_dots_before_first_delta() {
        let mut app = App::new(None);
        app.chat_lines.push(ChatLine::user("hi"));
        app.chat_lines.push(ChatLine::assistant(String::new()));
        app.chat_active = true;
        app.tick = 3;
        render(&app);
    }

    #[test]
    fn renders_narrow_terminal() {
        let mut app = App::new(None);
        app.chat_lines.push(ChatLine::user("窄屏下的换行测试"));
        app.chat_lines.push(ChatLine::assistant("x".repeat(200)));
        render_at(&app, 30, 12);
    }

    #[test]
    fn renders_scrolled_up() {
        let mut app = App::new(None);
        for i in 0..40 {
            app.chat_lines.push(ChatLine::user(format!(
                "message {i} with text to fill the screen area nicely and wrap"
            )));
            app.chat_lines.push(ChatLine::assistant(format!("reply {i}")));
        }
        app.chat_scroll = 120;
        render(&app);
    }

    #[test]
    fn renders_command_palette() {
        let mut app = App::new(None);
        app.chat_input = "/ne".into();
        render(&app);
        app.chat_input = "/list".into();
        render(&app);
    }

    #[test]
    fn renders_all_modals() {
        let mut app = App::new(None);
        app.instances = vec![fake_instance()];
        app.sessions = vec![SessionInfo {
            id: "abcd1234-xxxx".into(),
            messages: 3,
            phase: "idle".into(),
            consumed_tokens: 50,
        }];
        for modal in [Modal::Switch, Modal::Sessions, Modal::New, Modal::Help] {
            app.modal = Some(modal);
            render(&app);
        }
    }
}

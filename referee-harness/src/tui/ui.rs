//! TUI 渲染层 — 纯绘制，不修改状态。

use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState, Paragraph, Wrap},
};

use crate::protocol::InstanceState;
use crate::tui::app::{App, CreateForm, View};

/// 布局垂直切分
const HEADER: u16 = 1;
const STATUSBAR: u16 = 1;

/// 主渲染入口
pub fn draw(frame: &mut Frame, app: &App) {
    let [header, body, status] = Layout::vertical([
        Constraint::Length(HEADER),
        Constraint::Min(0),
        Constraint::Length(STATUSBAR),
    ])
    .areas(frame.area());

    draw_header(frame, header, app);
    draw_body(frame, body, app);
    draw_status(frame, status, app);
}

fn draw_header(frame: &mut Frame, area: Rect, app: &App) {
    let view_name = match app.view {
        View::List => "实例列表",
        View::Detail => "实例详情",
        View::Chat => "对话",
        View::Create => "创建实例",
    };
    let title = Line::from(vec![
        Span::styled(
            " referee-harness TUI ",
            Style::new()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("• ", Style::new().fg(Color::DarkGray)),
        Span::styled(view_name, Style::new().fg(Color::Yellow)),
    ])
    .centered();
    frame.render_widget(Paragraph::new(title), area);
}

fn draw_body(frame: &mut Frame, area: Rect, app: &App) {
    match app.view {
        View::List => draw_list(frame, area, app),
        View::Detail => draw_detail(frame, area, app),
        View::Chat => draw_chat(frame, area, app),
        View::Create => draw_create(frame, area, app),
    }
}

// ── 列表视图 ──────────────────────────────────

fn draw_list(frame: &mut Frame, area: Rect, app: &App) {
    let [list_area, help_area] =
        Layout::vertical([Constraint::Min(0), Constraint::Length(3)]).areas(area);

    let items: Vec<ListItem> = app
        .instances
        .iter()
        .map(|inst| {
            let state = if inst.state == InstanceState::Running {
                Span::styled("RUNNING", Style::new().fg(Color::Green))
            } else {
                Span::styled("STOPPED", Style::new().fg(Color::Red))
            };
            ListItem::new(Line::from(vec![
                Span::styled(inst.id.as_str(), Style::new().fg(Color::Yellow).bold()),
                Span::raw("  "),
                state,
                Span::raw(format!(
                    "  {}  {}sess  {}tok",
                    inst.model, inst.sessions, inst.consumed_tokens
                )),
            ]))
        })
        .collect();

    let empty_hint = List::new(vec![ListItem::new(Line::from(Span::styled(
        " (空) 按 c 创建实例",
        Style::new().fg(Color::DarkGray),
    )))]);
    let list = if items.is_empty() { empty_hint } else { List::new(items) }
        .block(
            Block::bordered()
                .title(" 实例列表 ")
                .border_type(BorderType::Rounded),
        )
        .highlight_style(
            Style::new()
                .bg(Color::Rgb(40, 40, 60))
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");
    let mut state = ListState::default();
    state.select(Some(app.selected));
    frame.render_stateful_widget(list, list_area, &mut state);

    draw_list_help(frame, help_area);
}

fn draw_list_help(frame: &mut Frame, area: Rect) {
    let help = vec![
        Line::from(vec![
            Span::styled(" c ", Style::new().fg(Color::Black).bg(Color::Cyan)),
            Span::raw(" 创建  "),
            Span::styled(" Enter ", Style::new().fg(Color::Black).bg(Color::Cyan)),
            Span::raw(" 详情  "),
            Span::styled(" ↑/↓ ", Style::new().fg(Color::Black).bg(Color::Cyan)),
            Span::raw(" 导航  "),
            Span::styled(" d ", Style::new().fg(Color::Black).bg(Color::Red)),
            Span::raw(" 删除  "),
            Span::styled(" q ", Style::new().fg(Color::Black).bg(Color::Red)),
            Span::raw(" 退出"),
        ]),
        Line::from(vec![
            Span::styled(" r ", Style::new().fg(Color::Black).bg(Color::Cyan)),
            Span::raw(" 刷新列表"),
        ]),
    ];
    frame.render_widget(Paragraph::new(Text::from(help)), area);
}

// ── 详情视图 ──────────────────────────────────

fn draw_detail(frame: &mut Frame, area: Rect, app: &App) {
    let [info, sessions, help_area] = Layout::vertical([
        Constraint::Length(8),
        Constraint::Min(0),
        Constraint::Length(2),
    ])
    .areas(area);

    let inst = &app.instances[app.selected];
    let info_text = Text::from(vec![
        Line::from(vec![
            Span::styled("ID: ", Style::new().fg(Color::Cyan)),
            Span::raw(inst.id.as_str()),
        ]),
        Line::from(vec![
            Span::styled("Model: ", Style::new().fg(Color::Cyan)),
            Span::raw(&inst.model),
        ]),
        Line::from(vec![
            Span::styled("State: ", Style::new().fg(Color::Cyan)),
            if inst.state == InstanceState::Running {
                Span::styled("Running", Style::new().fg(Color::Green))
            } else {
                Span::styled("Stopped", Style::new().fg(Color::Red))
            },
        ]),
        Line::from(vec![
            Span::styled("Sessions: ", Style::new().fg(Color::Cyan)),
            Span::raw(format!("{} / {}", inst.sessions, inst.max_sessions)),
        ]),
        Line::from(vec![
            Span::styled("Tokens: ", Style::new().fg(Color::Cyan)),
            Span::raw(inst.consumed_tokens.to_string()),
        ]),
        Line::from(vec![
            Span::styled("Created: ", Style::new().fg(Color::Cyan)),
            Span::raw(&inst.created_at),
        ]),
    ]);
    frame.render_widget(
        Paragraph::new(info_text).block(
            Block::bordered()
                .title(" 实例信息 ")
                .border_type(BorderType::Rounded),
        ),
        info,
    );

    let sess_items: Vec<ListItem> = app
        .sessions
        .iter()
        .enumerate()
        .map(|(i, s)| {
            ListItem::new(Line::from(vec![
                Span::raw(format!("{:>3}. ", i + 1)),
                Span::styled(&s.id, Style::new().fg(Color::Yellow)),
                Span::raw(format!("  {}msgs  {}tok  {}", s.messages, s.consumed_tokens, s.phase)),
            ]))
        })
        .collect();
    let sess_list = if sess_items.is_empty() {
        List::new(vec![ListItem::new("(无会话)")])
    } else {
        List::new(sess_items)
    }
    .block(
        Block::bordered()
            .title(" 会话列表 ")
            .border_type(BorderType::Rounded),
    )
    .highlight_style(Style::new().bg(Color::Rgb(40, 40, 60)))
    .highlight_symbol("> ");
    let mut state = ListState::default();
    state.select(Some(app.session_selected));
    frame.render_stateful_widget(sess_list, sessions, &mut state);

    let help = vec![Line::from(vec![
        Span::styled(" Enter ", Style::new().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" 对话  "),
        Span::styled(" Esc ", Style::new().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" 返回  "),
        Span::styled(" r ", Style::new().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" 刷新会话"),
    ])];
    frame.render_widget(Paragraph::new(Text::from(help)), help_area);
}

// ── 聊天视图 ──────────────────────────────────

fn draw_chat(frame: &mut Frame, area: Rect, app: &App) {
    let [chat_area, input_area] =
        Layout::vertical([Constraint::Min(0), Constraint::Length(3)]).areas(area);

    // 组装消息文本
    let lines: Vec<Line> = app
        .chat_lines
        .iter()
        .map(|m| match m.role {
            crate::tui::app::ChatRole::User => Line::from(vec![
                Span::styled("▶ ", Style::new().fg(Color::Cyan)),
                Span::raw(&m.text),
            ])
            .style(Style::new().fg(Color::White)),
            crate::tui::app::ChatRole::Assistant => Line::from(vec![
                Span::styled("◀ ", Style::new().fg(Color::Green)),
                Span::raw(&m.text),
            ])
            .style(Style::new().fg(Color::LightCyan)),
            crate::tui::app::ChatRole::System => Line::from(vec![
                Span::styled("◆ ", Style::new().fg(Color::Yellow)),
                Span::raw(&m.text),
            ])
            .style(Style::new().fg(Color::DarkGray).italic()),
        })
        .collect();

    // 按「距底部偏移」切片：chat_scroll=0 跟随最新，>0 向前翻
    let view_h = (chat_area.height as usize).saturating_sub(2);
    let total = lines.len();
    let end = total;
    let start = total.saturating_sub(view_h + app.chat_scroll).min(end);
    let visible: Vec<&Line> = lines.iter().skip(start).take(view_h).collect();

    let mut text = Text::default();
    for line in visible {
        text.push_line(line.clone());
    }
    frame.render_widget(
        Paragraph::new(text)
            .block(
                Block::bordered()
                    .title(format!(" 对话 — {} ", app.instance_id))
                    .border_type(BorderType::Rounded),
            )
            .wrap(Wrap { trim: false }),
        chat_area,
    );

    // 输入框
    let input_para = Paragraph::new(app.chat_input.as_str())
        .block(
            Block::bordered()
                .title(" 输入 (Enter 发送, Esc 返回) ")
                .border_type(BorderType::Rounded),
        )
        .style(Style::new().fg(Color::White));
    frame.render_widget(input_para, input_area);

    // 光标跟随输入（按字符数估算宽度）
    let col = app.chat_input.chars().count() as u16;
    let x = input_area
        .x
        .saturating_add(1)
        .saturating_add(col.min(input_area.width.saturating_sub(2)));
    frame.set_cursor_position(ratatui::layout::Position::new(x, input_area.y + 1));
}

// ── 创建表单 ──────────────────────────────────

fn draw_create(frame: &mut Frame, area: Rect, app: &App) {
    let [form_area, help_area] =
        Layout::vertical([Constraint::Min(0), Constraint::Length(2)]).areas(area);

    let labels = CreateForm::labels();
    let mut lines = Vec::new();
    for (i, label) in labels.iter().enumerate() {
        let val = app.form.field(i);
        let focused = i == app.form_field;
        let prefix = if focused { "> " } else { "  " };
        let label_span = if focused {
            Span::styled(
                format!("{prefix}{label}: "),
                Style::new().fg(Color::Yellow).bold(),
            )
        } else {
            Span::styled(format!("{prefix}{label}: "), Style::new().fg(Color::Cyan))
        };
        let value = if val.is_empty() {
            Span::styled("(空)", Style::new().fg(Color::DarkGray).italic())
        } else {
            Span::raw(val.to_string())
        };
        lines.push(Line::from(vec![label_span, value]));
        lines.push(Line::default());
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .block(
                Block::bordered()
                    .title(" 创建实例 ")
                    .border_type(BorderType::Rounded),
            )
            .wrap(Wrap { trim: false }),
        form_area,
    );

    let help = vec![Line::from(vec![
        Span::styled(" Tab ", Style::new().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" 下个字段  "),
        Span::styled(" Shift+Tab ", Style::new().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" 上个字段  "),
        Span::styled(" Enter ", Style::new().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" 提交  "),
        Span::styled(" Esc ", Style::new().fg(Color::Black).bg(Color::Cyan)),
        Span::raw(" 取消"),
    ])];
    frame.render_widget(Paragraph::new(Text::from(help)), help_area);
}

// ── 状态栏 ────────────────────────────────────

fn draw_status(frame: &mut Frame, area: Rect, app: &App) {
    let text = if app.status.is_empty() {
        "就绪".into()
    } else {
        app.status.clone()
    };
    let fg = if app.status_error {
        Color::Red
    } else {
        Color::DarkGray
    };
    frame.render_widget(
        Paragraph::new(Line::from(Span::raw(text)))
            .style(Style::new().fg(fg))
            .block(
                Block::default()
                    .borders(Borders::TOP)
                    .border_style(Style::new().fg(Color::Rgb(40, 40, 50))),
            ),
        area,
    );
}

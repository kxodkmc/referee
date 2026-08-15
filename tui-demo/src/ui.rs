//! 渲染层：把应用状态绘制成画面。只读状态，不做任何修改。

use ratatui::{
    Frame,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Paragraph, Tabs, Wrap},
};

use crate::{app::App, lessons::LESSONS};

/// 布局约束：标题栏 + 页签 + 主体 + 底部提示。
const HEADER: u16 = 1; // 标题栏高度
const TABS: u16 = 1; // 页签高度
const FOOTER: u16 = 1; // 底部提示高度

/// 绘制一帧。
pub fn draw(frame: &mut Frame, app: &App) {
    let [title_area, tabs_area, body_area, footer_area] = Layout::vertical([
        Constraint::Length(HEADER),
        Constraint::Length(TABS),
        Constraint::Min(0),
        Constraint::Length(FOOTER),
    ])
    .areas(frame.area());

    draw_title(frame, title_area);
    draw_tabs(frame, tabs_area, app);
    draw_body(frame, body_area, app);
    draw_footer(frame, footer_area);
}

/// 顶部标题栏。
fn draw_title(frame: &mut Frame, area: ratatui::layout::Rect) {
    let title = Line::from(vec![
        Span::styled(
            " 🍜 Ratatui 教学 ",
            Style::new()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "— 在终端里学习 TUI 开发",
            Style::new().fg(Color::DarkGray),
        ),
    ])
    .centered();
    frame.render_widget(Paragraph::new(title), area);
}

/// 课程页签。
fn draw_tabs(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let tabs = Tabs::new(LESSONS.iter().map(|l| l.title))
        .select(app.selected)
        .divider("│")
        .highlight_style(
            Style::new()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::new().fg(Color::Gray));
    frame.render_widget(tabs, area);
}

/// 主体：上方要点 + 下方代码示例，共用滚动偏移。
fn draw_body(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let [points_area, code_area] = Layout::vertical([
        Constraint::Percentage(55),
        Constraint::Percentage(45),
    ])
    .areas(area);

    draw_points(frame, points_area, app);
    draw_code(frame, code_area, app);
}

/// 要点区：课程标题 + 要点列表。
fn draw_points(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let lesson = app.lesson();

    let mut text = Text::default();
    text.push_line(Line::styled(
        lesson.heading,
        Style::new()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));
    text.push_line(Line::default());
    for point in lesson.points {
        text.push_line(Line::from(vec![
            Span::styled("▪ ", Style::new().fg(Color::Yellow)),
            Span::raw(*point),
        ]));
    }

    let paragraph = Paragraph::new(text)
        .block(
            Block::bordered()
                .title(" 要点 ")
                .border_type(BorderType::Rounded),
        )
        .wrap(Wrap { trim: false })
        .scroll((app.scroll, 0));
    frame.render_widget(paragraph, area);
}

/// 代码示例区：深色底、保持缩进、随要点同步滚动。
fn draw_code(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let paragraph = Paragraph::new(app.lesson().code)
        .block(
            Block::bordered()
                .title(" 代码示例 ")
                .border_type(BorderType::Rounded),
        )
        .style(
            Style::new()
                .fg(Color::White)
                .bg(Color::Rgb(25, 25, 35)),
        )
        .scroll((app.scroll, 0));
    frame.render_widget(paragraph, area);
}

/// 底部操作提示。
fn draw_footer(frame: &mut Frame, area: ratatui::layout::Rect) {
    let help = Line::from(vec![
        Span::styled("←/→", Style::new().fg(Color::Yellow)),
        Span::raw(" 切换课程  "),
        Span::styled("↑/↓", Style::new().fg(Color::Yellow)),
        Span::raw(" 滚动  "),
        Span::styled("q", Style::new().fg(Color::Yellow)),
        Span::raw(" 退出"),
    ])
    .centered();
    frame.render_widget(
        Paragraph::new(help).style(Style::new().fg(Color::DarkGray)),
        area,
    );
}

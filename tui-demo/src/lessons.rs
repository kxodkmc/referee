//! 教学内容数据：每节课包含标题、要点与代码示例。
//! 纯静态数据，与渲染/交互完全解耦。

/// 一节课的静态内容。
pub struct Lesson {
    /// 页签标题。
    pub title: &'static str,
    /// 标题行（内容区顶部的大标题）。
    pub heading: &'static str,
    /// 要点列表，逐条展示。
    pub points: &'static [&'static str],
    /// 代码示例（按行渲染，保持缩进）。
    pub code: &'static str,
}

/// 全部课程，顺序即页签顺序。
pub const LESSONS: &[Lesson] = &[
    Lesson {
        title: "简介",
        heading: "Ratatui 是什么？",
        points: &[
            "Rust 编写的终端用户界面（TUI）库，专注快速、轻量、富交互。",
            "即时模式渲染：每帧全量重建 UI，无状态持久化，零运行时开销。",
            "自带布局、组件、样式与事件系统，开箱即用。",
            "默认使用 crossterm 后端，也可切换 termion / termwiz。",
            "纯 Rust 实现，无 C 依赖，内存安全、线程安全、类型安全。",
        ],
        code: r#"// 在 Cargo.toml 中引入
[dependencies]
ratatui   = "0.30"   # 默认启用 crossterm 后端
crossterm = "0.29"   # 跨平台终端控制"#,
    },
    Lesson {
        title: "快速开始",
        heading: "第一个 TUI：Hello World",
        points: &[
            "ratatui::init() 进入备用屏幕并开启 raw mode；restore() 负责还原。",
            "主循环：先 terminal.draw() 渲染一帧，再读取键盘事件。",
            "按 q 退出；用 KeyEventKind::Press 过滤，避免 Windows 重复触发。",
            "draw 接收闭包，闭包里拿到 Frame，用 render_widget 把组件画上去。",
        ],
        code: r#"use std::io;
use ratatui::{
    crossterm::event::{self, KeyCode, KeyEventKind},
    style::Stylize,
    widgets::Paragraph,
    DefaultTerminal,
};

fn main() -> io::Result<()> {
    let mut terminal = ratatui::init();
    terminal.clear()?;
    let result = run(terminal);
    ratatui::restore();
    result
}

fn run(mut terminal: DefaultTerminal) -> io::Result<()> {
    loop {
        terminal.draw(|frame| {
            let msg = Paragraph::new("Hello Ratatui! (press 'q' to quit)")
                .white().on_blue();
            frame.render_widget(msg, frame.area());
        })?;
        if let event::Event::Key(key) = event::read()? {
            if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                return Ok(());
            }
        }
    }
}"#,
    },
    Lesson {
        title: "布局",
        heading: "响应式布局 Layout",
        points: &[
            "Constraint 决定区域尺寸：Length / Percentage / Min / Max / Fill。",
            "Layout::vertical / horizontal 负责切分，支持嵌套与 flex 分配。",
            "约束是相对的：终端尺寸变化时布局自动重算，天然响应式。",
            "areas() 解构一次拿到多个区域，代码更简洁。",
        ],
        code: r#"use ratatui::layout::{Constraint, Layout};

let [header, body, footer] = Layout::vertical([
    Constraint::Length(3),   // 顶部固定 3 行
    Constraint::Min(0),      // 中间占满剩余空间
    Constraint::Length(1),   // 底部固定 1 行
])
.areas(frame.area());

// 再对 body 做水平切分
let [left, right] = Layout::horizontal([
    Constraint::Percentage(30),
    Constraint::Fill(1),
])
.areas(body);"#,
    },
    Lesson {
        title: "组件",
        heading: "常用 Widgets",
        points: &[
            "Paragraph：文本展示，支持自动换行与滚动。",
            "List：可选中列表，高亮当前项，是菜单/导航的基石。",
            "Table、Tabs：表格与页签，组织信息与导航。",
            "Gauge、Chart、Sparkline：进度条与数据可视化。",
            "Block 提供边框与标题，是几乎所有组件的容器。",
        ],
        code: r#"use ratatui::{
    style::{Color, Style},
    widgets::{Block, Borders, List, ListItem},
};

let block = Block::bordered().title("菜单");

let items: Vec<ListItem> = vec!["新建", "打开", "保存"]
    .into_iter().map(ListItem::new).collect();

let list = List::new(items)
    .block(block)
    .highlight_style(Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD));

frame.render_widget(list, area);"#,
    },
    Lesson {
        title: "样式",
        heading: "样式 Style 与 Stylize",
        points: &[
            "Stylize 扩展 trait 提供 .red() .on_blue() .bold() 等链式简写。",
            "Style 组合前景/背景色与修饰符（加粗、斜体、下划线、反显）。",
            "颜色支持 16 色、256 色以及 RGB 真彩色。",
            "样式可以作用在文本、行、段落甚至整个组件上。",
        ],
        code: r#"use ratatui::style::{Color, Modifier, Style, Stylize};

// 链式简写
let text = "重要提示".red().bold().on_yellow();

// 显式 Style 构造
let style = Style::new()
    .fg(Color::Rgb(255, 165, 0))
    .bg(Color::Black)
    .add_modifier(Modifier::UNDERLINED);

Paragraph::new(text).style(style);"#,
    },
    Lesson {
        title: "事件",
        heading: "键盘事件与交互",
        points: &[
            "event::read() 阻塞读取下一个事件；event::poll() 支持超时轮询。",
            "事件包含键盘、鼠标、焦点、终端尺寸变化等。",
            "务必检查 KeyEventKind::Press，否则 Windows 上每个按键触发两次。",
            "在事件循环中修改应用状态，渲染时按状态画出界面。",
        ],
        code: r#"use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind};

loop {
    terminal.draw(|frame| ui::draw(frame, &app))?;

    if let Event::Key(key) = event::read()? {
        match (key.kind, key.code) {
            (KeyEventKind::Press, KeyCode::Char('q')) => return Ok(()),
            (KeyEventKind::Press, KeyCode::Left)  => app.previous_lesson(),
            (KeyEventKind::Press, KeyCode::Right) => app.next_lesson(),
            (KeyEventKind::Press, KeyCode::Down)  => app.scroll_down(),
            (KeyEventKind::Press, KeyCode::Up)    => app.scroll_up(),
            _ => {}
        }
    }
}"#,
    },
];

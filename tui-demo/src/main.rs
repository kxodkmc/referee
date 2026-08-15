//! Ratatui 教学页面入口。
//!
//! 在终端里运行 `cargo run -p tui-demo`，按 ←/→ 切换课程、↑/↓ 滚动内容、q 退出。

mod app;
mod lessons;
mod ui;

use std::io;

use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    DefaultTerminal,
};

use crate::app::App;

fn main() -> io::Result<()> {
    let mut terminal = ratatui::init();
    terminal.clear()?;
    let result = run(terminal);
    ratatui::restore();
    result
}

fn run(mut terminal: DefaultTerminal) -> io::Result<()> {
    let mut app = App::new();

    loop {
        // 每帧先画，再处理输入。
        terminal.draw(|frame| ui::draw(frame, &app))?;

        if let Event::Key(key) = event::read()? {
            // 只处理“按下”，避免 Windows 上重复触发。
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                KeyCode::Left => app.previous_lesson(),
                KeyCode::Right => app.next_lesson(),
                KeyCode::Up => app.scroll_up(),
                KeyCode::Down => app.scroll_down(),
                _ => {}
            }
        }
    }
}

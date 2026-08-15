//! 应用状态：当前课程索引与内容区滚动偏移。
//! 只持有状态，不碰渲染与输入。

use crate::lessons::LESSONS;

/// 整个应用的可变状态。
pub struct App {
    /// 当前选中课程（LESSONS 的索引）。
    pub selected: usize,
    /// 内容区（要点 + 代码）的滚动偏移。
    pub scroll: u16,
}

impl App {
    pub fn new() -> Self {
        Self {
            selected: 0,
            scroll: 0,
        }
    }

    /// 切换到下一课，循环到头；同时重置滚动。
    pub fn next_lesson(&mut self) {
        self.selected = (self.selected + 1) % LESSONS.len();
        self.scroll = 0;
    }

    /// 切换到上一课，循环到末尾；同时重置滚动。
    pub fn previous_lesson(&mut self) {
        self.selected = if self.selected == 0 {
            LESSONS.len() - 1
        } else {
            self.selected - 1
        };
        self.scroll = 0;
    }

    /// 内容区向下滚动一行。
    pub fn scroll_down(&mut self) {
        self.scroll = self.scroll.saturating_add(1);
    }

    /// 内容区向上滚动一行。
    pub fn scroll_up(&mut self) {
        self.scroll = self.scroll.saturating_sub(1);
    }

    /// 当前课程。
    pub fn lesson(&self) -> &'static crate::lessons::Lesson {
        &LESSONS[self.selected]
    }
}

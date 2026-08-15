//! TUI 应用状态机 — 纯状态，不碰 IO 与渲染。

use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::protocol::{InstanceInfo, SessionInfo};
use crate::tui::client::ChatEvent;

/// 模态弹层（None = 主对话视图）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Modal {
    /// 实例切换列表（/list）
    Switch,
    /// 会话切换列表（/sessions）
    Sessions,
    /// 创建实例表单（/new）
    New,
    /// 帮助（/help）
    Help,
}

/// 对话角色
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatRole {
    User,
    Assistant,
    System,
}

/// 一条对话消息
#[derive(Debug, Clone)]
pub struct ChatLine {
    pub role: ChatRole,
    pub text: String,
    /// 思考内容（reasoning_content 累积；空 = 无 thinking 块）
    pub reasoning: String,
    /// 思考块是否展开（默认折叠）
    pub show_thinking: bool,
}

impl ChatLine {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::User,
            text: text.into(),
            reasoning: String::new(),
            show_thinking: false,
        }
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Assistant,
            text: text.into(),
            reasoning: String::new(),
            show_thinking: false,
        }
    }

    pub fn system(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::System,
            text: text.into(),
            reasoning: String::new(),
            show_thinking: false,
        }
    }
}

/// 内置命令表（`/` 前缀唤起，命令面板与分发共用）
pub const COMMANDS: &[(&str, &str)] = &[
    ("/new", "创建新实例"),
    ("/list", "切换实例"),
    ("/sessions", "切换会话"),
    ("/clear", "清空当前对话"),
    ("/help", "帮助与快捷键"),
    ("/quit", "退出 TUI"),
];

/// 新建实例表单
#[derive(Debug, Clone)]
pub struct CreateForm {
    pub id: String,
    pub description: String,
    pub model: String,
    pub template: String,
    pub provider: String,
    pub api_key: String,
    pub root: String,
}

impl Default for CreateForm {
    fn default() -> Self {
        Self {
            id: String::new(),
            description: "TUI agent".into(),
            model: "deepseek/deepseek-v3".into(),
            template: "generic".into(),
            provider: "deepseek".into(),
            api_key: String::new(),
            root: String::new(),
        }
    }
}

impl CreateForm {
    /// 字段数量
    pub const FIELDS: usize = 7;

    /// 字段标签（与 `field` / `field_mut` 索引一致）
    pub fn labels() -> [&'static str; Self::FIELDS] {
        ["id", "描述", "模型", "模板", "厂商", "API Key", "工作区根"]
    }

    pub fn field(&self, i: usize) -> &str {
        match i {
            0 => &self.id,
            1 => &self.description,
            2 => &self.model,
            3 => &self.template,
            4 => &self.provider,
            5 => &self.api_key,
            _ => &self.root,
        }
    }

    pub fn field_mut(&mut self, i: usize) -> &mut String {
        match i {
            0 => &mut self.id,
            1 => &mut self.description,
            2 => &mut self.model,
            3 => &mut self.template,
            4 => &mut self.provider,
            5 => &mut self.api_key,
            _ => &mut self.root,
        }
    }
}

/// 全局应用状态
pub struct App {
    pub should_quit: bool,
    pub status: String,
    /// 状态是否为错误（供状态栏着色）
    pub status_error: bool,
    /// 当前模态弹层（None = 主对话视图）
    pub modal: Option<Modal>,
    /// 一次对话结束后需要刷新实例指标（token 用量）
    pub needs_refresh: bool,
    /// 帧计数（header 转圈 / 加载动画用；每轮事件循环 +1）
    pub tick: u64,
    /// 当前工作目录（header 路径展示；`referee` 启动时注入）
    pub cwd: String,

    // 实例
    pub instances: Vec<InstanceInfo>,
    pub instance_selected: usize,
    /// 当前实例身份
    pub instance_id: String,
    pub instance_model: String,
    pub consumed_tokens: u64,

    // 会话
    pub sessions: Vec<SessionInfo>,
    pub session_selected: usize,
    /// 当前会话身份（空 = 新会话）
    pub session_id: String,

    // 对话
    pub chat_lines: Vec<ChatLine>,
    pub chat_input: String,
    pub input_cursor: usize,
    /// 输入历史（↑/↓ 切换）
    pub chat_history: Vec<String>,
    /// 历史浏览位置（None = 不在历史浏览中）
    pub history_index: Option<usize>,
    /// 距底部行偏移（0 = 跟随最新；越大越往前翻）
    pub chat_scroll: usize,
    pub chat_active: bool,
    pub chat_rx: Option<mpsc::Receiver<ChatEvent>>,

    // 创建
    pub form: CreateForm,
    pub form_field: usize,
}

impl App {
    /// 构造应用状态。`default_root` 为「工作区根」初始值（如启动目录），
    /// 供创建实例时默认填入；`None` 则为空。
    pub fn new(default_root: Option<String>) -> Self {
        let form = CreateForm::default();
        let cwd = default_root
            .clone()
            .or_else(|| std::env::current_dir().ok().map(|p| p.to_string_lossy().into_owned()))
            .unwrap_or_default();
        Self {
            should_quit: false,
            status: "连接 daemon…".into(),
            status_error: false,
            modal: None,
            needs_refresh: false,
            tick: 0,
            cwd,
            instances: Vec::new(),
            instance_selected: 0,
            instance_id: String::new(),
            instance_model: String::new(),
            consumed_tokens: 0,
            sessions: Vec::new(),
            session_selected: 0,
            session_id: String::new(),
            chat_lines: Vec::new(),
            chat_input: String::new(),
            input_cursor: 0,
            chat_history: Vec::new(),
            history_index: None,
            chat_scroll: 0,
            chat_active: false,
            chat_rx: None,
            form: if let Some(root) = default_root {
                CreateForm { root, ..form }
            } else {
                form
            },
            form_field: 0,
        }
    }

    // ── 实例 / 会话导航 ──

    pub fn select_next_instance(&mut self) {
        if !self.instances.is_empty() {
            self.instance_selected = (self.instance_selected + 1).min(self.instances.len() - 1);
        }
    }

    pub fn select_prev_instance(&mut self) {
        self.instance_selected = self.instance_selected.saturating_sub(1);
    }

    pub fn select_next_session(&mut self) {
        if !self.sessions.is_empty() {
            self.session_selected = (self.session_selected + 1).min(self.sessions.len() - 1);
        }
    }

    pub fn select_prev_session(&mut self) {
        self.session_selected = self.session_selected.saturating_sub(1);
    }

    // ── 对话辅助 ──

    /// 追加系统提示消息（状态提示，非对话内容）。
    pub fn push_system(&mut self, msg: impl Into<String>) {
        self.chat_lines.push(ChatLine::system(msg));
        self.chat_scroll = 0;
    }

    /// 切换最后一条助手消息的 thinking 块展开/折叠。
    pub fn toggle_thinking(&mut self) {
        if let Some(line) = self
            .chat_lines
            .iter_mut()
            .rev()
            .find(|l| l.role == ChatRole::Assistant)
        {
            line.show_thinking = !line.show_thinking;
        }
    }

    /// 恢复会话：清空显示并提示（历史消息在服务端）。
    pub fn resume_session(&mut self, idx: usize) {
        let Some(s) = self.sessions.get(idx).cloned() else {
            return;
        };
        self.session_id = s.id.clone();
        self.session_selected = idx;
        self.chat_lines.clear();
        self.chat_active = false;
        self.chat_rx = None;
        self.chat_scroll = 0;
        if s.messages > 0 {
            self.push_system(format!(
                "已恢复会话 {}（{} 条消息）",
                short_id(&s.id),
                s.messages
            ));
        }
    }

    /// 开始新会话（丢弃当前显示，下次消息走新 session_id）。
    pub fn start_new_session(&mut self) {
        self.session_id = String::new();
        self.chat_lines.clear();
        self.chat_active = false;
        self.chat_rx = None;
        self.chat_scroll = 0;
        self.push_system("已开始新会话");
    }

    // ── 输入历史 ──

    pub fn history_save(&mut self, text: &str) {
        if !self.chat_history.iter().any(|h| h == text) {
            self.chat_history.push(text.to_string());
        }
        self.history_index = None;
    }

    pub fn history_prev(&mut self) {
        if self.chat_history.is_empty() {
            return;
        }
        let idx = match self.history_index {
            Some(i) if i > 0 => i - 1,
            _ => self.chat_history.len() - 1,
        };
        self.history_index = Some(idx);
        self.chat_input = self.chat_history[idx].clone();
        self.input_cursor = self.chat_input.chars().count();
    }

    pub fn history_next(&mut self) {
        let Some(i) = self.history_index else { return };
        if i + 1 < self.chat_history.len() {
            self.history_index = Some(i + 1);
            self.chat_input = self.chat_history[i + 1].clone();
        } else {
            self.history_index = None;
            self.chat_input.clear();
        }
        self.input_cursor = self.chat_input.chars().count();
    }

    /// 退出历史浏览（用户重新输入时调用）。
    pub fn history_reset(&mut self) {
        self.history_index = None;
    }

    // ── 聊天滚动（距底部行偏移）──

    pub fn chat_scroll_prev(&mut self) {
        self.chat_scroll = self.chat_scroll.saturating_add(5);
    }

    pub fn chat_scroll_next(&mut self) {
        self.chat_scroll = self.chat_scroll.saturating_sub(5);
    }

    // ── 构建创建实例的规格 JSON（全声明式，与 daemon 协议一致）──

    pub fn build_spec(&self) -> Value {
        let id = match self.form.id.trim() {
            "" => Value::Null,
            s => Value::String(s.to_string()),
        };
        let agent_id = match self.form.id.trim() {
            "" => "general".to_string(),
            s => s.to_string(),
        };
        let template = match self.form.template.trim() {
            "deepseek" => json!("deepseek"),
            "claude" => json!("claude"),
            s if s.starts_with("named:") => json!({ "named": s.trim_start_matches("named:") }),
            s if s.starts_with("inline:") => json!({ "inline": s.trim_start_matches("inline:") }),
            _ => json!("generic"),
        };
        let root = match self.form.root.trim() {
            "" => Value::Null,
            s => Value::String(s.to_string()),
        };
        let provider = match self.form.provider.trim() {
            "xiaomi" => json!({
                "type": "xiaomi", "api_key": self.form.api_key, "base_url": Value::Null,
            }),
            "openai" => json!({
                "type": "openai", "api_key": self.form.api_key,
                "base_url": Value::Null, "model": "",
            }),
            _ => json!({
                "type": "deepseek", "api_key": self.form.api_key,
                "base_url": Value::Null, "model": Value::Null,
            }),
        };
        json!({
            "id": id,
            "agent": {
                "id": agent_id,
                "description": self.form.description,
                "model": self.form.model,
                "template": template,
                "tools": [],
                "skills": [],
                "mcp_servers": [],
                "params": {}
            },
            // engine / template_vars 省略：InstanceSpec 均有 #[serde(default)]，走默认值
            "tools": {
                "fs": { "root": root, "max_file_bytes": 1048576, "default_limit_chars": 3000 },
                "artifact": false
            },
            "provider": provider,
        })
    }

    // ── 消费聊天流事件（每帧调用）──

    pub fn poll_chat(&mut self) {
        let mut events = Vec::new();
        let mut disconnected = false;
        if let Some(rx) = &mut self.chat_rx {
            loop {
                match rx.try_recv() {
                    Ok(ev) => events.push(ev),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }
        }
        for ev in events {
            self.apply_chat_event(ev);
        }
        // 连接关闭但无 finish/error 帧（如 daemon 退出）
        if disconnected && self.chat_rx.is_some() {
            self.status = "对话连接已断开".into();
            self.status_error = true;
            self.chat_active = false;
            self.chat_rx = None;
        }
    }

    fn apply_chat_event(&mut self, ev: ChatEvent) {
        match ev {
            ChatEvent::Delta { content, reasoning } => {
                let has_assistant = self
                    .chat_lines
                    .last()
                    .is_some_and(|l| l.role == ChatRole::Assistant);
                if !has_assistant {
                    self.chat_lines.push(ChatLine::assistant(String::new()));
                }
                let last = self.chat_lines.last_mut().unwrap();
                if !content.is_empty() {
                    last.text.push_str(&content);
                }
                if !reasoning.is_empty() {
                    last.reasoning.push_str(&reasoning);
                }
                self.chat_scroll = 0; // 跟随最新
            }
            ChatEvent::Finish { reason } => {
                self.chat_active = false;
                self.chat_rx = None;
                self.status = format!("完成 ({reason})");
                self.status_error = false;
                self.needs_refresh = true;
            }
            ChatEvent::Error { message } => {
                self.chat_active = false;
                self.chat_rx = None;
                self.status = format!("对话错误: {message}");
                self.status_error = true;
            }
        }
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new(None)
    }
}

/// 截断为短标识（8 字符前缀）供状态行展示
pub(crate) fn short_id(s: &str) -> String {
    if s.chars().count() <= 8 {
        s.to_string()
    } else {
        s.chars().take(8).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{InstanceSpec, ProviderConfig};

    /// 客户端构造的规格 JSON 必须能被 daemon 侧 `InstanceSpec` 反序列化（协议对齐）。
    #[test]
    fn build_spec_roundtrips_into_instance_spec() {
        let mut app = App::new(None);
        app.form.id = "my-agent".into();
        app.form.model = "deepseek/deepseek-v3".into();
        app.form.api_key = "sk-test".into();
        app.form.root = "/workspace".into();

        let v = app.build_spec();
        let spec: InstanceSpec = serde_json::from_value(v).unwrap();
        assert_eq!(spec.id.as_deref(), Some("my-agent"));
        assert_eq!(spec.agent.model, "deepseek/deepseek-v3");
        assert!(matches!(spec.provider, ProviderConfig::DeepSeek { .. }));
        // fs 根目录传入
        let root = spec.tools.fs.as_ref().and_then(|f| f.root.clone()).unwrap();
        assert_eq!(root, "/workspace");
    }

    /// 模板映射：named:xxx / inline:xxx → 对应 serde 变体。
    #[test]
    fn build_spec_template_mapping() {
        let mut app = App::new(None);
        app.form.template = "named:general".into();
        let v = app.build_spec();
        assert_eq!(v["agent"]["template"]["named"], "general");

        app.form.template = "inline:hello {{cwd}}".into();
        let v = app.build_spec();
        assert_eq!(v["agent"]["template"]["inline"], "hello {{cwd}}");

        app.form.template = "generic".into();
        let v = app.build_spec();
        assert_eq!(v["agent"]["template"], "generic");
    }

    #[test]
    fn history_cycles_and_resets() {
        let mut app = App::new(None);
        assert!(app.chat_history.is_empty());
        app.history_save("hi");
        app.history_save("hey");
        // 重复不入历史
        app.history_save("hi");
        assert_eq!(app.chat_history.len(), 2);

        app.history_prev(); // hey
        assert_eq!(app.chat_input, "hey");
        app.history_prev(); // hi
        assert_eq!(app.chat_input, "hi");
        app.history_next(); // 回到 hey
        assert_eq!(app.chat_input, "hey");
        app.history_next(); // 离开历史
        assert!(app.history_index.is_none());
    }

    #[test]
    fn start_new_session_clears_chat() {
        let mut app = App::new(None);
        app.session_id = "abc".into();
        app.chat_lines.push(ChatLine::user("x"));
        app.start_new_session();
        assert!(app.session_id.is_empty());
        assert!(app.chat_lines.iter().any(|l| l.role == ChatRole::System));
    }

    #[test]
    fn toggle_thinking_flips_last_assistant() {
        let mut app = App::new(None);
        app.chat_lines.push(ChatLine::user("q"));
        app.chat_lines.push(ChatLine::assistant("a"));
        assert!(!app.chat_lines[1].show_thinking);
        app.toggle_thinking();
        assert!(app.chat_lines[1].show_thinking);
        app.toggle_thinking();
        assert!(!app.chat_lines[1].show_thinking);
    }
}

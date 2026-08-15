//! TUI 应用状态机 — 纯状态，不碰 IO 与渲染。

use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use crate::protocol::{InstanceInfo, SessionInfo};
use crate::tui::client::ChatEvent;

/// 视图
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    List,
    Detail,
    Chat,
    Create,
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
}

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
    pub view: View,
    pub should_quit: bool,
    pub status: String,
    /// 状态是否为错误（供状态栏着色）
    pub status_error: bool,
    // 列表
    pub instances: Vec<InstanceInfo>,
    pub selected: usize,
    // 详情
    pub sessions: Vec<SessionInfo>,
    pub session_selected: usize,
    // 聊天
    pub instance_id: String,
    pub session_id: String,
    pub chat_lines: Vec<ChatLine>,
    pub chat_input: String,
    pub input_cursor: usize,
    /// 距底部偏移（0=跟随最新；越大越往前翻）
    pub chat_scroll: usize,
    pub chat_active: bool,
    pub chat_rx: Option<mpsc::Receiver<ChatEvent>>,
    // 创建
    pub form: CreateForm,
    pub form_field: usize,
}

impl App {
    pub fn new() -> Self {
        Self {
            view: View::List,
            should_quit: false,
            status: "连接 daemon…".into(),
            status_error: false,
            instances: Vec::new(),
            selected: 0,
            sessions: Vec::new(),
            session_selected: 0,
            instance_id: String::new(),
            session_id: String::new(),
            chat_lines: Vec::new(),
            chat_input: String::new(),
            input_cursor: 0,
            chat_scroll: 0,
            chat_active: false,
            chat_rx: None,
            form: CreateForm::default(),
            form_field: 0,
        }
    }

    pub fn select_next(&mut self) {
        if !self.instances.is_empty() {
            self.selected = (self.selected + 1).min(self.instances.len() - 1);
        }
    }

    pub fn select_prev(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    // ── 视图切换 ──

    /// 进入创建表单（重置字段）。
    pub fn open_create(&mut self) {
        self.form = CreateForm::default();
        self.form_field = 0;
        self.view = View::Create;
    }

    /// 进入聊天视图：优先复用选中的会话，否则新建会话。
    pub fn open_chat(&mut self) {
        self.session_id = self
            .sessions
            .get(self.session_selected)
            .map(|s| s.id.clone())
            .unwrap_or_default();
        self.chat_lines.clear();
        self.chat_input.clear();
        self.input_cursor = 0;
        self.chat_scroll = 0;
        self.chat_active = false;
        self.chat_rx = None;
        self.view = View::Chat;
    }

    // ── 聊天滚动（距底部偏移）──

    pub fn chat_scroll_prev(&mut self) {
        self.chat_scroll = self.chat_scroll.saturating_add(1);
    }

    pub fn chat_scroll_next(&mut self) {
        self.chat_scroll = self.chat_scroll.saturating_sub(1);
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
            self.chat_active = false;
            self.chat_rx = None;
        }
    }

    fn apply_chat_event(&mut self, ev: ChatEvent) {
        match ev {
            ChatEvent::Delta { content } => {
                let has_assistant = self
                    .chat_lines
                    .last()
                    .is_some_and(|l| l.role == ChatRole::Assistant);
                if has_assistant {
                    self.chat_lines.last_mut().unwrap().text.push_str(&content);
                } else {
                    self.chat_lines.push(ChatLine {
                        role: ChatRole::Assistant,
                        text: content,
                    });
                }
                self.chat_scroll = 0; // 跟随最新
            }
            ChatEvent::Finish { reason } => {
                self.chat_active = false;
                self.chat_rx = None;
                self.status = format!("完成 ({reason})");
                self.status_error = false;
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
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{InstanceSpec, ProviderConfig};

    /// 客户端构造的规格 JSON 必须能被 daemon 侧 `InstanceSpec` 反序列化（协议对齐）。
    #[test]
    fn build_spec_roundtrips_into_instance_spec() {
        let mut app = App::new();
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
        let mut app = App::new();
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
}

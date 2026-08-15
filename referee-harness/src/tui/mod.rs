//! 官方 TUI 客户端（feature `tui`）
//!
//! 连接常驻 daemon（TCP JSON-RPC 2.0），提供**聊天优先**界面（OpenCode/ClaudeCode
//! 风格）：单一对话视图 + 底部输入框 + `/` 命令 + 弹层管理实例/会话。
//! 职责边界：只做「展示 + 用户输入 → JSON-RPC 请求」，业务判定全部在 daemon。
//! 与 Web / CLI 共用同一 daemon（见 [`crate`] 分层设计）。

pub mod app;
pub mod client;
pub mod ui;

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    DefaultTerminal,
};
use serde_json::{json, Value};
use tokio::time::timeout;

use crate::protocol::InstanceInfo;
use crate::tui::app::{App, ChatLine, CreateForm, Modal};
use crate::tui::client::{open_chat_stream, RpcClient};

/// 管理类 RPC 超时
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// 事件轮询间隔（兼顾聊天流的刷新频率与 CPU 占用）
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// TUI 入口：连接 daemon 并进入界面。
///
/// `default_root` 为「工作区根」初始值（如启动目录），创建实例时默认填入。
pub async fn run(daemon: SocketAddr, default_root: Option<String>) -> io::Result<()> {
    let mut mgmt = RpcClient::connect(daemon)
        .await
        .map_err(|e| io::Error::other(format!("连接 daemon {daemon} 失败: {e}")))?;

    let mut app = App::new(default_root);
    startup(&mut mgmt, &mut app).await;

    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut mgmt, &mut app, daemon).await;
    ratatui::restore();
    result
}

/// 启动：自动选中实例（无实例则提示），保持 Home 空态（不推系统消息以显示居中界面）。
async fn startup(mgmt: &mut RpcClient, app: &mut App) {
    refresh_list(mgmt, app).await;
    if let Some(info) = app.instances.first().cloned() {
        app.instance_id = info.id.as_str().to_string();
        app.instance_model = info.model.clone();
        app.consumed_tokens = info.consumed_tokens;
        set_status(app, format!("✦ {}", app.instance_id), false);
    } else {
        set_status(app, "暂无实例 — 输入 /new 创建实例".into(), false);
    }
}

/// 主循环：处理聊天流事件 → 绘制 → 轮询键盘。
async fn event_loop(
    terminal: &mut DefaultTerminal,
    mgmt: &mut RpcClient,
    app: &mut App,
    daemon: SocketAddr,
) -> io::Result<()> {
    loop {
        app.tick = app.tick.wrapping_add(1);
        app.poll_chat();
        // 对话结束：刷新实例指标（token 用量）
        if app.needs_refresh {
            app.needs_refresh = false;
            refresh_instance(mgmt, app).await;
        }
        terminal.draw(|frame| ui::draw(frame, app))?;

        if event::poll(POLL_INTERVAL)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    handle_key(app, mgmt, daemon, key).await;
                }
            }
        }

        if app.should_quit {
            break;
        }
    }
    Ok(())
}

/// 设置状态（错误标记）
fn set_status(app: &mut App, msg: String, error: bool) {
    app.status = msg;
    app.status_error = error;
}

/// 带超时的管理类 RPC 调用
async fn rpc_call(mgmt: &mut RpcClient, method: &str, params: Value) -> Result<Value, String> {
    timeout(RPC_TIMEOUT, mgmt.call(method, params))
        .await
        .map_err(|_| "请求超时".to_string())?
        .map_err(|e| e.to_string())
}

// ── 键盘分发 ──────────────────────────────────

async fn handle_key(app: &mut App, mgmt: &mut RpcClient, daemon: SocketAddr, key: KeyEvent) {
    if let Some(modal) = app.modal {
        handle_modal_key(app, mgmt, modal, key).await;
        return;
    }
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match key.code {
        KeyCode::Char('c') | KeyCode::Char('d') if ctrl => app.should_quit = true,
        KeyCode::Esc => {
            if app.chat_active {
                interrupt(mgmt, app).await;
            } else {
                app.chat_input.clear();
                app.input_cursor = 0;
                app.history_reset();
            }
        }
        KeyCode::Enter => submit(app, mgmt, daemon).await,
        KeyCode::Char('u') if ctrl => {
            app.chat_input.clear();
            app.input_cursor = 0;
            app.history_reset();
        }
        KeyCode::Char('w') if ctrl => delete_word(&mut app.chat_input, &mut app.input_cursor),
        KeyCode::Char('t') if ctrl => app.toggle_thinking(),
        KeyCode::Up => app.history_prev(),
        KeyCode::Down => app.history_next(),
        KeyCode::PageUp => app.chat_scroll_prev(),
        KeyCode::PageDown => app.chat_scroll_next(),
        other => {
            app.history_reset();
            edit_text(&mut app.chat_input, &mut app.input_cursor, other);
        }
    }
}

async fn handle_modal_key(app: &mut App, mgmt: &mut RpcClient, modal: Modal, key: KeyEvent) {
    match modal {
        Modal::Switch => match key.code {
            KeyCode::Up => app.select_prev_instance(),
            KeyCode::Down => app.select_next_instance(),
            KeyCode::Enter => {
                if !app.instances.is_empty() {
                    app.modal = None;
                    attach_instance(mgmt, app, app.instance_selected).await;
                }
            }
            KeyCode::Esc => app.modal = None,
            _ => {}
        },
        Modal::Sessions => match key.code {
            KeyCode::Up => app.select_prev_session(),
            KeyCode::Down => app.select_next_session(),
            KeyCode::Enter => {
                if !app.sessions.is_empty() {
                    app.modal = None;
                    app.resume_session(app.session_selected);
                }
            }
            KeyCode::Esc => app.modal = None,
            _ => {}
        },
        Modal::New => match key.code {
            KeyCode::Tab => app.form_field = (app.form_field + 1) % CreateForm::FIELDS,
            KeyCode::BackTab => {
                app.form_field = (app.form_field + CreateForm::FIELDS - 1) % CreateForm::FIELDS;
            }
            KeyCode::Enter => submit_create(mgmt, app).await,
            KeyCode::Esc => app.modal = None,
            KeyCode::Char(c) => app.form.field_mut(app.form_field).push(c),
            KeyCode::Backspace => {
                app.form.field_mut(app.form_field).pop();
            }
            _ => {}
        },
        Modal::Help => {
            if matches!(key.code, KeyCode::Esc | KeyCode::Enter) {
                app.modal = None;
            }
        }
    }
}

/// 提交输入：命令 → 分发；否则 → 发送消息
async fn submit(app: &mut App, mgmt: &mut RpcClient, daemon: SocketAddr) {
    if app.chat_active {
        set_status(app, "正在生成…（Esc 中断）".into(), false);
        return;
    }
    let text = app.chat_input.trim().to_string();
    if text.is_empty() {
        return;
    }
    app.history_save(&text);
    app.chat_input.clear();
    app.input_cursor = 0;

    if text.starts_with('/') {
        run_command(app, mgmt, &text).await;
        return;
    }
    start_chat(app, daemon, &text).await;
}

/// 斜杠命令分发
async fn run_command(app: &mut App, mgmt: &mut RpcClient, cmd: &str) {
    let name = cmd.split_whitespace().next().unwrap_or(cmd);
    match name {
        "/help" => app.modal = Some(Modal::Help),
        "/new" => {
            app.form = CreateForm::default();
            app.form_field = 0;
            app.modal = Some(Modal::New);
        }
        "/list" => {
            refresh_list(mgmt, app).await;
            // 选中项对齐当前实例
            if let Some(idx) = app.instances.iter().position(|i| i.id.as_str() == app.instance_id) {
                app.instance_selected = idx;
            }
            app.modal = Some(Modal::Switch);
        }
        "/sessions" => {
            if app.instance_id.is_empty() {
                set_status(app, "无实例 — /new 创建".into(), true);
                return;
            }
            refresh_sessions(mgmt, app).await;
            app.modal = Some(Modal::Sessions);
        }
        "/clear" => {
            app.chat_lines.clear();
            app.chat_scroll = 0;
            set_status(app, "已清空对话".into(), false);
        }
        "/quit" => app.should_quit = true,
        other => set_status(app, format!("未知命令 {other} — /help 查看帮助"), true),
    }
}

/// 文本编辑（支持光标移动）
fn edit_text(buf: &mut String, cursor: &mut usize, code: KeyCode) {
    match code {
        KeyCode::Char(c) => {
            buf.insert(*cursor, c);
            *cursor += 1;
        }
        KeyCode::Backspace => {
            if *cursor > 0 {
                buf.remove(*cursor - 1);
                *cursor -= 1;
            }
        }
        KeyCode::Delete => {
            if *cursor < buf.len() {
                buf.remove(*cursor);
            }
        }
        KeyCode::Left => *cursor = cursor.saturating_sub(1),
        KeyCode::Right => {
            if *cursor < buf.len() {
                *cursor += 1;
            }
        }
        KeyCode::Home => *cursor = 0,
        KeyCode::End => *cursor = buf.len(),
        _ => {}
    }
}

/// 删除光标前一个单词
fn delete_word(buf: &mut String, cursor: &mut usize) {
    let mut chars: Vec<char> = buf.chars().collect();
    let mut c = *cursor;
    // 跳过空白
    while c > 0 && chars.get(c - 1).is_some_and(|ch| ch.is_whitespace()) {
        c -= 1;
    }
    // 删除单词
    while c > 0 && !chars.get(c - 1).is_some_and(|ch| ch.is_whitespace()) {
        c -= 1;
    }
    chars.drain(c..*cursor);
    *buf = chars.into_iter().collect();
    *cursor = c;
}

// ── RPC 操作 ─────────────────────────────────

/// 刷新实例列表（保持选中项）
async fn refresh_list(mgmt: &mut RpcClient, app: &mut App) {
    match rpc_call(mgmt, "instance.list", json!({})).await {
        Ok(v) => match serde_json::from_value::<Vec<InstanceInfo>>(v) {
            Ok(list) => {
                app.instance_selected = app.instance_selected.min(list.len().saturating_sub(1));
                app.instances = list;
            }
            Err(e) => set_status(app, format!("解析失败: {e}"), true),
        },
        Err(e) => set_status(app, format!("连接错误: {e}"), true),
    }
}

/// 刷新当前实例指标（token 用量等）
async fn refresh_instance(mgmt: &mut RpcClient, app: &mut App) {
    if app.instance_id.is_empty() {
        return;
    }
    match rpc_call(mgmt, "instance.get", json!({ "id": app.instance_id })).await {
        Ok(v) => {
            if let Ok(info) = serde_json::from_value::<InstanceInfo>(v) {
                app.consumed_tokens = info.consumed_tokens;
                app.instance_model = info.model.clone();
            }
        }
        Err(e) => set_status(app, format!("实例刷新失败: {e}"), true),
    }
}

/// 刷新当前实例的会话列表
async fn refresh_sessions(mgmt: &mut RpcClient, app: &mut App) {
    if app.instance_id.is_empty() {
        return;
    }
    match rpc_call(mgmt, "instance.sessions", json!({ "id": app.instance_id })).await {
        Ok(v) => {
            app.sessions = serde_json::from_value(v).unwrap_or_default();
            app.session_selected = app
                .session_selected
                .min(app.sessions.len().saturating_sub(1));
        }
        Err(e) => set_status(app, format!("会话列表失败: {e}"), true),
    }
}

/// 切换当前实例：拉取会话并自动恢复消息最多的会话（或提示新会话）
async fn attach_instance(mgmt: &mut RpcClient, app: &mut App, idx: usize) {
    let Some(info) = app.instances.get(idx).cloned() else {
        return;
    };
    app.instance_id = info.id.as_str().to_string();
    app.instance_model = info.model.clone();
    app.consumed_tokens = info.consumed_tokens;
    app.session_id = String::new();
    app.sessions.clear();
    app.session_selected = 0;
    app.chat_lines.clear();
    app.chat_active = false;
    app.chat_rx = None;
    app.chat_scroll = 0;

    refresh_sessions(mgmt, app).await;

    let best = app
        .sessions
        .iter()
        .enumerate()
        .max_by_key(|(_, s)| s.messages)
        .map(|(i, _)| i);
    if let Some(i) = best {
        app.resume_session(i);
    } else {
        app.push_system(format!("已连接实例 {} — 输入消息开始新会话", app.instance_id));
    }
    set_status(app, format!("✦ {}", app.instance_id), false);
}

/// 中断当前会话的进行中回合
async fn interrupt(mgmt: &mut RpcClient, app: &mut App) {
    if app.session_id.is_empty() {
        set_status(app, "当前无会话".into(), false);
        return;
    }
    match rpc_call(
        mgmt,
        "instance.interrupt",
        json!({ "id": app.instance_id, "session_id": app.session_id }),
    )
    .await
    {
        Ok(v) => {
            let cancelled = v.get("cancelled").and_then(Value::as_bool).unwrap_or(false);
            set_status(
                app,
                if cancelled {
                    "已中断".into()
                } else {
                    "无进行中回合".into()
                },
                false,
            );
        }
        Err(e) => set_status(app, format!("中断失败: {e}"), true),
    }
}

/// 提交创建表单
async fn submit_create(mgmt: &mut RpcClient, app: &mut App) {
    let spec = app.build_spec();
    match rpc_call(mgmt, "instance.create", spec).await {
        Ok(v) => {
            let id = v.get("id").and_then(Value::as_str).unwrap_or("?").to_string();
            set_status(app, format!("实例已创建: {id}"), false);
            app.modal = None;
            refresh_list(mgmt, app).await;
            // 自动选中并进入新实例
            if let Some(idx) = app.instances.iter().position(|i| i.id.as_str() == id) {
                attach_instance(mgmt, app, idx).await;
            }
        }
        Err(e) => set_status(app, format!("创建失败: {e}"), true),
    }
}

/// 发起流式对话（独立连接 + 后台任务推送事件）
async fn start_chat(app: &mut App, daemon: SocketAddr, message: &str) {
    if app.instance_id.is_empty() {
        set_status(app, "无实例 — /new 创建".into(), true);
        return;
    }
    if app.session_id.is_empty() {
        app.session_id = uuid::Uuid::new_v4().to_string();
    }
    let params = json!({
        "id": app.instance_id,
        "session_id": app.session_id,
        "message": message,
        "stream": true,
    });

    app.chat_lines.push(ChatLine::user(message.to_string()));
    app.chat_lines.push(ChatLine::assistant(String::new()));
    app.chat_scroll = 0;

    match open_chat_stream(daemon, params).await {
        Ok(rx) => {
            app.chat_rx = Some(rx);
            app.chat_active = true;
            set_status(app, "生成中…".into(), false);
        }
        Err(e) => {
            app.chat_lines.pop(); // 移除空助手占位
            set_status(app, format!("对话失败: {e}"), true);
        }
    }
}

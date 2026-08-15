//! 官方 TUI 客户端（feature `tui`）
//!
//! 连接常驻 daemon（TCP JSON-RPC 2.0），提供实例管理与流式对话界面。
//! 职责边界：只做「展示 + 用户输入 → JSON-RPC 请求」，业务判定全部在 daemon。
//! 与 Web / CLI 共用同一 daemon（见 [`crate`] 分层设计）。

pub mod app;
pub mod client;
pub mod ui;

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind},
    DefaultTerminal,
};
use serde_json::{json, Value};
use tokio::time::timeout;

use crate::protocol::InstanceInfo;
use crate::tui::app::{App, ChatLine, ChatRole, CreateForm, View};
use crate::tui::client::{open_chat_stream, RpcClient};

/// 管理类 RPC 超时
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// 事件轮询间隔（兼顾聊天流的刷新频率与 CPU 占用）
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// TUI 入口：连接 daemon 并进入界面。
pub async fn run(daemon: SocketAddr) -> io::Result<()> {
    let mut mgmt = RpcClient::connect(daemon)
        .await
        .map_err(|e| io::Error::other(format!("连接 daemon {daemon} 失败: {e}")))?;

    let mut app = App::new();
    refresh_list(&mut mgmt, &mut app).await;
    if app.instances.is_empty() {
        set_status(&mut app, format!("已连接 {daemon}，暂无实例（按 c 创建）"), false);
    }

    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut mgmt, &mut app, daemon).await;
    ratatui::restore();
    result
}

/// 主循环：处理聊天流事件 → 绘制 → 轮询键盘。
async fn event_loop(
    terminal: &mut DefaultTerminal,
    mgmt: &mut RpcClient,
    app: &mut App,
    daemon: SocketAddr,
) -> io::Result<()> {
    loop {
        app.poll_chat();
        terminal.draw(|frame| ui::draw(frame, app))?;

        if event::poll(POLL_INTERVAL)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    handle_key(app, mgmt, daemon, key.code).await;
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

async fn handle_key(app: &mut App, mgmt: &mut RpcClient, daemon: SocketAddr, code: KeyCode) {
    match app.view {
        View::List => handle_list_key(app, mgmt, code).await,
        View::Detail => handle_detail_key(app, mgmt, code).await,
        View::Chat => handle_chat_key(app, mgmt, daemon, code).await,
        View::Create => handle_create_key(app, mgmt, code).await,
    }
}

async fn handle_list_key(app: &mut App, mgmt: &mut RpcClient, code: KeyCode) {
    match code {
        KeyCode::Up => app.select_prev(),
        KeyCode::Down => app.select_next(),
        KeyCode::Enter => open_detail(mgmt, app).await,
        KeyCode::Char('c') => app.open_create(),
        KeyCode::Char('d') => remove_selected(mgmt, app).await,
        KeyCode::Char('r') => refresh_list(mgmt, app).await,
        KeyCode::Char('q') | KeyCode::Esc => app.should_quit = true,
        _ => {}
    }
}

async fn handle_detail_key(app: &mut App, mgmt: &mut RpcClient, code: KeyCode) {
    match code {
        KeyCode::Up => app.session_selected = app.session_selected.saturating_sub(1),
        KeyCode::Down => {
            if !app.sessions.is_empty() {
                app.session_selected = (app.session_selected + 1).min(app.sessions.len() - 1);
            }
        }
        KeyCode::Enter | KeyCode::Char('c') => {
            app.chat_scroll = 0;
            app.open_chat();
        }
        KeyCode::Char('r') => refresh_sessions(mgmt, app).await,
        KeyCode::Esc => {
            app.view = View::List;
            refresh_list(mgmt, app).await;
        }
        _ => {}
    }
}

async fn handle_chat_key(app: &mut App, mgmt: &mut RpcClient, daemon: SocketAddr, code: KeyCode) {
    match code {
        KeyCode::Esc => app.view = View::Detail,
        KeyCode::Up => app.chat_scroll_prev(),
        KeyCode::Down => app.chat_scroll_next(),
        KeyCode::PageUp => app.chat_scroll = app.chat_scroll.saturating_add(10),
        KeyCode::PageDown => app.chat_scroll = app.chat_scroll.saturating_sub(10),
        KeyCode::Enter => {
            if app.chat_active {
                set_status(app, "正在生成…".into(), false);
            } else {
                start_chat(app, daemon).await;
            }
        }
        KeyCode::Char('i') if app.chat_active => interrupt(mgmt, app).await,
        other => edit_text(&mut app.chat_input, &mut app.input_cursor, other),
    }
}

async fn handle_create_key(app: &mut App, mgmt: &mut RpcClient, code: KeyCode) {
    match code {
        KeyCode::Tab => app.form_field = (app.form_field + 1) % CreateForm::FIELDS,
        KeyCode::BackTab => {
            app.form_field = (app.form_field + CreateForm::FIELDS - 1) % CreateForm::FIELDS;
        }
        KeyCode::Enter => submit_create(mgmt, app).await,
        KeyCode::Esc => app.view = View::List,
        KeyCode::Char(c) => app.form.field_mut(app.form_field).push(c),
        KeyCode::Backspace => {
            app.form.field_mut(app.form_field).pop();
        }
        _ => {}
    }
}

/// 文本编辑（聊天输入，支持光标移动）
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

// ── RPC 操作 ─────────────────────────────────

/// 刷新实例列表（保持当前选中项）
async fn refresh_list(mgmt: &mut RpcClient, app: &mut App) {
    match rpc_call(mgmt, "instance.list", json!({})).await {
        Ok(v) => match serde_json::from_value::<Vec<InstanceInfo>>(v) {
            Ok(list) => {
                app.selected = app.selected.min(list.len().saturating_sub(1));
                app.instances = list;
                set_status(app, format!("{} 个实例", app.instances.len()), false);
            }
            Err(e) => set_status(app, format!("解析失败: {e}"), true),
        },
        Err(e) => set_status(app, format!("连接错误: {e}"), true),
    }
}

/// 刷新当前实例的会话列表
async fn refresh_sessions(mgmt: &mut RpcClient, app: &mut App) {
    match rpc_call(mgmt, "instance.sessions", json!({ "id": app.instance_id })).await {
        Ok(v) => {
            app.sessions = serde_json::from_value(v).unwrap_or_default();
            app.session_selected = app
                .session_selected
                .min(app.sessions.len().saturating_sub(1));
            set_status(app, format!("{} 个会话", app.sessions.len()), false);
        }
        Err(e) => set_status(app, format!("会话列表失败: {e}"), true),
    }
}

/// 进入实例详情（拉取会话列表）
async fn open_detail(mgmt: &mut RpcClient, app: &mut App) {
    let Some(info) = app.instances.get(app.selected).cloned() else {
        set_status(app, "无实例".into(), false);
        return;
    };
    app.instance_id = info.id.as_str().to_string();
    app.sessions.clear();
    app.session_selected = 0;
    refresh_sessions(mgmt, app).await;
    app.view = View::Detail;
}

/// 删除选中实例
async fn remove_selected(mgmt: &mut RpcClient, app: &mut App) {
    let Some(info) = app.instances.get(app.selected).cloned() else {
        return;
    };
    let id = info.id.as_str().to_string();
    match rpc_call(mgmt, "instance.remove", json!({ "id": id })).await {
        Ok(_) => {
            set_status(app, format!("已删除 {id}"), false);
            refresh_list(mgmt, app).await;
        }
        Err(e) => set_status(app, format!("删除失败: {e}"), true),
    }
}

/// 提交创建表单
async fn submit_create(mgmt: &mut RpcClient, app: &mut App) {
    let spec = app.build_spec();
    match rpc_call(mgmt, "instance.create", spec).await {
        Ok(v) => {
            let id = v.get("id").and_then(Value::as_str).unwrap_or("?");
            set_status(app, format!("实例已创建: {id}"), false);
            app.view = View::List;
            refresh_list(mgmt, app).await;
        }
        Err(e) => set_status(app, format!("创建失败: {e}"), true),
    }
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

/// 发起流式对话（独立连接 + 后台任务推送事件）
async fn start_chat(app: &mut App, daemon: SocketAddr) {
    let message = app.chat_input.trim().to_string();
    if message.is_empty() {
        set_status(app, "输入为空".into(), false);
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

    app.chat_lines.push(ChatLine {
        role: ChatRole::User,
        text: message,
    });
    app.chat_lines.push(ChatLine {
        role: ChatRole::Assistant,
        text: String::new(),
    });
    app.chat_input.clear();
    app.input_cursor = 0;
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

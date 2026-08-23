//! 适配器行为测试：对手工搭建的本地 mock iLink 服务端收发，
//! 覆盖 A3 的可自动化部分——回环过滤、入站映射、游标/令牌落盘时机、
//! 出站令牌使用、令牌过期容错。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use referee_channel::adapter::{ChannelAdapter, ChannelIo};
use referee_channel::message::{ChannelContent, OutboundCommand};
use referee_channel_wechat::{Credentials, WechatAdapter, WechatConfig, WechatState};

// ───────────────────────────────────────────────
// 最小 iLink mock 服务端：一连接一请求（Connection: close）
// ───────────────────────────────────────────────

struct MockIlink {
    base_url: String,
    /// 首次 getupdates 下发的入站消息脚本，之后返回空体（无新消息）
    scripted: Mutex<Vec<Value>>,
    /// sendmessage 请求体记录
    sent_bodies: Mutex<Vec<Value>>,
    /// sendmessage 应答脚本（弹尽后返回 "{}"）
    send_acks: Mutex<Vec<String>>,
}

impl MockIlink {
    async fn start() -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = Arc::new(Self {
            base_url,
            scripted: Mutex::new(Vec::new()),
            sent_bodies: Mutex::new(Vec::new()),
            send_acks: Mutex::new(Vec::new()),
        });
        let task = server.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let server = task.clone();
                tokio::spawn(async move {
                    let _ = server.handle_conn(socket).await;
                });
            }
        });
        server
    }

    async fn handle_conn(&self, mut socket: TcpStream) -> std::io::Result<()> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 2048];
        let request = loop {
            if let Some(request) = parse_request(&buf) {
                break request;
            }
            let n = socket.read(&mut chunk).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&chunk[..n]);
        };
        let ack = self.route(&request.0, &request.1);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{ack}",
            ack.len()
        );
        socket.write_all(response.as_bytes()).await?;
        socket.shutdown().await
    }

    fn route(&self, path: &str, body: &str) -> String {
        match path {
            "/ilink/bot/getupdates" => {
                let msgs: Vec<Value> = self.scripted.lock().drain(..).collect();
                if msgs.is_empty() {
                    String::new() // 空体 = 服务端长轮询超时无新消息
                } else {
                    json!({ "get_updates_buf": "cursor-1", "ret": 0, "errcode": 0, "msgs": msgs })
                        .to_string()
                }
            }
            "/ilink/bot/sendmessage" => {
                self.sent_bodies
                    .lock()
                    .push(serde_json::from_str(body).unwrap_or(Value::Null));
                self.send_acks.lock().pop().unwrap_or_else(|| "{}".into())
            }
            _ => "{}".into(),
        }
    }
}

/// 解析 (path, body)；请求不完整返回 None（调用方继续读）
fn parse_request(buf: &[u8]) -> Option<(String, String)> {
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&buf[..header_end]).ok()?;
    let path = head.split("\r\n").next()?.split(' ').nth(1)?.to_owned();
    let content_length: usize = head
        .split("\r\n")
        .skip(1)
        .find_map(|line| {
            let lower = line.to_ascii_lowercase();
            lower
                .starts_with("content-length:")
                .then(|| line[15..].trim().parse().ok())
                .flatten()
        })
        .unwrap_or(0);
    let body_start = header_end + 4;
    if buf.len() < body_start + content_length {
        return None;
    }
    Some((
        path,
        String::from_utf8_lossy(&buf[body_start..body_start + content_length]).into_owned(),
    ))
}

fn inbound_msg(peer: &str, message_type: i32, text: &str, token: &str, client_id: &str) -> Value {
    json!({
        "from_user_id": peer,
        "to_user_id": "bot",
        "client_id": client_id,
        "message_type": message_type,
        "context_token": token,
        "item_list": [{ "type": 1, "text_item": { "text": text } }],
    })
}

fn temp_state_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("wechat-adapter-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn read_state(dir: &Path) -> Value {
    std::fs::read_to_string(dir.join("bot-state.json"))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or(Value::Null)
}

async fn eventually(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..300 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("condition not met within 3s: {what}");
}

fn test_config(base_url: &str, dir: &Path) -> WechatConfig {
    WechatConfig {
        state_dir: dir.to_owned(),
        base_url: base_url.to_owned(),
        rate_base_ms: 1,
        rate_jitter_ms: 0,
        send_retries: 1,
        poll_idle_ms: 10,
        ..Default::default()
    }
}

fn credentials() -> Credentials {
    Credentials {
        bot_token: "test-token".into(),
        ilink_bot_id: "bid-1".into(),
        ilink_user_id: "owner".into(),
    }
}

/// 组装 io + run 任务；返回 (出站发送端, 入站接收端, 停机发送端, run 句柄)
#[allow(clippy::type_complexity)]
fn spawn_adapter(
    adapter: WechatAdapter,
    inbound_capacity: usize,
) -> (
    tokio::sync::mpsc::Sender<OutboundCommand>,
    tokio::sync::mpsc::Receiver<referee_channel::InboundMessage>,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<Result<(), referee_channel::AdapterError>>,
) {
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(inbound_capacity);
    let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let run = tokio::spawn(async move {
        adapter
            .run(ChannelIo {
                inbound_tx,
                outbound_rx,
                shutdown: shutdown_rx,
            })
            .await
    });
    (outbound_tx, inbound_rx, shutdown_tx, run)
}

// ───────────────────────────────────────────────
// 入站：映射 + 回环过滤 + 游标/令牌落盘
// ───────────────────────────────────────────────

#[tokio::test]
async fn inbound_maps_filters_and_persists_cursor() {
    let mock = MockIlink::start().await;
    *mock.scripted.lock() = vec![
        inbound_msg("u1", 1, "你好", "T1", "m1"),
        inbound_msg("u2", 2, "回环", "T2", "m2"), // BOT 回环，必须过滤
    ];
    let dir = temp_state_dir("inbound");
    let adapter = WechatAdapter::with_credentials(test_config(&mock.base_url, &dir), credentials())
        .await
        .unwrap();
    let (_out_tx, mut inbound_rx, shutdown_tx, run) = spawn_adapter(adapter, 8);

    let msg = tokio::time::timeout(std::time::Duration::from_secs(2), inbound_rx.recv())
        .await
        .expect("收到入站消息")
        .unwrap();
    assert_eq!(msg.endpoint, "wechat/bid-1");
    assert_eq!(msg.peer, "u1");
    assert_eq!(msg.message_id, "m1");
    assert_eq!(msg.content, ChannelContent::Text("你好".into()));
    assert_eq!(msg.session_ctx, "T1");

    // 回环消息被过滤：短窗口内无第二条入站
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), inbound_rx.recv())
            .await
            .is_err()
    );

    // 游标与令牌落盘（只记录用户消息的令牌，回环 T2 不入库）
    eventually("state persisted", || {
        let state = read_state(&dir);
        state["cursor"] == "cursor-1" && state["context_tokens"]["u1"] == "T1"
    })
    .await;
    assert_eq!(read_state(&dir)["context_tokens"].as_object().unwrap().len(), 1);

    shutdown_tx.send(true).unwrap();
    run.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────────────────────────────
// 出站：使用记录的令牌；无令牌丢弃；-14 放弃但循环存活
// ───────────────────────────────────────────────

#[tokio::test]
async fn outbound_uses_recorded_token_and_survives_expiry() {
    let mock = MockIlink::start().await;
    // 第一条出站收到 errcode=-14（令牌过期），之后的出站正常受理
    *mock.send_acks.lock() = vec![
        r#"{"ret":-1,"errcode":-14,"errmsg":"expired"}"#.into(),
        "{}".into(),
    ];
    let dir = temp_state_dir("outbound");
    // 预置会话令牌：u1 → T9（该 peer 发过消息）
    WechatState::load(&dir)
        .unwrap()
        .advance("", &[("u1".into(), "T9".into())])
        .unwrap();
    let adapter = WechatAdapter::with_credentials(test_config(&mock.base_url, &dir), credentials())
        .await
        .unwrap();
    let (out_tx, _inbound_rx, shutdown_tx, run) = spawn_adapter(adapter, 8);

    // 无令牌的 peer：丢弃，不产生请求
    out_tx
        .send(OutboundCommand {
            endpoint: "wechat/bid-1".into(),
            peer: "ghost".into(),
            content: ChannelContent::Text("无令牌".into()),
        })
        .await
        .unwrap();
    // 有令牌但令牌过期：发出一次请求后放弃（不重试）
    out_tx
        .send(OutboundCommand {
            endpoint: "wechat/bid-1".into(),
            peer: "u1".into(),
            content: ChannelContent::Text("第一条".into()),
        })
        .await
        .unwrap();
    eventually("first send attempted", || mock.sent_bodies.lock().len() == 1).await;
    assert_eq!(
        mock.sent_bodies.lock()[0]["msg"]["context_token"], "T9",
        "出站必须使用入站记录的会话令牌"
    );
    assert_eq!(mock.sent_bodies.lock()[0]["msg"]["to_user_id"], "u1");
    assert_eq!(
        mock.sent_bodies.lock()[0]["msg"]["item_list"][0]["text_item"]["text"],
        "第一条"
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        mock.sent_bodies.lock().len(),
        1,
        "TokenExpired 不重试，也不拖垮循环"
    );

    // 循环存活：后续出站正常受理
    out_tx
        .send(OutboundCommand {
            endpoint: "wechat/bid-1".into(),
            peer: "u1".into(),
            content: ChannelContent::Text("第二条".into()),
        })
        .await
        .unwrap();
    eventually("second send accepted", || mock.sent_bodies.lock().len() == 2).await;

    shutdown_tx.send(true).unwrap();
    run.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────────────────────────────
// 背压：入站通道满时游标不推进，腾空后推进并落盘
// ───────────────────────────────────────────────

#[tokio::test]
async fn cursor_advances_only_after_inbound_delivered() {
    let mock = MockIlink::start().await;
    *mock.scripted.lock() = vec![
        inbound_msg("u1", 1, "一", "T1", "m1"),
        inbound_msg("u1", 1, "二", "T1", "m2"),
    ];
    let dir = temp_state_dir("backpressure");
    let adapter = WechatAdapter::with_credentials(test_config(&mock.base_url, &dir), credentials())
        .await
        .unwrap();
    let (_out_tx, mut inbound_rx, shutdown_tx, run) = spawn_adapter(adapter, 1); // 容量 1

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        read_state(&dir).is_null(),
        "第二条未投递成功前，游标不得落盘（崩溃宁可重放不可丢）"
    );

    inbound_rx.recv().await.unwrap(); // 腾出容量
    eventually("cursor persisted after delivery", || {
        read_state(&dir)["cursor"] == "cursor-1"
    })
    .await;

    shutdown_tx.send(true).unwrap();
    run.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

//! A1 验收：统一消息模型 serde 往返 / Envelope 编解码 / 畸形载荷容错

use serde_json::{json, Value};
use uuid::Uuid;

use referee_channel::message::{kind, meta};
use referee_channel::{
    ChannelContent, ChannelError, InboundMessage, OutboundCommand, PeerKey, SendReceipt,
    SentNotice,
};
use referee_core::Envelope;

fn inbound(content: ChannelContent, raw: Option<Value>) -> InboundMessage {
    InboundMessage {
        endpoint: "wechat/bot-001".into(),
        peer: "user-甲".into(),
        message_id: "m-1".into(),
        content,
        session_ctx: "ctx-token".into(),
        occurred_at: 1_756_000_000_000,
        raw,
    }
}

fn outbound() -> OutboundCommand {
    OutboundCommand {
        endpoint: "wechat/bot-001".into(),
        peer: "user-甲".into(),
        content: ChannelContent::Text("正在查询…".into()),
    }
}

fn assert_payload_is_json(env: &Envelope) {
    let bytes = env.payload.as_deref().expect("payload 必须存在");
    serde_json::from_slice::<Value>(bytes).expect("payload 必须是合法 JSON");
}

#[test]
fn inbound_roundtrip_keeps_raw_escape_hatch() {
    let msg = inbound(
        ChannelContent::Text("你好".into()),
        Some(json!({"message_type": 1, "seq": 42})),
    );
    let env = msg.to_envelope();
    assert_eq!(env.metadata.get(meta::KIND).map(String::as_str), Some(kind::INBOUND));
    assert_eq!(env.priority, 100);
    assert_payload_is_json(&env);
    assert_eq!(InboundMessage::from_envelope(&env).unwrap(), msg);
}

#[test]
fn inbound_media_variant_roundtrip() {
    let msg = inbound(
        ChannelContent::Media {
            media_kind: "image".into(),
            cdn_ref: "cdn-ref-1".into(),
            aes_key: Some("a2V5".into()),
        },
        None,
    );
    let env = msg.to_envelope();
    assert_eq!(InboundMessage::from_envelope(&env).unwrap(), msg);
}

#[test]
fn outbound_send_carries_attribution() {
    let cmd = outbound();
    let session_id = Uuid::new_v4();
    let env = cmd.to_send_envelope(session_id, Some(7));
    assert_eq!(env.metadata.get(meta::KIND).map(String::as_str), Some(kind::SEND));
    assert_eq!(env.metadata.get(meta::SESSION_ID).cloned(), Some(session_id.to_string()));
    assert_eq!(env.metadata.get(meta::TURN_ID).map(String::as_str), Some("7"));
    // turn 未知（router 兜底路径）：不写 turn_id，host 跳过 im.sent 归因
    let env = cmd.to_send_envelope(session_id, None);
    assert!(!env.metadata.contains_key(meta::TURN_ID));
    assert_payload_is_json(&env);
    assert_eq!(OutboundCommand::from_envelope(&env).unwrap(), cmd);
    assert_eq!(cmd.peer_key().peer, "user-甲");
}

#[test]
fn outbound_system_has_no_attribution() {
    let cmd = outbound();
    let env = cmd.to_system_envelope();
    assert_eq!(env.metadata.get(meta::KIND).map(String::as_str), Some(kind::SYSTEM));
    assert!(!env.metadata.contains_key(meta::SESSION_ID));
    assert_eq!(OutboundCommand::from_envelope(&env).unwrap(), cmd);
}

#[test]
fn receipt_and_sent_notice_roundtrip() {
    let receipt = SendReceipt {
        accepted: true,
        queue_depth: 3,
    };
    let env = receipt.to_envelope();
    assert_eq!(env.metadata.get(meta::KIND).map(String::as_str), Some(kind::RECEIPT));
    assert_eq!(SendReceipt::from_envelope(&env).unwrap(), receipt);

    let notice = SentNotice {
        endpoint: "wechat/bot-001".into(),
        peer: "user-甲".into(),
        session_id: Uuid::new_v4(),
        turn_id: 9,
    };
    let env = notice.to_envelope();
    assert_eq!(env.metadata.get(meta::KIND).map(String::as_str), Some(kind::SENT));
    assert_eq!(SentNotice::from_envelope(&env).unwrap(), notice);
}

#[test]
fn unknown_payload_fields_are_tolerated() {
    let msg = inbound(ChannelContent::Text("x".into()), None);
    let mut env = msg.to_envelope();
    let mut value = serde_json::from_slice::<Value>(env.payload.as_deref().unwrap()).unwrap();
    value["future_field"] = json!("unknown");
    env.payload = Some(serde_json::to_vec(&value).unwrap().into());
    assert_eq!(InboundMessage::from_envelope(&env).unwrap(), msg);
}

#[test]
fn session_convention_keys_do_not_interfere() {
    // 会话协议走 metadata["_msg"]，与通道层 kind/payload 并存互不影响
    let msg = inbound(ChannelContent::Text("x".into()), None);
    let mut env = msg.to_envelope();
    env.metadata.insert("_msg".into(), "{}".into());
    assert_eq!(InboundMessage::from_envelope(&env).unwrap(), msg);

    // 纯会话信封（无 kind、无 payload）在通道层视角是畸形消息：报 Decode，不 panic
    assert!(matches!(
        InboundMessage::from_envelope(&Envelope::new()),
        Err(ChannelError::Decode(_))
    ));
}

#[test]
fn malformed_payloads_decode_to_error_without_panic() {
    let msg = inbound(ChannelContent::Text("x".into()), None);

    // 截断
    let mut env = msg.to_envelope();
    let bytes = env.payload.as_deref().unwrap().to_vec();
    env.payload = Some(bytes[..bytes.len() / 2].to_vec().into());
    assert!(matches!(InboundMessage::from_envelope(&env), Err(ChannelError::Decode(_))));

    // 错型：载荷是回信结构
    let mut env = msg.to_envelope();
    env.payload = Some(
        serde_json::to_vec(&SendReceipt { accepted: true, queue_depth: 0 }).unwrap().into(),
    );
    assert!(matches!(InboundMessage::from_envelope(&env), Err(ChannelError::Decode(_))));

    // 缺载荷
    let mut env = msg.to_envelope();
    env.payload = None;
    assert!(matches!(InboundMessage::from_envelope(&env), Err(ChannelError::Decode(_))));

    // kind 不匹配：SENT 信封当 INBOUND 解
    let notice = SentNotice {
        endpoint: "e".into(),
        peer: "p".into(),
        session_id: Uuid::new_v4(),
        turn_id: 0,
    };
    assert!(matches!(
        InboundMessage::from_envelope(&notice.to_envelope()),
        Err(ChannelError::Decode(_))
    ));
}

#[test]
fn peer_key_equality_and_hash() {
    use std::collections::HashSet;

    let a = inbound(ChannelContent::Text("x".into()), None).peer_key();
    let b = inbound(ChannelContent::Text("y".into()), None).peer_key();
    let mut set = HashSet::new();
    set.insert(a.clone());
    set.insert(b);
    assert_eq!(set.len(), 1);

    set.insert(PeerKey {
        endpoint: "feishu/app-1".into(),
        peer: a.peer.clone(),
    });
    assert_eq!(set.len(), 2);
}

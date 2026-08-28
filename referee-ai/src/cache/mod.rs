//! 内存 LRU + TTL 缓存 — Phase 5
//!
//! 基于请求内容哈希做语义缓存：完全一致的请求直接返回缓存响应，
//! 避免重复 LLM 调用（降低 API 成本与响应延迟）。
//!
//! ## 缓存键（AGENT_RUNTIME_PLAN §5.6 + §7 风险 4）
//! `CacheKey = provider/model + content_hash + params_hash`
//! - `content_hash`：messages + tools 的序列化哈希（确定性内容）
//! - `params_hash`：**影响输出的全部参数**（temperature / max_tokens /
//!   thinking / tool_choice）——绝不做「排除动态字段」的简化，否则不同
//!   温度会错误共享缓存（规划风险 4 明确要求参数进键）。
//!
//! ## 并发与容量
//! - `DashMap` 数据存储 + `Mutex<VecDeque>` 访问顺序，锁序约定：
//!   **永不持 DashMap guard 时锁 lru**（get 先 clone 再刷新顺序）。
//! - 容量为有界软上限（DashMap len 为近似值，并发 set 可能短暂超限；
//!   与 Phase 1 背压语义一致：拒绝无界增长）。
//! - **无死键堆积**：TTL 过期只移除 map；lru 队列中的失效键在 evict 时
//!   惰性跳过，绝不无界增长。
//!
//! ## 流式一致性
//! [`synthetic_stream`] 将缓存响应切分为 Delta 块 + Finish 块，保持
//! 流式接口语义一致（拼接结果与原文完全等价）。

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::stream::{self, Stream};
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::provider::{ChatRequest, ChatResponse, LlmError, StreamChunk, ToolChoice};

/// 缓存键 — provider/model + 内容哈希 + 参数哈希
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    /// 厂商 + 模型唯一标识（`ProviderId.as_str()`，已含 model，如 "deepseek/deepseek-v4-flash"）
    pub provider: String,
    /// 请求内容哈希（messages + tools 序列化）
    pub content_hash: u64,
    /// 请求参数哈希（temperature / max_tokens / thinking / tool_choice）
    pub params_hash: u64,
}

/// 缓存配置 — 挂载于 `AgentConfig`（`enabled=false` 时完全禁用）
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CacheConfig {
    pub enabled: bool,
    /// 最大条目数（有界，超限按 LRU 淘汰）
    pub capacity: usize,
    /// 条目生存时间（过期失效）
    pub ttl: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            // 默认关闭：多轮会话历史逐轮增长，全量哈希键命中率≈0，纯白付序列化开销
            // （AI-4 修复）。需显式开启的典型场景：幂等重试 / 无状态问答。
            enabled: false,
            capacity: 1000,
            ttl: Duration::from_secs(3600),
        }
    }
}

impl CacheConfig {
    /// 禁用缓存
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            capacity: 0,
            ttl: Duration::ZERO,
        }
    }
}

/// 缓存条目
struct CacheEntry {
    response: ChatResponse,
    created_at: Instant,
}

/// 内存 LRU + TTL 缓存（`Send + Sync`，可在多 task 间共享）
pub struct InMemoryCache {
    map: DashMap<CacheKey, CacheEntry>,
    /// 访问顺序提示：队尾最近、队首最久。仅作顺序记录，
    /// 失效键在 evict 时惰性跳过（与 map 保持最终一致，绝无死键堆积）。
    lru: Mutex<VecDeque<CacheKey>>,
    capacity: usize,
    ttl: Duration,
}

impl InMemoryCache {
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            map: DashMap::new(),
            lru: Mutex::new(VecDeque::with_capacity(capacity.min(64))),
            capacity,
            ttl,
        }
    }

    /// 当前缓存条目数（观测用）
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 命中：返回响应副本 + 刷新 LRU 顺序；过期或缺失返回 None
    pub fn get(&self, key: &CacheKey) -> Option<ChatResponse> {
        // 1. 存在性 + TTL 检查
        //    注意：过期分支必须先 drop Ref guard 再 remove —— DashMap 同 shard
        //    读锁未释放就取写锁（parking_lot RwLock 非重入）会死锁。
        let entry = self.map.get(key)?;
        if entry.created_at.elapsed() > self.ttl {
            drop(entry);
            self.map.remove(key);
            return None;
        }
        let response = entry.response.clone();
        drop(entry);

        // 2. 刷新 LRU（移到队尾）
        let mut lru = self.lru.lock();
        lru.retain(|k| *k != *key);
        lru.push_back(key.clone());
        Some(response)
    }

    /// 写入缓存：同键覆盖（刷新顺序）；容量满时淘汰最久未使用（惰性跳过失效键）
    ///
    /// `capacity == 0` 视为禁用（与 `CacheConfig::disabled` 语义一致，绝不存储）。
    pub fn set(&self, key: CacheKey, response: ChatResponse) {
        if self.capacity == 0 {
            return;
        }
        // 同键已存在 → 覆盖并刷新顺序
        if self.map.contains_key(&key) {
            self.map.insert(
                key.clone(),
                CacheEntry {
                    response,
                    created_at: Instant::now(),
                },
            );
            let mut lru = self.lru.lock();
            lru.retain(|k| *k != key);
            lru.push_back(key);
            return;
        }

        // 容量淘汰：从队首弹出，直到弹出一个仍存在于 map 的键（跳过 TTL 死键）
        while self.map.len() >= self.capacity {
            let victim = self.lru.lock().pop_front();
            match victim {
                Some(v) => {
                    if self.map.remove(&v).is_some() {
                        break;
                    }
                    // 死键（已被淘汰/过期移除）→ 继续弹出
                }
                None => break, // 队列空（容量 > 0 时理论不会）
            }
        }

        self.map.insert(
            key.clone(),
            CacheEntry {
                response,
                created_at: Instant::now(),
            },
        );
        self.lru.lock().push_back(key);
    }

    /// 从最终请求计算缓存键（provider + content_hash + params_hash）
    pub fn key_for_request(&self, req: &ChatRequest, provider: &str) -> CacheKey {
        let tools_json = serde_json::to_string(&req.tools).unwrap_or_default();
        let content_hash = Self::hash_request(&req.messages, &tools_json, None, None, true);
        let params_hash = Self::hash_params(req);
        CacheKey {
            provider: provider.to_string(),
            content_hash,
            params_hash,
        }
    }

    /// 请求内容哈希
    ///
    /// `exclude_dynamic_fields = true` 时排除 temperature / max_tokens
    /// （供 content_hash 使用——动态参数已由 [`Self::hash_params`] 单独
    /// 进键，绝不从缓存键整体剔除）。
    pub fn hash_request(
        messages: &[crate::provider::Message],
        tools_json: &str,
        temp: Option<f32>,
        max_tok: Option<usize>,
        exclude_dynamic_fields: bool,
    ) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        if let Ok(s) = serde_json::to_string(messages) {
            s.hash(&mut hasher);
        }
        tools_json.hash(&mut hasher);

        if !exclude_dynamic_fields {
            if let Some(t) = temp {
                t.to_bits().hash(&mut hasher);
            }
            if let Some(m) = max_tok {
                m.hash(&mut hasher);
            }
        }
        hasher.finish()
    }

    /// 请求参数哈希 — 全部影响输出的参数（规划风险 4：参数必须进键）
    ///
    /// 覆盖：temperature / max_tokens / thinking（enabled + effort）/
    /// tool_choice / `extra`（厂商透传参数，直接影响请求 body 与输出）。
    fn hash_params(req: &ChatRequest) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        if let Some(t) = req.temperature {
            t.to_bits().hash(&mut hasher);
        }
        if let Some(m) = req.max_tokens {
            m.hash(&mut hasher);
        }
        req.thinking.enabled.hash(&mut hasher);
        // ReasoningEffort 未实现 Hash，按判别式编码（enabled=true 时 effort 决定输出深度）
        reasoning_effort_code(req.thinking.effort).hash(&mut hasher);
        // ToolChoice 未实现 Hash，按判别式编码
        tool_choice_code(req.tool_choice).hash(&mut hasher);
        // extra：厂商透传参数直接进入请求 body，同样影响输出，必须进键
        if !req.extra.is_empty() {
            if let Ok(s) = serde_json::to_string(&req.extra) {
                s.hash(&mut hasher);
            }
        }
        hasher.finish()
    }
}

fn reasoning_effort_code(e: Option<crate::provider::ReasoningEffort>) -> u8 {
    match e {
        None => 0,
        Some(crate::provider::ReasoningEffort::Low) => 1,
        Some(crate::provider::ReasoningEffort::High) => 2,
        Some(crate::provider::ReasoningEffort::Max) => 3,
    }
}

fn tool_choice_code(c: ToolChoice) -> u8 {
    match c {
        ToolChoice::Auto => 0,
        ToolChoice::None => 1,
        ToolChoice::Required => 2,
    }
}

/// 将缓存的静态响应转换为流式输出（流式接口一致性）
///
/// 按固定大小（10 字符）切分为多个 `StreamChunk::Delta`，最后发一个
/// `StreamChunk::Finish`（携带 finish_reason + usage）。拼接所有 Delta
/// 内容与原始响应完全一致。
pub fn synthetic_stream(
    response: ChatResponse,
) -> impl Stream<Item = Result<StreamChunk, LlmError>> + Send + 'static {
    const CHUNK_SIZE: usize = 10;

    let text = response.message.content.as_text().unwrap_or("").to_string();
    let chars: Vec<char> = text.chars().collect();
    let total_chunks = chars.len().div_ceil(CHUNK_SIZE);
    let finish_reason = response.finish_reason;
    let usage = response.usage;

    stream::iter(0..=total_chunks).map(move |i| {
        if i < total_chunks {
            let start = i * CHUNK_SIZE;
            let end = (start + CHUNK_SIZE).min(chars.len());
            Ok(StreamChunk::Delta {
                content: Some(chars[start..end].iter().collect()),
                reasoning_content: None,
                tool_calls: Vec::new(),
                role: None,
            })
        } else {
            Ok(StreamChunk::Finish {
                finish_reason: finish_reason.clone(),
                usage: usage.clone(),
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{FinishReason, Message, MessageContent, TokenUsage};

    fn response(content: &str) -> ChatResponse {
        ChatResponse {
            id: "cached".into(),
            model: "mock".into(),
            message: Message::assistant(content),
            finish_reason: FinishReason::Stop,
            usage: Some(TokenUsage::default()),
        }
    }

    fn key(a: u64) -> CacheKey {
        CacheKey {
            provider: "mock".into(),
            content_hash: a,
            params_hash: 0,
        }
    }

    #[test]
    fn get_updates_lru_order() {
        let cache = InMemoryCache::new(2, Duration::from_secs(60));
        cache.set(key(1), response("one"));
        cache.set(key(2), response("two"));

        // 访问 1 → 1 成为最近使用
        assert!(cache.get(&key(1)).is_some());

        // 插入 3 → 淘汰最久未使用的 2
        cache.set(key(3), response("three"));
        assert!(cache.get(&key(1)).is_some(), "1 refreshed, must survive");
        assert!(cache.get(&key(2)).is_none(), "2 evicted (LRU)");
        assert!(cache.get(&key(3)).is_some());
    }

    #[test]
    fn evicts_lru_when_capacity_reached() {
        let cache = InMemoryCache::new(2, Duration::from_secs(60));
        cache.set(key(1), response("a"));
        cache.set(key(2), response("b"));
        cache.set(key(3), response("c"));
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key(1)).is_none(), "oldest evicted");
        assert!(cache.get(&key(2)).is_some());
        assert!(cache.get(&key(3)).is_some());
    }

    #[test]
    fn ttl_expiry_removes_entry() {
        let cache = InMemoryCache::new(4, Duration::from_millis(20));
        cache.set(key(1), response("a"));
        assert!(cache.get(&key(1)).is_some());
        std::thread::sleep(Duration::from_millis(40));
        assert!(cache.get(&key(1)).is_none(), "expired entry must miss");
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn expired_keys_do_not_clog_lru() {
        // 过期条目经 get 移除后 lru 残留死键；evict 时必须跳过死键、
        // 只淘汰仍在 map 中的活键——lru 队列绝不无界堆积。
        let cache = InMemoryCache::new(2, Duration::from_millis(20));
        cache.set(key(1), response("a"));
        cache.set(key(2), response("b"));
        std::thread::sleep(Duration::from_millis(40)); // 1、2 均过期

        assert!(cache.get(&key(1)).is_none()); // 过期移除 → lru 残留死键 1
        assert_eq!(cache.len(), 1); // map 中仅剩 2（也已过期但未被触碰）

        cache.set(key(3), response("c")); // len=1 < 2 → 不淘汰 → map={2,3}
        cache.set(key(4), response("d")); // len=2 → evict：pop 1（死键跳过）→ pop 2（活键淘汰）
        assert_eq!(cache.len(), 2);
        assert!(
            cache.get(&key(2)).is_none(),
            "2 must be evicted (1 was a stale key)"
        );
        assert!(
            cache.get(&key(3)).is_some(),
            "3 fresh (inserted after sleep), must hit"
        );
        assert!(cache.get(&key(4)).is_some(), "4 fresh, must hit");
    }

    #[test]
    fn overwrite_refreshes_order() {
        let cache = InMemoryCache::new(2, Duration::from_secs(60));
        cache.set(key(1), response("a"));
        cache.set(key(2), response("b"));
        cache.set(key(1), response("a2")); // 覆盖同键 → 刷新顺序
        cache.set(key(3), response("c")); // 淘汰 2（1 被刷新过）
        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(2)).is_none());
    }

    #[test]
    fn params_affect_cache_key() {
        // 规划风险 4：不同温度 → 不同键（绝不因「排除动态字段」共享缓存）
        let mut req = ChatRequest::simple("hello");
        req.temperature = Some(0.0);
        let k1 = InMemoryCache::new(2, Duration::from_secs(60)).key_for_request(&req, "mock");
        req.temperature = Some(0.7);
        let k2 = InMemoryCache::new(2, Duration::from_secs(60)).key_for_request(&req, "mock");
        assert_ne!(k1.params_hash, k2.params_hash);
        assert_ne!(k1, k2);

        // max_tokens 同理
        let mut req = ChatRequest::simple("hello");
        req.max_tokens = Some(100);
        let k1 = InMemoryCache::new(2, Duration::from_secs(60)).key_for_request(&req, "mock");
        req.max_tokens = Some(200);
        let k2 = InMemoryCache::new(2, Duration::from_secs(60)).key_for_request(&req, "mock");
        assert_ne!(k1.params_hash, k2.params_hash);
    }

    #[test]
    fn thinking_effort_affects_cache_key() {
        // effort 决定输出深度（DeepSeek reasoning_effort 映射），必须进键
        let mut req = ChatRequest::simple("hello");
        req.thinking.enabled = true;
        req.thinking.effort = Some(crate::provider::ReasoningEffort::Low);
        let k1 = InMemoryCache::new(2, Duration::from_secs(60)).key_for_request(&req, "mock");
        req.thinking.effort = Some(crate::provider::ReasoningEffort::Max);
        let k2 = InMemoryCache::new(2, Duration::from_secs(60)).key_for_request(&req, "mock");
        assert_ne!(k1.params_hash, k2.params_hash);
        assert_ne!(k1, k2);
    }

    #[test]
    fn extra_params_affect_cache_key() {
        // extra 透传参数进入请求 body，必须进键
        let key_of = |v: serde_json::Value| {
            let mut req = ChatRequest::simple("hello");
            req.extra.insert("vendor_param".into(), v);
            InMemoryCache::new(2, Duration::from_secs(60)).key_for_request(&req, "mock")
        };
        let k1 = key_of(serde_json::json!({"a": 1}));
        let k2 = key_of(serde_json::json!({"a": 2}));
        assert_ne!(k1.params_hash, k2.params_hash);
    }

    #[test]
    fn identical_request_identical_key() {
        let cache = InMemoryCache::new(2, Duration::from_secs(60));
        let k1 = cache.key_for_request(&ChatRequest::simple("hello"), "mock");
        let k2 = cache.key_for_request(&ChatRequest::simple("hello"), "mock");
        assert_eq!(k1, k2);

        let k3 = cache.key_for_request(&ChatRequest::simple("world"), "mock");
        assert_ne!(k1, k3);

        // provider 不同 → 键不同
        let k4 = cache.key_for_request(&ChatRequest::simple("hello"), "other");
        assert_ne!(k1, k4);
    }

    #[test]
    fn content_hash_excludes_params_by_default() {
        // hash_request 的 exclude 参数：content_hash 只含 messages+tools，
        // 动态参数由 params_hash 承载（见 params_affect_cache_key）
        let msgs = vec![Message::user("hello")];
        let h1 = InMemoryCache::hash_request(&msgs, "", Some(0.0), None, true);
        let h2 = InMemoryCache::hash_request(&msgs, "", Some(0.7), None, true);
        assert_eq!(h1, h2, "content_hash must exclude dynamic params");
        let h3 = InMemoryCache::hash_request(&msgs, "", Some(0.7), None, false);
        assert_ne!(h1, h3, "excluding=false must fold params into hash");
    }

    #[test]
    fn synthetic_stream_reassembles() {
        use futures::StreamExt;

        let content = "你好，这是用于验证合成流的完整响应内容，共二十五个字以上。";
        let response = ChatResponse {
            id: "cached".into(),
            model: "mock".into(),
            message: Message::assistant(content),
            finish_reason: FinishReason::Stop,
            usage: Some(TokenUsage {
                prompt_tokens: 5,
                completion_tokens: 40,
                total_tokens: 45,
                ..Default::default()
            }),
        };

        let mut stream = synthetic_stream(response.clone());
        let mut joined = String::new();
        let mut delta_count = 0usize;
        let mut finish: Option<StreamChunk> = None;
        while let Some(Ok(chunk)) = futures::executor::block_on(stream.next()) {
            match chunk {
                StreamChunk::Delta { content, .. } => {
                    joined.push_str(content.as_deref().unwrap_or(""));
                    delta_count += 1;
                }
                StreamChunk::Finish { .. } => finish = Some(chunk),
            }
        }
        assert_eq!(joined, content, "reassembled stream must equal original");
        assert!(
            delta_count > 1,
            "expected multiple delta chunks, got {delta_count}"
        );
        match finish.expect("must end with Finish") {
            StreamChunk::Finish {
                finish_reason,
                usage,
            } => {
                assert_eq!(finish_reason, FinishReason::Stop);
                assert_eq!(usage, response.usage);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn synthetic_stream_chunks_are_bounded() {
        use futures::StreamExt;

        let content = "a".repeat(25);
        let mut stream = synthetic_stream(response(&content));
        let mut sizes = Vec::new();
        while let Some(Ok(chunk)) = futures::executor::block_on(stream.next()) {
            if let StreamChunk::Delta { content, .. } = chunk {
                sizes.push(content.unwrap_or_default().chars().count());
            }
        }
        assert!(sizes.iter().all(|&s| s <= 10), "chunk sizes: {sizes:?}");
    }

    #[test]
    fn empty_content_still_emits_finish() {
        use futures::StreamExt;
        let mut stream = synthetic_stream(response(""));
        let mut finished = false;
        while let Some(Ok(chunk)) = futures::executor::block_on(stream.next()) {
            if let StreamChunk::Finish { .. } = chunk {
                finished = true;
            }
        }
        assert!(finished, "empty content must still emit Finish");
    }

    #[test]
    fn message_content_helpers() {
        let c = MessageContent::text("hi");
        assert_eq!(c.as_text(), Some("hi"));
    }
}

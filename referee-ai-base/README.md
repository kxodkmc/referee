# Referee AI Base — Agent 核心支撑层（地基）

业务无关的**基础 AI 设施**积木，提供「接 LLM → 组装 prompt → 调工具 → 管预算 →
回复」的最小闭环，并在此基础上提供**流式输出**与**会话生命周期管理**（快照 / 枚举 /
删除 / 空闲回收）。不预置记忆、MCP、Skills 等业务策略；业务化、开箱即用的完整 Agent
封装在 `referee-agent`。

## 模块

| 模块 | 职责 |
|------|------|
| `provider` | 厂商唯一 I/O 边界：`LLMProvider` trait、纯数据模型、错误归一与重试、能力声明、OpenAI 兼容底座 + 厂商适配器 |
| `session` | 会话状态机（Idle/Thinking/AwaitingCalls）、超时、终态自管（`run_turn`） |
| `tool` | 工具抽象 `Tool` + 有界注册表 + 并行/截断/panic 隔离/超时执行器 + 同步/异步派发（`wait` 分流） |
| `store` | 通用有界 KV 存储抽象（成果/大结果落库），后端可替换 |
| `budget` | Token 预算治理（Session 级 + 全局共享计数器） |
| `prompt` | 提示词组装与优先级截断（杜绝 Prompt 爆炸），`PromptParts` 统一参数封装 |
| `cache` | LRU + TTL 语义缓存，流式一致性合成 |
| `observe` | 可观测门面（tracing span、metrics、计时、LLM 重试计数） |
| `engine` | 会话引擎：最小闭环收敛到单任务顺序异步流程；流式输出（`chat_stream`）与会话生命周期管理（快照/枚举/删除/空闲回收） |

## 快速上手

```rust
use referee_ai_base::{Engine, EngineConfig, LLMProvider, ...};

// provider 由适配器构造（如 XiaomiProvider / DeepSeekProvider / 自实现）
let engine = Engine::new(provider, EngineConfig::default());

// 发起一轮会话（快速返回句柄）
let handle = engine.chat(session_id, ChatPayload::default() /* 或构造 */).unwrap();
// 等待结果（带超时防护）
let reply = tokio::time::timeout(Duration::from_secs(30), handle.wait()).await??;
// 中断
handle.cancel();

// 流式发起一轮会话：句柄 wait() 得到 EngineReply::Streaming，逐 chunk 消费
let handle = engine.chat_stream(session_id, ChatPayload::default())?;
if let referee_ai_base::EngineReply::Streaming(mut chunks) = handle.wait().await? {
    while let Some(chunk) = chunks.next().await {
        // chunk: Result<StreamChunk, LlmError> —— 边生成边消费
    }
}
```

## 流式与会话生命周期

- **流式输出**：`engine.chat_stream` 与 `chat` 同构（同一 `prepare_round` / `finish_thinking`
  收敛），仅 LLM 调用段改为 `provider.chat_stream`；chunk 逐条转发给调用方并交给
  `StreamAccumulator` 累积，收敛出的完整 `ChatResponse` 走与非流式一致的终态 ——
  **流式与非流式在 Session 上结果一致**。取消逐 chunk 检查、超时覆盖整体、panic 兜底。
- **会话快照**：`session_info(id)` 返回 `SessionSnapshot { state / history_len /
  consumed_tokens / peer_depth }`（`state` 为 `SessionPhase::Idle | Thinking | AwaitingCalls`）。
- **枚举与删除**：`list_sessions()` 枚举全部会话 ID；`remove_session(id)` 直接移除。
- **空闲回收**：`start_idle_reaper()` 启动后台任务，周期移除「Idle 且空闲超时」的会话
  （Thinking / AwaitingCalls 在途任务不受影响），返回 `ReaperHandle`（`stop()` 优雅退出）。

## 设计约束

- 分层单向依赖；模块间只经 trait。
- 会话短暂持锁、无跨 await 持 guard；回合级取消用协作式信号。
- 错误绝不静默丢弃：busy/预算/解码/工具失败均显式可见。
- 有限依赖：`referee-core`、`reqwest`、`serde_json` 及基础设施 crate。

## 并发与中断模型

- **原子回合启动（根治 TOCTOU）**：`Session::start_round` 在单一 guard 内完成
  busy 检查、turn_id 分配、取消通道创建与 history 写入。同会话并发 `chat`
  恰一个成功，其余显式返回 `Busy`，绝不污染 history、不错乱取消标志。
- **回合级中断下沉到会话**：中断标志（`AtomicBool`）与 `cancel` 通道互补——
  前者在轮隙间（工具执行/思考间隙）拦截，后者在 LLM 等待中即时打断。
  空闲会话调用 `interrupt` 返回 `false`（区分"无可取消"与"已取消"）。
- **中断后复位**：回合收敛（完成/取消/超时）后会话回到 Idle，可再次发起
  Chat，不会永久卡死 busy。
- **存储安全**：会话/工具注册表统一经 `or_insert_with`（dashmap 的 insert
  路径），规避裸 `entry()` match 触发的 shrink 死锁。

## 验证

```bash
cargo test -p referee-ai-base
cargo clippy -p referee-ai-base --all-targets -- -D warnings
```

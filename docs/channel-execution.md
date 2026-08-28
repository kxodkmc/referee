# 通道基座执行文档（referee-channel）

> **定位**：为 referee 构建 IM 通道接入基座——首期微信（iLink 协议），预留飞书 / QQ。
> 基座提供"消息进来 → 任务化 → 智能体处理 → 结果确定交付"的完整闭环，通道差异封装在适配器内。
>
> **协议事实来源**：微信侧全部协议细节见 `docs/wechat-clawbot-integration.md`（已对照官方
> 仓库源码核对 + 线上探针实测）。本文不重复协议内容，只引用结论。
>
> **代码事实来源**：本文所有对内核 / 会话引擎接口的引用均已对照当前代码核实
> （`referee-core` / `referee-ai` / `referee-agent`，2026-08-23）。关键事实：
> - `KernelContext` 只有 `emit` / `spawn_blocking` / `reply`，**没有 `invoke`**（编译期禁止
>   `handle` 内嵌套请求-响应）；完整 `Kernel` 句柄可 Clone，供扩展后台任务使用。
> - 内核 `catch_unwind` 边界**只在 `Extension::handle` 上**；扩展后台 tokio 任务的 panic
>   表现为 `JoinError`，不改变内核治理状态。适配器监督因此由 host 自带（§4.6）。
> - 回合结束**没有独立事件**（不存在 TurnFinished）；`invoke(AgentRuntime, Chat)` 的回信
>   `SessionReply` 即回合终态（AgentRuntime 在回合结束后才回信）。
> - 内核 WAL 只覆盖**经内核路由的 envelope**（dispatch 前 append、handle 完成后 ack，
>   崩溃后至少一次重投）；扩展内部状态（批次、任务队列、会话映射）不在覆盖范围。
>
> **适用范围**：Phase 1 = 文本闭环。媒体 / 流式 / 补投 / 飞书 / QQ 见第 9 节路线图。

---

## 1. 设计原则（实现与评审的共同基准）

1. **通道差异锁进适配器**：基座只见统一消息模型；加通道 = 写一个 adapter，基座零改动。
2. **数据与行为分离**：跨组件传递全部是纯数据（serde 可序列化），行为只存在于扩展与适配器。
3. **有界是硬约束**：所有队列显式容量；满了要么显式拒绝（`ResourceExhausted` 语义），要么
   反压上游（不推进游标），绝不无界堆积。
4. **隔离即防御**：每通道账号一个 host 扩展实例；adapter panic 由 host 内部监督熔断
   （退避重启 → 超限降级，§4.6），不影响内核与其余扩展。
5. **emit / invoke 各司其职**：`emit` = 尽力而为的**通知类事件**（`im.inbound` / `im.sent`，
   丢失可容忍、有 DLQ 审计）；`invoke` = 受理确认（发送、任务派发、中断）。流式增量
   **不在 emit 的能力范围内**——现有会话协议无法经 envelope 传输流（`SessionReply` 无流式
   变体），Phase 3 流式中继需要 AgentRuntime 侧新增事件类型（协议扩展，见 §9）。
6. **交付契约确定化**：回合**最终结果一律由 router 兜底管道交付**（确定性系统行为）；
   `im_send_text` 工具只承担回合内的**中间回执 / 主动汇报**（模型行为）。这样"用户是否
   收到结果"不依赖模型是否调用工具，从结构上杜绝"结果丢失"与"重复转发"两类事故（§4.6）。
7. **轻量**：Phase 1 新增依赖仅 `rand`、`base64`（微信文本链路）；错误类型沿用全线 `thiserror`
   惯例，不引入 `anyhow`；关闭信号用 `tokio::sync::watch`，不引入 `tokio-util`；媒体依赖
   （aes/ecb/md-5/hex）推迟到媒体阶段；登录二维码渲染 `qrcode` 以 feature `qr`
   按需引入（默认关闭、零新增依赖，回退为链接输出）。

## 2. 总体架构

```
 微信 iLink(长轮询) ──┐                                 ┌─ AgentRuntime (Extension)
 未来: 飞书/QQ      ──┤                                 │    ↕ SessionMessage::Chat(invoke)
                     ▼                                 ▼
              ┌──────────────┐   im.inbound(emit)  ┌──────────────────────────┐
              │ ChannelHost   │────────────────────▶│ ImRouter                  │
              │ (Extension,   │◀────────────────────│  会话映射 → 批次累积器     │
              │  每账号一个,   │   im.send(invoke)   │  → 任务队列 → 调度器       │
              │  自带adapter   │   im.system(invoke) │    (会话道+并发上限)       │
              │  监督)        │   im.sent(emit,观测) └──────────────────────────┘
              └──────────────┘                              ▲          │
                      ▲                                      └── invoke 回信 = 回合终态
                      └────── Kernel.invoke ────────────────────────┘
                          （im_send_text 工具，在引擎回合内执行）      ▼
                                                              兜底交付最终输出
                                                              （交付契约 §4.6）
```

数据流（Phase 1 主路径）：

1. adapter 长轮询收到消息 → 回环过滤（仅保留 `message_type == 1` 的用户文本）→ 投入**有界**
   入站通道（满则挂起投递 = 反压，游标不推进）。
2. ChannelHost 后台循环从通道取件 → `emit(im.inbound)` 给 ImRouter（失败由内核自动落 DLQ，
   可审计不丢失）→ 推进并持久化游标。
3. ImRouter 按 peer 批次累积（8s 静默闭合）→ 生成 Task 入**有界**任务队列（满则回系统提示，
   显式拒绝）。
4. 调度器：同会话严格串行（会话道），全局并发上限（信号量）→ 经完整 Kernel 句柄
   `invoke(AgentRuntime, Chat)`。
5. 模型在回合内调用 `im_send_text` 发送**中间回执 / 主动汇报** → `invoke(im.send)` →
   host 受理（有界出站队列）→ adapter 限速落线；受理后 host `emit(im.sent)` 给 router
   （仅观测归因，不参与交付判断）。
6. 回合终态 = 调度 worker 收到 invoke 回信（`SessionReply`）→ 按交付契约处理（§4.6）：
   `Success` 非空 → **兜底 `im.send` 最终输出**（4000 分段在 adapter）；`Cancelled` → 不兜底；
   `Busy` → 回队尾重试；`Error` → 系统提示。

## 3. 模块划分与职责

### 3.1 crate 布局

```
referee-channel/                 # 基座（不含任何具体通道）
├── src/message.rs               # 统一消息模型 + PeerKey + Envelope 编解码
├── src/adapter.rs               # ChannelAdapter trait + ChannelIo + 能力声明
├── src/host.rs                  # ChannelHost<A>（Extension；adapter 监督 + 收发桥接）
├── src/batch.rs                 # BatchAccumulator（批次累积器）
├── src/dispatch.rs              # 任务队列 + 调度器（会话道 + 信号量 + 回信处理）
├── src/router.rs                # ImRouter（Extension：会话映射，串联 batch→dispatch→兜底交付）
├── src/policy.rs                # 交付契约（回信分类处理）/ 中断关键字
├── src/tools.rs                 # im_send_text 工具（impl referee_ai::Tool）
└── src/error.rs                 # ChannelError 分类

referee-channel-wechat/          # 微信 iLink 适配器
├── src/lib.rs                   # WechatAdapter（impl ChannelAdapter）+ 工厂
├── src/client.rs                # IlinkClient（照抄协议文档 §5）
├── src/types.rs                 # 协议结构（照抄协议文档 §4）
├── src/login.rs                 # 扫码登录（协议文档 §6）
├── src/state.rs                 # 游标/令牌/凭据持久化
└── src/ratelimit.rs             # 限速器（协议文档 §9）
```

### 3.2 职责边界（谁负责什么，越界即错）

| 模块 | 负责 | 明确不负责 |
|---|---|---|
| adapter（微信） | 协议编解码、登录、长轮询、回环过滤、游标推进时机、限速、4000 分段、**peer→context_token 协议映射自管**（随入站刷新，约 1h 有效） | 业务会话映射（peer↔SessionId）、任务排队、交付 |
| ChannelHost | 入/出站有界通道、emit/invoke 桥接、**adapter 监督（退避重启 / 超限降级）**、DLQ 上报、shutdown 落盘、`im.sent` 观测通知 | 业务决策 |
| BatchAccumulator | 8s 静默/条数/总窗三条件闭合、合并文本 | 意图理解（修正是模型的事） |
| 调度器 | 会话道 FIFO、全局并发上限、回信分类处理（含超时） | 回复内容 |
| ImRouter | **peer↔SessionId 会话映射（惰性创建，与工具共享）**、串联以上、交付契约执行、中断关键字 | 通道细节 |
| im_send_text 工具 | 中间回执/主动汇报的发送、参数校验、metadata 携带 session/turn、经 `ToolContext.kernel` 调用 host | **最终结果交付（兜底管道负责）**、令牌/限速（host/adapter 管） |

两种"映射"必须区分（实现时不得混淆）：
- **协议映射**（adapter 内部）：peer → context_token，协议层会话句柄，协议文档 §5 已有现成模式；
  `OutboundCommand` 因此**不携带** session_ctx。
- **业务映射**（ImRouter）：PeerKey ↔ SessionId，决定哪条 IM 消息进哪个智能体会话；
  `Arc<DashMap<SessionId, PeerKey>>` 惰性创建（每 peer 一个 `Uuid::new_v4` 会话），
  构造时与 `ImSendText` 工具共享同一 Arc。Phase 1 为进程内状态，随重启重建
  （配合游标不重放，不会重复派发；持久化见 §9 Phase 2）。

## 4. 接口规范

### 4.1 统一消息模型（`message.rs`，纯数据）

```rust
/// 会话对方键 = (通道账号, 对端)。批次/调度/会话映射统一用它，避免同 peer 跨账号混淆
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerKey {
    pub endpoint: String,
    pub peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub enum ChannelContent {
    Text(String),
    /// Phase 2：媒体引用（CDN ref + 可选 AES 密钥）
    Media { media_kind: String, cdn_ref: String, aes_key: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundMessage {
    pub endpoint: String,        // 通道账号标识，如 "wechat/<ilink_bot_id>"
    pub peer: String,            // 对端用户标识
    pub message_id: String,
    pub content: ChannelContent,
    /// 通道级会话句柄（微信 = context_token），对上层 opaque，由 adapter 保管
    pub session_ctx: String,
    pub occurred_at: i64,        // 毫秒时间戳
    pub raw: Option<serde_json::Value>, // 通道特有字段逃生口
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundCommand {
    pub endpoint: String,
    pub peer: String,
    pub content: ChannelContent,
    // 注意：不带 session_ctx —— adapter 用自己的 peer→context_token 映射补齐协议字段
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendReceipt {          // im.send 的 invoke 回信
    pub accepted: bool,
    pub queue_depth: usize,       // 观测用
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentNotice {           // im.sent 事件载荷 —— 仅观测归因/指标，不参与交付判断
    pub endpoint: String,
    pub peer: String,
    pub session_id: Uuid,
    pub turn_id: u64,             // 来自工具写入的 metadata，作 tracing/指标维度
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelCapabilities {
    pub max_text_len: usize,      // 单条文本上限（adapter 声明，如微信 4000）
    pub batch_idle_window_ms: u64,// 批次静默闭合窗口（adapter 声明，如微信 8000）
    pub max_batch_messages: usize,// 批次条数上限（adapter 声明，如 10）
    pub max_batch_window_ms: u64, // 批次总窗上限（adapter 声明，如 30000）
}
```

### 4.2 适配器契约（`adapter.rs`）

```rust
/// 通道适配器 — 每通道一个实现，传输方式无关（长轮询/webhook/websocket 均可）
#[async_trait]
pub trait ChannelAdapter: Send + Sync {   // Sync 必须：run(&self) 的 future 跨 await 需保持 Send
    fn kind(&self) -> &'static str;             // "wechat" / "feishu" / "qq"
    fn capabilities(&self) -> ChannelCapabilities;

    /// 长期运行：登录/重连 + 收发循环，直到 shutdown 置位。
    /// 义务：
    /// - 入站消息 send 进 inbound_tx 成功后，才允许推进并持久化游标（背压点）
    /// - 出站命令从 outbound_rx 逐条取件、限速后落线；受理即回（线级失败自行重试 ≤3 次退避）
    /// - 自维护 peer→context_token 映射（随入站消息刷新）
    /// - panic 只允许发生在本方法内（host 经 JoinHandle 监督，见 §4.6）
    async fn run(&self, io: ChannelIo) -> Result<(), AdapterError>;
}

pub struct ChannelIo {
    pub inbound_tx: mpsc::Sender<InboundMessage>,      // 有界
    pub outbound_rx: mpsc::Receiver<OutboundCommand>,  // 有界
    pub shutdown: watch::Receiver<bool>,
    pub state: Box<dyn AdapterState>, // 游标/令牌持久化（trait：load/save/flush）
}
```

### 4.3 Envelope 编解码约定（`message.rs`）

| kind（metadata） | 方向 | 原语 | 载荷 |
|---|---|---|---|
| `im.inbound` | host → router | emit | `InboundMessage` |
| `im.send` | 任意 → host | invoke | `OutboundCommand`（metadata 附 `session_id`/`turn_id`）；回信 kind 为 `im.receipt` |
| `im.receipt` | host → 调用方 | invoke 回信 | `SendReceipt` |
| `im.sent` | host → router | emit | `SentNotice` |
| `im.system` | router → host | invoke | `OutboundCommand`（系统提示文本，无回合归因） |
| `im.delivery` | host → 日志 | emit | 线级投递结果（Phase 2 路由回会话） |

**载荷位置约定**：通道层消息统一走 `Envelope.payload`（JSON → `Bytes`）+ `metadata["kind"]`
区分类型。注意现有会话协议（`SessionMessage`/`SessionReply`）走的是 `metadata["_msg"]`/
`["_reply"]` 惯例——通道层**自成约定、不与混用**；`payload` 经内核路由直通，两种约定互不干扰。

编解码函数签名统一为 `to_envelope() / from_envelope(&Envelope)`，解码失败回 `ChannelError::Decode`
并丢弃（带 warn 日志），绝不让畸形消息打断循环。

### 4.4 错误分类（`error.rs`）

```rust
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    Decode(String),          // 载荷编解码失败（丢弃 + 日志）
    Rejected,                // 队列满，显式拒绝（对应 ResourceExhausted 语义）
    TargetUnreachable,       // 路由目标不可达（对齐内核错误）
    TokenExpired,            // 通道会话句柄失效（微信 errcode=-14）
    Adapter(String),         // 适配器内部错误（重试后仍失败 / adapter 已降级）
}
```

### 4.5 组装（main / referee-aura 层）

```rust
let adapter = WechatAdapter::new(config);              // 协议文档 §4–§10
let caps = adapter.capabilities();                     // 移动前先取（批次参数来源）
let host = ChannelHost::new(adapter);                  // 内部 Arc，可 Clone；id 构造时生成（Uuid v4）
let host_id = host.id();

let router = ImRouter::new(kernel.clone(), ImRouterConfig {
    host: host_id,
    agent: agent_runtime_id,
    batch: BatchConfig::from(caps),
    concurrency: 3,               // 全局并发上限
    task_queue: 64,               // 任务队列容量
    chat_timeout_ms: 600_000,     // 回合 invoke 超时（长任务，分钟级；AgentTool 的 30s 先例不适用）
    send_timeout_ms: 5_000,       // im.send / im.system invoke 超时
    interrupt_keywords: vec![],   // 默认空 = 全部排队
});
let router_id = router.id();
let session_map = router.session_map().clone();        // 注册前取出（与工具共享）

// 注册顺序即依赖顺序：router 先就位，host 的 im.inbound 才不会因目标不存在而落 DLQ。
// router_id 经 start() 注入，避免构造期互相持 id 的循环依赖。
kernel.register(Box::new(router), 16, SupervisionPolicy::Transient).await?;
kernel.register(Box::new(host.clone()), 16, SupervisionPolicy::Transient).await?;
host.start(kernel.clone(), router_id);                 // 注册完成后才启动 adapter 收发循环

agent.register_tool(Arc::new(ImSendText::new(
    kernel.clone(),          // ToolContext.kernel 之外的自持句柄（同 AgentTool 模式）
    host_id,
    session_map,             // session → PeerKey 反查来源
)))?;
```

### 4.6 关键机制（本文档的核心不变量，实现与评审据此对齐）

**a) 回合终态与交付契约**（`policy.rs` + `dispatch.rs`）

不存在 TurnFinished 事件；回合终态 = 调度 worker 收到 `invoke(AgentRuntime, Chat)` 的回信。
`SessionReply` 分类处理如下（**穷尽且互斥**，实现处用 match 不留 `_ =>` 通配）：

| 回信 | 处理 |
|---|---|
| `Success`（message 文本非空） | 兜底 `im.send` 最终输出（取 `message` 的文本部分；多模态部分 Phase 1 忽略并记日志） |
| `Success`（文本为空） | 跳过兜底，info 日志（回合全靠工具交付或确无输出） |
| `Busy` | 任务回队尾重试，≤2 次；仍 Busy → `im.system` 提示用户"忙，请稍后再问" |
| `Error` | `im.system` 提示用户任务失败（附错误摘要） |
| `Cancelled` | 不兜底（用户主动中断），释放会话道 |
| `Unhandled` | warn 日志 + `im.system` 提示（协议层异常，不应出现） |
| invoke 超时（`KernelError::Timeout`） | 按 `Error` 处理并释放会话道；回合可能仍在运行，后续同会话任务的 `Busy` 由回队机制兜住 |

**为什么兜底不设"已发送则抑制"（sent-flag）判断**：抑制条件"本回合发生过工具发送"无法区分
"模型已交付结果"与"模型只发了中间回执"——后者会把兜底抑制掉，用户永远收不到结果（丢失比
重复严重得多）。因此交付出口唯一化：结果只从兜底管道走（确定性），工具只发中间内容（显式性）。
重复风险只剩"模型违规用工具发最终结果"一种，由工具 description 约束（见 c），列入 §8 风险表。
`im.sent` 事件保留但仅作观测归因，不参与任何控制流判断——也因此 `im.sent`（emit 路径）与
回信（invoke oneshot 路径）之间**不存在顺序依赖**，无竞态面。

**b) adapter 监督与降级**（`host.rs`）

内核 `catch_unwind` 只覆盖 `Extension::handle`；`adapter.run` 跑在 host 自己 spawn 的后台
任务里，panic 表现为 `JoinError`。host 由此自带监督循环：

- `JoinError::is_panic` 或 `run` 返回 `Err` → 退避重启 adapter（100ms × 2ⁿ，上限 3 次，
  复用内核 OneForOne 的节奏）；
- 重启耗尽 → host 进入 **degraded**：停止收发循环，`im.send` / `im.system` 的 invoke 回
  `SendReceipt{accepted:false}` + `ChannelError::Adapter`（编码进回信 metadata），ERROR 日志
  + 计数器告警；host 扩展本身保持注册（内核治理状态不变），修复凭据后可通过重启进程恢复；
- host 的 `handle` 本身只做 `try_send` / emit / reply，天然非阻塞、不会 panic。

**c) im_send_text 工具语义**（`tools.rs`）

- 参数仅 `text`；peer 由 `ctx.session_id` 经共享 `session_map` 反查（**不暴露给模型**，
  模型无法选择收件人——安全边界）；
- `description` 必须明确："仅用于向用户发送**中间进展 / 回执 / 主动汇报**；最终答案直接作为
  回复输出返回，由系统自动送达，**不要**用本工具发送最终答案"；
- invoke 超时 `send_timeout_ms`（秒级）；metadata 写入 `session_id`/`turn_id` 供 `im.sent`
  归因。

**d) 后台任务与 Kernel 句柄**（全模块通则）

`handle` 内只有 `try_send` / emit / reply（非阻塞）；所有等待型工作（adapter.run、入站搬运、
调度 worker、批次 sweeper）都在扩展构造 / `start()` 时经 `tokio::spawn` 起的后台任务里，
持有 Clone 的完整 `Kernel` 句柄调用 `emit` / `invoke`。这与 `KernelContext` 不提供 `invoke`
的设计一致：嵌套禁令约束的是 `handle` 栈，不约束后台任务（AgentTool / AgentRuntime 的
`tokio::spawn + handle.wait()` 即同模式）。

## 5. 执行步骤

### 阶段 0：骨架（半天）

1. 新建 `referee-channel`、`referee-channel-wechat` 两个 crate，加入 workspace members。
2. `referee-channel` 只依赖：referee-core、referee-ai（Tool trait）、serde、serde_json、uuid、
   thiserror、async-trait、tokio、tracing、futures、dashmap；
   `referee-channel-wechat` 额外：reqwest、rand、base64。
3. 更新 AGENTS.md 依赖白名单：新增两个 crate 的清单（`rand`/`base64` 为全新条目；
   `reqwest` 适用范围从"referee-ai 专用"扩展至 `referee-channel-wechat`）。
4. 微信侧先把协议文档 §4/§5/§9 的代码落进 `types.rs`/`client.rs`/`ratelimit.rs`。

### 阶段 1：消息模型与编解码（半天）

1. 实现 `message.rs` 全部类型（含 `PeerKey`）+ `to_envelope / from_envelope`。
2. 单元测试：serde 往返（含 `raw` 逃生口、`Media` 变体）、kind 写入 metadata、payload 为
   合法 JSON Bytes、未知字段容忍（`extra` flatten）、畸形载荷返回 `Decode`。

### 阶段 2：ChannelHost + Mock 适配器（1 天）

1. 实现 `adapter.rs`（trait / ChannelIo / AdapterState）与 `host.rs`：
   - `handle(im.send)` → 出站队列 `try_send`，满则回 `SendReceipt{accepted:false}`（Rejected
     语义，**不挂起**）；受理后 `emit(im.sent)`（先 emit 后 reply，保证观测事件不晚于受理）；
   - `start(kernel, router_id)`：spawn adapter.run 监督任务（§4.6b）+ 入站搬运循环
     （`emit(im.inbound)` 失败由内核落 DLQ，host 记 warn + 计数）；
   - `shutdown()`（Extension 钩子）：watch 置位 → 2s 超时 join 后台任务 →
     `AdapterState::flush`（幂等）。
2. 写 `MockAdapter`（可注入：出消息序列、模拟 panic、模拟慢消费、游标推进钩子）。
3. 测试见验收标准 A2。

### 阶段 3：微信适配器（1–2 天，含真机验证）

1. `login.rs`（扫码，渲染 `qrcode_img_content`）+ `state.rs`（游标/凭据持久化）。
2. `WechatAdapter::run`：长轮询循环——回环过滤（`message_type != 1` 丢弃，协议文档 §3/§12-5）
   → inbound_tx send 成功后 save 游标；出站循环——限速（12s+4s 抖动）→ `send_text`
   （4000 字符分段）→ `errcode=-14` 映射 `TokenExpired`；维护 peer→context_token 映射。
3. 真机验证见验收标准 A3。

### 阶段 4：ImRouter（1–2 天）

1. `batch.rs`：`dashmap<PeerKey, PendingBatch>` + 500ms sweeper；三条件闭合
   （8s 静默 / 条数上限 / 总窗上限）；shutdown flush。
2. `dispatch.rs`：有界任务队列（满 → `im.system` 拒绝提示，原任务不 enqueue）+ 会话道 +
   全局信号量。会话道实现为 `dashmap<PeerKey, Mutex<LaneState{pending: VecDeque, running: bool}>>`
   + `Notify`：任务出全局队列时若本会话在跑则挂入 pending，回合终态后从 pending 头取下一个
   ——无 head-of-line 阻塞、无无界 spawn。
3. `router.rs` + `policy.rs`：会话映射（惰性创建 + 与工具共享）；worker 持 Kernel 句柄
   `invoke(AgentRuntime, Chat)`，回信按 §4.6a 契约穷尽处理；中断关键字在批次闭合后对合并
   文本匹配 → 该会话在跑则 `invoke(Interrupt)`（收到 `Cancelled` 即释放会话道），未跑则丢弃该批。
4. 测试见验收标准 A4。

### 阶段 5：工具与端到端（1 天）

1. `tools.rs`：`ImSendText`（§4.6c 全部约束）。
2. 端到端联调（验收标准 A5）。

## 6. 验收标准

> 约定：自动化测试全部进各 crate 的 `tests/`（命名沿用 `xxx_test.rs`）；涉及内核语义的
> 用例复用 `referee-core` 测试的搭建方式。真机项标注【真机】。

**A0 骨架**
- [x] `cargo check --workspace` 与 `cargo test --workspace` 全绿。
- [x] 两个新 crate 依赖清单与 AGENTS.md 一致（无白名单外依赖；无 `anyhow`）。

**A1 消息模型**
- [x] 往返测试：每种类型 encode→decode 相等；`raw` 逃生口字段不丢；`PeerKey` Hash/Eq 成立。
- [x] envelope 测试：`metadata["kind"]` 正确、payload 为合法 JSON Bytes、与
  `metadata["_msg"]` 会话惯例互不干扰。
- [x] 容错测试：截断/错型载荷返回 `ChannelError::Decode`，进程不崩。

**A2 ChannelHost（MockAdapter）**
- [x] invoke `im.send` → mock 出站收到命令、回信 `SendReceipt{accepted:true}`。
- [x] 出站队列满（容量 1 + 慢消费）→ invoke 返回 `accepted:false`，**不挂起**。
- [x] **adapter 监督**：mock 在 `run` 中 panic → host 自动退避重启（重启计数可断言）；
  连续 panic 超限（3 次）→ host 进入 degraded：`im.send` 回 `accepted:false` + Adapter 错误，
  ERROR 日志可见；**内核与另一注册扩展治理状态不变、正常收发**（对齐 isolation_test 断言方式）。
- [x] 入站反压：入站通道满 → mock 的游标推进钩子未被调用（游标停滞），通道腾空后恢复推进。
- [x] `im.inbound` emit 目标不存在 → 消息出现在 DLQ，host 循环继续。
- [x] `shutdown()` → `run` 在 2 秒内退出，`AdapterState::flush` 被调用且幂等（连调两次不重写）。
- [x] `im.send` 受理后 router 收到 `im.sent`（含正确 session/turn）；host 侧 emit 先于回信
  发出（顺序可在 mock 断言）。

**A3 微信适配器**
- [x] 单元：`split_for_wechat`（中文 4000 边界）、`channel_version_u32`（2.4.6 → 33816582）、
  限速器间隔分布 ≥12s。
- [x] 【真机】扫码登录凭据落盘；重启进程免扫码复用。
- [x] 【真机】手机发消息 → host 侧收到 `InboundMessage`（文本正确、peer 非空、
  `session_ctx` 非空）；自己发的消息（message_type=2）被过滤。
- [ ] 【真机】两台设备对发验证游标：重启进程 → 已收消息不重放、新消息正常收。
- [ ] 【真机】发送 errcode≠0 / 伪 token 场景映射 `TokenExpired`/`Adapter`，错误可见于日志。

**A4 ImRouter**
- [x] 批次（tokio `start_paused` 时间操纵）：间隔 ≤8s 的两条合并为一个 Task；第 8.1s 闭合；
  静默计时随每条重置；11 条立即闭合；总窗 30s 到期闭合。
- [x] 队列满：第 65 个任务触发 `im.system` 拒绝提示（mock host 断言收到一条），原任务不 enqueue。
- [x] 会话道：同 peer 两个任务串行（第二个在第一个回信之后才 invoke）；不同 peer 两任务并行；
  并发 4 任务 + 信号量 3 → 同时运行 ≤3。
- [x] **交付契约**：mock agent 回 `Success`（非空）→ 兜底 `im.send` 恰好一次、内容为最终输出；
  回合内先经工具发过中间回执 → 回执与最终输出**各恰好一次**（不抑制、不重复）；
  `Success`（空文本）→ 无兜底发送；`Error` → `im.system` 一条；`Busy` → 回队后二次派发成功；
  `Cancelled` → 不兜底。
- [x] 中断关键字命中时任务未派发 → 丢弃该批；已运行 → 转发 `Interrupt` 并收到 `Cancelled`，
  会话道释放。
- [x] 回合 invoke 超时（mock agent 挂起 + `chat_timeout_ms` 调小）→ 按 `Error` 处理，
  会话道释放，worker 不泄漏。

**A5 端到端（【真机】全链路）**

> 自动化前置已全绿：`im_send_text` 工具单测（收件人反查 / session+turn 归因 /
> 未知会话与空文本拒绝 / 未受理回执报错，`tests/tools_test.rs`）。
> 真机乎架：`cargo run -p referee-channel-wechat --example agent`（DEEPSEEK_API_KEY=…，
> 全栈组装含批次/调度/工具/兜底交付）。

- [x] 发「帮我看看股票情况」+ 5s 后「要今天的」→ ≤10s 内收到工具回执（模型行为，验证的是
  工具路径时延）→ 回合结束后收到兜底交付的结果 → 两者各恰好一次。
- [x] 随后发「看看今天的天气如何」→ 独立批次、独立任务、正常回复。
- [x] 连续布置 5 个任务 → 依次完成（会话道串行），出站间隔 ≥12s。
- [x] kill 进程 → 重启（WAL 启用配置）→ **游标不重放旧消息**；仍在内核 mailbox 的
  `im.inbound` 经 WAL 重投后正常处理；已在扩展内部（批次/任务队列）的任务**允许丢失**
  （Phase 1 明确接受，Phase 2 任务日志补齐，见 §8/§9）；新消息正常收发。
- [x] 全程 tracing 无 ERROR 级别以下静默丢弃；DLQ 为空。

## 7. 依赖与约束对齐

| 约束 | 落实方式 |
|---|---|
| 内核不承载业务 | 通道层为独立 crate，经 Extension 协议接入；referee-agent 只依赖 `referee-channel` |
| 有界通道 | 入站/出站/任务队列容量全部显式配置（§4.5），满 = 显式拒绝或游标反压 |
| Panic 隔离 | 内核 catch_unwind 覆盖 `handle`（host/router 的 handle 只做非阻塞操作）；adapter panic 由 host 监督熔断（§4.6b），验收 A2-3 |
| handle 非阻塞 | host/router 的 `handle` 只做 try_send / emit / reply；等待型工作全在后台任务（§4.6d） |
| 嵌套 invoke 禁令 | `KernelContext` 无 invoke（编译期禁）；工具在引擎回合内、调度 worker 在后台任务里经完整 `Kernel` 句柄 invoke——均不在 `handle` 栈内，与 AgentTool 同模式 |
| WAL 边界 | 内核 WAL 只覆盖经路由 envelope（至少一次重投）；扩展内部状态不覆盖，崩溃语义见 A5 |
| 交付确定性 | 最终结果只从兜底管道交付（§4.6a），不受模型行为影响；工具只发中间内容 |

## 8. 已知风险与待验证项

| 风险 | 影响 | 缓解 |
|---|---|---|
| iLink 协议随时变更（内部协议） | 收发中断 | 协议层集中在 wechat crate 三个文件；订阅官方仓库 release；`CHANNEL_VERSION` 常量一处改 |
| 模型违规用 `im_send_text` 发送最终答案 | 与兜底交付重复，用户收到两次 | 工具 description 硬约束（§4.6c）；`im.sent` 观测可离线发现该模式；Phase 2 可加内容指纹去重。注意：重复远轻于丢失，故不回退到抑制式设计 |
| `context_token` ~1h 过期 × 长任务 | 兜底发送失败（TokenExpired），结果发不出 | 兜底失败时 ERROR 日志 + `im.sent`/指标可见；Phase 2 补投队列：失败挂起，下次入站消息触发补投 |
| 兜底受理 ≠ 投递（隐藏出站字段缺失被静默丢弃） | 用户实际未收到 | A3 真机对发验证；`im.delivery`（Phase 2）补线级回执；异常时按协议文档 §12 清单逐项排查 |
| 流式 `message_state` 更新规则未验证 | Phase 2/3"逐字输出"不可用 | 先 `FinalOnly`（已验证路径）；且流式需 AgentRuntime 新增 emit 事件（协议扩展，§9）；抓包后再开 `InPlaceUpdate` |
| 8s 批次引入固定时延 | 响应慢一拍 | 参数可调（`ChannelCapabilities`）；后续可加"句号/问号即闭合"启发式 |
| kill 进程丢扩展内任务 | 已受理任务无回复 | Phase 1 明确接受（A5）；Phase 2 任务日志（闭合批次 append 后入队，启动重放）与补投队列一起补 |

## 9. 路线图（Phase 1 之后，另行细化验收）

- **Phase 2**：任务日志与重放（崩溃恢复）、补投队列（context_token 过期兜底）、会话映射
  持久化、typing、媒体上传（协议文档 §7）、多账号、`im.delivery` 路由回会话、
  `im_task_status` 只读工具。
- **Phase 3**：流式中继——**前置条件是 AgentRuntime 侧新增回合内 emit 事件类型（协议扩展，
  现有 `SessionReply` 无法承载流）**；ReplyPolicy 开关 + Chunked/InPlaceUpdate 渲染档；
  飞书 / QQ adapter（webhook ingress 挂 referee-aura）；每通道运维指标
  （队列水位 / 限速丢弃 / 重连次数 / 兜底交付成功率）。

## 修订记录

| 日期 | 说明 |
|---|---|
| 2026-08-22 | 初版：四轮设计讨论收敛（通道抽象 / 回复管道 / 任务队列 / 批次累积器），含 Phase 1 执行步骤与 A0–A5 验收标准 |
| 2026-08-23 | 对照代码全面修订：① 移除不存在的 TurnFinished 事件，明确"invoke 回信即回合终态"；② 交付契约反转——兜底管道确定交付最终结果、工具仅中间回执（消除 sent-flag 抑制导致的结果丢失与 emit/invoke 乱序竞态）；③ adapter panic 治理改为 host 内监督（内核 catch_unwind 只覆盖 handle）；④ 明确 WAL 只覆盖经路由 envelope，Phase 1 接受扩展内任务丢失（A5 修正）；⑤ 补齐 ImRouter Kernel 句柄注入、peer↔session 映射归属、chat/send 超时策略、Busy/Error/Timeout 回信分支；⑥ 依赖修正（去 anyhow，统一 thiserror；reqwest 适用范围扩展）；⑦ 新增 §4.6 关键机制与代码事实来源；⑧ 阶段 1 实现补充：im.send 受理回信定为独立 kind `im.receipt`；⑨ 阶段 3 实现补充：`ChannelIo` 去掉 state 字段（adapter 经 `self.state()` 自持）；`WechatConfig` 预设（serde 可配）；登录二维码 feature `qr`（默认关闭，`qrcode` 白名单按需）；`examples/echo.rs` 为集成范式与 A3 真机乎架；适配器新增本地 mock iLink 服务端单测（回环过滤/游标落盘时机/令牌使用/过期容错）；⑩ 阶段 4 实现补充：即时闭合与 sweeper 到期闭合统一经有界任务队列受理（满即拒绝，有界保证对全部闭合来源生效）；内核事实补充——扩展 handle 完成但不回信时回信通道随 ctx 丢弃、invoke 立即返回 `TargetUnreachable`（而非等待超时），"挂起"语义须由持有 ctx 的后台任务承担；`to_send_envelope` 的 turn_id 改为 `Option`（兜底路径无 turn 归因）；⑪ 阶段 5 完成：`ImSendText` 工具落地（收件人反查不暴露给模型、session+turn 归因、拒绝回执显式报错）；referee-agent 补 `AgentRuntime::register_tool`（通用工具注册，§4.5 组装文档假设的 API）；`examples/agent.rs` 为 A5 真机乎架；各 crate 落地 AGENTS.md |
| 2026-08-23 | A5 端到端真机验收通过（`examples/agent`，DeepSeek 实机链路），§6 A5 五个子项全部补勾；A3 两项真机项（两台设备对发验证游标 / `errcode≠0` 与伪 token 映射）仍待验 |

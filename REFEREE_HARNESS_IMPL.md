# Referee Harness — Phase 1 执行实现规划（referee-harness 核心）

> 本文档是 [REFEREE_HARNESS_PLAN.md](REFEREE_HARNESS_PLAN.md) 的 Phase 1 落地细化，
> **只回答"每个文件怎么落地"**，不重复架构与决策（见 PLAN §2 决策记录）。

## 1. 范围与职责

| 项 | 归属 | 说明 |
|---|---|---|
| 新建 crate | `referee-harness`（workspace 第 4 个成员） | 依赖 agent/base/core，职责=入口/宿主层 |
| 落地差距 | G1 / G2 / G3 / G4 / G6 / G7 / G8 | G9 留 P2、G5 留 P3 |
| 配套改动 | `referee-ai-base` 新增 `persist` feature | 会话落盘 sink（默认关，零依赖） |
| 硬约束 | 零新增依赖；不吞异常；背压有界 | 对齐项目内核哲学 |

## 2. 文件结构

```
referee-harness/
├── Cargo.toml
├── src/
│   ├── lib.rs                # 库入口：重导出 protocol / instance / persist / transport
│   ├── protocol.rs           # §4 协议类型（serde 载荷，与传输解耦）
│   ├── instance.rs           # §5 Instance + InstanceManager
│   ├── persist.rs            # §6 JSONL 持久化 + 崩溃恢复
│   ├── transport.rs          # §7 TCP JSON-RPC 2.0（feature "tcp"）
│   └── bin/
│       └── referee-harness.rs # §8 daemon 二进制入口
├── referee-ai-base/（配套改动）
│   └── src/session/log.rs    # §9 SessionLogSink + PersistedSessionLog（feature "persist"）
└── tests/
    └── harness_test.rs        # §10 集成测试
```

模块职责边界：
- `protocol`：纯数据（serde 类型 + 错误码），零逻辑。
- `instance`：实例生命周期 + 多实例有界管理 + 请求路由（transport-agnostic）。
- `persist`：文件 IO + 崩溃恢复（依赖 protocol 的 InstanceSpec）。
- `transport`：网络 IO + JSON-RPC 编解码（仅调用 instance / persist）。
- `bin`：参数解析 + 装配（拉起 manager / persist / transport）。

## 3. Cargo.toml 与 feature 设计

```toml
[package]
name = "referee-harness"
version = "0.1.0"
edition = "2021"
description = "Referee Harness — 智能体入口/宿主层：常驻 daemon，多实例并行管理 + 崩溃恢复 + TCP JSON-RPC 2.0"

[features]
default = ["tcp", "deepseek"]
# 传输层
tcp        = []       # TCP JSON-RPC 2.0 over NDJSON（默认开）
# 厂商适配器（转发到 base）
deepseek   = ["referee-agent/deepseek"]
xiaomi     = ["referee-agent/xiaomi"]
openai     = ["referee-agent/openai"]

[dependencies]
referee-core    = { path = "../referee-core" }
referee-ai-base = { path = "../referee-ai-base", features = ["persist"] }
referee-agent   = { path = "../referee-agent" }
tokio           = { version = "1", features = ["full"] }
serde           = { version = "1", features = ["derive"] }
serde_json      = "1"
thiserror       = "1"
tracing         = "0.1"
dashmap         = "6"
uuid            = { version = "1", features = ["v4", "serde"] }
async-trait     = "0.1"
futures         = "0.3"
bytes           = "1"

[dev-dependencies]
tracing-subscriber = "0.3"
```

**base 配套改动**：`referee-ai-base/Cargo.toml` 新增 `persist = []`（默认关，零依赖）。

## 4. protocol.rs — serde 协议类型

**职责**：与传输解耦的纯数据载体；所有类型 `derive(Serialize, Deserialize)`。

```rust
// ── 实例身份 ────────────────────────────────
/// kebab-case 实例标识（与 AgentId 同规则校验）
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstanceId(String);

// ── 实例规格（声明式 JSON，create 时提交）────
/// 实例规格 — 全声明式 JSON，零代码创建实例
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceSpec {
    /// 实例身份（kebab-case；空则自动生成）
    pub id: Option<String>,
    /// Agent 定义（复用 AgentDefinition：model / template / tools / skills / params）
    pub agent: AgentDefinition,
    /// 引擎配置
    #[serde(default)]
    pub engine: EngineConfig,
    /// 模板变量（bind_with 插值，如 {"cwd": "/workspace"}）
    #[serde(default)]
    pub template_vars: HashMap<String, String>,
    /// 工具选配
    #[serde(default)]
    pub tools: InstanceTools,
    /// 厂商配置
    #[serde(default)]
    pub provider: ProviderConfig,
}

/// 实例的工具选配
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceTools {
    /// 启用文件读写编辑工具（read / write / edit）
    pub fs: Option<FsToolConfig>,
    /// 启用成果板工具（list_my_board / read_artifact）
    pub artifact: bool,
}

/// 文件工具配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsToolConfig {
    pub root: Option<String>,       // 根目录约束
    pub max_file_bytes: u64,        // 单文件读取上限
    pub default_limit_chars: usize, // 默认窗口字符数
}

impl Default for FsToolConfig {
    fn default() -> Self {
        Self { root: None, max_file_bytes: 1_048_576, default_limit_chars: 3000 }
    }
}

/// 厂商配置（运行时决定，不硬编码 feature）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProviderConfig {
    #[serde(rename = "deepseek")]
    DeepSeek { api_key: String, base_url: Option<String>,
               #[serde(default)] model: Option<String> }, // model 覆盖 AgentDefinition.model
    #[serde(rename = "xiaomi")]
    XiaoMi { api_key: String, base_url: Option<String> },
    #[serde(rename = "openai")]
    OpenAI { api_key: String, base_url: Option<String>, model: String },
}

// ── 实例信息（list/get 响应）────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub id: InstanceId,
    pub model: String,
    pub state: InstanceState,
    pub sessions: usize,
    pub max_sessions: usize,
    pub consumed_tokens: u64,
    pub cache_entries: usize,
    pub created_at: String, // ISO 8601
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InstanceState { Running, Stopped }

// ── 对话协议 ────────────────────────────────
/// 单轮对话请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRequest {
    pub session_id: Option<String>, // 空则自动生成
    pub message: String,
    #[serde(default)] pub stream: bool,
    #[serde(default)] pub temperature: Option<f32>,
    #[serde(default)] pub max_tokens: Option<usize>,
}

/// 单轮对话响应（非流式）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatReply {
    pub session_id: String,
    pub content: String,
    pub finish_reason: String,
    pub usage: Option<TokenUsageData>,
}

/// 流式帧（对齐 base StreamChunk + serde）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamFrame {
    #[serde(rename = "delta")]
    Delta { content: Option<String>, reasoning_content: Option<String> },
    #[serde(rename = "finish")]
    Finish { finish_reason: String, usage: Option<TokenUsageData> },
    #[serde(rename = "error")]
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsageData { pub prompt_tokens: u64, pub completion_tokens: u64, pub total_tokens: u64 }

// ── 管理错误 ────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerError { pub code: i32, pub message: String }

pub const ERR_INSTANCE_NOT_FOUND: i32 = -32000;
pub const ERR_INSTANCE_FULL: i32 = -32001;
pub const ERR_SESSION_BUSY: i32 = -32002;
pub const ERR_INTERNAL: i32 = -32003;
pub const ERR_INVALID_SPEC: i32 = -32004;
```

## 5. instance.rs — Instance + InstanceManager

**职责**：实例生命周期（create/chat/interrupt/stop/snapshot）+ 多实例有界管理 + 请求路由。

```rust
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::RwLock;
use referee_ai_base::engine::{ChatHandle, Engine, EngineConfig, EngineReply, EngineStartError};
use referee_ai_base::session::{ChatPayload, SessionId};
use referee_agent::AgentRuntime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstanceStatus { Running, Stopped }

/// 一个可寻址、可治理、相互隔离的智能体实例
pub struct Instance {
    id: InstanceId,
    spec: InstanceSpec,
    runtime: AgentRuntime,
    templates: TemplateRegistry,   // 可替换模板
    system_prompt: String,         // bind 后注入 SessionConfig
    status: RwLock<InstanceStatus>,
    created_at: SystemTime,
    global_budget: Arc<AtomicU64>, // 系统级总预算
}

impl Instance {
    /// 由 spec 构造实例（见下方"create 内部流程"）
    pub fn create(spec: InstanceSpec, id: InstanceId,
                  global_budget: Arc<AtomicU64>) -> Result<Self, ServerError> { ... }

    pub fn chat(&self, session_id: SessionId, payload: ChatPayload)
        -> Result<ChatHandle, EngineStartError> { ... }

    pub fn interrupt(&self, session_id: SessionId) -> bool { ... }

    /// 停止实例：取消全部在飞回合 + 置 Stopped
    pub fn stop(&self) { ... }

    pub fn snapshot(&self) -> InstanceInfo { ... }
}

/// 有界实例管理器
#[derive(Clone)]
pub struct InstanceManager {
    instances: Arc<DashMap<InstanceId, Instance>>,
    config: InstanceManagerConfig,
}

#[derive(Debug, Clone)]
pub struct InstanceManagerConfig {
    pub max_instances: usize,
    pub max_sessions_per_instance: usize,
    pub global_budget_limit: u64, // 0 = 无限制
}

impl InstanceManager {
    pub fn new(config: InstanceManagerConfig) -> Self { ... }

    /// 创建实例（有界：满则 ERR_INSTANCE_FULL）
    pub fn create(&self, spec: InstanceSpec) -> Result<InstanceId, ServerError> { ... }
    pub fn list(&self) -> Vec<InstanceInfo> { ... }
    pub fn get(&self, id: &InstanceId) -> Result<Instance, ServerError> { ... }
    /// 停止并移除实例
    pub fn remove(&self, id: &InstanceId) -> Result<(), ServerError> { ... }
    /// 遍历 id（崩溃恢复用，无锁持跨 await）
    pub fn iter(&self) -> impl Iterator<Item = InstanceId> + '_ { ... }
}
```

**create 内部流程**（G3 配置装载的关键接线）：

```
InstanceSpec
  ├─1. ProviderConfig → Arc<dyn LLMProvider>
  │       match spec.provider:
  │         DeepSeek{api_key, model?, base_url}
  │           → DeepSeekProvider::new(model, cfg)          // feature deepseek
  │         XiaoMi{api_key, base_url}
  │           → XiaoMiProvider::new(cfg)                   // feature xiaomi
  │         OpenAI{api_key, base_url, model}
  │           → OpenAiCompatClient::new(cfg)               // feature openai
  │       （未启用的厂商 feature → ERR_INVALID_SPEC，显式报错不静默）
  ├─2. AgentDefinition → Engine
  │       Engine::new(provider, spec.engine)
  │         .with_tools(ToolRegistry::with_defaults(), ToolExecutor::with_defaults())
  │         .with_global_budget(global_budget)             // G7 系统级总预算
  ├─3. TemplateRegistry（优先 spec 附带，否则内置 with_builtins）
  │       bound = Arc::new(spec.agent).bind_with(Some(&templates), &template_vars)?
  ├─4. 注入系统提示词
  │       spec.engine.session.default_system_prompt = Some(bound 拼接文本)
  ├─5. 注册工具（按 spec.tools，实例隔离关键点）
  │       runtime.register_read_tool(ReadToolConfig{root: spec.tools.fs.root, ...})
  │       runtime.register_fs_write_tools(FsConfig{root: spec.tools.fs.root, ...})
  │       runtime.register_artifact_tools()                // artifact=true 时
  │       # 每个实例的 fs.root 即其工作区根，实例间文件视图互不可见
  └─6. 返回 Instance { runtime, templates, system_prompt, ... }
```

**stop 语义**（G6）：遍历该实例全部会话 `Engine.interrupt`（取消在飞回合）→
状态置 `Stopped` → 实例保留在池中供观测，`remove` 才真正出池并 drop。

## 6. persist.rs — JSONL 持久化 + 崩溃恢复

**职责**：实例规格与会话事实的磁盘读写 + 启动恢复。**职责边界**：不做会话语义，
只做 `Message` 的序列化存取；损坏处理（broken 清单）在此显式完成。

```
持久化目录：
  <state_dir>/
    instances/<id>.json              # InstanceSpec
    sessions/<instance_id>/<session>.jsonl   # 会话事实，一行一条 Message

重启恢复流程：
1. 扫描 instances/*.json → 反序列化 InstanceSpec
2. 对每个 spec 调用 InstanceManager::create 重建实例
3. 扫描 sessions/*.jsonl → 回放 Message 到对应实例会话历史
4. 不可恢复的实例/会话 → broken 清单，打印启动日志，不阻塞启动
5. 启动 TCP 监听，进入就绪
```

```rust
/// 持久化后端
pub struct PersistStore { state_dir: PathBuf }

impl PersistStore {
    pub fn new(state_dir: PathBuf) -> Result<Self, PersistError>; // 创建 instances/ 与 sessions/

    // ── 实例规格 ──
    pub fn save_instance(&self, id: &InstanceId, spec: &InstanceSpec) -> Result<(), PersistError>;
    pub fn remove_instance(&self, id: &InstanceId) -> Result<(), PersistError>;
    pub fn load_instances(&self) -> Result<Vec<(InstanceId, InstanceSpec)>, PersistError>;

    // ── 会话事实 ──
    pub fn append_session_event(&self, instance_id: &InstanceId, session_id: &SessionId,
                                msg: &Message) -> Result<(), PersistError>;
    pub fn load_session_events(&self, instance_id: &InstanceId, session_id: &SessionId)
        -> Result<Vec<Message>, PersistError>;
    pub fn remove_session(&self, instance_id: &InstanceId, session_id: &SessionId)
        -> Result<(), PersistError>;
}

/// 崩溃恢复结果
pub struct RecoveryResult {
    pub recovered_instances: usize,
    pub recovered_sessions: usize,
    pub broken: Vec<BrokenEntry>,
}
pub struct BrokenEntry { pub path: String, pub reason: String }
```

**实现要点**：
- 写入用 `tokio::fs::OpenOptions::new().append(true)` + `AsyncWriteExt::write_all`；
  每条 `Message` 经 `serde_json::to_vec` + `\n` 追加。
- 落盘失败必须**显式返回错误**（不吞异常）；调用方决定重试或上抛。
- fsync 策略（已定）：**周期 flush + 优雅关闭时 flush**；强一致（每次 append fsync）开关留作将来。

## 7. transport.rs — TCP JSON-RPC 2.0 传输

**职责**：NDJSON 逐行编解码 + 连接管理 + 请求分发。**职责边界**：只做 IO 与 JSON-RPC
帧映射，业务判定全部委托 `InstanceManager`。

**协议帧**（换行分隔 JSON，每行一个完整 JSON-RPC 2.0 请求/响应）：
```json
{"jsonrpc":"2.0","id":1,"method":"instance.create","params":{...}}
{"jsonrpc":"2.0","id":1,"result":{"id":"my-agent","state":"Running",...}}
{"jsonrpc":"2.0","id":2,"result":{"type":"delta","content":"Hello"}}
{"jsonrpc":"2.0","id":2,"result":{"type":"finish","finish_reason":"stop","usage":null}}
{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"instance not found"}}
```

**JSON-RPC 方法清单**：

| 方法 | params | 响应 | 说明 |
|---|---|---|---|
| `instance.create` | `InstanceSpec` | `InstanceId` | 创建实例（有界） |
| `instance.list` | `{}` | `Vec<InstanceInfo>` | 列出全部实例 |
| `instance.get` | `{"id":"..."}` | `InstanceInfo` | 单个实例详情 |
| `instance.remove` | `{"id":"..."}` | `{}` | 停止并移除实例 |
| `instance.chat` | `{"id","session_id","message","stream","temperature","max_tokens"}` | `ChatReply` 或 `StreamFrame` 流 | 对话 |
| `instance.interrupt` | `{"id","session_id"}` | `{}` | 中断会话当前回合 |
| `instance.sessions` | `{"id":"..."}` | `Vec<SessionInfo>` | 列出实例会话 |

**传输实现骨架**：
```rust
/// TCP JSON-RPC 2.0 服务器（常驻）
pub async fn serve_tcp(
    bind_addr: SocketAddr,
    instances: InstanceManager,
    persist: Option<PersistStore>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let listener = TcpListener::bind(bind_addr).await?;
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            result = listener.accept() => {
                let (stream, addr) = result?;
                tokio::spawn(handle_connection(stream, addr, instances.clone(), persist.clone()));
            }
        }
    }
}

/// 单连接：逐行读 NDJSON → dispatch → 逐行写响应（流式多帧同 id）
async fn handle_connection(stream: TcpStream, addr: SocketAddr,
                           instances: InstanceManager, persist: Option<PersistStore>) { ... }

/// 请求分发（transport-agnostic 核心，可被 HTTP 传输复用）
async fn dispatch(instances: &InstanceManager, persist: &Option<PersistStore>,
                  request: JsonRpcRequest) -> Vec<JsonRpcResponse> { ... }
```

**连接级背压治理**：
- `max_concurrent_requests = 16`（信号量，每连接独立）
- `max_request_len = 1_048_576`（单行 ≤ 1MB，超限关闭连接）
- 流式 chat 写端保持打开直到 finish/error 或断开

## 8. cli.rs — daemon 二进制入口

**职责**：参数解析 + 装配 + 生命周期（启动/优雅关闭）。不含业务逻辑。

```rust
// referee-harness（daemon）
#[tokio::main]
async fn main() {
    // 参数：--state-dir <dir>（默认 ~/.referee/state）、--bind <addr>（默认 127.0.0.1:7100）
    //       --max-instances <N>（默认 64）、--max-sessions <N>（默认 100）
    // 1. 初始化 InstanceManager
    // 2. 若 state-dir 存在 → 崩溃恢复（打印 recovered_instances / sessions / broken）
    // 3. 启动 TCP 监听（serve_tcp）
    // 4. 信号处理（SIGTERM/SIGINT → 优雅关闭：flush persist + 退出码 0）
}
```

## 9. 配套 base 改动 — 会话落盘 sink

**职责**：在 `referee-ai-base` 提供可插拔的会话事实落盘能力；**默认关闭、零行为变化**。

改动文件：`referee-ai-base/src/session/log.rs`（`#[cfg(feature = "persist")]` 门控）。

```rust
/// 会话事实落盘 sink trait（可插拔，对齐 WalSink append 语义）
#[cfg(feature = "persist")]
#[async_trait]
pub trait SessionLogSink: Send + Sync {
    async fn append(&self, session_id: &SessionId, msg: &Message) -> Result<(), LogError>;
}

/// 带落盘 sink 的会话事实日志
#[cfg(feature = "persist")]
pub struct PersistedSessionLog {
    inner: SessionLog,
    sink: Arc<dyn SessionLogSink>,
    session_id: SessionId,
}

#[cfg(feature = "persist")]
impl PersistedSessionLog {
    pub fn new(max_events: usize, session_id: SessionId, sink: Arc<dyn SessionLogSink>) -> Self { ... }

    /// 追加并落盘；落盘失败显式记录（不吞异常，不阻塞内存会话）
    pub fn append(&mut self, msg: Message) -> Result<usize, LogError> { ... }
}
```

**接线点**：`Session`/`SessionConfig` 增加可选 `Option<Arc<dyn SessionLogSink>>`；
未配置时走原有内存路径。`Instance::create`（§5）在配置 persist 时把 `PersistStore`
适配为 `SessionLogSink` 注入。

## 10. 集成测试（tests/harness_test.rs）

| # | 用例 | 断言 |
|---|---|---|
| 1 | `manager_create_list_get` | create → list 含该实例 → get 信息正确 |
| 2 | `manager_create_duplicate_rejected` | 同 id 二次 create → 错误 |
| 3 | `manager_max_instances_rejected` | max=1 建第二个 → ERR_INSTANCE_FULL |
| 4 | `manager_remove` | remove 后 list 为空、get 返回 NotFound |
| 5 | `instance_chat_roundtrip`（MockProvider） | chat → 得到回复 |
| 6 | `instance_chat_stream`（MockProvider） | 流式 chat → Delta×N + Finish |
| 7 | `instance_interrupt`（MockProvider 延迟） | interrupt → Cancelled |
| 8 | `parallel_instances_independent` | 2 实例并行 chat、互不干扰 |
| 9 | `crash_recovery_roundtrip` | 持久化后新 manager 恢复，实例/会话一致 |
| 10 | `persist_broken_file_does_not_block_start` | 损坏 JSONL → broken 清单 + 正常就绪 |
| 11 | `tcp_transport_roundtrip` | daemon + 客户端 JSON-RPC → 响应 |
| 12 | `tcp_transport_stream` | daemon + 流式 chat → Delta/Finish 帧流 |
| 13 | `graceful_shutdown` | SIGTERM → 优雅退出（码 0） |

## 11. 实现顺序

| 步骤 | 依赖 | 输出 |
|---|---|---|
| Step 1 | — | `referee-harness/Cargo.toml` + `lib.rs` 骨架 |
| Step 2 | — | `protocol.rs` 全部 serde 类型 + 单测 |
| Step 3 | Step 2 | `instance.rs`：Instance + InstanceManager（MockProvider 直连） |
| Step 4 | Step 3 | `persist.rs`：JSONL 持久化 + 恢复 |
| Step 5 | Step 2,4 | base 改动：`SessionLogSink` + `PersistedSessionLog` |
| Step 6 | Step 3,5 | 接线：Instance::create 挂载 persist sink |
| Step 7 | Step 2,3 | `transport.rs`：TCP JSON-RPC 2.0 |
| Step 8 | Step 7 | `cli.rs`：daemon 入口 |
| Step 9 | Step 3-8 | 集成测试（mock + TCP 客户端） |
| Step 10 | Step 9 | 崩溃恢复端到端验收 |

## 12. 验收标准

1. `cargo build -p referee-harness` 通过
2. `cargo test -p referee-harness` 13+ 集成测试通过
3. `cargo test -p referee-ai-base` 既有测试不回归
4. `cargo clippy -p referee-harness --all-targets` 零告警
5. `cargo clippy -p referee-ai-base --all-targets` 零新增告警
6. 手动：`cargo run --bin referee-harness -- --state-dir /tmp/test-state` 启动后，
   用 `nc`/`socat` 发 JSON-RPC 请求得到响应
7. 手动：kill -9 后重启 → 实例恢复、历史对话可查

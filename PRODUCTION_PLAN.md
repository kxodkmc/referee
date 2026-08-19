# Referee 生产就绪优化方案

> 目标：补齐 referee-core / referee-ai / referee-agent / referee-aura 从「工程完成」到「生产可用」的缺口。每个方案给出问题、设计、关键代码片段和执行步骤，严格模块化，可直接实施。

---

## 一、referee-core（微内核）

### 1.1 Extension 生命周期钩子（Graceful Shutdown）

**问题**：当前 `Extension::handle` 没有 `shutdown` 钩子。daemon 收到 SIGTERM 时，`Kernel::shutdown` 只关闭通道，不通知 Extension 清理资源（如 MCP 子进程、文件句柄）。`McpServer::shutdown` 已存在但需手动调用——如果忘了，子进程变成孤儿。

**位置**：`referee-core/src/extension.rs`（Extension trait）

**设计**：在 Extension trait 加一个默认空实现的 `shutdown` 方法，Kernel 停机时依次调用所有已注册 Extension 的 shutdown。

```rust
// referee-core/src/extension.rs

#[async_trait::async_trait]
pub trait Extension: Send + Sync {
    // ... 既有方法不变 ...

    /// 停机钩子 — Kernel shutdown 时依次调用，Extension 清理资源
    ///（子进程、文件句柄、flush 缓冲）。默认空实现，不强制覆盖。
    ///
    /// 约束：实现必须非阻塞（≤5s），超时由 Kernel 强制丢弃。
    async fn shutdown(&self) {}
}
```

```rust
// referee-core/src/kernel.rs — shutdown 路径增加 Extension 清理

impl Kernel {
    pub async fn shutdown(&self) {
        // 1. 停止接收新消息（既有）
        self.shutdown_tx.send(true);

        // 2. 依次调用 Extension shutdown（带超时）
        for entry in self.extensions.iter() {
            let ext = entry.value();
            // 5s 超时，超时则 warn 并跳过
            match tokio::time::timeout(
                Duration::from_secs(5),
                ext.shutdown(),
            ).await {
                Ok(()) => {}
                Err(_) => tracing::warn!(
                    ext_id = %entry.key(),
                    "extension shutdown timed out, skipping"
                ),
            }
        }

        // 3. 排空 backlog（既有）
        // ...
    }
}
```

**AgentRuntime 侧实现**：

```rust
// referee-agent/src/agent/mod.rs

#[async_trait::async_trait]
impl Extension for AgentRuntime {
    // ... 既有 handle 不变 ...

    async fn shutdown(&self) {
        // 关闭所有 MCP 子进程
        for tool in self.tool_registry.all() {
            if let Some(mcp) = tool.as_any().downcast_ref::<McpServer>() {
                mcp.shutdown().await;
            }
        }
    }
}
```

**执行步骤**：
1. `Extension` trait 加 `shutdown` 默认方法（零破坏性）
2. `Kernel::shutdown` 增加逐个调用逻辑（带 5s 超时）
3. `AgentRuntime` 实现 `shutdown` 清理 MCP 子进程
4. 测试：注册一个带 shutdown 标志的 mock Extension，调用 `kernel.shutdown()`，断言 shutdown 被调用

---

### 1.2 Extension 注册时返回 Handle（资源回收）

**问题**：`Kernel::register` 当前返回 `()`，调用方无法在运行时移除 Extension。MCP 服务器动态卸载、热重载 Agent 定义等场景下，旧 Extension 无法被清理。

**位置**：`referee-core/src/kernel.rs`

```rust
// referee-core/src/kernel.rs

/// Extension 注册句柄 — 持有此句柄可移除 Extension
pub struct ExtensionHandle {
    ext_id: ExtId,
    kernel: Kernel,
}

impl ExtensionHandle {
    /// 移除并停机该 Extension
    pub async fn remove(&self) {
        if let Some(ext) = self.kernel.extensions.remove(&self.ext_id) {
            ext.shutdown().await;
        }
    }
}

impl Kernel {
    /// 注册 Extension，返回可回收句柄
    pub fn register(&self, ext_id: impl Into<ExtId>, ext: Arc<dyn Extension>) -> ExtensionHandle {
        let id = ext_id.into();
        self.extensions.insert(id.clone(), ext);
        ExtensionHandle {
            ext_id: id,
            kernel: self.clone(),
        }
    }
}
```

**执行步骤**：
1. 新增 `ExtensionHandle` 类型
2. `register` 返回值从 `()` 改为 `ExtensionHandle`
3. 既有调用方加 `let _handle = kernel.register(...)`（或 `let _ =`）
4. 测试：注册后 `remove`，断言后续 `dispatch` 返回 `ExtensionNotFound`

---

## 二、referee-ai（AI 地基）

### 2.1 Provider 凭证外部化（Provider Registry）

**问题**：`InstanceSpec` 直接在请求体中传 `api_key`，密钥出现在 HTTP 请求、JSONL 持久化文件、tracing 日志中。这是生产环境的硬阻断问题——任何能连到 daemon 的人都能拿到 provider 密钥。

**位置**：`referee-ai/src/provider/mod.rs` + `referee-aura/src/instance.rs`

**设计**：引入 `ProviderRegistry`——daemon 启动时从环境变量 / 配置文件加载凭证，运行时按 `provider_name` 引用。`InstanceSpec` 只传 `provider: "agnes"`，不传 `api_key`。

```rust
// referee-ai/src/provider/registry.rs（新文件）

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use crate::provider::LLMProvider;

/// Provider 注册表 — 按 name 引用预配置的 Provider 实例
/// daemon 启动时从配置加载，运行时按 name 查找
#[derive(Clone)]
pub struct ProviderRegistry {
    providers: Arc<RwLock<HashMap<String, Arc<dyn LLMProvider>>>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册一个预配置的 Provider
    pub fn register(&self, name: impl Into<String>, provider: Arc<dyn LLMProvider>) {
        self.providers.write().insert(name.into(), provider);
    }

    /// 按 name 查找 Provider（InstanceSpec 只传 name，不传凭证）
    pub fn get(&self, name: &str) -> Option<Arc<dyn LLMProvider>> {
        self.providers.read().get(name).cloned()
    }

    /// 列出所有已注册的 Provider name（供 /v1/providers API）
    pub fn names(&self) -> Vec<String> {
        self.providers.read().keys().cloned().collect()
    }
}
```

```rust
// referee-aura/src/protocol.rs — InstanceSpec 改为引用

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    /// 引用预注册的 Provider name（如 "agnes"、"deepseek"）
    /// 不再内嵌 api_key
    pub name: String,
    /// 可选的模型覆盖（否则用 Provider 默认模型）
    #[serde(default)]
    pub model: Option<String>,
}

// 旧字段 api_key 移除——凭证由 daemon 管理，不暴露给 API
```

```rust
// referee-aura/src/bin/referee-aura.rs — 启动时加载凭证

fn build_provider_registry() -> ProviderRegistry {
    let registry = ProviderRegistry::new();

    // 从环境变量加载（优先）
    if let Ok(key) = std::env::var("AGNES_API_KEY") {
        let provider = AgnesProvider::new(
            AgnesModel::V25Flash,
            AgnesConfig::new(&key),
        ).expect("create agnes provider");
        registry.register("agnes", Arc::new(provider));
    }

    if let Ok(key) = std::env::var("DEEPSEEK_API_KEY") {
        let provider = DeepSeekProvider::new(&key, DeepSeekModel::Chat)
            .expect("create deepseek provider");
        registry.register("deepseek", Arc::new(provider));
    }

    // 未配置任何 provider 则 panic（fail-fast）
    if registry.names().is_empty() {
        eprintln!("FATAL: no providers configured. Set AGNES_API_KEY or DEEPSEEK_API_KEY");
        std::process::exit(1);
    }

    registry
}
```

```rust
// referee-aura/src/instance.rs — create 时从 registry 查找

impl InstanceManager {
    pub fn create(&self, spec: InstanceSpec) -> Result<InstanceId, ServerError> {
        // 从 registry 查找 provider，而非从 spec 构造
        let provider = self.provider_registry
            .get(&spec.provider.name)
            .ok_or_else(|| ServerError::new(
                ERR_INVALID_SPEC,
                format!("unknown provider: {}", spec.provider.name),
            ))?;

        // ...
    }
}
```

**执行步骤**：
1. 新增 `referee-ai/src/provider/registry.rs`（ProviderRegistry）
2. `ProviderConfig` 删除 `api_key` 字段，改为 `name` 引用
3. `InstanceManager` 持有 `ProviderRegistry`，`create` 从中查找
4. `referee-aura` bin 启动时从环境变量构建 registry
5. 测试：注册两个 provider，create 时指定 name，断言使用了正确的 provider
6. **破坏性变更**：既有 API 消费方需改为传 `provider.name` 而非 `provider.config.api_key`

---

### 2.2 Session 历史崩溃恢复

**问题**：`PersistStore` 把会话消息以 JSONL 追加落盘，但恢复时**只恢复 InstanceSpec，不重放会话历史**。daemon 重启后，之前的对话上下文全部丢失，JSONL 变成纯审计日志。

**位置**：`referee-aura/src/persist.rs` + `referee-ai/src/engine/mod.rs`

**设计**：恢复时读取 `sessions/<instance>/<session>.jsonl`，重放 `Message` 回 `Session` 的 history。

```rust
// referee-aura/src/persist.rs — 新增会话历史读取

impl PersistStore {
    /// 读取一个实例下所有会话的 JSONL 历史
    /// 返回 (SessionId, Vec<Message>) 列表
    pub fn read_session_histories(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Vec<(SessionId, Vec<Message>)>, PersistError> {
        let dir = self.sessions_dir.join(instance_id.as_str());
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        for entry in std::fs::read_dir(&dir)
            .map_err(|e| PersistError::Io { path: dir.clone(), source: e })?
        {
            let entry = entry.map_err(|e| PersistError::Io {
                path: dir.clone(),
                source: e,
            })?;

            let path = entry.path();
            let filename = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");

            // 解析 SessionId
            let Ok(session_id) = SessionId::parse_str(filename) else {
                continue; // 跳过无法解析的文件
            };

            // 逐行读取 JSONL
            let content = std::fs::read_to_string(&path)
                .map_err(|e| PersistError::Io { path, source: e })?;

            let mut messages = Vec::new();
            for line in content.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Message>(line) {
                    Ok(msg) => messages.push(msg),
                    Err(e) => {
                        tracing::warn!(
                            instance_id = %instance_id,
                            session_id = %session_id,
                            error = %e,
                            "persist: skipping unparseable session line"
                        );
                    }
                }
            }

            if !messages.is_empty() {
                result.push((session_id, messages));
            }
        }
        Ok(result)
    }
}
```

```rust
// referee-aura/src/persist.rs — 恢复入口增加会话重放

pub fn recover(&self) -> Result<RecoveryResult, PersistError> {
    let mut recovered = Vec::new();
    let mut broken = Vec::new();

    // 1. 恢复实例规格（既有逻辑）
    for entry in std::fs::read_dir(self.instances_dir())? {
        // ... 解析 InstanceSpec，push 到 recovered ...
    }

    // 2. 恢复会话历史（新增）
    for (instance_id, spec) in &recovered {
        match self.read_session_histories(instance_id) {
            Ok(histories) => {
                for (session_id, messages) in histories {
                    // 标记到 RecoveryResult，由 InstanceManager 重放
                    // ...
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "persist: failed to read session histories");
            }
        }
    }

    Ok(RecoveryResult { recovered, broken })
}
```

```rust
// referee-ai/src/engine/mod.rs — Engine 增加恢复会话方法

impl Engine {
    /// 恢复一个会话的历史消息（崩溃恢复）
    /// 在 create_session 之后调用，将 JSONL 中的 Message 逐条 push
    pub fn restore_session_history(
        &self,
        session_id: SessionId,
        messages: Vec<Message>,
    ) -> Result<usize, EngineStartError> {
        // 确保 session 存在
        if !self.sessions.contains_key(&session_id) {
            self.create_session(session_id)?;
        }

        let mut count = 0;
        if let Some(mut s) = self.sessions.get_mut(&session_id) {
            for msg in messages {
                match s.push_history(msg) {
                    Ok(()) => count += 1,
                    Err(_) => break, // 容量满：停止，保留已恢复前缀
                }
            }
        }
        Ok(count)
    }
}
```

**执行步骤**：
1. `PersistStore` 新增 `read_session_histories` 方法
2. `RecoveryResult` 增加 `session_histories: HashMap<(InstanceId, SessionId), Vec<Message>>`
3. `InstanceManager::recover_instance` 恢复后调用 `engine.restore_session_history`
4. 测试：创建实例 + 对话若干轮 → 模拟重启（drop + 重建 InstanceManager）→ 断言会话历史恢复
5. 注意：恢复的会话状态为 `Idle`，不恢复 `Thinking` / `AwaitingCalls` 中间态

---

### 2.3 结构化错误类型（Error Typing）

**问题**：当前 `LlmError` 有完善的错误分类，但 `EngineReply::Error(String)` 和 `ServerError` 把所有错误压平成字符串。客户端无法程序化区分「预算超限」「认证失败」「provider 不可用」「实例不存在」。

**位置**：`referee-ai/src/engine/mod.rs` + `referee-aura/src/protocol.rs`

```rust
// referee-ai/src/engine/mod.rs — EngineReply 增加 ErrorKind

/// 引擎错误种类 — 供上层结构化响应
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EngineError {
    #[error("budget exceeded: {message}")]
    BudgetExceeded { message: String },

    #[error("provider error: {message}")]
    ProviderError { message: String },

    #[error("tool execution failed: {message}")]
    ToolFailed { message: String },

    #[error("session not found")]
    SessionNotFound,

    #[error("max retries exhausted: {message}")]
    MaxRetriesExhausted { message: String },

    #[error("internal error: {message}")]
    Internal { message: String },
}

pub enum EngineReply {
    Success(Box<ChatResponse>),
    Streaming(BoxStream<'static, Result<StreamChunk, LlmError>>),
    Busy { turn_id: u64 },
    // 旧：Error(String)
    // 新：
    Error(EngineError),
    Cancelled,
    Timeout,
}
```

```rust
// referee-aura/src/protocol.rs — HTTP 错误响应结构化

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiError {
    #[serde(rename = "budget_exceeded")]
    BudgetExceeded { message: String, session_id: Option<String> },

    #[serde(rename = "provider_error")]
    ProviderError { message: String, provider: String },

    #[serde(rename = "instance_not_found")]
    InstanceNotFound { id: String },

    #[serde(rename = "session_busy")]
    SessionBusy { turn_id: u64 },

    #[serde(rename = "auth_failed")]
    AuthFailed,

    #[serde(rename = "rate_limited")]
    RateLimited { retry_after: Option<u64> },

    #[serde(rename = "invalid_request")]
    InvalidRequest { message: String },

    #[serde(rename = "internal")]
    Internal { message: String },
}

impl HttpError {
    pub fn status_code(&self) -> StatusCode {
        match &self.0 {
            ApiError::BudgetExceeded { .. } => StatusCode::PAYMENT_REQUIRED,         // 402
            ApiError::ProviderError { .. } => StatusCode::BAD_GATEWAY,              // 502
            ApiError::InstanceNotFound { .. } => StatusCode::NOT_FOUND,             // 404
            ApiError::SessionBusy { .. } => StatusCode::CONFLICT,                   // 409
            ApiError::AuthFailed => StatusCode::UNAUTHORIZED,                       // 401
            ApiError::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,          // 429
            ApiError::InvalidRequest { .. } => StatusCode::BAD_REQUEST,            // 400
            ApiError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,          // 500
        }
    }
}
```

**执行步骤**：
1. 定义 `EngineError` enum，`EngineReply::Error` 改用它
2. 引擎内部错误路径分类映射到 `EngineError` 变体
3. `ServerError` 扩展为 `ApiError`，HTTP handler 输出结构化 JSON
4. TCP JSON-RPC error code 与 `ApiError` 对齐
5. 测试：触发各类错误，断言响应的 `type` 字段和 status code

---

### 2.4 Provider 健康检查（Health Probe）

**问题**：Provider 构造时只验证 API Key 格式，不验证连通性。daemon 启动后，第一个用户请求才发现 provider 不可用（DNS 解析失败、Key 过期、服务下线）。

**位置**：`referee-ai/src/provider/mod.rs`

```rust
// referee-ai/src/provider/mod.rs — LLMProvider trait 增加 health check

#[async_trait]
pub trait LLMProvider: Send + Sync {
    // ... 既有方法不变 ...

    /// 健康检查 — 发送一个最小请求验证 provider 可用性
    /// 返回 Ok(()) 表示可用，Err 表示不可用及原因
    ///
    /// 默认实现：发送 "ping" 单 token 请求，检查 HTTP 200
    async fn health_check(&self) -> Result<(), LlmError> {
        let req = ChatRequest {
            messages: vec![Message::user("ping")],
            max_tokens: Some(1),
            temperature: Some(0.0),
            ..Default::default()
        };
        self.chat(req).await.map(|_| ())
    }
}
```

```rust
// referee-aura/src/server.rs — 启动时对所有 provider 做健康检查

impl Server {
    pub async fn new(config: ServerConfig) -> Result<Self, ServerError> {
        let manager = InstanceManager::new(/* ... */);
        let registry = &manager.provider_registry;

        // 并行健康检查所有已注册 provider
        let names = registry.names();
        let mut checks = futures::future::join_all(
            names.iter().map(|name| async {
                let result = registry.get(name)
                    .map(|p| p.health_check().await);
                (name.clone(), result)
            })
        ).await;

        let failed: Vec<_> = checks.iter()
            .filter(|(_, r)| r.is_none() || r.as_ref().unwrap().is_err())
            .collect();

        if !failed.is_empty() {
            tracing::warn!(
                failed = ?failed.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
                "some providers failed health check"
            );
        }

        let healthy: Vec<_> = checks.into_iter()
            .filter(|(_, r)| r.is_some() && r.as_ref().unwrap().is_ok())
            .map(|(n, _)| n)
            .collect();
        tracing::info!(providers = ?healthy, "providers ready");

        Ok(Self { manager, persist: config.persist })
    }
}
```

**执行步骤**：
1. `LLMProvider` trait 加 `health_check` 默认方法
2. `Server::new` 启动时并行检查所有 provider
3. 失败的 provider 标记为 unavailable（不注册进 registry）
4. 全部失败时 panic（fail-fast）
5. 测试：mock provider 返回 Err，断言未被注册

---

## 三、referee-agent（业务封装）

### 3.1 Agent 定义热加载（TOML → 运行时注册）

**问题**：当前 `AgentDefinition` 只能通过代码构造或 JSON API 创建。修改 Agent 的 system prompt 需要重新编译。生产环境需要运维人员不改代码调整 Agent 行为。

**位置**：`referee-agent/src/agent/definition.rs` + 新增 `loader.rs`

```rust
// referee-agent/src/agent/loader.rs（新文件）

use std::path::Path;
use crate::agent::{AgentDefinition, AgentId, TemplateRef, WILDCARD_ALL};

/// Agent 定义文件格式（TOML）
/// ```toml
/// id = "analyst"
/// system_prompt = "你是一个数据分析师..."
/// tools = ["read", "write"]
/// skills = []
/// mcp_servers = []
/// temperature = 0.2
/// max_tokens = 4096
/// ```
#[derive(Debug, Clone, serde::Deserialize)]
struct AgentFile {
    id: String,
    system_prompt: String,
    #[serde(default)]
    tools: Vec<String>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default)]
    mcp_servers: Vec<String>,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    max_tokens: Option<usize>,
}

/// 从 TOML 文件加载单个 Agent 定义
pub fn load_agent_from_file(path: &Path) -> Result<AgentDefinition, AgentLoadError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| AgentLoadError::Io(path.to_path_buf(), e))?;
    let file: AgentFile = toml::from_str(&content)
        .map_err(|e| AgentLoadError::Parse(path.to_path_buf(), e.to_string()))?;

    let id = AgentId::new(&file.id)
        .map_err(|e| AgentLoadError::InvalidId(file.id.clone(), e.to_string()))?;

    Ok(AgentDefinition {
        id,
        system_prompt: TemplateRef::Inline(file.system_prompt),
        tools: if file.tools == ["*"] { WILDCARD_ALL.to_vec() } else { file.tools },
        skills: file.skills,
        mcp_servers: file.mcp_servers,
        chat_params: crate::agent::ChatParams {
            temperature: file.temperature,
            max_tokens: file.max_tokens,
            ..Default::default()
        },
    })
}

/// 从目录加载所有 Agent 定义
/// 扫描 `<dir>/*.toml`，每个文件一个 Agent
pub fn load_agents_from_dir(dir: &Path) -> Result<Vec<AgentDefinition>, AgentLoadError> {
    let mut agents = Vec::new();
    if !dir.exists() {
        return Ok(agents);
    }
    for entry in std::fs::read_dir(dir)
        .map_err(|e| AgentLoadError::Io(dir.to_path_buf(), e))?
    {
        let entry = entry.map_err(|e| AgentLoadError::Io(dir.to_path_buf(), e))?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            match load_agent_from_file(&path) {
                Ok(def) => agents.push(def),
                Err(e) => tracing::warn!(path = ?path, error = %e, "skip unparseable agent file"),
            }
        }
    }
    Ok(agents)
}

#[derive(Debug, thiserror::Error)]
pub enum AgentLoadError {
    #[error("io error at {0}: {1}")]
    Io(std::path::PathBuf, std::io::Error),
    #[error("parse error in {0}: {1}")]
    Parse(std::path::PathBuf, String),
    #[error("invalid agent id '{0}': {1}")]
    InvalidId(String, String),
}
```

```rust
// referee-agent/src/agent/mod.rs — 导出
pub mod loader;
pub use loader::{load_agent_from_file, load_agents_from_dir, AgentLoadError};
```

```toml
# agents/analyst.toml — 运维人员编写的 Agent 定义
id = "analyst"
system_prompt = """
你是一个数据分析专家。使用 socstat-mcp 工具完成统计分析任务。
工作流程：理解需求 → 选择方法 → 执行计算 → 解释结果。
"""
tools = ["read", "write", "edit"]
skills = []
mcp_servers = ["socstat"]
temperature = 0.2
max_tokens = 4096
```

**执行步骤**：
1. 新增 `referee-agent/src/agent/loader.rs`
2. `Cargo.toml` 加 `toml = "0.8"` 依赖（referee-agent 专用，不污染内核）
3. `referee-aura` bin 启动时扫描 `--agents-dir` 目录加载
4. 测试：写一个临时 TOML 文件，`load_agent_from_file` 断言字段正确
5. 注意：热加载（运行时重新扫描）作为后续扩展，当前只做启动时加载

---

### 3.2 Tool 执行结果结构化（ToolOutput 增强）

**问题**：`ToolOutput` 只有一个 `content: String` 字段。工具执行的结果、状态、结构化数据全部被压平为字符串。LLM 需要从字符串中重新解析 JSON，且无法区分「工具成功但结果为空」和「工具失败」。

**位置**：`referee-agent/src/tool/` — 不改 `referee-ai` 的 `Tool` trait（地基保持不变），在业务层增强。

```rust
// referee-agent/src/tool/enhanced.rs（新文件）

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 工具执行状态
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolStatus {
    Success,
    PartialSuccess,
    Failed,
}

/// 增强工具输出 — 供业务层工具使用
/// 序列化为 JSON 字符串后写入 ToolOutput.content（不改地基 Tool trait）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedToolOutput {
    pub status: ToolStatus,
    /// 人类可读的摘要（LLM 主要看这个）
    pub summary: String,
    /// 结构化数据（LLM 可按需解析）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    /// 执行耗时（毫秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
}

impl EnhancedToolOutput {
    /// 成功结果
    pub fn success(summary: impl Into<String>) -> Self {
        Self {
            status: ToolStatus::Success,
            summary: summary.into(),
            data: None,
            elapsed_ms: None,
        }
    }

    /// 成功 + 结构化数据
    pub fn success_with_data(summary: impl Into<String>, data: Value) -> Self {
        Self {
            status: ToolStatus::Success,
            summary: summary.into(),
            data: Some(data),
            elapsed_ms: None,
        }
    }

    /// 失败
    pub fn failed(summary: impl Into<String>) -> Self {
        Self {
            status: ToolStatus::Failed,
            summary: summary.into(),
            data: None,
            elapsed_ms: None,
        }
    }

    /// 序列化为 JSON 字符串（写入 ToolOutput.content）
    pub fn to_content(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            format!("{{\"status\":\"failed\",\"summary\":\"{}\"}}",
                self.summary.replace('"', "\\\""))
        })
    }
}
```

**使用方式**（业务层工具自行选择用不用）：

```rust
// referee-agent/src/tool/read.rs — ReadTool 使用 EnhancedToolOutput

impl Tool for ReadTool {
    async fn execute(&self, ctx: ToolContext, args: Value) -> Result<ToolOutput, ToolError> {
        let start = std::time::Instant::now();
        let path = args["path"].as_str().ok_or(...)?;

        match self.read_file(path) {
            Ok(content) => {
                let output = EnhancedToolOutput::success_with_data(
                    format!("Read {} bytes from {}", content.len(), path),
                    serde_json::json!({ "path": path, "bytes": content.len() }),
                );
                // elapsed_ms 在这里设
                let mut output = output;
                output.elapsed_ms = Some(start.elapsed().as_millis() as u64);
                Ok(ToolOutput { content: output.to_content() })
            }
            Err(e) => {
                let output = EnhancedToolOutput::failed(format!("read error: {e}"));
                Ok(ToolOutput { content: output.to_content() })
            }
        }
    }
}
```

**执行步骤**：
1. 新增 `referee-agent/src/tool/enhanced.rs`
2. 新建工具优先使用 `EnhancedToolOutput`
3. 既有工具不强制改造（向后兼容）
4. 测试：`EnhancedToolOutput::success("ok").to_content()` 断言含 `"status":"success"`

---

## 四、referee-aura（服务层）

### 4.1 API 鉴权（Bearer Token）

**问题**：HTTP 和 TCP 端口零认证。任何人能创建实例、用任意 provider 花钱、读取所有会话。

**位置**：`referee-aura/src/http/mod.rs` + `referee-aura/src/transport.rs`

```rust
// referee-aura/src/auth.rs（新文件）

use std::sync::Arc;
use axum::extract::Request;
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;
use axum::extract::State;

/// 鉴权配置
#[derive(Clone)]
pub struct AuthConfig {
    /// Bearer token（None = 不鉴权，仅本地开发用）
    expected_token: Option<String>,
}

impl AuthConfig {
    /// 从环境变量构造
    pub fn from_env() -> Self {
        Self {
            expected_token: std::env::var("REFEREE_AUTH_TOKEN").ok(),
        }
    }

    /// 无鉴权（仅 localhost 开发用）
    pub fn none() -> Self {
        Self { expected_token: None }
    }
}

/// axum 中间件 — Bearer Token 校验
pub async fn bearer_auth(
    State(auth): State<AuthConfig>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // 未配置 token → 跳过鉴权（仅开发环境）
    let Some(expected) = &auth.expected_token else {
        return Ok(next.run(req).await);
    };

    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));

    match header {
        Some(token) if token == expected => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}
```

```rust
// referee-aura/src/http/mod.rs — 路由加中间件

pub fn router(instances: InstanceManager, auth: AuthConfig) -> Router {
    Router::new()
        .route("/v1/instances", post(handlers::create))
        .route("/v1/instances", get(handlers::list))
        // ... 既有路由 ...
        .route("/health", get(handlers::health))   // 不鉴权
        .route("/ready", get(handlers::ready))      // 不鉴权
        .route("/metrics", get(handlers::metrics)) // 不鉴权
        .with_state(instances)
        // 对 /v1/* 路由应用鉴权中间件
        .layer(axum::middleware::from_fn_with_state(auth, bearer_auth))
}
```

```rust
// referee-aura/src/transport.rs — TCP 握手帧鉴权

// TCP 连接建立后，第一帧必须是 auth：
// {"jsonrpc":"2.0","method":"auth","params":{"token":"xxx"}}
// 鉴权通过后续帧才被处理；不通过则关闭连接。

async fn handle_connection(
    stream: TcpStream,
    instances: InstanceManager,
    auth: AuthConfig,
) {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    // 鉴权握手（如配置了 token）
    if auth.expected_token.is_some() {
        match wait_auth_frame(&mut reader).await {
            Ok(()) => {}
            Err(_) => {
                let _ = writer.write_all(b"{\"error\":\"auth failed\"}\n").await;
                return;
            }
        }
    }

    // 正常请求循环（既有）
    // ...
}

async fn wait_auth_frame(reader: &mut BufReader<&TcpStream>) -> Result<(), AuthError> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let v: serde_json::Value = serde_json::from_str(&line)?;
    let method = v.get("method").and_then(|m| m.as_str());
    let token = v
        .get("params")
        .and_then(|p| p.get("token"))
        .and_then(|t| t.as_str());
    match (method, token) {
        (Some("auth"), Some(_)) => Ok(()),
        _ => Err(AuthError::InvalidAuthFrame),
    }
}
```

**执行步骤**：
1. 新增 `referee-aura/src/auth.rs`
2. HTTP router 加 `bearer_auth` 中间件
3. TCP transport 加握手帧
4. `referee-aura` bin 从 `REFEREE_AUTH_TOKEN` 环境变量读取
5. 未设置 token 时打印 warning 但允许运行（开发友好）
6. 测试：设 token，无 Authorization header → 401；正确 token → 正常

---

### 4.2 Health / Ready / Metrics 端点

**问题**：没有 `/health` 端点，无法部署在 k8s / 负载均衡后面。`observe` 模块有 metrics 调用但没有 exporter。

**位置**：`referee-aura/src/http/handlers.rs`

```rust
// referee-aura/src/http/handlers.rs

/// GET /health — 存活探针（daemon 进程是否运行）
pub async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

/// GET /ready — 就绪探针（provider 是否可用 + 持久化目录是否可写）
pub async fn ready(
    State(m): State<InstanceManager>,
) -> Result<Json<Value>, HttpError> {
    let providers = m.provider_registry.names();
    if providers.is_empty() {
        return Err(HttpError(ServerError::new(
            err::ERR_INTERNAL,
            "no providers configured",
        )));
    }

    // 检查持久化目录可写
    if let Some(persist) = &m.persist {
        let test_path = persist.state_dir.join(".ready_check");
        match std::fs::write(&test_path, b"ok") {
            Ok(()) => { let _ = std::fs::remove_file(&test_path); }
            Err(e) => {
                return Err(HttpError(ServerError::new(
                    err::ERR_INTERNAL,
                    format!("persist dir not writable: {e}"),
                )));
            }
        }
    }

    Ok(Json(serde_json::json!({
        "status": "ready",
        "providers": providers,
    })))
}
```

```rust
// referee-aura/src/http/metrics.rs（新文件）
// Prometheus 格式指标导出

use axum::response::IntoResponse;
use axum::http::StatusCode;

/// GET /metrics — Prometheus 格式指标
pub async fn metrics() -> impl IntoResponse {
    // metrics crate 的 recorder 输出
    // 需加依赖：metrics-exporter-prometheus
    let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .expect("install prometheus recorder");
    let body = handle.render();
    (
        StatusCode::OK,
        [("Content-Type", "text/plain; version=0.0.4")],
        body,
    )
}
```

```rust
// referee-aura/src/http/mod.rs — 路由注册

pub fn router(instances: InstanceManager, auth: AuthConfig) -> Router {
    Router::new()
        // 公开端点（不鉴权）
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        .route("/metrics", get(metrics::metrics))
        // 受保护端点
        .route("/v1/instances", post(handlers::create))
        .route("/v1/instances", get(handlers::list))
        .route("/v1/instances/{id}", get(handlers::get))
        .route("/v1/instances/{id}", delete(handlers::remove))
        .route("/v1/instances/{id}/chat", post(handlers::chat))
        .route("/v1/instances/{id}/chat/stream", post(handlers::chat_stream))
        .route("/v1/instances/{id}/interrupt", post(handlers::interrupt))
        .route("/v1/instances/{id}/sessions", get(handlers::sessions))
        .with_state(instances)
        .layer(axum::middleware::from_fn_with_state(auth, bearer_auth))
}
```

**执行步骤**：
1. 新增 `health` / `ready` handler
2. 新增 `metrics` handler（加 `metrics-exporter-prometheus` 依赖）
3. 路由注册，公开端点不鉴权
4. `referee-aura` bin 初始化时安装 Prometheus recorder
5. 测试：`GET /health` → 200；`GET /ready` 在无 provider 时 → 503

---

### 4.3 配置文件（referee.toml）

**问题**：所有配置走 CLI 参数。provider 凭证、auth token、agent 定义等无法统一管理。

**位置**：`referee-aura/src/config.rs`（新文件）

```rust
// referee-aura/src/config.rs（新文件）

use std::path::PathBuf;
use serde::Deserialize;

/// 顶层配置文件
#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub providers: ProvidersConfig,
    #[serde(default)]
    pub agents: AgentsConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default)]
    pub http_bind: Option<String>,
    #[serde(default = "default_max_instances")]
    pub max_instances: usize,
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct AuthConfig {
    /// Bearer token（None = 不鉴权）
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ProvidersConfig {
    #[serde(default)]
    pub agnes: Option<ProviderEntry>,
    #[serde(default)]
    pub deepseek: Option<ProviderEntry>,
    #[serde(default)]
    pub xiaomi: Option<ProviderEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ProviderEntry {
    /// API Key（直接写或 ${ENV_VAR} 引用环境变量）
    pub api_key: String,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AgentsConfig {
    /// Agent 定义文件目录
    #[serde(default)]
    pub dir: Option<PathBuf>,
}

impl Config {
    /// 从 TOML 文件加载，环境变量覆盖
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(path.to_path_buf(), e))?;
        let mut config: Config = toml::from_str(&content)
            .map_err(|e| ConfigError::Parse(e.to_string()))?;

        // 环境变量覆盖：REFEREE_AUTH_TOKEN > config.auth.token
        if let Ok(token) = std::env::var("REFEREE_AUTH_TOKEN") {
            config.auth.token = Some(token);
        }

        // 解析 ${ENV_VAR} 引用
        for entry in [&mut config.providers.agnes, &mut config.providers.deepseek] {
            if let Some(e) = entry.as_mut() {
                e.api_key = resolve_env(&e.api_key);
            }
        }

        Ok(config)
    }
}

/// 解析 ${VAR} 语法 → 环境变量值
fn resolve_env(s: &str) -> String {
    if let Some(var) = s.strip_prefix("${").and_then(|s| s.strip_suffix("}")) {
        std::env::var(var).unwrap_or_else(|_| {
            panic!("env var {var} not set but referenced in config");
        })
    } else {
        s.to_string()
    }
}
```

```toml
# referee.toml — 配置文件示例

[server]
bind = "127.0.0.1:7101"
max_instances = 64
max_sessions = 100
state_dir = "~/.referee/state"

[auth]
token = "${REFEREE_AUTH_TOKEN}"

[providers.agnes]
api_key = "${AGNES_API_KEY}"
model = "agnes-2.5-flash"

[providers.deepseek]
api_key = "${DEEPSEEK_API_KEY}"

[agents]
dir = "./agents"
```

**执行步骤**：
1. 新增 `referee-aura/src/config.rs`
2. `Cargo.toml` 加 `toml = "0.8"` 依赖
3. `referee-aura` bin 加 `--config <path>` 参数，加载后传递给 Server
4. CLI 参数保留作为配置文件覆盖（`--bind` 覆盖 `config.server.bind`）
5. 测试：写临时 TOML，`Config::load` 断言字段正确

---

### 4.4 速率限制（Rate Limiting）

**问题**：一个客户端可以高频发 `chat` 请求打满 provider 配额。`budget` 限的是 token 总量，不是请求频率。

**位置**：`referee-aura/src/rate_limit.rs`（新文件）

```rust
// referee-aura/src/rate_limit.rs（新文件）

use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::Arc;
use parking_lot::Mutex;

/// 令牌桶限流器 — 按客户端 IP / 实例 ID 限流
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<M<HashMap<String, Bucket>>>,
    /// 每秒补充的令牌数
    refill_rate: f64,
    /// 桶容量
    capacity: u32,
}

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(refill_rate: f64, capacity: u32) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            refill_rate,
            capacity,
        }
    }

    /// 尝试消费一个令牌
    /// 返回 true 表示允许，false 表示限流
    pub fn try_consume(&self, key: &str) -> bool {
        let mut map = self.inner.lock();
        let now = Instant::now();
        let bucket = map.entry(key.to_string()).or_insert(Bucket {
            tokens: self.capacity as f64,
            last_refill: now,
        });

        // 补充令牌
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate)
            .min(self.capacity as f64);
        bucket.last_refill = now;

        // 消费
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// 清理过期的桶（超过 5 分钟未使用）
    pub fn gc(&self) {
        let mut map = self.inner.lock();
        let now = Instant::now();
        map.retain(|_, bucket| {
            now.duration_since(bucket.last_refill) < Duration::from_secs(300)
        });
    }
}
```

```rust
// referee-aura/src/http/handlers.rs — chat 端点加限流

pub async fn chat(
    State(m): State<InstanceManager>,
    State(limiter): State<RateLimiter>,
    Path(id): Path<InstanceId>,
    Json(req): Json<ChatRequest>,
) -> Result<Json<ChatReply>, HttpError> {
    // 限流 key = 实例 ID（也可改为客户端 IP）
    if !limiter.try_consume(&id.to_string()) {
        return Err(HttpError(ApiError::RateLimited {
            retry_after: Some(1),
        }));
    }

    // ... 既有逻辑 ...
}
```

**执行步骤**：
1. 新增 `referee-aura/src/rate_limit.rs`
2. `InstanceManager` 持有 `RateLimiter`
3. `chat` / `chat_stream` 端点加限流检查
4. 后台任务定期调用 `gc()` 清理过期桶
5. 默认 10 req/s/instance，可配置
6. 测试：快速发 15 个请求，断言第 11+ 个返回 429

---

### 4.5 Docker 部署制品

**问题**：没有 Dockerfile，无法容器化部署。

**位置**：`referee/Dockerfile`（新文件）

```dockerfile
# referee/Dockerfile — 多阶段构建

# Stage 1: Build
FROM rust:1.82-slim AS builder
WORKDIR /app

# 依赖缓存层
COPY Cargo.toml Cargo.lock ./
COPY referee-core/Cargo.toml referee-core/
COPY referee-ai/Cargo.toml referee-ai/
COPY referee-agent/Cargo.toml referee-agent/
COPY referee-aura/Cargo.toml referee-aura/
RUN mkdir -p referee-core/src referee-ai/src referee-agent/src referee-aura/src && \
    echo "fn main() {}" > referee-core/src/lib.rs && \
    echo "fn main() {}" > referee-ai/src/lib.rs && \
    echo "fn main() {}" > referee-agent/src/lib.rs && \
    echo "fn main() {}" > referee-aura/src/lib.rs && \
    cargo build --release -p referee-aura 2>/dev/null || true

# 实际源码
COPY . .
RUN cargo build --release -p referee-aura

# Stage 2: Runtime（最小镜像）
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/referee-aura /usr/local/bin/referee-aura

# 默认配置目录
RUN mkdir -p /var/lib/referee/state /etc/referee/agents
VOLUME ["/var/lib/referee", "/etc/referee"]

EXPOSE 7101

ENTRYPOINT ["referee-aura"]
CMD ["--config", "/etc/referee/referee.toml"]
```

```yaml
# referee/docker-compose.yml
version: "3.8"
services:
  referee:
    build: .
    ports:
      - "7101:7101"
    volumes:
      - ./referee.toml:/etc/referee/referee.toml:ro
      - ./agents:/etc/referee/agents:ro
      - referee-state:/var/lib/referee
    environment:
      - AGNES_API_KEY=${AGNES_API_KEY}
      - REFEREE_AUTH_TOKEN=${REFEREE_AUTH_TOKEN}
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7101/health"]
      interval: 30s
      timeout: 5s
      retries: 3

volumes:
  referee-state:
```

**执行步骤**：
1. 写 Dockerfile（多阶段构建）
2. 写 docker-compose.yml
3. `docker build -t referee .` 验证构建
4. `docker-compose up` 验证运行
5. 镜像大小目标 < 50MB

---

## 五、实施优先级

| 优先级 | 方案 | 层 | 理由 |
|--------|------|-----|------|
| **P0** | 4.1 API 鉴权 | aura | 无认证 = 不可上线 |
| **P0** | 2.1 Provider 凭证外部化 | ai/aura | 密钥泄露在请求/日志/持久化中 |
| **P0** | 4.2 Health/Ready/Metrics | aura | 无探针 = 无法部署 |
| **P0** | 2.2 Session 历史恢复 | ai/aura | 重启丢上下文 = 不可用 |
| **P1** | 4.3 配置文件 | aura | 12-factor 合规 |
| **P1** | 2.3 结构化错误 | ai/aura | 客户端无法程序化处理错误 |
| **P1** | 4.4 速率限制 | aura | 防止单客户端耗尽 quota |
| **P1** | 4.5 Docker 制品 | aura | 容器化部署必需 |
| **P1** | 1.1 Extension shutdown 钩子 | core | 子进程泄漏 |
| **P2** | 2.4 Provider 健康检查 | ai | fail-fast，启动即知 |
| **P2** | 3.1 Agent 热加载 | agent | 不改代码调整行为 |
| **P2** | 3.2 ToolOutput 增强 | agent | 结构化工具结果 |
| **P2** | 1.2 Extension Handle | core | 动态卸载能力 |

---

## 六、设计约束（贯穿所有方案）

1. **破坏性变更**：2.1（凭证外部化）改动 `InstanceSpec` / `ProviderConfig` 公开 API，既有消费方需适配。其他方案均为增量。
2. **依赖白名单**：新增 `toml`、`metrics-exporter-prometheus` 两个依赖，仅在 referee-aura / referee-agent 引入，不污染 referee-core。
3. **向后兼容**：所有新功能默认关闭或提供 fallback（如未配置 auth token 时允许裸跑）。
4. **测试覆盖**：每个方案至少一个单元测试，关键路径（鉴权、恢复、限流）需集成测试。
5. **分层不越界**：
   - referee-core 只加生命周期钩子，不感知 AI 概念
   - referee-ai 只加错误类型和恢复方法，不感知 HTTP/TCP
   - referee-agent 只加业务能力，不感知 daemon
   - referee-aura 承担所有生产化（auth/config/health/limit/docker）

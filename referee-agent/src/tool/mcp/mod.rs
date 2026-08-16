//! MCP 2.0 stdio 客户端桥 — 将远程 MCP 服务器工具接入 referee 的 `Tool` 抽象
//!
//! 业务层能力（AGENTS.md：MCP 属「业务策略不预置」，由使用方/二次封装搭建）。
//! 本模块在 `referee-agent` 内实现 MCP 2.0（2026-07-28 规范）stdio 客户端：
//!
//! - [`protocol`]：协议类型与 JSON-RPC 编解码（纯数据）
//! - [`transport`]：stdio 子进程传输层（有界读取 / 并发分发 / 取消 / 停机）
//! - [`client`]：无状态请求编排 + `_meta` 注入 + 版本协商
//! - [`tool`]：`McpToolClient` 将 MCP 工具映射为 base `Tool`，含 MRTR 三策略
//!
//! 顶层入口 [`McpServer`]：spawn 子进程 → `server/discover` → `tools/list` →
//! 批量构造 `Arc<dyn Tool>`，供调用方注册进 `ToolRegistry`。
//!
//! ## 覆盖范围（P0 + P1，对照规范）
//! - P0：`server/discover` + `tools/list` + `tools/call` + `_meta` 注入 + 版本协商
//! - P1：MRTR `InputRequiredResult` 三策略 + `notifications/cancelled`（stdio 取消）
//! - 未实现（留扩展点，不预置）：Streamable HTTP / OAuth 2.1 / resources / prompts
//!   / subscriptions（需新依赖，按 AGENTS.md 依赖白名单约束默认不启用）
//!
//! ## 设计约束（对齐 AGENTS.md）
//! - **零新增依赖**：仅用 tokio / serde_json / thiserror / async-trait / parking_lot
//! - **有界**：单行长度上限 + in-flight 上限，防 OOM
//! - **隔离**：子进程崩溃/超时 → 显式 `ToolError`，不污染内核与其余扩展
//! - **数据/行为分离**：协议类型在 `protocol`，行为在 `transport`/`client`/`tool`

pub mod client;
pub mod protocol;
pub mod tool;
pub mod transport;

pub use client::McpClient;
pub use protocol::{ServerInfo, ToolCallResult};
pub use tool::{McpToolClient, MrtrStrategy};
pub use transport::{StdioTransport, TransportConfig};

use std::sync::Arc;

use referee_ai::tool::Tool;
use thiserror::Error;

/// MCP 客户端错误
#[derive(Debug, Error, Clone)]
pub enum McpError {
    /// JSON-RPC 错误（含 MCP 错误码；`data` 供版本协商等解析）
    #[error("rpc error {code}: {message}")]
    Rpc {
        code: i64,
        message: String,
        #[allow(missing_docs)]
        data: Option<serde_json::Value>,
    },
    /// 请求超时
    #[error("mcp request timed out")]
    Timeout,
    /// 连接已关闭 / 服务器退出
    #[error("mcp connection closed: {0}")]
    Closed(String),
    /// 待处理请求数超限
    #[error("mcp pending limit exceeded (max {0})")]
    PendingLimit(usize),
    /// stdout 单行超长（防 OOM）
    #[error("mcp line too long ({0} bytes)")]
    LineTooLong(usize),
    /// IO 错误
    #[error("mcp io error: {0}")]
    Io(String),
    /// 协议解析错误
    #[error("mcp protocol error: {0}")]
    Protocol(String),
}

/// MCP 服务器接入配置
#[derive(Debug, Clone)]
pub struct McpServerConfig {
    /// 启动命令（如 `npx`）
    pub command: String,
    /// 命令参数（如 `["-y", "@modelcontextprotocol/server-filesystem", "/tmp"]`）
    pub args: Vec<String>,
    /// 注入子进程的环境变量（覆盖/新增）
    pub envs: Vec<(String, String)>,
    /// 客户端标识（`_meta.clientInfo`）
    pub client_name: String,
    pub client_version: String,
    /// `tools/list` 分页上限（防失控）
    pub max_list_pages: usize,
    /// 传输层配置（有界硬约束）
    pub transport: TransportConfig,
    /// MRTR 默认策略
    pub mrtr: MrtrStrategy,
}

impl McpServerConfig {
    /// 最小构造：命令 + 客户端身份
    pub fn new(command: impl Into<String>, client_name: &str, client_version: &str) -> Self {
        Self {
            command: command.into(),
            args: Vec::new(),
            envs: Vec::new(),
            client_name: client_name.to_string(),
            client_version: client_version.to_string(),
            max_list_pages: 8,
            transport: TransportConfig::default(),
            mrtr: MrtrStrategy::default(),
        }
    }
}

/// 一个已连接的 MCP 服务器（stdio 子进程 + 已发现工具）
pub struct McpServer {
    client: McpClient,
    info: ServerInfo,
    tools: Vec<Arc<dyn Tool>>,
}

impl std::fmt::Debug for McpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpServer")
            .field("name", &self.info.name)
            .field("version", &self.info.version)
            .field("tools", &self.tools.len())
            .field("protocol_version", &self.client.protocol_version())
            .finish()
    }
}

impl McpServer {
    /// 连接 MCP 服务器：spawn 子进程 → `server/discover` → `tools/list` → 构造工具
    pub async fn connect(config: McpServerConfig) -> Result<Self, McpError> {
        let transport = StdioTransport::spawn(
            &config.command,
            &config.args,
            &config.envs,
            config.transport,
        )
        .await?;
        let client = McpClient::new(
            Arc::new(transport),
            config.client_name,
            config.client_version,
        );

        let info = client.discover().await?;
        let schemas = client.list_tools(config.max_list_pages).await?;
        let tools: Vec<Arc<dyn Tool>> = schemas
            .into_iter()
            .map(|schema| {
                let tool = McpToolClient::new(client.clone(), schema, config.mrtr.clone());
                Arc::new(tool) as Arc<dyn Tool>
            })
            .collect();

        Ok(Self { client, info, tools })
    }

    /// 已发现的工具（`Arc<dyn Tool>`，供注册进 `ToolRegistry`）
    pub fn tools(&self) -> &[Arc<dyn Tool>] {
        &self.tools
    }

    /// 发现结果（服务器身份与能力）
    pub fn info(&self) -> &ServerInfo {
        &self.info
    }

    /// 当前目标协议版本
    pub fn protocol_version(&self) -> String {
        self.client.protocol_version()
    }

    /// 优雅停机（关闭子进程并回收后台任务）
    pub async fn shutdown(&self) {
        self.client.shutdown().await;
    }
}
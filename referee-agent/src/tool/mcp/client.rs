//! MCP 客户端编排 — 无状态请求 + 版本协商
//!
//! ## 职责
//! - 每个请求注入 `_meta`（protocolVersion / clientInfo / clientCapabilities）
//! - 版本协商：服务器返回 `UnsupportedProtocolVersionError`（code -32002）时，
//!   从 `data.supported` 选一个最高版本重试（无状态协议，每请求自携带版本）
//! - 核心方法：`server/discover`、`tools/list`（分页）、`tools/call`、取消
//!
//! ## 设计约束
//! - 无状态：不维护会话，仅保存目标协议版本与客户端身份
//! - 共享：`McpClient` 为 `Clone`（内部 `Arc`），可被多个 `McpToolClient` 共享

use std::sync::Arc;

use serde_json::{json, Value};
use tracing::debug;

use crate::tool::mcp::protocol::{
    parse_server_info, parse_tool_result, parse_tools_page, with_meta, McpToolSchema,
    RequestMeta, ServerInfo, ToolCallResult, ToolsPage, DEFAULT_PROTOCOL_VERSION,
};
use crate::tool::mcp::{McpError, StdioTransport};

/// 版本协商最大重试次数（无状态协议下足够）
const MAX_NEGOTIATE: usize = 3;

/// MCP 客户端 — 无状态请求编排
#[derive(Clone)]
pub struct McpClient {
    transport: Arc<StdioTransport>,
    protocol_version: Arc<parking_lot::Mutex<String>>,
    client_name: String,
    client_version: String,
}

impl std::fmt::Debug for McpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpClient")
            .field("protocol_version", &self.protocol_version.lock().clone())
            .field("client_name", &self.client_name)
            .field("transport", &self.transport)
            .finish()
    }
}

impl McpClient {
    /// 构造客户端（协议版本默认取最新，随后可由协商降级）
    pub fn new(
        transport: Arc<StdioTransport>,
        client_name: impl Into<String>,
        client_version: impl Into<String>,
    ) -> Self {
        Self {
            transport,
            protocol_version: Arc::new(parking_lot::Mutex::new(
                DEFAULT_PROTOCOL_VERSION.to_string(),
            )),
            client_name: client_name.into(),
            client_version: client_version.into(),
        }
    }

    /// 当前目标协议版本
    pub fn protocol_version(&self) -> String {
        self.protocol_version.lock().clone()
    }

    /// 发送请求：注入 `_meta` + 版本协商（最多重试 `MAX_NEGOTIATE` 次）
    async fn send(&self, method: &str, params: Value) -> Result<Value, McpError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let meta = RequestMeta::new(
                self.protocol_version.lock().clone(),
                &self.client_name,
                &self.client_version,
            );
            let full = with_meta(params.clone(), &meta);
            match self.transport.request(method, full).await {
                // 官方 UnsupportedProtocolVersionError 错误码为 -32022
                // （-32002 为旧版 resource-not-found，语义不同，勿混淆）
                Err(McpError::Rpc { code: -32022, data, .. }) if attempts <= MAX_NEGOTIATE => {
                    let supported = extract_supported(data);
                    let picked = pick_mutually_supported(&supported)?;
                    debug!(from = %self.protocol_version.lock(), to = %picked, "mcp version negotiated");
                    *self.protocol_version.lock() = picked;
                    continue;
                }
                other => return other,
            }
        }
    }

    /// `server/discover` — 一次性获取版本、能力与身份
    pub async fn discover(&self) -> Result<ServerInfo, McpError> {
        let result = self.send("server/discover", json!({})).await?;
        parse_server_info(&result).map_err(McpError::Protocol)
    }

    /// `tools/list` — 拉取全部工具声明（分页聚合，带页数上限防失控）
    pub async fn list_tools(&self, max_pages: usize) -> Result<Vec<McpToolSchema>, McpError> {
        let mut tools = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..max_pages {
            let params = match &cursor {
                Some(c) => json!({"cursor": c}),
                None => json!({}),
            };
            let result = self.send("tools/list", params).await?;
            let page: ToolsPage = parse_tools_page(&result).map_err(McpError::Protocol)?;
            tools.extend(page.tools);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(tools)
    }

    /// `tools/call` — 调用工具
    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<ToolCallResult, McpError> {
        let result = self
            .send("tools/call", json!({"name": name, "arguments": arguments}))
            .await?;
        parse_tool_result(&result).map_err(McpError::Protocol)
    }

    /// `tools/call`（MRTR 重试）— 携带输入响应与 requestState 重发原请求
    ///
    /// 注意：JSON-RPC `id` 由传输层自动新分配（不同于初始请求），符合规范要求。
    pub async fn call_tool_with_inputs(
        &self,
        name: &str,
        arguments: Value,
        input_responses: Value,
        request_state: &str,
    ) -> Result<ToolCallResult, McpError> {
        let result = self
            .send(
                "tools/call",
                json!({
                    "name": name,
                    "arguments": arguments,
                    "inputResponses": input_responses,
                    "requestState": request_state,
                }),
            )
            .await?;
        parse_tool_result(&result).map_err(McpError::Protocol)
    }

    /// 取消指定请求（`notifications/cancelled`）
    pub async fn cancel(&self, request_id: u64, reason: &str) -> Result<(), McpError> {
        self.transport.notify_cancelled(request_id, reason).await
    }

    /// 优雅停机底层传输
    pub async fn shutdown(&self) {
        self.transport.shutdown().await;
    }
}

/// 从 `UnsupportedProtocolVersionError.data.supported` 提取版本列表
fn extract_supported(data: Option<Value>) -> Vec<String> {
    data.and_then(|d| {
        d.get("supported")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
    })
    .unwrap_or_default()
}

/// 从服务器 `supported` 列表中挑选一个**双方共同支持**的协议版本。
///
/// 客户端已实现版本集为 [`DEFAULT_PROTOCOL_VERSION`]（当前唯一）。若服务器
/// 支持列表包含它，直接采用；否则（服务器只支持更旧版本）取支持列表中的
/// 最高版本作为降级尝试。两列表均空时返回协议错误。
fn pick_mutually_supported(supported: &[String]) -> Result<String, McpError> {
    if supported.is_empty() {
        return Err(McpError::Protocol(
            "server advertised no supported versions".into(),
        ));
    }
    if supported
        .iter()
        .any(|v| v == DEFAULT_PROTOCOL_VERSION)
    {
        return Ok(DEFAULT_PROTOCOL_VERSION.to_string());
    }
    Ok(supported.iter().max().cloned().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_prefers_mutually_supported() {
        let v = vec!["2025-11-25".to_string(), "2026-07-28".to_string()];
        assert_eq!(pick_mutually_supported(&v).unwrap(), "2026-07-28");
    }

    #[test]
    fn pick_falls_back_to_highest_when_client_version_absent() {
        // 服务器只支持更旧版本：降级到支持列表最高版本
        let v = vec!["2025-06-18".to_string(), "2025-11-25".to_string()];
        assert_eq!(pick_mutually_supported(&v).unwrap(), "2025-11-25");
    }

    #[test]
    fn pick_empty_is_error() {
        assert!(pick_mutually_supported(&[]).is_err());
    }

    #[test]
    fn extract_supported_from_data() {
        let data = Some(json!({"supported": ["2026-07-28", "2025-11-25"]}));
        let supported = extract_supported(data);
        assert_eq!(supported, vec!["2026-07-28", "2025-11-25"]);
    }

    #[test]
    fn extract_supported_none() {
        assert!(extract_supported(None).is_empty());
        assert!(extract_supported(Some(json!({"other": 1}))).is_empty());
    }
}
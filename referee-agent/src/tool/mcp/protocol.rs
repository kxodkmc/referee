//! MCP 2.0 协议类型与 JSON-RPC 编解码（纯数据载体）
//!
//! 本模块只承载协议数据与双向编解码，不含任何行为/句柄（数据/行为分离）。
//! 行为（子进程管理、请求编排、版本协商）在 `transport` / `client` 中。
//!
//! ## 覆盖范围（对照 2026-07-28 规范）
//! - JSON-RPC 2.0 请求/响应/通知帧
//! - `_meta` 元数据注入（protocolVersion / clientInfo / clientCapabilities）
//! - `server/discover` 响应（版本、能力、身份）
//! - `tools/list` 分页
//! - `tools/call` 三态结果（complete / input_required / error）
//! - MRTR `InputRequiredResult` 的类型载体

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// 默认目标协议版本（规范 2026-07-28）
pub const DEFAULT_PROTOCOL_VERSION: &str = "2026-07-28";

// ── _meta 元数据 ──────────────────────────────

/// 客户端信息（`_meta.io.modelcontextprotocol/clientInfo`）
#[derive(Debug, Clone, Serialize)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

/// 客户端能力声明（`_meta.io.modelcontextprotocol/clientCapabilities`）
///
/// MCP 2.0 为无状态协议，能力随每个请求携带。当前仅声明 `tools`（工具原语）。
#[derive(Debug, Clone, Serialize)]
pub struct ClientCapabilities {
    pub tools: Value,
}

/// `_meta` 元数据 — 每个请求必须携带
#[derive(Debug, Clone, Serialize)]
pub struct RequestMeta {
    #[serde(rename = "io.modelcontextprotocol/protocolVersion")]
    pub protocol_version: String,
    #[serde(rename = "io.modelcontextprotocol/clientInfo")]
    pub client_info: ClientInfo,
    #[serde(rename = "io.modelcontextprotocol/clientCapabilities")]
    pub client_capabilities: ClientCapabilities,
}

impl RequestMeta {
    /// 构造默认能力声明（tools 原语）
    pub fn new(protocol_version: String, name: &str, version: &str) -> Self {
        Self {
            protocol_version,
            client_info: ClientInfo {
                name: name.to_string(),
                version: version.to_string(),
            },
            client_capabilities: ClientCapabilities { tools: json!({}) },
        }
    }
}

/// 将 `_meta` 注入请求 params：规范字段合入已有 `_meta`（保留调用方自定义项），
/// 无 `_meta` 时新建。
pub fn with_meta(params: Value, meta: &RequestMeta) -> Value {
    let mut obj = match params {
        Value::Object(map) => map,
        other => serde_json::Map::from_iter([("value".to_string(), other)]),
    };
    let meta_value = serde_json::to_value(meta).unwrap_or(json!({}));
    let merged = match obj.get("_meta") {
        Some(Value::Object(existing)) => {
            let mut m = existing.clone();
            if let Value::Object(add) = meta_value {
                for (k, v) in add {
                    m.insert(k, v);
                }
            }
            Value::Object(m)
        }
        _ => meta_value,
    };
    obj.insert("_meta".to_string(), merged);
    Value::Object(obj)
}

// ── server/discover 响应 ──────────────────────

/// 服务器能力声明（`server/discover` 返回）
///
/// 对照官方 DiscoverResult：`supportedVersions` 为支持的协议版本列表；
/// `capabilities` 为能力声明；`serverInfo` 位于 `_meta.io.modelcontextprotocol/serverInfo`
/// （服务器自报，仅用于展示/日志，不用于安全决策）。
#[derive(Debug, Clone, Deserialize)]
pub struct ServerInfo {
    /// 服务器软件名称（自报，仅供展示）
    pub name: String,
    /// 服务器软件版本（自报，仅供展示）
    pub version: String,
    /// 支持的协议版本列表（`supportedVersions`）
    pub supported_versions: Vec<String>,
    /// 能力声明（`tools` / `resources` / `extensions` 等）
    #[serde(default)]
    pub capabilities: Value,
}

/// 从 `server/discover` 结果解析服务器信息
pub fn parse_server_info(result: &Value) -> Result<ServerInfo, String> {
    let supported_versions = result
        .get("supportedVersions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    let capabilities = result.get("capabilities").cloned().unwrap_or(Value::Null);
    let (name, version) = result
        .get("_meta")
        .and_then(|m| m.get("io.modelcontextprotocol/serverInfo"))
        .map(|info| {
            (
                info.get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                info.get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
            )
        })
        .unwrap_or_default();
    Ok(ServerInfo {
        name,
        version,
        supported_versions,
        capabilities,
    })
}

// ── tools/list ────────────────────────────────

/// 工具声明（`tools/list` 返回的单个工具）
#[derive(Debug, Clone, Deserialize)]
pub struct McpToolSchema {
    pub name: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: String,
    /// 输入参数 JSON Schema
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    #[serde(default, rename = "outputSchema")]
    pub output_schema: Option<Value>,
}

/// 工具列表页（支持分页 cursor）
#[derive(Debug, Clone, Deserialize)]
pub struct ToolsPage {
    pub tools: Vec<McpToolSchema>,
    #[serde(default, rename = "nextCursor")]
    pub next_cursor: Option<String>,
}

/// 从 `tools/list` 结果解析
pub fn parse_tools_page(result: &Value) -> Result<ToolsPage, String> {
    serde_json::from_value(result.clone()).map_err(|e| format!("invalid tools/list result: {e}"))
}

// ── tools/call 结果 ───────────────────────────

/// 工具调用的三态结果
#[derive(Debug, Clone)]
pub enum ToolCallResult {
    /// 完整结果（resultType: complete；`isError: true` 归入 [`ToolCallResult::Error`]）
    Complete {
        content: Vec<ContentBlock>,
        structured: Option<Value>,
    },
    /// 需要输入（resultType: input_required，MRTR）
    InputRequired {
        input_requests: Value,
        request_state: String,
    },
    /// 执行错误（resultType 完整但 isError: true，或 resultType: error）
    Error { message: String },
}

/// 内容块（text / image / audio / resource_link / resource）
#[derive(Debug, Clone)]
pub enum ContentBlock {
    Text(String),
    Image { data: String, mime_type: Option<String> },
    Audio { data: String, mime_type: Option<String> },
    ResourceLink { uri: String, name: Option<String> },
    Resource { uri: String, text: Option<String> },
}

/// 从 `tools/call` 结果解析为三态
pub fn parse_tool_result(result: &Value) -> Result<ToolCallResult, String> {
    // 1. MRTR：resultType == input_required
    if result.get("resultType").and_then(|v| v.as_str()) == Some("input_required") {
        let input_requests = result
            .get("inputRequests")
            .cloned()
            .unwrap_or(Value::Null);
        let request_state = result
            .get("requestState")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        return Ok(ToolCallResult::InputRequired {
            input_requests,
            request_state,
        });
    }

    // 2. 显式错误：resultType == error
    if result.get("resultType").and_then(|v| v.as_str()) == Some("error") {
        let message = result
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error")
            .to_string();
        return Ok(ToolCallResult::Error { message });
    }

    // 3. complete（可能带 isError: true）
    if result.get("isError").and_then(|v| v.as_bool()) == Some(true) {
        let message = result
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("tool execution error")
            .to_string();
        return Ok(ToolCallResult::Error { message });
    }

    let content = result
        .get("content")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_content_block).collect())
        .unwrap_or_default();
    let structured = result.get("structuredContent").cloned();
    Ok(ToolCallResult::Complete { content, structured })
}

/// 解析单个内容块
fn parse_content_block(v: &Value) -> Option<ContentBlock> {
    let kind = v.get("type").and_then(|t| t.as_str())?;
    match kind {
        "text" => v.get("text").and_then(|t| t.as_str()).map(|s| ContentBlock::Text(s.to_string())),
        "image" => v.get("data").and_then(|d| d.as_str()).map(|data| ContentBlock::Image {
            data: data.to_string(),
            mime_type: v.get("mimeType").and_then(|m| m.as_str()).map(|s| s.to_string()),
        }),
        "audio" => v.get("data").and_then(|d| d.as_str()).map(|data| ContentBlock::Audio {
            data: data.to_string(),
            mime_type: v.get("mimeType").and_then(|m| m.as_str()).map(|s| s.to_string()),
        }),
        "resource_link" => v.get("uri").and_then(|u| u.as_str()).map(|uri| ContentBlock::ResourceLink {
            uri: uri.to_string(),
            name: v.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()),
        }),
        "resource" => v.get("resource").and_then(|r| r.get("uri")).and_then(|u| u.as_str()).map(|uri| {
            ContentBlock::Resource {
                uri: uri.to_string(),
                text: v.get("resource").and_then(|r| r.get("text")).and_then(|t| t.as_str()).map(|s| s.to_string()),
            }
        }),
        _ => None,
    }
}

/// 将内容块渲染为单文本表示（`ToolOutput.content` 是字符串）
///
/// 文本块直接拼接；结构化内容序列化为 JSON；图片/音频转 data URI 摘要；
/// 资源链接保留 uri。
pub fn render_content(content: &[ContentBlock], structured: Option<&Value>) -> String {
    let mut parts: Vec<String> = Vec::new();
    for block in content {
        match block {
            ContentBlock::Text(s) => parts.push(s.clone()),
            ContentBlock::Image { data, mime_type } => {
                let mime = mime_type.as_deref().unwrap_or("image/png");
                parts.push(format!("data:{mime};base64,{data}"));
            }
            ContentBlock::Audio { data, mime_type } => {
                let mime = mime_type.as_deref().unwrap_or("audio/wav");
                parts.push(format!("data:{mime};base64,{data}"));
            }
            ContentBlock::ResourceLink { uri, name } => {
                parts.push(match name {
                    Some(n) => format!("{n} ({uri})"),
                    None => uri.clone(),
                });
            }
            ContentBlock::Resource { uri, text } => {
                parts.push(match text {
                    Some(t) => format!("{uri}\n{t}"),
                    None => uri.clone(),
                });
            }
        }
    }
    if let Some(s) = structured {
        if !parts.is_empty() {
            parts.push(s.to_string());
        } else {
            return s.to_string();
        }
    }
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tool_result(json: &str) -> Value {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn meta_injected_when_absent() {
        let meta = RequestMeta::new(DEFAULT_PROTOCOL_VERSION.into(), "referee", "0.1");
        let params = with_meta(json!({"name": "get_weather"}), &meta);
        let meta_v = &params["_meta"];
        assert_eq!(
            meta_v["io.modelcontextprotocol/protocolVersion"],
            DEFAULT_PROTOCOL_VERSION
        );
        assert_eq!(meta_v["io.modelcontextprotocol/clientInfo"]["name"], "referee");
        assert!(meta_v["io.modelcontextprotocol/clientCapabilities"]["tools"].is_object());
    }

    #[test]
    fn meta_keeps_existing_meta() {
        let meta = RequestMeta::new(DEFAULT_PROTOCOL_VERSION.into(), "referee", "0.1");
        let params = with_meta(json!({"_meta": {"custom": 1}, "name": "x"}), &meta);
        assert_eq!(params["_meta"]["custom"], 1);
        assert!(params["_meta"]["io.modelcontextprotocol/protocolVersion"].is_string());
    }

    #[test]
    fn parse_complete_text_result() {
        let v = tool_result(r#"{"resultType":"complete","content":[{"type":"text","text":"sunny"}]}"#);
        let r = parse_tool_result(&v).unwrap();
        match r {
            ToolCallResult::Complete { content, .. } => {
                assert_eq!(render_content(&content, None), "sunny");
            }
            _ => panic!("expected complete"),
        }
    }

    #[test]
    fn parse_complete_with_structured_content() {
        let v = tool_result(
            r#"{"resultType":"complete","structuredContent":{"temperature":23,"conditions":"sunny"}}"#,
        );
        let r = parse_tool_result(&v).unwrap();
        match r {
            ToolCallResult::Complete { content, structured } => {
                assert!(content.is_empty());
                let out = render_content(&content, structured.as_ref());
                assert!(out.contains("temperature"));
            }
            _ => panic!("expected complete"),
        }
    }

    #[test]
    fn parse_is_error_true_maps_to_error() {
        let v = tool_result(r#"{"resultType":"complete","isError":true,"message":"boom"}"#);
        let r = parse_tool_result(&v).unwrap();
        match r {
            ToolCallResult::Error { message } => assert_eq!(message, "boom"),
            _ => panic!("expected error"),
        }
    }

    #[test]
    fn parse_input_required() {
        let v = tool_result(
            r#"{"resultType":"input_required","inputRequests":{"login":{"method":"elicitation/create"}},"requestState":"blob"}"#,
        );
        let r = parse_tool_result(&v).unwrap();
        match r {
            ToolCallResult::InputRequired { input_requests, request_state } => {
                assert_eq!(request_state, "blob");
                assert_eq!(input_requests["login"]["method"], "elicitation/create");
            }
            _ => panic!("expected input_required"),
        }
    }

    #[test]
    fn parse_image_data_uri_rendering() {
        let v = tool_result(
            r#"{"resultType":"complete","content":[{"type":"image","data":"AAA","mimeType":"image/png"}]}"#,
        );
        let r = parse_tool_result(&v).unwrap();
        match r {
            ToolCallResult::Complete { content, .. } => {
                assert_eq!(render_content(&content, None), "data:image/png;base64,AAA");
            }
            _ => panic!("expected complete"),
        }
    }

    #[test]
    fn parse_tools_list_with_cursor() {
        let v = serde_json::json!({
            "tools": [{"name":"a","description":"A","inputSchema":{"type":"object"}}],
            "nextCursor": "abc"
        });
        let page = parse_tools_page(&v).unwrap();
        assert_eq!(page.tools.len(), 1);
        assert_eq!(page.tools[0].name, "a");
        assert_eq!(page.next_cursor.as_deref(), Some("abc"));
    }

    #[test]
    fn parse_server_info_versions() {
        // 对照官方 DiscoverResult：supportedVersions + _meta.serverInfo
        let v = serde_json::json!({
            "resultType": "complete",
            "supportedVersions": ["2026-07-28", "2025-11-25"],
            "capabilities": {"tools": {}, "resources": {}},
            "_meta": {
                "io.modelcontextprotocol/serverInfo": {"name": "srv", "version": "1.0.0"}
            }
        });
        let info = parse_server_info(&v).unwrap();
        assert_eq!(info.name, "srv");
        assert_eq!(info.version, "1.0.0");
        assert_eq!(info.supported_versions.len(), 2);
        assert!(info.capabilities.get("tools").is_some());
    }

    #[test]
    fn parse_server_info_without_server_info_defaults() {
        let v = serde_json::json!({
            "supportedVersions": ["2026-07-28"]
        });
        let info = parse_server_info(&v).unwrap();
        assert_eq!(info.name, "");
        assert_eq!(info.version, "");
        assert_eq!(info.supported_versions, vec!["2026-07-28"]);
    }
}
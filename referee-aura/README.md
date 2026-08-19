# Referee Aura — 智能体运行气场（常驻 daemon）

把 referee（Rust 智能体库）做成可被 TUI / Web / CLI 调用的**常驻服务**：支持
**多个独立实例并行运行与管理**、**非正常中断可恢复**。分层严格不越界、默认零新增依赖。

## 分层

| 模块 | 职责 |
|------|------|
| `protocol` | 纯数据 serde 类型 + 错误码（与传输解耦，零业务逻辑）：`InstanceSpec` / `InstanceInfo` / `ChatRequest` / `ChatReply` / `StreamFrame` / `ProviderConfig` / `ServerError` 等 |
| `instance` | 实例生命周期 + 多实例有界管理 + 请求路由（transport-agnostic）：`InstanceManager` / `Instance` / `InstanceStatus` |
| `persist` | 文件 IO + 崩溃恢复（依赖 `protocol::InstanceSpec`）：`PersistStore` / `RecoveryResult` / `BrokenEntry` |
| `chat` | 对话公共助手（载荷构造 / 流收敛 / 帧映射，TCP 与 HTTP 复用） |
| `transport`（feature `tcp`） | TCP JSON-RPC 2.0 over NDJSON 网络 IO（仅调用 instance/persist）：`serve_tcp` / `dispatch` |
| `http`（feature `http`） | HTTP + SSE 网络 IO（仅调用 instance/chat）：`serve_http` |
| `tui`（feature `tui`） | 官方 TUI 客户端（JSON-RPC 客户端，连接 daemon） |
| `server` | 服务装配入口 |

## 特性

```toml
[features]
default = ["tcp", "deepseek"]     # TCP JSON-RPC 2.0 默认开；deepseek 厂商适配转发
tcp    = []                       # TCP JSON-RPC 2.0 over NDJSON
http   = ["dep:axum"]             # HTTP + SSE（默认关、核心零依赖；对齐 mcp-stdio 按需拓展）
tui    = ["dep:ratatui", "dep:unicode-width"]  # 官方 TUI 客户端（默认关、按需）
deepseek / xiaomi / openai       # 厂商适配器（转发到 referee-agent）
```

## 快速使用

```toml
[dependencies]
referee-aura = { path = "../referee-aura" }          # 默认：TCP + deepseek
referee-aura = { path = "../referee-aura", features = ["http", "xiaomi"] }  # 需要 HTTP/SSE
```

启动 daemon（TCP）后，可经 JSON-RPC 调用创建实例、发起对话（含流式）、管理多实例；
`persist` 负责崩溃恢复（重启后按 `InstanceSpec` 恢复实例，会话历史按需重放）。

## 三个二进制入口

| bin | 说明 | 所需特性 |
|-----|------|----------|
| `referee-aura` | daemon（服务器） | 默认 `tcp` |
| `referee-tui` | 官方终端 TUI 客户端 | `tui` |
| `referee` | 聚合入口 | `tui` |

## 硬约束（继承 referee 内核哲学）

- **零新增依赖**：核心构建（无 `http` / `tui` feature）不含 axum / ratatui 及其传递依赖。
- **不吞异常**：错误显式可见，不静默丢弃。
- **背压有界**：实例/会话数量有界，传输行长度与并发受控。
- **结构化错误**：对内核 `EngineReply::Error(EngineError)` 结构化序列化，不再压平成字符串。

## 验证

```bash
cargo test -p referee-aura          # 20 条（默认）
cargo test -p referee-aura --features "http tui"   # 含 HTTP / 与 TUI 相关按需拓展
cargo clippy -p referee-aura --all-targets -- -D warnings
```

## 相关文档

| 文档 | 说明 |
|------|------|
| [`../README.md`](../README.md) | 仓库总览 |
| [`../referee-core/README.md`](../referee-core/README.md) | 内核模块 |
| [`../referee-ai/README.md`](../referee-ai/README.md) | 核心支撑积木 |
| [`../referee-agent/README.md`](../referee-agent/README.md) | 业务封装层 |
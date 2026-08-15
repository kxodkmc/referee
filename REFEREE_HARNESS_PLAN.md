# Referee Harness — 总体方案（架构 / 决策 / 差距 / 阶段规划）

> 目标：把 referee（Rust 智能体库）做成**可被 TUI / Web / CLI 调用的智能体服务**，
> 支持**多个独立实例并行运行与管理、非正常中断可恢复**。
> 参考 DeepSeek Harness 的入口分层：`agent-spine`（运行时）→ `sdk`（传输/协议）→
> CLI / ACP / Web。

## 文档分工

| 文档 | 职责 | 读者 |
|---|---|---|
| [REFEREE_HARNESS_PLAN.md](REFEREE_HARNESS_PLAN.md)（本文档） | **总体方案**：目标架构、决策记录（G1/G2）、现状盘点、差距清单、阶段规划、待确认项 | 评审 / 决策 |
| [REFEREE_HARNESS_IMPL.md](REFEREE_HARNESS_IMPL.md) | **Phase 1 执行实现**：文件结构、类型签名、内部流程、集成测试、验收标准 | 开发执行 |

职责边界：**本文档回答"做什么、为什么、分几个阶段"；实现文档回答"每个文件怎么落地"**。

## 1. 目标架构

```
                 ┌─────────────── 客户端 ───────────────┐
                 │   TUI（本地）   │   Web（浏览器）   │   CLI / 脚本   │
                 └───────┬───────────────┬───────────┘
                         │   HTTP + SSE（待定） / TCP JSON-RPC
              ┌──────────▼───────────────▼──────────┐
              │   referee-harness（入口/宿主层，新 crate）       │
              │   · protocol：serde 协议类型（Chat/Stream/Info）  │
              │   · instance：InstanceManager —— N 个实例并行管理  │
              │   · persist：实例/会话 JSONL 持久化 + 崩溃恢复     │
              │   · transport：TCP JSON-RPC 2.0（P1）/ HTTP+SSE（P2）│
              └──────────┬─────────────────────────┘
                         │ 实例 = AgentRuntime（Engine + 工具 + 模板注册表）
              ┌──────────▼─────────────────────────┐
              │   referee-agent ｜ referee-ai-base ｜ referee-core    │
              └─────────────────────────────────────┘
```

- **多个独立实例并行**：每个实例持有独立的 `AgentRuntime`（内含独立 `Engine`、独立
  会话表、独立模板注册表与工具集），异步并发天然并行；`InstanceManager` 负责
  创建 / 列出 / 查询 / 停止 / 移除与资源有界治理。
- **TUI 与 Web 共用同一 daemon**：一个常驻服务进程承载所有实例，客户端经传输层接入。
- **非正常中断可恢复**：实例规格与会话事实 JSONL 落盘，daemon 重启后重建并回放。

## 2. 决策记录（G1/G2 已收敛）

### G1 — 入口层
- 进程形态：**常驻 daemon**（`referee-harness`），CLI 为其一次性客户端（会话状态在内存，必须常驻）。
- P1 传输：**TCP + JSON-RPC 2.0 over NDJSON**（tokio `TcpListener` + serde_json，零新增依赖）。
- P2 传输：HTTP + SSE（axum，feature 门控，依赖决策待定）；与 P1 共用同一 `protocol` 层。
- `protocol`（serde 载荷）与传输解耦，后续接入第二传输不重写业务层。

### G2 — 实例抽象与多实例管理
- 实例身份：**kebab-case**（用户可读、可拼地址，与 `AgentId` 同规则；管理器强制唯一）。
- 实例独立性：**每实例独立 Engine**（不挂共享 kernel；对等协作后续再议）。
- 实例规格：**全声明式 JSON**（`InstanceSpec` 复用 `AgentDefinition` + engine + provider + tools + template_vars）。
- 崩溃恢复（硬要求，非正常结束可恢复）：
  - 持久化实现：**JSONL 零依赖**（`instances/<id>.json` + `sessions/<instance>/<session>.jsonl`）。
  - 事实落点：**base 挂钩子**（feature 门控：`Session`/`SessionLog` 加可选落盘 sink，对齐 `WalSink` append 语义）——完整持久化含中间工具轮事实，可忠实重建对话。
  - 进行中回合：**丢弃未完成回合**（只恢复已确认前缀，无重复副作用，简单安全）。
  - 不吞异常：落盘失败显式报错（对齐 `LogError::CapacityExceeded` 绝不静默丢弃）；恢复失败启动时列出 broken 清单。

### 其余决策（已收敛）

- HTTP/SSE（G9 / P2）：**`http` feature 门控引入 axum**（默认关、核心零依赖），对齐 `mcp-stdio` / `skills` 模式。
- 实例宿主方式：**仅库 API 直连 `AgentRuntime`，每实例独立 Engine**；实例间**完全隔离、互不影响**
  （各自独立的会话表、工具集、工作区根目录）。支持把不同任务并行派发给不同实例（如各自操作
  不同代码库互不干扰），也可对同一任务多实例并行对比。
- 实例隔离落地：每个实例的 `InstanceSpec.tools.fs.root` 即其工作区根，实例间文件视图互不可见。
- 模板磁盘装载（G5）：**Phase 3** 做（`templates/` 目录加载 → `TemplateRegistry`）；P1 仅内存覆盖替换。
- 会话落盘 fsync：**周期 flush + 优雅关闭时 flush**（性能优先；强一致开关留作将来）。
- 实例配置格式：**JSON**（零依赖），不引入 TOML（白名单外）。
- daemon 默认值：绑定 `127.0.0.1:7100`；state-dir 默认 `~/.referee/state`。
- `InstanceSpec.id` 为空 → 自动生成 kebab-case（`general-<uuid8>`）。
- provider 对应 feature 未启用 → `ERR_INVALID_SPEC` 显式报错（不静默）。

## 3. 现状盘点（已有能力）

| 层 | 已有 | 代码位置 |
|---|---|---|
| 内核 | Kernel 路由 / 治理 / 背压 / WAL；`Extension` | `referee-core` |
| provider | `LLMProvider` trait、`ChatRequest/Response`、`StreamChunk{Delta,Finish}`、DeepSeek / 小米 / OpenAI 兼容适配器 | `referee-ai-base/src/provider` |
| 引擎 | `Engine`：`chat` / `chat_stream` / `interrupt` / 会话生命周期 / 预算 / 缓存 / Token 计量；`EngineConfig` | `referee-ai-base/src/engine` |
| 会话 | `SessionId`、`ChatPayload`、`ChatOptions`、`SessionReply`、`SessionSnapshot`、`SessionConfig` | `referee-ai-base/src/session` |
| 工具 | `ToolExecutor` / `ToolRegistry`（有界、等待/派发分流、截断/panic 隔离/超时） | `referee-ai-base/src/tool` |
| 观测 | metrics 计数器 + tracing spans | `referee-ai-base/src/observe` |
| 业务封装 | `AgentRuntime`（Extension + 库 API）：`chat_stream` / 会话 / 用量 / 工具注入 | `referee-agent/src/lib.rs` |
| Agent 定义 | `AgentDefinition`（JSON）、`AgentBuilder`、`AgentRegistry`、`BoundAgent.bind_with` | `referee-agent/src/agent` |
| 可替换模板 | `TemplateRef::Named` + `TemplateRegistry`（覆盖替换、`{{var}}` 严格插值） | `referee-agent/src/agent/template.rs` |
| 工具链 | read / write / edit / 成果板 / AgentTool；MCP（feature）、Skills（feature） | `referee-agent/src/tool` |
| 工件 | `ArtifactStore`（ACL、有界、成果板） | `referee-agent/src/artifact` |

## 4. 差距清单

| # | 差距 | 状态 | 影响 |
|---|---|---|---|
| G1 | **无任何入口层** | ✅ 已定（§2）：常驻 daemon + TCP/JSON-RPC 2.0 | 待建 `referee-harness` |
| G2 | **无"实例"抽象与多实例管理** | ✅ 已定（§2）：InstanceManager + 崩溃恢复 | 待建 `instance.rs` / `persist` |
| G3 | **无实例配置装载** | 无"一份实例配置 → 可运行实例"的组合入口 | 无法声明式创建实例 |
| G4 | **无流式传输帧** | `StreamChunk` 非 serde，无跨进程帧定义 | 流式对话无法跨传输层 |
| G5 | **模板无磁盘装载** | `TemplateRegistry` 仅内存覆盖 | "不重编译替换"缺磁盘路径（可选） |
| G6 | **实例生命周期与资源回收** | 无实例级 stop（取消在飞回合、回收） | 实例停止/回收不干净 |
| G7 | **聚合观测** | 无实例级 `InstanceInfo`（状态/指标）视图 | 管理列表缺关键信息 |
| G8 | **缺少集成验证** | 无 daemon / 多实例 / 协议往返测试骨架 | 无法验收 |
| G9 | **HTTP 依赖未决** | 白名单无 HTTP 框架 | 阻断 Web/SSE（P2） |

## 5. 阶段规划

### Phase 1 — `referee-harness` 核心（零新依赖）

落地 G1/G2/G3/G4/G6/G7/G8：新建 `referee-harness` crate，含 `protocol` / `instance` /
`persist`（JSONL 崩溃恢复）/ `transport`（TCP JSON-RPC 2.0）/ daemon 二进制，以及
base 的 `persist` feature 会话落盘 sink。**详细执行见 [REFEREE_HARNESS_IMPL.md](REFEREE_HARNESS_IMPL.md)。**

- 验收要点：多实例并行各自独立；实例列表含指标；stop/remove 干净回收；
  TCP 客户端单轮 + 流式往返；kill -9 后重启可恢复实例与已确认会话事实；
  `cargo test -p referee-harness` + `cargo clippy` 零告警；base 既有测试不回归。

### Phase 2 — HTTP + SSE（G9）

- 依赖（已定）：以 `http` feature 门控引入 axum（默认关，核心零依赖），
  对齐 `mcp-stdio` / `skills` 模式（见 §2 其余决策）。
- 路由（接入同一 `protocol` 层与 `InstanceManager`）：
  - `POST /v1/instances` / `GET /v1/instances` / `DELETE /v1/instances/{id}`
  - `POST /v1/instances/{id}/chat`（单轮）、`POST /v1/instances/{id}/chat/stream`（SSE）
  - `POST /v1/instances/{id}/interrupt`、`GET /v1/instances/{id}/sessions`
- 验收：Web 端可建实例、流式对话、中断；TUI 本地连 `localhost` 同一 daemon。

### Phase 3 — 多实例治理增强（可选）

- 实例级预算（`Engine::with_global_budget` 合并系统级总预算）、配额、状态机
  （Running / Idle / Busy / Stopped）、`/v1/instances/{id}/metrics`。
- 模板磁盘装载（G5）：`templates/` 目录加载 → `TemplateRegistry`，完成"不重编译替换"闭环。

## 6. 待确认项（已全部收敛）

此前所有待定项已定案，见 §2「决策记录」。仅保留一条作为未来可选演进，不阻塞实现：

| 项 | 结论 | 备注 |
|---|---|---|
| 会话落盘 fsync 强一致开关 | 默认周期 flush（已定） | 如需断电零丢失，将来加每次 append fsync 选项 |
| 实例间对等协作（Agent-as-Tool） | 本期不做（已定） | 如需，将来以"挂共享 kernel"方式叠加 |

## 7. 结论

引擎与业务能力已完备，**缺的是入口层与"多实例"这个抽象**（G1/G2 为关键路径）。
Phase 1 以零新依赖把 `referee-harness` 核心落地，即可验收"多实例并行 + 管理 +
非正常中断可恢复"；HTTP/SSE 与 Web/TUI 在 Phase 2 接入。

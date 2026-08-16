# Referee Agent — 开箱即用的完整 Agent 业务封装

> 建立在 `referee-ai`（地基）之上的**业务层**：把 referee-ai 的积木（厂商抽象、
> 会话引擎、工具执行、预算、缓存）组装为可直接使用的 Agent 运行时，并提供业务能力：
> **Agent 定义/配置**（`agent`）、Extension 集成、对等协作（Agent as Tool）、带 ACL 的
> 工件存储与成果板读取工具，以及 MCP 2.0 stdio 客户端桥（按需 feature `mcp-stdio`）
> 与 Agent Skills 注入（按需 feature `skills`）。
>
> **分层**：`referee-core`（内核，通信与治理）→ `referee-ai`（核心支撑积木）
> → `referee-agent`（本模块，业务封装，开箱即用）。

## 1. 定位

| 项 | 说明 |
|----|------|
| 业务层 | 基于 `referee-ai` 组装；referee-ai 提供最小闭环积木，本模块提供「如何把它们变成完整、可用、协作的 Agent」 |
| Agent 定义/配置 | `agent` 模块：`AgentDefinition`（纯数据）+ `AgentBuilder` + `AgentRegistry` + `bind` → `BoundAgent`；能力白名单**封闭默认**（`["*"]`=全部、`["a"]`=白名单、`[]`=无该能力） |
| 可替换模板 | `TemplateRef::Named` 命名槽位 + `TemplateRegistry`（覆盖语义、有界）：不重编译即可替换提示词；`bind_with(templates, vars)` 传递设计（参考 DSH persona 槽位） |
| Extension 集成 | `AgentRuntime` 实现 `referee-core::Extension`，把 referee-ai 引擎接入内核消息路由（`Chat` / `Interrupt`） |
| 业务能力 | 对等/子 Agent 协作（`AgentTool`，Agent as Tool）、ACL 工件存储（`artifact`）、成果板读取工具（`list_my_board` / `read_artifact`） |
| 按需拓展 | MCP 2.0 stdio 客户端桥（`tool::mcp`）以 feature `mcp-stdio` 加载；Agent Skills 注入（`skill`）以 feature `skills` 加载，默认不编译，核心保持轻量 |
| 不预置 | 记忆等业务策略由使用者/二次封装搭建 |

### 启用方式

```toml
# 按需启用业务扩展（协议桥接默认关闭、零新增依赖）
referee-agent = { path = "referee-agent", features = ["xiaomi", "deepseek", "mcp-stdio", "skills"] }
```

`xiaomi` / `deepseek` / `openai` / `anthropic` / `responses` 通过 `referee-agent` 转发到
`referee-ai` 裁剪厂商适配器；`mcp-stdio` / `skills` 为协议与技能注入（默认关闭）。

## 2. 架构

```
┌──────────────────────────────────────────────────────────────┐
│  referee-core：Kernel（路由 / 治理）                           │
└───────────────────────────────┬──────────────────────────────┘
                                │ Envelope (Chat / Interrupt)
┌───────────────────────────────▼──────────────────────────────┐
│  referee-agent：AgentRuntime (implements Extension)            │
│    · handle_chat / handle_interrupt → 转译 referee-ai 引擎调用        │
│    · agent：AgentDefinition / AgentBuilder / AgentRegistry     │
│    · register_peer_tool / with_artifact_store（业务能力）       │
│    · register_artifact_tools（list_my_board / read_artifact）   │
│    · chat_stream / remove_session / list_sessions / session_info│
├──────────────────────────────────────────────────────────────┤
│  referee-ai：Engine（会话引擎，最小闭环）                   │
│    provider │ session │ tool │ store │ budget │ prompt │ cache│
└──────────────────────────────────────────────────────────────┘
```

## 3. 模块

| 模块 | 职责 |
|------|------|
| [`agent`](src/agent/mod.rs) | **Agent 定义/配置**：`AgentId`（可调用、唯一、kebab-case 校验）、`AgentDefinition`（纯数据，可来自 JSON/TOML/builder）、`AgentBuilder`（链式）、`AgentRegistry`（以 `AgentId` 为 key，重复拒绝）、`AgentDefinition::bind` → `BoundAgent`（解析白名单 + 渲染模板为 `SystemSection`） |
| [`agent::template`](src/agent/template.rs) | **可替换模板**（参考 DSH persona 槽位）：`TemplateRef::Named` 命名槽位 + `TemplateRegistry`（有界、`register` 覆盖替换语义）+ `interpolate`（`{{variable}}` 严格插值，未知/畸形 fail-loud） |
| [`AgentRuntime`](src/lib.rs) | `Extension` 实现：接收 `Chat` / `Interrupt` 消息，委托 base `Engine` 驱动回合；观测（会话数 / token / 缓存）；转发会话管理（`remove_session` / `list_sessions` / `session_info`）；`chat_stream` 库级流式；`register_peer_tool` / `register_artifact_tools` |
| [`tool::AgentTool`](src/tool/agent_tool.rs) | 对等/子 Agent 工具（Agent as Tool）：`Local` 分类不占 IO 槽位，同步 RPC 调用目标会话，大结果 ACL 落库；`peer_depth` 嵌套深度限制 |
| [`tool::ArtifactReader`](src/tool/artifact_reader.rs) | 成果板读取工具：`list_my_board` 列本会话板内条目、`read_artifact` 按 ID 凭证读取成果正文（读取路径仍经 ArtifactStore ACL 校验） |
| [`tool::mcp`](src/tool/mcp/mod.rs) | MCP 2.0 stdio 客户端桥（按需 feature `mcp-stdio`）：子进程管理（有界读取/并发分发/取消/停机）、`server/discover` + `tools/list` + `tools/call`、`_meta` 注入 + 版本协商（-32022）、MRTR `InputRequiredResult` 三策略；`McpServer` 把远程工具经 `Tool` 抽象接入注册表 |
| [`artifact`](src/artifact/mod.rs) | 带 ACL 的工件存储：owner / 授权读者读取校验，有界（数量 + 字节双上限），成果板（`BoardId`）组织 |
| [`skill`](src/skill/mod.rs) | Agent Skills 开放标准（SKILL.md）注入（按需 feature `skills`）：极简 frontmatter 解析（零依赖 YAML）、有界注册表（`SkillRegistry`）、关键词路由（含 CJK 匹配）、`render_skill_context` 注入渲染；渐进式披露 L1 元数据 / L2 正文 / L3 资源，零新增依赖 |

## 4. Agent 定义与配置

Agent 是**纯数据**（`AgentDefinition`，可来自 JSON/TOML/builder），装配是**行为**
（`AgentBuilder` 链式构造 + `bind` 解析）。能力白名单**封闭默认**：未声明的能力
不进提示词（"没启用的工具/技能/MCP 就不注入"）。

```rust
use referee_agent::{
    AgentDefinition, AgentId, AgentRegistry, TemplateRef,
};

// 方式一：builder（类型安全）
let def = AgentDefinition::builder()
    .id(AgentId::new("coder")?)
    .description("代码 Agent")
    .model("deepseek/deepseek-v3")
    .template(TemplateRef::DeepSeek)
    .tools(["apply_patch", "grep"])
    .build()?;

// 方式二：声明式（纯数据，可来自 JSON/TOML，serde 载体，零新增依赖）
// let def: AgentDefinition = serde_json::from_str(json_or_toml)?;

// 注册（以 AgentId 为 key，重复 → Duplicate）
let registry = AgentRegistry::with_defaults();
registry.register(def)?;

// 装配：解析白名单 + 渲染模板 → BoundAgent
let bound = registry.get(&AgentId::new("coder")?)?.bind();
```

白名单语义：
- `["*"]` = 继承全部能力
- `["a", "b"]` = 仅白名单内能力
- `[]`（缺省） = 明确无该能力（空则不进提示词）

`BoundAgent` 持有解析后的白名单与渲染后的系统片段（`SystemSection`），可对接
base 的 `prompt::assemble` 分段编排与 `ToolRegistry::declarations_visible` 白名单过滤。

### 可替换模板（参考 DSH persona 槽位）

模板默认是**内联**的（`Generic` / `DeepSeek` / `Claude` / `Inline`）；如需"不重编译
即替换提示词"，用**命名槽位** `TemplateRef::Named` + `TemplateRegistry`：

```rust
use referee_agent::{
    TemplateRegistry, TemplateRef,
};

// 1. 注册表：内置通用智能的默认提示词作为可替换的命名槽位 `general`
let templates = TemplateRegistry::with_builtins();

// 2. 替换提示词（覆盖同名槽位，不重编译；支持 {{variable}} 严格插值）
templates.register("general", "你是我的专属助手，工作目录：{{cwd}}")?;

// 3. 装配（传递设计）：注册表 + 变量 → 解析 Named + 插值 → SystemSection
let bound = def.bind_with(Some(&templates), &[("cwd", "/workspace")])?;
//    bound.system_sections → base prompt::assemble → 模型请求
```

- `register` 为**覆盖**语义（同名替换、不新增），注册表有界（防 OOM）。
- `{{variable}}` 插值对齐 DSH `renderPrompt`：未知 / 畸形引用 **fail-loud**（显式报错），
  孤立的 `{{`（无闭合）保留为字面文本。
- 内置通用智能 [`builtin::general`](src/agent/builtin.rs) 使用命名槽位 `general`，
  其默认提示词引用 `{{cwd}}`（工作目录变量），装配时必须提供。

## 5. 快速上手

```rust
use std::sync::Arc;
use referee_ai::engine::{Engine, EngineConfig};
use referee_ai::provider::deepseek::{DeepSeekConfig, DeepSeekModel, DeepSeekProvider};
use referee_ai::tool::{ExecutorConfig, ToolExecutor};
use referee_agent::AgentRuntime;
use referee_core::{Kernel, SupervisionPolicy};
use uuid::Uuid;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let kernel = Kernel::new();

    // 1. 构造地基引擎（provider + 工具 + 预算 + 缓存）
    let provider = Arc::new(DeepSeekProvider::new(
        DeepSeekModel::V4Pro,
        DeepSeekConfig::new(std::env::var("DEEPSEEK_API_KEY")?),
    )?);
    let executor = ToolExecutor::with_defaults().with_kernel(kernel.clone());
    let engine = Engine::new(provider, EngineConfig::default())
        .with_tools(
            referee_ai::tool::ToolRegistry::with_defaults(),
            executor,
        );

    // 2. 业务封装为内核扩展
    let runtime = AgentRuntime::new(engine);
    let rid = runtime.id();
    kernel
        .register(Box::new(runtime), 64, SupervisionPolicy::Transient)
        .await?;

    // 3. 发起对话（invoke：请求-响应）
    use referee_ai::session::{ChatPayload, Message, SessionMessage};
    let msg = SessionMessage::Chat {
        session_id: Uuid::new_v4(),
        payload: ChatPayload {
            message: Message::user("你好"),
            options: Default::default(),
        },
    };
    let resp_env = kernel.invoke(rid, msg.to_envelope(), 30_000).await?;
    let reply = referee_ai::session::SessionReply::from_envelope(&resp_env)?;
    println!("{reply:?}");
    Ok(())
}
```

### 接入 Agent Skills（`skills` feature）

装载一次、逐轮注入：`skills/` 目录（每个技能一个子目录，含 `SKILL.md`）→ 注册表 →
关键词路由 → 渲染注入到本轮 system prompt：

```rust
use std::path::Path;
use std::sync::Arc;
use referee_agent::skill::{load_root, SkillConfig, SkillRegistry,
                           KeywordRouter, render_skill_context};
use referee_ai::session::ChatOptions;

// 启动时装载一次（有界：单资源/总字节/条数均有上限）
let registry = SkillRegistry::with_defaults();
for s in load_root(Path::new("./skills"), &SkillConfig::default())? {
    registry.register(Arc::new(s))?;
}
let router = KeywordRouter::default();

// 每轮对话前：用用户消息路由 → 渲染 L1/L2 → 拼进本轮 system prompt
let activated = router.select(user_message, &registry.all());
let mut options = ChatOptions::default();
options.system_prompt = Some(format!("你是助手。\n\n{}", render_skill_context(&activated)));
```

技能正文经 `ChatOptions.system_prompt` 注入，再由 base `build_prompt` 做预算截断，
**完全复用现有背压治理**（base 零改动）。Skill 是纯数据注入（渐进式披露），并非工具，
不执行 `scripts/`。

## 6. 设计约束（继承 base + 业务）

- base 保证最小闭环的并发正确性（回合内顺序异步、协作取消、无跨 await 持锁、错误显式可见），并提供流式输出与会话生命周期管理。
- `AgentRuntime.handle` 零阻塞：只做转译 + spawn，回复在派生任务中异步完成。
- 对等能力信任边界：`kernel` / artifact 句柄仅授予可信注册工具（`register_peer_tool`）。
- 成果读取工具读取路径仍经 ArtifactStore ACL 强制校验（凭证式 ID 不代表越权）。
- `referee-core` 零改动。

## 7. 测试

```bash
cargo test -p referee-agent                    # 库单测（agent 定义/注册/白名单 + artifact ACL + 成果读取）+ 集成（peer 对等协作 6 条）
cargo test -p referee-agent --features mcp-stdio   # MCP 2.0 协议单测（_meta 注入 / 版本协商 / discover / tools/call / MRTR）
cargo test -p referee-agent --features skills      # Agent Skills（frontmatter / 注册 / 路由 / 端到端注入 build_prompt）
cargo clippy -p referee-agent --all-targets -- -D warnings
```

## 8. 相关文档

| 文档 | 说明 |
|------|------|
| [`../README.md`](../README.md) | 仓库总览 |
| [`../referee-ai/README.md`](../referee-ai/README.md) | 地基模块（核心支撑能力，含提示词分段编排与用量计量） |
| [`../referee-core/README.md`](../referee-core/README.md) | 内核模块 |
| [`../REFACTOR_PLAN.md`](../REFACTOR_PLAN.md) | 重构执行规划（分层边界与验收口径） |
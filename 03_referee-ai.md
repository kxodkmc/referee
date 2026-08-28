# referee-ai 代码审查报告

- **审查对象**：`referee-ai`（commit `a56c428`）
- **模块定位**：Agent Runtime 核心——provider（厂商抽象 13 文件 5407 行）、engine（会话引擎）、session（状态机）、prompt（组装与预算截断）、tool（执行器）、cache、budget、observe
- **本次更新重点**：engine 可观测性（observer）、prompt 裁剪重构、新增 anthropic_compat/sse/minimax/openai/openrouter、上下文窗口支持
- **审查方式**：全量细读 engine/session/prompt/sse/cache/openai_compat，其余 provider 抽样对照；`cargo clippy --workspace --all-targets` 全绿（13 条警告已归类）

## 结论摘要

| 编号 | 类型 | 严重度 | 一句话描述 | 位置 |
|------|------|--------|-----------|------|
| AI-1 | 缺陷 | 高 | 事实日志容量满后会话永久不可用，且被误报为 Busy | session/mod.rs:362-369, engine/mod.rs:461-464 |
| AI-2 | 缺陷 | 中 | SSE 解析器不兼容 `\r\n\r\n` 分隔符，与模块文档声明矛盾 | provider/sse.rs:96-110 |
| AI-3 | 缺陷 | 中 | Token 估算不计 tool_calls/reasoning_content，重工具场景预算截断失效 | prompt/mod.rs TokenEstimator |
| AI-4 | 缺陷 | 中 | 响应缓存默认开启但对会话负载永不命中，白付每轮全量序列化成本 | cache/mod.rs:57-64, 182-191 |
| AI-5 | 缺陷 | 中 | engine 层重试与 provider 层重试无退避叠加，限流场景雪崩放大 | engine/mod.rs llm_call_with_retry |
| AI-6 | 缺陷 | 中 | dispatch 后台结果注入时会话已移除则静默丢弃，无日志 | engine/mod.rs:877-900 |
| AI-7 | 缺陷 | 低 | resume 循环内 push 失败仅 warn，工具结果丢失可致模型循环调用 | session/mod.rs append_tool_results |
| AI-8 | 缺陷 | 低 | Retry-After 头只支持整数秒，HTTP-date 格式静默忽略 | openai_compat.rs:380-385 |
| AI-9 | 缺陷 | 低 | max_completion_tokens 硬编码，旧 OpenAI 兼容网关 400/忽略 | openai_compat.rs:258 + agnes.rs:162 补丁佐证 |
| AI-10 | 设计 | 中 | 7 个 vendor 适配器各复制 ~200 行，真实差异 <15 行 | agnes/deepseek/kimi/xiaomi/minimax/openai/openrouter |
| AI-11 | 设计 | 中 | 异步工具结果伪装 user 消息注入，模型视角语义混乱 | session/mod.rs flush_injections |
| AI-12 | 设计 | 低 | 预留参数 `wait` 静默剥离，与用户工具同名参数冲突无声明 | tool/executor.rs strip_wait |
| AI-13 | 设计 | 低 | 流式路径无缓存，与 `chat()` "语义等价" 声明不完全一致 | engine/stream.rs |
| AI-14 | 杂项 | 低 | clippy 13 条警告（含测试代码 MutexGuard 跨 await） | 见附录 |

---

## 缺陷详情

### AI-1【高】会话事实日志容量满 → 永久 Busy 死局

**位置**：`src/session/mod.rs:362-369`（start_round）→ `src/engine/mod.rs:461-464`

**链条**：
```
SessionLog::append 满 → LogError::CapacityExceeded
  → Session::start_round 内 push_history 失败 → return None
    → engine: None.ok_or(EngineStartError::Busy)
      → 上游（含 referee-channel Dispatcher）按 Busy 处理：回队重试 → 再 Busy → 放弃并提示用户"稍后再发"
```

**影响**：`max_events` 默认 4096（`session/mod.rs:169`）。重工具会话（每轮 = user + assistant + N×tool 结果，一轮 20-40 条事实）约 100-200 回合触顶。之后该会话**永久不可用**：每次发消息都报 Busy，用户重试永远失败，且错误信息完全误导（提示"忙"而非"会话已满"）。`SessionLog` 未提供 compact/裁剪/归档 API（`log.rs` 全文确认），唯一出路是上层主动 `remove_session` 重建——但上层从 Busy 回信中无法得知需要这么做。

**修复建议**：① `start_round` 对 `CapacityExceeded` 返回独立错误变体（如 `SessionFull`），映射到 `ErrorKind::Internal` 并携带明确 message；② `SessionLog` 提供 `compact(keep_last: usize)` 语义（保留窗口内事实，丢弃头部）作为正常降级路径；③ 该场景 metrics 计数暴露。

### AI-2【中】SSE 解析器不兼容 `\r\n\r\n` 事件分隔符

**位置**：`src/provider/sse.rs:96-110`（take_sse_event）

模块头注释（第 5 行）声称"缓冲字节直到出现完整事件（`\n\n` / `\r\n\r\n` 分隔）"，实现只匹配 `buf[i] == \n && buf[i+1] == \n`。对 `\r\n\r\n` 字节序列（`\r \n \r \n`），相邻两个 `\n` 之间隔 `\r`，**永远无法切出事件**。第 96 行注释"兼容 \r\n\r\n：剔除尾部 \r"实际只在已找到 `\n\n` 后剔除事件尾 `\r`，处理不了分隔符本身是 CRLF 的流。

**后果**：CRLF 风格 SSE 流（部分代理/网关）流式场景一个事件都发不出；流结束后 `parse_data_field` 把缓冲内**所有**事件的 data 行 `join("\n")` 成单个字符串 → JSON 解析失败 → `LlmError::Protocol`，错误信息指向 JSON 而非真实原因（分隔符不识别）。

**修复建议**：`take_sse_event` 扫描时将 `\r\n` 归一为 `\n`（或匹配四种规范分隔符 `\n\n` / `\r\r` / `\r\n\r\n`）。补一个 CRLF 端到端测试。

### AI-3【中】Token 估算盲区：tool_calls 与 reasoning_content 计 0

**位置**：`src/prompt/mod.rs`（TokenEstimator::estimate / estimate_message）

估算只对 `content.as_text()` 计字符。两类真实载荷完全漏计：
- **`assistant.tool_calls[].function.arguments`**：工具调用参数 JSON——`write_file`/`edit_file` 类工具的参数可达数千 token（整个文件内容），是 agent 负载中最大的单条消息之一；
- **`reasoning_content`**：推理模型的思考输出（有的厂商数百~数千 token）。

**后果**：重工具/推理模型场景下估算值可能低估数倍，`prompt_budget`（默认 128K）截断在估算达标的情况下实际已超窗，最终靠 engine 的 `ModelSpec.context_window_tokens` 硬护栏 fail-loud 拒绝——用户看到的是回合失败而非优雅裁剪。预算防线形同虚设。

**修复建议**：estimate_message 覆盖 `tool_calls`（name + arguments 长度）与 `reasoning_content`；单测锁定两类消息估算非零。

### AI-4【中】响应缓存默认开启，会话负载下纯开销

**位置**：`src/cache/mod.rs:57-64`（Default: enabled=true, capacity=1000）、`182-191`（key_for_request）

缓存键 = hash(完整 messages + tools JSON + params)。多轮会话**每轮 history 都在增长**，键逐轮不同 → 命中率≈0；但每轮都要 `serde_json::to_string(&req.tools)` + `hash_request(&req.messages, ...)`（完整序列化全部历史消息，长会话单次数百 KB～MB 级），再走 LRU 维护。有效场景仅剩"同一请求原地重试"。

**修复建议**：默认 `enabled=false`；或在 engine 会话路径提供"仅末轮请求哈希"的轻量键（last-message + params）；文档写明适用场景（幂等重试/无状态问答）。

### AI-5【中】双层重试无退避叠加

**位置**：`src/engine/mod.rs`（llm_call_with_retry）× `src/provider/openai_compat.rs:100-110`（compute_backoff）

provider 底座已对 `Network/Server/RateLimited` 做指数退避重试（RetryPolicy）；engine 层 `max_retries`（默认 1）对同类错误**立即**再发起一轮完整调用（无间隔）。最坏情形：`RateLimited` 时 engine 触发 provider 整条重试链（N 次退避请求）耗尽后返回，engine 不等待再复制一遍 → 总请求 = (1+provider_retries)×(1+engine_retries)，且第二波无退避，与限流窗口冲突。

**修复建议**：engine 层重试前按 provider 返回的 `retry_after`（或自身退避）等待；或 engine 层只重试 provider 已放弃的确定性场景之外的类别并文档化叠加关系。

### AI-6【中】dispatch 后台结果注入的静默丢弃分支

**位置**：`src/engine/mod.rs:877-900`（dispatch 监控 spawn 块）

```rust
if let Some(mut s) = engine.sessions.get_mut(&sid) {
    s.inject_tool_result(...)   // 会话已移除时此分支整体跳过
}
// 无 else —— 工具已执行完、消耗了预算与时间，结果无声消失
```

会话超时回收/容量驱逐/上层主动 remove 都会造成该窗口。观测侧 `on_tool_finished` 照常触发（结果看起来"成功"），但结果从未进入任何会话。

**修复建议**：补 `else { tracing::warn!(...) + metrics }`；文档写明 dispatch 结果的交付语义是 best-effort。

### AI-7【低】resume 循环中事实写入失败被降级为 warn

**位置**：`src/session/mod.rs`（finish_thinking / append_tool_results 的 push_history 错误分支）

AI-1 的姊妹问题：回合**中途**（assistant 带 tool_calls 落 history、tool 结果落 history）push 失败仅 `error!/warn!`，循环继续。模型下一轮看不到自己上一轮的工具调用与结果 → 大概率再次发起相同调用 → 再次失败写入 → 循环直至 thinking_timeout。表现为"回合超时"，根因（日志已满）被淹没在循环日志里。

**修复建议**：回合内 push 失败应立即终止回合并返回带原因的 Error（宁可失败也不要语义残缺的继续）。

### AI-8【低】Retry-After 仅支持整数秒

**位置**：`src/provider/openai_compat.rs:380-385`

`.parse::<u64>()` 对 HTTP-date 格式（`Fri, 28 Aug 2026 11:00:00 GMT`，RFC 7231 允许）解析失败 → `retry_after=None` → 走默认指数退避。功能降级非错误，但对尊重 Retry-After 的强限流端点（Anthropic/OpenAI 均会用 date 格式）会低估等待时间。建议补 HTTP-date 解析或至少日志记录未识别格式。

### AI-9【低】`max_completion_tokens` 硬编码写入共享底座

**位置**：`src/provider/openai_compat.rs:258`；反证：`agnes.rs:159-165` 已在 vendor 层 `remove("max_completion_tokens")` 打补丁

共享底座统一写 OpenAI 新参数名，vLLM/Ollama/部分国产网关只认 `max_tokens` → 要么 400 要么静默忽略上限。agnes 的 remove 补丁证明痛点真实。建议底座改为可配置参数名（`max_tokens_param: MaxTokensStyle`），去掉 vendor 层补丁。

---

## 设计问题详情

### AI-10【中】vendor 适配器的复制粘贴矩阵

**位置**：`agnes.rs`(267) / `deepseek.rs`(298) / `kimi.rs`(255) / `minimax.rs`(282) / `openai.rs`(233) / `openrouter.rs`(280) / `xiaomi.rs`(251)

7 个文件结构完全同构：`Config struct + build_body（10-20 行真差异）+ From<Config> for ProviderRegistryEntry + LLMProvider impl（转发 OpenAiCompatClient/AnthropicClient）+ tests`。逐文件 diff 确认真实差异集中在：base_url、model 名、thinking 字段映射（enabled→各厂商字面量）、2-3 个特殊 body 键。其余 ~180 行/文件是逐字重复（含各自复制的测试样板）。

**建议**：提取声明式 `VendorSpec { id, base_url, default_model, context_window, thinking: ThinkingMap, patch: fn(&mut Value) }`，一个通用 `impl LLMProvider for GenericOpenAiVendor` 消费它；vendor 文件萎缩为 20-40 行纯数据 + 特例 patch。openrouter 的 extra headers、agnes 的 max_tokens 补丁都是 patch 函数的天然用例。**收益**：新增 OpenAI 兼容厂商从 250 行降到 30 行，修 bug（如 AI-8/9）只改一处。

### AI-11【中】异步工具结果的"user 消息伪装"

**位置**：`src/session/mod.rs`（flush_injections）

dispatch 类工具完成的结果以 `Message::user("[async tool 'x' completed]\n<result>")` 注入下一回合。模型视角：一个从未调用过工具的"用户"声称工具完成——① 模型可能模仿该格式伪造注入；② 无法与真实用户输入区分；③ 与 OpenAI/Anthropic 的 tool 消息语义脱节。当前设计规避了"主动触发 LLM"（合理动机），但注入格式可至少用系统约定前缀 + 文档化，并在 prompt 组装层明确告知模型该格式的含义。

### AI-12【低】保留参数 `wait` 的静默劫持

**位置**：`src/tool/executor.rs`（strip_wait，测试 `reserved wait key must be stripped` 佐证）

引擎层把工具参数中的 `"wait"` 键剥离用于等待/派发分流。若工具作者自己的工具恰好有 `wait` 业务参数（如轮询类工具），该参数被静默吞掉，工具行为异常且难以排查。建议：文档显式声明保留字；或改用前缀键 `__wait` 之类不易冲突的名字。

### AI-13【低】流式路径无缓存

`run_chat_inner` 有 cache get/set，`stream_loop`（engine/stream.rs）完全没有。trait 文档（provider/mod.rs:610）声称"chunk 收敛后必须与 chat() 语义等价"，缓存行为不对齐（同一请求流式消费后，后续非流式调用不命中）。若 AI-4 采纳"默认关闭"则此项自然消解；否则需补齐或文档声明差异。

### AI-14【低】clippy 警告清单（全部为警告级，无一 blocking）

| 位置 | 警告 | 评价 |
|------|------|------|
| engine/tests.rs:1350 | MutexGuard 跨 await（parking_lot 锁） | 测试代码，并发下可能自锁，建议改先 clone 再 await |
| prompt/mod.rs:389 | too_many_arguments (9/7) | 参数对象化（PromptParts 已存在，补一个 FinalizeArgs） |
| tool/executor.rs:197 | too_many_arguments (8/7) | execute_batch 参数对象化 |
| session/message.rs:41 | large_enum_variant | Chat 变体 Box 化 payload |
| provider/registry.rs:17 | unused import LlmError | 清理 |
| anthropic_compat.rs:604 | match → let 简化 | 风格 |
| channel-wechat/lib.rs:225 | collapsible_if | 风格 |

---

## 运行时验证证据

- **AI-1 已复现**：独立测试 crate（`max_events=3` 的会话走完 2 回合后，第 3 回合 `engine.chat()` 返回 `EngineStartError::Busy`），断言通过：`test capacity_full_misreported_as_busy ... ok`。
- **AI-2 已复现**：`take_sse_event` 逻辑独立编译验证——LF 流切出 2 个事件；CRLF 流（`data: {...}\r\n\r\n` × 2）切出 **0 个事件**，缓冲堆积至流结束，多事件 data 行合并后 JSON 解析必然失败。补充细节：单事件 CRLF 流会在流结束 flush 时"歪打正着"解析成功，因此该缺陷在简单手测中不可见，多事件流才暴露。
- **全库测试基线**：`cargo test --workspace` 352 passed / 0 failed；`cargo clippy --workspace --all-targets` 0 error / 13 warnings（已全部归类入上表）。所有报告问题均未被现有测试覆盖。

## 值得肯定的点

- 会话状态机把"事实日志（append-only）"与"模型可见窗口（tail 派生）"分离的设计干净，容量拒绝不静默丢弃的立场明确（问题只在错误上报语义，见 AI-1）。
- observer 契约文档（非阻塞、catch_unwind、数据/行为分离）与实现一致；`observe_chunk_deltas` 作为双路径唯一推送点避免了流式/非流式观测分叉。
- prompt 组装的角色修正（截断后首条不能是 tool/空 assistant）考虑了厂商 400 的现实坑，测试覆盖到位。
- `sse_fold_into_response` 作为双协议共享收敛器，工具调用增量按 index 累积的实现（含跨 chunk 分片 arguments 拼接）正确且有测试锁定。

## 修复优先级建议

1. **AI-1**（含 AI-7）——会话可用性死局，用户可直接感知；
2. **AI-3**——预算防线失效是超窗故障的前置根因；
3. **AI-2**——对接非主流网关前的必修项；
4. **AI-4/5**——性能与稳定性，可一次改动（缓存默认值 + 重试退避）；
5. **AI-10**——重构收益最大，建议独立 PR；
6. 其余随迭代。

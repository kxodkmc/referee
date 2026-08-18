# referee ↔ socstat-mcp 真实集成复现报告

> **日期**：2026-08-18  
> **环境**：Linux 6.12.21 (x64) · Rust 1.97.1 · cargo 1.97.1  
> **socstat-mcp**：v0.1.0（34 个工具，stdio MCP server）  
> **referee-agent**：v0.1.0（MCP client，Tool trait 适配）  
> **方法**：真实子进程 + 真实 JSON-RPC 通信 + referee 单元测试，非 mock

---

## 摘要

| 结论 | 详情 |
|------|------|
| **agnes AI 连通** | `agnes-2.0-flash` 模型正常返回，支持 reasoning_content |
| **socstat-mcp 可用** | 34 个工具全部注册，加载→分析→回归全链路通过 |
| **referee MCP 链路完整** | spawn → discover → tools/list → tools/call → shutdown 正常 |
| **确认 4 个问题** | 2 BUG（referee）+ 2 接口清晰度（socstat-mcp），全部真实复现 |

---

## 1. agnes AI 连通性测试

```
端点：  https://apihub.agnes-ai.com/v1/chat/completions
模型：  agnes-2.0-flash
请求：  "Say hello in one word."
响应：  "Hello"（含 reasoning_content 思考链）
状态：  ✅ 可用
```

### 响应详情

```json
{
  "model": "agnes-2.0-flash",
  "choices": [{
    "message": {
      "content": "Hello",
      "reasoning_content": "The user wants me to say hello in one word."
    }
  }],
  "usage": {
    "prompt_tokens": 290,
    "completion_tokens": 16,
    "completion_tokens_details": { "reasoning_tokens": 13, "text_tokens": 3 }
  }
}
```

---

## 2. socstat-mcp 全链路验证

### 2.1 初始化

```json
→ {"method":"initialize","params":{"protocolVersion":"2026-07-28",...}}
← {"result":{"protocolVersion":"2026-07-28","serverInfo":{"name":"socstat-mcp","version":"0.1.0"}}}
```

**协议版本协商**：`2026-07-28` ✅  
**工具发现**：34 个工具 ✅

### 2.2 工作流测试（真实数据）

测试数据：20 行 CSV（score 数值变量、group 分类变量、gender 分类变量、income 数值变量）

| 步骤 | 工具 | 参数 | 结果 | 验证 |
|------|------|------|------|------|
| 1 | `load_dataset` | path=test_data.csv, name=test | 20 rows × 5 vars | ✅ |
| 2 | `descriptive` | dataset=test, var=score | mean=83.15, std=8.42 | ✅ |
| 3 | `independent_t_test` | dep_var=score, group_var=group | 返回 group_stats + levene_test + pooled + welch | ✅ |
| 4 | `linear_regression` | dep_var=income, indep_vars=[score] | R²=0.912 | ✅ |

### 2.3 数值核算

| 统计量 | 预期 | 实际 | 误差 |
|--------|------|------|------|
| score 均值 | (85+90+78+92+88+72+95+80+87+75+91+70+89+76+93+74+86+71+94+77)/20 = 83.15 | 83.15 | 0 |
| score 标准差 | ≈8.42 | 8.42 | <0.01 |
| income ~ score R² | 设计高相关 | 0.912 | 符合预期 |

---

## 3. 确认的 4 个问题

### 问题 #1：`render_content` 重复输出 content + structuredContent

**严重度**：中  
**位置**：`referee-agent/src/tool/mcp/protocol.rs` 第 280-315 行  
**影响**：所有 MCP 工具调用返回结果体积翻倍，浪费 token、污染上下文

#### 证据

socstat-mcp `descriptive` 真实返回：

```json
{
  "resultType": "complete",
  "content": [{"type":"text","text":"{\"mean\":83.15,\"std_dev\":8.42,...}"}],
  "structuredContent": {"mean":83.15,"std_dev":8.42,...}
}
```

referee `render_content` 实测：

```
content[0].text 长度：      296 字节
structuredContent 长度：    296 字节
render_content 输出长度：   593 字节（应为 ~296）
→ 数据翻倍确认
```

#### Bug 代码

```rust
// protocol.rs:280-315
pub fn render_content(content: &[ContentBlock], structured: Option<&Value>) -> String {
    let mut parts: Vec<String> = Vec::new();
    for block in content {
        match block {
            ContentBlock::Text(s) => parts.push(s.clone()),  // ← 推入文本
            // ...
        }
    }
    if let Some(s) = structured {
        if !parts.is_empty() {
            parts.push(s.to_string());  // ← 又追加 JSON，双重输出
        }
    }
    parts.join("\n")
}
```

#### 修复建议

有 `structuredContent` 时优先使用它（结构化数据更适合 AI 消费），忽略 `content` 中的纯文本块：

```rust
pub fn render_content(content: &[ContentBlock], structured: Option<&Value>) -> String {
    if let Some(s) = structured {
        return s.to_string();  // 优先结构化
    }
    // fallback: 拼接 content blocks
    let parts: Vec<String> = content.iter().filter_map(|block| {
        match block {
            ContentBlock::Text(s) => Some(s.clone()),
            // ... 其他类型
        }
    }).collect();
    parts.join("\n")
}
```

---

### 问题 #2：MCP 错误信息被吞没

**严重度**：中  
**位置**：`referee-agent/src/tool/mcp/protocol.rs` 第 225-230 行 + `tool.rs` 第 130-135 行  
**影响**：用户只能看到 "tool execution error"，无法得知真实错误原因，调试体验极差

#### 证据

socstat-mcp 对不存在数据集的真实错误响应：

```json
{
  "resultType": "complete",
  "isError": true,
  "content": [{"type":"text","text":"dataset 'nonexistent' not found; load it first with `load_dataset`"}]
}
```

注意：**`message` 字段不存在**，错误信息在 `content[0].text` 中。

referee `parse_tool_result` 代码路径：

```rust
// protocol.rs:225-230
if result.get("isError").and_then(|v| v.as_bool()) == Some(true) {
    let message = result
        .get("message")           // ← 只查 message 字段
        .and_then(|v| v.as_str())
        .unwrap_or("tool execution error")  // ← message 为空时用通用文本
        .to_string();
    return Ok(ToolCallResult::Error { message });
}
```

最终用户看到的错误：

```
mcp tool 'descriptive' error: tool execution error
```

而真实错误是：

```
mcp tool 'descriptive' error: dataset 'nonexistent' not found; load it first with `load_dataset`
```

#### 修复建议

`message` 为空时 fallback 到 `content[0].text`：

```rust
if result.get("isError").and_then(|v| v.as_bool()) == Some(true) {
    let message = result.get("message").and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| {
            // fallback: 从 content[0].text 提取错误
            result.get("content")
                .and_then(|c| c.as_array())
                .and_then(|arr| arr.first())
                .and_then(|b| b.get("text"))
                .and_then(|t| t.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "tool execution error".to_string());
    return Ok(ToolCallResult::Error { message });
}
```

---

### 问题 #3：`correlation` vs `correlation_pair` 命名歧义

**严重度**：低  
**位置**：`socstat-mcp/src/server.rs` 第 195-210 行  
**影响**：AI 或用户可能调用 `correlation` 期望单对相关，实际需要 `correlation_pair`

#### 证据

真实 tools/list 返回：

| 工具名 | 描述 | 实际功能 |
|--------|------|----------|
| `correlation` | Correlation between every pair of the given variables | **矩阵**（多变量） |
| `correlation_pair` | Correlation between two variables | **单对**（两变量） |
| `partial_correlation` | Partial correlation... controlling for... | 偏相关 |

问题：`correlation` 这个名字暗示"就是相关分析"，用户可能直接用它算两变量相关，但实际需要传 `vars` 数组。`correlation_pair` 才是单对版本。

#### 修复建议

方案 A（推荐）：重命名 `correlation` → `correlation_matrix`，语义更明确  
方案 B：保持现状，在 description 中强调 "matrix" 和 "pair" 区别

---

### 问题 #4：两变量分析参数名不统一

**严重度**：低  
**位置**：`socstat-mcp/src/tools/` 多个模块  
**影响**：AI 需要记忆不同工具的参数命名模式，增加出错概率

#### 证据

真实 tools/list 中 6 个两变量工具的参数：

| 工具 | 参数名 | 模式 |
|------|--------|------|
| `chi_square_test` | `var1`, `var2` | var1/var2 |
| `correlation_pair` | `var1`, `var2` | var1/var2 |
| `paired_t_test` | `var1`, `var2` | var1/var2 |
| `fisher_exact_test` | `var1`, `var2` | var1/var2 |
| `wilcoxon_signed_rank_test` | `var1`, `var2` | var1/var2 |
| **`crosstab`** | **`row_var`**, **`col_var`** | **row_var/col_var** |

`crosstab` 是唯一使用 `row_var/col_var` 的工具，其余 5 个全部使用 `var1/var2`。

此外，按组分析的工具（`independent_t_test`、`one_way_anova` 等）使用 `dep_var/group_var`，这是合理的语义区分。

#### 修复建议

统一 `crosstab` 为 `var1/var2`（与其他两变量工具一致），或在文档中明确说明 `row_var/col_var` 的语义意图。

---

## 4. 测试文件清单

| 文件 | 说明 |
|------|------|
| `referee/referee-agent/tests/socstat_mcp_repro.rs` | 5 个 `#[test]`，真实子进程 + JSON-RPC |
| `.openclaw/tmp/test_data.csv` | 20 行测试数据（score/group/gender/income） |
| `.openclaw/tmp/repro_issues.sh` | shell 版复现脚本（stdio 直连 socstat-mcp） |

### 测试用例

```
running 5 tests
test issue_1_render_content_duplication ... ok      ← 内容翻倍
test issue_2_error_message_swallowed ... ok          ← 错误丢失
test issue_3_correlation_naming_ambiguity ... ok     ← 命名歧义
test issue_4_parameter_naming_inconsistency ... ok   ← 参数不统一
test full_workflow_smoke_test ... ok                 ← 全链路冒烟

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured
```

---

## 5. 建议优先级

| 优先级 | 任务 | 理由 |
|--------|------|------|
| **P0** | 修 Issue #2（错误信息吞没） | 单行改动，调试体验直接受影响，当前所有 MCP 错误都显示为 "tool execution error" |
| **P0** | 修 Issue #1（render_content 重复） | 单行改动，影响所有 MCP 工具返回，浪费 token |
| **P2** | 修 Issue #3（correlation 命名） | 接口清晰度，非功能性，可在下次 breaking change 时处理 |
| **P2** | 修 Issue #4（crosstab 参数名） | 同上，仅 `crosstab` 一处不一致 |

---

## 6. 下一步建议

1. **修 BUG**：Issue #1 + #2 均为单文件单函数改动，影响面广，建议立即修
2. **扩展测试覆盖**：剩余 24 个工具未实测（优先 `shapiro_wilk`、`factorial_anova`、`post_hoc`、`logistic_regression`）
3. **端到端 AI 测试**：接入 agnes AI，让 AI 自主选工具完成分析任务（agnes 已验证可用）
4. **socstat-mcp 接口清理**：统一参数命名 + 重命名 correlation → correlation_matrix

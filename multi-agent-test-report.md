# 多智能体（Agent as Tool）端到端测试报告

> **日期**：2026-08-18  
> **agnes 模型**：agnes-2.0-flash（含 reasoning_content）  
> **MCP 服务**：socstat-mcp v0.1.0（34 个工具）  
> **架构**：Main Agent → Sub-Agent Tool → MCP Tools → 统计计算  
> **方法**：真实 agnes API + 真实 MCP 子进程，零 mock

---

## 架构

```
Main Agent (agnes)
  ├── "analyst" sub-agent     ← 统计分析（独立 MCP 实例）
  ├── "writer" sub-agent      ← 报告撰写（独立 MCP 实例）
  ├── "sales_expert" sub-agent ← 销售数据分析
  └── "survey_expert" sub-agent ← 调查数据分析
```

每个 Sub-Agent 是独立的 agnes AI 实例，有自己的 system prompt、工具列表、独立的 socstat-mcp 进程。

---

## 测试结果

| 测试 | 场景 | 结果 | 委派次数 | 子智能体工具调用 | AI 轮次 |
|------|------|------|---------|----------------|---------|
| 1 | 单一委派：主→分析师 | ⚠️ PARTIAL | 7 | 210 | 5 |
| 2 | 链式委派：主→分析师→报告员 | ✅ PASS | 2 | 18 | 2 |
| 3 | 并行委派：主→销售+调查同时 | ✅ PASS | 2 | 13 | 2 |
| 4 | 错误恢复：子智能体自适应 | ⚠️ PARTIAL | 7 | 222 | 5 |
| **合计** | | **2 PASS / 2 PARTIAL** | **18** | **463** | |

---

## Test 1：单一委派

**任务**：分析临床试验数据，t 检验比较治疗组血压变化

**AI 行为**：
- 主智能体正确识别需要委派给 analyst 子智能体
- 子智能体调用了 8~46 个工具/轮
- 子智能体尝试 `filter(var="group", value="treatment")` → 失败（filter 只接受数值，不接受字符串）
- 子智能体尝试 `compute` 创建数值编码变量来绕过

**问题**：子智能体在 filter 工具上反复失败（字符串 vs 数值），浪费大量轮次

**主智能体最终行为**：尝试了 7 次委派，每次用不同策略描述任务，表现出良好的容错和重试能力

---

## Test 2：链式委派 ✅

**任务**：ANOVA 比较教学方法 → 写报告

**AI 行为**：
- Round 1：同时委派给 analyst（ANOVA 分析）和 writer（报告撰写）
- analyst：`one_way_anova` + `post_hoc` → F(2,147)=16.72, p<0.001 ✅
- writer：拿到结果后直接写报告 ✅
- Round 2：主智能体综合两者输出

**主智能体最终输出**：

> ## ANOVA Results: Student Scores by Teaching Method
> 
> | Method | Mean | SD | n |
> |--------|------|-----|---|
> | Traditional | 70.98 | 7.02 | 50 |
> | Online | 79.85 | 11.97 | 50 |
> | Hybrid | 79.35 | 9.55 | 50 |
> 
> F(2,147) = 16.72, p < 0.001 → 教学方法对成绩有显著影响

**评估**：✅ 链式委派完美工作，2 轮完成

---

## Test 3：并行委派 ✅

**任务**：同时分析销售数据（回归）和调查数据（ANOVA）

**AI 行为**：
- Round 1：同时调用 sales_expert 和 survey_expert（并行）
- sales_expert：`linear_regression` + `correlation` + `vif` → R²=0.97 ✅
- survey_expert：`one_way_anova` + `chi_square_test` + `frequencies` ✅
- Round 2：主智能体综合两个子智能体的结果

**并行执行证据**：两个子智能体同时启动，总耗时 ≈ max(10.5s, 49.4s) = 49.4s（而非 10.5+49.4=59.9s）

**评估**：✅ 并行委派完美工作，1 轮完成两个独立分析

---

## Test 4：错误恢复

**任务**：偏态数据，先检查正态性再选检验方法

**AI 行为**：
- 子智能体尝试 Shapiro-Wilk 检验各部门收入的正态性
- 遇到 filter 工具无法按部门筛选（字符串值）的问题
- 子智能体尝试了多种替代方案（compute、frequencies 等）

**问题**：与 Test 1 相同——`filter` 工具不支持字符串值筛选

---

## 发现的问题

### 问题 A：`filter` 工具不支持字符串值筛选（高优先级）

**位置**：socstat-mcp `filter` 工具  
**现象**：AI 调用 `filter(var="group", op="eq", value="treatment")` → 失败，因为 `value` 是 `double` 类型  
**影响**：子智能体无法按分类变量筛选数据，导致大量轮次浪费  
**根因**：`filter` 的 `value` 字段定义为 `format: "double"`，只能接受数值  
**建议**：增加字符串值支持，或提供 `filter_categorical` 工具

### 问题 B：子智能体反复尝试加载已存在数据集（中优先级）

**位置**：子智能体 system prompt 与 AI 行为  
**现象**：尽管 prompt 说 "Dataset 'X' is already loaded"，子智能体仍多次尝试 `load_dataset`  
**影响**：浪费 3-5 个工具调用/轮  
**建议**：在 prompt 中更强调 "NEVER call load_dataset"，或在工具层面返回更明确的提示

### 问题 C：子智能体 max_rounds 导致结果丢失（中优先级）

**位置**：子智能体循环限制  
**现象**：子智能体达到 max_rounds(4) 时返回 "(max)" 而非已有结果  
**影响**：主智能体收到空结果，无法利用已完成的分析  
**建议**：达到 max_rounds 时返回最后一条 assistant 消息的内容而非 "(max)"

### 问题 D（已知）：错误信息被吞没

**现象**：子智能体调用失败时只看到 "tool execution error"  
**影响**：子智能体无法根据错误信息调整参数

---

## 多智能体能力评估

| 维度 | 评分 | 说明 |
|------|------|------|
| **工具发现** | ⭐⭐⭐⭐⭐ | 主智能体正确发现并使用子智能体工具 |
| **委派决策** | ⭐⭐⭐⭐⭐ | 4/4 场景都正确选择了委派策略 |
| **并行执行** | ⭐⭐⭐⭐⭐ | Test 3 两个子智能体真正并行运行 |
| **链式协作** | ⭐⭐⭐⭐⭐ | Test 2 分析师→报告员链式完美工作 |
| **容错重试** | ⭐⭐⭐⭐ | 主智能体遇到失败后换策略重试 |
| **子智能体自主性** | ⭐⭐⭐ | 子智能体能选对工具，但被 filter 问题卡住 |
| **综合** | ⭐⭐⭐⭐ | 多智能体架构可用，需修 filter 工具 |

---

## referee AgentTool 验证（Rust 单元测试）

referee 的 `peer_test.rs` 已有 6 个验收测试，全部通过：

| # | 测试 | 验证点 |
|---|------|--------|
| 1 | `resource_pool_deadlock_fixed` | AgentTool 为 Local 不占 IO 槽位，避免死锁 |
| 2 | `cyclic_call_rejected` | A→B→A 循环调用被 Busy 拒绝 |
| 3 | `artifact_board_end_to_end` | 成果落库 + ACL 读取 |
| 4 | `peer_registration_parallel` | 并行调用两个子智能体 |
| 5 | `async_dispatch_peer_result_injected_next_turn` | 异步派发 + 下轮注入 |
| 6 | `subagent_nesting_depth_limit_chain` | 嵌套深度限制 A→B→C→D |

---

## 建议优先级

| 优先级 | 任务 | 理由 |
|--------|------|------|
| **P0** | 修 `filter` 支持字符串值 | 直接影响子智能体分析能力 |
| **P1** | 子智能体达到 max_rounds 时返回已有结果 | 避免分析成果丢失 |
| **P1** | 修 Issue D（错误信息吞没） | 子智能体无法自动修正参数 |
| **P2** | 优化子智能体 prompt（不要猜路径） | 减少无效工具调用 |

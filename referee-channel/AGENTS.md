# AGENTS.md — referee-channel（IM 通道基座）

## 定位

通用 IM 通道闭环：**消息进来 → 批次累积 → 智能体回合 → 结果确定交付**。
本 crate 零通道知识，通道差异全部锁在适配器 crate（如 `referee-channel-wechat`）。
设计与验收：`docs/channel-execution.md`。

## 模块地图

| 模块 | 职责 |
|---|---|
| `message` | 统一消息模型 + Envelope 编解码（载荷走 `payload`，`metadata["kind"]` 区分类型） |
| `adapter` | `ChannelAdapter` / `ChannelIo` / `AdapterState` 契约——新通道只需实现它，基座零改动 |
| `host` | ChannelHost：出站受理（有界队列 `try_send`）、入站搬运（emit）、adapter 监督（退避重启 → 超限降级） |
| `batch` | 批次累积：静默窗（每条重置）/ 条数上限 / 总窗上限，三条件任一满足即闭合 |
| `dispatch` | 会话道 FIFO + 全局信号量 + 回信处置（交付契约执行处） |
| `policy` | 交付契约（`SessionReply` 穷尽分类）+ 中断关键字匹配 |
| `router` | ImRouter：peer↔session 映射、串联以上、有界任务队列准入（满即拒绝） |
| `tools` | `im_send_text`：回合内中间回执——收件人经会话映射反查，**不暴露给模型** |

## 不可破坏的不变量

- **背压三点位**：入站通道满 → adapter 游标停滞；出站队列满 → `accepted:false` 显式拒绝；任务队列满 → `im.system` 拒绝提示。
- **交付契约**：最终输出**只**由 dispatch 兜底管道发送（`Success` 非空 → `im.send` 恰好一次）；`im_send_text` 只发中间内容，最终答案直接作为回复输出。
- **handle 非阻塞**：host/router 的 `Extension::handle` 只做 push / `try_send` / reply；一切等待型 invoke 都在后台任务，持完整 `Kernel` 句柄。
- **内核语义**：扩展 handle 完成但不回信 → 回信通道随 ctx 丢弃 → invoke **立即** `TargetUnreachable`（不是等超时）；adapter panic 由 host 内监督熔断（内核 catch_unwind 只覆盖 handle）。

## 组装顺序（防 DLQ 与循环依赖）

```text
agent(runtime) → 先构造 host 取 id → router(config 引用 agent/host id) →
runtime.register_tool(ImSendText::new(kernel, host_id, router.session_map())) →
注册 agent → 注册 router → 注册 host → host.start(kernel, router_id)
```
router 必须先于 host 注册（host 的 `im.inbound` 才不落 DLQ）；工具注册在流量开始前完成。

## 测试

```bash
cargo test -p referee-channel   # message / host / router / tools 四套
```
时间边界（8s/30s 批次、超时、退避）全部用 `start_paused` 虚拟时钟操纵，测试瞬时完成。

# AGENTS.md — referee-channel-wechat（微信 iLink 适配器）

## 定位

微信通道的协议实现，对接 `referee-channel` 的 `ChannelAdapter` 契约。
协议事实**唯一来源**：`docs/wechat-clawbot-integration.md`（结构 / 端点 / 限速 / 陷阱清单），改协议先改文档。

## 模块地图

| 模块 | 职责 |
|---|---|
| `config` | `WechatConfig` 预设：限速 12s+4s 抖动（≤5 条/分钟安全线）、线级重试 3 次，serde 可配 |
| `types` / `client` | 协议结构与 HTTP 客户端；错误 thiserror 分类，`errcode=-14` → `TokenExpired` |
| `login` | 扫码登录；二维码呈现 `Url` 或 `Terminal`（后者需 feature `qr`，默认关闭） |
| `state` | 凭据一次落盘（重启免扫码）；游标 + peer→context_token 即时落盘（`flush` 仅补写脏数据） |
| `ratelimit` | 出站限速：基准间隔 + 随机抖动（tokio 时钟，可虚拟时间测试） |

## 适配器关键义务（违反即事故）

- 入站 **send 成功后才推进游标**——背压点；崩溃宁可重放不可丢消息。
- 回环过滤：`message_type != 1` 一律丢弃，否则自己回复自己、死循环 + 风控。
- 任何 type-1 消息都刷新 `peer→context_token`（**含纯媒体消息**，否则只发图片的用户令牌过期）。
- 网络错误**不熔断**（warn + 1s 重试，通道自愈）；只有 panic 交给 host 监督。
- 出站重试仅限瞬时错误；`TokenExpired` / 服务端拒绝立即放弃并 ERROR 记录（Phase 2 补投接管）。

## 入口

```rust
// 开箱即用：凭据在则复用，无则扫码登录并落盘
let adapter = WechatAdapter::connect(WechatConfig::default()).await?;
```
- 真机乎架：`examples/agent.rs`（A5 全栈：批次/调度/工具/兜底交付）、`examples/echo.rs`（最小集成范式——任意"大脑"接 Extension 即可）。

## 测试

```bash
cargo test -p referee-channel-wechat   # 含手工搭建的本地 mock iLink 服务端（tests/adapter_test.rs）
```
无需真实微信即可验证：回环过滤、游标落盘时机（背压语义）、令牌使用、过期容错。

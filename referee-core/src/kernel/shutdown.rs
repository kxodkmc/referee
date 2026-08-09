//! 优雅停机信号 — 广播 + 保留最新状态
//!
//! 基于 `tokio::sync::watch`：`trigger` 后所有已存在及后续订阅的接收端
//! 都能立即感知，无 `Notify` 的「通知丢失」竞态（`notify_waiters` 不存储
//! permit，先 trigger 后注册的 waiter 会永久挂起）。

use tokio::sync::watch;

/// 停机信号发送端 — 内核持有，可派生出任意多个接收端
#[derive(Clone)]
pub struct ShutdownTx {
    tx: watch::Sender<bool>,
}

/// 停机信号接收端 — 每个扩展运行循环持有一个
pub struct ShutdownRx {
    rx: watch::Receiver<bool>,
}

/// 创建一对停机信号通道
pub fn shutdown_channel() -> (ShutdownTx, ShutdownRx) {
    let (tx, rx) = watch::channel(false);
    (ShutdownTx { tx }, ShutdownRx { rx })
}

impl ShutdownTx {
    /// 触发停机：广播信号并永久保持触发态
    pub fn trigger(&self) {
        let _ = self.tx.send(true);
    }

    /// 派生新的接收端（感知当前触发态）
    pub fn subscribe(&self) -> ShutdownRx {
        ShutdownRx {
            rx: self.tx.subscribe(),
        }
    }
}

impl ShutdownRx {
    /// 等待停机信号；已触发则立即返回（可安全重复调用）
    pub async fn wait(&self) {
        let mut rx = self.rx.clone();
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
    }
}

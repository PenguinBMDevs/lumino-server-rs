//! 环形日志缓冲 + 实时日志广播（/logs 面板与 subscribeLogs 协议）。
//!
//! 设计要点：
//! - 环形缓冲最多保留 `CAPACITY` 条（默认 1000），内存占用有界。
//! - 日志只记录**低频事件**（连接/房间/错误）。热路径（noteBatch/mouseMove
//!   的逐条转发）禁止打日志——这是 CPU 开销控制在 5% 内的关键之一。
//! - 订阅者通过无界 mpsc 接收预序列化的 JSON 字符串，广播路径与业务消息
//!   完全复用连接发送通道，不额外加锁。

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::protocol::LogEntry;
use crate::util::now_ms;

const CAPACITY: usize = 1000;

/// 全局日志器（进程内单例，通过 `log()` 免费函数使用）
pub struct LogRing {
    buf: Mutex<VecDeque<LogEntry>>,
    /// 订阅者：socket_id → 发送通道（与业务广播同构：有界 + try_send）
    subs: DashMap<u64, mpsc::Sender<Arc<str>>>,
}

impl LogRing {
    fn new() -> Self {
        Self {
            buf: Mutex::new(VecDeque::with_capacity(CAPACITY)),
            subs: DashMap::new(),
        }
    }
    /// 记录一条日志并推送订阅者
    pub fn push(&self, level: &'static str, message: String) {
        let ts = iso_now();
        let entry = LogEntry {
            timestamp: ts.clone(),
            level,
            message,
        };

        // 写入环形缓冲（有界，满了丢弃最旧）
        {
            let mut buf = self.buf.lock().unwrap_or_else(|p| p.into_inner());
            if buf.len() >= CAPACITY {
                buf.pop_front();
            }
            buf.push_back(entry.clone());
        }

        // 推送给订阅者（序列化一次，所有订阅者共享 Arc）
        if !self.subs.is_empty() {
            let json = serde_json::to_string(&crate::protocol::ServerMessage::Log {
                timestamp: &entry.timestamp,
                level: entry.level,
                message: &entry.message,
            });
            if let Ok(json) = json {
                self.broadcast(Arc::from(json));
            }
        }
    }

    /// 获取最近 n 条历史日志
    pub fn history(&self, n: usize) -> Vec<LogEntry> {
        let buf = self.buf.lock().unwrap_or_else(|p| p.into_inner());
        buf.iter().rev().take(n).cloned().collect()
    }

    /// 添加订阅者
    pub fn subscribe(&self, socket_id: u64, tx: mpsc::Sender<Arc<str>>) {
        self.subs.insert(socket_id, tx);
    }

    /// 移除订阅者
    pub fn unsubscribe(&self, socket_id: u64) {
        self.subs.remove(&socket_id);
    }

    fn broadcast(&self, json: Arc<str>) {
        for entry in self.subs.iter() {
            let _ = entry.try_send(json.clone()); // 慢订阅者丢日志，不阻塞
        }
    }
}

/// 全局单例
pub static LOG: std::sync::LazyLock<LogRing> = std::sync::LazyLock::new(LogRing::new);

/// 记录日志（非热路径专用）
pub fn log(level: &'static str, message: impl Into<String>) {
    let message = message.into();
    // 控制台输出（tracing 默认 info 级别）
    match level {
        "error" => tracing::error!("{}", message),
        "warn" => tracing::warn!("{}", message),
        "debug" => tracing::debug!("{}", message),
        _ => tracing::info!("{}", message),
    }
    LOG.push(level, message);
}

/// ISO8601 时间戳（毫秒精度，如 2026-08-12T10:00:00.123Z）
fn iso_now() -> String {
    let ms = now_ms();
    let secs = ms / 1000;
    let millis = ms % 1000;
    let days = secs / 86400;
    let rem = secs % 86400;
    let (h, rem) = (rem / 3600, rem % 3600);
    let (m, s) = (rem / 60, rem % 60);

    // 从 1970-01-01 计算年月日（不使用 chrono，保持零重依赖）
    let (y, mo, d) = civil_from_days(days as i64);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        y, mo, d, h, m, s, millis
    )
}

/// 天数 → 公历日期（Howard Hinnant 算法，无外部依赖）
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_format() {
        let s = iso_now();
        assert_eq!(s.len(), 24);
        assert!(s.ends_with('Z'));
        assert!(s.as_bytes()[4] == b'-' && s.as_bytes()[7] == b'-');
    }

    #[test]
    fn civil_conversion() {
        // 1970-01-01
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2026-08-12：1970-01-01 起 56 年 ×365 + 14 个闰日 + 223 天（2026 年前 7 个月 + 11 天）
        assert_eq!(civil_from_days(20677), (2026, 8, 12));
        // 边界：2026-01-01
        assert_eq!(civil_from_days(20454), (2026, 1, 1));
    }

    #[test]
    fn ring_capacity() {
        let ring = LogRing::new();
        for i in 0..CAPACITY + 50 {
            ring.push("info", format!("msg-{i}"));
        }
        let h = ring.history(2000);
        assert_eq!(h.len(), CAPACITY);
        assert!(h.last().unwrap().message.starts_with("msg-"));
    }

    #[test]
    fn history_order() {
        let ring = LogRing::new();
        ring.push("info", "first".into());
        ring.push("warn", "second".into());
        let h = ring.history(2);
        assert_eq!(h[0].message, "second"); // 最新在前
        assert_eq!(h[1].message, "first");
        assert_eq!(h[0].level, "warn");
    }
}

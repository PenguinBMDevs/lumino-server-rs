//! 服务器指标：全部基于原子计数器，热路径（消息处理）零锁、零日志。
//!
//! `Sampler` 用于将累计计数转换为每秒速率（差分采样）。

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

#[derive(Default)]
pub struct Metrics {
    /// 累计建立连接数
    pub conns_total: AtomicU64,
    /// 当前连接数（可正可负，用 i64）
    pub conns_current: AtomicI64,
    /// 当前房间数
    pub rooms_current: AtomicI64,
    /// 当前用户数
    pub users_current: AtomicI64,
    /// 收到的消息总数
    pub msgs_in: AtomicU64,
    /// 发送的消息总数
    pub msgs_out: AtomicU64,
    /// 音符操作总数
    pub note_ops: AtomicU64,
    /// 收到字节总数
    pub bytes_in: AtomicU64,
    /// 发送字节总数
    pub bytes_out: AtomicU64,
    /// 丢弃的消息数（背压保护）
    pub msgs_dropped: AtomicU64,
}

impl Metrics {
    /// 记录一次收到的消息
    #[inline]
    pub fn on_msg_in(&self, bytes: usize) {
        self.msgs_in.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// 记录一次广播发送（每条连接计 1 条消息）
    #[inline]
    pub fn on_msg_out(&self, bytes: usize) {
        self.msgs_out.fetch_add(1, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// 记录音符操作
    #[inline]
    pub fn on_note_op(&self) {
        self.note_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// 连接建立
    #[inline]
    pub fn on_connect(&self) {
        self.conns_total.fetch_add(1, Ordering::Relaxed);
        self.conns_current.fetch_add(1, Ordering::Relaxed);
    }

    /// 连接断开
    #[inline]
    pub fn on_disconnect(&self) {
        self.conns_current.fetch_sub(1, Ordering::Relaxed);
    }

    /// 一次瞬时快照
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            conns_total: self.conns_total.load(Ordering::Relaxed),
            conns_current: self.conns_current.load(Ordering::Relaxed),
            rooms_current: self.rooms_current.load(Ordering::Relaxed),
            users_current: self.users_current.load(Ordering::Relaxed),
            msgs_in: self.msgs_in.load(Ordering::Relaxed),
            msgs_out: self.msgs_out.load(Ordering::Relaxed),
            note_ops: self.note_ops.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            msgs_dropped: self.msgs_dropped.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub conns_total: u64,
    pub conns_current: i64,
    /// 房间/用户当前数量由 AppState 统计提供，此处保留字段用于快照完整性
    #[allow(dead_code)]
    pub rooms_current: i64,
    #[allow(dead_code)]
    pub users_current: i64,
    pub msgs_in: u64,
    pub msgs_out: u64,
    pub note_ops: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub msgs_dropped: u64,
}

/// 差分采样器：两次采样之间的速率
pub struct Sampler {
    last: MetricsSnapshot,
    last_t: Instant,
}

impl Sampler {
    pub fn new() -> Self {
        Self {
            last: MetricsSnapshot::default(),
            last_t: Instant::now(),
        }
    }

    /// 采样并返回自上次采样以来的速率（每秒）
    pub fn sample(&mut self, snap: MetricsSnapshot) -> RateSample {
        let now = Instant::now();
        let dt = now.duration_since(self.last_t).as_secs_f64().max(0.001);
        let r = RateSample {
            msgs_in: (snap.msgs_in.saturating_sub(self.last.msgs_in)) as f64 / dt,
            msgs_out: (snap.msgs_out.saturating_sub(self.last.msgs_out)) as f64 / dt,
            note_ops: (snap.note_ops.saturating_sub(self.last.note_ops)) as f64 / dt,
            bytes_in: (snap.bytes_in.saturating_sub(self.last.bytes_in)) as f64 / dt,
            bytes_out: (snap.bytes_out.saturating_sub(self.last.bytes_out)) as f64 / dt,
        };
        self.last = snap;
        self.last_t = now;
        r
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RateSample {
    pub msgs_in: f64,
    pub msgs_out: f64,
    pub note_ops: f64,
    pub bytes_in: f64,
    pub bytes_out: f64,
}

impl Default for Sampler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_work() {
        let m = Metrics::default();
        m.on_connect();
        m.on_connect();
        m.on_msg_in(100);
        m.on_msg_in(50);
        m.on_note_op();
        assert_eq!(m.snapshot().conns_current, 2);
        assert_eq!(m.snapshot().msgs_in, 2);
        assert_eq!(m.snapshot().bytes_in, 150);
        assert_eq!(m.snapshot().note_ops, 1);
        m.on_disconnect();
        assert_eq!(m.snapshot().conns_current, 1);
    }

    #[test]
    fn sampler_rates() {
        let m = Metrics::default();
        let mut s = Sampler::new();
        m.on_msg_in(10);
        let r = s.sample(m.snapshot());
        assert!(r.msgs_in >= 0.0);
        let r2 = s.sample(m.snapshot());
        assert_eq!(r2.msgs_in, 0.0);
    }
}

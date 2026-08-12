//! 用户（连接）句柄

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::util::{generate_id, generate_user_color, now_ms};

/// 一个已建立的 WebSocket 连接（用户级句柄）
pub struct User {
    pub id: String,
    pub username: String,
    pub color: &'static str,
    /// 全局唯一 socket id（u64，比字符串更快更省内存）
    pub socket_id: u64,
    pub last_active: AtomicU64,
    /// 发送通道：有界 + try_send（背压即丢）
    pub tx: mpsc::Sender<Arc<str>>,
    /// 是否日志订阅客户端
    pub is_log_client: AtomicBool,
}

impl User {
    pub(crate) fn new(socket_id: u64, username: String, tx: mpsc::Sender<Arc<str>>) -> Arc<Self> {
        Arc::new(Self {
            id: generate_id(),
            username,
            color: generate_user_color(),
            socket_id,
            last_active: AtomicU64::new(now_ms()),
            tx,
            is_log_client: AtomicBool::new(false),
        })
    }

    pub(crate) fn touch(&self) {
        self.last_active.store(now_ms(), Ordering::Relaxed);
    }
}

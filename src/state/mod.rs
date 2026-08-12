//! 状态层：全局应用状态（用户表、房间表、指标）与广播原语。
//!
//! 架构决策（The Algorithm 的 delete/simplify 步）：
//! - **房间 ID = 邀请码**：砍掉 TS 版的 `inviteCodeToRoomId` 映射，
//!   消除两套 ID 并存的混乱，全部路径统一。
//! - **广播路径无锁**：`users` 表读锁 + mpsc `try_send`（有界通道），
//!   慢消费者丢消息而非阻塞全体（与 TS 版 bufferedAmount 背压语义一致）。

pub mod room;
pub mod user;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;

pub use room::{ProjectState, Room, MAX_OPS_LOG, MAX_USERS_PER_ROOM};
pub use user::User;

use crate::metrics::{Metrics, Sampler};
use crate::protocol::ServerMessage;
use crate::util::{generate_invite_code, now_ms};

/// 发送通道容量：慢消费者丢消息而非阻塞广播。
/// 注意：容量过大会显著推高每连接内存（1W 连接 × 容量 × 槽位大小），
/// 256 槽在实时协作场景下足以覆盖广播突发，且每连接仅几 KB。
pub const CHANNEL_CAPACITY: usize = 256;

/// 全局应用状态（单例，所有 handler 共享）
pub struct AppState {
    pub users: DashMap<String, Arc<User>>,
    /// 房间表：key = 邀请码
    pub rooms: DashMap<String, Arc<Room>>,
    /// user_id → room_id
    pub user_to_room: DashMap<String, String>,
    pub metrics: Arc<Metrics>,
    /// 速率差分采样器（/metrics 与监控面板使用）
    pub sampler: std::sync::Mutex<Sampler>,
    next_socket_id: AtomicU64,
}

impl AppState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            users: DashMap::new(),
            rooms: DashMap::new(),
            user_to_room: DashMap::new(),
            metrics: Arc::new(Metrics::default()),
            sampler: std::sync::Mutex::new(Sampler::new()),
            next_socket_id: AtomicU64::new(1),
        })
    }

    /// 分配全局 socket id
    pub fn next_socket_id(&self) -> u64 {
        self.next_socket_id.fetch_add(1, Ordering::Relaxed)
    }

    /// 创建用户（认证成功时调用）
    pub fn create_user(
        &self,
        socket_id: u64,
        username: String,
        tx: mpsc::Sender<Arc<str>>,
    ) -> Arc<User> {
        let user = User::new(socket_id, username, tx);
        self.users.insert(user.id.clone(), Arc::clone(&user));
        self.metrics.users_current.fetch_add(1, Ordering::Relaxed);
        user
    }

    /// 移除用户（连接断开时调用）
    ///
    /// 注意：从 `rooms.get()` 拿到的 DashMap Ref 持有 shard 读锁，
    /// 在 Ref 存活期间调用 `rooms.remove()` 会请求同一 shard 写锁，
    /// 导致同线程读写锁重入死锁（DashMap 已知陷阱）。必须先用
    /// `.map(|r| Arc::clone(&r))` 脱离 Ref 生命周期。
    pub fn remove_user(&self, user: &Arc<User>) {
        let room_id = self.user_to_room.remove(&user.id);
        if let Some((_, rid)) = room_id {
            let room = self.rooms.get(&rid).map(|r| Arc::clone(&r));
            if let Some(room) = room {
                room.remove_user(&user.id);
                if room.user_count() == 0 {
                    self.rooms.remove(&rid);
                    self.metrics.rooms_current.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        self.users.remove(&user.id);
        self.metrics.users_current.fetch_sub(1, Ordering::Relaxed);
    }

    /// 获取或创建房间（URL ?roomId= 路径，与 TS 版 getOrCreateRoom 一致）
    pub fn get_or_create_room(&self, code: &str, user: &Arc<User>) -> Result<Arc<Room>, String> {
        // 用户已在其他房间则先离开
        self.leave_room(user);

        let room = match self.rooms.get(code) {
            Some(r) => r.clone(),
            None => {
                let room = Room::new(code.to_string(), format!("协作房间 {code}"), None);
                self.rooms.insert(code.to_string(), Arc::clone(&room));
                self.metrics.rooms_current.fetch_add(1, Ordering::Relaxed);
                room
            }
        };

        if room.user_count() >= room.max_users {
            return Err("房间已满".to_string());
        }

        // 第一个加入的用户成为房主
        {
            let mut host = room.host_id.write().unwrap_or_else(|p| p.into_inner());
            if host.is_none() {
                *host = Some(user.id.clone());
            }
        }
        room.add_user(user);
        self.user_to_room.insert(user.id.clone(), room.id.clone());
        Ok(room)
    }

    /// 创建新房间（createRoom 消息路径）
    ///
    /// 注意：**不**离开旧房间——与 TS 原版行为一致。原版 `createRoom`
    /// 直接让 host 加入新房间而不清理旧房间；若此处调用 leave_room，
    /// 会删除用户离开后变空的旧房间，破坏"预创建房间"场景。
    pub fn create_room(&self, user: &Arc<User>, name: String) -> Result<Arc<Room>, String> {
        let mut code = generate_invite_code();
        let mut guard = 0;
        while self.rooms.contains_key(&code) && guard < 16 {
            code = generate_invite_code();
            guard += 1;
        }
        let room = Room::new(
            code.clone(),
            if name.is_empty() {
                format!("协作房间 {code}")
            } else {
                name
            },
            Some(user),
        );
        self.rooms.insert(code, Arc::clone(&room));
        self.metrics.rooms_current.fetch_add(1, Ordering::Relaxed);
        self.user_to_room.insert(user.id.clone(), room.id.clone());
        Ok(room)
    }

    /// 通过邀请码加入房间
    pub fn join_room(&self, code: &str, user: &Arc<User>) -> Result<Arc<Room>, String> {
        let room = self
            .rooms
            .get(code)
            .map(|r| Arc::clone(&r))
            .ok_or_else(|| "邀请码无效或房间不存在".to_string())?;
        if room.user_count() >= room.max_users {
            return Err("房间已满".to_string());
        }
        self.leave_room(user);
        room.add_user(user);
        self.user_to_room.insert(user.id.clone(), room.id.clone());
        Ok(room)
    }

    /// 预创建房间（HTTP API 通道，等待第一个 WS 用户加入成为房主）
    pub fn create_room_api(&self, name: String) -> Arc<Room> {
        let mut code = generate_invite_code();
        while self.rooms.contains_key(&code) {
            code = generate_invite_code();
        }
        let room = Room::new(code.clone(), name, None);
        self.rooms.insert(code, Arc::clone(&room));
        self.metrics.rooms_current.fetch_add(1, Ordering::Relaxed);
        room
    }

    /// 离开房间；房主离开时移交新房主；空房间删除
    pub fn leave_room(&self, user: &Arc<User>) -> bool {
        let Some((_, rid)) = self.user_to_room.remove(&user.id) else {
            return false;
        };
        let Some(room) = self.rooms.get(&rid).map(|r| Arc::clone(&r)) else {
            return false;
        };
        room.remove_user(&user.id);

        let remaining = room.user_count();
        if remaining == 0 {
            self.rooms.remove(&rid);
            self.metrics.rooms_current.fetch_sub(1, Ordering::Relaxed);
        } else {
            // 房主离开 → 移交给第一个剩余用户
            let mut host = room.host_id.write().unwrap_or_else(|p| p.into_inner());
            if host.as_deref() == Some(user.id.as_str()) {
                let next = room
                    .users
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .keys()
                    .next()
                    .cloned();
                *host = next;
            }
        }
        true
    }

    /// 用户所在房间
    pub fn room_of(&self, user: &Arc<User>) -> Option<Arc<Room>> {
        self.user_to_room
            .get(&user.id)
            .and_then(|e| self.rooms.get(e.value()).map(|r| Arc::clone(&r)))
    }

    /// 按邀请码取房间
    pub fn room_by_code(&self, code: &str) -> Option<Arc<Room>> {
        self.rooms.get(code).map(|r| Arc::clone(&r))
    }

    /// 清理不活跃的空房间
    pub fn cleanup_inactive(&self, max_idle_ms: u64) -> usize {
        let now = now_ms();
        let mut cleaned = 0;
        let expired: Vec<String> = self
            .rooms
            .iter()
            .filter(|e| {
                e.user_count() == 0
                    && now
                        - e.project
                            .read()
                            .unwrap_or_else(|p| p.into_inner())
                            .last_modified
                        > max_idle_ms
            })
            .map(|e| e.key().clone())
            .collect();
        for id in expired {
            if self.rooms.remove(&id).is_some() {
                self.metrics.rooms_current.fetch_sub(1, Ordering::Relaxed);
                cleaned += 1;
            }
        }
        cleaned
    }

    /// 统计信息
    pub fn stats(&self) -> (i64, i64) {
        (self.rooms.len() as i64, self.users.len() as i64)
    }
}

/// 向单个用户发送消息（序列化一次 + try_send）
pub fn send_to_user(user: &Arc<User>, msg: &ServerMessage<'_>, metrics: &Metrics) {
    let Ok(json) = serde_json::to_string(msg) else {
        return;
    };
    let len = json.len();
    if user.tx.try_send(Arc::from(json)).is_err() {
        metrics
            .msgs_dropped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return;
    }
    metrics.on_msg_out(len);
}

/// 向房间广播消息（序列化一次，所有成员共享同一个 Arc<str>）
pub fn broadcast_to_room(
    room: &Room,
    msg: &ServerMessage<'_>,
    exclude_user_id: Option<&str>,
    metrics: &Metrics,
) {
    let Ok(json) = serde_json::to_string(msg) else {
        return;
    };
    let arc: Arc<str> = Arc::from(json);
    let len = arc.len();
    let users = room.users.read().unwrap_or_else(|p| p.into_inner());
    for user in users.values() {
        if exclude_user_id.is_some_and(|id| id == user.id) {
            continue;
        }
        if user.tx.try_send(arc.clone()).is_err() {
            metrics.msgs_dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics.on_msg_out(len);
        }
    }
}
#[cfg(test)]
#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;

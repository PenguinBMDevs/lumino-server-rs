//! WebSocket 连接处理与消息分发。
//!
//! 热路径设计：
//! - 每个连接一个 task：`select!` 双路复用（上行消息 / 下行发送队列），
//!   下行全部走 `mpsc::Sender<Arc<str>>`，广播方零等待。
//! - 消息处理 `dispatch()` 为纯同步函数：解析一次 → 应用状态 → 广播一次。
//! - 热路径（noteBatch / mouseMove）**零日志**，日志只留给低频生命周期事件。
//! - 心跳：30s 发送 ping 帧，90s 无活动断开。

pub mod dispatch;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::protocol::ServerMessage;
use crate::state::{broadcast_to_room, AppState, User, CHANNEL_CAPACITY};

/// 心跳间隔
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// 超过此时长无活动（含 pong）即断开
const MAX_IDLE: Duration = Duration::from_secs(90);

/// WS 升级入口
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let pending_room = params.get("roomId").cloned();
    ws.max_message_size(1024 * 1024) // 1MB 消息上限（默认 64MB）
        .write_buffer_size(4096) // 写缓冲目标 4KB（默认 128KB，实测影响每连接内存）
        .on_upgrade(move |socket| handle_socket(state, socket, pending_room))
}

/// 单连接生命周期
async fn handle_socket(state: Arc<AppState>, socket: WebSocket, pending_room: Option<String>) {
    let socket_id = state.next_socket_id();
    let metrics = Arc::clone(&state.metrics);
    let (reply_tx, mut reply_rx) = mpsc::channel::<Arc<str>>(CHANNEL_CAPACITY);

    let (mut ws_sink, mut ws_stream) = socket.split();
    let mut user: Option<Arc<User>> = None;
    let mut last_active = Instant::now();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // 关键：tokio interval 第一次 tick 立即触发——必须消耗掉，
    // 否则连接建立瞬间就发 Ping，与 authenticated 响应在 select 中
    // 随机竞争，导致客户端把 Ping 误认为第一条消息（"意外的消息类型"）
    heartbeat.tick().await;

    metrics.on_connect();

    loop {
        tokio::select! {
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        last_active = Instant::now();
                        dispatch::dispatch(&state, &mut user, socket_id, &pending_room, &reply_tx, &text);
                    }
                    Some(Ok(Message::Binary(_))) => { /* 协议仅文本，忽略 */ }
                    Some(Ok(Message::Ping(p))) => {
                        if ws_sink.send(Message::Pong(p)).await.is_err() { break; }
                    }
                    Some(Ok(Message::Pong(_))) => { last_active = Instant::now(); }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                }
            }
            outgoing = reply_rx.recv() => {
                match outgoing {
                    Some(text) => {
                        if ws_sink.send(Message::Text(text.to_string().into())).await.is_err() { break; }
                    }
                    None => break,
                }
            }
            _ = heartbeat.tick() => {
                if last_active.elapsed() > MAX_IDLE {
                    break;
                }
                if ws_sink.send(Message::Ping(vec![0].into())).await.is_err() { break; }
            }
        }
    }

    // ===== 连接清理 =====
    metrics.on_disconnect();
    if let Some(u) = &user {
        crate::log::LOG.unsubscribe(u.socket_id);
        if let Some(room) = state.room_of(u) {
            let msg = ServerMessage::UserLeft { user_id: &u.id };
            broadcast_to_room(&room, &msg, Some(&u.id), &metrics);
        }
        state.remove_user(u);
        crate::log::log("info", format!("用户离开: {} ({})", u.username, u.id));
    }
}

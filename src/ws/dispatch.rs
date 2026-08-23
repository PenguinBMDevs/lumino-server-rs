//! 消息分发（纯同步，热路径）
//!
//! 从 WS 连接层剥离的独立模块：解析一次 → 应用状态 → 广播一次。
//! 认证前无 User 句柄，使用 `send_raw` 直接投递。

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::mpsc;

use crate::protocol::{AuthRoomInfo, ClientMessage, ServerMessage, UserData, UserInfo, ViewState};
use crate::state::{broadcast_to_room, send_to_user, AppState, User};
use crate::util::validate_username;

/// 消息分发（纯同步，热路径）
pub(crate) fn dispatch(
    state: &Arc<AppState>,
    user: &mut Option<Arc<User>>,
    socket_id: u64,
    pending_room: &Option<String>,
    reply_tx: &mpsc::Sender<Arc<str>>,
    text: &str,
) {
    let metrics = &state.metrics;
    metrics.on_msg_in(text.len());

    let msg: ClientMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => {
            send_raw(
                reply_tx,
                &ServerMessage::Error {
                    error: "无效的消息格式",
                },
                metrics,
            );
            return;
        }
    };

    match msg {
        ClientMessage::Auth { username, password } => {
            handle_auth(
                state,
                user,
                socket_id,
                pending_room,
                reply_tx,
                &username,
                &password,
            )
        }
        ClientMessage::CreateRoom { name } => {
            let Some(u) = user.as_ref() else { return };
            match state.create_room(u, name) {
                Ok(room) => {
                    send_raw(
                        reply_tx,
                        &ServerMessage::RoomCreated { room: room.info() },
                        metrics,
                    );
                }
                Err(e) => send_raw(reply_tx, &ServerMessage::RoomError { error: &e }, metrics),
            }
        }
        ClientMessage::JoinRoom { invite_code } => {
            let Some(u) = user.as_ref() else { return };
            match state.join_room(&invite_code, u) {
                Ok(room) => {
                    let project = room.project.read().unwrap_or_else(|p| p.into_inner());
                    let msg = ServerMessage::RoomJoined {
                        room: room.info(),
                        users: room.users_info(),
                        project_state: project.snapshot(),
                    };
                    send_raw(reply_tx, &msg, metrics);
                    // 通知房间其他成员
                    let ui = UserInfo {
                        id: u.id.clone(),
                        username: u.username.clone(),
                        color: u.color,
                        is_host: room
                            .host_id
                            .read()
                            .unwrap_or_else(|p| p.into_inner())
                            .as_deref()
                            == Some(u.id.as_str()),
                    };
                    broadcast_to_room(
                        &room,
                        &ServerMessage::UserJoined { user: ui },
                        Some(&u.id),
                        metrics,
                    );
                }
                Err(e) => send_raw(reply_tx, &ServerMessage::RoomError { error: &e }, metrics),
            }
        }
        ClientMessage::LeaveRoom => {
            let Some(u) = user.as_ref() else { return };
            if let Some(room) = state.room_of(u) {
                let msg = ServerMessage::UserLeft { user_id: &u.id };
                broadcast_to_room(&room, &msg, Some(&u.id), metrics);
            }
            state.leave_room(u);
        }
        ClientMessage::MouseMove { position } => {
            let Some(u) = user.as_ref() else { return };
            let Some(room) = state.room_of(u) else { return };
            u.touch();
            let msg = ServerMessage::MouseUpdate {
                user_id: &u.id,
                username: &u.username,
                position: &position,
                color: u.color,
            };
            broadcast_to_room(&room, &msg, Some(&u.id), metrics);
        }
        ClientMessage::NoteBatch { mut notes } => {
            let Some(u) = user.as_ref() else { return };
            let Some(room) = state.room_of(u) else { return };
            metrics.on_note_op();
            {
                let mut project = room.project.write().unwrap_or_else(|p| p.into_inner());
                project.apply(&mut notes, &u.id);
            }
            let msg = ServerMessage::NoteBatchUpdate {
                user_id: &u.id,
                operation: &notes,
            };
            broadcast_to_room(&room, &msg, Some(&u.id), metrics);
        }
        ClientMessage::MidiEvent { event } => {
            let Some(u) = user.as_ref() else { return };
            let Some(room) = state.room_of(u) else { return };
            let msg = ServerMessage::MidiEventUpdate {
                user_id: &u.id,
                event: &event,
            };
            broadcast_to_room(&room, &msg, Some(&u.id), metrics);
        }
        ClientMessage::MidiEventBatch { events } => {
            let Some(u) = user.as_ref() else { return };
            let Some(room) = state.room_of(u) else { return };
            let msg = ServerMessage::MidiEventBatchUpdate {
                user_id: &u.id,
                events: &events,
            };
            broadcast_to_room(&room, &msg, Some(&u.id), metrics);
        }
        ClientMessage::ProjectUpdate { update } => {
            let Some(u) = user.as_ref() else { return };
            let Some(room) = state.room_of(u) else { return };
            // 尝试应用视图状态（对两种客户端格式均生效：都看 data 字段）
            if let Some(data) = update.get("data").and_then(Value::as_object) {
                if let Ok(vs) =
                    serde_json::from_value::<ViewState>(serde_json::Value::Object(data.clone()))
                {
                    room.project
                        .write()
                        .unwrap_or_else(|p| p.into_inner())
                        .set_view_state(vs, &u.id);
                }
            }
            let msg = ServerMessage::ProjectStateUpdate {
                user_id: &u.id,
                update: &update,
            };
            broadcast_to_room(&room, &msg, Some(&u.id), metrics);
        }
        ClientMessage::RequestSync => {
            let Some(u) = user.as_ref() else { return };
            let Some(room) = state.room_of(u) else { return };
            let project = room.project.read().unwrap_or_else(|p| p.into_inner());
            let msg = ServerMessage::FullSync {
                project_state: project.snapshot(),
                users: room.users_info(),
            };
            send_to_user(u, &msg, metrics);
        }
        ClientMessage::Ping { timestamp } => {
            let msg = ServerMessage::Pong {
                timestamp,
                server_time: crate::util::now_ms(),
            };
            send_raw(reply_tx, &msg, metrics);
        }
        ClientMessage::Selection { selection } => {
            let Some(u) = user.as_ref() else { return };
            let Some(room) = state.room_of(u) else { return };
            u.touch();
            let msg = ServerMessage::Selection {
                user_id: &u.id,
                selection: &selection,
            };
            // 广播给房间内其他成员（发送方已本地持有选择，无需回发）
            broadcast_to_room(&room, &msg, Some(&u.id), metrics);
        }
        ClientMessage::SubscribeLogs => {
            crate::log::LOG.subscribe(socket_id, reply_tx.clone());
            let history = crate::log::LOG.history(100);
            let msg = ServerMessage::LogHistory { logs: history };
            send_raw(reply_tx, &msg, metrics);
            if let Some(u) = user.as_ref() {
                u.is_log_client
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            crate::log::log("info", format!("日志客户端订阅: socket-{socket_id}"));
        }
    }
}

/// 认证处理（可顺带自动加入 ?roomId= 房间）
pub(crate) fn handle_auth(
    state: &Arc<AppState>,
    user: &mut Option<Arc<User>>,
    socket_id: u64,
    pending_room: &Option<String>,
    reply_tx: &mpsc::Sender<Arc<str>>,
    username: &str,
    password: &str,
) {
    let metrics = &state.metrics;
    let username = match validate_username(username) {
        Ok(u) => u,
        Err(e) => {
            send_raw(reply_tx, &ServerMessage::AuthError { error: e }, metrics);
            return;
        }
    };

    // 账号校验：仅允许已注册账户（用户名 + 密码）连接。
    // 凭据错误直接拒绝，绝不创建 User —— 未授权连接既不能进入房间，
    // 也无法收发任何业务消息。
    let store = crate::auth::auth_store();
    if !store.account_exists(&username) {
        send_raw(
            reply_tx,
            &ServerMessage::Error {
                error: "用户不存在".into(),
            },
            metrics,
        );
        return;
    }
    if store.verify_login(&username, password).is_none() {
        send_raw(
            reply_tx,
            &ServerMessage::Error {
                error: "密码错误".into(),
            },
            metrics,
        );
        return;
    }

    let new_user = state.create_user(socket_id, username.clone(), reply_tx.clone());
    let base_ui = UserInfo {
        id: new_user.id.clone(),
        username: username.clone(),
        color: new_user.color,
        is_host: false,
    };

    // 尝试自动加入房间
    if let Some(code) = pending_room {
        tracing::debug!("[auth] 尝试加入房间: {code}");
        match state.get_or_create_room(code, &new_user) {
            Ok(room) => {
                tracing::debug!("[auth] 加入房间成功: {code}");
                let is_host = room
                    .host_id
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .as_deref()
                    == Some(new_user.id.as_str());
                let host_owned = room
                    .host_id
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .unwrap_or_default();
                let room_info_proj_name = room
                    .project_name
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .unwrap_or_default();
                let room_info_proj_hash = room
                    .project_hash
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .unwrap_or_default();
                let room_info = AuthRoomInfo {
                    id: &room.id,
                    invite_code: &room.id,
                    name: &room.name,
                    host_id: &host_owned,
                    user_count: room.user_count(),
                    max_users: room.max_users,
                    project_name: &room_info_proj_name,
                    project_hash: &room_info_proj_hash,
                };
                let users = room.users_info();
                tracing::debug!("[auth] users_info 完成: {} 人", users.len());
                let ui = UserInfo { is_host, ..base_ui };
                let msg = ServerMessage::Authenticated {
                    user_id: &new_user.id,
                    room: Some(room_info),
                    user: UserData {
                        id: &new_user.id,
                        username: &new_user.username,
                        extra: &serde_json::json!({ "color": ui.color, "isHost": is_host }),
                    },
                    users,
                };
                tracing::debug!("[auth] 序列化并发送 authenticated");
                send_raw(reply_tx, &msg, metrics);
                tracing::debug!("[auth] authenticated 已投递");
                broadcast_to_room(
                    &room,
                    &ServerMessage::UserJoined { user: ui },
                    Some(&new_user.id),
                    metrics,
                );
                *user = Some(new_user);
                crate::log::log(
                    "info",
                    format!("用户认证并加入房间: {} ({})", username, room.id),
                );
                return;
            }
            Err(e) => {
                crate::log::log("warn", format!("用户 {username} 加入房间 {code} 失败: {e}"));
                // 落回基本认证（room: null）
            }
        }
    }

    let msg = ServerMessage::Authenticated {
        user_id: &new_user.id,
        room: None,
        user: UserData {
            id: &new_user.id,
            username: &new_user.username,
            extra: &serde_json::json!({ "color": base_ui.color, "isHost": false }),
        },
        users: vec![],
    };
    send_raw(reply_tx, &msg, metrics);
    *user = Some(new_user);
    crate::log::log("info", format!("用户认证: {username} (socket-{socket_id})"));
}

/// 直接向连接发送（认证前无 User 句柄时使用）
#[inline]
pub(crate) fn send_raw(
    tx: &mpsc::Sender<Arc<str>>,
    msg: &ServerMessage<'_>,
    metrics: &crate::metrics::Metrics,
) {
    let Ok(json) = serde_json::to_string(msg) else {
        return;
    };
    let len = json.len();
    if tx.try_send(Arc::from(json)).is_err() {
        metrics
            .msgs_dropped
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return;
    }
    metrics.on_msg_out(len);
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;

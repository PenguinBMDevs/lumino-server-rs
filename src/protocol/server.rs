//! 服务器 → 客户端消息（与客户端 `client/message.rs` 的 `ServerMessage` 对称）
//!
//! 所有变体字段均为引用（`&str` / `&Value` / `&NoteBatchOperation`），
//! 广播路径序列化一次、零拷贝共享。

use serde::Serialize;
use serde_json::Value;

use super::types::{NoteBatchOperation, ViewState};

/// 房间信息（与客户端 `RoomInfo` 一致）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomInfo {
    pub id: String,
    pub invite_code: String,
    pub name: String,
    pub host_id: String,
    pub user_count: usize,
    pub max_users: usize,
}

/// 用户信息（与客户端 `UserInfo` 一致）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    pub id: String,
    pub username: String,
    pub color: &'static str,
    pub is_host: bool,
}

/// 认证响应中的房间信息（与客户端 `AuthRoomInfo` 一致）
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthRoomInfo<'a> {
    pub id: &'a str,
    pub invite_code: &'a str,
    pub name: &'a str,
    pub host_id: &'a str,
    pub user_count: usize,
    pub max_users: usize,
}

/// 认证响应中的用户数据（与客户端 `UserData` 一致：id + flatten extra）
#[derive(Debug, Serialize)]
pub struct UserData<'a> {
    pub id: &'a str,
    pub username: &'a str,
    #[serde(flatten)]
    pub extra: &'a Value,
}

/// fullSync 下发的项目状态快照：
/// `notes` 字段为客户端可重放的操作列表（与客户端 `ProjectState` 的 notes 字段一致），
/// 其余字段通过 flatten 进入客户端 `extra`。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectStateSnapshot {
    pub notes: Vec<NoteBatchOperation>,
    pub view_state: ViewState,
    pub last_modified: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_by: Option<String>,
}

/// 日志条目（供 /logs 面板与 subscribeLogs 使用）
#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub level: &'static str,
    pub message: String,
}

/// 服务器 → 客户端消息（与客户端 `ServerMessage` 对称，Serialize 端）
#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum ServerMessage<'a> {
    /// 认证成功（含房间信息与房间内用户列表）
    Authenticated {
        #[serde(rename = "userId")]
        user_id: &'a str,
        room: Option<AuthRoomInfo<'a>>,
        user: UserData<'a>,
        users: Vec<UserInfo>,
    },
    /// 认证成功（简化版，预留兼容——客户端协议保留该变体，服务器暂不发送）
    #[allow(dead_code)]
    AuthSuccess {
        #[serde(rename = "userId")]
        user_id: &'a str,
        #[serde(rename = "inviteCode")]
        invite_code: &'a str,
    },
    /// 认证失败
    AuthError { error: &'a str },
    /// 房间创建成功
    RoomCreated { room: RoomInfo },
    /// 加入房间成功（含完整项目状态）
    RoomJoined {
        room: RoomInfo,
        users: Vec<UserInfo>,
        #[serde(rename = "projectState")]
        project_state: ProjectStateSnapshot,
    },
    /// 房间操作失败
    RoomError { error: &'a str },
    /// 新用户加入
    UserJoined { user: UserInfo },
    /// 用户离开
    UserLeft {
        #[serde(rename = "userId")]
        user_id: &'a str,
    },
    /// 鼠标位置更新
    MouseUpdate {
        #[serde(rename = "userId")]
        user_id: &'a str,
        username: &'a str,
        position: &'a Value,
        color: &'a str,
    },
    /// 音符批量操作更新
    NoteBatchUpdate {
        #[serde(rename = "userId")]
        user_id: &'a str,
        operation: &'a NoteBatchOperation,
    },
    /// MIDI 事件更新
    MidiEventUpdate {
        #[serde(rename = "userId")]
        user_id: &'a str,
        event: &'a Value,
    },
    /// MIDI 事件批量更新
    MidiEventBatchUpdate {
        #[serde(rename = "userId")]
        user_id: &'a str,
        events: &'a Value,
    },
    /// 项目状态更新
    ProjectStateUpdate {
        #[serde(rename = "userId")]
        user_id: &'a str,
        update: &'a Value,
    },
    /// 完整状态同步
    FullSync {
        #[serde(rename = "projectState")]
        project_state: ProjectStateSnapshot,
        users: Vec<UserInfo>,
    },
    /// 心跳响应
    Pong {
        timestamp: u64,
        #[serde(rename = "serverTime")]
        server_time: u64,
    },
    /// 通用错误
    Error { error: &'a str },
    /// 实时日志（仅日志订阅客户端接收）
    Log {
        timestamp: &'a str,
        level: &'a str,
        message: &'a str,
    },
    /// 历史日志（仅日志订阅客户端接收）
    LogHistory { logs: Vec<LogEntry> },
}

/// 便捷构造：心跳响应（测试用）
#[cfg(test)]
pub fn pong_msg(timestamp: u64) -> ServerMessage<'static> {
    ServerMessage::Pong {
        timestamp,
        server_time: crate::util::now_ms(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::NoteAction;

    #[test]
    fn serialize_authenticated() {
        let msg = ServerMessage::Authenticated {
            user_id: "u1",
            room: Some(AuthRoomInfo {
                id: "ABC123",
                invite_code: "ABC123",
                name: "room",
                host_id: "u1",
                user_count: 1,
                max_users: 10,
            }),
            user: UserData {
                id: "u1",
                username: "小明",
                extra: &serde_json::json!({"color": "#FF6B6B", "isHost": true}),
            },
            users: vec![],
        };
        let s = serde_json::to_string(&msg).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "authenticated");
        assert_eq!(v["userId"], "u1");
        assert_eq!(v["room"]["inviteCode"], "ABC123");
        assert_eq!(v["user"]["color"], "#FF6B6B");
        assert_eq!(v["user"]["isHost"], true);
    }

    #[test]
    fn serialize_note_batch_update() {
        let op = NoteBatchOperation {
            action: NoteAction::Add,
            notes: vec![crate::protocol::types::SyncNote {
                id: "n1".into(),
                tick: 960.0,
                key: 60,
                length: 480.0,
                velocity: 100,
                channel: 0,
                track_index: 0,
            }],
            source_track: None,
            target_track: None,
            tick_offset: None,
            key_offset: None,
            timestamp: 1700000000000,
        };
        let msg = ServerMessage::NoteBatchUpdate {
            user_id: "u1",
            operation: &op,
        };
        let s = serde_json::to_string(&msg).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "noteBatchUpdate");
        assert_eq!(v["userId"], "u1");
        assert_eq!(v["operation"]["action"], "add");
        assert_eq!(v["operation"]["notes"][0]["trackIndex"], 0);
    }

    #[test]
    fn serialize_pong() {
        let msg = pong_msg(12345);
        let s = serde_json::to_string(&msg).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["type"], "pong");
        assert_eq!(v["timestamp"], 12345);
        assert!(v["serverTime"].as_u64().unwrap() > 0);
    }
}

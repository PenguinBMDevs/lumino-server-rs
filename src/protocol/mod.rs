//! 线协议定义：与 lumino-rs 客户端（crates/collaboration）逐字段兼容。
//!
//! 客户端发送方向使用 `#[serde(tag = "type")]` + camelCase 的 JSON 信封；
//! 服务器发送方向保持相同信封格式（客户端用 Deserialize 接收）。
//!
//! 性能设计：
//! - `ServerMessage` 携带引用字段（`&str` / `&Value` / `&NoteBatchOperation`），
//!   广播路径零拷贝、只序列化一次。
//! - `midiEvent` / `projectUpdate` / `mouseMove.position` 等不透明载荷用
//!   `serde_json::Value` 原样透传，避免枚举漂移导致的协议不兼容。

pub mod server;
pub mod types;

pub use server::*;
pub use types::*;

use serde::Deserialize;
use serde_json::Value;

/// 客户端消息（与客户端 `ClientMessage` 对称，Deserialize 端）
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum ClientMessage {
    /// 认证：携带用户名与密码；仅已注册账户（凭据校验通过）方可连接，
    /// 否则连接被拒绝（无法进入任何房间）。
    Auth { username: String, password: String },
    /// 创建房间
    CreateRoom { name: String },
    /// 通过邀请码加入房间（注意：enum 的 rename_all 不作用于字段，
    /// 必须显式 rename——与客户端定义保持一致）
    JoinRoom {
        #[serde(rename = "inviteCode")]
        invite_code: String,
    },
    /// 离开当前房间
    LeaveRoom,
    /// 鼠标位置同步（position 原样透传，避免协议漂移）
    MouseMove { position: Value },
    /// 音符批量操作（需要解析以维护项目状态）
    NoteBatch { notes: NoteBatchOperation },
    /// MIDI 事件（透传）
    MidiEvent { event: Value },
    /// MIDI 事件批量（透传）
    MidiEventBatch { events: Value },
    /// 项目状态更新（透传）
    ProjectUpdate { update: Value },
    /// 请求完整同步
    RequestSync,
    /// 心跳
    Ping { timestamp: u64 },
    /// 订阅实时日志（浏览器 /logs 面板专用；Rust 客户端不使用）
    SubscribeLogs,
    /// 框选状态同步（selection 为不透明 JSON 载荷，避免协议漂移）
    /// 载荷结构：`{ active: bool, userId: str, timestamp: u64, fingerprints: [[track,tick,key,length], ...] }`
    Selection { selection: Value },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_client_auth() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type":"auth","username":"小明","password":"secret"}"#).unwrap();
        match msg {
            ClientMessage::Auth { username, .. } => assert_eq!(username, "小明"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_client_join_room() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type":"joinRoom","inviteCode":"ABC123"}"#).unwrap();
        match msg {
            ClientMessage::JoinRoom { invite_code } => assert_eq!(invite_code, "ABC123"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_note_batch() {
        let raw = r#"{"type":"noteBatch","notes":{"action":"add","notes":[{"tick":960.0,"key":60,"length":480.0,"velocity":100,"channel":0,"trackIndex":0}],"timestamp":1700000000000}}"#;
        let msg: ClientMessage = serde_json::from_str(raw).unwrap();
        match msg {
            ClientMessage::NoteBatch { notes } => {
                assert_eq!(notes.action, NoteAction::Add);
                assert_eq!(notes.notes.len(), 1);
                assert_eq!(notes.notes[0].tick, 960.0);
                assert_eq!(notes.notes[0].key, 60);
                assert_eq!(notes.notes[0].track_index, 0);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parse_note_batch_move() {
        let raw = r#"{"type":"noteBatch","notes":{"action":"move","notes":[{"tick":960,"key":60,"length":480,"velocity":100,"channel":0,"trackIndex":0}],"tickOffset":480,"keyOffset":2,"timestamp":1700000000000}}"#;
        let msg: ClientMessage = serde_json::from_str(raw).unwrap();
        match msg {
            ClientMessage::NoteBatch { notes } => {
                assert_eq!(notes.action, NoteAction::Move);
                assert_eq!(notes.tick_offset, Some(480.0));
                assert_eq!(notes.key_offset, Some(2));
            }
            _ => panic!("wrong variant"),
        }
    }
}

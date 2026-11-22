//! 音符批量操作与视图状态（与客户端 `types/note.rs`、`types/view.rs` 一致）

use serde::{Deserialize, Serialize};

/// 音符同步结构（与客户端 `SyncNote` 一致）
///
/// 身份即音乐内容 `(tick, key, length, velocity, channel, track_index)`：
/// 按值引用，不再使用音符级全局 ID（与客户端 d792c6d7 保持一致）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncNote {
    pub tick: f32,
    pub key: u16,
    pub length: f32,
    pub velocity: u8,
    pub channel: u8,
    pub track_index: usize,
}

/// 音符按值身份键：服务端 `notes` 快照的 HashMap 主键。
///
/// f32 字段用 `to_bits` 转为整数以满足 `Eq + Hash`（值与客户端按值身份一致）。
/// 见客户端 `types/note.rs` 的 SyncNote 文档：身份即音乐内容。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NoteKey {
    tick_bits: u32,
    key: u16,
    length_bits: u32,
    velocity: u8,
    channel: u8,
    track_index: usize,
}

impl From<&SyncNote> for NoteKey {
    fn from(n: &SyncNote) -> Self {
        Self {
            tick_bits: n.tick.to_bits(),
            key: n.key,
            length_bits: n.length.to_bits(),
            velocity: n.velocity,
            channel: n.channel,
            track_index: n.track_index,
        }
    }
}

/// 音符批量操作（与客户端 `NoteBatchOperation` 一致）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteBatchOperation {
    pub action: NoteAction,
    pub notes: Vec<SyncNote>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_track: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_track: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tick_offset: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_offset: Option<i16>,
    pub timestamp: u64,
}

/// 音符操作类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NoteAction {
    Add,
    Update,
    Delete,
    Move,
    Copy,
    Paste,
}

/// 视图状态（与客户端 `ViewState` 一致）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewState {
    pub scroll_x: f32,
    pub scroll_y: f32,
    pub zoom_x: f32,
    pub zoom_y: f32,
    pub total_ticks: u32,
    pub key_count: u16,
    pub visible_key_count: u16,
    pub ppq: u16,
    pub keyboard_width: f32,
    pub snap_precision: f32,
    pub default_note_length: f32,
}

impl Default for ViewState {
    fn default() -> Self {
        Self {
            scroll_x: 0.0,
            scroll_y: 0.0,
            zoom_x: 0.1,
            zoom_y: 20.0,
            total_ticks: 1920 * 4 * 100,
            key_count: 128,
            visible_key_count: 128,
            ppq: 1920,
            keyboard_width: 120.0,
            snap_precision: 960.0,
            default_note_length: 960.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_view_state_sane() {
        let vs = ViewState::default();
        assert_eq!(vs.ppq, 1920);
        assert_eq!(vs.key_count, 128);
        assert!(vs.zoom_x > 0.0);
    }

    #[test]
    fn note_batch_serde_roundtrip() {
        let note = SyncNote {
            tick: 960.0,
            key: 60,
            length: 480.0,
            velocity: 100,
            channel: 0,
            track_index: 0,
        };
        let op = NoteBatchOperation {
            action: NoteAction::Add,
            notes: vec![note.clone()],
            source_track: None,
            target_track: None,
            tick_offset: None,
            key_offset: None,
            timestamp: 1700000000000,
        };
        let json = serde_json::to_string(&op).unwrap();
        // wire 协议红线：按值字段齐全，且不得再出现已废弃的 id 字段
        assert!(
            json.contains(r#""tick":960.0"#),
            "tick 必须序列化，实际 JSON: {json}"
        );
        assert!(
            !json.contains(r#""id""#),
            "按值协议不得含 id 字段，实际 JSON: {json}"
        );
        let back: NoteBatchOperation = serde_json::from_str(&json).unwrap();
        assert_eq!(back.action, NoteAction::Add);
        assert_eq!(back.notes[0], note);
        assert!(back.tick_offset.is_none(), "None 字段应被跳过");
    }

    /// 按值身份键：值相同则键相同；任一身份字段变化则键不同。
    #[test]
    fn note_key_is_by_value_identity() {
        let base = SyncNote {
            tick: 960.0,
            key: 60,
            length: 480.0,
            velocity: 100,
            channel: 0,
            track_index: 0,
        };
        assert_eq!(NoteKey::from(&base), NoteKey::from(&base.clone()));
        let mut moved = base.clone();
        moved.tick += 480.0;
        assert_ne!(NoteKey::from(&base), NoteKey::from(&moved));
        let mut other_track = base.clone();
        other_track.track_index = 1;
        assert_ne!(NoteKey::from(&base), NoteKey::from(&other_track));
    }
}

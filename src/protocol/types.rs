//! 音符批量操作与视图状态（与客户端 `types/note.rs`、`types/view.rs` 一致）

use serde::{Deserialize, Serialize};

/// 音符同步结构（与客户端 `SyncNote` 一致）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncNote {
    pub id: String,
    pub tick: f32,
    pub key: u16,
    pub length: f32,
    pub velocity: u8,
    pub channel: u8,
    pub track_index: usize,
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
        let op = NoteBatchOperation {
            action: NoteAction::Add,
            notes: vec![SyncNote {
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
        let json = serde_json::to_string(&op).unwrap();
        let back: NoteBatchOperation = serde_json::from_str(&json).unwrap();
        assert_eq!(back.action, NoteAction::Add);
        assert_eq!(back.notes[0].track_index, 0);
        assert!(back.tick_offset.is_none(), "None 字段应被跳过");
    }
}

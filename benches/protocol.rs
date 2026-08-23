//! 协议层微基准：消息解析 / 序列化的单条成本。
//!
//! 这是广播路径的 CPU 基本盘——1W 次瞬时操作的总 CPU 开销 ≈
//! 单次序列化成本 × 广播次数。目标：单条 < 10µs。

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use lumino_server_rs::protocol::{
    AuthRoomInfo, ClientMessage, NoteAction, NoteBatchOperation, ServerMessage, SyncNote, UserData,
    UserInfo,
};
use serde_json::{json, Value};

/// 构造一条含 n 个音符的 noteBatch 客户端消息
fn note_batch_json(n: usize) -> String {
    let notes: Vec<Value> = (0..n)
        .map(|i| {
            json!({
                "id": i as u64,
                "tick": 960.0, "key": 60, "length": 480.0,
                "velocity": 100, "channel": 0, "trackIndex": 0
            })
        })
        .collect();
    json!({
        "type": "noteBatch",
        "notes": { "action": "add", "notes": notes, "timestamp": 1700000000000i64 }
    })
    .to_string()
}

/// 构造一条 n 个音符的 NoteBatchOperation（服务器广播载荷）
fn note_batch_op(n: usize) -> NoteBatchOperation {
    NoteBatchOperation {
        action: NoteAction::Add,
        notes: (0..n)
            .map(|i| SyncNote {
                id: i as u64,
                tick: 960.0,
                key: 60,
                length: 480.0,
                velocity: 100,
                channel: 0,
                track_index: 0,
            })
            .collect(),
        source_track: None,
        target_track: None,
        tick_offset: None,
        key_offset: None,
        timestamp: 1700000000000,
    }
}

fn bench_deserialize(c: &mut Criterion) {
    let raw_1 = note_batch_json(1);
    let raw_50 = note_batch_json(50);

    c.bench_function("deserialize/note_batch/1_note", |b| {
        b.iter(|| {
            let m: ClientMessage = serde_json::from_str(black_box(&raw_1)).unwrap();
            black_box(m);
        })
    });
    c.bench_function("deserialize/note_batch/50_notes", |b| {
        b.iter(|| {
            let m: ClientMessage = serde_json::from_str(black_box(&raw_50)).unwrap();
            black_box(m);
        })
    });
}

fn bench_serialize(c: &mut Criterion) {
    let op_1 = note_batch_op(1);
    let op_50 = note_batch_op(50);

    c.bench_function("serialize/note_batch_update/1_note", |b| {
        b.iter(|| {
            let msg = ServerMessage::NoteBatchUpdate {
                user_id: "u1",
                operation: black_box(&op_1),
            };
            let s = serde_json::to_string(&msg).unwrap();
            black_box(s);
        })
    });
    c.bench_function("serialize/note_batch_update/50_notes", |b| {
        b.iter(|| {
            let msg = ServerMessage::NoteBatchUpdate {
                user_id: "u1",
                operation: black_box(&op_50),
            };
            let s = serde_json::to_string(&msg).unwrap();
            black_box(s);
        })
    });

    c.bench_function("serialize/authenticated/10_users", |b| {
        b.iter(|| {
            let users: Vec<UserInfo> = (0..10)
                .map(|i| UserInfo {
                    id: format!("u{i}"),
                    username: format!("用户{i}"),
                    color: "#FF6B6B",
                    is_host: i == 0,
                })
                .collect();
            let msg = ServerMessage::Authenticated {
                user_id: "u1",
                room: Some(AuthRoomInfo {
                    id: "ABC123",
                    invite_code: "ABC123",
                    name: "协作房间",
                    host_id: "u1",
                    user_count: 10,
                    max_users: 10,
                    project_name: "协作工程",
                    project_hash: "abc123hash",
                }),
                user: UserData {
                    id: "u1",
                    username: "用户1",
                    extra: &json!({"color": "#FF6B6B", "isHost": true}),
                },
                users,
            };
            let s = serde_json::to_string(&msg).unwrap();
            black_box(s);
        })
    });
}

criterion_group!(benches, bench_deserialize, bench_serialize);
criterion_main!(benches);

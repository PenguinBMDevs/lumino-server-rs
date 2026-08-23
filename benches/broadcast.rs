//! 广播路径基准（内存内，无网络）：序列化一次 + 投递到房间内全部成员。
//!
//! 这是 1W 人次瞬时同步的放大核心：每一条 noteBatch 操作都会触发
//! 一次序列化 + N 次投递。基准直接测量服务器广播函数的真实开销，
//! 不经过网络（channel try_send 即"投递成功"的语义）。

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lumino_server_rs::protocol::{NoteAction, NoteBatchOperation, ServerMessage, SyncNote};
use lumino_server_rs::state::{broadcast_to_room, AppState, CHANNEL_CAPACITY};
use tokio::sync::mpsc;

/// 构造广播用的房间：N 个成员，每个成员一个有界 channel（迭代内手动 drain）
fn build_room(
    state: &Arc<AppState>,
    n: usize,
) -> (
    Vec<mpsc::Receiver<Arc<str>>>,
    Arc<lumino_server_rs::state::Room>,
) {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let host = state.create_user(state.next_socket_id(), "host".into(), tx);
    let room = state.create_room(&host, "bench".into()).unwrap();

    let mut rxs = vec![rx];
    for i in 0..n - 1 {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let user = state.create_user(state.next_socket_id(), format!("u{i}"), tx);
        room.add_user(&user);
        rxs.push(rx);
    }
    (rxs, room)
}

/// 广播 + 消费一次（迭代体）
fn broadcast_once(
    room: &lumino_server_rs::state::Room,
    rxs: &mut [mpsc::Receiver<Arc<str>>],
    user_id: &str,
    op: &NoteBatchOperation,
    metrics: &lumino_server_rs::metrics::Metrics,
) {
    let msg = ServerMessage::NoteBatchUpdate {
        user_id,
        operation: op,
    };
    broadcast_to_room(room, &msg, None, metrics);
    // drain：模拟每个成员消费消息（等价于连接 task 写 TCP）
    for rx in rxs.iter_mut() {
        while rx.try_recv().is_ok() {}
    }
}

fn bench_broadcast(c: &mut Criterion) {
    let op = NoteBatchOperation {
        action: NoteAction::Add,
        notes: vec![SyncNote {
            id: 1,
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

    for &n in &[2usize, 10, 50] {
        let state = AppState::new();
        let (mut rxs, room) = build_room(&state, n);
        let user_id = "host";
        let metrics = state.metrics.clone();

        // 预热
        for _ in 0..1000 {
            broadcast_once(&room, &mut rxs, user_id, &op, &metrics);
        }

        let mut group = c.benchmark_group("broadcast");
        group.bench_function(BenchmarkId::new("room_members", n), |b| {
            b.iter(|| {
                broadcast_once(
                    black_box(&room),
                    &mut rxs,
                    user_id,
                    black_box(&op),
                    &metrics,
                );
                black_box(())
            })
        });
        group.finish();
    }
}

criterion_group!(benches, bench_broadcast);
criterion_main!(benches);

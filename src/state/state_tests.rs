
use super::*;
use crate::protocol::NoteBatchOperation;
use crate::protocol::{NoteAction, NoteKey, SyncNote};

fn make_user(state: &Arc<AppState>) -> Arc<User> {
    let (tx, _rx) = mpsc::channel(CHANNEL_CAPACITY);
    state.create_user(
        state.next_socket_id(),
        format!("user-{}", fastrand::u32(..)),
        tx,
    )
}

/// 按值身份的音符：用 tick 区分不同音符（值相同即同一音符）。
fn note(tick: f32) -> SyncNote {
    note_with(tick, 60, 0)
}

fn note_with(tick: f32, key: u16, track_index: usize) -> SyncNote {
    SyncNote {
        tick,
        key,
        length: 480.0,
        velocity: 100,
        channel: 0,
        track_index,
    }
}

fn op(action: NoteAction, notes: Vec<SyncNote>) -> NoteBatchOperation {
    NoteBatchOperation {
        action,
        notes,
        source_track: None,
        target_track: None,
        tick_offset: None,
        key_offset: None,
        timestamp: now_ms(),
    }
}

#[test]
fn room_create_and_join() {
    let state = AppState::new();
    let host = make_user(&state);
    let room = state.create_room(&host, "测试房".into()).unwrap();
    assert_eq!(room.user_count(), 1);
    assert_eq!(state.stats(), (1, 1));

    let guest = make_user(&state);
    let joined = state.join_room(&room.id, &guest).unwrap();
    assert_eq!(joined.user_count(), 2);

    // 房间满员（host + guest + 8 = 10）
    for _ in 0..8 {
        let u = make_user(&state);
        state.join_room(&room.id, &u).unwrap();
    }
    assert_eq!(room.user_count(), MAX_USERS_PER_ROOM);
    let extra = make_user(&state);
    assert!(state.join_room(&room.id, &extra).is_err());

    // 离开 + 空房间回收
    let last = make_user(&state);
    state.leave_room(&last);
    state.leave_room(&host);
    assert_eq!(state.stats().0, 1);
}

#[test]
fn lifetime_after_leave_deletes_after_delay() {
    let state = AppState::new();
    let host = make_user(&state);
    let room = state.create_room_api_with("延迟房".into(), RoomLifetime::AfterLeave(30));
    let code = room.id.clone();
    state.join_room(&code, &host).unwrap();
    state.leave_room(&host);
    // 用户离开后不应立即删除，仍有 30 分钟宽限
    assert!(state.room_by_code(&code).is_some());
    assert_eq!(state.lifetime_sweep(), 0, "未满宽限期不应回收");
    // 模拟空置已超 31 分钟，下一次 sweep 应删除
    *room.empty_since.write().unwrap() = Some(now_ms() - 31 * 60_000);
    assert_eq!(state.lifetime_sweep(), 1);
    assert!(state.room_by_code(&code).is_none());
}

#[test]
fn lifetime_never_keeps_room() {
    let state = AppState::new();
    let host = make_user(&state);
    let room = state.create_room_api_with("常驻房".into(), RoomLifetime::Never);
    let code = room.id.clone();
    state.join_room(&code, &host).unwrap();
    state.leave_room(&host);
    assert!(state.room_by_code(&code).is_some(), "永不关闭的房间离开后须保留");
    assert_eq!(state.lifetime_sweep(), 0, "永不关闭不应被回收");
}

#[test]
fn lifetime_timed_deletes_at_time() {
    let state = AppState::new();
    let room = state.create_room_api_with("定时房".into(), RoomLifetime::Timed(now_ms() - 1000));
    let code = room.id.clone();
    assert!(state.room_by_code(&code).is_some());
    assert_eq!(state.lifetime_sweep(), 1, "到点后应强制解散");
    assert!(state.room_by_code(&code).is_none());
}

#[test]
fn lifetime_timed_future_keeps() {
    let state = AppState::new();
    let room = state.create_room_api_with("未来定时房".into(), RoomLifetime::Timed(now_ms() + 60_000));
    let code = room.id.clone();
    assert_eq!(state.lifetime_sweep(), 0, "未到点不应删除");
    assert!(state.room_by_code(&code).is_some());
}

#[test]
fn host_transfer() {
    let state = AppState::new();
    let host = make_user(&state);
    let room = state.create_room(&host, "房".into()).unwrap();
    let guest = make_user(&state);
    state.join_room(&room.id, &guest).unwrap();
    state.leave_room(&host);
    let new_host = room.host_id.read().unwrap();
    assert_eq!(new_host.as_deref(), Some(guest.id.as_str()));
}

#[test]
fn project_add_delete() {
    let mut p = ProjectState::new();
    p.apply(&mut op(NoteAction::Add, vec![note(1.0), note(2.0)]), "u1");
    assert_eq!(p.notes.len(), 2);
    p.apply(&mut op(NoteAction::Delete, vec![note(1.0)]), "u1");
    assert_eq!(p.notes.len(), 1);
    let snap = p.snapshot();
    assert_eq!(snap.notes.len(), 2); // add + delete 两条 op
}

#[test]
fn project_paste_uses_client_computed_value() {
    let mut p = ProjectState::new();
    // 客户端已按值算好目标位置（tick/key/轨道），服务端直接按值插入，不再重分配 id。
    let mut paste = op(NoteAction::Paste, vec![note(960.0)]);
    paste.key_offset = Some(12);
    paste.target_track = Some(2);
    paste.notes[0].key = 72;
    paste.notes[0].track_index = 2;
    p.apply(&mut paste, "u1");
    assert_eq!(p.notes.len(), 1);
    let n = p
        .notes
        .get(&NoteKey::from(&note_with(960.0, 72, 2)))
        .expect("paste 后的音符应按值身份在快照中");
    assert_eq!(n.tick, 960.0);
    assert_eq!(n.key, 72);
    assert_eq!(n.track_index, 2);
}

/// 按值身份：相同值 paste 互相覆盖（幂等），不同值并存
#[test]
fn project_paste_by_value_identity() {
    let mut p = ProjectState::new();
    // 同样的值 paste 三次 → 按值身份相同，快照只有 1 个
    for _ in 0..3 {
        let paste = op(NoteAction::Paste, vec![note(0.0)]);
        p.apply(&mut paste.clone(), "u1");
    }
    assert_eq!(p.notes.len(), 1, "相同值 paste 应按值去重");
    // 不同 tick 的 paste → 不同身份，并存
    let paste2 = op(NoteAction::Paste, vec![note(480.0)]);
    p.apply(&mut paste2.clone(), "u1");
    assert_eq!(p.notes.len(), 2, "不同值 paste 应并存");
}

#[test]
fn project_snapshot_after_truncate() {
    let mut p = ProjectState::new();
    for i in 0..(MAX_OPS_LOG + 10) {
        let mut op = op(NoteAction::Add, vec![note(i as f32)]);
        op.timestamp = i as u64;
        p.apply(&mut op, "u1");
    }
    assert!(p.truncated);
    assert_eq!(p.ops.len(), MAX_OPS_LOG);
    let snap = p.snapshot();
    assert_eq!(snap.notes.len(), 1); // 合成一条完整 add
    assert_eq!(snap.notes[0].notes.len(), MAX_OPS_LOG + 10); // 全部音符
}

#[test]
fn project_move() {
    let mut p = ProjectState::new();
    p.apply(&mut op(NoteAction::Add, vec![note(960.0)]), "u1");
    let mut mv = op(NoteAction::Move, vec![note(960.0)]);
    mv.tick_offset = Some(480.0);
    mv.key_offset = Some(2);
    p.apply(&mut mv, "u1");
    // Move 改了 tick/key → 按值身份也变，旧键应被移除、新键插入。
    assert!(!p.notes.contains_key(&NoteKey::from(&note(960.0))));
    let n = p
        .notes
        .get(&NoteKey::from(&note_with(1440.0, 62, 0)))
        .expect("move 后的音符应按新值身份存在");
    assert_eq!(n.tick, 1440.0);
    assert_eq!(n.key, 62);
}

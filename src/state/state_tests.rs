
use super::*;
use crate::protocol::NoteBatchOperation;
use crate::protocol::{NoteAction, SyncNote};

fn make_user(state: &Arc<AppState>) -> Arc<User> {
    let (tx, _rx) = mpsc::channel(CHANNEL_CAPACITY);
    state.create_user(
        state.next_socket_id(),
        format!("user-{}", fastrand::u32(..)),
        tx,
    )
}

fn note(id: u64) -> SyncNote {
    SyncNote {
        id,
        tick: 0.0,
        key: 60,
        length: 480.0,
        velocity: 100,
        channel: 0,
        track_index: 0,
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
    p.apply(&mut op(NoteAction::Add, vec![note(1), note(2)]), "u1");
    assert_eq!(p.notes.len(), 2);
    p.apply(&mut op(NoteAction::Delete, vec![note(1)]), "u1");
    assert_eq!(p.notes.len(), 1);
    let snap = p.snapshot();
    assert_eq!(snap.notes.len(), 2); // add + delete 两条 op
}

#[test]
fn project_paste_regenerates_id() {
    let mut p = ProjectState::new();
    let mut paste = op(NoteAction::Paste, vec![note(999)]);
    paste.tick_offset = Some(960.0);
    paste.key_offset = Some(12);
    paste.target_track = Some(2);
    let old_id = paste.notes[0].id;
    p.apply(&mut paste, "u1");
    let new_id = paste.notes[0].id;
    assert_ne!(new_id, old_id);
    assert_eq!(new_id, 1, "服务端 paste id 从 1 开始自增");
    assert_eq!(p.notes.len(), 1);
    assert!(p.notes.contains_key(&new_id));
    let n = p.notes.get(&new_id).expect("paste 后的音符应在快照中");
    assert_eq!(n.tick, 960.0);
    assert_eq!(n.key, 72);
    assert_eq!(n.track_index, 2);
}

/// paste id 计数器必须单调自增，多次 paste 不能互相覆盖
#[test]
fn project_paste_id_monotonic() {
    let mut p = ProjectState::new();
    let mut ids = Vec::new();
    for _ in 0..3 {
        let mut paste = op(NoteAction::Paste, vec![note(999)]);
        paste.target_track = Some(0);
        p.apply(&mut paste, "u1");
        ids.push(paste.notes[0].id);
    }
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(p.notes.len(), 3, "三次 paste 应产生三个独立音符");
    assert_eq!(p.next_paste_id, 4);
}

#[test]
fn project_snapshot_after_truncate() {
    let mut p = ProjectState::new();
    for i in 0..(MAX_OPS_LOG + 10) {
        let mut op = op(NoteAction::Add, vec![note(i as u64)]);
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
    p.apply(&mut op(NoteAction::Add, vec![note(1)]), "u1");
    let mut mv = op(NoteAction::Move, vec![note(1)]);
    mv.tick_offset = Some(480.0);
    mv.key_offset = Some(2);
    p.apply(&mut mv, "u1");
    let n = p.notes.get(&1).expect("move 后音符 1 应存在");
    assert_eq!(n.tick, 480.0);
    assert_eq!(n.key, 62);
}

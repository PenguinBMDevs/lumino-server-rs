use super::*;
use crate::state::MAX_USERS_PER_ROOM;
use serde_json::Value;

#[test]
fn dispatch_unknown_message_type() {
    // 未知 type → 无效的消息格式（不应 panic）
    let state = AppState::new();
    let (tx, mut rx) = mpsc::channel::<Arc<str>>(16);
    let mut user = None;
    dispatch(&state, &mut user, 1, &None, &tx, r#"{"type":"hack"}"#);
    let msg = rx.try_recv().unwrap();
    let v: Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["type"], "error");
    assert!(v["error"].as_str().unwrap().contains("无效"));
}

#[test]
fn dispatch_bad_json() {
    let state = AppState::new();
    let (tx, mut rx) = mpsc::channel::<Arc<str>>(16);
    let mut user = None;
    dispatch(&state, &mut user, 1, &None, &tx, "not-json");
    let msg = rx.try_recv().unwrap();
    let v: Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["type"], "error");
}

#[test]
fn dispatch_auth_then_note_batch() {
    let state = AppState::new();
    let (tx, mut rx) = mpsc::channel::<Arc<str>>(64);
    let mut user = None;

    // 认证
    dispatch(
        &state,
        &mut user,
        1,
        &None,
        &tx,
        r#"{"type":"auth","username":"测试"}"#,
    );
    let msg = rx.try_recv().unwrap();
    let v: Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["type"], "authenticated");
    assert!(user.is_some());

    // 创建房间
    dispatch(
        &state,
        &mut user,
        1,
        &None,
        &tx,
        r#"{"type":"createRoom","name":"房"}"#,
    );
    let msg = rx.try_recv().unwrap();
    let v: Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["type"], "roomCreated");
    let invite = v["room"]["inviteCode"].as_str().unwrap().to_string();

    // 第二个用户加入
    let (tx2, mut rx2) = mpsc::channel::<Arc<str>>(64);
    let mut user2 = None;
    dispatch(
        &state,
        &mut user2,
        2,
        &None,
        &tx2,
        r#"{"type":"auth","username":"用户2"}"#,
    );
    rx2.try_recv().unwrap(); // authenticated

    dispatch(
        &state,
        &mut user2,
        2,
        &None,
        &tx2,
        &format!(r#"{{"type":"joinRoom","inviteCode":"{invite}"}}"#),
    );
    let msg2 = rx2.try_recv().unwrap();
    let v2: Value = serde_json::from_str(&msg2).unwrap();
    assert_eq!(v2["type"], "roomJoined");
    assert_eq!(v2["users"].as_array().unwrap().len(), 2);

    // user1 收到 userJoined
    let msg1 = rx.try_recv().unwrap();
    let v1: Value = serde_json::from_str(&msg1).unwrap();
    assert_eq!(v1["type"], "userJoined");

    // user2 发 noteBatch → user1 收到广播
    let note_op = r#"{"type":"noteBatch","notes":{"action":"add","notes":[{"id":"n1","tick":960,"key":60,"length":480,"velocity":100,"channel":0,"trackIndex":0}],"timestamp":1700000000000}}"#;
    dispatch(&state, &mut user2, 2, &None, &tx2, note_op);
    let msg1 = rx.try_recv().unwrap();
    let v1: Value = serde_json::from_str(&msg1).unwrap();
    assert_eq!(v1["type"], "noteBatchUpdate");
    assert_eq!(v1["operation"]["notes"][0]["id"], "n1");
    // user2 自己（排除发送者）不应收到
    assert!(rx2.try_recv().is_err());
}

#[test]
fn dispatch_full_sync() {
    let state = AppState::new();
    let (tx, mut rx) = mpsc::channel::<Arc<str>>(64);
    let mut user = None;
    dispatch(
        &state,
        &mut user,
        1,
        &None,
        &tx,
        r#"{"type":"auth","username":"A"}"#,
    );
    rx.try_recv().unwrap();
    dispatch(
        &state,
        &mut user,
        1,
        &None,
        &tx,
        r#"{"type":"createRoom","name":"房"}"#,
    );
    rx.try_recv().unwrap();
    // 发 noteBatch 让状态有内容（单用户房间，广播排除自己，无消息可收）
    dispatch(
        &state,
        &mut user,
        1,
        &None,
        &tx,
        r#"{"type":"noteBatch","notes":{"action":"add","notes":[{"id":"n1","tick":0,"key":60,"length":480,"velocity":100,"channel":0,"trackIndex":0}],"timestamp":1}}"#,
    );
    // requestSync
    dispatch(
        &state,
        &mut user,
        1,
        &None,
        &tx,
        r#"{"type":"requestSync"}"#,
    );
    let msg = rx.try_recv().unwrap();
    let v: Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["type"], "fullSync");
    assert_eq!(v["projectState"]["notes"][0]["action"], "add");
}

#[test]
fn dispatch_room_full() {
    let state = AppState::new();
    let mut users = Vec::new();
    let mut txs = Vec::new();
    let invite;
    {
        let (tx, mut rx) = mpsc::channel::<Arc<str>>(64);
        let mut u = None;
        dispatch(
            &state,
            &mut u,
            1,
            &None,
            &tx,
            r#"{"type":"auth","username":"h"}"#,
        );
        rx.try_recv().unwrap();
        dispatch(
            &state,
            &mut u,
            1,
            &None,
            &tx,
            r#"{"type":"createRoom","name":"房"}"#,
        );
        let v: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        invite = v["room"]["inviteCode"].as_str().unwrap().to_string();
        users.push(u.unwrap());
        txs.push(tx);
    }
    for i in 2..=MAX_USERS_PER_ROOM + 1 {
        let (tx, mut rx) = mpsc::channel::<Arc<str>>(64);
        let mut u = None;
        dispatch(
            &state,
            &mut u,
            i as u64,
            &None,
            &tx,
            &format!(r#"{{"type":"auth","username":"u{i}"}}"#),
        );
        rx.try_recv().unwrap();
        dispatch(
            &state,
            &mut u,
            i as u64,
            &None,
            &tx,
            &format!(r#"{{"type":"joinRoom","inviteCode":"{invite}"}}"#),
        );
        let v: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        if i <= MAX_USERS_PER_ROOM {
            assert_eq!(v["type"], "roomJoined", "第 {i} 个用户应加入成功");
        } else {
            assert_eq!(v["type"], "roomError", "第 {i} 个用户应被拒绝");
            assert!(v["error"].as_str().unwrap().contains("已满"));
        }
    }
    let _ = users;
    let _ = txs;
}

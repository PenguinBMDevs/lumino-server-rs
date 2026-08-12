//! 协议端到端集成测试：启动真实服务器，用 tokio-tungstenite 客户端
//! 模拟 lumino-rs 客户端的完整交互流程，验证消息格式逐字段兼容。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use lumino_server_rs::http;
use lumino_server_rs::state::AppState;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// 启动服务器，返回端口
async fn start_server() -> u16 {
    let state = AppState::new();
    let app = http::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    // 等待服务器就绪
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    port
}

async fn connect(port: u16, room_param: Option<&str>) -> WsStream {
    let url = match room_param {
        Some(r) => format!("ws://127.0.0.1:{port}/ws?roomId={r}"),
        None => format!("ws://127.0.0.1:{port}/ws"),
    };
    let (ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WS 连接失败");
    ws
}

async fn send_json(ws: &mut WsStream, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

/// 等待指定 type 的消息
async fn wait_for(ws: &mut WsStream, msg_type: &str) -> Value {
    for _ in 0..100 {
        if let Ok(Some(Ok(Message::Text(text)))) =
            tokio::time::timeout(Duration::from_secs(2), ws.next()).await
        {
            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                if v["type"] == msg_type {
                    return v;
                }
            }
        }
    }
    panic!("等待消息 {msg_type} 超时");
}

#[tokio::test]
async fn auth_flow_matches_client_protocol() {
    let port = start_server().await;
    let mut ws = connect(port, None).await;

    send_json(&mut ws, json!({"type":"auth","username":"小明"})).await;
    let resp = wait_for(&mut ws, "authenticated").await;

    // 客户端 ServerMessage::Authenticated 期望的字段
    assert!(resp["userId"].as_str().unwrap().len() >= 16, "userId 存在");
    assert!(resp["room"].is_null(), "无房间参数时 room 为 null");
    assert_eq!(resp["users"].as_array().unwrap().len(), 0);
    // UserData: {id, username, flatten extra}
    assert!(resp["user"]["id"].as_str().is_some());
    assert_eq!(resp["user"]["username"], "小明");
    // extra 包含 color / isHost（客户端 UserData flatten 收集）
    assert!(resp["user"]["color"].as_str().unwrap().starts_with('#'));
    assert_eq!(resp["user"]["isHost"], false);

    // ping → pong
    send_json(&mut ws, json!({"type":"ping","timestamp":123456789})).await;
    let pong = wait_for(&mut ws, "pong").await;
    assert_eq!(pong["timestamp"], 123456789);
    assert!(pong["serverTime"].as_u64().unwrap() > 0);

    // 未认证状态发其他消息不应 panic
    send_json(
        &mut ws,
        json!({"type":"mouseMove","position":{"x":1.0,"y":2.0}}),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn auth_with_room_param_auto_join() {
    let port = start_server().await;
    let mut ws = connect(port, Some("TESTROOM")).await;

    send_json(&mut ws, json!({"type":"auth","username":"房主"})).await;
    let resp = wait_for(&mut ws, "authenticated").await;

    // AuthRoomInfo 字段完整性
    let room = &resp["room"];
    assert_eq!(room["id"], "TESTROOM");
    assert_eq!(room["inviteCode"], "TESTROOM");
    assert_eq!(room["name"], "协作房间 TESTROOM");
    assert_eq!(room["hostId"], resp["userId"]);
    assert_eq!(room["userCount"], 1);
    assert_eq!(room["maxUsers"], 10);
    assert_eq!(resp["user"]["isHost"], true);

    // 第二个用户加入 → 第一个用户收到 userJoined
    let mut ws2 = connect(port, Some("TESTROOM")).await;
    send_json(&mut ws2, json!({"type":"auth","username":"成员2"})).await;
    let resp2 = wait_for(&mut ws2, "authenticated").await;
    assert_eq!(resp2["room"]["userCount"], 2);

    let joined = wait_for(&mut ws, "userJoined").await;
    assert_eq!(joined["user"]["username"], "成员2");
    assert_eq!(joined["user"]["isHost"], false);
}

#[tokio::test]
async fn create_join_room_full_flow() {
    let port = start_server().await;
    let mut ws1 = connect(port, None).await;
    send_json(&mut ws1, json!({"type":"auth","username":"用户A"})).await;
    wait_for(&mut ws1, "authenticated").await;

    // 创建房间
    send_json(&mut ws1, json!({"type":"createRoom","name":"我的协作"})).await;
    let created = wait_for(&mut ws1, "roomCreated").await;
    let invite = created["room"]["inviteCode"].as_str().unwrap().to_string();
    assert_eq!(created["room"]["name"], "我的协作");

    // 用户 B 加入
    let mut ws2 = connect(port, None).await;
    send_json(&mut ws2, json!({"type":"auth","username":"用户B"})).await;
    wait_for(&mut ws2, "authenticated").await;
    send_json(&mut ws2, json!({"type":"joinRoom","inviteCode":invite})).await;
    let joined = wait_for(&mut ws2, "roomJoined").await;
    assert_eq!(joined["users"].as_array().unwrap().len(), 2);
    // projectState: {notes, viewState, lastModified, modifiedBy}
    assert!(joined["projectState"]["notes"].is_array());
    assert!(joined["projectState"]["viewState"]["ppq"].is_number());

    // A 收到 userJoined
    let notice = wait_for(&mut ws1, "userJoined").await;
    assert_eq!(notice["user"]["username"], "用户B");

    // B 离开 → A 收到 userLeft
    send_json(&mut ws2, json!({"type":"leaveRoom"})).await;
    let left = wait_for(&mut ws1, "userLeft").await;
    assert_eq!(left["userId"], notice["user"]["id"]);

    // 无效邀请码
    send_json(&mut ws2, json!({"type":"joinRoom","inviteCode":"XXXXXX"})).await;
    let err = wait_for(&mut ws2, "roomError").await;
    assert!(err["error"].as_str().unwrap().contains("无效"));
}

#[tokio::test]
async fn note_batch_broadcast_and_sync() {
    let port = start_server().await;
    let mut ws1 = connect(port, None).await;
    send_json(&mut ws1, json!({"type":"auth","username":"A"})).await;
    wait_for(&mut ws1, "authenticated").await;
    send_json(&mut ws1, json!({"type":"createRoom","name":"房"})).await;
    let created = wait_for(&mut ws1, "roomCreated").await;
    let invite = created["room"]["inviteCode"].as_str().unwrap().to_string();

    let mut ws2 = connect(port, None).await;
    send_json(&mut ws2, json!({"type":"auth","username":"B"})).await;
    let auth2 = wait_for(&mut ws2, "authenticated").await;
    let user_b_id = auth2["userId"].as_str().unwrap().to_string();
    send_json(&mut ws2, json!({"type":"joinRoom","inviteCode":invite})).await;
    wait_for(&mut ws2, "roomJoined").await;
    wait_for(&mut ws1, "userJoined").await;

    // B 发送 noteBatch（add）
    let op = json!({
        "type": "noteBatch",
        "notes": {
            "action": "add",
            "notes": [{
                "id": "note-1", "tick": 960.0, "key": 60, "length": 480.0,
                "velocity": 100, "channel": 0, "trackIndex": 0
            }],
            "timestamp": 1700000000000i64,
        }
    });
    send_json(&mut ws2, op).await;

    // A 收到 noteBatchUpdate（B 自己收不到）
    let update = wait_for(&mut ws1, "noteBatchUpdate").await;
    assert_eq!(update["userId"], user_b_id);
    assert_eq!(update["operation"]["action"], "add");
    assert_eq!(update["operation"]["notes"][0]["id"], "note-1");
    assert_eq!(update["operation"]["notes"][0]["trackIndex"], 0);

    // B requestSync → fullSync 包含刚才的操作
    send_json(&mut ws2, json!({"type":"requestSync"})).await;
    let sync = wait_for(&mut ws2, "fullSync").await;
    let notes = sync["projectState"]["notes"].as_array().unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0]["notes"][0]["id"], "note-1");
    assert_eq!(sync["users"].as_array().unwrap().len(), 2);
}

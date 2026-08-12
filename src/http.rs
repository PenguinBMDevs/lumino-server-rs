//! HTTP API 与监控面板路由。
//!
//! 端点：
//! - `GET /health`     健康检查 + 统计摘要
//! - `GET /info`       服务器信息
//! - `GET /metrics`    指标 JSON（含进程 RSS / CPU%，供监控面板轮询）
//! - `GET /monitor`    监控面板（HTML，Chart.js）
//! - `GET /logs`       日志面板（HTML，WebSocket 订阅实时日志）
//! - `POST /api/room/create`   预创建房间（返回 WebSocket URL）
//! - `GET /api/room/{code}/info`  房间信息

use std::sync::Arc;
use std::sync::Mutex;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::state::AppState;
use crate::util::now_ms;

/// 进程系统资源采样器（sysinfo 实例复用，降低重复初始化的开销）
struct SysSampler {
    sys: sysinfo::System,
    pid: sysinfo::Pid,
    last_cpu: f32,
}

impl SysSampler {
    fn new() -> Self {
        let mut sys = sysinfo::System::new();
        let pid = sysinfo::Pid::from_u32(std::process::id());
        // 首次采样建立基准（cpu_usage 为两次采样间差值）
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), false);
        Self {
            sys,
            pid,
            last_cpu: 0.0,
        }
    }

    /// 返回 (RSS MB, CPU %)
    fn sample(&mut self) -> (f32, f32) {
        self.sys
            .refresh_processes(sysinfo::ProcessesToUpdate::Some(&[self.pid]), false);
        if let Some(p) = self.sys.process(self.pid) {
            let cpu = p.cpu_usage();
            self.last_cpu = cpu;
            (p.memory() as f32 / 1024.0 / 1024.0, cpu)
        } else {
            (0.0, self.last_cpu)
        }
    }
}

static SYS: Mutex<Option<SysSampler>> = Mutex::new(None);

fn sys_sample() -> (f32, f32) {
    let mut guard = SYS.lock().unwrap_or_else(|p| p.into_inner());
    if guard.is_none() {
        *guard = Some(SysSampler::new());
    }
    guard.as_mut().map(|s| s.sample()).unwrap_or((0.0, 0.0))
}

/// 组装路由
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ws", get(crate::ws::ws_handler))
        .route("/health", get(health))
        .route("/info", get(info))
        .route("/metrics", get(metrics_json))
        .route("/monitor", get(monitor_html))
        .route("/logs", get(logs_html))
        .route("/api/room/create", post(create_room_api))
        .route("/api/room/{code}/info", get(room_info_api))
        .with_state(state)
}

/// CORS 头（对所有响应生效）
pub fn cors_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*".parse().unwrap());
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        "GET, POST, PUT, DELETE, OPTIONS".parse().unwrap(),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        "Content-Type, Authorization".parse().unwrap(),
    );
    headers
}

/// GET /health
async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let (rooms, users) = state.stats();
    let (rss_mb, cpu_pct) = sys_sample();
    let m = state.metrics.snapshot();
    let body = json!({
        "status": "ok",
        "timestamp": now_ms(),
        "rooms": { "totalRooms": rooms, "totalUsers": m.users_current },
        "users": { "totalUsers": users },
        "connections": { "current": m.conns_current, "total": m.conns_total },
        "resources": { "rss_mb": rss_mb, "cpu_pct": cpu_pct },
    });
    (cors_headers(), Json(body))
}

/// GET /info
async fn info() -> impl IntoResponse {
    let body = json!({
        "name": "Lumino Collaborative Server (Rust)",
        "version": env!("CARGO_PKG_VERSION"),
        "websocket": { "path": "/ws", "protocol": "json" },
        "features": [
            "real-time-collaboration",
            "mouse-tracking",
            "note-batch-operations",
            "midi-event-sync",
            "project-state-sync",
            "monitor-dashboard",
        ],
    });
    (cors_headers(), Json(body))
}

/// GET /metrics — 监控面板数据源（低频调用，1s 一次）
async fn metrics_json(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let m = state.metrics.snapshot();
    let rates = state
        .sampler
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .sample(m);
    let (rss_mb, cpu_pct) = sys_sample();
    let (rooms, users) = state.stats();

    let body = json!({
        "status": "ok",
        "timestamp": now_ms(),
        "uptime_sec": rates_uptime(),
        "connections": { "current": m.conns_current, "total": m.conns_total },
        "rooms": rooms,
        "users": users,
        "rates": {
            "msgs_in": rates.msgs_in,
            "msgs_out": rates.msgs_out,
            "note_ops": rates.note_ops,
            "bytes_in": rates.bytes_in,
            "bytes_out": rates.bytes_out,
        },
        "bytes": { "in": m.bytes_in, "out": m.bytes_out },
        "resources": { "rss_mb": rss_mb, "cpu_pct": cpu_pct, "cpu_cores": num_cpus() },
        "dropped": m.msgs_dropped,
    });
    (cors_headers(), Json(body))
}

fn rates_uptime() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs()
}

fn num_cpus() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    })
}

/// GET /monitor
async fn monitor_html() -> impl IntoResponse {
    (
        cors_headers(),
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        include_str!("../assets/monitor.html"),
    )
}

/// GET /logs
async fn logs_html() -> impl IntoResponse {
    (
        cors_headers(),
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        include_str!("../assets/logs.html"),
    )
}

/// POST /api/room/create — 预创建房间（HTTP 通道，供网页/客户端拿 WebSocket URL）
async fn create_room_api(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    tracing::debug!("[api] create_room 请求到达");
    let name = payload
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "error": "Missing required field: name" })),
        );
    }

    // 预创建房间（无用户占用：临时 host 占位由第一个 WS 用户接管）
    tracing::debug!("[api] 调用 create_room_api: {name}");
    let room = state.create_room_api(name);
    tracing::debug!("[api] 房间已创建: {}", room.id);

    // 构造 WebSocket URL（优先 X-Forwarded-Proto 与 Host）
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|p| if p == "https" { "wss:" } else { "ws:" })
        .unwrap_or("ws:");
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost:3000");
    let ws_url = format!("{proto}//{host}/ws?roomId={}", room.id);

    (
        StatusCode::OK,
        cors_headers(),
        Json(json!({
            "success": true,
            "room": {
                "id": room.id,
                "inviteCode": room.id,
                "name": room.name,
                "hostId": "",
            },
            "webSocketUrl": ws_url,
        })),
    )
}

/// GET /api/room/{code}/info
async fn room_info_api(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> impl IntoResponse {
    match state.room_by_code(&code) {
        Some(room) => {
            let info = room.info();
            (
                StatusCode::OK,
                cors_headers(),
                Json(json!({
                    "id": info.id,
                    "inviteCode": info.invite_code,
                    "name": info.name,
                    "hostId": info.host_id,
                    "userCount": info.user_count,
                    "maxUsers": info.max_users,
                })),
            )
        }
        None => (
            StatusCode::NOT_FOUND,
            cors_headers(),
            Json(json!({ "error": "Room not found" })),
        ),
    }
}

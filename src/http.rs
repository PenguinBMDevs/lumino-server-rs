//! HTTP API 与监控面板路由。
//!
//! 端点：
//! - `GET /health`     健康检查 + 统计摘要
//! - `GET /info`       服务器信息
//! - `GET /metrics`    指标 JSON（含进程 RSS / CPU%，供监控面板轮询）
//! - `GET /monitor`    监控面板（HTML，自绘 sparkline，无外部 CDN）
//! - `GET /logs`       日志面板（HTML，WebSocket 订阅实时日志）
//! - `POST /api/room/create`   预创建房间（返回 WebSocket URL）
//! - `GET /api/room/{code}/info`  房间信息

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::state::{AppState, RoomLifetime};
use crate::util::now_ms;

/// 房间工程上传请求体上限（`.lmpj` 可能包含渲染音频，体积较大）。
/// axum 的 `Bytes` 提取器默认 2 MiB，超出即 `length limit exceeded`，
/// 这里放宽到 512 MiB（LAN 协作场景下足够，且仍设上限防止内存被撑爆）。
const MAX_ROOM_PROJECT_UPLOAD: usize = 512 * 1024 * 1024;

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
        // ── 认证相关 ──
        .route("/", get(index))
        .route("/login", get(login_html))
        .route("/dashboard", get(dashboard_html))
        .route("/change-password", get(change_password_html))
        .route("/register", get(register_html))
        .route("/logout", get(logout))
        // ── 品牌图标（二进制，编译期嵌入；favicon 与 <img> 共用）──
        .route("/app-icon.ico", get(app_icon))
        .route("/favicon.ico", get(app_icon))
        .route("/api/auth/login", post(login_api))
        .route("/api/auth/change-password", post(change_password_api))
        .route("/api/auth/change-username", post(change_username_api))
        .route("/api/auth/register", post(register_api))
        .route("/api/auth/me", get(auth_me))
        // ── 受保护的控制台（需登录；/monitor、/logs、/rooms 统一进入 dashboard）──
        .route("/monitor", get(dashboard_html))
        .route("/logs", get(dashboard_html))
        .route("/rooms", get(dashboard_html))
        // ── 用户管理（仅管理员）──
        .route("/api/users", get(users_list).post(create_user))
        .route("/api/users/{username}", delete(delete_user))
        .route("/api/users/{username}/reset", post(reset_password))
        .route("/api/users/{username}/role", post(set_role))
        // ── 人机验证（Turnstile）设置 ──
        // GET 公开（仅返回 enabled + site_key，供登录/注册页渲染；不含私密密钥）
        // POST 仅管理员：保存站点密钥 + 服务端密钥 + 启用开关
        .route("/api/settings/turnstile/test", post(turnstile_test))
        .route("/api/settings/turnstile", get(turnstile_get).post(turnstile_set))
        .route("/api/room/create", post(create_room_api))
        .route("/api/room/{code}/info", get(room_info_api))
        .route("/api/room/{code}/delete", post(delete_room_api))
        .route("/api/room/{code}/kick", post(kick_user_api))
        .route(
            "/api/room/{code}/project",
            post(upload_room_project_api).get(download_room_project_api),
        )
        .route_layer(DefaultBodyLimit::max(MAX_ROOM_PROJECT_UPLOAD))
        .route("/api/rooms", get(rooms_api))
        .with_state(state)
}

/// Cloudflare Turnstile 服务端校验（人机测试）。
///
/// 配置来自后台持久化的 `TurnstileConfig`（与凭据同密钥加密落盘），不再读取环境变量。
/// 默认 `enabled=false`：模块关闭时直接放行。启用后：
/// 调用 Cloudflare Turnstile siteverify 校验 token。
/// - token 合法性由调用方前置判断；
/// - siteverify 必须 `success===true` 且 `action` 与前端一致；
/// - 失败时透出 Cloudflare 的 error-codes，便于定位（如 invalid-input-secret 表示服务端密钥错误）。
async fn siteverify(secret: &str, token: &str, action: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let resp = client
        .post("https://challenges.cloudflare.com/turnstile/v0/siteverify")
        .form(&[("secret", secret), ("response", token), ("action", action)])
        .timeout(Duration::from_secs(10))
        .send()
        .await;
    let resp = match resp {
        Ok(r) => r,
        Err(e) => return Err(format!("人机验证服务暂不可用: {e}")),
    };
    let data: Value = match resp.json().await {
        Ok(d) => d,
        Err(e) => return Err(format!("人机验证响应解析失败: {e}")),
    };
    if data.get("success").and_then(Value::as_bool) != Some(true) {
        if let Some(Value::Array(arr)) = data.get("error-codes") {
            if !arr.is_empty() {
                return Err(format!(
                    "人机验证未通过: {}",
                    arr.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")
                ));
            }
        }
        return Err("人机验证未通过".to_string());
    }
    if data.get("action").and_then(Value::as_str) != Some(action) {
        return Err("人机验证 action 不匹配".to_string());
    }
    Ok(())
}

/// 登录/注册入口的 Turnstile 校验：
///  - 未启用（`enabled=false`）直接放行；
///  - 必须配置服务端密钥，否则拒绝（避免“启用却无密钥”的默认放行陷阱）；
///  - token 缺失/超长视为非法；
///  - siteverify 必须 `success===true` 且 `action` 与前端一致。
/// 域名白名单本次不做（用户明确要求暂搁置）。
async fn verify_turnstile(cfg: &crate::auth::TurnstileConfig, token: &str, action: &str) -> Result<(), String> {
    if !cfg.enabled {
        return Ok(()); // 默认不启用人机验证
    }
    if cfg.secret_key.trim().is_empty() {
        return Err("人机验证已启用但未配置服务端密钥".to_string());
    }
    if token.is_empty() || token.len() > 2048 {
        return Err("人机验证 token 缺失或非法".to_string());
    }
    siteverify(&cfg.secret_key, token, action).await
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

/// GET / —— 未登录跳 /login；改密中跳 /change-password；否则进 /dashboard
async fn index(headers: HeaderMap) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    if store.session_from_headers(&headers, "full").is_some() {
        return redirect("/dashboard").into_response();
    }
    if store.session_from_headers(&headers, "pwchange").is_some() {
        return redirect("/change-password").into_response();
    }
    redirect("/login").into_response()
}

/// GET /dashboard | /monitor | /logs —— 受保护的控制台外壳（未登录跳 /login）
async fn dashboard_html(headers: HeaderMap) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    if store.session_from_headers(&headers, "full").is_none()
        && store.session_from_headers(&headers, "pwchange").is_none()
    {
        return redirect("/login").into_response();
    }
    (
        cors_headers(),
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("../assets/dashboard.html"),
    )
        .into_response()
}

/// `GET /app-icon.ico` 与 `GET /favicon.ico` —— 返回编译期嵌入的品牌图标。
///
/// 图标为二进制 `.ico`，无法用 `include_str!` 嵌入，故用 `include_bytes!` 并以
/// `image/x-icon` 提供；`.gitattributes` 已将其标记为二进制，避免被 Git 当文本做 diff/换行转换。
async fn app_icon() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "image/x-icon")],
        Bytes::from_static(include_bytes!("../assets/app-icon.ico")),
    )
        .into_response()
}

/// 从创建房间请求体解析生命周期策略（缺省按 AfterLeave(0) 瞬时房间处理）
fn parse_lifetime(payload: &Value) -> RoomLifetime {
    let Some(lt) = payload.get("lifetime") else {
        return RoomLifetime::AfterLeave(0);
    };
    let mode = lt.get("mode").and_then(Value::as_str).unwrap_or("after_leave");
    match mode {
        "never" => RoomLifetime::Never,
        "timed" => {
            let at = lt.get("at").and_then(Value::as_u64).unwrap_or(0);
            if at == 0 {
                RoomLifetime::AfterLeave(0)
            } else {
                RoomLifetime::Timed(at)
            }
        }
        _ => {
            let minutes = lt
                .get("minutes")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            RoomLifetime::AfterLeave(minutes)
        }
    }
}

/// 将生命周期策略序列化为前端可读 JSON
fn lifetime_to_json(lt: &RoomLifetime) -> Value {
    match lt {
        RoomLifetime::AfterLeave(m) => json!({ "mode": "after_leave", "minutes": *m }),
        RoomLifetime::Never => json!({ "mode": "never" }),
        RoomLifetime::Timed(at) => json!({ "mode": "timed", "at": *at }),
    }
}

/// POST /api/room/create — 预创建房间（HTTP 通道，供网页/客户端拿 WebSocket URL）
///
/// 支持附带生命周期策略 `lifetime`：
/// - `{"mode":"after_leave","minutes":N}` 用户离开后 N 分钟关闭（N=0 即离开即关）
/// - `{"mode":"never"}` 永不自动关闭
/// - `{"mode":"timed","at":<epoch_ms>}` 到达指定时间后强制解散
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

    let lifetime = parse_lifetime(&payload);

    // 预创建房间（无用户占用：临时 host 占位由第一个 WS 用户接管）
    tracing::debug!("[api] 调用 create_room_api: {name}");
    let room = state.create_room_api_with(name, lifetime);
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
                "lifetime": lifetime_to_json(&room.lifetime()),
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
                    "projectName": info.project_name,
                    "projectHash": info.project_hash,
                    "lifetime": lifetime_to_json(&room.lifetime()),
                    "emptySince": room.empty_since(),
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

/// POST /api/room/{code}/delete — 解散房间（房间管理）
async fn delete_room_api(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> impl IntoResponse {
    match state.delete_room(&code) {
        Ok(()) => (
            StatusCode::OK,
            cors_headers(),
            Json(json!({ "success": true })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "error": e })),
        ),
    }
}

/// POST /api/room/{code}/kick — 将用户移出房间
async fn kick_user_api(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let user_id = payload
        .get("user_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if user_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "error": "缺少 user_id" })),
        );
    }
    match state.kick_user(&code, &user_id) {
        Ok(()) => (
            StatusCode::OK,
            cors_headers(),
            Json(json!({ "success": true })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "error": e })),
        ),
    }
}

/// 房主上传工程文件时的查询参数（工程名 + 内容哈希，供加入者判断“是否同一份工程”）
#[derive(serde::Deserialize)]
struct RoomProjectQuery {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    hash: Option<String>,
}

/// POST /api/room/{code}/project — 房主异步上传当前工程单文件 lmpj（二进制）
///
/// 落盘到程序目录 `user-data/rooms/<code>.lmpj`，并记录工程名/哈希到房间元数据。
async fn upload_room_project_api(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
    Query(params): Query<RoomProjectQuery>,
    body: Bytes,
) -> impl IntoResponse {
    let Some(room) = state.room_by_code(&code) else {
        return (
            StatusCode::NOT_FOUND,
            cors_headers(),
            Json(json!({ "error": "Room not found" })),
        )
            .into_response();
    };
    let dir = crate::state::room_projects_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            cors_headers(),
            Json(json!({ "error": format!("创建目录失败: {e}") })),
        )
            .into_response();
    }
    let path = crate::state::room_project_file_path(&code);
    if let Err(e) = std::fs::write(&path, &body[..]) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            cors_headers(),
            Json(json!({ "error": format!("写入工程文件失败: {e}") })),
        )
            .into_response();
    }
    room.set_project_meta(params.name, params.hash);
    (
        StatusCode::OK,
        cors_headers(),
        Json(json!({ "success": true, "size": body.len() })),
    )
        .into_response()
}

/// GET /api/room/{code}/project — 加入者下载房主上传的工程文件
async fn download_room_project_api(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> impl IntoResponse {
    let path = crate::state::room_project_file_path(&code);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let mut headers = cors_headers();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            (StatusCode::OK, headers, bytes).into_response()
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            cors_headers(),
            Json(json!({ "error": "该房间尚未上传工程文件" })),
        )
            .into_response(),
    }
}

/// GET /api/rooms — 房间管理监控面板数据源：列出所有活跃房间及其成员（内部用户）。
/// 任何已登录用户均可访问（与 /monitor 控制台的可见性一致）。
async fn rooms_api(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    if store
        .session_from_headers(&headers, "full")
        .or_else(|| store.session_from_headers(&headers, "pwchange"))
        .is_none()
    {
        return (
            StatusCode::UNAUTHORIZED,
            cors_headers(),
            Json(json!({ "error": "未登录" })),
        );
    }
    let mut rooms: Vec<Value> = state
        .rooms
        .iter()
        .map(|e| {
            let r = e.value();
            let last = r
                .project
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .last_modified;
            let host = r
                .host_id
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .unwrap_or_default();
            json!({
                "id": r.id,
                "inviteCode": r.id,
                "name": r.name,
                "hostId": host,
                "userCount": r.user_count(),
                "maxUsers": r.max_users,
                "lastModified": last,
                "lifetime": lifetime_to_json(&r.lifetime()),
                "emptySince": r.empty_since(),
                "members": r.users_info(),
            })
        })
        .collect();
    // 按最近活跃时间倒序，最活跃的房间置顶
    rooms.sort_by(|a, b| {
        b.get("lastModified")
            .and_then(Value::as_u64)
            .cmp(&a.get("lastModified").and_then(Value::as_u64))
    });
    (
        StatusCode::OK,
        cors_headers(),
        Json(json!({ "rooms": rooms })),
    )
}

// ───────────────────────────────────────────────────────────────────────────
// 认证相关端点
// ───────────────────────────────────────────────────────────────────────────

/// GET /login —— 登录页
async fn login_html() -> impl IntoResponse {
    (
        cors_headers(),
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("../assets/login.html"),
    )
}

/// GET /change-password —— 强制改密页
async fn change_password_html() -> impl IntoResponse {
    (
        cors_headers(),
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("../assets/change-password.html"),
    )
}

/// GET /logout —— 清除会话并跳转登录页
async fn logout(headers: HeaderMap) -> impl IntoResponse {
    let secure = is_secure(&headers);
    let cookie = format!(
        "lumino_sid=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0{}",
        if secure { "; Secure" } else { "" }
    );
    let mut h = HeaderMap::new();
    h.insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
    let (status, mut rh) = redirect("/login");
    h.extend(rh.drain());
    (status, h)
}

/// GET /api/auth/me —— 返回当前登录态（含角色，前端据此渲染侧栏与跳转）
async fn auth_me(headers: HeaderMap) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    if let Some(c) = store.session_from_headers(&headers, "full") {
        return (
            cors_headers(),
            Json(json!({
                "authenticated": true,
                "scope": c.scope,
                "role": c.role,
                "username": c.sub,
                "requires_change": store.requires_change(&c.sub)
            })),
        );
    }
    if let Some(c) = store.session_from_headers(&headers, "pwchange") {
        return (
            cors_headers(),
            Json(json!({
                "authenticated": true,
                "scope": c.scope,
                "role": c.role,
                "username": c.sub,
                "requires_change": true
            })),
        );
    }
    (cors_headers(), Json(json!({ "authenticated": false })))
}

/// POST /api/auth/login —— 校验用户名/密码并签发会话
async fn login_api(headers: HeaderMap, Json(payload): Json<Value>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let key = client_key(&headers);

    // 暴力破解锁定检查
    let remaining = store.lock_remaining(&key);
    if remaining > 0 {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            cors_headers(),
            Json(json!({
                "success": false,
                "error": format!("尝试次数过多，请 {remaining} 秒后再试"),
                "lock_seconds": remaining
            })),
        );
    }

    let username = payload
        .get("username")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let password = payload
        .get("password")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    // 人机验证（Turnstile）：先过验证码，再校验凭证
    let turnstile_token = payload
        .get("turnstile_token")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let turnstile_cfg = store.get_turnstile();
    if let Err(e) = verify_turnstile(&turnstile_cfg, &turnstile_token, "login").await {
        return (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        );
    }

    if username.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": "请输入用户名" })),
        );
    }

    let Some(role) = store.verify_login(&username, &password) else {
        store.record_fail(&key);
        let rem = store.lock_remaining(&key);
        return (
            StatusCode::UNAUTHORIZED,
            cors_headers(),
            Json(json!({
                "success": false,
                "error": "用户名或密码错误",
                "lock_seconds": rem
            })),
        );
    };

    store.record_ok(&key);
    let requires = store.requires_change(&username);
    let scope = if requires { "pwchange" } else { "full" };
    let token = store.sign_session(scope, &username, &role);
    let max_age = if requires { 900 } else { 43_200 };

    let mut h = cors_headers();
    h.insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&session_cookie(&token, max_age, is_secure(&headers))).unwrap(),
    );
    (
        StatusCode::OK,
        h,
        Json(json!({
            "success": true,
            "requires_change": requires,
            "role": role,
            "username": username
        })),
    )
}

/// POST /api/auth/change-password —— 校验当前密码 + 强度策略后更新（自改）
async fn change_password_api(headers: HeaderMap, Json(payload): Json<Value>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let key = client_key(&headers);

    // 必须持有有效的"仅改密"会话
    let Some(claims) = store.session_from_headers(&headers, "pwchange") else {
        return (
            StatusCode::UNAUTHORIZED,
            cors_headers(),
            Json(json!({ "success": false, "error": "会话已过期，请重新登录" })),
        );
    };
    let username = claims.sub.clone();

    let current = payload
        .get("current_password")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let new = payload
        .get("new_password")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    // 再次校验当前密码（纵深防御：即便拿到改密令牌也需知道原密码）
    if store.verify_login(&username, &current).is_none() {
        store.record_fail(&key);
        return (
            StatusCode::UNAUTHORIZED,
            cors_headers(),
            Json(json!({ "success": false, "error": "当前密码错误" })),
        );
    }

    match store.change_own_password(&username, &current, &new) {
        Ok(()) => {
            store.record_ok(&key);
            // 改密成功后签发完整会话
            let token = store.sign_session("full", &username, &claims.role);
            let mut h = cors_headers();
            h.insert(
                header::SET_COOKIE,
                HeaderValue::from_str(&session_cookie(&token, 43_200, is_secure(&headers))).unwrap(),
            );
            (StatusCode::OK, h, Json(json!({ "success": true })))
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

/// GET /register —— 开放注册页
async fn register_html() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        include_str!("../assets/register.html"),
    )
        .into_response()
}

/// POST /api/auth/change-username —— 修改自己的用户名（需当前密码，刷新会话）
async fn change_username_api(headers: HeaderMap, Json(payload): Json<Value>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store
        .session_from_headers(&headers, "full")
        .or_else(|| store.session_from_headers(&headers, "pwchange"))
    else {
        return (
            StatusCode::UNAUTHORIZED,
            cors_headers(),
            Json(json!({ "success": false, "error": "请先登录" })),
        );
    };
    let current = payload
        .get("current_password")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let new_username = payload
        .get("new_username")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match store.change_own_username(&claims.sub, &current, &new_username) {
        Ok(()) => {
            let token = store.sign_session(&claims.scope, &new_username, &claims.role);
            let max_age = if claims.scope == "pwchange" { 900 } else { 43_200 };
            let mut h = cors_headers();
            h.insert(
                header::SET_COOKIE,
                HeaderValue::from_str(&session_cookie(&token, max_age, is_secure(&headers))).unwrap(),
            );
            (StatusCode::OK, h, Json(json!({ "success": true, "username": new_username })))
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

/// POST /api/auth/register —— 开放注册（默认普通用户）
async fn register_api(Json(payload): Json<Value>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let username = payload
        .get("username")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let password = payload
        .get("password")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    // 人机验证（Turnstile）：先过验证码，再创建账户
    let turnstile_token = payload
        .get("turnstile_token")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let turnstile_cfg = store.get_turnstile();
    if let Err(e) = verify_turnstile(&turnstile_cfg, &turnstile_token, "signup").await {
        return (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        );
    }
    match store.register(&username, &password) {
        Ok(()) => (StatusCode::OK, cors_headers(), Json(json!({ "success": true }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

// ── 用户管理（仅管理员）──

/// GET /api/users —— 列出全部账户（不含密码）
async fn users_list(headers: HeaderMap) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store.session_from_headers(&headers, "full") else {
        return (StatusCode::UNAUTHORIZED, cors_headers(), Json(json!({ "error": "未登录" })));
    };
    if claims.role != crate::auth::ROLE_ADMIN {
        return (StatusCode::FORBIDDEN, cors_headers(), Json(json!({ "error": "权限不足" })));
    }
    (
        StatusCode::OK,
        cors_headers(),
        Json(json!({ "accounts": store.list_accounts() })),
    )
}

/// POST /api/users —— 新增账户
async fn create_user(headers: HeaderMap, Json(payload): Json<Value>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store.session_from_headers(&headers, "full") else {
        return (StatusCode::UNAUTHORIZED, cors_headers(), Json(json!({ "success": false, "error": "未登录" })));
    };
    if claims.role != crate::auth::ROLE_ADMIN {
        return (StatusCode::FORBIDDEN, cors_headers(), Json(json!({ "success": false, "error": "权限不足" })));
    }
    let username = payload
        .get("username")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let password = payload
        .get("password")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let role = payload
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("user")
        .to_string();
    match store.add_account(&claims.role, &username, &password, &role) {
        Ok(()) => (StatusCode::OK, cors_headers(), Json(json!({ "success": true }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

/// DELETE /api/users/{username} —— 删除账户
async fn delete_user(headers: HeaderMap, Path(username): Path<String>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store.session_from_headers(&headers, "full") else {
        return (StatusCode::UNAUTHORIZED, cors_headers(), Json(json!({ "success": false, "error": "未登录" })));
    };
    if claims.role != crate::auth::ROLE_ADMIN {
        return (StatusCode::FORBIDDEN, cors_headers(), Json(json!({ "success": false, "error": "权限不足" })));
    }
    match store.remove_account(&claims.sub, &username) {
        Ok(()) => (StatusCode::OK, cors_headers(), Json(json!({ "success": true }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

/// POST /api/users/{username}/reset —— 管理员重置某账户密码
async fn reset_password(
    headers: HeaderMap,
    Path(username): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store.session_from_headers(&headers, "full") else {
        return (StatusCode::UNAUTHORIZED, cors_headers(), Json(json!({ "success": false, "error": "未登录" })));
    };
    if claims.role != crate::auth::ROLE_ADMIN {
        return (StatusCode::FORBIDDEN, cors_headers(), Json(json!({ "success": false, "error": "权限不足" })));
    }
    let new = payload
        .get("new_password")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match store.reset_password(&claims.role, &username, &new) {
        Ok(()) => (StatusCode::OK, cors_headers(), Json(json!({ "success": true }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

/// POST /api/users/{username}/role —— 管理员修改某账户角色（禁止修改自己）
async fn set_role(
    headers: HeaderMap,
    Path(username): Path<String>,
    Json(payload): Json<Value>,
) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store.session_from_headers(&headers, "full") else {
        return (StatusCode::UNAUTHORIZED, cors_headers(), Json(json!({ "success": false, "error": "未登录" })));
    };
    if claims.role != crate::auth::ROLE_ADMIN {
        return (StatusCode::FORBIDDEN, cors_headers(), Json(json!({ "success": false, "error": "权限不足" })));
    }
    if username == claims.sub {
        return (StatusCode::BAD_REQUEST, cors_headers(), Json(json!({ "success": false, "error": "不能修改自己的角色，请退出后用其他管理员账户操作" })));
    }
    let new_role = payload
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match store.set_role(&claims.role, &username, &new_role) {
        Ok(()) => (StatusCode::OK, cors_headers(), Json(json!({ "success": true }))),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

// ── 人机验证（Turnstile）设置 ──

/// GET /api/settings/turnstile — 公开（供登录/注册页判断是否渲染组件）
/// 仅返回 `enabled` + `site_key`，绝不返回私密的服务端密钥。
async fn turnstile_get() -> impl IntoResponse {
    let cfg = crate::auth::auth_store().get_turnstile();
    (
        StatusCode::OK,
        cors_headers(),
        Json(json!({ "enabled": cfg.enabled, "site_key": cfg.site_key })),
    )
}

/// POST /api/settings/turnstile — 仅管理员：保存配置并加密落盘
async fn turnstile_set(headers: HeaderMap, Json(payload): Json<Value>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store.session_from_headers(&headers, "full") else {
        return (StatusCode::UNAUTHORIZED, cors_headers(), Json(json!({ "success": false, "error": "未登录" })));
    };
    if claims.role != crate::auth::ROLE_ADMIN {
        return (StatusCode::FORBIDDEN, cors_headers(), Json(json!({ "success": false, "error": "权限不足" })));
    }
    let current = store.get_turnstile();
    let new_secret = payload
        .get("secret_key")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    let cfg = crate::auth::TurnstileConfig {
        enabled: payload.get("enabled").and_then(Value::as_bool).unwrap_or(false),
        site_key: payload
            .get("site_key")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string(),
        // 服务端密钥为空时保留已有值（避免仅改站点密钥就清空密钥）
        secret_key: if new_secret.is_empty() {
            current.secret_key
        } else {
            new_secret
        },
    };
    match store.set_turnstile(cfg) {
        Ok(()) => (StatusCode::OK, cors_headers(), Json(json!({ "success": true }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            cors_headers(),
            Json(json!({ "success": false, "error": e })),
        ),
    }
}

/// POST /api/settings/turnstile/test — 仅管理员：用真实挑战校验 site_key + secret_key。
/// 前端先在设置面板用输入的 site_key 渲染 Turnstile 组件、完成挑战拿到 token，再提交此处校验；
/// 通过后才能保存，避免把无效密钥写盘。secret_key 为空时回退到已存储密钥（用于“保留原密钥”场景）。
async fn turnstile_test(headers: HeaderMap, Json(payload): Json<Value>) -> impl IntoResponse {
    let store = crate::auth::auth_store();
    let Some(claims) = store.session_from_headers(&headers, "full") else {
        return (StatusCode::UNAUTHORIZED, cors_headers(), Json(json!({ "ok": false, "error": "未登录" })));
    };
    if claims.role != crate::auth::ROLE_ADMIN {
        return (StatusCode::FORBIDDEN, cors_headers(), Json(json!({ "ok": false, "error": "权限不足" })));
    }
    let site_key = payload.get("site_key").and_then(Value::as_str).unwrap_or("").trim().to_string();
    let token = payload.get("token").and_then(Value::as_str).unwrap_or("").trim().to_string();
    // 服务端密钥：输入为空则回退到已存储密钥（用于“保留原密钥”场景的测试）
    let mut secret = payload.get("secret_key").and_then(Value::as_str).unwrap_or("").trim().to_string();
    if secret.is_empty() {
        secret = store.get_turnstile().secret_key;
    }
    if site_key.is_empty() {
        return (StatusCode::BAD_REQUEST, cors_headers(), Json(json!({ "ok": false, "error": "请先填写站点密钥" })));
    }
    if token.is_empty() {
        return (StatusCode::BAD_REQUEST, cors_headers(), Json(json!({ "ok": false, "error": "请先完成人机验证组件挑战以获取 token" })));
    }
    if secret.is_empty() {
        return (StatusCode::BAD_REQUEST, cors_headers(), Json(json!({ "ok": false, "error": "无可用服务端密钥（请填写，或先在已启用配置中保存）" })));
    }
    match siteverify(&secret, &token, "test").await {
        Ok(()) => (StatusCode::OK, cors_headers(), Json(json!({ "ok": true }))),
        Err(e) => (StatusCode::OK, cors_headers(), Json(json!({ "ok": false, "error": e }))),
    }
}

// ── 工具函数 ──

/// 构造会话 Cookie 字符串
fn session_cookie(value: &str, max_age_secs: i64, secure: bool) -> String {
    let mut c = format!(
        "lumino_sid={value}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age_secs}"
    );
    if secure {
        c.push_str("; Secure");
    }
    c
}

/// 302 跳转到指定路径
fn redirect(to: &str) -> (StatusCode, HeaderMap) {
    let mut h = HeaderMap::new();
    h.insert(
        header::LOCATION,
        HeaderValue::from_str(to).unwrap_or_else(|_| HeaderValue::from_static("/login")),
    );
    (StatusCode::FOUND, h)
}

/// 客户端标识（用于暴力破解锁定）：优先 X-Forwarded-For，否则 "direct"
fn client_key(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or("").trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "direct".to_string())
}

/// 是否处于安全传输（HTTPS）：通过 X-Forwarded-Proto 或 Host 推断
fn is_secure(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|p| p.eq_ignore_ascii_case("https"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Path, State};

    #[tokio::test]
    async fn upload_then_download_then_delete_room_project() {
        let state = AppState::new();
        let room = state.create_room_api("工程测试".into());
        let code = room.id.clone();

        // 上传工程文件
        let resp = upload_room_project_api(
            State(Arc::clone(&state)),
            Path(code.clone()),
            Query(RoomProjectQuery {
                name: Some("我的工程".into()),
                hash: Some("abc123".into()),
            }),
            Bytes::from_static(b"LMPJ-fake-bytes"),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        // 房间元数据已记录工程名/哈希
        let info = room.info();
        assert_eq!(info.project_name, "我的工程");
        assert_eq!(info.project_hash, "abc123");
        assert!(crate::state::room_project_file_path(&code).exists());

        // 下载工程文件
        let resp2 = download_room_project_api(State(Arc::clone(&state)), Path(code.clone()))
            .await
            .into_response();
        assert_eq!(resp2.status(), StatusCode::OK);

        // 房间删除 → 工程文件立即删除
        state.delete_room(&code).unwrap();
        assert!(!crate::state::room_project_file_path(&code).exists());
    }

    #[tokio::test]
    async fn download_missing_room_project_returns_404() {
        let state = AppState::new();
        let room = state.create_room_api("空房间".into());
        let code = room.id.clone();
        let resp = download_room_project_api(State(Arc::clone(&state)), Path(code.clone()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// 读取 JSON 响应体
    async fn resp_json(resp: impl IntoResponse) -> Value {
        let resp = resp.into_response();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn create_room_timed_lifetime_reflected() {
        let state = AppState::new();
        let body = resp_json(
            create_room_api(
                State(Arc::clone(&state)),
                HeaderMap::new(),
                Json(json!({ "name": "定时房", "lifetime": { "mode": "timed", "at": 1700000000000u64 } })),
            )
            .await,
        )
        .await;
        assert_eq!(body["success"], true);
        assert_eq!(body["room"]["lifetime"]["mode"], "timed");
        assert_eq!(body["room"]["lifetime"]["at"], 1700000000000u64);
    }

    #[tokio::test]
    async fn create_room_never_lifetime_reflected() {
        let state = AppState::new();
        let body = resp_json(
            create_room_api(
                State(Arc::clone(&state)),
                HeaderMap::new(),
                Json(json!({ "name": "常驻房", "lifetime": { "mode": "never" } })),
            )
            .await,
        )
        .await;
        assert_eq!(body["success"], true);
        assert_eq!(body["room"]["lifetime"]["mode"], "never");
    }

    #[tokio::test]
    async fn create_room_after_leave_lifetime_reflected() {
        let state = AppState::new();
        let body = resp_json(
            create_room_api(
                State(Arc::clone(&state)),
                HeaderMap::new(),
                Json(json!({ "name": "延迟房", "lifetime": { "mode": "after_leave", "minutes": 15 } })),
            )
            .await,
        )
        .await;
        assert_eq!(body["success"], true);
        assert_eq!(body["room"]["lifetime"]["mode"], "after_leave");
        assert_eq!(body["room"]["lifetime"]["minutes"], 15);
    }

    #[tokio::test]
    async fn create_room_default_lifetime_is_transient() {
        let state = AppState::new();
        let body = resp_json(
            create_room_api(
                State(Arc::clone(&state)),
                HeaderMap::new(),
                Json(json!({ "name": "瞬时房" })),
            )
            .await,
        )
        .await;
        assert_eq!(body["success"], true);
        assert_eq!(body["room"]["lifetime"]["mode"], "after_leave");
        assert_eq!(body["room"]["lifetime"]["minutes"], 0);
    }
}

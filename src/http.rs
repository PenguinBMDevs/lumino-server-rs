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
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
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
        // ── 认证相关 ──
        .route("/", get(index))
        .route("/login", get(login_html))
        .route("/dashboard", get(dashboard_html))
        .route("/change-password", get(change_password_html))
        .route("/logout", get(logout))
        .route("/api/auth/login", post(login_api))
        .route("/api/auth/change-password", post(change_password_api))
        .route("/api/auth/me", get(auth_me))
        // ── 受保护的控制台（需登录；/monitor、/logs 统一进入 dashboard）──
        .route("/monitor", get(dashboard_html))
        .route("/logs", get(dashboard_html))
        // ── 用户管理（仅管理员）──
        .route("/api/users", get(users_list).post(create_user))
        .route("/api/users/{username}", delete(delete_user))
        .route("/api/users/{username}/reset", post(reset_password))
        .route("/api/users/{username}/role", post(set_role))
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

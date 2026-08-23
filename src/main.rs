//! lumino-server-rs —— Lumino 多人协作服务器（Rust 重写版）
//!
//! 设计目标：超低内存/CPU 占用 + 超高吞吐。
//! 协议与 lumino-rs 客户端（crates/collaboration）逐字段兼容。

use std::sync::Arc;

use lumino_server_rs::auth;
use lumino_server_rs::http;
use lumino_server_rs::log;
use lumino_server_rs::state::AppState;

/// 启动配置
struct Config {
    host: String,
    port: u16,
    workers: Option<usize>,
}

impl Config {
    fn from_env() -> Self {
        let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = std::env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3000);
        Self {
            host,
            port,
            workers: None,
        }
    }

    /// 解析 CLI 参数：--port N / --workers N
    fn parse_args(mut self) -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--port" | "-p" => {
                    if let Some(v) = args.get(i + 1).and_then(|s| s.parse().ok()) {
                        self.port = v;
                    }
                    i += 2;
                }
                "--host" => {
                    if let Some(v) = args.get(i + 1) {
                        self.host = v.clone();
                    }
                    i += 2;
                }
                "--workers" => {
                    self.workers = args.get(i + 1).and_then(|s| s.parse().ok());
                    i += 2;
                }
                _ => i += 1,
            }
        }
        self
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env().parse_args();
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let workers = config.workers.unwrap_or_else(|| cores.min(8));

    // 日志初始化（热路径不打日志，此层仅控制台输出）
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_writer(std::io::stderr)
        .compact()
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("lumino")
        .enable_all()
        .build()?;

    runtime.block_on(async_main(config, workers))
}

async fn async_main(config: Config, workers: usize) -> Result<(), Box<dyn std::error::Error>> {
    // 初始化认证模块（生成/加载主密钥与凭据，缺失则用默认密码 admin 初始化）
    auth::init().map_err(|e| {
        eprintln!("认证模块初始化失败: {e}");
        e
    })?;

    let state = AppState::new();
    let app = http::router(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind((config.host.as_str(), config.port)).await?;
    let local_addr = listener.local_addr()?;

    log::log("info", "=================================".to_string());
    log::log(
        "info",
        format!(
            "Lumino 协作服务器 (Rust) v{} 已启动",
            env!("CARGO_PKG_VERSION")
        ),
    );
    log::log("info", format!("监听: http://{}", local_addr));
    log::log("info", format!("WebSocket: ws://{}/ws", local_addr));
    log::log("info", format!("监控面板: http://{}/monitor", local_addr));
    log::log("info", format!("日志面板: http://{}/logs", local_addr));
    log::log(
        "info",
        format!(
            "Tokio workers: {workers} (CPU 核数: {})",
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        ),
    );
    log::log("info", "=================================".to_string());

    // 定期清理：空房间 24h 回收（仅“离开即关闭”的瞬时房间）
    let cleanup_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30 * 60));
        ticker.tick().await; // 跳过立即执行
        loop {
            ticker.tick().await;
            let cleaned = cleanup_state.cleanup_inactive(24 * 60 * 60 * 1000);
            if cleaned > 0 {
                log::log("info", format!("清理了 {cleaned} 个不活跃房间"));
            }
        }
    });

    // 生命周期回收：处理“用户离开后延迟关闭 / 定时关闭”房间（10s 粒度足够分钟级计时）
    let sweep_state = Arc::clone(&state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        ticker.tick().await; // 跳过立即执行
        loop {
            ticker.tick().await;
            let cleaned = sweep_state.lifetime_sweep();
            if cleaned > 0 {
                log::log("info", format!("生命周期回收了 {cleaned} 个房间"));
            }
        }
    });

    // 优雅关闭：Ctrl+C / SIGTERM
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    log::log("info", "服务器已关闭".to_string());
    Ok(())
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c().await.ok();
    log::log("info", "收到退出信号，正在关闭服务器...".to_string());
}

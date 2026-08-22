//! lumino-server-rs —— Lumino 多人协作服务器（Rust 重写版）核心库。
//!
//! 协议与 lumino-rs 客户端（crates/collaboration）逐字段兼容。
//! 集成测试通过 `http::router()` + `state::AppState` 启动真实服务器验证。

pub mod auth;
pub mod http;
pub mod log;
pub mod metrics;
pub mod protocol;
pub mod state;
pub mod util;
pub mod ws;

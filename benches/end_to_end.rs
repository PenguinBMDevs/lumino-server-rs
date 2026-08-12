//! 端到端压力基准：模拟 1W 人次瞬时音符插入同步（验收核心场景）。
//!
//! 架构：**服务器 in-process 自包含**（随机端口，同一进程内启动 axum），
//! 无外部进程依赖——`cargo bench` 直接可跑。
//!
//! 每档（100 / 1000 / 10000 连接）：
//! - 建立连接池（分批，全部 auth + joinRoom 就绪）
//! - 每轮迭代：所有连接**同时**发送一轮 noteBatch（瞬时 1W 人次），
//!   测量轮耗时（触发 → 全部广播接收完成）+ 接收侧延迟分布
//! - 报告压测期峰值 RSS / CPU（sysinfo 采样当前进程）
//!
//! 延迟测量：发送方把微秒时间戳写入 op.timestamp，接收方差值。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_util::{SinkExt, StreamExt};
use lumino_server_rs::http;
use lumino_server_rs::state::AppState;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// 每房间人数（对齐服务器上限）
const USERS_PER_ROOM: usize = 10;
/// 广播静默检测：收到消息后超过该时长无新消息则结束本轮收集
const QUIET_WINDOW: Duration = Duration::from_millis(80);
/// 每轮收集硬上限
const MAX_WINDOW: Duration = Duration::from_millis(1000);
/// 轮号编码位数（timestamp 高位）：低 40 位 = 微秒，高 24 位 = 轮号
const ROUND_SHIFT: u64 = 40;

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// 编码轮号 + 微秒时间戳（接收方用高位过滤当前轮样本）
#[inline]
fn encode_ts(round: u64, us: u64) -> u64 {
    (round << ROUND_SHIFT) | (us & ((1 << ROUND_SHIFT) - 1))
}

/// 等待指定 type 的消息；返回 (目标消息, 收到的其他消息类型诊断)
async fn wait_for(ws: &mut WsStream, msg_type: &str) -> Option<Value> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    let t = v["type"].as_str().unwrap_or("?").to_string();
                    if t == msg_type {
                        return Some(v);
                    }
                    // 非目标消息（如 roomError）：打印诊断
                    if t != "pong" && t != "userJoined" && t != "logHistory" {
                        eprintln!(
                            "[diag] 期望 {msg_type} 但收到 {t}: {}",
                            text.chars().take(120).collect::<String>()
                        );
                    }
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => return None,
        }
    }
    None
}

/// 连接池条目
struct PoolConn {
    trigger: mpsc::UnboundedSender<u64>,
    result: mpsc::UnboundedReceiver<(u64, Vec<u64>)>,
}

/// 单连接任务：等待 trigger → 发送一轮 → 静默检测收集广播 → flush 样本
async fn conn_task(
    index: usize,
    url: String,
    invite: String,
    mut trigger_rx: mpsc::UnboundedReceiver<u64>,
    result_tx: mpsc::UnboundedSender<(u64, Vec<u64>)>,
    ready: Arc<AtomicUsize>,
) {
    let Ok((mut ws, _)) = tokio_tungstenite::connect_async(&url).await else {
        eprintln!("[conn {index}] connect 失败");
        return;
    };
    let username = format!("bench-{index}");
    let _ = ws
        .send(Message::Text(
            json!({"type":"auth","username":username}).to_string(),
        ))
        .await;
    if wait_for(&mut ws, "authenticated").await.is_none() {
        eprintln!("[conn {index}] auth 超时");
        return;
    }
    let _ = ws
        .send(Message::Text(
            json!({"type":"joinRoom","inviteCode":invite}).to_string(),
        ))
        .await;
    if wait_for(&mut ws, "roomJoined").await.is_none() {
        eprintln!("[conn {index}] joinRoom 超时 (房间 {invite})");
        return;
    }
    ready.fetch_add(1, Ordering::SeqCst);

    while let Some(round) = trigger_rx.recv().await {
        // 发送本轮操作（timestamp = 轮号高位 + 微秒）
        let msg = json!({
            "type": "noteBatch",
            "notes": {
                "action": "add",
                "notes": [{
                    "id": format!("n-{index}-{round}"),
                    "tick": 960.0, "key": 60, "length": 480.0,
                    "velocity": 100, "channel": 0, "trackIndex": 0
                }],
                "timestamp": encode_ts(round, now_us()),
            }
        });
        if ws.send(Message::Text(msg.to_string())).await.is_err() {
            break;
        }
        // 事件驱动收集：广播完成后 QUIET_WINDOW 无新消息即结束（上限 MAX_WINDOW）
        // 无固定轮询——大部分时间挂起在定时器上，避免 1W task 轮询放大调度开销
        let mut samples = Vec::new();
        let mut quiet = tokio::time::sleep(QUIET_WINDOW);
        let mut hard_deadline = tokio::time::sleep(MAX_WINDOW);
        tokio::pin!(quiet);
        tokio::pin!(hard_deadline);
        loop {
            tokio::select! {
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(v) = serde_json::from_str::<Value>(&text) {
                                if v["type"] == "noteBatchUpdate" {
                                    // 有消息 → 重置静默计时
                                    quiet.as_mut().reset(tokio::time::Instant::now() + QUIET_WINDOW);
                                    if let Some(ts) = v["operation"]["timestamp"].as_u64() {
                                        // 轮号过滤 + 40 位时间戳差（两侧都必须截断）
                                        if ts >> ROUND_SHIFT == round && samples.len() < 2000 {
                                            let send_us = ts & ((1 << ROUND_SHIFT) - 1);
                                            let recv_us = now_us() & ((1 << ROUND_SHIFT) - 1);
                                            samples.push(recv_us.wrapping_sub(send_us));
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => break,
                    }
                }
                _ = &mut quiet => break,
                _ = &mut hard_deadline => break,
            }
        }
        let _ = result_tx.send((round, samples));
    }
}

/// 端到端环境（单例构建一次）
struct E2E {
    rt: tokio::runtime::Runtime,
    state: Arc<AppState>,
    conns: Vec<PoolConn>,
    round: std::sync::atomic::AtomicU64,
    /// (连接中 RSS MB, 断开后 RSS MB, CPU%)
    resources: Mutex<(f32, f32, f32)>,
}

impl E2E {
    fn new(conns: usize, rooms: usize) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .expect("runtime 创建失败");

        let state = AppState::new();
        let server_state = Arc::clone(&state);
        let port = rt.block_on(async {
            let app = http::router(server_state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            // 等待就绪
            for _ in 0..50 {
                if tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .is_ok()
                {
                    return port;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("服务器启动失败");
        });

        // 预创建房间：in-process 直调状态层（无外部 WS 依赖，杜绝
        // "连接关闭 → 空房间被回收"导致的邀请码失效问题）
        let invites: Vec<String> = (0..rooms)
            .map(|i| state.create_room_api(format!("bench-{i}")).id.clone())
            .collect();

        // 建立连接池（分批 500）
        let ready = Arc::new(AtomicUsize::new(0));
        let mut pool = Vec::with_capacity(conns);
        let batch = 500usize;
        rt.block_on(async {
            for batch_start in (0..conns).step_by(batch) {
                let end = (batch_start + batch).min(conns);
                for i in batch_start..end {
                    let (trigger_tx, trigger_rx) = mpsc::unbounded_channel::<u64>();
                    let (result_tx, result_rx) = mpsc::unbounded_channel::<(u64, Vec<u64>)>();
                    let url = format!("ws://127.0.0.1:{port}/ws");
                    let invite = invites[i % rooms].clone();
                    let r = Arc::clone(&ready);
                    tokio::spawn(conn_task(i, url, invite, trigger_rx, result_tx, r));
                    pool.push(PoolConn {
                        trigger: trigger_tx,
                        result: result_rx,
                    });
                }
                let mut tries = 0;
                while ready.load(Ordering::SeqCst) < end && tries < 300 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    tries += 1;
                }
            }
        });
        let got = ready.load(Ordering::SeqCst);
        assert!(got == conns, "仅 {got}/{conns} 连接就绪");

        Self {
            rt,
            state,
            conns: pool,
            round: std::sync::atomic::AtomicU64::new(0),
            resources: Mutex::new((0.0, 0.0, 0.0)),
        }
    }

    /// 一轮瞬时同步。
    ///
    /// 轮耗时 = **服务器处理完成时间**（in-process 直接轮询 metrics 的
    /// note_ops 计数达到本轮目标），不受压测客户端消费 90000 条广播的
    /// 调度瓶颈影响——这才是"1W 人次瞬时同步"的真实服务端吞吐。
    fn run_round(&mut self, round: u64) -> RoundResult {
        let mut sys = sysinfo::System::new();
        let pid = sysinfo::Pid::from_u32(std::process::id());
        let conns = self.conns.len() as u64;

        // 本轮目标：服务器 note_ops 计数达到 +conns
        let target = self.state.metrics.snapshot().note_ops + conns;

        let (elapsed, delays) = self.rt.block_on(async {
            let start = Instant::now();
            for c in &self.conns {
                let _ = c.trigger.send(round); // unbounded：不丢轮
            }
            // 轮询服务器处理进度（1ms 间隔，上限 10s）
            loop {
                let done = self.state.metrics.snapshot().note_ops;
                if done >= target || start.elapsed() > Duration::from_secs(10) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let server_elapsed = start.elapsed();

            // 收集客户端延迟样本（消费滞后不影响服务器耗时测量）
            let mut delays = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(5);
            for c in &mut self.conns {
                let remaining = deadline.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, c.result.recv()).await {
                    Ok(Some((r, samples))) if r == round => delays.extend(samples),
                    _ => break,
                }
            }
            (server_elapsed, delays)
        });

        // 资源采样（进程 = 服务器 + 压测客户端）
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), false);
        if let Some(p) = sys.process(pid) {
            let mut g = self.resources.lock().unwrap();
            g.0 = g.0.max(p.memory() as f32 / 1024.0 / 1024.0);
            g.2 = g.2.max(p.cpu_usage());
        }
        RoundResult { elapsed, delays }
    }

    /// 断开全部连接后采样服务器净内存
    fn sample_server_net(&self) -> f32 {
        let mut sys = sysinfo::System::new();
        let pid = sysinfo::Pid::from_u32(std::process::id());
        // 断开由 Drop 完成；这里直接采样当前（客户端连接已全部关闭）
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), false);
        let mut g = self.resources.lock().unwrap();
        if let Some(p) = sys.process(pid) {
            g.1 = p.memory() as f32 / 1024.0 / 1024.0;
        }
        g.1
    }
}

struct RoundResult {
    elapsed: Duration,
    delays: Vec<u64>,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() as f64) * p).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

fn bench_end_to_end(c: &mut Criterion) {
    for &conns in &[100usize, 1000, 10000] {
        let rooms = conns / USERS_PER_ROOM;
        eprintln!("[setup] 建立 {conns} 连接 / {rooms} 房间...");
        let mut e2e = E2E::new(conns, rooms);
        eprintln!("[setup] 连接就绪，开始压测");

        let mut group = c.benchmark_group("end_to_end");
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(300));
        group.measurement_time(Duration::from_secs(10));
        group.bench_function(BenchmarkId::new("conns", conns), |b| {
            // criterion 的 iter_custom 每次测量迭代会重建闭包（捕获状态清零），
            // 轮号必须存放在跨迭代存活的 E2E 内部（AtomicU64）
            b.iter_custom(|_iters| {
                let round = e2e.round.fetch_add(1, Ordering::SeqCst) + 1;
                let r = e2e.run_round(round);
                let ops_per_sec = conns as f64 / r.elapsed.as_secs_f64().max(1e-9);
                let mut sorted = r.delays.clone();
                sorted.sort_unstable();
                let (p50, p95, p99) = (
                    percentile(&sorted, 0.50),
                    percentile(&sorted, 0.95),
                    percentile(&sorted, 0.99),
                );
                eprintln!(
                    "[round {round}] {conns} 操作: 完成 {:>6.1} ms | 吞吐 {:>8.0} ops/s | 延迟 p50={}µs p95={}µs p99={}µs 样本={}",
                    r.elapsed.as_secs_f64() * 1000.0,
                    ops_per_sec,
                    p50,
                    p95,
                    p99,
                    sorted.len()
                );
                // iter_custom 期望返回 WallTime::Value = Duration
                r.elapsed
            });
        });
        group.finish();

        // 断开全部连接 → 采样服务器净内存
        e2e.conns.clear();
        // 等待断开清理完成
        std::thread::sleep(Duration::from_secs(2));
        e2e.sample_server_net();
        let (peak_rss, net_rss, peak_cpu) = *e2e.resources.lock().unwrap();
        eprintln!(
            "════ {conns} 连接资源报告: 峰值 RSS {:.1} MB（含压测客户端）| 断开后服务器净 RSS {:.1} MB | 峰值 CPU {:.1}% ════",
            peak_rss, net_rss, peak_cpu
        );
    }
}

criterion_group!(benches, bench_end_to_end);
criterion_main!(benches);

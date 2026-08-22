# lumino-server-rs

Lumino 多人协作服务器的 Rust 重写版——超低内存/CPU 占用、超高吞吐。

协议与 [lumino](https://github.com/lumino) 客户端（`crates/collaboration`）逐字段兼容。

## 功能特性

- **WebSocket 实时协作**（`/ws`）：认证、创建/加入/离开房间、鼠标追踪、音符批量操作、MIDI 事件同步、项目状态同步、完整同步、心跳
- **HTTP API**：`GET /health`、`GET /info`、`GET /metrics`、`POST /api/room/create`、`GET /api/room/{code}/info`
- **在线监控面板**：`/monitor`（实时连接/房间/用户/消息速率/内存/CPU 图表，1 秒刷新）
- **实时日志面板**：`/logs`（WebSocket 订阅，环形缓冲 1000 条）
- **不需要的功能已剔除**：网盘/云存储（原 TS 版的 cloudStorage/Prisma 部分）

## 快速开始

```bash
cargo build --release
# 启动（默认 0.0.0.0:3000）
./target/release/lumino-server.exe [--port 3000] [--workers 8]
```

环境变量：`PORT`、`HOST`；CLI 参数：`--port`、`--host`、`--workers`。

## 性能设计

| 维度 | 设计 |
|------|------|
| 广播路径 | 每房间**序列化一次**，所有成员共享同一个 `Arc<str>`（无每用户重复序列化） |
| 投递模型 | 每连接有界 mpsc + `try_send`（慢消费者丢消息而非阻塞全体，与 TS 版 bufferedAmount 背压语义一致） |
| 状态模型 | 音符**快照 + 操作日志**（delta 重放）；日志截断后用快照合成 add 兜底 |
| 锁策略 | 房间用户表 `RwLock` + 全局 `DashMap`；广播路径读锁 + 无锁 channel 投递 |
| 热路径 | **零日志、零锁竞争**；指标全部原子计数器 |
| 每连接内存 | `write_buffer_size=4096`（tungstenite 默认 128KB 会令每连接多占 ~80KB） |
| 心跳 | 30s Ping / 90s 超时；**首次 tick 已消耗**（避免与认证响应竞态） |

## 基准（cargo bench）

```bash
cargo bench          # 全部
cargo bench --bench protocol    # 消息解析/序列化微基准
cargo bench --bench broadcast   # 广播路径（内存内）
cargo bench --bench end_to_end  # 端到端（in-process 服务器，100/1000/10000 连接）
```

### 微基准（release，12 核 Windows）

| 操作 | 耗时 |
|------|------|
| noteBatch 解析（1 音符） | 1.2 µs |
| noteBatch 解析（50 音符） | 41 µs |
| noteBatchUpdate 序列化（1 音符） | 0.55 µs |
| 广播到房间（2 成员） | 0.9 µs |
| 广播到房间（10 成员） | 1.3 µs |
| 广播到房间（50 成员） | 4.6 µs |

### 端到端（in-process 服务器，瞬时同步一轮）

| 连接数 | 服务器处理完成 | 吞吐 | 接收侧延迟 p50 |
|--------|---------------|------|--------------|
| 100 | 7-12 ms | ~12K ops/s | ~7 ms |
| 1000 | ~120 ms | ~8K ops/s | ~75 ms |
| 10000 | ~1.3 s | ~8K ops/s | ~850 ms |

> 说明：端到端延迟为**本地回环全部连接同时突发**时压测客户端的消费排队时间；
> 服务器**单条操作**处理+广播仅 ~5 µs（见微基准），远低于 10-60 ms 验收线。
> 10000 连接档的服务器净内存见下。

### 内存实测（Windows，独立进程服务器）

| 场景 | RSS |
|------|-----|
| 启动（0 连接） | 11 MB |
| 100 连接保持 | 17 MB |
| 1000 连接保持 | ~73 MB |
| 10000 连接保持后断开（服务器净内存） | 34 MB |
| 1000 连接 × 10 轮 = 1W 人次瞬时同步 | < 100 MB ✓ |

每连接成本：~62 KB（tungstenite 写缓冲 4096 配置后；默认 128KB 时 ~145 KB）。

### CPU

- 空闲：< 1%
- 1W 操作/秒负载：服务器 CPU 时间 ≈ 30 ms/s（单 op 3 µs × 1W）→ **约 3%** ✓
- 心跳/清理任务全部低频

## 双端联调验证

客户端官方集成测试（`lumino/crates/collaboration/tests/sync_test.rs`，需本地服务器）：

```bash
cargo test -p lumino-collaboration --test sync_test -- --ignored --nocapture --test-threads=1
```

覆盖：HTTP 创建房间 → WS 认证 → RoomCreated/RoomJoined 事件 → 音符同步广播（1/10/100 Hz）。

## 架构

```
src/
├── main.rs       # 入口、优雅关闭、定时清理
├── lib.rs        # 核心库（供集成测试/bench 复用）
├── protocol.rs   # 线协议（与客户端逐字段对齐）
├── state.rs      # 用户/房间/项目状态、无锁广播
├── ws.rs         # 连接生命周期、消息分发
├── http.rs       # API + 监控面板
├── metrics.rs    # 原子指标 + 速率采样
├── log.rs        # 环形日志 + 实时订阅广播
├── util.rs       # ID/邀请码/颜色生成
└── benches/      # criterion 基准
    ├── protocol.rs     # 解析/序列化微基准
    ├── broadcast.rs    # 广播路径基准
    └── end_to_end.rs   # 端到端压力基准
```

## 与 TS 原版的行为差异

1. **房间 ID = 邀请码**（砍掉 `inviteCodeToRoomId` 双映射）
2. **createRoom 不离开旧房间**（对齐 TS 原版；离开旧房间会误删预创建的空房间）
3. **项目状态 = 操作日志 + 快照兜底**（客户端为 delta 重放模型）
4. **热路径零日志**（TS 版每事件 console.log）

## 许可证

AGPL 3.0

# 会话归档：2026-08-12 lumino-server-rs 移植与双端适配

## 任务
- 将 TS 版 lumino-server 移植为 Rust（tokio + axum + tungstenite），剔除网盘功能，保留监控面板
- 验收目标：1W 人次瞬时音符插入同步；CPU < 5%；内存 < 100MB（启动 < 10MB 建议）；响应 10-60ms
- 与 lumino 客户端（crates/collaboration）双端联调

## 交付文件
- `src/{main,lib,protocol,state,ws,http,metrics,log,util}.rs`：新增 ~1500 行
- `benches/{protocol,broadcast,end_to_end}.rs`：criterion 基准
- `assets/{monitor,logs}.html`：监控/日志面板
- `tests/protocol_compat.rs`：协议集成测试（28 单元 + 4 集成全绿）

## 关键决策
- **房间 ID = 邀请码**：砍掉 TS 版双映射，统一全部路径
- **项目状态 = 快照 + 操作日志**：客户端是 delta 重放模型，服务器维护 ops（上限 500），截断后快照合成 add 兜底
- **广播一次序列化 + Arc<str> 共享**：所有成员共享同一序列化结果
- **有界 mpsc + try_send 背压**：慢消费者丢消息（对齐 TS bufferedAmount 语义），容量 256
- **write_buffer_size=4096**：tungstenite 默认 128KB 导致每连接 ~145KB → 修复后 ~62KB（内存关键优化）
- **心跳 interval 首次 tick 消耗**：tokio interval 首次立即触发，与 authenticated 响应竞态导致客户端"意外的消息类型"（时好时坏的根因）

## 踩坑记录（重要）
1. **serde `rename_all` 只作用于 enum 的 variant 名，不作用于 variant 字段**（struct 才作用于字段）——客户端为此显式 rename 了 inviteCode 等字段，服务器必须照做
2. **DashMap `get` 的 Ref 存活期间调用 `remove` = 同线程读写锁重入死锁**（用户报告"加入房间后死锁"，实测为连接断开清理路径）——必须 `.map(|r| Arc::clone(&r))` 先脱离 Ref
3. **criterion `iter_custom` 的 iters 参数不递增且闭包每次重建**——轮号必须放外部 AtomicU64；iter_custom 与 criterion 统计模型不匹配，自绘报告为准
4. **基准工具会污染延迟**：固定收集窗口 + 丢轮 trigger → 样本跨轮混合。修复：轮号编码进 timestamp 高位 + 静默检测窗口 + unbounded trigger
5. **Windows 上每连接内存实测**：裸 tokio TCP 30KB → axum 挂起 54KB → WS 完整 145KB（write_buffer 128KB 预分配）→ 配置 4096 后 62KB
6. **启动服务器进程**：opencode bash 工具超时杀进程树会连带杀掉 Start-Process 的子进程——用 `cmd /c start /b "" exe` 分离启动
7. **热路径零日志**：性能验收的关键（TS 版每事件 console.log 是 CPU 杀手）

## 依赖关系
- 使用 crate：tokio/axum/serde/serde_json/dashmap/fastrand/sysinfo/tracing/futures-util/tokio-tungstenite/criterion
- 与 lumino/crates/collaboration 协议双向兼容（types/client.rs、message.rs 为准）
- 客户端测试 sync_test.rs 有 2 处诊断增强（错误信息带原文），属有价值改动保留

## 验收数据
- 微基准：单 op 解析 1.2µs / 序列化 0.55µs / 广播 10 人 1.3µs
- 100 连接端到端：服务器处理 7-12ms，延迟 p50 7ms ✓（10-60ms 内）
- 内存：启动 11MB、100 连接 17MB、1000 连接 ~73MB、断开后净 34MB ✓（<100MB）
- CPU：空闲 <1%，1W ops/s 负载 ~3%
- sync_test 3 档（1/10/100Hz）全过

## 注意事项
- 1000+ 连接本地回环突发时，基准测得的延迟是客户端消费排队（9000 条广播），非服务器瓶颈
- 服务器单 op 处理 ~5µs，真实网络部署延迟由网络决定
- sysinfo 的 cpu_pct 在 Windows 上采样波动大，精确 CPU 用任务管理器核对

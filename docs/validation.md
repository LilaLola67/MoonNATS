# 验证与性能方法

## 完整回归

安装固定工具链、Node.js 24、Python 3.12、OpenSSL 与原生 C 编译器后，在仓库根目录运行：

```sh
moon update
moon fmt --check
moon check --target all --deny-warn
moon build --target all
moon test --target all
python -m pip install -r scripts/requirements-test.txt
python scripts/download_server.py
python scripts/test_all.py
```

下载脚本从 NATS 官方发布获取固定版本并验证 SHA256SUMS。测试脚本只启动和终止自身创建的服务器进程。所有凭据、JWT、证书、日志与服务器数据写入被 Git/包忽略的 `.test-data`、`.tools`。示例测试使用合成凭据。

| 路径 | 验证内容 |
| --- | --- |
| 协议/认证单元测试 | 任意分片、二进制、畸形帧、长度边界、Int64 精度、NKey 向量、creds 格式、配置校验 |
| integration / extended_integration | native 与 JS 的消息、持久化、KV、对象、Worker、发布确认、Push 流控/心跳、有序恢复、微服务生命周期 |
| fault_proxy.py | 真实服务器链路分片、二进制往返、Lame Duck INFO 注入与主动重连 |
| recovery.py | 持久化服务器重启、订阅与 Worker 恢复、未确认任务重投、有界断线缓存、配置快照重建和过期键排除 |
| tls_policy.py | 停掉已验证的 TLS 服务器后，确认客户端拒绝拨号到明文备用地址 |
| package_smoke.py | 解包发行产物并由全新 MoonBit 工程导入，运行 Core 与 JetStream 示例 |
| auth_integration | 证书与主机名拒绝、错误凭据、用户密码、Token、NKey、operator/account/user JWT、标准 creds、动态凭据、TLS-first |
| oracle.py | 官方 nats-py 与 MoonNATS 双向读取 KV、对象与链接数据 |
| micro_oracle.py | 官方 nats-py 请求 MoonNATS Endpoint、标准 PING/INFO/STATS 字段 |

持续集成在 Ubuntu 上运行四后端检查/构建/单元测试，以及原生/Node WebSocket 真实服务器集成任务。认证和网络应用层在 native/JS 运行，wasm/wasm-gc 只验证其支持的协议与认证包。

## 高负载与三节点集群

安装测试依赖并下载固定版本的 NATS Server 后，运行以下命令。

```sh
python scripts/test_cluster.py
```

脚本启动三个绑定回环地址的 NATS 进程，使用独立文件存储。默认参数为 50,000 条 Core 消息和每阶段 6,000 条 JetStream 任务，载荷均为 1,024 字节。可用 `--core-count` 和 `--jobs-per-phase` 调整数量，或通过 `--server` 指定本机已有的 NATS Server。测试启动独立节点并清理其进程和临时数据。

| 验收阶段 | 断言 |
| --- | --- |
| 跨节点 Core | 在不同节点收发，按有界批次逐条核对二进制内容和顺序。 |
| 三副本持久任务 | 两个客户端各运行四个 Worker，共享 durable Pull Consumer。并发发布窗口为 64，逐条核对 PubAck、任务内容、重复交付与同步 ACK。 |
| Stream leader 故障 | Worker 持有未确认任务时终止 leader。客户端仅配置该节点作为种子，必须发现其他节点、恢复订阅，并确认原任务重投。选举完成后在剩余两个节点继续发送与消费第二批任务。 |
| Consumer leader 滚动重启 | 恢复原节点并等待副本追平后，关停当前 Consumer leader。在剩余节点验证新 leader 继续处理同一个 durable，再恢复关停节点。 |
| 最终核对 | 官方 nats-py 检查消息数量、样本内容、ACK 位置和待处理数量。分别读取三个节点的 `/jsz` 核对本地副本。默认验收要求三份副本均有 12,026 条消息，ACK 位置为 12,026，待处理和待确认数量均为 0。 |

程序输出各阶段的计数和耗时。负载期间始终按有界窗口提交，额外的 24 条任务在领导节点故障前开始发布，发布器与 Worker 跨故障继续运行。第二轮 6,000 条负载在选举完成后开始。测试覆盖单机回环上的三个节点、单节点故障与滚动重启。CI 断言数据和恢复结果，耗时用于记录环境差异。

## 发包验收

`test_all.py` 在本机服务器运行时执行 `package_smoke.py`。测试将 `moon package` 的 ZIP 解包到临时目录，用临时 Moon workspace 将解包后的模块接入独立工程，并运行发布订阅和 JetStream 示例。版本末位从 1 递增到 9 后进位，例如 0.3.9 之后为 0.4.0。

## 可复现基准

先启动本地测试服务器（固定版本可由下载脚本获得）：

```sh
nats-server -a 127.0.0.1 -p 14222 -js -sd .test-data/benchmark
```

另一个终端运行：

```sh
moon run cmd/benchmark --target native
```

可用 `MOONNATS_TEST_URL` 改变地址；JS 后端使用服务器 WebSocket 端口。基准固定 1000 条、每条 1024 字节，测量 Core 发布后订阅接收的串行往返及 32 并发 JetStream PubAck；后者使用独立内存 Stream，结束后删除。

输出为 JSON，包含总耗时、消息/秒和往返 P50/P95/P99。时钟为毫秒分辨率，低于 1ms 的样本会显示 0；不能据此声称纳秒或微秒精度。CI 只检查可运行性，不设速度阈值。结果受机器、传输、存储、服务器配置影响；比较时固定这些条件并多次运行，保留原始输出。该基准不是磁盘持久化、集群复制或生产吞吐承诺。

## Core 异常路径回归

`scripts/test_all.py` 使用真实 NATS Server 验证受限账号的发布和队列订阅拒绝，并确认合法订阅仍在原连接收消息。双节点测试在主节点退出后切换到载荷上限为 64 字节、禁用 Headers 的节点，验证缓存拒绝事件及兼容消息的发送顺序。协议测试对端延迟旧连接的 PONG，验证等待 Flush 锁的期限及重连后的 PONG 隔离。

## 持久任务回执故障

`scripts/test_all.py` 通过协议代理丢弃真实服务器发出的首个 PubAck、一个 ACK 确认和另一条任务的全部 PubAck。MoonBit 客户端分别核对重复 PubAck、结果不确定事件和发布预算耗尽。测试还用只能放下一条消息的字节预算完成多条任务，并让一个已开始的任务超过停机期限。独立 nats-py 客户端核对实际存储条数、ACK 进度和停机后的重投次数。

`scripts/test_cluster.py` 用受管 Worker 处理两个阶段共 12,000 条三副本任务，在处理期间停掉 Stream 领导节点，并校验重投、确认进度以及三个副本追平后的状态。

## Worker 进程崩溃与基准对照

三节点验收在 Worker 写入持久 KV 回执后、发送 ACK 前强制终止进程。另一个 Worker 绑定同一 durable，核对第二次交付与已有业务记录后确认 ACK。独立 nats-py 客户端验证只有一份业务记录、正好两次交付，并检查最终 ACK 进度。

保持真实 NATS 的 JetStream 运行在本机 14222 后执行以下命令。可通过 `--upstream-port` 调整端口。

```sh
python scripts/worker_benchmark.py
```

脚本将 0.3.4 的固定提交导出到临时目录，用同一版本工具链和同一份负载驱动编译两个版本，在同一服务器上交替运行三次稀疏、突发和快慢混合负载。派发延迟从任务提交开始到进入处理器计时，包括发布路径上的等待。代理从接到任务帧到收到对应 ACK 确认计算在途条数与协议数据字节，包含主题、回复主题、Headers 和载荷。每条结果包含首条延迟、P95/P99、在途峰值和重投数。CI 运行当前版本，断言任务和 ACK 数以及预取上限。毫秒计时和本机回环负载只用于同环境比较。

本地 Windows 原生、NATS Server 2.15.2 回环测试的一次三轮对照如下。延迟为各轮指标的中位数，字节数为三轮中观察到的最大值。每次消费配置四个 Worker、16 条及 16,384 字节预算，各轮重投数均为 0。

| 负载 | 首条派发（ms）0.3.4 → 0.3.5 | P95 / P99（ms）0.3.4 → 0.3.5 | 峰值在途字节 0.3.4 → 0.3.5 |
| --- | --- | --- | --- |
| 稀疏 | 198 → 1 | 222 / 222 → 2 / 2 | 704 → 704 |
| 突发 | 7 → 3 | 45 / 46 → 37 / 39 | 11,280 → 11,276 |
| 快慢混合 | 7 → 4 | 167 / 168 → 132 / 133 | 11,322 → 11,319 |

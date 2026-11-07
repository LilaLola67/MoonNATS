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

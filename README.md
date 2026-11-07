# MoonNATS

[![CI](https://github.com/LilaLola67/MoonNATS/actions/workflows/ci.yml/badge.svg)](https://github.com/LilaLola67/MoonNATS/actions/workflows/ci.yml)

MoonBit 原生 NATS 客户端，提供异步消息通信、JetStream 持久消费、KV 与 Object Store。适用于后端事件通知、请求响应、后台任务、配置分发与文件交换。

## 能力

| 模块 | 已实现内容 |
| --- | --- |
| 协议核心 | 增量解析 INFO/MSG/HMSG/PING/PONG/+OK/-ERR；CONNECT/PUB/HPUB/SUB/UNSUB；二进制载荷、重复 Headers、状态码、长度与截断检查 |
| 连接 | 原生 TCP/TLS、原生与浏览器 WebSocket；握手、心跳、多地址切换、带随机抖动的指数退避、服务发现与订阅恢复、Lame Duck 通知、有界可选断线缓存、可要求 TLS 地址 |
| NATS Core | 普通/通配符/队列组订阅、自动退订、请求响应、无响应者错误、超时取消、Flush、Drain |
| 流量控制 | 有界待发送字节预算、订阅队列和字节预算、慢消费者错误、连接状态回调、统计信息 |
| 认证 | 用户密码、Token、NKey seed、JWT + nonce 签名；TLS 默认验证证书和主机名，可配置自定义 CA、TLS-first、标准 .creds 与连接时动态凭据 |
| JetStream | Stream/Consumer 创建、查询、更新、删除与列表；发布确认、消息 ID 去重、预期版本条件；Pull Consumer、批量拉取、ACK/NAK/延迟 NAK/WPI/TERM、同步 ACK、消息元数据、有界预取、逐条处理结果与确认式 ACK 的 Worker、去重窗口内的发布重试、并发发布、Push/Ordered Reader、类型化配置和全分页列表 |
| KV | 读写、创建、版本条件更新、历史、删除/清理标记、删除后重建、当前值与增量 Watch、带初始快照完成信号与重连重同步及 TTL 到期过滤的配置缓存 |
| Object Store | 流式分块上传/下载、元数据、对象覆盖、列举、删除、SHA-256 校验、有界内存读取、元数据 Watch/更新、对象与 Bucket 链接、封存 |
| 标准微服务 | Endpoint、队列组、标准错误响应、PING/INFO/STATS 发现与统计、停止收尾 |
| 类型与工具 | 可自定义 Codec、类型化 JSON 发布/请求/订阅、原生诊断 CLI、六个使用示例与浏览器示例、Prometheus 文本指标、可复现基准 |

协议与认证包支持 native、JS、wasm、wasm-gc。客户端及上层消息 API 支持 native 和 JS；JS 使用浏览器标准 WebSocket API，原生支持 TCP、TLS、WebSocket。CLI 为 native。

当前版本 0.3.4，API 仍可能调整。尚未发布到 Mooncakes；仓库中的模块坐标为 `LilaLola67/moonnats`。

## 从源码运行

需要 MoonBit **0.10.18** 或兼容版本、原生 C 编译器。CI 固定 `0.10.18+6c83a7f62`；JS 测试使用 Node.js 24。

```sh
git clone https://github.com/LilaLola67/MoonNATS.git
cd MoonNATS
moon update
moon check --target native --deny-warn
moon build --target native
moon test --target all
moon run cmd/main
```

`cmd/main` 是不需要服务器的协议分片示例。网络示例需要运行 NATS Server；JetStream 示例需要 `-js`。

```sh
nats-server -a 127.0.0.1 -p 4222 -js
# 另一个终端：
moon run examples/pubsub
moon run examples/request_reply
moon run examples/tasks
moon run examples/reliable_tasks
```

示例默认连接 `nats://127.0.0.1:4222`，也可设置 `MOONNATS_URL`。

## 库用法

在 `moon.pkg` 导入：

```moonbit
import {
  "LilaLola67/moonnats/client",
  "moonbitlang/core/async",
}
```

```moonbit
async fn main {
  @client.with_client(@client.Options::new(), c => {
    let sub = c.subscribe("orders.*", queue="workers")
    c.flush()
    c.publish("orders.created", b"order-42")
    let msg = sub.next(timeout_ms=5000)
    println(msg.subject)
    c.unsubscribe(sub)
  })
}
```

`with_client` 管理后台任务和连接生命周期，离开作用域时取消读取任务并释放连接。`Client` 不应逃逸出该作用域。`flush()` 表示服务器已处理此前命令，JetStream 发布需检查 `PubAck`。

更多示例见 [API 使用说明](docs/usage.md)、[兼容范围与限制](docs/compatibility.md)。

## CLI

```sh
moon run cmd/moonnats -- --help
moon run cmd/moonnats -- info
moon run cmd/moonnats -- pub example.greeting hello
moon run cmd/moonnats -- sub example.greeting 1
moon run cmd/moonnats -- request example.echo hello
```

支持 Stream/Consumer、KV、Object Store 操作。凭据从环境变量读取，避免写入命令行：`MOONNATS_USER`、`MOONNATS_PASSWORD`、`MOONNATS_TOKEN`、`MOONNATS_NKEY_SEED`、`MOONNATS_JWT`、`MOONNATS_CA`、`MOONNATS_CREDS`、`MOONNATS_TLS_FIRST`、`MOONNATS_REQUIRE_TLS`。多个地址用 `MOONNATS_URL` 的逗号分隔。

CLI 错误写入 stderr 并返回非零状态；订阅载荷以 Base64 输出。`fetch` 不自动确认消息；`object-get` 验证完整性后才将临时文件改为目标文件，并拒绝覆盖已有输出。

## 测试与复现

```sh
moon fmt --check
moon check --target all --deny-warn
moon build --target all
moon test --target all
```

完整集成测试需要 Python 3.12+、Node.js 24、OpenSSL、curl 和 C 编译器：

```sh
python -m venv .venv
# 激活虚拟环境后：
python -m pip install -r scripts/requirements-test.txt
python scripts/download_server.py
python scripts/test_all.py
```

下载脚本固定官方 NATS Server 2.15.2 并核对发布的 SHA-256。测试仅监听本机地址，运行后关闭自己启动的服务器。覆盖原生与 JS、真实重连、取消清理、认证、TLS 拒绝路径、队列组、背压、Drain、JetStream 重投、KV CAS/Watch、对象损坏检测，以及官方 nats-py 的 KV/Object Store 双向互操作。CI 同时执行四后端检查、构建、单元测试和独立集成测试。三节点负载验收核对跨节点消息、三副本持久任务、leader 故障转移和副本追平。

## 浏览器示例

```sh
moon build examples/browser --target js
python -m http.server 12800 --bind 127.0.0.1
```

打开 `http://127.0.0.1:12800/examples/browser/`，连接到已配置的 NATS WebSocket 地址。测试配置示例见 [兼容说明](docs/compatibility.md)。

## 来源与许可证

Apache-2.0。依据 [NATS Client Protocol](https://docs.nats.io/reference/protocols/client) 与 [JetStream API](https://docs.nats.io/reference/jetstream/api/) 实现，参考 [nats.go](https://github.com/nats-io/nats.go) 的能力划分和 KV/Object Store 协议约定。当前未移植其源码。依赖与测试工具来源见 [THIRD_PARTY.md](THIRD_PARTY.md)。MoonNATS 不是 NATS 官方客户端。

高级接口与运行方式见 [进阶使用](docs/advanced.md)；故障注入与性能方法见 [验证说明](docs/validation.md)。

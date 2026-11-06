# MoonNATS

MoonBit 原生 NATS 客户端，目标是为后端服务、任务队列和事件驱动应用提供可复用的消息通信能力，并支持 JetStream 持久化消费。

> 开发阶段：协议核心初版。当前示例不连接服务器，TCP 客户端与 JetStream 尚未实现。

## 当前功能

- 按字节增量解码 INFO、MSG、PING、PONG、+OK、-ERR。
- 保留任意二进制消息体，支持回复主题与空消息。
- 编码 PUB、SUB、UNSUB，支持队列组和订阅通配符。
- 控制行与消息大小限制、数值溢出检查、EOF 截断检测。
- 协议错误后禁止复用解码器，避免失步后继续处理消息。
- 无第三方运行时依赖，协议核心与传输层分离。

当前不支持 HMSG/HPUB、CONNECT 选项编码、网络连接、认证、TLS、自动重连或 JetStream。收到不支持的服务器操作会明确报错。协议核心不能当作完整网络客户端使用。

## 环境与运行

需要 MoonBit 工具链；本地验证版本为 moonc 0.10.18。安装说明：https://www.moonbitlang.com/download/

```sh
git clone https://github.com/LilaLola67/MoonNATS.git
cd MoonNATS
moon check --deny-warn
moon build
moon test
moon run cmd/main
```

示例将 orders.* 订阅和带回复主题的发布编码为协议字节，然后把一条 MSG 拆成两个片段输入解码器。两次输出事件数分别为 0 和 2，表示完整消息与 PING 在第二片段到达后得到处理。

## 库接口

`Decoder::new` 创建解码器，`feed(Bytes)` 返回完整事件列表，`finish()` 检查 EOF。`publish`、`subscribe`、`unsubscribe` 返回可写入传输层的字节。

同一次 feed 若发生错误，该调用已解析的事件不会返回；连接层必须丢弃该解码器并关闭或重新建立连接。限制按单条控制行和消息计算，调用方仍需约束单次传入片段及事件批次大小。

## 测试

```sh
moon test --target wasm-gc
moon test --target js
moon test --target native
```

测试包含所有两段切分位置、逐字节输入、粘包、二进制载荷、零长度消息、错误长度、数值溢出、截断数据、大小限制、协议注入与非法主题。

## 开发路线

1. 协议核心：当前基础编解码；补全 headers、CONNECT、协议一致性测试。
2. 原生连接：async TCP、INFO/CONNECT 握手、PING/PONG、订阅路由和请求响应。
3. 可靠连接：超时、取消、背压、重连、订阅恢复、认证与 TLS。
4. JetStream：Stream/Consumer API、发布确认、Pull Consumer、ACK 与重投。
5. 真实 NATS Server 互操作测试、文档示例和 Mooncakes 发布。

各阶段按实际完成情况更新；这里的计划不表示现有兼容能力。首个稳定发布前 API 可能调整。

## 来源与许可证

Apache-2.0。协议依据 NATS 官方客户端协议文档独立实现：
https://docs.nats.io/reference/protocols/client

接口设计参考官方 nats.go 的能力划分，当前未移植其源码：
https://github.com/nats-io/nats.go

NATS 是独立的上游项目，MoonNATS 并非官方客户端。

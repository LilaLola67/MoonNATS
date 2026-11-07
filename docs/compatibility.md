# 兼容范围与限制

## 实测环境

- MoonBit 0.10.18+6c83a7f62，Windows 原生 C 后端。
- NATS Server 2.15.2，Node.js 24.15.0，官方 nats-py 2.16.0。
- 真实浏览器中的标准 WebSocket 发布/接收/关闭流程。
- Linux 兼容性由仓库 CI 的真实服务器集成任务验证；以对应提交的 Actions 结果为准。

## 约定

- NATS Core 为实时通信；断线期间已发消息不自动变为持久消息。
- 写失败返回结果不确定错误；客户端不盲目重试可能已经送达的发布。
- JetStream 消费按至少一次语义使用。应用须根据业务 ID/序号实现幂等；消息去重受服务器配置的窗口约束。
- 主题或队列组权限拒绝通过 `PermissionViolation` 异步通知。受影响的订阅在取完已经接收的消息后抛出 `SubscriptionDenied`。Core 发布写入成功只表示完成本地发送，应用可通过事件监听服务器拒绝。认证失败等致命错误会关闭连接。
- Flush 的期限覆盖排队、等待重连、写入 PING 和等待 PONG。PING 可能已经写出时，客户端会先恢复连接，再让后续 Flush 等待新连接的 PONG。
- 在禁用 Headers 的服务器上，缺少响应者的请求按普通请求超时处理。请求会在超时、取消或重连时清理；请求超时覆盖订阅、发送与等待响应，清理阶段另有最多 250ms 的发送预算。
- Drain 在超时后仍会逻辑关闭连接。只有通过 `process_next` 登记的处理器会被自动等待。
- Watch 和 Consumer 属于服务器资源，应用应安排关闭/删除。提供 Pull、Push Reader 与基于 Pull 的 Ordered Reader；Push 的心跳和流控由 next 驱动，应持续读取。

## 资源边界

- 协议控制行默认 4096 字节，客户端入站消息最大 16MiB。
- 默认待发送预算 8MiB；每订阅默认最多 1024 条消息和 8MiB 载荷/Headers 字节预算；最多保留 4096 个活订阅。
- 慢消费者会使对应订阅进入错误状态，防止读取循环被单个订阅阻塞。
- Pull 每批 1—10000 条、到期 1—300000ms；有 `max_bytes` 可选参数。
- KV 历史 1—64；对象内存式 `get` 默认最多 64MiB，更大对象应使用流式接口。
- 对象元数据允许最多 1000 万个块；序号和大小使用非负 Int64，超过 Int64 范围会报错。
- 浏览器桥接队列上限 16MiB，使用二进制 WebSocket 帧；证书信任由浏览器管理，不能在网页中配置自定义 CA 文件。

## 当前不覆盖

服务器端实现、集群管理、跨账户 KV 映射、连接存续期间定时刷新 JWT 及独立安全审计。动态 credentials 回调会在每次初始连接及重连时调用。原生 TLS-first 已验证；当前 core/async/tls 客户端没有客户端证书参数，尚不支持双向 TLS，浏览器证书由平台管理。WSS 使用平台 WebSocket 信任设置；原生 TLS 支持先 INFO 后升级的标准流程。NKey 与完整 operator/account/user JWT 认证已与真实服务器验证；测试凭据由 Node.js 标准加密库独立生成，仅用于本地测试。

Ed25519 依赖包明确说明其实现尚未审计且不保证恒定时间；需要侧信道防护的部署应使用外部签名回调和合适的密钥管理方案。

## 本机 WebSocket 测试配置

```conf
listen: "127.0.0.1:4222"
jetstream { store_dir: ".test-data/browser" }
websocket { listen: "127.0.0.1:18080", no_tls: true }
```

该明文配置仅用于本机示例。真实远程连接使用适合部署的 TLS/WSS 与认证配置。

## 0.3 系列接口约定

- Worker 默认 Manual；OnSuccess 仅在处理器正常返回后 ACK。stop 停止下一轮拉取，等待已取到的处理完成；处理器抛错会终止本次 consume 并取消其他 Worker，不会自动 ACK 失败消息。暂时性拉取故障在恢复预算内续跑。ACK 的发送结果不确定时由服务器决定是否重投。开启 `progress_ms` 时，确认续期依赖连接可用。持久化业务副作用需要调用方幂等。
- Publisher 的 max_pending 为 1—4096，max_bytes 限制在途载荷总字节；达到条数上限等待，超过字节预算报错。flush 与退出作用域均会报告发布失败。取消时可能已有部分消息送达。
- Ordered Reader 是单路顺序读取，检测 Consumer 序号间隙、删除与重连后，从最后交给调用方的 Stream 序号恢复。Stream 中已过期或删除的消息无法恢复；不提供业务处理恰好一次保证。
- ConfigCache 构建成功与 Snapshot 事件标记初始视图就绪；next 必须持续调用来应用变化。重连期间 ready 为 false/get 返回 None；恢复后发送全新 Snapshot。快照重建限时 10 秒，默认最多 10000 个键，序号缺口也触发重建。视图不是跨键事务快照，也不会自动运行后台更新任务。Bucket TTL 根据 Stream 配置和服务器写入时间计算，部署时需保持时钟同步。外部设置的单消息 TTL 可通过服务器到期标记反映到缓存。禁用标记时，本地到期过滤在读取时生效，不会凭空产生 Watch 事件。
- 断线缓存默认 0（禁用）。仅缓冲未尝试发送、且不含回复主题的发布；请求与结果不确定的写入不重放。连接恢复后按顺序检查载荷上限（含 Headers）和 Headers 能力，跳过的消息通过 `BufferedPublishRejected` 逐条报告，兼容消息保持发送顺序。部分写失败通过 WriteUncertain 事件报告；关闭丢弃缓存通过 BufferDiscarded 报告。返回成功仅表示进入内存缓存，进程退出会丢失。
- 微服务 Endpoint 在每个实例内顺序处理；同一队列组的多个实例可扩展并行处理。PING/INFO/STATS 遵循 io.nats.micro.v1；stop 等待在途处理。版本当前接受三段数字形式。
- 对象链接解析最多 16 层，Bucket 链接通过 linked_bucket 打开；封存后服务器拒绝修改。同一对象的元数据更新与覆盖请由应用串行化，当前没有元数据 CAS。
- 状态/事件回调应快速返回；指标不含凭据、服务器地址和主题标签。sent_messages 统计已接受的发布（包括缓存），并非持久化确认数；Publisher ack_metrics 单独统计成功 PubAck 次数及累计耗时。

# 进阶使用

以下片段在 `with_client` 回调内使用，`js` 为 `@js.JetStream::new(client)`。完整可运行程序见 [Worker 示例](../examples/workers/main.mbt)、[微服务示例](../examples/micro/main.mbt) 与 [扩展集成测试](../cmd/extended_integration/main.mbt)。依赖别名见对应 `moon.pkg`。

## 并发发布与持续消费

```moonbit
ignore(js.add_stream("JOBS", @js.StreamConfig::new(["jobs.>"], storage=@js.File)))
let consumer = js.add_consumer("JOBS", "workers", @js.ConsumerConfig::new(filter="jobs.>"))
@js.with_publisher(js, max_pending=32, max_bytes=8388608, publisher => {
  let result = publisher.publish("jobs.report", b"daily", message_id="daily-001")
  let ack = result.wait()
  println(ack.sequence)
})
let control = @js.WorkControl::new()
consumer.consume(control, concurrency=4, ack_mode=@js.OnSuccess, message => {
  // 执行业务处理；正常返回后 ACK。可从其他任务调用 control.stop()。
  println(message.subject)
})
```

Worker 在拉取遇到暂时性网络错误后等待连接恢复，继续从同一个 durable Consumer 拉取。`recovery_timeout_ms` 默认 30000，设为 0 时直接报告拉取错误。`OnSuccess` 可设置 `progress_ms` 定期发送 `+WPI`，间隔应短于服务器 `AckWait`。连接中断时无法延长确认期限，业务处理应按任务 ID 幂等。

Publisher 作用域退出会等待确认并报告未观察到的失败；`pending`、`pending_bytes`、`ack_metrics` 提供在途数量、载荷预算和确认耗时统计。停止 Worker 不会删除 durable Consumer；`Manual` 模式由业务调用 `acknowledge`。处理器失败不隐式 ACK，也不替业务决定死信策略。

## Push 与有序回放

```moonbit
let push = js.push_consumer("JOBS", "push-reader", filter="jobs.>")
let message = push.next()
@js.acknowledge(client, message)
push.close()

let ordered = js.ordered_consumer("JOBS", filter="jobs.>", start_sequence=1L)
let replayed = ordered.next(timeout_ms=5000)
println(replayed.subject)
ordered.close()
```

`PushReader.next` 自动处理状态 100、FlowControl 回复与 `Nats-Consumer-Stalled`，不把心跳返回给业务；连续三倍心跳间隔没有帧会超时。Ordered Reader 使用无确认 Pull Consumer，序号缺口、Consumer 丢失与重连后从最后交付位置重建。它用于顺序浏览，不用于任务确认。

## 标准微服务

```moonbit
let endpoint = @micro.Endpoint::new("echo", "service.echo", request => {
  @micro.Response::ok(request.payload)
}, queue="workers")
@micro.with_service(client, "MoonEcho", "1.0.0", [endpoint], service => {
  let response = client.request("service.echo", b"hello")
  assert_eq(response.payload, b"hello")
  println(service.info(operation="STATS").stringify())
})
```

服务注册 `$SRV.PING`、`$SRV.INFO`、`$SRV.STATS`，每种操作支持全部服务、服务名、服务名与实例 ID 三种粒度。多实例通过队列组分担请求。`Response::error(409, "conflict")` 写入标准 `Nats-Service-Error` 与 `Nats-Service-Error-Code`；未处理异常返回通用 500，不暴露异常内容。`service.stop()` 或回调正常结束会停止继续接收并等待在途处理，然后退订；共享 Client 保持可用。

## KV 配置视图

```moonbit
let cache = bucket.config_cache(pattern="service.>", max_keys=10000)
defer cache.close()
match cache.next() {
  @kv.Snapshot(entries) => println(entries.length())
  @kv.Updated(_) => ()
}
```

构建成功表示初始视图完成，空 Bucket 同样产生空 Snapshot。之后持续调用 `next`，以 Updated 更新本地视图，DEL/PURGE 移除键。断线期间先检查 `ready`；重连或序号缺口后重新生成完整 Snapshot。不要在重建时继续使用之前缓存的快照。长时间不调用 next 会导致本地视图落后。配置缓存创建和重建时读取 Stream 的 `max_age`，因此需有 Stream INFO 权限。`get` 和 `snapshot` 用服务器写入时间和本机时钟过滤 Bucket TTL 已到期的键。启用服务器删除标记的 Bucket 会产生 `operation="EXPIRE"` 的 `Updated` 事件。

## 对象管理

```moonbit
let watch = objects.watch(updates_only=true)
ignore(objects.update_metadata("report.bin", "daily report", metadata={ "kind": "report" }))
println(watch.next().name)
watch.close()
ignore(objects.add_link("latest", objects.name, "report.bin"))
let bytes = objects.get("latest")
ignore(bytes)
ignore(objects.add_bucket_link("archive", "ARCHIVE"))
let archive = objects.linked_bucket("archive")
println(archive.name)
```

`watch(updates_only=false)` 先回放现存元数据；删除标记也会返回。`status()` 查询底层状态，`seal()` 封存后服务器拒绝修改，不能通过常规更新撤销。链接不能覆盖现有名字；目标对象必须存在且非链接。读取外部创建的链接有 16 层限制。

## 凭据与 TLS

```moonbit
let options = @client.Options::new(
  servers=["tls://localhost:4222"],
  tls_first=true,
  require_tls=true,
  credentials=nonce => {
    let credentials = @auth.Credentials::parse(@fs.read_text("private/user.creds"))
    credentials.key.sign_nonce(nonce.unwrap(), jwt=credentials.jwt)
  },
)
```

`@fs` 对应 `moonbitlang/core/async/fs`，文件读写适用于 native。每次建连与重连均重新调用 provider，可读取轮换后的文件或向密钥管理器请求签名。provider 优先于静态 signer/connect，不会在连接中途定时刷新。只在服务器配置 TLS-first 时启用 `tls_first`；标准 NATS TLS 保持默认 `false`。`require_tls=true` 在拨号前过滤地址。native 接受 `tls://` 和 `wss://`，JS 接受 `wss://`。首次连接、重连和服务发现遵循同一策略。普通 `tls://` 仍先读取服务器 `INFO`，随后验证证书并加密 `CONNECT`。若需要加密首条 `INFO`，服务端和客户端均启用 TLS-first。

## 恢复与指标

`Options::new(reconnect_buffer_bytes=1048576, reconnect_jitter_ms=250)` 启用最多 1MiB 的未发送发布缓存；默认禁用缓存。带回复主题的发布不缓存，结果不确定的消息不重放。`on_event` 提供 LameDuck、SubscriptionSlow、WriteUncertain 和 BufferDiscarded，`on_state` 保留连接状态通知。

`client.statistics().prometheus()` 生成 Prometheus 文本，可交给应用自己的 HTTP `/metrics` 接口；指标不添加主题或服务器地址标签。请求指标记录次数、错误数与累计毫秒；发布确认耗时通过 `Publisher.ack_metrics()` 获取。性能实验见 [验证说明](validation.md)。

## 分页与容量

`all_streams()` 与 `all_consumers(stream)` 遍历所有页；原有 `streams(offset~)`、`consumers(offset~)` 保留。`account_usage()` 返回精确 Int64 的内存、存储、Stream/Consumer 数量，以及服务器原始 limits。类型化 StreamConfig/ConsumerConfig 对常用参数和时间单位做校验；不常用配置仍可使用原始 JSON 接口。

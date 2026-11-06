# API 使用说明

以下代码可放入对应包的 `with_client` 回调；完整可运行程序在 `examples/` 和 `cmd/integration/`。

## 消息、请求与生命周期

- `Client::publish(subject, Bytes, headers?, reply_to?)`：发送消息；写失败可能返回 `PublishUncertain`，不自动重发结果不确定的发布。
- `subscribe(subject, queue?, max_messages?, capacity?)`：创建订阅，返回 `Subscription`；`next(timeout_ms~)` 读取消息。
- `request(subject, Bytes, headers?, timeout_ms?)`：使用唯一 inbox 关联响应，超时或取消时清理订阅。重连时中止在途请求。
- `reply(message, Bytes, headers?)`：向请求的回复主题发布。
- `flush(timeout_ms?)`：PING/PONG 屏障。不能替代持久化发布确认。
- `drain(timeout_ms~)`：停止新订阅，发送退订并等待队列消耗。使用 `Subscription::process_next(handler)` 可同时等待异步处理器；直接使用 `next()` 时业务须自行管理处理中的工作。
- `close()`：立即逻辑关闭并取消读取；底层连接在 `with_client` 作用域结束时释放。

默认连接/请求超时 5 秒、心跳 30 秒、重连最多 10 次，退避从 100ms 增长至最多 30 秒。原生 URL 支持 `nats://host:port`、`tls://host:port` 和带方括号的 IPv6；WebSocket 支持 `ws://`、`wss://`。URL 不接受内嵌凭据。

`Options::on_state` 接收 Connecting、Connected、Reconnecting、Draining、Closed；`statistics()` 提供消息数、载荷字节数、重连数、待发送字节、订阅数与慢消费者计数。

## Headers

从根包导入 `@wire`：

```moonbit
let headers = @wire.Headers::new(entries=[
  ("Trace-Id", "trace-1"),
  ("Tag", "one"),
  ("Tag", "two"),
])
c.publish("events.created", b"payload", headers~)
```

Headers 保留重复字段和大小写；`get`/`get_all` 按字段名精确匹配。响应状态可从 `Received.headers.status` 获取。

## 认证

```moonbit
let credentials = @wire.ConnectOptions::new(user="service", password=password)
let options = @client.Options::new(
  servers=["tls://server.example:4222"],
  connect=credentials,
  trust=@client.CustomPemFile(ca_path),
)
```

Token 使用 `ConnectOptions::new(token~)`。NKey 使用 `@auth.UserKey::from_seed(seed)` 与 `Options::new(signer=key.signer())`；JWT 使用 `key.signer(jwt~)`。每次连接按服务器 nonce 重新签名。也可提供外部签名回调，实现密钥托管。密钥对象不实现调试输出，错误不包含 seed。

## JetStream

导入 `LilaLola67/moonnats/jetstream`，别名 `@js`：

```moonbit
let js = @js.JetStream::new(c)
ignore(js.create_stream("JOBS", ["jobs.>"], storage="file"))
let worker = js.create_consumer("JOBS", "worker", filter="jobs.>")
let stored = js.publish("jobs.report", b"report-42", message_id="report-42")
for msg in worker.fetch(batch=10, expires_ms=1000) {
  let meta = @js.metadata(msg)
  println(meta.stream_sequence)
  @js.acknowledge(c, msg, sync=true)
}
```

`create_stream_config`、`update_stream` 和 `create_consumer_config` 接受服务器标准 JSON 配置；`stream_info`、`streams`、`consumers`、`Consumer::info/update/delete` 管理资源。`JetStream::new(domain~)` 使用独立 API 前缀。

发布支持 `expected_stream`、`expected_last_sequence`、`expected_last_subject_sequence`。`PubAck` 包含 stream、sequence 和 duplicate。

`acknowledge(kind="nak", delay_ms=...)` 延迟重投；`kind="progress"` 延长处理时间；`kind="term"` 终止重投；同步确认只用于 `kind="ack"`。元数据兼容经典与带 domain/account 前缀的 ACK subject，序号使用精确 Int64。

## KV

```moonbit
let bucket = @kv.create(js, "CONFIG", history=8)
let revision = bucket.create("service.port", b"8080")
ignore(bucket.update("service.port", b"8081", revision))
let entry = bucket.get("service.port")
let changes = bucket.watch(pattern="service.>", updates_only=true)
// changes.next() 等待后续变化；完成后 changes.close()
```

`history` 返回保存的版本，包含 DEL/PURGE 标记；`get` 同样返回标记，调用方检查 `operation`。`delete(purge=true)` 清理旧历史并留下 PURGE 标记。`create` 可在删除标记之后重新创建；`update` 与有条件删除使用服务器版本检查。Watch 默认先传当前值，再接收变化；空快照不会产生虚构的 Entry。Watch 是活资源，需显式关闭。

## Object Store

```moonbit
let objects = @objects.create(js, "FILES")
ignore(objects.put("report.bin", bytes, chunk_size=131072))
let restored = objects.get("report.bin", max_bytes=67108864)
```

大对象使用 `put_stream(name, next)`，其中 `next` 返回下一块 `Bytes?`；`get_to(name, sink)` 逐块输出。上传和下载都计算 SHA-256，下载还校验总字节数。`get_to` 的 sink 会在最终校验前接收数据，应写入临时目标，成功后再对外公开；CLI 已按此方式实现。

`get_info`、`list` 和 `delete` 管理对象；`Bucket::destroy` 删除整个桶。覆盖先提交新元数据，再清理旧块。连接结果不确定时可能留下孤立块，不将未确认的发布伪装为成功；必要时由管理员检查存储。

## 类型化消息

`typed.Codec[T]` 包含 `encode`/`decode` 两个可抛错函数。`json_codec[T]()` 用于实现 `ToJson` 与 `FromJson` 的类型。`typed.publish`、`typed.request`、`typed.next` 在 Bytes 接口上增加类型检查，不改变底层消息语义。

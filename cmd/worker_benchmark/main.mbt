///|
async fn main {
  let url = @env.get_env_var("MOONNATS_WORKER_BENCH_URL").unwrap()
  let mode = @env.get_env_var("MOONNATS_WORKER_BENCH_MODE").unwrap()
  assert_true(mode == "sparse" || mode == "burst" || mode == "mixed")
  let count = if mode == "sparse" { 12 } else { 128 }
  @client.with_client(@client.Options::new(servers=[url], heartbeat_ms=0), client => {
    let js = @js.JetStream::new(client)
    let stream = "MN_BENCH_" + client.new_inbox().replace_all(old=".", new="_")
    let subject = "moon.workerbench." + stream
    ignore(
      js.add_stream(
        stream,
        @js.StreamConfig::new([subject], storage=@js.Memory),
      ),
    )
    defer @async.protect_from_cancel(() => js.delete_stream(stream))
    let worker = js.add_consumer(
      stream,
      "workers",
      @js.ConsumerConfig::new(filter=subject, ack_wait_ms=5000),
    )
    let control = @js.WorkControl::new()
    let starts = FixedArray::make(count, 0L)
    let seen = FixedArray::make(count, false)
    let samples : Array[Int64] = []
    let mut first_ms = 0L
    let began = @async.now()
    @async.with_timeout(30000, () => {
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          worker.consume_reliably(
            control,
            concurrency=4,
            prefetch=16,
            prefetch_bytes=16384,
            (message, _) => {
              let id = message.payload[0].to_int()
              assert_true(id < count && !seen[id])
              seen[id] = true
              let latency = @async.now() - starts[id]
              if samples.is_empty() {
                first_ms = latency
              }
              samples.push(latency)
              if mode == "mixed" && id % 8 == 0 {
                @async.sleep(30)
              }
              if samples.length() == count {
                control.stop()
              }
              @js.Confirm
            },
          )
        })
        @async.sleep(50)
        if mode == "sparse" {
          for i in 0..<count {
            starts[i] = @async.now()
            ignore(
              js.publish(
                subject,
                Bytes::make(512, i.to_byte()),
                message_id=i.to_string(),
              ),
            )
            while control.completed() < i.to_int64() + 1L {
              @async.sleep(1)
            }
            @async.sleep(20)
          }
        } else {
          @js.with_publisher(js, max_pending=16, publisher => {
            for i in 0..<count {
              starts[i] = @async.now()
              ignore(
                publisher.publish(
                  subject,
                  Bytes::make(512, i.to_byte()),
                  message_id=i.to_string(),
                ),
              )
            }
          })
        }
      })
    })
    assert_eq(samples.length(), count)
    assert_eq(control.completed(), count.to_int64())
    assert_eq(control.uncertain(), 0L)
    samples.sort()
    println(
      Json::object({
        "mode": Json(mode),
        "messages": Json(count),
        "first_ms": @js.integer(first_ms),
        "p95_ms": @js.integer(samples[(count * 95 + 99) / 100 - 1]),
        "p99_ms": @js.integer(samples[(count * 99 + 99) / 100 - 1]),
        "elapsed_ms": @js.integer(@async.now() - began),
      }).stringify(),
    )
  })
}

///|
async fn main {
  @client.with_client(
    @client.Options::new(
      servers=[
        @env.get_env_var("MOONNATS_TEST_URL").unwrap_or(
          "nats://127.0.0.1:14222",
        ),
      ],
      heartbeat_ms=0,
    ),
    c => {
      let count = 1000
      let payload = Bytes::make(1024, 42)
      let sub = c.subscribe("benchmark.roundtrip")
      c.flush()
      let latencies : Array[Int64] = []
      let started = @async.now()
      for _ in 0..<count {
        let before = @async.now()
        c.publish(sub.subject, payload)
        assert_eq(sub.next().payload, payload)
        latencies.push(@async.now() - before)
      }
      let core_ms = (@async.now() - started).max(1L)
      latencies.sort()
      c.unsubscribe(sub)
      let js = @js.JetStream::new(c)
      let name = "MN_BENCH_" + c.new_inbox().replace_all(old=".", new="_")
      let subject = "benchmark." + name
      ignore(
        js.add_stream(
          name,
          @js.StreamConfig::new([subject], storage=@js.Memory),
        ),
      )
      defer @async.protect_from_cancel(() => {
        js.delete_stream(name) catch {
          _ => ()
        }
      })
      let before = @async.now()
      @js.with_publisher(js, max_pending=32, publisher => {
        for _ in 0..<count {
          ignore(publisher.publish(subject, payload))
        }
      })
      let pipeline_ms = (@async.now() - before).max(1L)
      let result : Json = {
        "messages": Json(count),
        "payload_bytes": 1024,
        "clock_resolution": "milliseconds",
        "core_roundtrip_ms": Json(core_ms.to_int()),
        "core_messages_per_second": Json(
          count.to_double() * 1000.0 / core_ms.to_double(),
        ),
        "roundtrip_p50_ms": Json(latencies[500].to_int()),
        "roundtrip_p95_ms": Json(latencies[950].to_int()),
        "roundtrip_p99_ms": Json(latencies[990].to_int()),
        "jetstream_acknowledged_pipeline_ms": Json(pipeline_ms.to_int()),
        "jetstream_messages_per_second": Json(
          count.to_double() * 1000.0 / pipeline_ms.to_double(),
        ),
        "pipeline_concurrency": 32,
        "storage": "memory",
      }
      println(result.stringify())
    },
  )
}

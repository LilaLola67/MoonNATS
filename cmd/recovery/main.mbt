///|
async fn main {
  let states = []
  let mut credential_calls = 0
  let mut stage = "setup"
  @client.with_client(
    @client.Options::new(
      servers=["nats://127.0.0.1:14226", "nats://127.0.0.1:14225"],
      heartbeat_ms=50,
      timeout_ms=500,
      reconnect_delay_ms=20,
      on_state=s => states.push(s),
      reconnect_attempts=30,
      reconnect_buffer_bytes=128,
      reconnect_jitter_ms=10,
      credentials=_ => {
        credential_calls += 1
        @wire.ConnectOptions::new()
      },
    ),
    c => {
      let sub = c.subscribe("recovery", max_messages=2)
      c.flush()
      c.publish("recovery", b"before")
      assert_eq(sub.next().payload, b"before")
      let js = @js.JetStream::new(c)
      js.delete_stream("KV_MN_RECOVERY") catch {
        _ => ()
      }
      let bucket = @kv.create(js, "MN_RECOVERY")
      ignore(bucket.put("config", b"saved"))
      let cache = bucket.config_cache()
      ignore(cache.next())
      let ordered = js.ordered_consumer(
        "KV_MN_RECOVERY",
        filter="$KV.MN_RECOVERY.>",
      )
      ignore(ordered.next())
      js.delete_stream("MN_WORKER_RECOVERY") catch {
        _ => ()
      }
      ignore(
        js.add_stream(
          "MN_WORKER_RECOVERY",
          @js.StreamConfig::new(["recovery.jobs"], storage=@js.File),
        ),
      )
      let worker = js.add_consumer(
        "MN_WORKER_RECOVERY",
        "durable",
        @js.ConsumerConfig::new(filter="recovery.jobs", ack_wait_ms=500),
      )
      ignore(js.publish("recovery.jobs", b"retry after restart"))
      js.delete_stream("KV_MN_EXPIRING") catch {
        _ => ()
      }
      let expiring = @kv.create(js, "MN_EXPIRING", ttl_ms=1500)
      ignore(expiring.put("temporary", b"valid before outage"))
      let expiring_cache = expiring.config_cache()
      assert_true(
        expiring_cache.next() is @kv.Snapshot(entries) && entries.length() == 1,
      )
      let control = @js.WorkControl::new()
      let mut started = false
      let mut redelivered = false
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          worker.consume(control, concurrency=2, recovery_timeout_ms=7000, msg => {
            if msg.payload == b"retry after restart" &&
              @js.metadata(msg).deliveries == 1L {
              started = true
              while c.state() == @client.Connected {
                @async.sleep(5)
              }
              // Leave the first delivery unconfirmed so JetStream can redeliver it.
            } else {
              @js.acknowledge(c, msg, sync=true)
              if msg.payload == b"retry after restart" {
                redelivered = true
              }
              if msg.payload == b"new job" {
                control.stop()
              }
            }
          }) catch {
            @js.Status(code, description) => {
              @stdio.stderr.text.write(
                "Worker pull status " +
                code.to_string() +
                " " +
                description +
                "\n",
              )
              raise @js.Status(code, description)
            }
            error => raise error
          }
        })
        @async.with_timeout(3000, () => {
          while !started {
            @async.sleep(5)
          }
        })
        stage = "restart"
        @stdio.stderr.text.write("RESTART\n")
        @async.with_timeout(3000, () => {
          while c.state() != @client.Reconnecting {
            @async.sleep(5)
          }
        })
        assert_true(!cache.ready())
        c.publish("recovery", b"buffered")
        assert_true(c.statistics().buffered_bytes > 0)
        assert_true(
          try {
            c.publish("recovery", Bytes::make(200, 0))
            false
          } catch {
            @client.SlowConsumer => true
            _ => false
          },
        )
        @async.with_timeout(5000, () => {
          while c.statistics().reconnects == 0L {
            @async.sleep(10)
          }
        })
        c.flush()
        assert_eq(sub.next().payload, b"buffered")
        assert_eq(c.statistics().buffered_bytes, 0)
        assert_true(credential_calls >= 2)
        stage = "KV resnapshot"
        assert_true(
          cache.next() is @kv.Snapshot(entries) && entries.length() == 1,
        )
        assert_eq(cache.get("config").unwrap().value, b"saved")
        ignore(bucket.put("config", b"new"))
        stage = "ordered replay"
        assert_eq(ordered.next().payload, b"new")
        ordered.close()
        cache.close()
        bucket.destroy()
        stage = "TTL resnapshot"
        assert_true(
          expiring_cache.next() is @kv.Snapshot(entries) && entries.is_empty(),
        )
        assert_true(expiring_cache.get("temporary") is None)
        expiring_cache.close()
        expiring.destroy()
        @async.with_timeout(5000, () => {
          while !redelivered {
            @async.sleep(5)
          }
        })
        stage = "worker follow-up"
        ignore(js.publish("recovery.jobs", b"new job"))
      })
      assert_true(redelivered)
      assert_eq(control.completed(), 3L)
      js.delete_stream("MN_WORKER_RECOVERY")
      println(
        "durable worker resumed after restart, redelivery acknowledged and expired config omitted PASS",
      )
      assert_true(states.contains(@client.Reconnecting))
      assert_true(
        try {
          ignore(sub.next(timeout_ms=50))
          false
        } catch {
          _ => true
        },
      )
      let silent = c.subscribe("silent")
      c.flush()
      assert_true(
        try {
          ignore(c.request("silent", b"", timeout_ms=25))
          false
        } catch {
          @async.TimeoutError => true
          _ => false
        },
      )
      assert_eq(c.statistics().subscriptions, 1)
      assert_true(
        try {
          @async.with_timeout(25, () => {
            ignore(c.request("silent", b"", timeout_ms=5000))
          })
          false
        } catch {
          @async.TimeoutError => true
          _ => false
        },
      )
      assert_eq(c.statistics().subscriptions, 1)
      c.unsubscribe(silent)
      let slow = c.subscribe("slow", capacity=1)
      c.flush()
      c.publish("slow", b"1")
      c.publish("slow", b"2")
      c.flush()
      assert_eq(slow.next().payload, b"1")
      assert_true(
        try {
          ignore(slow.next())
          false
        } catch {
          @client.SlowConsumer => true
          _ => false
        },
      )
      assert_eq(c.statistics().slow_consumers, 1L)
      let a = c.subscribe("group", queue="workers")
      let b = c.subscribe("group", queue="workers")
      c.flush()
      for _ in 0..<20 {
        c.publish("group", b"job")
      }
      c.flush()
      assert_eq(a.pending() + b.pending(), 20)
      assert_true(a.pending() > 0 && b.pending() > 0)
      c.unsubscribe(a)
      c.unsubscribe(b)
      let drain_sub = c.subscribe("drain", max_messages=1)
      c.flush()
      c.publish("drain", b"work")
      @async.with_task_group(g => {
        let mut completed = false
        g.spawn_bg(() => {
          drain_sub.process_next(_ => {
            @async.sleep(50)
            completed = true
          })
        })
        @async.sleep(5)
        c.drain(timeout_ms=1000)
        assert_true(completed)
      })
      assert_eq(c.state(), @client.Closed)
      @stdio.stderr.text.write(
        "recovery: failover, server restart, subscription limits, timeout/cancel cleanup, slow consumers, queue groups, heartbeat and drain PASS",
      )
    },
  ) catch {
    @js.Status(code, description) => {
      @stdio.stderr.text.write(
        "Recovery stage " +
        stage +
        " status " +
        code.to_string() +
        " " +
        description +
        "\n",
      )
      raise @js.Status(code, description)
    }
    error => raise error
  }
}

///|
async fn main {
  let states = []
  @client.with_client(
    @client.Options::new(
      servers=["nats://127.0.0.1:14226", "nats://127.0.0.1:14225"],
      heartbeat_ms=50,
      timeout_ms=500,
      reconnect_delay_ms=20,
      on_state=s => states.push(s),
    ),
    c => {
      let sub = c.subscribe("recovery", max_messages=2)
      c.flush()
      c.publish("recovery", b"before")
      assert_eq(sub.next().payload, b"before")
      @stdio.stderr.text.write("RESTART\n")
      @async.sleep(200)
      @async.with_timeout(5000, () => {
        while c.statistics().reconnects == 0L {
          @async.sleep(10)
        }
      })
      c.flush()
      c.publish("recovery", b"after")
      assert_eq(sub.next().payload, b"after")
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
  )
}

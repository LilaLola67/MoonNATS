///|
async fn permissions_case() -> Unit {
  let denials : Array[@client.PermissionDenial] = []
  @client.with_client(
    @client.Options::new(
      servers=["nats://127.0.0.1:14232"],
      connect=@wire.ConnectOptions::new(
        user="test-permissions",
        password="synthetic-test-password",
      ),
      heartbeat_ms=0,
      on_event=event => {
        if event is @client.PermissionViolation(denial) {
          denials.push(denial)
        }
      },
    ),
    c => {
      let allowed = c.subscribe("permission.allowed")
      let workers = c.subscribe("permission.jobs", queue="workers")
      let denied = c.subscribe("permission.jobs", queue="blocked")
      c.publish("permission.denied", b"denied")
      c.flush()
      let rejected = @async.with_timeout(1000, () => {
        ignore(denied.next())
        false
      }) catch {
        @client.SubscriptionDenied("permission.jobs") => true
        e => raise e
      }
      assert_true(rejected)
      assert_eq(denials.length(), 2)
      assert_true(denials[0].operation is @client.Subscribe)
      assert_eq(denials[0].subject, Some("permission.jobs"))
      assert_eq(denials[0].queue, Some("blocked"))
      assert_true(denials[1].operation is @client.Publish)
      assert_eq(denials[1].subject, Some("permission.denied"))
      let too_long = c.subscribe("permission.too.long", queue="workers")
      c.flush()
      let limit_rejected = @async.with_timeout(1000, () => {
        ignore(too_long.next())
        false
      }) catch {
        @client.SubscriptionDenied("permission.too.long") => true
        e => raise e
      }
      assert_true(limit_rejected)
      c.publish(allowed.subject, b"allowed")
      c.publish(workers.subject, b"job")
      @async.with_timeout(1000, () => {
        assert_eq(allowed.next().payload, b"allowed")
        assert_eq(workers.next().payload, b"job")
      })
      assert_eq(c.statistics().reconnects, 0L)
    },
  )
  println(
    "Permissions: denied publish and queue isolated on the original connection",
  )
}

///|
async fn buffer_case() -> Unit {
  let rejected : Array[@client.BufferRejectReason] = []
  @client.with_client(
    @client.Options::new(
      servers=["nats://127.0.0.1:14233", "nats://127.0.0.1:14234"],
      reconnect_delay_ms=1000,
      reconnect_jitter_ms=0,
      reconnect_buffer_bytes=4096,
      heartbeat_ms=0,
      on_event=event => {
        if event is @client.BufferedPublishRejected("buffer.check", reason) {
          rejected.push(reason)
        }
      },
    ),
    c => {
      let sub = c.subscribe("buffer.check")
      c.flush()
      @stdio.stderr.text.write("DROP_PRIMARY\n")
      @async.with_timeout(3000, () => {
        while !(c.state() is @client.Reconnecting) {
          @async.sleep(5)
        }
      })
      let headers = @wire.Headers::new(entries=[("Key", "value")])
      let boundary = Bytes::make(64, b'a')
      c.publish(sub.subject, Bytes::make(80, b'x'))
      c.publish(sub.subject, b"first")
      c.publish(sub.subject, b"x", headers~)
      c.publish(sub.subject, boundary, headers~)
      c.publish(sub.subject, boundary)
      assert_true(c.statistics().buffered_bytes > 0)
      c.wait_connected()
      c.flush()
      assert_eq(rejected.length(), 3)
      assert_true(rejected[0] is @client.PayloadTooLarge(80, 64))
      assert_true(rejected[1] is @client.HeadersUnsupported)
      assert_true(rejected[2] is @client.PayloadTooLarge(_, 64))
      assert_eq(c.statistics().buffered_bytes, 0)
      assert_eq(c.statistics().reconnects, 1L)
      @async.with_timeout(1000, () => {
        assert_eq(sub.next().payload, b"first")
        assert_eq(sub.next().payload, boundary)
      })
      let direct_rejected = try c.publish(sub.subject, b"x", headers~) catch {
        @client.InvalidOption(_) => true
        e => raise e
      } noraise {
        _ => false
      }
      assert_true(direct_rejected)
      c.publish(sub.subject, b"after")
      @async.with_timeout(1000, () => assert_eq(sub.next().payload, b"after"))
    },
  )
  println(
    "Reconnect buffer: payload and header checks preserve compatible frame order",
  )
}

///|
async fn expect_flush_timeout(c : @client.Client, timeout_ms : Int32) -> Unit {
  let timed_out = try c.flush(timeout_ms~) catch {
    @async.TimeoutError => true
    e => raise e
  } noraise {
    _ => false
  }
  assert_true(timed_out)
}

///|
async fn flush_case() -> Unit {
  @client.with_client(
    @client.Options::new(
      servers=[@env.get_env_var("MOONNATS_FAULT_URL").unwrap()],
      reconnect_delay_ms=10,
      reconnect_jitter_ms=0,
      heartbeat_ms=0,
    ),
    c => {
      let mut first_done = false
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          expect_flush_timeout(c, 450)
          first_done = true
        })
        @async.sleep(30)
        expect_flush_timeout(c, 60)
        assert_false(first_done)
      })
      @stdio.stderr.text.write("FLUSH_TIMEOUT\n")
      c.flush(timeout_ms=3500)
      assert_eq(c.statistics().reconnects, 1L)
    },
  )
  println("Flush: lock wait has its own deadline and late PONG is isolated")
}

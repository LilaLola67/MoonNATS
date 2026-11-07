///|
let fault_stream = "MN_ACK_FAULTS"

///|
let fault_subject = "moon.faults.tasks"

///|
async fn jetstream_fault_case() -> Unit {
  @client.with_client(
    @client.Options::new(
      servers=[@env.get_env_var("MOONNATS_FAULT_URL").unwrap()],
      heartbeat_ms=0,
    ),
    client => {
      let js = @js.JetStream::new(client)
      ignore(
        js.add_stream(
          fault_stream,
          @js.StreamConfig::new([fault_subject], storage=@js.Memory),
        ),
      )
      let consumer = js.add_consumer(
        fault_stream,
        "worker",
        @js.ConsumerConfig::new(filter=fault_subject, ack_wait_ms=500),
      )
      let recovered = js.publish_reliably(
        fault_subject,
        Bytes::make(512, b'f'),
        message_id="first",
        expected_stream=fault_stream,
        attempt_timeout_ms=150,
      )
      assert_eq(recovered.sequence, 1L)
      assert_true(recovered.duplicate)
      ignore(js.publish(fault_subject, Bytes::make(512, b's')))
      let oversized = try consumer.fetch(batch=2, max_bytes=100) catch {
        @js.Status(409, "Message Size Exceeds MaxBytes") => true
        error => raise error
      } noraise {
        _ => false
      }
      assert_true(oversized)
      let control = @js.WorkControl::new()
      let seen : Array[Int64] = []
      let uncertain : Array[@js.UncertainDisposition] = []
      @async.with_timeout(5000, () => {
        consumer.consume_reliably(
          control,
          prefetch=4,
          prefetch_bytes=900,
          ack_timeout_ms=150,
          on_uncertain=event => uncertain.push(event),
          (_, meta) => {
            assert_eq(meta.deliveries, 1L)
            seen.push(meta.stream_sequence)
            if meta.stream_sequence == 2L {
              control.stop()
            }
            @js.Confirm
          },
        )
      })
      assert_eq(seen, [1L, 2L])
      assert_eq(uncertain.length(), 1)
      assert_eq(uncertain[0].message.stream_sequence, 1L)
      assert_true(uncertain[0].outcome is @js.Confirm)
      assert_true(uncertain[0].cause is @async.TimeoutError)
      assert_eq(control.completed(), 1L)
      assert_eq(control.uncertain(), 1L)
      let exhausted = try
        js.publish_reliably(
          fault_subject,
          b"unknown",
          message_id="unknown",
          expected_stream=fault_stream,
          timeout_ms=500,
          attempt_timeout_ms=80,
        )
      catch {
        @js.PublishOutcomeUnknown("MN_ACK_FAULTS", "unknown") => true
        error => raise error
      } noraise {
        _ => false
      }
      assert_true(exhausted)
      let invalid_window = try
        js.publish_reliably(
          fault_subject,
          b"outside-window",
          message_id="outside-window",
          expected_stream=fault_stream,
          timeout_ms=120000,
        )
      catch {
        @js.Invalid(_) => true
        error => raise error
      } noraise {
        _ => false
      }
      assert_true(invalid_window)
      drain_case(js)
      println(
        "JetStream fault client: duplicate PubAck, unconfirmed ACK and expired publish budget PASS",
      )
    },
  )
}

///|
async fn drain_case(js : @js.JetStream) -> Unit {
  let consumer = js.add_consumer(
    fault_stream,
    "drain",
    @js.ConsumerConfig::new(
      filter=fault_subject,
      start_sequence=3L,
      ack_wait_ms=500,
    ),
  )
  let control = @js.WorkControl::new()
  let mut started = false
  let mut stopped_at = 0L
  @async.with_timeout(4000, () => {
    @async.with_task_group(group => {
      group.spawn_bg(() => {
        while !started {
          @async.sleep(5)
        }
        stopped_at = @async.now()
        control.stop()
      })
      let timed_out = try
        consumer.consume_reliably(control, poll_ms=100, drain_timeout_ms=80, (
          message,
          meta,
        ) => {
          assert_eq(message.payload, b"unknown")
          assert_eq(meta.deliveries, 1L)
          started = true
          @async.sleep(3000)
          @js.Confirm
        })
      catch {
        @js.WorkerDrainTimedOut => true
        error => raise error
      } noraise {
        _ => false
      }
      assert_true(timed_out)
      assert_true(@async.now() - stopped_at < 1500L)
    })
  })
  assert_eq(control.active(), 0)
  assert_eq(control.completed(), 0L)
}

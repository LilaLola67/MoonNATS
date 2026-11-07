///|
let stream_name = "MOON_CLUSTER_JOBS"

///|
let jobs = "moon.cluster.jobs"

///|
let durable = "workers"

///|
let payload_size = 1024

///|
fn count(name : String, fallback : Int32) -> Int32 raise {
  match @env.get_env_var(name) {
    Some(value) => {
      let value = @string.parse_int(value)
      guard value > 0 && value <= 1000000 else {
        raise @js.Invalid("load count")
      }
      value
    }
    None => fallback
  }
}

///|
fn payload(id : Int32) -> Bytes {
  Bytes::makei(payload_size, i => {
    if i < 4 {
      ((id >> ((3 - i) * 8)) & 255).to_byte()
    } else {
      (i % 251).to_byte()
    }
  })
}

///|
fn payload_id(bytes : Bytes) -> Int32 raise {
  guard bytes.length() == payload_size else {
    raise @js.Invalid("load payload length")
  }
  (bytes[0].to_int() << 24) |
  (bytes[1].to_int() << 16) |
  (bytes[2].to_int() << 8) |
  bytes[3].to_int()
}

///|
fn options(url : String) -> @client.Options {
  @client.Options::new(
    servers=[url],
    heartbeat_ms=250,
    reconnect_attempts=40,
    reconnect_delay_ms=25,
    reconnect_jitter_ms=25,
    timeout_ms=4000,
  )
}

///|
async fn core_load(
  sender : @client.Client,
  receiver : @client.Client,
  total : Int32,
) -> Unit {
  let sub = receiver.subscribe("moon.cluster.core", capacity=256)
  defer @async.protect_from_cancel(() => {
    receiver.unsubscribe(sub) catch {
      _ => ()
    }
  })
  receiver.flush()
  // Wait for subscription interest to propagate across the cluster route.
  @async.with_timeout(5000, () => {
    while true {
      sender.publish(sub.subject, b"ready")
      let received = Some(sub.next(timeout_ms=100)) catch {
        @async.TimeoutError => None
        e => raise e
      }
      if received is Some(_) {
        break
      }
    }
  })
  let start = @async.now()
  for base = 0; base < total; {
    let limit = (base + 128).min(total)
    for id in base..<limit {
      sender.publish(sub.subject, payload(id))
    }
    sender.flush()
    for id in base..<limit {
      let mut received = sub.next()
      while received.payload == b"ready" {
        received = sub.next()
      }
      assert_eq(received.payload, payload(id))
    }
    continue limit
  }
  assert_eq(receiver.statistics().slow_consumers, 0L)
  let report : Json = {
    "phase": "core-cross-node",
    "messages": Json(total),
    "payload_bytes": Json(payload_size),
    "elapsed_ms": @js.integer(@async.now() - start),
  }
  println(report.stringify())
}

///|
async fn job_load(
  first : @client.Client,
  second : @client.Client,
  begin : Int32,
  total : Int32,
  phase : String,
) -> Unit {
  let clients = [first, second]
  let controls = [@js.WorkControl::new(), @js.WorkControl::new()]
  let delivered = [0, 0]
  let seen = FixedArray::make(total, false)
  let mut done = 0
  let js = @js.JetStream::new(first)
  let start = @async.now()
  @async.with_timeout(120000, () => {
    @async.with_task_group(group => {
      for i in 0..<2 {
        let connection = clients[i]
        let worker = @js.JetStream::new(connection).consumer(
          stream_name, durable,
        )
        group.spawn_bg(() => {
          worker.consume_reliably(
            controls[i],
            concurrency=4,
            prefetch=32,
            prefetch_bytes=65536,
            recovery_timeout_ms=20000,
            (msg, _) => {
              let id = payload_id(msg.payload)
              guard id >= begin && id < begin + total && !seen[id - begin] else {
                raise @js.Invalid(
                  "duplicate or unexpected job: " + id.to_string(),
                )
              }
              assert_eq(msg.payload, payload(id))
              seen[id - begin] = true
              delivered[i] += 1
              done += 1
              if done == total {
                for control in controls {
                  control.stop()
                }
              }
              @js.Confirm
            },
          )
        })
      }
      @js.with_publisher(js, max_pending=64, max_bytes=64 * payload_size, publisher => {
        for base = begin; base < begin + total; {
          let limit = (base + 64).min(begin + total)
          let futures = []
          for id in base..<limit {
            futures.push(
              publisher.publish(jobs, payload(id), message_id=id.to_string()),
            )
          }
          for future in futures {
            let ack = future.wait() catch {
              @client.NoResponders =>
                raise @js.Invalid(phase + ": publish had no responder")
              e => raise e
            }
            assert_eq(ack.stream, stream_name)
            assert_true(!ack.duplicate)
          }
          continue limit
        }
      })
    })
  })
  assert_eq(done, total)
  assert_eq(controls[0].completed() + controls[1].completed(), total.to_int64())
  assert_eq(controls[0].uncertain() + controls[1].uncertain(), 0L)
  assert_true(delivered[0] > 0 && delivered[1] > 0)
  let status = js.consumer(stream_name, durable).info()
  assert_eq(@js.integer_field(status, "num_pending"), 0L)
  assert_eq(@js.integer_field(status, "num_ack_pending"), 0L)
  let report : Json = {
    "phase": Json(phase),
    "messages": Json(total),
    "payload_bytes": Json(payload_size),
    "worker_deliveries": Json(delivered),
    "elapsed_ms": @js.integer(@async.now() - start),
  }
  println(report.stringify())
}

///|
async fn leader_loss(client : @client.Client) -> Unit {
  let sub = client.subscribe("moon.cluster.recovered")
  client.flush()
  let js = @js.JetStream::new(client)
  let worker = js.consumer(stream_name, durable)
  let control = @js.WorkControl::new()
  let mut first_sequence = 0L
  let mut delivery_count = 0L
  ignore(js.publish(jobs, b"in-flight", message_id="in-flight"))
  @async.with_timeout(35000, () => {
    worker.consume_reliably(control, recovery_timeout_ms=20000, (msg, meta) => {
      assert_eq(msg.payload, b"in-flight")
      if first_sequence == 0L {
        first_sequence = meta.stream_sequence
        @stdio.stderr.text.write("STOP_STREAM_LEADER\n")
        while client.statistics().reconnects == 0L {
          @async.sleep(10)
        }
        @js.RetryAfter(50)
      } else {
        assert_eq(meta.stream_sequence, first_sequence)
        assert_true(meta.deliveries >= 2L)
        delivery_count = meta.deliveries
        control.stop()
        @js.Confirm
      }
    })
  })
  assert_eq(control.completed(), 1L)
  client.publish(sub.subject, b"restored")
  client.flush()
  assert_eq(sub.next().payload, b"restored")
  client.unsubscribe(sub)
  let report : Json = {
    "phase": "stream-leader-failover",
    "deliveries": @js.integer(delivery_count),
    "reconnects": @js.integer(client.statistics().reconnects),
  }
  println(report.stringify())
}

///|
async fn main {
  let primary = @env.get_env_var("MOONNATS_CLUSTER_PRIMARY").unwrap()
  let peer = @env.get_env_var("MOONNATS_CLUSTER_PEER").unwrap()
  let args = @env.args()
  @client.with_client(options(primary), client => {
    let js = @js.JetStream::new(client)
    if args.contains("--prepare") {
      @client.with_client(options(peer), receiver => {
        core_load(client, receiver, count("MOONNATS_CLUSTER_CORE_COUNT", 50000))
      })
      // Routes can be up just before all peers become eligible for JetStream placement.
      @async.with_timeout(20000, () => {
        while true {
          try
            js.add_stream(
              stream_name,
              @js.StreamConfig::new([jobs], replicas=3),
            )
          catch {
            @js.Api(_, 10005, _) => @async.sleep(200)
            e => raise e
          } noraise {
            _ => break
          }
        }
      })
      ignore(
        js.add_consumer(
          stream_name,
          durable,
          @js.ConsumerConfig::new(filter=jobs, ack_wait_ms=2000),
        ),
      )
    } else if args.contains("--after-roll") {
      let worker = js.consumer(stream_name, durable)
      ignore(js.publish(jobs, b"after-roll", message_id="after-roll"))
      let messages = worker.fetch(batch=1, expires_ms=2000)
      assert_eq(messages.length(), 1)
      assert_eq(messages[0].payload, b"after-roll")
      @js.acknowledge(client, messages[0], sync=true)
      println("Consumer leader roll: publish, resume durable and confirm PASS")
    } else {
      @client.with_client(options(peer), other => {
        let total = count("MOONNATS_CLUSTER_JOB_COUNT", 6000)
        job_load(client, other, 0, total, "replicated-load")
        leader_loss(client)
        // Client reconnect can complete before JetStream finishes electing leaders.
        let permission = @async.with_timeout(35000, () => {
          @stdio.stdin.read_until(b"\n", include_delimiter=false)
        })
        assert_eq(permission, Some(b"resume"))
        job_load(client, other, total, total, "quorum-load")
      })
    }
  }) catch {
    @js.Status(code, description) => {
      @stdio.stderr.text.write(
        "JetStream status " + code.to_string() + " " + description + "\n",
      )
      raise @js.Status(code, description)
    }
    @js.Api(code, err, description) => {
      @stdio.stderr.text.write(
        "JetStream API " +
        code.to_string() +
        "/" +
        err.to_string() +
        " " +
        description +
        "\n",
      )
      raise @js.Api(code, err, description)
    }
    @js.Invalid(description) => {
      @stdio.stderr.text.write(description + "\n")
      raise @js.Invalid(description)
    }
    e => raise e
  }
}

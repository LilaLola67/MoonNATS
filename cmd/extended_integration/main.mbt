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
      let js = @js.JetStream::new(c)
      js.delete_stream("MN_EXT") catch {
        _ => ()
      }
      ignore(
        js.add_stream(
          "MN_EXT",
          @js.StreamConfig::new(
            ["extended.jobs", "extended.failed"],
            storage=@js.Memory,
          ),
        ),
      )
      let consumer = js.add_consumer(
        "MN_EXT",
        "worker",
        @js.ConsumerConfig::new(filter="extended.jobs"),
      )
      @js.with_publisher(js, max_pending=4, max_bytes=100, publisher => {
        let futures = []
        for i in 0..<12 {
          futures.push(
            publisher.publish(
              "extended.jobs",
              @utf8.encode(i.to_string()),
              message_id=i.to_string(),
            ),
          )
        }
        for future in futures {
          assert_eq(future.wait().stream, "MN_EXT")
        }
        publisher.flush()
        assert_eq(publisher.pending(), 0)
        assert_eq(publisher.pending_bytes(), 0)
        assert_eq(publisher.ack_metrics().0, 12L)
      })
      let control = @js.WorkControl::new()
      let mut handled = 0
      consumer.consume(control, concurrency=3, ack_mode=@js.OnSuccess, msg => {
        assert_true(!msg.payload.is_empty())
        handled += 1
        if handled == 12 {
          control.stop()
        }
        @async.sleep(2)
      })
      assert_eq(control.completed(), 12L)
      assert_eq(control.active(), 0)
      assert_eq(consumer.fetch(expires_ms=100).length(), 0)
      consumer.delete()
      let reader = js.ordered_consumer("MN_EXT", filter="extended.jobs")
      let first = reader.next()
      assert_eq(@js.metadata(first).stream_sequence, 1L)
      js.consumer("MN_EXT", @js.metadata(first).consumer).delete()
      for sequence in 2L..<=12L {
        assert_eq(@js.metadata(reader.next()).stream_sequence, sequence)
      }
      assert_eq(reader.sequence(), 12L)
      reader.close()
      let push = js.push_consumer(
        "MN_EXT",
        "push",
        filter="extended.jobs",
        heartbeat_ms=100,
      )
      for _ in 0..<12 {
        @js.acknowledge(c, push.next())
      }
      guard push.consumer.info()
        is { "config": { "deliver_subject": String(delivery), .. }, .. } else {
        raise @js.Invalid("push info")
      }
      let flow = c.subscribe("extended.flow")
      let stalled = c.subscribe("extended.stalled")
      c.flush()
      c.publish(delivery, b"", reply_to="extended.flow", headers=@wire.Headers::{
        entries: [("Nats-Consumer-Stalled", "extended.stalled")],
        status: Some(100),
        description: "FlowControl Request",
      })
      ignore(js.publish("extended.jobs", b"after-flow"))
      let after_flow = push.next()
      assert_eq(after_flow.payload, b"after-flow")
      @js.acknowledge(c, after_flow)
      assert_eq(flow.next().payload, b"")
      assert_eq(stalled.next().payload, b"")
      c.unsubscribe(flow)
      c.unsubscribe(stalled)
      // Multiple idle heartbeats must not be exposed as application messages.
      assert_true(
        try {
          ignore(push.next(timeout_ms=350))
          false
        } catch {
          @async.TimeoutError => true
          _ => false
        },
      )
      push.close()
      assert_true(
        js
        .all_streams()
        .iter()
        .any(j => j is { "config": { "name": "MN_EXT", .. }, .. }),
      )
      assert_true(js.account_usage().streams > 0L)
      assert_eq(js.all_consumers("MN_EXT").length(), 0)
      assert_true(
        try {
          @js.with_publisher(js, publisher => {
            ignore(publisher.publish("unbound.no-stream", b""))
          })
          false
        } catch {
          @client.NoResponders => true
          _ => false
        },
      )
      let failed_worker = js.add_consumer(
        "MN_EXT",
        "failed-worker",
        @js.ConsumerConfig::new(filter="extended.failed", ack_wait_ms=100),
      )
      ignore(js.publish("extended.failed", b"fail"))
      let failed_control = @js.WorkControl::new()
      assert_true(
        try {
          failed_worker.consume(failed_control, ack_mode=@js.OnSuccess, _ => {
            raise @js.Invalid("expected handler failure")
          })
          false
        } catch {
          @js.Invalid("expected handler failure") => true
          _ => false
        },
      )
      assert_eq(failed_control.completed(), 0L)
      assert_eq(failed_control.active(), 0)
      @async.sleep(150)
      let retried = failed_worker.fetch()[0]
      assert_eq(@js.metadata(retried).deliveries, 2L)
      @js.acknowledge(c, retried)
      failed_worker.delete()
      let progress_worker = js.add_consumer(
        "MN_EXT",
        "progress-worker",
        @js.ConsumerConfig::new(filter="extended.failed", ack_wait_ms=200),
      )
      let progress_control = @js.WorkControl::new()
      let mut progress_started = false
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          progress_worker.consume(
            progress_control,
            ack_mode=@js.OnSuccess,
            progress_ms=30,
            msg => {
              progress_started = true
              assert_eq(msg.payload, b"fail")
              @async.sleep(550)
              progress_control.stop()
            },
          )
        })
        @async.with_timeout(2000, () => {
          while !progress_started {
            @async.sleep(5)
          }
        })
        @async.sleep(250)
        assert_true(progress_worker.fetch(expires_ms=150).is_empty())
      })
      assert_eq(progress_control.completed(), 1L)
      progress_worker.delete()
      js.delete_stream("MN_EXT")
      println(
        "extended JetStream: bounded async publishing, worker stop/drain, ordered replay, push heartbeat, typed config and pagination PASS",
      )
      js.delete_stream("KV_MN_CACHE") catch {
        _ => ()
      }
      let bucket = @kv.create(js, "MN_CACHE")
      ignore(bucket.put("config.a", b"one"))
      let cache = bucket.config_cache(pattern="config.>")
      assert_true(cache.ready())
      assert_true(
        cache.next() is @kv.Snapshot(entries) && entries.length() == 1,
      )
      assert_eq(cache.get("config.a").unwrap().value, b"one")
      ignore(bucket.put("config.a", b"two"))
      assert_true(cache.next() is @kv.Updated(entry) && entry.value == b"two")
      ignore(bucket.delete("config.a"))
      ignore(cache.next())
      assert_true(cache.get("config.a") is None)
      let cache_consumers = js.all_consumers("KV_MN_CACHE")
      for config in cache_consumers {
        js.consumer("KV_MN_CACHE", @js.text_field(config, "name")).delete()
      }
      assert_true(cache.next() is @kv.Snapshot(entries) && entries.is_empty())
      cache.close()
      assert_true(!cache.ready())
      let empty = bucket.config_cache()
      assert_true(empty.next() is @kv.Snapshot(entries) && entries.is_empty())
      empty.close()
      ignore(bucket.put("limit.a", b"a"))
      ignore(bucket.put("limit.b", b"b"))
      assert_true(
        try {
          ignore(bucket.config_cache(max_keys=1))
          false
        } catch {
          @js.Invalid("cache key limit exceeded") => true
          _ => false
        },
      )
      assert_eq(js.all_consumers("KV_MN_CACHE").length(), 0)
      bucket.destroy()
      js.delete_stream("KV_MN_TTL") catch {
        _ => ()
      }
      let ttl = @kv.create(js, "MN_TTL", ttl_ms=1200)
      ignore(ttl.put("old", b"old value"))
      let live = ttl.config_cache(max_keys=1)
      assert_true(live.next() is @kv.Snapshot(entries) && entries.length() == 1)
      @async.sleep(1300)
      assert_true(live.snapshot().is_empty())
      assert_true(live.get("old") is None)
      ignore(ttl.put("new", b"new value"))
      assert_true(live.next() is @kv.Updated(entry) && entry.key == "new")
      live.close()
      ttl.destroy()
      js.delete_stream("KV_MN_TTL_MARKER") catch {
        _ => ()
      }
      let marked = @kv.create(js, "MN_TTL_MARKER", ttl_ms=1200)
      guard js.stream_info("KV_MN_TTL_MARKER")
        is { "config": Object(config), .. } else {
        raise @js.Invalid("TTL marker stream config")
      }
      config["allow_msg_ttl"] = true
      config["subject_delete_marker_ttl"] = @js.integer(5000000000L)
      ignore(js.update_stream("KV_MN_TTL_MARKER", Json::object(config)))
      let marked_cache = marked.config_cache()
      ignore(marked_cache.next())
      ignore(marked.put("key", b"temporary"))
      assert_true(
        marked_cache.next() is @kv.Updated(entry) && entry.operation == "PUT",
      )
      assert_true(
        marked_cache.next() is @kv.Updated(entry) && entry.operation == "EXPIRE",
      )
      assert_true(marked_cache.get("key") is None)
      assert_eq(marked.get("key").operation, "EXPIRE")
      ignore(marked.create("key", b"recreated"))
      marked_cache.close()
      marked.destroy()
      println(
        "config cache: initial snapshot completion, updates, tombstones, empty snapshot and close PASS",
      )
      js.delete_stream("OBJ_MN_LINKS") catch {
        _ => ()
      }
      let objects = @objects.create(js, "MN_LINKS")
      let watch = objects.watch()
      ignore(objects.put("target", b"content"))
      assert_eq(watch.next().name, "target")
      ignore(
        objects.update_metadata("target", "description", metadata={
          "kind": "test",
        }),
      )
      assert_true(
        watch.next().raw
        is {
          "description": "description",
          "metadata": { "kind": "test", .. },
          ..
        },
      )
      ignore(objects.add_link("alias", "MN_LINKS", "target"))
      assert_eq(watch.next().name, "alias")
      assert_eq(objects.get("alias"), b"content")
      ignore(objects.add_bucket_link("bucket-alias", "MN_LINKS"))
      assert_eq(objects.linked_bucket("bucket-alias").name, "MN_LINKS")
      watch.close()
      objects.seal()
      assert_true(objects.status() is { "config": { "sealed": True, .. }, .. })
      assert_true(
        try {
          ignore(objects.put("forbidden", b"no"))
          false
        } catch {
          @js.Api(_, _, _) => true
          _ => false
        },
      )
      objects.destroy()
      println(
        "object management: metadata watch, metadata updates, object/bucket links and sealing PASS",
      )
      let echo = @micro.Endpoint::new("echo", "extended.echo", msg => {
        @micro.Response::ok(msg.payload)
      })
      let failure = @micro.Endpoint::new("failure", "extended.failure", _ => {
        @micro.Response::error(409, "conflict")
      })
      @micro.with_service(c, "MoonEcho", "1.0.0", [echo, failure], service => {
        assert_eq(c.request("extended.echo", b"hello").payload, b"hello")
        let failed = c.request("extended.failure", b"")
        assert_eq(failed.headers.get("Nats-Service-Error-Code"), Some("409"))
        for op in ["PING", "INFO", "STATS"] {
          for
            subject in [
              "$SRV." + op,
              "$SRV." + op + ".MoonEcho",
              "$SRV." + op + ".MoonEcho." + service.id,
            ] {
            let json = @js.decode_json(c.request(subject, b"").payload)
            assert_true(json is { "name": "MoonEcho", "version": "1.0.0", .. })
          }
        }
        assert_true(
          service.info(operation="STATS") is { "endpoints": Array(entries), .. } &&
          entries.length() == 2,
        )
      })
      assert_true(
        try {
          ignore(c.request("extended.echo", b""))
          false
        } catch {
          @client.NoResponders => true
          _ => false
        },
      )
      let mut started = false
      let mut completed = false
      let slow_endpoint = @micro.Endpoint::new("slow", "extended.slow", _ => {
        started = true
        @async.sleep(50)
        completed = true
        @micro.Response::ok(b"finished")
      })
      @async.with_task_group(group => {
        @micro.with_service(c, "SlowService", "1.0.0", [slow_endpoint], service => {
          group.spawn_bg(() => {
            assert_eq(c.request("extended.slow", b"").payload, b"finished")
          })
          while !started {
            @async.sleep(1)
          }
          service.stop()
        })
        assert_true(completed)
      })
      assert_true(
        c.statistics().prometheus().contains("moonnats_requests_total"),
      )
      println(
        "micro: endpoint requests, standard errors, nine discovery/statistics subjects, graceful stop and metrics PASS",
      )
    },
  ) catch {
    @js.Api(code, err, description) => {
      println(
        "API failure: " +
        code.to_string() +
        "/" +
        err.to_string() +
        " " +
        description,
      )
      raise @js.Invalid(description)
    }
    @js.Status(code, description) => {
      println("STATUS: " + code.to_string() + " " + description)
      raise @js.Invalid(description)
    }
    e => raise e
  }
}

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
      let sub = c.subscribe("integration.*")
      c.flush()
      c.publish(
        "integration.binary",
        b"\x00\xff",
        headers=@wire.Headers::new(entries=[("Test", "one"), ("Test", "two")]),
      )
      let msg = sub.next()
      assert_eq(msg.payload, b"\x00\xff")
      assert_eq(msg.headers.get_all("Test"), ["one", "two"])
      @async.with_task_group(g => {
        let responder = c.subscribe("integration.rpc", max_messages=1)
        c.flush()
        g.spawn_bg(() => {
          let request = responder.next()
          c.reply(request, b"response")
        })
        assert_eq(c.request("integration.rpc", b"request").payload, b"response")
      })
      assert_true(
        try {
          ignore(c.request("no.such.responder", b""))
          false
        } catch {
          @client.NoResponders => true
          _ => false
        },
      )
      let js = @js.JetStream::new(c)
      let name = "MN_INTEGRATION"
      js.delete_stream(name) catch {
        _ => ()
      }
      ignore(js.create_stream(name, ["integration.jobs"], storage="memory"))
      let ack = js.publish("integration.jobs", b"job", message_id="id-1")
      let duplicate = js.publish("integration.jobs", b"job", message_id="id-1")
      assert_true(duplicate.duplicate)
      assert_eq(duplicate.sequence, ack.sequence)
      let consumer = js.create_consumer(
        name,
        "worker",
        filter="integration.jobs",
        ack_wait_ms=200,
      )
      let messages = consumer.fetch(batch=1)
      assert_eq(messages.length(), 1)
      assert_eq(messages[0].payload, b"job")
      assert_eq(@js.metadata(messages[0]).stream_sequence, ack.sequence)
      @js.acknowledge(c, messages[0], kind="nak", delay_ms=100)
      let again = consumer.fetch(batch=1)
      assert_eq(@js.metadata(again[0]).deliveries, 2L)
      @js.acknowledge(c, again[0], sync=true)
      assert_eq(consumer.fetch(batch=1, expires_ms=100).length(), 0)
      consumer.delete()
      js.delete_stream(name)
      println(
        "jetstream: management, dedup, pull, metadata, delayed NAK, ACK sync PASS",
      )
      js.delete_stream("KV_MN_TEST") catch {
        _ => ()
      }
      let kv = @kv.create(js, "MN_TEST", history=4)
      let revision = kv.create("config.service", b"v1")
      assert_eq(kv.get("config.service").value, b"v1")
      let watch = kv.watch(pattern="config.>", updates_only=true)
      let revision2 = kv.update("config.service", b"v2", revision)
      assert_eq(watch.next().revision, revision2)
      watch.close()
      assert_true(
        try {
          ignore(kv.update("config.service", b"bad", revision))
          false
        } catch {
          @js.Api(_, _, _) => true
          _ => false
        },
      )
      assert_eq(kv.history("config.service").length(), 2)
      ignore(kv.delete("config.service"))
      assert_eq(kv.get("config.service").operation, "DEL")
      ignore(kv.create("config.service", b"after-delete"))
      assert_eq(kv.get("config.service").value, b"after-delete")
      ignore(kv.delete("config.service", purge=true))
      assert_eq(kv.history("config.service").length(), 1)
      kv.destroy()
      println("kv: CAS, history, watch and tombstones PASS")
      js.delete_stream("OBJ_MN_OBJECTS") catch {
        _ => ()
      }
      let objects = @objects.create(js, "MN_OBJECTS")
      let bytes = Bytes::makei(300000, i => (i % 251).to_byte())
      let stored = objects.put("files/binary.dat", bytes, chunk_size=65536)
      assert_eq(stored.chunks, 5)
      assert_eq(objects.get("files/binary.dat"), bytes)
      ignore(objects.put("empty", b""))
      assert_eq(objects.get("empty"), b"")
      assert_eq(objects.list().length(), 2)
      objects.delete("empty")
      assert_true(objects.get_info("empty").deleted)
      ignore(objects.put("files/binary.dat", b"replacement"))
      assert_eq(objects.get("files/binary.dat"), b"replacement")
      ignore(objects.put("tamper", b"correct"))
      let tampered = objects.get_info("tamper")
      let chunk_subject = "$O.MN_OBJECTS.C." + tampered.nuid
      js.purge_stream("OBJ_MN_OBJECTS", filter=chunk_subject)
      ignore(js.publish(chunk_subject, b"corrupt"))
      assert_true(
        try {
          ignore(objects.get("tamper"))
          false
        } catch {
          @js.Invalid("object integrity mismatch") => true
          _ => false
        },
      )
      objects.destroy()
      println(
        "objects: chunked binary upload, SHA-256, listing, empty object and deletion PASS",
      )
      c.unsubscribe(sub)
      c.flush()
      println(
        "core: binary headers, publish/subscribe, request/reply, no responders PASS",
      )
    },
  )
}

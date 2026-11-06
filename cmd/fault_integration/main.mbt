///|
async fn main {
  let mut lame_duck = false
  @client.with_client(
    @client.Options::new(
      servers=["nats://127.0.0.1:14229"],
      heartbeat_ms=0,
      reconnect_delay_ms=10,
      reconnect_jitter_ms=5,
      on_event=event => if event is @client.LameDuck { lame_duck = true },
    ),
    c => {
      let sub = c.subscribe("fault.fragmented")
      c.flush()
      let payload = Bytes::makei(65536, i => (i % 251).to_byte())
      c.publish(sub.subject, payload)
      assert_eq(sub.next().payload, payload)
      @stdio.stderr.text.write("LDM\n")
      @async.with_timeout(5000, () => {
        while c.statistics().reconnects == 0L {
          @async.sleep(5)
        }
      })
      assert_true(lame_duck)
      c.flush()
      c.publish(sub.subject, b"restored")
      assert_eq(sub.next().payload, b"restored")
      c.unsubscribe(sub)
      println(
        "fault proxy: fragmented binary delivery, Lame Duck notification and proactive reconnect PASS",
      )
    },
  )
}

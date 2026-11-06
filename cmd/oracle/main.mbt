///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=["nats://127.0.0.1:14222"], heartbeat_ms=0),
    c => {
      let js = @js.JetStream::new(c)
      if @env.args().contains("--verify") {
        let kv = @kv.open(js, "MN_ORACLE")
        assert_eq(kv.get("python.value").value, b"from-python")
        let objects = @objects.open(js, "MN_ORACLE")
        assert_eq(
          objects.get("python.bin"),
          Bytes::makei(200000, i => (i % 199).to_byte()),
        )
        assert_eq(objects.get("python-empty"), b"")
        kv.destroy()
        objects.destroy()
        println("oracle: MoonNATS read nats-py KV and Object Store PASS")
      } else {
        for name in ["KV_MN_ORACLE", "OBJ_MN_ORACLE"] {
          js.delete_stream(name) catch {
            _ => ()
          }
        }
        let kv = @kv.create(js, "MN_ORACLE", history=4)
        ignore(kv.put("moon.value", b"from-moonbit"))
        let objects = @objects.create(js, "MN_ORACLE")
        ignore(
          objects.put(
            "moon.bin",
            Bytes::makei(300000, i => (i % 251).to_byte()),
            chunk_size=65536,
          ),
        )
        ignore(objects.put("moon-empty", b""))
        println("oracle fixtures ready")
      }
    },
  )
}

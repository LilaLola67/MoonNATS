///|
struct Payload {
  id : Int32
  label : String
} derive(ToJson, @json.FromJson)

///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=["ws://127.0.0.1:18080"], heartbeat_ms=0),
    c => {
      let sub = c.subscribe("websocket.typed", max_messages=1)
      c.flush()
      let codec : @typed.Codec[Payload] = @typed.json_codec()
      @typed.publish(
        c,
        "websocket.typed",
        { id: 42, label: "MoonNATS", },
        codec,
      )
      let result = @typed.next(sub, codec)
      assert_eq(result.id, 42)
      assert_eq(result.label, "MoonNATS")
      println("websocket and typed JSON roundtrip PASS")
    },
  )
}

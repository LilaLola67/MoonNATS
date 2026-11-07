///|
struct Payload {
  id : Int32
  label : String
} derive(ToJson, @json.FromJson)

///|
async fn main {
  let mut credentials_called = false
  assert_true(
    try {
      @client.with_client(
        @client.Options::new(
          servers=["ws://127.0.0.1:18080"],
          require_tls=true,
          credentials=_ => {
            credentials_called = true
            @wire.ConnectOptions::new()
          },
        ),
        _ => (),
      )
      false
    } catch {
      @client.InvalidOption(_) => true
      _ => false
    },
  )
  assert_true(!credentials_called)
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

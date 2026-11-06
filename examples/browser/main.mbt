///|
extern "js" fn server_url() -> String =
  #| () => document.getElementById('server').value

///|
extern "js" fn result(text : String) -> Unit =
  #| text => { document.getElementById('result').textContent=text; }

///|
async fn main {
  result("Connecting…")
  @client.with_client(
    @client.Options::new(servers=[server_url()], heartbeat_ms=0),
    c => {
      let subject = "example.browser." + c.new_inbox()
      let sub = c.subscribe(subject, max_messages=1)
      c.flush()
      c.publish(subject, @utf8.encode("Hello from MoonBit WebSocket"))
      let msg = sub.next()
      result(
        "Connected → published → received → closed\n" +
        @utf8.decode(msg.payload),
      )
    },
  ) catch {
    error => result("Connection failed: " + error.to_string())
  }
}

///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=[
      @env.get_env_var("MOONNATS_URL").unwrap_or("nats://127.0.0.1:4222"),
    ]),
    c => {
      let sub = c.subscribe("example.greeting", max_messages=1)
      c.flush()
      c.publish("example.greeting", @utf8.encode("Hello, MoonNATS!"))
      println(@utf8.decode(sub.next().payload))
    },
  )
}

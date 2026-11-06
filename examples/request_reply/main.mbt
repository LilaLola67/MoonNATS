///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=[
      @env.get_env_var("MOONNATS_URL").unwrap_or("nats://127.0.0.1:4222"),
    ]),
    c => {
      let requests = c.subscribe("example.echo", max_messages=1)
      c.flush()
      @async.with_task_group(group => {
        group.spawn_bg(() => {
          requests.process_next(msg => c.reply(msg, msg.payload))
        })
        let response = c.request("example.echo", b"echo this")
        println(@utf8.decode(response.payload))
      })
    },
  )
}

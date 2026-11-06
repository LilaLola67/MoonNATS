///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=[
      @env.get_env_var("MOONNATS_URL").unwrap_or("nats://127.0.0.1:4222"),
    ]),
    client => {
      let echo = @svc.Endpoint::new("echo", "example.echo", request => {
        @svc.Response::ok(request.payload)
      })
      @svc.with_service(client, "MoonEcho", "1.0.0", [echo], service => {
        assert_eq(client.request("example.echo", b"hello").payload, b"hello")
        println(service.info().stringify())
        println(service.info(operation="STATS").stringify())
      })
    },
  )
}

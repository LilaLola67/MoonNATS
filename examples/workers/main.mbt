///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=[
      @env.get_env_var("MOONNATS_URL").unwrap_or("nats://127.0.0.1:4222"),
    ]),
    client => {
      let js = @js.JetStream::new(client)
      let name = "MN_WORKERS_" +
        client.new_inbox().replace_all(old=".", new="_")
      let subject = "example.jobs." + name
      ignore(
        js.add_stream(
          name,
          @js.StreamConfig::new([subject], storage=@js.Memory),
        ),
      )
      defer @async.protect_from_cancel(() => js.delete_stream(name))
      let consumer = js.add_consumer(
        name,
        "workers",
        @js.ConsumerConfig::new(filter=subject),
      )
      @js.with_publisher(js, max_pending=4, publisher => {
        for _ in 0..<10 {
          ignore(publisher.publish(subject, b"generate report"))
        }
      })
      let control = @js.WorkControl::new()
      let mut count = 0
      consumer.consume(control, concurrency=2, ack_mode=@js.OnSuccess, message => {
        assert_eq(message.payload, b"generate report")
        count += 1
        if count == 10 {
          control.stop()
        }
      })
      println("completed: " + control.completed().to_string())
    },
  )
}

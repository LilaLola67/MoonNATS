///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=[
      @env.get_env_var("MOONNATS_URL").unwrap_or("nats://127.0.0.1:4222"),
    ]),
    c => {
      let js = @js.JetStream::new(c)
      let name = "EXAMPLE_" + c.new_inbox().replace_all(old=".", new="_")
      ignore(js.create_stream(name, ["example.jobs." + name], storage="memory"))
      defer @async.protect_from_cancel(() => js.delete_stream(name))
      let consumer = js.create_consumer(
        name,
        "worker",
        filter="example.jobs." + name,
      )
      let ack = js.publish(
        "example.jobs." + name,
        b"generate report",
        message_id="example-job",
      )
      println("stored sequence: " + ack.sequence.to_string())
      for msg in consumer.fetch(batch=1) {
        println("task: " + @utf8.decode(msg.payload))
        @js.acknowledge(c, msg, sync=true)
      }
    },
  )
}

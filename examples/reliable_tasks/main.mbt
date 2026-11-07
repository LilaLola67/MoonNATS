///|
async fn main {
  @client.with_client(
    @client.Options::new(servers=[
      @env.get_env_var("MOONNATS_URL").unwrap_or("nats://127.0.0.1:4222"),
    ]),
    client => {
      let js = @js.JetStream::new(client)
      let stream = "ORDERS_" + client.new_inbox().replace_all(old=".", new="_")
      let subject = "example.orders." + stream
      ignore(js.add_stream(stream, @js.StreamConfig::new([subject])))
      defer @async.protect_from_cancel(() => js.delete_stream(stream))
      let receipts = @kv.create(js, stream, history=4)
      defer @async.protect_from_cancel(() => receipts.destroy())
      let worker = js.add_consumer(
        stream,
        "shipping",
        @js.ConsumerConfig::new(filter=subject, ack_wait_ms=1000, max_deliver=5),
      )
      let order = "order-42"
      let payload = b"ship order-42"
      let receipt = b"shipping record for order-42"
      ignore(
        js.publish_reliably(
          subject,
          payload,
          message_id=order,
          expected_stream=stream,
        ),
      )
      ignore(js.publish(subject, b"invalid order", message_id="invalid-order"))
      let control = @js.WorkControl::new()
      let mut rejected = 0
      let mut deliveries = 0L
      @async.with_timeout(8000, () => {
        worker.consume_reliably(
          control,
          prefetch=4,
          poll_ms=100,
          progress_ms=300,
          (message, meta) => {
            if message.payload != payload {
              rejected += 1
              return @js.Terminate
            }
            // Creating the receipt is the business effect in this example.
            // Compare-and-set gives one durable record even when the job is redelivered.
            ignore(receipts.put(order, receipt, expected_revision=0L)) catch {
              @js.Api(_, 10071, _) =>
                assert_eq(receipts.get(order).value, receipt)
              error => raise error
            }
            deliveries = meta.deliveries
            if meta.deliveries == 1L {
              // Simulate an interruption after persisting the receipt and before ACK.
              return @js.RetryAfter(80)
            }
            control.stop()
            @js.Confirm
          },
        )
      })
      assert_eq(rejected, 1)
      assert_eq(deliveries, 2L)
      assert_eq(control.completed(), 1L)
      assert_eq(receipts.history(order).length(), 1)
      println("Order delivered twice, one durable receipt, ACK confirmed")
    },
  )
}

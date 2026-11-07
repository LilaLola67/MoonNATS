///|
async fn crash_worker(js : @js.JetStream, recovering : Bool) -> Unit {
  let stream = "MN_WORKER_CRASH"
  let receipts = @kv.open(js, stream)
  let worker = js.consumer(stream, "worker")
  let control = @js.WorkControl::new()
  @async.with_timeout(15000, () => {
    worker.consume_reliably(control, (message, meta) => {
      assert_eq(message.payload, b"order-42")
      if recovering {
        assert_eq(meta.deliveries, 2L)
        let exists = try
          receipts.put("order-42", b"shipped", expected_revision=0L)
        catch {
          @js.Api(_, 10071, _) => true
          error => raise error
        } noraise {
          _ => false
        }
        assert_true(exists)
        assert_eq(receipts.get("order-42").value, b"shipped")
        control.stop()
      } else {
        assert_eq(meta.deliveries, 1L)
        ignore(receipts.put("order-42", b"shipped", expected_revision=0L))
        @stdio.stderr.text.write("RECEIPT_COMMITTED_BEFORE_ACK\n")
        // The test supervisor kills this process without letting it ACK.
        @async.sleep(30000)
      }
      @js.Confirm
    })
  })
  assert_true(recovering)
  assert_eq(control.completed(), 1L)
}

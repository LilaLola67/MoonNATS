///|
fn main raise {
  let decoder = @nats.Decoder::new()
  println(@nats.subscribe("orders.*", 1))
  println(@nats.publish("orders.created", b"order-42", reply_to="replies.42"))
  println(decoder.feed(b"MSG orders.created 1 8\r\norder").length())
  println(decoder.feed(b"-42\r\nPING\r\n").length())
  decoder.finish()
}

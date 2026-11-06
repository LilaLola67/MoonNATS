///|
pub struct PushReader {
  consumer : Consumer
  priv subscription : @client.Subscription
  priv lock : @async.Mutex
  priv heartbeat_ms : Int32
}

///|
pub async fn JetStream::push_consumer(
  self : JetStream,
  stream : String,
  name : String,
  filter~ : String = ">",
  heartbeat_ms~ : Int32 = 1000,
  ack_policy~ : AckPolicy = Explicit,
  deliver_policy~ : String = "all",
) -> PushReader {
  guard heartbeat_ms >= 100 && heartbeat_ms <= 60000 else {
    raise Invalid("heartbeat interval")
  }
  let subject = self.client.new_inbox()
  let subscription = self.client.subscribe(subject)
  errdefer @async.protect_from_cancel(() => {
    self.client.unsubscribe(subscription) catch {
      _ => ()
    }
  })
  let config = ConsumerConfig::new(filter~, ack_policy~).to_json()
  guard config is Object(m) else { raise Invalid("push config") }
  guard ["all", "new", "last_per_subject"].contains(deliver_policy) else {
    raise Invalid("push delivery policy")
  }
  m["deliver_policy"] = Json(deliver_policy)
  m["deliver_subject"] = Json(subject)
  m["flow_control"] = true
  m["idle_heartbeat"] = integer(heartbeat_ms.to_int64() * 1000000L)
  let consumer = self.create_consumer_config(stream, name, Json::object(m))
  { consumer, subscription, lock: @async.Mutex(), heartbeat_ms, }
}

///|
pub async fn PushReader::next(
  self : PushReader,
  timeout_ms~ : Int32 = 5000,
) -> @client.Received {
  guard timeout_ms > 0 else { raise Invalid("push timeout") }
  @async.with_timeout(timeout_ms, () => {
    self.lock.acquire()
    defer self.lock.release()
    while true {
      let msg = self.subscription.next(timeout_ms=self.heartbeat_ms * 3)
      match msg.headers.status {
        Some(100) => {
          if msg.reply_to is Some(reply) {
            self.consumer.js.client.publish(reply, b"")
          }
          if msg.headers.get("Nats-Consumer-Stalled") is Some(reply) {
            self.consumer.js.client.publish(reply, b"")
          }
        }
        Some(code) => raise Status(code, msg.headers.description)
        None => return msg
      }
    }
    raise Invalid("unreachable")
  })
}

///|
pub async fn PushReader::close(self : PushReader) -> Unit {
  defer @async.protect_from_cancel(() => {
    self.consumer.js.client.unsubscribe(self.subscription) catch {
      _ => ()
    }
  })
  self.consumer.delete()
}

///|
pub struct OrderedReader {
  priv js : JetStream
  priv stream : String
  priv filter : String
  priv mut consumer : Consumer
  priv mut last_stream : Int64
  priv mut delivered : Int64
  priv mut generation : Int64
  priv mut closed : Bool
  priv lock : @async.Mutex
}

///|
async fn JetStream::ordered_at(
  self : JetStream,
  stream : String,
  filter : String,
  sequence : Int64,
) -> Consumer {
  let name = "MN_" + self.client.new_inbox().replace_all(old=".", new="_")
  let config = ConsumerConfig::new(
    filter~,
    ack_policy=NoAck,
    start_sequence=sequence,
  ).to_json()
  guard config is Object(m) else { raise Invalid("ordered config") }
  m["inactive_threshold"] = integer(300000000000L)
  self.create_consumer_config(stream, name, Json::object(m))
}

///|
pub async fn JetStream::ordered_consumer(
  self : JetStream,
  stream : String,
  filter~ : String = ">",
  start_sequence~ : Int64 = 1L,
) -> OrderedReader {
  let consumer = self.ordered_at(stream, filter, start_sequence)
  {
    js: self,
    stream,
    filter,
    consumer,
    last_stream: start_sequence - 1L,
    delivered: 0L,
    generation: self.client.statistics().reconnects,
    closed: false,
    lock: @async.Mutex(),
  }
}

///|
async fn OrderedReader::reset(self : OrderedReader) -> Unit {
  self.consumer.delete() catch {
    _ => ()
  }
  self.consumer = self.js.ordered_at(
    self.stream,
    self.filter,
    self.last_stream + 1L,
  )
  self.delivered = 0L
  self.generation = self.js.client.statistics().reconnects
}

///|
/// Ordered reads recover gaps from the last delivered stream sequence.
/// They are for replay/inspection, not acknowledged work distribution.
pub async fn OrderedReader::next(
  self : OrderedReader,
  timeout_ms~ : Int32 = 5000,
) -> @client.Received {
  guard !self.closed && timeout_ms > 0 else {
    raise Invalid("ordered reader closed or invalid timeout")
  }
  @async.with_timeout(timeout_ms, () => {
    self.lock.acquire()
    defer self.lock.release()
    while true {
      if self.generation != self.js.client.statistics().reconnects {
        self.reset()
      }
      let batch = self.consumer.fetch(expires_ms=250) catch {
        @client.Disconnected
        | Api(404, _, _)
        | Status(404, _)
        | Status(409, _)
        | Status(503, _) => {
          self.reset()
          continue
        }
        e => raise e
      }
      if batch.is_empty() {
        continue
      }
      let msg = batch[0]
      let meta = metadata(msg)
      if meta.consumer_sequence != self.delivered + 1L ||
        meta.stream_sequence <= self.last_stream {
        self.reset()
        continue
      }
      self.delivered = meta.consumer_sequence
      self.last_stream = meta.stream_sequence
      return msg
    }
    raise Invalid("unreachable")
  })
}

///|
pub fn OrderedReader::sequence(self : OrderedReader) -> Int64 {
  self.last_stream
}

///|
pub async fn OrderedReader::close(self : OrderedReader) -> Unit {
  if !self.closed {
    self.closed = true
    self.consumer.delete()
  }
}

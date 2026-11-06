///|
pub struct Publisher {
  priv js : JetStream
  priv group : @async.TaskGroup
  priv slots : @async.Semaphore
  priv max_bytes : Int32
  priv mut bytes : Int32
  priv mut pending : Int32
  priv mut failure : Error?
  priv mut count : Int64
  priv mut closed : Bool
  priv mut elapsed_ms : Int64
}

///|
pub struct PublishFuture {
  priv task : @async.Task[PubAck]
}

///|
pub async fn PublishFuture::wait(self : PublishFuture) -> PubAck {
  self.task.wait()
}

///|
pub fn Publisher::pending(self : Publisher) -> Int32 {
  self.pending
}

///|
pub fn Publisher::pending_bytes(self : Publisher) -> Int32 {
  self.bytes
}

///|
pub fn Publisher::ack_metrics(self : Publisher) -> (Int64, Int64) {
  (self.count, self.elapsed_ms)
}

///|
pub async fn Publisher::publish(
  self : Publisher,
  subject : String,
  payload : Bytes,
  message_id? : String,
) -> PublishFuture {
  guard !self.closed else { raise Invalid("publisher scope closed") }
  guard payload.length() <= self.max_bytes else {
    raise Invalid("publish byte budget")
  }
  self.slots.acquire()
  if self.closed {
    self.slots.release()
    raise Invalid("publisher scope closed")
  }
  if payload.length() > self.max_bytes - self.bytes {
    self.slots.release()
    raise Invalid("pending publish byte budget exhausted")
  }
  self.pending += 1
  self.bytes += payload.length()
  let task = self.group.spawn(allow_failure=true, no_wait=true, () => {
    defer {
      self.pending -= 1
      self.bytes -= payload.length()
      self.slots.release()
    }
    let started = @async.now()
    let result = Some(self.js.publish(subject, payload, message_id?)) catch {
      e => {
        self.failure = Some(e)
        None
      }
    }
    guard result is Some(ack) else { raise self.failure.unwrap() }
    self.count += 1L
    self.elapsed_ms += @async.now() - started
    ack
  })
  { task, }
}

///|
pub async fn Publisher::flush(self : Publisher) -> Unit {
  while self.pending > 0 {
    @async.sleep(1)
  }
  if self.failure is Some(e) {
    raise e
  }
}

///|
/// The scoped publisher waits for all acks and surfaces even unobserved failures.
pub async fn[X] with_publisher(
  js : JetStream,
  f : async (Publisher) -> X,
  max_pending~ : Int32 = 256,
  max_bytes~ : Int32 = 8388608,
) -> X {
  guard max_pending > 0 && max_pending <= 4096 && max_bytes > 0 else {
    raise Invalid("publisher limits")
  }
  let mut result = None
  @async.with_task_group(group => {
    let publisher : Publisher = {
      js,
      group,
      slots: @async.Semaphore(max_pending),
      max_bytes,
      bytes: 0,
      pending: 0,
      failure: None,
      count: 0L,
      elapsed_ms: 0L,
      closed: false,
    }
    defer {
      publisher.closed = true
    }
    result = Some(f(publisher))
    publisher.flush()
  })
  result.unwrap()
}

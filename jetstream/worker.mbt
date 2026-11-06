///|
pub(all) enum AckMode {
  Manual
  OnSuccess
}

///|
pub struct WorkControl {
  priv mut stopped : Bool
  priv mut active : Int32
  priv mut completed : Int64
}

///|
pub fn WorkControl::new() -> WorkControl {
  { stopped: false, active: 0, completed: 0L, }
}

///|
pub fn WorkControl::stop(self : WorkControl) -> Unit {
  self.stopped = true
}

///|
pub fn WorkControl::active(self : WorkControl) -> Int32 {
  self.active
}

///|
pub fn WorkControl::completed(self : WorkControl) -> Int64 {
  self.completed
}

///|
/// Stop prevents new fetches; already fetched messages finish before returning.
/// Handler errors propagate. OnSuccess never acknowledges a failed handler.
pub async fn Consumer::consume(
  self : Consumer,
  control : WorkControl,
  handler : async (@client.Received) -> Unit,
  concurrency~ : Int32 = 1,
  ack_mode~ : AckMode = Manual,
  poll_ms~ : Int32 = 250,
) -> Unit {
  guard concurrency > 0 && concurrency <= 256 && poll_ms > 0 && poll_ms <= 1000 else {
    raise Invalid("worker limits")
  }
  @async.with_task_group(group => {
    for _ in 0..<concurrency {
      group.spawn_bg(() => {
        while !control.stopped {
          let batch = self.fetch(expires_ms=poll_ms)
          for msg in batch {
            control.active += 1
            defer {
              control.active -= 1
            }
            handler(msg)
            if ack_mode is OnSuccess {
              acknowledge(self.js.client, msg)
            }
            control.completed += 1L
          }
        }
      })
    }
  })
}

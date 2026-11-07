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
  recovery_timeout_ms~ : Int32 = 30000,
  progress_ms~ : Int32 = 0,
) -> Unit {
  guard concurrency > 0 &&
    concurrency <= 256 &&
    poll_ms > 0 &&
    poll_ms <= 1000 &&
    recovery_timeout_ms >= 0 &&
    progress_ms >= 0 else {
    raise Invalid("worker limits")
  }
  guard progress_ms == 0 || ack_mode is OnSuccess else {
    raise Invalid("automatic progress requires OnSuccess")
  }
  @async.with_task_group(group => {
    for _ in 0..<concurrency {
      group.spawn_bg(() => {
        while !control.stopped {
          let batch = self.worker_fetch(control, poll_ms, recovery_timeout_ms)
          for msg in batch {
            control.active += 1
            defer {
              control.active -= 1
            }
            if progress_ms == 0 {
              handler(msg)
            } else {
              @async.with_task_group(handler_group => {
                let progress = handler_group.spawn(no_wait=true, () => {
                  while true {
                    @async.sleep(progress_ms)
                    acknowledge(self.js.client, msg, kind="progress") catch {
                      @client.Disconnected | @client.PublishUncertain => ()
                      e => raise e
                    }
                  }
                })
                defer progress.cancel()
                handler(msg)
              })
            }
            if ack_mode is OnSuccess {
              acknowledge(self.js.client, msg) catch {
                @client.Disconnected | @client.PublishUncertain if recovery_timeout_ms >
                  0 => continue
                e => raise e
              }
            }
            control.completed += 1L
          }
        }
      })
    }
  })
}

///|
async fn Consumer::worker_fetch(
  self : Consumer,
  control : WorkControl,
  poll_ms : Int32,
  recovery_timeout_ms : Int32,
) -> Array[@client.Received] {
  @async.with_timeout(poll_ms + 1500, () => self.fetch(expires_ms=poll_ms)) catch {
    error if recovery_timeout_ms > 0 && recoverable_pull_error(error) =>
      @async.with_timeout(recovery_timeout_ms, () => {
        let mut retry_delay_ms = 25
        while !control.stopped {
          self.js.client.wait_connected(timeout_ms=100) catch {
            @async.TimeoutError => continue
            e => raise e
          }
          if control.stopped {
            break
          }
          try self.fetch(expires_ms=poll_ms) catch {
            error if recoverable_pull_error(error) => {
              @async.sleep(retry_delay_ms)
              retry_delay_ms = (retry_delay_ms * 2).min(500)
            }
            e => raise e
          } noraise {
            batch => return batch
          }
        }
        []
      })
    e => raise e
  }
}

///|
fn recoverable_pull_error(error : Error) -> Bool {
  match error {
    @client.Disconnected | @client.PublishUncertain | @async.TimeoutError =>
      true
    // During a graceful shutdown NATS can release an in-flight pull before
    // closing TCP. It uses the same status as deleting the consumer. Retry
    // against the existing durable only, within the caller's recovery budget.
    Status(409, "Consumer Deleted")
    | Status(409, "Leadership Change")
    | Status(503, _) => true
    _ => false
  }
}

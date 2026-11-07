///|
pub(all) enum AckMode {
  Manual
  OnSuccess
}

///|
pub struct WorkControl {
  priv stop_signal : @async.Semaphore
  priv mut stopped_at : Int64?
  priv mut active : Int32
  priv mut completed : Int64
  priv mut uncertain : Int64
  priv mut peak_messages : Int32
  priv mut peak_bytes : Int32
}

///|
pub fn WorkControl::new() -> WorkControl {
  {
    stop_signal: @async.Semaphore(1, initial_value=0),
    stopped_at: None,
    active: 0,
    completed: 0L,
    uncertain: 0L,
    peak_messages: 0,
    peak_bytes: 0,
  }
}

///|
pub fn WorkControl::stop(self : WorkControl) -> Unit {
  if self.stopped_at is None {
    self.stopped_at = Some(@async.now())
    self.stop_signal.release()
  }
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
/// Count dispositions with an unconfirmed outcome from managed consumption.
pub fn WorkControl::uncertain(self : WorkControl) -> Int64 {
  self.uncertain
}

///|
/// Largest admitted message count and byte total observed during managed consumption.
pub fn WorkControl::prefetch_peaks(self : WorkControl) -> (Int32, Int32) {
  (self.peak_messages, self.peak_bytes)
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
        while control.stopped_at is None {
          let batch = self.worker_fetch(control, poll_ms, recovery_timeout_ms)
          for msg in batch {
            control.active += 1
            defer {
              control.active -= 1
            }
            with_work_progress(self.js.client, msg, progress_ms, () => {
              handler(msg)
            })
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
        while control.stopped_at is None {
          self.js.client.wait_connected(timeout_ms=100) catch {
            @async.TimeoutError => continue
            e => raise e
          }
          if control.stopped_at is Some(_) {
            break
          }
          try self.fetch(expires_ms=poll_ms) catch {
            error if recoverable_pull_error(error) => {
              @async.sleep(retry_delay_ms)
              retry_delay_ms = (retry_delay_ms * 2).min(500)
            }
            e => raise e
          } noraise {
            messages => return messages
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
    // A broker can release an in-flight pull before closing the connection.
    // Retry against the existing durable only, within the recovery budget.
    Status(409, "Server Shutdown")
    | Status(409, "Consumer Deleted")
    | Status(409, "Leadership Change")
    | Status(503, _) => true
    _ => false
  }
}

///|
async fn[T] with_work_progress(
  client : @client.Client,
  message : @client.Received,
  interval_ms : Int32,
  run : async () -> T,
) -> T {
  if interval_ms == 0 {
    return run()
  }
  let mut result = None
  @async.with_task_group(group => {
    let progress = group.spawn(no_wait=true, () => {
      while true {
        @async.sleep(interval_ms)
        acknowledge(client, message, kind="progress") catch {
          @client.Disconnected | @client.PublishUncertain => ()
          e => raise e
        }
      }
    })
    defer progress.cancel()
    result = Some(run())
  })
  result.unwrap()
}

///|
/// A handler's decision for one delivery on an explicit-ACK pull consumer.
pub(all) enum WorkOutcome {
  Confirm
  RetryAfter(Int32)
  Terminate
}

///|
/// A disposition may have reached the server before the connection or reply failed.
pub(all) struct UncertainDisposition {
  message : Metadata
  outcome : WorkOutcome
  cause : Error
}

///|
/// Consume using bounded per-worker pull batches and server-confirmed ACKs.
/// Stopping leaves prefetched but unstarted deliveries unacknowledged.
pub async fn Consumer::consume_reliably(
  self : Consumer,
  control : WorkControl,
  handler : async (@client.Received, Metadata) -> WorkOutcome,
  concurrency~ : Int32 = 1,
  prefetch~ : Int32 = 8,
  prefetch_bytes~ : Int32 = 8388608,
  poll_ms~ : Int32 = 250,
  recovery_timeout_ms~ : Int32 = 30000,
  progress_ms~ : Int32 = 0,
  ack_timeout_ms~ : Int32 = 1000,
  drain_timeout_ms~ : Int32 = 5000,
  on_uncertain~ : (UncertainDisposition) -> Unit = _ => (),
) -> Unit {
  guard concurrency > 0 &&
    concurrency <= 256 &&
    prefetch >= concurrency &&
    prefetch <= 10000 &&
    prefetch_bytes >= concurrency &&
    poll_ms > 0 &&
    poll_ms <= 1000 &&
    recovery_timeout_ms >= 0 &&
    progress_ms >= 0 &&
    ack_timeout_ms > 0 &&
    drain_timeout_ms > 0 else {
    raise Invalid("reliable worker limits")
  }
  if control.stopped_at is Some(_) {
    return
  }
  @async.with_task_group(group => {
    group.spawn_bg(no_wait=true, () => {
      control.stop_signal.acquire()
      control.stop_signal.release()
      let remaining = control.stopped_at.unwrap() +
        drain_timeout_ms.to_int64() -
        @async.now()
      if remaining > 0L {
        @async.sleep(remaining.to_int())
      }
      raise WorkerDrainTimedOut
    })
    let info = self.info()
    guard info is { "config": { "ack_policy": String("explicit"), .. }, .. } else {
      raise Invalid("reliable worker requires explicit ACK policy")
    }
    for i in 0..<concurrency {
      let batch = prefetch / concurrency +
        (if i < prefetch % concurrency { 1 } else { 0 })
      let max_bytes = prefetch_bytes / concurrency +
        (if i < prefetch_bytes % concurrency { 1 } else { 0 })
      group.spawn_bg(() => {
        while control.stopped_at is None {
          let messages = self.worker_fetch(
            control,
            poll_ms,
            recovery_timeout_ms,
            batch~,
            max_bytes~,
          )
          for message in messages {
            if control.stopped_at is Some(_) {
              break
            }
            control.active += 1
            defer {
              control.active -= 1
            }
            let meta = metadata(message)
            let result = with_work_progress(
              self.js.client,
              message,
              progress_ms,
              () => handler(message, meta),
            )
            self.finish_work(
              control, message, meta, result, ack_timeout_ms, on_uncertain,
            )
          }
        }
      })
    }
  })
}

///|
async fn Consumer::finish_work(
  self : Consumer,
  control : WorkControl,
  message : @client.Received,
  meta : Metadata,
  outcome : WorkOutcome,
  timeout_ms : Int32,
  on_uncertain : (UncertainDisposition) -> Unit,
) -> Unit {
  let client = self.js.client
  try {
    match outcome {
      Confirm => acknowledge(client, message, sync=true, timeout_ms~)
      RetryAfter(delay_ms) =>
        acknowledge(client, message, kind="nak", delay_ms~)
      Terminate => acknowledge(client, message, kind="term")
    }
  } catch {
    @client.Disconnected
    | @client.PublishUncertain
    | @client.NoResponders
    | @async.TimeoutError as cause => {
      control.uncertain += 1L
      on_uncertain({ message: meta, outcome, cause, })
    }
    error if @async.is_being_cancelled() => {
      control.uncertain += 1L
      on_uncertain({ message: meta, outcome, cause: error, })
      raise error
    }
    error => raise error
  } noraise {
    _ => if outcome is Confirm { control.completed += 1L }
  }
}

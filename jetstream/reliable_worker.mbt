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
/// Consume using a shared pull budget and server-confirmed ACKs.
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
    let window : PullWindow = {
      queue: @aqueue.Queue(kind=Blocking(prefetch)),
      released: @aqueue.Queue(kind=Blocking(1)),
      max_messages: prefetch,
      max_bytes: prefetch_bytes,
      messages: 0,
      bytes: 0,
    }
    let producer = group.spawn(() => {
      defer window.queue.close(clear=true)
      let info = self.info()
      guard info is { "config": { "ack_policy": String("explicit"), .. }, .. } else {
        raise Invalid("reliable worker requires explicit ACK policy")
      }
      self.fill_work(control, window, poll_ms, recovery_timeout_ms)
    })
    group.spawn_bg(no_wait=true, () => {
      control.stop_signal.acquire()
      control.stop_signal.release()
      producer.cancel()
      let remaining = control.stopped_at.unwrap() +
        drain_timeout_ms.to_int64() -
        @async.now()
      if remaining > 0L {
        @async.sleep(remaining.to_int())
      }
      raise WorkerDrainTimedOut
    })
    for _ in 0..<concurrency {
      group.spawn_bg(() => {
        while control.stopped_at is None {
          let (message, bytes) = window.queue.get() catch {
            @aqueue.QueueAlreadyClosed => break
            error => raise error
          }
          if control.stopped_at is Some(_) {
            break
          }
          control.active += 1
          defer {
            control.active -= 1
            window.messages -= 1
            window.bytes -= bytes
            ignore(window.released.try_put(())) catch {
              _ => ()
            }
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
      })
    }
  })
}

///|
priv struct PullWindow {
  queue : @aqueue.Queue[(@client.Received, Int32)]
  released : @aqueue.Queue[Unit]
  max_messages : Int32
  max_bytes : Int32
  mut messages : Int32
  mut bytes : Int32
}

///|
fn PullWindow::admit(
  self : PullWindow,
  control : WorkControl,
  message : @client.Received,
  header_size : Int32,
) -> Unit raise {
  let reply = message.reply_to.map(s => @utf8.encode(s).length()).unwrap_or(0)
  let size = message.payload.length() +
    header_size +
    @utf8.encode(message.subject).length() +
    reply
  guard self.messages < self.max_messages && size <= self.max_bytes - self.bytes else {
    raise Invalid("pull delivered beyond local prefetch budget")
  }
  guard self.queue.try_put((message, size)) else {
    raise Invalid("prefetch queue full")
  }
  self.messages += 1
  self.bytes += size
  control.peak_messages = control.peak_messages.max(self.messages)
  control.peak_bytes = control.peak_bytes.max(self.bytes)
}

///|
async fn Consumer::fill_work(
  self : Consumer,
  control : WorkControl,
  window : PullWindow,
  poll_ms : Int32,
  recovery_timeout_ms : Int32,
) -> Unit {
  let mut recover_until = None
  let mut delay_ms = 25
  while control.stopped_at is None {
    if window.messages == window.max_messages ||
      window.bytes == window.max_bytes {
      window.released.get()
      continue
    }
    // Only one pull is outstanding. It reserves at most the free credits at this point.
    ignore(window.released.try_get())
    let batch = window.max_messages - window.messages
    let max_bytes = window.max_bytes - window.bytes
    let budget = match recover_until {
      Some(until) =>
        (until - @async.now()).max(1L).min((poll_ms + 1500).to_int64()).to_int()
      None => poll_ms + 1500
    }
    try
      @async.with_timeout(budget, () => {
        self.pull_each(batch, poll_ms, max_bytes~, (message, header_size) => {
          window.admit(control, message, header_size)
        })
      })
    catch {
      Status(409, "Message Size Exceeds MaxBytes") if max_bytes <
        window.max_bytes => {
        // A delivery may fit the full window after another handler releases its credits.
        recover_until = None
        delay_ms = 25
        window.released.get()
      }
      error if recovery_timeout_ms > 0 && recoverable_pull_error(error) => {
        let now = @async.now()
        let until = recover_until.unwrap_or(
          now + recovery_timeout_ms.to_int64(),
        )
        guard now < until else { raise error }
        recover_until = Some(until)
        @async.sleep((until - now).min(delay_ms.to_int64()).to_int())
        delay_ms = (delay_ms * 2).min(500)
      }
      error => raise error
    } noraise {
      _ => {
        recover_until = None
        delay_ms = 25
      }
    }
  }
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

///|
/// Retry only against the named stream and within its observed deduplication window.
/// A message ID identifies a single immutable logical publish within that stream.
pub async fn JetStream::publish_reliably(
  self : JetStream,
  subject : String,
  payload : Bytes,
  message_id~ : String,
  expected_stream~ : String,
  timeout_ms~ : Int32 = 5000,
  attempt_timeout_ms~ : Int32 = 1000,
  retry_delay_ms~ : Int32 = 50,
) -> PubAck {
  validate_name(expected_stream)
  guard !message_id.trim().is_empty() &&
    timeout_ms > 0 &&
    attempt_timeout_ms > 0 &&
    retry_delay_ms > 0 else {
    raise Invalid("reliable publish options")
  }
  let deadline = @async.now() + timeout_ms.to_int64()
  let info = @async.with_timeout(timeout_ms, () => {
    self.stream_info(expected_stream)
  })
  guard info is { "config": config, .. } else { raise Missing("stream config") }
  let window_ns = integer_field(config, "duplicate_window")
  guard timeout_ms.to_int64() * 1000000L < window_ns else {
    raise Invalid(
      "publish retry timeout must be shorter than stream duplicate window",
    )
  }

  let mut attempted = false
  let mut uncertain = false
  let mut delay = retry_delay_ms.min(500)
  while true {
    let remaining = deadline - @async.now()
    if remaining <= 0L {
      if uncertain {
        raise PublishOutcomeUnknown(expected_stream, message_id)
      }
      if attempted {
        raise @client.NoResponders
      }
      raise PublishDeadlineExceeded
    }
    let attempt_budget = remaining.to_int().min(attempt_timeout_ms)
    attempted = true
    try
      self.publish(
        subject,
        payload,
        message_id~,
        expected_stream~,
        timeout_ms=attempt_budget,
      )
    catch {
      @client.Disconnected | @client.PublishUncertain | @async.TimeoutError =>
        uncertain = true
      @client.NoResponders => ()
      Api(_, _, _) | @client.InvalidOption(_) as error => {
        if uncertain {
          raise PublishOutcomeUnknown(expected_stream, message_id)
        }
        raise error
      }
      error => {
        if @async.is_being_cancelled() {
          raise error
        }
        raise PublishOutcomeUnknown(expected_stream, message_id)
      }
    } noraise {
      ack => {
        guard ack.stream == expected_stream else {
          raise PublishOutcomeUnknown(expected_stream, message_id)
        }
        return ack
      }
    }
    let pause = (deadline - @async.now()).min(delay.to_int64())
    if pause > 0L {
      @async.sleep(pause.to_int())
    }
    delay = (delay * 2).min(500)
  }
  abort("unreachable")
}

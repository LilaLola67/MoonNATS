///|
pub(all) enum PermissionOperation {
  Publish
  Subscribe
  PublishReply
} derive(Debug)

///|
pub(all) struct PermissionDenial {
  operation : PermissionOperation
  subject : String?
  queue : String?
  sid : Int32?
  message : String
} derive(Debug)

///|
fn quoted_argument(input : String) -> (String, String)? {
  guard input.has_prefix("\"") else { return None }
  let mut escaped = false
  for i in 1..<input.length() {
    let unit = input[i].to_int()
    if escaped {
      escaped = false
    } else if unit == 92 {
      escaped = true
    } else if unit == 34 {
      let value = @json.parse(input[:i + 1].to_owned()) catch {
        _ => return None
      }
      guard value is String(argument) else { return None }
      return Some((argument, input[i + 1:].to_owned()))
    }
  }
  None
}

///|
fn permission_text(message : String) -> String {
  let text = message.trim().to_owned()
  if text.length() >= 2 && text.has_prefix("'") && text.has_suffix("'") {
    text[1:text.length() - 1].to_owned()
  } else {
    text
  }
}

///|
fn permission_denial(message : String) -> PermissionDenial? {
  let text = permission_text(message)
  let publish = "Permissions Violation for Publish to "
  let subscribe = "Permissions Violation for Subscription to "
  let reply = "Permissions Violation for Publish with Reply of "
  let (operation, prefix) : (PermissionOperation, String) = if text.has_prefix(
      publish,
    ) {
    (Publish, publish)
  } else if text.has_prefix(subscribe) {
    (Subscribe, subscribe)
  } else if text.has_prefix(reply) {
    (PublishReply, reply)
  } else {
    return None
  }
  let unknown : PermissionDenial = {
    operation,
    subject: None,
    queue: None,
    sid: None,
    message,
  }
  guard quoted_argument(text[prefix.length():].to_owned())
    is Some((subject, suffix)) else {
    // Preserve the nonfatal event even if a server uses a different quoting format.
    return Some(unknown)
  }
  let mut tail = suffix
  let mut queue = None
  if tail.has_prefix(" using queue ") {
    guard quoted_argument(tail[13:].to_owned()) is Some((name, rest)) else {
      return Some(unknown)
    }
    queue = Some(name)
    tail = rest
  }
  let mut sid = None
  if tail.has_prefix(" (sid ") {
    guard quoted_argument(tail[6:].to_owned()) is Some((id, ")")) else {
      return Some(unknown)
    }
    let number = @string.parse_int(id) catch { _ => return Some(unknown) }
    sid = Some(number)
    tail = ""
  }
  guard tail.is_empty() || tail == ", too many tokens" else {
    return Some(unknown)
  }
  Some({ operation, subject: Some(subject), queue, sid, message, })
}

///|
fn Client::report_denial(self : Client, denial : PermissionDenial) -> Unit {
  if denial.operation is Subscribe && denial.subject is Some(subject) {
    // A subject token limit rejects the same subject regardless of queue.
    let subject_limit = permission_text(denial.message).has_suffix(
      ", too many tokens",
    )
    for sid, sub in self.subscriptions {
      if !sub.closed &&
        sub.subject == subject &&
        (subject_limit || sub.queue_group == denial.queue) &&
        (denial.sid is None || denial.sid == Some(sid)) {
        sub.closed = true
        sub.inbox.close(error=SubscriptionDenied(subject))
      }
    }
    self.prune()
  }
  (self.options.on_event)(PermissionViolation(denial))
}

///|
pub extend PermissionOperation with @debug.Debug::{to_repr}

///|
pub extend PermissionDenial with @debug.Debug::{to_repr}

///|
pub fn publish(
  subject_name : String,
  payload : Bytes,
  reply_to? : String,
) -> Bytes raise ProtocolError {
  guard subject(subject_name, false) else {
    raise Invalid("invalid publish subject")
  }
  let reply = match reply_to {
    Some(r) => {
      guard subject(r, false) else { raise Invalid("invalid reply subject") }
      " " + r
    }
    None => ""
  }
  let head = @utf8.encode("PUB \{subject_name}\{reply} \{payload.length()}\r\n")
  head + payload + b"\r\n"
}

///|
pub fn subscribe(
  subject_name : String,
  sid : Int32,
  queue? : String,
) -> Bytes raise ProtocolError {
  guard subject(subject_name, true) && sid > 0 else {
    raise Invalid("invalid subscription")
  }
  let q = match queue {
    Some(q) => {
      guard token(q) else { raise Invalid("invalid queue") }
      " " + q
    }
    None => ""
  }
  @utf8.encode("SUB \{subject_name}\{q} \{sid}\r\n")
}

///|
pub fn unsubscribe(
  sid : Int32,
  max_messages? : Int32,
) -> Bytes raise ProtocolError {
  guard sid > 0 else { raise Invalid("invalid subscription id") }
  let tail = match max_messages {
    Some(n) => {
      guard n > 0 else { raise Invalid("invalid message limit") }
      " " + n.to_string()
    }
    None => ""
  }
  @utf8.encode("UNSUB \{sid}\{tail}\r\n")
}

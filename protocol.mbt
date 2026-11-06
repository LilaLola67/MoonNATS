///|
pub suberror ProtocolError {
  Invalid(String)
} derive(Debug)

///|
pub(all) struct Message {
  subject : String
  sid : Int32
  reply_to : String?
  payload : Bytes
} derive(Debug, Eq)

///|
pub(all) enum Event {
  Info(String)
  Msg(Message)
  Ping
  Pong
  Ok
  ServerError(String)
} derive(Debug, Eq)

///|
pub struct Decoder {
  mut buffer : Array[Byte]
  mut pending : (String, Int32, String?, Int32)?
  max_line : Int32
  max_payload : Int32
  mut failed : Bool
}

///|
pub fn Decoder::new(
  max_line~ : Int32 = 4096,
  max_payload~ : Int32 = 1048576,
) -> Decoder raise ProtocolError {
  guard max_line > 0 &&
    max_line <= 2147483645 &&
    max_payload >= 0 &&
    max_payload <= 2147483645 else {
    raise Invalid("invalid decoder limits")
  }
  { buffer: [], pending: None, max_line, max_payload, failed: false, }
}

///|
fn number(s : String) -> Int32 raise ProtocolError {
  guard !s.is_empty() else { raise Invalid("empty number") }
  let mut n = 0
  for c in s.iter() {
    guard c >= '0' && c <= '9' else { raise Invalid("invalid number") }
    let digit = c.to_int() - 48
    guard n <= (2147483647 - digit) / 10 else {
      raise Invalid("number overflow")
    }
    n = n * 10 + digit
  }
  n
}

///|
fn token(s : String) -> Bool {
  !s.is_empty() && !s.iter().any(c => c <= ' ' || c == '\u{7f}')
}

///|
fn subject(s : String, wildcard : Bool) -> Bool {
  if !token(s) {
    return false
  }
  let parts = s.split(".").to_array()
  for i, p in parts {
    if p.is_empty() {
      return false
    }
    if p.contains("*") && (!wildcard || p != "*") {
      return false
    }
    if p.contains(">") && (!wildcard || p != ">" || i != parts.length() - 1) {
      return false
    }
  }
  true
}

///|
fn Decoder::line(
  self : Decoder,
  line : String,
  out : Array[Event],
) -> Unit raise ProtocolError {
  let fields = line
    .replace_all(old="\t", new=" ")
    .split(" ")
    .filter(s => !s.is_empty())
    .map(s => s.to_owned())
    .to_array()
  guard fields.length() > 0 else { raise Invalid("empty control line") }
  match fields[0].to_upper() {
    "PING" => {
      guard fields.length() == 1 else { raise Invalid("PING arguments") }
      out.push(Ping)
    }
    "PONG" => {
      guard fields.length() == 1 else { raise Invalid("PONG arguments") }
      out.push(Pong)
    }
    "+OK" => {
      guard fields.length() == 1 else { raise Invalid("OK arguments") }
      out.push(Ok)
    }
    "INFO" => {
      guard line.length() > 5 else { raise Invalid("missing INFO") }
      let data = line[5:].to_owned()
      let parsed = @json.parse(data) catch {
        _ => raise Invalid("invalid INFO JSON")
      }
      guard parsed is Object(_) else { raise Invalid("INFO must be object") }
      out.push(Info(data))
    }
    "-ERR" => {
      guard line.length() > 5 else { raise Invalid("missing server error") }
      out.push(ServerError(line[5:].to_owned()))
    }
    "MSG" => {
      guard fields.length() == 4 || fields.length() == 5 else {
        raise Invalid("MSG arity")
      }
      guard subject(fields[1], false) else {
        raise Invalid("invalid message subject")
      }
      let sid = number(fields[2])
      guard sid > 0 else { raise Invalid("invalid subscription id") }
      let reply = if fields.length() == 5 { Some(fields[3]) } else { None }
      if reply is Some(r) {
        guard subject(r, false) else { raise Invalid("invalid reply subject") }
      }
      let size = number(fields[fields.length() - 1])
      guard size <= self.max_payload else {
        raise Invalid("payload limit exceeded")
      }
      self.pending = Some((fields[1], sid, reply, size))
    }
    _ => raise Invalid("unsupported server operation")
  }
}

///|
fn Decoder::consume(
  self : Decoder,
  chunk : Bytes,
  out : Array[Event],
) -> Unit raise ProtocolError {
  for byte in chunk {
    self.buffer.push(byte)
    let n = self.buffer.length()
    if self.pending is Some((subj, sid, reply, size)) {
      if n == size + 2 {
        guard self.buffer[size] == 13 && self.buffer[size + 1] == 10 else {
          raise Invalid("payload terminator")
        }
        out.push(
          Msg({
            subject: subj,
            sid,
            reply_to: reply,
            payload: Bytes::from_array(self.buffer[:size]),
          }),
        )
        self.pending = None
        self.buffer = []
      }
    } else {
      guard n <= self.max_line + 2 else {
        raise Invalid("control line limit exceeded")
      }
      if n >= 2 && self.buffer[n - 2] == 13 && byte == 10 {
        let raw = Bytes::from_array(self.buffer[:n - 2])
        let line = @utf8.decode(raw) catch {
          _ => raise Invalid("invalid UTF-8 control line")
        }
        self.buffer = []
        self.line(line, out)
      }
    }
  }
}

///|
/// Feed arbitrary transport fragments. A protocol error permanently invalidates this decoder.
pub fn Decoder::feed(
  self : Decoder,
  chunk : Bytes,
) -> Array[Event] raise ProtocolError {
  guard !self.failed else {
    raise Invalid("decoder failed; create a new decoder")
  }
  let out = []
  errdefer {
    self.failed = true
    self.buffer = []
    self.pending = None
  }
  self.consume(chunk, out)
  out
}

///|
/// Call on transport EOF to detect truncated control lines or payloads.
pub fn Decoder::finish(self : Decoder) -> Unit raise ProtocolError {
  guard !self.failed else { raise Invalid("decoder failed") }
  if !self.buffer.is_empty() || self.pending is Some(_) {
    self.failed = true
    raise Invalid("truncated frame")
  }
}

///|
pub extend ProtocolError with @debug.Debug::{to_repr}

///|
pub extend Message with @debug.Debug::{to_repr}

///|
pub extend Message with Eq::{equal, not_equal}

///|
pub extend Event with @debug.Debug::{to_repr}

///|
pub extend Event with Eq::{equal, not_equal}

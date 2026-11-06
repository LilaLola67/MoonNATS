///|
pub(all) struct Headers {
  entries : Array[(String, String)]
  status : Int32?
  description : String
} derive(Eq, Debug)

///|
pub extend Headers with Eq::{equal, not_equal}

///|
pub extend Headers with @debug.Debug::{to_repr}

///|
pub fn Headers::new(entries~ : Array[(String, String)] = []) -> Headers {
  { entries: entries.copy(), status: None, description: "", }
}

///|
pub fn Headers::get_all(self : Headers, name : String) -> Array[String] {
  self.entries
  .iter()
  .filter_map(pair => if pair.0 == name { Some(pair.1) } else { None })
  .to_array()
}

///|
pub fn Headers::get(self : Headers, name : String) -> String? {
  for (key, value) in self.entries {
    if key == name {
      return Some(value)
    }
  }
  None
}

///|
pub fn Headers::encode(self : Headers) -> Bytes raise ProtocolError {
  let out = StringBuilder()
  out.write_string("NATS/1.0")
  if self.status is Some(code) {
    guard code >= 100 && code <= 999 else {
      raise Invalid("invalid header status")
    }
    guard !self.description.contains("\r") && !self.description.contains("\n") else {
      raise Invalid("invalid status description")
    }
    out.write_string(" \{code}")
    if !self.description.is_empty() {
      out.write_string(" " + self.description)
    }
  }
  out.write_string("\r\n")
  for (name, value) in self.entries {
    guard token(name) &&
      !name.contains(":") &&
      !value.contains("\r") &&
      !value.contains("\n") &&
      !value.contains("\u0000") else {
      raise Invalid("invalid header field")
    }
    out.write_string("\{name}: \{value}\r\n")
  }
  out.write_string("\r\n")
  @utf8.encode(out.to_string())
}

///|
pub fn Headers::parse(raw : Bytes) -> Headers raise ProtocolError {
  let text = @utf8.decode(raw) catch {
    _ => raise Invalid("invalid header UTF-8")
  }
  guard text.has_suffix("\r\n\r\n") else { raise Invalid("header terminator") }
  let lines = text.split("\r\n").map(s => s.to_owned()).to_array()
  guard lines[0] == "NATS/1.0" || lines[0].has_prefix("NATS/1.0 ") else {
    raise Invalid("header version")
  }
  let mut status = None
  let mut description = ""
  if lines[0].length() > 8 {
    let parts = lines[0][9:].split(" ").map(s => s.to_owned()).to_array()
    let code = number(parts[0])
    guard parts[0].length() == 3 && code >= 100 else {
      raise Invalid("invalid status")
    }
    status = Some(code)
    description = parts[1:].iter().join(" ")
  }
  let entries = []
  for i in 1..<(lines.length() - 2) {
    let line = lines[i]
    let colon = line.find(":")
    guard colon is Some(at) && at > 0 else {
      raise Invalid("invalid header line")
    }
    let name = line[:at].to_owned()
    let value = line[at + 1:].trim().to_owned()
    guard token(name) &&
      !value.contains("\r") &&
      !value.contains("\n") &&
      !value.contains("\u0000") else {
      raise Invalid("invalid header field")
    }
    entries.push((name, value))
  }
  { entries, status, description, }
}

///|
pub fn publish_headers(
  subject_name : String,
  headers : Headers,
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
  let h = headers.encode()
  @utf8.encode(
    "HPUB \{subject_name}\{reply} \{h.length()} \{h.length() + payload.length()}\r\n",
  ) +
  h +
  payload +
  b"\r\n"
}

///|
pub(all) suberror JetStreamError {
  Api(Int32, Int32, String)
  Invalid(String)
  Status(Int32, String)
  Missing(String)
  PublishDeadlineExceeded
  PublishOutcomeUnknown(String, String)
  WorkerDrainTimedOut
} derive(Debug)

///|
pub extend JetStreamError with @debug.Debug::{to_repr}

///|
pub fn validate_name(name : String) -> Unit raise JetStreamError {
  guard !name.is_empty() &&
    name.length() <= 255 &&
    !name
    .iter()
    .any(c => {
      c <= ' ' ||
      c == '.' ||
      c == '*' ||
      c == '>' ||
      c == '/' ||
      c == '\\' ||
      c == '\u007f'
    }) else {
    raise Invalid("invalid resource name")
  }
}

///|
pub fn text_field(j : Json, key : String) -> String raise JetStreamError {
  guard j is Object(m) && m.get(key) is Some(String(s)) else {
    raise Missing(key)
  }
  s
}

///|
pub fn integer_field(j : Json, key : String) -> Int64 raise {
  guard j is Object(m) && m.get(key) is Some(Number(n, repr~)) else {
    raise Missing(key)
  }
  match repr {
    Some(s) => @string.parse_int64(s)
    None => {
      guard n >= 0 && n < 9007199254740992.0 && n == n.floor() else {
        raise Invalid("inexact integer")
      }
      n.to_int64()
    }
  }
}

///|
pub fn integer(n : Int64) -> Json {
  Json::number(n.to_double(), repr=n.to_string())
}

///|
pub fn decode_json(bytes : Bytes) -> Json raise {
  @json.parse(@utf8.decode(bytes), preserve_number_repr=true)
}

///|
pub fn check_response(j : Json) -> Json raise JetStreamError {
  if j
    is {
      "error": {
        "code": Number(code, ..),
        "err_code": Number(err, ..),
        "description": String(desc),
        ..
      },
      ..
    } {
    raise Api(code.to_int(), err.to_int(), desc)
  }
  if j is { "error": _, .. } {
    raise Invalid("malformed API error")
  }
  j
}

///|
pub struct JetStream {
  client : @client.Client
  prefix : String
}

///|
pub fn JetStream::new(
  client : @client.Client,
  domain? : String,
) -> JetStream raise JetStreamError {
  let prefix = match domain {
    Some(d) => {
      validate_name(d)
      "$JS." + d + ".API"
    }
    None => "$JS.API"
  }
  { client, prefix, }
}

///|
pub async fn JetStream::api(
  self : JetStream,
  suffix : String,
  data~ : Json = {},
) -> Json {
  let msg = self.client.request(
    self.prefix + "." + suffix,
    @utf8.encode(data.stringify()),
  )
  check_response(decode_json(msg.payload))
}

///|
pub async fn JetStream::account_info(self : JetStream) -> Json {
  self.api("INFO")
}

///|
pub async fn JetStream::create_stream(
  self : JetStream,
  name : String,
  subjects : Array[String],
  storage~ : String = "file",
  history~ : Int32 = -1,
) -> Json {
  validate_name(name)
  guard storage == "file" || storage == "memory" else {
    raise Invalid("storage")
  }
  self.api("STREAM.CREATE." + name, data={
    "name": Json(name),
    "subjects": Json(subjects),
    "storage": Json(storage),
    "max_msgs_per_subject": Json(history),
  })
}

///|
pub async fn JetStream::create_stream_config(
  self : JetStream,
  name : String,
  config : Json,
) -> Json {
  validate_name(name)
  guard config is Object(m) else { raise Invalid("stream config") }
  let copy = m.copy()
  copy["name"] = Json(name)
  self.api("STREAM.CREATE." + name, data=Json::object(copy))
}

///|
pub async fn JetStream::update_stream(
  self : JetStream,
  name : String,
  config : Json,
) -> Json {
  validate_name(name)
  guard config is Object(m) else { raise Invalid("stream config") }
  let copy = m.copy()
  copy["name"] = Json(name)
  self.api("STREAM.UPDATE." + name, data=Json::object(copy))
}

///|
pub async fn JetStream::stream_info(self : JetStream, name : String) -> Json {
  validate_name(name)
  self.api("STREAM.INFO." + name)
}

///|
pub async fn JetStream::streams(self : JetStream, offset~ : Int32 = 0) -> Json {
  self.api("STREAM.LIST", data={ "offset": Json(offset) })
}

///|
pub async fn JetStream::delete_stream(self : JetStream, name : String) -> Unit {
  validate_name(name)
  ignore(self.api("STREAM.DELETE." + name))
}

///|
pub async fn JetStream::purge_stream(
  self : JetStream,
  name : String,
  filter? : String,
) -> Unit {
  validate_name(name)
  let data : Json = match filter {
    Some(s) => { "filter": Json(s) }
    None => {}
  }
  ignore(self.api("STREAM.PURGE." + name, data~))
}

///|
pub async fn JetStream::get_message(
  self : JetStream,
  stream : String,
  subject : String,
  sequence? : Int64,
) -> Json {
  validate_name(stream)
  let data : Json = match sequence {
    Some(n) => { "seq": integer(n) }
    None => { "last_by_subj": Json(subject) }
  }
  self.api("STREAM.MSG.GET." + stream, data~)
}

///|
pub(all) struct PubAck {
  stream : String
  sequence : Int64
  duplicate : Bool
}

///|
pub async fn JetStream::publish(
  self : JetStream,
  subject : String,
  payload : Bytes,
  message_id? : String,
  expected_stream? : String,
  expected_last_sequence? : Int64,
  expected_last_subject_sequence? : Int64,
  headers~ : @wire.Headers = @wire.Headers::new(),
  timeout_ms? : Int32,
) -> PubAck {
  let entries = headers.entries.copy()
  for
    (k, v) in [
      ("Nats-Msg-Id", message_id),
      ("Nats-Expected-Stream", expected_stream),
    ] {
    if v is Some(v) {
      entries.push((k, v))
    }
  }
  for
    (k, v) in [
      ("Nats-Expected-Last-Sequence", expected_last_sequence),
      ("Nats-Expected-Last-Subject-Sequence", expected_last_subject_sequence),
    ] {
    if v is Some(v) {
      guard v >= 0L else { raise Invalid("expected sequence") }
      entries.push((k, v.to_string()))
    }
  }
  let msg = self.client.request(
    subject,
    payload,
    headers=@wire.Headers::new(entries~),
    timeout_ms?,
  )
  let j = check_response(decode_json(msg.payload))
  {
    stream: text_field(j, "stream"),
    sequence: integer_field(j, "seq"),
    duplicate: j is { "duplicate": True, .. },
  }
}

///|
pub struct Consumer {
  js : JetStream
  stream : String
  name : String
}

///|
pub async fn JetStream::create_consumer(
  self : JetStream,
  stream : String,
  name : String,
  filter~ : String = ">",
  ack_wait_ms~ : Int32 = 30000,
  max_deliver~ : Int32 = -1,
) -> Consumer {
  guard ack_wait_ms > 0 else { raise Invalid("ack_wait") }
  self.create_consumer_config(stream, name, {
    "durable_name": Json(name),
    "ack_policy": "explicit",
    "deliver_policy": "all",
    "filter_subject": Json(filter),
    "ack_wait": integer(ack_wait_ms.to_int64() * 1000000L),
    "max_deliver": Json(max_deliver),
  })
}

///|
pub async fn JetStream::create_consumer_config(
  self : JetStream,
  stream : String,
  name : String,
  config : Json,
) -> Consumer {
  validate_name(stream)
  validate_name(name)
  ignore(
    self.api("CONSUMER.CREATE." + stream + "." + name, data={
      "stream_name": Json(stream),
      "config": config,
    }),
  )
  { js: self, stream, name, }
}

///|
pub fn JetStream::consumer(
  self : JetStream,
  stream : String,
  name : String,
) -> Consumer raise JetStreamError {
  validate_name(stream)
  validate_name(name)
  { js: self, stream, name, }
}

///|
pub async fn Consumer::info(self : Consumer) -> Json {
  self.js.api("CONSUMER.INFO." + self.stream + "." + self.name)
}

///|
pub async fn Consumer::update(self : Consumer, config : Json) -> Unit {
  ignore(
    self.js.api("CONSUMER.CREATE." + self.stream + "." + self.name, data={
      "stream_name": Json(self.stream),
      "config": config,
      "action": "update",
    }),
  )
}

///|
pub async fn Consumer::delete(self : Consumer) -> Unit {
  ignore(self.js.api("CONSUMER.DELETE." + self.stream + "." + self.name))
}

///|
pub async fn JetStream::consumers(
  self : JetStream,
  stream : String,
  offset~ : Int32 = 0,
) -> Json {
  validate_name(stream)
  self.api("CONSUMER.LIST." + stream, data={ "offset": Json(offset) })
}

///|
pub async fn Consumer::fetch(
  self : Consumer,
  batch~ : Int32 = 1,
  expires_ms~ : Int32 = 1000,
  max_bytes? : Int32,
) -> Array[@client.Received] {
  guard batch > 0 && batch <= 10000 && expires_ms > 0 && expires_ms <= 300000 else {
    raise Invalid("fetch limits")
  }
  let c = self.js.client
  let sub = c.subscribe(c.new_inbox(), capacity=batch + 16, restore=false)
  let inbox = sub.subject
  defer @async.protect_from_cancel(() => {
    @async.with_timeout(250, () => c.unsubscribe(sub)) catch {
      _ => ()
    }
  })
  let data : Map[String, Json] = {
    "batch": Json(batch),
    "expires": integer(expires_ms.to_int64() * 1000000L),
  }
  if max_bytes is Some(n) {
    guard n > 0 else { raise Invalid("max_bytes") }
    data["max_bytes"] = Json(n)
  }
  c.publish(
    self.js.prefix + ".CONSUMER.MSG.NEXT." + self.stream + "." + self.name,
    @utf8.encode(Json::object(data).stringify()),
    reply_to=inbox,
  )
  let out = []
  let deadline = @async.now() + expires_ms.to_int64() + 1000L
  while out.length() < batch {
    let remaining = deadline - @async.now()
    if remaining <= 0L {
      break
    }
    let msg = sub.next(timeout_ms=remaining.to_int())
    match msg.headers.status {
      Some(100) => ()
      Some(404) | Some(408) => break
      Some(409) if msg.headers.description == "Batch Completed" => break
      Some(409) if !out.is_empty() &&
        msg.headers.description == "Message Size Exceeds MaxBytes" => break
      Some(code) => raise Status(code, msg.headers.description)
      None => out.push(msg)
    }
  }
  out
}

///|
pub(all) struct Metadata {
  stream : String
  consumer : String
  deliveries : Int64
  stream_sequence : Int64
  consumer_sequence : Int64
  timestamp_ns : Int64
  pending : Int64
}

///|
pub fn metadata(message : @client.Received) -> Metadata raise {
  guard message.reply_to is Some(reply) else {
    raise Invalid("missing ACK reply subject")
  }
  let p = reply.split(".").to_array()
  let n = p.length()
  guard (n == 9 || n >= 11) && p[0] == "$JS" && p[1] == "ACK" else {
    raise Invalid("ACK subject")
  }
  let base = if n == 9 { 2 } else { 4 }
  {
    stream: p[base].to_owned(),
    consumer: p[base + 1].to_owned(),
    deliveries: @string.parse_int64(p[base + 2]),
    stream_sequence: @string.parse_int64(p[base + 3]),
    consumer_sequence: @string.parse_int64(p[base + 4]),
    timestamp_ns: @string.parse_int64(p[base + 5]),
    pending: @string.parse_int64(p[base + 6]),
  }
}

///|
pub async fn acknowledge(
  client : @client.Client,
  message : @client.Received,
  kind~ : String = "ack",
  delay_ms~ : Int32 = 0,
  sync~ : Bool = false,
  timeout_ms? : Int32,
) -> Unit {
  guard message.reply_to is Some(subject) && subject.has_prefix("$JS.ACK.") else {
    raise Invalid("missing ACK reply subject")
  }
  guard !sync || kind == "ack" else {
    raise Invalid("synchronous confirmation requires ACK")
  }
  guard delay_ms >= 0 else { raise Invalid("negative redelivery delay") }
  let body = match kind {
    "ack" => "+ACK"
    "nak" =>
      if delay_ms == 0 {
        "-NAK"
      } else {
        "-NAK {\"delay\":" + (delay_ms.to_int64() * 1000000L).to_string() + "}"
      }
    "progress" => "+WPI"
    "term" => "+TERM"
    _ => raise Invalid("ACK kind")
  }
  if sync {
    let response = client.request(subject, @utf8.encode(body), timeout_ms?)
    if response.headers.status is Some(code) && code >= 300 {
      raise Status(code, response.headers.description)
    }
  } else {
    client.publish(subject, @utf8.encode(body), buffer_on_reconnect=false)
  }
}

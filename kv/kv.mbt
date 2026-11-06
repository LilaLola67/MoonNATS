///|
pub struct Bucket {
  js : @js.JetStream
  name : String
}

///|
pub(all) struct Entry {
  bucket : String
  key : String
  value : Bytes
  revision : Int64
  operation : String
  pending : Int64
}

///|
fn key_valid(key : String, wildcard : Bool) -> Unit raise @js.JetStreamError {
  guard !key.is_empty() &&
    !key.has_prefix(".") &&
    !key.has_suffix(".") &&
    !key.contains("..") else {
    raise @js.Invalid("invalid KV key")
  }
  for c in key.iter() {
    guard (c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') ||
      c == '.' ||
      c == '-' ||
      c == '_' ||
      c == '/' ||
      c == '=' ||
      (wildcard && (c == '*' || c == '>')) else {
      raise @js.Invalid("invalid KV key")
    }
  }
}

///|
pub async fn create(
  js : @js.JetStream,
  name : String,
  history~ : Int32 = 1,
  ttl_ms~ : Int32 = 0,
) -> Bucket {
  @js.validate_name(name)
  guard history >= 1 && history <= 64 && ttl_ms >= 0 else {
    raise @js.Invalid("KV configuration")
  }
  ignore(
    js.create_stream_config("KV_" + name, {
      "subjects": Json(["$KV." + name + ".>"]),
      "storage": "file",
      "max_msgs_per_subject": Json(history),
      "max_age": @js.integer(ttl_ms.to_int64() * 1000000L),
      "allow_rollup_hdrs": true,
      "deny_delete": true,
      "allow_direct": true,
    }),
  )
  { js, name, }
}

///|
pub fn open(
  js : @js.JetStream,
  name : String,
) -> Bucket raise @js.JetStreamError {
  @js.validate_name(name)
  { js, name, }
}

///|
pub async fn Bucket::put(
  self : Bucket,
  key : String,
  value : Bytes,
  expected_revision? : Int64,
) -> Int64 {
  key_valid(key, false)
  self.js.publish(
    "$KV." + self.name + "." + key,
    value,
    expected_last_subject_sequence?=expected_revision,
  ).sequence
}

///|
pub async fn Bucket::create(
  self : Bucket,
  key : String,
  value : Bytes,
) -> Int64 {
  self.put(key, value, expected_revision=0L)
}

///|
pub async fn Bucket::update(
  self : Bucket,
  key : String,
  value : Bytes,
  revision : Int64,
) -> Int64 {
  guard revision > 0L else { raise @js.Invalid("revision") }
  self.put(key, value, expected_revision=revision)
}

///|
pub async fn Bucket::get(
  self : Bucket,
  key : String,
  revision? : Int64,
) -> Entry {
  key_valid(key, false)
  let j = self.js.get_message(
    "KV_" + self.name,
    "$KV." + self.name + "." + key,
    sequence?=revision,
  )
  guard j is { "message": Object(m), .. } else {
    raise @js.Invalid("KV get response")
  }
  let msg = Json::object(m)
  let actual = @js.text_field(msg, "subject")
  guard actual == "$KV." + self.name + "." + key else {
    raise @js.Invalid("revision belongs to another key")
  }
  let value = match m.get("data") {
    Some(String(data)) => @base64.decode(data)
    _ => b""
  }
  let headers = match m.get("hdrs") {
    Some(String(data)) => @wire.Headers::parse(@base64.decode(data))
    _ => @wire.Headers::new()
  }
  let operation = headers.get("KV-Operation").unwrap_or("PUT")
  {
    bucket: self.name,
    key,
    value,
    revision: @js.integer_field(msg, "seq"),
    operation,
    pending: 0L,
  }
}

///|
pub async fn Bucket::delete(
  self : Bucket,
  key : String,
  purge~ : Bool = false,
  expected_revision? : Int64,
) -> Int64 {
  key_valid(key, false)
  let entries = [("KV-Operation", if purge { "PURGE" } else { "DEL" })]
  if purge {
    entries.push(("Nats-Rollup", "sub"))
  }
  self.js.publish(
    "$KV." + self.name + "." + key,
    b"",
    expected_last_subject_sequence?=expected_revision,
    headers=@wire.Headers::new(entries~),
  ).sequence
}

///|
fn Bucket::entry(self : Bucket, msg : @client.Received) -> Entry raise {
  let prefix = "$KV." + self.name + "."
  guard msg.subject.has_prefix(prefix) else { raise @js.Invalid("KV subject") }
  let metadata = @js.metadata(msg)
  {
    bucket: self.name,
    key: msg.subject[prefix.length():].to_owned(),
    value: msg.payload,
    revision: metadata.stream_sequence,
    operation: msg.headers.get("KV-Operation").unwrap_or("PUT"),
    pending: metadata.pending,
  }
}

///|
pub async fn Bucket::history(self : Bucket, key : String) -> Array[Entry] {
  key_valid(key, false)
  let name = "MN_" + self.js.client.new_inbox().replace_all(old=".", new="_")
  let c = self.js.create_consumer_config("KV_" + self.name, name, {
    "ack_policy": "none",
    "deliver_policy": "all",
    "filter_subject": Json("$KV." + self.name + "." + key),
  })
  defer @async.protect_from_cancel(() => c.delete() catch { _ => () })
  c.fetch(batch=64, expires_ms=200).map(msg => self.entry(msg))
}

///|
pub struct Watch {
  bucket : Bucket
  subscription : @client.Subscription
  consumer : @js.Consumer
}

///|
pub async fn Bucket::watch(
  self : Bucket,
  pattern~ : String = ">",
  updates_only~ : Bool = false,
) -> Watch {
  key_valid(pattern, true)
  let inbox = self.js.client.new_inbox()
  let sub = self.js.client.subscribe(inbox)
  errdefer @async.protect_from_cancel(() => {
    self.js.client.unsubscribe(sub) catch {
      _ => ()
    }
  })
  let name = "MN_" + inbox.replace_all(old=".", new="_")
  let consumer = self.js.create_consumer_config("KV_" + self.name, name, {
    "ack_policy": "none",
    "deliver_policy": Json(
      if updates_only {
        "new"
      } else {
        "last_per_subject"
      },
    ),
    "filter_subject": Json("$KV." + self.name + "." + pattern),
    "deliver_subject": Json(inbox),
  })
  { bucket: self, subscription: sub, consumer, }
}

///|
pub async fn Watch::next(self : Watch, timeout_ms~ : Int32 = 5000) -> Entry {
  self.bucket.entry(self.subscription.next(timeout_ms~))
}

///|
pub async fn Watch::close(self : Watch) -> Unit {
  defer @async.protect_from_cancel(() => {
    self.bucket.js.client.unsubscribe(self.subscription) catch {
      _ => ()
    }
  })
  self.consumer.delete()
}

///|
pub async fn Bucket::destroy(self : Bucket) -> Unit {
  self.js.delete_stream("KV_" + self.name)
}

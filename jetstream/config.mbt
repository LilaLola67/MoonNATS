///|
pub(all) enum Storage {
  File
  Memory
}

///|
pub(all) enum Retention {
  Limits
  Interest
  WorkQueue
}

///|
pub(all) enum AckPolicy {
  Explicit
  NoAck
  All
}

///|
pub(all) struct StreamConfig {
  subjects : Array[String]
  storage : Storage
  retention : Retention
  replicas : Int32
  max_messages : Int64
  max_bytes : Int64
  max_age_ms : Int64
}

///|
pub fn StreamConfig::new(
  subjects : Array[String],
  storage~ : Storage = File,
  retention~ : Retention = Limits,
  replicas~ : Int32 = 1,
  max_messages~ : Int64 = -1L,
  max_bytes~ : Int64 = -1L,
  max_age_ms~ : Int64 = 0L,
) -> StreamConfig {
  {
    subjects: subjects.copy(),
    storage,
    retention,
    replicas,
    max_messages,
    max_bytes,
    max_age_ms,
  }
}

///|
pub fn StreamConfig::to_json(self : StreamConfig) -> Json raise {
  guard !self.subjects.is_empty() &&
    self.replicas >= 1 &&
    self.replicas <= 5 &&
    self.max_messages >= -1L &&
    self.max_bytes >= -1L &&
    self.max_age_ms >= 0L &&
    self.max_age_ms <= 9223372036854L else {
    raise Invalid("stream configuration")
  }
  for subject in self.subjects {
    ignore(@wire.subscribe(subject, 1))
  }
  {
    "subjects": Json(self.subjects),
    "storage": Json(
      match self.storage {
        File => "file"
        Memory => "memory"
      },
    ),
    "retention": Json(
      match self.retention {
        Limits => "limits"
        Interest => "interest"
        WorkQueue => "workqueue"
      },
    ),
    "num_replicas": Json(self.replicas),
    "max_msgs": integer(self.max_messages),
    "max_bytes": integer(self.max_bytes),
    "max_age": integer(self.max_age_ms * 1000000L),
  }
}

///|
pub async fn JetStream::add_stream(
  self : JetStream,
  name : String,
  config : StreamConfig,
) -> Json {
  self.create_stream_config(name, config.to_json())
}

///|
pub(all) struct ConsumerConfig {
  filter : String
  ack_policy : AckPolicy
  ack_wait_ms : Int32
  max_deliver : Int32
  max_ack_pending : Int32
  start_sequence : Int64?
}

///|
pub fn ConsumerConfig::new(
  filter~ : String = ">",
  ack_policy~ : AckPolicy = Explicit,
  ack_wait_ms~ : Int32 = 30000,
  max_deliver~ : Int32 = -1,
  max_ack_pending~ : Int32 = 1000,
  start_sequence? : Int64,
) -> ConsumerConfig {
  {
    filter,
    ack_policy,
    ack_wait_ms,
    max_deliver,
    max_ack_pending,
    start_sequence,
  }
}

///|
pub fn ConsumerConfig::to_json(self : ConsumerConfig) -> Json raise {
  ignore(@wire.subscribe(self.filter, 1))
  guard self.ack_wait_ms > 0 &&
    (self.max_deliver == -1 || self.max_deliver > 0) &&
    self.max_ack_pending > 0 else {
    raise Invalid("consumer configuration")
  }
  let fields : Map[String, Json] = {
    "filter_subject": Json(self.filter),
    "ack_policy": Json(
      match self.ack_policy {
        Explicit => "explicit"
        NoAck => "none"
        All => "all"
      },
    ),
    "ack_wait": integer(self.ack_wait_ms.to_int64() * 1000000L),
    "max_deliver": Json(self.max_deliver),
    "max_ack_pending": Json(self.max_ack_pending),
    "deliver_policy": "all",
  }
  if self.ack_policy is NoAck {
    fields.remove("ack_wait")
    fields.remove("max_ack_pending")
  }
  if self.start_sequence is Some(n) {
    guard n > 0L else { raise Invalid("start sequence") }
    fields["deliver_policy"] = "by_start_sequence"
    fields["opt_start_seq"] = integer(n)
  }
  Json::object(fields)
}

///|
pub async fn JetStream::add_consumer(
  self : JetStream,
  stream : String,
  name : String,
  config : ConsumerConfig,
) -> Consumer {
  guard config.to_json() is Object(fields) else {
    raise Invalid("consumer config")
  }
  fields["durable_name"] = Json(name)
  self.create_consumer_config(stream, name, Json::object(fields))
}

///|
async fn JetStream::list_all(
  self : JetStream,
  suffix : String,
  field : String,
) -> Array[Json] {
  let out = []
  let mut offset = 0L
  while true {
    let page = self.api(suffix, data={ "offset": integer(offset) })
    let total = integer_field(page, "total")
    guard page is Object(m) else { raise Invalid("list response") }
    let items = match m.get(field) {
      Some(Array(items)) => items
      _ => []
    }
    for item in items {
      out.push(item)
    }
    offset += items.length().to_int64()
    if offset >= total {
      break
    }
    guard !items.is_empty() else {
      raise Invalid("pagination made no progress")
    }
  }
  out
}

///|
pub async fn JetStream::all_streams(self : JetStream) -> Array[Json] {
  self.list_all("STREAM.LIST", "streams")
}

///|
pub async fn JetStream::all_consumers(
  self : JetStream,
  stream : String,
) -> Array[Json] {
  validate_name(stream)
  self.list_all("CONSUMER.LIST." + stream, "consumers")
}

///|
pub(all) struct AccountUsage {
  memory : Int64
  storage : Int64
  streams : Int64
  consumers : Int64
  limits : Json
}

///|
pub async fn JetStream::account_usage(self : JetStream) -> AccountUsage {
  let j = self.account_info()
  guard j is { "limits": limits, .. } else { raise Missing("limits") }
  {
    memory: integer_field(j, "memory"),
    storage: integer_field(j, "storage"),
    streams: integer_field(j, "streams"),
    consumers: integer_field(j, "consumers"),
    limits,
  }
}

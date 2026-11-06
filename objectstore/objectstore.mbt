///|
pub struct Bucket {
  js : @js.JetStream
  name : String
}

///|
pub(all) struct ObjectInfo {
  name : String
  bucket : String
  nuid : String
  size : Int64
  chunks : Int32
  digest : String
  deleted : Bool
  raw : Json
}

///|
fn url64(bytes : Bytes) -> String {
  @base64.encode(bytes)
  .replace_all(old="+", new="-")
  .replace_all(old="/", new="_")
}

///|
fn meta_subject(
  bucket : String,
  name : String,
) -> String raise @js.JetStreamError {
  guard !name.is_empty() && name.length() <= 1024 && !name.contains("\u0000") else {
    raise @js.Invalid("object name")
  }
  "$O." + bucket + ".M." + url64(@utf8.encode(name))
}

///|
fn info(j : Json) -> ObjectInfo raise {
  let chunks = @js.integer_field(j, "chunks")
  guard chunks >= 0L && chunks <= 10000000L else {
    raise @js.Invalid("chunk count limit")
  }
  @js.validate_name(@js.text_field(j, "nuid"))
  {
    name: @js.text_field(j, "name"),
    bucket: @js.text_field(j, "bucket"),
    nuid: @js.text_field(j, "nuid"),
    size: @js.integer_field(j, "size"),
    chunks: chunks.to_int(),
    digest: match j {
      { "digest": String(d), .. } => d
      _ => ""
    },
    deleted: j is { "deleted": True, .. },
    raw: j,
  }
}

///|
pub async fn create(
  js : @js.JetStream,
  name : String,
  max_bytes~ : Int64 = -1L,
) -> Bucket {
  @js.validate_name(name)
  ignore(
    js.create_stream_config("OBJ_" + name, {
      "subjects": Json(["$O." + name + ".C.>", "$O." + name + ".M.>"]),
      "storage": "file",
      "discard": "new",
      "allow_rollup_hdrs": true,
      "allow_direct": true,
      "max_bytes": @js.integer(max_bytes),
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
pub async fn Bucket::get_info(self : Bucket, name : String) -> ObjectInfo {
  let j = self.js.get_message("OBJ_" + self.name, meta_subject(self.name, name))
  guard j is { "message": { "data": String(data), .. }, .. } else {
    raise @js.Invalid("object metadata")
  }
  let result = info(@js.decode_json(@base64.decode(data)))
  guard result.bucket == self.name && result.name == name else {
    raise @js.Invalid("object identity mismatch")
  }
  result
}

///|
pub async fn Bucket::put_stream(
  self : Bucket,
  name : String,
  next : async () -> Bytes?,
  chunk_size~ : Int32 = 131072,
  description~ : String = "",
) -> ObjectInfo {
  let meta = meta_subject(self.name, name)
  guard chunk_size > 0 && chunk_size <= 1048576 else {
    raise @js.Invalid("chunk size")
  }
  let old = try self.get_info(name) catch {
    @js.Api(404, _, _) => None
    e => raise e
  } noraise {
    v => Some(v)
  }
  let id = self.js.client.new_inbox().replace_all(old=".", new="_")
  let chunk_subject = "$O." + self.name + ".C." + id
  let mut committing = false
  errdefer @async.protect_from_cancel(() => {
    if committing {
      return
    }
    self.js.purge_stream("OBJ_" + self.name, filter=chunk_subject) catch {
      _ => ()
    }
  })
  let hash = @crypto.SHA256::new()
  let mut size = 0L
  let mut chunks = 0
  while next() is Some(bytes) {
    let mut offset = 0
    while offset < bytes.length() {
      let end = (offset + chunk_size).min(bytes.length())
      let chunk = bytes[offset:end].to_owned()
      ignore(self.js.publish(chunk_subject, chunk))
      hash.update(chunk)
      size += chunk.length().to_int64()
      chunks += 1
      offset = end
    }
  }
  let digest = "SHA-256=" + url64(Bytes::from_array(hash.finalize()[:]))
  let modified = @time.unix(@env.now().reinterpret_as_int64() / 1000L)
    .to_plain_date_time()
    .to_string() +
    "Z"
  let j : Json = {
    "name": Json(name),
    "description": Json(description),
    "bucket": Json(self.name),
    "nuid": Json(id),
    "size": @js.integer(size),
    "chunks": Json(chunks),
    "digest": Json(digest),
    "mtime": Json(modified),
    "options": { "max_chunk_size": Json(chunk_size) },
  }
  committing = true
  ignore(
    self.js.publish(
      meta,
      @utf8.encode(j.stringify()),
      headers=@wire.Headers::new(entries=[("Nats-Rollup", "sub")]),
    ),
  )
  if old is Some(previous) {
    self.js.purge_stream(
      "OBJ_" + self.name,
      filter="$O." + self.name + ".C." + previous.nuid,
    )
  }
  info(j)
}

///|
pub async fn Bucket::put(
  self : Bucket,
  name : String,
  bytes : Bytes,
  chunk_size~ : Int32 = 131072,
) -> ObjectInfo {
  let mut sent = false
  self.put_stream(
    name,
    () => {
      if sent {
        None
      } else {
        sent = true
        Some(bytes)
      }
    },
    chunk_size~,
  )
}

///|
pub async fn Bucket::get_to(
  self : Bucket,
  name : String,
  sink : async (Bytes) -> Unit,
) -> ObjectInfo {
  let info = self.get_info(name)
  guard !info.deleted else { raise @js.Invalid("object deleted") }
  guard info.size >= 0L && info.chunks >= 0 && info.chunks <= 10000000 else {
    raise @js.Invalid("object limits")
  }
  let hash = @crypto.SHA256::new()
  let mut size = 0L
  let mut received = 0
  if info.chunks > 0 {
    let consumer_name = "MN_" +
      self.js.client.new_inbox().replace_all(old=".", new="_")
    let c = self.js.create_consumer_config("OBJ_" + self.name, consumer_name, {
      "ack_policy": "none",
      "deliver_policy": "all",
      "filter_subject": Json("$O." + self.name + ".C." + info.nuid),
    })
    defer @async.protect_from_cancel(() => c.delete() catch { _ => () })
    while received < info.chunks {
      let messages = c.fetch(batch=(info.chunks - received).min(64))
      guard !messages.is_empty() else {
        raise @js.Invalid("missing object chunks")
      }
      for msg in messages {
        hash.update(msg.payload)
        size += msg.payload.length().to_int64()
        received += 1
        guard size <= info.size else {
          raise @js.Invalid("object size exceeded")
        }
        sink(msg.payload)
      }
    }
  }
  let digest = "SHA-256=" + url64(Bytes::from_array(hash.finalize()[:]))
  guard size == info.size && digest == info.digest else {
    raise @js.Invalid("object integrity mismatch")
  }
  info
}

///|
pub async fn Bucket::get(
  self : Bucket,
  name : String,
  max_bytes~ : Int32 = 67108864,
) -> Bytes {
  guard max_bytes >= 0 else { raise @js.Invalid("object memory limit") }
  let out : Array[Byte] = []
  ignore(
    self.get_to(name, bytes => {
      guard bytes.length() <= max_bytes - out.length() else {
        raise @js.Invalid("object memory limit exceeded")
      }
      for byte in bytes {
        out.push(byte)
      }
    }),
  )
  Bytes::from_array(out)
}

///|
pub async fn Bucket::delete(self : Bucket, name : String) -> Unit {
  let old = self.get_info(name)
  guard old.raw is Object(m) else { raise @js.Invalid("metadata") }
  let deleted = m.copy()
  deleted["deleted"] = true
  deleted["size"] = 0
  deleted["chunks"] = 0
  deleted["digest"] = Json("")
  ignore(
    self.js.publish(
      meta_subject(self.name, name),
      @utf8.encode(Json::object(deleted).stringify()),
      headers=@wire.Headers::new(entries=[("Nats-Rollup", "sub")]),
    ),
  )
  self.js.purge_stream(
    "OBJ_" + self.name,
    filter="$O." + self.name + ".C." + old.nuid,
  )
}

///|
pub async fn Bucket::list(self : Bucket) -> Array[ObjectInfo] {
  let consumer_name = "MN_" +
    self.js.client.new_inbox().replace_all(old=".", new="_")
  let c = self.js.create_consumer_config("OBJ_" + self.name, consumer_name, {
    "ack_policy": "none",
    "deliver_policy": "last_per_subject",
    "filter_subject": Json("$O." + self.name + ".M.>"),
  })
  defer @async.protect_from_cancel(() => c.delete() catch { _ => () })
  let out = []
  while true {
    let batch = c.fetch(batch=64, expires_ms=200)
    for m in batch {
      let item = info(@js.decode_json(m.payload))
      if !item.deleted {
        out.push(item)
      }
    }
    if batch.length() < 64 {
      break
    }
  }
  out
}

///|
pub async fn Bucket::destroy(self : Bucket) -> Unit {
  self.js.delete_stream("OBJ_" + self.name)
}

///|
pub async fn Bucket::status(self : Bucket) -> Json {
  self.js.stream_info("OBJ_" + self.name)
}

///|
pub async fn Bucket::seal(self : Bucket) -> Unit {
  let status = self.status()
  guard status is { "config": Object(config), .. } else {
    raise @js.Invalid("bucket configuration")
  }
  let updated = config.copy()
  updated["sealed"] = true
  ignore(self.js.update_stream("OBJ_" + self.name, Json::object(updated)))
}

///|
async fn Bucket::save_info(self : Bucket, raw : Json) -> ObjectInfo {
  let item = info(raw)
  ignore(
    self.js.publish(
      meta_subject(self.name, item.name),
      @utf8.encode(raw.stringify()),
      headers=@wire.Headers::new(entries=[("Nats-Rollup", "sub")]),
    ),
  )
  item
}

///|
pub async fn Bucket::update_metadata(
  self : Bucket,
  name : String,
  description : String,
  metadata~ : Json = {},
) -> ObjectInfo {
  let previous = self.get_info(name)
  guard !previous.deleted &&
    metadata is Object(_) &&
    previous.raw is Object(fields) else {
    raise @js.Invalid("object metadata update")
  }
  let updated = fields.copy()
  updated["description"] = Json(description)
  updated["metadata"] = metadata
  self.save_info(Json::object(updated))
}

///|
async fn Bucket::link(
  self : Bucket,
  name : String,
  bucket : String,
  target : String?,
) -> ObjectInfo {
  @js.validate_name(bucket)
  ignore(meta_subject(self.name, name))
  let exists = try {
    ignore(self.get_info(name))
    true
  } catch {
    @js.Api(404, _, _) => false
    e => raise e
  }
  guard !exists else { raise @js.Invalid("link name already exists") }
  let link : Map[String, Json] = { "bucket": Json(bucket) }
  if target is Some(value) {
    link["name"] = Json(value)
  }
  self.save_info({
    "name": Json(name),
    "bucket": Json(self.name),
    "nuid": Json(self.js.client.new_inbox().replace_all(old=".", new="_")),
    "size": 0,
    "chunks": 0,
    "digest": "",
    "mtime": Json(
      @time.unix(@env.now().reinterpret_as_int64() / 1000L)
      .to_plain_date_time()
      .to_string() +
      "Z",
    ),
    "options": { "link": Json::object(link) },
  })
}

///|
pub async fn Bucket::add_link(
  self : Bucket,
  name : String,
  target_bucket : String,
  target_name : String,
) -> ObjectInfo {
  let bucket = open(self.js, target_bucket)
  let target = bucket.get_info(target_name)
  guard !target.deleted && !(target.raw is { "options": { "link": _, .. }, .. }) else {
    raise @js.Invalid("link target must be a live object")
  }
  self.link(name, target_bucket, Some(target_name))
}

///|
pub async fn Bucket::add_bucket_link(
  self : Bucket,
  name : String,
  target_bucket : String,
) -> ObjectInfo {
  ignore(open(self.js, target_bucket).status())
  self.link(name, target_bucket, None)
}

///|
pub async fn Bucket::linked_bucket(self : Bucket, name : String) -> Bucket {
  let item = self.get_info(name)
  guard !item.deleted &&
    item.raw is { "options": { "link": Object(link), .. }, .. } &&
    link.get("name") is None else {
    raise @js.Invalid("not a bucket link")
  }
  open(self.js, @js.text_field(Json::object(link), "bucket"))
}

///|
pub struct Watch {
  priv reader : @js.PushReader
}

///|
pub async fn Bucket::watch(self : Bucket, updates_only~ : Bool = true) -> Watch {
  let name = "MN_" + self.js.client.new_inbox().replace_all(old=".", new="_")
  let reader = self.js.push_consumer(
    "OBJ_" + self.name,
    name,
    filter="$O." + self.name + ".M.>",
    ack_policy=@js.NoAck,
    deliver_policy=if updates_only { "new" } else { "last_per_subject" },
  )
  { reader, }
}

///|
pub async fn Watch::next(
  self : Watch,
  timeout_ms~ : Int32 = 5000,
) -> ObjectInfo {
  info(@js.decode_json(self.reader.next(timeout_ms~).payload))
}

///|
pub async fn Watch::close(self : Watch) -> Unit {
  self.reader.close()
}

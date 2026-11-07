///|
pub(all) enum CacheEvent {
  Snapshot(Array[Entry])
  Updated(Entry)
}

///|
pub struct ConfigCache {
  priv bucket : Bucket
  priv pattern : String
  priv max_keys : Int32
  priv values : Map[String, CachedEntry]
  priv mut max_age_ns : Int64
  priv mut consumer : @js.Consumer
  priv mut generation : Int64
  priv mut delivered : Int64
  priv mut ready_ : Bool
  priv mut initial : Bool
  priv mut closed : Bool
  priv lock : @async.Mutex
}

///|
async fn Bucket::snapshot_consumer(
  self : Bucket,
  pattern : String,
) -> @js.Consumer {
  key_valid(pattern, true)
  let name = "MN_" + self.js.client.new_inbox().replace_all(old=".", new="_")
  self.js.create_consumer_config("KV_" + self.name, name, {
    "ack_policy": "none",
    "deliver_policy": "last_per_subject",
    "filter_subject": Json("$KV." + self.name + "." + pattern),
    "inactive_threshold": @js.integer(300000000000L),
  })
}

///|
pub async fn Bucket::config_cache(
  self : Bucket,
  pattern~ : String = ">",
  max_keys~ : Int32 = 10000,
) -> ConfigCache {
  guard max_keys > 0 && max_keys <= 1000000 else {
    raise @js.Invalid("cache key limit")
  }
  let consumer = self.snapshot_consumer(pattern)
  let cache : ConfigCache = {
    bucket: self,
    pattern,
    max_keys,
    values: Map([]),
    max_age_ns: 0L,
    consumer,
    generation: -1L,
    delivered: 0L,
    ready_: false,
    initial: true,
    closed: false,
    lock: @async.Mutex(),
  }
  errdefer @async.protect_from_cancel(() => cache.close() catch { _ => () })
  cache.load_snapshot()
  cache
}

///|
priv struct CachedEntry {
  entry : Entry
  expires_at_ms : Int64?
}

///|
fn CachedEntry::expired(self : CachedEntry, now_ms : Int64) -> Bool {
  self.expires_at_ms is Some(deadline) && deadline <= now_ms
}

///|
fn apply(
  values : Map[String, CachedEntry],
  entry : Entry,
  timestamp_ns : Int64,
  max_age_ns : Int64,
  max_keys : Int32,
) -> Entry raise {
  if removed(entry.operation) {
    values.remove(entry.key)
    return entry
  }
  let now_ms = @env.now().reinterpret_as_int64()
  let expires_at_ms = if max_age_ns > 0L {
    guard timestamp_ns > 0L else {
      raise @js.Invalid("missing KV message timestamp")
    }
    Some(timestamp_ns / 1000000L + max_age_ns / 1000000L)
  } else {
    None
  }
  let cached : CachedEntry = { entry, expires_at_ms, }
  if cached.expired(now_ms) {
    values.remove(entry.key)
    return { ..entry, operation: "EXPIRE", value: b"", }
  }
  if !values.contains(entry.key) && values.length() >= max_keys {
    let expired = values
      .iter()
      .filter_map(pair => {
        if pair.1.expired(now_ms) {
          Some(pair.0)
        } else {
          None
        }
      })
      .to_array()
    for key in expired {
      values.remove(key)
    }
    guard values.length() < max_keys else {
      raise @js.Invalid("cache key limit exceeded")
    }
  }
  values[entry.key] = cached
  entry
}

///|
async fn ConfigCache::load_snapshot(self : ConfigCache) -> Unit {
  self.ready_ = false
  let generation = self.bucket.js.client.statistics().reconnects
  let staged : Map[String, CachedEntry] = Map([])
  self.delivered = 0L
  @async.with_timeout(10000, () => {
    let stream = self.bucket.js.stream_info("KV_" + self.bucket.name)
    guard stream is { "config": config, .. } else {
      raise @js.Invalid("KV stream config")
    }
    self.max_age_ns = @js.integer_field(config, "max_age")
    guard self.max_age_ns >= 0L else { raise @js.Invalid("KV stream max_age") }
    let status = self.consumer.info()
    let mut pending = @js.integer_field(status, "num_pending")
    while pending > 0L {
      let batch = self.consumer.fetch(batch=64, expires_ms=250)
      if batch.is_empty() {
        // Entries may have expired since the pending count was sampled.
        guard @js.integer_field(self.consumer.info(), "num_pending") == 0L else {
          raise @js.Invalid("snapshot incomplete")
        }
        break
      }
      for msg in batch {
        let meta = @js.metadata(msg)
        guard meta.consumer_sequence == self.delivered + 1L else {
          raise @js.Invalid("snapshot sequence gap")
        }
        self.delivered = meta.consumer_sequence
        let entry = self.bucket.entry(msg)
        ignore(
          apply(
            staged,
            entry,
            meta.timestamp_ns,
            self.max_age_ns,
            self.max_keys,
          ),
        )
        pending = entry.pending
      }
    }
  })
  guard generation == self.bucket.js.client.statistics().reconnects else {
    raise @client.Disconnected
  }
  self.values.clear()
  for k, v in staged {
    self.values[k] = v
  }
  self.generation = generation
  self.ready_ = true
}

///|
pub fn ConfigCache::ready(self : ConfigCache) -> Bool {
  self.ready_ &&
  !self.closed &&
  self.generation == self.bucket.js.client.statistics().reconnects &&
  self.bucket.js.client.state() == @client.Connected
}

///|
pub fn ConfigCache::get(self : ConfigCache, key : String) -> Entry? {
  guard self.ready() && self.values.get(key) is Some(cached) else {
    return None
  }
  if cached.expired(@env.now().reinterpret_as_int64()) {
    self.values.remove(key)
    None
  } else {
    Some(cached.entry)
  }
}

///|
pub fn ConfigCache::snapshot(self : ConfigCache) -> Array[Entry] raise {
  guard self.ready() else { raise @js.Invalid("cache needs resynchronization") }
  let now_ms = @env.now().reinterpret_as_int64()
  self.values
  .values()
  .filter_map(cached => {
    if cached.expired(now_ms) {
      None
    } else {
      Some(cached.entry)
    }
  })
  .to_array()
}

///|
async fn ConfigCache::resync(self : ConfigCache) -> Unit {
  self.ready_ = false
  self.consumer.delete() catch {
    _ => ()
  }
  self.consumer = self.bucket.snapshot_consumer(self.pattern)
  self.load_snapshot()
}

///|
/// Call next continuously to advance the local view. Reconnect emits a fresh snapshot.
pub async fn ConfigCache::next(
  self : ConfigCache,
  timeout_ms~ : Int32 = 5000,
) -> CacheEvent {
  guard !self.closed && timeout_ms > 0 else {
    raise @js.Invalid("cache closed or invalid timeout")
  }
  @async.with_timeout(timeout_ms, () => {
    self.lock.acquire()
    defer self.lock.release()
    if !self.ready() {
      self.resync()
      self.initial = true
    }
    if self.initial {
      self.initial = false
      return Snapshot(self.snapshot())
    }
    while true {
      let batch = self.consumer.fetch(expires_ms=250) catch {
        @client.Disconnected
        | @js.Api(404, _, _)
        | @js.Status(404, _)
        | @js.Status(409, _)
        | @js.Status(503, _) => {
          self.resync()
          return Snapshot(self.snapshot())
        }
        e => {
          self.ready_ = false
          raise e
        }
      }
      if self.generation != self.bucket.js.client.statistics().reconnects {
        self.resync()
        return Snapshot(self.snapshot())
      }
      if batch.is_empty() {
        continue
      }
      let msg = batch[0]
      let meta = @js.metadata(msg)
      if meta.consumer_sequence != self.delivered + 1L {
        self.resync()
        return Snapshot(self.snapshot())
      }
      self.delivered = meta.consumer_sequence
      let entry = self.bucket.entry(msg)
      errdefer {
        self.ready_ = false
      }
      return Updated(
        apply(
          self.values,
          entry,
          meta.timestamp_ns,
          self.max_age_ns,
          self.max_keys,
        ),
      )
    }
    raise @js.Invalid("unreachable")
  })
}

///|
pub async fn ConfigCache::close(self : ConfigCache) -> Unit {
  if !self.closed {
    self.closed = true
    self.ready_ = false
    self.consumer.delete()
  }
}

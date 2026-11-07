///|
pub(all) suberror ClientError {
  ConnectionClosed
  Disconnected
  NoResponders
  SlowConsumer
  InvalidOption(String)
  Server(String)
  RetryExhausted
  PublishUncertain
} derive(Debug)

///|
pub extend ClientError with @debug.Debug::{to_repr}

///|
pub(all) enum State {
  Connecting
  Connected
  Reconnecting
  Draining
  Closed
} derive(Eq, Debug)

///|
pub extend State with Eq::{equal, not_equal}

///|
pub extend State with @debug.Debug::{to_repr}

///|
pub(all) struct Options {
  servers : Array[String]
  connect : @wire.ConnectOptions
  timeout_ms : Int32
  reconnect_attempts : Int32
  reconnect_delay_ms : Int32
  heartbeat_ms : Int32
  max_pending_bytes : Int32
  subscription_capacity : Int32
  trust : Trust
  on_state : (State) -> Unit
  signer : ((String) -> @wire.ConnectOptions raise)?
  credentials : (async (String?) -> @wire.ConnectOptions)?
  tls_first : Bool
  require_tls : Bool
  reconnect_jitter_ms : Int32
  reconnect_buffer_bytes : Int32
  on_event : (ClientEvent) -> Unit
}

///|
pub fn Options::new(
  servers~ : Array[String] = ["nats://127.0.0.1:4222"],
  connect~ : @wire.ConnectOptions = @wire.ConnectOptions::new(),
  timeout_ms~ : Int32 = 5000,
  reconnect_attempts~ : Int32 = 10,
  reconnect_delay_ms~ : Int32 = 100,
  heartbeat_ms~ : Int32 = 30000,
  max_pending_bytes~ : Int32 = 8388608,
  subscription_capacity~ : Int32 = 1024,
  trust~ : Trust = SystemRoots,
  on_state~ : (State) -> Unit = _ => (),
  signer? : (String) -> @wire.ConnectOptions raise,
  credentials? : async (String?) -> @wire.ConnectOptions,
  tls_first~ : Bool = false,
  require_tls~ : Bool = false,
  reconnect_jitter_ms~ : Int32 = 100,
  reconnect_buffer_bytes~ : Int32 = 0,
  on_event~ : (ClientEvent) -> Unit = _ => (),
) -> Options {
  {
    servers: servers.copy(),
    connect,
    timeout_ms,
    reconnect_attempts,
    reconnect_delay_ms,
    heartbeat_ms,
    max_pending_bytes,
    subscription_capacity,
    trust,
    on_state,
    signer,
    credentials,
    tls_first,
    require_tls,
    reconnect_jitter_ms,
    reconnect_buffer_bytes,
    on_event,
  }
}

///|
pub(all) struct Received {
  subject : String
  reply_to : String?
  payload : Bytes
  headers : @wire.Headers
}

///|
pub struct Subscription {
  sid : Int32
  subject : String
  priv queue_group : String?
  priv inbox : @aqueue.Queue[Received]
  priv mut remaining : Int32?
  priv mut pending : Int32
  priv mut pending_bytes : Int32
  priv mut received : Int64
  priv mut closed : Bool
  priv owner : String
  priv transient : Bool
  priv mut active : Int32
}

///|
pub(all) struct Statistics {
  sent_messages : Int64
  received_messages : Int64
  sent_bytes : Int64
  received_bytes : Int64
  reconnects : Int64
  pending_bytes : Int32
  subscriptions : Int32
  slow_consumers : Int64
  buffered_bytes : Int32
  requests : Int64
  request_errors : Int64
  request_time_ms : Int64
}

///|
pub struct Client {
  priv options : Options
  priv servers : Array[String]
  priv mut server_url : String
  priv retired_sids : Array[Int32]
  priv mut transport : Transport
  priv mut decoder : @wire.Decoder
  mut info : @wire.ServerInfo
  priv mut reader : @async.Task[Unit]?
  priv mut needs_reconnect : Bool
  priv mut state_ : State
  priv lock : @async.Mutex
  priv flush_lock : @async.Mutex
  priv mut pong : @aqueue.Queue[Unit]?
  priv subscriptions : Map[Int32, Subscription]
  priv mut next_sid : Int32
  priv inbox_prefix : String
  priv mut next_inbox : Int64
  priv mut epoch : Int32
  priv mut draining : Bool
  priv mut pending_bytes : Int32
  priv mut sent_messages : Int64
  priv mut received_messages : Int64
  priv mut sent_bytes : Int64
  priv mut received_bytes : Int64
  priv mut reconnects : Int64
  priv mut slow_consumers : Int64
  priv buffered : Array[Bytes]
  priv mut buffered_bytes : Int32
  priv mut requests : Int64
  priv mut request_errors : Int64
  priv mut request_time_ms : Int64
}

///|
pub fn Client::state(self : Client) -> State {
  self.state_
}

///|
fn Client::set_state(self : Client, state : State) -> Unit {
  self.state_ = state
  (self.options.on_state)(state)
}

///|
pub fn Client::statistics(self : Client) -> Statistics {
  self.prune()
  {
    sent_messages: self.sent_messages,
    received_messages: self.received_messages,
    sent_bytes: self.sent_bytes,
    received_bytes: self.received_bytes,
    reconnects: self.reconnects,
    pending_bytes: self.pending_bytes,
    subscriptions: self.subscriptions.length(),
    slow_consumers: self.slow_consumers,
    buffered_bytes: self.buffered_bytes,
    requests: self.requests,
    request_errors: self.request_errors,
    request_time_ms: self.request_time_ms,
  }
}

///|
fn unique_id() -> String raise {
  let b = StringBuilder()
  let bytes = @env.rand(16).unwrap_or_else(() => {
    raise InvalidOption("secure random unavailable")
  })
  for v in bytes {
    b.write_string(v.to_int().to_string(radix=16).pad_start(2, '0'))
  }
  b.to_string()
}

///|
pub async fn[X] with_client(options : Options, f : async (Client) -> X) -> X {
  guard !options.servers.is_empty() &&
    options.timeout_ms > 0 &&
    options.reconnect_attempts >= 0 &&
    options.reconnect_delay_ms >= 0 &&
    options.reconnect_delay_ms <= 60000 &&
    options.heartbeat_ms >= 0 &&
    options.reconnect_jitter_ms >= 0 &&
    options.reconnect_jitter_ms <= 60000 &&
    options.reconnect_buffer_bytes >= 0 &&
    options.subscription_capacity > 0 &&
    options.max_pending_bytes > 0 else {
    raise InvalidOption("invalid client limits")
  }
  (options.on_state)(Connecting)
  let mut connection = None
  let mut last_error : Error = RetryExhausted
  for url in options.servers {
    try {
      let (transport, decoder, info) = open(url, options)
      connection = Some((transport, decoder, info, url))
      break
    } catch {
      e => last_error = e
    }
  }
  guard connection is Some((transport, decoder, info, url)) else {
    raise last_error
  }
  let c = {
    options,
    servers: options.servers.copy(),
    server_url: url,
    retired_sids: [],
    transport,
    decoder,
    info,
    reader: None,
    needs_reconnect: false,
    state_: Connected,
    lock: @async.Mutex(),
    flush_lock: @async.Mutex(),
    pong: None,
    subscriptions: {},
    next_sid: 1,
    inbox_prefix: "_INBOX." + unique_id(),
    next_inbox: 0L,
    epoch: 0,
    draining: false,
    pending_bytes: 0,
    sent_messages: 0L,
    received_messages: 0L,
    sent_bytes: 0L,
    received_bytes: 0L,
    reconnects: 0L,
    slow_consumers: 0L,
    buffered: [],
    buffered_bytes: 0,
    requests: 0L,
    request_errors: 0L,
    request_time_ms: 0L,
  }
  defer {
    c.close()
    c.transport.close()
  }
  c.remember(c.info, initial=true)
  (options.on_state)(Connected)
  let mut result : X? = None
  @async.with_task_group(group => {
    defer c.close()
    let reader = group.spawn(no_wait=true, () => c.read_loop())
    c.reader = Some(reader)
    if options.heartbeat_ms > 0 {
      group.spawn_bg(no_wait=true, () => {
        while c.state_ != Closed {
          @async.sleep(options.heartbeat_ms)
          if c.state_ == Connected {
            c.flush() catch {
              _ => c.needs_reconnect = true
            }
          }
        }
      })
    }
    result = Some(f(c))
    c.close()
    reader.cancel()
  })
  result.unwrap()
}

///|
pub fn Client::close(self : Client) -> Unit {
  if self.state_ == Closed {
    return
  }
  if self.reader is Some(task) {
    task.cancel()
  }
  self.set_state(Closed)
  if self.buffered_bytes > 0 {
    (self.options.on_event)(BufferDiscarded(self.buffered_bytes))
    self.buffered.clear()
    self.buffered_bytes = 0
  }
  if self.pong is Some(q) {
    q.close(error=ConnectionClosed)
  }
  for _, sub in self.subscriptions {
    sub.closed = true
    sub.inbox.close(error=ConnectionClosed)
  }
  self.subscriptions.clear()
}

///|
async fn Client::recover(self : Client, epoch : Int32) -> Unit {
  self.lock.acquire()
  defer self.lock.release()
  if self.epoch != epoch {
    return
  }
  guard self.state_ != Closed else { raise ConnectionClosed }
  self.set_state(Reconnecting)
  self.transport.close()
  if self.pong is Some(q) {
    q.close(error=Disconnected)
    self.pong = None
  }
  for (sid, sub) in self.subscriptions.iter().to_array() {
    if sub.transient {
      sub.closed = true
      sub.inbox.close(error=Disconnected)
      self.subscriptions.remove(sid)
    }
  }
  // In-flight requests have an uncertain outcome and must not be silently replayed.
  let mut success = false
  for attempt in 0..<self.options.reconnect_attempts {
    if @async.is_being_cancelled() {
      return
    }
    let delay = self.options.reconnect_delay_ms.to_int64() *
      (1L << attempt.min(8))
    let jitter = match @env.rand(2) {
      Some(b) =>
        (b[0].to_int() * 256 + b[1].to_int()) %
        (self.options.reconnect_jitter_ms + 1)
      None => 0
    }
    @async.sleep(delay.min(30000L).to_int() + jitter)
    for url in self.servers.copy() {
      let conn = Some(open(url, self.options)) catch { _ => None }
      if conn is Some((transport, decoder, info)) {
        let restored = try {
          for (_, sub) in self.subscriptions.iter().to_array() {
            if !sub.closed {
              transport.write(
                @wire.subscribe(sub.subject, sub.sid, queue?=sub.queue_group),
              )
              if sub.remaining is Some(n) {
                transport.write(@wire.unsubscribe(sub.sid, max_messages=n))
              }
            }
          }
          // Remove before writing: a partial write must never be replayed.
          while !self.buffered.is_empty() {
            let frame = self.buffered.remove(0)
            self.buffered_bytes -= frame.length()
            errdefer (self.options.on_event)(WriteUncertain)
            transport.write(frame)
          }
          true
        } catch {
          _ => {
            transport.close()
            false
          }
        }
        if restored {
          self.transport = transport
          self.decoder = decoder
          self.info = info
          self.server_url = url
          self.remember(info, initial=true)
          self.epoch += 1
          self.needs_reconnect = false
          self.reconnects += 1
          self.set_state(if self.draining { Draining } else { Connected })
          success = true
          break
        }
      }
    }
    if success {
      break
    }
  }
  if !success {
    self.close()
    raise RetryExhausted
  }
}

///|
async fn Client::send(
  self : Client,
  bytes : Bytes,
  generation? : Int32,
) -> Unit {
  guard self.state_ != Closed else { raise ConnectionClosed }
  guard bytes.length() <= self.options.max_pending_bytes - self.pending_bytes else {
    raise SlowConsumer
  }
  self.pending_bytes += bytes.length()
  defer {
    self.pending_bytes -= bytes.length()
  }
  self.lock.acquire()
  defer self.lock.release()
  guard self.state_ != Closed else { raise ConnectionClosed }
  if generation is Some(epoch) {
    guard self.epoch == epoch &&
      !self.needs_reconnect &&
      self.state_ != Reconnecting else {
      raise Disconnected
    }
  }
  errdefer {
    self.needs_reconnect = true
  }
  self.transport.write(bytes) catch {
    _ => {
      self.needs_reconnect = true
      (self.options.on_event)(WriteUncertain)
      raise PublishUncertain
    }
  }
}

///|
fn Client::dispatch(
  self : Client,
  msg : @wire.Message,
  headers : @wire.Headers,
) -> Unit raise {
  self.received_messages += 1
  self.received_bytes += msg.payload.length().to_int64()
  if self.subscriptions.get(msg.sid) is Some(sub) && !sub.closed {
    let item = {
      subject: msg.subject,
      reply_to: msg.reply_to,
      payload: msg.payload,
      headers,
    }
    let cost = msg.payload.length() + headers.encode().length()
    if cost > self.options.max_pending_bytes - sub.pending_bytes ||
      !sub.inbox.try_put(item) {
      sub.closed = true
      sub.inbox.close(error=SlowConsumer)
      self.slow_consumers += 1
      (self.options.on_event)(SubscriptionSlow(sub.subject))
      self.subscriptions.remove(sub.sid)
      self.retired_sids.push(sub.sid)
    } else {
      sub.pending += 1
      sub.pending_bytes += cost
      sub.received += 1
      if sub.remaining is Some(n) {
        sub.remaining = Some(n - 1)
        if n == 1 {
          sub.closed = true
          sub.inbox.close()
        }
      }
    }
  }
}

///|
async fn Client::read_loop(self : Client) -> Unit {
  errdefer self.close()
  while self.state_ != Closed && !@async.is_being_cancelled() {
    let epoch = self.epoch
    if self.needs_reconnect {
      self.recover(epoch)
      continue
    }
    let result = @async.with_timeout(1000, () => self.transport.read()) catch {
      @async.TimeoutError => {
        if self.state_ == Closed {
          break
        }
        if self.needs_reconnect {
          None
        } else {
          continue
        }
      }
      _ => None
    }
    if result is Some(bytes) {
      let events = self.decoder.feed(bytes)
      for event in events {
        match event {
          Msg(m) => self.dispatch(m, @wire.Headers::new())
          Hmsg(m, h) => self.dispatch(m, h)
          Ping => self.send(b"PONG\r\n") catch { _ => () }
          Pong =>
            if self.pong is Some(q) {
              ignore(q.try_put(()))
              self.pong = None
            }
          Info(s) => {
            self.info = @wire.ServerInfo::parse(s)
            self.remember(self.info)
            if self.info.raw is { "ldm": True, .. } {
              (self.options.on_event)(LameDuck)
              self.needs_reconnect = true
            }
          }
          ServerError(s) => {
            self.close()
            raise Server(s)
          }
          Ok => ()
        }
      }
      while self.retired_sids.pop() is Some(sid) {
        self.send(@wire.unsubscribe(sid)) catch {
          _ => self.needs_reconnect = true
        }
      }
    } else {
      if self.state_ == Closed {
        break
      }
      self.recover(epoch)
    }
  }
}

///|
pub async fn Client::publish(
  self : Client,
  subject : String,
  payload : Bytes,
  headers? : @wire.Headers,
  reply_to? : String,
  buffer_on_reconnect~ : Bool = true,
) -> Unit {
  guard self.state_ != Closed else { raise ConnectionClosed }
  guard payload.length() <= self.info.max_payload else {
    raise InvalidOption("server max_payload")
  }
  let bytes = match headers {
    Some(h) => {
      guard h.encode().length() <= self.info.max_payload - payload.length() else {
        raise InvalidOption("server max_payload including headers")
      }
      @wire.publish_headers(subject, h, payload, reply_to?)
    }
    None => @wire.publish(subject, payload, reply_to?)
  }
  if self.state_ == Reconnecting || self.needs_reconnect {
    guard buffer_on_reconnect && self.options.reconnect_buffer_bytes > 0 else {
      raise Disconnected
    }
    guard bytes.length() <=
      self.options.reconnect_buffer_bytes - self.buffered_bytes else {
      raise SlowConsumer
    }
    // Requests must fail, rather than queue a reply subject that may expire.
    guard reply_to is None else { raise Disconnected }
    self.buffered.push(bytes)
    self.buffered_bytes += bytes.length()
  } else {
    let generation = if reply_to is Some(_) || !buffer_on_reconnect {
      Some(self.epoch)
    } else {
      None
    }
    self.send(bytes, generation?)
  }
  self.sent_messages += 1
  self.sent_bytes += payload.length().to_int64()
}

///|
pub async fn Client::subscribe(
  self : Client,
  subject : String,
  queue? : String,
  max_messages? : Int32,
  capacity? : Int32,
  restore~ : Bool = true,
) -> Subscription {
  guard !self.draining && self.state_ != Closed else { raise ConnectionClosed }
  if !restore {
    guard self.state_ == Connected && !self.needs_reconnect else {
      raise Disconnected
    }
  }
  self.prune()
  guard self.subscriptions.length() < 4096 else {
    raise InvalidOption("subscription limit")
  }
  let generation = if restore { None } else { Some(self.epoch) }
  let sid = self.next_sid
  guard sid < 2147483647 else {
    raise InvalidOption("subscription id exhausted")
  }
  self.next_sid += 1
  let wire = @wire.subscribe(subject, sid, queue?)
  let cap = capacity.unwrap_or(self.options.subscription_capacity)
  guard cap > 0 else { raise InvalidOption("subscription capacity") }
  if max_messages is Some(n) {
    guard n > 0 else { raise InvalidOption("message limit") }
  }
  let sub = {
    sid,
    subject,
    queue_group: queue,
    inbox: @aqueue.Queue(kind=Blocking(cap)),
    remaining: max_messages,
    pending: 0,
    pending_bytes: 0,
    received: 0L,
    closed: false,
    owner: self.inbox_prefix,
    transient: !restore,
    active: 0,
  }
  self.subscriptions[sid] = sub
  errdefer {
    self.subscriptions.remove(sid)
    sub.inbox.close(error=ConnectionClosed)
  }
  self.send(wire, generation?)
  if max_messages is Some(n) {
    self.send(@wire.unsubscribe(sid, max_messages=n), generation?)
  }
  sub
}

///|
pub async fn Client::unsubscribe(self : Client, sub : Subscription) -> Unit {
  guard sub.owner == self.inbox_prefix else {
    raise InvalidOption("subscription belongs to another client")
  }
  self.subscriptions.remove(sub.sid)
  sub.closed = true
  sub.inbox.close()
  if self.state_ != Closed {
    self.send(@wire.unsubscribe(sub.sid))
  }
}

///|
pub async fn Subscription::next(
  self : Subscription,
  timeout_ms~ : Int32 = 5000,
) -> Received {
  guard timeout_ms > 0 else { raise InvalidOption("timeout") }
  let item = @async.with_timeout(timeout_ms, () => self.inbox.get())
  self.pending -= 1
  self.pending_bytes -= item.payload.length() + item.headers.encode().length()
  item
}

///|
pub fn Subscription::pending(self : Subscription) -> Int32 {
  self.pending
}

///|
pub async fn Client::flush(self : Client, timeout_ms? : Int32) -> Unit {
  self.flush_lock.acquire()
  defer self.flush_lock.release()
  let q = @aqueue.Queue(kind=Blocking(1))
  self.pong = Some(q)
  errdefer {
    self.pong = None
    self.needs_reconnect = true
  }
  self.send(b"PING\r\n")
  @async.with_timeout(timeout_ms.unwrap_or(self.options.timeout_ms), () => {
    q.get()
  })
}

///|
pub async fn Client::request(
  self : Client,
  subject : String,
  payload : Bytes,
  headers? : @wire.Headers,
  timeout_ms? : Int32,
) -> Received {
  let timeout = timeout_ms.unwrap_or(self.options.timeout_ms)
  guard timeout > 0 else { raise InvalidOption("request timeout") }
  self.requests += 1
  let started = @async.now()
  defer {
    self.request_time_ms += @async.now() - started
  }
  errdefer {
    self.request_errors += 1
  }
  @async.with_timeout(timeout, () => {
    let inbox = self.new_inbox()
    let sub = self.subscribe(inbox, max_messages=1, restore=false)
    defer @async.protect_from_cancel(() => {
      @async.with_timeout(250, () => self.unsubscribe(sub)) catch {
        _ => ()
      }
    })
    self.publish(subject, payload, headers?, reply_to=inbox)
    let msg = sub.next(timeout_ms=timeout)
    if msg.headers.status == Some(503) {
      raise NoResponders
    }
    msg
  })
}

///|
pub async fn Client::reply(
  self : Client,
  msg : Received,
  payload : Bytes,
  headers? : @wire.Headers,
) -> Unit {
  guard msg.reply_to is Some(reply) else {
    raise InvalidOption("message has no reply subject")
  }
  self.publish(reply, payload, headers?)
}

///|
pub async fn Client::drain(self : Client, timeout_ms~ : Int32 = 5000) -> Unit {
  guard timeout_ms > 0 else { raise InvalidOption("drain timeout") }
  defer self.close()
  self.draining = true
  self.set_state(Draining)
  @async.with_timeout(timeout_ms, () => {
    for (sid, _) in self.subscriptions.iter().to_array() {
      self.send(@wire.unsubscribe(sid))
    }
    self.flush()
    while self.subscriptions.iter().any(p => p.1.pending > 0 || p.1.active > 0) {
      @async.sleep(1)
    }
    self.close()
  })
}

///|
pub fn Client::new_inbox(self : Client) -> String {
  self.next_inbox += 1L
  self.inbox_prefix + "." + self.next_inbox.to_string()
}

///|
pub(all) enum Trust {
  SystemRoots
  CustomPemFile(String)
}

///|
/// Track an asynchronous handler so drain waits for the handler to complete.
pub async fn Subscription::process_next(
  self : Subscription,
  handler : async (Received) -> Unit,
  timeout_ms~ : Int32 = 5000,
) -> Unit {
  let msg = self.next(timeout_ms~)
  self.active += 1
  defer {
    self.active -= 1
  }
  handler(msg)
}

///|
fn Client::prune(self : Client) -> Unit {
  for (sid, sub) in self.subscriptions.iter().to_array() {
    if sub.closed && sub.pending == 0 && sub.active == 0 {
      self.subscriptions.remove(sid)
    }
  }
}

///|
fn Client::remember(
  self : Client,
  info : @wire.ServerInfo,
  initial~ : Bool = false,
) -> Unit {
  // In standard TCP/TLS the first INFO arrives before server authentication.
  if initial &&
    self.options.require_tls &&
    self.server_url.has_prefix("tls://") &&
    !self.options.tls_first {
    return
  }
  let initial = self.server_url
  let websocket = initial.has_prefix("ws://") || initial.has_prefix("wss://")
  let prefix = if websocket {
    if initial.has_prefix("wss://") {
      "wss://"
    } else {
      "ws://"
    }
  } else if initial.has_prefix("tls://") || info.tls_required {
    "tls://"
  } else {
    "nats://"
  }
  let urls = if websocket {
    match info.raw {
      { "ws_connect_urls": Array(values), .. } =>
        values
        .iter()
        .filter_map(v => {
          match v {
            String(s) => Some(s)
            _ => None
          }
        })
        .to_array()
      _ => []
    }
  } else {
    info.connect_urls
  }
  for url in urls {
    let full = prefix + url
    if self.servers.length() < 64 && !self.servers.contains(full) {
      self.servers.push(full)
    }
  }
}

///|
pub fn Subscription::received_count(self : Subscription) -> Int64 {
  self.received
}

///|
pub(all) enum ClientEvent {
  LameDuck
  SubscriptionSlow(String)
  WriteUncertain
  BufferDiscarded(Int32)
} derive(Debug)

///|
pub extend ClientEvent with @debug.Debug::{to_repr}

///|
/// Wait for the active connection, including any recovery already scheduled.
pub async fn Client::wait_connected(
  self : Client,
  timeout_ms~ : Int32 = 5000,
) -> Unit {
  guard timeout_ms > 0 else { raise InvalidOption("connection wait timeout") }
  @async.with_timeout(timeout_ms, () => {
    while self.state_ != Connected || self.needs_reconnect {
      guard self.state_ != Closed && !self.draining else {
        raise ConnectionClosed
      }
      @async.sleep(10)
    }
  })
}

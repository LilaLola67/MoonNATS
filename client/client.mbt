///|
pub suberror ClientError {
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
  trust : @tls.TrustedRoot
  on_state : (State) -> Unit
  signer : ((String) -> @wire.ConnectOptions raise)?
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
  trust~ : @tls.TrustedRoot = @tls.SystemRoot,
  on_state~ : (State) -> Unit = _ => (),
  signer? : (String) -> @wire.ConnectOptions raise,
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
  queue_group : String?
  inbox : @aqueue.Queue[Received]
  mut remaining : Int32?
  mut pending : Int32
  mut pending_bytes : Int32
  mut received : Int64
  mut closed : Bool
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
}

///|
enum Transport {
  Tcp(@socket.Tcp)
  Secure(@tls.Tls)
  WebSocket(@ws.Conn)
}

///|
fn Transport::close(self : Transport) -> Unit {
  match self {
    Tcp(t) => t.close()
    Secure(t) => t.close()
    WebSocket(t) => t.close()
  }
}

///|
async fn Transport::read(self : Transport) -> Bytes? {
  match self {
    Tcp(t) => t.read_some(max_len=65536)
    Secure(t) => t.read_some(max_len=65536)
    WebSocket(t) => {
      let msg = t.recv()
      let chunks = []
      let mut size = 0
      while msg.read_some(max_len=65536) is Some(b) {
        size += b.length()
        guard size <= 16777216 else {
          raise InvalidOption("WebSocket frame limit")
        }
        chunks.push(b)
      }
      Some(chunks.iter().fold(init=b"", (a, b) => a + b))
    }
  }
}

///|
async fn Transport::write(self : Transport, bytes : Bytes) -> Unit {
  match self {
    Tcp(t) => t.write(bytes)
    Secure(t) => t.write(bytes)
    WebSocket(t) => t.send_binary(bytes)
  }
}

///|
pub struct Client {
  options : Options
  mut transport : Transport
  mut decoder : @wire.Decoder
  mut info : @wire.ServerInfo
  mut state_ : State
  lock : @async.Mutex
  flush_lock : @async.Mutex
  mut pong : @aqueue.Queue[Unit]?
  subscriptions : Map[Int32, Subscription]
  mut next_sid : Int32
  inbox_prefix : String
  mut next_inbox : Int64
  mut epoch : Int32
  mut draining : Bool
  mut pending_bytes : Int32
  mut sent_messages : Int64
  mut received_messages : Int64
  mut sent_bytes : Int64
  mut received_bytes : Int64
  mut reconnects : Int64
  mut slow_consumers : Int64
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
  {
    sent_messages: self.sent_messages,
    received_messages: self.received_messages,
    sent_bytes: self.sent_bytes,
    received_bytes: self.received_bytes,
    reconnects: self.reconnects,
    pending_bytes: self.pending_bytes,
    subscriptions: self.subscriptions.length(),
    slow_consumers: self.slow_consumers,
  }
}

///|
#warnings("-alert_internal")
fn unique_id() -> String raise {
  let b = StringBuilder()
  for v in @tls.rand_bytes(16) {
    b.write_string(v.to_int().to_string(radix=16).pad_start(2, '0'))
  }
  b.to_string()
}

///|
fn endpoint(url : String) -> (String, Int32, Bool) raise ClientError {
  let secure = url.has_prefix("tls://")
  let raw = if secure {
    url[6:].to_owned()
  } else if url.has_prefix("nats://") {
    url[7:].to_owned()
  } else {
    raise InvalidOption("use nats://, tls://, ws:// or wss://")
  }
  guard !raw.contains("@") && !raw.contains("/") else {
    raise InvalidOption("credentials belong in ConnectOptions")
  }
  if raw.has_prefix("[") {
    guard raw.find("]:") is Some(i) else {
      raise InvalidOption("IPv6 endpoint")
    }
    let port = @string.parse_int(raw[i + 2:]) catch {
      _ => raise InvalidOption("port")
    }
    guard port > 0 && port <= 65535 else { raise InvalidOption("port") }
    return (raw[1:i].to_owned(), port, secure)
  }
  let parts = raw.split(":").to_array()
  guard parts.length() == 2 && !parts[0].is_empty() else {
    raise InvalidOption("host:port required")
  }
  let port = @string.parse_int(parts[1]) catch {
    _ => raise InvalidOption("port")
  }
  guard port > 0 && port <= 65535 else { raise InvalidOption("port") }
  (parts[0].to_owned(), port, secure)
}

///|
async fn open(
  url : String,
  opts : Options,
) -> (Transport, @wire.Decoder, @wire.ServerInfo) {
  @async.with_timeout(opts.timeout_ms, () => {
    let websocket = url.has_prefix("ws://") || url.has_prefix("wss://")
    let mut transport = if websocket {
      WebSocket(@ws.Conn::connect(url))
    } else {
      let (host, port, _) = endpoint(url)
      Tcp(@socket.Tcp::connect_to_host(host, port~))
    }
    errdefer transport.close()
    let decoder = @wire.Decoder::new(max_payload=16777216)
    let mut info : @wire.ServerInfo? = None
    while info is None {
      guard transport.read() is Some(bytes) else { raise Disconnected }
      for e in decoder.feed(bytes) {
        match e {
          Info(s) => info = Some(@wire.ServerInfo::parse(s))
          Ping => transport.write(b"PONG\r\n")
          ServerError(s) => raise Server(s)
          _ => raise Server("expected INFO")
        }
      }
    }
    guard info is Some(info) else { raise Disconnected }
    if !websocket {
      let (host, _, secure) = endpoint(url)
      if secure || info.tls_required {
        guard transport is Tcp(tcp) else { raise Disconnected }
        transport = Secure(@tls.Tls::client(tcp, host~, trust=opts.trust))
      }
    }
    let connect = match (opts.signer, info.nonce) {
      (Some(signer), Some(nonce)) => signer(nonce)
      _ => opts.connect
    }
    transport.write(@wire.connect_command(connect) + b"PING\r\n")
    let mut ready = false
    while !ready {
      guard transport.read() is Some(bytes) else { raise Disconnected }
      for e in decoder.feed(bytes) {
        match e {
          Pong => ready = true
          Ping => transport.write(b"PONG\r\n")
          ServerError(s) => raise Server(s)
          Info(_) | Ok => ()
          _ => raise Server("unexpected handshake message")
        }
      }
    }
    (transport, decoder, info)
  })
}

///|
pub async fn[X] with_client(options : Options, f : async (Client) -> X) -> X {
  guard !options.servers.is_empty() &&
    options.timeout_ms > 0 &&
    options.reconnect_attempts >= 0 &&
    options.reconnect_delay_ms >= 0 &&
    options.reconnect_delay_ms <= 60000 &&
    options.heartbeat_ms >= 0 &&
    options.subscription_capacity > 0 &&
    options.max_pending_bytes > 0 else {
    raise InvalidOption("invalid client limits")
  }
  (options.on_state)(Connecting)
  let mut connection = None
  let mut last_error : Error = RetryExhausted
  for url in options.servers {
    try {
      connection = Some(open(url, options))
      break
    } catch {
      e => last_error = e
    }
  }
  guard connection is Some((transport, decoder, info)) else { raise last_error }
  let c = {
    options,
    transport,
    decoder,
    info,
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
  }
  defer c.close()
  (options.on_state)(Connected)
  @async.with_task_group(group => {
    group.spawn_bg(no_wait=true, () => c.read_loop())
    if options.heartbeat_ms > 0 {
      group.spawn_bg(no_wait=true, () => {
        while c.state_ != Closed {
          @async.sleep(options.heartbeat_ms)
          if c.state_ == Connected {
            c.flush() catch {
              _ => c.transport.close()
            }
          }
        }
      })
    }
    f(c)
  })
}

///|
pub fn Client::close(self : Client) -> Unit {
  if self.state_ == Closed {
    return
  }
  self.transport.close()
  self.set_state(Closed)
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
  // In-flight requests have an uncertain outcome and must not be silently replayed.
  let mut success = false
  for attempt in 0..<self.options.reconnect_attempts {
    let delay = self.options.reconnect_delay_ms.to_int64() *
      (1L << attempt.min(8))
    @async.sleep(delay.min(30000L).to_int())
    for url in self.options.servers {
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
          self.epoch += 1
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
async fn Client::send(self : Client, bytes : Bytes) -> Unit {
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
  errdefer self.transport.close()
  self.transport.write(bytes) catch {
    _ => {
      self.transport.close()
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
    if msg.payload.length() > self.options.max_pending_bytes - sub.pending_bytes ||
      !sub.inbox.try_put(item) {
      sub.closed = true
      sub.inbox.close(error=SlowConsumer)
      self.slow_consumers += 1
      self.subscriptions.remove(sub.sid)
    } else {
      sub.pending += 1
      sub.pending_bytes += msg.payload.length()
      sub.received += 1
      if sub.remaining is Some(n) {
        sub.remaining = Some(n - 1)
        if n == 1 {
          sub.closed = true
          sub.inbox.close()
          self.subscriptions.remove(sub.sid)
        }
      }
    }
  }
}

///|
async fn Client::read_loop(self : Client) -> Unit {
  while self.state_ != Closed {
    let epoch = self.epoch
    let result = self.transport.read() catch { _ => None }
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
          Info(s) => self.info = @wire.ServerInfo::parse(s)
          ServerError(s) => {
            self.close()
            raise Server(s)
          }
          Ok => ()
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
) -> Unit {
  guard !self.draining else { raise ConnectionClosed }
  guard payload.length() <= self.info.max_payload else {
    raise InvalidOption("server max_payload")
  }
  let bytes = match headers {
    Some(h) => @wire.publish_headers(subject, h, payload, reply_to?)
    None => @wire.publish(subject, payload, reply_to?)
  }
  self.send(bytes)
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
) -> Subscription {
  guard !self.draining else { raise ConnectionClosed }
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
  }
  self.subscriptions[sid] = sub
  errdefer {
    self.subscriptions.remove(sid)
    sub.inbox.close(error=ConnectionClosed)
  }
  self.send(wire)
  if max_messages is Some(n) {
    self.send(@wire.unsubscribe(sid, max_messages=n))
  }
  sub
}

///|
pub async fn Client::unsubscribe(self : Client, sub : Subscription) -> Unit {
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
  self.pending_bytes -= item.payload.length()
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
    self.transport.close()
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
  let inbox = self.new_inbox()
  let sub = self.subscribe(inbox, max_messages=1)
  defer @async.protect_from_cancel(() => self.unsubscribe(sub) catch { _ => () })
  self.publish(subject, payload, headers?, reply_to=inbox)
  let msg = sub.next(timeout_ms=timeout_ms.unwrap_or(self.options.timeout_ms))
  if msg.headers.status == Some(503) {
    raise NoResponders
  }
  msg
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
  self.draining = true
  self.set_state(Draining)
  @async.with_timeout(timeout_ms, () => {
    for sid, _ in self.subscriptions {
      self.send(@wire.unsubscribe(sid))
    }
    self.flush()
    while self.subscriptions.iter().any(p => p.1.pending > 0) {
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

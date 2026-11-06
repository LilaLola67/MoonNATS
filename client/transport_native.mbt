///|
priv enum Transport {
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
    Tcp(t) => t.read()
    Secure(t) => t.read()
    WebSocket(t) => {
      let msg = t.recv()
      let chunks = []
      let mut size = 0
      while msg.read() is Some(b) {
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
        transport = Secure(
          @tls.Tls::client(
            tcp,
            host~,
            trust=match opts.trust {
              SystemRoots => @tls.SystemRoot
              CustomPemFile(path) => @tls.CustomPemFile(path)
            },
          ),
        )
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

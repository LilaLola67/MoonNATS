///|
#external
priv type BrowserSocket

///|
priv enum Transport {
  Browser(BrowserSocket)
}

///|
extern "js" fn dial(
  url : String,
  signal : @js_async.AbortSignal,
) -> @js_async.Promise[BrowserSocket] =
  #| (url, signal) => new Promise((resolve, reject) => {
  #|   const ws = new WebSocket(url); ws.binaryType = 'arraybuffer';
  #|   const h = {ws, queue: [], size: 0, waiter: null, closed: false};
  #|   const abort = () => { ws.close(); reject(new Error('cancelled')); };
  #|   signal.addEventListener('abort', abort, {once:true});
  #|   ws.onopen = () => { signal.removeEventListener('abort',abort); resolve(h); };
  #|   ws.onmessage = event => {
  #|     if (!(event.data instanceof ArrayBuffer)) { ws.close(1003); return; }
  #|     const bytes = new Uint8Array(event.data);
  #|     if (h.waiter) { const w=h.waiter; h.waiter=null; w.resolve(bytes); }
  #|     else { h.size+=bytes.length; if(h.size>16777216){ws.close(1009);return;} h.queue.push(bytes); }
  #|   };
  #|   ws.onerror = () => { reject(new Error('WebSocket connection error')); };
  #|   ws.onclose = () => { h.closed=true; if(h.waiter){h.waiter.reject(new Error('WebSocket closed'));h.waiter=null;} reject(new Error('WebSocket closed')); };
  #| })

///|
extern "js" fn receive(
  socket : BrowserSocket,
  signal : @js_async.AbortSignal,
) -> @js_async.Promise[Bytes] =
  #| (h, signal) => new Promise((resolve,reject) => {
  #|   if(h.queue.length){const b=h.queue.shift();h.size-=b.length;resolve(b);return;}
  #|   if(h.closed){reject(new Error('WebSocket closed'));return;}
  #|   const abort=()=>{h.waiter=null;reject(new Error('cancelled'));};
  #|   const done=f=>v=>{signal.removeEventListener('abort',abort);f(v);};
  #|   h.waiter={resolve:done(resolve),reject:done(reject)};
  #|   signal.addEventListener('abort',abort,{once:true});
  #| })

///|
extern "js" fn transmit(
  socket : BrowserSocket,
  bytes : Bytes,
  signal : @js_async.AbortSignal,
) -> @js_async.Promise[Unit] =
  #| (h, bytes, signal) => new Promise((resolve,reject) => {
  #|   const send=()=>{
  #|     if(signal.aborted){reject(new Error('cancelled'));return;}
  #|     if(h.ws.readyState!==1){reject(new Error('WebSocket closed'));return;}
  #|     if(h.ws.bufferedAmount>1048576){setTimeout(send,2);return;}
  #|     h.ws.send(bytes);resolve();
  #|   };send();
  #| })

///|
extern "js" fn hangup(socket : BrowserSocket) -> Unit =
  #| h => { h.closed=true;h.queue=[];h.size=0;h.ws.close(); }

///|
fn Transport::close(self : Transport) -> Unit {
  let Browser(s) = self
  hangup(s)
}

///|
async fn Transport::read(self : Transport) -> Bytes? {
  let Browser(s) = self
  Some(@js_async.run_promise(signal => receive(s, signal)))
}

///|
async fn Transport::write(self : Transport, bytes : Bytes) -> Unit {
  let Browser(s) = self
  @js_async.run_promise(signal => transmit(s, bytes, signal))
}

///|
async fn open(
  url : String,
  opts : Options,
) -> (Transport, @wire.Decoder, @wire.ServerInfo) {
  guard url.has_prefix("ws://") || url.has_prefix("wss://") else {
    raise InvalidOption("JavaScript transport requires ws:// or wss://")
  }
  guard opts.trust is SystemRoots else {
    raise InvalidOption("browser certificates are managed by the browser")
  }
  @async.with_timeout(opts.timeout_ms, () => {
    let transport = Browser(@js_async.run_promise(signal => dial(url, signal)))
    errdefer transport.close()
    let decoder = @wire.Decoder::new(max_payload=16777216)
    let mut info = None
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
    let connect = match (opts.signer, info.nonce) {
      (Some(s), Some(n)) => s(n)
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
          _ => ()
        }
      }
    }
    (transport, decoder, info)
  })
}

///|
let usage : String =
  #|MoonNATS 0.2.0
  #|Usage: moonnats COMMAND [ARGS]
  #|  info | stats
  #|  pub SUBJECT TEXT | request SUBJECT TEXT | sub SUBJECT [COUNT]
  #|  stream-create NAME SUBJECT | stream-info NAME
  #|  consumer-create STREAM NAME SUBJECT | fetch STREAM NAME [COUNT]
  #|  kv-create BUCKET | kv-put BUCKET KEY TEXT | kv-get BUCKET KEY
  #|  object-create BUCKET | object-put BUCKET NAME FILE
  #|  object-get BUCKET NAME OUTPUT | object-list BUCKET
  #|Environment: MOONNATS_URL, MOONNATS_USER, MOONNATS_PASSWORD,
  #|MOONNATS_TOKEN, MOONNATS_NKEY_SEED, MOONNATS_JWT, MOONNATS_CA.
  #|fetch displays messages without acknowledging them.
  #|object-get refuses to overwrite an existing output.

///|
fn options() -> @client.Options raise {
  let connect = @wire.ConnectOptions::new(
    user?=@env.get_env_var("MOONNATS_USER"),
    password?=@env.get_env_var("MOONNATS_PASSWORD"),
    token?=@env.get_env_var("MOONNATS_TOKEN"),
  )
  let signer = match @env.get_env_var("MOONNATS_NKEY_SEED") {
    Some(seed) =>
      Some(
        @auth.UserKey::from_seed(seed).signer(
          jwt?=@env.get_env_var("MOONNATS_JWT"),
        ),
      )
    None => None
  }
  let trust = match @env.get_env_var("MOONNATS_CA") {
    Some(path) => @client.CustomPemFile(path)
    None => @client.SystemRoots
  }
  @client.Options::new(
    servers=@env.get_env_var("MOONNATS_URL")
      .unwrap_or("nats://127.0.0.1:4222")
      .split(",")
      .map(s => s.to_owned())
      .to_array(),
    connect~,
    trust~,
    signer?,
  )
}

///|
fn show(msg : @client.Received) -> Unit {
  println(
    (
      {
        "subject": Json(msg.subject),
        "data_base64": Json(@base64.encode(msg.payload)),
        "reply_to": Json(msg.reply_to),
        "status": Json(msg.headers.status),
      } : Json).stringify(),
  )
}

///|
async fn run(args : Array[String]) -> Unit {
  @client.with_client(options(), c => {
    let js = @js.JetStream::new(c)
    match args {
      ["info"] => println(c.info.raw.stringify())
      ["stats"] => {
        let s = c.statistics()
        println(
          (
            {
              "sent_messages": @js.integer(s.sent_messages),
              "received_messages": @js.integer(s.received_messages),
              "reconnects": @js.integer(s.reconnects),
              "pending_bytes": Json(s.pending_bytes),
            } : Json).stringify(),
        )
      }
      ["pub", subject, text] => {
        c.publish(subject, @utf8.encode(text))
        c.flush()
        println("published")
      }
      ["request", subject, text] => show(c.request(subject, @utf8.encode(text)))
      ["sub", subject, .. rest] => {
        let count = if rest.is_empty() { 1 } else { @string.parse_int(rest[0]) }
        guard count > 0 else { raise @js.Invalid("positive count required") }
        let sub = c.subscribe(subject, max_messages=count)
        c.flush()
        for _ in 0..<count {
          show(sub.next(timeout_ms=60000))
        }
      }
      ["stream-create", name, subject] =>
        println(js.create_stream(name, [subject]).stringify())
      ["stream-info", name] => println(js.stream_info(name).stringify())
      ["consumer-create", stream, name, subject] => {
        ignore(js.create_consumer(stream, name, filter=subject))
        println("created")
      }
      ["fetch", stream, name, .. rest] => {
        let count = if rest.is_empty() { 1 } else { @string.parse_int(rest[0]) }
        for msg in js.consumer(stream, name).fetch(batch=count) {
          show(msg)
        }
      }
      ["kv-create", bucket] => {
        ignore(@kv.create(js, bucket))
        println("created")
      }
      ["kv-put", bucket, key, text] =>
        println(@kv.open(js, bucket).put(key, @utf8.encode(text)))
      ["kv-get", bucket, key] => {
        let e = @kv.open(js, bucket).get(key)
        println(
          (
            {
              "key": Json(e.key),
              "revision": @js.integer(e.revision),
              "operation": Json(e.operation),
              "data_base64": Json(@base64.encode(e.value)),
            } : Json).stringify(),
        )
      }
      ["object-create", bucket] => {
        ignore(@objects.create(js, bucket))
        println("created")
      }
      ["object-put", bucket, name, path] => {
        let file = @fs.open(path, mode=ReadOnly)
        defer file.close()
        println(
          @objects.open(js, bucket).put_stream(name, () => file.read()).raw.stringify(),
        )
      }
      ["object-get", bucket, name, path] => {
        let temporary = path + "." + c.new_inbox() + ".part"
        let file = @fs.create(temporary)
        errdefer @fs.remove(temporary)
        let info = {
          defer file.close()
          @objects.open(js, bucket).get_to(name, bytes => file.write(bytes))
        }
        @fs.rename(temporary, path, replace=false)
        println(info.raw.stringify())
      }
      ["object-list", bucket] =>
        for info in @objects.open(js, bucket).list() {
          println(info.raw.stringify())
        }
      _ => raise @js.Invalid("unknown command or invalid arguments; run --help")
    }
  })
}

///|
async fn main {
  let args = @env.args()[1:].to_owned()
  if args.is_empty() || args == ["--help"] || args == ["help"] {
    println(usage)
    return
  }
  if args == ["--version"] {
    println("0.2.0")
    return
  }
  run(args) catch {
    error => {
      @stdio.stderr.text.write(
        ({ "error": Json(error.to_string()) } : Json).stringify() + "\n",
      )
      @sys.exit(1)
    }
  }
}

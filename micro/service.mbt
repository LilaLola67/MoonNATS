///|
pub struct Response {
  priv payload : Bytes
  priv headers : @wire.Headers
}

///|
pub fn Response::ok(
  payload : Bytes,
  headers~ : @wire.Headers = @wire.Headers::new(),
) -> Response {
  { payload, headers, }
}

///|
pub fn Response::error(
  code : Int32,
  description : String,
  payload~ : Bytes = b"",
) -> Response raise {
  guard code >= 100 && code <= 999 else {
    raise @client.InvalidOption("service error code")
  }
  {
    payload,
    headers: @wire.Headers::new(entries=[
      ("Nats-Service-Error", description),
      ("Nats-Service-Error-Code", code.to_string()),
    ]),
  }
}

///|
pub struct Endpoint {
  name : String
  subject : String
  priv handler : async (@client.Received) -> Response
  priv queue : String
  priv mut requests : Int64
  priv mut errors : Int64
  priv mut elapsed_ms : Int64
  priv mut last_error : String
}

///|
pub fn Endpoint::new(
  name : String,
  subject : String,
  handler : async (@client.Received) -> Response,
  queue~ : String = "q",
) -> Endpoint {
  {
    name,
    subject,
    handler,
    queue,
    requests: 0L,
    errors: 0L,
    elapsed_ms: 0L,
    last_error: "",
  }
}

///|
pub struct Service {
  name : String
  id : String
  version : String
  priv endpoints : Array[Endpoint]
  priv metadata : Json
  priv started : String
  priv mut stopped : Bool
}

///|
pub fn Service::stop(self : Service) -> Unit {
  self.stopped = true
}

///|
fn identifier(name : String) -> Bool {
  !name.is_empty() &&
  name.length() <= 128 &&
  name
  .iter()
  .all(c => {
    (c >= 'a' && c <= 'z') ||
    (c >= 'A' && c <= 'Z') ||
    (c >= '0' && c <= '9') ||
    c == '_' ||
    c == '-'
  })
}

///|
pub fn Service::info(self : Service, operation~ : String = "INFO") -> Json {
  let fields : Map[String, Json] = {
    "name": Json(self.name),
    "id": Json(self.id),
    "version": Json(self.version),
    "metadata": self.metadata,
    "type": Json("io.nats.micro.v1." + operation.to_lower() + "_response"),
  }
  if operation != "PING" {
    fields["endpoints"] = Json::array(
      self.endpoints.map(ep => {
        let m : Map[String, Json] = {
          "name": Json(ep.name),
          "subject": Json(ep.subject),
          "queue_group": Json(ep.queue),
        }
        if operation == "STATS" {
          m["num_requests"] = Json::number(
            ep.requests.to_double(),
            repr=ep.requests.to_string(),
          )
          m["num_errors"] = Json::number(
            ep.errors.to_double(),
            repr=ep.errors.to_string(),
          )
          let total = ep.elapsed_ms * 1000000L
          m["processing_time"] = Json::number(
            total.to_double(),
            repr=total.to_string(),
          )
          let avg = if ep.requests == 0L { 0L } else { total / ep.requests }
          m["average_processing_time"] = Json::number(
            avg.to_double(),
            repr=avg.to_string(),
          )
          m["last_error"] = Json(ep.last_error)
        }
        Json::object(m)
      }),
    )
  }
  if operation == "STATS" {
    fields["started"] = Json(self.started)
  }
  Json::object(fields)
}

///|
/// Each endpoint processes sequentially; scale with queue-group service instances.
/// Stop waits for already received requests, without closing the shared client.
pub async fn[X] with_service(
  client : @client.Client,
  name : String,
  version : String,
  endpoints : Array[Endpoint],
  f : async (Service) -> X,
  metadata~ : Json = {},
) -> X {
  guard identifier(name) && !endpoints.is_empty() && metadata is Object(_) else {
    raise @client.InvalidOption("service configuration")
  }
  let parts = version.split(".").to_array()
  guard parts.length() == 3 &&
    parts
    .iter()
    .all(p => !p.is_empty() && p.iter().all(c => c >= '0' && c <= '9')) else {
    raise @client.InvalidOption("service version must be major.minor.patch")
  }
  let seen : Map[String, Bool] = Map([])
  for ep in endpoints {
    guard identifier(ep.name) && !seen.contains(ep.name) else {
      raise @client.InvalidOption("endpoint name")
    }
    seen[ep.name] = true
    ignore(@wire.subscribe(ep.subject, 1, queue=ep.queue))
  }
  let service : Service = {
    name,
    id: client.new_inbox().replace_all(old=".", new="_"),
    version,
    endpoints: endpoints.copy(),
    metadata,
    started: @time.unix(@env.now().reinterpret_as_int64() / 1000L)
    .to_plain_date_time()
    .to_string() +
    "Z",
    stopped: false,
  }
  let subscriptions : Array[@client.Subscription] = []
  defer @async.protect_from_cancel(() => {
    service.stop()
    for sub in subscriptions {
      client.unsubscribe(sub) catch {
        _ => ()
      }
    }
  })
  let mut result = None
  @async.with_task_group(group => {
    for ep in service.endpoints {
      let sub = client.subscribe(ep.subject, queue=ep.queue)
      subscriptions.push(sub)
      group.spawn_bg(() => {
        while !service.stopped || sub.pending() > 0 {
          let msg = Some(sub.next(timeout_ms=100)) catch {
            @async.TimeoutError => continue
            e => {
              if service.stopped {
                break
              }
              raise e
            }
          }
          guard msg is Some(msg) else { continue }
          let start = @async.now()
          ep.requests += 1L
          let response = (ep.handler)(msg) catch {
            _ => Response::error(500, "handler failed")
          }
          ep.elapsed_ms += @async.now() - start
          if response.headers.get("Nats-Service-Error") is Some(error) {
            ep.errors += 1L
            ep.last_error = error
          }
          if msg.reply_to is Some(_) {
            client.reply(msg, response.payload, headers=response.headers)
          }
        }
      })
    }
    for operation in ["PING", "INFO", "STATS"] {
      for
        subject in [
          "$SRV." + operation,
          "$SRV." + operation + "." + name,
          "$SRV." + operation + "." + name + "." + service.id,
        ] {
        let sub = client.subscribe(subject)
        subscriptions.push(sub)
        group.spawn_bg(() => {
          while !service.stopped {
            let msg = sub.next(timeout_ms=100) catch {
              @async.TimeoutError => continue
              e => {
                if service.stopped {
                  break
                }
                raise e
              }
            }
            if msg.reply_to is Some(_) {
              client.reply(
                msg,
                @utf8.encode(service.info(operation~).stringify()),
              )
            }
          }
        })
      }
    }
    client.flush()
    defer service.stop()
    result = Some(f(service))
    service.stop()
  })
  result.unwrap()
}

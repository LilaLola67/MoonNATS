///|
pub(all) struct ConnectOptions {
  name : String
  user : String?
  password : String?
  token : String?
  jwt : String?
  nkey : String?
  signature : String?
  echo : Bool
}

///|
pub fn ConnectOptions::new(
  name~ : String = "MoonNATS",
  user? : String,
  password? : String,
  token? : String,
  jwt? : String,
  nkey? : String,
  signature? : String,
  echo~ : Bool = true,
) -> ConnectOptions {
  { name, user, password, token, jwt, nkey, signature, echo, }
}

///|
pub fn connect_command(
  options : ConnectOptions,
  headers~ : Bool = true,
) -> Bytes {
  let values : Map[String, Json] = {
    "verbose": false,
    "pedantic": true,
    "tls_required": false,
    "name": Json(options.name),
    "lang": "moonbit",
    "version": "0.3.4",
    "protocol": 1,
    "headers": Json(headers),
    "no_responders": Json(headers),
    "echo": Json(options.echo),
  }
  for
    (key, value) in [
      ("user", options.user),
      ("pass", options.password),
      ("auth_token", options.token),
      ("jwt", options.jwt),
      ("nkey", options.nkey),
      ("sig", options.signature),
    ] {
    if value is Some(v) {
      values[key] = Json(v)
    }
  }
  @utf8.encode("CONNECT " + Json::object(values).stringify() + "\r\n")
}

///|
pub(all) struct ServerInfo {
  raw : Json
  max_payload : Int32
  headers : Bool
  tls_required : Bool
  auth_required : Bool
  jetstream : Bool
  nonce : String?
  connect_urls : Array[String]
}

///|
pub fn ServerInfo::parse(text : String) -> ServerInfo raise ProtocolError {
  let j = @json.parse(text) catch { _ => raise Invalid("invalid INFO JSON") }
  guard j is Object(_) else { raise Invalid("INFO must be object") }
  server_info(j)
}

///|
/// Merge an asynchronous INFO update with capabilities from the current connection.
pub fn ServerInfo::update(
  self : ServerInfo,
  text : String,
) -> ServerInfo raise ProtocolError {
  let patch = @json.parse(text) catch {
    _ => raise Invalid("invalid INFO JSON")
  }
  guard self.raw is Object(current) && patch is Object(fields) else {
    raise Invalid("INFO must be object")
  }
  let merged = current.copy()
  for key, value in fields {
    merged[key] = value
  }
  server_info(Json::object(merged))
}

///|
fn server_info(j : Json) -> ServerInfo {
  let max_payload = match j {
    { "max_payload": Number(n, ..), .. } if n > 0 && n <= 2147483645.0 =>
      n.to_int()
    _ => 1048576
  }
  let urls = match j {
    { "connect_urls": Array(a), .. } =>
      a
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
  {
    raw: j,
    max_payload,
    headers: j is { "headers": True, .. },
    tls_required: j is { "tls_required": True, .. },
    auth_required: j is { "auth_required": True, .. },
    jetstream: j is { "jetstream": True, .. },
    nonce: match j {
      { "nonce": String(s), .. } => Some(s)
      _ => None
    },
    connect_urls: urls,
  }
}

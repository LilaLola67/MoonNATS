///|
pub(all) struct Codec[T] {
  encode : (T) -> Bytes raise
  decode : (Bytes) -> T raise
}

///|
pub fn[T : ToJson + @json.FromJson] json_codec() -> Codec[T] {
  {
    encode: value => @utf8.encode(Json(value).stringify()),
    decode: bytes => @json.from_json(@json.parse(@utf8.decode(bytes))),
  }
}

///|
pub async fn[T] publish(
  client : @client.Client,
  subject : String,
  value : T,
  codec : Codec[T],
) -> Unit {
  client.publish(subject, (codec.encode)(value))
}

///|
pub async fn[Request, Response] request(
  client : @client.Client,
  subject : String,
  value : Request,
  input : Codec[Request],
  output : Codec[Response],
  timeout_ms~ : Int32 = 5000,
) -> Response {
  let message = client.request(subject, (input.encode)(value), timeout_ms~)
  (output.decode)(message.payload)
}

///|
pub async fn[T] next(
  subscription : @client.Subscription,
  codec : Codec[T],
  timeout_ms~ : Int32 = 5000,
) -> T {
  (codec.decode)(subscription.next(timeout_ms~).payload)
}

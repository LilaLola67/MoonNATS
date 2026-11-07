///|
pub(all) enum BufferRejectReason {
  PayloadTooLarge(Int32, Int32)
  HeadersUnsupported
} derive(Debug)

///|
priv struct PublishFrame {
  subject : String
  bytes : Bytes
  message_bytes : Int32
  has_headers : Bool
}

///|
fn PublishFrame::incompatibility(
  self : PublishFrame,
  info : @wire.ServerInfo,
) -> BufferRejectReason? {
  if self.message_bytes > info.max_payload {
    Some(PayloadTooLarge(self.message_bytes, info.max_payload))
  } else if self.has_headers && !info.headers {
    Some(HeadersUnsupported)
  } else {
    None
  }
}

///|
fn PublishFrame::validate(
  self : PublishFrame,
  info : @wire.ServerInfo,
) -> Unit raise ClientError {
  match self.incompatibility(info) {
    Some(PayloadTooLarge(_, _)) =>
      raise InvalidOption("server max_payload including headers")
    Some(HeadersUnsupported) =>
      raise InvalidOption("server does not support headers")
    None => ()
  }
}

///|
pub extend BufferRejectReason with @debug.Debug::{to_repr}

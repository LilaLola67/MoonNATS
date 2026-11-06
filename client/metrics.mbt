///|
/// Prometheus text exposition. No server URLs, subjects or credentials are labels.
pub fn Statistics::prometheus(self : Statistics) -> String {
  let out = StringBuilder()
  for
    (name, value, kind) in [
      ("sent_messages_total", self.sent_messages, "counter"),
      ("received_messages_total", self.received_messages, "counter"),
      ("sent_payload_bytes_total", self.sent_bytes, "counter"),
      ("received_payload_bytes_total", self.received_bytes, "counter"),
      ("reconnects_total", self.reconnects, "counter"),
      ("slow_consumers_total", self.slow_consumers, "counter"),
      ("requests_total", self.requests, "counter"),
      ("request_errors_total", self.request_errors, "counter"),
      ("request_duration_milliseconds_total", self.request_time_ms, "counter"),
      ("pending_bytes", self.pending_bytes.to_int64(), "gauge"),
      ("reconnect_buffer_bytes", self.buffered_bytes.to_int64(), "gauge"),
      ("subscriptions", self.subscriptions.to_int64(), "gauge"),
    ] {
    out.write_string("# TYPE moonnats_" + name + " " + kind + "\n")
    out.write_string("moonnats_" + name + " " + value.to_string() + "\n")
  }
  out.to_string()
}

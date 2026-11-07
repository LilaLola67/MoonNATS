///|
test "HMSG every split boundary preserves duplicate headers and binary body" {
  let headers = @moonnats.Headers::new(entries=[
    ("Trace", "one"),
    ("Trace", "two"),
  ])
  let encoded = headers.encode()
  let prefix = "HMSG a 1 inbox.1 \{encoded.length()} \{encoded.length()+2}\r\n"
  let wire = @utf8.encode(prefix) + encoded + b"\xff\x00\r\n"
  let expected = @moonnats.Decoder::new().feed(wire)
  assert_eq(
    expected[0],
    @moonnats.Event::Hmsg(
      { subject: "a", sid: 1, reply_to: Some("inbox.1"), payload: b"\xff\x00", },
      headers,
    ),
  )
  for split in 0..<=wire.length() {
    let d = @moonnats.Decoder::new()
    let events = d.feed(wire[:split].to_owned())
    for e in d.feed(wire[split:].to_owned()) {
      events.push(e)
    }
    assert_eq(events, expected)
    d.finish()
  }
}

///|
test "headers status malformed fields and injection" {
  let h = @moonnats.Headers::parse(b"NATS/1.0 503 No Responders\r\n\r\n")
  assert_eq(h.status, Some(503))
  assert_eq(h.description, "No Responders")
  assert_eq(h.encode(), b"NATS/1.0 503 No Responders\r\n\r\n")
  for
    raw in [
      b"NATS/2.0\r\n\r\n", b"NATS/1.0\r\nBad\r\n\r\n", b"NATS/1.0\r\n\r\nX", b"NATS/1.0 99 X\r\n\r\n",
    ] {
    assert_true(
      try {
        ignore(@moonnats.Headers::parse(raw))
        false
      } catch {
        _ => true
      },
    )
  }
  assert_true(
    try {
      ignore(
        @moonnats.Headers::new(entries=[("X", "ok\r\nInjected: yes")]).encode(),
      )
      false
    } catch {
      _ => true
    },
  )
}

///|
test "CONNECT uses JSON escaping and INFO exposes negotiated capabilities" {
  let bytes = @moonnats.connect_command(
    @moonnats.ConnectOptions::new(
      name="quote\"\r\n",
      token="synthetic-test-token",
    ),
  )
  let text = @utf8.decode(bytes)
  assert_true(text.has_prefix("CONNECT "))
  let j = @json.parse(text[8:])
  assert_true(
    j
    is {
      "name": String("quote\"\r\n"),
      "auth_token": String("synthetic-test-token"),
      "headers": True,
      ..
    },
  )
  let info = @moonnats.ServerInfo::parse(
    "{\"headers\":true,\"max_payload\":512,\"tls_required\":true}",
  )
  assert_eq(info.max_payload, 512)
  assert_true(info.headers && info.tls_required)
}

///|
test "topology-only INFO preserves negotiated publish capabilities" {
  let info = @moonnats.ServerInfo::parse(
    "{\"server_id\":\"seed\",\"max_payload\":64,\"headers\":true}",
  )
  let updated = info.update("{\"connect_urls\":[\"127.0.0.1:4223\"]}")
  assert_true(updated.raw is { "server_id": String("seed"), .. })
  assert_eq(updated.max_payload, 64)
  assert_true(updated.headers)
  assert_eq(updated.connect_urls, ["127.0.0.1:4223"])
}

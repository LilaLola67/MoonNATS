///|
test "all split boundaries preserve binary messages and control frames" {
  let wire = b"INFO {}\r\nMSG orders.created 1 inbox.1 5\r\na\x00\r\nb\r\nPING\r\nPONG\r\n+OK\r\n-ERR 'example'\r\n"
  let expected = @moonnats.Decoder::new().feed(wire)
  assert_eq(expected.length(), 6)
  assert_eq(
    expected[1],
    @moonnats.Event::Msg({
      subject: "orders.created",
      sid: 1,
      reply_to: Some("inbox.1"),
      payload: b"a\x00\r\nb",
    }),
  )
  for split in 0..<=wire.length() {
    let d = @moonnats.Decoder::new()
    let actual = d.feed(wire[:split].to_owned())
    for e in d.feed(wire[split:].to_owned()) {
      actual.push(e)
    }
    assert_eq(actual, expected)
    d.finish()
  }
}

///|
test "byte by byte and zero payload" {
  let d = @moonnats.Decoder::new()
  let events = []
  for b in b"MSG a 1 0\r\n\r\nPING\r\n" {
    for e in d.feed(Bytes::from_array([b])) {
      events.push(e)
    }
  }
  assert_eq(events.length(), 2)
  assert_eq(
    events[0],
    @moonnats.Event::Msg({ subject: "a", sid: 1, reply_to: None, payload: b"", }),
  )
  d.finish()
}

///|
test "malformed frames reject and poison decoder" {
  for
    wire in [
      b"MSG a 1 -1\r\n", b"MSG a 0 1\r\nx\r\n", b"MSG a 1 2147483648\r\n", b"MSG a 1 1\r\nx!!",
      b"PING extra\r\n", b"INFO []\r\n", b"HMSG a 1 0 0\r\n", b"MSG a..b 1 0\r\n",
      b"\xff\r\n",
    ] {
    let d = @moonnats.Decoder::new()
    let failed = try {
      ignore(d.feed(wire))
      false
    } catch {
      _ => true
    }
    assert_true(failed)
    let poisoned = try {
      ignore(d.feed(b"PING\r\n"))
      false
    } catch {
      _ => true
    }
    assert_true(poisoned)
  }
}

///|
test "limits and EOF" {
  let d = @moonnats.Decoder::new(max_payload=2)
  assert_true(
    try {
      ignore(d.feed(b"MSG a 1 3\r\n"))
      false
    } catch {
      _ => true
    },
  )
  let line = @moonnats.Decoder::new(max_line=4)
  assert_true(
    try {
      ignore(line.feed(b"1234567"))
      false
    } catch {
      _ => true
    },
  )
  for wire in [b"PING\r", b"MSG a 1 2\r\nx", b"MSG a 1 0\r\n"] {
    let d = @moonnats.Decoder::new()
    ignore(d.feed(wire))
    assert_true(
      try {
        d.finish()
        false
      } catch {
        _ => true
      },
    )
  }
}

///|
test "encoders produce protocol bytes" {
  assert_eq(
    @moonnats.publish("a", b"\xff\x00", reply_to="inbox.1"),
    b"PUB a inbox.1 2\r\n\xff\x00\r\n",
  )
  assert_eq(@moonnats.publish("a", b""), b"PUB a 0\r\n\r\n")
  assert_eq(
    @moonnats.subscribe("orders.*", 1, queue="workers"),
    b"SUB orders.* workers 1\r\n",
  )
  assert_eq(@moonnats.unsubscribe(1, max_messages=2), b"UNSUB 1 2\r\n")
  assert_eq(@moonnats.unsubscribe(1), b"UNSUB 1\r\n")
}

///|
test "encoders reject command injection and invalid wildcards" {
  for s in ["a\r\nPING", "a b", "a..b", "", "a.*", "a.>"] {
    assert_true(
      try {
        ignore(@moonnats.publish(s, b"x"))
        false
      } catch {
        _ => true
      },
    )
  }
  for s in ["a.>.b", "a*", "a..b"] {
    assert_true(
      try {
        ignore(@moonnats.subscribe(s, 1))
        false
      } catch {
        _ => true
      },
    )
  }
  assert_true(
    try {
      ignore(@moonnats.subscribe("a", 1, queue="q\nPING"))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(@moonnats.unsubscribe(0))
      false
    } catch {
      _ => true
    },
  )
}

///|
fn message(reply : String) -> @client.Received {
  {
    subject: "a",
    reply_to: Some(reply),
    payload: b"",
    headers: @wire.Headers::new(),
  }
}

///|
test "legacy and domain ACK metadata preserve exact sequences" {
  let old = @jetstream.metadata(
    message("$JS.ACK.S.C.2.9007199254740993.7.1700000000000000000.4"),
  )
  assert_eq(old.stream, "S")
  assert_eq(old.stream_sequence, 9007199254740993L)
  let domain = @jetstream.metadata(
    message(
      "$JS.ACK.DOMAIN.HASH.S.C.3.9007199254740993.8.1700000000000000000.5.random",
    ),
  )
  assert_eq(domain.stream, "S")
  assert_eq(domain.consumer, "C")
  assert_eq(domain.pending, 5L)
  assert_eq(domain.deliveries, 3L)
  assert_true(
    try {
      ignore(@jetstream.metadata(message("$JS.ACK.bad")))
      false
    } catch {
      _ => true
    },
  )
}

///|
test "API integers and errors remain structured" {
  let j = @jetstream.decode_json(b"{\"seq\":9007199254740993}")
  assert_eq(@jetstream.integer_field(j, "seq"), 9007199254740993L)
  assert_eq(
    @jetstream.integer(9007199254740993L).stringify(),
    "9007199254740993",
  )
  assert_true(
    try {
      ignore(
        @jetstream.check_response({
          "error": { "code": 404, "err_code": 10059, "description": "missing" },
        }),
      )
      false
    } catch {
      @jetstream.Api(404, 10059, "missing") => true
      _ => false
    },
  )
  for name in ["", "a.b", "a*", "a>b", "a/b", "a\n"] {
    assert_true(
      try {
        @jetstream.validate_name(name)
        false
      } catch {
        _ => true
      },
    )
  }
}

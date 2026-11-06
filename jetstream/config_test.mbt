///|
test "typed config validates units, replicas, sequence and ack policy" {
  assert_true(
    try {
      ignore(@jetstream.StreamConfig::new(["jobs"], replicas=0).to_json())
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(
        @jetstream.StreamConfig::new(["jobs"], max_age_ms=9223372036855L).to_json(),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(@jetstream.ConsumerConfig::new(start_sequence=0L).to_json())
      false
    } catch {
      _ => true
    },
  )
  let config = @jetstream.ConsumerConfig::new(
    ack_policy=NoAck,
    start_sequence=9007199254740993L,
  ).to_json()
  assert_eq(
    @jetstream.integer_field(config, "opt_start_seq"),
    9007199254740993L,
  )
  assert_true(
    config is Object(fields) &&
    !fields.contains("max_ack_pending") &&
    !fields.contains("ack_wait"),
  )
  assert_true(
    @jetstream.StreamConfig::new(["jobs"], max_age_ms=1234L).to_json()
    is { "max_age": Number(_, repr=Some("1234000000")), .. },
  )
}

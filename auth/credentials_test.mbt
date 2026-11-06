///|
test "standard creds envelope parses CRLF and rejects incomplete secrets" {
  let text = "-----BEGIN NATS USER JWT-----\r\na.b.c\r\n------END NATS USER JWT------\r\n\r\n-----BEGIN USER NKEY SEED-----\r\nSUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEQ\r\n------END USER NKEY SEED------\r\n"
  let credentials = @auth.Credentials::parse(text)
  assert_eq(credentials.jwt, "a.b.c")
  assert_eq(
    credentials.key.public_key,
    "UA5WUJ54Z23KILLCUOUNAKTPBVZWKMQVO4O6EQ5GHLAERIMLLHNCTYM5",
  )
  assert_true(
    try {
      ignore(@auth.Credentials::parse("a.b.c"))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(@auth.Credentials::parse(text + text))
      false
    } catch {
      _ => true
    },
  )
}

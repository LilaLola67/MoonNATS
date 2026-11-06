///|
test "NKey user prefix signature and corrupt seed rejection" {
  let key = @auth.UserKey::from_raw_seed(Bytes::make(32, 0))
  assert_true(key.public_key.has_prefix("U"))
  assert_eq(key.public_key.length(), 56)
  let options = key.sign_nonce("nonce")
  assert_true(options.signature is Some(s) && s.length() == 86)
  assert_true(
    try {
      ignore(@auth.UserKey::from_seed("SUAA"))
      false
    } catch {
      _ => true
    },
  )
}

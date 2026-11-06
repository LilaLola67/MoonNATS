///|
// Public test vector: all-zero seed, CRC independently calculated with Python binascii.
test "standard NKey seed encoding and known Ed25519 public key" {
  let key = @auth.UserKey::from_seed(
    "SUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEQ",
  )
  assert_eq(
    key.public_key,
    "UA5WUJ54Z23KILLCUOUNAKTPBVZWKMQVO4O6EQ5GHLAERIMLLHNCTYM5",
  )
  let raw = @auth.UserKey::from_raw_seed(Bytes::make(32, 0))
  assert_eq(raw.public_key, key.public_key)
  assert_true(
    try {
      ignore(
        @auth.UserKey::from_seed(
          "SUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEA",
        ),
      )
      false
    } catch {
      _ => true
    },
  )
  let options = key.sign_nonce("nonce", jwt="synthetic-jwt")
  assert_eq(options.jwt, Some("synthetic-jwt"))
  assert_eq(options.nkey, None)
}

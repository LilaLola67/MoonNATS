///|
test "JSON codec preserves Unicode and rejects mismatched types" {
  let codec : @typed.Codec[Array[String]] = @typed.json_codec()
  let value = ["月兔", "quote\"", "line\n"]
  assert_eq((codec.decode)((codec.encode)(value)), value)
  assert_true(
    try {
      ignore((codec.decode)(b"{\"not\":\"array\"}"))
      false
    } catch {
      _ => true
    },
  )
}

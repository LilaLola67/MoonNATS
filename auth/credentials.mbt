///|
pub struct Credentials {
  jwt : String
  key : UserKey
}

///|
/// Parse the standard NATS user .creds envelope, without logging its contents.
pub fn Credentials::parse(text : String) -> Credentials raise {
  fn section(begin : String, end : String) -> String raise {
    guard text.split(begin).to_array() is [_, rest] else { raise InvalidSeed }
    guard rest.split(end).to_array() is [value, _] else { raise InvalidSeed }
    value.trim().to_owned()
  }
  let jwt = section(
    "-----BEGIN NATS USER JWT-----", "------END NATS USER JWT------",
  )
  guard jwt.split(".").count() == 3 else { raise InvalidSeed }
  let seed = section(
    "-----BEGIN USER NKEY SEED-----", "------END USER NKEY SEED------",
  )
  { jwt, key: UserKey::from_seed(seed), }
}

///|
pub fn Credentials::signer(
  self : Credentials,
) -> (String) -> @wire.ConnectOptions raise {
  self.key.signer(jwt=self.jwt)
}

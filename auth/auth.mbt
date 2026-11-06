///|
pub suberror AuthError {
  InvalidSeed
} derive(Debug)

///|
pub extend AuthError with @debug.Debug::{to_repr}

///|
fn crc16(bytes : Bytes) -> Int32 {
  let mut crc = 0
  for byte in bytes {
    crc = crc ^ (byte.to_int() << 8)
    for _ in 0..<8 {
      crc = if (crc & 32768) != 0 {
        ((crc << 1) ^ 4129) & 65535
      } else {
        (crc << 1) & 65535
      }
    }
  }
  crc
}

///|
fn base32_decode(text : String) -> Bytes raise AuthError {
  let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
  let out = []
  let mut bits = 0
  let mut value = 0
  for c in text.iter() {
    guard alphabet.find(c.to_string()) is Some(n) else { raise InvalidSeed }
    value = (value << 5) | n
    bits += 5
    if bits >= 8 {
      bits -= 8
      out.push(((value >> bits) & 255).to_byte())
      value = value & ((1 << bits) - 1)
    }
  }
  guard value == 0 else { raise InvalidSeed }
  Bytes::from_array(out)
}

///|
fn base32_encode(bytes : Bytes) -> String {
  let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".iter().to_array()
  let out = StringBuilder()
  let mut bits = 0
  let mut value = 0
  for byte in bytes {
    value = (value << 8) | byte.to_int()
    bits += 8
    while bits >= 5 {
      bits -= 5
      out.write_char(alphabet[(value >> bits) & 31])
      value = value & ((1 << bits) - 1)
    }
  }
  if bits > 0 {
    out.write_char(alphabet[(value << (5 - bits)) & 31])
  }
  out.to_string()
}

///|
fn checksum(bytes : Bytes) -> Bytes {
  let crc = crc16(bytes)
  bytes + Bytes::from_array([(crc & 255).to_byte(), (crc >> 8).to_byte()])
}

///|
pub struct UserKey {
  priv key : @ed25519.SigningKey
  public_key : String
}

///|
pub fn UserKey::from_seed(encoded : String) -> UserKey raise {
  let raw = base32_decode(encoded)
  guard encoded.length() == 58 &&
    raw.length() == 36 &&
    raw[0] == 149 &&
    raw[1] == 0 else {
    raise InvalidSeed
  }
  let payload = raw[:34].to_owned()
  guard checksum(payload) == raw else { raise InvalidSeed }
  UserKey::from_raw_seed(raw[2:34].to_owned())
}

///|
pub fn UserKey::from_raw_seed(seed : Bytes) -> UserKey raise {
  guard seed.length() == 32 else { raise InvalidSeed }
  let key = @ed25519.SigningKey::from_seed(seed)
  { key, public_key: base32_encode(checksum(b"\xa0" + key.public_key())), }
}

///|
pub fn UserKey::sign_nonce(
  self : UserKey,
  nonce : String,
  jwt? : String,
  name~ : String = "MoonNATS",
) -> @wire.ConnectOptions {
  let signature = @base64.encode(
      self.key.sign(@utf8.encode(nonce)),
      padding=false,
    )
    .replace_all(old="+", new="-")
    .replace_all(old="/", new="_")
  match jwt {
    Some(jwt) => @wire.ConnectOptions::new(name~, jwt~, signature~)
    None => @wire.ConnectOptions::new(name~, nkey=self.public_key, signature~)
  }
}

///|
pub fn UserKey::signer(
  self : UserKey,
  jwt? : String,
) -> (String) -> @wire.ConnectOptions raise {
  nonce => self.sign_nonce(nonce, jwt?)
}

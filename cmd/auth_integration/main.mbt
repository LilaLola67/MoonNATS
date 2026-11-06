///|
async fn main {
  let key = @auth.UserKey::from_raw_seed(Bytes::make(32, 0))
  if @env.args().contains("--public") {
    println(key.public_key)
    return
  }
  let tls_server = ["tls://localhost:14223"]
  @client.with_client(
    @client.Options::new(
      servers=tls_server,
      connect=@wire.ConnectOptions::new(
        user="test-user",
        password="synthetic-test-password",
      ),
      trust=@client.CustomPemFile(".test-data/cert.pem"),
      heartbeat_ms=0,
    ),
    c => c.flush(),
  )
  @client.with_client(
    @client.Options::new(
      servers=tls_server,
      signer=key.signer(),
      trust=@client.CustomPemFile(".test-data/cert.pem"),
      heartbeat_ms=0,
    ),
    c => c.flush(),
  )
  assert_true(
    try {
      @client.with_client(
        @client.Options::new(
          servers=tls_server,
          connect=@wire.ConnectOptions::new(user="test-user", password="wrong"),
          trust=@client.CustomPemFile(".test-data/cert.pem"),
          timeout_ms=1000,
          heartbeat_ms=0,
        ),
        _ => (),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      @client.with_client(
        @client.Options::new(
          servers=tls_server,
          timeout_ms=1000,
          heartbeat_ms=0,
        ),
        _ => (),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      @client.with_client(
        @client.Options::new(
          servers=["tls://127.0.0.1:14223"],
          trust=@client.CustomPemFile(".test-data/cert.pem"),
          timeout_ms=1000,
          heartbeat_ms=0,
        ),
        _ => (),
      )
      false
    } catch {
      _ => true
    },
  )
  @client.with_client(
    @client.Options::new(
      servers=["nats://127.0.0.1:14224"],
      connect=@wire.ConnectOptions::new(token="synthetic-test-token"),
      heartbeat_ms=0,
    ),
    c => c.flush(),
  )
  println(
    "auth: TLS certificate trust, hostname rejection, invalid credentials, user/password, token and NKey nonce authentication PASS",
  )
}

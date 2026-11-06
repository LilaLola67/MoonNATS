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
  let jwt = @fs.read_text(".test-data/user.jwt")
  @client.with_client(
    @client.Options::new(
      servers=["nats://127.0.0.1:14227"],
      signer=key.signer(jwt~),
      heartbeat_ms=0,
    ),
    c => c.flush(),
  )
  let credentials = @auth.Credentials::parse(
    @fs.read_text(".test-data/user.creds"),
  )
  let mut calls = 0
  @client.with_client(
    @client.Options::new(servers=["nats://127.0.0.1:14227"], heartbeat_ms=0, credentials=nonce => {
      calls += 1
      let refreshed = @auth.Credentials::parse(
        @fs.read_text(".test-data/user.creds"),
      )
      refreshed.key.sign_nonce(nonce.unwrap(), jwt=refreshed.jwt)
    }),
    c => c.flush(),
  )
  assert_eq(calls, 1)
  assert_eq(credentials.key.public_key, key.public_key)
  @client.with_client(
    @client.Options::new(
      servers=["tls://localhost:14228"],
      tls_first=true,
      trust=@client.CustomPemFile(".test-data/cert.pem"),
      heartbeat_ms=0,
      connect=@wire.ConnectOptions::new(
        user="test-user",
        password="synthetic-test-password",
      ),
    ),
    c => c.flush(),
  )
  println(
    "auth: TLS certificate trust, hostname rejection, invalid credentials, user/password, token, NKey and operator/account JWT nonce authentication, standard creds, dynamic credentials and TLS-first PASS",
  )
}

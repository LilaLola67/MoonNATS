# Third-party sources

MoonNATS source is licensed under Apache-2.0. Protocol definitions and compatibility behavior are referenced from the following primary sources; no nats.go source has been copied or translated into this repository.

| Source | Use | License |
| --- | --- | --- |
| [NATS documentation](https://docs.nats.io/reference/protocols/client) | Client wire protocol and JetStream API behavior | Upstream documentation terms |
| [nats-io/nats.go](https://github.com/nats-io/nats.go) | API capability reference; KV and Object Store interoperability conventions | Apache-2.0 |
| [MoonBit core](https://github.com/moonbitlang/core) | Bundled async runtime, networking, TLS, JSON and encodings | Apache-2.0 |
| [moonbitlang/x](https://github.com/moonbitlang/x) | SHA-256, UTC time formatting and system utilities | Apache-2.0 |
| [hustcer/mbt-ed25519](https://github.com/hustcer/mbt-ed25519) | Dependency for Ed25519 key derivation and signatures | Apache-2.0 |
| [nats-io/nats-server](https://github.com/nats-io/nats-server) | External test server, downloaded separately; not bundled | Apache-2.0 |
| [nats-io/nats.py](https://github.com/nats-io/nats.py) | External independent interoperability tests; not runtime code | Apache-2.0 |

Dependencies are resolved by the MoonBit toolchain and are not vendored into this repository. Test downloads, generated certificates and server data are excluded from both Git and package archives. The all-zero signing seed in the authentication regression vector is public synthetic test data and must never be used as a deployment key.

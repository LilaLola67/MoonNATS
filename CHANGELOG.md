# Changelog

## 0.3.0

- Add scoped bounded JetStream publishers, continuous concurrent workers, typed configs and full pagination.
- Add Push heartbeat/flow-control handling and ordered replay with consumer-loss/reconnect recovery.
- Add standard microservice endpoints, queue groups, discovery, errors, statistics and graceful stopping.
- Add KV configuration snapshots and resynchronization; Object Store metadata watches, links and sealing.
- Add reconnect jitter, Lame Duck events, opt-in bounded unsent-message buffering and Prometheus metrics.
- Add standard .creds parsing, connection-time credential refresh and native TLS-first.
- Add independent micro/object-link interop, fragmentation/Lame Duck injection, persistent restart tests and benchmarks.

## 0.2.0

- Complete headers, connection negotiation and exact JetStream metadata parsing.
- Add scoped async TCP/TLS and browser-standard WebSocket clients.
- Add request/reply, queue groups, reconnect, cancellation cleanup, backpressure and drain.
- Add NKey and JWT nonce signing helpers, typed codecs and diagnostics CLI.
- Add JetStream management/pull consumption, KV and SHA-256-verified Object Store.
- Add independent-client interoperability, authentication, server restart and browser examples.

## 0.1.0

- Initial incremental protocol core, boundary tests and offline example.

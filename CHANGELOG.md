# Changelog

## 0.3.2

- Add a real three-node cluster acceptance suite with 50,000 cross-node Core messages and 12,000 replicated tasks across two load phases.
- Verify seed-only discovery, subscription recovery, task redelivery after a stream leader crash, and durable progress through a consumer leader restart.
- Check all three local replica stores and durable acknowledgement progress using the official Python client and server monitoring.

## 0.3.1

- Resume durable workers after transient fetch failures and optionally extend the acknowledgement deadline during a handler.
- Add a TLS-only endpoint policy for initial connections, failover and discovery.
- Exclude expired bucket values from KV cache reads and recognize server expiration markers.
- Verify Core and JetStream examples from a packaged dependency in a fresh MoonBit module.

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

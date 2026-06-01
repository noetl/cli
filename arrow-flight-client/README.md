# noetl-arrow-flight-client

Rust client for the [NoETL server's Arrow Flight `do_get` endpoint][server-flight]
(R-2.3 Phase A).  Fetches tabular result-store payloads as Arrow IPC
streams over gRPC, returning typed `RecordBatch`es with zero JSON
serialisation overhead in the consumer leg.

See [Appendix H §H.4 (data plane)][appendix-h] for the architectural
context and the [Arrow Flight Result Fetch wiki page][wiki] for the
wire protocol contract.

## Use

```rust
use noetl_arrow_flight_client::FlightResolver;

let resolver = FlightResolver::connect("grpc://noetl.noetl.svc.cluster.local:8083").await?;
let batches = resolver
    .resolve("noetl://execution/12345/result/big_select/abcd1234")
    .await?;
```

The returned `Vec<RecordBatch>` can be inspected via the `arrow`
crate's `RecordBatch::column` / `RecordBatch::schema` accessors, or
flattened to JSON via the [`resolve_rows`][resolve-rows] convenience
helper.

## Why a separate crate

The Flight client is reusable across multiple Rust consumers:

- The **noetl-worker** that needs to fetch a cross-node tabular
  result before a downstream step runs.
- The **Rust noetl-server** (under `repos/server`) once it gains a
  result-store backend and needs to proxy fetches.
- The **CLI** tree walker (under `noetl-executor`) for local-mode
  consumers that share the same wire format.

Keeping the client in its own crate avoids coupling these consumers
to each other's Cargo build graphs.

## R-2.3 phase coordination

| Phase | Status |
|-------|--------|
| A — Python server `do_get` endpoint | ✅ landed (noetl/noetl#643) |
| **B — this crate** | ✅ shipping (skeleton); worker callsite deferred to a follow-up once a real consumer surfaces |
| C — `FlightInfo` discovery + mTLS | Planned |

## See also

- [`noetl-arrow-cache`](../arrow-cache) — same workspace's sibling
  crate for the same-node shared-memory cache that consumers can
  attach to when the Flight server's `IpcHint` indicates a colocated
  shm region exists.
- [`noetl-tools::arrow_codec`](https://github.com/noetl/tools/blob/main/src/arrow_codec.rs)
  — the encoder the server uses to produce these IPC streams.

[server-flight]: https://github.com/noetl/noetl/blob/main/noetl/server/api/result/flight_server.py
[appendix-h]: https://noetl.dev/docs/architecture/noetl_global_hybrid_cloud_grid_distributed_architecture_blueprint#h4-data-plane
[wiki]: https://github.com/noetl/noetl/wiki/arrow_flight_result_fetch
[resolve-rows]: https://docs.rs/noetl-arrow-flight-client/latest/noetl_arrow_flight_client/struct.FlightResolver.html#method.resolve_rows

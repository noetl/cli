# noetl-arrow-cache

Rust mirror of Python's
[`ArrowIpcSharedMemoryCache`](https://github.com/noetl/noetl/blob/main/noetl/core/storage/ipc_cache.py).
Same-node zero-copy IPC for the NoETL columnar data plane — R-2.1
of the Rust migration roadmap (Appendix H of the global hybrid
cloud blueprint).

## What this gives you

A small in-process cache keyed by an `IpcHint` token.  Producers
push Arrow IPC byte streams into shared memory; colocated consumers
read them back without going through the durable storage path.
The hint is JSON-serialisable and crosses process boundaries
through any transport (NATS, HTTP, files, etc.) — but the bytes
themselves stay on the same machine.

## Cross-stack compatibility

The shm name format, lease policy, eviction order, and `IpcHint`
JSON shape match the Python implementation 1:1 so a hint produced
by either stack deserialises cleanly on the other.  Both sides
use POSIX `shm_open` + `mmap` (Python via
`multiprocessing.shared_memory.SharedMemory`, Rust via the
`shared_memory` crate).

## Example

```rust
use noetl_arrow_cache::{ArrowIpcSharedMemoryCache, IpcHint};

// One cache per process.  Defaults read NOETL_IPC_CACHE_BUDGET_BYTES,
// HOSTNAME, NOETL_NODE_ID from env.
let cache = ArrowIpcSharedMemoryCache::new();

// Produce — `payload` is the Arrow IPC stream bytes from
// `noetl_tools::arrow_codec::encode_record_batch`.
let hint = cache.put_arrow_ipc(
    payload_bytes,
    schema_digest,
    Some(row_count as u64),
    None, // default lease (60s)
)?;

// Pass `hint` over the wire to a colocated consumer ...

// Consume — same node, same hint JSON, gets the bytes back.
let bytes = cache.get(&hint)?;
```

## Configuration

| Env var | Default | Purpose |
|---|---|---|
| `NOETL_IPC_CACHE_BUDGET_BYTES` | 268435456 (256 MB) | Soft budget; cache evicts oldest-by-lease until it fits. |
| `HOSTNAME` | `unknown` | Stamped onto every produced hint as the `producer` field. |
| `NOETL_NODE_ID` / `NODE_NAME` / `K8S_NODE_NAME` / `HOSTNAME` | `unknown` | Producer node identity; consumers refuse to attach when this differs from their local node id. |

## What this crate is NOT

- Not a durable store.  Bytes evict on lease expiry + budget pressure.
  The durable copy (e.g. SeaweedFS / GCS / disk) is the authority;
  this cache is an acceleration for the colocated hot path.
- Not an Arrow IPC codec.  Encoding / decoding lives in
  `noetl-tools::arrow_codec`; this crate moves opaque bytes.
- Not network-aware.  The `IpcHint.node_id` field tells consumers
  to fall back to the durable copy when the producer is on a
  different node.

## See also

- [`noetl-tools::arrow_codec`](https://docs.rs/noetl-tools) — Arrow IPC encode/decode (the layer that produces the bytes this cache stores).
- Python counterpart: [`noetl.core.storage.ipc_cache`](https://github.com/noetl/noetl/blob/main/noetl/core/storage/ipc_cache.py).
- Appendix H, § R-2: [global hybrid cloud blueprint](https://noetl.dev/docs/architecture/noetl_global_hybrid_cloud_grid_distributed_architecture_blueprint).

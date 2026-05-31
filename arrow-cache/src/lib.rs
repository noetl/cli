//! Arrow IPC shared-memory cache — same-node zero-copy data plane.
//!
//! Rust mirror of the Python
//! [`ArrowIpcSharedMemoryCache`][py-cache] in
//! `noetl/core/storage/ipc_cache.py`.  Both implementations use POSIX
//! `shm_open` + `mmap` under the hood (Python via
//! `multiprocessing.shared_memory.SharedMemory`, Rust via the
//! [`shared_memory`](https://docs.rs/shared_memory) crate), so a
//! payload written by either side is readable by the other AS LONG AS
//! they share the same node identity (the `node_id` field on the
//! produced [`IpcHint`]).
//!
//! Per Appendix H of the global hybrid cloud blueprint — R-2.1 of the
//! Rust migration roadmap.  See
//! [executor-crate-architecture][cli-wiki] on the noetl/cli wiki for
//! the wider architectural context.
//!
//! ## Contract with the Python side
//!
//! The Python `ArrowIpcSharedMemoryCache` produces `IpcHint` JSON
//! payloads with these fields (`noetl/core/storage/models.py`):
//!
//! - `kind` (always `"arrow_ipc"`)
//! - `shm_name`
//! - `schema_digest`
//! - `byte_length`
//! - `row_count` (optional)
//! - `producer` (optional)
//! - `node_id` (optional)
//! - `lease_expires_at` (optional, UTC ISO 8601)
//! - `media_type` (defaults to `"application/vnd.apache.arrow.stream"`)
//!
//! [`IpcHint`] in this crate serialises to the exact same JSON shape.
//!
//! ## Lease + eviction
//!
//! Entries carry a lease expiry.  [`ArrowIpcSharedMemoryCache::put_arrow_ipc`]
//! calls [`ArrowIpcSharedMemoryCache::sweep_expired`] + budget-based
//! eviction before allocating, so under steady-state load the cache
//! self-trims to fit `budget_bytes` (default 256 MB, configurable via
//! the `NOETL_IPC_CACHE_BUDGET_BYTES` env var on construction).
//!
//! ## Why a separate crate
//!
//! The shared-memory dep (`shared_memory`) brings POSIX-specific
//! crates that don't belong in `noetl-tools` (which builds for many
//! contexts including the worker binary).  The cache is also a
//! standalone concept — both the CLI's local runtime and the worker's
//! NATS dispatch loop can use it to publish IPC hints for the
//! consuming step.  Keeping it as a sibling crate to `noetl-executor`
//! mirrors the layout of the broader Rust workspace.
//!
//! [py-cache]: https://github.com/noetl/noetl/blob/main/noetl/core/storage/ipc_cache.py
//! [cli-wiki]: https://github.com/noetl/cli/wiki/executor-crate-architecture

#![warn(missing_docs)]

mod cache;
mod hint;

pub use cache::{ArrowIpcSharedMemoryCache, CacheConfig};
pub use hint::IpcHint;

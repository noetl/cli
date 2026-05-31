//! [`ArrowIpcSharedMemoryCache`] — the put/get/delete surface.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use sha2::{Digest, Sha256};
use shared_memory::ShmemConf;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::hint::IpcHint;

const DEFAULT_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_LEASE_SECONDS: f64 = 60.0;
const NAMESPACE_PREFIX_MAX: usize = 12;

/// Construction parameters for [`ArrowIpcSharedMemoryCache`].
///
/// All fields have sensible defaults sourced from environment when
/// applicable — call sites only override what they need.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Short prefix attached to every shm name (max 12 chars after
    /// sanitisation; matches Python's behaviour).  Defaults to
    /// `"noetl"`.
    pub namespace: String,

    /// Budget in bytes the cache stays under via lease + LRU eviction.
    /// Defaults to `NOETL_IPC_CACHE_BUDGET_BYTES` env var or 256 MB.
    pub budget_bytes: u64,

    /// Lease length applied to entries when the caller doesn't
    /// specify one explicitly.  Defaults to 60s — same as Python.
    pub default_lease_seconds: f64,

    /// Identity of the producing process.  Defaults to the
    /// `HOSTNAME` env var or `"unknown"`.  Stamped onto every
    /// produced hint for diagnostic correlation.
    pub producer: String,

    /// Producer node identity.  Defaults to (in order):
    /// `NOETL_NODE_ID`, `NODE_NAME`, `K8S_NODE_NAME`, `HOSTNAME`,
    /// or `"unknown"`.  Consumers refuse to attach when this
    /// differs from their local node id — POSIX shm is
    /// per-machine.
    pub node_id: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        let budget_bytes = std::env::var("NOETL_IPC_CACHE_BUDGET_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_BUDGET_BYTES);
        Self {
            namespace: "noetl".to_string(),
            budget_bytes,
            default_lease_seconds: DEFAULT_LEASE_SECONDS,
            producer: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
            node_id: detect_node_id(),
        }
    }
}

fn detect_node_id() -> String {
    for key in ["NOETL_NODE_ID", "NODE_NAME", "K8S_NODE_NAME", "HOSTNAME"] {
        if let Ok(v) = std::env::var(key) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    "unknown".to_string()
}

/// Same-node IPC cache for Arrow IPC byte streams.
///
/// Wraps POSIX `shm_open` + `mmap` (via the `shared_memory` crate)
/// to expose a small in-memory key/value store keyed by the cache-
/// produced `shm_name`.  Producers call [`put_arrow_ipc`][put];
/// consumers receive the returned [`IpcHint`] over the wire and
/// call [`get`][get] to materialise the bytes.
///
/// Durable payload storage is the authority — this cache is an
/// **optional** acceleration for colocated producer/consumer pairs.
/// A consumer that finds the entry expired or evicted falls back
/// to the durable copy via the normal `result_uri` path.
///
/// [put]: ArrowIpcSharedMemoryCache::put_arrow_ipc
/// [get]: ArrowIpcSharedMemoryCache::get
pub struct ArrowIpcSharedMemoryCache {
    config: CacheConfig,
    /// Tracks live shm regions for budget accounting + eviction.
    /// Held behind a Mutex because `put_arrow_ipc` / `delete` mutate
    /// it; the lock window is short (no shm syscalls happen inside
    /// it).
    entries: Mutex<HashMap<String, EntryMeta>>,
}

#[derive(Debug, Clone)]
struct EntryMeta {
    byte_length: u64,
    lease_expires_at: DateTime<Utc>,
}

impl ArrowIpcSharedMemoryCache {
    /// Construct a cache with default config (env-derived).
    pub fn new() -> Self {
        Self::with_config(CacheConfig::default())
    }

    /// Construct a cache with an explicit config.
    pub fn with_config(mut config: CacheConfig) -> Self {
        // Sanitize + truncate the namespace the same way Python does
        // so cross-stack names round-trip.  Python's _SAFE_NAME regex
        // is `[^A-Za-z0-9_]`; we apply the same here.
        config.namespace = sanitize_namespace(&config.namespace);
        Self {
            config,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Total bytes currently allocated across live entries.
    pub fn used_bytes(&self) -> u64 {
        self.entries
            .lock()
            .expect("cache entries mutex poisoned")
            .values()
            .map(|e| e.byte_length)
            .sum()
    }

    /// Borrow the active config (read-only).  Useful for tests +
    /// diagnostics that want to log the effective budget / node id.
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Allocate a shared-memory region, copy `payload` into it,
    /// register it under a lease, and return the [`IpcHint`] a
    /// consumer needs to attach.
    ///
    /// Workflow:
    ///
    /// 1. Sweep expired entries (lazy GC piggy-backed on writes).
    /// 2. Evict-by-lease until `used_bytes + payload.len() <= budget`.
    /// 3. Compute a fresh shm name from `(namespace, time-stamp,
    ///    sha256(payload)[:8])`.
    /// 4. Open + size the shm region; copy bytes.
    /// 5. Stamp the hint with producer + node id + lease.
    ///
    /// Errors:
    ///
    /// - `payload` larger than the budget after eviction.
    /// - shm allocation failure (out-of-space, name collision, etc.).
    pub fn put_arrow_ipc(
        &self,
        payload: &[u8],
        schema_digest: &str,
        row_count: Option<u64>,
        lease_seconds: Option<f64>,
    ) -> Result<IpcHint> {
        if schema_digest.is_empty() {
            return Err(anyhow!("schema_digest is required"));
        }
        let payload_len = payload.len() as u64;
        if payload_len > self.config.budget_bytes {
            return Err(anyhow!(
                "payload exceeds IPC cache budget: {} > {}",
                payload_len,
                self.config.budget_bytes
            ));
        }

        // Lazy GC + budget enforcement BEFORE we open a new region.
        self.sweep_expired(Utc::now(), 0.0)?;
        self.evict_until_fits(payload_len)?;

        let name = self.next_shm_name(payload);
        let mut region = ShmemConf::new()
            .size(payload.len().max(1))
            .os_id(&name)
            .create()
            .with_context(|| format!("create shm region {}", name))?;

        // SAFETY: `region.as_ptr()` is a valid, exclusive
        // pointer to a region of `payload.len()` bytes we just
        // allocated.  We never alias it elsewhere within this scope.
        unsafe {
            let dst = region.as_ptr();
            std::ptr::copy_nonoverlapping(payload.as_ptr(), dst, payload.len());
        }

        // CRITICAL: release ownership BEFORE dropping the handle.
        // `shared_memory` defaults `owner = true` on `create()`,
        // which means `Drop::drop` unlinks the OS region — that
        // would defeat the whole point of the cache.  We keep the
        // region alive across the producer call; eviction +
        // `delete()` are the only paths that unlink.
        region.set_owner(false);
        drop(region);

        let lease_seconds = lease_seconds.unwrap_or(self.config.default_lease_seconds);
        let lease_expires_at = Utc::now() + Duration::milliseconds((lease_seconds * 1000.0) as i64);

        {
            let mut entries = self.entries.lock().expect("cache entries mutex poisoned");
            entries.insert(
                name.clone(),
                EntryMeta {
                    byte_length: payload_len,
                    lease_expires_at,
                },
            );
        }

        let mut hint = IpcHint::new(name, schema_digest, payload_len);
        hint.row_count = row_count;
        hint.producer = Some(self.config.producer.clone());
        hint.node_id = Some(self.config.node_id.clone());
        hint.lease_expires_at = Some(lease_expires_at);
        Ok(hint)
    }

    /// Read the bytes referenced by `hint` out of shared memory.
    ///
    /// Returns an error if:
    /// - the hint has expired,
    /// - the hint's `node_id` differs from this cache's node id
    ///   (POSIX shm is per-machine; cross-node attach is a bug),
    /// - the underlying shm region was evicted or is unreadable.
    pub fn get(&self, hint: &IpcHint) -> Result<Vec<u8>> {
        if hint.is_expired(Utc::now()) {
            return Err(anyhow!("IPC hint expired: {}", hint.shm_name));
        }
        if let Some(remote_node) = &hint.node_id {
            if remote_node != &self.config.node_id {
                return Err(anyhow!(
                    "IPC hint belongs to node {}; local node is {}",
                    remote_node,
                    self.config.node_id
                ));
            }
        }

        let region = ShmemConf::new()
            .os_id(&hint.shm_name)
            .open()
            .with_context(|| format!("open shm region {}", hint.shm_name))?;

        let length = hint.byte_length as usize;
        if region.len() < length {
            return Err(anyhow!(
                "shm region {} smaller than expected: {} < {}",
                hint.shm_name,
                region.len(),
                length
            ));
        }

        // SAFETY: `region.as_ptr()` is a valid, exclusive pointer to
        // at least `region.len() >= length` bytes; we copy
        // immediately into an owned `Vec<u8>` and drop the region.
        let mut buf = vec![0u8; length];
        unsafe {
            std::ptr::copy_nonoverlapping(region.as_ptr(), buf.as_mut_ptr(), length);
        }
        Ok(buf)
    }

    /// Delete the shm region referenced by `name`.  Returns `true`
    /// on successful unlink, `false` when the region wasn't found
    /// (idempotent).
    pub fn delete(&self, name: &str) -> Result<bool> {
        {
            let mut entries = self.entries.lock().expect("cache entries mutex poisoned");
            entries.remove(name);
        }
        // Open + drop with `set_owner_to_drop(true)` triggers the
        // unlink path on macOS/Linux.
        match ShmemConf::new().os_id(name).open() {
            Ok(mut region) => {
                region.set_owner(true);
                drop(region);
                Ok(true)
            }
            Err(shared_memory::ShmemError::MapOpenFailed(_)) => Ok(false),
            Err(e) => Err(anyhow!("delete shm region {}: {}", name, e)),
        }
    }

    /// Reclaim entries whose lease has expired (plus an optional
    /// grace window).  Returns the count of regions unlinked.
    pub fn sweep_expired(&self, now: DateTime<Utc>, grace_seconds: f64) -> Result<usize> {
        let grace = Duration::milliseconds((grace_seconds * 1000.0) as i64);
        let to_delete: Vec<String> = {
            let entries = self.entries.lock().expect("cache entries mutex poisoned");
            entries
                .iter()
                .filter(|(_, meta)| now > meta.lease_expires_at + grace)
                .map(|(name, _)| name.clone())
                .collect()
        };

        let mut deleted = 0usize;
        for name in to_delete {
            if self.delete(&name)? {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    fn evict_until_fits(&self, incoming_bytes: u64) -> Result<()> {
        loop {
            let (would_overflow, oldest_name) = {
                let entries = self.entries.lock().expect("cache entries mutex poisoned");
                let used: u64 = entries.values().map(|e| e.byte_length).sum();
                if used + incoming_bytes <= self.config.budget_bytes {
                    return Ok(());
                }
                if entries.is_empty() {
                    return Err(anyhow!(
                        "not enough IPC cache budget after eviction: \
                         {} (incoming) + {} (used) > {} (budget)",
                        incoming_bytes,
                        used,
                        self.config.budget_bytes
                    ));
                }
                // Pick oldest by lease expiry (matches Python's
                // `min(_, key=lease_expires_at)`).
                let oldest = entries
                    .iter()
                    .min_by_key(|(_, meta)| meta.lease_expires_at)
                    .map(|(name, _)| name.clone())
                    .expect("non-empty checked above");
                (true, oldest)
            };
            if would_overflow {
                self.delete(&oldest_name)?;
            }
        }
    }

    fn next_shm_name(&self, payload: &[u8]) -> String {
        // Match the Python format exactly:
        //   {namespace[:12]}_{stamp:8 hex chars of micros}_{digest:8}
        let digest_full = Sha256::digest(payload);
        let digest = hex::encode(&digest_full[..4]); // 8 hex chars
        let micros = Utc::now().timestamp_micros() as u64;
        // 8 hex chars of the low bits of micros (matches Python's
        // `format(int(time*1e6), 'x')[-8:]`).
        let stamp = format!("{:08x}", micros & 0xFFFF_FFFF);
        format!("{}_{}_{}", self.config.namespace, stamp, digest)
    }
}

impl Default for ArrowIpcSharedMemoryCache {
    fn default() -> Self {
        Self::new()
    }
}

fn sanitize_namespace(input: &str) -> String {
    let mut out = String::with_capacity(input.len().min(NAMESPACE_PREFIX_MAX));
    for ch in input.chars().take(NAMESPACE_PREFIX_MAX * 2) {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
        if out.len() >= NAMESPACE_PREFIX_MAX {
            break;
        }
    }
    if out.is_empty() {
        out.push_str("noetl");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_test_namespace() -> String {
        // Avoid collisions between tests + concurrent runs.
        let micros = Utc::now().timestamp_micros();
        format!("t{:x}", micros & 0xFF_FFFF)
    }

    fn test_cache() -> ArrowIpcSharedMemoryCache {
        ArrowIpcSharedMemoryCache::with_config(CacheConfig {
            namespace: unique_test_namespace(),
            budget_bytes: 4 * 1024,
            default_lease_seconds: 5.0,
            producer: "test".to_string(),
            node_id: "test-node".to_string(),
        })
    }

    #[test]
    fn round_trip_payload_is_byte_for_byte() {
        let cache = test_cache();
        let payload = b"hello arrow ipc world";
        let hint = cache.put_arrow_ipc(payload, "deadbeef", Some(1), None).expect("put");
        assert_eq!(hint.byte_length, payload.len() as u64);
        assert_eq!(hint.schema_digest, "deadbeef");
        assert_eq!(hint.node_id.as_deref(), Some("test-node"));
        assert_eq!(hint.producer.as_deref(), Some("test"));

        let got = cache.get(&hint).expect("get");
        assert_eq!(got.as_slice(), payload);

        cache.delete(&hint.shm_name).expect("delete");
    }

    #[test]
    fn schema_digest_required() {
        let cache = test_cache();
        let err = cache.put_arrow_ipc(b"data", "", None, None).unwrap_err();
        assert!(err.to_string().contains("schema_digest is required"));
    }

    #[test]
    fn payload_over_budget_rejected() {
        let cache = ArrowIpcSharedMemoryCache::with_config(CacheConfig {
            namespace: unique_test_namespace(),
            budget_bytes: 16,
            default_lease_seconds: 5.0,
            producer: "test".to_string(),
            node_id: "test-node".to_string(),
        });
        let err = cache.put_arrow_ipc(&[0u8; 32], "feedface", None, None).unwrap_err();
        assert!(err.to_string().contains("exceeds IPC cache budget"));
    }

    #[test]
    fn cross_node_get_is_rejected() {
        let cache = ArrowIpcSharedMemoryCache::with_config(CacheConfig {
            namespace: unique_test_namespace(),
            budget_bytes: 1024,
            default_lease_seconds: 5.0,
            producer: "test".to_string(),
            node_id: "node-a".to_string(),
        });
        let payload = b"node-a data";
        let mut hint = cache.put_arrow_ipc(payload, "deadbeef", None, None).expect("put");
        hint.node_id = Some("node-b".to_string());

        let err = cache.get(&hint).unwrap_err();
        assert!(err.to_string().contains("belongs to node"));
        cache.delete(&hint.shm_name).expect("delete");
    }

    #[test]
    fn expired_hint_is_rejected_on_get() {
        let cache = test_cache();
        let payload = b"soon to expire";
        let mut hint = cache
            .put_arrow_ipc(payload, "deadbeef", None, Some(0.001))
            .expect("put");
        // Force expiry by setting the lease in the past.
        hint.lease_expires_at = Some(Utc::now() - Duration::seconds(1));
        let err = cache.get(&hint).unwrap_err();
        assert!(err.to_string().contains("expired"));
        cache.delete(&hint.shm_name).expect("delete");
    }

    #[test]
    fn sweep_expired_reclaims_lease_expired_entries() {
        let cache = test_cache();
        let payload = b"some bytes";
        let hint = cache
            .put_arrow_ipc(payload, "deadbeef", None, Some(0.001))
            .expect("put");
        // Sleep just past the 1ms lease.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let count = cache.sweep_expired(Utc::now(), 0.0).expect("sweep");
        assert!(count >= 1, "at least one entry must have been swept");
        // Re-get should fail because the shm region is gone.
        let err = cache.get(&hint).unwrap_err();
        assert!(
            err.to_string().contains("expired") || err.to_string().contains("open shm region"),
            "got: {}",
            err
        );
    }

    #[test]
    fn delete_is_idempotent() {
        let cache = test_cache();
        let hint = cache.put_arrow_ipc(b"x", "deadbeef", None, None).expect("put");
        assert!(cache.delete(&hint.shm_name).expect("first delete"));
        // Second delete returns false (region already gone) without
        // erroring.
        assert!(!cache.delete(&hint.shm_name).expect("second delete"));
    }

    #[test]
    fn evict_until_fits_drops_oldest_to_make_room() {
        let cache = ArrowIpcSharedMemoryCache::with_config(CacheConfig {
            namespace: unique_test_namespace(),
            budget_bytes: 64,
            default_lease_seconds: 60.0,
            producer: "test".to_string(),
            node_id: "test-node".to_string(),
        });
        // First put fills ~ half the budget.
        let first = cache
            .put_arrow_ipc(&[1u8; 30], "deadbeef", None, None)
            .expect("first put");
        // Second put would push the budget over → must evict the first.
        let second = cache
            .put_arrow_ipc(&[2u8; 40], "feedface", None, None)
            .expect("second put");
        // First entry is gone.
        let err = cache.get(&first).unwrap_err();
        assert!(err.to_string().contains("open shm region"), "got: {}", err);
        // Second entry is still readable.
        let bytes = cache.get(&second).expect("second get");
        assert_eq!(bytes, vec![2u8; 40]);
        cache.delete(&second.shm_name).expect("cleanup");
    }

    #[test]
    fn used_bytes_tracks_live_entries() {
        let cache = test_cache();
        assert_eq!(cache.used_bytes(), 0);
        let h1 = cache.put_arrow_ipc(&[0u8; 100], "a", None, None).expect("put");
        assert_eq!(cache.used_bytes(), 100);
        let h2 = cache.put_arrow_ipc(&[0u8; 200], "b", None, None).expect("put");
        assert_eq!(cache.used_bytes(), 300);
        cache.delete(&h1.shm_name).expect("delete h1");
        assert_eq!(cache.used_bytes(), 200);
        cache.delete(&h2.shm_name).expect("delete h2");
        assert_eq!(cache.used_bytes(), 0);
    }

    #[test]
    fn namespace_sanitisation_matches_python() {
        // Python's `_SAFE_NAME = re.compile(r"[^A-Za-z0-9_]")`
        // replaces every non-alphanumeric/underscore character with
        // `_`, then truncates to 12 chars.
        assert_eq!(sanitize_namespace("ok"), "ok");
        assert_eq!(sanitize_namespace("noetl-prod"), "noetl_prod");
        assert_eq!(sanitize_namespace("a/b.c@d#e"), "a_b_c_d_e");
        // Truncated to 12.
        assert_eq!(sanitize_namespace("abcdefghijklmnop"), "abcdefghijkl");
        // Empty falls back to default.
        assert_eq!(sanitize_namespace(""), "noetl");
    }

    /// Smoke test: cache name format follows the same shape as
    /// Python's `{namespace[:12]}_{stamp[:8]}_{digest[:8]}` so an
    /// observer parsing names off `ls /dev/shm` can tell which
    /// producer made them.
    #[test]
    fn shm_name_shape_matches_python_format() {
        let cache = ArrowIpcSharedMemoryCache::with_config(CacheConfig {
            namespace: "tns".to_string(),
            budget_bytes: 1024,
            default_lease_seconds: 5.0,
            producer: "test".to_string(),
            node_id: "test-node".to_string(),
        });
        let hint = cache
            .put_arrow_ipc(b"name shape test", "deadbeef", None, None)
            .expect("put");
        let parts: Vec<&str> = hint.shm_name.splitn(3, '_').collect();
        assert_eq!(parts.len(), 3, "expected three `_`-joined segments");
        assert_eq!(parts[0], "tns");
        assert_eq!(parts[1].len(), 8, "stamp segment is 8 hex chars");
        assert_eq!(parts[2].len(), 8, "digest segment is 8 hex chars");
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()), "stamp must be hex");
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()), "digest must be hex");
        cache.delete(&hint.shm_name).expect("delete");
    }
}

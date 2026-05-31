//! [`IpcHint`] — the wire-format handle the cache produces.
//!
//! Mirrors the Python `IpcHint` Pydantic model in
//! `noetl/core/storage/models.py` 1:1 so a hint produced by either
//! stack deserialises cleanly on the other.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Best-effort same-node shared-memory hint for a durable payload
/// reference.
///
/// JSON shape (matches Python's `IpcHint`):
///
/// ```json
/// {
///   "kind": "arrow_ipc",
///   "shm_name": "noetl_abc12345_def67890",
///   "schema_digest": "8badf00d",
///   "byte_length": 4096,
///   "row_count": 128,
///   "producer": "noetl-worker-rust-7",
///   "node_id": "kind-noetl",
///   "lease_expires_at": "2026-05-31T08:00:00Z",
///   "media_type": "application/vnd.apache.arrow.stream"
/// }
/// ```
///
/// `kind` is fixed to `"arrow_ipc"` for forward compatibility — if
/// future cache variants surface (e.g. `"arrow_file"`), they get
/// their own dispatched type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpcHint {
    /// Discriminator — always `"arrow_ipc"`.  Defaulted so
    /// pre-discriminator JSON deserialises cleanly.
    #[serde(default = "default_kind")]
    pub kind: String,

    /// Shared-memory region name as it appears in `/dev/shm` on
    /// Linux.  Producer fills this from
    /// [`ArrowIpcSharedMemoryCache::put_arrow_ipc`]; consumer feeds
    /// it back into `get`.
    pub shm_name: String,

    /// Schema fingerprint — 8 hex chars of the Arrow schema's
    /// SHA-256.  Lets consumers cheaply detect schema drift before
    /// attaching the shared-memory region.
    pub schema_digest: String,

    /// Size of the payload bytes in the shared-memory region.
    pub byte_length: u64,

    /// Optional row count for diagnostic + telemetry use.  The
    /// cache itself doesn't read or validate the bytes — this is
    /// metadata only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,

    /// Identity of the process that produced the entry.  Useful
    /// for cross-component correlation in distributed traces;
    /// purely informational on this side of the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<String>,

    /// Producer node identity.  Consumers MUST skip IPC attach
    /// when this differs from the local node — POSIX shm is
    /// per-machine, so cross-node attach would either silently
    /// return the wrong bytes (if names collide) or fail with
    /// `ENOENT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,

    /// UTC instant after which the entry MAY be reclaimed by the
    /// producer's eviction sweep.  Consumers SHOULD attempt the
    /// read promptly; `None` means "no lease, eviction-only" (the
    /// producer manages lifetime by hand).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,

    /// Media type of the bytes in the region.  Defaults to the
    /// Arrow IPC stream MIME so consumers without a hint can still
    /// pick the right decoder.
    #[serde(default = "default_media_type")]
    pub media_type: String,
}

impl IpcHint {
    /// Construct a fresh hint with defaults for `kind` +
    /// `media_type`.  Producers prefer this over the struct
    /// literal so the discriminator + media type stay in sync.
    pub fn new(shm_name: impl Into<String>, schema_digest: impl Into<String>, byte_length: u64) -> Self {
        Self {
            kind: default_kind(),
            shm_name: shm_name.into(),
            schema_digest: schema_digest.into(),
            byte_length,
            row_count: None,
            producer: None,
            node_id: None,
            lease_expires_at: None,
            media_type: default_media_type(),
        }
    }

    /// Returns true if `lease_expires_at` is set AND `now` is past
    /// it.  Hints without a lease never expire (the producer
    /// manages lifetime out-of-band).
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        match self.lease_expires_at {
            Some(expires) => now > expires,
            None => false,
        }
    }
}

fn default_kind() -> String {
    "arrow_ipc".to_string()
}

fn default_media_type() -> String {
    "application/vnd.apache.arrow.stream".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python `IpcHint` JSON deserialises cleanly into the Rust
    /// struct — the cross-stack handoff is the load-bearing
    /// contract for R-2.1.
    #[test]
    fn deserialises_python_ipc_hint_json() {
        let wire = serde_json::json!({
            "kind": "arrow_ipc",
            "shm_name": "noetl_abc12345_def67890",
            "schema_digest": "8badf00d",
            "byte_length": 4096,
            "row_count": 128,
            "producer": "noetl-worker-rust-7",
            "node_id": "kind-noetl",
            "lease_expires_at": "2026-05-31T08:00:00Z",
            "media_type": "application/vnd.apache.arrow.stream"
        });
        let hint: IpcHint = serde_json::from_value(wire).unwrap();
        assert_eq!(hint.kind, "arrow_ipc");
        assert_eq!(hint.shm_name, "noetl_abc12345_def67890");
        assert_eq!(hint.schema_digest, "8badf00d");
        assert_eq!(hint.byte_length, 4096);
        assert_eq!(hint.row_count, Some(128));
        assert_eq!(hint.producer.as_deref(), Some("noetl-worker-rust-7"));
        assert_eq!(hint.node_id.as_deref(), Some("kind-noetl"));
        assert_eq!(hint.media_type, "application/vnd.apache.arrow.stream");
        assert!(hint.lease_expires_at.is_some());
    }

    /// Older Python clients (or future minimal Rust callers) send
    /// only the required fields — the `kind` + `media_type`
    /// defaults must kick in.
    #[test]
    fn deserialises_minimal_ipc_hint_json() {
        let wire = serde_json::json!({
            "shm_name": "noetl_test",
            "schema_digest": "00000000",
            "byte_length": 0,
        });
        let hint: IpcHint = serde_json::from_value(wire).unwrap();
        assert_eq!(hint.kind, "arrow_ipc");
        assert_eq!(hint.media_type, "application/vnd.apache.arrow.stream");
        assert!(hint.row_count.is_none());
        assert!(hint.producer.is_none());
        assert!(hint.node_id.is_none());
        assert!(hint.lease_expires_at.is_none());
    }

    /// `IpcHint::new` returns a hint with sensible defaults.
    #[test]
    fn new_uses_canonical_defaults() {
        let hint = IpcHint::new("noetl_x", "deadbeef", 1024);
        assert_eq!(hint.kind, "arrow_ipc");
        assert_eq!(hint.media_type, "application/vnd.apache.arrow.stream");
        assert!(hint.lease_expires_at.is_none());
    }

    /// `is_expired` honors the lease.
    #[test]
    fn is_expired_respects_lease() {
        let now = Utc::now();
        let mut hint = IpcHint::new("noetl_x", "deadbeef", 1024);

        // No lease → never expired.
        assert!(!hint.is_expired(now));

        // Lease in the future → not expired.
        hint.lease_expires_at = Some(now + chrono::Duration::seconds(60));
        assert!(!hint.is_expired(now));

        // Lease in the past → expired.
        hint.lease_expires_at = Some(now - chrono::Duration::seconds(1));
        assert!(hint.is_expired(now));
    }

    /// JSON shape — kind defaults, optional fields omitted when None.
    #[test]
    fn serialises_to_python_compatible_shape() {
        let hint = IpcHint::new("noetl_x", "deadbeef", 1024);
        let json = serde_json::to_value(&hint).unwrap();
        // Required top-level fields present.
        assert_eq!(json["kind"], "arrow_ipc");
        assert_eq!(json["shm_name"], "noetl_x");
        assert_eq!(json["schema_digest"], "deadbeef");
        assert_eq!(json["byte_length"], 1024);
        assert_eq!(json["media_type"], "application/vnd.apache.arrow.stream");
        // None fields omitted from JSON.
        assert!(json.get("row_count").is_none());
        assert!(json.get("producer").is_none());
        assert!(json.get("node_id").is_none());
        assert!(json.get("lease_expires_at").is_none());
    }
}

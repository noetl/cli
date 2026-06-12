//! Local event sinks for `noetl subscribe` (RFC #90 Phase 6, §5.3).
//!
//! The in-cluster subscription runtime emits its event-sourced trail through
//! the worker's `ControlPlaneClient` (`POST /api/events` → `noetl.event`).
//! Local mode has no server, so it emits the **same** [`ExecutorEvent`]
//! envelope through a [`FileEventSink`] — one JSON object per line (JSONL) on
//! local disk — producing a replayable event-sourced log identical in shape to
//! the in-cluster / Cloud Run trail.  Only the sink differs.
//!
//! Two sinks ship here:
//!
//! - [`FileEventSink`] — appends one event per line to a JSONL file, flushing
//!   on every emit so the trail survives a crash mid-run (replayability is the
//!   whole point).
//! - [`StdoutEventSink`] — pretty single-line summary to stderr for live
//!   visibility; used alongside the file sink, never instead of it.
//!
//! Both implement the shared [`noetl_events::EventSink`] trait, so the local
//! runtime threads events through `EventSink` exactly as the worker does.

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use anyhow::{Context, Result};
use async_trait::async_trait;
use noetl_events::{EventSink, ExecutorEvent};

/// NoETL snowflake epoch (`2024-01-01T00:00:00Z` in ms) — the fixed offset the
/// application-side id generator counts from (`observability.md` Principle 3).
const NOETL_EPOCH_MS: u64 = 1_704_067_200_000;

/// Application-side id generator for CLI local mode.
///
/// Per `observability.md` Principle 3, ids that need to be available *before*
/// a row would hit a database (here: the subscription's own id + every
/// per-message child execution id) are generated in the process, not by the
/// DB.  Local mode has no DB at all, so this is the only generator — a
/// snowflake-shaped `(timestamp, machine_id, sequence)` layout that stays
/// unique + sortable within a host.  `machine_id` is a stable hash of
/// hostname + pid (the value Principle 3 prescribes for CLI local mode).
#[derive(Debug)]
pub struct LocalIdGen {
    machine_id: u64,
    /// The last id handed out — every `next()` returns strictly more than this
    /// even if the wall clock stalls or the per-ms sequence (12 bits) would
    /// wrap, so ids stay unique + sortable regardless of emit rate.
    last: AtomicU64,
}

impl LocalIdGen {
    /// Build a generator seeded from hostname + pid.
    pub fn new() -> Self {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        hostname().hash(&mut h);
        std::process::id().hash(&mut h);
        let machine_id = h.finish() & 0x3FF; // 10 bits
        Self {
            machine_id,
            last: AtomicU64::new(0),
        }
    }

    /// Next monotonic snowflake id: `(ms << 22) | (machine_id << 12) | seq`,
    /// forced strictly above the previous id via an atomic CAS so bursts >4096
    /// ids/ms (which would otherwise wrap the sequence) still increase.
    pub fn next(&self) -> i64 {
        let now = now_ms().saturating_sub(NOETL_EPOCH_MS);
        loop {
            let prev = self.last.load(Ordering::SeqCst);
            // The natural snowflake value for "now" with a zero sequence.
            let base = (now << 22) | (self.machine_id << 12);
            // Strictly greater than the previous id: advance the time/seq
            // component when the clock hasn't moved past the last emit.
            let candidate = base.max(prev + 1);
            if self
                .last
                .compare_exchange(prev, candidate, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return (candidate & 0x7FFF_FFFF_FFFF_FFFF) as i64;
            }
        }
    }
}

impl Default for LocalIdGen {
    fn default() -> Self {
        Self::new()
    }
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// Wall-clock epoch millis.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Appends each [`ExecutorEvent`] as one JSON line to a file (JSONL).
///
/// The file is the local, replayable event-sourced trail — the same envelope
/// the in-cluster runtime writes to `noetl.event`, just on disk.  Flushed on
/// every emit so a crash mid-outage still leaves a complete trail up to the
/// last event.
pub struct FileEventSink {
    file: Mutex<std::fs::File>,
    /// Mirror a one-line summary to stderr so a live run is visible without
    /// tailing the file.
    echo: bool,
}

impl FileEventSink {
    /// Open (creating + truncating) the JSONL trail at `path`.
    pub fn create(path: impl AsRef<Path>, echo: bool) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("create event-sink dir {}", parent.display()))?;
            }
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .with_context(|| format!("open event sink {}", path.display()))?;
        Ok(Self {
            file: Mutex::new(file),
            echo,
        })
    }
}

#[async_trait]
impl EventSink for FileEventSink {
    async fn emit(&self, event: ExecutorEvent) -> Result<()> {
        let line = serde_json::to_string(&event).context("serialize event")?;
        {
            let mut f = self
                .file
                .lock()
                .map_err(|_| anyhow::anyhow!("event sink mutex poisoned"))?;
            f.write_all(line.as_bytes())
                .and_then(|_| f.write_all(b"\n"))
                .and_then(|_| f.flush())
                .context("write event line")?;
        }
        if self.echo {
            eprintln!(
                "  event {:<38} exec={} status={}",
                event.event_type, event.execution_id, event.status
            );
        }
        Ok(())
    }
}

/// Pretty single-line event sink to stderr — visibility without a file.
/// Available as an alternate sink (RFC §5.3 `stdout` vs `file`); the
/// `noetl subscribe` command defaults to the [`FileEventSink`] (the replayable
/// trail) with stderr echo, so this is wired for embedders / future flags.
#[allow(dead_code)]
pub struct StdoutEventSink;

#[async_trait]
impl EventSink for StdoutEventSink {
    async fn emit(&self, event: ExecutorEvent) -> Result<()> {
        eprintln!(
            "event {:<38} exec={} step={} status={} {}",
            event.event_type,
            event.execution_id,
            event.step,
            event.status,
            serde_json::to_string(&event.context).unwrap_or_default()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::sync::Arc;

    fn ev(t: &str, exec: i64) -> ExecutorEvent {
        ExecutorEvent {
            execution_id: exec,
            event_type: t.to_string(),
            step: "ingress".to_string(),
            status: "OK".to_string(),
            created_at: Utc::now(),
            context: serde_json::json!({ "k": "v" }),
            event_id: None,
            worker_id: Some("cli-local".to_string()),
            meta: None,
        }
    }

    #[tokio::test]
    async fn file_sink_writes_one_json_line_per_event() {
        let dir = std::env::temp_dir().join(format!("noetl-sink-{}", std::process::id()));
        let path = dir.join("trail.jsonl");
        let sink: Arc<dyn EventSink> = Arc::new(FileEventSink::create(&path, false).unwrap());
        sink.emit(ev("subscription.lifecycle", 1)).await.unwrap();
        sink.emit(ev("subscription.message.received", 2)).await.unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2, "one JSON object per line");
        // Each line round-trips back into the same envelope (replayable).
        let first: ExecutorEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first.event_type, "subscription.lifecycle");
        assert_eq!(first.execution_id, 1);
        let second: ExecutorEvent = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second.event_type, "subscription.message.received");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn idgen_is_monotonic_and_unique() {
        let gen = LocalIdGen::new();
        let mut prev = gen.next();
        for _ in 0..5_000 {
            let id = gen.next();
            assert!(id > prev, "ids strictly increase: {id} <= {prev}");
            prev = id;
        }
    }
}

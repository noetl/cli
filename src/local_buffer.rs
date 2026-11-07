//! R-1.4 — the local-mode Arrow Feather event buffer (blueprint § H.11).
//!
//! One directory per local execution:
//!
//! ```text
//! <root>/<execution_id>/
//!   events.feather   Feather V2 (Arrow IPC *file* format), § H.11.3 schema
//!   metadata.json    execution_id, started_at, playbook_path, status, stack, …
//! ```
//!
//! `<root>` is `$NOETL_LOCAL_DIR`, else `~/.noetl/local` (§ H.11.3).
//!
//! # Crash safety
//!
//! § H.11.2 calls the file "append-only with chunk-boundary footers". An Arrow
//! IPC *file* has exactly one footer, at the end, so a literal in-place append
//! would leave the file unreadable until the footer is rewritten. This writer
//! keeps the stronger property instead: every flush writes the complete file
//! (existing batches + the new one) to `events.feather.tmp`, fsyncs it, and
//! renames it over `events.feather`. A crash at any point leaves the previous
//! complete file, never a torn one. Each flush is one more `RecordBatch`
//! (chunk) in the file, so the chunk boundaries § H.11 describes are preserved.
//! Cost is O(file) per flush, which is fine at the sizes the flush policy
//! (§ H.11.4: 4 MiB threshold) produces; a multi-file layout is the upgrade
//! path if that ever stops being true.
//!
//! Concurrent writers to one execution (two shell steps calling
//! `noetl buffer append` at once) are serialised by an exclusive lock on
//! `<dir>/.lock`.
//!
//! # Not a new state model
//!
//! The buffer stores events. IaC state is the `provider_fact` events among them
//! (event_type `provider.fact`, the fact under `result.data.provider_fact`, the
//! path noetl-tools' `fact_in_record` probes first), folded by the unchanged
//! `fold_facts` semantics. Feather changes the encoding, not the facts.

use std::collections::BTreeMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

/// The media type § H.11.5 sends the file under.
pub const MEDIA_TYPE: &str = "application/vnd.apache.arrow.file";
/// Arrow IPC file magic (`ARROW1`), first six bytes of every Feather V2 file.
pub const ARROW_MAGIC: &[u8; 6] = b"ARROW1";
/// `metadata.json` `format` value this build writes.
pub const FORMAT: &str = "noetl.local_buffer.v1";
/// § H.11.4 trigger 1: flush when the in-memory batch reaches this size.
pub const DEFAULT_FLUSH_BYTES: usize = 4 * 1024 * 1024;
/// The event type an IaC `provider_fact` is recorded under.
pub const PROVIDER_FACT_EVENT: &str = "provider.fact";

pub const EVENTS_FILE: &str = "events.feather";
pub const METADATA_FILE: &str = "metadata.json";

/// `$NOETL_LOCAL_DIR`, else `~/.noetl/local`.
pub fn default_root() -> PathBuf {
    if let Ok(dir) = std::env::var("NOETL_LOCAL_DIR") {
        let dir = dir.trim();
        if !dir.is_empty() {
            return PathBuf::from(shellexpand::tilde(dir).to_string());
        }
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".noetl")
        .join("local")
}

/// The § H.11.3 `events.feather` schema, column for column.
pub fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("execution_id", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("step", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("context", DataType::Binary, false),
        Field::new("result", DataType::Binary, false),
        Field::new("meta", DataType::Binary, false),
        Field::new("parent_event_id", DataType::Int64, true),
        Field::new("payload_ref", DataType::Utf8, true),
    ]))
}

/// One buffered event. JSON columns are carried parsed; they are serialised to
/// JSON bytes only at the Arrow boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BufferedEvent {
    pub event_id: i64,
    pub execution_id: i64,
    pub event_type: String,
    pub step: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub context: serde_json::Value,
    #[serde(default)]
    pub result: serde_json::Value,
    #[serde(default)]
    pub meta: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_event_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_ref: Option<String>,
}

impl BufferedEvent {
    fn approx_bytes(&self) -> usize {
        64 + self.event_type.len()
            + self.step.len()
            + self.status.len()
            + json_len(&self.context)
            + json_len(&self.result)
            + json_len(&self.meta)
    }
}

fn json_len(v: &serde_json::Value) -> usize {
    serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0)
}

/// `metadata.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub format: String,
    /// String on the wire: an i64 snowflake does not survive a JSON number in
    /// every consumer (jq, JavaScript).
    #[serde(with = "i64_string")]
    pub execution_id: i64,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub playbook_path: String,
    /// `RUNNING` until closed, then `COMPLETED` / `FAILED`.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub event_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed_to: Option<String>,
}

mod i64_string {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum N {
            S(String),
            I(i64),
        }
        match N::deserialize(d)? {
            N::S(s) => s.parse().map_err(serde::de::Error::custom),
            N::I(i) => Ok(i),
        }
    }
}

// ---------------------------------------------------------------------------
// ids
// ---------------------------------------------------------------------------

/// 2024-01-01T00:00:00Z in unix ms — the local snowflake epoch.
const EPOCH_MS: i64 = 1_704_067_200_000;
static SEQ: AtomicU64 = AtomicU64::new(0);

/// A locally allocated snowflake: 41 bits of ms since 2024-01-01, 10 random
/// bits (two processes in the same ms), 12 bits of per-process sequence.
/// Monotonic per process; time-ordered across processes, which is what makes
/// "sort by execution_id" mean "sort by start time" for `list` and `push --all`.
pub fn new_id() -> i64 {
    use rand::Rng;
    let ms = (Utc::now().timestamp_millis() - EPOCH_MS).max(0) & ((1 << 41) - 1);
    // Fixed per process, so ids from one process are strictly increasing.
    static NODE: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    let node = *NODE.get_or_init(|| rand::thread_rng().gen_range(0..1024));
    let seq = (SEQ.fetch_add(1, Ordering::Relaxed) & 0xFFF) as i64;
    (ms << 22) | (node << 12) | seq
}

// ---------------------------------------------------------------------------
// codec
// ---------------------------------------------------------------------------

fn to_record_batch(events: &[BufferedEvent]) -> Result<RecordBatch> {
    let json_bytes = |f: fn(&BufferedEvent) -> &serde_json::Value| -> Result<ArrayRef> {
        let mut v: Vec<Vec<u8>> = Vec::with_capacity(events.len());
        for e in events {
            v.push(serde_json::to_vec(f(e))?);
        }
        Ok(Arc::new(BinaryArray::from_iter_values(v.iter())) as ArrayRef)
    };
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(events.iter().map(|e| e.event_id))),
        Arc::new(Int64Array::from_iter_values(events.iter().map(|e| e.execution_id))),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|e| e.event_type.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(events.iter().map(|e| e.step.as_str()))),
        Arc::new(StringArray::from_iter_values(events.iter().map(|e| e.status.as_str()))),
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(events.iter().map(|e| e.created_at.timestamp_micros()))
                .with_timezone("UTC"),
        ),
        json_bytes(|e| &e.context)?,
        json_bytes(|e| &e.result)?,
        json_bytes(|e| &e.meta)?,
        Arc::new(Int64Array::from(
            events.iter().map(|e| e.parent_event_id).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            events.iter().map(|e| e.payload_ref.as_deref()).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(events_schema(), cols).context("build events RecordBatch")
}

/// Encode batches (one per flush) as one Feather V2 file.
pub fn encode_feather(batches: &[Vec<BufferedEvent>]) -> Result<Vec<u8>> {
    let schema = events_schema();
    let mut buf = Vec::with_capacity(8 * 1024);
    {
        let mut w = FileWriter::try_new(&mut buf, &schema).context("create Feather writer")?;
        for b in batches.iter().filter(|b| !b.is_empty()) {
            w.write(&to_record_batch(b)?).context("write Feather chunk")?;
        }
        w.finish().context("finish Feather file")?;
    }
    Ok(buf)
}

fn col<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .with_context(|| format!("events.feather is missing column `{name}`"))?
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("events.feather column `{name}` has the wrong type"))
}

fn json_at(a: &BinaryArray, i: usize) -> Result<serde_json::Value> {
    if a.is_null(i) || a.value(i).is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(a.value(i)).context("a JSON column holds invalid JSON")
}

/// Decode a Feather V2 file into its chunks, in file order. Columns are read
/// by name, so a writer that adds a column stays readable.
pub fn decode_feather_chunks(bytes: &[u8]) -> Result<Vec<Vec<BufferedEvent>>> {
    if bytes.len() < 6 || &bytes[..6] != ARROW_MAGIC {
        bail!("not a Feather V2 / Arrow IPC file (missing ARROW1 magic)");
    }
    let reader = FileReader::try_new(Cursor::new(bytes), None).context("open Feather file")?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.context("read Feather chunk")?;
        let event_id = col::<Int64Array>(&batch, "event_id")?;
        let execution_id = col::<Int64Array>(&batch, "execution_id")?;
        let event_type = col::<StringArray>(&batch, "event_type")?;
        let step = col::<StringArray>(&batch, "step")?;
        let status = col::<StringArray>(&batch, "status")?;
        let created_at = col::<TimestampMicrosecondArray>(&batch, "created_at")?;
        let context = col::<BinaryArray>(&batch, "context")?;
        let result = col::<BinaryArray>(&batch, "result")?;
        let meta = col::<BinaryArray>(&batch, "meta")?;
        let parent = col::<Int64Array>(&batch, "parent_event_id")?;
        let payload_ref = col::<StringArray>(&batch, "payload_ref")?;
        let mut chunk = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            chunk.push(BufferedEvent {
                event_id: event_id.value(i),
                execution_id: execution_id.value(i),
                event_type: event_type.value(i).to_string(),
                step: step.value(i).to_string(),
                status: status.value(i).to_string(),
                created_at: Utc
                    .timestamp_micros(created_at.value(i))
                    .single()
                    .context("created_at out of range")?,
                context: json_at(context, i)?,
                result: json_at(result, i)?,
                meta: json_at(meta, i)?,
                parent_event_id: (!parent.is_null(i)).then(|| parent.value(i)),
                payload_ref: (!payload_ref.is_null(i)).then(|| payload_ref.value(i).to_string()),
            });
        }
        out.push(chunk);
    }
    Ok(out)
}

/// Decode a Feather V2 file into one flat event list.
pub fn decode_feather(bytes: &[u8]) -> Result<Vec<BufferedEvent>> {
    Ok(decode_feather_chunks(bytes)?.into_iter().flatten().collect())
}

// ---------------------------------------------------------------------------
// the buffer
// ---------------------------------------------------------------------------

/// The buffer for one local execution.
pub struct LocalEventBuffer {
    dir: PathBuf,
    execution_id: i64,
    pending: Vec<BufferedEvent>,
    pending_bytes: usize,
    flush_bytes: usize,
}

/// Options for [`LocalEventBuffer::create`].
#[derive(Debug, Default, Clone)]
pub struct CreateOptions {
    pub execution_id: Option<i64>,
    pub playbook_path: String,
    pub stack: Option<String>,
    pub labels: BTreeMap<String, String>,
}

/// A partial event as callers supply it; ids and timestamps are filled in.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct NewEvent {
    pub event_type: String,
    #[serde(default)]
    pub step: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub context: serde_json::Value,
    #[serde(default)]
    pub result: serde_json::Value,
    #[serde(default)]
    pub meta: serde_json::Value,
    #[serde(default)]
    pub parent_event_id: Option<i64>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
}

pub fn execution_dir(root: &Path, execution_id: i64) -> PathBuf {
    root.join(execution_id.to_string())
}

struct DirLock(#[allow(dead_code)] fs::File);

fn lock_dir(dir: &Path) -> Result<DirLock> {
    let f = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(".lock"))
        .with_context(|| format!("open lock in {}", dir.display()))?;
    f.lock().with_context(|| format!("lock {}", dir.display()))?;
    Ok(DirLock(f))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("")
    ));
    {
        let mut f = fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path).with_context(|| format!("rename into {}", path.display()))?;
    Ok(())
}

pub fn read_metadata(dir: &Path) -> Result<Metadata> {
    let p = dir.join(METADATA_FILE);
    let s = fs::read_to_string(&p).with_context(|| format!("read {}", p.display()))?;
    serde_json::from_str(&s).with_context(|| format!("parse {}", p.display()))
}

pub fn write_metadata(dir: &Path, m: &Metadata) -> Result<()> {
    let mut s = serde_json::to_string_pretty(m)?;
    s.push('\n');
    write_atomic(&dir.join(METADATA_FILE), s.as_bytes())
}

/// Read every chunk of one execution's `events.feather` (empty if none yet).
pub fn read_chunks(dir: &Path) -> Result<Vec<Vec<BufferedEvent>>> {
    let p = dir.join(EVENTS_FILE);
    match fs::read(&p) {
        Ok(bytes) => decode_feather_chunks(&bytes).with_context(|| format!("decode {}", p.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).with_context(|| format!("read {}", p.display())),
    }
}

pub fn read_events(dir: &Path) -> Result<Vec<BufferedEvent>> {
    Ok(read_chunks(dir)?.into_iter().flatten().collect())
}

/// Every execution under `root`, oldest first (snowflake order = start order).
/// A directory without a readable `metadata.json` is skipped, not fatal.
pub fn list(root: &Path) -> Result<Vec<(PathBuf, Metadata)>> {
    let mut out = Vec::new();
    let rd = match fs::read_dir(root) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e).with_context(|| format!("list {}", root.display())),
    };
    for entry in rd {
        let entry = entry?;
        let path = entry.path();
        if !path.is_dir()
            || path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.parse::<i64>().is_err())
                .unwrap_or(true)
        {
            continue;
        }
        if let Ok(m) = read_metadata(&path) {
            out.push((path, m));
        }
    }
    out.sort_by_key(|(_, m)| m.execution_id);
    Ok(out)
}

impl LocalEventBuffer {
    /// Start a new execution buffer (`metadata.json` status `RUNNING`).
    /// Refuses to reuse an existing execution id.
    pub fn create(root: &Path, opts: CreateOptions) -> Result<Self> {
        let execution_id = opts.execution_id.unwrap_or_else(new_id);
        let dir = execution_dir(root, execution_id);
        if dir.join(METADATA_FILE).exists() {
            bail!("execution {execution_id} already has a buffer at {}", dir.display());
        }
        fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let m = Metadata {
            format: FORMAT.to_string(),
            execution_id,
            started_at: Utc::now(),
            playbook_path: opts.playbook_path,
            status: "RUNNING".to_string(),
            stack: opts.stack,
            labels: opts.labels,
            event_count: 0,
            completed_at: None,
            pushed_at: None,
            pushed_to: None,
        };
        write_metadata(&dir, &m)?;
        Ok(Self::at(dir, execution_id))
    }

    /// Open an existing execution buffer for more appends.
    pub fn open(root: &Path, execution_id: i64) -> Result<Self> {
        let dir = execution_dir(root, execution_id);
        let m = read_metadata(&dir)
            .with_context(|| format!("no buffer for execution {execution_id} under {}", root.display()))?;
        if m.status != "RUNNING" {
            bail!(
                "execution {execution_id} is closed ({}); a closed buffer is immutable",
                m.status
            );
        }
        Ok(Self::at(dir, execution_id))
    }

    fn at(dir: PathBuf, execution_id: i64) -> Self {
        Self {
            dir,
            execution_id,
            pending: Vec::new(),
            pending_bytes: 0,
            flush_bytes: DEFAULT_FLUSH_BYTES,
        }
    }

    pub fn with_flush_bytes(mut self, n: usize) -> Self {
        self.flush_bytes = n.max(1);
        self
    }

    pub fn execution_id(&self) -> i64 {
        self.execution_id
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Buffer one event; flushes when the § H.11.4 size threshold is reached.
    /// Returns the allocated event id.
    pub fn append(&mut self, ev: NewEvent) -> Result<i64> {
        if ev.event_type.trim().is_empty() {
            bail!("event_type is required");
        }
        let event_id = new_id();
        let e = BufferedEvent {
            event_id,
            execution_id: self.execution_id,
            event_type: ev.event_type,
            step: ev.step,
            status: if ev.status.is_empty() {
                "COMPLETED".into()
            } else {
                ev.status
            },
            created_at: ev.created_at.unwrap_or_else(Utc::now),
            context: null_to_object(ev.context),
            result: null_to_object(ev.result),
            meta: null_to_object(ev.meta),
            parent_event_id: ev.parent_event_id,
            payload_ref: None,
        };
        self.pending_bytes += e.approx_bytes();
        self.pending.push(e);
        if self.pending_bytes >= self.flush_bytes {
            self.flush()?;
        }
        Ok(event_id)
    }

    /// Write pending events as one new chunk (atomic rewrite + rename).
    pub fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let _lock = lock_dir(&self.dir)?;
        let mut m = read_metadata(&self.dir)?;
        if m.status != "RUNNING" {
            bail!(
                "execution {} was closed ({}) while buffering",
                self.execution_id,
                m.status
            );
        }
        let mut chunks = read_chunks(&self.dir)?;
        let n = self.pending.len() as u64;
        chunks.push(std::mem::take(&mut self.pending));
        self.pending_bytes = 0;
        write_atomic(&self.dir.join(EVENTS_FILE), &encode_feather(&chunks)?)?;
        m.event_count += n;
        write_metadata(&self.dir, &m)
    }

    /// § H.11.4 trigger 2: flush, then mark the execution `COMPLETED`/`FAILED`.
    pub fn close(mut self, status: &str) -> Result<Metadata> {
        let status = status.to_ascii_uppercase();
        if !matches!(status.as_str(), "COMPLETED" | "FAILED") {
            bail!("close status must be COMPLETED or FAILED, not {status}");
        }
        self.flush()?;
        let _lock = lock_dir(&self.dir)?;
        let mut m = read_metadata(&self.dir)?;
        if !self.dir.join(EVENTS_FILE).exists() {
            // A closed execution always has a (possibly empty) file, so a
            // reader never has to tell "no events" from "file missing".
            write_atomic(&self.dir.join(EVENTS_FILE), &encode_feather(&[])?)?;
        }
        m.status = status;
        m.completed_at = Some(Utc::now());
        write_metadata(&self.dir, &m)?;
        Ok(m)
    }
}

impl Drop for LocalEventBuffer {
    /// Best effort: never lose buffered events on an early return.
    fn drop(&mut self) {
        if !self.pending.is_empty() {
            let _ = self.flush();
        }
    }
}

fn null_to_object(v: serde_json::Value) -> serde_json::Value {
    if v.is_null() {
        serde_json::json!({})
    } else {
        v
    }
}

/// One event as the JSON line `noetl buffer cat --jsonl` prints, with the
/// execution's stack attached. Ids are strings for the same reason as in
/// `metadata.json`.
pub fn event_json(e: &BufferedEvent, stack: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "event_id": e.event_id.to_string(),
        "execution_id": e.execution_id.to_string(),
        "event_type": e.event_type,
        "step": e.step,
        "status": e.status,
        "created_at": e.created_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        "context": e.context,
        "result": e.result,
        "meta": e.meta,
        "parent_event_id": e.parent_event_id.map(|p| p.to_string()),
        "payload_ref": e.payload_ref,
        "stack": stack,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("noetl-lb-{tag}-{}", new_id()));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn fact_event(urn: &str, verb: &str, outcome: &str) -> NewEvent {
        NewEvent {
            event_type: PROVIDER_FACT_EVENT.into(),
            step: "record_state".into(),
            result: serde_json::json!({"data": {"provider_fact": {
                "urn": urn, "stack": "s", "provider": "kubernetes", "service": "core",
                "resource_type": "ConfigMap", "verb": verb, "outcome": outcome,
                "desired": {"digest": "abc"}, "execution_id": 1
            }}}),
            ..Default::default()
        }
    }

    #[test]
    fn ids_are_time_ordered_and_unique() {
        let a: Vec<i64> = (0..2000).map(|_| new_id()).collect();
        assert!(
            a.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing within a process"
        );
        assert!(a.iter().all(|&i| i > 0));
    }

    #[test]
    fn schema_matches_h11_3() {
        let names: Vec<String> = events_schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(
            names,
            [
                "event_id",
                "execution_id",
                "event_type",
                "step",
                "status",
                "created_at",
                "context",
                "result",
                "meta",
                "parent_event_id",
                "payload_ref"
            ]
        );
    }

    #[test]
    fn file_is_feather_v2_and_round_trips_chunks() {
        let root = tmp("rt");
        let mut b = LocalEventBuffer::create(
            &root,
            CreateOptions {
                stack: Some("s".into()),
                playbook_path: "p.yaml".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let eid = b.execution_id();
        b.append(fact_event("k8s://a", "ensure", "changed")).unwrap();
        b.flush().unwrap();
        b.append(fact_event("k8s://b", "ensure", "noop")).unwrap();
        b.append(fact_event("k8s://a", "delete", "deleted")).unwrap();
        let m = b.close("completed").unwrap();
        assert_eq!(m.status, "COMPLETED");
        assert_eq!(m.event_count, 3);

        let bytes = fs::read(execution_dir(&root, eid).join(EVENTS_FILE)).unwrap();
        assert_eq!(&bytes[..6], ARROW_MAGIC, "Feather V2 header magic");
        assert_eq!(&bytes[bytes.len() - 6..], ARROW_MAGIC, "Feather V2 footer magic");
        let chunks = decode_feather_chunks(&bytes).unwrap();
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, 2],
            "one chunk per flush"
        );
        let evs: Vec<_> = chunks.into_iter().flatten().collect();
        assert!(evs.iter().all(|e| e.execution_id == eid));
        assert_eq!(evs[2].result["data"]["provider_fact"]["verb"], "delete");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn closed_buffer_is_immutable_and_empty_close_writes_a_file() {
        let root = tmp("closed");
        let b = LocalEventBuffer::create(&root, CreateOptions::default()).unwrap();
        let eid = b.execution_id();
        b.close("FAILED").unwrap();
        assert!(read_events(&execution_dir(&root, eid)).unwrap().is_empty());
        assert!(execution_dir(&root, eid).join(EVENTS_FILE).exists());
        assert!(LocalEventBuffer::open(&root, eid).is_err());
        assert!(LocalEventBuffer::create(
            &root,
            CreateOptions {
                execution_id: Some(eid),
                ..Default::default()
            }
        )
        .is_err());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn reopen_appends_a_new_chunk_and_list_orders_by_start() {
        let root = tmp("reopen");
        let mut first = LocalEventBuffer::create(
            &root,
            CreateOptions {
                stack: Some("s".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let e1 = first.execution_id();
        first.append(fact_event("k8s://a", "ensure", "changed")).unwrap();
        drop(first); // Drop flushes
        let mut again = LocalEventBuffer::open(&root, e1).unwrap();
        again.append(fact_event("k8s://b", "ensure", "changed")).unwrap();
        again.close("COMPLETED").unwrap();
        let second = LocalEventBuffer::create(&root, CreateOptions::default()).unwrap();
        let e2 = second.execution_id();
        second.close("COMPLETED").unwrap();
        assert_eq!(read_chunks(&execution_dir(&root, e1)).unwrap().len(), 2);
        let ids: Vec<i64> = list(&root).unwrap().into_iter().map(|(_, m)| m.execution_id).collect();
        assert_eq!(ids, vec![e1, e2]);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn threshold_flush_and_torn_tmp_does_not_corrupt() {
        let root = tmp("thresh");
        let mut b = LocalEventBuffer::create(&root, CreateOptions::default())
            .unwrap()
            .with_flush_bytes(1);
        let eid = b.execution_id();
        b.append(fact_event("k8s://a", "ensure", "changed")).unwrap();
        // A crash mid-write leaves only a stray tmp file; the real file is intact.
        fs::write(execution_dir(&root, eid).join("events.feather.tmp"), b"ARROW1garbage").unwrap();
        assert_eq!(read_events(&execution_dir(&root, eid)).unwrap().len(), 1);
        b.append(fact_event("k8s://b", "ensure", "changed")).unwrap();
        b.close("COMPLETED").unwrap();
        assert_eq!(read_events(&execution_dir(&root, eid)).unwrap().len(), 2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn rejects_non_feather_and_missing_columns() {
        assert!(decode_feather(b"{\"provider_fact\":{}}").is_err());
        // A valid Arrow file with the wrong schema must fail loudly, not read as empty.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef]).unwrap();
        let mut buf = Vec::new();
        {
            let mut w = FileWriter::try_new(&mut buf, &schema).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let err = decode_feather(&buf).unwrap_err().to_string();
        assert!(err.contains("missing column"), "{err}");
    }

    #[test]
    fn provider_facts_fold_through_noetl_tools_unchanged() {
        // The point of the whole design: Feather is an encoding. The facts read
        // back from the file fold with the shipped fold_facts exactly as the
        // JSONL `--facts-out` records do.
        use noetl_tools::tools::provider_state as ps;
        let root = tmp("fold");
        let mut b = LocalEventBuffer::create(&root, CreateOptions::default()).unwrap();
        let eid = b.execution_id();
        b.append(fact_event("k8s://a", "ensure", "changed")).unwrap();
        b.append(fact_event("k8s://b", "ensure", "changed")).unwrap();
        b.append(fact_event("k8s://a", "delete", "deleted")).unwrap();
        b.close("COMPLETED").unwrap();
        let records: Vec<serde_json::Value> = read_events(&execution_dir(&root, eid))
            .unwrap()
            .iter()
            .map(|e| serde_json::json!({ "result": e.result }))
            .collect();
        let model = ps::fold_tier_records(&records);
        let owned: Vec<&str> = model.owned.keys().map(String::as_str).collect();
        assert_eq!(owned, vec!["k8s://b"]);
        let _ = fs::remove_dir_all(&root);
    }
}

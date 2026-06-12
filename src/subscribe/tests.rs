//! Integration tests for `noetl subscribe` local mode (RFC #90 Phase 6).
//!
//! These drive the **real** engine surface — the `noetl_tools` spool engine,
//! circuit breaker, local_disk backend, ordering, idempotency, dead-letter —
//! through the local runtime, with a fake in-memory source + a recording
//! dispatcher so the proofs are deterministic and run in CI (no broker / no
//! cluster).  The live NATS proof is documented separately in the PR.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use noetl_events::{EventSink, ExecutorEvent};
use noetl_tools::spool::SpoolSpec;
use noetl_tools::tools::source::{
    DispatchPlan, PollOptions, PollOutcome, PolledMessage, SourceClient,
};
use noetl_tools::ToolError;

use super::dispatch::{DispatchResult, Dispatcher};
use super::sink::{FileEventSink, LocalIdGen};
use super::spec::parse_spec;
use super::spool::{LocalSpoolRuntime, Routing};

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

/// A source that yields one fixed batch, then is empty forever.
struct VecSource {
    batches: Mutex<VecDeque<Vec<PolledMessage>>>,
}

impl VecSource {
    fn new(batch: Vec<PolledMessage>) -> Self {
        let mut q = VecDeque::new();
        q.push_back(batch);
        Self {
            batches: Mutex::new(q),
        }
    }
}

#[async_trait]
impl SourceClient for VecSource {
    fn source_name(&self) -> &'static str {
        "nats"
    }
    async fn poll(&self, _opts: &PollOptions) -> Result<PollOutcome, ToolError> {
        let batch = self.batches.lock().unwrap().pop_front().unwrap_or_default();
        Ok(PollOutcome {
            messages: batch,
            acked: true,
            ack_ids: Vec::new(),
        })
    }
}

/// A dispatcher that records every dispatch and can be toggled to fail
/// (simulating a downstream outage on the in-process run).
#[derive(Clone, Default)]
struct RecordingDispatcher {
    dispatched: Arc<Mutex<Vec<String>>>,
    fail: Arc<Mutex<bool>>,
    ids: Arc<Mutex<i64>>,
}

impl RecordingDispatcher {
    fn set_fail(&self, fail: bool) {
        *self.fail.lock().unwrap() = fail;
    }
    fn dispatched(&self) -> Vec<String> {
        self.dispatched.lock().unwrap().clone()
    }
}

#[async_trait]
impl Dispatcher for RecordingDispatcher {
    async fn dispatch(
        &self,
        _playbook: &str,
        _pool: Option<&str>,
        msg: &PolledMessage,
        _plan: &DispatchPlan,
        _payload_from: &str,
        _subscription: &str,
        _source: &str,
    ) -> anyhow::Result<DispatchResult> {
        let id = {
            let mut g = self.ids.lock().unwrap();
            *g += 1;
            *g
        };
        if *self.fail.lock().unwrap() {
            return Ok(DispatchResult {
                execution_id: id,
                status: "FAILED".into(),
                error: Some("downstream outage".into()),
            });
        }
        self.dispatched.lock().unwrap().push(msg.id.clone());
        Ok(DispatchResult {
            execution_id: id,
            status: "COMPLETED".into(),
            error: None,
        })
    }
    fn label(&self) -> String {
        "recording".into()
    }
}

fn msg(id: &str, data: serde_json::Value) -> PolledMessage {
    PolledMessage {
        id: id.to_string(),
        data,
        headers: serde_json::Map::new(),
        attributes: serde_json::json!({}),
        metadata: serde_json::json!({}),
        ack_id: None,
    }
}

fn read_events(path: &std::path::Path) -> Vec<ExecutorEvent> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).expect("event line round-trips"))
        .collect()
}

fn tmp(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("noetl-sub-{}-{}", name, std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

// ---------------------------------------------------------------------------
// Full-loop event-sourcing proof (no broker)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_loop_emits_event_sourced_trail_to_filesink() {
    let dir = tmp("loop");
    let events = dir.join("trail.jsonl");
    let sink: Arc<dyn EventSink> = Arc::new(FileEventSink::create(&events, false).unwrap());
    let dispatcher = Arc::new(RecordingDispatcher::default());
    let ids = Arc::new(LocalIdGen::new());

    let yaml: serde_yaml::Value = serde_yaml::from_str(
        "kind: Subscription\nspec:\n  source: nats\n  url: nats://x\n  stream: S\n  consumer: C\n  dispatch: { playbook: ./p.yaml }\n",
    )
    .unwrap();
    let spec = parse_spec(&yaml, "subs/orders", None).unwrap();

    let rt = super::runtime::LocalRuntime::new(
        spec,
        sink,
        dispatcher.clone(),
        ids,
        None,
        super::runtime::StopWhen::OneDrain,
    );
    let source = Box::new(VecSource::new(vec![
        msg("m1", serde_json::json!({"order_id": 1})),
        msg("m2", serde_json::json!({"order_id": 2})),
    ]));

    let summary = rt
        .run_with_source(source, futures_pending())
        .await
        .unwrap();
    assert_eq!(summary.received, 2);
    assert_eq!(summary.dispatched, 2);
    assert_eq!(dispatcher.dispatched(), vec!["m1", "m2"]);

    let evs = read_events(&events);
    let types: Vec<&str> = evs.iter().map(|e| e.event_type.as_str()).collect();
    // Lifecycle brackets + per-message ingress + playbook.started/completed.
    assert!(types.contains(&"subscription.lifecycle"));
    assert!(types.contains(&"subscription.message.received"));
    assert!(types.contains(&"playbook.started"));
    assert!(types.contains(&"playbook.completed"));
    // Two messages → two playbook.completed.
    assert_eq!(types.iter().filter(|t| **t == "playbook.completed").count(), 2);
    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Local outage → local_disk spool → recovery → ordered replay → idempotency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn outage_buffers_to_local_disk_then_replays_in_order_on_recovery() {
    let dir = tmp("spool");
    let events = dir.join("trail.jsonl");
    let spool_dir = dir.join("spool");
    let sink: Arc<dyn EventSink> = Arc::new(FileEventSink::create(&events, false).unwrap());
    let dispatcher = Arc::new(RecordingDispatcher::default());

    // A TCP downstream the test controls: bind = up, drop = down. Discover a
    // free port, then keep it free (down) to start the outage.
    let probe_port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
        // listener dropped here → port closed (downstream "down")
    };
    let target = format!("127.0.0.1:{probe_port}");

    let spec_yaml = serde_json::json!({
        "mode": "buffer_and_ack",
        "backend": "local_disk",
        "path": spool_dir.to_str().unwrap(),
        "ordering": "global",
        "circuit": {
            "trip_after": 1,
            "probe_after_ms": 10,
            "probe_interval_ms": 0,
            "downstream": [ { "name": "warehouse", "type": "tcp", "target": target } ]
        },
        "drain": { "max_replay_attempts": 5, "on_recovery": "ordered_then_live" }
    });
    let spool_spec = SpoolSpec::parse(Some(&spec_yaml)).unwrap();
    assert!(spool_spec.buffers());

    let mut spool = LocalSpoolRuntime::build(
        &spool_spec,
        sink.clone(),
        dispatcher.clone(),
        "subs/iot".into(),
        9999,
        "nats".into(),
        "./ingest.yaml".into(),
        Some("warehouse".into()),
        "message.json".into(),
    )
    .await
    .unwrap()
    .expect("spool buffers");

    let plan = DispatchPlan::default();

    // --- Outage: the in-process dispatch fails → circuit opens. ---
    dispatcher.set_fail(true);
    let m0 = msg("m0", serde_json::json!({"v": 0}));
    // First message dispatches (circuit still closed), fails → breaker opens.
    let r0 = spool.route_message(&m0, &plan).await;
    assert_eq!(r0, Routing::Dispatch);
    spool.report_dispatch(&plan, &m0, false).await; // trip_after=1 → opens

    // Next 6 messages are buffered durably to local_disk (no dispatch).
    for i in 1..=6 {
        let m = msg(&format!("m{i}"), serde_json::json!({ "v": i }));
        let routing = spool.route_message(&m, &plan).await;
        assert_eq!(routing, Routing::Spooled, "message {i} spooled while circuit open");
    }
    assert_eq!(spool.pending().await, 6, "6 buffered durably");
    // The spool dir holds the durable items on disk.
    let on_disk = std::fs::read_dir(&spool_dir).unwrap().count();
    assert!(on_disk >= 1, "items written to local_disk");

    // --- Recovery: bring the downstream up, probe closes the circuit. ---
    let _listener = std::net::TcpListener::bind(format!("127.0.0.1:{probe_port}")).unwrap();
    dispatcher.set_fail(false);
    let recovered = spool.maybe_probe().await;
    assert!(recovered.contains(&"warehouse".to_string()), "circuit recovered: {recovered:?}");

    // --- Drain: ordered replay of the 6 buffered messages. ---
    spool.drain().await.unwrap();
    assert_eq!(spool.pending().await, 0, "spool fully drained");
    assert_eq!(
        dispatcher.dispatched(),
        vec!["m1", "m2", "m3", "m4", "m5", "m6"],
        "replayed in receive order (ordering: global)"
    );

    // --- Idempotency: a second drain replays nothing. ---
    let before = dispatcher.dispatched().len();
    spool.drain().await.unwrap();
    assert_eq!(dispatcher.dispatched().len(), before, "no duplicate replay");

    // The whole outage is reconstructable from the event trail.
    let evs = read_events(&events);
    let types: Vec<&str> = evs.iter().map(|e| e.event_type.as_str()).collect();
    assert!(types.contains(&"subscription.circuit.opened"));
    assert!(types.contains(&"subscription.message.spooled"));
    assert!(types.contains(&"subscription.circuit.closed"));
    assert!(types.contains(&"subscription.spool.draining"));
    assert!(types.contains(&"subscription.message.replayed"));
    assert_eq!(
        types.iter().filter(|t| **t == "subscription.message.spooled").count(),
        6
    );
    assert_eq!(
        types.iter().filter(|t| **t == "subscription.message.replayed").count(),
        6
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// A future that never resolves — the loop is bounded by the stop condition,
/// not the shutdown signal, in these tests.
async fn futures_pending() {
    std::future::pending::<()>().await
}

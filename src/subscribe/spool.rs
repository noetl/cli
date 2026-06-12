//! Store-and-forward spool for `noetl subscribe` local mode (RFC #90 Phase 6,
//! §8.6 — `local_disk` backend).
//!
//! This is the local-mode sibling of the worker runtime's `SpoolRuntime`
//! (`repos/worker/src/spool_runtime.rs`).  The spool *engine* + *circuit
//! breaker* + *ordering* + *idempotency* + *dead-letter* + *retention* logic
//! all live in [`noetl_tools::spool`] (pure, unit-tested upstream); this module
//! is the local glue:
//!
//! - durable backend = [`noetl_tools::spool::LocalDiskBackend`] (the only
//!   backend local mode supports, §8.6) under the subscription's spool dir;
//! - circuit state persists to a **JSON file** next to the spool (mirroring the
//!   in-cluster NATS-KV persistence) so a restart mid-outage rehydrates the
//!   breaker phase;
//! - events emit through the [`EventSink`] (the local `FileEventSink`), so the
//!   six spool/circuit event types land in the same replayable JSONL trail;
//! - replay dispatches **in-process** through the same [`Dispatcher`] the live
//!   path uses.
//!
//! The loss-safety contract is identical to the worker's: the bounded `poll`
//! already acked the batch on fetch, so a message in hand is no longer on the
//! source; `buffer_and_ack` durably writes it to the spool before doing
//! anything else, and the circuit only drains after the active probe confirms
//! the downstream is reachable.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use noetl_events::{EventSink, ExecutorEvent};
use noetl_tools::spool::{
    probe_downstream, Admission, CircuitDecision, CircuitRegistry, DeadLetter, LocalDiskBackend,
    SpoolEngine, SpoolItem, SpoolSpec,
};
use noetl_tools::tools::source::{DispatchPlan, PolledMessage};

use crate::subscribe::dispatch::Dispatcher;
use crate::subscribe::sink::now_ms;

/// What the runtime should do with a message after spool routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Routing {
    /// Circuit closed (or half-open probe) — dispatch normally.
    Dispatch,
    /// Circuit open — message was spooled (already durable); skip dispatch.
    Spooled,
    /// Retention ceiling hit with `on_full: stop_acking` — not buffered.
    Dropped,
}

/// Per-subscription local spool + circuit runtime.
pub struct LocalSpoolRuntime {
    engine: SpoolEngine,
    circuits: CircuitRegistry,
    circuit_file: PathBuf,
    sink: Arc<dyn EventSink>,
    dispatcher: Arc<dyn Dispatcher>,
    subscription_path: String,
    subscription_id: i64,
    source_name: String,
    default_playbook: String,
    default_pool: Option<String>,
    payload_from: String,
    probe_interval_ms: u64,
    last_probe_ms: u64,
    recv_seq: u64,
}

impl LocalSpoolRuntime {
    /// Build the local spool runtime, or `None` when the spec declares no
    /// buffering (`spool.mode: off` / absent).
    #[allow(clippy::too_many_arguments)]
    pub async fn build(
        spec: &SpoolSpec,
        sink: Arc<dyn EventSink>,
        dispatcher: Arc<dyn Dispatcher>,
        subscription_path: String,
        subscription_id: i64,
        source_name: String,
        default_playbook: String,
        default_pool: Option<String>,
        payload_from: String,
    ) -> Result<Option<Self>> {
        if !spec.buffers() {
            return Ok(None);
        }
        let path = spec
            .path
            .clone()
            .context("local spool requires a path (set via --spool-dir or spool.path)")?;
        let backend = LocalDiskBackend::open(&path)
            .await
            .map_err(|e| anyhow::anyhow!("open local spool {path}: {e}"))?;
        let dlq = LocalDiskBackend::open(format!("{path}/dlq"))
            .await
            .map_err(|e| anyhow::anyhow!("open local spool dlq: {e}"))?;

        let mut circuits = CircuitRegistry::new(&spec.circuit);
        // Circuit state lives next to the spool, not NATS KV — restart-durable
        // locally (RFC §8.6: "local file").  It goes in a sibling `control/`
        // dir, NOT the live spool dir: the LocalDiskBackend counts every
        // `*.json` file in its dir as a spooled item, so a circuit-state.json
        // there would inflate the pending count.
        let control_dir = PathBuf::from(&path).join("control");
        let _ = std::fs::create_dir_all(&control_dir);
        let circuit_file = control_dir.join("circuit-state.json");
        if let Ok(bytes) = std::fs::read(&circuit_file) {
            if let Ok(snapshot) = serde_json::from_slice(&bytes) {
                circuits.restore(&snapshot);
                tracing::info!("restored local circuit state from {}", circuit_file.display());
            }
        }

        let probe_interval_ms = spec.circuit.probe_interval_ms;
        let engine = SpoolEngine::new(spec.clone(), Box::new(backend), Box::new(dlq));

        tracing::info!(
            subscription_id,
            mode = spec.mode.as_str(),
            backend = "local_disk",
            path = %path,
            ordering = spec.ordering.as_str(),
            downstreams = circuits.downstreams().count(),
            "local spool runtime active"
        );

        Ok(Some(Self {
            engine,
            circuits,
            circuit_file,
            sink,
            dispatcher,
            subscription_path,
            subscription_id,
            source_name,
            default_playbook,
            default_pool,
            payload_from,
            probe_interval_ms,
            last_probe_ms: 0,
            recv_seq: 0,
        }))
    }

    fn route(&mut self, plan: &DispatchPlan) -> (String, CircuitDecision) {
        let resolved = plan
            .execution_pool_override
            .as_deref()
            .or(self.default_pool.as_deref());
        let downstream = self.circuits.route(resolved).to_string();
        let now = now_ms();
        let decision = self.circuits.breaker_mut(&downstream).decide(now);
        (downstream, decision)
    }

    /// Route one message: dispatch when closed, spool when open.
    pub async fn route_message(&mut self, msg: &PolledMessage, plan: &DispatchPlan) -> Routing {
        let (downstream, decision) = self.route(plan);
        match decision {
            CircuitDecision::Dispatch | CircuitDecision::Probe => Routing::Dispatch,
            CircuitDecision::Spool => self.spool(msg, plan, &downstream, "circuit_open").await,
        }
    }

    async fn spool(
        &mut self,
        msg: &PolledMessage,
        plan: &DispatchPlan,
        downstream: &str,
        reason: &str,
    ) -> Routing {
        let now = now_ms();
        self.recv_seq += 1;
        let ordering_key = self.resolve_ordering_key(msg);
        let item = SpoolItem::new(
            self.subscription_path.clone(),
            self.source_name.clone(),
            msg.clone(),
            plan.idempotency_key.clone(),
            self.recv_seq,
            ordering_key,
            downstream.to_string(),
            reason,
            now,
        );
        let incoming = item.to_bytes().len() as u64;
        match self.engine.admit(now, incoming).await {
            Ok(Admission::Accept) => {}
            Ok(Admission::AcceptWithAlert { spool_bytes }) => {
                self.emit(
                    "subscription.spool.alert",
                    "ALERT",
                    serde_json::json!({ "downstream": downstream, "spool_bytes": spool_bytes }),
                )
                .await;
            }
            Ok(Admission::AcceptAfterEvict(evicted)) => {
                for d in evicted {
                    self.emit_dead_letter(&d).await;
                }
            }
            Ok(Admission::RejectStopAck) => {
                self.emit(
                    "subscription.message.dropped",
                    "DROPPED",
                    serde_json::json!({ "message_id": msg.id, "downstream": downstream, "reason": "retention_full" }),
                )
                .await;
                return Routing::Dropped;
            }
            Err(e) => {
                tracing::error!(error = %e, "local spool admit failed");
                return Routing::Dropped;
            }
        }

        match self.engine.spool(&item).await {
            Ok(spooled) => {
                self.update_gauge().await;
                self.emit(
                    "subscription.message.spooled",
                    "SPOOLED",
                    serde_json::json!({
                        "message_id": msg.id,
                        "recv_seq": spooled.recv_seq,
                        "spool_ref": spooled.spool_ref,
                        "sha256": spooled.sha256,
                        "downstream": downstream,
                        "reason": reason,
                    }),
                )
                .await;
                Routing::Spooled
            }
            Err(e) => {
                tracing::error!(message_id = %msg.id, error = %e, "local spool write failed — message NOT durable");
                Routing::Dropped
            }
        }
    }

    /// Feed a live-dispatch outcome to the breaker (passive signal).
    pub async fn report_dispatch(&mut self, plan: &DispatchPlan, msg: &PolledMessage, ok: bool) {
        let resolved = plan
            .execution_pool_override
            .as_deref()
            .or(self.default_pool.as_deref());
        let downstream = self.circuits.route(resolved).to_string();
        let now = now_ms();
        if ok {
            let dedup = plan.idempotency_key.clone().unwrap_or_else(|| msg.id.clone());
            self.engine.mark_dispatched(&dedup);
            if self.circuits.breaker_mut(&downstream).record_success(now) {
                self.on_circuit_closed(&downstream).await;
            }
        } else if self.circuits.breaker_mut(&downstream).record_failure(now) {
            self.on_circuit_opened(&downstream).await;
        }
    }

    /// Run the active downstream probes if the interval elapsed.  Returns the
    /// downstreams that just recovered (closed) so the caller can drain.
    pub async fn maybe_probe(&mut self) -> Vec<String> {
        let now = now_ms();
        if now.saturating_sub(self.last_probe_ms) < self.probe_interval_ms {
            return Vec::new();
        }
        self.last_probe_ms = now;
        let specs: Vec<_> = self.circuits.downstreams().cloned().collect();
        let mut recovered = Vec::new();
        for ds in specs {
            let Some(up) = probe_downstream(&ds).await else {
                continue;
            };
            let breaker = self.circuits.breaker_mut(&ds.name);
            if up {
                if breaker.record_success(now) {
                    self.on_circuit_closed(&ds.name).await;
                    recovered.push(ds.name.clone());
                }
            } else if breaker.record_failure(now) {
                self.on_circuit_opened(&ds.name).await;
            }
        }
        self.persist_circuit();
        recovered
    }

    /// Drain the spool: replay each item in order (idempotency + dead-letter
    /// via the engine), dispatching in-process and emitting
    /// `subscription.message.replayed` per item.
    pub async fn drain(&mut self) -> Result<()> {
        let pending = self.engine.len().await.unwrap_or(0);
        if pending == 0 {
            return Ok(());
        }
        self.emit(
            "subscription.spool.draining",
            "DRAINING",
            serde_json::json!({ "pending": pending }),
        )
        .await;

        let dispatcher = self.dispatcher.clone();
        let sink = self.sink.clone();
        let subscription_path = self.subscription_path.clone();
        let source_name = self.source_name.clone();
        let default_playbook = self.default_playbook.clone();
        let default_pool = self.default_pool.clone();
        let payload_from = self.payload_from.clone();

        let report = self
            .engine
            .drain(|item: SpoolItem| {
                let dispatcher = dispatcher.clone();
                let sink = sink.clone();
                let subscription_path = subscription_path.clone();
                let source_name = source_name.clone();
                let default_playbook = default_playbook.clone();
                let default_pool = default_pool.clone();
                let payload_from = payload_from.clone();
                async move {
                    // Re-resolve a minimal plan for the replayed message so
                    // routing matches the live path (header redirect honored).
                    let plan = DispatchPlan::default();
                    let playbook = item
                        .message
                        .headers
                        .get("x-noetl-route")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| default_playbook.clone());
                    let result = dispatcher
                        .dispatch(
                            &playbook,
                            default_pool.as_deref(),
                            &item.message,
                            &plan,
                            &payload_from,
                            &subscription_path,
                            &source_name,
                        )
                        .await
                        .map_err(|e| {
                            noetl_tools::ToolError::ExecutionFailed(format!("replay dispatch: {e}"))
                        })?;
                    if !result.ok() {
                        return Err(noetl_tools::ToolError::ExecutionFailed(
                            result.error.unwrap_or_else(|| "replay run failed".into()),
                        ));
                    }
                    // Per-item replayed audit (best-effort).
                    let _ = sink
                        .emit(ExecutorEvent {
                            execution_id: result.execution_id,
                            event_type: "subscription.message.replayed".to_string(),
                            step: "ingress".to_string(),
                            status: "REPLAYED".to_string(),
                            created_at: chrono::Utc::now(),
                            context: serde_json::json!({
                                "message_id": item.message_id,
                                "recv_seq": item.recv_seq,
                                "spool_ref": item.spool_ref(),
                                "execution_id": result.execution_id,
                            }),
                            event_id: None,
                            worker_id: Some("cli-local".to_string()),
                            meta: Some(serde_json::json!({ "emitter": "spool_drain" })),
                        })
                        .await;
                    Ok(())
                }
            })
            .await
            .map_err(|e| anyhow::anyhow!("drain: {e}"))?;

        for d in &report.dead_lettered {
            self.emit_dead_letter(d).await;
        }
        self.update_gauge().await;
        tracing::info!(
            subscription_id = self.subscription_id,
            replayed = report.replayed,
            deduped = report.deduped,
            dead_lettered = report.dead_lettered.len(),
            remaining = report.remaining,
            fully_drained = report.fully_drained,
            "local spool drain pass complete"
        );
        Ok(())
    }

    /// Whether the runtime should drain backlog before resuming live.
    pub fn drain_before_live(&self) -> bool {
        self.engine.drain_before_live()
    }

    /// Pending spooled item count (for the report + tests).
    pub async fn pending(&self) -> usize {
        self.engine.len().await.unwrap_or(0)
    }

    fn resolve_ordering_key(&self, msg: &PolledMessage) -> Option<String> {
        let key_name = self.engine.spec().ordering_key.as_deref()?;
        msg.headers
            .get(key_name)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    }

    async fn on_circuit_opened(&mut self, downstream: &str) {
        let state = self
            .circuits
            .breaker(downstream)
            .map(|b| b.state().clone())
            .unwrap_or_default();
        tracing::warn!(downstream, trips = state.trips, "circuit opened — buffering to local spool");
        self.emit(
            "subscription.circuit.opened",
            "OPEN",
            serde_json::json!({
                "downstream": downstream,
                "consecutive_failures": state.consecutive_failures,
                "trips": state.trips,
            }),
        )
        .await;
        self.persist_circuit();
    }

    async fn on_circuit_closed(&mut self, downstream: &str) {
        tracing::info!(downstream, "circuit closed — downstream recovered");
        self.emit(
            "subscription.circuit.closed",
            "CLOSED",
            serde_json::json!({ "downstream": downstream }),
        )
        .await;
        self.persist_circuit();
    }

    async fn emit_dead_letter(&self, d: &DeadLetter) {
        self.emit(
            "subscription.message.dead_lettered",
            "DEAD_LETTERED",
            serde_json::json!({
                "message_id": d.message_id,
                "recv_seq": d.recv_seq,
                "spool_ref": d.spool_ref,
                "attempts": d.attempts,
                "reason": d.reason,
            }),
        )
        .await;
    }

    async fn update_gauge(&self) {
        if let Ok(bytes) = self.engine.spool_bytes().await {
            tracing::debug!(spool_bytes = bytes, "local spool bytes");
        }
    }

    fn persist_circuit(&self) {
        let snapshot = self.circuits.snapshot();
        if let Ok(bytes) = serde_json::to_vec(&snapshot) {
            if let Err(e) = std::fs::write(&self.circuit_file, bytes) {
                tracing::debug!(error = %e, "local circuit persist failed (non-fatal)");
            }
        }
    }

    async fn emit(&self, event_type: &str, status: &str, context: serde_json::Value) {
        let event = ExecutorEvent {
            execution_id: self.subscription_id,
            event_type: event_type.to_string(),
            step: "ingress".to_string(),
            status: status.to_string(),
            created_at: chrono::Utc::now(),
            context,
            event_id: None,
            worker_id: Some("cli-local".to_string()),
            meta: Some(serde_json::json!({ "emitter": "local_spool_runtime" })),
        };
        if let Err(e) = self.sink.emit(event).await {
            tracing::debug!(event_type, error = %e, "local spool event emit failed (non-fatal)");
        }
    }
}

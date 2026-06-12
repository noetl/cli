//! The local subscription drain loop (RFC #90 Phase 6, §5.3).
//!
//! Holds a `kind: Subscription`'s source open in-process and turns each
//! received message into one in-process playbook run (or a `POST /api/execute`
//! when `--dispatch server`), honoring the header-directive allowlist (§7) and
//! the store-and-forward spool (§8, `local_disk`).  Every step emits the same
//! [`ExecutorEvent`] envelope the in-cluster runtime emits — to a local
//! [`FileEventSink`] (JSONL) here — so a local run produces a replayable
//! event-sourced log identical in shape to the in-cluster / Cloud Run trail.
//!
//! Lifecycle events (`subscription.lifecycle` registered/activated/drained/
//! deactivated) bracket the run; per message the loop emits
//! `subscription.message.received` → `playbook.started` → `playbook.completed`
//! /`failed`, plus `subscription.message.directives_applied` when directives
//! fire and the six spool/circuit events when buffering kicks in.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use noetl_events::{EventSink, ExecutorEvent};
use noetl_tools::tools::source::{
    AckMode, DispatchPlan, PollOptions, PolledMessage, SourceClient,
};
use noetl_tools::tools::build_source;
use noetl_tools::ExecutionContext;

use crate::subscribe::dispatch::Dispatcher;
use crate::subscribe::sink::LocalIdGen;
use crate::subscribe::spec::ParsedSpec;
use crate::subscribe::spool::{LocalSpoolRuntime, Routing};

/// Idle backoff when a poll returns nothing.
const POLL_IDLE_MS: u64 = 300;

/// How the loop terminates.
#[derive(Debug, Clone, Copy)]
pub enum StopWhen {
    /// Run forever (until Ctrl-C / SIGTERM).
    Never,
    /// Stop after `n` messages have been *handled* (dispatched + spooled).
    /// Used by `--max-messages` for bounded local runs / proofs.
    Handled(u64),
    /// Drain the source once (one non-empty poll) then exit (`--once`).
    OneDrain,
}

/// Runs one local subscription to completion / stop condition.
pub struct LocalRuntime {
    spec: ParsedSpec,
    sink: Arc<dyn EventSink>,
    dispatcher: Arc<dyn Dispatcher>,
    ids: Arc<LocalIdGen>,
    /// Optional local credential JSON injected into the source-build context
    /// (e.g. a NATS token / Pub/Sub bearer) by alias.  Never a server call.
    credential: Option<(String, String)>,
    stop: StopWhen,
}

/// Summary returned for the final report.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct RunSummary {
    pub subscription_id: i64,
    pub received: u64,
    pub dispatched: u64,
    pub failed: u64,
    pub spooled: u64,
    pub replayed: u64,
    pub pending_spooled: usize,
}

impl LocalRuntime {
    pub fn new(
        spec: ParsedSpec,
        sink: Arc<dyn EventSink>,
        dispatcher: Arc<dyn Dispatcher>,
        ids: Arc<LocalIdGen>,
        credential: Option<(String, String)>,
        stop: StopWhen,
    ) -> Self {
        Self {
            spec,
            sink,
            dispatcher,
            ids,
            credential,
            stop,
        }
    }

    /// Run the loop until `shutdown` resolves or the stop condition is met.
    /// Builds the source client from the spec, then drives the drain loop.
    pub async fn run<F>(&self, shutdown: F) -> Result<RunSummary>
    where
        F: std::future::Future<Output = ()>,
    {
        // Build the source client (resolve the credential alias into the
        // build context — local file/env, never a server round-trip).
        let mut ctx = ExecutionContext::default();
        if let Some((alias, json)) = &self.credential {
            ctx.set_secret(alias.clone(), json.clone());
        }
        let source = build_source(&self.spec.source_cfg, &ctx)
            .map_err(|e| anyhow::anyhow!("build source: {e}"))?;
        self.run_with_source(source, shutdown).await
    }

    /// Drive the loop against a pre-built source (the test seam + the body of
    /// [`run`]).  Brackets the loop with the register/activate +
    /// drain/deactivate lifecycle events and builds the local spool.
    pub async fn run_with_source<F>(
        &self,
        source: Box<dyn SourceClient>,
        shutdown: F,
    ) -> Result<RunSummary>
    where
        F: std::future::Future<Output = ()>,
    {
        let subscription_id = self.ids.next();
        let mut summary = RunSummary {
            subscription_id,
            ..Default::default()
        };
        let source_name = source.source_name();

        // Lifecycle: registered → activated (event-sourced, RFC §4.3).
        self.lifecycle(subscription_id, "registered", "REGISTERED").await;
        self.lifecycle(subscription_id, "activated", "ACTIVE").await;
        tracing::info!(
            subscription_id,
            path = %self.spec.path,
            source = source_name,
            playbook = %self.spec.default_playbook,
            dispatch = %self.dispatcher.label(),
            "local subscription activated"
        );

        // Build the local spool runtime (None when spool.mode: off).
        let mut spool = LocalSpoolRuntime::build(
            &self.spec.spool,
            self.sink.clone(),
            self.dispatcher.clone(),
            self.spec.path.clone(),
            subscription_id,
            source_name.to_string(),
            self.spec.default_playbook.clone(),
            self.spec.default_pool.clone(),
            self.spec.payload_from.clone(),
        )
        .await
        .context("build local spool runtime")?;

        let opts = PollOptions::new(Some(self.spec.batch), self.spec.timeout_ms, AckMode::OnSuccess);

        let result = self
            .run_loop(
                &*source,
                source_name,
                subscription_id,
                &opts,
                spool.as_mut(),
                &mut summary,
                shutdown,
            )
            .await;

        if let Some(s) = spool.as_ref() {
            summary.pending_spooled = s.pending().await;
        }

        // Lifecycle: drained → deactivated.
        self.lifecycle(subscription_id, "drained", "DRAINING").await;
        self.lifecycle(subscription_id, "deactivated", "DEACTIVATED").await;
        tracing::info!(subscription_id, "local subscription stopped");

        result.map(|_| summary)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_loop<F>(
        &self,
        source: &dyn SourceClient,
        source_name: &str,
        subscription_id: i64,
        opts: &PollOptions,
        mut spool: Option<&mut LocalSpoolRuntime>,
        summary: &mut RunSummary,
        shutdown: F,
    ) -> Result<()>
    where
        F: std::future::Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        loop {
            // Spool maintenance: probe declared downstreams; drain on recovery.
            if let Some(s) = spool.as_deref_mut() {
                let recovered = s.maybe_probe().await;
                if !recovered.is_empty() && s.drain_before_live() {
                    let before = s.pending().await;
                    if let Err(e) = s.drain().await {
                        tracing::warn!(error = %e, "local spool drain failed (will retry)");
                    } else {
                        let after = s.pending().await;
                        summary.replayed += before.saturating_sub(after) as u64;
                    }
                }
            }

            let outcome = tokio::select! {
                biased;
                _ = &mut shutdown => { tracing::info!(subscription_id, "shutdown signal received"); break; }
                r = source.poll(opts) => r,
            };
            let outcome = match outcome {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!(source = source_name, error = %e, "source poll failed");
                    tokio::time::sleep(Duration::from_millis(POLL_IDLE_MS)).await;
                    continue;
                }
            };
            let received = outcome.count() as u64;
            if received == 0 {
                if matches!(self.stop, StopWhen::OneDrain) && summary.received > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(POLL_IDLE_MS)).await;
                continue;
            }
            summary.received += received;

            for msg in &outcome.messages {
                let plan = self.spec.directives.resolve(&msg.headers);
                self.emit_received(subscription_id, msg, source_name).await;

                // Spool/circuit routing first.
                if let Some(s) = spool.as_deref_mut() {
                    match s.route_message(msg, &plan).await {
                        Routing::Spooled => {
                            summary.spooled += 1;
                            if self.reached_stop(summary) {
                                return Ok(());
                            }
                            continue;
                        }
                        Routing::Dropped => {
                            summary.failed += 1;
                            continue;
                        }
                        Routing::Dispatch => {}
                    }
                }

                let res = self.dispatch_one(subscription_id, msg, &plan).await;
                if let Some(s) = spool.as_deref_mut() {
                    s.report_dispatch(&plan, msg, res.as_ref().map(|r| r.ok()).unwrap_or(false))
                        .await;
                }
                match res {
                    Ok(r) if r.ok() => summary.dispatched += 1,
                    Ok(r) => {
                        summary.failed += 1;
                        tracing::warn!(message_id = %msg.id, error = ?r.error, "message run failed");
                    }
                    Err(e) => {
                        summary.failed += 1;
                        tracing::warn!(message_id = %msg.id, error = %e, "message dispatch failed");
                    }
                }
                if self.reached_stop(summary) {
                    return Ok(());
                }
            }

            if matches!(self.stop, StopWhen::OneDrain) {
                break;
            }
        }
        Ok(())
    }

    fn reached_stop(&self, summary: &RunSummary) -> bool {
        match self.stop {
            StopWhen::Handled(n) => summary.dispatched + summary.spooled + summary.failed >= n,
            _ => false,
        }
    }

    /// Dispatch one message + emit the `playbook.started`/`completed` pair
    /// (RFC §3.4) and a `directives_applied` audit when directives fired.
    async fn dispatch_one(
        &self,
        subscription_id: i64,
        msg: &PolledMessage,
        plan: &DispatchPlan,
    ) -> Result<crate::subscribe::dispatch::DispatchResult> {
        let playbook = plan
            .playbook_override
            .clone()
            .unwrap_or_else(|| self.spec.default_playbook.clone());
        let pool = plan
            .execution_pool_override
            .clone()
            .or_else(|| self.spec.default_pool.clone());

        let result = self
            .dispatcher
            .dispatch(
                &playbook,
                pool.as_deref(),
                msg,
                plan,
                &self.spec.payload_from,
                &self.spec.path,
                self.source_label(),
            )
            .await?;

        let exec = result.execution_id;
        // playbook.started (child execution).
        self.emit_child(
            exec,
            "playbook.started",
            "STARTED",
            serde_json::json!({
                "subscription": self.spec.path,
                "parent_execution_id": subscription_id,
                "message_id": msg.id,
                "playbook": playbook,
                "pool": pool,
            }),
        )
        .await;
        // playbook.completed / failed.
        let (etype, status) = if result.ok() {
            ("playbook.completed", "COMPLETED")
        } else {
            ("playbook.failed", "FAILED")
        };
        self.emit_child(
            exec,
            etype,
            status,
            serde_json::json!({
                "subscription": self.spec.path,
                "message_id": msg.id,
                "error": result.error,
            }),
        )
        .await;

        // directives_applied audit (RFC §7.6).
        if !plan.applied.is_empty() || plan.trace.is_some() {
            self.emit_child(
                exec,
                "subscription.message.directives_applied",
                "APPLIED",
                serde_json::json!({
                    "message_id": msg.id,
                    "applied": plan.applied,
                    "route_override": { "playbook": plan.playbook_override, "pool": plan.execution_pool_override },
                    "trace": plan.trace,
                }),
            )
            .await;
        }
        Ok(result)
    }

    fn source_label(&self) -> &str {
        &self.spec.source_cfg.source
    }

    async fn emit_received(&self, subscription_id: i64, msg: &PolledMessage, source: &str) {
        self.emit_child(
            subscription_id,
            "subscription.message.received",
            "RECEIVED",
            serde_json::json!({
                "subscription": self.spec.path,
                "source": source,
                "message_id": msg.id,
                "headers": msg.headers,
            }),
        )
        .await;
    }

    async fn lifecycle(&self, subscription_id: i64, phase: &str, status: &str) {
        self.emit_child(
            subscription_id,
            "subscription.lifecycle",
            status,
            serde_json::json!({ "subscription": self.spec.path, "phase": phase }),
        )
        .await;
    }

    async fn emit_child(
        &self,
        execution_id: i64,
        event_type: &str,
        status: &str,
        context: serde_json::Value,
    ) {
        let event = ExecutorEvent {
            execution_id,
            event_type: event_type.to_string(),
            step: "ingress".to_string(),
            status: status.to_string(),
            created_at: chrono::Utc::now(),
            context,
            event_id: None,
            worker_id: Some("cli-local".to_string()),
            meta: Some(serde_json::json!({ "emitter": "subscribe_runtime" })),
        };
        if let Err(e) = self.sink.emit(event).await {
            tracing::debug!(event_type, error = %e, "local event emit failed (non-fatal)");
        }
    }
}

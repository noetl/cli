//! `CommandSource` — worker-only abstraction over how the worker
//! receives the next command to run.
//!
//! This module is **worker-only**.  The CLI does NOT consume types
//! from here — its local-mode runner is a tree walker that doesn't
//! need a pull-model command source.  See § H.10 of the global hybrid
//! cloud blueprint for the architectural rationale.
//!
//! The worker (R-1.2 PR-2d-2) implements a NATS-backed
//! `CommandSource` that pulls from a durable consumer and uses the
//! control-plane HTTP API to claim individual commands.  Future
//! worker implementations (e.g. an HTTP-poll source for serverless
//! deployments under § H.2's Cloud Run compute substrate) implement
//! the same trait.
//!
//! ## R-1.2 PR-2d-1 — trait redesign
//!
//! The 0.2.x version of this module had a thin
//! `next() -> Result<Option<Command>>` trait.  The worker's real
//! NATS pull loop turned out to need:
//!
//! 1. **Per-pull ack/nack lifecycle** distinct from `next()` so the
//!    caller can ack BEFORE executing the command (NATS redelivery
//!    semantics) or nack on transient claim failures.
//! 2. **A 4-state outcome** because claiming a command can succeed
//!    (`Claimed`), be raced by another worker (`AlreadyClaimed`),
//!    fail transiently and warrant redelivery (`RetryLater`), or
//!    fail terminally (`Failed`) — each maps to a different
//!    follow-up.
//! 3. **A richer `Command` shape** with `render_context` (variables
//!    rendered against the merged step context) and `attempts` (for
//!    backoff decisions in the dispatcher).
//!
//! The 0.3.0 redesign captures all three.  The breaking change is
//! safe because no production consumer exists yet — noetl-worker
//! 1.1.2 doesn't import this module; PR-2d-2 will be its first
//! adoption.

use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

/// One command the worker will dispatch to a tool.
///
/// The shape mirrors the Python-side `noetl.command` row + envelope
/// + render-context map.  Keep field naming aligned with the wire
/// format so JSON serde round-trips both directions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Command {
    /// Stable identifier for this command.  Worker uses the
    /// snowflake id from `noetl.command`; future sources (HTTP poll,
    /// local-mode mock) may use UUIDs.
    pub command_id: String,

    /// Execution this command belongs to.  `i64` mirrors the
    /// Python `noetl.command.execution_id` bigint column and the
    /// `BridgeContext.execution_id` field in the cli's tools_bridge.
    pub execution_id: i64,

    /// Step name from the playbook (e.g. `"fetch_calendar"`).
    pub step: String,

    /// Tool kind that dispatch will route to (e.g. `"http"`,
    /// `"postgres"`, `"rhai"`).  Matches noetl-tools' registry kind.
    pub tool_kind: String,

    /// Tool-specific input payload (the `ToolConfig.config` JSON
    /// passed to `noetl_tools::registry::Tool::execute`).
    pub input: serde_json::Value,

    /// Variables already rendered against the merged step context
    /// (workload + vars + step results).  Tools read this through
    /// `noetl_tools::context::ExecutionContext.variables`.
    ///
    /// R-1.2 PR-2d-1: added so the worker's
    /// `Command.render_context()` method maps cleanly onto this
    /// field.
    #[serde(default)]
    pub render_context: HashMap<String, serde_json::Value>,

    /// Number of times this command has been attempted (incremented
    /// by the control plane on each redelivery).  Used by the
    /// dispatcher for backoff and giving-up decisions.
    ///
    /// R-1.2 PR-2d-1: added so retry policy decisions can flow
    /// through the trait.
    #[serde(default)]
    pub attempts: u32,
}

/// Outcome of an attempt to claim a command from the source.
///
/// The four variants mirror the worker's pre-PR-2d-1 inline
/// `ClaimResult` enum (in `crate::client::ControlPlaneClient`):
///
/// - **Claimed**: this worker successfully owns the command.  Caller
///   should `ack` the source handle and dispatch the command.
/// - **AlreadyClaimed**: another worker raced us and already owns
///   the command.  Caller should `ack` the source handle (no
///   redelivery needed) and skip dispatch.
/// - **RetryLater**: claim failed transiently (overload, contention,
///   network blip).  Caller should `nack` the source handle so the
///   source's redelivery policy gives another worker a shot.
/// - **Failed**: claim failed terminally (catalog mismatch, malformed
///   command, permission denied).  Caller should `nack` and emit a
///   diagnostic event so the failure is visible.
#[derive(Debug, Clone)]
pub enum ClaimOutcome {
    Claimed(Command),
    AlreadyClaimed,
    RetryLater(String),
    Failed(String),
}

/// One pulled item from the source — a claim outcome plus the
/// opaque handle the caller passes back to `ack` or `nack`.
///
/// Generic over the source's `AckHandle` associated type so each
/// `CommandSource` impl can choose its own underlying handle shape
/// (e.g. `async_nats::jetstream::Message` for the NATS source,
/// `()` for an in-memory mock).
#[derive(Debug, Clone)]
pub struct Pulled<H> {
    pub outcome: ClaimOutcome,
    pub ack: H,
}

/// Pull-model command source.
///
/// `next()` returns:
/// - `Ok(Some(Pulled { outcome, ack }))` — one pulled item.  The
///   caller inspects `outcome` (Claimed / AlreadyClaimed /
///   RetryLater / Failed) and decides whether to call `ack` (commit
///   the pull) or `nack` (redeliver).  Both ack and nack are
///   required exactly once per pulled item.
/// - `Ok(None)` — the source is exhausted (local-mode playbook
///   complete, mock source drained).  Long-running sources (worker
///   NATS) never return `None` in normal operation.
/// - `Err(e)` — transient or terminal source error before any pull
///   happened; the caller's retry policy decides whether to call
///   `next()` again.
///
/// ## Lifecycle invariants
///
/// 1. Each `Pulled.ack` handle must be consumed exactly once via
///    `ack(handle)` or `nack(handle)` before the next `next()` call
///    in the same task.  (Multiple concurrent next() calls are
///    safe; each gets its own handle.)
/// 2. `ack` and `nack` are idempotent at the trait level — calling
///    them multiple times on the same handle is undefined; the
///    source impl may panic or treat the duplicate as a no-op.
#[async_trait]
pub trait CommandSource: Send + Sync {
    /// Opaque ack handle returned alongside each pulled item.
    /// Source impls choose their own type:
    /// - NATS source uses `async_nats::jetstream::Message`.
    /// - Mock sources can use `()` or a counter.
    type AckHandle: Send + Sync;

    /// Pull one command from the source.  See trait docs for return
    /// shape and lifecycle.
    async fn next(&mut self) -> Result<Option<Pulled<Self::AckHandle>>>;

    /// Acknowledge a pulled item (commit the pull; do not redeliver).
    async fn ack(&self, handle: Self::AckHandle) -> Result<()>;

    /// Negative-acknowledge a pulled item (redeliver per the source's
    /// own policy).
    async fn nack(&self, handle: Self::AckHandle) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    /// In-memory mock source for testability.  Yields a fixed
    /// sequence of `ClaimOutcome` values and records every ack /
    /// nack call so tests can assert on the lifecycle.
    pub struct MockSource {
        queue: std::collections::VecDeque<ClaimOutcome>,
        ack_log: Arc<Mutex<Vec<MockAck>>>,
        next_ack_id: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum MockAck {
        Acked(u64),
        Nacked(u64),
    }

    impl MockSource {
        pub fn new(outcomes: Vec<ClaimOutcome>) -> Self {
            Self {
                queue: outcomes.into(),
                ack_log: Arc::new(Mutex::new(Vec::new())),
                next_ack_id: 0,
            }
        }

        pub fn ack_log(&self) -> Arc<Mutex<Vec<MockAck>>> {
            Arc::clone(&self.ack_log)
        }
    }

    #[async_trait]
    impl CommandSource for MockSource {
        type AckHandle = u64;

        async fn next(&mut self) -> Result<Option<Pulled<u64>>> {
            match self.queue.pop_front() {
                None => Ok(None),
                Some(outcome) => {
                    let id = self.next_ack_id;
                    self.next_ack_id += 1;
                    Ok(Some(Pulled { outcome, ack: id }))
                }
            }
        }

        async fn ack(&self, handle: u64) -> Result<()> {
            self.ack_log.lock().await.push(MockAck::Acked(handle));
            Ok(())
        }

        async fn nack(&self, handle: u64) -> Result<()> {
            self.ack_log.lock().await.push(MockAck::Nacked(handle));
            Ok(())
        }
    }

    fn dummy_command(id: &str) -> Command {
        Command {
            command_id: id.to_string(),
            execution_id: 12345,
            step: "fetch".to_string(),
            tool_kind: "http".to_string(),
            input: serde_json::json!({"url": "https://example.com"}),
            render_context: HashMap::new(),
            attempts: 0,
        }
    }

    #[tokio::test]
    async fn empty_source_returns_none() {
        let mut source = MockSource::new(vec![]);
        assert!(source.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn next_yields_in_order_and_increments_handles() {
        let mut source = MockSource::new(vec![
            ClaimOutcome::Claimed(dummy_command("a")),
            ClaimOutcome::Claimed(dummy_command("b")),
        ]);

        let first = source.next().await.unwrap().unwrap();
        let second = source.next().await.unwrap().unwrap();

        assert_eq!(first.ack, 0);
        assert_eq!(second.ack, 1);
        if let ClaimOutcome::Claimed(c) = first.outcome {
            assert_eq!(c.command_id, "a");
        } else {
            panic!("expected Claimed");
        }
    }

    #[tokio::test]
    async fn ack_and_nack_recorded_in_order() {
        let source = MockSource::new(vec![]);
        let log = source.ack_log();
        source.ack(7).await.unwrap();
        source.nack(9).await.unwrap();
        source.ack(11).await.unwrap();

        let log = log.lock().await;
        assert_eq!(
            *log,
            vec![MockAck::Acked(7), MockAck::Nacked(9), MockAck::Acked(11)]
        );
    }

    #[tokio::test]
    async fn already_claimed_outcome_carries_handle() {
        let mut source = MockSource::new(vec![ClaimOutcome::AlreadyClaimed]);
        let pulled = source.next().await.unwrap().unwrap();
        assert!(matches!(pulled.outcome, ClaimOutcome::AlreadyClaimed));
        // Caller should ack even when AlreadyClaimed — the message
        // shouldn't redeliver because another worker has the command.
        source.ack(pulled.ack).await.unwrap();
        let log = source.ack_log.lock().await;
        assert_eq!(*log, vec![MockAck::Acked(0)]);
    }

    #[tokio::test]
    async fn retry_later_outcome_carries_error_message() {
        let mut source = MockSource::new(vec![
            ClaimOutcome::RetryLater("overload".to_string()),
        ]);
        let pulled = source.next().await.unwrap().unwrap();
        match pulled.outcome {
            ClaimOutcome::RetryLater(msg) => assert_eq!(msg, "overload"),
            _ => panic!("expected RetryLater"),
        }
    }

    #[tokio::test]
    async fn failed_outcome_carries_error_message() {
        let mut source = MockSource::new(vec![
            ClaimOutcome::Failed("malformed payload".to_string()),
        ]);
        let pulled = source.next().await.unwrap().unwrap();
        match pulled.outcome {
            ClaimOutcome::Failed(msg) => assert_eq!(msg, "malformed payload"),
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn command_round_trips_through_serde_with_defaults() {
        // No render_context or attempts in JSON → defaults applied.
        let json = serde_json::json!({
            "command_id": "cmd-1",
            "execution_id": 7,
            "step": "s",
            "tool_kind": "http",
            "input": {"url": "https://example.com"},
        });
        let cmd: Command = serde_json::from_value(json).unwrap();
        assert!(cmd.render_context.is_empty());
        assert_eq!(cmd.attempts, 0);
    }

    #[test]
    fn command_round_trips_through_serde_with_full_fields() {
        let json = serde_json::json!({
            "command_id": "cmd-2",
            "execution_id": 12345,
            "step": "process",
            "tool_kind": "rhai",
            "input": {"code": "1 + 1"},
            "render_context": {"workload.region": "us-east-1"},
            "attempts": 3,
        });
        let cmd: Command = serde_json::from_value(json).unwrap();
        assert_eq!(cmd.attempts, 3);
        assert_eq!(
            cmd.render_context.get("workload.region"),
            Some(&serde_json::json!("us-east-1"))
        );
    }
}

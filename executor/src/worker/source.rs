//! `CommandSource` — worker-only abstraction over how the worker
//! receives the next command to run.
//!
//! This module is **worker-only**.  The CLI does NOT consume types
//! from here — its local-mode runner is a tree walker that doesn't
//! need a pull-model command source.  See § H.10 of the global hybrid
//! cloud blueprint for the architectural rationale.
//!
//! The worker (R-1.3) implements a NATS-backed `CommandSource` that
//! pulls from a durable consumer.  Future worker implementations
//! (e.g. an HTTP-poll source for serverless deployments under § H.2's
//! Cloud Run compute substrate) implement the same trait.

use anyhow::Result;
use async_trait::async_trait;

/// One command the worker will dispatch to a tool.
///
/// The shape mirrors the Python-side `noetl.command` row + envelope as
/// of v2.103.x — keep the field names aligned so wire-format
/// compatibility is automatic.  R-1.3 will add the remaining fields
/// (input, render context, parent execution id, etc.) when the worker
/// reaches feature parity with the Python worker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Command {
    /// Stable identifier for this command.  CLI generates a UUID;
    /// the worker uses the snowflake id from `noetl.command`.
    pub command_id: String,

    /// Execution this command belongs to.
    pub execution_id: String,

    /// Step name from the playbook (e.g. `"fetch_calendar"`).
    pub step: String,

    /// Tool kind that dispatch will route to (e.g. `"http"`, `"postgres"`).
    pub tool_kind: String,

    /// Tool-specific input payload, already rendered against the merged
    /// step context.
    pub input: serde_json::Value,
}

/// Pull-model command source.
///
/// `next()` returns:
/// - `Ok(Some(cmd))` — one command to dispatch.
/// - `Ok(None)` — the source is exhausted (local-mode playbook
///   complete) and the executor should drain its outstanding work and
///   exit.  Long-running sources (worker NATS) never return `None` in
///   normal operation.
/// - `Err(e)` — transient or terminal source error; the caller's retry
///   policy decides whether to call `next()` again.
#[async_trait]
pub trait CommandSource: Send + Sync {
    async fn next(&mut self) -> Result<Option<Command>>;
}

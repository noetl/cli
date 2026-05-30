//! `CommandSource` — abstraction over how the executor receives the
//! next command to run.
//!
//! The CLI's local mode parses YAML into a graph of commands and
//! supplies them via [`crate::sources::local_playbook::LocalPlaybookSource`].
//! The worker (R-1.3) implements a NATS-backed source that pulls from
//! a durable consumer.  The executor never cares which one it has.

use anyhow::Result;
use async_trait::async_trait;

/// One command the executor will dispatch to a tool.
///
/// The shape mirrors the Python-side `noetl.command` row + envelope as
/// of v2.103.x — keep the field names aligned so wire-format
/// compatibility is automatic.  R-1.2 will add the remaining fields
/// (step, tool kind, input, render context, etc.) by porting the YAML
/// command builder from `repos/cli/src/playbook_runner.rs`.
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

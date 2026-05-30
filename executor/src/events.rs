//! Event emission — pluggable sink that captures the events the
//! executor produces during one execution.
//!
//! Both CLI and worker emit the same shape; only the sink differs:
//!
//! - CLI: `StdoutEventSink` — pretty-prints events to the terminal
//!   (R-1.2 will introduce this).
//! - Worker: `NatsEventSink` — publishes to the configured NATS
//!   subject (R-1.3 will introduce this).
//!
//! The envelope shape mirrors the Python-side
//! `noetl.runtime.events.report_event` payload so wire-format
//! compatibility holds across stacks.  See the noetl/noetl wiki page
//! `handle_event_timing` for the field catalogue.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// One event the executor emits.  Keep field naming aligned with the
/// Python side (`noetl.event` table columns) so envelopes can be
/// projected by either stack.
///
/// R-1.2 PR-2a: `execution_id` is now `i64` (matching the Python
/// `noetl.event.execution_id` bigint column, the CLI's
/// `BridgeContext.execution_id`, the worker's
/// `CommandNotification.execution_id`, and
/// `noetl_tools::context::ExecutionContext.execution_id`).  In 0.1.x
/// the field was `String` as a placeholder; cross-binary work in
/// R-1.2 makes the alignment load-bearing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutorEvent {
    pub execution_id: i64,
    pub event_type: String,
    /// Step name (`node_id` / `node_name` in the Python projector).
    pub step: String,
    pub status: String,
    /// Wall-clock when the event was produced.
    pub created_at: DateTime<Utc>,
    /// Free-form payload; the projector reads typed fields out of this.
    pub context: serde_json::Value,
}

#[async_trait]
pub trait EventSink: Send + Sync {
    /// Emit `event` to whatever durable surface the sink is bound to.
    /// Implementations should be idempotent per `event_id` once R-1.2
    /// adds id assignment.
    async fn emit(&self, event: ExecutorEvent) -> Result<()>;
}

/// Trait alias used by the dispatch loop — wraps a sink and a
/// pre-computed `execution_id` so callers don't have to thread it
/// through every call site.
pub struct EventEmitter {
    pub sink: std::sync::Arc<dyn EventSink>,
    pub execution_id: i64,
}

impl EventEmitter {
    pub fn new(execution_id: i64, sink: std::sync::Arc<dyn EventSink>) -> Self {
        Self { sink, execution_id }
    }

    pub async fn emit(
        &self,
        event_type: &str,
        step: &str,
        status: &str,
        context: serde_json::Value,
    ) -> Result<()> {
        let event = ExecutorEvent {
            execution_id: self.execution_id,
            event_type: event_type.to_string(),
            step: step.to_string(),
            status: status.to_string(),
            created_at: Utc::now(),
            context,
        };
        self.sink.emit(event).await
    }
}

/// Drops every event on the floor.  Useful in tests and as the default
/// during the R-1.1 skeleton phase before the real CLI/worker sinks
/// exist.
#[derive(Default)]
pub struct NoopSink;

#[async_trait]
impl EventSink for NoopSink {
    async fn emit(&self, _event: ExecutorEvent) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn noop_sink_accepts_any_event() {
        let sink: Arc<dyn EventSink> = Arc::new(NoopSink);
        let emitter = EventEmitter::new(12345, sink);
        emitter
            .emit(
                "batch.completed",
                "start",
                "COMPLETED",
                serde_json::json!({"processing_ms": 12.3}),
            )
            .await
            .expect("noop emit");
    }
}

//! Event emission — pluggable sink that captures the events the
//! executor produces during one execution.
//!
//! EE-4 (noetl/ai-meta#49 follow-up): the canonical envelope and
//! sink types moved into the sibling `noetl-events` crate so the
//! noetl-server can depend on them without dragging in the rest of
//! the executor's surface (playbook parser, runtime trait, tools
//! bridge, …).  This module now re-exports everything verbatim so
//! existing call sites (the cli binary's local-mode runner, the
//! noetl-worker via `pub use noetl_executor::events::ExecutorEvent`)
//! keep working unchanged.
//!
//! The wire shape is documented in `noetl-events`'s crate-level
//! docs; this re-export layer preserves the legacy import path
//! `noetl_executor::events::*` while the new canonical path is
//! `noetl_events::*`.
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

pub use noetl_events::{EventEmitter, EventSink, ExecutorEvent, NoopSink};

//! # `noetl-executor` — shared execution core
//!
//! Hosts the execution logic that the NoETL CLI's `noetl run --runtime
//! local` path and the `noetl-worker` NATS daemon share.  See Appendix
//! H of the global hybrid cloud blueprint at
//! <https://noetl.dev/docs/architecture/noetl_global_hybrid_cloud_grid_distributed_architecture_blueprint>
//! for the architectural rationale.
//!
//! ## Shape
//!
//! ```text
//!   CommandSource  ->  dispatch::route  ->  ToolRegistry  ->  EventSink
//!   (LocalPlaybook                                            (CLI stdout or
//!    or NATS)                                                  worker NATS)
//! ```
//!
//! Both surfaces (`noetl run --runtime local` and `noetl-worker`) plug
//! a different [`source::CommandSource`] and a different
//! [`events::EventSink`] into the same [`runtime::ExecutionContext`].
//!
//! ## Stability
//!
//! `0.1.x` is a pre-production skeleton.  Public API is expected to
//! churn through phases R-1.1 (this skeleton) and R-1.2 (CLI wires
//! into it).  Treat the crate as internal until it ships in
//! `noetl-worker` (R-1.3).

#![allow(dead_code)]

pub mod runtime;
pub mod source;
pub mod sources;
pub mod dispatch;
pub mod events;
pub mod playbook;

/// Re-exports for downstream crates (the CLI and the worker) so they
/// can import via the crate root.
pub use events::{EventEmitter, EventSink};
pub use runtime::{CredentialResolver, ExecutionContext};
pub use source::{Command, CommandSource};

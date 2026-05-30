//! # `noetl-executor` — shared utilities and types for CLI + worker
//!
//! Hosts the type definitions, template rendering, condition
//! evaluation, capability validation, and event-emission shape that
//! the NoETL CLI's `noetl run --runtime local` path AND the
//! `noetl-worker` NATS daemon share.
//!
//! ## What this crate is
//!
//! A **utilities-and-types** crate.  See Appendix H of the global
//! hybrid cloud blueprint at
//! <https://noetl.dev/docs/architecture/noetl_global_hybrid_cloud_grid_distributed_architecture_blueprint>
//! and especially § H.10 (the tree-walker vs pull-model finding)
//! for the architectural rationale.
//!
//! ## What this crate is NOT
//!
//! A **control-loop crate**.  The CLI keeps its recursive tree
//! walker in `repos/cli/src/playbook_runner.rs` — that shape is the
//! natural fit for local YAML execution.  The worker keeps its
//! NATS pull loop.  These control loops are fundamentally different;
//! attempts to unify them produce more abstraction than they remove.
//!
//! ## Module layout
//!
//! - [`playbook`] — Pydantic-like YAML playbook types (R-1.1 PR-2a).
//! - [`template`] — `render_template`, `render_template_with_result`,
//!   `get_json_path`, `json_to_rhai`, `rhai_to_json_string` (R-1.1 PR-2b).
//! - [`condition`] — `evaluate_condition`, `evaluate_rhai_condition`
//!   (R-1.1 PR-2b).
//! - [`capabilities`] — `validate_capabilities` + `ValidationReport`
//!   (R-1.1 PR-2b).
//! - [`runtime`] — `ExecutionContext`, `CredentialResolver` trait
//!   (R-1.1 PR-1, kept; concrete CLI / worker contexts live in their
//!   own crates).
//! - [`events`] — `ExecutorEvent`, `EventSink` trait, `NoopSink`,
//!   `EventEmitter` helper (R-1.1 PR-1).
//! - [`worker`] — worker-only abstractions: [`worker::source`]
//!   contains the `Command` struct + `CommandSource` trait.  The CLI
//!   does NOT consume these; they exist for the worker's NATS pull
//!   loop.
//!
//! ## Stability
//!
//! `0.1.x` is pre-production.  Public API churns through R-1.1's
//! sub-PRs and stabilises around R-1.3 (worker depends on the
//! crate).  Treat as internal until then; the crate ships with
//! `publish = false`.

#![allow(dead_code)]

pub mod capabilities;
pub mod condition;
pub mod events;
pub mod playbook;
pub mod runtime;
pub mod template;
pub mod tools_bridge;
pub mod worker;

/// Re-exports for downstream crates (the CLI and the worker) so they
/// can import from the crate root.
pub use events::{EventEmitter, EventSink};
pub use runtime::{CredentialResolver, ExecutionContext};

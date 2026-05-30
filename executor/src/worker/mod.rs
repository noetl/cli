//! Worker-only abstractions.
//!
//! The CLI does NOT consume types from this module — its local-mode
//! runner is a tree walker that doesn't need a pull-model command
//! source.  See § H.10 of the global hybrid cloud blueprint for the
//! architectural rationale.
//!
//! Submodules:
//!
//! - [`source`] — the `Command` struct + `CommandSource` trait that the
//!   worker's NATS pull loop will implement (R-1.3).

pub mod source;

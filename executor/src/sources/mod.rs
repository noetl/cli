//! Concrete [`crate::source::CommandSource`] implementations.
//!
//! - [`local_playbook`] parses a YAML playbook and emits commands
//!   inline.  Used by the CLI's `noetl run --runtime local`.
//! - A `nats` module will land in R-1.3 when the worker depends on
//!   this crate.

pub mod local_playbook;

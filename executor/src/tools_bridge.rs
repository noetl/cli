//! Bridge from the CLI's YAML-parsed [`crate::playbook::Tool`] enum
//! onto the [`noetl_tools`] registry's dispatch API.
//!
//! Added in R-1.1 PR-2c-1 per § H.10.4 of Appendix H of the global
//! hybrid cloud blueprint.  This module is the integration surface
//! between the CLI's parsed playbook and the shared tool registry the
//! worker (R-1.3) also uses.
//!
//! ## Strategy
//!
//! Replacement of the CLI's inline tool implementations happens
//! incrementally — one tool kind per sub-PR (PR-2c-2 noop, PR-2c-3
//! rhai, PR-2c-4 shell, PR-2c-5 http, PR-2c-6 duckdb, PR-2c-7
//! playbook, PR-2c-8 auth/sink bridge).  This module's surface grows
//! one function per sub-PR; this PR (PR-2c-1) ships the scaffold and
//! the dependency wiring only.  No CLI call sites change yet.
//!
//! ## Why a bridge instead of converting the Tool enum directly
//!
//! The CLI's `Tool` enum and the registry's `ToolConfig` carry
//! different invariants:
//!
//! - The CLI's `Tool::Auth { provider, scopes, project }` resolves
//!   credentials inline during dispatch.  The worker resolves them at
//!   credential-resolution time (before tool dispatch).  The bridge
//!   needs to know which mode to use; it's not a trivial enum cast.
//! - The CLI's `Tool::Sink { target, format }` writes outputs through
//!   the runner's filesystem helpers.  The registry would dispatch
//!   sinks through the same `noetl-tools` registry, but the tool kind
//!   doesn't exist on the worker side yet (PR-2c-8 may add it).
//! - The CLI's `Tool::DuckDb { db, query, params }` opens a fresh
//!   DuckDB connection per call.  `noetl-tools::tools::duckdb`
//!   manages a pool.  Semantic difference; needs careful migration.
//!
//! Keeping the bridge explicit forces these decisions into one place
//! instead of scattering them across each tool-kind sub-PR.

#![allow(dead_code)] // scaffold-only this PR; subsequent PRs fill it in.

use crate::playbook::Tool;

/// Marker type — outcome of a bridged tool dispatch.  R-1.1 PR-2c-2
/// onwards extends this with the concrete result envelope each tool
/// returns once its inline implementation is replaced.
#[derive(Debug)]
pub struct BridgeOutcome {
    /// Free-form tool result as a string.  Matches the existing CLI
    /// shape where `execute_tool` returns `Result<Option<String>>`.
    pub result: Option<String>,
}

/// Bridge dispatch for one tool.  Currently a stub that returns
/// `None` for every tool kind; subsequent sub-PRs replace each
/// inline implementation in `playbook_runner.rs` with a call into
/// here and fill in the matching arm.
pub fn dispatch_via_registry(_tool: &Tool) -> anyhow::Result<BridgeOutcome> {
    // R-1.1 PR-2c-1: scaffold only.  No tool kind is bridged yet.
    // R-1.1 PR-2c-2 onwards fills in arms per Strategy B.
    Ok(BridgeOutcome { result: None })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_via_registry_stub_returns_none() {
        // The scaffold returns BridgeOutcome { result: None } for any
        // tool kind.  Subsequent sub-PRs will replace this test with
        // per-tool-kind dispatch assertions.
        let tool = Tool::Unsupported;
        let outcome = dispatch_via_registry(&tool).unwrap();
        assert!(outcome.result.is_none());
    }
}

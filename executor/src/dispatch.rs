//! Tool dispatch — routes a [`crate::source::Command`] to the tool
//! kind that should execute it.
//!
//! Skeleton.  R-1.2 wires this to the `noetl-tools` registry so the
//! same code that the CLI uses today (via `playbook_runner.rs`'s
//! tool dispatch) services every executor consumer.

use anyhow::Result;

use crate::runtime::ExecutionContext;
use crate::source::Command;

/// Result of running one command.  R-1.2 will replace the
/// `serde_json::Value` payload with the concrete tool-result envelope
/// that the Python `noetl.runtime.events.report_event` builds (status,
/// duration, error, output, render context updates).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandOutcome {
    pub command_id: String,
    pub status: CommandStatus,
    pub output: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandStatus {
    Completed,
    Failed,
}

/// Dispatches one command through the tool registry.
///
/// R-1.1 skeleton: always returns a `Completed` outcome with an empty
/// payload so downstream wiring can be exercised end-to-end without a
/// real tool surface.  R-1.2 replaces the body with the real
/// `noetl-tools` registry dispatch.
pub async fn dispatch_command(
    _ctx: &ExecutionContext,
    cmd: Command,
) -> Result<CommandOutcome> {
    Ok(CommandOutcome {
        command_id: cmd.command_id,
        status: CommandStatus::Completed,
        output: serde_json::json!({}),
    })
}

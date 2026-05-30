//! Local-playbook command source.
//!
//! Skeleton.  R-1.2 extracts the actual playbook-parsing + command-
//! generation logic from `repos/cli/src/playbook_runner.rs` (~2,700
//! LoC) into this module and reduces the CLI to a thin orchestration
//! layer around the executor.
//!
//! For now the type is a placeholder that satisfies the trait without
//! reading any YAML — wiring R-1.2 lifts the real implementation
//! verbatim.

use anyhow::Result;
use async_trait::async_trait;
use std::collections::VecDeque;

use crate::source::{Command, CommandSource};

/// Reads commands from an in-memory queue.  R-1.2 will replace this
/// with the real YAML parser + command builder lifted from
/// `repos/cli/src/playbook_runner.rs`.
pub struct LocalPlaybookSource {
    pending: VecDeque<Command>,
}

impl LocalPlaybookSource {
    /// Constructs a source from a pre-built command queue.  Useful for
    /// tests and as the temporary shape during the R-1.1 skeleton —
    /// the CLI doesn't depend on this crate yet so no real playbook
    /// path is needed.
    pub fn from_queue(commands: Vec<Command>) -> Self {
        Self {
            pending: commands.into(),
        }
    }
}

#[async_trait]
impl CommandSource for LocalPlaybookSource {
    async fn next(&mut self) -> Result<Option<Command>> {
        Ok(self.pending.pop_front())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cmd(step: &str, kind: &str) -> Command {
        Command {
            command_id: format!("cmd_{step}"),
            execution_id: "exec_test".into(),
            step: step.into(),
            tool_kind: kind.into(),
            input: json!({}),
        }
    }

    #[tokio::test]
    async fn returns_commands_in_order_then_none() {
        let mut src = LocalPlaybookSource::from_queue(vec![
            cmd("a", "http"),
            cmd("b", "postgres"),
        ]);

        let first = src.next().await.unwrap().expect("a");
        assert_eq!(first.step, "a");
        let second = src.next().await.unwrap().expect("b");
        assert_eq!(second.step, "b");
        assert!(src.next().await.unwrap().is_none(), "drained");
    }

    #[tokio::test]
    async fn empty_queue_is_immediately_drained() {
        let mut src = LocalPlaybookSource::from_queue(vec![]);
        assert!(src.next().await.unwrap().is_none());
    }
}

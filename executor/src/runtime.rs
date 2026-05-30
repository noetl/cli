//! Per-execution runtime context shared between sources, dispatch, and
//! event emission.
//!
//! `ExecutionContext` is the durable carrier for one playbook execution.
//! Both the CLI's local-mode runner and the worker's NATS daemon
//! construct one of these per invocation; the dispatch layer reads from
//! it to resolve credentials, look up rendered step input, and emit
//! events into the configured sink.
//!
//! This module deliberately starts minimal — the skeleton commits the
//! shape, R-1.2 (extraction PR) fills in the concrete fields by porting
//! them from `repos/cli/src/playbook_runner.rs`.

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// Identifies one playbook execution end-to-end.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionId(pub String);

/// Trait for resolving keychain credential aliases at step-execution
/// time.  Mirrors the Python-side `noetl.core.credential_refs`
/// resolution flow; see noetl/noetl wiki page `credential-resolution-flow`.
#[async_trait::async_trait]
pub trait CredentialResolver: Send + Sync {
    /// Resolve `alias` (e.g. `"duffel"`) to its concrete credential
    /// envelope.  Implementations consult the NoETL keychain (worker
    /// path) or the CLI's local keychain cache (CLI path).
    async fn resolve(&self, alias: &str) -> Result<serde_json::Value>;
}

/// Carries everything one execution needs.  Cheap to clone via `Arc`s
/// on the inner heavy fields.
#[derive(Clone)]
pub struct ExecutionContext {
    pub execution_id: ExecutionId,
    pub credentials: Arc<dyn CredentialResolver>,
    /// Step-level results accumulated during execution.  Used by the
    /// template layer to render `{{ step_name.result }}` references.
    pub step_results: Arc<tokio::sync::RwLock<HashMap<String, serde_json::Value>>>,
    /// Workload payload (`workload:` block from the YAML, plus any
    /// `--set` overrides from the CLI or runtime defaults from NATS).
    pub workload: Arc<serde_json::Value>,
}

impl ExecutionContext {
    /// Construct a fresh context.  R-1.2 will add a builder that
    /// reads the workload from the YAML's `workload:` block and
    /// applies CLI overrides.
    pub fn new(
        execution_id: ExecutionId,
        credentials: Arc<dyn CredentialResolver>,
        workload: serde_json::Value,
    ) -> Self {
        Self {
            execution_id,
            credentials,
            step_results: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            workload: Arc::new(workload),
        }
    }
}

//! Local dispatch model for `noetl subscribe` (RFC #90 Phase 6, §5.3).
//!
//! RFC §5.3 spells out what a received message *does* in pure-local mode:
//!
//! > For each message runs the `dispatch.playbook` **in-process** (reusing the
//! > local `PlaybookRunner`), **or** posts to a configured `server_url`.
//!
//! So the default — the "pure local" mode the phase is about — runs the target
//! playbook **in-process** with the message as its workload, via the same
//! [`crate::playbook_runner::PlaybookRunner`] `noetl exec --runtime local`
//! uses.  No server, no NATS-dispatch.  The `--dispatch server` variant (or a
//! `--server-url`) instead POSTs `/api/execute` to a control plane, keeping
//! the event model identical — only the dispatch sink differs.
//!
//! Both produce a [`DispatchResult`] carrying the per-message child
//! `execution_id` and terminal status, which the runtime turns into the
//! `playbook.started` / `playbook.completed` event-sourced pair (RFC §3.4).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use noetl_tools::tools::source::{DispatchPlan, PolledMessage};

use crate::subscribe::sink::LocalIdGen;

/// Outcome of dispatching one message.
#[derive(Debug, Clone)]
pub struct DispatchResult {
    /// The per-message child execution id (generated app-side for local
    /// dispatch; returned by the server for `--dispatch server`).
    pub execution_id: i64,
    /// Terminal status — `"COMPLETED"` / `"FAILED"`.
    pub status: String,
    /// Error detail when the run failed.
    pub error: Option<String>,
}

impl DispatchResult {
    pub fn ok(&self) -> bool {
        self.status == "COMPLETED"
    }
}

/// Turns one polled message + its resolved directive plan into an execution.
#[async_trait]
pub trait Dispatcher: Send + Sync {
    /// Dispatch `msg` to `playbook` on the resolved `pool`.  The
    /// `payload_from` selects which part of the normalized envelope becomes
    /// the playbook body (`message.json` default).
    #[allow(clippy::too_many_arguments)]
    async fn dispatch(
        &self,
        playbook: &str,
        pool: Option<&str>,
        msg: &PolledMessage,
        plan: &DispatchPlan,
        payload_from: &str,
        subscription: &str,
        source: &str,
    ) -> Result<DispatchResult>;

    /// Human label for the report (`"in-process"` / `"server <url>"`).
    fn label(&self) -> String;
}

// ---------------------------------------------------------------------------
// Local in-process dispatcher (the pure-local default)
// ---------------------------------------------------------------------------

/// Runs the target playbook in-process via [`crate::playbook_runner`].
pub struct LocalDispatcher {
    /// Base dir for resolving relative `dispatch.playbook` refs (defaults to
    /// the subscription spec's directory).
    playbook_dir: PathBuf,
    verbose: bool,
    ids: Arc<LocalIdGen>,
}

impl LocalDispatcher {
    pub fn new(playbook_dir: PathBuf, verbose: bool, ids: Arc<LocalIdGen>) -> Self {
        Self {
            playbook_dir,
            verbose,
            ids,
        }
    }

    fn resolve_path(&self, playbook: &str) -> PathBuf {
        let p = PathBuf::from(playbook);
        if p.is_absolute() || p.exists() {
            p
        } else {
            self.playbook_dir.join(playbook)
        }
    }
}

#[async_trait]
impl Dispatcher for LocalDispatcher {
    #[allow(clippy::too_many_arguments)]
    async fn dispatch(
        &self,
        playbook: &str,
        _pool: Option<&str>,
        msg: &PolledMessage,
        plan: &DispatchPlan,
        payload_from: &str,
        subscription: &str,
        source: &str,
    ) -> Result<DispatchResult> {
        let execution_id = self.ids.next();
        let path = self.resolve_path(playbook);
        if !path.exists() {
            anyhow::bail!(
                "dispatch.playbook '{}' not found (resolved to {}); local dispatch needs a file path",
                playbook,
                path.display()
            );
        }
        let envelope = build_envelope(msg, payload_from, plan, subscription, source);
        let variables = envelope_to_variables(&envelope);
        let verbose = self.verbose;

        // PlaybookRunner is synchronous + CPU/IO-bound; run it off the async
        // executor so the drain loop stays responsive.
        let outcome = tokio::task::spawn_blocking(move || {
            crate::playbook_runner::PlaybookRunner::new(path)
                .with_variables(variables)
                .with_verbose(verbose)
                .with_quiet(true)
                .run()
        })
        .await
        .context("local dispatch task panicked")?;

        match outcome {
            Ok(run) => Ok(DispatchResult {
                execution_id,
                status: if run.status == "ok" { "COMPLETED".into() } else { "FAILED".into() },
                error: run.error,
            }),
            Err(e) => Ok(DispatchResult {
                execution_id,
                status: "FAILED".into(),
                error: Some(format!("{e:#}")),
            }),
        }
    }

    fn label(&self) -> String {
        "in-process (PlaybookRunner)".to_string()
    }
}

// ---------------------------------------------------------------------------
// Server dispatcher (the `--dispatch server` / `--server-url` variant)
// ---------------------------------------------------------------------------

/// POSTs `/api/execute` to a control plane.  Keeps the event model identical
/// to the in-cluster runtime — the server records the run; the local trail
/// still captures the ingress + dispatch records.
pub struct ServerDispatcher {
    client: reqwest::Client,
    server_url: String,
}

impl ServerDispatcher {
    pub fn new(server_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            server_url: server_url.trim_end_matches('/').to_string(),
        }
    }
}

#[async_trait]
impl Dispatcher for ServerDispatcher {
    #[allow(clippy::too_many_arguments)]
    async fn dispatch(
        &self,
        playbook: &str,
        pool: Option<&str>,
        msg: &PolledMessage,
        plan: &DispatchPlan,
        payload_from: &str,
        subscription: &str,
        source: &str,
    ) -> Result<DispatchResult> {
        let envelope = build_envelope(msg, payload_from, plan, subscription, source);
        let mut body = serde_json::json!({
            "path": playbook,
            "workload": envelope,
        });
        if let Some(p) = pool {
            body["execution_pool"] = serde_json::json!(p);
        }
        let resp = self
            .client
            .post(format!("{}/api/execute", self.server_url))
            .json(&body)
            .send()
            .await
            .context("POST /api/execute")?;
        let status = resp.status();
        let json: serde_json::Value = resp.json().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("server /api/execute returned {status}: {json}");
        }
        let execution_id = json
            .get("execution_id")
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0);
        Ok(DispatchResult {
            execution_id,
            status: "COMPLETED".into(),
            error: None,
        })
    }

    fn label(&self) -> String {
        format!("server {}", self.server_url)
    }
}

// ---------------------------------------------------------------------------
// Envelope construction (shared, mirrors the worker's build_payload)
// ---------------------------------------------------------------------------

/// Build the per-message execution payload, identical in shape to the worker
/// runtime's `build_payload` so a playbook reads the same envelope whether it
/// ran in-cluster or locally.  The full normalized message rides under
/// `message`; the `payload_from` selection is merged to the top level (object
/// body) or placed under `body` (scalar); idempotency key + content type from
/// directives ride alongside.
pub fn build_envelope(
    msg: &PolledMessage,
    payload_from: &str,
    plan: &DispatchPlan,
    subscription: &str,
    source: &str,
) -> serde_json::Value {
    let mut payload = serde_json::Map::new();
    payload.insert(
        "message".to_string(),
        serde_json::to_value(msg).unwrap_or(serde_json::Value::Null),
    );
    payload.insert("subscription".to_string(), serde_json::json!(subscription));
    payload.insert("source".to_string(), serde_json::json!(source));

    let primary = match payload_from {
        "message.attributes" => msg.attributes.clone(),
        "message.body" => match &msg.data {
            serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
            other => serde_json::Value::String(other.to_string()),
        },
        _ => msg.data.clone(),
    };
    match primary {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                payload.entry(k).or_insert(v);
            }
        }
        other => {
            payload.insert("body".to_string(), other);
        }
    }
    if let Some(k) = plan.idempotency_key.as_ref() {
        payload.insert("idempotency_key".to_string(), serde_json::json!(k));
    }
    if let Some(c) = plan.content_type.as_ref() {
        payload.insert("content_type".to_string(), serde_json::json!(c));
    }
    serde_json::Value::Object(payload)
}

/// Flatten the envelope object into the string-keyed variables
/// [`crate::playbook_runner::PlaybookRunner`] accepts.  PlaybookRunner prefixes
/// each key with `workload.`, so a top-level scalar field `order_id` resolves
/// as `{{ workload.order_id }}`; nested objects (`message`, the merged body)
/// are passed as compact JSON strings so `{{ workload.message }}` is available
/// as a string the playbook can re-parse.
pub fn envelope_to_variables(envelope: &serde_json::Value) -> HashMap<String, String> {
    let mut vars = HashMap::new();
    if let Some(map) = envelope.as_object() {
        for (k, v) in map {
            let s = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            };
            vars.insert(k.clone(), s);
        }
    }
    vars
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(data: serde_json::Value, headers: serde_json::Value) -> PolledMessage {
        PolledMessage {
            id: "stream:7".to_string(),
            data,
            headers: headers.as_object().cloned().unwrap_or_default(),
            attributes: json!({}),
            metadata: json!({}),
            ack_id: None,
        }
    }

    #[test]
    fn envelope_merges_json_body_to_top_level() {
        let m = msg(json!({ "order_id": 42, "amount": 9 }), json!({}));
        let env = build_envelope(&m, "message.json", &DispatchPlan::default(), "subs/orders", "nats");
        assert_eq!(env["order_id"], 42);
        assert_eq!(env["message"]["data"]["order_id"], 42);
        assert_eq!(env["subscription"], "subs/orders");
        assert_eq!(env["source"], "nats");
    }

    #[test]
    fn envelope_scalar_body_under_body_key() {
        let m = msg(json!("raw"), json!({}));
        let env = build_envelope(&m, "message.json", &DispatchPlan::default(), "p", "nats");
        assert_eq!(env["body"], "raw");
    }

    #[test]
    fn variables_flatten_scalars_and_stringify_nested() {
        let m = msg(json!({ "order_id": 42, "nested": { "x": 1 } }), json!({}));
        let env = build_envelope(&m, "message.json", &DispatchPlan::default(), "subs/orders", "nats");
        let vars = envelope_to_variables(&env);
        assert_eq!(vars.get("order_id").map(String::as_str), Some("42"));
        assert_eq!(vars.get("subscription").map(String::as_str), Some("subs/orders"));
        // nested object is passed as compact JSON for re-parse.
        assert!(vars.get("message").unwrap().contains("\"order_id\":42"));
        assert!(vars.get("nested").unwrap().contains("\"x\":1"));
    }
}

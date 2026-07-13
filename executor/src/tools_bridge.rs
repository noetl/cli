//! Bridge from the CLI's YAML-parsed [`crate::playbook::Tool`] enum
//! onto the [`noetl_tools`] registry's dispatch API.
//!
//! Added in R-1.1 PR-2c-1 per § H.10.4 of Appendix H of the global
//! hybrid cloud blueprint; fleshed out with adapter helpers in
//! R-1.1 PR-2c-2.  This module is the integration surface between
//! the CLI's parsed playbook and the shared tool registry the
//! worker (R-1.3) also uses.
//!
//! ## Strategy B rollout
//!
//! Replacement of the CLI's inline tool implementations happens
//! incrementally — one tool kind per sub-PR (PR-2c-3 rhai, PR-2c-4
//! shell, PR-2c-5 http, PR-2c-6 duckdb, PR-2c-7 playbook, PR-2c-8
//! auth + sink).  This module ships the adapter layer in PR-2c-2;
//! each subsequent sub-PR fills in one [`dispatch_via_registry`]
//! match arm and replaces the matching CLI call site in
//! `repos/cli/src/playbook_runner.rs`.
//!
//! ## Why a bridge instead of converting the Tool enum directly
//!
//! The CLI's [`crate::playbook::Tool`] enum and the registry's
//! [`noetl_tools::registry::ToolConfig`] carry different invariants:
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
//!
//! ## GCS upload helper (R-3, noetl/ai-meta#31)
//!
//! [`gcs_upload`] wraps `object_store::gcp::GoogleCloudStorageBuilder`
//! so the CLI's `SinkTarget::Gcs` arm no longer shells out to `gsutil`.
//! Auth flows through the same provider chain as
//! [`resolve_auth_to_bearer`]: workload identity on GKE, Application
//! Default Credentials on dev hosts.  The helper accepts a pluggable
//! `Arc<dyn ObjectStore>` so integration tests substitute an
//! `object_store::memory::InMemory` store without real GCS.  See
//! [`gcs_upload`] for the full credential-chain and error-shape notes.

#![allow(dead_code)] // until PR-2c-4 onwards wires the call sites in.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use bytes::Bytes;
use object_store::path::Path as StorePath;
use object_store::ObjectStore;
use object_store::PutPayload;
use noetl_tools::auth::GcpAuth;
use noetl_tools::context::ExecutionContext as ToolsExecutionContext;
use noetl_tools::registry::{AuthConfig as ToolsAuthConfig, Tool as ToolsRegistryTool, ToolConfig};
use noetl_tools::result::{ToolResult, ToolStatus};
use noetl_tools::tools::{DuckdbTool, HttpTool, ProviderTool, RhaiTool, ShellTool};
use tracing::{info_span, Instrument};

use crate::playbook::{AuthConfig as CliAuthConfig, CmdsList, SinkFormat, Tool};

// ---------------------------------------------------------------------------
// Bridge outcome — what the dispatch returns back to the caller.
// ---------------------------------------------------------------------------

/// Outcome of a bridged tool dispatch.
///
/// The shape matches the existing CLI surface where
/// `PlaybookRunner::execute_tool` returns `Result<Option<String>>`:
/// `result == Some(s)` for a successful tool execution that produced
/// output the runner stores in `step_results[step].result`; `None`
/// for tools that do not produce a per-step string result (e.g.
/// fire-and-forget sinks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeOutcome {
    pub result: Option<String>,
}

impl BridgeOutcome {
    pub fn empty() -> Self {
        Self { result: None }
    }
}

// ---------------------------------------------------------------------------
// Bridge context — what the dispatch needs from the caller.
// ---------------------------------------------------------------------------

/// Per-call context for the bridge.  Groups together what would
/// otherwise be many parameters threaded through every dispatch site.
///
/// The CLI's `ExecutionContext` (`repos/cli/src/playbook_runner.rs`)
/// has a different shape than [`ToolsExecutionContext`] — the CLI
/// uses `HashMap<String, String>` for variables and tracks step
/// results separately; `noetl-tools` uses `HashMap<String,
/// serde_json::Value>` and bundles many more execution-level fields
/// (server_url, worker_id, command_id, etc.).
///
/// `BridgeContext` is the narrow view the CLI hands to the bridge;
/// [`to_tools_context`] expands it into the full
/// [`ToolsExecutionContext`] shape.
pub struct BridgeContext<'a> {
    /// Execution id — required by [`ToolsExecutionContext`].  CLI
    /// local mode synthesises this from the start time / playbook
    /// path; the worker uses the snowflake id from `noetl.command`.
    pub execution_id: i64,

    /// Step name the bridged tool is running under.
    pub step: &'a str,

    /// CLI variables map (workload.*, vars.*, <step>.result, etc.).
    pub variables: &'a HashMap<String, String>,

    /// Control-plane server URL.  Empty string when running in
    /// CLI local mode without a server backend.
    pub server_url: String,

    /// Worker id / command id — `None` in CLI local mode.
    pub worker_id: Option<String>,
    pub command_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Adapters
// ---------------------------------------------------------------------------

/// Convert a [`BridgeContext`] into the [`ToolsExecutionContext`]
/// shape `noetl-tools` tools expect.  String variables become
/// [`serde_json::Value::String`] entries; secrets stay empty (CLI
/// local mode resolves credentials at the credential-resolver layer,
/// not at tool dispatch).
///
/// Variable shape: **flat**.  Each CLI variable `workload.region`
/// becomes a JSON value at the same flat key in the resulting map.
/// This matches what most `noetl-tools` tools (http / postgres / etc.)
/// expect from their template engine.  The rhai tool needs a
/// *nested* shape so `workload.region` is reachable as a Rhai field
/// access on a `workload` map; see [`to_tools_context_for_rhai`] for
/// the restructured variant used inside the rhai dispatch arm.
pub fn to_tools_context(bridge: &BridgeContext) -> ToolsExecutionContext {
    let variables: HashMap<String, serde_json::Value> = bridge
        .variables
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();

    ToolsExecutionContext {
        execution_id: bridge.execution_id,
        step: bridge.step.to_string(),
        variables,
        server_url: bridge.server_url.clone(),
        worker_id: bridge.worker_id.clone(),
        command_id: bridge.command_id.clone(),
        ..ToolsExecutionContext::default()
    }
}

/// Build a [`ToolsExecutionContext`] whose `variables` map matches the
/// scope shape the CLI's inline `execute_rhai_script` produced — flat
/// `workload.region` / `vars.x` / `<step>.<field>` keys grouped into
/// nested objects so Rhai's `workload.region` / `vars.x` / `<step>.<field>`
/// field-access syntax works.
///
/// PR-2c-3 introduces this for the rhai dispatch arm.  Other tool
/// kinds (http, postgres, duckdb, etc.) continue to consume the flat
/// shape from [`to_tools_context`] because their template engines
/// expect the `{{workload.region}}` lookup style, not Rhai-style
/// field navigation.
pub fn to_tools_context_for_rhai(bridge: &BridgeContext) -> ToolsExecutionContext {
    let mut variables: HashMap<String, serde_json::Value> = HashMap::new();
    let mut workload_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    let mut vars_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    let mut step_maps: HashMap<String, serde_json::Map<String, serde_json::Value>> =
        HashMap::new();

    for (key, value) in bridge.variables {
        let val = serde_json::Value::String(value.clone());
        if let Some(suffix) = key.strip_prefix("workload.") {
            workload_map.insert(suffix.to_string(), val);
        } else if let Some(suffix) = key.strip_prefix("vars.") {
            vars_map.insert(suffix.to_string(), val);
        } else if let Some((step, field)) = key.split_once('.') {
            step_maps
                .entry(step.to_string())
                .or_default()
                .insert(field.to_string(), val);
        } else {
            // Unprefixed keys land at the top level — same shape as
            // [`to_tools_context`].
            variables.insert(key.clone(), val);
        }
    }

    if !workload_map.is_empty() {
        variables.insert(
            "workload".to_string(),
            serde_json::Value::Object(workload_map),
        );
    }
    if !vars_map.is_empty() {
        variables.insert("vars".to_string(), serde_json::Value::Object(vars_map));
    }
    for (step, map) in step_maps {
        variables.insert(step, serde_json::Value::Object(map));
    }

    ToolsExecutionContext {
        execution_id: bridge.execution_id,
        step: bridge.step.to_string(),
        variables,
        server_url: bridge.server_url.clone(),
        worker_id: bridge.worker_id.clone(),
        command_id: bridge.command_id.clone(),
        ..ToolsExecutionContext::default()
    }
}

/// Build a [`ToolConfig`] from a CLI [`Tool`] enum variant.
///
/// The `kind` string matches what [`noetl_tools::registry::ToolRegistry`]
/// uses for dispatch.  The `config` payload is the variant's fields
/// serialized as JSON; the receiving tool deserializes its own
/// expected schema from this value (e.g. `noetl_tools::tools::shell`
/// expects `{"cmds": [...]}`).
///
/// `Tool::Unsupported` returns a `ToolConfig` with `kind: "unsupported"`
/// — dispatch will fail at registry lookup, which matches the CLI's
/// current behaviour of emitting an error.
pub fn to_tools_config(tool: &Tool) -> ToolConfig {
    let (kind, config) = match tool {
        Tool::Shell { cmds } => {
            // noetl-tools::ShellConfig expects a single `command`
            // string.  CLI's CmdsList::Multiple becomes a newline-
            // joined block (one bash invocation with a multi-line
            // script); CmdsList::Single becomes the string verbatim.
            //
            // Important: this is the per-call ToolConfig shape.  The
            // Tool::Shell arm of `dispatch_via_registry` does NOT use
            // this helper because the CLI's runtime semantics require
            // one bash invocation PER command (independent process,
            // no shared cwd/env state) — the dispatch arm loops and
            // builds per-command ToolConfigs via [`shell_command_config`].
            (
                "shell",
                serde_json::json!({
                    "command": match cmds {
                        CmdsList::Single(s) => s.clone(),
                        CmdsList::Multiple(v) => v.join("\n"),
                    },
                    "shell": "bash",
                    "capture": true,
                }),
            )
        }
        Tool::Http {
            method,
            url,
            headers,
            params,
            body,
            auth: _, // resolved at dispatch time into a Bearer header; not threaded through ToolConfig.auth (see PR-2c-5)
        } => (
            "http",
            // noetl-tools' HttpConfig deserializes the method via
            // `#[serde(rename_all = "UPPERCASE")]`, so we emit the
            // uppercased CLI string here.  The body is wrapped as a
            // JSON Value: if the CLI's body parses as JSON we pass the
            // parsed Value (so reqwest serialises it as JSON with the
            // right Content-Type); otherwise we pass it as a JSON
            // string which noetl-tools sends verbatim as the body.
            serde_json::json!({
                "method": method.to_uppercase(),
                "url": url,
                "headers": headers,
                "params": params,
                "body": body.as_deref().map(http_body_value),
            }),
        ),
        Tool::Playbook { path, args, input } => (
            "playbook",
            serde_json::json!({
                "path": path,
                "args": args,
                "input": input,
            }),
        ),
        Tool::DuckDb { db, query, params } => (
            // noetl-tools' DuckdbConfig schema uses `db_path` (not
            // `db`), `query` is required (so we substitute an empty
            // string when the CLI doesn't carry one — the dispatch
            // arm short-circuits in that case), and params are
            // `Vec<serde_json::Value>` rather than `Vec<String>`.
            // Conversion is faithful: a CLI string param becomes a
            // JSON string value bound at the `?` placeholder by
            // noetl-tools' DuckdbTool.
            //
            // Compatibility note: the CLI's pre-PR-2c-6
            // `execute_duckdb_query` accepted but **ignored** the
            // `params` field (signature was `_params: &[String]`).
            // The bridge now binds them, which is a feature gain
            // documented in the PR body and on the executor-crate-
            // architecture wiki page.
            "duckdb",
            serde_json::json!({
                "db_path": db,
                "query": query.clone().unwrap_or_default(),
                "params": params
                    .iter()
                    .map(|p| serde_json::Value::String(p.clone()))
                    .collect::<Vec<_>>(),
                "as_objects": true,
            }),
        ),
        Tool::Rhai { code, args } => (
            "rhai",
            serde_json::json!({
                "code": code,
                "args": args,
            }),
        ),
        Tool::Auth { provider, scopes, project } => (
            "auth",
            serde_json::json!({
                "provider": provider,
                "scopes": scopes,
                "project": project,
            }),
        ),
        Tool::Sink { target, format } => (
            "sink",
            serde_json::json!({
                "target": target_to_value(target),
                "format": format!("{:?}", format).to_lowercase(),
            }),
        ),
        Tool::Provider {
            provider,
            runtime,
            action,
            service,
            dry_run,
            input,
            poll,
            endpoint,
            stack,
            confirm,
            // `auth` is mapped to ToolConfig.auth in the dispatch arm, not into
            // the config body (the ProviderSpec ignores an `auth` config key).
            auth: _,
        } => {
            // Assemble the provider config body the noetl-tools ProviderSpec
            // deserializes.  Optional fields are omitted when absent so the
            // tool's own serde defaults (e.g. runtime=rest, dry_run=true) apply.
            let mut cfg = serde_json::Map::new();
            cfg.insert("provider".into(), serde_json::json!(provider));
            cfg.insert("action".into(), serde_json::json!(action));
            if let Some(r) = runtime {
                cfg.insert("runtime".into(), serde_json::json!(r));
            }
            if let Some(s) = service {
                cfg.insert("service".into(), serde_json::json!(s));
            }
            if let Some(d) = dry_run {
                cfg.insert("dry_run".into(), yaml_to_json(d));
            }
            if let Some(i) = input {
                cfg.insert("input".into(), yaml_to_json(i));
            }
            if let Some(p) = poll {
                cfg.insert("poll".into(), yaml_to_json(p));
            }
            if let Some(e) = endpoint {
                cfg.insert("endpoint".into(), yaml_to_json(e));
            }
            if let Some(s) = stack {
                cfg.insert("stack".into(), serde_json::json!(s));
            }
            if let Some(c) = confirm {
                cfg.insert("confirm".into(), serde_json::json!(c));
            }
            ("provider", serde_json::Value::Object(cfg))
        }
        Tool::Unsupported => ("unsupported", serde_json::json!({})),
    };

    ToolConfig {
        kind: kind.to_string(),
        config,
        timeout: None,
        retry: None,
        auth: None,
    }
}

/// Build a single-command ToolConfig for the shell tool.  Used by
/// the `Tool::Shell` dispatch arm to preserve the CLI's per-command
/// bash-invocation semantics (independent process, no shared
/// cwd/env state across commands).
fn shell_command_config(command: &str) -> ToolConfig {
    ToolConfig {
        kind: "shell".to_string(),
        config: serde_json::json!({
            "command": command,
            "shell": "bash",
            "capture": true,
        }),
        timeout: None,
        retry: None,
        auth: None,
    }
}

/// Convert a CLI HTTP body string into a JSON [`serde_json::Value`]
/// suitable for noetl-tools' `HttpConfig.body` field.  If the body
/// parses as JSON, the parsed value is returned (and `reqwest` sends
/// it with `Content-Type: application/json`).  Otherwise the body
/// is wrapped as a [`Value::String`] which `reqwest` writes
/// verbatim as the request body.
fn http_body_value(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|_| serde_json::Value::String(body.to_string()))
}

/// Resolve a CLI [`AuthConfig`] to a Bearer token using noetl-tools'
/// [`GcpAuth`] provider.
///
/// CLI providers `"gcp"`, `"google"`, and `"adc"` all map to GCP
/// Application Default Credentials.  Any other provider value
/// returns an error matching the CLI's pre-PR-2c-5 behaviour.
///
/// This replaces the CLI's inline `get_auth_token` (which shelled
/// out to `gcloud auth print-access-token`).  See semantic
/// divergence row on the executor-crate-architecture wiki page.
pub async fn resolve_auth_to_bearer(cfg: &CliAuthConfig) -> Result<String> {
    match cfg.provider.as_str() {
        "gcp" | "google" | "adc" => {
            let gcp = GcpAuth::new();
            let scopes: Vec<&str> = cfg.scopes.iter().map(|s| s.as_str()).collect();
            let token = if scopes.is_empty() {
                gcp.get_default_token()
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to get GCP access token: {}", e))?
            } else {
                gcp.get_token(&scopes)
                    .await
                    .map_err(|e| anyhow::anyhow!("failed to get GCP access token: {}", e))?
            };
            Ok(token)
        }
        other => anyhow::bail!(
            "unsupported auth provider: {}. Supported: gcp, google, adc",
            other
        ),
    }
}

/// Build the noetl-tools [`ToolConfig`] for an HTTP request.
///
/// Identical to the [`to_tools_config`] `Tool::Http` arm but pulled
/// out so the dispatch arm can also inject an `Authorization:
/// Bearer <token>` header when a CLI `AuthConfig` is present
/// (resolved via [`resolve_auth_to_bearer`]).
///
/// CLI's `auth` is intentionally NOT mapped to noetl-tools'
/// `ToolConfig.auth` field: that field expects an `AuthConfig` with
/// `credential` / `token` lookup against `ExecutionContext.secrets`,
/// which CLI local mode does not populate.  Pre-resolving the
/// token and injecting it as a header keeps the CLI's existing
/// authority semantics (the CLI process's gcloud / ADC chain) and
/// avoids reshaping the credential resolver path.
fn http_tool_config(
    method: &str,
    url: &str,
    headers: &HashMap<String, String>,
    params: &HashMap<String, String>,
    body: Option<&str>,
    bearer: Option<&str>,
) -> ToolConfig {
    let mut merged_headers = headers.clone();
    if let Some(token) = bearer {
        merged_headers.insert(
            "Authorization".to_string(),
            format!("Bearer {}", token),
        );
    }
    ToolConfig {
        kind: "http".to_string(),
        config: serde_json::json!({
            "method": method.to_uppercase(),
            "url": url,
            "headers": merged_headers,
            "params": params,
            "body": body.map(http_body_value),
        }),
        timeout: None,
        retry: None,
        auth: None,
    }
}

/// Reshape noetl-tools' HTTP result envelope back to the CLI's
/// pre-PR-2c-5 shape.
///
/// noetl-tools' HttpTool always packs `data: {"status_code":
/// u16, "headers": {...}, "body": <json>}` into the ToolResult,
/// regardless of whether the HTTP response was 2xx (Success) or
/// 4xx/5xx (Error).  The CLI's `execute_http_request` returned the
/// envelope `{"status": <int>, "body": <json>}` for ALL HTTP
/// responses (including 4xx/5xx) so playbook steps could branch on
/// the status code.  We preserve that contract here: only network-
/// transport failures bubble up as `anyhow::Error`; HTTP error
/// statuses come back inside the JSON envelope.
fn reshape_http_result(result: ToolResult) -> Result<BridgeOutcome> {
    if let Some(data) = result.data {
        let status_code = data
            .get("status_code")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as i32;
        let body = data
            .get("body")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let envelope = serde_json::json!({
            "status": status_code,
            "body": body,
        });
        return Ok(BridgeOutcome {
            result: Some(envelope.to_string()),
        });
    }
    // No data — fall back to the generic from_tools_result path so
    // we surface whatever error / stdout the tool emitted.
    from_tools_result(result)
}

/// Build a [`ToolConfig`] for a DuckDB query.
///
/// Used by the `Tool::DuckDb` dispatch arm.  Path resolution
/// (playbook-relative vs absolute) and `mkdir -p` of the parent
/// directory are handled at the CLI call site BEFORE the bridge is
/// invoked, so this helper receives an already-resolved absolute
/// path string (or `:memory:` for in-memory mode).
fn duckdb_tool_config(
    db_path: &str,
    query: &str,
    params: &[String],
) -> ToolConfig {
    ToolConfig {
        kind: "duckdb".to_string(),
        config: serde_json::json!({
            "db_path": db_path,
            "query": query,
            "params": params
                .iter()
                .map(|p| serde_json::Value::String(p.clone()))
                .collect::<Vec<_>>(),
            // CLI's pre-PR-2c-6 SELECT result shape was an array of
            // JSON objects keyed by column name; `as_objects: true`
            // matches that.  `reshape_duckdb_result` then unwraps
            // the noetl-tools envelope back to the raw array.
            "as_objects": true,
        }),
        timeout: None,
        retry: None,
        auth: None,
    }
}

/// Reshape noetl-tools' DuckDB result envelope back to the CLI's
/// pre-PR-2c-6 shape.
///
/// noetl-tools' DuckdbTool returns:
/// - SELECT / WITH: `data: {"columns": [...], "rows": [{...}, ...],
///   "row_count": N}`
/// - non-SELECT:    `data: {"affected_rows": N}`
///
/// The CLI's `execute_duckdb_query` returned:
/// - SELECT / WITH: a JSON array of objects (pretty-printed)
/// - non-SELECT:    the literal string `{"status": "ok"}`
///
/// `reshape_duckdb_result` maps the former onto the latter so
/// playbook steps that read `<step>.result[0].col_name` keep
/// working.  `affected_rows` from the noetl-tools envelope is
/// dropped on purpose — the CLI never exposed it.
fn reshape_duckdb_result(result: ToolResult) -> Result<BridgeOutcome> {
    let data = match result.data {
        Some(d) => d,
        None => return from_tools_result(result),
    };

    if let Some(rows) = data.get("rows").and_then(|v| v.as_array()) {
        // SELECT path.  Return the rows array as a pretty-printed
        // JSON string — matches the CLI's
        // `serde_json::to_string_pretty(&results)`.
        let pretty = serde_json::to_string_pretty(rows)?;
        return Ok(BridgeOutcome { result: Some(pretty) });
    }

    if data.get("affected_rows").is_some() {
        // Non-SELECT path.  CLI emitted the literal `{"status":
        // "ok"}` here; preserve that.
        return Ok(BridgeOutcome {
            result: Some(r#"{"status": "ok"}"#.to_string()),
        });
    }

    // Unknown shape — fall back to the generic from_tools_result
    // path so we still surface whatever the tool emitted.
    from_tools_result(ToolResult {
        status: result.status,
        data: Some(data),
        error: result.error,
        stdout: result.stdout,
        stderr: result.stderr,
        exit_code: result.exit_code,
        duration_ms: result.duration_ms,
        // noetl-tools 2.21 added this marker field; the executor
        // bridge has nothing to attach here (DuckDB doesn't dispatch
        // async work), so it always falls through as `None`.
        pending_callback: result.pending_callback,
    })
}

/// Prepare the variable map for a sub-playbook invocation.
///
/// Used by the CLI's `Tool::Playbook` arm (which keeps owning the
/// tree-walker recursion per § H.10).  The helper merges the
/// parent context's variables with the sub-playbook's
/// `input:` (DSL v2) or `args:` (DSL v1 legacy), each rendered
/// against the parent context via the caller-supplied
/// `render_template` closure and prefixed with `workload.` to
/// match the sub-playbook's expected variable shape.
///
/// `input` takes precedence over `args` when both are present —
/// same precedence the CLI's pre-PR-2c-7 inline implementation
/// applied.
///
/// `parent_vars`, `args`, and `input` correspond directly to the
/// caller's `context.variables`, `Tool::Playbook.args`, and
/// `Tool::Playbook.input` fields.  The `render` closure receives
/// each template string and is expected to return the rendered
/// value (the CLI passes `|t| self.render_template(t, context)`).
///
/// Returning a fresh `HashMap` rather than mutating in place makes
/// the helper easy to test and matches how the inline
/// implementation operated.
pub fn prepare_sub_playbook_vars<F>(
    parent_vars: &HashMap<String, String>,
    args: &HashMap<String, String>,
    input: &HashMap<String, serde_yaml::Value>,
    mut render: F,
) -> Result<HashMap<String, String>>
where
    F: FnMut(&str) -> Result<String>,
{
    let mut sub_vars = parent_vars.clone();

    if !input.is_empty() {
        // DSL v2: tool.input takes precedence — render and prefix
        // with `workload.`.
        for (key, value_yaml) in input {
            let template = match value_yaml {
                serde_yaml::Value::String(s) => s.clone(),
                serde_yaml::Value::Number(n) => n.to_string(),
                serde_yaml::Value::Bool(b) => b.to_string(),
                other => serde_yaml::to_string(other)?.trim().to_string(),
            };
            let value = render(&template)?;
            sub_vars.insert(format!("workload.{}", key), value);
        }
    } else if !args.is_empty() {
        // DSL v1 legacy: args field — prefix with `workload.`.
        for (key, template) in args {
            let value = render(template)?;
            sub_vars.insert(format!("workload.{}", key), value);
        }
    }

    Ok(sub_vars)
}

/// Apply post-resolution `Tool::Auth` side-effects to the CLI's
/// execution context.
///
/// Returns the (key, value) pairs the caller should
/// `set_variable` on its `ExecutionContext` so subsequent steps
/// can reference `{{ auth.token }}` etc.  Wrapping this in a
/// helper means future call sites (the worker, integration tests)
/// don't have to re-derive which keys to set.
///
/// `project` is the **already-rendered** project string (the CLI
/// renders templates against its own context before calling this
/// helper), or `None` if the playbook didn't supply one.
///
/// Output order:
///  - `auth.project` (only if `project` is `Some` and non-empty)
///  - `auth.token`
///  - `auth.provider`
///
/// Matching the CLI's pre-PR-2c-8 ordering — `auth.project` set
/// first by the inline arm, then the token + provider after the
/// `resolve_auth_to_bearer` call.
pub fn auth_context_updates(
    provider: &str,
    token: &str,
    project: Option<&str>,
) -> Vec<(String, String)> {
    let mut updates: Vec<(String, String)> = Vec::with_capacity(3);
    if let Some(p) = project {
        if !p.is_empty() {
            updates.push(("auth.project".to_string(), p.to_string()));
        }
    }
    updates.push(("auth.token".to_string(), token.to_string()));
    updates.push(("auth.provider".to_string(), provider.to_string()));
    updates
}

/// Format the payload a `Tool::Sink` writes to its target.
///
/// Pure transformation lifted from the CLI's inline
/// `Tool::Sink` arm.  The CLI passes the last step's result
/// (already a JSON-serialized string in `ExecutionContext`) and
/// the playbook's declared `format:` field; the helper returns
/// the formatted string ready to write to file / DuckDB / GCS.
///
/// Format rules:
/// - [`SinkFormat::Json`]: pass-through.  Same as CLI's
///   pre-PR-2c-8 behaviour (the raw step-result string).
/// - [`SinkFormat::Yaml`]: parse the input as JSON, then dump as
///   YAML.  Falls back to pass-through if the input doesn't parse.
/// - [`SinkFormat::Csv`]: see [`json_to_csv`] for the rules.
pub fn format_sink_payload(format: &SinkFormat, raw: &str) -> Result<String> {
    match format {
        SinkFormat::Json => Ok(raw.to_string()),
        SinkFormat::Yaml => {
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(raw) {
                Ok(serde_yaml::to_string(&json_val).unwrap_or_else(|_| raw.to_string()))
            } else {
                Ok(raw.to_string())
            }
        }
        SinkFormat::Csv => json_to_csv(raw),
    }
}

/// Convert a JSON-array-of-objects string into CSV.
///
/// Pure helper lifted from the CLI's inline `json_to_csv`.  Returns
/// the input unchanged if:
/// - it doesn't parse as JSON,
/// - it parses as a non-array value, or
/// - it's an empty array, or
/// - the first element isn't a JSON object.
///
/// Otherwise: emits a header row from the first object's keys
/// followed by one row per array element.  Values are converted
/// via `Display`; strings that contain `,` or `"` are
/// double-quoted with embedded `"` doubled — minimal RFC 4180
/// quoting, matching the CLI's pre-PR-2c-8 implementation.
pub fn json_to_csv(json_str: &str) -> Result<String> {
    let value: serde_json::Value =
        serde_json::from_str(json_str).unwrap_or(serde_json::Value::String(json_str.to_string()));

    match value {
        serde_json::Value::Array(arr) if !arr.is_empty() => {
            let headers: Vec<String> = if let Some(serde_json::Value::Object(obj)) = arr.first() {
                obj.keys().cloned().collect()
            } else {
                return Ok(json_str.to_string());
            };

            let mut csv = headers.join(",") + "\n";

            for item in &arr {
                if let serde_json::Value::Object(obj) = item {
                    let row: Vec<String> = headers
                        .iter()
                        .map(|h| {
                            obj.get(h)
                                .map(|v| match v {
                                    serde_json::Value::String(s) => {
                                        if s.contains(',') || s.contains('"') {
                                            format!("\"{}\"", s.replace('"', "\"\""))
                                        } else {
                                            s.clone()
                                        }
                                    }
                                    _ => v.to_string(),
                                })
                                .unwrap_or_default()
                        })
                        .collect();
                    csv.push_str(&row.join(","));
                    csv.push('\n');
                }
            }
            Ok(csv)
        }
        _ => Ok(json_str.to_string()),
    }
}

// ---------------------------------------------------------------------------
// GCS upload helper (R-3, noetl/ai-meta#31)
// ---------------------------------------------------------------------------

/// Upload `data` to `gs://<bucket>/<key>` using the `object_store` crate.
///
/// # Credential chain
///
/// Authentication defaults to the same Application Default Credentials
/// (ADC) / workload-identity chain that [`resolve_auth_to_bearer`] uses
/// via `gcp_auth`.  Concretely: `GoogleCloudStorageBuilder::from_env()`
/// reads (in priority order):
///
/// 1. `GOOGLE_SERVICE_ACCOUNT_KEY` env var (JSON service-account key
///    inline — useful for CI / test containers).
/// 2. `GOOGLE_SERVICE_ACCOUNT` env var (path to a JSON key file).
/// 3. The ambient Application Default Credentials
///    (`~/.config/gcloud/application_default_credentials.json` on dev
///    hosts; the GKE metadata server on cluster pods).
///
/// This matches GKE workload-identity on cluster and `gcloud auth
/// application-default login` on dev hosts — the same two paths the
/// former `gsutil cp` subprocess relied on.
///
/// # Error shape
///
/// Returns `anyhow::Error` with a human-readable message on failure
/// (instead of a gsutil exit-code string).  The CLI's `sink_to_gcs`
/// wrapper maps this through the usual `?` chain.
///
/// # Observability
///
/// Wraps the upload in a `gcs.upload` tracing span that carries
/// `bucket`, `key`, and `bytes` fields so the span is grep-able in
/// structured logs.  Upload duration is emitted as a debug-level event
/// (`gcs.upload.duration_ms`) so tooling can aggregate latency without
/// a Prometheus registry in the executor crate.  A future PR can
/// promote this to a proper histogram once the executor crate grows a
/// metrics registry.
///
/// # Pluggable store (testing)
///
/// The `store` parameter is `Arc<dyn ObjectStore>`.  Production callers
/// pass `None` (the default GCS store is built from env); integration
/// tests inject `Arc<object_store::memory::InMemory::new()>` to avoid
/// real GCS calls.  See `gcs_upload_with_store` for the inner
/// implementation that both paths share.
pub async fn gcs_upload(bucket: &str, key: &str, data: &str) -> Result<()> {
    use object_store::gcp::GoogleCloudStorageBuilder;

    let store = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build GCS store for bucket {:?}: {}", bucket, e))?;

    gcs_upload_with_store(Arc::new(store), key, data).await
}

/// Inner upload path shared by production and test callers.
///
/// Production: called by [`gcs_upload`] with a real
/// `GoogleCloudStorage` store.
/// Tests: called directly with `Arc<InMemory>` — no GCS dependency.
pub async fn gcs_upload_with_store(
    store: Arc<dyn ObjectStore>,
    key: &str,
    data: &str,
) -> Result<()> {
    let bytes = Bytes::from(data.to_string());
    let byte_len = bytes.len();
    let path = StorePath::from(key);

    let span = info_span!(
        "gcs.upload",
        key = key,
        bytes = byte_len,
    );

    async move {
        let start = Instant::now();

        store
            .put(&path, PutPayload::from_bytes(bytes))
            .await
            .map_err(|e| anyhow::anyhow!("GCS upload failed for key {:?}: {}", key, e))?;

        let elapsed_ms = start.elapsed().as_millis();
        tracing::debug!(
            target: "noetl::gcs",
            duration_ms = elapsed_ms,
            key = key,
            bytes = byte_len,
            "gcs.upload complete"
        );

        Ok(())
    }
    .instrument(span)
    .await
}

fn target_to_value(target: &crate::playbook::SinkTarget) -> serde_json::Value {
    match target {
        crate::playbook::SinkTarget::File { path } => {
            serde_json::json!({"type": "file", "path": path})
        }
        crate::playbook::SinkTarget::DuckDb { db, table } => {
            serde_json::json!({"type": "duckdb", "db": db, "table": table})
        }
        crate::playbook::SinkTarget::Gcs { bucket, path } => {
            serde_json::json!({"type": "gcs", "bucket": bucket, "path": path})
        }
    }
}

/// Convert a [`ToolResult`] back into the bridge outcome shape the
/// CLI consumes.  Success results carry `data` (or `stdout` if no
/// `data` was populated) as the result string; failures bubble up
/// as `anyhow::Error` so the CLI's existing error-handling chain
/// continues to work.
pub fn from_tools_result(result: ToolResult) -> Result<BridgeOutcome> {
    match result.status {
        ToolStatus::Success => {
            let payload = result
                .data
                .map(|v| match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                })
                .or(result.stdout);
            Ok(BridgeOutcome { result: payload })
        }
        ToolStatus::Error => Err(anyhow::anyhow!(
            "tool execution failed: {}",
            result.error.unwrap_or_else(|| "unknown error".to_string())
        )),
        ToolStatus::Timeout => Err(anyhow::anyhow!(
            "tool execution timed out after {} ms",
            result.duration_ms.unwrap_or(0)
        )),
    }
}

// ---------------------------------------------------------------------------
// Dispatch — per-tool-kind match scaffold.
// ---------------------------------------------------------------------------

/// Bridge dispatch entry point.  Each tool kind is replaced
/// incrementally in subsequent sub-PRs (PR-2c-3 onwards).
///
/// The function is async because every concrete `noetl-tools` tool
/// implementation is async (`Tool::execute` is `async`).  The CLI
/// adapts via `tokio::runtime::Handle::current().block_on(...)` if
/// the call site is sync — see PR-2c-3's wiring for the pattern.
pub async fn dispatch_via_registry(
    tool: &Tool,
    bridge: &BridgeContext<'_>,
) -> Result<BridgeOutcome> {
    let _config = to_tools_config(tool);
    let _ctx = to_tools_context(bridge);

    match tool {
        Tool::Rhai { .. } => {
            // PR-2c-3: first real tool replacement.  Builds a
            // RhaiTool from noetl-tools, dispatches against the
            // adapter-converted config + context, and converts the
            // result back through `from_tools_result`.
            //
            // Semantic note documented in the PR body: noetl-tools'
            // `timestamp()` returns the Unix epoch as a string
            // (e.g. "1716847425"), whereas the CLI's inline
            // implementation returned `chrono::Local::now()
            // .format("%H:%M:%S")` (e.g. "14:23:45").  Other
            // helpers (log, print, parse_json, contains, http_*,
            // get_gcp_token, sleep, sleep_ms) match.
            let rhai_tool = RhaiTool::new();
            let config = to_tools_config(tool);
            // rhai needs a nested variable shape so
            // `workload.region` is a Rhai field-access expression.
            let ctx = to_tools_context_for_rhai(bridge);
            let result = rhai_tool
                .execute(&config, &ctx)
                .await
                .map_err(|e| anyhow::anyhow!("rhai dispatch failed: {}", e))?;
            from_tools_result(result)
        }
        Tool::Shell { cmds } => {
            // PR-2c-4: dispatch through noetl_tools::ShellTool.
            //
            // CLI semantics preserved:
            // - CmdsList::Single splits on newlines into individual
            //   commands; each runs in its own bash invocation.
            // - CmdsList::Multiple runs each element in its own
            //   bash invocation in order.
            // - Bails on first non-zero exit (CLI's existing
            //   `anyhow::bail!("Command failed ...")`).
            // - Returns the last command's stdout as the step result.
            //
            // Note vs CLI: noetl-tools' ShellTool collects stdout +
            // stderr and returns them in the ToolResult at the end
            // of execution.  The CLI's inline implementation
            // streamed output to the terminal line-by-line as the
            // command ran.  For long-running shell steps users no
            // longer see real-time output.  Documented in the PR
            // body and on the executor-crate-architecture wiki
            // page's semantic-divergence table.
            let commands: Vec<String> = match cmds {
                CmdsList::Single(cmd) => cmd
                    .lines()
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect(),
                CmdsList::Multiple(c) => c.clone(),
            };

            let shell_tool = ShellTool::new();
            let ctx = to_tools_context(bridge);
            let mut last_outcome = BridgeOutcome::empty();
            for command in commands {
                let config = shell_command_config(&command);
                let result = shell_tool
                    .execute(&config, &ctx)
                    .await
                    .map_err(|e| anyhow::anyhow!("shell dispatch failed: {}", e))?;

                // noetl-tools' shell tool packs the result into
                // ToolResult.data as a typed JSON object:
                //   {"exit_code": i32, "stdout": String, "stderr": String}
                // For the CLI's step-result contract (a single
                // string = the command's stdout), we unwrap stdout
                // directly here.  `from_tools_result` would
                // otherwise stringify the whole JSON dict.
                if result.status != ToolStatus::Success {
                    let exit_code = result
                        .data
                        .as_ref()
                        .and_then(|d| d.get("exit_code"))
                        .and_then(|v| v.as_i64());
                    anyhow::bail!(
                        "Command failed with exit code: {:?}",
                        exit_code
                    );
                }
                let stdout = result
                    .data
                    .as_ref()
                    .and_then(|d| d.get("stdout"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim_end_matches('\n').to_string());
                last_outcome = BridgeOutcome { result: stdout };
            }
            Ok(last_outcome)
        }
        Tool::Http {
            method,
            url,
            headers,
            params,
            body,
            auth,
        } => {
            // PR-2c-5: dispatch through noetl_tools::HttpTool.
            //
            // CLI semantics preserved:
            // - Auth resolution via GCP ADC (gcp / google / adc).
            // - Step result is the JSON envelope
            //     `{"status": <int>, "body": <json-or-string>}`
            //   regardless of HTTP status code (so playbook steps
            //   can branch on `<step>.body.status`).
            //
            // Semantic divergences (documented on the executor-crate-
            // architecture wiki page):
            // - HTTP transport: curl subprocess → reqwest direct.
            // - GCP token: `gcloud auth print-access-token` shellout
            //   → `gcp_auth` crate (workload-identity aware on GKE).
            // - Body bytes: CLI sent the body string verbatim via
            //   `curl -d`.  noetl-tools serializes the body as JSON
            //   when the string parses as JSON (adding Content-Type:
            //   application/json automatically), otherwise sends it
            //   verbatim.  See `http_body_value`.
            let bearer = if let Some(auth_cfg) = auth {
                Some(resolve_auth_to_bearer(auth_cfg).await?)
            } else {
                None
            };
            let config = http_tool_config(
                method,
                url,
                headers,
                params,
                body.as_deref(),
                bearer.as_deref(),
            );
            let http_tool = HttpTool::new();
            let ctx = to_tools_context(bridge);
            let result = http_tool
                .execute(&config, &ctx)
                .await
                .map_err(|e| anyhow::anyhow!("http dispatch failed: {}", e))?;
            reshape_http_result(result)
        }
        Tool::DuckDb { db, query, params } => {
            // PR-2c-6: dispatch through noetl_tools::DuckdbTool.
            //
            // CLI semantics preserved:
            // - The CLI's call site already resolved playbook-
            //   relative paths (`resolve_duckdb_path`) and ran
            //   `mkdir -p` on the parent directory before invoking
            //   the bridge, so `db` here is an absolute path
            //   string ready to hand to DuckDB.
            // - SELECT / WITH queries return a JSON array of
            //   objects (pretty-printed).
            // - Non-SELECT queries return the literal envelope
            //   `{"status": "ok"}` (CLI never exposed
            //   noetl-tools' `affected_rows`).
            // - Empty / missing query short-circuits to an empty
            //   outcome, matching the CLI arm's
            //   `if let Some(query_str) = query` guard.
            //
            // Feature gain: CLI's pre-PR-2c-6 inline impl took a
            // `_params: &[String]` and silently ignored it.  The
            // bridge now binds those params as JSON values at
            // `?` placeholders.  Playbooks that had a stale
            // `params:` list under a query without `?` placeholders
            // continue to work (DuckDB ignores extra params); any
            // playbook that *intended* the params would now see
            // them applied — documented in the PR body.
            let query = match query {
                Some(q) if !q.trim().is_empty() => q,
                _ => return Ok(BridgeOutcome::empty()),
            };
            let config = duckdb_tool_config(db, query, params);
            let duckdb_tool = DuckdbTool::new();
            let ctx = to_tools_context(bridge);
            let result = duckdb_tool
                .execute(&config, &ctx)
                .await
                .map_err(|e| anyhow::anyhow!("duckdb dispatch failed: {}", e))?;
            reshape_duckdb_result(result)
        }
        Tool::Playbook { .. } => {
            // PR-2c-7: encodes the § H.10 architectural finding.
            //
            // `Tool::Playbook` is the recursion case of the CLI's
            // tree walker — it loads a sub-playbook YAML and
            // dispatches it through the same `PlaybookRunner` the
            // top-level invocation uses.  `PlaybookRunner` lives in
            // the CLI binary, not in `noetl-executor` or
            // `noetl-tools`, so routing this tool through the
            // bridge would require either:
            //   - dragging the tree walker into `noetl-executor`,
            //     re-opening the § H.10 question that re-scoped
            //     the crate to a utilities-and-types crate; or
            //   - adding a callback trait to `noetl-tools` that
            //     delegates back to the CLI binary, an
            //     infrastructure layer nothing else in the
            //     registry uses.
            //
            // The architecturally honest answer is that this tool
            // kind is NOT bridgeable.  The CLI's `Tool::Playbook`
            // arm stays inline by design.  Bailing loudly here
            // ensures any future code that tries to dispatch
            // `Tool::Playbook` through the bridge gets an
            // immediate, descriptive error instead of a silent
            // empty outcome.
            //
            // Sub-playbook variable preparation (the input + args
            // merging logic the CLI's call site performs before
            // recursing) DOES move into the executor as
            // [`prepare_sub_playbook_vars`] — that part is reusable
            // and testable independent of the tree walker.
            anyhow::bail!(
                "Tool::Playbook is not bridgeable: sub-playbook \
                 execution stays in the CLI's tree walker per \
                 § H.10 of the Rust migration roadmap. Use \
                 `PlaybookRunner::new(path).run()` directly from \
                 the CLI."
            );
        }
        Tool::Auth { .. } => {
            // PR-2c-8: `Tool::Auth` does not dispatch through the
            // registry.  Token resolution lives in
            // [`resolve_auth_to_bearer`] (added in PR-2c-5);
            // applying the resulting token to the CLI's
            // `ExecutionContext` lives in [`auth_context_updates`]
            // (added in PR-2c-8).  Both are sync helpers the CLI
            // calls directly without going through dispatch.  The
            // arm bails so any future code path that tries to
            // route a `Tool::Auth` through the registry gets a
            // clear, descriptive error instead of silently
            // returning an empty outcome.
            anyhow::bail!(
                "Tool::Auth is not bridge-dispatched: use \
                 `resolve_auth_to_bearer` for token resolution and \
                 `auth_context_updates` for applying the token to \
                 the caller's execution context. See § H.10 of the \
                 Rust migration roadmap."
            );
        }
        Tool::Sink { .. } => {
            // PR-2c-8: `Tool::Sink` does not dispatch through the
            // registry either.  noetl-tools' `TransferTool` is
            // database-to-database only (snowflake / postgres /
            // duckdb / http source → snowflake / postgres /
            // duckdb target); it has no file / GCS / object-store
            // target.  The CLI's three sink targets (File,
            // DuckDb, Gcs) each stay inline:
            //
            // - **File**: `fs::write` is a one-liner; the format
            //   conversion (json / yaml / csv) DID extract into
            //   [`format_sink_payload`] so it's reusable and
            //   testable.
            // - **DuckDb**: complex `INSERT INTO ... SELECT FROM
            //   read_json_auto(...)` with a single-object fallback;
            //   no `noetl-tools` equivalent.  Stays inline by
            //   design (§ H.10-style finding).
            // - **Gcs**: gsutil shellout.  A follow-up sub-PR
            //   (tracked separately) will migrate this to the
            //   `object_store` crate per § H.4 of Appendix H.
            //
            // The arm bails so misuse is loud.
            anyhow::bail!(
                "Tool::Sink is not bridge-dispatched: noetl-tools \
                 has no file / GCS / object-store target. Use \
                 `format_sink_payload` for format conversion; the \
                 CLI's sink targets (file / duckdb / gcs) stay \
                 inline per § H.10. GCS migration to `object_store` \
                 is tracked as a separate follow-up."
            );
        }
        Tool::Provider { auth, .. } => {
            // Dispatch through noetl-tools' ProviderTool — the SAME tool the
            // distributed worker runs.  Local mode therefore executes the cloud
            // provider action grammar, plan/apply, and LRO polling identically;
            // there is no CLI-side reimplementation.
            //
            // Dry-run (the default, and the credential-free plan path) needs no
            // auth.  For an apply-mode call the provider's `auth:` block is
            // mapped into the noetl-tools AuthConfig here; the ProviderTool
            // refuses an apply with no auth (no ambient ADC fallback).
            let mut config = to_tools_config(tool);
            config.auth = match auth {
                Some(a) => Some(provider_auth_to_tools(a)?),
                None => None,
            };
            let ctx = to_tools_context(bridge);
            let provider_tool = ProviderTool::new();
            let result = provider_tool
                .execute(&config, &ctx)
                .await
                .map_err(|e| anyhow::anyhow!("provider dispatch failed: {}", e))?;
            from_tools_result(result)
        }
        Tool::Unsupported => {
            anyhow::bail!("unsupported tool kind");
        }
    }
}

/// Convert a `serde_yaml::Value` to a `serde_json::Value` for the noetl-tools
/// config body.  Provider config sub-blocks (`input`, `poll`, `dry_run`) arrive
/// as YAML values from the CLI playbook parse; the tool consumes JSON.
fn yaml_to_json(v: &serde_yaml::Value) -> serde_json::Value {
    serde_json::to_value(v).unwrap_or(serde_json::Value::Null)
}

/// Map a provider `auth:` block (raw YAML) into the noetl-tools `AuthConfig`.
///
/// The provider auth shape (`type: gcp_adc`, `credential:`, `scopes:`) maps
/// directly onto the noetl-tools `AuthConfig` serde surface, so this is a
/// convert-through-JSON.  A malformed block is reported rather than silently
/// dropped — an apply-mode step with a broken `auth:` should fail loudly.
fn provider_auth_to_tools(auth: &serde_yaml::Value) -> Result<ToolsAuthConfig> {
    let json = yaml_to_json(auth);
    serde_json::from_value::<ToolsAuthConfig>(json).map_err(|e| {
        anyhow::anyhow!(
            "invalid provider `auth:` block (expected e.g. `type: gcp_adc`, \
             `credential: <alias>`, `scopes: [...]`): {}",
            e
        )
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::playbook::{AuthConfig as CliAuthConfig, SinkFormat, SinkTarget};

    fn empty_vars() -> HashMap<String, String> {
        HashMap::new()
    }

    fn bridge_ctx<'a>(vars: &'a HashMap<String, String>) -> BridgeContext<'a> {
        BridgeContext {
            execution_id: 12345,
            step: "test_step",
            variables: vars,
            server_url: String::new(),
            worker_id: None,
            command_id: None,
        }
    }

    #[test]
    fn to_tools_context_wraps_string_variables_as_json_value() {
        let vars: HashMap<String, String> =
            [("workload.region".into(), "us-west-1".into())].into();
        let ctx = to_tools_context(&bridge_ctx(&vars));
        assert_eq!(ctx.execution_id, 12345);
        assert_eq!(ctx.step, "test_step");
        assert_eq!(
            ctx.variables.get("workload.region"),
            Some(&serde_json::Value::String("us-west-1".into()))
        );
        assert!(ctx.secrets.is_empty(), "secrets stay empty by default");
    }

    #[test]
    fn to_tools_config_shell_single_cmd() {
        let tool = Tool::Shell {
            cmds: CmdsList::Single("ls -la".into()),
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "shell");
        assert_eq!(cfg.config["command"], "ls -la");
        assert_eq!(cfg.config["shell"], "bash");
        assert_eq!(cfg.config["capture"], true);
        assert!(cfg.timeout.is_none());
    }

    #[test]
    fn to_tools_config_provider_assembles_config_body() {
        let tool = Tool::Provider {
            provider: "google".into(),
            runtime: Some("rest".into()),
            action: "google.cloudresourcemanager.folders.list".into(),
            service: None,
            dry_run: Some(serde_yaml::Value::Bool(true)),
            input: Some(serde_yaml::to_value(serde_json::json!({
                "parent": "organizations/1"
            }))
            .unwrap()),
            poll: Some(serde_yaml::to_value(serde_json::json!({
                "max_attempts": 5
            }))
            .unwrap()),
            endpoint: None,
            stack: None,
            confirm: None,
            auth: None,
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "provider");
        assert_eq!(cfg.config["provider"], "google");
        assert_eq!(
            cfg.config["action"],
            "google.cloudresourcemanager.folders.list"
        );
        assert_eq!(cfg.config["runtime"], "rest");
        assert_eq!(cfg.config["dry_run"], true);
        assert_eq!(cfg.config["input"]["parent"], "organizations/1");
        assert_eq!(cfg.config["poll"]["max_attempts"], 5);
        // Absent optionals are omitted so the tool's serde defaults apply.
        assert!(cfg.config.get("service").is_none());
        // Auth is threaded through ToolConfig.auth by the dispatch arm, never
        // into the config body.
        assert!(cfg.config.get("auth").is_none());
    }

    #[test]
    fn provider_auth_to_tools_maps_gcp_adc_block() {
        let auth = serde_yaml::to_value(serde_json::json!({
            "type": "gcp_adc",
            "credential": "gcp_org_admin",
            "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        }))
        .unwrap();
        let mapped = provider_auth_to_tools(&auth).unwrap();
        assert_eq!(mapped.credential.as_deref(), Some("gcp_org_admin"));
        assert_eq!(
            mapped.scopes.as_deref(),
            Some(&["https://www.googleapis.com/auth/cloud-platform".to_string()][..])
        );

        // A malformed block is reported, not silently dropped.
        let bad = serde_yaml::to_value(serde_json::json!({ "type": "not_a_real_type" })).unwrap();
        assert!(provider_auth_to_tools(&bad).is_err());
    }

    #[tokio::test]
    async fn dispatch_provider_dry_run_returns_would_call_no_auth() {
        // Dry-run provider dispatch through the bridge: no auth needed, no
        // network, returns the plan the tool would send.
        let tool = Tool::Provider {
            provider: "google".into(),
            runtime: None,
            action: "google.cloudresourcemanager.folders.list".into(),
            service: None,
            dry_run: Some(serde_yaml::Value::Bool(true)),
            input: Some(
                serde_yaml::to_value(serde_json::json!({ "parent": "organizations/42" })).unwrap(),
            ),
            poll: None,
            endpoint: None,
            stack: None,
            confirm: None,
            auth: None,
        };
        let vars = empty_vars();
        let outcome = dispatch_via_registry(&tool, &bridge_ctx(&vars))
            .await
            .unwrap();
        let result = outcome.result.expect("provider dry-run returns a result");
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["dry_run"], serde_json::json!(true));
        assert_eq!(json["would_call"]["method"], serde_json::json!("GET"));
        assert_eq!(
            json["would_call"]["url"],
            serde_json::json!(
                "https://cloudresourcemanager.googleapis.com/v3/folders?parent=organizations/42"
            )
        );
    }

    #[tokio::test]
    async fn dispatch_provider_apply_without_auth_errors_no_network() {
        // Apply mode (dry_run=false) with no auth block → the tool refuses with
        // a Configuration error and makes no network call.  The bridge surfaces
        // it as a dispatch error rather than a silent empty outcome.
        let tool = Tool::Provider {
            provider: "google".into(),
            runtime: None,
            action: "google.serviceusage.services.enable".into(),
            service: None,
            dry_run: Some(serde_yaml::Value::Bool(false)),
            input: Some(
                serde_yaml::to_value(serde_json::json!({
                    "project_id": "p", "service_name": "youtube.googleapis.com"
                }))
                .unwrap(),
            ),
            poll: None,
            endpoint: None,
            stack: None,
            confirm: None,
            auth: None,
        };
        let vars = empty_vars();
        let err = dispatch_via_registry(&tool, &bridge_ctx(&vars))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("apply mode") || err.to_string().contains("auth"),
            "error names the missing auth: {err}"
        );
    }

    #[test]
    fn to_tools_config_shell_multiple_cmds_joins_with_newlines() {
        // The to_tools_config helper produces a SINGLE-command shape
        // by joining; the dispatch arm instead loops per command to
        // preserve the CLI's "fresh bash per command" semantics.
        let tool = Tool::Shell {
            cmds: CmdsList::Multiple(vec!["echo one".into(), "echo two".into()]),
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "shell");
        assert_eq!(cfg.config["command"], "echo one\necho two");
    }

    #[test]
    fn shell_command_config_emits_per_cmd_shape() {
        let cfg = shell_command_config("echo hi");
        assert_eq!(cfg.kind, "shell");
        assert_eq!(cfg.config["command"], "echo hi");
        assert_eq!(cfg.config["shell"], "bash");
        assert_eq!(cfg.config["capture"], true);
    }

    #[test]
    fn to_tools_config_http_round_trips_essentials() {
        let tool = Tool::Http {
            method: "post".into(), // lowercase to verify uppercasing
            url: "https://example.com/api".into(),
            headers: HashMap::new(),
            params: HashMap::new(),
            body: Some(r#"{"k":"v"}"#.into()),
            auth: None,
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "http");
        // noetl-tools' HttpConfig.method deserializes via
        // #[serde(rename_all = "UPPERCASE")] so the bridge always
        // uppercases the CLI's method string.
        assert_eq!(cfg.config["method"], "POST");
        assert_eq!(cfg.config["url"], "https://example.com/api");
        // JSON bodies are parsed into a JSON Value so reqwest
        // serialises them with Content-Type: application/json.
        assert_eq!(cfg.config["body"], serde_json::json!({"k": "v"}));
    }

    #[test]
    fn to_tools_config_http_keeps_non_json_body_as_string() {
        let tool = Tool::Http {
            method: "POST".into(),
            url: "https://example.com".into(),
            headers: HashMap::new(),
            params: HashMap::new(),
            body: Some("not json at all".into()),
            auth: None,
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.config["body"], "not json at all");
    }

    #[test]
    fn http_body_value_parses_json_strings() {
        let v = http_body_value(r#"{"a":1}"#);
        assert_eq!(v, serde_json::json!({"a": 1}));
    }

    #[test]
    fn http_body_value_falls_back_to_string() {
        let v = http_body_value("plain text body");
        assert_eq!(v, serde_json::Value::String("plain text body".into()));
    }

    #[test]
    fn http_tool_config_injects_bearer_header() {
        let cfg = http_tool_config(
            "GET",
            "https://example.com",
            &HashMap::new(),
            &HashMap::new(),
            None,
            Some("test-token-123"),
        );
        assert_eq!(cfg.kind, "http");
        assert_eq!(
            cfg.config["headers"]["Authorization"],
            "Bearer test-token-123"
        );
    }

    #[test]
    fn http_tool_config_preserves_caller_headers_with_bearer() {
        let mut hdrs = HashMap::new();
        hdrs.insert("X-Trace-Id".into(), "abc123".into());
        let cfg = http_tool_config(
            "POST",
            "https://example.com",
            &hdrs,
            &HashMap::new(),
            None,
            Some("token"),
        );
        assert_eq!(cfg.config["headers"]["X-Trace-Id"], "abc123");
        assert_eq!(cfg.config["headers"]["Authorization"], "Bearer token");
    }

    #[test]
    fn http_tool_config_no_auth_omits_authorization_header() {
        let cfg = http_tool_config(
            "GET",
            "https://example.com",
            &HashMap::new(),
            &HashMap::new(),
            None,
            None,
        );
        let hdrs = cfg.config["headers"].as_object().unwrap();
        assert!(!hdrs.contains_key("Authorization"));
    }

    #[test]
    fn reshape_http_result_extracts_envelope() {
        let mut result = ToolResult::success(serde_json::json!({
            "status_code": 200,
            "headers": {},
            "body": {"ok": true},
        }));
        result.exit_code = Some(0);
        let outcome = reshape_http_result(result).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
        assert_eq!(parsed["status"], 200);
        assert_eq!(parsed["body"], serde_json::json!({"ok": true}));
    }

    #[test]
    fn reshape_http_result_preserves_4xx_envelope_without_erroring() {
        // CLI contract: HTTP error statuses come back inside the
        // `{status, body}` envelope, NOT as anyhow::Error.  Only
        // network-transport failures bubble up.
        let mut result = ToolResult {
            status: ToolStatus::Error,
            data: Some(serde_json::json!({
                "status_code": 404,
                "headers": {},
                "body": {"error": "not found"},
            })),
            error: Some("HTTP 404 response".into()),
            stdout: None,
            stderr: None,
            exit_code: Some(1),
            duration_ms: Some(5),
            pending_callback: None,
        };
        result.exit_code = Some(1);
        let outcome = reshape_http_result(result).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
        assert_eq!(parsed["status"], 404);
        assert_eq!(parsed["body"], serde_json::json!({"error": "not found"}));
    }

    #[tokio::test]
    async fn resolve_auth_to_bearer_rejects_unknown_provider() {
        let cfg = CliAuthConfig {
            provider: "azure".into(),
            scopes: vec![],
        };
        let err = resolve_auth_to_bearer(&cfg).await.unwrap_err();
        assert!(err.to_string().contains("unsupported auth provider"));
    }

    // ---- PR-2c-6 — Tool::DuckDb bridge integration -------------------

    #[test]
    fn duckdb_tool_config_emits_noetl_tools_schema() {
        let cfg = duckdb_tool_config(
            ":memory:",
            "SELECT 1",
            &["arg1".to_string()],
        );
        assert_eq!(cfg.kind, "duckdb");
        assert_eq!(cfg.config["db_path"], ":memory:");
        assert_eq!(cfg.config["query"], "SELECT 1");
        assert_eq!(cfg.config["as_objects"], true);
        assert_eq!(
            cfg.config["params"],
            serde_json::json!([serde_json::Value::String("arg1".into())])
        );
    }

    #[test]
    fn to_tools_config_duckdb_carries_path_and_query() {
        let tool = Tool::DuckDb {
            db: "warehouse.db".into(),
            query: Some("SELECT count(*) FROM orders".into()),
            params: vec![],
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "duckdb");
        assert_eq!(cfg.config["db_path"], "warehouse.db");
        assert_eq!(cfg.config["query"], "SELECT count(*) FROM orders");
        assert_eq!(cfg.config["as_objects"], true);
    }

    #[test]
    fn to_tools_config_duckdb_missing_query_becomes_empty_string() {
        let tool = Tool::DuckDb {
            db: ":memory:".into(),
            query: None,
            params: vec![],
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.config["query"], "");
    }

    #[test]
    fn reshape_duckdb_result_select_returns_rows_array() {
        let result = ToolResult::success(serde_json::json!({
            "columns": ["id", "name"],
            "rows": [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
            ],
            "row_count": 2
        }));
        let outcome = reshape_duckdb_result(result).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
        let arr = parsed.as_array().expect("result is an array");
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["name"], "alice");
        assert_eq!(arr[1]["name"], "bob");
    }

    #[test]
    fn reshape_duckdb_result_select_empty_returns_empty_array() {
        let result = ToolResult::success(serde_json::json!({
            "columns": ["id"],
            "rows": [],
            "row_count": 0
        }));
        let outcome = reshape_duckdb_result(result).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
        assert_eq!(parsed.as_array().unwrap().len(), 0);
    }

    #[test]
    fn reshape_duckdb_result_non_select_returns_status_envelope() {
        let result = ToolResult::success(serde_json::json!({
            "affected_rows": 3
        }));
        let outcome = reshape_duckdb_result(result).unwrap();
        // CLI returned the literal `{"status": "ok"}` string for
        // non-SELECT queries; `affected_rows` is intentionally
        // dropped (CLI never exposed it, so playbooks can't depend
        // on it).
        assert_eq!(outcome.result.as_deref(), Some(r#"{"status": "ok"}"#));
    }

    // Requires the real DuckDB engine — noetl-tools >= 3.20.0 gates it behind
    // `duckdb-integration`, so without the feature this dispatch hits the stub.
    // The cli enables the feature by default; run
    // `cargo test -p noetl-executor --features duckdb-integration` for this test.
    #[cfg(feature = "duckdb-integration")]
    #[tokio::test]
    async fn dispatch_duckdb_select_returns_rows_array() {
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::DuckDb {
            db: ":memory:".into(),
            query: Some("SELECT 1 AS num, 'hello' AS msg".into()),
            params: vec![],
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
        let arr = parsed.as_array().expect("result is an array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["num"], 1);
        assert_eq!(arr[0]["msg"], "hello");
    }

    #[tokio::test]
    async fn dispatch_duckdb_missing_query_returns_empty_outcome() {
        // Mirrors the CLI arm's `if let Some(query_str) = query` guard:
        // a Tool::DuckDb with no query falls through to None.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::DuckDb {
            db: ":memory:".into(),
            query: None,
            params: vec![],
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert!(outcome.result.is_none());
    }

    #[tokio::test]
    async fn dispatch_duckdb_empty_query_returns_empty_outcome() {
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::DuckDb {
            db: ":memory:".into(),
            query: Some("   ".into()), // whitespace only
            params: vec![],
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert!(outcome.result.is_none());
    }

    // ---- PR-2c-7 — sub-playbook variable preparation ------------------

    #[test]
    fn prepare_sub_playbook_vars_passes_parent_vars_through() {
        let parent: HashMap<String, String> =
            [("vars.timeout".into(), "30".into())].into();
        let sub = prepare_sub_playbook_vars(
            &parent,
            &HashMap::new(),
            &HashMap::new(),
            |t| Ok(t.to_string()),
        )
        .unwrap();
        assert_eq!(sub.get("vars.timeout"), Some(&"30".to_string()));
    }

    #[test]
    fn prepare_sub_playbook_vars_v2_input_takes_precedence_over_v1_args() {
        let parent: HashMap<String, String> = HashMap::new();
        let mut input = HashMap::new();
        input.insert(
            "region".into(),
            serde_yaml::Value::String("us-east-1".into()),
        );
        let mut args = HashMap::new();
        args.insert("region".into(), "us-west-1".into());

        let sub = prepare_sub_playbook_vars(&parent, &args, &input, |t| {
            Ok(t.to_string())
        })
        .unwrap();
        // input wins; args ignored when input is non-empty.
        assert_eq!(sub.get("workload.region"), Some(&"us-east-1".to_string()));
    }

    #[test]
    fn prepare_sub_playbook_vars_v1_args_used_when_input_empty() {
        let parent: HashMap<String, String> = HashMap::new();
        let mut args = HashMap::new();
        args.insert("tier".into(), "prod".into());
        let sub = prepare_sub_playbook_vars(
            &parent,
            &args,
            &HashMap::new(),
            |t| Ok(t.to_string()),
        )
        .unwrap();
        assert_eq!(sub.get("workload.tier"), Some(&"prod".to_string()));
    }

    #[test]
    fn prepare_sub_playbook_vars_renders_input_templates() {
        let parent: HashMap<String, String> = HashMap::new();
        let mut input = HashMap::new();
        input.insert(
            "url".into(),
            serde_yaml::Value::String("{{base}}/api".into()),
        );
        let sub = prepare_sub_playbook_vars(
            &parent,
            &HashMap::new(),
            &input,
            |t| Ok(t.replace("{{base}}", "https://example.com")),
        )
        .unwrap();
        assert_eq!(
            sub.get("workload.url"),
            Some(&"https://example.com/api".to_string())
        );
    }

    #[test]
    fn prepare_sub_playbook_vars_coerces_yaml_numbers_and_bools() {
        let parent: HashMap<String, String> = HashMap::new();
        let mut input = HashMap::new();
        input.insert(
            "timeout".into(),
            serde_yaml::Value::Number(serde_yaml::Number::from(30)),
        );
        input.insert("verbose".into(), serde_yaml::Value::Bool(true));
        let sub = prepare_sub_playbook_vars(
            &parent,
            &HashMap::new(),
            &input,
            |t| Ok(t.to_string()),
        )
        .unwrap();
        assert_eq!(sub.get("workload.timeout"), Some(&"30".to_string()));
        assert_eq!(sub.get("workload.verbose"), Some(&"true".to_string()));
    }

    #[test]
    fn prepare_sub_playbook_vars_passes_through_when_both_empty() {
        let parent: HashMap<String, String> = [(
            "workload.region".into(),
            "us-east-1".into(),
        )]
        .into();
        let sub = prepare_sub_playbook_vars(
            &parent,
            &HashMap::new(),
            &HashMap::new(),
            |t| Ok(t.to_string()),
        )
        .unwrap();
        // No input or args; parent vars come through unchanged.
        assert_eq!(sub.len(), 1);
        assert_eq!(
            sub.get("workload.region"),
            Some(&"us-east-1".to_string())
        );
    }

    #[test]
    fn prepare_sub_playbook_vars_render_error_propagates() {
        let parent: HashMap<String, String> = HashMap::new();
        let mut input = HashMap::new();
        input.insert(
            "bad".into(),
            serde_yaml::Value::String("{{nope}}".into()),
        );
        let result = prepare_sub_playbook_vars(
            &parent,
            &HashMap::new(),
            &input,
            |_| Err(anyhow::anyhow!("render exploded")),
        );
        assert!(result.unwrap_err().to_string().contains("render exploded"));
    }

    // ---- PR-2c-8 — Tool::Auth context updates -------------------------

    #[test]
    fn auth_context_updates_includes_token_and_provider() {
        let updates = auth_context_updates("gcp", "tok-123", None);
        let map: HashMap<String, String> = updates.into_iter().collect();
        assert_eq!(map.get("auth.token"), Some(&"tok-123".to_string()));
        assert_eq!(map.get("auth.provider"), Some(&"gcp".to_string()));
        assert!(map.get("auth.project").is_none());
    }

    #[test]
    fn auth_context_updates_includes_project_when_set() {
        let updates = auth_context_updates("adc", "t", Some("my-project"));
        let map: HashMap<String, String> = updates.into_iter().collect();
        assert_eq!(
            map.get("auth.project"),
            Some(&"my-project".to_string())
        );
        assert_eq!(map.get("auth.token"), Some(&"t".to_string()));
        assert_eq!(map.get("auth.provider"), Some(&"adc".to_string()));
    }

    #[test]
    fn auth_context_updates_skips_empty_project() {
        let updates = auth_context_updates("gcp", "t", Some(""));
        let map: HashMap<String, String> = updates.into_iter().collect();
        assert!(map.get("auth.project").is_none());
    }

    #[test]
    fn auth_context_updates_orders_project_before_token() {
        // The CLI's pre-PR-2c-8 inline arm set `auth.project` first,
        // then the token + provider after the auth call.  Preserve
        // that ordering so observable side-effects (logs, traces)
        // match.
        let updates = auth_context_updates("gcp", "t", Some("p"));
        assert_eq!(updates[0].0, "auth.project");
        assert_eq!(updates[1].0, "auth.token");
        assert_eq!(updates[2].0, "auth.provider");
    }

    // ---- PR-2c-8 — Sink payload formatting + CSV ----------------------

    #[test]
    fn format_sink_payload_json_passthrough() {
        let raw = r#"{"k": "v"}"#;
        let out = format_sink_payload(&SinkFormat::Json, raw).unwrap();
        assert_eq!(out, raw);
    }

    #[test]
    fn format_sink_payload_yaml_converts_json_object() {
        let raw = r#"{"k": "v"}"#;
        let out = format_sink_payload(&SinkFormat::Yaml, raw).unwrap();
        let reparsed: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        assert_eq!(reparsed["k"].as_str(), Some("v"));
    }

    #[test]
    fn format_sink_payload_yaml_falls_back_when_not_json() {
        let raw = "not even close to json";
        let out = format_sink_payload(&SinkFormat::Yaml, raw).unwrap();
        assert_eq!(out, raw);
    }

    #[test]
    fn format_sink_payload_csv_uses_json_to_csv() {
        let raw = r#"[{"a":1,"b":2},{"a":3,"b":4}]"#;
        let out = format_sink_payload(&SinkFormat::Csv, raw).unwrap();
        assert!(out.contains("a,b\n") || out.contains("b,a\n"));
        // Two data rows + header.
        assert_eq!(out.lines().count(), 3);
    }

    #[test]
    fn json_to_csv_returns_input_for_non_array() {
        assert_eq!(json_to_csv("not json").unwrap(), "not json");
        assert_eq!(json_to_csv(r#"{"k":"v"}"#).unwrap(), r#"{"k":"v"}"#);
    }

    #[test]
    fn json_to_csv_returns_input_for_empty_array() {
        assert_eq!(json_to_csv("[]").unwrap(), "[]");
    }

    #[test]
    fn json_to_csv_emits_header_and_rows_for_object_array() {
        let raw = r#"[{"name":"alice","age":30},{"name":"bob","age":25}]"#;
        let csv = json_to_csv(raw).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(lines.len(), 3);
        // Header derived from first object's keys (order
        // preserved by serde_json::Map).
        assert!(lines[0] == "name,age" || lines[0] == "age,name");
        // Each subsequent line should contain both values.
        assert!(lines[1].contains("alice") && lines[1].contains("30"));
        assert!(lines[2].contains("bob") && lines[2].contains("25"));
    }

    #[test]
    fn json_to_csv_quotes_strings_with_commas() {
        let raw = r#"[{"label":"a, b","n":1}]"#;
        let csv = json_to_csv(raw).unwrap();
        // Quoted field with the comma preserved inside.
        assert!(csv.contains("\"a, b\""), "csv: {csv}");
    }

    #[test]
    fn json_to_csv_doubles_embedded_quotes() {
        let raw = r#"[{"q":"she said \"hi\""}]"#;
        let csv = json_to_csv(raw).unwrap();
        // RFC-4180-style: embedded `"` doubled, whole field quoted.
        assert!(csv.contains("\"she said \"\"hi\"\"\""), "csv: {csv}");
    }

    #[test]
    fn json_to_csv_missing_field_emits_empty() {
        let raw = r#"[{"a":1,"b":2},{"a":3}]"#; // second row missing `b`
        let csv = json_to_csv(raw).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        // The second data row should end with a trailing comma or
        // have an empty field for `b`.
        assert!(
            lines[2].ends_with(",") || lines[2].contains(",,"),
            "csv: {csv}"
        );
    }

    #[test]
    fn to_tools_config_rhai_carries_code() {
        let tool = Tool::Rhai {
            code: "let x = 1; x + 1".into(),
            args: HashMap::new(),
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "rhai");
        assert_eq!(cfg.config["code"], "let x = 1; x + 1");
    }

    #[test]
    fn to_tools_config_sink_emits_typed_target() {
        let tool = Tool::Sink {
            target: SinkTarget::File {
                path: "/tmp/out.json".into(),
            },
            format: SinkFormat::Json,
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "sink");
        assert_eq!(cfg.config["target"]["type"], "file");
        assert_eq!(cfg.config["target"]["path"], "/tmp/out.json");
        assert_eq!(cfg.config["format"], "json");
    }

    #[test]
    fn from_tools_result_success_returns_data_string() {
        let result = ToolResult::success(serde_json::Value::String("hello".into()));
        let outcome = from_tools_result(result).unwrap();
        assert_eq!(outcome.result, Some("hello".into()));
    }

    #[test]
    fn from_tools_result_success_serialises_non_string_data() {
        let result = ToolResult::success(serde_json::json!({"k": "v"}));
        let outcome = from_tools_result(result).unwrap();
        assert_eq!(outcome.result, Some(r#"{"k":"v"}"#.into()));
    }

    #[test]
    fn from_tools_result_success_falls_back_to_stdout() {
        let mut result = ToolResult::success(serde_json::Value::Null);
        result.data = None;
        result.stdout = Some("script output".into());
        let outcome = from_tools_result(result).unwrap();
        assert_eq!(outcome.result, Some("script output".into()));
    }

    #[test]
    fn from_tools_result_error_propagates_message() {
        let result = ToolResult::error("connection refused");
        let err = from_tools_result(result).unwrap_err();
        assert!(err.to_string().contains("connection refused"));
    }

    // PR-2c-8 removed the
    // `dispatch_via_registry_returns_empty_for_unwired_kind` test:
    // every Tool variant now either dispatches through the registry
    // (Rhai/Shell/Http/DuckDb), bails with a § H.10 finding
    // (Playbook/Auth/Sink), or bails as unsupported.  See the
    // per-variant dispatch tests for the wired kinds and the bail
    // tests for Playbook/Auth/Sink/Unsupported.

    #[tokio::test]
    async fn dispatch_auth_bails_pointing_at_helper() {
        // PR-2c-8: Tool::Auth has no bridge dispatch path.  The
        // bridge bails with a message pointing at
        // `resolve_auth_to_bearer` + `auth_context_updates` so
        // misuse is loud rather than silent.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Auth {
            provider: "adc".into(),
            scopes: vec![],
            project: None,
        };
        let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Tool::Auth")
                && msg.contains("resolve_auth_to_bearer")
                && msg.contains("auth_context_updates"),
            "error should point at the helpers: {msg}"
        );
    }

    #[tokio::test]
    async fn dispatch_sink_bails_pointing_at_helper() {
        // PR-2c-8: Tool::Sink has no bridge dispatch path either —
        // noetl-tools' TransferTool is database-to-database only.
        // The bridge bails with a message pointing at
        // `format_sink_payload` for format conversion.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Sink {
            target: crate::playbook::SinkTarget::File {
                path: "/tmp/out.json".into(),
            },
            format: SinkFormat::Json,
        };
        let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Tool::Sink") && msg.contains("format_sink_payload"),
            "error should point at the helper: {msg}"
        );
    }

    #[tokio::test]
    async fn dispatch_playbook_bails_with_h10_finding() {
        // PR-2c-7: `Tool::Playbook` is not bridgeable.  Make sure
        // the dispatch arm bails with a descriptive error rather
        // than silently returning an empty outcome.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Playbook {
            path: "sub.yaml".into(),
            args: HashMap::new(),
            input: HashMap::new(),
        };
        let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Tool::Playbook")
                && msg.contains("not bridgeable")
                && msg.contains("§ H.10"),
            "error message should explain the § H.10 finding: {msg}"
        );
    }

    // ---- PR-2c-4 — Tool::Shell bridge integration --------------------

    #[tokio::test]
    async fn dispatch_shell_single_command_returns_stdout() {
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Shell {
            cmds: CmdsList::Single("echo bridged".into()),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        // The bridge trims the trailing newline that `echo` adds so
        // the step result matches the CLI's pre-PR-2c-4 contract
        // (per-line stdout joined without trailing whitespace).
        assert_eq!(outcome.result, Some("bridged".into()));
    }

    #[tokio::test]
    async fn dispatch_shell_multiple_returns_last_command_stdout() {
        // CLI semantic: with CmdsList::Multiple, each command runs
        // in its own bash invocation; the step result is the last
        // command's stdout.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Shell {
            cmds: CmdsList::Multiple(vec![
                "echo first".into(),
                "echo second".into(),
                "echo third".into(),
            ]),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert_eq!(outcome.result, Some("third".into()));
    }

    #[tokio::test]
    async fn dispatch_shell_failure_propagates_error() {
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Shell {
            cmds: CmdsList::Single("exit 7".into()),
        };
        let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
        // noetl-tools' shell tool reports non-zero exit codes by
        // surfacing ToolResult.status == Error or by returning
        // result with exit_code set; either way the bridge's
        // from_tools_result converts that into an anyhow::Error.
        assert!(
            err.to_string().contains("shell")
                || err.to_string().contains("exit")
                || err.to_string().contains("failed"),
            "error message: {}",
            err
        );
    }

    #[tokio::test]
    async fn dispatch_shell_single_with_newlines_runs_each_line_independently() {
        // CLI semantic: CmdsList::Single splits on newlines into
        // separate bash invocations.  This means `cd /tmp` on one
        // line doesn't change the cwd of the next line.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Shell {
            cmds: CmdsList::Single("echo first_line\necho second_line".into()),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert_eq!(outcome.result, Some("second_line".into()));
    }

    #[tokio::test]
    async fn dispatch_via_registry_unsupported_errors() {
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Unsupported;
        let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
        assert!(err.to_string().contains("unsupported"));
    }

    // ---- PR-2c-3 — Tool::Rhai bridge integration ---------------------

    #[tokio::test]
    async fn dispatch_rhai_evaluates_simple_arithmetic() {
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Rhai {
            code: "let x = 40; let y = 2; (x + y).to_string()".into(),
            args: HashMap::new(),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert_eq!(outcome.result, Some("42".into()));
    }

    #[tokio::test]
    async fn dispatch_rhai_reads_workload_variable_via_scope() {
        // `to_tools_context_for_rhai` groups the CLI's flat
        // `workload.region` key into a nested `workload` Map.
        // Rhai's `workload.region` then resolves as field access.
        let vars: HashMap<String, String> =
            [("workload.region".into(), "us-west-1".into())].into();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Rhai {
            code: r#"workload.region.to_string()"#.into(),
            args: HashMap::new(),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert_eq!(outcome.result, Some("us-west-1".into()));
    }

    #[tokio::test]
    async fn dispatch_rhai_reads_step_result_via_field_access() {
        // Step results in the CLI surface as `<step>.result` keys.
        // The nested-shape adapter groups them under a step-named map.
        let vars: HashMap<String, String> = [
            ("check_health.result".into(), "ok".into()),
            ("check_health.status".into(), "200".into()),
        ]
        .into();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Rhai {
            code: r#"check_health.result.to_string()"#.into(),
            args: HashMap::new(),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert_eq!(outcome.result, Some("ok".into()));
    }

    #[test]
    fn to_tools_context_for_rhai_groups_workload_prefix() {
        let vars: HashMap<String, String> = [
            ("workload.region".into(), "us-west-1".into()),
            ("workload.tier".into(), "prod".into()),
            ("vars.timeout".into(), "30".into()),
            ("step_a.result".into(), "done".into()),
            ("toplevel".into(), "kept_at_root".into()),
        ]
        .into();
        let bridge = bridge_ctx(&vars);
        let ctx = to_tools_context_for_rhai(&bridge);

        let workload = ctx
            .variables
            .get("workload")
            .expect("workload group should exist")
            .as_object()
            .expect("workload should be an object");
        assert_eq!(workload.get("region"), Some(&serde_json::json!("us-west-1")));
        assert_eq!(workload.get("tier"), Some(&serde_json::json!("prod")));

        let vars_map = ctx.variables.get("vars").and_then(|v| v.as_object()).unwrap();
        assert_eq!(vars_map.get("timeout"), Some(&serde_json::json!("30")));

        let step_a = ctx.variables.get("step_a").and_then(|v| v.as_object()).unwrap();
        assert_eq!(step_a.get("result"), Some(&serde_json::json!("done")));

        assert_eq!(
            ctx.variables.get("toplevel"),
            Some(&serde_json::json!("kept_at_root"))
        );
    }

    #[tokio::test]
    async fn dispatch_rhai_string_literal_returns_unquoted() {
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Rhai {
            code: r#""hello world""#.into(),
            args: HashMap::new(),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        // noetl-tools' RhaiTool returns the result through ToolResult.data
        // as a JSON value; for string results that means a JSON-quoted
        // string.  from_tools_result strips the JSON quotes when data
        // is a Value::String.
        assert_eq!(outcome.result, Some("hello world".into()));
    }

    // ---- Compiler proof: AuthConfig from playbook is still constructable
    // even though we don't pass it through to the bridge yet.  Locks in
    // the field surface so PR-2c-5 / PR-2c-8 see a deliberate gap, not
    // a missing type.
    #[test]
    fn cli_auth_config_constructs() {
        let _auth = CliAuthConfig {
            provider: "adc".into(),
            scopes: vec!["https://www.googleapis.com/auth/cloud-platform".into()],
        };
    }

    // ---- gcs_upload helper (R-3, noetl/ai-meta#31) ------------------
    //
    // These tests exercise `gcs_upload_with_store` — the inner path
    // shared by production (real GCS) and test (InMemory) callers.
    // The `gcs_upload` function (which builds the real GCS store from
    // env) is NOT tested here — real GCS credentials are not available
    // in CI.  The call shape (bucket → builder → store → put) is the
    // same in both paths; the InMemory tests lock in the object_store
    // API surface and the helper's error-handling contract.

    #[tokio::test]
    async fn gcs_upload_with_store_writes_data_to_object_store() {
        // Verifies the happy path: data is uploaded and can be read
        // back from the same InMemory store — proving gcs_upload_with_store
        // calls ObjectStore::put with the correct path + payload.
        use object_store::memory::InMemory;
        use object_store::ObjectStore;

        let store = Arc::new(InMemory::new());
        gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, "output/data.json", r#"{"k":"v"}"#)
            .await
            .expect("upload should succeed");

        let path = StorePath::from("output/data.json");
        let retrieved = store.get(&path).await.expect("should read back uploaded object");
        let body = retrieved.bytes().await.expect("should get bytes");
        assert_eq!(body, bytes::Bytes::from(r#"{"k":"v"}"#));
    }

    #[tokio::test]
    async fn gcs_upload_with_store_overwrites_existing_key() {
        // Second upload to the same key must overwrite the first — the
        // InMemory store's put is idempotent on the key, which is the
        // same contract the real GCS object-level PUT provides.
        use object_store::memory::InMemory;
        use object_store::ObjectStore;

        let store = Arc::new(InMemory::new());
        gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, "data.csv", "first").await.unwrap();
        gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, "data.csv", "second").await.unwrap();

        let path = StorePath::from("data.csv");
        let body = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(body, bytes::Bytes::from("second"));
    }

    #[tokio::test]
    async fn gcs_upload_with_store_handles_nested_key_paths() {
        // GCS object keys can contain slashes (they are logical paths
        // within a bucket, not filesystem paths).  StorePath should
        // preserve the full slash-separated key.
        use object_store::memory::InMemory;
        use object_store::ObjectStore;

        let store = Arc::new(InMemory::new());
        gcs_upload_with_store(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            "runs/2026-06-01/output/result.json",
            "[]",
        )
        .await
        .unwrap();

        let path = StorePath::from("runs/2026-06-01/output/result.json");
        let body = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(body, bytes::Bytes::from("[]"));
    }

    #[tokio::test]
    async fn gcs_upload_with_store_uploads_empty_string() {
        // An empty payload is a valid GCS object — the helper must not
        // short-circuit or error on empty data.
        use object_store::memory::InMemory;
        use object_store::ObjectStore;

        let store = Arc::new(InMemory::new());
        gcs_upload_with_store(Arc::clone(&store) as Arc<dyn ObjectStore>, "empty.txt", "").await.unwrap();

        let path = StorePath::from("empty.txt");
        let body = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(body.len(), 0);
    }
}

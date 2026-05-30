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

#![allow(dead_code)] // until PR-2c-4 onwards wires the call sites in.

use std::collections::HashMap;

use anyhow::Result;
use noetl_tools::auth::GcpAuth;
use noetl_tools::context::ExecutionContext as ToolsExecutionContext;
use noetl_tools::registry::{Tool as ToolsRegistryTool, ToolConfig};
use noetl_tools::result::{ToolResult, ToolStatus};
use noetl_tools::tools::{HttpTool, RhaiTool, ShellTool};

use crate::playbook::{AuthConfig as CliAuthConfig, CmdsList, Tool};

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
            "duckdb",
            serde_json::json!({
                "db": db,
                "query": query,
                "params": params,
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
        Tool::DuckDb { .. } => {
            // PR-2c-6 fills this in.
            Ok(BridgeOutcome::empty())
        }
        Tool::Playbook { .. } => {
            // PR-2c-7 fills this in.
            Ok(BridgeOutcome::empty())
        }
        Tool::Auth { .. } | Tool::Sink { .. } => {
            // PR-2c-8 fills these in; both need new tool kinds in
            // noetl-tools (or specific bridge-side handling).
            Ok(BridgeOutcome::empty())
        }
        Tool::Unsupported => {
            anyhow::bail!("unsupported tool kind");
        }
    }
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

    #[tokio::test]
    async fn dispatch_via_registry_returns_empty_for_unwired_kind() {
        // PR-2c-3 wired `Tool::Rhai`; PR-2c-4 wired `Tool::Shell`;
        // PR-2c-5 wired `Tool::Http`.  The remaining stub kinds
        // (DuckDb, Playbook, Auth, Sink) still return empty.  Use
        // `Tool::DuckDb` here — PR-2c-6 fills it in next.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::DuckDb {
            db: ":memory:".into(),
            query: Some("SELECT 1".into()),
            params: vec![],
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert!(outcome.result.is_none());
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
}

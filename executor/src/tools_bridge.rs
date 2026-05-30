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
use noetl_tools::context::ExecutionContext as ToolsExecutionContext;
use noetl_tools::registry::{Tool as ToolsRegistryTool, ToolConfig};
use noetl_tools::result::{ToolResult, ToolStatus};
use noetl_tools::tools::RhaiTool;

use crate::playbook::{CmdsList, Tool};

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
        Tool::Shell { cmds } => (
            "shell",
            serde_json::json!({
                "cmds": cmds_to_value(cmds),
            }),
        ),
        Tool::Http {
            method,
            url,
            headers,
            params,
            body,
            auth: _, // CLI's AuthConfig handled at credential-resolver layer; PR-2c-5 decides whether to inline-pass.
        } => (
            "http",
            serde_json::json!({
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "body": body,
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

fn cmds_to_value(cmds: &CmdsList) -> serde_json::Value {
    match cmds {
        CmdsList::Single(s) => serde_json::Value::String(s.clone()),
        CmdsList::Multiple(v) => {
            serde_json::Value::Array(v.iter().map(|s| serde_json::Value::String(s.clone())).collect())
        }
    }
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
        Tool::Shell { .. } => {
            // PR-2c-4 fills this in.
            Ok(BridgeOutcome::empty())
        }
        Tool::Http { .. } => {
            // PR-2c-5 fills this in.
            Ok(BridgeOutcome::empty())
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
        assert_eq!(cfg.config["cmds"], serde_json::json!("ls -la"));
        assert!(cfg.timeout.is_none());
    }

    #[test]
    fn to_tools_config_shell_multiple_cmds() {
        let tool = Tool::Shell {
            cmds: CmdsList::Multiple(vec!["echo one".into(), "echo two".into()]),
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "shell");
        assert_eq!(
            cfg.config["cmds"],
            serde_json::json!(["echo one", "echo two"])
        );
    }

    #[test]
    fn to_tools_config_http_round_trips_essentials() {
        let tool = Tool::Http {
            method: "POST".into(),
            url: "https://example.com/api".into(),
            headers: HashMap::new(),
            params: HashMap::new(),
            body: Some(r#"{"k":"v"}"#.into()),
            auth: None,
        };
        let cfg = to_tools_config(&tool);
        assert_eq!(cfg.kind, "http");
        assert_eq!(cfg.config["method"], "POST");
        assert_eq!(cfg.config["url"], "https://example.com/api");
        assert_eq!(cfg.config["body"], r#"{"k":"v"}"#);
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
        // PR-2c-3 wired `Tool::Rhai`.  This test still exercises the
        // "unwired stub returns empty" branch using `Tool::Shell`,
        // which PR-2c-4 fills in next.
        let vars = empty_vars();
        let bridge = bridge_ctx(&vars);
        let tool = Tool::Shell {
            cmds: CmdsList::Single("echo hi".into()),
        };
        let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
        assert!(outcome.result.is_none());
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

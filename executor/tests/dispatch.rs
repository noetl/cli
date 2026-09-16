//! End-to-end integration tests for
//! [`noetl_executor::tools_bridge::dispatch_via_registry`].
//!
//! These tests exercise the full chain from CLI [`Tool`] variant →
//! [`to_tools_config`] → noetl-tools registry → result envelope
//! reshape → [`BridgeOutcome`].  Unit tests inside the bridge module
//! cover the individual helpers (config translation, envelope
//! reshaping, helper purity); these complement them with a single
//! external-tool round-trip per kind to catch breakage at the seams
//! between layers.
//!
//! Tools covered here: **Rhai**, **Shell**, **DuckDB**.  Tool::Http
//! integration testing needs an HTTP server target and is gated
//! behind manual smoke verification rather than CI — the unit tests
//! in `tools_bridge.rs` cover the config + envelope-reshape logic.
//! Tool::Auth / Tool::Sink stay inline per § H.10 and are exercised
//! via their helper unit tests in `tools_bridge.rs`; their dispatch
//! arms bail loudly which is asserted by the in-crate unit tests.
//!
//! Added in R-1.1 PR-2d as part of closing noetl/cli#19.

use std::collections::HashMap;

use noetl_executor::playbook::{CmdsList, Tool};
use noetl_executor::tools_bridge::{dispatch_via_registry, BridgeContext, BridgeOutcome};

fn empty_vars() -> HashMap<String, String> {
    HashMap::new()
}

fn ctx<'a>(vars: &'a HashMap<String, String>) -> BridgeContext<'a> {
    BridgeContext {
        execution_id: 999,
        step: "integration_test",
        variables: vars,
        server_url: String::new(),
        worker_id: None,
        command_id: None,
    }
}

// ---- Tool::Rhai -----------------------------------------------------

#[tokio::test]
async fn rhai_round_trip_evaluates_expression() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Rhai {
        code: "let x = 7; let y = 6; (x * y).to_string()".into(),
        args: HashMap::new(),
    };
    let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
    assert_eq!(outcome.result, Some("42".into()));
}

#[tokio::test]
async fn rhai_reads_workload_field_via_nested_scope() {
    // PR-2c-3 introduced `to_tools_context_for_rhai` so that flat
    // `workload.region` keys in the CLI variable map are reshaped
    // into a nested `workload.region` Rhai field access.
    let vars: HashMap<String, String> =
        [("workload.region".into(), "eu-west-2".into())].into();
    let bridge = ctx(&vars);
    let tool = Tool::Rhai {
        code: r#"workload.region.to_string()"#.into(),
        args: HashMap::new(),
    };
    let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
    assert_eq!(outcome.result, Some("eu-west-2".into()));
}

// ---- Tool::Shell ----------------------------------------------------

#[tokio::test]
async fn shell_single_command_returns_trimmed_stdout() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Shell {
        cmds: CmdsList::Single("echo hello-bridge".into()),
    };
    let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
    // Bridge trims the trailing newline `echo` adds so the
    // step-result string matches the CLI's pre-PR-2c-4 contract.
    assert_eq!(outcome.result, Some("hello-bridge".into()));
}

#[tokio::test]
async fn shell_multiple_returns_last_command_stdout() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Shell {
        cmds: CmdsList::Multiple(vec![
            "echo first".into(),
            "echo second".into(),
            "echo last".into(),
        ]),
    };
    let outcome = dispatch_via_registry(&tool, &bridge).await.unwrap();
    assert_eq!(outcome.result, Some("last".into()));
}

#[tokio::test]
async fn shell_nonzero_exit_propagates_error() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Shell {
        cmds: CmdsList::Single("exit 42".into()),
    };
    let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Command failed") || msg.contains("42"),
        "expected error to mention exit code: {msg}"
    );
}

// ---- Tool::DuckDb ---------------------------------------------------

// ⚠ These three need the DuckDB C++ engine, which sits behind the non-default
// `duckdb-integration` feature (noetl/ai-meta#185) precisely so ordinary builds
// skip the multi-hour libduckdb-sys compile.  Ungated, all three `unwrap()` a
// Configuration error saying the tool is not compiled in — the tool behaving
// correctly, reported as three test failures.  They were invisible until CI
// started selecting this package at all (see .github/workflows/test.yml in this
// change set).
//
// The obvious fix — `#[cfg(feature = "duckdb-integration")]` on each — is the
// wrong one: it stops COMPILING them without the feature, so they could rot
// silently and nobody would know until someone built the feature.  Instead they
// stay compiled unconditionally and branch on `cfg!()` at RUNTIME, and the
// no-engine branch is a real assertion rather than a skip: absence must be
// reported as a loud Configuration error naming the feature.  Silently
// returning an empty rows array instead would be the far worse failure, and
// nothing else in this suite would catch it.

/// Assert the no-engine outcome is an honest refusal, not a quiet empty answer.
/// Returns `true` when the engine is absent and the caller should stop.
fn duckdb_engine_absent(outcome: &anyhow::Result<BridgeOutcome>) -> bool {
    if cfg!(feature = "duckdb-integration") {
        return false;
    }
    let err = outcome
        .as_ref()
        .err()
        .expect("without the duckdb-integration feature the dispatch MUST fail loudly, not return a result");
    let msg = err.to_string();
    assert!(
        msg.contains("not compiled into this build") && msg.contains("duckdb-integration"),
        "the refusal must name the missing feature so the operator can act on it: {msg}"
    );
    true
}

#[tokio::test]
async fn duckdb_in_memory_select_returns_rows_array() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::DuckDb {
        db: ":memory:".into(),
        query: Some("SELECT 1 AS id, 'alpha' AS name UNION ALL SELECT 2, 'beta'".into()),
        params: vec![],
    };
    let outcome = dispatch_via_registry(&tool, &bridge).await;
    if duckdb_engine_absent(&outcome) {
        return;
    }
    let outcome = outcome.unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
    let arr = parsed.as_array().expect("result should be a JSON array");
    assert_eq!(arr.len(), 2);
    // Order is implementation-defined for UNION without ORDER BY;
    // verify by id rather than positional.
    let ids: Vec<i64> = arr
        .iter()
        .filter_map(|v| v["id"].as_i64())
        .collect();
    assert!(ids.contains(&1) && ids.contains(&2));
}

#[tokio::test]
async fn duckdb_in_memory_non_select_returns_status_ok() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::DuckDb {
        db: ":memory:".into(),
        query: Some("CREATE TABLE t (id INTEGER)".into()),
        params: vec![],
    };
    let outcome = dispatch_via_registry(&tool, &bridge).await;
    if duckdb_engine_absent(&outcome) {
        return;
    }
    let outcome = outcome.unwrap();
    // CLI's pre-PR-2c-6 envelope for non-SELECT was the literal
    // `{"status": "ok"}` string; the bridge preserves it.
    assert_eq!(outcome.result.as_deref(), Some(r#"{"status": "ok"}"#));
}

#[tokio::test]
async fn duckdb_select_empty_result_returns_empty_array() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::DuckDb {
        db: ":memory:".into(),
        query: Some("SELECT 1 AS id WHERE 1 = 0".into()),
        params: vec![],
    };
    let outcome = dispatch_via_registry(&tool, &bridge).await;
    if duckdb_engine_absent(&outcome) {
        return;
    }
    let outcome = outcome.unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(outcome.result.as_deref().unwrap()).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 0);
}

// ---- Bail-loudly paths (§ H.10) -------------------------------------

#[tokio::test]
async fn playbook_bridge_arm_bails_with_h10_message() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Playbook {
        path: "child.yaml".into(),
        args: HashMap::new(),
        input: HashMap::new(),
    };
    let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Tool::Playbook") && msg.contains("§ H.10"),
        "expected message to cite § H.10: {msg}"
    );
}

#[tokio::test]
async fn auth_bridge_arm_bails_pointing_at_helpers() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Auth {
        provider: "adc".into(),
        scopes: vec![],
        project: None,
    };
    let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("resolve_auth_to_bearer")
            && msg.contains("auth_context_updates"),
        "expected message to point at helpers: {msg}"
    );
}

#[tokio::test]
async fn sink_bridge_arm_bails_pointing_at_helper() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Sink {
        target: noetl_executor::playbook::SinkTarget::File {
            path: "/tmp/out.json".into(),
        },
        format: noetl_executor::playbook::SinkFormat::Json,
    };
    let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("format_sink_payload"),
        "expected message to point at helper: {msg}"
    );
}

#[tokio::test]
async fn unsupported_tool_bails() {
    let vars = empty_vars();
    let bridge = ctx(&vars);
    let tool = Tool::Unsupported;
    let err = dispatch_via_registry(&tool, &bridge).await.unwrap_err();
    assert!(err.to_string().contains("unsupported"));
}

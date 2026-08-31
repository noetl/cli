//! Pydantic-like YAML playbook types.
//!
//! Extracted from `repos/cli/src/playbook_runner.rs` lines 15-446 in
//! R-1.1 PR-2a per Appendix H of the global hybrid cloud blueprint.
//! These are the shape of a parsed `Playbook` YAML — the data model
//! that the executor (both CLI local-mode and worker NATS-mode)
//! operates on.
//!
//! ## What lives here
//!
//! - [`RuntimeCapabilities`] — feature/tool advertisement for the
//!   local vs distributed profiles.
//! - [`Playbook`] — top-level parsed playbook envelope.
//! - [`Step`], [`StepSpec`], [`Tool`], [`Loop config`] — workflow
//!   step shape.
//! - [`NextFormat`] + impl — v10 router AND legacy array routing
//!   normalisation.
//! - [`CaseCondition`], [`ThenBlock`], [`WhenCondition`] —
//!   conditional routing.
//! - [`SinkTarget`], [`SinkFormat`], [`AuthConfig`] — tool support
//!   types.
//!
//! ## What does NOT live here
//!
//! - `RunOutcome` — the CLI's JSON output envelope.  Stays in
//!   `playbook_runner.rs` because it's not a YAML input shape and
//!   the worker has its own output schema.
//! - `PlaybookRunner` — the orchestration shim.  R-1.1 PR-2d
//!   replaces its body with a call into the executor core; the
//!   public struct stays on the CLI side so `main.rs` continues to
//!   use it unchanged.
//! - `ExecutionContext` (lines 2420+) — a CLI-side per-step
//!   render context that R-1.1 PR-2b folds into
//!   [`crate::runtime::ExecutionContext`].

use serde::Deserialize;
use std::collections::HashMap;

/// Runtime capability set — defines what features a runtime supports.
#[derive(Debug, Clone, serde::Serialize, Deserialize)]
pub struct RuntimeCapabilities {
    pub runtime: String, // "local" or "distributed"
    pub version: String, // "noetl-runtime/1"
    pub tools: Vec<String>,
    pub features: Vec<String>,
}

impl RuntimeCapabilities {
    /// Local runtime capabilities.
    pub fn local() -> Self {
        Self {
            runtime: "local".to_string(),
            version: "noetl-runtime/1".to_string(),
            tools: vec![
                "shell".to_string(),
                "http".to_string(),
                "duckdb".to_string(),
                "rhai".to_string(),
                "playbook".to_string(),
                "auth".to_string(),
                "sink".to_string(),
            ],
            features: vec![
                "case_v1".to_string(),
                "case_v2".to_string(), // Rhai conditions
                "loop_v1".to_string(),
                "vars_v1".to_string(),
                "jinja2".to_string(),
            ],
        }
    }

    /// Distributed runtime capabilities.
    #[allow(dead_code)]
    pub fn distributed() -> Self {
        Self {
            runtime: "distributed".to_string(),
            version: "noetl-runtime/1".to_string(),
            tools: vec![
                "shell".to_string(),
                "http".to_string(),
                "postgres".to_string(),
                "duckdb".to_string(),
                "python".to_string(),
                "playbook".to_string(),
                "iterator".to_string(),
            ],
            features: vec![
                "case_v1".to_string(),
                "case_v2".to_string(),
                "loop_v1".to_string(),
                "loop_v2".to_string(), // Pagination
                "vars_v1".to_string(),
                "vars_v2".to_string(), // Cross-step results
                "sink_v1".to_string(),
                "jinja2".to_string(),
                "event_sourcing".to_string(),
            ],
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Playbook {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    #[allow(dead_code)]
    pub kind: String,
    pub metadata: Metadata,
    /// Runtime requirements and capabilities (8-char root key).
    #[serde(default)]
    pub executor: Option<Executor>,
    pub workload: Option<HashMap<String, serde_yaml::Value>>,
    /// Credential aliases this playbook resolves.  Entries stay untyped: the
    /// reference schema declares them as open objects, and their contents are
    /// keychain-resolver territory, not document structure.
    #[serde(default)]
    pub keychain: Option<Vec<serde_yaml::Value>>,
    /// Named reusable tasks referenced by `tool: {kind: workbook}` steps.
    #[serde(default)]
    pub workbook: Option<Vec<WorkbookTask>>,
    pub workflow: Vec<Step>,
}

/// Executor specification — runtime requirements and capabilities.
#[derive(Debug, Deserialize, Default)]
pub struct Executor {
    /// Runtime profile: local, distributed, or auto.
    #[serde(default = "default_profile")]
    pub profile: String,
    /// Semantic contract version: noetl-runtime/1.
    #[serde(default = "default_version")]
    pub version: String,
    /// Required capabilities.
    #[serde(default)]
    pub requires: Option<ExecutorRequires>,
    /// Executor spec for entry/final step configuration.
    #[serde(default)]
    pub spec: Option<ExecutorSpec>,
}

/// Executor spec for workflow entry and termination control.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ExecutorSpec {
    /// Override entry step (default: workflow[0]).
    #[serde(default)]
    pub entry_step: Option<String>,
    /// Optional finalization step run after quiescence.
    #[serde(default)]
    pub final_step: Option<String>,
    /// Treat "no next match" as error (default: false = branch terminates).
    #[serde(default)]
    pub no_next_is_error: Option<bool>,
    /// Execution-wide defaults, result handling and limits.
    #[serde(default)]
    pub policy: Option<ExecutorPolicy>,
}

pub fn default_profile() -> String {
    "auto".to_string()
}

pub fn default_version() -> String {
    "noetl-runtime/1".to_string()
}

/// Executor requirements.
#[derive(Debug, Deserialize, Default)]
pub struct ExecutorRequires {
    /// Required tool kinds.
    #[serde(default)]
    pub tools: Vec<String>,
    /// Required features.
    #[serde(default)]
    pub features: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct Metadata {
    pub name: String,
    #[allow(dead_code)]
    pub path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Step {
    pub step: String,
    pub desc: Option<String>,
    /// Step enablement guard — evaluated before step runs (canonical v2).
    #[serde(rename = "when")]
    pub when_guard: Option<String>,
    /// Step-level input data for cross-boundary propagation (DSL v2).
    #[serde(default)]
    pub input: Option<HashMap<String, serde_yaml::Value>>,
    pub tool: Option<Tool>,
    /// Next transitions — raw YAML, parsed manually to support both v10
    /// router and legacy formats.
    #[serde(default)]
    pub next: Option<serde_yaml::Value>,
    #[serde(rename = "case")]
    pub case: Option<Vec<CaseCondition>>,
    #[serde(rename = "loop")]
    #[allow(dead_code)]
    pub loop_config: Option<LoopConfig>,
    pub vars: Option<HashMap<String, String>>,
    /// Step spec for routing mode.
    #[serde(default)]
    pub spec: Option<StepSpec>,
}

/// Step specification for routing control.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct StepSpec {
    /// Routing mode: exclusive (default, first match) or inclusive (all matches).
    #[serde(default)]
    pub next_mode: Option<NextMode>,
    /// Step admission / lifecycle / failure / emit policy.
    #[serde(default)]
    pub policy: Option<StepPolicy>,
    /// Open in the reference schema, so it stays open here.
    #[serde(default)]
    pub timeout: Option<serde_yaml::Value>,
}

/// Next routing mode.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum NextMode {
    #[default]
    Exclusive,
    Inclusive,
}

/// V10 router spec for next transitions.
#[derive(Debug, Clone)]
pub struct NextRouterSpec {
    pub mode: Option<String>,
}

/// V10 arc for router format.
#[derive(Debug, Clone)]
pub struct NextArc {
    pub step: String,
    pub when_condition: Option<String>,
    pub args: Option<HashMap<String, serde_yaml::Value>>,
}

/// Next format — supports both v10 router and legacy array formats.
#[derive(Debug)]
pub enum NextFormat {
    /// V10 router format: `{ spec: { mode: ... }, arcs: [...] }`.
    Router {
        spec: Option<NextRouterSpec>,
        arcs: Vec<NextArc>,
    },
    /// Legacy array format: `[{ step: ... }, ...]`.
    Array(Vec<NextStep>),
}

impl NextFormat {
    /// Parse next field from `serde_yaml::Value`.
    pub fn from_yaml_value(value: &serde_yaml::Value) -> Option<NextFormat> {
        match value {
            serde_yaml::Value::Sequence(_arr) => {
                // Legacy array format.
                let steps: Vec<NextStep> = serde_yaml::from_value(value.clone()).ok()?;
                Some(NextFormat::Array(steps))
            }
            serde_yaml::Value::Mapping(map) => {
                // V10 router format: { spec: { mode: ... }, arcs: [...] }
                let spec = map.get(&serde_yaml::Value::String("spec".to_string())).and_then(|v| {
                    if let serde_yaml::Value::Mapping(spec_map) = v {
                        let mode = spec_map
                            .get(&serde_yaml::Value::String("mode".to_string()))
                            .and_then(|m| m.as_str().map(|s| s.to_string()));
                        Some(NextRouterSpec { mode })
                    } else {
                        None
                    }
                });

                let arcs = map.get(&serde_yaml::Value::String("arcs".to_string())).and_then(|v| {
                    if let serde_yaml::Value::Sequence(arcs_arr) = v {
                        let arcs: Vec<NextArc> = arcs_arr
                            .iter()
                            .filter_map(|arc_val| {
                                if let serde_yaml::Value::Mapping(arc_map) = arc_val {
                                    let step = arc_map
                                        .get(&serde_yaml::Value::String("step".to_string()))
                                        .and_then(|s| s.as_str().map(|s| s.to_string()))?;
                                    let when_condition = arc_map
                                        .get(&serde_yaml::Value::String("when".to_string()))
                                        .and_then(|w| w.as_str().map(|s| s.to_string()));
                                    let args = arc_map
                                        .get(&serde_yaml::Value::String("args".to_string()))
                                        .and_then(|a| serde_yaml::from_value(a.clone()).ok());
                                    Some(NextArc {
                                        step,
                                        when_condition,
                                        args,
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect();
                        Some(arcs)
                    } else {
                        None
                    }
                })?;

                Some(NextFormat::Router { spec, arcs })
            }
            _ => None,
        }
    }
}

/// Then block can be either a list of actions or a single action dict.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ThenBlock {
    /// Single action object (backwards compatible).
    Single(serde_yaml::Value),
    /// List of action objects.
    List(Vec<NextStep>),
}

#[derive(Debug, Deserialize)]
pub struct CaseCondition {
    #[serde(flatten)]
    pub when: WhenCondition,
    pub then: ThenBlock,
    #[serde(rename = "else")]
    pub else_steps: Option<Vec<NextStep>>,
}

/// Condition that can be either a simple template string or Rhai code.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WhenCondition {
    /// Rhai expression for complex conditions.
    Rhai {
        #[serde(alias = "when_rhai")]
        rhai: String,
    },
    /// Simple template string condition (Jinja2-style).
    Simple { when: String },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Tool {
    Shell {
        #[serde(default)]
        cmds: CmdsList,
    },
    Http {
        #[serde(default = "default_method")]
        method: String,
        url: String,
        #[serde(default)]
        headers: HashMap<String, String>,
        #[serde(default)]
        params: HashMap<String, String>,
        body: Option<String>,
        #[serde(default)]
        auth: Option<AuthConfig>,
    },
    Playbook {
        path: String,
        /// Legacy args field (DSL v1) — deprecated in favor of input.
        #[serde(default)]
        args: HashMap<String, String>,
        /// Canonical input field (DSL v2) — takes precedence over args.
        #[serde(default)]
        input: HashMap<String, serde_yaml::Value>,
    },
    #[serde(rename = "duckdb")]
    DuckDb {
        #[serde(default = "default_duckdb_path")]
        db: String,
        query: Option<String>,
        #[serde(default)]
        params: Vec<String>,
    },
    Auth {
        provider: String,
        #[serde(default)]
        scopes: Vec<String>,
        #[serde(default)]
        project: Option<String>,
    },
    Sink {
        target: SinkTarget,
        #[serde(default)]
        format: SinkFormat,
    },
    Rhai {
        code: String,
        #[serde(default)]
        args: HashMap<String, String>,
    },
    /// Cloud provider operation (`kind: provider`) — dispatched through the
    /// noetl-tools `ProviderTool`.  The CLI captures the provider block's
    /// fields loosely (`serde_yaml::Value` for the nested / polymorphic ones)
    /// and hands the assembled, template-rendered config to the tool, which
    /// owns the action grammar, plan/apply, and LRO polling.  Local mode runs
    /// the same tool the distributed worker does — see `tools_bridge`.
    Provider {
        provider: String,
        #[serde(default)]
        runtime: Option<String>,
        action: String,
        #[serde(default)]
        service: Option<String>,
        /// `bool` or a template string (`"{{ ... }}"`) — resolved by the tool.
        #[serde(default)]
        dry_run: Option<serde_yaml::Value>,
        #[serde(default)]
        input: Option<serde_yaml::Value>,
        #[serde(default)]
        poll: Option<serde_yaml::Value>,
        /// Config-level API endpoint override (Round 3) — testing / emulators
        /// only.  A base URL string or `{crm,billing,serviceusage}` object; lets
        /// a playbook be validated offline against wiremock / an emulator.
        #[serde(default)]
        endpoint: Option<serde_yaml::Value>,
        /// Ownership / stack label (Round 3, Fork 1) — scopes the resource
        /// ownership + drift + orphan projection.
        #[serde(default)]
        stack: Option<String>,
        /// Destroy / adopt confirmation digest (Round 3 destroy + Round 4 adopt)
        /// — required to apply a destroy verb or an `adopt`; must equal the
        /// `plan_digest` from a reviewed dry-run.
        #[serde(default)]
        confirm: Option<String>,
        /// Reconciliation policy (Round 4) — `report` (default) / `enforce` /
        /// `adopt`.  Governs how a drifted mutating ensure action is handled.
        #[serde(default)]
        reconcile: Option<String>,
        /// Last-known-desired spec for this resource's URN (Round 4), supplied by
        /// the caller's EHDB raw-eventlog-tier fold.  Used by `report` / `adopt`
        /// to compute the desired-vs-actual diff; absent → untracked / import.
        #[serde(default)]
        known_desired: Option<serde_yaml::Value>,
        /// Multi-org / multi-billing wrong-target guard (Stage-1 safety) —
        /// `{ require_org, require_org_display_name, require_billing_account }`.
        /// Pins the organization + billing account a run may touch; a mismatch
        /// is refused structurally (offline) and, in apply mode, live.
        #[serde(default)]
        guard: Option<serde_yaml::Value>,
        /// Provider auth block (apply mode only).  Captured raw and mapped to
        /// the noetl-tools `AuthConfig` at dispatch; dry-run ignores it.
        #[serde(default)]
        auth: Option<serde_yaml::Value>,
    },
    #[serde(other)]
    Unsupported,
}

pub fn default_method() -> String {
    "GET".to_string()
}

pub fn default_duckdb_path() -> String {
    ".noetl/state.duckdb".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    /// Auth provider type: adc (Application Default Credentials), token, basic.
    #[serde(alias = "source")]
    pub provider: String,
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkTarget {
    File {
        path: String,
    },
    #[serde(rename = "duckdb")]
    DuckDb {
        db: String,
        table: String,
    },
    Gcs {
        bucket: String,
        path: String,
    },
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SinkFormat {
    #[default]
    Json,
    Yaml,
    Csv,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CmdsList {
    Single(String),
    Multiple(Vec<String>),
}

impl Default for CmdsList {
    fn default() -> Self {
        CmdsList::Multiple(vec![])
    }
}

/// Next step definition — supports canonical v2 format.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum NextStep {
    /// Canonical v2 format: `{ step: "name", when: "condition", args: {...} }`.
    Canonical {
        step: String,
        #[serde(rename = "when")]
        when_condition: Option<String>,
        #[serde(default)]
        args: Option<HashMap<String, serde_yaml::Value>>,
    },
    /// Legacy conditional: `{ when: "condition", then: [...] }`.
    Conditional { when: Option<String>, then: Vec<NextStep> },
    /// Legacy next action: `{ next: [...] }`.
    NextAction { next: Vec<NextStep> },
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct LoopConfig {
    /// The collection to iterate.  OPTIONAL: a `mode: cursor` loop draws from
    /// `cursor:` instead, and this field being required previously made such a
    /// loop fail to parse.
    #[serde(default, rename = "in")]
    pub in_collection: Option<String>,
    pub iterator: String,
    /// Legacy top-level shorthand for `spec.mode`.
    #[serde(default)]
    pub mode: Option<String>,
    /// Cursor source for `mode: cursor`.
    #[serde(default)]
    pub cursor: Option<CursorSpec>,
    /// Loop mode, concurrency, policy and frame sizing.
    #[serde(default)]
    pub spec: Option<LoopSpec>,
}

// ---------------------------------------------------------------------------
// Playbook document model — the policy / output / loop surface.
//
// Everything below describes parts of a playbook that this crate previously
// accepted only as untyped YAML, or dropped on the floor: `policy:` blocks,
// tool `output:`, cursor loops, the `next:` router, `keychain:` and
// `workbook:`.  They are typed here so there is ONE model of what a playbook
// may contain rather than several partial ones, and so a JSON Schema can be
// derived from it (noetl/ai-meta#201).
//
// Grounded in two sources, not invented:
//   * the v10 Pydantic models this replaces, captured in noetl/ai-meta at
//     playbooks/dsl-schema-rust/python-model-reference/ before they were
//     deleted, and the JSON Schema they generated (draft 2020-12, 24 $defs);
//   * what this crate's parser already accepts, which is why `Step` keeps
//     `when` / `case` / `vars` -- fields the Python spec never described but
//     real playbooks use.
//
// Deliberately PERMISSIVE.  No `deny_unknown_fields` anywhere, matching both
// the previous model and the reference schema, whose `additionalProperties`
// was unset in all 24 definitions.  Adding these types therefore cannot make
// a playbook that parses today stop parsing: every field is optional and
// unknown keys are still ignored.
//
// Free-form regions stay `serde_yaml::Value` on purpose.  `rules`, `limits`,
// `lifecycle`, `failure` and `emit` are open-ended in the reference schema
// too (`Vec<Map>` / `Map`); typing them further would claim a structure the
// specification does not define.
// ---------------------------------------------------------------------------

/// Admission control for a step: which rules gate entry, and whether the
/// first match wins (`exclusive`) or all matching rules apply (`inclusive`).
#[derive(Debug, Deserialize, Clone, Default)]
pub struct AdmitPolicy {
    #[serde(default)]
    pub mode: Option<MatchMode>,
    #[serde(default)]
    pub rules: Vec<serde_yaml::Value>,
}

/// Match semantics shared by `AdmitPolicy`, `TaskPolicy` and `NextSpec`.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MatchMode {
    /// First matching rule wins.
    #[default]
    Exclusive,
    /// Every matching rule applies.
    Inclusive,
}

/// Step-level policy block.  Only `admit` has a defined shape in the
/// reference schema; the rest are open maps there and stay open here.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct StepPolicy {
    #[serde(default)]
    pub admit: Option<AdmitPolicy>,
    #[serde(default)]
    pub lifecycle: Option<serde_yaml::Value>,
    #[serde(default)]
    pub failure: Option<serde_yaml::Value>,
    #[serde(default)]
    pub emit: Option<serde_yaml::Value>,
}

/// Task-level policy: rule evaluation plus the before/after/finally hooks.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct TaskPolicy {
    #[serde(default)]
    pub mode: Option<MatchMode>,
    #[serde(default)]
    pub on_unmatched: Option<OnUnmatched>,
    #[serde(default)]
    pub rules: Vec<serde_yaml::Value>,
    #[serde(default)]
    pub before: Option<Vec<serde_yaml::Value>>,
    #[serde(default)]
    pub after: Option<Vec<serde_yaml::Value>>,
    /// `finally` is a Rust keyword-adjacent name; the wire key is unchanged.
    #[serde(default, rename = "finally")]
    pub finally_: Option<Vec<serde_yaml::Value>>,
}

/// What to do when no task rule matches.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OnUnmatched {
    #[default]
    Continue,
    Fail,
}

/// Per-task spec carried under `tool.spec`.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct TaskSpec {
    #[serde(default)]
    pub timeout: Option<serde_yaml::Value>,
    #[serde(default)]
    pub policy: Option<TaskPolicy>,
}

/// Executor-level policy block — all three members are open maps in the
/// reference schema.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct ExecutorPolicy {
    #[serde(default)]
    pub defaults: Option<serde_yaml::Value>,
    #[serde(default)]
    pub results: Option<serde_yaml::Value>,
    #[serde(default)]
    pub limits: Option<serde_yaml::Value>,
}

/// Where a tool's result is stored.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct OutputStore {
    #[serde(default)]
    pub kind: Option<OutputStoreKind>,
    #[serde(default)]
    pub driver: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub ttl: Option<String>,
    #[serde(default)]
    pub compression: Option<Compression>,
    /// Keychain alias, never an inline secret.
    #[serde(default)]
    pub credential: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OutputStoreKind {
    #[default]
    Auto,
    Memory,
    Kv,
    Disk,
    Object,
    S3,
    Gcs,
    Db,
    #[serde(rename = "duckdb")]
    DuckDb,
    #[serde(rename = "eventlog")]
    EventLog,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Lz4,
}

/// Extract one value out of a tool result and bind it to a name.
#[derive(Debug, Deserialize, Clone)]
pub struct OutputSelect {
    /// JSONPath into the result, e.g. `$.data.next`.
    pub path: String,
    /// Variable the extracted value is bound to.
    #[serde(rename = "as")]
    pub as_: String,
}

/// Result accumulation across loop iterations.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct OutputAccumulate {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub strategy: Option<AccumulateStrategy>,
    #[serde(default)]
    pub merge_path: Option<String>,
    #[serde(default)]
    pub manifest_as: Option<String>,
    #[serde(default)]
    pub on_success: Option<bool>,
    #[serde(default)]
    pub on_error: Option<bool>,
    #[serde(default)]
    pub max_items: Option<i64>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AccumulateStrategy {
    #[default]
    Append,
    Replace,
    Merge,
    Concat,
}

/// Lifetime of a stored tool result.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OutputScope {
    Step,
    #[default]
    Execution,
    Workflow,
    Permanent,
}

/// A tool's `output:` block.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct ToolOutput {
    #[serde(default)]
    pub store: Option<OutputStore>,
    #[serde(default)]
    pub select: Option<Vec<OutputSelect>>,
    #[serde(default)]
    pub accumulate: Option<OutputAccumulate>,
    #[serde(default)]
    pub inline_max_bytes: Option<i64>,
    #[serde(default)]
    pub preview_max_bytes: Option<i64>,
    #[serde(default)]
    pub scope: Option<OutputScope>,
    #[serde(default, rename = "as")]
    pub as_: Option<String>,
}

/// Frame sizing and leasing for a `mode: cursor` loop.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct FramePolicy {
    #[serde(default)]
    pub max_rows: Option<i64>,
    #[serde(default)]
    pub max_seconds: Option<f64>,
    #[serde(default)]
    pub max_bytes: Option<i64>,
    #[serde(default)]
    pub lease_seconds: Option<f64>,
    #[serde(default)]
    pub heartbeat_seconds: Option<f64>,
    #[serde(default)]
    pub row_concurrency: Option<i64>,
    #[serde(default)]
    pub process: Option<FrameProcess>,
    #[serde(default)]
    pub verify_ipc: Option<bool>,
    #[serde(default)]
    pub retry_mode: Option<String>,
    #[serde(default)]
    pub max_attempts: Option<i64>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FrameProcess {
    #[default]
    Row,
    Frame,
}

/// Where loop iterations execute.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct LoopPolicy {
    #[serde(default)]
    pub exec: Option<LoopExec>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LoopExec {
    Distributed,
    #[default]
    Local,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LoopMode {
    #[default]
    Sequential,
    Parallel,
    Cursor,
}

/// A cursor source for `mode: cursor` loops.
#[derive(Debug, Deserialize, Clone)]
pub struct CursorSpec {
    pub kind: String,
    /// Keychain alias.
    pub auth: String,
    pub claim: String,
    #[serde(default)]
    pub options: Option<serde_yaml::Value>,
}

/// The `spec:` block of a loop.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct LoopSpec {
    #[serde(default)]
    pub mode: Option<LoopMode>,
    /// Either a number or a template string, so it stays loose.
    #[serde(default)]
    pub max_in_flight: Option<serde_yaml::Value>,
    #[serde(default)]
    pub policy: Option<LoopPolicy>,
    #[serde(default)]
    pub frame: Option<FramePolicy>,
}

/// One `next:` arc.
#[derive(Debug, Deserialize, Clone)]
pub struct Arc {
    pub step: String,
    #[serde(default)]
    pub when: Option<String>,
    #[serde(default)]
    pub set: Option<serde_yaml::Value>,
    #[serde(default)]
    pub spec: Option<serde_yaml::Value>,
}

/// Routing behaviour for a `next:` block.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct NextSpec {
    #[serde(default)]
    pub mode: Option<MatchMode>,
    #[serde(default)]
    pub on_no_match: Option<OnNoMatch>,
    #[serde(default)]
    pub policy: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OnNoMatch {
    #[default]
    Complete,
    Quiet,
}

/// The v10 `next:` router form.  `Step::next` stays `serde_yaml::Value`
/// because the legacy shorthand forms are still accepted; this type
/// describes the structured form for the schema and for callers that want
/// it typed.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct NextRouter {
    #[serde(default)]
    pub spec: Option<NextSpec>,
    #[serde(default)]
    pub arcs: Vec<Arc>,
}

/// A named, reusable task in the `workbook:` list.
#[derive(Debug, Deserialize, Clone)]
pub struct WorkbookTask {
    pub name: String,
    pub tool: serde_yaml::Value,
}
